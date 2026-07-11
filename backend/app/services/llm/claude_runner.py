"""
Claude runner — гибридный пайплайн: Claude CLI + OpenRouter (llm_runner).

Все промпты — на английском (EN шаблоны из .claude/en/).
Claude CLI этапы используют Read/Write tool инструкции в промптах.
OpenRouter этапы — prompt_builder._clean_template_for_api() убирает CLI-инструкции.

Этапы через Claude CLI (Read/Write tools):
  - findings_merge   (Opus)
  - norm_verify      (Sonnet)
  - norm_fix         (Sonnet)
  - optimization     (Opus)

Этапы через OpenRouter (LLM API):
  - text_analysis    (GPT-5.4)
  - block_batch      (Gemini 3.1 Pro)
  - optimization_critic (GPT-5.4)
  - optimization_corrector (GPT-5.4)

Проверка замечаний (findings_critic/findings_corrector) вынесена в отдельный этап
«Верификатор» (stages/findings_verify) — детерминированные структурные проверки без
LLM-фильтра. Прежние LLM-обёртки run_findings_critic/corrector удалены.

Совместимость: pipeline_service ожидает сигнатуру (exit_code, text, result).
CLIResult и LLMResult имеют property-совместимость (result_text, session_id, num_turns, etc.)
"""
import json
import logging
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Callable, Awaitable, Sequence, Union

from backend.app.core.config import (
    CLAUDE_CLI,
    get_claude_cli,
    get_model_for_stage,
    TEXT_ANALYSIS_TOOLS, FINDINGS_MERGE_TOOLS, NORM_VERIFY_TOOLS,
    CLAUDE_TEXT_ANALYSIS_TIMEOUT, CLAUDE_FINDINGS_MERGE_TIMEOUT,
    CLAUDE_NORM_VERIFY_TIMEOUT, CLAUDE_NORM_FIX_TIMEOUT, CLAUDE_NORM_REQUOTE_TIMEOUT,
    CLAUDE_OPTIMIZATION_TIMEOUT,
    get_stage_model, is_claude_stage, is_codex_model, is_codex_stage, is_local_llm_model,
)

# Локальный GEMMA иногда отвергает слишком большие PNG ("Invalid image detected").
# На ошибке повторяем тот же single-block запрос с последовательно уменьшенными
# копиями картинки. scale = target_dpi / GEMMA_CROP_DPI (источник = 300 DPI).
_LOCAL_GEMMA_BLOCK_DPI_FALLBACKS: list[tuple[int, float]] = [
    (300, 1.0),
    (200, 200 / 300),
    (100, 100 / 300),
]
from backend.app.services.common.cli_utils import (
    is_cancelled, is_timeout, is_rate_limited,
    is_prompt_too_long,
    parse_rate_limit_reset, parse_cli_json_output, send_output,
)
from backend.app.pipeline.stages.prepare.task_builder import (
    prepare_norm_verify_task,
    prepare_norm_fix_task,
    prepare_norm_requote_task,
    prepare_optimization_task,
    prepare_text_analysis_task,
    prepare_block_batch_task,
    prepare_findings_merge_task,
    prepare_optimization_critic_task,
    prepare_optimization_corrector_task,
    prepare_tile_batch_task,
    prepare_main_audit_task,
    prepare_triage_task,
)
from backend.app.models.usage import CLIResult, LLMResult

logger = logging.getLogger(__name__)

# Тип результата — или CLIResult (Claude CLI), или LLMResult (OpenRouter)
AnyResult = Union[CLIResult, LLMResult]


def _is_agent_cli_stage(stage: str) -> bool:
    """True when a stage should run through an agent CLI transport."""
    return is_claude_stage(stage) or is_codex_stage(stage)


# ═══════════════════════════════════════════════════════════════════════════
# Audit Trail — сохранение промежуточных результатов LLM
# ═══════════════════════════════════════════════════════════════════════════

def _save_audit_trail(
    project_id: str,
    stage: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    duration_ms: int,
    result_data,
    *,
    output_dir: str | Path | None = None,
):
    """Сохранить копию результата LLM-вызова в _output/audit_trail/.

    Основные файлы в _output/ остаются для пайплайна,
    audit_trail/ хранит полную историю с метками времени.
    """
    from datetime import datetime

    try:
        trail_dir = _resolve_output_dir(project_id, output_dir=output_dir) / "audit_trail"
        trail_dir.mkdir(parents=True, exist_ok=True)

        now = datetime.now()
        timestamp_file = now.strftime("%Y-%m-%dT%H-%M-%S")
        filename = f"{stage}_{timestamp_file}.json"

        trail_data = {
            "stage": stage,
            "model": model,
            "timestamp": now.isoformat(),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "duration_ms": duration_ms,
            "result": result_data,
        }

        (trail_dir / filename).write_text(
            json.dumps(trail_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.debug("Audit trail saved: %s/%s", project_id, filename)
    except Exception:
        logger.warning("Failed to save audit trail for %s/%s", project_id, stage, exc_info=True)


def _build_llm_audit_payload(result: LLMResult) -> dict:
    """Сохранить не только распарсенный JSON, но и сырой ответ local/OpenRouter LLM."""
    return {
        "json_data": result.json_data,
        "raw_text": result.text,
        "is_error": result.is_error,
        "error_message": result.error_message,
        "finish_reason": result.finish_reason,
        "response_id": result.response_id,
        "reasoning_tokens": result.reasoning_tokens,
        "cost_source": result.cost_source,
    }

__all__ = [
    # cli_utils
    "is_cancelled", "is_timeout", "is_rate_limited",
    "parse_rate_limit_reset", "parse_cli_json_output",
    # task_builder
    "prepare_norm_verify_task", "prepare_norm_fix_task",
    "prepare_optimization_task",
    # runners
    "run_norm_verify", "run_norm_fix", "run_norm_requote",
    "run_optimization",
    # runners — блоковый пайплайн
    "run_text_analysis", "run_block_batch", "run_findings_merge",
    # runners — optimization review
    "run_optimization_critic", "run_optimization_corrector",
    # task_builder — блоковый пайплайн
    "prepare_text_analysis_task", "prepare_block_batch_task",
    "prepare_findings_merge_task",
    # legacy stubs (перенаправляют на новый пайплайн)
    "prepare_tile_batch_task", "prepare_main_audit_task",
    "prepare_triage_task",
    "run_tile_batch", "run_main_audit", "run_triage",
]


# ═══════════════════════════════════════════════════════════════════════════
# Claude CLI — вспомогательные функции
# ═══════════════════════════════════════════════════════════════════════════

def _build_cmd(tools: str, model: str | None = None) -> list[str]:
    """Собрать команду запуска Claude CLI."""
    resolved_model = model or get_model_for_stage("default")
    return [
        get_claude_cli(), "-p",
        "--model", resolved_model,
        "--allowedTools", tools,
        "--output-format", "json",
    ]


# Чистая cwd для запуска `claude -p` без подгрузки project CLAUDE.md / hooks / memory / skills.
# Эмпирически (КЖ5.1, 25 блоков) даёт −42% input/блок и −36% cli_cost при +35% findings —
# Sonnet работает прицельнее без harness'а Claude Code в качестве distractor'а.
# См. ideas.md (Идея 6) и memory/feedback_subscription_only.md.
_CLEAN_CWD_PATH = "/tmp/sonnet_clean"
_CLEAN_ENV_KEEP = {"HOME", "PATH", "LANG", "LC_ALL", "USER", "SHELL"}


def _ensure_clean_cwd() -> str:
    """Создать (если нужно) и очистить /tmp/sonnet_clean. Возвращает путь."""
    p = _CLEAN_CWD_PATH
    os.makedirs(p, exist_ok=True)
    # Чистим всё, что туда могло попасть от прошлых запусков (output JSON и пр.)
    for entry in os.listdir(p):
        full = os.path.join(p, entry)
        if os.path.isfile(full):
            try:
                os.unlink(full)
            except OSError:
                pass
    return p


def _build_clean_env_overrides() -> dict:
    """Построить env_overrides, удаляющий ВСЕ переменные кроме базовых
    (HOME/PATH/LANG/LC_ALL/USER/SHELL/XDG_*). Это исключает project memory,
    skills manifest и прочие context-dependent артефакты Claude CLI.
    """
    overrides = {}
    for k in os.environ:
        if k in _CLEAN_ENV_KEEP or k.startswith("XDG_"):
            continue
        overrides[k] = None
    return overrides


async def _run_cli(
    task_text: str,
    tools: str,
    timeout: int,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    stage: str = "",
    project_id: str = "",
    model: str | None = None,
    clean_cwd: bool = False,
    image_paths: Sequence[str | Path] | None = None,
) -> tuple[int, str, CLIResult]:
    """Запустить agent CLI с задачей через stdin, вернуть (exit_code, output, CLIResult).

    Claude CLI записывает результаты через Write tool (файлы). Codex exec получает
    тот же task_text и пишет требуемые JSON-файлы через filesystem-доступ.

    clean_cwd=True применяется только к Claude CLI. Для Codex exec рабочая папка
    должна оставаться корнем проекта, чтобы workspace-write sandbox видел output.
    """
    if is_codex_model(model):
        from backend.app.services.llm.codex_runner import run_codex_exec
        return await run_codex_exec(
            task_text,
            timeout=timeout,
            on_output=on_output,
            stage=stage,
            project_id=project_id,
            model=model,
            image_paths=image_paths,
        )

    from backend.app.services.common.process_runner import run_command

    cmd = _build_cmd(tools, model)

    if clean_cwd:
        env_overrides = _build_clean_env_overrides()
        cwd_arg = _ensure_clean_cwd()
    else:
        # Очистить все CLAUDE* переменные окружения, чтобы вложенный CLI
        # не думал что он внутри другой сессии
        env_overrides = {k: None for k in os.environ if k.startswith("CLAUDE")}
        cwd_arg = None

    exit_code, stdout, stderr = await run_command(
        cmd,
        input_text=task_text,
        timeout=timeout,
        on_output=on_output,
        env_overrides=env_overrides,
        cwd=cwd_arg,
        project_id=project_id,
    )

    combined = (stdout or "") + "\n" + (stderr or "")
    cli_result = parse_cli_json_output(stdout or "")

    return exit_code, combined, cli_result


# ═══════════════════════════════════════════════════════════════════════════
# OpenRouter — вспомогательные функции
# ═══════════════════════════════════════════════════════════════════════════

async def _send_status_llm(on_output, result: LLMResult):
    """Отправить статус OpenRouter вызова в live-log."""
    if result.is_error:
        status = f"[ERROR] {result.error_message}"
    else:
        status = f"[{result.model}] {result.input_tokens}->{result.output_tokens} tok, {result.duration_ms}ms"
    await send_output(on_output, status)


def _write_json(path, data):
    """Записать JSON атомарно (tmp + os.replace, с автосозданием директории).

    Пишет и мастер-файлы (03_findings.json): kill процесса посреди прямого
    write_text оставлял обрезанный/пустой JSON — как у MD в gemma_enrich.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(tmp, path)


def _resolve_output_dir(project_id: str, output_dir: str | Path | None = None):
    """Получить _output/ директорию активной версии проекта.

    Использует `bind_version()` ContextVar (выставляется на старте каждого
    pipeline job в `_run_batch_queue`) или latest_version_id.
    """
    if output_dir:
        return Path(output_dir)
    env_output_dir = os.environ.get("AUDIT_OUTPUT_DIR")
    if env_output_dir:
        return Path(env_output_dir)

    from backend.app.services.common import version_service
    from backend.app.services.common.project_service import resolve_project_dir
    try:
        return version_service.resolve_version_output_dir(project_id)
    except (version_service.VersionNotFoundError, FileNotFoundError):
        return resolve_project_dir(project_id) / "_output"


@contextmanager
def _scoped_audit_paths(
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    project_id: str | None = None,
    version_id: str | None = None,
):
    scoped_env = {}
    if output_dir is not None:
        scoped_env["AUDIT_OUTPUT_DIR"] = str(output_dir)
    if version_dir is not None:
        scoped_env["AUDIT_VERSION_DIR"] = str(version_dir)
    if project_id is not None:
        scoped_env["AUDIT_PROJECT_ID"] = str(project_id)
    if version_id is not None:
        scoped_env["AUDIT_VERSION_ID"] = str(version_id)

    previous = {key: os.environ.get(key) for key in scoped_env}
    os.environ.update(scoped_env)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


async def _run_codex_json_stage(
    *,
    stage: str,
    messages: list[dict],
    model: str,
    timeout: int,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]],
    output_filename: str,
    audit_stage: str,
    output_dir: str | Path | None = None,
) -> tuple[int, str, LLMResult]:
    """Run Codex exec in JSON-only mode and let backend write the artifact."""
    from backend.app.services.llm.codex_runner import run_codex_json_messages

    result = await run_codex_json_messages(
        messages,
        timeout=timeout,
        on_output=on_output,
        stage=stage,
        project_id=project_id,
        model=model,
    )

    if result.json_data is not None and not result.is_error:
        output_path = _resolve_output_dir(project_id, output_dir=output_dir) / output_filename
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, audit_stage, result.model or model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, _build_llm_audit_payload(result),
        output_dir=output_dir,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


def _codex_targeted_findings_enabled() -> bool:
    raw = os.environ.get("AUDIT_CODEX_TARGETED_FINDINGS", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _codex_optimization_images_enabled() -> bool:
    raw = os.environ.get("AUDIT_CODEX_OPTIMIZATION_IMAGES", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


async def _run_codex_targeted_findings_merge(
    *,
    project_info: dict,
    project_id: str,
    model: str,
    base_result: LLMResult,
    base_text: str,
    on_output: Optional[Callable[[str], Awaitable[None]]],
    output_dir: str | Path | None,
    version_dir: str | Path | None,
    version_id: str | None,
) -> tuple[int, str, LLMResult]:
    """Run optional Codex targeted findings passes and rewrite 03_findings.json.

    The base Codex merge remains available as ``03_findings_codex_base.json``.
    Targeted pass failures are non-fatal: the base merge is already valid and
    should continue through the pipeline.
    """
    if not _codex_targeted_findings_enabled() or base_result.is_error:
        return (0 if not base_result.is_error else 1), base_text, base_result

    from backend.app.pipeline.stages.prepare.codex_targeted_findings import (
        build_targeted_findings_passes,
        combine_findings_with_targeted,
        json_dumps,
    )

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        passes = build_targeted_findings_passes(project_info, project_id)
    if not passes:
        return 0, base_text, base_result

    resolved_output_dir = _resolve_output_dir(project_id, output_dir=output_dir)
    base_data = base_result.json_data
    if not isinstance(base_data, dict):
        try:
            base_data = json.loads((resolved_output_dir / "03_findings.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return 0, base_text, base_result
    if isinstance(base_data, list):
        # Codex вернул массив findings вместо объекта. Без ремонта combine ниже
        # выбросил бы ВСЮ базу (`dict(production) if isinstance(...) else {"findings": []}`),
        # и мастер-файл остался бы только с targeted-замечаниями. Чиним схему на
        # месте и сразу нормализуем мастер-файл (downstream ждёт dict).
        base_data = {"findings": base_data}
        _write_json(resolved_output_dir / "03_findings.json", base_data)
    if not isinstance(base_data, dict):
        return 0, base_text, base_result

    _write_json(resolved_output_dir / "03_findings_codex_base.json", base_data)

    targeted_payloads: list[tuple[str, dict]] = []
    combined_text_parts = [base_text or ""]
    try:
        targeted_timeout = int(os.environ.get("AUDIT_CODEX_TARGETED_TIMEOUT", "900"))
    except ValueError:
        logger.warning("AUDIT_CODEX_TARGETED_TIMEOUT не число — использую 900с")
        targeted_timeout = 900
    total_duration_ms = base_result.duration_ms
    total_output_tokens = base_result.output_tokens
    total_input_tokens = base_result.input_tokens

    for targeted_pass in passes:
        exit_code, targeted_text, targeted_result = await _run_codex_json_stage(
            stage=targeted_pass.stage,
            messages=targeted_pass.messages,
            model=model,
            timeout=targeted_timeout,
            project_id=project_id,
            on_output=on_output,
            output_filename=targeted_pass.output_filename,
            audit_stage=f"03_findings_targeted_{targeted_pass.stage}",
            output_dir=output_dir,
        )
        total_duration_ms += targeted_result.duration_ms
        total_output_tokens += targeted_result.output_tokens
        total_input_tokens += targeted_result.input_tokens
        if targeted_text:
            combined_text_parts.append(targeted_text)
        if exit_code == 0 and isinstance(targeted_result.json_data, dict):
            targeted_payloads.append((targeted_pass.stage, targeted_result.json_data))
        else:
            logger.warning(
                "Codex targeted findings pass failed for %s/%s: %s",
                project_id,
                targeted_pass.stage,
                targeted_result.error_message,
            )

    if not targeted_payloads:
        return 0, "\n".join(part for part in combined_text_parts if part), base_result

    combined = combine_findings_with_targeted(base_data, targeted_payloads)
    _write_json(resolved_output_dir / "03_findings.json", combined)

    base_result.json_data = combined
    base_result.text = json_dumps(combined)
    base_result.duration_ms = total_duration_ms
    base_result.output_tokens = total_output_tokens
    base_result.input_tokens = total_input_tokens
    _save_audit_trail(
        project_id,
        "03_findings_codex_targeted_union",
        base_result.model or model,
        base_result.input_tokens,
        base_result.output_tokens,
        base_result.duration_ms,
        {"json_data": combined, "targeted_passes": [stage for stage, _ in targeted_payloads]},
        output_dir=output_dir,
    )
    return 0, "\n".join(part for part in combined_text_parts if part), base_result


# ═══════════════════════════════════════════════════════════════════════════
# CLAUDE CLI ЭТАПЫ (5 этапов — Claude сам читает/пишет файлы)
# ═══════════════════════════════════════════════════════════════════════════

# ─── Анализ текста (Claude CLI, Sonnet) ───────────────────────────────

async def run_text_analysis(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Запустить анализ текста MD-файла -> 01_text_analysis.json (динамический выбор провайдера)."""
    model = get_stage_model("text_analysis")

    if is_codex_model(model):
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            messages = prompt_builder.build_text_analysis_messages(project_info, project_id)
        return await _run_codex_json_stage(
            stage="text_analysis", messages=messages, model=model,
            timeout=CLAUDE_TEXT_ANALYSIS_TIMEOUT, project_id=project_id,
            on_output=on_output, output_filename="01_text_analysis.json",
            audit_stage="01_text_analysis", output_dir=output_dir,
        )

    if is_claude_stage("text_analysis"):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_text_analysis_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, TEXT_ANALYSIS_TOOLS, CLAUDE_TEXT_ANALYSIS_TIMEOUT,
            on_output, stage="text_analysis", project_id=project_id, model=model,
        )
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            _save_audit_trail(project_id, "01_text_analysis", model, cli_result.input_tokens, cli_result.output_tokens, cli_result.duration_ms, cli_result.result_text)
        return exit_code, combined, cli_result

    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.llm_runner as llm_runner

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        messages = prompt_builder.build_text_analysis_messages(project_info, project_id)
    resolved_output_dir = _resolve_output_dir(project_id, output_dir=output_dir)
    result = await llm_runner.run_llm(stage="text_analysis", messages=messages, timeout=1800)

    if result.json_data and not result.is_error:
        output_path = resolved_output_dir / "01_text_analysis.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result.json_data, ensure_ascii=False, indent=2), encoding="utf-8",
        )

    if on_output:
        await _send_status_llm(on_output, result)

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        _save_audit_trail(
            project_id, "01_text_analysis", result.model,
            result.input_tokens, result.output_tokens,
            result.duration_ms, _build_llm_audit_payload(result),
        )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Свод замечаний (Claude CLI, Opus) ────────────────────────────────

async def run_findings_merge(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Запустить свод замечаний из текста + блоков -> 03_findings.json (динамический выбор провайдера)."""
    model = get_stage_model("findings_merge")

    if is_codex_model(model):
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            messages = prompt_builder.build_findings_merge_messages(project_info, project_id)
        exit_code, text, result = await _run_codex_json_stage(
            stage="findings_merge", messages=messages, model=model,
            timeout=CLAUDE_FINDINGS_MERGE_TIMEOUT, project_id=project_id,
            on_output=on_output, output_filename="03_findings.json",
            audit_stage="03_findings_merge", output_dir=output_dir,
        )
        if exit_code != 0:
            return exit_code, text, result
        try:
            return await _run_codex_targeted_findings_merge(
                project_info=project_info,
                project_id=project_id,
                model=model,
                base_result=result,
                base_text=text,
                on_output=on_output,
                output_dir=output_dir,
                version_dir=version_dir,
                version_id=version_id,
            )
        except Exception:
            # Контракт «targeted-провалы нефатальны»: базовый merge уже валиден и
            # записан — любое исключение усилителя не должно ронять стадию.
            logger.exception(
                "Codex targeted findings: исключение — продолжаю с base merge (%s)",
                project_id,
            )
            return 0, text, result

    if is_claude_stage("findings_merge"):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_findings_merge_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, FINDINGS_MERGE_TOOLS, CLAUDE_FINDINGS_MERGE_TIMEOUT,
            on_output, stage="findings_merge", project_id=project_id,
            model=model,
        )

        _save_audit_trail(
            project_id, "03_findings_merge", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
            output_dir=output_dir,
        )

        return exit_code, combined, cli_result

    # OpenRouter path
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.llm_runner as llm_runner

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        messages = prompt_builder.build_findings_merge_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="findings_merge", messages=messages, timeout=1800)

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id, output_dir=output_dir) / "03_findings.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "03_findings_merge", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, _build_llm_audit_payload(result),
        output_dir=output_dir,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Верификация нормативных ссылок (Claude CLI, Sonnet) ──────────────

async def run_norm_verify(
    norms_list_text: str,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    project_info: Optional[dict] = None,
    llm_out_filename: str = "norm_checks_llm.json",
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Запустить верификацию нормативных ссылок -> norm_checks_llm.json (динамический выбор провайдера)."""
    model = get_stage_model("norm_verify")

    if is_codex_model(model):
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            messages = prompt_builder.build_norm_verify_messages(norms_list_text, project_id, project_info)
        return await _run_codex_json_stage(
            stage="norm_verify", messages=messages, model=model,
            timeout=CLAUDE_NORM_VERIFY_TIMEOUT, project_id=project_id,
            on_output=on_output, output_filename=llm_out_filename,
            audit_stage="04_norm_verify", output_dir=output_dir,
        )

    if is_claude_stage("norm_verify"):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_norm_verify_task(
                norms_list_text, project_id,
                project_info=project_info, llm_out_filename=llm_out_filename,
            )
        exit_code, combined, cli_result = await _run_cli(
            task_text, NORM_VERIFY_TOOLS, CLAUDE_NORM_VERIFY_TIMEOUT,
            on_output, stage="norm_verify", project_id=project_id,
            model=model,
        )

        _save_audit_trail(
            project_id, "04_norm_verify", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
            output_dir=output_dir,
        )

        return exit_code, combined, cli_result

    # OpenRouter path
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.llm_runner as llm_runner

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        messages = prompt_builder.build_norm_verify_messages(norms_list_text, project_id, project_info)
    result = await llm_runner.run_llm(stage="norm_verify", messages=messages, timeout=600)

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id, output_dir=output_dir) / llm_out_filename
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "04_norm_verify", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, _build_llm_audit_payload(result),
        output_dir=output_dir,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Пересмотр замечаний по актуальным нормам (Claude CLI, Sonnet) ────

async def run_norm_fix(
    findings_to_fix_text: str,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    project_info: Optional[dict] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Запустить пересмотр замечаний с учётом актуальных норм (динамический выбор провайдера)."""
    model = get_stage_model("norm_fix")

    if is_codex_model(model):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_norm_fix_task(
                findings_to_fix_text, project_id,
                project_info=project_info,
            )
        task_text = (
            "## Codex exec mode override\n\n"
            "This task template may mention Claude Read/Write tools and MCP `norms` tools. "
            "In this Codex exec run, use direct filesystem access instead of Read/Write tools, "
            "do not call MCP or web tools, and treat `norm_checks.json` as the authoritative "
            "norm status source already produced by Python. Use `replacement_doc`, "
            "`current_version`, `status`, `paragraph_checks`, and `affected_findings` from "
            "`norm_checks.json`; if exact clause text is unavailable, mark the finding with "
            "`norm_status: warning` or preserve the wording while adding `norm_revision`. "
            "Do not refuse only because MCP tools are unavailable.\n\n"
            + task_text
        )
        exit_code, combined, cli_result = await _run_cli(
            task_text, NORM_VERIFY_TOOLS, CLAUDE_NORM_FIX_TIMEOUT,
            on_output, stage="norm_fix", project_id=project_id,
            model=model,
        )

        _save_audit_trail(
            project_id, "04b_norm_fix", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
            output_dir=output_dir,
        )

        return exit_code, combined, cli_result

    if is_claude_stage("norm_fix"):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_norm_fix_task(
                findings_to_fix_text, project_id,
                project_info=project_info,
            )
        exit_code, combined, cli_result = await _run_cli(
            task_text, NORM_VERIFY_TOOLS, CLAUDE_NORM_FIX_TIMEOUT,
            on_output, stage="norm_fix", project_id=project_id,
            model=model,
        )

        _save_audit_trail(
            project_id, "04b_norm_fix", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
            output_dir=output_dir,
        )

        return exit_code, combined, cli_result

    # OpenRouter path
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.llm_runner as llm_runner

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        messages = prompt_builder.build_norm_fix_messages(findings_to_fix_text, project_id, project_info)
    result = await llm_runner.run_llm(stage="norm_fix", messages=messages, timeout=600)

    if result.json_data and not result.is_error:
        # Пишем в 03_findings.json (pipeline сам создаст 03a как снэпшот)
        output_path = _resolve_output_dir(project_id, output_dir=output_dir) / "03_findings.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "04b_norm_fix", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, _build_llm_audit_payload(result),
        output_dir=output_dir,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Уточнение цитат норм через MCP (Claude CLI, Sonnet) ─────────────

async def run_norm_requote(
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    project_info: Optional[dict] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, "AnyResult"]:
    """Уточнить цитаты норм для замечаний с [ручная сверка] через MCP semantic search."""
    model = get_stage_model("norm_requote")
    if is_codex_model(model):
        msg = (
            "norm_requote Codex fallback skipped: native Python requote failed, "
            "and Codex filesystem/MCP agent mode is disabled"
        )
        await send_output(on_output, msg)
        return 1, msg, CLIResult(result_text=msg, is_error=True)

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        task_text = prepare_norm_requote_task(project_id, project_info=project_info)
    exit_code, combined, cli_result = await _run_cli(
        task_text, NORM_VERIFY_TOOLS, CLAUDE_NORM_REQUOTE_TIMEOUT,
        on_output, stage="norm_requote", project_id=project_id,
        model=model,
    )
    _save_audit_trail(
        project_id, "04c_norm_requote", model,
        0, 0, cli_result.duration_ms, cli_result.result_text,
        output_dir=output_dir,
    )
    return exit_code, combined, cli_result


# ─── Оптимизация проектных решений (Claude CLI, Opus) ─────────────────

async def run_optimization(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Запустить анализ оптимизации -> optimization.json (динамический выбор провайдера)."""
    model = get_stage_model("optimization")

    if is_codex_model(model):
        from backend.app.pipeline.stages.optimization.visual_context import (
            collect_optimization_visual_context,
        )

        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_optimization_task(project_info, project_id)

        image_paths: list[Path] = []
        visual_prompt = ""
        if _codex_optimization_images_enabled():
            resolved_output_dir = _resolve_output_dir(project_id, output_dir=output_dir)
            visual_context = collect_optimization_visual_context(
                resolved_output_dir,
                discipline=str((project_info or {}).get("section") or ""),
            )
            image_paths = visual_context.image_paths
            visual_prompt = visual_context.prompt_section
            if image_paths:
                await send_output(
                    on_output,
                    f"Codex optimization vision: attached {len(image_paths)} drawing block image(s)",
                )
        if visual_prompt:
            task_text = task_text.rstrip() + "\n\n" + visual_prompt

        exit_code, combined, cli_result = await _run_cli(
            task_text, TEXT_ANALYSIS_TOOLS, CLAUDE_OPTIMIZATION_TIMEOUT,
            on_output, stage="optimization", project_id=project_id,
            model=model, image_paths=image_paths,
        )

        _save_audit_trail(
            project_id, "05_optimization", model,
            0, 0, cli_result.duration_ms,
            {
                "result_text": cli_result.result_text,
                "codex_exec_agentic": True,
                "attached_images": [str(path) for path in image_paths],
            },
            output_dir=output_dir,
        )

        return exit_code, combined, cli_result

    if is_claude_stage("optimization"):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_optimization_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, TEXT_ANALYSIS_TOOLS, CLAUDE_OPTIMIZATION_TIMEOUT,
            on_output, stage="optimization", project_id=project_id,
            model=model,
        )

        _save_audit_trail(
            project_id, "05_optimization", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
            output_dir=output_dir,
        )

        return exit_code, combined, cli_result

    # OpenRouter path
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.llm_runner as llm_runner

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        messages = prompt_builder.build_optimization_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="optimization", messages=messages, timeout=3600)

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id, output_dir=output_dir) / "optimization.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "05_optimization", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, _build_llm_audit_payload(result),
        output_dir=output_dir,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ═══════════════════════════════════════════════════════════════════════════
# OPENROUTER ЭТАПЫ (5 этапов — Python записывает JSON)
# ═══════════════════════════════════════════════════════════════════════════

# ─── Анализ пакета image-блоков (OpenRouter, Gemini) ──────────────────

async def run_block_batch(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Запустить анализ одного пакета image-блоков -> block_batch_NNN.json (динамический выбор провайдера)."""
    from backend.app.core.config import CLAUDE_BLOCK_ANALYSIS_TIMEOUT, BLOCK_ANALYSIS_TOOLS

    batch_id = batch_data.get("batch_id", 0)
    stage_key = f"block_batch_{batch_id:03d}"
    model = get_stage_model("block_batch")

    if is_claude_stage("block_batch"):
        from backend.app.core.config import CLAUDE_BLOCK_BATCH_CLEAN_CWD
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_block_batch_task(batch_data, project_info, project_id, total_batches)
        exit_code, combined, cli_result = await _run_cli(
            task_text, BLOCK_ANALYSIS_TOOLS, CLAUDE_BLOCK_ANALYSIS_TIMEOUT,
            on_output, stage=stage_key, project_id=project_id, model=model,
            clean_cwd=CLAUDE_BLOCK_BATCH_CLEAN_CWD,
        )

        _save_audit_trail(
            project_id, f"02_block_batch_{batch_id:03d}", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
            output_dir=output_dir,
        )

        return exit_code, combined, cli_result

    # Direct Gemini path (activated when GEMINI_DIRECT_API_KEY is set AND model is a native gemini- ID)
    from backend.app.services.llm.gemini_direct_runner import is_gemini_direct_model
    from backend.app.core.config import GEMINI_DIRECT_API_KEY

    if is_gemini_direct_model(model) and GEMINI_DIRECT_API_KEY:
        from backend.app.services.llm.gemini_direct_runner import run_block_batch_gemini_direct
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

        exit_code, raw_text, gd_result = await run_block_batch_gemini_direct(
            batch_data, project_info, project_id, total_batches,
            model_id=model,
            api_key=GEMINI_DIRECT_API_KEY,
        )

        if on_output:
            status = "OK" if not gd_result.is_error else f"ERROR: {gd_result.error_message[:80]}"
            await on_output(
                f"[gemini_direct] batch {batch_id:03d}/{total_batches}: {status}"
                f" | tokens={gd_result.total_tokens} | ${gd_result.cost_usd:.4f}"
            )

        _save_audit_trail(
            project_id, f"02_block_batch_{batch_id:03d}", gd_result.model_id,
            gd_result.prompt_tokens, gd_result.output_tokens,
            gd_result.duration_ms, gd_result.parsed_data,
        )

        return exit_code, raw_text, gd_result

    # OpenRouter / local-GEMMA path
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.llm_runner as llm_runner

    local_gemma = is_local_llm_model(model)
    dpi_tiers = _LOCAL_GEMMA_BLOCK_DPI_FALLBACKS if local_gemma else [(0, 1.0)]

    result = None
    for attempt_idx, (dpi, scale) in enumerate(dpi_tiers):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            messages = prompt_builder.build_block_batch_messages(
                batch_data, project_info, project_id, total_batches,
                image_scale=scale,
            )
        result = await llm_runner.run_llm(
            stage=stage_key,
            messages=messages,
            timeout=600,
        )
        if not result.is_error:
            break
        if local_gemma and attempt_idx + 1 < len(dpi_tiers):
            next_dpi = dpi_tiers[attempt_idx + 1][0]
            err_snippet = (result.error_message or "no details").strip()[:160]
            if on_output:
                await on_output(
                    f"[{stage_key}] DPI {dpi} → ошибка ({err_snippet}); повтор на DPI {next_dpi}"
                )
            continue
        break

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id, output_dir=output_dir) / f"block_batch_{batch_id:03d}.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, f"02_block_batch_{batch_id:03d}", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
        output_dir=output_dir,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Critic — проверка оптимизации (OpenRouter, GPT) ──────────────────

async def run_optimization_critic(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Запустить критическую проверку оптимизации (динамический выбор провайдера)."""
    from backend.app.core.config import CLAUDE_OPTIMIZATION_CRITIC_TIMEOUT, OPTIMIZATION_REVIEW_TOOLS

    model = get_stage_model("optimization_critic")

    if is_codex_model(model):
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            messages = prompt_builder.build_optimization_critic_messages(project_info, project_id)
        return await _run_codex_json_stage(
            stage="optimization_critic", messages=messages, model=model,
            timeout=CLAUDE_OPTIMIZATION_CRITIC_TIMEOUT, project_id=project_id,
            on_output=on_output, output_filename="optimization_review.json",
            audit_stage="05b_optimization_critic", output_dir=output_dir,
        )

    if is_claude_stage("optimization_critic"):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_optimization_critic_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, OPTIMIZATION_REVIEW_TOOLS, CLAUDE_OPTIMIZATION_CRITIC_TIMEOUT,
            on_output, stage="optimization_critic", project_id=project_id, model=model,
        )
        _save_audit_trail(project_id, "05b_optimization_critic", model, cli_result.input_tokens, cli_result.output_tokens, cli_result.duration_ms, cli_result.result_text, output_dir=output_dir)
        return exit_code, combined, cli_result

    # OpenRouter path
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.llm_runner as llm_runner

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        messages = prompt_builder.build_optimization_critic_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="optimization_critic", messages=messages, timeout=1200)

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id, output_dir=output_dir) / "optimization_review.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "05b_optimization_critic", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, _build_llm_audit_payload(result),
        output_dir=output_dir,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Corrector — корректировка оптимизации (OpenRouter, GPT) ──────────

async def run_optimization_corrector(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Запустить корректировку оптимизации по вердиктам критика (динамический выбор провайдера).

    БЭКАП: optimization.json -> optimization_pre_review.json
    Затем перезаписывает optimization.json результатом.
    """
    from backend.app.core.config import CLAUDE_OPTIMIZATION_CORRECTOR_TIMEOUT, OPTIMIZATION_REVIEW_TOOLS

    output_dir = _resolve_output_dir(project_id, output_dir=output_dir)

    # БЭКАП перед перезаписью
    opt_path = output_dir / "optimization.json"
    pre_review_path = output_dir / "optimization_pre_review.json"
    if opt_path.exists():
        shutil.copy2(opt_path, pre_review_path)

    model = get_stage_model("optimization_corrector")

    if is_codex_model(model):
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            messages = prompt_builder.build_optimization_corrector_messages(project_info, project_id)
        return await _run_codex_json_stage(
            stage="optimization_corrector", messages=messages, model=model,
            timeout=CLAUDE_OPTIMIZATION_CORRECTOR_TIMEOUT, project_id=project_id,
            on_output=on_output, output_filename="optimization.json",
            audit_stage="05c_optimization_corrector", output_dir=output_dir,
        )

    if is_claude_stage("optimization_corrector"):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_optimization_corrector_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, OPTIMIZATION_REVIEW_TOOLS, CLAUDE_OPTIMIZATION_CORRECTOR_TIMEOUT,
            on_output, stage="optimization_corrector", project_id=project_id, model=model,
        )
        _save_audit_trail(project_id, "05c_optimization_corrector", model, cli_result.input_tokens, cli_result.output_tokens, cli_result.duration_ms, cli_result.result_text, output_dir=output_dir)
        return exit_code, combined, cli_result

    # OpenRouter path
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.llm_runner as llm_runner

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        messages = prompt_builder.build_optimization_corrector_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="optimization_corrector", messages=messages, timeout=1200)

    if result.json_data and not result.is_error:
        _write_json(opt_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "05c_optimization_corrector", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, _build_llm_audit_payload(result),
        output_dir=output_dir,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ═══════════════════════════════════════════════════════════════════════════
# Legacy stubs (перенаправляют на блоковый пайплайн)
# ═══════════════════════════════════════════════════════════════════════════

async def run_tile_batch(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, LLMResult]:
    """Legacy: перенаправляет на run_block_batch."""
    return await run_block_batch(
        batch_data, project_info, project_id, total_batches, on_output,
        output_dir=output_dir, version_dir=version_dir, version_id=version_id,
    )


async def run_main_audit(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Legacy: запускает text_analysis вместо старого монолитного аудита."""
    return await run_text_analysis(
        project_info, project_id, on_output,
        output_dir=output_dir, version_dir=version_dir, version_id=version_id,
    )


async def run_triage(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Legacy: запускает text_analysis вместо триажа."""
    return await run_text_analysis(
        project_info, project_id, on_output,
        output_dir=output_dir, version_dir=version_dir, version_id=version_id,
    )
