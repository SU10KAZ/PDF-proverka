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
  - findings_critic  (GPT-5.4)
  - findings_corrector (GPT-5.4)
  - optimization_critic (GPT-5.4)
  - optimization_corrector (GPT-5.4)

Совместимость: pipeline_service ожидает сигнатуру (exit_code, text, result).
CLIResult и LLMResult имеют property-совместимость (result_text, session_id, num_turns, etc.)
"""
import json
import logging
import os
import shutil
from typing import Optional, Callable, Awaitable, Union

from webapp.config import (
    CLAUDE_CLI,
    get_model_for_stage,
    TEXT_ANALYSIS_TOOLS, FINDINGS_MERGE_TOOLS, NORM_VERIFY_TOOLS,
    CLAUDE_TEXT_ANALYSIS_TIMEOUT, CLAUDE_FINDINGS_MERGE_TIMEOUT,
    CLAUDE_NORM_VERIFY_TIMEOUT, CLAUDE_NORM_FIX_TIMEOUT,
    CLAUDE_OPTIMIZATION_TIMEOUT,
    get_stage_model, is_claude_stage,
)
from webapp.services.cli_utils import (
    is_cancelled, is_timeout, is_rate_limited,
    is_prompt_too_long,
    parse_rate_limit_reset, parse_cli_json_output, send_output,
)
from webapp.services.task_builder import (
    prepare_norm_verify_task,
    prepare_norm_fix_task,
    prepare_optimization_task,
    prepare_text_analysis_task,
    prepare_block_batch_task,
    prepare_findings_merge_task,
    prepare_findings_critic_task,
    prepare_findings_corrector_task,
    prepare_optimization_critic_task,
    prepare_optimization_corrector_task,
    prepare_tile_batch_task,
    prepare_main_audit_task,
    prepare_triage_task,
    prepare_smart_merge_task,
)
from webapp.models.usage import CLIResult, LLMResult

logger = logging.getLogger(__name__)

# Тип результата — или CLIResult (Claude CLI), или LLMResult (OpenRouter)
AnyResult = Union[CLIResult, LLMResult]


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
):
    """Сохранить копию результата LLM-вызова в _output/audit_trail/.

    Основные файлы в _output/ остаются для пайплайна,
    audit_trail/ хранит полную историю с метками времени.
    """
    from datetime import datetime

    try:
        trail_dir = _resolve_output_dir(project_id) / "audit_trail"
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

__all__ = [
    # cli_utils
    "is_cancelled", "is_timeout", "is_rate_limited",
    "parse_rate_limit_reset", "parse_cli_json_output",
    # task_builder
    "prepare_norm_verify_task", "prepare_norm_fix_task",
    "prepare_optimization_task",
    # runners
    "run_norm_verify", "run_norm_fix",
    "run_optimization",
    # runners — блоковый пайплайн
    "run_text_analysis", "run_block_batch", "run_findings_merge",
    "run_findings_critic", "run_findings_corrector",
    # runners — v4 pipeline (fact-first)
    "run_block_batch_v4", "run_findings_merge_v4",
    # runners — optimization review
    "run_optimization_critic", "run_optimization_corrector",
    # task_builder — блоковый пайплайн
    "prepare_text_analysis_task", "prepare_block_batch_task",
    "prepare_findings_merge_task",
    "prepare_findings_critic_task", "prepare_findings_corrector_task",
    # legacy stubs (перенаправляют на новый пайплайн)
    "prepare_tile_batch_task", "prepare_main_audit_task",
    "prepare_triage_task", "prepare_smart_merge_task",
    "run_tile_batch", "run_main_audit", "run_triage", "run_smart_merge",
]


# ═══════════════════════════════════════════════════════════════════════════
# Claude CLI — вспомогательные функции
# ═══════════════════════════════════════════════════════════════════════════

def _build_cmd(tools: str, model: str | None = None) -> list[str]:
    """Собрать команду запуска Claude CLI."""
    resolved_model = model or get_model_for_stage("default")
    return [
        CLAUDE_CLI, "-p",
        "--model", resolved_model,
        "--allowedTools", tools,
        "--output-format", "json",
    ]


async def _run_cli(
    task_text: str,
    tools: str,
    timeout: int,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    stage: str = "",
    project_id: str = "",
    model: str | None = None,
) -> tuple[int, str, CLIResult]:
    """Запустить Claude CLI с задачей через stdin, вернуть (exit_code, combined_output, CLIResult).

    Claude CLI записывает результаты через Write tool (файлы) — Python не записывает JSON.
    """
    from webapp.services.process_runner import run_command

    cmd = _build_cmd(tools, model)

    # Очистить все CLAUDE* переменные окружения, чтобы вложенный CLI
    # не думал что он внутри другой сессии
    env_overrides = {k: None for k in os.environ if k.startswith("CLAUDE")}

    exit_code, stdout, stderr = await run_command(
        cmd,
        input_text=task_text,
        timeout=timeout,
        on_output=on_output,
        env_overrides=env_overrides,
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
    """Записать JSON в файл (с автосозданием директории)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _resolve_output_dir(project_id: str):
    """Получить _output/ директорию проекта."""
    from webapp.services.project_service import resolve_project_dir
    return resolve_project_dir(project_id) / "_output"


# ═══════════════════════════════════════════════════════════════════════════
# CLAUDE CLI ЭТАПЫ (5 этапов — Claude сам читает/пишет файлы)
# ═══════════════════════════════════════════════════════════════════════════

# ─── Анализ текста (Claude CLI, Sonnet) ───────────────────────────────

async def run_text_analysis(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """Запустить анализ текста MD-файла -> 01_text_analysis.json (динамический выбор провайдера)."""
    if is_claude_stage("text_analysis"):
        model = get_stage_model("text_analysis")
        task_text = prepare_text_analysis_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, TEXT_ANALYSIS_TOOLS, CLAUDE_TEXT_ANALYSIS_TIMEOUT,
            on_output, stage="text_analysis", project_id=project_id, model=model,
        )
        _save_audit_trail(project_id, "01_text_analysis", model, 0, 0, cli_result.duration_ms, cli_result.result_text)
        return exit_code, combined, cli_result

    from webapp.services import prompt_builder, llm_runner
    from webapp.services.project_service import resolve_project_dir

    messages = prompt_builder.build_text_analysis_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="text_analysis", messages=messages, timeout=1800)

    if result.json_data and not result.is_error:
        output_path = resolve_project_dir(project_id) / "_output" / "01_text_analysis.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result.json_data, ensure_ascii=False, indent=2), encoding="utf-8",
        )

    if on_output:
        await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "01_text_analysis", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Свод замечаний (Claude CLI, Opus) ────────────────────────────────

async def run_findings_merge(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """Запустить свод замечаний из текста + блоков -> 03_findings.json (динамический выбор провайдера)."""
    model = get_stage_model("findings_merge")

    if is_claude_stage("findings_merge"):
        task_text = prepare_findings_merge_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, FINDINGS_MERGE_TOOLS, CLAUDE_FINDINGS_MERGE_TIMEOUT,
            on_output, stage="findings_merge", project_id=project_id,
            model=model,
        )

        _save_audit_trail(
            project_id, "03_findings_merge", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
        )

        return exit_code, combined, cli_result

    # OpenRouter path
    from webapp.services import prompt_builder, llm_runner

    messages = prompt_builder.build_findings_merge_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="findings_merge", messages=messages, timeout=1800)

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id) / "03_findings.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "03_findings_merge", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── V4 pipeline (fact-first) — заменяет block_batch + findings_merge ─

async def run_block_batch_v4(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """V4 extraction для одного batch → typed_facts_batch_NNN.json.

    Совместим по сигнатуре с run_block_batch — можно подставить условно.
    Возвращает tuple (exit_code, combined_output, result) где result — CLIResult или LLMResult.
    """
    from webapp.services.v4.extraction import run_extraction_batch

    batch_id = batch_data.get("batch_id", 0)
    stage_key = f"v4_extraction_{batch_id:03d}"

    exit_code, combined, extra = await run_extraction_batch(
        batch_data, project_info, project_id, total_batches,
        on_output=on_output,
    )

    # Формируем result объект совместимый с downstream usage tracking
    duration_ms = int((extra.get("duration_s", 0.0) or 0.0) * 1000)
    result = LLMResult(
        text=combined[:5000] if combined else "",
        json_data=None,
        input_tokens=extra.get("input_tokens", 0),
        output_tokens=extra.get("output_tokens", 0),
        cost_usd=extra.get("cost_usd", 0.0),
        duration_ms=duration_ms,
        model=get_stage_model("block_batch"),
        is_error=(exit_code != 0),
        error_message=combined[:500] if exit_code != 0 else "",
    )

    _save_audit_trail(
        project_id, f"02_{stage_key}", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, {"mentions": extra.get("mentions", 0)},
    )

    return exit_code, combined, result


async def run_findings_merge_v4(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    stage_callback=None,
) -> tuple[int, str, AnyResult]:
    """V4 post-extraction: memory → candidates → formatter → 03_findings.json.

    Совместим по сигнатуре с run_findings_merge — можно подставить условно.
    stage_callback(stage_key, status) — опционально, для обновления v4_memory/v4_candidates/v4_formatter
    логов в real-time.
    """
    from webapp.services.v4.pipeline import run_post_extraction_pipeline
    from webapp.services.project_service import resolve_project_dir

    # Кол-во блоков — для meta.blocks_analyzed в findings
    blocks_dir = resolve_project_dir(project_id) / "_output" / "blocks"
    blocks_analyzed = 0
    if blocks_dir.exists():
        blocks_analyzed = sum(1 for _ in blocks_dir.glob("*.png"))

    # Код дисциплины для v4_config
    discipline_code = (project_info or {}).get("section", "EOM")

    try:
        results = await run_post_extraction_pipeline(
            project_id,
            blocks_analyzed=blocks_analyzed,
            stage_callback=stage_callback,
            discipline_code=discipline_code,
        )
    except Exception as e:
        err_msg = f"v4 post-extraction pipeline failed: {e}"
        result = LLMResult(
            text="", is_error=True, error_message=err_msg,
            model="v4_pipeline",
        )
        return 1, err_msg, result

    total_findings = results.get("formatter", {}).get("total_findings", 0)
    summary = (
        f"v4: memory={results['memory']['stats']} "
        f"candidates={results['candidates']['stats']} "
        f"findings={total_findings}"
    )

    result = LLMResult(
        text=summary,
        json_data=None,
        input_tokens=0,
        output_tokens=0,
        cost_usd=0.0,
        duration_ms=0,
        model="v4_pipeline",
        is_error=False,
    )

    _save_audit_trail(
        project_id, "03_findings_merge_v4", "v4_pipeline",
        0, 0, 0, results,
    )

    return 0, summary, result


# ─── Верификация нормативных ссылок (Claude CLI, Sonnet) ──────────────

async def run_norm_verify(
    norms_list_text: str,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    project_info: Optional[dict] = None,
    llm_out_filename: str = "norm_checks_llm.json",
) -> tuple[int, str, AnyResult]:
    """Запустить верификацию нормативных ссылок -> norm_checks_llm.json (динамический выбор провайдера)."""
    model = get_stage_model("norm_verify")

    if is_claude_stage("norm_verify"):
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
        )

        return exit_code, combined, cli_result

    # OpenRouter path
    from webapp.services import prompt_builder, llm_runner

    messages = prompt_builder.build_norm_verify_messages(norms_list_text, project_id, project_info)
    result = await llm_runner.run_llm(stage="norm_verify", messages=messages, timeout=600)

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id) / llm_out_filename
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "04_norm_verify", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Пересмотр замечаний по актуальным нормам (Claude CLI, Sonnet) ────

async def run_norm_fix(
    findings_to_fix_text: str,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    project_info: Optional[dict] = None,
) -> tuple[int, str, AnyResult]:
    """Запустить пересмотр замечаний с учётом актуальных норм (динамический выбор провайдера)."""
    model = get_stage_model("norm_fix")

    if is_claude_stage("norm_fix"):
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
        )

        return exit_code, combined, cli_result

    # OpenRouter path
    from webapp.services import prompt_builder, llm_runner

    messages = prompt_builder.build_norm_fix_messages(findings_to_fix_text, project_id, project_info)
    result = await llm_runner.run_llm(stage="norm_fix", messages=messages, timeout=600)

    if result.json_data and not result.is_error:
        # Пишем в 03_findings.json (pipeline сам создаст 03a как снэпшот)
        output_path = _resolve_output_dir(project_id) / "03_findings.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "04b_norm_fix", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Оптимизация проектных решений (Claude CLI, Opus) ─────────────────

async def run_optimization(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """Запустить анализ оптимизации -> optimization.json (динамический выбор провайдера)."""
    model = get_stage_model("optimization")

    if is_claude_stage("optimization"):
        task_text = prepare_optimization_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, TEXT_ANALYSIS_TOOLS, CLAUDE_OPTIMIZATION_TIMEOUT,
            on_output, stage="optimization", project_id=project_id,
            model=model,
        )

        _save_audit_trail(
            project_id, "05_optimization", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
        )

        return exit_code, combined, cli_result

    # OpenRouter path
    from webapp.services import prompt_builder, llm_runner

    messages = prompt_builder.build_optimization_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="optimization", messages=messages, timeout=3600)

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id) / "optimization.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "05_optimization", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
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
) -> tuple[int, str, AnyResult]:
    """Запустить анализ одного пакета image-блоков -> block_batch_NNN.json (динамический выбор провайдера)."""
    from webapp.config import CLAUDE_BLOCK_ANALYSIS_TIMEOUT, BLOCK_ANALYSIS_TOOLS

    batch_id = batch_data.get("batch_id", 0)
    stage_key = f"block_batch_{batch_id:03d}"

    if is_claude_stage("block_batch"):
        model = get_stage_model("block_batch")
        task_text = prepare_block_batch_task(batch_data, project_info, project_id, total_batches)
        exit_code, combined, cli_result = await _run_cli(
            task_text, BLOCK_ANALYSIS_TOOLS, CLAUDE_BLOCK_ANALYSIS_TIMEOUT,
            on_output, stage=stage_key, project_id=project_id, model=model,
        )

        _save_audit_trail(
            project_id, f"02_block_batch_{batch_id:03d}", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
        )

        return exit_code, combined, cli_result

    # OpenRouter path
    from webapp.services import prompt_builder, llm_runner

    messages = prompt_builder.build_block_batch_messages(
        batch_data, project_info, project_id, total_batches
    )
    result = await llm_runner.run_llm(
        stage=stage_key,
        messages=messages,
        timeout=600,
    )

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id) / f"block_batch_{batch_id:03d}.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, f"02_block_batch_{batch_id:03d}", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Critic — проверка замечаний (OpenRouter, GPT) ────────────────────

async def run_findings_critic(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    chunk_suffix: str = "",
) -> tuple[int, str, AnyResult]:
    """Запустить критическую проверку замечаний (динамический выбор провайдера).

    chunk_suffix: если задан (напр. "_001") — читает из
        03_findings_review_input{suffix}.json вместо 03_findings.json
        и записывает в 03_findings_review{suffix}.json.
    """
    from webapp.config import CLAUDE_FINDINGS_CRITIC_TIMEOUT, FINDINGS_REVIEW_TOOLS

    output_dir = _resolve_output_dir(project_id)

    if is_claude_stage("findings_critic"):
        model = get_stage_model("findings_critic")
        task_text = prepare_findings_critic_task(project_info, project_id, chunk_suffix=chunk_suffix)
        exit_code, combined, cli_result = await _run_cli(
            task_text, FINDINGS_REVIEW_TOOLS, CLAUDE_FINDINGS_CRITIC_TIMEOUT,
            on_output, stage="findings_critic", project_id=project_id, model=model,
        )
        _save_audit_trail(project_id, f"03b_findings_critic{chunk_suffix}", model, 0, 0, cli_result.duration_ms, cli_result.result_text)
        return exit_code, combined, cli_result

    # OpenRouter path
    from webapp.services import prompt_builder, llm_runner

    if chunk_suffix:
        messages = prompt_builder.build_findings_critic_messages(project_info, project_id)
        chunk_input_path = output_dir / f"03_findings_review_input{chunk_suffix}.json"
        if chunk_input_path.exists():
            chunk_data = chunk_input_path.read_text(encoding="utf-8")
            import re
            for msg in messages:
                if msg["role"] == "user" and isinstance(msg["content"], str):
                    msg["content"] = re.sub(
                        r"(## 03_findings\.json \(findings to review\):\n\n).*?(\n\n## 02_blocks_analysis)",
                        rf"\g<1>{chunk_data}\2",
                        msg["content"],
                        flags=re.DOTALL,
                    )
    else:
        messages = prompt_builder.build_findings_critic_messages(project_info, project_id)

    result = await llm_runner.run_llm(stage="findings_critic", messages=messages, timeout=1200)

    if result.json_data and not result.is_error:
        review_filename = f"03_findings_review{chunk_suffix}.json"
        review_path = output_dir / review_filename
        _write_json(review_path, result.json_data)

    await _send_status_llm(on_output, result)

    stage_name = f"03b_findings_critic{chunk_suffix}"
    _save_audit_trail(
        project_id, stage_name, result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Corrector — корректировка замечаний (OpenRouter, GPT) ────────────

async def run_findings_corrector(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """Запустить корректировку замечаний по вердиктам критика (динамический выбор провайдера).

    БЭКАП: 03_findings.json -> 03_findings_pre_review.json
    Затем перезаписывает 03_findings.json результатом.
    """
    from webapp.config import CLAUDE_FINDINGS_CORRECTOR_TIMEOUT, FINDINGS_REVIEW_TOOLS

    output_dir = _resolve_output_dir(project_id)

    # БЭКАП перед перезаписью
    findings_path = output_dir / "03_findings.json"
    pre_review_path = output_dir / "03_findings_pre_review.json"
    if findings_path.exists():
        shutil.copy2(findings_path, pre_review_path)

    if is_claude_stage("findings_corrector"):
        model = get_stage_model("findings_corrector")
        task_text = prepare_findings_corrector_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, FINDINGS_REVIEW_TOOLS, CLAUDE_FINDINGS_CORRECTOR_TIMEOUT,
            on_output, stage="findings_corrector", project_id=project_id, model=model,
        )
        _save_audit_trail(project_id, "03c_findings_corrector", model, 0, 0, cli_result.duration_ms, cli_result.result_text)
        return exit_code, combined, cli_result

    # OpenRouter path
    from webapp.services import prompt_builder, llm_runner

    messages = prompt_builder.build_findings_corrector_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="findings_corrector", messages=messages, timeout=1200)

    if result.json_data and not result.is_error:
        _write_json(findings_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "03c_findings_corrector", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Critic — проверка оптимизации (OpenRouter, GPT) ──────────────────

async def run_optimization_critic(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """Запустить критическую проверку оптимизации (динамический выбор провайдера)."""
    from webapp.config import CLAUDE_OPTIMIZATION_CRITIC_TIMEOUT, OPTIMIZATION_REVIEW_TOOLS

    if is_claude_stage("optimization_critic"):
        model = get_stage_model("optimization_critic")
        task_text = prepare_optimization_critic_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, OPTIMIZATION_REVIEW_TOOLS, CLAUDE_OPTIMIZATION_CRITIC_TIMEOUT,
            on_output, stage="optimization_critic", project_id=project_id, model=model,
        )
        _save_audit_trail(project_id, "05b_optimization_critic", model, 0, 0, cli_result.duration_ms, cli_result.result_text)
        return exit_code, combined, cli_result

    # OpenRouter path
    from webapp.services import prompt_builder, llm_runner

    messages = prompt_builder.build_optimization_critic_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="optimization_critic", messages=messages, timeout=1200)

    if result.json_data and not result.is_error:
        output_path = _resolve_output_dir(project_id) / "optimization_review.json"
        _write_json(output_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "05b_optimization_critic", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
    )

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.text, result


# ─── Corrector — корректировка оптимизации (OpenRouter, GPT) ──────────

async def run_optimization_corrector(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """Запустить корректировку оптимизации по вердиктам критика (динамический выбор провайдера).

    БЭКАП: optimization.json -> optimization_pre_review.json
    Затем перезаписывает optimization.json результатом.
    """
    from webapp.config import CLAUDE_OPTIMIZATION_CORRECTOR_TIMEOUT, OPTIMIZATION_REVIEW_TOOLS

    output_dir = _resolve_output_dir(project_id)

    # БЭКАП перед перезаписью
    opt_path = output_dir / "optimization.json"
    pre_review_path = output_dir / "optimization_pre_review.json"
    if opt_path.exists():
        shutil.copy2(opt_path, pre_review_path)

    if is_claude_stage("optimization_corrector"):
        model = get_stage_model("optimization_corrector")
        task_text = prepare_optimization_corrector_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, OPTIMIZATION_REVIEW_TOOLS, CLAUDE_OPTIMIZATION_CORRECTOR_TIMEOUT,
            on_output, stage="optimization_corrector", project_id=project_id, model=model,
        )
        _save_audit_trail(project_id, "05c_optimization_corrector", model, 0, 0, cli_result.duration_ms, cli_result.result_text)
        return exit_code, combined, cli_result

    # OpenRouter path
    from webapp.services import prompt_builder, llm_runner

    messages = prompt_builder.build_optimization_corrector_messages(project_info, project_id)
    result = await llm_runner.run_llm(stage="optimization_corrector", messages=messages, timeout=1200)

    if result.json_data and not result.is_error:
        _write_json(opt_path, result.json_data)

    await _send_status_llm(on_output, result)

    _save_audit_trail(
        project_id, "05c_optimization_corrector", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, result.json_data,
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
) -> tuple[int, str, LLMResult]:
    """Legacy: перенаправляет на run_block_batch."""
    return await run_block_batch(batch_data, project_info, project_id, total_batches, on_output)


async def run_main_audit(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """Legacy: запускает text_analysis вместо старого монолитного аудита."""
    return await run_text_analysis(project_info, project_id, on_output)


async def run_triage(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """Legacy: запускает text_analysis вместо триажа."""
    return await run_text_analysis(project_info, project_id, on_output)


async def run_smart_merge(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, AnyResult]:
    """Legacy: запускает findings_merge вместо smart_merge."""
    return await run_findings_merge(project_info, project_id, on_output)
