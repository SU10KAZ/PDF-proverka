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
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Callable, Awaitable, Sequence, Union

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_FOR_TEXT_FILENAME,
    TEXT_ANALYSIS_FILENAME,
    TEXT_ANALYSIS_STAGE,
    resolve_existing,
)

from backend.app.core.config import (
    CLAUDE_CLI,
    get_claude_cli,
    get_model_for_stage,
    TEXT_ANALYSIS_TOOLS, FINDINGS_MERGE_TOOLS, NORM_VERIFY_TOOLS,
    OPTIMIZATION_TOOLS,
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
from backend.app.services.common import audit_scope, resource_budget
from backend.app.services.common.cli_utils import (
    is_cancelled, is_timeout, is_rate_limited,
    is_prompt_too_long,
    parse_rate_limit_reset, parse_cli_json_output, send_output,
)
from backend.app.pipeline.stages.prepare.task_builder import (
    prepare_norm_verify_task,
    prepare_norm_fix_task,
    prepare_optimization_norm_fix_task,
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

def _provider_bridge():
    """Мост провайдеров воркера, если пакет доступен в этом процессе.

    Импорт локальный и мягкий: `audit_worker` устанавливается на ВОРКЕРЕ, а
    этот модуль живёт и на центре, где пакета может не быть вовсе. Отсутствие
    пакета — не ошибка и не повод для предупреждения: это штатное состояние
    центральной установки.
    """
    try:
        from audit_worker.providers import pipeline_bridge
    except ModuleNotFoundError:
        # Пакета НЕТ — это центр, штатное состояние. Ловится именно
        # `ModuleNotFoundError`, а не любое исключение: сломанный импорт
        # внутри самого пакета (синтаксис, отсутствующая зависимость) на
        # ВОРКЕРЕ раньше давал ровно тот же `None` — то есть тихий уход на
        # прежний транспорт с файловыми инструментами и выходом в веб.
        return None
    return pipeline_bridge


def _build_cmd(tools: str, model: str | None = None) -> list[str]:
    """Собрать команду запуска Claude CLI.

    Этапам без ``mcp__*`` в списке инструментов MCP-серверы не поднимаем:
    нормативный сервер (`norms/tools/mcp_server.py`) разрастается до ~2,8 ГБ
    RSS на КАЖДЫЙ вызов CLI, и на 11-гигабайтной машине два таких сервера
    загоняют систему в своп — ядро начинает убивать рабочие стадии
    (04.08.2026: text_analysis ЭО1-3 «код 143», optimization ОВ1-2.3 «exit -9»).
    Свод/текст/блоки его всё равно не вызывают — им он мёртвый груз.
    Отключается через AUDIT_STRICT_MCP_FOR_NON_NORM_STAGES=false.
    """
    resolved_model = model or get_model_for_stage("default")
    cmd = [
        get_claude_cli(), "-p",
        "--model", resolved_model,
        "--allowedTools", tools,
        "--output-format", "json",
    ]
    strict_enabled = os.environ.get(
        "AUDIT_STRICT_MCP_FOR_NON_NORM_STAGES", "true",
    ).strip().lower() not in ("false", "0", "no", "off")
    if strict_enabled and "mcp__" not in (tools or ""):
        cmd.append("--strict-mcp-config")
    return cmd


# Чистая cwd для запуска `claude -p` без подгрузки project CLAUDE.md / hooks / memory / skills.
# Эмпирически (КЖ5.1, 25 блоков) даёт −42% input/блок и −36% cli_cost при +35% findings —
# Sonnet работает прицельнее без harness'а Claude Code в качестве distractor'а.
# См. ideas.md (Идея 6) и memory/feedback_subscription_only.md.
#: Корень «чистых» каталогов. Вычисляется, а не задан литералом: на воркере
#: он обязан лежать внутри каталога попытки (см. config.clean_cli_cwd_root).
def _clean_cwd_root() -> str:
    from backend.app.core.config import clean_cli_cwd_root
    return clean_cli_cwd_root()
_CLEAN_ENV_KEEP = {"HOME", "PATH", "LANG", "LC_ALL", "USER", "SHELL"}


def _ensure_clean_cwd() -> str:
    """Отдать чистый рабочий каталог для одного запуска `claude -p`.

    Смысл каталога — запускать CLI вне репозитория, чтобы не подтягивались
    project CLAUDE.md, .claude/settings.json, hooks, memory и skills.

    Раньше это был ОДИН общий каталог `/tmp/sonnet_clean`, и каждый вызов
    удалял в нём все файлы. Пока CLI запускался по одному, это было безобидно.
    При параллельных проектах (до 20 одновременных `claude -p`) старт нового
    вызова стирает рабочие файлы уже бегущих — поэтому каталог теперь свой на
    каждый вызов. Общий путь остаётся корнем для них, чтобы не плодить мусор
    по всему /tmp и чтобы старая уборка по этому пути продолжала работать.
    """
    root = _clean_cwd_root()
    os.makedirs(root, exist_ok=True)
    _sweep_stale_run_dirs(root)
    return tempfile.mkdtemp(prefix="run_", dir=root)


# Через сколько каталог запуска считается брошенным. Больше самого длинного
# таймаута CLI, чтобы уборка не унесла файлы у живого процесса.
_RUN_DIR_TTL_SEC = 6 * 3600


def _sweep_stale_run_dirs(root: str) -> None:
    """Подмести каталоги давно умерших запусков.

    Основной путь удаляет свой каталог сам, но при kill -9 бэкенда мусор
    остаётся. Возрастная уборка не трогает живые запуски (TTL заведомо больше
    любого таймаута CLI) — в отличие от прежней логики «стереть всё сейчас»,
    которая при параллельных проектах убирала файлы у работающих соседей.
    """
    now = time.time()
    try:
        entries = os.listdir(root)
    except OSError:
        return
    for entry in entries:
        if not entry.startswith("run_"):
            continue
        full = os.path.join(root, entry)
        try:
            if os.path.isdir(full) and now - os.path.getmtime(full) > _RUN_DIR_TTL_SEC:
                shutil.rmtree(full, ignore_errors=True)
        except OSError:
            pass


def _release_clean_cwd(path: str | None) -> None:
    """Удалить каталог одного запуска. Ошибки глушим: это уборка, не логика."""
    if not path or not path.startswith(_clean_cwd_root()):
        return
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


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
    reasoning_effort: str | None = None,
) -> tuple[int, str, CLIResult]:
    """Запустить agent CLI с задачей через stdin, вернуть (exit_code, output, CLIResult).

    Claude CLI записывает результаты через Write tool (файлы). Codex exec получает
    тот же task_text и пишет требуемые JSON-файлы через filesystem-доступ.

    clean_cwd=True применяется только к Claude CLI. Для Codex exec рабочая папка
    должна оставаться корнем проекта, чтобы workspace-write sandbox видел output.
    """
    # ─── Мост провайдеров воркера (этап 11C) ─────────────────────────────────
    # ЕДИНСТВЕННАЯ развилка «claude или codex» во всём конвейере стоит ниже, и
    # именно поэтому перехват сделан здесь: до неё. Когда исполнитель воркера
    # выписал привязку провайдера, любой вызов CLI из конвейера обязан идти
    # через `ProviderAdapter` — с авторизацией по режиму, окружением с нуля и
    # отключёнными инструментами. Иначе конвейер нашёл бы бинарь по PATH и
    # запустил бы его из-под изолированного HOME каталога попытки, то есть
    # НЕавторизованным.
    #
    # На центре этой ветки не существует: `active()` смотрит на переменную
    # `AUDIT_WORKER_PROVIDER_BINDING`, которую ставит только исполнитель воркера
    # и только на время попытки. Без неё поведение ровно прежнее.
    bridge = _provider_bridge()
    if bridge is not None and bridge.active():
        import asyncio as _asyncio

        exit_code, text, usage = await _asyncio.to_thread(
            bridge.route_cli_call, stage=stage, prompt=task_text, timeout_sec=timeout,
        )
        cli_result = CLIResult(
            result_text=text,
            is_error=exit_code != 0,
            duration_ms=int(usage.get("duration_ms", 0) or 0),
            input_tokens=int(usage.get("input_tokens", 0) or 0),
            output_tokens=int(usage.get("output_tokens", 0) or 0),
            cache_creation_tokens=int(usage.get("cache_creation_input_tokens", 0) or 0),
            cache_read_tokens=int(usage.get("cache_read_input_tokens", 0) or 0),
            cost_usd=float(usage.get("total_cost_usd", 0.0) or 0.0),
        )
        if on_output:
            await send_output(on_output, text)
        return exit_code, text, cli_result

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
            reasoning_effort=reasoning_effort,
            allowed_tools=tools,
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

    # Бюджеты — общие на весь бэкенд (см. common/resource_budget.py).
    # Без них пять параллельных проектов дают ~20 одновременных `claude -p`,
    # а этапы с mcp__ поднимают норм-MCP по 5,6 ГБ на КАЖДЫЙ процесс.
    # Имя "_none" неизвестно бюджету и слот не занимает — так ветвление
    # обходится без второго контекст-менеджера.
    #
    # ПОРЯДОК ЗАХВАТА ВАЖЕН: сначала ДЕФИЦИТНЫЙ norms_mcp, потом обильный
    # claude_cli. При обратном порядке задача занимала слот CLI и с ним
    # вставала в очередь за норм-слотом — на нормативных этапах несколько
    # проектов держали слоты CLI, ничего не выполняя (hold-and-wait).
    # Порядок обязан быть ОДИНАКОВЫМ во всех точках захвата (здесь и в
    # codex_runner) — иначе взаимная блокировка.
    mcp_slot = "norms_mcp" if "mcp__" in (tools or "") else "_none"
    try:
        async with resource_budget.slot(mcp_slot), resource_budget.slot("claude_cli"):
            exit_code, stdout, stderr = await run_command(
                cmd,
                input_text=task_text,
                timeout=timeout,
                on_output=on_output,
                env_overrides=env_overrides,
                cwd=cwd_arg,
                project_id=project_id,
            )
    finally:
        # Каталог запуска свой на каждый вызов — убираем сразу, иначе при
        # параллельных проектах в /tmp/sonnet_clean копятся тысячи папок.
        if clean_cwd:
            _release_clean_cwd(cwd_arg)

    combined = (stdout or "") + "\n" + (stderr or "")
    cli_result = parse_cli_json_output(stdout or "")

    return exit_code, combined, cli_result


# ═══════════════════════════════════════════════════════════════════════════
# OpenRouter — вспомогательные функции
# ═══════════════════════════════════════════════════════════════════════════

_CODEX_USAGE_LIMIT_RE = re.compile(r"usage\s+limit", re.IGNORECASE)


def _codex_json_attempts() -> int:
    """Сколько раз пытаться получить разбираемый JSON от codex (1 = без повторов)."""
    try:
        return max(1, int(os.environ.get("AUDIT_CODEX_JSON_ATTEMPTS", "3") or "3"))
    except ValueError:
        return 3


_CODEX_JSON_ATTEMPTS = _codex_json_attempts()


def _codex_json_broken(result: LLMResult) -> bool:
    """Ответ не разобрался — и это именно порча JSON, а не сбой доступа.

    Ретраить можно ТОЛЬКО порчу разбора. Исчерпание лимита приходит в том же
    виде (codex_json_not_found: тело ошибки API codex печатает в stderr, а его
    мы намеренно не парсим), но повторять его здесь нельзя — этим занимается
    стадия, у неё есть ожидание сброса лимита.

    is_rate_limited() своей формулировки codex не знает: в списке есть
    «rate limit» и «hit your limit», а codex пишет «usage limit reached» —
    поэтому проверяем её дополнительно. Список в cli_utils не трогаем: он
    общий для всего конвейера, правка там имеет куда более широкий радиус.
    """
    if result.json_data is not None and not result.is_error:
        return False
    # Только чистый ``codex_json_not_found`` означает: CLI завершился успешно,
    # но финальный ответ модели оказался оборванным/невалидным. При ненулевом
    # exit runner добавляет ``codex_exec_exit_*`` — это уже транспортный сбой,
    # который нельзя маскировать несколькими немедленными повторами.
    if (result.error_message or "").strip() != "codex_json_not_found":
        return False
    text = result.text or ""
    if is_rate_limited(1, text, ""):
        return False
    return not _CODEX_USAGE_LIMIT_RE.search(text)


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
    scoped_output_dir = audit_scope.get_output_dir()
    if scoped_output_dir:
        return Path(scoped_output_dir)

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
    """Назначить пути аудита на время блока.

    Раньше писало в `os.environ` — при параллельной обработке проектов это
    приводило к записи артефактов одного проекта в каталог другого (пока A
    ждал ответа LLM, B перетирал AUDIT_OUTPUT_DIR). Теперь привязка живёт в
    ContextVar и изолирована по задачам; подробности и правило чтения —
    в `services/common/audit_scope.py`.
    """
    with audit_scope.bind_audit_scope(
        output_dir=output_dir,
        version_dir=version_dir,
        project_id=project_id,
        version_id=version_id,
    ):
        yield


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
    allowed_tools: str | None = None,
) -> tuple[int, str, LLMResult]:
    """Run Codex exec in JSON-only mode and let backend write the artifact."""
    from backend.app.services.llm.codex_runner import run_codex_json_messages

    for attempt in range(1, _CODEX_JSON_ATTEMPTS + 1):
        result = await run_codex_json_messages(
            messages,
            timeout=timeout,
            on_output=on_output,
            stage=stage,
            project_id=project_id,
            allowed_tools=allowed_tools,
            model=model,
        )
        if not _codex_json_broken(result):
            break
        if attempt >= _CODEX_JSON_ATTEMPTS:
            break
        # Модель печатает JSON без output-schema и на больших ответах изредка
        # рвёт структуру (16.07: 13АВ-РД-ВК2.2-ПА V1 и 13АВ-РД-ДК-К1 V1 — свод на
        # ~18K токенов выхода, не хватало закрывающей скобки → готовый аудит с 34
        # находками выбрасывался). Поломка не детерминирована: повтор того же
        # запроса даёт валидный JSON. Ретраим ТОЛЬКО разбор ответа — rate limit и
        # прочие сбои уходят наверх нетронутыми, их обрабатывает стадия.
        await send_output(
            on_output,
            f"[RETRY] {stage}: ответ модели не разобрался "
            f"({result.error_message or 'json'}), повтор {attempt + 1}/{_CODEX_JSON_ATTEMPTS}",
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
        enforce_stage01_atomicity,
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

    combined = combine_findings_with_targeted(
        base_data,
        targeted_payloads,
        output_dir=resolved_output_dir,
    )
    combined = enforce_stage01_atomicity(
        combined,
        resolved_output_dir / "01_blocks_analysis.json",
    )
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

async def _run_codex_text_analysis_chunked(
    *,
    message_sets: list[list[dict]],
    plan_meta: dict,
    model: str,
    timeout: int,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]],
    output_dir: str | Path | None,
) -> tuple[int, str, LLMResult]:
    """Прогнать N чанков text_analysis и слить их в 02_text_analysis.json.

    Каждый чанк пишет диагностический part-файл (02_text_analysis.partN.json).
    Финальный merge пишет канонический файл; токены/длительность суммируются в
    один combined-result (как у targeted findings-merge). Падение любого чанка —
    hard fail (стадия покажет ошибку), частичный merge не публикуем.
    """
    from backend.app.pipeline.stages.text_analysis.md_chunker import (
        merge_text_analysis_parts,
    )

    n = len(message_sets)
    await send_output(
        on_output,
        f"[text_analysis] промпт > лимита Codex → нарезка на {n} чанков "
        f"по листам (листов {plan_meta.get('total_pages', '?')}, скелет "
        f"{plan_meta.get('skeleton_pages', 0)} листов / "
        f"{plan_meta.get('skeleton_chars', 0)} симв."
        + (", скелет усечён" if plan_meta.get("skeleton_truncated") else "")
        + ")",
    )

    parts: list[dict] = []
    combined_in = combined_out = combined_dur = 0
    last_result: LLMResult | None = None
    for i, msgs in enumerate(message_sets, 1):
        exit_code, text, result = await _run_codex_json_stage(
            stage="text_analysis", messages=msgs, model=model, timeout=timeout,
            project_id=project_id, on_output=on_output,
            output_filename=f"02_text_analysis.part{i}.json",
            audit_stage=TEXT_ANALYSIS_STAGE, output_dir=output_dir,
        )
        last_result = result
        combined_in += result.input_tokens or 0
        combined_out += result.output_tokens or 0
        combined_dur += result.duration_ms or 0
        if exit_code != 0 or result.is_error:
            await send_output(
                on_output,
                f"[text_analysis] чанк {i}/{n} упал "
                f"({result.error_message or f'код {exit_code}'}) — прерываю нарезку",
            )
            return (exit_code or 1), text, result
        if isinstance(result.json_data, dict):
            parts.append(result.json_data)
        await send_output(
            on_output,
            f"[text_analysis] чанк {i}/{n} готов "
            f"(+{len((result.json_data or {}).get('text_findings', []) if isinstance(result.json_data, dict) else [])} замечаний)",
        )

    merged = merge_text_analysis_parts(parts)
    output_path = _resolve_output_dir(project_id, output_dir=output_dir) / TEXT_ANALYSIS_FILENAME
    _write_json(output_path, merged)

    # combined-result: суммарные токены/длительность, слитый JSON как ответ.
    combined = last_result
    combined.input_tokens = combined_in
    combined.output_tokens = combined_out
    combined.duration_ms = combined_dur
    combined.json_data = merged
    combined.text = json.dumps(merged, ensure_ascii=False)
    combined.is_error = False
    combined.error_message = None

    await send_output(
        on_output,
        f"[text_analysis] слито {len(merged.get('text_findings', []))} замечаний "
        f"из {n} чанков → {TEXT_ANALYSIS_FILENAME}",
    )
    return 0, combined.text, combined


#: Ключ этапа в белом списке привязки провайдера и в `pipeline_log`. Совпадает
#: с тем, что уходит в `_run_cli(stage=…)`, и НЕ совпадает с
#: `TEXT_ANALYSIS_STAGE` ("02_text_analysis") — это имя артефакта, а не этапа.
TEXT_ANALYSIS_STAGE_KEY = "text_analysis"


class ProviderOutputPathError(RuntimeError):
    """Путь записи вышел за каталог попытки. Тихой записи не бывает."""


def _assert_output_inside_attempt(output_path: Path, attempt_dir: Path) -> None:
    """Выход обязан лежать ВНУТРИ каталога попытки (§17, §AD/AE задания 11D).

    Проверка не декоративная. В provider-режиме корни данных процесса конвейера
    выставляет `audit_runner.isolated_roots`, и все они уводят внутрь попытки;
    путь наружу означал бы, что либо роль потеряна, либо кто-то передал
    `output_dir` руками. Оба случая — запись в чужой каталог на чужой машине, и
    узнавать о них по факту испорченного продового артефакта поздно.
    """
    resolved = Path(output_path).resolve()
    root = Path(attempt_dir).resolve()
    if not resolved.is_relative_to(root):
        raise ProviderOutputPathError(
            f"выход этапа ведёт вне каталога попытки: {resolved} ⊄ {root}. "
            "Запись отменена: в provider-режиме конвейер пишет только внутрь "
            "своей попытки"
        )


#: Потолок промпта provider-режима, символов. Не оценка, а рубеж: планировщика
#: бюджета (Tier 1 «снять норм-базу» / Tier 2 «нарезка по листам»), который есть
#: у ветки codex, у provider-маршрута нет. Без потолка большой проект дал бы
#: либо ошибку CLI, либо — что хуже — молчаливое усечение, при котором
#: инструкция «Analyze the MD content COMPLETELY» осталась бы, а хвоста
#: документа модель не увидела бы. Отказ ДО вызова бесплатен; усечённый аудит
#: стоит оплаченного вызова и выглядит как настоящий.
PROVIDER_PROMPT_MAX_CHARS = int(
    os.environ.get("AUDIT_PROVIDER_TEXT_ANALYSIS_MAX_CHARS", "600000") or "600000"
)


async def _run_text_analysis_via_provider(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]],
    *,
    output_dir: str | Path | None,
    version_dir: str | Path | None,
    version_id: str | None,
    stage_key: str = "text_analysis",
) -> tuple[int, str, CLIResult]:
    """`text_analysis` через ProviderAdapter: модель только рассуждает (11D).

    Отличие от ветки Claude CLI не в «другом транспорте», а в РАСПРЕДЕЛЕНИИ
    ОБЯЗАННОСТЕЙ. Там модель получала пути и сама делала файловую работу; здесь
    файловую работу целиком делает конвейер:

        читает MD  → строит промпт → зовёт модель → проверяет → пишет файл

    Инженерная часть промпта берётся у БОЕВОГО сборщика ветки API
    (`build_text_analysis_messages`), а не пишется заново: заводить второй
    text_analysis ради нового транспорта — верный способ получить два расходящихся
    аудита (§11 задания).

    Молчаливого возврата к прежнему пути здесь нет ни в одной ветке. Мост
    активен только тогда, когда исполнитель выписал привязку, УЖЕ списав
    разрешение оператора; уйти в этот момент на `claude -p` по PATH значило бы
    выполнить неавторизованный вызов из-под изолированного HOME и показать это
    как обычную ошибку этапа.
    """
    import asyncio as _asyncio

    from audit_worker.providers import pipeline_bridge
    from audit_worker.providers.pipeline_bridge import ProviderBridgeError
    from backend.app.pipeline.stages.text_analysis import provider_transport
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

    from backend.app.pipeline.stages.prepare.task_builder import _load_prompt_override

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        # Кастомный промпт проекта заменяемая CLI-ветка честно применяла
        # (`prepare_text_analysis_task` начинается с него). Сборщик API-ветки
        # про override не знает вовсе — для HTTP-транспорта это давняя данность,
        # но на воркере молча откатиться к стоковому шаблону значит выполнить
        # НЕ ТОТ аудит, о котором просил оператор, и не сказать об этом.
        override = _load_prompt_override(project_id, "text_analysis")
        if override:
            detail = (
                "для этапа text_analysis задан кастомный промпт проекта, а "
                "provider-режим его не поддерживает. Прогон отменён: тихая "
                "подмена промпта дала бы аудит по другим правилам"
            )
            await send_output(on_output, f"[text_analysis] {detail}")
            return 1, detail, CLIResult(result_text=detail, is_error=True)
        messages = prompt_builder.build_text_analysis_messages(project_info, project_id)
        blocks_for_text = resolve_existing(
            Path(_resolve_output_dir(project_id, output_dir=output_dir)),
            BLOCKS_FOR_TEXT_FILENAME,
        )
    built = provider_transport.build_provider_prompt(messages)
    prompt = built["prompt"]

    # Проверки промпта — в РАНТАЙМЕ, а не только в тестах. Тест доказывает, что
    # код умеет вычистить пути; артефакт прогона обязан доказать, что в ЭТОТ раз
    # их не осталось. Между «умеет» и «сделал» помещается вся разница между
    # проверкой и обещанием.
    guard_problems: list[str] = []
    if built["absolute_paths_remaining_in_instructions"]:
        guard_problems.append(
            f"в инструкциях остались абсолютные пути "
            f"({built['absolute_paths_remaining_in_instructions']}): §14 запрещает "
            "давать модели путь проекта"
        )
    if built["prompt_chars"] > PROVIDER_PROMPT_MAX_CHARS:
        guard_problems.append(
            f"промпт {built['prompt_chars']} симв. > потолка "
            f"{PROVIDER_PROMPT_MAX_CHARS}: планировщика нарезки в provider-режиме "
            "нет, а молчаливое усечение документа недопустимо"
        )
    if blocks_for_text is not None and Path(blocks_for_text).is_file():
        # Блочный контекст существует, но provider-режим его НЕ вкладывает.
        # Пройти мимо значило бы выполнить текстовый анализ вслепую там, где
        # данные блоков есть, и записать результат как полноценный.
        guard_problems.append(
            f"есть блочный контекст ({Path(blocks_for_text).name}), а "
            "provider-режим его не вкладывает: аудит вышел бы без данных, "
            "которые уже собраны"
        )
    if guard_problems:
        detail = "; ".join(guard_problems)
        await send_output(on_output, f"[text_analysis] {detail}")
        return 1, detail, CLIResult(result_text=detail, is_error=True)

    await send_output(
        on_output,
        f"[text_analysis] provider-режим: инструкции {built['system_chars']} симв., "
        f"документ {built['document_chars']} симв., путей вычищено "
        f"{built['filesystem_refs_stripped']}",
    )

    try:
        attempt_dir = pipeline_bridge.attempt_dir()
        outcome = await _asyncio.to_thread(
            lambda: pipeline_bridge.run_stage_inference(
                job_dir=attempt_dir,
                stage=stage_key,
                prompt=prompt,
                purpose=stage_key,
                required_result_fields=provider_transport.REQUIRED_RESULT_FIELDS,
                field_types=provider_transport.FIELD_TYPES,
                expected_semantics=provider_transport.EXPECTED_SEMANTICS,
                timeout_sec=float(CLAUDE_TEXT_ANALYSIS_TIMEOUT),
            )
        )
    except ProviderBridgeError as exc:
        # Отказ моста — отказ ЭТАПА. Возвращаем его кодом возврата, а не
        # исключением, чтобы боевой раннер обработал его своим штатным путём
        # (запись в pipeline_log, StageResult.fail) — но ни одна ветка отсюда
        # не ведёт к прежнему транспорту.
        detail = f"provider_bridge: {exc}"
        await send_output(on_output, f"[text_analysis] {detail}")
        return 1, detail, CLIResult(result_text=detail, is_error=True)

    result = outcome.provider_result
    usage = dict(result.usage)
    cli_result = CLIResult(
        result_text=json.dumps(result.result, ensure_ascii=False) if result.result else (result.detail or ""),
        is_error=not outcome.ok,
        duration_ms=int(result.duration_ms or 0),
        input_tokens=int(usage.get("input_tokens", 0) or 0),
        output_tokens=int(usage.get("output_tokens", 0) or 0),
        cache_creation_tokens=int(usage.get("cache_creation_input_tokens", 0) or 0),
        cache_read_tokens=int(usage.get("cache_read_input_tokens", 0) or 0),
        cost_usd=float(usage.get("total_cost_usd", 0.0) or 0.0),
    )

    validation = outcome.validation.as_dict() if outcome.validation else None
    payload = result.result if isinstance(result.result, dict) else {}
    # Ни промпта, ни ответа модели в отчёте о прогоне. Оба — текст документа
    # заказчика: промпт содержит его целиком, `provider_result.result` —
    # замечания по нему. Отчёт уезжает центру в пакете результата
    # (`03_analysis/` входит в возвращаемые префиксы) и разбирается руками, то
    # есть каждая такая копия — это ещё один экземпляр документа за пределами
    # артефакта. От результата остаются служебные поля и отпечаток; сам
    # результат лежит там, где ему и место, — в `02_text_analysis.json`.
    provider_facts = {
        k: v for k, v in result.as_dict().items() if k != "result"
    }
    provider_facts["result_keys"] = sorted(payload.keys())
    provider_facts["result_text_findings"] = len(payload.get("text_findings") or [])
    run_report = {
        "stage": stage_key,
        "transport": "provider_adapter",
        "prompt_build": built["map"],
        "prompt_sha256": pipeline_bridge.sha256_text(prompt),
        "performed_now": bool(outcome.performed),
        "provider_result": provider_facts,
        "validation": validation,
        "ledger": outcome.ledger.as_dict(),
        "soft_contract": provider_transport.soft_contract_report(payload),
        "content_excluded": (
            "Промпт и ответ модели в отчёт не кладутся: это текст документа "
            "заказчика. Отпечаток промпта — prompt_sha256, отпечаток ответа — "
            "provider_result.raw_sha256."
        ),
    }

    resolved_output_dir = _resolve_output_dir(project_id, output_dir=output_dir)
    try:
        # Проверяется КАТАЛОГ, а не один файл в нём: этап пишет три вещи —
        # артефакт, отчёт о прогоне и `audit_trail/`. Гейт на одном пути
        # оставлял бы две записи без проверки.
        _assert_output_inside_attempt(resolved_output_dir, attempt_dir)
    except ProviderOutputPathError as exc:
        detail = str(exc)
        await send_output(on_output, f"[text_analysis] {detail}")
        return 1, detail, CLIResult(result_text=detail, is_error=True)

    # Отчёт о прогоне пишется ВСЕГДА — и при отказе тоже: разбирать неудачу без
    # него пришлось бы по журналу вызовов на чужой машине.
    _write_json(resolved_output_dir / "text_analysis_provider_run.json", run_report)

    if not outcome.ok:
        failed = (validation or {}).get("failed") if validation else None
        if outcome.performed:
            detail = (
                f"provider_result.status={result.status!r} "
                f"error_code={result.error_code!r} detail={result.detail!r} "
                f"validation_failed={failed}"
            )
        else:
            # ПОВТОР уже сохранённой неудачи. Код ошибки сюда НЕ подставляется
            # намеренно: `rate_limited` в тексте распознаётся общим
            # классификатором конвейера как свежий лимит, и раннер уходил бы в
            # цикл ожидания сброса — который ничего не изменит, потому что
            # журнал попытки будет отдавать тот же сохранённый ответ. Повтор
            # обязан выглядеть тем, чем он является: невозможностью повтора.
            detail = (
                "повтор невозможен: результат этого вызова уже записан в журнал "
                f"попытки и неуспешен (проверка: {failed}). Новая попытка "
                "требует нового attempt_id и новой единицы разрешения"
            )
        await send_output(on_output, f"[text_analysis] {detail}")
        # Артефакт НЕ пишется: непринятый результат не имеет права выглядеть
        # как выполненный этап.
        return 1, detail, cli_result

    output_path = resolved_output_dir / TEXT_ANALYSIS_FILENAME
    _write_json(output_path, payload)

    with _scoped_audit_paths(
        output_dir=output_dir, version_dir=version_dir,
        project_id=project_id, version_id=version_id,
    ):
        _save_audit_trail(
            project_id, TEXT_ANALYSIS_STAGE, result.model or "",
            cli_result.input_tokens, cli_result.output_tokens,
            cli_result.duration_ms, payload,
        )

    await send_output(
        on_output,
        f"[text_analysis] provider-режим: {len(payload.get('text_findings') or [])} "
        f"замечаний, модель {result.model!r}, "
        f"{cli_result.input_tokens}->{cli_result.output_tokens} tok",
    )
    return 0, cli_result.result_text, cli_result


async def run_text_analysis(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
    stage_key: str = "text_analysis",
) -> tuple[int, str, AnyResult]:
    """Запустить анализ текста MD-файла -> 02_text_analysis.json (динамический выбор провайдера)."""
    # Мост провайдеров воркера (этап 11D). Развилка стоит ВЫШЕ выбора
    # codex/claude/OpenRouter намеренно: в provider-режиме различается не
    # «каким CLI», а КТО делает файловую работу, и решать это после сборки
    # промпта было бы поздно — промпт уже был бы собран под чужой транспорт.
    #
    # На центре ветки не существует: `active()` смотрит на файл привязки,
    # который выписывает только исполнитель воркера и только на время попытки.
    _bridge = _provider_bridge()
    if _bridge is not None and _bridge.active():
        return await _run_text_analysis_via_provider(
            project_info, project_id, on_output,
            output_dir=output_dir, version_dir=version_dir, version_id=version_id,
            stage_key=stage_key,
        )

    model = get_stage_model("text_analysis")

    if is_codex_model(model):
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
        from backend.app.pipeline.stages.text_analysis.md_chunker import (
            CODEX_TEXT_INPUT_BUDGET,
        )

        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            message_sets, plan_meta = prompt_builder.build_text_analysis_message_sets(
                project_info, project_id, budget=CODEX_TEXT_INPUT_BUDGET,
            )

        # single-pass (обычные проекты + Tier 1) — как раньше, один вызов пишет
        # канонический 02_text_analysis.json.
        if len(message_sets) == 1:
            if plan_meta.get("mode") == "single_no_norms":
                await send_output(
                    on_output,
                    "[text_analysis] промпт > лимита Codex → убрал inline норм-базу "
                    f"({plan_meta.get('chars', 0)} симв., этап 04 перепроверит нормы)",
                )
            return await _run_codex_json_stage(
                stage="text_analysis", messages=message_sets[0], model=model,
                timeout=CLAUDE_TEXT_ANALYSIS_TIMEOUT, project_id=project_id,
                on_output=on_output, output_filename=TEXT_ANALYSIS_FILENAME,
                audit_stage=TEXT_ANALYSIS_STAGE, output_dir=output_dir,
            )

        # Tier 2: нарезка по листам со скелетом → N проходов → merge.
        return await _run_codex_text_analysis_chunked(
            message_sets=message_sets, plan_meta=plan_meta, model=model,
            timeout=CLAUDE_TEXT_ANALYSIS_TIMEOUT, project_id=project_id,
            on_output=on_output, output_dir=output_dir,
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
            _save_audit_trail(project_id, TEXT_ANALYSIS_STAGE, model, cli_result.input_tokens, cli_result.output_tokens, cli_result.duration_ms, cli_result.result_text)
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
        output_path = resolved_output_dir / TEXT_ANALYSIS_FILENAME
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
            project_id, TEXT_ANALYSIS_STAGE, result.model,
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
        # NORM_VERIFY_TOOLS несёт mcp__norms__*: без него codex-ветка уходила в
        # JSON-режим вообще без сервера норм и сверяла их статус по памяти
        # модели — молча, без единой ошибки.
        return await _run_codex_json_stage(
            stage="norm_verify", messages=messages, model=model,
            timeout=CLAUDE_NORM_VERIFY_TIMEOUT, project_id=project_id,
            on_output=on_output, output_filename=llm_out_filename,
            audit_stage="04_norm_verify", output_dir=output_dir,
            allowed_tools=NORM_VERIFY_TOOLS,
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


# ─── Пересмотр ОПТИМИЗАЦИЙ после верификации норм ────────────────────

async def run_optimization_norm_fix(
    optimizations_to_fix_text: str,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    project_info: Optional[dict] = None,
    *,
    output_dir: str | Path | None = None,
    version_dir: str | Path | None = None,
    version_id: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Вернуть предложения автору с вердиктом по норме — пусть переосмыслит.

    Зеркало run_norm_fix, но артефакт — optimization.json, а исход шире, чем
    «поправить ссылку»: still_valid / revised / obsolete. Модель берём из
    `norm_fix`: это тот же норм-driven пересмотр, только над другим файлом, —
    отдельная строка в конфиге моделей не нужна.
    """
    model = get_stage_model("norm_fix")

    def _task() -> str:
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            return prepare_optimization_norm_fix_task(
                optimizations_to_fix_text, project_id,
                project_info=project_info,
            )

    if is_codex_model(model):
        task_text = (
            "## Codex exec mode override\n\n"
            "This task template may mention Claude Read/Write tools and MCP `norms` tools. "
            "In this Codex exec run, use direct filesystem access instead of Read/Write tools, "
            "do not call MCP or web tools, and treat `norm_checks.json` as the authoritative "
            "norm status source already produced by Python. Use `replacement_doc`, "
            "`current_version`, `status`, `paragraph_checks`, and `affected_optimizations` from "
            "`norm_checks.json`; if exact clause text is unavailable, mark the item with "
            "`norm_status: warning` or preserve the wording while adding `norm_revision`. "
            "Never delete items. Do not refuse only because MCP tools are unavailable.\n\n"
            + _task()
        )
        exit_code, combined, cli_result = await _run_cli(
            task_text, NORM_VERIFY_TOOLS, CLAUDE_NORM_FIX_TIMEOUT,
            on_output, stage="norm_fix", project_id=project_id,
            model=model,
        )
        _save_audit_trail(
            project_id, "05b_optimization_norm_fix", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
            output_dir=output_dir,
        )
        return exit_code, combined, cli_result

    if is_claude_stage("norm_fix"):
        exit_code, combined, cli_result = await _run_cli(
            _task(), NORM_VERIFY_TOOLS, CLAUDE_NORM_FIX_TIMEOUT,
            on_output, stage="norm_fix", project_id=project_id,
            model=model,
        )
        _save_audit_trail(
            project_id, "05b_optimization_norm_fix", model,
            0, 0, cli_result.duration_ms, cli_result.result_text,
            output_dir=output_dir,
        )
        return exit_code, combined, cli_result

    # OpenRouter: агентного доступа к файлам нет — модель возвращает JSON, пишем сами.
    import backend.app.services.llm.llm_runner as llm_runner

    result = await llm_runner.run_llm(
        stage="norm_fix",
        messages=[{"role": "user", "content": _task()}],
        timeout=CLAUDE_NORM_FIX_TIMEOUT,
    )
    if result.json_data and not result.is_error:
        target = _resolve_output_dir(project_id, output_dir=output_dir) / "optimization.json"
        _write_json(target, result.json_data)

    await _send_status_llm(on_output, result)
    _save_audit_trail(
        project_id, "05b_optimization_norm_fix", result.model,
        result.input_tokens, result.output_tokens,
        result.duration_ms, _build_llm_audit_payload(result),
        output_dir=output_dir,
    )
    return (0 if not result.is_error else 1), result.text, result


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
    model_override: str | None = None,
    visual_output_dir: str | Path | None = None,
    reasoning_effort_override: str | None = None,
) -> tuple[int, str, AnyResult]:
    """Запустить анализ оптимизации -> optimization.json (динамический выбор провайдера)."""
    model = model_override or get_stage_model("optimization")

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
            resolved_output_dir = _resolve_output_dir(
                project_id,
                output_dir=visual_output_dir if visual_output_dir is not None else output_dir,
            )
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

        cli_kwargs: dict[str, Any] = {
            "stage": "optimization",
            "project_id": project_id,
            "model": model,
            "image_paths": image_paths,
        }
        if reasoning_effort_override:
            cli_kwargs["reasoning_effort"] = reasoning_effort_override
        exit_code, combined, cli_result = await _run_cli(
            task_text, OPTIMIZATION_TOOLS, CLAUDE_OPTIMIZATION_TIMEOUT,
            on_output, **cli_kwargs,
        )

        _save_audit_trail(
            project_id, "05_optimization", model,
            0, 0, cli_result.duration_ms,
            {
                "result_text": cli_result.result_text,
                "codex_exec_agentic": True,
                "reasoning_effort": reasoning_effort_override or "default",
                "attached_images": [str(path) for path in image_paths],
            },
            output_dir=output_dir,
        )

        return exit_code, combined, cli_result

    if model.startswith("claude-"):
        with _scoped_audit_paths(
            output_dir=output_dir, version_dir=version_dir,
            project_id=project_id, version_id=version_id,
        ):
            task_text = prepare_optimization_task(project_info, project_id)
        exit_code, combined, cli_result = await _run_cli(
            task_text, OPTIMIZATION_TOOLS, CLAUDE_OPTIMIZATION_TIMEOUT,
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
            project_id, f"01_block_batch_{batch_id:03d}", model,
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
            project_id, f"01_block_batch_{batch_id:03d}", gd_result.model_id,
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
        project_id, f"01_block_batch_{batch_id:03d}", result.model,
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
    """Legacy: запускает text_analysis вместо триажа.

    `stage_key="triage"` существенен в provider-режиме: ключ вызова в журнале
    попытки считается по (attempt, provider, purpose, prompt). С общим purpose
    триаж и текстовый анализ в одной попытке делили бы одну запись, и второй
    молча получил бы ответ первого как свой результат.
    """
    return await run_text_analysis(
        project_info, project_id, on_output,
        output_dir=output_dir, version_dir=version_dir, version_id=version_id,
        stage_key="triage",
    )
