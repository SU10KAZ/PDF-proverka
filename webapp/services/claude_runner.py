"""
Claude CLI runner.
Запуск Claude CLI для различных задач аудита.
Формирование задач вынесено в task_builder.py, утилиты — в cli_utils.py.
"""
from typing import Optional, Callable, Awaitable

from webapp.config import (
    CLAUDE_CLI,
    NORM_VERIFY_TOOLS,
    TEXT_ANALYSIS_TOOLS, BLOCK_ANALYSIS_TOOLS, FINDINGS_MERGE_TOOLS,
    TILE_AUDIT_TOOLS, MAIN_AUDIT_TOOLS, TRIAGE_TOOLS, SMART_MERGE_TOOLS,
    get_claude_model,
    CLAUDE_NORM_VERIFY_TIMEOUT, CLAUDE_NORM_FIX_TIMEOUT,
    CLAUDE_OPTIMIZATION_TIMEOUT,
    CLAUDE_TEXT_ANALYSIS_TIMEOUT, CLAUDE_BLOCK_ANALYSIS_TIMEOUT,
    CLAUDE_FINDINGS_MERGE_TIMEOUT,
    CLAUDE_BATCH_TIMEOUT, CLAUDE_AUDIT_TIMEOUT,
    CLAUDE_TRIAGE_TIMEOUT, CLAUDE_SMART_MERGE_TIMEOUT,
)
from webapp.services.cli_utils import (
    is_cancelled, is_timeout, is_rate_limited,
    parse_rate_limit_reset,
    parse_cli_json_output, send_output,
)
from webapp.services.task_builder import (
    prepare_norm_verify_task,
    prepare_norm_fix_task,
    prepare_optimization_task,
    prepare_text_analysis_task,
    prepare_block_batch_task,
    prepare_findings_merge_task,
    prepare_tile_batch_task,
    prepare_main_audit_task,
    prepare_triage_task,
    prepare_smart_merge_task,
)
from webapp.services.process_runner import run_command
from webapp.models.usage import CLIResult

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
    # task_builder — блоковый пайплайн
    "prepare_text_analysis_task", "prepare_block_batch_task",
    "prepare_findings_merge_task",
    # legacy stubs (перенаправляют на новый пайплайн)
    "prepare_tile_batch_task", "prepare_main_audit_task",
    "prepare_triage_task", "prepare_smart_merge_task",
    "run_tile_batch", "run_main_audit", "run_triage", "run_smart_merge",
]


# ─── Вспомогательная функция для построения команды ───

def _build_cmd(tools: str) -> list[str]:
    """Построить базовую команду Claude CLI."""
    return [
        CLAUDE_CLI,
        "-p",
        "--model", get_claude_model(),
        "--allowedTools", tools,
        "--output-format", "json",
    ]


async def _run_cli(
    task_text: str,
    tools: str,
    timeout: int,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    include_stderr: bool = True,
) -> tuple[int, str, CLIResult]:
    """
    Общий запуск Claude CLI.

    Returns:
        (exit_code, combined_text, cli_result)
    """
    cmd = _build_cmd(tools)

    exit_code, stdout, stderr = await run_command(
        cmd,
        input_text=task_text,
        on_output=None,
        env_overrides={"CLAUDECODE": None},
        timeout=timeout,
    )

    cli_result = parse_cli_json_output(stdout)
    await send_output(on_output, cli_result.result_text)

    combined = cli_result.result_text
    if include_stderr and stderr and stderr.strip():
        await send_output(on_output, f"[STDERR]: {stderr.strip()}")
        combined += f"\n[STDERR]: {stderr.strip()}"

    return exit_code, combined, cli_result


# ─── Верификация нормативных ссылок ───

async def run_norm_verify(
    norms_list_text: str,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Запустить Claude CLI для верификации нормативных ссылок через WebSearch."""
    task_text = prepare_norm_verify_task(norms_list_text, project_id)
    return await _run_cli(task_text, NORM_VERIFY_TOOLS, CLAUDE_NORM_VERIFY_TIMEOUT, on_output, include_stderr=False)


async def run_norm_fix(
    findings_to_fix_text: str,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Запустить Claude CLI для пересмотра замечаний с учётом актуальных норм."""
    task_text = prepare_norm_fix_task(findings_to_fix_text, project_id)
    return await _run_cli(task_text, NORM_VERIFY_TOOLS, CLAUDE_NORM_FIX_TIMEOUT, on_output, include_stderr=False)


# ─── Оптимизация проектных решений ───

async def run_optimization(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Запустить Claude CLI для анализа оптимизации."""
    task_text = prepare_optimization_task(project_info, project_id)
    return await _run_cli(task_text, TEXT_ANALYSIS_TOOLS, CLAUDE_OPTIMIZATION_TIMEOUT, on_output)


# ─── Анализ текста ───

async def run_text_analysis(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Запустить Claude CLI для текстового анализа MD-файла."""
    task_text = prepare_text_analysis_task(project_info, project_id)
    return await _run_cli(task_text, TEXT_ANALYSIS_TOOLS, CLAUDE_TEXT_ANALYSIS_TIMEOUT, on_output)


# ─── Анализ пакета image-блоков ───

async def run_block_batch(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Запустить Claude CLI для одного пакета image-блоков."""
    task_text = prepare_block_batch_task(
        batch_data, project_info, project_id, total_batches
    )
    return await _run_cli(task_text, BLOCK_ANALYSIS_TOOLS, CLAUDE_BLOCK_ANALYSIS_TIMEOUT, on_output)


# ─── Свод замечаний ───

async def run_findings_merge(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Запустить Claude CLI для свода замечаний из текста + блоков."""
    task_text = prepare_findings_merge_task(project_info, project_id)
    return await _run_cli(task_text, FINDINGS_MERGE_TOOLS, CLAUDE_FINDINGS_MERGE_TIMEOUT, on_output)


# ─── Legacy stubs (перенаправляют на блоковый пайплайн) ───

async def run_tile_batch(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Legacy: перенаправляет на run_block_batch."""
    return await run_block_batch(batch_data, project_info, project_id, total_batches, on_output)


async def run_main_audit(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Legacy: запускает text_analysis вместо старого монолитного аудита."""
    return await run_text_analysis(project_info, project_id, on_output)


async def run_triage(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Legacy: запускает text_analysis вместо триажа."""
    return await run_text_analysis(project_info, project_id, on_output)


async def run_smart_merge(
    project_info: dict,
    project_id: str,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, CLIResult]:
    """Legacy: запускает findings_merge вместо smart_merge."""
    return await run_findings_merge(project_info, project_id, on_output)
