"""
context.py
----------
PipelineStageContext — общий контекст, передаваемый в stage runner-модули.

Цель: постепенно избавить _run_* методы PipelineManager от прямой зависимости
от self. Каждый stage runner получает только то, что ему нужно, через этот
dataclass — log, check_before_launch, run_subprocess и т.д.

Текущий статус: используется во всех stage runners (passes 4–13).
Финальный состав полей — в docs/pipeline_stage_context_plan.md.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional


@dataclass
class PipelineStageContext:
    """Контекст выполнения одного этапа пайплайна.

    Содержит только безопасные callback-и и общие зависимости.
    Не держит ссылку на PipelineManager — stage runner-ы должны быть
    тестируемы без него.
    """

    project_dir: Path
    project_id: str
    output_dir: Path

    # Async log: await ctx.log("сообщение") / await ctx.log("сообщение", "warn")
    log: Callable[..., Awaitable[None]]

    # Pre-launch gate: rate limit + pause check.
    # Returns True = можно запускать, False = job отменён или rate limit истёк.
    check_before_launch: Callable[[], Awaitable[bool]]

    # Async pause gate: returns True = продолжать, False = job отменён.
    check_pause: Callable[[], Awaitable[bool]]

    # Rate limit wait: returns True = лимит сброшен, False = отменено/таймаут.
    # Signature: (reason: str, cli_output: str) -> bool
    wait_for_rate_limit: Callable[[str, str], Awaitable[bool]]

    # Запись использования CLI (usage tracking).
    # Signature: (cli_result, stage_name: str, is_retry: bool = False) -> None
    record_cli_usage: Callable[..., None]

    # Обновление pipeline_log.json.
    # Signature: (stage_key: str, status: str, **kwargs) -> None
    update_pipeline_log: Callable[..., None]

    # Async subprocess runner (аналог self._run_script).
    # Signature: (*args, **kwargs) -> (exit_code, stdout, stderr)
    run_subprocess: Callable[..., Awaitable[tuple[int, str, str]]]

    # Опциональные поля — могут быть None если этап не нуждается в них.
    project_info: Optional[dict] = field(default=None)

    # Коллбэк для прогресса (current, total).
    progress: Optional[Callable[[int, int], Awaitable[None]]] = field(default=None)

    # ID объекта (для multi-object поддержки).
    object_id: Optional[str] = field(default=None)

    # Async WebSocket стрим findings-событий (для findings_review stage).
    # Signature: (stage: str) -> None
    # Допустимые значения stage: "merge", "critic", "corrector", "done"
    stream_findings_events: Optional[Callable[[str], Awaitable[None]]] = field(default=None)

    # Сброс прогресса job при переходе между этапами.
    # Signature: () -> None
    reset_job_progress: Optional[Callable[[], None]] = field(default=None)

    # Обновление deterministic quality-метаданных findings после corrector.
    # Signature: () -> None
    refresh_finding_quality: Optional[Callable[[], None]] = field(default=None)

    # Синхронный прогресс через run_coroutine_threadsafe (для block_analysis
    # где _on_progress вызывается из executor thread).
    # Signature: (current: int, total: int) -> None
    progress_sync: Optional[Callable[[int, int], None]] = field(default=None)

    # Учёт стоимости block_analysis (findings_only mode) в usage tracker.
    # Signature: (summary: dict) -> None
    record_block_analysis_usage: Optional[Callable[[dict], None]] = field(default=None)

    # Синхронная проверка отмены job (используется внутри thread callbacks).
    # Signature: () -> bool — True если job отменён.
    is_cancelled: Optional[Callable[[], bool]] = field(default=None)
