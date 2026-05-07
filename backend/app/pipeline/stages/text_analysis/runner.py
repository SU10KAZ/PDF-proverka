"""
text_analysis/runner.py
-----------------------
Stage runner для этапа text_analysis (анализ текста MD через Claude).

Покрывает все три call-site в manager.py:
  1. _run_resumed_pipeline  — стандартный run_text_analysis, без rate-limit retry
  2. _run_ocr_pipeline      — run_text_analysis с rate-limit retry
  3. _run_smart_pipeline    — run_triage (= run_text_analysis с тем же prompt)

Публичный API:
  run_text_analysis(ctx, *, stage_label, use_triage) -> StageResult
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import backend.app.services.llm.claude_runner as claude_runner
from backend.app.pipeline.stage_result import StageResult
from backend.app.services.common.cli_utils import is_cancelled, is_rate_limited

if TYPE_CHECKING:
    from backend.app.pipeline.context import PipelineStageContext


def _error_detail(exit_code: int, output: str, max_len: int = 120) -> str:
    if not output:
        return f"Exit code {exit_code}"
    lines = output.strip().splitlines()
    useful = []
    skip_prefixes = ("╭", "╰", "│", "─", "⎿", "⏎", "\\", "  ", "Usage:", "Duration:")
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if any(stripped.startswith(p) for p in skip_prefixes):
            continue
        lower = stripped.lower()
        if any(kw in lower for kw in ("error", "ошибка", "failed", "timeout", "timed out",
                                       "rate limit", "overloaded", "connection", "refused",
                                       "exception", "traceback", "permission", "not found",
                                       "invalid", "json", "unable", "cannot")):
            useful.insert(0, stripped)
            if len(useful) >= 3:
                break
        elif not useful:
            useful.append(stripped)
    if useful:
        msg = " | ".join(useful)
        return msg[:max_len - 3] + "..." if len(msg) > max_len else msg
    return f"Exit code {exit_code}"


async def run_text_analysis(
    ctx: "PipelineStageContext",
    *,
    stage_label: str = "text_analysis",
    use_triage: bool = False,
    with_rate_limit_retry: bool = True,
) -> StageResult:
    """Запуск текстового анализа или триажа страниц через claude_runner.

    Аргументы:
        stage_label: ключ для update_pipeline_log и record_cli_usage.
            Обычно "text_analysis". Используется как есть в pipeline_log.
        use_triage: если True — вызывает claude_runner.run_triage вместо
            run_text_analysis. По семантике идентично (run_triage = run_text_analysis),
            но stage_label отличается ("triage").
        with_rate_limit_retry: если True — при rate limit ждёт и повторяет.
            _run_resumed_pipeline не делал retry, _run_ocr_pipeline делал.

    Управляет:
    - update_pipeline_log("text_analysis" / stage_label, "running" → "done" / "error");
    - rate limit check + optional retry;
    - cancel check;
    - проверкой создания 01_text_analysis.json;
    - record_cli_usage.

    Не управляет:
    - job.stage / job.status (выставляет оркестратор);
    - heartbeat / cleanup (оркестратор);
    - очисткой старых файлов перед запуском (оркестратор);
    - чтением triage_data / priority_pages (оркестратор читает 01_text_analysis.json).
    """
    pid = ctx.project_id
    output_dir = ctx.output_dir
    project_info = ctx.project_info or {}

    log_stage = stage_label  # ключ в pipeline_log

    ctx.update_pipeline_log(log_stage, "running")

    # ── Pre-launch gate ──
    can_go = await ctx.check_before_launch()
    if not can_go:
        error = "Rate limit: ожидание превышено или отменено"
        ctx.update_pipeline_log(log_stage, "error", error=error)
        return StageResult.fail(error)

    # ── Запуск LLM ──
    _runner = claude_runner.run_triage if use_triage else claude_runner.run_text_analysis
    _usage_label = "triage" if use_triage else "text_analysis"

    exit_code, output, cli_result = await _runner(
        project_info, pid,
        on_output=ctx.log,
    )
    ctx.record_cli_usage(cli_result, _usage_label)

    if is_cancelled(exit_code):
        return StageResult.cancel()

    # ── Rate limit retry (опционально) ──
    if with_rate_limit_retry and is_rate_limited(exit_code, output or "", ""):
        await ctx.log(
            f"Rate limit при {'триаже' if use_triage else 'текстовом анализе'}, ожидание...",
            "warn",
        )
        can_continue = await ctx.wait_for_rate_limit(
            f"rate limit при {'триаже' if use_triage else 'текстовом анализе'}",
            output or "",
        )
        if not can_continue:
            error = "Rate limit: ожидание превышено или отменено"
            ctx.update_pipeline_log(log_stage, "error", error=error)
            return StageResult.fail(error)

        exit_code, output, cli_result = await _runner(
            project_info, pid,
            on_output=ctx.log,
        )
        ctx.record_cli_usage(cli_result, f"{_usage_label}_retry")

        if is_cancelled(exit_code):
            return StageResult.cancel()

    if exit_code != 0:
        error = _error_detail(exit_code, output or "")
        ctx.update_pipeline_log(log_stage, "error", error=error)
        label = "Триаж" if use_triage else "Текстовый анализ"
        return StageResult.fail(f"{label}: код {exit_code}")

    # ── Проверка выходного файла ──
    output_path = output_dir / "01_text_analysis.json"
    if not output_path.exists():
        error = "01_text_analysis.json не создан"
        ctx.update_pipeline_log(log_stage, "error", error=error)
        return StageResult.fail(error)

    ctx.update_pipeline_log(log_stage, "done", message="OK")
    return StageResult.ok(output_path=str(output_path))
