"""
decision_carryover/runner.py
----------------------------
Stage runner для переноса вердиктов эксперта (согласовано/отклонено) из
предыдущей проверенной версии в текущую.

Публичный API:
  run_decision_carryover_stage(ctx) -> StageResult

Особенности:
- fail-soft: любая ошибка этапа НЕ валит весь аудит (StageResult.ok());
- синхронный сервис (внутри `claude -p` Sonnet) вызывается через asyncio.to_thread,
  чтобы не блокировать event loop;
- для первой/единственной версии (нет предыдущей проверенной) этап skip.
"""
from __future__ import annotations

import asyncio

from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stage_result import StageResult

STAGE_KEY = "decision_carryover"


def _resolve_version_id(ctx: PipelineStageContext):
    """version_id этапа: из контекста, иначе latest версии проекта."""
    version_id = ctx.version_id
    if version_id:
        return version_id
    try:
        from backend.app.services.common import version_service
        from backend.app.services.common.project_service import resolve_project_dir
        pdir = resolve_project_dir(ctx.project_id)
        return version_service.get_latest_version_id(pdir, ctx.project_id)
    except Exception:
        return None


async def run_decision_carryover_stage(ctx: PipelineStageContext) -> StageResult:
    """Перенести вердикты из предыдущей проверенной версии в текущую."""
    ctx.update_pipeline_log(STAGE_KEY, "running")
    await ctx.log("═══ Перенос вердиктов из предыдущей версии ═══")

    from backend.app.services.findings import decision_carryover_service as dc

    if not dc.is_enabled():
        ctx.update_pipeline_log(STAGE_KEY, "skipped", message="disabled")
        await ctx.log("Перенос вердиктов отключён (DECISION_CARRYOVER_ENABLED=0)")
        return StageResult.ok()

    version_id = _resolve_version_id(ctx)
    if not version_id or version_id == "v1":
        ctx.update_pipeline_log(STAGE_KEY, "skipped", message="Нет предыдущей версии")
        await ctx.log("Первая/единственная версия — переносить вердикты не из чего")
        return StageResult.ok()

    try:
        result = await asyncio.to_thread(
            dc.run_decision_carryover, ctx.project_id, version_id
        )
    except Exception as e:  # noqa: BLE001 — fail-soft: не валим аудит
        error = str(e)[:200]
        ctx.update_pipeline_log(STAGE_KEY, "error", error=error)
        await ctx.log(f"Перенос вердиктов: ошибка — {error}", "warn")
        return StageResult.ok()

    status = result.get("status")
    if status == "skipped":
        ctx.update_pipeline_log(STAGE_KEY, "skipped", message=result.get("reason", ""))
        await ctx.log(f"Перенос вердиктов пропущен: {result.get('reason', '')}")
        return StageResult.ok()

    if result.get("source_version_id") is None:
        ctx.update_pipeline_log(STAGE_KEY, "skipped", message="Нет предыдущей проверенной версии")
        await ctx.log("Нет предыдущей проверенной версии — переносить нечего")
        return StageResult.ok()

    summary = result.get("summary") or {}
    msg = (
        f"Перенесено {summary.get('carried_over', 0)} "
        f"(согл. {summary.get('carried_accepted', 0)}, откл. {summary.get('carried_rejected', 0)}), "
        f"на ручную проверку {summary.get('needs_manual_review', 0)}"
    )
    ctx.update_pipeline_log(STAGE_KEY, "done", message=msg)
    await ctx.log(f"═══ {msg} ═══")
    return StageResult.ok()
