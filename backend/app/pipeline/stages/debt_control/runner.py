"""
debt_control/runner.py
----------------------
Stage runner «Контроль долгов»: согласованные (accepted) замечания предыдущей
проверенной версии не должны теряться при новом аудите.

Обёртка над migrated_findings_service.run_migrated_findings_check:
- V2-аудит нашёл то же замечание → существующее finding обогащается origin-метой
  (бейдж «Связано с V1»);
- не нашёл, но проблема ещё видна в документе → в 03_findings.json добавляется
  виртуальное MIG-замечание «согласовано в V1, осталось актуальным»;
- не нашёл совсем → отчёт «возможно устранено / проверить вручную».

Публичный API:
  run_debt_control_stage(ctx) -> StageResult

Особенности:
- fail-soft: ошибка этапа НЕ валит аудит;
- сервис синхронный (файловый I/O, LLM-recheck по умолчанию OFF) — вызывается
  через asyncio.to_thread;
- для первой версии / без проверенной предыдущей — skip;
- ставится ПЕРЕД decision_carryover: добавленные MIG-замечания тут же получают
  вердикт «согласовано» переносом (то же замечание V1 accepted).
"""
from __future__ import annotations

import asyncio
import os

from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stage_result import StageResult

STAGE_KEY = "debt_control"


def is_enabled() -> bool:
    """Kill-switch. Default ON («Контроль долгов» всегда в пайплайне для V2+)."""
    return os.environ.get("DEBT_CONTROL_ENABLED", "1").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _resolve_version_id(ctx: PipelineStageContext):
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


async def run_debt_control_stage(ctx: PipelineStageContext) -> StageResult:
    """Проверить, что согласованные замечания прошлой версии не потерялись."""
    ctx.update_pipeline_log(STAGE_KEY, "running")
    await ctx.log("═══ Контроль долгов (согласованные замечания прошлой версии) ═══")

    if not is_enabled():
        ctx.update_pipeline_log(STAGE_KEY, "skipped", message="disabled")
        await ctx.log("Контроль долгов отключён (DEBT_CONTROL_ENABLED=0)")
        return StageResult.ok()

    version_id = _resolve_version_id(ctx)
    if not version_id or version_id == "v1":
        ctx.update_pipeline_log(STAGE_KEY, "skipped", message="Нет предыдущей версии")
        await ctx.log("Первая/единственная версия — контролировать долги не из чего")
        return StageResult.ok()

    from backend.app.services.findings import migrated_findings_service as mfs

    try:
        result = await asyncio.to_thread(
            mfs.run_migrated_findings_check, ctx.project_id, version_id
        )
    except mfs.MigratedFindingsError as e:
        ctx.update_pipeline_log(STAGE_KEY, "skipped", message=str(e)[:120])
        await ctx.log(f"Контроль долгов пропущен: {e}")
        return StageResult.ok()
    except Exception as e:  # noqa: BLE001 — fail-soft: не валим аудит
        error = str(e)[:200]
        ctx.update_pipeline_log(STAGE_KEY, "error", error=error)
        await ctx.log(f"Контроль долгов: ошибка — {error}", "warn")
        return StageResult.ok()

    if result.get("source_version_id") is None:
        ctx.update_pipeline_log(STAGE_KEY, "skipped",
                                message="Нет предыдущей проверенной версии")
        await ctx.log("Нет предыдущей проверенной версии — долгов нет")
        return StageResult.ok()

    report = result.get("report") or {}
    msg = (
        f"Долгов V-пред: {report.get('total_previous_accepted_findings', 0)}; "
        f"найдены в новой версии: {report.get('duplicate_of_new_finding', 0)}, "
        f"добавлены как актуальные: {report.get('still_relevant', 0)}, "
        f"возможно устранены: {report.get('possibly_resolved', 0) + report.get('not_found_in_new_version', 0)}, "
        f"на ручную проверку: {report.get('needs_manual_review', 0)}"
    )
    ctx.update_pipeline_log(STAGE_KEY, "done", message=msg)
    await ctx.log(f"═══ Контроль долгов: {msg} ═══")
    return StageResult.ok()
