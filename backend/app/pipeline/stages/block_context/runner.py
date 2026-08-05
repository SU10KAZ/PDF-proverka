"""Pipeline runner for local block-context preparation."""
from __future__ import annotations

from backend.app.pipeline.stage_result import StageResult
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    STAGE02_BLOCKS_DIRNAME,
)

from .builder import build_block_context
from .contract import (
    STAGE_TITLE,
    block_context_up_to_date,
    validate_block_context_summary,
)


async def run_block_context_stage(ctx, *, force: bool = False) -> StageResult:
    index_path = ctx.output_dir / STAGE02_BLOCKS_DIRNAME / "index.json"

    # Контекст, уже собранный «Подготовкой данных», переиспользуем: запуск
    # аудита не должен гонять этап заново. Сверяем не только валидность
    # сводки, но и покрытие текущего index.json — иначе после докропа новые
    # блоки остались бы без контекста. force (retry/resume этапа) пропуск
    # игнорирует.
    if not force:
        state = block_context_up_to_date(ctx.output_dir, blocks_index_path=index_path)
        if state.get("ready"):
            summary = state.get("summary") or {}
            message = (
                f"{STAGE_TITLE}: уже собран, пропускаю "
                f"({summary.get('blocks_ready', 0)}/{summary.get('blocks_total', 0)})"
            )
            ctx.update_pipeline_log("block_context", "done", message=message)
            ctx.update_pipeline_log("gemma_enrichment", "done", message=message)
            return StageResult.ok(message=message, summary=summary)

    ctx.update_pipeline_log("block_context", "running")

    async def _progress(event: dict) -> None:
        if event.get("type") == "block_done" and ctx.progress_sync:
            ctx.progress_sync(event.get("completed", 0), event.get("total", 0))

    try:
        summary = await build_block_context(
            ctx.project_dir,
            output_dir=ctx.output_dir,
            blocks_index_path=index_path,
            progress_cb=_progress,
        )
    except Exception as exc:
        ctx.update_pipeline_log("block_context", "error", error=str(exc))
        return StageResult.fail(f"{STAGE_TITLE}: {exc}")

    validation = validate_block_context_summary(ctx.output_dir, canonical_only=True)
    if not validation.get("valid"):
        error = f"Канонический block_context summary невалиден: {validation.get('reason')}"
        ctx.update_pipeline_log("block_context", "error", error=error)
        return StageResult.fail(error)

    failed = int(summary.get("blocks_failed") or 0)
    total = int(summary.get("blocks_total") or 0)
    status = "partial" if failed else "done"
    catalog = summary.get("reference_catalog") or {}
    message = (
        f"{STAGE_TITLE}: {total - failed}/{total}; "
        f"каталог {catalog.get('catalog_version') or '?'}; "
        f"{summary.get('source_counts') or {}}"
    )
    ctx.update_pipeline_log("block_context", status, message=message)
    # Compatibility status key for old UI/API clients during the transition.
    ctx.update_pipeline_log("gemma_enrichment", status, message=message)
    return StageResult.ok(message=message, summary=summary)
