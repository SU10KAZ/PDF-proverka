"""Legacy stage alias for local block-context preparation."""
from __future__ import annotations

from backend.app.pipeline.stages.block_context.runner import run_block_context_stage


async def run_gemma_enrichment_stage(ctx, *, force: bool = False):
    """Accept the old stage call without invoking model-backed OCR."""
    return await run_block_context_stage(ctx, force=force)
