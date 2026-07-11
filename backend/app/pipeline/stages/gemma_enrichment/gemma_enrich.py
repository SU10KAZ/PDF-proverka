"""Compatibility adapter for the retired model-backed OCR implementation.

Existing imports may keep calling ``enrich_project`` during the transition.
The implementation is local and only builds the canonical block-context summary.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.app.pipeline.stages.block_context.builder import build_block_context
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    STAGE02_BLOCKS_DIRNAME,
    gemma_output_root,
)

DEFAULT_MODEL = "local-block-context"
DEFAULT_PARALLELISM = 1
DEFAULT_TIMEOUT_S = 60


async def enrich_project(
    project_dir: str | Path,
    *,
    progress_cb=None,
    output_dir: str | Path | None = None,
    **_legacy_options: Any,
) -> dict:
    project_path = Path(project_dir)
    target = Path(output_dir) if output_dir is not None else gemma_output_root(project_path)
    return await build_block_context(
        project_path,
        output_dir=target,
        blocks_index_path=target / STAGE02_BLOCKS_DIRNAME / "index.json",
        progress_cb=progress_cb,
    )


async def retry_failed_blocks(project_dir: str | Path, **options: Any) -> dict:
    """Legacy retry now performs a deterministic local context rebuild."""
    return await enrich_project(project_dir, **options)
