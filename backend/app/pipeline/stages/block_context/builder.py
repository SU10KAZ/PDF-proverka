"""Build block context locally from PDF vectors, Vectograph profiles, or PNG."""
from __future__ import annotations

import hashlib
import inspect
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from backend.app.pipeline.stages.block_grounding.block_source_router import resolve_block_source
from backend.app.services.storage.stage_artifacts import BLOCK_CONTEXT_SUMMARY_FILENAME

from .contract import SCHEMA_VERSION, STAGE

ProgressCb = Callable[[dict[str, Any]], Awaitable[None] | None]


async def _emit(callback: ProgressCb | None, event: dict[str, Any]) -> None:
    if callback is None:
        return
    result = callback(event)
    if inspect.isawaitable(result):
        await result


def _hash_text(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


async def build_block_context(
    project_dir: Path,
    *,
    output_dir: Path,
    blocks_index_path: Path,
    progress_cb: ProgressCb | None = None,
) -> dict[str, Any]:
    """Create the canonical summary without any model or network call."""
    project_dir = Path(project_dir)
    output_dir = Path(output_dir)
    if not blocks_index_path.is_file():
        raise FileNotFoundError(f"{blocks_index_path} не найден")
    index = json.loads(blocks_index_path.read_text(encoding="utf-8"))
    blocks = [
        item for item in index.get("blocks") or []
        if isinstance(item, dict) and str(item.get("block_type") or "").lower() == "image"
    ]
    await _emit(progress_cb, {"type": "started", "total": len(blocks)})

    prepared: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for pos, block in enumerate(blocks, start=1):
        block_id = str(block.get("block_id") or "")
        page = block.get("page")
        text, source = resolve_block_source(output_dir, block_id, page)
        if source == "gemma_fallback":
            source = "image_only"
        png = blocks_index_path.parent / str(block.get("file") or "")
        warnings: list[str] = []
        if source in {"no_sources", "block_not_found", "error"} and png.is_file():
            warnings.append(f"{source}: fallback to image")
            source = "image_only"
        if source == "image_only" and not png.is_file():
            source = "missing"
            warnings.append("PNG missing")
        coverage = "ready_image_only" if source == "image_only" else (
            "ready" if source != "missing" else "error"
        )
        counts[source] = counts.get(source, 0) + 1
        prepared.append({
            "block_id": block_id,
            "page": page,
            "source_kind": source,
            "coverage_status": coverage,
            "context_hash": _hash_text(text),
            "warnings": warnings,
        })
        await _emit(progress_cb, {
            "type": "block_done",
            "block_id": block_id,
            "page": page,
            "source_kind": source,
            "ok": coverage != "error",
            "completed": pos,
            "total": len(blocks),
        })

    ready = sum(item["coverage_status"] != "error" for item in prepared)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "status": "ok" if ready == len(prepared) else ("partial" if ready else "failed"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "project_dir": str(project_dir),
        "blocks_total": len(prepared),
        "blocks_ready": ready,
        "blocks_failed": len(prepared) - ready,
        "source_counts": counts,
        "blocks": prepared,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / BLOCK_CONTEXT_SUMMARY_FILENAME
    temp = target.with_suffix(target.suffix + ".tmp")
    temp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, target)
    await _emit(progress_cb, {"type": "completed", "summary": summary})
    return summary
