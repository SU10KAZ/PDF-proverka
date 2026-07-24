"""Build block context locally from PDF vectors, Vectograph profiles, or PNG."""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from backend.app.pipeline.stages.block_grounding.block_profile_registry import (
    ARTIFACT_DIRNAME,
    artifact_filename,
    make_package,
)
from backend.app.pipeline.stages.block_grounding.block_source_router import (
    resolve_block_package,
    resolve_block_source as _canonical_resolve_block_source,
)
from backend.app.pipeline.stages.block_context.reference_catalog import catalog_runtime_info

# Публичное имя оставлено для совместимости расширений/тестов, которые подменяли
# прежний двухэлементный резолвер builder.resolve_block_source.
resolve_block_source = _canonical_resolve_block_source
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
    profile_counts: dict[str, int] = {}
    reference_selection_counts: dict[str, int] = {}
    reference_confidence_counts: dict[str, int] = {}
    graph_dir = output_dir / ARTIFACT_DIRNAME
    graph_dir.mkdir(parents=True, exist_ok=True)
    expected_artifacts: set[str] = set()
    for pos, block in enumerate(blocks, start=1):
        block_id = str(block.get("block_id") or "")
        page = block.get("page")
        if resolve_block_source is not _canonical_resolve_block_source:
            legacy_text, legacy_source = await asyncio.to_thread(
                resolve_block_source,
                output_dir,
                block_id,
                page,
            )
            package = make_package(
                block_id=block_id, page=page, source_kind=legacy_source,
                user_text=legacy_text,
            )
        else:
            # PDF/vector parsing is CPU-heavy and synchronous. Running it in the
            # server event loop made the API and WebSocket unresponsive for the
            # entire block-context stage on large documents.
            package = await asyncio.to_thread(
                resolve_block_package,
                output_dir,
                block_id,
                page,
                prefer_prepared=False,
            )
        text = package.get("user_text")
        source = str(package.get("source_kind") or "error")
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
        if source != package.get("source_kind"):
            package["source_kind"] = source
            package["user_text"] = text
        if warnings:
            package["warnings"] = warnings
        coverage = "ready_image_only" if source == "image_only" else (
            "ready" if source != "missing" else "error"
        )
        counts[source] = counts.get(source, 0) + 1
        profile_id = str(package.get("profile_id") or "")
        if profile_id:
            profile_counts[profile_id] = profile_counts.get(profile_id, 0) + 1
        reference = package.get("reference") or {}
        selection_mode = str(reference.get("selection_mode") or (
            "embedded_profile_grammar" if not reference.get("block_id") else "catalog_reference"
        ))
        reference_selection_counts[selection_mode] = (
            reference_selection_counts.get(selection_mode, 0) + 1
        )
        confidence = str(reference.get("selection_confidence") or "not_scored")
        reference_confidence_counts[confidence] = (
            reference_confidence_counts.get(confidence, 0) + 1
        )
        artifact_name = artifact_filename(block_id)
        artifact_target = graph_dir / artifact_name
        artifact_temp = artifact_target.with_suffix(".json.tmp")
        artifact_temp.write_text(
            json.dumps(
                package, ensure_ascii=False, default=str,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        os.replace(artifact_temp, artifact_target)
        expected_artifacts.add(artifact_name)
        prepared.append({
            "block_id": block_id,
            "page": page,
            "source_kind": source,
            "coverage_status": coverage,
            "context_hash": _hash_text(text),
            "discipline": package.get("discipline"),
            "profile_id": package.get("profile_id"),
            "reference": package.get("reference"),
            "readiness": package.get("readiness"),
            "graph_artifact": f"{ARTIFACT_DIRNAME}/{artifact_name}",
            "warnings": warnings,
        })
        await _emit(progress_cb, {
            "type": "block_done",
            "block_id": block_id,
            "page": page,
            "source_kind": source,
            "profile_id": package.get("profile_id"),
            "ok": coverage != "error",
            "completed": pos,
            "total": len(blocks),
        })

    # Удаляем только устаревшие JSON этого собственного каталога: иначе UI может
    # показать граф блока, которого уже нет в новом crop/index.json.
    for stale in graph_dir.glob("*.json"):
        if stale.name not in expected_artifacts:
            try:
                stale.unlink()
            except OSError:
                pass

    ready = sum(item["coverage_status"] != "error" for item in prepared)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "pipeline_block": "block_vector_graph",
        "pipeline_block_title": "Векторные графы блоков",
        "status": "ok" if ready == len(prepared) else ("partial" if ready else "failed"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "project_dir": str(project_dir),
        "blocks_total": len(prepared),
        "blocks_ready": ready,
        "blocks_failed": len(prepared) - ready,
        "source_counts": counts,
        "profile_counts": profile_counts,
        "reference_catalog": catalog_runtime_info(),
        "reference_selection_counts": reference_selection_counts,
        "reference_confidence_counts": reference_confidence_counts,
        "blocks": prepared,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / BLOCK_CONTEXT_SUMMARY_FILENAME
    temp = target.with_suffix(target.suffix + ".tmp")
    temp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, target)
    await _emit(progress_cb, {"type": "completed", "summary": summary})
    return summary
