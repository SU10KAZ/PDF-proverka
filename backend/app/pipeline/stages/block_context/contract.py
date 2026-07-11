"""Canonical block-context artifact and legacy Gemma-summary adapter."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.app.services.storage.stage_artifacts import (
    BLOCK_CONTEXT_SUMMARY_FILENAME,
    resolve_existing,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BLOCKS_DIRNAME,
    STAGE02_BLOCKS_DIRNAME,
)

SCHEMA_VERSION = 1
STAGE = "block_context"
SOURCE_KINDS = {
    "structured_singleline",
    "structured_alia_scheme",
    "raw_vector",
    "image_only",
    "missing",
    "no_sources",
    "block_not_found",
    "error",
    "legacy_enrichment",
}


def resolve_blocks_dir(output_dir: Path) -> Path:
    """Prefer the canonical Stage 01 PNG directory, with read-only fallbacks."""
    output_dir = Path(output_dir)
    for name in (STAGE02_BLOCKS_DIRNAME, GEMMA_BLOCKS_DIRNAME, "blocks"):
        candidate = output_dir / name
        if (candidate / "index.json").is_file():
            return candidate
    return output_dir / STAGE02_BLOCKS_DIRNAME


def resolve_blocks_index(output_dir: Path) -> Path:
    return resolve_blocks_dir(output_dir) / "index.json"


def summary_path(output_dir: Path) -> Path:
    return Path(output_dir) / BLOCK_CONTEXT_SUMMARY_FILENAME


def _legacy_source(block: dict[str, Any]) -> str:
    response_source = str(block.get("base_response_source") or "")
    if response_source == "vector_skip":
        return "raw_vector"
    if response_source == "stage_disabled_skip":
        return "image_only"
    final_profile = str(block.get("final_profile") or "")
    if final_profile and final_profile != "none":
        return "legacy_enrichment"
    return "missing"


def adapt_legacy_summary(payload: dict[str, Any]) -> dict[str, Any]:
    blocks = []
    counts: dict[str, int] = {}
    for item in payload.get("blocks") or []:
        if not isinstance(item, dict) or not item.get("block_id"):
            continue
        source = _legacy_source(item)
        counts[source] = counts.get(source, 0) + 1
        blocks.append({
            "block_id": str(item["block_id"]),
            "page": item.get("page"),
            "source_kind": source,
            "coverage_status": "ready" if source != "missing" else "error",
            "context_hash": None,
            "warnings": list(item.get("warnings") or []),
            "legacy": True,
        })
    total = int(payload.get("blocks_total") or len(blocks))
    failed = int(payload.get("blocks_failed") or sum(b["coverage_status"] != "ready" for b in blocks))
    ready = int(payload.get("blocks_ok") or max(0, total - failed))
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "status": "ok" if blocks else str(payload.get("status") or "no_blocks"),
        "blocks_total": total,
        "blocks_ready": ready,
        "blocks_failed": failed,
        "source_counts": counts,
        "blocks": blocks,
        "legacy_source": "gemma_enrichment_summary.json",
    }


def load_block_context_summary(output_dir: Path) -> dict[str, Any]:
    path = resolve_existing(output_dir, BLOCK_CONTEXT_SUMMARY_FILENAME)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if path.name != BLOCK_CONTEXT_SUMMARY_FILENAME:
        return adapt_legacy_summary(payload)
    return payload if isinstance(payload, dict) else {}


def validate_block_context_summary(output_dir: Path, *, canonical_only: bool = False) -> dict[str, Any]:
    path = summary_path(output_dir) if canonical_only else resolve_existing(
        output_dir, BLOCK_CONTEXT_SUMMARY_FILENAME
    )
    if not path.is_file():
        return {"valid": False, "reason": f"{path.name} отсутствует"}
    summary = load_block_context_summary(output_dir)
    if not summary:
        return {"valid": False, "reason": "summary не читается"}
    if path.name != BLOCK_CONTEXT_SUMMARY_FILENAME and not summary.get("blocks"):
        return {"valid": False, "reason": "legacy summary не содержит block entries"}
    if summary.get("schema_version") != SCHEMA_VERSION or summary.get("stage") != STAGE:
        return {"valid": False, "reason": "schema/stage mismatch"}
    blocks = summary.get("blocks")
    if not isinstance(blocks, list):
        return {"valid": False, "reason": "blocks должен быть списком"}
    for block in blocks:
        if not isinstance(block, dict) or not block.get("block_id"):
            return {"valid": False, "reason": "block entry invalid"}
        if block.get("source_kind") not in SOURCE_KINDS:
            return {"valid": False, "reason": "unknown source_kind"}
    return {"valid": True, "path": str(path), "summary": summary}
