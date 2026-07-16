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

SCHEMA_VERSION = 2
STAGE = "block_context"
STAGE_TITLE = "Векторные графы блоков"
SOURCE_KINDS = {
    "structured_singleline",
    "structured_electrical",
    "structured_general_plan",
    "structured_architecture",
    "structured_structure",
    "structured_technology",
    "structured_hvac",
    "structured_water",
    "structured_alia_scheme",
    "raw_vector",
    "image_only",
    "missing",
    "no_sources",
    "block_not_found",
    "error",
    "legacy_enrichment",
}

# Эти источники не содержат встроенного векторного текста PDF. В частности,
# legacy_enrichment — старое OCR/vision-описание PNG, а не векторный слой.
NO_VECTOR_TEXT_SOURCE_KINDS = {
    "image_only",
    "gemma_fallback",
    "legacy_enrichment",
    "missing",
    "no_sources",
    "block_not_found",
    "error",
}
VECTOR_GRAPH_MISSING_MESSAGE = "Векторный граф блока отсутствует"


def source_has_vector_text(source_kind: Any) -> bool:
    """Есть ли у источника блока пригодный векторный текст PDF."""
    return str(source_kind or "error") not in NO_VECTOR_TEXT_SOURCE_KINDS


def block_context_sources(summary: Any) -> dict[str, str]:
    """Источник графа по block_id из результата стадии block_context."""
    if not isinstance(summary, dict):
        return {}
    return {
        str(item.get("block_id")): str(item.get("source_kind") or "")
        for item in summary.get("blocks") or []
        if isinstance(item, dict) and item.get("block_id") and item.get("source_kind")
    }


def decorate_blocks_vector_state(blocks: Any, summary: Any) -> None:
    """Добавить UI-признак наличия векторного текста, не меняя JSON на диске."""
    sources = block_context_sources(summary)
    for block in blocks or []:
        if not isinstance(block, dict):
            continue
        source = sources.get(str(block.get("block_id") or ""))
        if not source:
            continue
        available = source_has_vector_text(source)
        block["vector_text_available"] = available
        block["vector_graph_source_kind"] = source
        block["vector_graph_message"] = (
            None if available else VECTOR_GRAPH_MISSING_MESSAGE
        )


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
        "pipeline_block": "block_vector_graph",
        "pipeline_block_title": STAGE_TITLE,
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
    if path.name == BLOCK_CONTEXT_SUMMARY_FILENAME:
        catalog = summary.get("reference_catalog")
        if not isinstance(catalog, dict) or catalog.get("runtime_source") != "pipeline_stage_embedded_catalog":
            return {"valid": False, "reason": "встроенный каталог эталонов не указан"}
        if int(catalog.get("records_total") or 0) <= 0:
            return {"valid": False, "reason": "встроенный каталог эталонов пуст"}
    blocks = summary.get("blocks")
    if not isinstance(blocks, list):
        return {"valid": False, "reason": "blocks должен быть списком"}
    for block in blocks:
        if not isinstance(block, dict) or not block.get("block_id"):
            return {"valid": False, "reason": "block entry invalid"}
        if block.get("source_kind") not in SOURCE_KINDS:
            return {"valid": False, "reason": "unknown source_kind"}
    return {"valid": True, "path": str(path), "summary": summary}
