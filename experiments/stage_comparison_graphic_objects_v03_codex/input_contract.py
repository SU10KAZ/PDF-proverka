"""Resolve the real upstream ``blocks.json`` contract without inventing bboxes."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.app.services.common.blocks_json import load_blocks_json


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
GRAPHIC_TYPES = {"image", "imagine", "graphic"}
NOT_APPLICABLE_TYPES = {"text", "table", "stamp"}


class PreparedBlockError(ValueError):
    """The existing upstream artifact cannot resolve the requested block."""


def _resolve_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else REPOSITORY_ROOT / path


def _page_meta(data: dict[str, Any], page_index: int) -> dict[str, Any]:
    for row in data.get("pages") or []:
        if int(row.get("page_index", -1)) == page_index:
            return row
    raise PreparedBlockError(f"page_index {page_index} missing from blocks.json pages[]")


def _existing_graph(version_dir: Path, block_id: str) -> Path | None:
    candidates = [version_dir / "03_analysis/latest/block_vector_graphs" / f"{block_id}.json"]
    runs = version_dir / "03_analysis/runs"
    if runs.is_dir():
        candidates.extend(
            sorted(
                runs.glob(f"*/block_vector_graphs/{block_id}.json"),
                key=lambda path: path.stat().st_mtime_ns,
                reverse=True,
            )
        )
    return next((path for path in candidates if path.is_file()), None)


def _prepared_anchors(graph_path: Path | None) -> tuple[list[dict[str, Any]], str]:
    if graph_path is None:
        return [], "not_available"
    try:
        payload = json.loads(graph_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return [], "unreadable_existing_block_vector_graph"
    rows = ((payload.get("graph") or {}).get("semantic_ledger") or [])
    anchors = []
    for row in rows:
        bbox = row.get("bbox_page")
        text = str(row.get("text") or "").strip()
        if not text or not isinstance(bbox, list) or len(bbox) != 4:
            continue
        anchors.append({
            "anchor_id": str(row.get("id") or f"prepared-anchor-{len(anchors)+1}"),
            "text": text,
            "bbox_norm": [float(value) for value in bbox],
            "source": "existing_block_vector_graph.semantic_ledger",
        })
    return anchors, str(graph_path)


def resolve_prepared_block(reference: dict[str, Any]) -> dict[str, Any]:
    """Resolve ``{blocks_json, block_id}`` to the authoritative upstream row.

    ``coords_norm`` and polygon are never accepted from the benchmark manifest:
    they are loaded only from the referenced existing block artifact.
    """
    if "coords_norm" in reference or "bbox" in reference:
        raise PreparedBlockError("benchmark references must not duplicate upstream coordinates")
    blocks_path = _resolve_path(reference["blocks_json"])
    block_id = str(reference["block_id"])
    data = load_blocks_json(blocks_path)
    if data is None:
        raise PreparedBlockError(f"invalid blocks.json: {blocks_path}")
    if data.get("coordinate_space") != "normalized_page_top_left":
        raise PreparedBlockError(f"unsupported coordinate_space: {data.get('coordinate_space')!r}")
    matches = [row for row in data["blocks"] if str(row.get("block_id")) == block_id]
    if len(matches) != 1:
        raise PreparedBlockError(f"block_id {block_id} resolved {len(matches)} times in {blocks_path}")
    row = matches[0]
    page_index = int(row["page_index"])
    page = _page_meta(data, page_index)
    coords = row.get("coords_norm")
    if not isinstance(coords, list) or len(coords) != 4:
        raise PreparedBlockError(f"block {block_id} has no usable coords_norm")
    version_dir = blocks_path.parent.parent
    pdf_path = version_dir / "02_work/document.pdf"
    if not pdf_path.is_file():
        raise PreparedBlockError(f"source PDF missing: {pdf_path}")
    graph_path = _existing_graph(version_dir, block_id)
    anchors, anchor_source = _prepared_anchors(graph_path)
    block_type = str(row.get("block_type") or "").lower()
    not_applicable = block_type in NOT_APPLICABLE_TYPES
    return {
        "schema_version": "already-prepared-graphic-block-v1",
        "comparison_unit": "ALREADY_PREPARED_GRAPHIC_BLOCK",
        "block_id": block_id,
        "block_group_id": reference.get("block_group_id"),
        "blocks_json": str(blocks_path.relative_to(REPOSITORY_ROOT)),
        "source_pdf": str(pdf_path.relative_to(REPOSITORY_ROOT)),
        "source_pdf_path": str(pdf_path),
        "document_id": data.get("document_id"),
        "document_name": data.get("document_name"),
        "document_path": data.get("document_path"),
        "page_index": page_index,
        "page_label": row.get("page_label"),
        "page_artifact": {
            "width_px": int(page.get("width_px") or 0),
            "height_px": int(page.get("height_px") or 0),
            "rotation": int(page.get("rotation") or 0),
        },
        "block_type": block_type,
        "shape_type": str(row.get("shape_type") or "rectangle"),
        "coords_norm": [float(value) for value in coords],
        "polygon_points_norm": row.get("polygon_points"),
        "crop_url": row.get("crop_url"),
        "status": row.get("status"),
        "export_status": row.get("export_status"),
        "graphic_applicability": "GRAPHIC_NOT_APPLICABLE" if not_applicable else "GRAPHIC_APPLICABLE",
        "prepared_text_metadata": anchors,
        "prepared_text_metadata_source": anchor_source,
        "provenance": [
            "02_work/blocks.json:blocks[]",
            "02_work/blocks.json:pages[]",
            "02_work/document.pdf",
        ],
    }


def public_input_contract(resolved: dict[str, Any]) -> dict[str, Any]:
    """Remove the absolute convenience path from a retained artifact."""
    return {key: value for key, value in resolved.items() if key != "source_pdf_path"}


__all__ = ["PreparedBlockError", "resolve_prepared_block", "public_input_contract", "REPOSITORY_ROOT"]
