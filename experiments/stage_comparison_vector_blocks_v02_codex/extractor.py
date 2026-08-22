"""Cached v0.2 vector block extractor, isolated from production code."""
from __future__ import annotations

import collections
import math
import re
import unicodedata
from pathlib import Path
from typing import Any, Sequence

from experiments.stage_comparison_vector_blocks import extractor as baseline

from .page_cache import DEFAULT_EXTRACTOR_VERSION, PageCache


SCHEMA_VERSION = "vector-block-research-v0.2-codex"
DEFAULT_SEGMENT_STORAGE_CAP = 20_000
DEFAULT_TOPOLOGY_CAP = 8_000
DEFAULT_PATTERNS_CAP = 100
DEFAULT_TEXT_CAP = 5_000


def _span_intersects(bbox: Sequence[float], rect: Sequence[float]) -> bool:
    return baseline._rect_intersects(bbox, rect)


def assess_text_quality(items: Sequence[dict[str, Any]], *, uncapped_count: int | None = None) -> dict[str, Any]:
    value = "".join(str(item.get("text") or "") for item in items)
    total = len(value)
    printable = sum(char.isprintable() or char.isspace() for char in value)
    controls = sum(unicodedata.category(char) == "Cc" and not char.isspace() for char in value)
    replacements = value.count("\ufffd")
    private_use = sum("\ue000" <= char <= "\uf8ff" for char in value)
    cid_markers = len(re.findall(r"\(cid:\d+\)|cid\+\d+", value, re.IGNORECASE))
    odd_symbols = sum(
        unicodedata.category(char) in {"Co", "Cs", "Cn"} or (ord(char) < 32 and not char.isspace())
        for char in value
    )
    consistent = sum(
        bool(item.get("font"))
        and float(item.get("font_size") or 0) > 0
        and len(item.get("bbox") or []) == 4
        for item in items
    )
    printable_ratio = printable / max(total, 1)
    corruption_ratio = (controls + replacements + private_use + odd_symbols) / max(total, 1)
    span_consistency = consistent / max(len(items), 1)
    suspicious_fonts = sum(
        any(marker in str(item.get("font") or "").lower() for marker in ("identity", "unknown", "unnamed"))
        for item in items
    )
    suspicious_mapping_ratio = (cid_markers + suspicious_fonts) / max(len(items), 1)
    if not items:
        status = "TEXT_PARTIAL"
    elif (
        printable_ratio < 0.85
        or corruption_ratio >= 0.03
        or replacements >= 3
        or private_use / max(total, 1) >= 0.03
        or cid_markers >= 3
        or span_consistency < 0.5
    ):
        status = "TEXT_BROKEN"
    elif (
        printable_ratio < 0.98
        or corruption_ratio > 0
        or suspicious_mapping_ratio >= 0.1
        or span_consistency < 0.9
        or (uncapped_count is not None and uncapped_count > len(items))
    ):
        status = "TEXT_PARTIAL"
    else:
        status = "TEXT_GOOD"
    return {
        "status": status,
        "characters": total,
        "spans": len(items),
        "spans_uncapped": len(items) if uncapped_count is None else uncapped_count,
        "printable_ratio": round(printable_ratio, 6),
        "control_chars": controls,
        "replacement_chars": replacements,
        "private_use_chars": private_use,
        "glyph_corruption_ratio": round(corruption_ratio, 6),
        "suspicious_font_mapping_ratio": round(suspicious_mapping_ratio, 6),
        "span_consistency": round(span_consistency, 6),
    }


def _extract_primitives(
    payload: dict[str, Any],
    block_rect: Sequence[float],
    storage_cap: int,
    polygon_abs: Sequence[Sequence[float]] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    drawings = payload["drawings"]
    candidates = [
        drawing
        for drawing in drawings
        if drawing.get("rect") is not None
        and baseline._rect_intersects(
            [drawing["rect"].x0, drawing["rect"].y0, drawing["rect"].x1, drawing["rect"].y1],
            block_rect,
        )
    ]
    primitives: list[dict[str, Any]] = []
    source_counts: collections.Counter[str] = collections.Counter()
    for drawing_index, drawing in enumerate(candidates):
        source_counts.update(item[0] for item in drawing.get("items") or [])
        primitives.extend(
            baseline._drawing_primitives(
                drawing, drawing_index, block_rect, polygon_abs=polygon_abs
            )
        )
    # Baseline used ``filled_polygon`` as a mixed geometry/style label.  V02
    # deliberately keeps fill in ``style`` so fill add/remove cannot masquerade
    # as a geometry change.
    for primitive in primitives:
        if primitive["type"] == "filled_polygon":
            primitive["type"] = "rectangle" if primitive["source_kinds"] == ["re"] else "polygon"
    uncapped = len(primitives)
    if uncapped > storage_cap:
        primitives = sorted(
            primitives,
            key=lambda item: (
                item["type"] not in {"line", "polyline"},
                item["closed"],
                item["length_norm"],
            ),
            reverse=True,
        )[:storage_cap]
    for index, primitive in enumerate(primitives, 1):
        primitive["id"] = f"primitive-{index}"
    return primitives, {
        "page_drawings_total": len(drawings),
        "drawings_intersecting_block": len(candidates),
        "source_item_counts": dict(sorted(source_counts.items())),
        "primitives_uncapped": uncapped,
        "storage_cap": storage_cap,
        "storage_capped": uncapped > storage_cap,
    }


def _extract_text(
    payload: dict[str, Any],
    block_rect: Sequence[float],
    polygon_abs: Sequence[Sequence[float]] | None,
    text_cap: int,
) -> tuple[list[dict[str, Any]], int]:
    result: list[dict[str, Any]] = []
    for block in payload["text_dict"].get("blocks") or []:
        if block.get("type") != 0:
            continue
        for line in block.get("lines") or []:
            direction = line.get("dir") or (1.0, 0.0)
            rotation = math.degrees(math.atan2(float(direction[1]), float(direction[0])))
            for span in line.get("spans") or []:
                bbox = [float(value) for value in span.get("bbox") or []]
                if len(bbox) != 4 or not _span_intersects(bbox, block_rect):
                    continue
                text = re.sub(r"\s+", " ", str(span.get("text") or "")).strip()
                if not text:
                    continue
                center = ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2)
                if not baseline._point_in_polygon(center, polygon_abs):
                    continue
                normalized_bbox = baseline._norm_bbox(bbox, block_rect)
                category = "engineering_value" if baseline._ENGINEERING_RE.search(text) else (
                    "numeric" if baseline._VALUE_RE.fullmatch(text) else "label"
                )
                result.append(
                    {
                        "id": f"text-{len(result) + 1}",
                        "text": text,
                        "bbox": [baseline._round(value) for value in bbox],
                        "bbox_norm": normalized_bbox,
                        "x_norm": baseline._round((normalized_bbox[0] + normalized_bbox[2]) / 2),
                        "y_norm": baseline._round((normalized_bbox[1] + normalized_bbox[3]) / 2),
                        "rotation": baseline._round(rotation, 3),
                        "font_size": baseline._round(span.get("size") or 0.0, 3),
                        "font": span.get("font") or "",
                        "category": category,
                    }
                )
    uncapped = len(result)
    return result[:text_cap], uncapped


def _patterns(primitives: Sequence[dict[str, Any]], cap: int) -> tuple[list[dict[str, Any]], int]:
    groups: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for primitive in primitives:
        if primitive["type"] == "line" or primitive["length_norm"] < 0.002:
            continue
        groups[baseline._primitive_pattern(primitive)].append(primitive)
    rows = []
    for signature, members in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0])):
        if len(members) < 2:
            continue
        rows.append(
            {
                "pattern_id": f"pattern_{signature}",
                "count": len(members),
                "primitive_type": members[0]["type"],
                "segment_count": members[0]["segment_count"],
                "instances": [item["normalized"]["bbox"] for item in members[:100]],
                "instances_truncated": len(members) > 100,
            }
        )
    return rows[:cap], len(rows)


def _content_extent(
    primitives: Sequence[dict[str, Any]], texts: Sequence[dict[str, Any]]
) -> dict[str, Any]:
    bboxes = [item["normalized"]["bbox"] for item in primitives]
    bboxes.extend(item["bbox_norm"] for item in texts)
    if not bboxes:
        extent = [0.0, 0.0, 0.0, 0.0]
    else:
        extent = [
            min(box[0] for box in bboxes),
            min(box[1] for box in bboxes),
            max(box[2] for box in bboxes),
            max(box[3] for box in bboxes),
        ]
    edge_threshold = 0.02
    edge_counts = {
        "left": sum(box[0] <= edge_threshold for box in bboxes),
        "top": sum(box[1] <= edge_threshold for box in bboxes),
        "right": sum(box[2] >= 1 - edge_threshold for box in bboxes),
        "bottom": sum(box[3] >= 1 - edge_threshold for box in bboxes),
    }
    return {
        "bbox_norm": [round(value, 6) for value in extent],
        "width": round(max(0.0, extent[2] - extent[0]), 6),
        "height": round(max(0.0, extent[3] - extent[1]), 6),
        "edge_anchor_counts": edge_counts,
        "items": len(bboxes),
    }


def extract_block_from_payload(
    payload: dict[str, Any],
    *,
    bbox_norm: Sequence[float],
    block_id: str,
    polygon_norm: Sequence[Sequence[float]] | None = None,
    topology_tolerance: float = 0.0025,
    segment_storage_cap: int = DEFAULT_SEGMENT_STORAGE_CAP,
    topology_cap: int = DEFAULT_TOPOLOGY_CAP,
    patterns_cap: int = DEFAULT_PATTERNS_CAP,
    text_cap: int = DEFAULT_TEXT_CAP,
) -> dict[str, Any]:
    if len(bbox_norm) != 4 or bbox_norm[2] <= bbox_norm[0] or bbox_norm[3] <= bbox_norm[1]:
        raise ValueError("bbox_norm must be [x0, y0, x1, y1] with positive area")
    width, height = payload["page_width"], payload["page_height"]
    block_rect = [bbox_norm[0] * width, bbox_norm[1] * height, bbox_norm[2] * width, bbox_norm[3] * height]
    polygon_abs = None if not polygon_norm else [[point[0] * width, point[1] * height] for point in polygon_norm]
    primitives, extraction = _extract_primitives(payload, block_rect, segment_storage_cap, polygon_abs)
    texts, texts_uncapped = _extract_text(payload, block_rect, polygon_abs, text_cap)
    topology = baseline._topology(primitives, topology_tolerance, topology_cap)
    anchors = baseline._anchors(texts, primitives)
    repeated, patterns_uncapped = _patterns(primitives, patterns_cap)
    hatch_like = baseline._hatch_like_structures(primitives)
    cap_flags = {
        "segments_capped": bool(extraction["storage_capped"]),
        "topology_capped": bool(topology["segments_capped"]),
        "patterns_capped": patterns_uncapped > len(repeated),
        "text_capped": texts_uncapped > len(texts),
    }
    text_quality = assess_text_quality(texts, uncapped_count=texts_uncapped)
    dimensions = [
        {
            "text_id": item["id"], "text": item["text"], "bbox_norm": item["bbox_norm"],
            "geometry_id": next((a["geometry_id"] for a in anchors if a["text_id"] == item["id"]), None),
            "classification": "dimension_or_engineering_value_candidate",
        }
        for item in texts if item["category"] == "engineering_value"
    ]
    labels = [{"text_id": item["id"], "text": item["text"], "bbox_norm": item["bbox_norm"]} for item in texts if item["category"] == "label"]
    if not primitives or topology["segments_total"] < 3:
        quality = "VECTOR_DATA_INSUFFICIENT"
    elif any(cap_flags.values()) or text_quality["status"] != "TEXT_GOOD":
        quality = "LIMITED"
    else:
        quality = "GOOD"
    description: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "extractor_version": payload["extractor_version"],
        "research_only": True,
        "block_id": block_id,
        "page": payload["page_index"] + 1,
        "page_index": payload["page_index"],
        "bbox": [baseline._round(value) for value in block_rect],
        "bbox_norm_on_page": [baseline._round(value) for value in bbox_norm],
        "polygon_norm_on_page": polygon_norm,
        "source": {
            "pdf": payload["pdf"], "pdf_sha256": payload["pdf_sha256"],
            "page_width": baseline._round(width), "page_height": baseline._round(height),
            "source_layers": ["cached PyMuPDF page.get_drawings", "cached PyMuPDF full-page text spans"],
            "excluded_sources": ["OCR", "Vision", "raster recognition", "embeddings"],
            "page_cache": {
                "access": payload.get("cache_access"),
                "cache_path": payload.get("cache_path"),
                "payload_build_seconds": round(payload.get("build_seconds", 0.0), 6),
            },
        },
        "vector_quality": quality,
        "text_quality": text_quality,
        "cap_flags": cap_flags,
        "quality_notes": [],
        "coordinate_system": {
            "raw": "PDF points in page coordinates",
            "normalized": "block-relative [0,1] x [0,1]",
            "normalization_does_not_use": ["affine warp", "free deformation", "pair alignment"],
        },
        "geometry": {"extraction": extraction, "primitives": primitives},
        "primitive_summary": {}, "texts": texts, "anchors": anchors, "topology": topology,
        "repeated_elements": repeated, "patterns_uncapped": patterns_uncapped,
        "hatch_like_structures": hatch_like, "dimensions": dimensions, "labels": labels,
        "content_extent": _content_extent(primitives, texts),
        "structural_signature": {}, "size_metrics": {},
        "ambiguities": [
            "X-crossings remain unconnected without junction evidence.",
            "Text anchors and repeated motifs are geometric candidates, not semantic assertions.",
            "No affine warp or pair alignment is used.",
        ],
    }
    description["quality_notes"] = [
        note for condition, note in (
            (cap_flags["segments_capped"], "Segment storage cap reached."),
            (cap_flags["topology_capped"], "Topology cap reached."),
            (cap_flags["patterns_capped"], "Pattern group cap reached."),
            (cap_flags["text_capped"], "Text span cap reached."),
            (text_quality["status"] == "TEXT_BROKEN", "Vector text is broken and cannot drive semantic verdicts."),
            (quality == "VECTOR_DATA_INSUFFICIENT", "Useful vector geometry is absent or insufficient."),
        ) if condition
    ]
    description["primitive_summary"] = baseline._summary(primitives, texts, topology)
    description["structural_signature"] = baseline._signatures(primitives, texts, topology)
    description["size_metrics"] = baseline._size_metrics(description)
    return description


def extract_block(
    pdf_path: str | Path,
    *,
    page_index: int,
    bbox_norm: Sequence[float],
    block_id: str,
    page_cache: PageCache,
    polygon_norm: Sequence[Sequence[float]] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    payload = page_cache.get(pdf_path, page_index)
    return extract_block_from_payload(
        payload, bbox_norm=bbox_norm, polygon_norm=polygon_norm, block_id=block_id, **kwargs
    )


__all__ = [
    "DEFAULT_EXTRACTOR_VERSION", "PageCache", "assess_text_quality", "extract_block",
    "extract_block_from_payload",
]
