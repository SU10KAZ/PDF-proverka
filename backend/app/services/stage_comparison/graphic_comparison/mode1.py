"""Production Mode 1: physical registration followed by local visible-ink diff."""
from __future__ import annotations

import math
from typing import Any, Sequence

import cv2
import numpy as np

from .extraction import PreparedBlock, extract_ink, rasterize, text_spans
from .policy import GraphicMode1Policy
from .registration import (
    SimilarityTransform,
    register,
    transform_bbox,
    transform_polygon,
)


def _boxes_mask(
    boxes: Sequence[Sequence[float]],
    frame: Sequence[float],
    cell_pt: float,
    shape: tuple[int, int],
    pad_pt: float = 0.0,
) -> np.ndarray:
    output = np.zeros(shape, np.uint8)
    height, width = shape
    for box in boxes:
        x0 = max(0, int(math.floor((box[0] - pad_pt - frame[0]) / cell_pt)))
        y0 = max(0, int(math.floor((box[1] - pad_pt - frame[1]) / cell_pt)))
        x1 = min(width, int(math.ceil((box[2] + pad_pt - frame[0]) / cell_pt)))
        y1 = min(height, int(math.ceil((box[3] + pad_pt - frame[1]) / cell_pt)))
        if x1 > x0 and y1 > y0:
            output[y0:y1, x0:x1] = 1
    return output


def _inverse_bbox(bbox: Sequence[float], transform: SimilarityTransform) -> list[float]:
    return transform_bbox(bbox, transform.inverse())


def _region_type(region: dict[str, Any]) -> str:
    if region.get("border_status") == "UNRESOLVED":
        return "UNCERTAIN_GRAPHIC_CHANGE"
    left = int(region["left_only_cells"])
    right = int(region["right_only_cells"])
    if right == 0 or left / max(1, right) > 6:
        return "REMOVED_GRAPHIC"
    if left == 0 or right / max(1, left) > 6:
        return "ADDED_GRAPHIC"
    return "GEOMETRY_CHANGED"


def _bbox_gap(left: Sequence[float], right: Sequence[float]) -> float:
    dx = max(0.0, left[0] - right[2], right[0] - left[2])
    dy = max(0.0, left[1] - right[3], right[1] - left[3])
    return math.hypot(dx, dy)


def _merge_opposite_regions(
    regions: list[dict[str, Any]],
    transform: SimilarityTransform,
    policy: GraphicMode1Policy,
) -> list[dict[str, Any]]:
    """Join adjacent removal/addition evidence into conservative geometry.

    This does not claim movement or connectivity.  It only prevents one bent
    line from becoming two misleading object lifecycle events.
    """
    removed = [
        index for index, region in enumerate(regions)
        if region["left_only_cells"] > 6 * max(1, region["right_only_cells"])
    ]
    added = [
        index for index, region in enumerate(regions)
        if region["right_only_cells"] > 6 * max(1, region["left_only_cells"])
    ]
    consumed: set[int] = set()
    output: list[dict[str, Any]] = []
    maximum_gap = max(12.0, 4.0 * policy.merge_pt)
    for left_index in removed:
        if left_index in consumed:
            continue
        candidates = [
            right_index for right_index in added
            if right_index not in consumed
            and _bbox_gap(
                regions[left_index]["right_bbox_visual_pt"],
                regions[right_index]["right_bbox_visual_pt"],
            ) <= maximum_gap
        ]
        if not candidates:
            continue
        right_index = min(
            candidates,
            key=lambda index: (
                _bbox_gap(
                    regions[left_index]["right_bbox_visual_pt"],
                    regions[index]["right_bbox_visual_pt"],
                ),
                regions[index]["right_bbox_visual_pt"],
            ),
        )
        first, second = regions[left_index], regions[right_index]
        bbox = [
            min(first["right_bbox_visual_pt"][0], second["right_bbox_visual_pt"][0]),
            min(first["right_bbox_visual_pt"][1], second["right_bbox_visual_pt"][1]),
            max(first["right_bbox_visual_pt"][2], second["right_bbox_visual_pt"][2]),
            max(first["right_bbox_visual_pt"][3], second["right_bbox_visual_pt"][3]),
        ]
        statuses = {first["border_status"], second["border_status"]}
        border_status = (
            "UNRESOLVED" if "UNRESOLVED" in statuses
            else "REAL_BEYOND_BORDER" if "REAL_BEYOND_BORDER" in statuses
            else "INTERIOR"
        )
        merged = {
            "left_only_cells": first["left_only_cells"] + second["left_only_cells"],
            "right_only_cells": first["right_only_cells"] + second["right_only_cells"],
            "matched_context_cells": first["matched_context_cells"] + second["matched_context_cells"],
            "left_only_ink_pt": round(first["left_only_ink_pt"] + second["left_only_ink_pt"], 2),
            "right_only_ink_pt": round(first["right_only_ink_pt"] + second["right_only_ink_pt"], 2),
            "ink_pt": round(first["ink_pt"] + second["ink_pt"], 2),
            "text_overlap": max(first["text_overlap"], second["text_overlap"]),
            "right_bbox_visual_pt": [round(value, 2) for value in bbox],
            "left_bbox_visual_pt": [round(value, 2) for value in _inverse_bbox(bbox, transform)],
            "border_status": border_status,
            "classification": "GEOMETRY_CHANGED",
        }
        output.append(merged)
        consumed.update({left_index, right_index})
    output.extend(region for index, region in enumerate(regions) if index not in consumed)
    return output


def _transformed_text_spans(
    block: PreparedBlock,
    transform: SimilarityTransform,
    policy: GraphicMode1Policy,
) -> list[dict[str, Any]]:
    output = []
    for span in text_spans(block, policy):
        output.append({
            "text": span["text"],
            "bbox": transform_bbox(span["bbox"], transform),
        })
    return output


def _address_hints(
    bbox: Sequence[float],
    spans: list[dict[str, Any]],
    side: str,
    radius_pt: float,
) -> list[dict[str, Any]]:
    center_x = (bbox[0] + bbox[2]) / 2.0
    center_y = (bbox[1] + bbox[3]) / 2.0
    candidates = []
    for span in spans:
        box = span["bbox"]
        nearest_x = min(max(center_x, box[0]), box[2])
        nearest_y = min(max(center_y, box[1]), box[3])
        distance = math.hypot(center_x - nearest_x, center_y - nearest_y)
        if distance <= radius_pt:
            candidates.append((distance, str(span["text"])))
    candidates.sort(key=lambda item: (item[0], item[1]))
    return [
        {
            "kind": "PREPARED_TEXT_LABEL",
            "side": side,
            "text": text[:160],
            "distance_pt": round(distance, 2),
            "evidence_role": "ADDRESS_ONLY",
        }
        for distance, text in candidates[:5]
    ]


def _probe_block(source: PreparedBlock, bbox: Sequence[float], suffix: str) -> PreparedBlock:
    return PreparedBlock(
        pdf_path=source.pdf_path,
        pdf_hash=source.pdf_hash,
        page_index=source.page_index,
        block_id=f"{source.block_id}:{suffix}",
        block_type=source.block_type,
        bbox_visual_pt=tuple(map(float, bbox)),
        polygon_visual_pt=None,
        label=source.label,
        source=source.source,
    )


def _resolve_border_regions(
    regions: list[dict[str, Any]],
    left_block: PreparedBlock,
    right_block: PreparedBlock,
    transform: SimilarityTransform,
    policy: GraphicMode1Policy,
) -> None:
    radius = max(1, int(round(policy.tolerance_pt / policy.cell_pt)))
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * radius + 1, 2 * radius + 1))
    inverse = transform.inverse()
    for region in regions:
        if region.get("border_status") != "TOUCHES_COMMON_BORDER":
            continue
        bbox = region["right_bbox_visual_pt"]
        pad = policy.border_probe_pt
        right_probe_bbox = [bbox[0] - pad, bbox[1] - pad, bbox[2] + pad, bbox[3] + pad]
        left_probe_bbox = _inverse_bbox(right_probe_bbox, transform)
        try:
            left_probe = extract_ink(_probe_block(left_block, left_probe_bbox, "border_probe"), policy)
            right_probe = extract_ink(_probe_block(right_block, right_probe_bbox, "border_probe"), policy)
            left_segments = transform.apply_segments(left_probe["segments"])
            left_fills = transform.apply_fill_groups(left_probe["fills"])
            left_mask = rasterize(
                left_segments, None, right_probe_bbox, policy.cell_pt, fills=left_fills,
            )
            right_mask = rasterize(
                right_probe["segments"], None, right_probe_bbox, policy.cell_pt,
                fills=right_probe["fills"],
            )
            left_only = int((left_mask & ~cv2.dilate(right_mask, kernel)).sum())
            right_only = int((right_mask & ~cv2.dilate(left_mask, kernel)).sum())
            before = int(region["left_only_cells"] + region["right_only_cells"])
            after = left_only + right_only
            region["border_probe"] = {
                "unmatched_cells_in_block": before,
                "unmatched_cells_on_source_page": after,
                "right_probe_bbox_visual_pt": [round(value, 2) for value in right_probe_bbox],
                "lookup_only": True,
                "upstream_bbox_changed": False,
            }
            region["border_status"] = "CROP_ARTIFACT" if after <= 0.3 * max(1, before) else "REAL_BEYOND_BORDER"
        except (OSError, ValueError, RuntimeError, cv2.error):
            region["border_status"] = "UNRESOLVED"


def run_mode1(
    left_block: PreparedBlock,
    right_block: PreparedBlock,
    left_ink: dict[str, Any],
    right_ink: dict[str, Any],
    policy: GraphicMode1Policy,
) -> dict[str, Any]:
    """Return compact Mode 1 result; routing is decided by the outer router."""
    registration, transform, left_segments, right_segments = register(left_ink, right_ink, policy)
    frame = registration["frame_visual_pt"]
    transformed_left_segments = transform.apply_segments(left_segments)
    transformed_left_fills = transform.apply_fill_groups(left_ink["fills"])
    left_mask = rasterize(
        transformed_left_segments, None, frame, policy.cell_pt,
        fills=transformed_left_fills,
        clip_polygon=transform_polygon(left_block.polygon_visual_pt, transform),
    )
    right_mask = rasterize(
        right_segments, None, frame, policy.cell_pt,
        fills=right_ink["fills"], clip_polygon=right_block.polygon_visual_pt,
    )
    radius = max(1, int(round(policy.tolerance_pt / policy.cell_pt)))
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * radius + 1, 2 * radius + 1))
    left_dilated = cv2.dilate(left_mask, kernel)
    right_dilated = cv2.dilate(right_mask, kernel)
    left_only = (left_mask & ~right_dilated).astype(np.uint8)
    right_only = (right_mask & ~left_dilated).astype(np.uint8)
    matched = (left_mask & right_dilated).astype(np.uint8)

    left_texts = _transformed_text_spans(left_block, transform, policy)
    right_texts = text_spans(right_block, policy)
    text_mask = _boxes_mask(
        [span["bbox"] for span in left_texts], frame, policy.cell_pt,
        left_mask.shape, pad_pt=1.0,
    )
    text_mask |= _boxes_mask(
        [span["bbox"] for span in right_texts], frame, policy.cell_pt,
        left_mask.shape, pad_pt=1.0,
    )

    change_union = ((left_only | right_only) > 0).astype(np.uint8)
    merge_radius = max(1, int(round(policy.merge_pt / policy.cell_pt)))
    merge_kernel = cv2.getStructuringElement(
        cv2.MORPH_ELLIPSE,
        (2 * merge_radius + 1, 2 * merge_radius + 1),
    )
    grown = cv2.dilate(change_union, merge_kernel)
    component_count, labels, component_stats, _centroids = cv2.connectedComponentsWithStats(grown, connectivity=8)
    regions = []
    for component in range(1, component_count):
        selection = labels == component
        left_cells = int(left_only[selection].sum())
        right_cells = int(right_only[selection].sum())
        if left_cells + right_cells == 0:
            continue
        text_cells = int((text_mask[selection] & change_union[selection]).sum())
        x, y, width, height, _area = component_stats[component]
        right_bbox = [
            frame[0] + x * policy.cell_pt,
            frame[1] + y * policy.cell_pt,
            frame[0] + (x + width) * policy.cell_pt,
            frame[1] + (y + height) * policy.cell_pt,
        ]
        regions.append({
            "left_only_cells": left_cells,
            "right_only_cells": right_cells,
            "matched_context_cells": int(matched[selection].sum()),
            "left_only_ink_pt": round(left_cells * policy.cell_pt, 2),
            "right_only_ink_pt": round(right_cells * policy.cell_pt, 2),
            "ink_pt": round((left_cells + right_cells) * policy.cell_pt, 2),
            "text_overlap": round(text_cells / max(1, left_cells + right_cells), 4),
            "right_bbox_visual_pt": [round(value, 2) for value in right_bbox],
            "left_bbox_visual_pt": [round(value, 2) for value in _inverse_bbox(right_bbox, transform)],
            "border_status": "INTERIOR",
        })

    published: list[dict[str, Any]] = []
    filtered: list[dict[str, Any]] = []
    for region in regions:
        if region["ink_pt"] < policy.min_region_ink_pt:
            region["filtered_by"] = "BELOW_MIN_INK"
            filtered.append(region)
        elif region["text_overlap"] >= policy.text_overlap_drop:
            region["filtered_by"] = "TEXT_REGION"
            filtered.append(region)
        else:
            published.append(region)

    left_frame_bbox = transform_bbox(left_block.bbox_visual_pt, transform)
    right_frame_bbox = list(right_block.bbox_visual_pt)
    common = [
        max(left_frame_bbox[0], right_frame_bbox[0]),
        max(left_frame_bbox[1], right_frame_bbox[1]),
        min(left_frame_bbox[2], right_frame_bbox[2]),
        min(left_frame_bbox[3], right_frame_bbox[3]),
    ]
    border_candidates = []
    for region in published:
        bbox = region["right_bbox_visual_pt"]
        outside = (
            common[2] <= common[0] or common[3] <= common[1]
            or bbox[2] <= common[0] or bbox[0] >= common[2]
            or bbox[3] <= common[1] or bbox[1] >= common[3]
        )
        if outside:
            region["border_status"] = "OUTSIDE_COMMON_AREA"
            region["filtered_by"] = "OUTSIDE_COMMON_AREA"
            filtered.append(region)
            continue
        if (
            bbox[0] <= common[0] + policy.border_pt
            or bbox[1] <= common[1] + policy.border_pt
            or bbox[2] >= common[2] - policy.border_pt
            or bbox[3] >= common[3] - policy.border_pt
        ):
            region["border_status"] = "TOUCHES_COMMON_BORDER"
        border_candidates.append(region)
    published = border_candidates
    _resolve_border_regions(published, left_block, right_block, transform, policy)
    retained = []
    for region in published:
        if region["border_status"] == "CROP_ARTIFACT":
            region["filtered_by"] = "CROP_ARTIFACT"
            filtered.append(region)
        else:
            retained.append(region)
    published = retained

    published = _merge_opposite_regions(published, transform, policy)
    address_radius = max(
        12.0,
        0.02 * math.hypot(frame[2] - frame[0], frame[3] - frame[1]),
    )
    for region in published:
        region.setdefault("classification", _region_type(region))
        if region["right_only_cells"] >= region["left_only_cells"]:
            region["address_hints"] = _address_hints(
                region["right_bbox_visual_pt"], right_texts, "right", address_radius,
            )
        else:
            region["address_hints"] = _address_hints(
                region["right_bbox_visual_pt"], left_texts, "left", address_radius,
            )

    total_cells = int(left_mask.sum()) + int(right_mask.sum())
    changed_cells = int(left_only.sum()) + int(right_only.sum())
    published_cells = sum(region["left_only_cells"] + region["right_only_cells"] for region in published)
    diff = {
        "left_ink_cells": int(left_mask.sum()),
        "right_ink_cells": int(right_mask.sum()),
        "matched_cells": int(matched.sum()),
        "left_only_cells": int(left_only.sum()),
        "right_only_cells": int(right_only.sum()),
        "changed_ink_fraction": round(changed_cells / max(1, total_cells), 5),
        "published_ink_fraction": round(published_cells / max(1, total_cells), 5),
        "n_regions_raw": len(regions),
        "n_regions_published": len(published),
        "n_regions_filtered": len(filtered),
        "left_text_spans": len(left_texts),
        "right_text_spans": len(right_texts),
    }
    return {
        "registration": registration,
        "diff": diff,
        "regions": sorted(
            published,
            key=lambda region: (-region["ink_pt"], region["right_bbox_visual_pt"]),
        ),
        "filtered_regions": sorted(
            filtered,
            key=lambda region: (-region["ink_pt"], region["right_bbox_visual_pt"]),
        )[:policy.max_diagnostic_filtered_regions],
        "debug_view": {
            "frame_visual_pt": [round(value, 3) for value in frame],
            "cell_pt": policy.cell_pt,
            "common_area_visual_pt": [round(value, 3) for value in common],
            "mask_summary": {
                "matched_cells": int(matched.sum()),
                "left_only_cells": int(left_only.sum()),
                "right_only_cells": int(right_only.sum()),
            },
        },
    }


__all__ = ["run_mode1"]
