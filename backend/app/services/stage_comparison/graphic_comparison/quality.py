"""Independent precision and completeness checks for vector extraction."""
from __future__ import annotations

import math
from typing import Any, Sequence

import cv2
import numpy as np

from .extraction import (
    PreparedBlock,
    extract_ink,
    image_coverage,
    image_rects,
    rasterize,
    render_gray,
    text_spans,
)
from .policy import GraphicMode1Policy


def _boxes_mask(
    boxes: Sequence[Sequence[float]], frame: Sequence[float], cell_pt: float,
    shape: tuple[int, int], pad_pt: float = 0.0,
) -> np.ndarray:
    height, width = shape
    output = np.zeros(shape, np.uint8)
    for box in boxes:
        x0 = max(0, int(math.floor((box[0] - pad_pt - frame[0]) / cell_pt)))
        y0 = max(0, int(math.floor((box[1] - pad_pt - frame[1]) / cell_pt)))
        x1 = min(width, int(math.ceil((box[2] + pad_pt - frame[0]) / cell_pt)))
        y1 = min(height, int(math.ceil((box[3] + pad_pt - frame[1]) / cell_pt)))
        if x1 > x0 and y1 > y0:
            output[y0:y1, x0:x1] = 1
    return output


def extraction_quality(
    block: PreparedBlock,
    ink: dict[str, Any],
    policy: GraphicMode1Policy,
) -> dict[str, Any]:
    """Compare extracted vector ink against the rendered source block.

    Text and raster image areas are excluded because neither belongs to the
    vector graphic channel.  Precision and recall are deliberately reported
    separately: a precise but incomplete extractor is not eligible for Mode 1.
    """
    cell_pt = policy.quality_cell_pt
    frame = block.bbox_visual_pt
    gray = render_gray(block, policy, cell_pt)
    visible = (gray < policy.render_dark_threshold).astype(np.uint8)
    predicted = rasterize(
        ink["segments"], ink["widths"], frame, cell_pt,
        min_width_pt=0.35, fills=ink["fills"], clip_polygon=block.polygon_visual_pt,
    )
    height = min(visible.shape[0], predicted.shape[0])
    width = min(visible.shape[1], predicted.shape[1])
    visible = visible[:height, :width]
    predicted = predicted[:height, :width]
    shape = (height, width)

    texts = text_spans(block, policy)
    images = image_rects(block, policy)
    text_mask = _boxes_mask([item["bbox"] for item in texts], frame, cell_pt, shape, pad_pt=0.8)
    image_mask = _boxes_mask(images, frame, cell_pt, shape)
    interior = np.ones(shape, np.uint8)
    edge = max(1, int(math.ceil(1.0 / cell_pt)))
    if height > 2 * edge and width > 2 * edge:
        interior[:] = 0
        interior[edge:height - edge, edge:width - edge] = 1
    excluded = (text_mask | image_mask | (interior == 0)).astype(bool)
    visible_graphics = (visible & ~excluded).astype(np.uint8)
    predicted_graphics = (predicted & ~excluded).astype(np.uint8)

    # ``round(1.5)`` is banker's rounding (and the binary value can be just
    # below 1.5), which made the same rotated source fail recall depending on
    # stroke antialiasing direction.  Completeness uses the full configured
    # physical tolerance, therefore the raster radius rounds upward.
    tolerance_cells = max(1, int(math.ceil(policy.tolerance_pt / cell_pt)))
    kernel = cv2.getStructuringElement(
        cv2.MORPH_ELLIPSE,
        (2 * tolerance_cells + 1, 2 * tolerance_cells + 1),
    )
    visible_dilated = cv2.dilate(visible_graphics, kernel)
    predicted_dilated = cv2.dilate(predicted_graphics, kernel)
    visible_cells = int(visible_graphics.sum())
    predicted_cells = int(predicted_graphics.sum())
    precision_hits = int((predicted_graphics & visible_dilated).sum())
    recall_hits = int((visible_graphics & predicted_dilated).sum())
    precision = precision_hits / predicted_cells if predicted_cells else None
    recall = recall_hits / visible_cells if visible_cells else None
    missed = (visible_graphics & ~predicted_dilated).astype(np.uint8)
    components, _labels, stats, _centroids = cv2.connectedComponentsWithStats(missed, connectivity=8)
    component_areas = sorted(
        (int(stats[index, cv2.CC_STAT_AREA]) for index in range(1, components)),
        reverse=True,
    )
    return {
        "precision": round(precision, 4) if precision is not None else None,
        "recall": round(recall, 4) if recall is not None else None,
        "precision_pass": precision is None or precision >= policy.min_extraction_precision,
        "recall_pass": recall is None or recall >= policy.min_extraction_recall,
        "visible_graphic_cells": visible_cells,
        "predicted_graphic_cells": predicted_cells,
        "missed_cells": int(missed.sum()),
        "missed_largest_component": component_areas[0] if component_areas else 0,
        "excluded_text_cells": int(text_mask.sum()),
        "excluded_image_cells": int(image_mask.sum()),
        "text_spans": len(texts),
        "image_coverage": round(image_coverage(block, policy), 4),
        "segments": int(len(ink["segments"])),
        "fill_groups": len(ink["fills"]),
        "page_rotation": int(ink["page_rotation"]),
        "invisible_paths_removed": int(ink["invisible_paths"]),
        "segments_removed_as_invisible": int(ink["segments_dropped_invisible"]),
        "measurement": {
            "cell_pt": cell_pt,
            "tolerance_pt": policy.tolerance_pt,
            "dark_threshold": policy.render_dark_threshold,
        },
    }


def extraction_pair_quality(
    left_block: PreparedBlock,
    right_block: PreparedBlock,
    left_ink: dict[str, Any],
    right_ink: dict[str, Any],
    policy: GraphicMode1Policy,
) -> dict[str, Any]:
    left = extraction_quality(left_block, left_ink, policy)
    right = extraction_quality(right_block, right_ink, policy)
    left_curves = (
        left["text_spans"] == 0
        and left["segments"] > policy.text_as_curves_min_segments
        and right["text_spans"] >= policy.text_as_curves_peer_spans
    )
    right_curves = (
        right["text_spans"] == 0
        and right["segments"] > policy.text_as_curves_min_segments
        and left["text_spans"] >= policy.text_as_curves_peer_spans
    )
    flags = {
        "raster_backed_side": bool(
            left["image_coverage"] > policy.raster_backed_area_fraction
            or right["image_coverage"] > policy.raster_backed_area_fraction
        ),
        "text_as_curves_asymmetry": bool(left_curves != right_curves),
        "precision_insufficient": bool(
            not left["precision_pass"] or not right["precision_pass"]
        ),
        "completeness_insufficient": bool(
            not left["recall_pass"] or not right["recall_pass"]
        ),
    }
    return {
        "left": left,
        "right": right,
        "text_as_curves": [bool(left_curves), bool(right_curves)],
        "flags": flags,
    }


__all__ = ["extraction_pair_quality", "extraction_quality"]
