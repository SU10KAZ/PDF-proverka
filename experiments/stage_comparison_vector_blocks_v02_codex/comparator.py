"""Vector comparator v0.2: geometry, topology, text, crop and style evidence."""
from __future__ import annotations

import collections
import math
import re
from typing import Any, Sequence

from experiments.stage_comparison_vector_blocks import comparator as baseline


STYLE_COLOR_TOLERANCE = 0.015
STYLE_WIDTH_ABS_TOLERANCE_PT = 0.05
STYLE_WIDTH_REL_TOLERANCE = 0.05
STYLE_OPACITY_TOLERANCE = 0.02
STYLE_MATCH_CAP = 8_000


def _distance(left: Sequence[float], right: Sequence[float]) -> float:
    return math.hypot(float(left[0]) - float(right[0]), float(left[1]) - float(right[1]))


def _style_value(style: dict[str, Any], field: str) -> Any:
    aliases = {"width": "stroke_width", "dash": "dashes", "opacity": "stroke_opacity"}
    return style.get(aliases.get(field, field))


def _dash(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).replace("[] 0", "")


def _color_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    if len(left) != len(right):
        return False
    return all(abs(float(a) - float(b)) <= STYLE_COLOR_TOLERANCE for a, b in zip(left, right))


def _number_equal(left: Any, right: Any, *, absolute: float, relative: float = 0.0) -> bool:
    if left is None or right is None:
        return left is right
    tolerance = max(absolute, relative * max(abs(float(left)), abs(float(right)), 1e-9))
    return abs(float(left) - float(right)) <= tolerance


def _style_changes(left: dict[str, Any], right: dict[str, Any]) -> list[dict[str, Any]]:
    checks = {
        "stroke": _color_equal(left.get("stroke"), right.get("stroke")),
        "fill": _color_equal(left.get("fill"), right.get("fill")),
        "width": _number_equal(
            left.get("stroke_width"), right.get("stroke_width"),
            absolute=STYLE_WIDTH_ABS_TOLERANCE_PT, relative=STYLE_WIDTH_REL_TOLERANCE,
        ),
        "dash": _dash(left.get("dashes")) == _dash(right.get("dashes")),
        "stroke_opacity": _number_equal(left.get("stroke_opacity"), right.get("stroke_opacity"), absolute=STYLE_OPACITY_TOLERANCE),
        "fill_opacity": _number_equal(left.get("fill_opacity"), right.get("fill_opacity"), absolute=STYLE_OPACITY_TOLERANCE),
        "line_cap": list(left.get("line_cap") or []) == list(right.get("line_cap") or []),
        "line_join": left.get("line_join") == right.get("line_join"),
    }
    changes = []
    for field, equal in checks.items():
        if not equal:
            changes.append({"field": field, "left": _style_value(left, field), "right": _style_value(right, field)})
    return changes


def _feature(primitive: dict[str, Any]) -> dict[str, Any]:
    bbox = primitive["normalized"]["bbox"]
    return {
        "primitive": primitive,
        "center": ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2),
        "width": bbox[2] - bbox[0], "height": bbox[3] - bbox[1],
        "length": primitive["length_norm"], "angle": primitive["angle_degrees"],
    }


def compare_style(
    left: Sequence[dict[str, Any]],
    right: Sequence[dict[str, Any]],
    *,
    geometry_tolerance: float,
) -> dict[str, Any]:
    left_features = [_feature(item) for item in left]
    right_features = [_feature(item) for item in right]
    capped = len(left_features) > STYLE_MATCH_CAP or len(right_features) > STYLE_MATCH_CAP
    left_features = sorted(left_features, key=lambda item: item["length"], reverse=True)[:STYLE_MATCH_CAP]
    right_features = sorted(right_features, key=lambda item: item["length"], reverse=True)[:STYLE_MATCH_CAP]
    cell = max(geometry_tolerance * 2, 0.001)
    buckets: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for index, item in enumerate(right_features):
        buckets[(round(item["center"][0] / cell), round(item["center"][1] / cell))].append(index)
    proposals = []
    for left_index, first in enumerate(left_features):
        gx, gy = round(first["center"][0] / cell), round(first["center"][1] / cell)
        for x in range(gx - 2, gx + 3):
            for y in range(gy - 2, gy + 3):
                for right_index in buckets.get((x, y), []):
                    second = right_features[right_index]
                    a, b = first["primitive"], second["primitive"]
                    if a["type"] != b["type"] or a["closed"] != b["closed"]:
                        continue
                    center = _distance(first["center"], second["center"])
                    size = abs(first["width"] - second["width"]) + abs(first["height"] - second["height"])
                    length = abs(first["length"] - second["length"])
                    angle = baseline._angle_distance(first["angle"], second["angle"])
                    if center > geometry_tolerance * 3 or size > geometry_tolerance * 6 or length > geometry_tolerance * 8:
                        continue
                    if angle > max(1.0, geometry_tolerance * 500):
                        continue
                    proposals.append((center + size / 2 + length / 4 + angle / 180, left_index, right_index))
    used_left: set[int] = set()
    used_right: set[int] = set()
    field_counts: collections.Counter[str] = collections.Counter()
    samples = []
    changed_pairs = 0
    for _, left_index, right_index in sorted(proposals):
        if left_index in used_left or right_index in used_right:
            continue
        used_left.add(left_index); used_right.add(right_index)
        first = left_features[left_index]["primitive"]
        second = right_features[right_index]["primitive"]
        changes = _style_changes(first["style"], second["style"])
        if changes:
            changed_pairs += 1
            field_counts.update(change["field"] for change in changes)
            if len(samples) < 50:
                samples.append({"left_id": first["id"], "right_id": second["id"], "changes": changes})
    matched = len(used_left)
    coverage = 2 * matched / max(len(left_features) + len(right_features), 1)
    unchanged = matched - changed_pairs
    return {
        "matched_geometry_pairs": matched,
        "match_coverage": round(coverage, 6),
        "unchanged_pairs": unchanged,
        "changed_pairs": changed_pairs,
        "similarity": round(unchanged / max(matched, 1), 6),
        "field_change_counts": dict(sorted(field_counts.items())),
        "changes": samples,
        "changes_truncated": changed_pairs > len(samples),
        "capped": capped,
        "reliable": not capped and (
            (matched >= 3 and coverage >= 0.8)
            or (matched >= 1 and matched == len(left_features) == len(right_features))
        ),
        "tolerances": {
            "color_channel": STYLE_COLOR_TOLERANCE,
            "width_absolute_pt": STYLE_WIDTH_ABS_TOLERANCE_PT,
            "width_relative": STYLE_WIDTH_REL_TOLERANCE,
            "opacity": STYLE_OPACITY_TOLERANCE,
            "dash_cap_join": "normalized/exact",
        },
    }


def crop_diagnostics(left: dict[str, Any], right: dict[str, Any], geometry: dict[str, Any]) -> dict[str, Any]:
    def aspect(description: dict[str, Any]) -> float:
        bbox = description["bbox"]
        return (bbox[2] - bbox[0]) / max(bbox[3] - bbox[1], 1e-9)

    left_aspect, right_aspect = aspect(left), aspect(right)
    aspect_delta = abs(left_aspect - right_aspect) / max(left_aspect, right_aspect, 1e-9)
    left_extent = left.get("content_extent") or {"bbox_norm": [0, 0, 1, 1], "edge_anchor_counts": {}}
    right_extent = right.get("content_extent") or {"bbox_norm": [0, 0, 1, 1], "edge_anchor_counts": {}}
    extent_deltas = [abs(float(a) - float(b)) for a, b in zip(left_extent["bbox_norm"], right_extent["bbox_norm"])]
    coverage_asymmetry = abs(float(geometry["left_coverage"]) - float(geometry["right_coverage"]))
    edges = ("left", "top", "right", "bottom")
    edge_changes = {
        edge: {
            "left": int(left_extent.get("edge_anchor_counts", {}).get(edge, 0)),
            "right": int(right_extent.get("edge_anchor_counts", {}).get(edge, 0)),
        }
        for edge in edges
    }
    edge_imbalance = max(
        (abs(row["left"] - row["right"]) / max(row["left"], row["right"], 1) for row in edge_changes.values()),
        default=0.0,
    )
    strong_aspect = aspect_delta > 0.12
    asymmetric_content = max(extent_deltas, default=0.0) > 0.08 and coverage_asymmetry > 0.05
    border_disagreement = edge_imbalance > 0.75 and coverage_asymmetry > 0.08
    mismatch = strong_aspect or asymmetric_content or border_disagreement
    confidence = "HIGH" if sum((strong_aspect, asymmetric_content, border_disagreement)) >= 2 else ("MEDIUM" if mismatch else "LOW")
    return {
        "mismatch": mismatch,
        "confidence": confidence,
        "aspect": {"left": round(left_aspect, 6), "right": round(right_aspect, 6), "relative_delta": round(aspect_delta, 6)},
        "content_extent": {"left": left_extent["bbox_norm"], "right": right_extent["bbox_norm"], "edge_deltas": [round(v, 6) for v in extent_deltas]},
        "asymmetric_geometry_coverage": round(coverage_asymmetry, 6),
        "edge_geometry": edge_changes,
        "edge_anchor_imbalance": round(edge_imbalance, 6),
        "signals": {
            "aspect": strong_aspect,
            "asymmetric_content": asymmetric_content,
            "border_disagreement": border_disagreement,
        },
        "affine_warp_used": False,
    }


def _caps(description: dict[str, Any]) -> dict[str, bool]:
    defaults = {"segments_capped": False, "topology_capped": False, "patterns_capped": False, "text_capped": False}
    defaults.update(description.get("cap_flags") or {})
    return defaults


def _evidence(left: dict[str, Any], right: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    text = comparison["text"]
    topology = comparison["topology"]
    patterns = comparison["repeated_patterns"]
    left_segments = left["primitive_summary"]["total_segment_count"]
    right_segments = right["primitive_summary"]["total_segment_count"]
    return {
        "geometry": {
            "left_segments": left_segments, "right_segments": right_segments,
            "segment_delta": right_segments - left_segments,
            "similarity": comparison["geometry"]["similarity"],
            "left_coverage": comparison["geometry"]["left_coverage"],
            "right_coverage": comparison["geometry"]["right_coverage"],
        },
        "topology": {
            "similarity": topology["similarity"],
            "branch_points": {"left": left["topology"]["branch_points"], "right": right["topology"]["branch_points"], "delta": right["topology"]["branch_points"] - left["topology"]["branch_points"]},
            "components": {"left": left["topology"]["connected_components"], "right": right["topology"]["connected_components"], "delta": right["topology"]["connected_components"] - left["topology"]["connected_components"]},
        },
        "text": {
            "quality": {"left": left["text_quality"]["status"], "right": right["text_quality"]["status"]},
            "changed_values": text["value_changes"], "added": text["added"], "removed": text["removed"],
        },
        "patterns": {"changes": patterns["changes"]},
        "style": {
            "changed_pairs": comparison["style"]["changed_pairs"],
            "field_change_counts": comparison["style"]["field_change_counts"],
            "changes": comparison["style"]["changes"],
        },
        "crop": comparison["crop"],
    }


def compare_descriptions(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    comparison = baseline.compare_descriptions(left, right)
    geometry = comparison["geometry"]
    style = compare_style(
        left["geometry"]["primitives"], right["geometry"]["primitives"],
        geometry_tolerance=float(geometry["selected_tolerance"]),
    )
    crop = crop_diagnostics(left, right, geometry)
    comparison["schema_version"] = "vector-block-comparison-research-v0.2-codex"
    comparison["style"] = style
    comparison["crop"] = crop
    comparison["text"]["left_layer_quality"] = left["text_quality"]
    comparison["text"]["right_layer_quality"] = right["text_quality"]
    comparison["text"]["reliable"] = left["text_quality"]["status"] == right["text_quality"]["status"] == "TEXT_GOOD"
    if crop["mismatch"]:
        comparison["status"] = "CROP_MISMATCH"
    elif left["vector_quality"] == "VECTOR_DATA_INSUFFICIENT" or right["vector_quality"] == "VECTOR_DATA_INSUFFICIENT":
        comparison["status"] = "INSUFFICIENT_VECTOR_DATA"
    elif (
        style["changed_pairs"] > 0 and style["reliable"]
        and geometry["similarity"] >= 0.995
        and geometry["left_coverage"] >= 0.995 and geometry["right_coverage"] >= 0.995
        and comparison["topology"]["similarity"] >= 0.95
        and left["primitive_summary"]["total_segment_count"] == right["primitive_summary"]["total_segment_count"]
        and not any(_caps(left).values()) and not any(_caps(right).values())
        and (not comparison["text"]["reliable"] or comparison["text"]["effective_similarity"] >= 0.92)
    ):
        comparison["status"] = "STYLE_ONLY_CHANGED"
    comparison["cap_flags"] = {"left": _caps(left), "right": _caps(right)}
    comparison["evidence"] = _evidence(left, right, comparison)
    return comparison


__all__ = ["compare_descriptions", "compare_style", "crop_diagnostics"]
