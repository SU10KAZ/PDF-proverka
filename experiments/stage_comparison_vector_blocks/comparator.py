#!/usr/bin/env python3
"""Order-independent experimental comparator for two vector block descriptions."""
from __future__ import annotations

import argparse
import collections
import difflib
import json
import math
import re
from pathlib import Path
from typing import Any, Sequence


TOLERANCES = (0.001, 0.0025, 0.005, 0.01)
PRIMITIVE_MATCH_CAP = 8_000
SEGMENT_COVERAGE_CAP = 12_000
STATUSES = {
    "IDENTICAL",
    "NEAR_IDENTICAL",
    "STRUCTURE_SAME_VALUES_CHANGED",
    "STRUCTURE_CHANGED",
    "INSUFFICIENT_VECTOR_DATA",
}


def _distance(left: Sequence[float], right: Sequence[float]) -> float:
    return math.hypot(float(left[0]) - float(right[0]), float(left[1]) - float(right[1]))


def _f1(matches: int, left_total: int, right_total: int) -> float:
    if left_total == right_total == 0:
        return 1.0
    precision = matches / max(right_total, 1)
    recall = matches / max(left_total, 1)
    return 2 * precision * recall / max(precision + recall, 1e-12)


def _primitive_feature(primitive: dict[str, Any]) -> dict[str, Any]:
    bbox = primitive["normalized"]["bbox"]
    return {
        "id": primitive["id"],
        "type": primitive["type"],
        "center": ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2),
        "width": bbox[2] - bbox[0],
        "height": bbox[3] - bbox[1],
        "length": primitive["length_norm"],
        "angle": primitive["angle_degrees"],
        "segments": primitive["segment_count"],
        "closed": primitive["closed"],
    }


def _angle_distance(left: float | None, right: float | None) -> float:
    if left is None or right is None:
        return 0.0 if left is right else 180.0
    difference = abs(float(left) - float(right)) % 180
    return min(difference, 180 - difference)


def _match_primitives(
    left: Sequence[dict[str, Any]], right: Sequence[dict[str, Any]], tolerance: float
) -> dict[str, Any]:
    left_features = [_primitive_feature(item) for item in left]
    right_features = [_primitive_feature(item) for item in right]
    left_source_total, right_source_total = len(left_features), len(right_features)
    if len(left_features) > PRIMITIVE_MATCH_CAP:
        left_features = sorted(left_features, key=lambda item: item["length"], reverse=True)[
            :PRIMITIVE_MATCH_CAP
        ]
    if len(right_features) > PRIMITIVE_MATCH_CAP:
        right_features = sorted(right_features, key=lambda item: item["length"], reverse=True)[
            :PRIMITIVE_MATCH_CAP
        ]
    cell = max(tolerance * 2.0, 0.001)
    buckets: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for index, feature in enumerate(right_features):
        buckets[(round(feature["center"][0] / cell), round(feature["center"][1] / cell))].append(index)
    proposals = []
    for left_index, first in enumerate(left_features):
        gx, gy = round(first["center"][0] / cell), round(first["center"][1] / cell)
        candidates = set()
        for x in range(gx - 2, gx + 3):
            for y in range(gy - 2, gy + 3):
                candidates.update(buckets.get((x, y), []))
        for right_index in candidates:
            second = right_features[right_index]
            if first["type"] != second["type"] or first["closed"] != second["closed"]:
                continue
            if abs(first["segments"] - second["segments"]) > max(2, first["segments"] * 0.1):
                continue
            center_distance = _distance(first["center"], second["center"])
            size_distance = abs(first["width"] - second["width"]) + abs(first["height"] - second["height"])
            length_distance = abs(first["length"] - second["length"])
            angle_distance = _angle_distance(first["angle"], second["angle"])
            if center_distance > tolerance * 3.0:
                continue
            if size_distance > tolerance * 6.0 or length_distance > tolerance * 8.0:
                continue
            if angle_distance > max(1.0, tolerance * 500):
                continue
            cost = center_distance + 0.5 * size_distance + 0.25 * length_distance + angle_distance / 180
            proposals.append((cost, left_index, right_index))
    used_left, used_right, matches = set(), set(), []
    for cost, left_index, right_index in sorted(proposals):
        if left_index in used_left or right_index in used_right:
            continue
        used_left.add(left_index)
        used_right.add(right_index)
        matches.append(
            {
                "left_id": left_features[left_index]["id"],
                "right_id": right_features[right_index]["id"],
                "cost": round(cost, 6),
            }
        )
    return {
        "tolerance": tolerance,
        "matched": len(matches),
        "left_total": len(left),
        "right_total": len(right),
        "left_used": len(left_features),
        "right_used": len(right_features),
        "capped": left_source_total > len(left_features) or right_source_total > len(right_features),
        "similarity": round(_f1(len(matches), len(left), len(right)), 6),
        "matches_sample": matches[:200],
        "matches_truncated": len(matches) > 200,
        "left_unmatched": [
            left_features[index]["id"]
            for index in range(len(left_features))
            if index not in used_left
        ][:200],
        "right_unmatched": [
            right_features[index]["id"]
            for index in range(len(right_features))
            if index not in used_right
        ][:200],
    }


def _segment_features(primitives: Sequence[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    result = []
    for primitive in primitives:
        for start, end in primitive["normalized"]["segments"]:
            dx, dy = end[0] - start[0], end[1] - start[1]
            length = math.hypot(dx, dy)
            if length <= 1e-8:
                continue
            result.append(
                {
                    "primitive_id": primitive["id"],
                    "center": ((start[0] + end[0]) / 2, (start[1] + end[1]) / 2),
                    "length": length,
                    "angle": math.degrees(math.atan2(dy, dx)) % 180,
                }
            )
    total = len(result)
    if total > SEGMENT_COVERAGE_CAP:
        result = sorted(result, key=lambda item: item["length"], reverse=True)[:SEGMENT_COVERAGE_CAP]
    return result, total


def _directional_segment_coverage(
    source: Sequence[dict[str, Any]], target: Sequence[dict[str, Any]], tolerance: float
) -> tuple[int, list[str]]:
    cell = max(tolerance * 2.0, 0.001)
    buckets: dict[tuple[int, int], list[dict[str, Any]]] = collections.defaultdict(list)
    for feature in target:
        buckets[(round(feature["center"][0] / cell), round(feature["center"][1] / cell))].append(
            feature
        )
    matched = 0
    unmatched = []
    for first in source:
        gx, gy = round(first["center"][0] / cell), round(first["center"][1] / cell)
        found = False
        for x in range(gx - 2, gx + 3):
            for y in range(gy - 2, gy + 3):
                for second in buckets.get((x, y), []):
                    if _distance(first["center"], second["center"]) > tolerance * 3.0:
                        continue
                    if abs(first["length"] - second["length"]) > tolerance * 8.0:
                        continue
                    if _angle_distance(first["angle"], second["angle"]) > max(
                        1.0, tolerance * 500
                    ):
                        continue
                    found = True
                    break
                if found:
                    break
            if found:
                break
        if found:
            matched += 1
        elif len(unmatched) < 100:
            unmatched.append(first["primitive_id"])
    return matched, unmatched


def _segment_coverage_runs(
    left: Sequence[dict[str, Any]], right: Sequence[dict[str, Any]], tolerances: Sequence[float]
) -> list[dict[str, Any]]:
    left_features, left_total = _segment_features(left)
    right_features, right_total = _segment_features(right)
    result = []
    for tolerance in tolerances:
        left_matches, left_unmatched = _directional_segment_coverage(
            left_features, right_features, tolerance
        )
        right_matches, right_unmatched = _directional_segment_coverage(
            right_features, left_features, tolerance
        )
        left_coverage = left_matches / max(len(left_features), 1)
        right_coverage = right_matches / max(len(right_features), 1)
        result.append(
            {
                "tolerance": tolerance,
                "similarity": round((left_coverage + right_coverage) / 2, 6),
                "left_coverage": round(left_coverage, 6),
                "right_coverage": round(right_coverage, 6),
                "left_used": len(left_features),
                "right_used": len(right_features),
                "left_total": left_total,
                "right_total": right_total,
                "capped": left_total > len(left_features) or right_total > len(right_features),
                "left_unmatched_primitive_ids": left_unmatched,
                "right_unmatched_primitive_ids": right_unmatched,
            }
        )
    return result


def _counter_f1(left: collections.Counter[Any], right: collections.Counter[Any]) -> float:
    matches = sum((left & right).values())
    return _f1(matches, sum(left.values()), sum(right.values()))


def _text_diff(left: Sequence[dict[str, Any]], right: Sequence[dict[str, Any]]) -> dict[str, Any]:
    left_counter = collections.Counter(item["text"] for item in left)
    right_counter = collections.Counter(item["text"] for item in right)
    removed_counter = left_counter - right_counter
    added_counter = right_counter - left_counter
    removed = [text for text, count in removed_counter.items() for _ in range(count)]
    added = [text for text, count in added_counter.items() for _ in range(count)]

    left_unmatched = [item for item in left if removed_counter[item["text"]] > 0]
    right_unmatched = [item for item in right if added_counter[item["text"]] > 0]
    value_changes = []
    used = set()
    for first in left_unmatched:
        candidates = [
            (index, second)
            for index, second in enumerate(right_unmatched)
            if index not in used and first["category"] == second["category"]
        ]
        if not candidates:
            continue
        index, second = min(
            candidates,
            key=lambda row: _distance(
                (first["x_norm"], first["y_norm"]),
                (row[1]["x_norm"], row[1]["y_norm"]),
            ),
        )
        position_distance = _distance(
            (first["x_norm"], first["y_norm"]),
            (second["x_norm"], second["y_norm"]),
        )
        if position_distance <= 0.04 and first["text"] != second["text"]:
            used.add(index)
            value_changes.append(
                {
                    "left": first["text"],
                    "right": second["text"],
                    "category": first["category"],
                    "position_distance_norm": round(position_distance, 5),
                }
            )
    def layer_quality(items: Sequence[dict[str, Any]]) -> dict[str, Any]:
        value = "".join(item["text"] for item in items)
        suspicious = sum(1 for char in value if ord(char) < 32 and not char.isspace())
        ratio = suspicious / max(len(value), 1)
        status = "UNDECODABLE" if suspicious >= 5 or ratio >= 0.02 else (
            "ABSENT" if not value else "GOOD"
        )
        return {
            "status": status,
            "characters": len(value),
            "suspicious_control_characters": suspicious,
            "suspicious_ratio": round(ratio, 6),
        }

    def text_stream(items: Sequence[dict[str, Any]]) -> str:
        ordered = sorted(items, key=lambda item: (round(item["y_norm"], 2), item["x_norm"]))
        return re.sub(r"\s+", " ", " ".join(item["text"] for item in ordered)).strip()

    multiset_similarity = _counter_f1(left_counter, right_counter)
    stream_similarity = difflib.SequenceMatcher(
        None, text_stream(left), text_stream(right), autojunk=False
    ).ratio()
    left_quality, right_quality = layer_quality(left), layer_quality(right)
    reliable = left_quality["status"] == right_quality["status"] == "GOOD"
    return {
        "similarity": round(multiset_similarity, 6),
        "character_stream_similarity": round(stream_similarity, 6),
        "effective_similarity": round(max(multiset_similarity, stream_similarity), 6),
        "reliable": reliable,
        "left_layer_quality": left_quality,
        "right_layer_quality": right_quality,
        "unchanged_occurrences": sum((left_counter & right_counter).values()),
        "left_total": len(left),
        "right_total": len(right),
        "removed": removed[:200],
        "added": added[:200],
        "value_changes": value_changes[:100],
        "truncated": len(removed) > 200 or len(added) > 200 or len(value_changes) > 100,
    }


def _numeric_similarity(left: float, right: float) -> float:
    return 1.0 - abs(left - right) / max(abs(left), abs(right), 1.0)


def _topology_diff(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "node_count",
        "edge_count",
        "connected_components",
        "endpoints",
        "branch_points",
        "t_junctions",
        "x_crossings_unconnected",
        "closed_contours",
        "nested_contours",
    )
    changes = {key: {"left": left.get(key, 0), "right": right.get(key, 0)} for key in keys}
    similarities = [_numeric_similarity(float(row["left"]), float(row["right"])) for row in changes.values()]
    return {"similarity": round(sum(similarities) / len(similarities), 6), "counts": changes}


def _patterns_diff(left: Sequence[dict[str, Any]], right: Sequence[dict[str, Any]]) -> dict[str, Any]:
    left_counter = collections.Counter({item["pattern_id"]: item["count"] for item in left})
    right_counter = collections.Counter({item["pattern_id"]: item["count"] for item in right})
    all_ids = sorted(left_counter.keys() | right_counter.keys())
    changes = [
        {"pattern_id": key, "left": left_counter[key], "right": right_counter[key]}
        for key in all_ids
        if left_counter[key] != right_counter[key]
    ]
    return {
        "similarity": round(_counter_f1(left_counter, right_counter), 6),
        "changes": changes[:100],
        "truncated": len(changes) > 100,
    }


def compare_descriptions(
    left: dict[str, Any], right: dict[str, Any], tolerances: Sequence[float] = TOLERANCES
) -> dict[str, Any]:
    insufficient = {
        "VECTOR_DATA_INSUFFICIENT",
    } & {left["vector_quality"], right["vector_quality"]}
    primitive_runs = [
        _match_primitives(left["geometry"]["primitives"], right["geometry"]["primitives"], tolerance)
        for tolerance in tolerances
    ]
    geometry_runs = _segment_coverage_runs(
        left["geometry"]["primitives"], right["geometry"]["primitives"], tolerances
    )
    selected = next((run for run in geometry_runs if run["similarity"] >= 0.985), geometry_runs[-1])
    text = _text_diff(left["texts"], right["texts"])
    topology = _topology_diff(left["topology"], right["topology"])
    patterns = _patterns_diff(left["repeated_elements"], right["repeated_elements"])
    exact = (
        left["structural_signature"]["level_1_exact_vector"]
        == right["structural_signature"]["level_1_exact_vector"]
    )
    left_items = left["geometry"]["extraction"]["source_item_counts"]
    right_items = right["geometry"]["extraction"]["source_item_counts"]
    line_count_stable = _numeric_similarity(float(left_items.get("l", 0)), float(right_items.get("l", 0))) >= 0.99
    rectangle_count_changed = left_items.get("re", 0) != right_items.get("re", 0)
    endpoints_stable = _numeric_similarity(
        float(left["topology"]["endpoints"]), float(right["topology"]["endpoints"])
    ) >= 0.99
    encoding_rewrite_suspected = (
        line_count_stable
        and rectangle_count_changed
        and endpoints_stable
        and max(selected["left_coverage"], selected["right_coverage"]) >= 0.995
    )
    text_similarity = text["effective_similarity"]
    if insufficient:
        status = "INSUFFICIENT_VECTOR_DATA"
    elif exact:
        status = "IDENTICAL"
    elif encoding_rewrite_suspected and text["reliable"] and text_similarity < 0.92:
        status = "STRUCTURE_SAME_VALUES_CHANGED"
    elif (
        selected["similarity"] >= 0.985
        and topology["similarity"] >= 0.85
        and (not text["reliable"] or text_similarity >= 0.92)
    ):
        status = "NEAR_IDENTICAL"
    elif (
        selected["similarity"] >= 0.97
        and topology["similarity"] >= 0.85
        and text["reliable"]
        and text_similarity < 0.92
    ):
        status = "STRUCTURE_SAME_VALUES_CHANGED"
    else:
        status = "STRUCTURE_CHANGED"
    assert status in STATUSES
    differences = []
    if text["value_changes"]:
        differences.extend(
            f"Текст/значение {item['left']} → {item['right']}" for item in text["value_changes"][:20]
        )
    if text["added"]:
        differences.append(f"Добавлено text items: {', '.join(text['added'][:20])}")
    if text["removed"]:
        differences.append(f"Удалено text items: {', '.join(text['removed'][:20])}")
    primitive_left = left["primitive_summary"]["primitive_count"]
    primitive_right = right["primitive_summary"]["primitive_count"]
    if primitive_left != primitive_right:
        differences.append(f"Число примитивов: {primitive_left} → {primitive_right}")
    if patterns["changes"]:
        differences.append(f"Изменены повторяющиеся motifs: {len(patterns['changes'])}")
    if topology["similarity"] < 0.99:
        differences.append(
            f"Топология изменилась (similarity={topology['similarity']:.3f}, "
            f"ветвления {left['topology']['branch_points']} → {right['topology']['branch_points']})"
        )
    return {
        "schema_version": "vector-block-comparison-research-v0.1",
        "research_only": True,
        "left_block_id": left["block_id"],
        "right_block_id": right["block_id"],
        "status": status,
        "exact_vector_signature_equal": exact,
        "normalized_signature_equal": (
            left["structural_signature"]["level_2_normalized_geometry"]
            == right["structural_signature"]["level_2_normalized_geometry"]
        ),
        "structural_signature_equal": (
            left["structural_signature"]["level_3_structural_topology"]
            == right["structural_signature"]["level_3_structural_topology"]
        ),
        "geometry": {
            "tolerance_experiment": geometry_runs,
            "primitive_matching_experiment": primitive_runs,
            "selected_tolerance": selected["tolerance"],
            "similarity": selected["similarity"],
            "left_coverage": selected["left_coverage"],
            "right_coverage": selected["right_coverage"],
            "encoding_rewrite_suspected": encoding_rewrite_suspected,
        },
        "text": text,
        "topology": topology,
        "repeated_patterns": patterns,
        "differences": differences,
        "caveats": [
            "Segment coverage is order- and PDF-path-packaging-independent and uses block-normalized geometry; it does not use affine warping.",
            "Dense blocks are compared on the longest deterministic segment sample when the explicit cap is reached.",
            "A geometric X-crossing is not promoted to a connection without junction evidence.",
            "Undecodable embedded-font text is reported and excluded from status selection; OCR is not used.",
            "Status thresholds are research thresholds evaluated on this benchmark, not production policy.",
        ],
    }


def render_markdown(comparison: dict[str, Any]) -> str:
    geometry = comparison["geometry"]
    text = comparison["text"]
    topology = comparison["topology"]
    lines = [
        f"# Vector comparison `{comparison['left_block_id']}` ↔ `{comparison['right_block_id']}`",
        "",
        f"## Вердикт: **{comparison['status']}**",
        "",
        f"- Geometry similarity: {geometry['similarity']:.3f} при tolerance {geometry['selected_tolerance']:.2%}",
        f"- Text similarity: {text['effective_similarity']:.3f} (reliable={text['reliable']})",
        f"- Topology similarity: {topology['similarity']:.3f}",
        f"- Exact signature equal: {comparison['exact_vector_signature_equal']}",
        f"- Normalized signature equal: {comparison['normalized_signature_equal']}",
        f"- Structural signature equal: {comparison['structural_signature_equal']}",
        "",
        "## Эксперимент tolerances",
        "",
        "| Tolerance | Left coverage | Right coverage | Used L/R | Similarity |",
        "|---:|---:|---:|---:|---:|",
    ]
    lines.extend(
        f"| {run['tolerance']:.2%} | {run['left_coverage']:.3f} | {run['right_coverage']:.3f} | {run['left_used']}/{run['right_used']} | {run['similarity']:.3f} |"
        for run in geometry["tolerance_experiment"]
    )
    lines.extend(["", "## Изменения", ""])
    if comparison["differences"]:
        lines.extend(f"- {item}" for item in comparison["differences"])
    else:
        lines.append("- Детерминированных изменений не найдено.")
    lines.extend(["", "## Ограничения", ""])
    lines.extend(f"- {item}" for item in comparison["caveats"])
    return "\n".join(lines) + "\n"


def render_overlay_svg(
    left: dict[str, Any], right: dict[str, Any], comparison: dict[str, Any], max_segments: int = 8_000
) -> str:
    def rows(description: dict[str, Any], color: str) -> list[tuple[float, str]]:
        result = []
        for primitive in description["geometry"]["primitives"]:
            for start, end in primitive["normalized"]["segments"]:
                length = _distance(start, end)
                result.append(
                    (
                        length,
                        f'<line x1="{40 + start[0] * 900:.2f}" y1="{55 + start[1] * 900:.2f}" '
                        f'x2="{40 + end[0] * 900:.2f}" y2="{55 + end[1] * 900:.2f}" '
                        f'stroke="{color}" stroke-width="0.65" stroke-opacity="0.48"/>',
                    )
                )
        return sorted(result, reverse=True)[:max_segments]

    left_rows = rows(left, "#d62728")
    right_rows = rows(right, "#1f77b4")
    body = "\n".join(item[1] for item in left_rows + right_rows)
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="1000" viewBox="0 0 1000 1000">
<rect width="1000" height="1000" fill="white"/>
<text x="40" y="24" font-family="sans-serif" font-size="16">{comparison['status']} · red=left · blue=right · normalized overlay</text>
<rect x="40" y="55" width="900" height="900" fill="none" stroke="#777"/>
{body}
</svg>
'''


def save_comparison(
    comparison: dict[str, Any], left: dict[str, Any], right: dict[str, Any], output_dir: str | Path
) -> None:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "comparison.json").write_text(
        json.dumps(comparison, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    (output_dir / "comparison.md").write_text(render_markdown(comparison), encoding="utf-8")
    (output_dir / "overlay.svg").write_text(
        render_overlay_svg(left, right, comparison), encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left")
    parser.add_argument("right")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    left = json.loads(Path(args.left).read_text(encoding="utf-8"))
    right = json.loads(Path(args.right).read_text(encoding="utf-8"))
    comparison = compare_descriptions(left, right)
    save_comparison(comparison, left, right, args.output)


if __name__ == "__main__":
    main()
