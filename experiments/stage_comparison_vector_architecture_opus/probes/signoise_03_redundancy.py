#!/usr/bin/env python3
"""signoise probe 3 — redundancy between fields of VectorBlockDescription v0.1.

Loads the 20 Track A descriptions once, dumps a compact per-block feature record
(artifacts/signoise_03_block_features.json, reused by probe 4), then measures
Pearson/Spearman correlation between fields that plausibly duplicate each other and the
agreement of the three structural_signature levels across all C(20,2)=190 cross pairings.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_03_redundancy
"""
from __future__ import annotations

import collections
import itertools
import json
import math
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
DESC = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

GRID = 4
ANGLE_BINS = ("h_0", "d_45", "v_90", "d_135", "other")


def angle_bin(degrees: float) -> str:
    d = degrees % 180
    for centre, name in ((0.0, "h_0"), (45.0, "d_45"), (90.0, "v_90"), (135.0, "d_135"), (180.0, "h_0")):
        if abs(d - centre) <= 3.0:
            return name
    return "other"


def block_features(description: dict[str, Any]) -> dict[str, Any]:
    summary = description["primitive_summary"]
    topology = description["topology"]
    angles = collections.Counter()
    grid = [0] * (GRID * GRID)
    lengths: list[float] = []
    for primitive in description["geometry"]["primitives"]:
        for start, end in primitive["normalized"]["segments"]:
            dx, dy = end[0] - start[0], end[1] - start[1]
            length = math.hypot(dx, dy)
            if length <= 1e-9:
                continue
            lengths.append(length)
            angles[angle_bin(math.degrees(math.atan2(dy, dx)))] += 1
            cx, cy = (start[0] + end[0]) / 2, (start[1] + end[1]) / 2
            gx = min(GRID - 1, max(0, int(cx * GRID)))
            gy = min(GRID - 1, max(0, int(cy * GRID)))
            grid[gy * GRID + gx] += 1
    total_segments = max(len(lengths), 1)
    categories = collections.Counter(t["category"] for t in description["texts"])
    text_total = max(len(description["texts"]), 1)
    bbox = description["bbox"]
    return {
        "block_id": description["block_id"],
        "vector_quality": description["vector_quality"],
        "pdf_sha256_16": description["source"]["pdf_sha256"][:16],
        "counts": {
            "primitive_count": summary["primitive_count"],
            "total_segment_count": summary["total_segment_count"],
            "stroke_paths": summary["stroke_paths"],
            "filled_paths": summary["filled_paths"],
            "closed_paths": summary["closed_paths"],
            "text_items": summary["text_items"],
            "engineering_values": summary["engineering_values"],
            "connected_components": summary["connected_components"],
            "node_count": topology["node_count"],
            "edge_count": topology["edge_count"],
            "segments_total": topology["segments_total"],
            "segments_used": topology["segments_used"],
            "endpoints": topology["endpoints"],
            "branch_points": topology["branch_points"],
            "t_junctions": topology["t_junctions"],
            "x_crossings_unconnected": topology["x_crossings_unconnected"],
            "closed_contours": topology["closed_contours"],
            "nested_contours": topology["nested_contours"],
            "anchors": len(description["anchors"]),
            "repeated_elements": len(description["repeated_elements"]),
            "hatch_like_structures": len(description["hatch_like_structures"]),
            "dimensions": len(description["dimensions"]),
            "labels": len(description["labels"]),
            "size_l0_bytes": description["size_metrics"]["level_0_raw_vector"]["bytes"],
            "size_l1_bytes": description["size_metrics"]["level_1_normalized_primitives"]["bytes"],
            "size_l2_bytes": description["size_metrics"]["level_2_groups_topology"]["bytes"],
            "size_l3_bytes": description["size_metrics"]["level_3_compact_description"]["bytes"],
        },
        "signatures": {
            "l1": description["structural_signature"]["level_1_exact_vector"],
            "l2": description["structural_signature"]["level_2_normalized_geometry"],
            "l3": description["structural_signature"]["level_3_structural_topology"],
        },
        "l3_payload": description["structural_signature"]["level_3_payload"],
        "coarse": {
            "angle_shares": {name: angles.get(name, 0) / total_segments for name in ANGLE_BINS},
            "grid_shares": [value / total_segments for value in grid],
            "length_mean_norm": sum(lengths) / total_segments,
            "length_p90_norm": sorted(lengths)[int(0.9 * (len(lengths) - 1))] if lengths else 0.0,
            "log_segments": math.log10(1 + total_segments),
            "log_components": math.log10(1 + summary["connected_components"]),
            "log_texts": math.log10(1 + summary["text_items"]),
            "endpoint_ratio": topology["endpoints"] / max(topology["node_count"], 1),
            "branch_ratio": topology["branch_points"] / max(topology["node_count"], 1),
            "t_junction_ratio": topology["t_junctions"] / max(topology["node_count"], 1),
            "closed_ratio": topology["closed_contours"] / max(topology["segments_used"], 1),
            "category_shares": {
                name: categories.get(name, 0) / text_total
                for name in ("label", "numeric", "engineering_value")
            },
            "aspect_ratio": (bbox[2] - bbox[0]) / max(bbox[3] - bbox[1], 1e-9),
        },
    }


def pearson(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return num / dx / dy if dx and dy else float("nan")


def ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda i: values[i])
    result = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        average = (i + j) / 2 + 1
        for k in range(i, j + 1):
            result[order[k]] = average
        i = j + 1
    return result


def spearman(xs: list[float], ys: list[float]) -> float:
    return pearson(ranks(xs), ranks(ys))


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    features: dict[str, Any] = {}
    for pair_dir in sorted(DESC.iterdir()):
        for side in ("left", "right"):
            path = pair_dir / side / "vector_block.json"
            if not path.exists():
                continue
            description = json.loads(path.read_text(encoding="utf-8"))
            features[f"{pair_dir.name}/{side}"] = block_features(description)
            del description
            print("  features:", pair_dir.name, side)
    (OUT / "signoise_03_block_features.json").write_text(
        json.dumps(features, ensure_ascii=False, indent=1), encoding="utf-8")

    names = list(features)
    fields = list(features[names[0]]["counts"])
    columns = {f: [float(features[n]["counts"][f]) for n in names] for f in fields}

    focus = [
        ("primitive_count", "total_segment_count"),
        ("primitive_count", "size_l0_bytes"),
        ("total_segment_count", "size_l0_bytes"),
        ("total_segment_count", "size_l1_bytes"),
        ("node_count", "edge_count"),
        ("node_count", "segments_total"),
        ("edge_count", "segments_total"),
        ("node_count", "segments_used"),
        ("edge_count", "segments_used"),
        ("endpoints", "node_count"),
        ("branch_points", "t_junctions"),
        ("text_items", "anchors"),
        ("text_items", "labels"),
        ("engineering_values", "dimensions"),
        ("connected_components", "primitive_count"),
        ("size_l2_bytes", "text_items"),
        ("size_l3_bytes", "text_items"),
        ("size_l3_bytes", "repeated_elements"),
    ]
    focus_rows = [
        {
            "a": a, "b": b,
            "pearson": round(pearson(columns[a], columns[b]), 4),
            "spearman": round(spearman(columns[a], columns[b]), 4),
            "identical_values": columns[a] == columns[b],
            "max_abs_relative_gap": round(max(
                abs(x - y) / max(abs(x), abs(y), 1.0) for x, y in zip(columns[a], columns[b])), 4),
        }
        for a, b in focus
    ]

    # full correlation matrix (Spearman) for the count fields
    matrix = {a: {b: round(spearman(columns[a], columns[b]), 3) for b in fields} for a in fields}
    high = sorted(
        (
            {"a": a, "b": b, "spearman": matrix[a][b]}
            for a, b in itertools.combinations(fields, 2)
            if not math.isnan(matrix[a][b]) and abs(matrix[a][b]) >= 0.9
        ),
        key=lambda row: -abs(row["spearman"]),
    )

    # signature level agreement over all cross pairings
    contingency = collections.Counter()
    pair_of = {n: n.split("/")[0] for n in names}
    same_block_pair_rows = []
    for a, b in itertools.combinations(names, 2):
        sa, sb = features[a]["signatures"], features[b]["signatures"]
        key = (sa["l1"] == sb["l1"], sa["l2"] == sb["l2"], sa["l3"] == sb["l3"])
        contingency[key] += 1
        if pair_of[a] == pair_of[b]:
            same_block_pair_rows.append({"pair": pair_of[a], "l1_eq": key[0], "l2_eq": key[1], "l3_eq": key[2]})
    l2_l3_agree = sum(v for k, v in contingency.items() if k[1] == k[2])
    l2_l3_disagree = sum(v for k, v in contingency.items() if k[1] != k[2])

    payload = {
        "probe": "signoise_03_redundancy",
        "research_only": True,
        "blocks": names,
        "focus_correlations": focus_rows,
        "spearman_matrix": matrix,
        "pairs_with_abs_spearman_ge_0.9": high,
        "signature_cross_pairings": {
            "total_unordered_pairings": sum(contingency.values()),
            "contingency_l1_l2_l3_equal": {str(list(k)): v for k, v in sorted(contingency.items())},
            "l2_and_l3_agree": l2_l3_agree,
            "l2_and_l3_disagree": l2_l3_disagree,
            "same_benchmark_pair_rows": same_block_pair_rows,
        },
    }
    (OUT / "signoise_03_redundancy.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    lines = [
        "# signoise probe 3 — redundancy between VectorBlockDescription fields",
        "",
        "Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_03_redundancy`",
        "",
        f"n = {len(names)} blocks.",
        "",
        "## Suspected-duplicate field pairs",
        "",
        "| field A | field B | Pearson r | Spearman ρ | identical values? | max relative gap |",
        "|---|---|---:|---:|:--:|---:|",
    ]
    for row in focus_rows:
        lines.append(
            f"| `{row['a']}` | `{row['b']}` | {row['pearson']:.4f} | {row['spearman']:.4f} | "
            f"{'YES' if row['identical_values'] else 'no'} | {row['max_abs_relative_gap']:.4f} |"
        )
    lines += ["", "## All count-field pairs with |Spearman| >= 0.9", "",
              "| A | B | ρ |", "|---|---|---:|"]
    for row in high:
        lines.append(f"| `{row['a']}` | `{row['b']}` | {row['spearman']:.3f} |")
    lines += [
        "", "## structural_signature levels over all C(20,2) = 190 cross pairings", "",
        f"- pairings where level_2 equality and level_3 equality AGREE: **{l2_l3_agree}/190**",
        f"- pairings where they DISAGREE: **{l2_l3_disagree}/190**",
        "",
        "| (l1_eq, l2_eq, l3_eq) | pairings |", "|---|---:|",
    ]
    for key, value in sorted(contingency.items()):
        lines.append(f"| {key} | {value} |")
    lines += ["", "## The 10 benchmark pairs (true counterparts)", "",
              "| pair | l1 equal | l2 equal | l3 equal |", "|---|:--:|:--:|:--:|"]
    for row in same_block_pair_rows:
        lines.append(f"| {row['pair']} | {row['l1_eq']} | {row['l2_eq']} | {row['l3_eq']} |")
    (OUT / "signoise_03_redundancy.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("written:", OUT / "signoise_03_redundancy.json")


if __name__ == "__main__":
    main()
