#!/usr/bin/env python3
"""signoise probe 7 — how much information a nearest-geometry anchor actually carries.

For every block: how many DISTINCT primitives are tied for "nearest" within a small epsilon of the
winner, and how concentrated the anchor targets are. An anchor that has 20 equally-near candidates
cannot ground a value on an object.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_07_anchor_information
"""
from __future__ import annotations

import collections
import json
import math
from pathlib import Path
from typing import Any

from experiments.stage_comparison_vector_blocks import extractor as E

ROOT = Path(__file__).resolve().parents[3]
DESC = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

MAX_DISTANCE = 0.035          # extractor._anchors default
HIGH_THRESHOLD = 0.012        # extractor._anchors "high" confidence cut
EPSILON = 0.002               # "practically the same distance"
TEXT_SAMPLE = 300


def analyse(description: dict[str, Any]) -> dict[str, Any]:
    primitives = description["geometry"]["primitives"]
    segments = E._all_segments(primitives)
    cell = MAX_DISTANCE
    grid: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for index, segment in enumerate(segments):
        bbox = E._bbox([segment["p1"], segment["p2"]])
        for gx in range(math.floor((bbox[0] - MAX_DISTANCE) / cell),
                        math.floor((bbox[2] + MAX_DISTANCE) / cell) + 1):
            for gy in range(math.floor((bbox[1] - MAX_DISTANCE) / cell),
                            math.floor((bbox[3] + MAX_DISTANCE) / cell) + 1):
                grid[(gx, gy)].append(index)

    texts = description["texts"][:TEXT_SAMPLE]
    tie_counts, within_counts, nearest_distances = [], [], []
    for text in texts:
        point = (text["x_norm"], text["y_norm"])
        best: dict[str, float] = {}
        for index in grid.get((math.floor(point[0] / cell), math.floor(point[1] / cell)), []):
            segment = segments[index]
            distance = E._point_segment_distance(point, [segment["p1"], segment["p2"]])
            key = segment["primitive_id"]
            if key not in best or distance < best[key]:
                best[key] = distance
        if not best:
            tie_counts.append(0)
            within_counts.append(0)
            continue
        minimum = min(best.values())
        nearest_distances.append(minimum)
        tie_counts.append(sum(1 for d in best.values() if d <= minimum + EPSILON))
        within_counts.append(sum(1 for d in best.values() if d <= MAX_DISTANCE))

    anchors = description["anchors"]
    targets = collections.Counter(a["geometry_id"] for a in anchors if a["geometry_id"])
    bound = sum(targets.values())
    ordered = sorted(nearest_distances)
    return {
        "texts_total": len(description["texts"]),
        "texts_sampled": len(texts),
        "anchors_total": len(anchors),
        "anchor_confidence": dict(collections.Counter(a["confidence"] for a in anchors)),
        "distinct_anchor_targets": len(targets),
        "distinct_targets_per_bound_anchor": round(len(targets) / max(bound, 1), 4),
        "top_target_share": round(max(targets.values()) / max(bound, 1), 4) if targets else None,
        "median_tied_primitives_within_eps": sorted(tie_counts)[len(tie_counts) // 2] if tie_counts else None,
        "mean_tied_primitives_within_eps": round(sum(tie_counts) / max(len(tie_counts), 1), 3),
        "share_texts_with_ambiguous_nearest": round(
            sum(1 for c in tie_counts if c > 1) / max(len(tie_counts), 1), 4),
        "mean_primitives_within_max_distance": round(
            sum(within_counts) / max(len(within_counts), 1), 2),
        "median_nearest_distance": round(ordered[len(ordered) // 2], 5) if ordered else None,
        "share_nearest_below_high_threshold": round(
            sum(1 for d in nearest_distances if d <= HIGH_THRESHOLD) / max(len(nearest_distances), 1), 4),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    rows = {}
    for pair_dir in sorted(DESC.iterdir()):
        path = pair_dir / "left" / "vector_block.json"
        if not path.exists():
            continue
        description = json.loads(path.read_text(encoding="utf-8"))
        rows[pair_dir.name] = analyse(description)
        print(f"  {pair_dir.name}: {rows[pair_dir.name]}")
        del description
    payload = {"probe": "signoise_07_anchor_information", "research_only": True,
               "epsilon_norm": EPSILON, "max_distance_norm": MAX_DISTANCE,
               "text_sample_cap": TEXT_SAMPLE, "side": "left", "per_block": rows}
    (OUT / "signoise_07_anchor_information.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    lines = [
        "# signoise probe 7 — information content of `anchors`",
        "",
        "Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_07_anchor_information`",
        "",
        f"For up to {TEXT_SAMPLE} text spans per block (left side), the number of DISTINCT primitives "
        f"whose distance to the span is within {EPSILON} of the nearest one — i.e. how many objects the "
        "anchor could equally well have picked.",
        "",
        "| block | texts | mean tied primitives | median | % spans with ambiguous nearest | "
        "mean primitives within 0.035 | distinct anchor targets | top target share |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, row in rows.items():
        lines.append(
            f"| {name} | {row['texts_total']} | {row['mean_tied_primitives_within_eps']} | "
            f"{row['median_tied_primitives_within_eps']} | "
            f"{100*row['share_texts_with_ambiguous_nearest']:.1f} % | "
            f"{row['mean_primitives_within_max_distance']} | {row['distinct_anchor_targets']} | "
            f"{row['top_target_share']} |"
        )
    (OUT / "signoise_07_anchor_information.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("written:", OUT / "signoise_07_anchor_information.json")


if __name__ == "__main__":
    main()
