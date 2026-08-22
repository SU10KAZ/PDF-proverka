#!/usr/bin/env python3
"""TCF probe 8 — signal vs noise: can «Добавлены два ответвления» be derived?

Injects a realistic engineering change into a real block description — k new
branches, each a 3-segment polyline hung off an existing graph node and ending in a
24-gon device circle — and measures what the Track A pipeline reports:

  * the comparator topology similarity base vs injected,
  * the comparator segment-coverage similarity and the resulting status branch,
  * whether `branch_points` / `endpoints` move by the injected amount,

and compares those against the noise floor already measured in probes 1 and 2
(the same block against itself at a neighbouring tolerance or a different cap).

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p8_injection
"""
from __future__ import annotations

import json
import math
import pathlib
import random

from experiments.stage_comparison_vector_blocks.comparator import (
    _segment_coverage_runs,
    _topology_diff,
)
from experiments.stage_comparison_vector_blocks.extractor import _all_segments, _distance
from experiments.stage_comparison_vector_architecture_opus.probes import tcf_topo

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p8_injection.json")
BLOCKS = ("ss_simple_node", "ss_scheme_text_changed", "eom_singleline_changed", "vk_plan", "ss_plan_dense", "ar_plan")
TOL = 0.0025
CAP = 8_000
SEED = 20260823


def make_primitive(points, pid, kind, closed):
    segs = [[list(points[i]), list(points[i + 1])] for i in range(len(points) - 1)]
    if closed and _distance(points[0], points[-1]) > 1e-9:
        segs.append([list(points[-1]), list(points[0])])
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return {
        "id": pid,
        "type": kind,
        "normalized": {"segments": segs, "bbox": [min(xs), min(ys), max(xs), max(ys)]},
        "raw": {"segments": segs, "bbox": [min(xs), min(ys), max(xs), max(ys)]},
        "length_norm": round(sum(_distance(*s) for s in segs), 6),
        "angle_degrees": 0.0,
        "segment_count": len(segs),
        "closed": closed,
        "style": {},
        "source_kinds": ["l"],
        "drawing_index": -1,
        "item_indexes": [],
        "length": round(sum(_distance(*s) for s in segs), 6),
    }


def inject(primitives, k, rng, branch_len=0.04, device_radius=0.006):
    segments = _all_segments(primitives)
    anchors = rng.sample(segments, min(len(segments), max(20, k)))
    extra = []
    for i in range(k):
        base = anchors[i % len(anchors)]["p2"]
        angle = rng.uniform(0, 2 * math.pi)
        step = branch_len / 3
        pts = [tuple(base)]
        for s in range(3):
            pts.append((pts[-1][0] + step * math.cos(angle), pts[-1][1] + step * math.sin(angle)))
        extra.append(make_primitive(pts, f"injected-branch-{i}", "polyline", False))
        cx, cy = pts[-1]
        circle = [
            (cx + device_radius * math.cos(2 * math.pi * j / 24),
             cy + device_radius * math.sin(2 * math.pi * j / 24))
            for j in range(24)
        ]
        extra.append(make_primitive(circle, f"injected-device-{i}", "circle", True))
    return list(primitives) + extra


def main() -> None:
    rng = random.Random(SEED)
    out = {"probe": "tcf_p8_injection", "tolerance": TOL, "cap": CAP, "blocks": {}}
    for name in BLOCKS:
        d = json.loads((ROOT / name / "left" / "vector_block.json").read_text(encoding="utf-8"))
        primitives = d["geometry"]["primitives"]
        base_topo = tcf_topo.topology(primitives, TOL, CAP)
        rows = {}
        for k in (1, 2, 5, 20):
            injected = inject(primitives, k, random.Random(SEED + k))
            topo = tcf_topo.topology(injected, TOL, CAP)
            coverage = _segment_coverage_runs(primitives, injected, (0.001, 0.0025, 0.005, 0.01))
            selected = next((r for r in coverage if r["similarity"] >= 0.985), coverage[-1])
            rows[f"k={k}"] = {
                "segments_added": 3 * k + 24 * k,
                "topology_similarity": _topology_diff(base_topo, topo)["similarity"],
                "geometry_similarity_selected": selected["similarity"],
                "geometry_selected_tolerance": selected["tolerance"],
                "endpoints": [base_topo["endpoints"], topo["endpoints"]],
                "branch_points": [base_topo["branch_points"], topo["branch_points"]],
                "node_count": [base_topo["node_count"], topo["node_count"]],
                "connected_components": [base_topo["connected_components"], topo["connected_components"]],
                "closed_contours": [base_topo["closed_contours"], topo["closed_contours"]],
                "segments_used": [base_topo["segments_used"], topo["segments_used"]],
                "geometry_similarity_ge_0985": selected["similarity"] >= 0.985,
                "topology_similarity_ge_085": _topology_diff(base_topo, topo)["similarity"] >= 0.85,
            }
        out["blocks"][name] = {
            "segments_total": base_topo["segments_total"],
            "segments_capped": base_topo["segments_capped"],
            "runs": rows,
        }
        print(name, json.dumps(rows, ensure_ascii=False), flush=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
