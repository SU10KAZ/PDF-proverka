#!/usr/bin/env python3
"""TCF probe 4b — the realistic "two versions flatten the same circle differently" case.

`extractor._drawing_primitives` has two mutually exclusive routes for a circle:
  * `_ellipse_kind` fires only when EVERY item of the path is a cubic ('c'); the
    circle is then replaced by a 24-gon sampled at equal angles of the path *rect*;
  * otherwise each cubic is flattened on its own with CURVE_STEPS = 6 samples,
    i.e. equal Bezier parameter steps, which are NOT equal angles.

A circle drawn as 4 cubics takes route 1.  The same circle drawn as 4 cubics plus a
closing line, or as 2 cubics plus 2 lines, or with a different start quadrant takes
route 2.  Both routes emit 24 segments, so the repeated-element fingerprint cannot
tell them apart, but the geometry differs.  This probe measures both effects.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p4b_bezier
"""
from __future__ import annotations

import json
import math
import pathlib
import collections

from experiments.stage_comparison_vector_blocks import extractor as ex
from experiments.stage_comparison_vector_blocks.comparator import _segment_coverage_runs

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p4b_bezier.json")
K = 0.5522847498307936
TOLERANCES = (0.001, 0.0025, 0.005, 0.01)


def quarter_beziers(cx, cy, r, start_quadrant=0):
    out = []
    for q in range(4):
        a0 = math.pi / 2 * (q + start_quadrant)
        a1 = a0 + math.pi / 2
        p0 = (cx + r * math.cos(a0), cy + r * math.sin(a0))
        p3 = (cx + r * math.cos(a1), cy + r * math.sin(a1))
        t0 = (-math.sin(a0), math.cos(a0))
        t1 = (-math.sin(a1), math.cos(a1))
        p1 = (p0[0] + K * r * t0[0], p0[1] + K * r * t0[1])
        p2 = (p3[0] - K * r * t1[0], p3[1] - K * r * t1[1])
        out.append((p0, p1, p2, p3))
    return out


def cubic_samples(control, steps):
    p0, p1, p2, p3 = control
    pts = []
    for i in range(steps + 1):
        t = i / steps
        u = 1 - t
        pts.append(
            (
                u ** 3 * p0[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t ** 3 * p3[0],
                u ** 3 * p0[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t ** 3 * p3[1],
            )
        )
    return pts


def as_primitive(points, pid, closed=True):
    segs = [[list(points[i]), list(points[i + 1])] for i in range(len(points) - 1)]
    if closed and ex._distance(points[0], points[-1]) > 1e-9:
        segs.append([list(points[-1]), list(points[0])])
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return {
        "id": pid,
        "type": "circle",
        "normalized": {"segments": segs, "bbox": [min(xs), min(ys), max(xs), max(ys)]},
        "raw": {"segments": segs, "bbox": [min(xs), min(ys), max(xs), max(ys)]},
        "length_norm": round(sum(ex._distance(*s) for s in segs), 6),
        "angle_degrees": 0.0,
        "segment_count": len(segs),
        "closed": True,
        "style": {},
    }


def route1(cx, cy, r):
    pts = [
        (cx + r * math.cos(2 * math.pi * i / 24), cy + r * math.sin(2 * math.pi * i / 24))
        for i in range(24)
    ]
    return as_primitive(pts, "primitive-1")


def route2(cx, cy, r, start_quadrant=0):
    pts = []
    for control in quarter_beziers(cx, cy, r, start_quadrant):
        samples = cubic_samples(control, ex.CURVE_STEPS)
        pts.extend(samples if not pts else samples[1:])
    return as_primitive(pts[:-1] if ex._distance(pts[0], pts[-1]) < 1e-9 else pts, "primitive-1")


def main() -> None:
    result = {"probe": "tcf_p4b_bezier", "curve_steps": ex.CURVE_STEPS, "cases": {}}
    for r in (0.01, 0.03, 0.08):
        a = route1(0.5, 0.5, r)
        b = route2(0.5, 0.5, r)
        c = route2(0.5, 0.5, r, start_quadrant=0.5)  # exporter starts the circle elsewhere
        result["cases"][f"radius_{r}"] = {
            "segments_route1_24gon": a["segment_count"],
            "segments_route2_bezier": b["segment_count"],
            "route1_vs_route2": {
                str(run["tolerance"]): run["similarity"]
                for run in _segment_coverage_runs([a], [b], TOLERANCES)
            },
            "route2_vs_route2_rotated_start": {
                str(run["tolerance"]): run["similarity"]
                for run in _segment_coverage_runs([b], [c], TOLERANCES)
            },
            "route1_perimeter": a["length_norm"],
            "route2_perimeter": b["length_norm"],
            "true_circumference": round(2 * math.pi * r, 6),
        }
    census = {}
    for pair_dir in sorted(ROOT.iterdir()):
        d = json.loads((pair_dir / "left" / "vector_block.json").read_text(encoding="utf-8"))
        types = collections.Counter(p["type"] for p in d["geometry"]["primitives"])
        census[pair_dir.name] = {
            "source_item_counts": d["geometry"]["extraction"]["source_item_counts"],
            "primitive_types": dict(types),
        }
    result["corpus_census"] = census
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    for k, v in result["cases"].items():
        print(k, json.dumps({x: v[x] for x in ("route1_vs_route2", "route2_vs_route2_rotated_start",
                                               "route1_perimeter", "route2_perimeter",
                                               "true_circumference")}, ensure_ascii=False))
    print("\ncensus:")
    for k, v in census.items():
        print(" ", k, v["source_item_counts"], v["primitive_types"])


if __name__ == "__main__":
    main()
