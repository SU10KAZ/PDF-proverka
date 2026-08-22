#!/usr/bin/env python3
"""TCF probe 5 — how wrong is `nested_contours`?

`extractor._topology` counts an inner contour as nested when SOME other closed
primitive's bbox strictly contains its bbox, scanning only the first 1000 closed
primitives and breaking at the first container found.  This probe measures, per block:

  * whether the 1000-primitive limit fires at all in this corpus;
  * how many distinct containers the count actually rests on, and the share of
    nested contours whose first container is the single largest closed contour;
  * how many bbox-containments are false when the inner centroid is tested against
    the container's real polygon (point in polygon);
  * proper nesting depth (depth >= 1, >= 2, >= 3) instead of a Boolean per contour;
  * whether the largest closed contour is simply the block frame.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p5_nesting
"""
from __future__ import annotations

import json
import pathlib

from experiments.stage_comparison_vector_blocks.extractor import _point_in_polygon

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p5_nesting.json")


def polygon_of(primitive):
    pts = []
    for a, b in primitive["normalized"]["segments"]:
        pts.append(tuple(a))
    if pts:
        pts.append(tuple(primitive["normalized"]["segments"][-1][1]))
    return pts


def main() -> None:
    rows = {}
    for pair_dir in sorted(ROOT.iterdir()):
        for side in ("left",):
            d = json.loads((pair_dir / side / "vector_block.json").read_text(encoding="utf-8"))
            closed = [p for p in d["geometry"]["primitives"] if p["closed"]]
            n = len(closed)
            boxes = [p["normalized"]["bbox"] for p in closed]
            areas = [(b[2] - b[0]) * (b[3] - b[1]) for b in boxes]
            largest = max(range(n), key=lambda i: areas[i]) if n else None
            container_of = {}
            nested = 0
            for i in range(min(n, 1000)):
                a = boxes[i]
                for j in range(min(n, 1000)):
                    if i == j:
                        continue
                    b = boxes[j]
                    if b[0] < a[0] and b[1] < a[1] and b[2] > a[2] and b[3] > a[3]:
                        nested += 1
                        container_of[i] = j
                        break
            # geometric verification of the container that Track A actually picked
            false_bbox = 0
            for i, j in container_of.items():
                a = boxes[i]
                centroid = ((a[0] + a[2]) / 2, (a[1] + a[3]) / 2)
                if not _point_in_polygon(centroid, polygon_of(closed[j])):
                    false_bbox += 1
            # proper depth by bbox containment
            depth = []
            for i in range(min(n, 1000)):
                a = boxes[i]
                d_i = sum(
                    1
                    for j in range(min(n, 1000))
                    if j != i
                    and boxes[j][0] < a[0]
                    and boxes[j][1] < a[1]
                    and boxes[j][2] > a[2]
                    and boxes[j][3] > a[3]
                )
                depth.append(d_i)
            frame_like = False
            if largest is not None:
                b = boxes[largest]
                frame_like = (b[2] - b[0]) > 0.9 and (b[3] - b[1]) > 0.9
            counts = {}
            for j in container_of.values():
                counts[j] = counts.get(j, 0) + 1
            top_container_share = round(max(counts.values()) / max(1, len(container_of)), 4) if counts else None
            rows[pair_dir.name] = {
                "closed_contours": n,
                "limit_1000_fires": n > 1000,
                "nested_track_a": nested,
                "nested_share": round(nested / n, 4) if n else None,
                "distinct_first_containers": len(counts),
                "top_container_share_of_nested": top_container_share,
                "top_container_is_largest": bool(counts and max(counts, key=counts.get) == largest),
                "largest_contour_is_block_frame": frame_like,
                "false_containment_by_polygon_test": false_bbox,
                "false_containment_share": round(false_bbox / max(1, len(container_of)), 4),
                "contours_depth_ge1": sum(1 for x in depth if x >= 1),
                "contours_depth_ge2": sum(1 for x in depth if x >= 2),
                "contours_depth_ge3": sum(1 for x in depth if x >= 3),
                "max_containers_over_one_contour": max(depth) if depth else 0,
            }
            print(pair_dir.name, rows[pair_dir.name], flush=True)
    OUT.write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
