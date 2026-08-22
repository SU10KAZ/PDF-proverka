#!/usr/bin/env python3
"""TCF probe 2b — does the topology cap mutilate objects, and are round shapes
encoded with a stable vertex count?

A. For every capped block: how many primitives survive the "keep the 8000 longest
   segments" selection whole, how many survive with only part of their segments
   (a mutilated object in the graph), and how many disappear completely.
B. Corpus census of near-circular closed polylines by vertex count: if the same
   physical circle can appear with different vertex counts in two versions, the
   segment-coverage comparator scores it 0 (see probe 4).

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p2b_capobjects
"""
from __future__ import annotations

import collections
import json
import math
import pathlib

from experiments.stage_comparison_vector_blocks.extractor import _all_segments

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p2b_capobjects.json")
CAP = 8_000


def main() -> None:
    payload = {"probe": "tcf_p2b_capobjects", "cap": CAP, "A_object_mutilation": {}, "B_round_shape_census": {}}
    for pair_dir in sorted(ROOT.iterdir()):
        d = json.loads((pair_dir / "left" / "vector_block.json").read_text(encoding="utf-8"))
        primitives = d["geometry"]["primitives"]
        segments = _all_segments(primitives)
        per_primitive = collections.Counter(s["primitive_id"] for s in segments)
        if len(segments) > CAP:
            kept = sorted(segments, key=lambda item: item["length"], reverse=True)[:CAP]
            kept_per = collections.Counter(s["primitive_id"] for s in kept)
            whole = sum(1 for pid, n in per_primitive.items() if kept_per.get(pid, 0) == n)
            partial = sum(1 for pid, n in per_primitive.items() if 0 < kept_per.get(pid, 0) < n)
            gone = sum(1 for pid, n in per_primitive.items() if kept_per.get(pid, 0) == 0)
            payload["A_object_mutilation"][pair_dir.name] = {
                "primitives_with_segments": len(per_primitive),
                "fully_retained": whole,
                "partially_retained_mutilated": partial,
                "fully_dropped": gone,
                "mutilated_share": round(partial / len(per_primitive), 4),
            }
        # B: closed primitives that look like a circle
        counts = collections.Counter()
        for p in primitives:
            if not p["closed"]:
                continue
            b = p["raw"]["bbox"]
            w, h = b[2] - b[0], b[3] - b[1]
            if w <= 0 or h <= 0 or abs(w - h) / max(w, h) > 0.12:
                continue
            cx, cy = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
            r = (w + h) / 4
            pts = [seg[0] for seg in p["raw"]["segments"]]
            if len(pts) < 6:
                continue
            errors = [abs(math.hypot(x - cx, y - cy) - r) / r for x, y in pts]
            if max(errors) <= 0.08:
                counts[p["segment_count"]] += 1
        if counts:
            payload["B_round_shape_census"][pair_dir.name] = dict(sorted(counts.items()))
        aspects = []
        for p in primitives:
            if p["type"] in {"circle", "ellipse"}:
                nb = p["normalized"]["bbox"]
                w, h = nb[2] - nb[0], nb[3] - nb[1]
                if w > 0 and h > 0:
                    aspects.append(round(w / h, 3))
        if aspects:
            payload.setdefault("C_circle_aspect_after_normalization", {})[pair_dir.name] = {
                "circles": len(aspects),
                "min_aspect": min(aspects),
                "max_aspect": max(aspects),
            }
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print("A object mutilation under the 8000-segment cap:")
    for k, v in payload["A_object_mutilation"].items():
        print(" ", k, v)
    print("\nB near-circular closed shapes by vertex count (raw page coords):")
    for k, v in payload["B_round_shape_census"].items():
        print(" ", k, v)
    print("\nC aspect of circles AFTER block normalization (1.0 would mean a circle stays a circle):")
    for k, v in payload.get("C_circle_aspect_after_normalization", {}).items():
        print(" ", k, v)


if __name__ == "__main__":
    main()
