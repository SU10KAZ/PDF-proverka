#!/usr/bin/env python3
"""relgraph_control -- Track-B control: how much of the eom_singleline "signal"
is just a mismatched normalization frame?

The eom pair is the only benchmark pair with a real geometric design change, but
its two block rects have different aspect ratios (see printout). This script
distorts the LEFT block by exactly that aspect factor -- content byte-identical --
and measures what each metric reports. Anything at or below the real pair's score
means that metric cannot attribute the difference to the design change.

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_control.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import relgraph_core as R  # noqa: E402
from relgraph_crop import coverage, renormalize, text_multiset  # noqa: E402
from relgraph_granularity import project  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"


def load(pid, side):
    return json.loads((A / "descriptions" / pid / side / "vector_block.json").read_text())


def frame_stats(desc):
    b = desc["bbox"]
    w, h = b[2] - b[0], b[3] - b[1]
    return {"w": round(w, 2), "h": round(h, 2), "aspect": round(w / h, 5)}


def main() -> None:
    out = {"research_only": True, "controls": []}
    for pid in ("eom_singleline_changed", "ss_scheme_text_changed", "vk_nodes"):
        l, r = load(pid, "left"), load(pid, "right")
        fl, fr = frame_stats(l), frame_stats(r)
        distortion = fr["aspect"] / fl["aspect"]
        gl, gr = R.build_relation_graph(l), R.build_relation_graph(r)
        real = {
            "segment_coverage": coverage(l, r),
            "rel_G3": round(R.weighted_jaccard(gl["relations"], gr["relations"]), 6),
            "rel_G1": round(R.weighted_jaccard(project(gl["relations"], 1),
                                               project(gr["relations"], 1)), 6),
            "rel_G0": round(R.weighted_jaccard(project(gl["relations"], 0),
                                               project(gr["relations"], 0)), 6),
            "entity": round(R.weighted_jaccard(gl["entities"], gr["entities"]), 6),
            "text": round(R.weighted_jaccard(text_multiset(l), text_multiset(r)), 6),
        }
        # control: identical content, LEFT frame stretched by the observed aspect factor
        b = l["bbox"]
        w, h = b[2] - b[0], b[3] - b[1]
        ctrl_rect = [b[0], b[1], b[0] + w * distortion, b[1] + h]
        lc = renormalize(l, ctrl_rect)
        glc = R.build_relation_graph(lc)
        ctrl = {
            "segment_coverage": coverage(l, lc),
            "rel_G3": round(R.weighted_jaccard(gl["relations"], glc["relations"]), 6),
            "rel_G1": round(R.weighted_jaccard(project(gl["relations"], 1),
                                               project(glc["relations"], 1)), 6),
            "rel_G0": round(R.weighted_jaccard(project(gl["relations"], 0),
                                               project(glc["relations"], 0)), 6),
            "entity": round(R.weighted_jaccard(gl["entities"], glc["entities"]), 6),
            "text": 1.0,
        }
        row = {"pair_id": pid, "left_frame": fl, "right_frame": fr,
               "aspect_distortion": round(distortion, 5),
               "left_text_rotation_hist": _rot(l), "right_text_rotation_hist": _rot(r),
               "real_pair": real, "aspect_control_same_content": ctrl,
               "verdict": {
                   k: ("CONFOUNDED (control <= real)" if _v(ctrl, k) <= _v(real, k)
                       else "signal above frame noise")
                   for k in ("segment_coverage@0.01", "segment_coverage@0.005",
                             "rel_G3", "rel_G1", "rel_G0", "entity")}}
        out["controls"].append(row)
        print(f"\n=== {pid}  aspect {fl['aspect']} vs {fr['aspect']}  distortion x{distortion:.4f}")
        print(f"    rotation hist L={row['left_text_rotation_hist']} R={row['right_text_rotation_hist']}")
        for k in ("segment_coverage@0.005", "segment_coverage@0.01", "rel_G3", "rel_G1",
                  "rel_G0", "entity", "text"):
            print(f"    {k:26s} real_pair={_v(real,k):.4f}   aspect_control={_v(ctrl,k):.4f}"
                  f"   {'<-- CONTROL <= REAL: confounded' if _v(ctrl,k) <= _v(real,k) else ''}")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "relgraph_frame_control.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
    print("\nwrote", OUT / "relgraph_frame_control.json")


def _rot(desc):
    import collections
    return dict(collections.Counter(t["rotation"] for t in desc["texts"]))


def _v(d, key):
    if key.startswith("segment_coverage@"):
        return d["segment_coverage"]["tol_" + key.split("@")[1]]
    if key == "segment_coverage":
        return d["segment_coverage"]["tol_0.01"]
    return d[key]


if __name__ == "__main__":
    main()
