"""Probe `obj` — object-level change rate vs segment-level change rate, all 10 pairs.

For each benchmark pair: build object candidates on both sides, match them across
versions by (normalised position, decomposition-insensitive shape descriptor), and
report added/removed.  Compare that change rate against the raw segment-count change
rate and against Track A's `geometry_similarity`.

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/obj_pairdiff.py
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
ROOT = HERE.parent.parent.parent
ART = HERE.parent / "artifacts"
import obj_poc as P  # noqa: E402

PAIRS = json.loads((ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json").read_text())["pairs"]
TRACK_A = {p["pair_id"]: p for p in json.loads(
    (ROOT / "experiments/stage_comparison_vector_blocks/artifacts/benchmark_results.json").read_text())["pairs"]}

POS_TOL = 0.03     # normalised block coordinates
SHAPE_EPS = 0.15   # L1 distance between descriptors


def side(entry):
    blk = P.extract_segments(str(ROOT / entry["pdf"]), entry["page_index"], entry["bbox_norm"])
    res = P.group_objects(blk)
    segs = blk["segments"]
    d = blk["disp_rect"]
    w = max(d[2] - d[0], 1e-6)
    h = max(d[3] - d[1], 1e-6)
    objs = []
    for i, o in enumerate(res["objects"]):
        if o["class"] not in ("symbol_candidate", "closed_area_object", "dense_region"):
            continue
        o["oid"] = i
        o["desc"] = P.shape_descriptor(o["members"], segs)
        bb = o["bbox"]
        o["cx"] = ((bb[0] + bb[2]) / 2 - d[0]) / w
        o["cy"] = ((bb[1] + bb[3]) / 2 - d[1]) / h
        objs.append(o)
    return blk, res, objs


def match(ol, orr):
    cell = POS_TOL
    grid = {}
    for o in orr:
        grid.setdefault((int(o["cx"] // cell), int(o["cy"] // cell)), []).append(o)
    cand = []
    for a in ol:
        i, j = int(a["cx"] // cell), int(a["cy"] // cell)
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                for b in grid.get((i + di, j + dj), ()):
                    if abs(a["cx"] - b["cx"]) > POS_TOL or abs(a["cy"] - b["cy"]) > POS_TOL:
                        continue
                    dist = P.descriptor_distance(a["desc"], b["desc"])
                    if dist <= SHAPE_EPS:
                        pos = math.hypot(a["cx"] - b["cx"], a["cy"] - b["cy"])
                        cand.append((dist + pos, a["oid"], b["oid"]))
    cand.sort()
    ua, ub, n = set(), set(), 0
    for _d, a, b in cand:
        if a in ua or b in ub:
            continue
        ua.add(a)
        ub.add(b)
        n += 1
    return n, len(ol) - n, len(orr) - n


def main():
    out = {"probe": "obj", "research_only": True,
           "params": {"pos_tol_norm": POS_TOL, "shape_eps_L1": SHAPE_EPS},
           "pairs": {}}
    for pr in PAIRS:
        bl, rl, ol = side(pr["left"])
        br, rr, orr = side(pr["right"])
        n, rem, add = match(ol, orr)
        segs_l, segs_r = len(bl["segments"]), len(br["segments"])
        row = {
            "objects_left": len(ol), "objects_right": len(orr),
            "matched": n, "removed": rem, "added": add,
            "object_change_rate": round((rem + add) / max(1, len(ol) + len(orr)), 4),
            "segments_left": segs_l, "segments_right": segs_r,
            "segment_count_change_rate": round(abs(segs_l - segs_r) / max(1, max(segs_l, segs_r)), 4),
            "trackA_geometry_similarity": TRACK_A[pr["pair_id"]]["geometry_similarity"],
            "trackA_status": TRACK_A[pr["pair_id"]]["status"],
            "human_expected": pr["human_expected"],
        }
        out["pairs"][pr["pair_id"]] = row
        print(f"{pr['pair_id']:24s} obj {len(ol):5d}/{len(orr):5d} matched {n:5d} "
              f"+{add:4d} -{rem:4d} rate {row['object_change_rate']:.4f} | "
              f"seg rate {row['segment_count_change_rate']:.4f} | trackA geom "
              f"{row['trackA_geometry_similarity']:.4f}", flush=True)
    (ART / "obj_pair_object_diff.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
    print("wrote", ART / "obj_pair_object_diff.json")


if __name__ == "__main__":
    main()
