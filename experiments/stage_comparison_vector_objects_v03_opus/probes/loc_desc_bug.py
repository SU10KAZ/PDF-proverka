# -*- coding: utf-8 -*-
"""DEFECT-3: the 25-number shape descriptor of `v03_objects` is not stable under a pure
translation, because the 4x4 occupancy grid quantises on a knife edge.

Method, no synthetic geometry: inside one real block, find groups of objects whose INK is
exactly congruent (identical segment multiset after translating to the origin — repeated
symbols are everywhere in CAD).  Any descriptor worth its name must give distance 0 inside
such a group.  Measured: it often does not.

    python probes/loc_desc_bug.py [n_blocks]
"""
from __future__ import annotations
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G      # noqa: E402
import v03_objects as O     # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402


def congruent_groups(ex, ol):
    segs_of = defaultdict(list)
    for i, oi in enumerate(ol.seg2obj):
        if oi >= 0:
            segs_of[oi].append(i)
    keys = {}
    for oi, ix in segs_of.items():
        if len(ix) < 2:
            continue
        pts = sorted((round(ex.segments[i]["p0"][0], 3), round(ex.segments[i]["p0"][1], 3),
                      round(ex.segments[i]["p1"][0], 3), round(ex.segments[i]["p1"][1], 3))
                     for i in ix)
        x0, y0 = pts[0][0], pts[0][1]
        norm = tuple(sorted((round(a - x0, 3), round(b - y0, 3),
                             round(c - x0, 3), round(d - y0, 3)) for a, b, c, d in pts))
        keys.setdefault(norm, []).append(oi)
    return {k: v for k, v in keys.items() if len(v) > 1}


def main():
    n_blocks = int(sys.argv[1]) if len(sys.argv) > 1 else 40
    rows = []
    for r in pick_carriers():
        if len(rows) >= n_blocks:
            break
        if r["bucket"] not in ("sparse", "medium", "dense"):
            continue
        try:
            pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
            ex = G.extract(pb)
            if not ex.segments or len(ex.segments) > 20000:
                continue
            ol = O.build_objects(ex)
        except Exception as e:
            print("skip", r["block_id"], repr(e), flush=True)
            continue
        groups = congruent_groups(ex, ol)
        d_all, ids_lost = [], 0
        n_pairs = 0
        for _k, members in groups.items():
            for i in range(len(members)):
                for j in range(i + 1, len(members)):
                    a, b = ol.objects[members[i]], ol.objects[members[j]]
                    d = O.descriptor_distance(a["desc"], b["desc"])
                    d_all.append(d)
                    n_pairs += 1
                    if a["object_id"] != b["object_id"]:
                        ids_lost += 1
        rows.append({"block_id": r["block_id"], "discipline": r["discipline"],
                     "n_seg": len(ex.segments), "n_obj": len(ol.objects),
                     "n_congruent_groups": len(groups), "n_pairs": n_pairs,
                     "n_pairs_desc_nonzero": sum(1 for d in d_all if d > 1e-9),
                     "n_pairs_d_gt_0.01": sum(1 for d in d_all if d > 0.01),
                     "n_pairs_d_gt_0.05": sum(1 for d in d_all if d > 0.05),
                     "n_pairs_d_gt_0.30": sum(1 for d in d_all if d > 0.30),
                     "median_d": round(statistics.median(d_all), 6) if d_all else None,
                     "max_d": round(max(d_all), 6) if d_all else None})
        print(rows[-1], flush=True)
    P = [x for x in rows if x["n_pairs"]]
    allp = sum(x["n_pairs"] for x in P)
    nz = sum(x["n_pairs_desc_nonzero"] for x in P)
    out = {"note": "congruent = identical segment multiset after translation to origin; "
                   "a translation-invariant descriptor must give distance 0",
           "n_blocks_measured": len(rows), "n_blocks_with_congruent_pairs": len(P),
           "n_congruent_pairs": allp,
           "share_pairs_with_nonzero_descriptor_distance": round(nz / max(allp, 1), 4),
           "share_pairs_d_gt_0.01": round(sum(x["n_pairs_d_gt_0.01"] for x in P) / max(allp, 1), 4),
           "share_pairs_d_gt_0.05": round(sum(x["n_pairs_d_gt_0.05"] for x in P) / max(allp, 1), 4),
           "share_pairs_d_gt_0.30": round(sum(x["n_pairs_d_gt_0.30"] for x in P) / max(allp, 1), 4),
           "median_nonzero_distance": round(statistics.median(
               [x["median_d"] for x in P if x["median_d"] and x["median_d"] > 0] or [0]), 6),
           "max_distance": max([x["max_d"] for x in P if x["max_d"] is not None] or [0]),
           "per_block": rows}
    json.dump(out, open(ART / "loc_desc_bug.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print({k: v for k, v in out.items() if k != "per_block"})


if __name__ == "__main__":
    main()
