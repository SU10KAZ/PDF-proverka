# -*- coding: utf-8 -*-
"""Why does swapping two 'like' objects sometimes move no ink at all?

For every C4 instance the ledger did NOT localise, measure how far the two swapped
objects actually are from being the same shape: translate one onto the other by the
centroid offset and take the worst endpoint-to-endpoint distance in both directions.
If that number is below the matching tolerance (0.8 pt), the swap is invisible to ink
matching BY CONSTRUCTION, and the miss is a property of the tolerance, not of the design.
"""
from __future__ import annotations
import ast
import json
import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402
from loc_agg2 import merged     # noqa: E402


def haus(A, B, dx, dy):
    """Worst nearest-neighbour distance between the endpoint clouds, both directions."""
    pa = [(x + dx, y + dy) for s in A for (x, y) in (s["p0"], s["p1"])]
    pb = [(x, y) for s in B for (x, y) in (s["p0"], s["p1"])]
    if not pa or not pb:
        return None
    def one(P, Q):
        return max(min(math.hypot(px - qx, py - qy) for qx, qy in Q) for px, py in P)
    return max(one(pa, pb), one(pb, pa))


def main():
    R, _ = merged()
    miss = sorted({r["block_id"] for r in R if r.get("cf_id") == "C4_swap_objects"
                   and r["noise"] == "none" and not r["score"]["L2_localised"]})
    hit = sorted({r["block_id"] for r in R if r.get("cf_id") == "C4_swap_objects"
                  and r["noise"] == "none" and r["score"]["L2_localised"]})
    carriers = {r["block_id"]: r for r in pick_carriers()}
    out = []
    for group, bids in (("miss", miss), ("hit", hit[:20])):
        for bid in bids:
            r = carriers[bid]
            try:
                pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
                ex = G.extract(pb)
                if not ex.segments or len(ex.segments) > 60000:
                    out.append({"group": group, "block_id": bid, "skip": "too heavy"})
                    continue
                ol = O.build_objects(ex)
                _e2, man = C.apply(ex, ol, "C4_swap_objects")
            except Exception as e:
                out.append({"group": group, "block_id": bid, "skip": repr(e)})
                continue
            ids = [o["object_id"] for o in man["touched_objects"]]
            idx = [i for i, o in enumerate(ol.objects) if o["object_id"] in ids]
            if len(idx) != 2:
                out.append({"group": group, "block_id": bid, "skip": "ids"})
                continue
            A = [ex.segments[i] for i, z in enumerate(ol.seg2obj) if z == idx[0]]
            B = [ex.segments[i] for i, z in enumerate(ol.seg2obj) if z == idx[1]]
            a, b = ol.objects[idx[0]], ol.objects[idx[1]]
            d = haus(A, B, b["cx"] - a["cx"], b["cy"] - a["cy"])
            out.append({"group": group, "block_id": bid, "discipline": r["discipline"],
                        "n_seg": [a["n_seg"], b["n_seg"]],
                        "shape_mismatch_pt": round(d, 4) if d is not None else None,
                        "tol_pt": L.DEF["tol_pt"],
                        "below_tolerance": bool(d is not None and d <= L.DEF["tol_pt"])})
            print(out[-1], flush=True)
    ms = [x for x in out if x.get("group") == "miss" and "shape_mismatch_pt" in x]
    hs = [x for x in out if x.get("group") == "hit" and "shape_mismatch_pt" in x]
    summary = {
        "n_miss_measured": len(ms),
        "miss_share_below_tolerance": round(sum(1 for x in ms if x["below_tolerance"]) /
                                            max(len(ms), 1), 4),
        "miss_median_mismatch_pt": sorted(x["shape_mismatch_pt"] for x in ms)[len(ms) // 2]
        if ms else None,
        "n_hit_measured": len(hs),
        "hit_share_below_tolerance": round(sum(1 for x in hs if x["below_tolerance"]) /
                                           max(len(hs), 1), 4),
        "hit_median_mismatch_pt": sorted(x["shape_mismatch_pt"] for x in hs)[len(hs) // 2]
        if hs else None,
    }
    json.dump({"summary": summary, "rows": out},
              open(ART / "loc_c4_tolerance.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(summary)


if __name__ == "__main__":
    main()
