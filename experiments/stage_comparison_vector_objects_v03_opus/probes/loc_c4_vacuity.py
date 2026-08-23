# -*- coding: utf-8 -*-
"""Are the C4 'swap two like objects' counterfactuals real changes at all?

If the two swapped objects are exactly congruent (identical ink up to a translation),
swapping them leaves the DRAWING untouched: the union of ink is the same set of strokes.
Then a comparator that says 'no graphic change' is right and the ground truth is vacuous.
Measured for every C4 instance, not assumed.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402


def norm(segs):
    pts = sorted((round(s["p0"][0], 3), round(s["p0"][1], 3),
                  round(s["p1"][0], 3), round(s["p1"][1], 3)) for s in segs)
    if not pts:
        return ()
    x0, y0 = pts[0][0], pts[0][1]
    return tuple(sorted((round(a - x0, 3), round(b - y0, 3),
                         round(c - x0, 3), round(d - y0, 3)) for a, b, c, d in pts))


def main():
    out = []
    for r in pick_carriers():
        try:
            pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
            ex = G.extract(pb)
            if not ex.segments:
                continue
            ol = O.build_objects(ex)
            _ex2, man = C.apply(ex, ol, "C4_swap_objects")
        except Exception as e:
            out.append({"block_id": r["block_id"], "skip": repr(e)})
            continue
        ids = [o["object_id"] for o in man["touched_objects"]]
        idx = [i for i, o in enumerate(ol.objects) if o["object_id"] in ids]
        if len(idx) != 2:
            out.append({"block_id": r["block_id"], "skip": "object ids not resolvable"})
            continue
        segs = {k: [ex.segments[i] for i, z in enumerate(ol.seg2obj) if z == k] for k in idx}
        a, b = ol.objects[idx[0]], ol.objects[idx[1]]
        congruent = norm(segs[idx[0]]) == norm(segs[idx[1]])
        out.append({"block_id": r["block_id"], "discipline": r["discipline"],
                    "bucket": r["bucket"],
                    "object_ids": ids, "n_seg": [a["n_seg"], b["n_seg"]],
                    "seg_len": [round(a["seg_len"], 3), round(b["seg_len"], 3)],
                    "descriptor_distance": round(O.descriptor_distance(a["desc"], b["desc"]), 6),
                    "ink_congruent": congruent})
        print(out[-1], flush=True)
    ok = [x for x in out if "ink_congruent" in x]
    summary = {"n_carriers": len(out), "n_measured": len(ok),
               "n_ink_congruent": sum(1 for x in ok if x["ink_congruent"]),
               "share_ink_congruent": round(sum(1 for x in ok if x["ink_congruent"]) /
                                            max(len(ok), 1), 4),
               "note": "a congruent swap is a NO-OP on the drawing: the ledger is right to "
                       "stay silent and the counterfactual's ground truth is vacuous"}
    json.dump({"summary": summary, "rows": out},
              open(ART / "loc_c4_vacuity.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(summary)


if __name__ == "__main__":
    main()
