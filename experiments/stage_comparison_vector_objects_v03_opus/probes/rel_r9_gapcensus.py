# -*- coding: utf-8 -*-
"""R9 — how FAR apart are the objects each type calls related?

ALIGNED never produced a visually false tile: sharing a centre coordinate within a
tolerance is true by construction.  That makes the false rate the wrong question for it.
The right question for an ADDRESS is distance: "the object aligned with X" is useless if
X is half a sheet away.  So the census reports, per type, the distance between the two
objects in POINTS and in units of the block's characteristic scale S.

Usage: rel_r9_gapcensus.py [shard nshards]
"""
from __future__ import annotations
import json, math, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import rel_common as C
import rel_relations as R

N_BLOCKS = 80


def bbox_gap(a, b):
    dx = max(a[0] - b[2], b[0] - a[2], 0.0)
    dy = max(a[1] - b[3], b[1] - a[3], 0.0)
    return math.hypot(dx, dy)


def one(rec):
    row = {"rec": rec, "error": None}
    try:
        pb = C.G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            row["error"] = "no block"; return row
        ex = C.G.extract(pb)
        if len(ex.segments) < 40:
            row["error"] = "few segments"; return row
        L = C.O.build_objects(ex)
        rels = R.build_relations(L, ex)
        S = L.S or 1.0
        fr = ex.frame["clip_display"]
        diag = math.hypot(fr[2] - fr[0], fr[3] - fr[1])
        out = {}
        for r in rels:
            t = r["type"]
            if t == "LABEL_ANCHOR":
                g = r.get("gap_pt", 0.0)
            elif r.get("b") is None:
                continue
            else:
                g = bbox_gap(L.objects[r["a"]]["bbox"], L.objects[r["b"]]["bbox"])
            out.setdefault(t, []).append(round(g, 3))
        row["S"] = round(S, 4)
        row["block_diag_pt"] = round(diag, 2)
        row["gaps"] = {t: {"n": len(v), "med": sorted(v)[len(v) // 2],
                           "p90": sorted(v)[int(0.9 * (len(v) - 1))], "max": max(v),
                           "share_gt_10S": round(sum(1 for x in v if x > 10 * S) / len(v), 4),
                           "share_gt_0.1diag": round(sum(1 for x in v if x > 0.1 * diag) / len(v), 4)}
                       for t, v in out.items()}
    except Exception:
        row["error"] = traceback.format_exc()[-300:]
    return row


def main(shard, n):
    smp = json.load(open(C.ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    pool = [r for r in smp if 60 <= r["n_seg"] <= 25000][:N_BLOCKS]
    mine = [r for i, r in enumerate(pool) if i % n == shard]
    out = []
    for k, rec in enumerate(mine):
        r = one(rec)
        print(f"[{shard}] {k+1}/{len(mine)} {rec['block_id']} err={r['error']}", flush=True)
        out.append(r)
        C.F.clear_caches()
    json.dump(out, open(C.ART / f"rel_r9_{shard}.json", "w", encoding="utf-8"),
              ensure_ascii=False)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 0,
         int(sys.argv[2]) if len(sys.argv) > 2 else 1)
