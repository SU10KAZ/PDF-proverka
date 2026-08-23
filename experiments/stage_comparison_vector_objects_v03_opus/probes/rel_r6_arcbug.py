# -*- coding: utf-8 -*-
"""R6 — the RECTANGLE->CIRCLE defect of the object layer, and what it costs relations.

Found while grading R4 by eye: in PART_OF tiles the inner object was drawn OUTSIDE the
container.  The container's own segments are a rectangle, but the layer had replaced the
chain by its CIRCUMSCRIBED CIRCLE, so the object's bbox (and therefore every bbox-based
relation) was inflated.

Mechanism (v03_objects._build_primitives): a closed chain of a rectangle has 5 points;
the four corners of ANY rectangle are concyclic exactly, so ``_fit_circle`` returns
residual 0, ``arc_min_pts=4`` lets it through, the span is 360 deg and the chain becomes
a canonical circle of radius = the circumradius.

The guard needs no code change: a rectangle chain has 5 points, a circle written as five
chords (A4b) has 6, so ``arc_min_pts=6`` separates them exactly.

Measures, per block:
  * share of objects whose bbox is inflated by the arc closure, and by how much;
  * share of INSIDE / CONTAINS / PART_OF / ADJACENT relations that exist ONLY under the
    defective layer (they disappear when the guard is on);
  * relation counts both ways.

Usage: rel_r6_arcbug.py [shard nshards]
"""
from __future__ import annotations
import json, math, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import rel_common as C
import rel_relations as R

GUARD = {"arc_min_pts": 6}
BBOX_TYPES = ("INSIDE", "CONTAINS", "PART_OF", "ADJACENT")


def seg_bbox(ex, seglist):
    xs0 = ys0 = 1e18; xs1 = ys1 = -1e18
    for g in seglist:
        s = ex.segments[g]
        for p in (s["p0"], s["p1"]):
            xs0 = min(xs0, p[0]); ys0 = min(ys0, p[1])
            xs1 = max(xs1, p[0]); ys1 = max(ys1, p[1])
    return [xs0, ys0, xs1, ys1]


def area(b):
    return max(b[2] - b[0], 0.0) * max(b[3] - b[1], 0.0)


def one(rec):
    row = {"rec": rec, "error": None}
    try:
        pb = C.G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            row["error"] = "no block"; return row
        ex = C.G.extract(pb)
        if len(ex.segments) < 40:
            row["error"] = "too few segments"; return row
        L = C.O.build_objects(ex)
        Lg = C.O.build_objects(ex, **GUARD)
        rel = R.build_relations(L, ex)
        relg = R.build_relations(Lg, ex)

        # --- how many objects have an inflated bbox, and by how much -----------------
        infl = []
        n_rect_circle = 0
        for o in L.objects:
            tb = seg_bbox(ex, o["segments"])
            a_true, a_lay = area(tb), area(o["bbox"])
            if a_true > 1e-9:
                r = a_lay / a_true
                if r > 1.05:
                    infl.append(round(r, 4))
        for pr in L.prims:
            if pr["kind"] == "arc" and pr.get("closed") and len(pr["members"]) <= 4:
                n_rect_circle += 1
        row.update({
            "n_seg": len(ex.segments), "n_obj": len(L.objects), "n_obj_guard": len(Lg.objects),
            "n_prim": len(L.prims), "n_prim_guard": len(Lg.prims),
            "arc_prims": sum(1 for pr in L.prims if pr["kind"] == "arc"),
            "arc_prims_guard": sum(1 for pr in Lg.prims if pr["kind"] == "arc"),
            "closed_arc_le4seg": n_rect_circle,
            "n_obj_inflated": len(infl),
            "share_obj_inflated": round(len(infl) / max(len(L.objects), 1), 4),
            "inflation_med": round(sorted(infl)[len(infl) // 2], 3) if infl else None,
            "inflation_p90": round(sorted(infl)[int(0.9 * (len(infl) - 1))], 3) if infl else None,
            "counts": R.relation_counts(rel), "counts_guard": R.relation_counts(relg),
        })
        # --- relations that exist only because of the inflation ----------------------
        # compare on GEOMETRY, not object index: a relation is "the same" if the two
        # objects' true segment sets are the same on both layers.
        def sig(layer, oi):
            return tuple(sorted(layer.objects[oi]["segments"]))
        setg = set()
        for r in relg:
            if r["type"] not in BBOX_TYPES:
                continue
            a, b = sig(Lg, r["a"]), sig(Lg, r["b"]) if r["b"] is not None else None
            setg.add((r["type"], a, b) if not r.get("sym") else
                     (r["type"],) + tuple(sorted((a, b))))
        lost = {t: 0 for t in BBOX_TYPES}
        tot = {t: 0 for t in BBOX_TYPES}
        for r in rel:
            if r["type"] not in BBOX_TYPES:
                continue
            tot[r["type"]] += 1
            a, b = sig(L, r["a"]), sig(L, r["b"]) if r["b"] is not None else None
            k = (r["type"], a, b) if not r.get("sym") else (r["type"],) + tuple(sorted((a, b)))
            if k not in setg:
                lost[r["type"]] += 1
        row["bbox_rel_total"] = tot
        row["bbox_rel_only_defective"] = lost
        row["bbox_rel_share_defective"] = {t: (round(lost[t] / tot[t], 4) if tot[t] else None)
                                           for t in BBOX_TYPES}
    except Exception:
        row["error"] = traceback.format_exc()[-400:]
    return row


def main(shard, n):
    smp = json.load(open(C.ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    pool = [r for r in smp if 60 <= r["n_seg"] <= 40000][:160]
    mine = [r for i, r in enumerate(pool) if i % n == shard]
    out = []
    for k, rec in enumerate(mine):
        t0 = time.time()
        r = one(rec)
        print(f"[{shard}] {k+1}/{len(mine)} {rec['block_id']} {rec['n_seg']} "
              f"{round(time.time()-t0,1)}s infl={r.get('share_obj_inflated')} err={r['error']}",
              flush=True)
        out.append(r)
        C.F.clear_caches()
    json.dump(out, open(C.ART / f"rel_r6_{shard}.json", "w", encoding="utf-8"),
              ensure_ascii=False)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 0,
         int(sys.argv[2]) if len(sys.argv) > 2 else 1)
