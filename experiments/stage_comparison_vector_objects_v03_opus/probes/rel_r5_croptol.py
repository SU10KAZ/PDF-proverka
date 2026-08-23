# -*- coding: utf-8 -*-
"""R5 — the two mandatory caveats, checked on our own data.

(1) Tolerances must live in PDF POINTS.  v0.2 measured that at a 10 % crop the cluster
    count moved by -11.7 % when the tolerance was a fraction of the block.  Here the
    SAME block is read twice — full and cropped by 10 % on every side — and the
    relations are rebuilt in two tolerance regimes:
        points   — the contract (absolute points and k*S, S in points)
        fraction — the forbidden variant: the same tolerance expressed as a fraction
                   of the block diagonal, so it shrinks with the crop
    Survival is measured only over objects that lie entirely inside the shrunk frame,
    so "the object left the crop" is not counted as "the relation died".

(2) Object TYPING by normalised size must not be part of a relation key.  Measured as:
    how many objects change their normalised-size type when the same block is cropped.

Also produces the relation census (how many relations of each type a block carries,
and what that costs), because a type that emits 18 000 edges on one block is a cost
statement as much as a quality one.

Usage:  rel_r5_croptol.py [shard nshards]
"""
from __future__ import annotations
import json, math, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import rel_common as C
import rel_relations as R

CROP = 0.10
MAX_SEG = 40000
N_BLOCKS = 160


def abs_params(tol, S):
    """Express a set of ABSOLUTE point tolerances in the builder's k*S parameters."""
    S = max(S, 1e-6)
    return {"conn_abs_pt": tol["conn"], "conn_k_S": 0.0,
            "adj_k_S": tol["adj"] / S, "adj_max_pt": 1e9,
            "align_k_S": tol["align"] / S,
            "label_k_S": tol["label"] / S,
            "leader_text_k_S": tol["label"] / S}


def size_type(o, diag):
    r = o["diag"] / max(diag, 1e-9)
    return "tiny" if r < 0.01 else ("small" if r < 0.05 else ("mid" if r < 0.25 else "big"))


def one(rec):
    row = {"rec": rec, "error": None}
    try:
        pb = C.G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            row["error"] = "no block"; return row
        t0 = time.time()
        ex = C.G.extract(pb)
        t_ex = time.time() - t0
        if len(ex.segments) < 40:
            row["error"] = "too few segments"; return row
        t0 = time.time()
        L = C.O.build_objects(ex)
        t_lay = time.time() - t0
        t0 = time.time()
        rel = R.build_relations(L, ex)
        t_rel = time.time() - t0
        fr = ex.frame["clip_display"]
        diag = math.hypot(fr[2] - fr[0], fr[3] - fr[1])
        row.update({"n_seg": len(ex.segments), "n_obj": len(L.objects),
                    "S": round(L.S, 4), "scale_src": L.scale_source,
                    "diag_pt": round(diag, 2),
                    "counts": R.relation_counts(rel), "n_rel": len(rel),
                    "t_extract": round(t_ex, 3), "t_layer": round(t_lay, 3),
                    "t_rel": round(t_rel, 3)})

        # ---- cropped read: 10 % off every side, in the block's own px coordinates ----
        x0, y0, x1, y1 = pb.coords_px
        w, h = x1 - x0, y1 - y0
        cpx = (x0 + CROP * w, y0 + CROP * h, x1 - CROP * w, y1 - CROP * h)
        ex2 = C.F.extract_block(pb.pdf_path, pb.page_index, cpx, pb.page_px_w, pb.page_px_h)
        if len(ex2.segments) < 20:
            row["error"] = "crop empty"; return row
        fr2 = ex2.frame["clip_display"]
        diag2 = math.hypot(fr2[2] - fr2[0], fr2[3] - fr2[1])
        row["crop_diag_pt"] = round(diag2, 2)
        row["crop_n_seg"] = len(ex2.segments)

        def inside(o):
            b = o["bbox"]
            return (b[0] >= fr2[0] and b[1] >= fr2[1] and b[2] <= fr2[2] and b[3] <= fr2[3])

        # tolerances actually in force on the FULL block, in points
        tol_full = {"conn": R.DEFAULTS["conn_abs_pt"] + R.DEFAULTS["conn_k_S"] * L.S,
                    "adj": min(R.DEFAULTS["adj_k_S"] * L.S, R.DEFAULTS["adj_max_pt"]),
                    "align": max(R.DEFAULTS["align_k_S"] * L.S, 0.05),
                    "label": R.DEFAULTS["label_k_S"] * L.S}
        row["tol_full_pt"] = {k: round(v, 4) for k, v in tol_full.items()}
        shrink = diag2 / diag                      # exactly what a fraction-of-block loses
        row["crop_shrink"] = round(shrink, 4)

        for regime in ("points", "fraction"):
            LB = C.O.build_objects(ex2, S_override=L.S)
            if regime == "points":
                relA, relB = rel, R.build_relations(LB, ex2)
            else:
                relA = R.build_relations(L, ex, **abs_params(tol_full, L.S))
                tol_c = {k: v * shrink for k, v in tol_full.items()}
                relB = R.build_relations(LB, ex2, **abs_params(tol_c, LB.S))
            a2b, b2a, ov = C.match_by_ink(L, ex.segments, LB, ex2.segments, (0.0, 0.0))
            keep = {i for i, o in enumerate(L.objects) if inside(o)}
            sub = [r for r in relA
                   if r["a"] in keep and (r["b"] in keep if r.get("b") is not None else True)]
            surv = C.survival(sub, relB, a2b, L, LB)
            row[f"crop_{regime}"] = {
                "n_obj_b": len(LB.objects), "kept_objects": len(keep),
                "matched": sum(1 for i in keep if a2b[i] >= 0),
                "n_rel_a_interior": len(sub),
                "S_b_free": round(C.O.build_objects(ex2).S, 4),
                "surv": surv, "counts_b": R.relation_counts(relB),
            }

        # ---- (2) normalised-size typing under the same crop -------------------------
        LB = C.O.build_objects(ex2, S_override=L.S)
        a2b, b2a, ov = C.match_by_ink(L, ex.segments, LB, ex2.segments, (0.0, 0.0))
        same = tot = 0
        for i, o in enumerate(L.objects):
            j = a2b[i]
            if j < 0:
                continue
            tot += 1
            same += int(size_type(o, diag) == size_type(LB.objects[j], diag2))
        row["size_type_stable"] = {"n": tot, "same": same,
                                   "share": round(same / tot, 4) if tot else None}
    except Exception:
        row["error"] = traceback.format_exc()[-400:]
    return row


def main(shard, n):
    smp = json.load(open(C.ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    pool = [r for r in smp if 60 <= r["n_seg"] <= MAX_SEG][:N_BLOCKS]
    mine = [r for i, r in enumerate(pool) if i % n == shard]
    out = []
    for k, rec in enumerate(mine):
        t0 = time.time()
        r = one(rec)
        print(f"[{shard}] {k+1}/{len(mine)} {rec['block_id']} {rec['n_seg']} "
              f"{round(time.time()-t0,1)}s err={r['error']}", flush=True)
        out.append(r)
        C.F.clear_caches()
    json.dump(out, open(C.ART / f"rel_r5_{shard}.json", "w", encoding="utf-8"),
              ensure_ascii=False)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 0,
         int(sys.argv[2]) if len(sys.argv) > 2 else 1)
