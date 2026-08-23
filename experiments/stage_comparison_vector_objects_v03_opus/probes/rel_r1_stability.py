# -*- coding: utf-8 -*-
"""R1 — STABILITY of each relation type between two sides.

Two independent sources, never mixed:

  [CF]   controlled representation rewrites (class A of the counterfactual engine)
         applied to a REAL prepared block.  Object correspondence is EXACT there,
         because every rewritten segment carries the id of the segment it came from.
  [REAL] cross-revision pairs of the `mine` benchmark whose verdict is
         NO_GRAPHIC_CHANGE.  Object correspondence is geometric (ink under a measured
         translation), the same machinery `grp` used for churn.

Both denominators are always reported (gate-fix lesson):
  raw  = survived / all relations of that type on side A
  cond = survived / relations whose BOTH endpoints found a partner on side B
A type can look stable only because its endpoints died.

Usage:  rel_r1_stability.py cf [shard nshards] | real
"""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import rel_common as C
import rel_relations as R
import grp_match as M
import v03_counterfactual as CF
import cf_build_set as CB

REWRITES = ["A1_path_split", "A2_path_merge", "A5_order_shuffle", "A8_lineweight",
            "A3_curve_resample_down", "A3_curve_resample_up",
            "A4_circle_to_bezier", "A4b_circle_to_chords5",
            "A6_round_0.1", "A6_round_0.25"]
BSPECS = [("B1_translate", {"frac": 0.02}), ("B3_crop_jitter", {"frac": 0.10})]


def one_carrier(rec):
    row = {"carrier": rec, "runs": [], "error": None}
    try:
        pb = C.G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            row["error"] = "no block"
            return row
        ex = C.G.extract(pb)
        if not ex.segments:
            row["error"] = "no geometry"
            return row
        L0 = C.O.build_objects(ex)
        rel0 = R.build_relations(L0, ex)
        row["S"] = round(L0.S, 4)
        row["n_obj"] = len(L0.objects)
        row["n_seg"] = len(ex.segments)
        row["counts0"] = R.relation_counts(rel0)
        for cf_id in REWRITES:
            t0 = time.time()
            try:
                ex2, man = CF.apply(ex, L0, cf_id)
            except Exception as e:
                row["runs"].append({"cf": cf_id, "skipped": repr(e)[:120]})
                continue
            L2 = C.O.build_objects(ex2, S_override=L0.S)
            rel2 = R.build_relations(L2, ex2)
            a2b, b2a, ov = C.match_by_provenance(L0, ex.segments, L2, ex2.segments)
            surv = C.survival(rel0, rel2, a2b, L0, L2)
            row["runs"].append({"cf": cf_id, "n_obj_b": len(L2.objects),
                                "obj_matched": sum(1 for v in a2b if v >= 0),
                                "counts_b": R.relation_counts(rel2),
                                "surv": surv, "t": round(time.time() - t0, 2)})
        for cf_id, kw in BSPECS:
            t0 = time.time()
            try:
                ex2, man = CF.apply(ex, L0, cf_id, **kw)
            except Exception as e:
                row["runs"].append({"cf": cf_id, "skipped": repr(e)[:120]})
                continue
            L2 = C.O.build_objects(ex2, S_override=L0.S)
            rel2 = R.build_relations(L2, ex2)
            if cf_id == "B1_translate":
                d = man["delta"]
                off = (d["dx_pt"], d["dy_pt"])
            else:
                off = (0.0, 0.0)
            a2b, b2a, ov = C.match_by_ink(L0, ex.segments, L2, ex2.segments, off)
            surv = C.survival(rel0, rel2, a2b, L0, L2)
            row["runs"].append({"cf": cf_id + "@" + str(list(kw.values())[0]),
                                "n_obj_b": len(L2.objects),
                                "obj_matched": sum(1 for v in a2b if v >= 0),
                                "counts_b": R.relation_counts(rel2),
                                "surv": surv, "t": round(time.time() - t0, 2)})
    except Exception:
        row["error"] = traceback.format_exc()[-400:]
    return row


def run_cf(shard, n):
    carriers = CB.pick_carriers()
    mine = [r for i, r in enumerate(carriers) if i % n == shard]
    out = []
    for k, rec in enumerate(mine):
        r = one_carrier(rec)
        print(f"[{shard}] {k+1}/{len(mine)} {rec['block_id']} {rec['n_seg']} "
              f"err={bool(r['error'])}", flush=True)
        out.append(r)
        C.F.clear_caches()
    p = C.ART / f"rel_r1_cf_{shard}.json"
    json.dump(out, open(p, "w", encoding="utf-8"), ensure_ascii=False)
    print("wrote", p)


# ------------------------------------------------------------------ REAL pairs

def side(sd, S_override=None):
    ex = C.F.extract_block(str(C.ROOT / sd["pdf"]), sd["page_index"], sd["coords_px"],
                           sd["page_px"][0], sd["page_px"][1])
    L = C.O.build_objects(ex, **({"S_override": S_override} if S_override else {}))
    return ex, L


def run_real():
    pairs = json.load(open(C.ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    out = []
    for p in pairs:
        row = {"pair_id": p["pair_id"], "discipline": p["discipline"],
               "classes": p["classes"], "expected": p["expected_verdict"]}
        try:
            exA, LA0 = side(p["side_a"])
            exB, LB0 = side(p["side_b"])
            if not exA.segments or not exB.segments:
                row["error"] = "no geometry"
                out.append(row); continue
            S = max(LA0.S, LB0.S)                      # G2-2b: shared characteristic scale
            LA = C.O.build_objects(exA, S_override=S)
            LB = C.O.build_objects(exB, S_override=S)
            relA = R.build_relations(LA, exA)
            relB = R.build_relations(LB, exB)
            clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
            base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
            seed = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
            seeds = {(0.0, 0.0), base, (float(seed[0]), float(seed[1])),
                     (base[0] + float(seed[0]), base[1] + float(seed[1]))}
            dx, dy, score = M.register(exA.segments, exB.segments, seeds)
            a2b, b2a, ov = C.match_by_ink(LA, exA.segments, LB, exB.segments, (dx, dy))
            row.update({
                "S_a": round(LA0.S, 3), "S_b": round(LB0.S, 3), "S_shared": round(S, 3),
                "n_seg": [len(exA.segments), len(exB.segments)],
                "n_obj": [len(LA.objects), len(LB.objects)],
                "obj_matched": sum(1 for v in a2b if v >= 0),
                "reg": [round(dx, 3), round(dy, 3), round(score, 4)],
                "countsA": R.relation_counts(relA), "countsB": R.relation_counts(relB),
                "surv": C.survival(relA, relB, a2b, LA, LB),
            })
        except Exception:
            row["error"] = traceback.format_exc()[-400:]
        print(row["pair_id"], row.get("obj_matched"), row.get("reg"), flush=True)
        out.append(row)
        C.F.clear_caches()
    p = C.ART / "rel_r1_real.json"
    json.dump(out, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("wrote", p)


if __name__ == "__main__":
    if sys.argv[1] == "cf":
        run_cf(int(sys.argv[2]), int(sys.argv[3]))
    else:
        run_real()
