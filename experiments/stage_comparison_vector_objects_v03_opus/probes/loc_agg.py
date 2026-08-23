# -*- coding: utf-8 -*-
"""Aggregate the `loc` runs into the artefacts the report cites."""
from __future__ import annotations
import json
import glob
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"


def rows(pat):
    out = []
    for f in sorted(glob.glob(str(ART / "loc_runs" / pat))):
        for l in open(f, encoding="utf-8"):
            l = l.strip()
            if l:
                out.append(json.loads(l))
    return out


def pct(v, q):
    if not v:
        return None
    s = sorted(v)
    return s[min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))]


def med(v):
    return round(statistics.median(v), 6) if v else None


def rate(v):
    return round(sum(1 for x in v if x) / len(v), 4) if v else None


DENS = ("sparse", "medium", "dense", "very_dense")
SIZE = ("tiny", "small", "large")


def sensitivity(R):
    pos = [r for r in R if r.get("cf_class") == "C"]
    neg = [r for r in R if r.get("inst") == "NEG"]
    out = {"n_pos": len(pos), "n_neg": len(neg)}

    # -- the C1 table: density x object size, per noise ---------------------------
    tab = {}
    for noise in sorted({r["noise"] for r in R}):
        t = {}
        for d in DENS:
            for s in SIZE:
                sel = [r for r in pos if r["noise"] == noise and r["bucket"] == d
                       and r.get("size_bucket") == s
                       and r["cf_id"] in ("C1_remove_object", "C2_add_object", "C3_move_object")]
                if not sel:
                    continue
                t[f"{d}|{s}"] = {
                    "n": len(sel),
                    "L2_localised": rate([r["score"]["L2_localised"] for r in sel]),
                    "L3_right_type": rate([r["score"]["L3_right_type"] for r in sel]),
                    "L4_right_object": rate([r["score"]["L4_right_object"] for r in sel]),
                    "median_touched_ink_pt": med([r["touched_ink_pt"] for r in sel]),
                    "median_touched_ink_frac": med([r["touched_ink_frac"] for r in sel]),
                    "median_ink_similarity": med([r["scalar"]["ink_similarity"] for r in sel]),
                    "share_sim_ge_0.999": rate([r["scalar"]["ink_similarity"] >= 0.999 for r in sel]),
                    "median_false_records": med([r["score"]["n_false_records"] for r in sel]),
                }
        tab[noise] = t
    out["by_density_and_size"] = tab

    # -- per counterfactual id ----------------------------------------------------
    per = {}
    for noise in sorted({r["noise"] for r in R}):
        d = {}
        for cf in sorted({r["cf_id"] for r in pos}):
            sel = [r for r in pos if r["noise"] == noise and r["cf_id"] == cf]
            if not sel:
                continue
            d[cf] = {"n": len(sel),
                     "L2": rate([r["score"]["L2_localised"] for r in sel]),
                     "L3": rate([r["score"]["L3_right_type"] for r in sel]),
                     "L4": rate([r["score"]["L4_right_object"] for r in sel]),
                     "median_sim": med([r["scalar"]["ink_similarity"] for r in sel]),
                     "median_false": med([r["score"]["n_false_records"] for r in sel]),
                     "counts_blind": rate([r["verdict_counts"] == "NO_GRAPHIC_CHANGE" for r in sel])}
        per[noise] = d
    out["by_cf_id"] = per

    # -- the threshold: detection against the ink actually touched ----------------
    bins = [(0, 0.5), (0.5, 1), (1, 2), (2, 5), (5, 10), (10, 25), (25, 50),
            (50, 100), (100, 300), (300, 1e12)]
    curve = {}
    for noise in sorted({r["noise"] for r in R}):
        c = []
        for lo, hi in bins:
            sel = [r for r in pos if r["noise"] == noise and lo <= r["touched_ink_pt"] < hi]
            if not sel:
                continue
            c.append({"ink_pt": f"{lo}-{hi if hi < 1e11 else 'inf'}", "n": len(sel),
                      "L2": rate([r["score"]["L2_localised"] for r in sel]),
                      "L4": rate([r["score"]["L4_right_object"] for r in sel]),
                      "median_sim": med([r["scalar"]["ink_similarity"] for r in sel])})
        curve[noise] = c
    out["detection_vs_touched_ink_pt"] = curve

    fbins = [(0, 1e-5), (1e-5, 1e-4), (1e-4, 1e-3), (1e-3, 1e-2), (1e-2, 1e-1), (1e-1, 1.1)]
    fcurve = {}
    for noise in sorted({r["noise"] for r in R}):
        c = []
        for lo, hi in fbins:
            sel = [r for r in pos if r["noise"] == noise and lo <= r["touched_ink_frac"] < hi]
            if not sel:
                continue
            c.append({"ink_frac": f"{lo:g}-{hi:g}", "n": len(sel),
                      "L2": rate([r["score"]["L2_localised"] for r in sel]),
                      "L4": rate([r["score"]["L4_right_object"] for r in sel]),
                      "median_sim": med([r["scalar"]["ink_similarity"] for r in sel]),
                      "median_n_seg_block": med([r["n_seg"] for r in sel])})
        fcurve[noise] = c
    out["detection_vs_touched_ink_frac"] = fcurve

    # -- the point of the probe: scalar blind, ledger not -------------------------
    blind = {}
    for thr in (0.999, 0.9995, 0.9999):
        sel = [r for r in pos if r["noise"] in ("none", "round025")
               and r["scalar"]["ink_similarity"] >= thr]
        blind[str(thr)] = {
            "n_positives_a_scalar_calls_identical": len(sel),
            "share_of_all_positives": round(len(sel) / max(1, len([r for r in pos if r["noise"] in ("none", "round025")])), 4),
            "ledger_L2": rate([r["score"]["L2_localised"] for r in sel]),
            "ledger_L4": rate([r["score"]["L4_right_object"] for r in sel]),
            "median_touched_ink_frac": med([r["touched_ink_frac"] for r in sel]),
        }
    out["scalar_blind_zone"] = blind

    # -- the scalar as a separator: overlap of positives and negatives ------------
    sep = {}
    for noise in sorted({r["noise"] for r in R}):
        p = [1 - r["scalar"]["ink_similarity"] for r in pos if r["noise"] == noise]
        n = [1 - r["scalar"]["ink_similarity"] for r in neg if r["noise"] == noise]
        if not p or not n:
            continue
        auc = 0.0
        ns = sorted(n)
        for x in p:
            lo, hi = 0, len(ns)
            while lo < hi:
                mid = (lo + hi) // 2
                if ns[mid] < x:
                    lo = mid + 1
                else:
                    hi = mid
            ties = sum(1 for y in ns if abs(y - x) < 1e-12)
            auc += (lo + 0.5 * ties) / len(ns)
        sep[noise] = {
            "n_pos": len(p), "n_neg": len(n),
            "auc_scalar": round(auc / len(p), 4),
            "pos_dissimilarity_median": med(p), "pos_p10": pct(p, 0.10),
            "neg_dissimilarity_median": med(n), "neg_p90": pct(n, 0.90),
            "neg_max": max(n),
            "share_pos_below_neg_p90": rate([x <= pct(n, 0.90) for x in p]),
        }
    out["scalar_separation"] = sep
    return out


def roc(R):
    """Threshold sweep on the ledger record size (points of ink)."""
    pos = [r for r in R if r.get("cf_class") == "C"]
    neg = [r for r in R if r.get("inst") == "NEG"]
    T = [0.0, 0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 12, 20, 35, 60, 100, 200]
    out = {}
    for noise in sorted({r["noise"] for r in R}):
        for scope in ("all", "interior_only"):
            for dens in ("ALL",) + DENS:
                P = [r for r in pos if r["noise"] == noise and (dens == "ALL" or r["bucket"] == dens)]
                N = [r for r in neg if r["noise"] == noise and (dens == "ALL" or r["bucket"] == dens)]
                if not P or not N:
                    continue
                curve = []
                for t in T:
                    tp = 0
                    for r in P:
                        recs = r["score"]["recs"]
                        if any(x[0] >= t and x[1] == 1 and (scope == "all" or x[2] == 0) for x in recs):
                            tp += 1
                    fp_counts, fp_pairs = [], 0
                    for r in N:
                        c = sum(1 for x in r["score"]["recs"]
                                if x[0] >= t and (scope == "all" or x[2] == 0))
                        fp_counts.append(c)
                        fp_pairs += 1 if c else 0
                    # false records on positives that are NOT the planted change
                    fp_on_pos = [sum(1 for x in r["score"]["recs"]
                                     if x[0] >= t and x[1] == 0 and (scope == "all" or x[2] == 0))
                                 for r in P]
                    curve.append({
                        "T_pt": t, "recall": round(tp / len(P), 4),
                        "false_records_per_quiet_pair_mean": round(sum(fp_counts) / len(N), 4),
                        "false_records_per_quiet_pair_p90": pct(fp_counts, 0.9),
                        "share_quiet_pairs_with_any_record": round(fp_pairs / len(N), 4),
                        "false_records_per_positive_mean": round(sum(fp_on_pos) / len(P), 4),
                    })
                out[f"{noise}|{scope}|{dens}"] = {"n_pos": len(P), "n_neg": len(N), "curve": curve}
    return out


def samecounts(R):
    """§12 — counters identical, graphics different."""
    sel = [r for r in R if r.get("cf_id") in ("C4_swap_objects", "C5_swap_unlike",
                                              "C6_reshape_object")]
    out = {"n": len(sel)}
    per = {}
    for cf in sorted({r["cf_id"] for r in sel}):
        for noise in sorted({r["noise"] for r in sel}):
            s = [r for r in sel if r["cf_id"] == cf and r["noise"] == noise]
            if not s:
                continue
            per[f"{cf}|{noise}"] = {
                "n": len(s),
                "counters_identical_measured": rate([r["counts"]["d_obj"] == 0 and r["counts"]["d_seg"] == 0 for r in s]),
                "d_seg_zero": rate([r["counts"]["d_seg"] == 0 for r in s]),
                "d_obj_zero": rate([r["counts"]["d_obj"] == 0 for r in s]),
                "counts_verdict_blind": rate([r["verdict_counts"] == "NO_GRAPHIC_CHANGE" for r in s]),
                "scalar_999_blind": rate([r["verdict_scalar_999"] == "NO_GRAPHIC_CHANGE" for r in s]),
                "ledger_L2": rate([r["score"]["L2_localised"] for r in s]),
                "ledger_L3": rate([r["score"]["L3_right_type"] for r in s]),
                "ledger_L4": rate([r["score"]["L4_right_object"] for r in s]),
                "median_records": med([r["score"]["n_records"] for r in s]),
                "median_sim": med([r["scalar"]["ink_similarity"] for r in s]),
            }
    out["per"] = per
    # strict subset: counters really identical
    strict = [r for r in sel if r["counts"]["d_obj"] == 0 and r["counts"]["d_seg"] == 0]
    out["strict_counters_identical"] = {
        "n": len(strict),
        "ledger_L2": rate([r["score"]["L2_localised"] for r in strict]),
        "ledger_L4": rate([r["score"]["L4_right_object"] for r in strict]),
        "by_noise": {nz: {"n": len([r for r in strict if r["noise"] == nz]),
                          "L2": rate([r["score"]["L2_localised"] for r in strict if r["noise"] == nz])}
                     for nz in sorted({r["noise"] for r in strict})},
    }
    return out


def dilution(D):
    ok = [r for r in D if "score" in r]
    out = {"n_rows": len(ok), "n_carriers": len({r["block_id"] for r in ok})}
    bins = [(0, 10), (10, 50), (50, 200), (200, 1000), (1000, 5000), (5000, 30000), (30000, 1e12)]
    per = {}
    for noise in sorted({r["noise"] for r in ok}):
        c = []
        for lo, hi in bins:
            sel = [r for r in ok if r["noise"] == noise and lo <= r["frame_area_over_target"] < hi]
            if not sel:
                continue
            c.append({"frame_area_over_target": f"{lo}-{hi if hi < 1e11 else 'inf'}",
                      "n": len(sel),
                      "median_n_seg_frame": med([r["n_seg_frame"] for r in sel]),
                      "median_deleted_ink_frac": med([r["deleted_ink_frac"] for r in sel]),
                      "median_ink_similarity": med([r["scalar"]["ink_similarity"] for r in sel]),
                      "scalar_999_calls_identical": rate([r["verdict_scalar_999"] == "NO_GRAPHIC_CHANGE" for r in sel]),
                      "scalar_9999_calls_identical": rate([r["verdict_scalar_9999"] == "NO_GRAPHIC_CHANGE" for r in sel]),
                      "counts_calls_identical": rate([r["verdict_counts"] == "NO_GRAPHIC_CHANGE" for r in sel]),
                      "ledger_L2": rate([r["score"]["L2_localised"] for r in sel]),
                      "median_false_records": med([r["score"]["n_false_records"] for r in sel]),
                      "p90_false_records": pct([r["score"]["n_false_records"] for r in sel], 0.9),
                      "median_t_sec": med([r["t_sec"] for r in sel]),
                      "max_t_sec": max(r["t_sec"] for r in sel)})
        per[noise] = c
    out["by_frame_growth"] = per
    # per carrier: the largest frame at which the change is still found
    trail = []
    for bid in sorted({r["block_id"] for r in ok}):
        for noise in sorted({r["noise"] for r in ok}):
            s = sorted([r for r in ok if r["block_id"] == bid and r["noise"] == noise],
                       key=lambda r: r["frame_area_over_target"])
            if not s:
                continue
            found = [r for r in s if r["score"]["L2_localised"]]
            trail.append({"block_id": bid, "noise": noise, "n_frames": len(s),
                          "max_frame_area_ratio": s[-1]["frame_area_over_target"],
                          "max_n_seg": s[-1]["n_seg_frame"],
                          "all_frames_detected": len(found) == len(s),
                          "n_detected": len(found),
                          "min_deleted_ink_frac": min(r["deleted_ink_frac"] for r in s),
                          "max_sim_at_detection": max([r["scalar"]["ink_similarity"] for r in found] or [0]),
                          "false_at_largest": s[-1]["score"]["n_false_records"]})
    out["per_carrier"] = trail
    out["summary"] = {
        "carrier_noise_arms": len(trail),
        "arms_detected_at_every_frame": rate([t["all_frames_detected"] for t in trail]),
        "median_max_frame_area_ratio": med([t["max_frame_area_ratio"] for t in trail]),
        "max_ink_similarity_with_correct_record": max([t["max_sim_at_detection"] for t in trail] or [0]),
    }
    return out


def main():
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    if what in ("all", "sens"):
        R = [r for r in rows("sens_*.jsonl") if "score" in r]
        json.dump(sensitivity(R), open(ART / "loc_sensitivity.json", "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
        json.dump(roc(R), open(ART / "loc_roc.json", "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
        json.dump(samecounts(R), open(ART / "loc_samecounts.json", "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
        print("sens rows", len(R))
    if what in ("all", "dil"):
        D = rows("dil_*.jsonl")
        json.dump(dilution(D), open(ART / "loc_dilution.json", "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
        print("dil rows", len(D))


if __name__ == "__main__":
    main()
