# -*- coding: utf-8 -*-
"""Aggregation over the MERGED sensitivity grid (first run + fixed-scorer replay).

Rows from `rescore*.jsonl` (fixed scorer) override the matching rows of `sens_*.jsonl`
(first scorer).  Both defects fixed in the replay are monotone, so any row that is not
replayed was already right at all three levels.  `provenance` counts what came from where.
"""
from __future__ import annotations
import glob
import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import loc_agg as A  # noqa: E402


def merged():
    base = {}
    for f in sorted(glob.glob(str(ART / "loc_runs" / "sens_*.jsonl"))):
        for line in open(f, encoding="utf-8"):
            r = json.loads(line)
            if "score" in r:
                base[(r["block_id"], r["inst"], r["noise"])] = r
    n_fixed = 0
    for f in sorted(glob.glob(str(ART / "loc_runs" / "rescore*.jsonl"))):
        for line in open(f, encoding="utf-8"):
            r = json.loads(line)
            if "score" not in r:
                continue
            k = (r["block_id"], r["inst"], r["noise"])
            if k in base:
                n_fixed += 1
            base[k] = r
    rows = list(base.values())
    stale = 0
    for r in rows:
        if r.get("cf_class") == "C":
            s = r["score"]
            if not (s["L2_localised"] and s["L3_right_type"] and s["L4_right_object"]) \
                    and "rec_boxes" not in s:
                stale += 1
    return rows, {"n_rows": len(rows), "n_rows_from_replay": n_fixed,
                  "n_C_rows_still_scored_by_the_defective_scorer": stale}


def size_curve(R):
    """Finer than the three brief buckets: detection against the true area share."""
    pos = [r for r in R if r.get("cf_class") == "C" and r.get("obj_area_frac") is not None
           and r["cf_id"] in ("C1_remove_object", "C2_add_object", "C3_move_object",
                              "C6_reshape_object")]
    bins = [(0, 1e-4), (1e-4, 3e-4), (3e-4, 1e-3), (1e-3, 3e-3), (3e-3, 1e-2),
            (1e-2, 3e-2), (3e-2, 1e-1), (1e-1, 1.01)]
    out = {}
    for noise in sorted({r["noise"] for r in pos}):
        c = []
        for lo, hi in bins:
            sel = [r for r in pos if r["noise"] == noise and lo <= r["obj_area_frac"] < hi]
            if not sel:
                continue
            c.append({"area_frac": f"{lo:g}-{hi:g}", "n": len(sel),
                      "L2": A.rate([r["score"]["L2_localised"] for r in sel]),
                      "L4": A.rate([r["score"]["L4_right_object"] for r in sel]),
                      "median_obj_n_seg": A.med([r["obj_n_seg"] for r in sel]),
                      "median_block_n_seg": A.med([r["n_seg"] for r in sel]),
                      "median_sim": A.med([r["scalar"]["ink_similarity"] for r in sel]),
                      "scalar_999_blind": A.rate([r["verdict_scalar_999"] == "NO_GRAPHIC_CHANGE"
                                                  for r in sel]),
                      "counts_blind": A.rate([r["verdict_counts"] == "NO_GRAPHIC_CHANGE"
                                              for r in sel])})
        out[noise] = c
    # the same axis inside each density band, noise = none
    per_dens = {}
    for d in A.DENS:
        c = []
        for lo, hi in bins:
            sel = [r for r in pos if r["noise"] == "none" and r["bucket"] == d
                   and lo <= r["obj_area_frac"] < hi]
            if len(sel) < 5:
                continue
            c.append({"area_frac": f"{lo:g}-{hi:g}", "n": len(sel),
                      "L2": A.rate([r["score"]["L2_localised"] for r in sel]),
                      "L4": A.rate([r["score"]["L4_right_object"] for r in sel])})
        per_dens[d] = c
    return {"by_area_share": out, "by_area_share_and_density_noise_none": per_dens}


def hybrid_roc():
    """ROC whose two halves come from the two arms, each where it is honest.

    recall  — [CF]: only a counterfactual knows which record is the right one;
    false   — [REAL]: only a real quiet pair knows what a real re-export produces.
              (The CF negatives are byte-identical apart from the injected noise and
              therefore cannot model a re-export at all: median 0 records.)

    The threshold T is the same quantity on both sides — points of changed ink in one
    ledger record — so the two halves are comparable even though the samples are not.
    """
    R, prov = merged()
    real = json.load(open(ART / "loc_real_pairs.json", encoding="utf-8"))["pairs"]
    quiet = [p for p in real if "error" not in p and p["expected"] == "NO_GRAPHIC_CHANGE"]
    T = [0.0, 0.5, 1, 2, 3, 5, 8, 12, 20, 35, 60, 100, 200]
    out = {"provenance": prov, "n_quiet_real_pairs": len(quiet)}
    for pop, sel in (("small_cf_changes",
                      lambda r: r.get("size_bucket") in ("tiny", "small")),
                     ("all_cf_changes", lambda r: True)):
        for noise in ("none", "round025"):
            P = [r for r in R if r.get("cf_class") == "C" and r["noise"] == noise and sel(r)]
            if not P:
                continue
            curve = []
            for t in T:
                tp = sum(1 for r in P
                         if any(x[0] >= t and x[1] == 1 and x[2] == 0
                                for x in r["score"]["recs"]))
                fp_rec = [sum(1 for x in p["rec_len_interior"] if x >= t) for p in quiet]
                fp_pair = sum(1 for x in fp_rec if x)
                curve.append({
                    "T_pt": t,
                    "recall_cf": round(tp / len(P), 4),
                    "false_records_per_real_quiet_pair_mean": round(sum(fp_rec) / len(quiet), 3),
                    "false_records_per_real_quiet_pair_max": max(fp_rec),
                    "share_real_quiet_pairs_with_any_record": round(fp_pair / len(quiet), 4),
                })
            out[f"{pop}|{noise}"] = {"n_cf_positives": len(P), "curve": curve}
    json.dump(out, open(ART / "loc_hybrid_roc.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    return out


def main():
    R, prov = merged()
    print(prov)
    S = A.sensitivity([r for r in R if "score" in r])
    S["provenance"] = prov
    S.update(size_curve(R))
    json.dump(S, open(ART / "loc_sensitivity.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    json.dump(A.roc(R), open(ART / "loc_roc.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    sc = A.samecounts(R)
    sc["provenance"] = prov
    json.dump(sc, open(ART / "loc_samecounts.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("rows", len(R))
    hybrid_roc()


if __name__ == "__main__":
    main()


