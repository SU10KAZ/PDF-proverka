# -*- coding: utf-8 -*-
"""[REAL] arm: what the ledger says on the 33 benchmark pairs, and the pair-level ROC.

No counterfactual is involved.  Ground truth is the human verdict of `mine_pairs.json`
(one annotator — that limit is carried into the report).  The corpus has no P->RD pairs
(probe `pd`), so this axis is CROSS-REVISION only.
"""
from __future__ import annotations
import json
import statistics
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"
T_PT = [0.0, 0.5, 1, 2, 3, 5, 8, 12, 20, 35, 60, 100, 200, 500]


def med(v):
    return round(statistics.median(v), 4) if v else None


def main():
    P = json.load(open(ART / "loc_real_pairs.json", encoding="utf-8"))["pairs"]
    bench = {p["pair_id"]: p for p in
             json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]}
    rows = []
    for r in P:
        b = bench[r["pair_id"]]
        row = {"pair_id": r["pair_id"], "discipline": r["discipline"],
               "classes": r["classes"], "expected": r["expected"],
               "label_confidence": r["label_confidence"],
               "expected_changed_objects": r.get("expected_changed_objects"),
               "human": r["human"], "error": r.get("error")}
        if "error" not in r:
            ri = r["rec_len_interior"]
            row.update({
                "n_seg_a": r["n_seg_a"], "n_seg_b": r["n_seg_b"],
                "ink_similarity": r["scalar"]["ink_similarity"],
                "S_a": r["S_a"], "S_b": r["S_b"], "S_shared": r["S_shared"],
                "n_records": r["n_records"], "n_records_interior": r["n_records_interior"],
                "max_interior_len": max(ri) if ri else 0.0,
                "median_interior_len": med(ri),
                "verdict_scalar_999": r["verdict_scalar_999"],
                "verdict_scalar_9999": r["verdict_scalar_9999"],
                "verdict_counts": r["verdict_counts"],
                "flag_at": {str(t): int(any(x >= t for x in ri)) for t in T_PT},
                "n_interior_at": {str(t): sum(1 for x in ri if x >= t) for t in T_PT},
                "flag_all_at": {str(t): int(any(x >= t for x in r["rec_len_all"]))
                                for t in T_PT},
                "n_all_at": {str(t): sum(1 for x in r["rec_len_all"] if x >= t)
                             for t in T_PT},
                "top_records": [{k: v for k, v in x.items() if k != "objects_a"}
                                for x in r["records_top"][:3]],
            })
        rows.append(row)

    usable = [r for r in rows if r["expected"] in ("GRAPHIC_CHANGE", "NO_GRAPHIC_CHANGE")
              and not r["error"]]
    pos = [r for r in usable if r["expected"] == "GRAPHIC_CHANGE"]
    neg = [r for r in usable if r["expected"] == "NO_GRAPHIC_CHANGE"]

    roc_all = []
    for t in T_PT:
        tp = sum(r["flag_all_at"][str(t)] for r in pos)
        fp = sum(r["flag_all_at"][str(t)] for r in neg)
        roc_all.append({
            "T_pt": t,
            "recall_pairs": round(tp / len(pos), 4),
            "false_alarm_pairs": round(fp / len(neg), 4),
            "false_records_per_quiet_pair_mean":
                round(sum(r["n_all_at"][str(t)] for r in neg) / len(neg), 3),
            "false_records_per_quiet_pair_max":
                max(r["n_all_at"][str(t)] for r in neg),
        })

    roc = []
    for t in T_PT:
        tp = sum(r["flag_at"][str(t)] for r in pos)
        fp = sum(r["flag_at"][str(t)] for r in neg)
        roc.append({
            "T_pt": t,
            "recall_pairs": round(tp / len(pos), 4), "n_pos": len(pos),
            "false_alarm_pairs": round(fp / len(neg), 4), "n_neg": len(neg),
            "false_records_per_quiet_pair_mean":
                round(sum(r["n_interior_at"][str(t)] for r in neg) / len(neg), 3),
            "false_records_per_quiet_pair_max":
                max(r["n_interior_at"][str(t)] for r in neg),
            "records_per_changed_pair_mean":
                round(sum(r["n_interior_at"][str(t)] for r in pos) / len(pos), 3),
        })

    base = {}
    for name, key in (("scalar_0.999", "verdict_scalar_999"),
                      ("scalar_0.9999", "verdict_scalar_9999"),
                      ("counts", "verdict_counts")):
        base[name] = {
            "recall_pairs": round(sum(1 for r in pos if r[key] == "GRAPHIC_CHANGE") / len(pos), 4),
            "false_alarm_pairs": round(sum(1 for r in neg if r[key] == "GRAPHIC_CHANGE") / len(neg), 4),
        }

    out = {
        "note": "cross-revision pairs only; the corpus has no P->RD pairs (probe pd). "
                "Ground truth = one human annotator (mine M7).",
        "n_pairs": len(rows), "n_usable": len(usable), "n_pos": len(pos), "n_neg": len(neg),
        "pair_level_roc_interior_records": roc,
        "pair_level_roc_all_records": roc_all,
        "baselines_pair_level": base,
        "small_local_change_pairs": [r["pair_id"] for r in usable
                                     if "small_local_change" in r["classes"]],
        "rows": rows,
    }
    json.dump(out, open(ART / "loc_real_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print("pairs", len(rows), "usable", len(usable), "pos", len(pos), "neg", len(neg))
    for x in roc:
        print(x)
    print(base)


if __name__ == "__main__":
    main()
