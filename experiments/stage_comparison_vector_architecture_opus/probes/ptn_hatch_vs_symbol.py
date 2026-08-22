"""Which feature separates real engineering symbols from hatch tiles / packaging rectangles?

Uses the eye labels in artifacts/ptn_visual_labels.json against the measured group features
from ptn_group_features.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_hatch_vs_symbol
Writes artifacts/ptn_hatch_vs_symbol.json
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from experiments.stage_comparison_vector_architecture_opus.probes.ptn_group_features import analyse  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
ART = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

SETS = [
    ("ar_plan", "ar_plan_left_S1_top36", 36),
    ("ss_scheme_text_changed", "ss_scheme_text_changed_left_S1_top10", 10),
    ("eom_singleline_changed", "eom_singleline_changed_left_S1_top10", 10),
]

RULES = {
    "R1 nseg>=6": lambda r: r["nseg"] >= 6,
    "R2 closure>=0.9 & straightness>0.10": lambda r: r["closure"] >= 0.9 and r["straightness"] > 0.10,
    "R3 text_inside_frac>0.5": lambda r: r["text_inside_frac"] > 0.5,
    "R4 labeled_frac>0.5": lambda r: r["labeled_frac"] > 0.5,
    "R5 lattice<0.3": lambda r: r["lattice"] < 0.3,
    "R6 ext_touch_mean<0.5": lambda r: r["ext_touch_mean"] < 0.5,
    "R7 junctions>=1": lambda r: r["junctions"] >= 1,
    "R2+R3": lambda r: (r["closure"] >= 0.9 and r["straightness"] > 0.10) or r["text_inside_frac"] > 0.5,
    "R2&R4": lambda r: r["closure"] >= 0.9 and r["straightness"] > 0.10 and r["labeled_frac"] > 0.5,
}


def main() -> None:
    labels = json.load(open(ART / "ptn_visual_labels.json", encoding="utf-8"))
    rows = []
    for pair, key, top_n in SETS:
        res = analyse(pair, "left", "S1", top_n)
        lab = labels[key]
        for row in res["rows"]:
            row["block"] = pair
            row["label"] = lab[str(row["rank"])]
            rows.append(row)

    out = {"n_groups": len(rows), "label_counts": {}, "rules": {}, "rows": rows}
    for row in rows:
        out["label_counts"][row["label"]] = out["label_counts"].get(row["label"], 0) + 1

    for name, fn in RULES.items():
        stats = {}
        for target, negatives in (("SYMBOL_vs_HATCH", {"HATCH"}), ("SYMBOL_vs_HATCH+RECT", {"HATCH", "RECT"})):
            tp = sum(1 for r in rows if r["label"] == "SYMBOL" and fn(r))
            fn_ = sum(1 for r in rows if r["label"] == "SYMBOL" and not fn(r))
            fp = sum(1 for r in rows if r["label"] in negatives and fn(r))
            tn = sum(1 for r in rows if r["label"] in negatives and not fn(r))
            stats[target] = {
                "symbol_groups": tp + fn_, "negative_groups": fp + tn,
                "recall": round(tp / max(tp + fn_, 1), 3),
                "precision": round(tp / max(tp + fp, 1), 3),
                "false_positives": fp,
            }
        out["rules"][name] = stats
        print(f"{name:38s} S-vs-H recall {stats['SYMBOL_vs_HATCH']['recall']:.2f} "
              f"prec {stats['SYMBOL_vs_HATCH']['precision']:.2f} FP {stats['SYMBOL_vs_HATCH']['false_positives']}"
              f" | S-vs-H+RECT prec {stats['SYMBOL_vs_HATCH+RECT']['precision']:.2f} "
              f"FP {stats['SYMBOL_vs_HATCH+RECT']['false_positives']}")

    with open(ART / "ptn_hatch_vs_symbol.json", "w", encoding="utf-8") as handle:
        json.dump(out, handle, ensure_ascii=False, indent=1)
    print(ART / "ptn_hatch_vs_symbol.json", out["label_counts"])


if __name__ == "__main__":
    main()
