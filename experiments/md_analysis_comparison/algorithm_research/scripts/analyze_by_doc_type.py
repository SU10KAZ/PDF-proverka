"""analyze_by_doc_type — per-document_type aggregate for A1-v2 vs A0.

Output:
  algorithm_research/reports/_doc_type_analysis.md
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from statistics import mean

HERE = Path(__file__).resolve()
RESEARCH = HERE.parents[1]
SCORES = RESEARCH / "results" / "_scores.json"
DATASETS = RESEARCH.parent / "datasets"
OUT = RESEARCH / "reports" / "_doc_type_analysis.md"


def main():
    data = json.loads(SCORES.read_text(encoding="utf-8"))
    rows = data.get("rows") or []

    # Load document_type for each case
    dt = {}
    for case_dir in DATASETS.iterdir():
        cj = case_dir / "case.json"
        if cj.exists():
            try:
                d = json.loads(cj.read_text(encoding="utf-8"))
                dt[case_dir.name] = d.get("document_type", "?")
            except Exception:
                pass

    def agg(rows_):
        if not rows_:
            return None
        return {
            "cases": len(rows_),
            "matched": sum(r["matched_gt"] for r in rows_),
            "missed_crit": sum(r["missed_critical"] for r in rows_),
            "fp": sum(r["false_positives"] for r in rows_),
            "total": sum(r["total_findings"] for r in rows_),
            "avg_strict": round(mean(r["scores"]["strict_production"] for r in rows_), 1),
            "avg_balanced": round(mean(r["scores"]["balanced_engineering"] for r in rows_), 1),
        }

    # Group by doc_type
    by_dt: dict[str, dict[str, list]] = {}
    for r in rows:
        d = dt.get(r["case_id"], "?")
        algo = f"{r['algorithm']}__{r['prompt_set']}"
        by_dt.setdefault(d, {}).setdefault(algo, []).append(r)

    lines = ["# Per-document_type Analysis", "",
             "Aggregated by document_type across the cases done so far.",
             ""]

    for doc_type in ("full_rd", "audit_comparison", "tz_vs_rd", "specification_only"):
        if doc_type not in by_dt:
            continue
        lines.append(f"## {doc_type}")
        lines.append("")
        lines.append("| Algorithm | cases | matched | missed_crit | FP | total | avg_strict | avg_balanced |")
        lines.append("|---|---|---|---|---|---|---|---|")
        algos = sorted(by_dt[doc_type].keys())
        for a in algos:
            ag = agg(by_dt[doc_type][a])
            if ag is None:
                continue
            lines.append(
                f"| {a} | {ag['cases']} | {ag['matched']} | {ag['missed_crit']} | "
                f"{ag['fp']} | {ag['total']} | {ag['avg_strict']} | {ag['avg_balanced']} |"
            )
        lines.append("")

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT}")
    for doc_type in ("full_rd", "audit_comparison", "tz_vs_rd", "specification_only"):
        if doc_type not in by_dt:
            continue
        print(f"\n--- {doc_type} ---")
        for a in sorted(by_dt[doc_type]):
            ag = agg(by_dt[doc_type][a])
            print(f"  {a:50s} cases={ag['cases']:>2} matched={ag['matched']:>3} miss={ag['missed_crit']} fp={ag['fp']:>3} strict={ag['avg_strict']:>6.1f}")


if __name__ == "__main__":
    main()
