"""build_per_case_delta — emits per-case A0 vs A1-v2 comparison.

Output:
  algorithm_research/reports/_per_case_delta.md
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
RESEARCH = HERE.parents[1]
SCORES = RESEARCH / "results" / "_scores.json"
DATASETS = RESEARCH.parent / "datasets"
REPORT_PATH = RESEARCH / "reports" / "_per_case_delta.md"


def main():
    data = json.loads(SCORES.read_text(encoding="utf-8"))
    rows = data.get("rows") or []

    a0_by_case = {r["case_id"]: r for r in rows if r["algorithm"] == "A0_baseline_current"}
    v2_by_case = {r["case_id"]: r for r in rows if r["algorithm"] == "A1_hybrid_lite" and r["prompt_set"] == "v2"}
    v1_by_case = {r["case_id"]: r for r in rows if r["algorithm"] == "A1_hybrid_lite" and r["prompt_set"] == "v1"}

    # Load document_type for each case
    dt_by_case = {}
    for case_dir in DATASETS.iterdir():
        cj = case_dir / "case.json"
        if cj.exists():
            d = json.loads(cj.read_text(encoding="utf-8"))
            dt_by_case[case_dir.name] = d.get("document_type", "?")

    lines = ["# Per-Case Delta (A0 vs A1-v2)", "",
             "Side-by-side A0 baseline vs A1-v2 candidate per case.",
             "Only cases present in BOTH algorithms are shown.",
             "",
             "| Case | doc_type | matched (A0→v2) | missed_crit (A0→v2) | FP (A0→v2) | total (A0→v2) | strict (A0→v2) | Δstrict |",
             "|---|---|---|---|---|---|---|---|"]
    cases_both = sorted(set(a0_by_case) & set(v2_by_case))
    for c in cases_both:
        a0 = a0_by_case[c]
        v2 = v2_by_case[c]
        dt = dt_by_case.get(c, "?")
        s0 = a0["scores"]["strict_production"]
        s2 = v2["scores"]["strict_production"]
        lines.append(
            f"| {c} | {dt} | {a0['matched_gt']}→{v2['matched_gt']} | "
            f"{a0['missed_critical']}→{v2['missed_critical']} | "
            f"{a0['false_positives']}→{v2['false_positives']} | "
            f"{a0['total_findings']}→{v2['total_findings']} | "
            f"{s0:.1f}→{s2:.1f} | {s2-s0:+.1f} |"
        )

    # Cases with v2 only
    v2_only = sorted(set(v2_by_case) - set(a0_by_case))
    if v2_only:
        lines.append("\n## Cases with A1-v2 only (no A0 baseline)")
        lines.append("")
        lines.append("| Case | doc_type | matched | missed_crit | FP | total | strict |")
        lines.append("|---|---|---|---|---|---|---|")
        for c in v2_only:
            v2 = v2_by_case[c]
            dt = dt_by_case.get(c, "?")
            lines.append(
                f"| {c} | {dt} | {v2['matched_gt']} | "
                f"{v2['missed_critical']} | {v2['false_positives']} | "
                f"{v2['total_findings']} | {v2['scores']['strict_production']} |"
            )

    # v1 vs v2 head-to-head
    cases_v1_v2 = sorted(set(v1_by_case) & set(v2_by_case))
    if cases_v1_v2:
        lines.append("\n## v1 vs v2 head-to-head (cases with both)")
        lines.append("")
        lines.append("| Case | matched (v1→v2) | missed_crit (v1→v2) | FP (v1→v2) | strict (v1→v2) |")
        lines.append("|---|---|---|---|---|")
        for c in cases_v1_v2:
            v1 = v1_by_case[c]
            v2 = v2_by_case[c]
            lines.append(
                f"| {c} | {v1['matched_gt']}→{v2['matched_gt']} | "
                f"{v1['missed_critical']}→{v2['missed_critical']} | "
                f"{v1['false_positives']}→{v2['false_positives']} | "
                f"{v1['scores']['strict_production']:.1f}→{v2['scores']['strict_production']:.1f} |"
            )

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {REPORT_PATH}")
    for c in cases_both:
        a0 = a0_by_case[c]; v2 = v2_by_case[c]
        delta = v2['scores']['strict_production'] - a0['scores']['strict_production']
        print(f"  {c:35s} A0={a0['scores']['strict_production']:.1f} v2={v2['scores']['strict_production']:.1f} delta={delta:+.1f}")


if __name__ == "__main__":
    main()
