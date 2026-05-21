"""build_phase_comparison — emits the Phase 0 / Phase 1 comparison report.

Reads `algorithm_research/results/_scores.json` and writes:
  algorithm_research/reports/_phase_comparison.md
  algorithm_research/reports/_phase_comparison.json

The report has three sections:
  1) Algorithm-level aggregate comparison.
  2) Per-case overlap matrix (which cases each algorithm was evaluated on).
  3) Coverage notes (which cases are NOT yet evaluated, what's pending).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
RESEARCH = HERE.parents[1]
SCORES = RESEARCH / "results" / "_scores.json"
REPORTS_DIR = RESEARCH / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def main():
    if not SCORES.exists():
        print(f"ERROR: {SCORES} missing — run metrics/score_algorithms.py first",
              file=sys.stderr)
        sys.exit(1)
    data = json.loads(SCORES.read_text(encoding="utf-8"))
    rows = data.get("rows") or []
    agg = data.get("aggregates") or {}

    # Per-algorithm aggregate row
    lines: list[str] = ["# Phase 0 / Phase 1 Comparison", ""]
    lines.append("## 1. Aggregate Comparison")
    lines.append("")
    lines.append("| Algorithm | Cases | matched_gt | missed_crit | FP | dupes | beyond | strict | recall | balanced | cost_aware | human | avg_sec |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for key in sorted(agg.keys()):
        a = agg[key]
        sc = a.get("avg_score") or {}
        lines.append(
            f"| {key} | {a.get('cases')} | {a.get('matched_gt')} | {a.get('missed_critical')} | "
            f"{a.get('false_positives')} | {a.get('duplicates_internal')} | "
            f"{a.get('beyond_gt')} | {sc.get('strict_production','-')} | "
            f"{sc.get('recall_priority','-')} | {sc.get('balanced_engineering','-')} | "
            f"{sc.get('cost_aware','-')} | {sc.get('human_review_load','-')} | "
            f"{a.get('avg_cost_sec','-')} |"
        )

    # Per-case coverage matrix
    case_alg_map: dict[str, set] = {}
    for r in rows:
        case_alg_map.setdefault(r["case_id"], set()).add(f"{r['algorithm']}__{r['prompt_set']}")

    all_cases = sorted(case_alg_map.keys())
    all_algos = sorted({f"{r['algorithm']}__{r['prompt_set']}" for r in rows})
    lines.append("")
    lines.append("## 2. Coverage matrix (case × algorithm)")
    lines.append("")
    lines.append("| Case | " + " | ".join(all_algos) + " |")
    lines.append("|" + "|".join(["---"] * (len(all_algos) + 1)) + "|")
    for c in all_cases:
        row = [c]
        for a in all_algos:
            row.append("✓" if a in case_alg_map.get(c, set()) else "·")
        lines.append("| " + " | ".join(row) + " |")

    # Pending coverage
    DATASETS = RESEARCH.parent / "datasets"
    dataset_cases = sorted([p.name for p in DATASETS.iterdir() if p.is_dir()])
    pending_cases = sorted(set(dataset_cases) - set(all_cases))
    lines.append("")
    lines.append(f"## 3. Coverage gaps")
    lines.append("")
    lines.append(f"- Total dataset cases:  **{len(dataset_cases)}**")
    lines.append(f"- Cases with at least one algorithm output: **{len(all_cases)}**")
    lines.append(f"- Cases with zero algorithm output: **{len(pending_cases)}**")
    if pending_cases:
        lines.append("")
        lines.append("Pending cases (no results yet):")
        for c in pending_cases:
            lines.append(f"  - {c}")

    out_md = REPORTS_DIR / "_phase_comparison.md"
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    out_json = REPORTS_DIR / "_phase_comparison.json"
    out_json.write_text(json.dumps({
        "aggregates": agg,
        "rows": rows,
        "coverage_matrix": {c: sorted(case_alg_map.get(c, set())) for c in all_cases},
        "pending_cases": pending_cases,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {out_md} ({len(lines)} lines)")
    print(f"Wrote {out_json}")


if __name__ == "__main__":
    main()
