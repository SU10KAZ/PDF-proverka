"""audit_a1v2_fp — classifies A1-v2 false positives as either:
  - 'beyond_gt_useful'   — real engineering finding not in GT (a positive)
  - 'duplicate_of_gt'    — covers a GT but did not match by substring
  - 'speculative_noise'  — speculative / not actionable / vague
  - 'wrong_severity'     — could be downgraded
  - 'unknown'            — needs human review

Quick heuristic — used to estimate whether the FP regression is real noise
or just unflagged beyond-GT engineering value.

Output:
  algorithm_research/reports/a1v2_fp_audit.md
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from statistics import median

HERE = Path(__file__).resolve()
RESEARCH = HERE.parents[1]
RESULTS = RESEARCH / "results"
A1_V2 = RESULTS / "A1_hybrid_lite__v2"
DATASETS = RESEARCH.parent / "datasets"
REPORT_PATH = RESEARCH / "reports" / "a1v2_fp_audit.md"


def _gt_substrings(case_id: str) -> set[str]:
    gt = json.loads((DATASETS / case_id / "ground_truth.json").read_text(encoding="utf-8"))
    return {(g.get("must_match_substring") or "").lower() for g in gt.get("expected_findings", [])
            if not g.get("is_trap")}


def _classify_finding(f: dict, gt_subs: set[str]) -> str:
    desc = (f.get("description") or "") + " " + (f.get("problem") or "")
    desc_l = desc.lower()
    # Match GT by substring
    if any(s and s in desc_l for s in gt_subs if s):
        return "duplicate_of_gt"
    if f.get("is_beyond_gt_useful"):
        return "beyond_gt_useful"
    sev = (f.get("severity") or "").upper()
    weak_words = ["возможно", "может быть", "рекомендуется проверить", "проверить",
                   "может потребоваться", "следует уточнить"]
    if any(w in desc_l for w in weak_words):
        return "speculative_noise"
    if sev in ("РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ"):
        return "wrong_severity"
    return "beyond_gt_useful"   # default lean: assume genuine if specific


def audit_case(case_path: Path) -> dict:
    data = json.loads(case_path.read_text(encoding="utf-8"))
    case_id = data.get("case_id") or case_path.stem
    gt_subs = _gt_substrings(case_id)
    counts: dict[str, int] = {
        "duplicate_of_gt": 0, "beyond_gt_useful": 0,
        "speculative_noise": 0, "wrong_severity": 0, "unknown": 0,
    }
    examples: dict[str, list[str]] = {k: [] for k in counts}
    for f in data.get("findings") or []:
        cls = _classify_finding(f, gt_subs)
        counts[cls] = counts.get(cls, 0) + 1
        if len(examples[cls]) < 3:
            examples[cls].append(f.get("problem") or f.get("description", "")[:160])
    total = sum(counts.values())
    return {
        "case_id": case_id,
        "n_findings": total,
        "counts": counts,
        "ratio": {k: round(v / total, 2) if total else 0 for k, v in counts.items()},
        "examples": examples,
    }


def main():
    if not A1_V2.exists():
        print(f"no A1-v2 dir at {A1_V2}", file=sys.stderr)
        sys.exit(1)
    rows = []
    for f in sorted(A1_V2.glob("*.json")):
        rows.append(audit_case(f))

    lines: list[str] = ["# A1-v2 False-Positive Audit", "",
                        "Heuristic classification of A1-v2 findings against GT substrings.",
                        "**NOT a substitute for expert manual review** — just a triage signal.",
                        "",
                        "## Per-case summary", "",
                        "| Case | total | dup_of_gt | beyond_useful | spec_noise | wrong_sev | unknown |",
                        "|---|---|---|---|---|---|---|"]
    for r in rows:
        c = r["counts"]
        lines.append(
            f"| {r['case_id']} | {r['n_findings']} | {c.get('duplicate_of_gt')} | "
            f"{c.get('beyond_gt_useful')} | {c.get('speculative_noise')} | "
            f"{c.get('wrong_severity')} | {c.get('unknown')} |"
        )

    lines.append("")
    lines.append("## Examples (first 3 per class)")
    for r in rows:
        lines.append(f"\n### {r['case_id']}")
        for cls, exs in r["examples"].items():
            if not exs:
                continue
            lines.append(f"\n**{cls}** ({len(exs)})")
            for e in exs:
                lines.append(f"- {e[:200]}")

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Audited {len(rows)} cases. Wrote {REPORT_PATH}")
    for r in rows:
        c = r["counts"]
        print(f"  {r['case_id']:35s} dup={c['duplicate_of_gt']:>2} useful={c['beyond_gt_useful']:>2}"
              f" noise={c['speculative_noise']:>2} wrong_sev={c['wrong_severity']:>2}")


if __name__ == "__main__":
    main()
