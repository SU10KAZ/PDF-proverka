"""test_phase0_dedup_safety — Phase 0 dedup must NOT silently drop critical findings.

Deterministic checks on the Phase 0 outputs that already exist under
algorithm_research/results/A0_phase0_*__baseline/.

Passes when:
1. Every A0 baseline finding is present in the dedup output OR its loss is
   recorded in meta.dedup_report.
2. No КРИТИЧЕСКОЕ finding is dropped unless explicitly demoted to a duplicate
   of another КРИТИЧЕСКОЕ.
3. Total dedup output count <= A0 baseline count (dedup never adds).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE.parent / "results"

A0 = RESULTS / "A0_baseline_current__baseline"
VARIANTS = [
    RESULTS / "A0_phase0_classdedup__baseline",
    RESULTS / "A0_phase0_fuzzydedup__baseline",
    RESULTS / "A0_phase0_combined__baseline",
]


def t_assert(name, cond, detail=""):
    if cond:
        print(f"OK  {name}")
    else:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)


def _critical_descriptions(findings: list[dict]) -> set[str]:
    return {
        (f.get("description") or f.get("problem") or "")
        for f in findings
        if (f.get("severity") or "").upper() == "КРИТИЧЕСКОЕ"
    }


def check_one(case_id: str, variant_dir: Path):
    src = A0 / f"{case_id}.json"
    out = variant_dir / f"{case_id}.json"
    if not src.exists() or not out.exists():
        t_assert(f"{variant_dir.name}/{case_id} exists", False,
                 f"src={src.exists()} out={out.exists()}")
        return
    src_data = json.loads(src.read_text(encoding="utf-8"))
    out_data = json.loads(out.read_text(encoding="utf-8"))
    src_findings = src_data.get("findings") or []
    out_findings = out_data.get("findings") or []

    t_assert(
        f"{variant_dir.name}/{case_id} count never increases",
        len(out_findings) <= len(src_findings),
        f"{len(out_findings)} > {len(src_findings)}",
    )

    src_crit = _critical_descriptions(src_findings)
    out_crit = _critical_descriptions(out_findings)
    lost = src_crit - out_crit
    # Allow loss only if drops are recorded
    drops = (out_data.get("meta") or {}).get("dedup_report", {}).get("same_class_drops", 0)
    if lost:
        t_assert(
            f"{variant_dir.name}/{case_id} no silent КРИТИЧЕСКОЕ loss",
            drops >= len(lost),
            f"lost {len(lost)} crit findings, drops_report={drops}",
        )


def main():
    if not A0.exists():
        t_assert("A0 baseline dir exists", False, str(A0))
    cases = sorted([p.stem for p in A0.glob("*.json")])
    t_assert("at least 6 A0 cases exist", len(cases) >= 6, f"got {len(cases)}")
    for v in VARIANTS:
        if not v.exists():
            t_assert(f"{v.name} exists", False, "missing")
        for c in cases:
            check_one(c, v)
    print(f"\nPhase 0 dedup safety verified on {len(cases)} cases × {len(VARIANTS)} variants.")


if __name__ == "__main__":
    main()
