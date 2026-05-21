"""Validate that every dataset case is well-formed:
- case.json exists with id/discipline/md_file
- input.md exists
- ground_truth.json has expected_findings with required fields
- each must_match_substring is actually present in input.md
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from configs import config as cfg  # noqa: E402


def test_all_cases_well_formed():
    cases = [p for p in cfg.DATASETS_DIR.iterdir() if p.is_dir()]
    assert len(cases) >= 6, f"expected >=6 cases, got {len(cases)}"
    issues: list[str] = []
    for c in cases:
        ci = c / "case.json"
        md = c / "input.md"
        gt = c / "ground_truth.json"
        for f in (ci, md, gt):
            if not f.exists():
                issues.append(f"{c.name}: missing {f.name}")
                continue
        if not all(p.exists() for p in (ci, md, gt)):
            continue
        info = json.loads(ci.read_text(encoding="utf-8"))
        for key in ("id", "discipline", "md_file"):
            if key not in info:
                issues.append(f"{c.name}/case.json: missing key '{key}'")
        gt_data = json.loads(gt.read_text(encoding="utf-8"))
        if "expected_findings" not in gt_data:
            issues.append(f"{c.name}/ground_truth.json: missing expected_findings")
            continue
        md_text = md.read_text(encoding="utf-8")
        for ef in gt_data["expected_findings"]:
            sub = ef.get("must_match_substring", "")
            if sub and sub not in md_text:
                issues.append(f"{c.name}/{ef.get('id','?')}: substring '{sub}' not in input.md")
            for k in ("severity", "description"):
                if k not in ef:
                    issues.append(f"{c.name}/{ef.get('id','?')}: missing key '{k}'")
    if issues:
        print("DATASET INTEGRITY ISSUES:")
        for i in issues:
            print("  -", i)
        raise AssertionError(f"{len(issues)} dataset integrity issues")
    print(f"test_all_cases_well_formed OK ({len(cases)} cases)")


if __name__ == "__main__":
    test_all_cases_well_formed()
    print("\nALL DATASET INTEGRITY TESTS PASSED")
