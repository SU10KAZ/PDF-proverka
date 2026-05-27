"""test_fallback_to_a0 — when completeness lens fails or returns nothing,
A1 must still produce a usable output equal to (or superset of) A0 findings.

We simulate a Sonnet failure (no parsed_json) and verify:
- A1 runner does not raise;
- post-dedup findings contain current_method findings;
- output schema is intact.

This uses monkeypatching the underlying claude subprocess so no real LLM
call is performed.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESEARCH = HERE.parent
sys.path.insert(0, str(RESEARCH))

import runners._common as common  # noqa: E402
from runners.algorithm_runner import run_A1  # noqa: E402


def t_assert(name, cond, detail=""):
    if cond:
        print(f"OK  {name}")
    else:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)


def main():
    """Use cross_01 (small, has known content); patch run_claude to:
       - return one minimal finding for current_method
       - return empty/failed result for completeness
    Verify that A1 produces at least 1 finding (from current_method)
    and that meta.completeness_findings == 0.
    """
    call_log = {"count": 0, "labels": []}

    class CurFakeResult:
        ok = True
        raw_stdout = ""
        raw_stderr = ""
        parsed_json = {
            "applicability": "applicable",
            "findings": [{
                "id": "F-001",
                "severity": "КРИТИЧЕСКОЕ",
                "category": "calculation",
                "problem": "stub from A0",
                "description": "stub current finding",
                "evidence_quote": "stub",
                "discipline": "EOM",
                "source_agent": "current_method",
                "confidence": 0.9,
            }],
        }
        findings_text = ""
        duration_sec = 0.05
        exit_code = 0
        model = "fake"

    class CompFailResult:
        ok = False
        raw_stdout = ""
        raw_stderr = "simulated lens failure"
        parsed_json = None
        findings_text = ""
        duration_sec = 0.05
        exit_code = 1
        model = "fake"

    def fake_run_claude(*, prompt, model, timeout, label):
        call_log["count"] += 1
        call_log["labels"].append(label)
        if "completeness" in label or "lens_" in label:
            return CompFailResult()
        return CurFakeResult()

    orig = common.run_claude
    common.run_claude = fake_run_claude

    # Force fresh run (don't use cached)
    out_path = common.RESULTS_DIR / "A1_hybrid_lite__v2" / "cross_01_eom_ov_loads.json"
    backup_path = None
    if out_path.exists():
        backup_path = out_path.with_suffix(".json.test-backup")
        out_path.rename(backup_path)
    try:
        res = run_A1("cross_01_eom_ov_loads", "v2", skip_existing=False)
        t_assert("A1 returns result without raising", isinstance(res, dict))
        t_assert("A1 wrote output", out_path.exists())
        data = json.loads(out_path.read_text(encoding="utf-8"))
        meta = data.get("meta") or {}
        t_assert("completeness_findings == 0 on lens fail",
                 meta.get("completeness_findings") == 0,
                 f"got {meta.get('completeness_findings')}")
        t_assert("current_method_findings >= 1 from stub",
                 (meta.get("current_method_findings") or 0) >= 1,
                 f"got {meta.get('current_method_findings')}")
        t_assert("output findings list non-empty",
                 len(data.get("findings") or []) >= 1,
                 f"got {len(data.get('findings') or [])}")
    finally:
        common.run_claude = orig
        try:
            out_path.unlink()
        except FileNotFoundError:
            pass
        if backup_path and backup_path.exists():
            backup_path.rename(out_path)
    print("\ntest_fallback_to_a0 PASSED")


if __name__ == "__main__":
    main()
