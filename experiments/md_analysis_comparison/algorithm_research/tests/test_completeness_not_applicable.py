"""test_completeness_not_applicable — completeness lens must be allowed to
return applicability=not_applicable with 0 findings.

Verifies the prompt rules allow it, and that run_lens correctly drops findings
when applicability=not_applicable.
"""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESEARCH = HERE.parent
sys.path.insert(0, str(RESEARCH))

from runners._common import load_prompt, run_lens  # noqa: E402


def t_assert(name, cond, detail=""):
    if cond:
        print(f"OK  {name}")
    else:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)


def test_prompts_support_not_applicable():
    for prompt_set in ("v1", "v2"):
        prompt = load_prompt(prompt_set, "completeness")
        t_assert(f"{prompt_set} mentions applicability",
                 "applicability" in prompt or "applicable" in prompt,
                 "no applicability instructions")
        t_assert(f"{prompt_set} mentions not_applicable",
                 "not_applicable" in prompt, "not_applicable not described")


def test_run_lens_drops_findings_when_not_applicable():
    import runners._common as common_mod

    class FakeResult:
        ok = True
        raw_stdout = ""
        raw_stderr = ""
        parsed_json = {
            "applicability": "not_applicable",
            "findings": [
                {"problem": "should be dropped", "severity": "КРИТИЧЕСКОЕ",
                 "description": "x", "category": "completeness"}
            ],
        }
        findings_text = ""
        duration_sec = 0.1
        exit_code = 0
        model = "test"

    orig = common_mod.run_claude
    common_mod.run_claude = lambda **kw: FakeResult()
    try:
        findings, _, _ = run_lens(
            lens="completeness", md="### md\n",
            discipline="AR", prompt_set="v2", case_id="test-na",
            checklist="### checklist\n",
            document_type="specification_only",
        )
        t_assert("findings list empty when applicability=not_applicable",
                 len(findings) == 0, f"got {len(findings)}")
    finally:
        common_mod.run_claude = orig


def main():
    test_prompts_support_not_applicable()
    test_run_lens_drops_findings_when_not_applicable()
    print("\ntest_completeness_not_applicable PASSED")


if __name__ == "__main__":
    main()
