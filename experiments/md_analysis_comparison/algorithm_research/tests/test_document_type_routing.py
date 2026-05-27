"""Tests for document_type routing.

Validates:
1. All 8 case.json files have a valid `document_type` and signal flags.
2. completeness prompts (v1/v2) carry a `{DOCUMENT_TYPE}` placeholder and a
   document-type routing section.
3. The shared `run_lens` helper substitutes `{DOCUMENT_TYPE}` properly.
4. The default fallback when document_type is missing is `full_rd`.
5. cross_01 (audit_comparison) and multi_01 (tz_vs_rd) are properly tagged.

No LLM calls. Pure assertions on prompt files and case metadata.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESEARCH_ROOT = HERE.parent
sys.path.insert(0, str(RESEARCH_ROOT))

from runners._common import load_prompt, run_lens  # noqa: E402

EXP_ROOT = RESEARCH_ROOT.parent
DATASETS = EXP_ROOT / "datasets"

ALLOWED_DOC_TYPES = {"full_rd", "audit_comparison", "tz_vs_rd", "specification_only"}

KNOWN_TAGS = {
    "cross_01_eom_ov_loads": "audit_comparison",
    "multi_01_tz_vs_rd": "tz_vs_rd",
}


def t_assert(name, cond, detail=""):
    if cond:
        print(f"OK  {name}")
    else:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)


def test_cases_have_document_type():
    missing, bad = [], []
    for case_dir in sorted(DATASETS.iterdir()):
        if not case_dir.is_dir():
            continue
        case_json = case_dir / "case.json"
        if not case_json.exists():
            continue
        data = json.loads(case_json.read_text(encoding="utf-8"))
        dt = data.get("document_type")
        if not dt:
            missing.append(case_dir.name)
        elif dt not in ALLOWED_DOC_TYPES:
            bad.append((case_dir.name, dt))
        for flag in ("has_cross_discipline", "has_completeness_gaps",
                     "has_calculation_errors", "has_normative_errors",
                     "has_hidden_contradictions"):
            t_assert(f"case[{case_dir.name}].{flag} is bool",
                     isinstance(data.get(flag), bool),
                     f"got {data.get(flag)!r}")
    t_assert("no case missing document_type", not missing, f"missing in {missing}")
    t_assert("no case has invalid document_type", not bad, f"bad: {bad}")


def test_known_tags():
    for cid, expected in KNOWN_TAGS.items():
        p = DATASETS / cid / "case.json"
        data = json.loads(p.read_text(encoding="utf-8"))
        t_assert(f"{cid}.document_type == {expected}",
                 data.get("document_type") == expected,
                 f"got {data.get('document_type')}")


def test_prompts_have_document_type_placeholder():
    for prompt_set in ("v1", "v2"):
        prompt = load_prompt(prompt_set, "completeness")
        t_assert(f"{prompt_set} completeness has {{DOCUMENT_TYPE}}",
                 "{DOCUMENT_TYPE}" in prompt,
                 "placeholder missing")
        t_assert(f"{prompt_set} completeness has routing section",
                 "Document-type routing" in prompt or "document-type routing" in prompt.lower(),
                 "routing section missing")
        for dt in ("full_rd", "audit_comparison", "tz_vs_rd", "specification_only"):
            t_assert(f"{prompt_set} completeness mentions {dt}",
                     dt in prompt,
                     f"{dt} not described")


def test_run_lens_substitutes_document_type(monkeypatch_target):
    """Capture the prompt that run_lens would send, verify substitution.

    We monkeypatch `run_claude` at the module level to capture the prompt
    instead of actually calling Claude.
    """
    captured = {}
    import runners._common as common_mod

    class FakeResult:
        ok = True
        raw_stdout = ""
        raw_stderr = ""
        parsed_json = {"applicability": "applicable", "findings": []}
        findings_text = ""
        duration_sec = 0.01
        exit_code = 0
        model = "test"

    def fake_run_claude(*, prompt, model, timeout, label):
        captured["prompt"] = prompt
        return FakeResult()

    orig = common_mod.run_claude
    common_mod.run_claude = fake_run_claude
    try:
        run_lens(
            lens="completeness", md="### test md\n",
            discipline="EOM", prompt_set="v2", case_id="test-case",
            checklist="### test checklist\n",
            document_type="audit_comparison",
        )
        prompt = captured.get("prompt", "")
        t_assert("prompt contains audit_comparison", "audit_comparison" in prompt,
                 prompt[:200])
        t_assert("prompt has no unsubstituted {DOCUMENT_TYPE}",
                 "{DOCUMENT_TYPE}" not in prompt,
                 "placeholder still present")
    finally:
        common_mod.run_claude = orig


def test_run_lens_defaults_to_full_rd_when_missing(monkeypatch_target):
    captured = {}
    import runners._common as common_mod

    class FakeResult:
        ok = True
        raw_stdout = ""
        raw_stderr = ""
        parsed_json = {"applicability": "applicable", "findings": []}
        findings_text = ""
        duration_sec = 0.01
        exit_code = 0
        model = "test"

    def fake_run_claude(*, prompt, model, timeout, label):
        captured["prompt"] = prompt
        return FakeResult()

    orig = common_mod.run_claude
    common_mod.run_claude = fake_run_claude
    try:
        run_lens(
            lens="completeness", md="### test\n",
            discipline="OV", prompt_set="v2", case_id="test-default",
            checklist="### checklist\n",
            document_type=None,
        )
        prompt = captured["prompt"]
        t_assert("default document_type falls back to full_rd",
                 "full_rd" in prompt and "{DOCUMENT_TYPE}" not in prompt,
                 prompt[:200])
    finally:
        common_mod.run_claude = orig


def main():
    test_cases_have_document_type()
    test_known_tags()
    test_prompts_have_document_type_placeholder()
    test_run_lens_substitutes_document_type(None)
    test_run_lens_defaults_to_full_rd_when_missing(None)
    print("\nAll document_type_routing tests passed.")


if __name__ == "__main__":
    main()
