"""Schema/coerce tests — don't require Claude CLI."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runners.unified_output_schema import (  # noqa: E402
    Finding, RunResult, coerce_finding, load_run_result, VALID_SEVERITIES, CATEGORIES,
)


def test_coerce_basic():
    raw = {
        "id": "T-001",
        "severity": "Критическое",
        "category": "normative",
        "problem": "Test problem",
        "norm": "СП 256, п. 7.4",
        "norm_confidence": 0.8,
        "evidence_quote": "verbatim quote",
        "confidence": 0.9,
    }
    f = coerce_finding(raw, 1)
    assert f.id == "T-001"
    assert f.severity == "КРИТИЧЕСКОЕ"
    assert f.category == "normative"
    assert f.norm_confidence == 0.8
    assert f.confidence == 0.9
    print("test_coerce_basic OK")


def test_coerce_garbage_severity_normalized():
    raw = {"severity": "Junk", "category": "weird", "problem": "x"}
    f = coerce_finding(raw, 5)
    assert f.severity in VALID_SEVERITIES
    assert f.category in CATEGORIES
    assert f.id == "F-005"
    print("test_coerce_garbage_severity_normalized OK")


def test_run_result_roundtrip():
    f = coerce_finding({"severity": "ЭКОНОМИЧЕСКОЕ", "category": "economy",
                        "problem": "Test", "confidence": "0.7"}, 1)
    rr = RunResult(method="current_method", case_id="x", discipline="EOM",
                   model_main="claude-opus-4-7", duration_sec=12.3, findings=[f])
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "out.json"
        rr.save(p)
        loaded = load_run_result(p)
    assert loaded.method == "current_method"
    assert len(loaded.findings) == 1
    assert loaded.findings[0].severity == "ЭКОНОМИЧЕСКОЕ"
    assert loaded.findings[0].confidence == 0.7
    print("test_run_result_roundtrip OK")


def test_finding_with_unknown_keys_no_crash():
    f = coerce_finding({"unknown_field": 42, "severity": "X", "problem": "p"}, 1)
    assert f.severity in VALID_SEVERITIES
    print("test_finding_with_unknown_keys_no_crash OK")


if __name__ == "__main__":
    test_coerce_basic()
    test_coerce_garbage_severity_normalized()
    test_run_result_roundtrip()
    test_finding_with_unknown_keys_no_crash()
    print("\nALL SCHEMA TESTS PASSED")
