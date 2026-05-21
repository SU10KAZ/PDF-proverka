"""Compare metric tests — use synthetic results, no Claude needed."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.compare_results import (  # noqa: E402
    evaluate_case, compare_dataset, _detect_internal_dupes,
)
from runners.unified_output_schema import RunResult, Finding, coerce_finding  # noqa: E402


def _make_result(method: str, case_id: str, findings_raw: list[dict]) -> dict:
    findings = [coerce_finding(r, i) for i, r in enumerate(findings_raw, start=1)]
    rr = RunResult(method=method, case_id=case_id, discipline="TEST",
                   model_main="claude-opus-4-7", duration_sec=10.0, findings=findings)
    return rr.to_dict()


def _write(p: Path, obj: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def test_full_match_high_score():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        ds = root / "datasets" / "case_a"
        ds.mkdir(parents=True)
        (ds / "case.json").write_text(json.dumps({"id": "case_a", "discipline": "TEST"}))
        (ds / "ground_truth.json").write_text(json.dumps({
            "case_id": "case_a",
            "expected_findings": [
                {"id": "GT-01", "severity": "КРИТИЧЕСКОЕ", "is_critical": True,
                 "description": "Кабель занижен по сечению, 95 мм² при токе 302 А",
                 "must_match_substring": "95"},
            ],
        }, ensure_ascii=False))
        res = root / "results" / "case_a"
        _write(res / "current.json",
               _make_result("current_method", "case_a", [
                   {"severity": "КРИТИЧЕСКОЕ", "category": "calculation",
                    "problem": "Кабель 95 мм² не проходит по току",
                    "description": "Сечение 95 мм², ток 302 А — недостаточно",
                    "evidence_quote": "АВВГ 4x95"},
               ]))
        out = root / "out"
        result = compare_dataset(root / "datasets", root / "results", out)
        row = result["table_rows"][0]
        assert row["current_score"] > 50, row
        print(f"test_full_match_high_score OK (score={row['current_score']})")


def test_missed_critical_penalized():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        ds = root / "datasets" / "case_b"
        ds.mkdir(parents=True)
        (ds / "case.json").write_text(json.dumps({"id": "case_b", "discipline": "TEST"}))
        (ds / "ground_truth.json").write_text(json.dumps({
            "case_id": "case_b",
            "expected_findings": [
                {"id": "GT-01", "severity": "КРИТИЧЕСКОЕ", "is_critical": True,
                 "description": "X", "must_match_substring": "VERY_RARE_TOKEN"},
            ],
        }))
        res = root / "results" / "case_b"
        _write(res / "current.json",
               _make_result("current_method", "case_b", [
                   {"severity": "РЕКОМЕНДАТЕЛЬНОЕ", "category": "other",
                    "problem": "irrelevant", "description": "nothing related"},
               ]))
        result = compare_dataset(root / "datasets", root / "results", root / "out")
        row = result["table_rows"][0]
        assert row["missed_critical_current"] == 1, row
        assert row["current_score"] < 0, row
        print(f"test_missed_critical_penalized OK (score={row['current_score']})")


def test_dedup_detection():
    findings = [
        coerce_finding({"severity": "К", "category": "calc",
                        "problem": "Кабель 95 мм² не проходит"}, 1),
        coerce_finding({"severity": "К", "category": "calc",
                        "problem": "Кабель 95 мм² не проходит по току"}, 2),
        coerce_finding({"severity": "Э", "category": "norm",
                        "problem": "Совершенно другое замечание про норму"}, 3),
    ]
    dupes = _detect_internal_dupes(findings)
    assert dupes == 1, f"expected 1 dupe, got {dupes}"
    print("test_dedup_detection OK")


if __name__ == "__main__":
    test_full_match_high_score()
    test_missed_critical_penalized()
    test_dedup_detection()
    print("\nALL COMPARE TESTS PASSED")
