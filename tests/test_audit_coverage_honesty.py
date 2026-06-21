"""reserc.md #96 (дешёвые метрики) — тесты честности покрытия.

Чистые функции read_findings_coverage / aggregate_coverage (без обхода реального
парка). Проверяем классификацию проектов и агрегацию.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

_MOD = Path(__file__).resolve().parent.parent / "scripts" / "audit_coverage_honesty.py"
_spec = importlib.util.spec_from_file_location("audit_coverage_honesty", _MOD)
ach = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ach)


def _write_findings(tmp_path, payload):
    out = tmp_path / "_output"
    out.mkdir(parents=True, exist_ok=True)
    p = out / "03_findings.json"
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_no_findings(tmp_path):
    res = ach.read_findings_coverage(tmp_path / "_output" / "03_findings.json")
    assert res["status"] == "no_findings"


def test_legacy_no_coverage(tmp_path):
    p = _write_findings(tmp_path, {"findings": [{"id": "F-1"}]})  # без analysis_coverage
    assert ach.read_findings_coverage(p)["status"] == "no_coverage"


def test_full_coverage(tmp_path):
    p = _write_findings(tmp_path, {
        "findings": [],
        "analysis_coverage": {"summary": {"excluded_from_full_analysis_count": 0}},
    })
    assert ach.read_findings_coverage(p)["status"] == "full"


def test_partial_coverage(tmp_path):
    p = _write_findings(tmp_path, {
        "analysis_coverage": {"summary": {
            "excluded_from_full_analysis_count": 3,
            "gemma_uncovered_count": 1,
            "single_block_failed_count": 2,
        }},
    })
    res = ach.read_findings_coverage(p)
    assert res["status"] == "partial"
    assert res["summary"]["excluded_from_full_analysis_count"] == 3


def test_broken_json(tmp_path):
    out = tmp_path / "_output"
    out.mkdir()
    (out / "03_findings.json").write_text("{ broken", encoding="utf-8")
    assert ach.read_findings_coverage(out / "03_findings.json")["status"] == "error"


def test_aggregate_counts_and_honesty():
    items = [
        {"project_id": "a", "status": "full", "summary": {"base_gemma_total_count": 10}},
        {"project_id": "b", "status": "partial", "summary": {
            "excluded_from_full_analysis_count": 4, "gemma_uncovered_count": 1,
            "single_block_failed_count": 3}},
        {"project_id": "c", "status": "no_coverage", "summary": {}},
        {"project_id": "d", "status": "no_findings", "summary": {}},
        {"project_id": "e", "status": "error", "summary": {}},
    ]
    rep = ach.aggregate_coverage(items)
    c = rep["counts"]
    assert c["projects_total"] == 5
    assert c["with_findings"] == 4          # d (no_findings) исключён
    assert c["with_coverage_metadata"] == 2  # a + b
    assert c["legacy_no_coverage"] == 1      # c
    assert c["errors"] == 1                  # e
    assert c["full"] == 1 and c["partial"] == 1
    assert rep["totals"]["excluded_from_full_analysis_count"] == 4
    # доля проверяемого покрытия = 2 (с coverage) / 4 (с findings) = 0.5
    assert rep["coverage_metadata_ratio"] == 0.5
    # worst offender — проект b
    assert rep["worst_offenders"][0]["project_id"] == "b"
    assert rep["worst_offenders"][0]["excluded"] == 4
