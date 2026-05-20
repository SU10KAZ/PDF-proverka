"""
test_phase0_integration.py
--------------------------
Integration test for apply_phase0_dedup() in findings_merge/runner.py.

Validates:
  - Feature flag OFF → returns None, file untouched.
  - Feature flag ON, no dupes → no-op (but writes meta.dedup_report).
  - Feature flag ON, has dupes → fewer findings + meta.dedup_report set.
  - Fail-open: if dedup raises, original findings are preserved.
  - Critical findings never lost across the integration.

Run: python -m pytest tests/findings/dedup/test_phase0_integration.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pytest  # noqa: E402

from backend.app.pipeline.stages.findings_merge import runner as fm_runner  # noqa: E402


def _make_findings_file(tmpdir: Path, items: list[dict]) -> Path:
    out_dir = tmpdir / "_output"
    out_dir.mkdir(parents=True, exist_ok=True)
    fp = out_dir / "03_findings.json"
    fp.write_text(
        json.dumps({"findings": items, "meta": {"total_findings": len(items)}},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return fp


def _f(idx: int, **kw) -> dict:
    base = {
        "id": f"T-{idx:03d}",
        "problem_class": "missing_mandatory_schedule",
        "affected_system": "ВРУ",
        "severity": "ЭКСПЛУАТАЦИОННОЕ",
        "category": "completeness",
        "problem": "Кабельный журнал отсутствует",
        "description": "desc",
        "evidence_quote": "цитата",
        "confidence": 0.8,
    }
    base.update(kw)
    return base


@pytest.fixture
def tmp_project(tmp_path, monkeypatch):
    """Patch _version_output_dir to point to a temp dir."""
    out_dir = tmp_path / "_output"
    out_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(fm_runner, "_version_output_dir", lambda pid: out_dir)
    return tmp_path


def test_flag_off_returns_none(tmp_project, monkeypatch):
    monkeypatch.setattr("backend.app.core.config.STAGE01_DEDUP_ENABLED", False)
    _make_findings_file(tmp_project, [_f(1), _f(2)])
    result = fm_runner.apply_phase0_dedup("dummy-project")
    assert result is None


def test_flag_on_writes_dedup_report(tmp_project, monkeypatch):
    monkeypatch.setattr("backend.app.core.config.STAGE01_DEDUP_ENABLED", True)
    fp = _make_findings_file(tmp_project, [_f(1), _f(2)])
    result = fm_runner.apply_phase0_dedup("dummy-project")
    assert result is not None
    assert result["enabled"] is True
    assert result["before"] == 2
    assert result["after"] == 1, "same class → collapsed"
    assert result["error"] is None
    # File written back
    data = json.loads(fp.read_text(encoding="utf-8"))
    assert len(data["findings"]) == 1
    assert "dedup_report" in data["meta"]


def test_flag_on_no_dupes_writes_meta_no_change(tmp_project, monkeypatch):
    monkeypatch.setattr("backend.app.core.config.STAGE01_DEDUP_ENABLED", True)
    # Semantically distinct findings — class_dedup AND fuzzy_dedup should keep
    # all of them (different problem_class + different content signatures).
    fp = _make_findings_file(tmp_project, [
        _f(1,
            problem_class="outdated_norm_reference",
            category="normative",
            affected_system="ВРУ",
            problem="Устаревшая ссылка на СП 31-110-2003",
            evidence_quote="ev_a"),
        _f(2,
            problem_class="arithmetic_error",
            category="calculations",
            affected_system="ГРЩ",
            problem="Арифметическая ошибка в таблице нагрузок: 56 кВт ≠ 47 кВт",
            evidence_quote="ev_b"),
        _f(3,
            problem_class="missing_diagram",
            category="completeness",
            affected_system="Освещение",
            problem="Однолинейная схема освещения не приведена",
            evidence_quote="ev_c"),
    ])
    result = fm_runner.apply_phase0_dedup("dummy-project")
    assert result["before"] == 3
    assert result["after"] == 3, "semantically distinct → no-op"
    data = json.loads(fp.read_text(encoding="utf-8"))
    assert "dedup_report" in data["meta"]


def test_fail_open_returns_original_on_corrupted_json(tmp_project, monkeypatch):
    monkeypatch.setattr("backend.app.core.config.STAGE01_DEDUP_ENABLED", True)
    out_dir = tmp_project / "_output"
    out_dir.mkdir(parents=True, exist_ok=True)
    fp = out_dir / "03_findings.json"
    fp.write_text("{ this is not valid json", encoding="utf-8")
    result = fm_runner.apply_phase0_dedup("dummy-project")
    assert result is not None
    assert result.get("error") is not None
    # File should not have been overwritten
    assert "this is not valid json" in fp.read_text(encoding="utf-8")


def test_missing_findings_file_returns_none(tmp_project, monkeypatch):
    monkeypatch.setattr("backend.app.core.config.STAGE01_DEDUP_ENABLED", True)
    result = fm_runner.apply_phase0_dedup("dummy-project")
    assert result is None


def test_empty_findings_list_safe(tmp_project, monkeypatch):
    monkeypatch.setattr("backend.app.core.config.STAGE01_DEDUP_ENABLED", True)
    _make_findings_file(tmp_project, [])
    result = fm_runner.apply_phase0_dedup("dummy-project")
    assert result is not None
    assert result["before"] == 0
    assert result["after"] == 0


def test_critical_finding_never_lost(tmp_project, monkeypatch):
    monkeypatch.setattr("backend.app.core.config.STAGE01_DEDUP_ENABLED", True)
    items = [
        _f(1, severity="РЕКОМЕНДАТЕЛЬНОЕ"),
        _f(2, severity="КРИТИЧЕСКОЕ"),
        _f(3, severity="КРИТИЧЕСКОЕ"),
        _f(4, severity="РЕКОМЕНДАТЕЛЬНОЕ"),
    ]
    fp = _make_findings_file(tmp_project, items)
    result = fm_runner.apply_phase0_dedup("dummy-project")
    data = json.loads(fp.read_text(encoding="utf-8"))
    crit_count = sum(1 for f in data["findings"] if f["severity"] == "КРИТИЧЕСКОЕ")
    assert crit_count >= 2, "two КРИТ must survive"
    # critical_collapsed_count > 0 indicates the safeguard fired
    assert result["critical_collapsed_count"] >= 1


def test_dedup_report_meta_fields(tmp_project, monkeypatch):
    monkeypatch.setattr("backend.app.core.config.STAGE01_DEDUP_ENABLED", True)
    fp = _make_findings_file(tmp_project, [_f(1), _f(2)])
    fm_runner.apply_phase0_dedup("dummy-project")
    data = json.loads(fp.read_text(encoding="utf-8"))
    dr = data["meta"]["dedup_report"]
    assert "class_dedup" in dr
    assert "fuzzy_dedup" in dr
    assert "before" in dr
    assert "after" in dr
    assert "critical_collapsed_count" in dr
    assert "fuzzy_threshold" in dr


def test_by_severity_refreshed(tmp_project, monkeypatch):
    monkeypatch.setattr("backend.app.core.config.STAGE01_DEDUP_ENABLED", True)
    items = [_f(1), _f(2), _f(3, severity="КРИТИЧЕСКОЕ")]
    fp = _make_findings_file(tmp_project, items)
    # Pre-set by_severity to wrong values to confirm refresh
    data = json.loads(fp.read_text(encoding="utf-8"))
    data["meta"]["by_severity"] = {"WRONG": 99}
    fp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    fm_runner.apply_phase0_dedup("dummy-project")
    data = json.loads(fp.read_text(encoding="utf-8"))
    assert "WRONG" not in data["meta"]["by_severity"]
    assert "КРИТИЧЕСКОЕ" in data["meta"]["by_severity"]
