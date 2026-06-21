"""reserc.md #36/#37 — доверие native paragraph cache + метрики верификации.

#36: verified_via="native_python" теперь доверенный префикс (после #34 native
читает authoritative индекс напрямую) → trusted_skipped работает.
#37: merge_llm_norm_results выносит verified_true/false/total + by_source в
norm_checks.meta и возвращает счётчики для лога runner.
"""
from __future__ import annotations

import json

from backend.app.pipeline.stages.norms import _core


def test_native_python_is_trusted_paragraph_entry():
    # #36: native-запись считается доверенной (раньше — нет → лишняя ре-верификация).
    assert _core._is_trusted_paragraph_entry({"verified_via": "native_python"}) is True
    assert _core._is_trusted_paragraph_entry({"verified_via": "norms_mcp_paragraph"}) is True
    # легаси websearch — по-прежнему НЕ доверенный
    assert _core._is_trusted_paragraph_entry({"verified_via": "websearch"}) is False


def test_merge_surfaces_paragraph_verification_metrics(tmp_path, monkeypatch):
    # #37: метрики подтверждения цитат попадают в meta и в return-stats.
    monkeypatch.setattr(
        _core, "merge_paragraph_checks",
        lambda checks, project_id="x": {"added": 0, "updated": 0},
    )
    det = tmp_path / "norm_checks.json"
    llm = tmp_path / "norm_checks_llm.json"
    det.write_text(
        json.dumps({"meta": {"project_id": "p"}, "checks": [{"status": "active"}]}),
        encoding="utf-8",
    )
    llm.write_text(json.dumps({"checks": [], "paragraph_checks": [
        {"finding_id": "F-1", "paragraph_verified": True, "verified_via": "native_python"},
        {"finding_id": "F-2", "paragraph_verified": False, "verified_via": "native_python"},
        {"finding_id": "F-3", "paragraph_verified": True, "verified_via": "norms_mcp_paragraph"},
    ]}), encoding="utf-8")

    stats = _core.merge_llm_norm_results(det, llm)
    assert stats["paragraph_verified_true"] == 2
    assert stats["paragraph_verified_total"] == 3

    pv = json.loads(det.read_text(encoding="utf-8"))["meta"]["paragraph_verification"]
    assert pv["verified_true"] == 2
    assert pv["verified_false"] == 1
    assert pv["total"] == 3
    assert pv["by_source"] == {"native_python": 2, "norms_mcp_paragraph": 1}


def test_merge_no_paragraph_checks_metrics_zero(tmp_path, monkeypatch):
    monkeypatch.setattr(
        _core, "merge_paragraph_checks",
        lambda checks, project_id="x": {"added": 0, "updated": 0},
    )
    det = tmp_path / "norm_checks.json"
    llm = tmp_path / "norm_checks_llm.json"
    det.write_text(json.dumps({"meta": {}, "checks": []}), encoding="utf-8")
    llm.write_text(json.dumps({"checks": [], "paragraph_checks": []}), encoding="utf-8")
    stats = _core.merge_llm_norm_results(det, llm)
    assert stats["paragraph_verified_total"] == 0
    pv = json.loads(det.read_text(encoding="utf-8"))["meta"]["paragraph_verification"]
    assert pv == {"verified_true": 0, "verified_false": 0, "total": 0, "by_source": {}}
