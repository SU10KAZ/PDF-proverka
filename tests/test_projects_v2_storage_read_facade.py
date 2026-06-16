"""
Тесты storage_read_facade: выбор backend, default legacy, projects_v2 не
используется по умолчанию, production не подключён.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
from backend.app.services.storage import storage_read_facade as F  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _mini_v2(tmp_path):
    v2 = tmp_path / "projects_v2"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": "x", "display_name": "213", "folder_name": OBJF})
    doc = v2 / "objects" / OBJF / "disciplines" / "AI" / "documents" / "doc-c"
    _wj(doc / "document.json", {"document_code": "doc-c", "object_id": "x",
                                "discipline": "AI", "kind": "plain",
                                "versions": [{"version_id": "v001", "version_no": 1}],
                                "current_version": "v001"})
    (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
    _wj(doc / "versions/v001/version.json",
        {"version_id": "v001", "version_no": 1, "analysis_status": "complete"})
    _wj(doc / "versions/v001/03_analysis/latest/03_findings.json", {"findings": [1, 2]})
    return v2


# ---------------------------------------------------------------------------
# mode selection
# ---------------------------------------------------------------------------


def test_default_mode_legacy(monkeypatch):
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    assert F.get_storage_mode() == "legacy"
    f = F.StorageReadFacade()
    assert f.is_legacy() and not f.uses_projects_v2()


def test_unknown_value_is_legacy(monkeypatch):
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "garbage")
    assert F.get_storage_mode() == "legacy"


def test_mode_projects_v2(monkeypatch):
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    f = F.StorageReadFacade()
    assert f.is_v2() and f.uses_projects_v2()


def test_mode_dual_read_shadow(monkeypatch):
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "dual_read_shadow")
    f = F.StorageReadFacade()
    assert f.is_dual_read_shadow() and f.uses_projects_v2()


def test_production_uses_v2_always_false(monkeypatch):
    for v in ("legacy", "projects_v2", "dual_read_shadow"):
        monkeypatch.setenv("AUDIT_STORAGE_BACKEND", v)
        assert F.production_uses_v2() is False


# ---------------------------------------------------------------------------
# read behaviour
# ---------------------------------------------------------------------------


def test_legacy_mode_does_not_use_v2(tmp_path, monkeypatch):
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    f = F.StorageReadFacade(v2_root=_mini_v2(tmp_path))
    snap = f.document_snapshot("doc-c")
    assert snap["backend"] == "legacy"
    assert snap["v2_used"] is False
    assert snap["handled_by"] == "existing_legacy_services"


def test_v2_mode_reads_adapter(tmp_path):
    f = F.StorageReadFacade(v2_root=_mini_v2(tmp_path), mode="projects_v2")
    snap = f.document_snapshot("doc-c")
    assert snap["backend"] == "projects_v2" and snap["v2_used"] is True
    assert snap["found"] is True
    assert snap["snapshot"]["current_version"] == "v001"


def test_v2_mode_missing_document(tmp_path):
    f = F.StorageReadFacade(v2_root=_mini_v2(tmp_path), mode="projects_v2")
    snap = f.document_snapshot("nope")
    assert snap["backend"] == "projects_v2" and snap["found"] is False


def test_dual_read_mode_returns_comparison(tmp_path):
    # без legacy/map dual-read вернёт missing_v2/legacy, но не упадёт
    f = F.StorageReadFacade(v2_root=_mini_v2(tmp_path), mode="dual_read_shadow")
    snap = f.document_snapshot("doc-c")
    assert snap["backend"] == "dual_read_shadow"
    assert "dual_read" in snap and "status" in snap["dual_read"]


def test_fixed_mode_overrides_env(tmp_path, monkeypatch):
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    f = F.StorageReadFacade(mode="legacy")
    assert f.mode == "legacy" and f.is_legacy()
