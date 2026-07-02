"""
Тесты opt-in read-only canary для projects_v2 (backend/app/services/storage/read_canary.py)
на endpoint'ах GET /api/projects и GET /api/findings/{project_id}.

Изолировано от реального projects_v2 через AUDIT_PROJECTS_V2_DIR на синтетическое
дерево в tmp_path. conftest.py выставляет PORTAL_AUTH_ENABLED=false до импорта app.

Покрытие (по ТЗ этапа):
  * без opt-in → legacy (canary-маркера нет; production-path не меняется);
  * opt-in + флаг OFF → 403;
  * opt-in + флаг ON → projects_v2 (canary shape, нужные поля);
  * findings_count / version_count не теряются;
  * source_only / legacy_partial не падают;
  * opt-in через header X-Audit-Storage;
  * v2 doc не найден → 404 canary-error (НЕ silent legacy fallback);
  * endpoint read-only (дерево projects_v2 не меняется);
  * canary независим от AUDIT_STORAGE_BACKEND (остаётся legacy).
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
from backend.app.main import app  # noqa: E402
from backend.app.services.storage import read_canary as RC  # noqa: E402
import backend.app.services.common.object_service as object_service  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"
client = TestClient(app)


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _doc(v2, disc, code, *, kind="plain", versions=None, current=None,
         migration_kind=None, statuses=None, findings=None):
    versions = versions or [{"version_id": "v001", "version_no": 1}]
    statuses = statuses or {}
    findings = findings or {}
    doc = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    dj = {"document_code": code, "object_id": "0b540226", "discipline": disc,
          "kind": kind, "versions": versions,
          "current_version": current or versions[-1]["version_id"]}
    if migration_kind:
        dj["migration_kind"] = migration_kind
    _wj(doc / "document.json", dj)
    (doc / "current_version.txt").write_text(
        (current or versions[-1]["version_id"]) + "\n", encoding="utf-8")
    for v in versions:
        vid = v["version_id"]
        vj = {"version_id": vid, "version_no": v["version_no"],
              "analysis_status": statuses.get(vid, "none")}
        if migration_kind:
            vj["migration_kind"] = migration_kind
        _wj(doc / "versions" / vid / "version.json", vj)
        (doc / "versions" / vid / "01_input").mkdir(parents=True, exist_ok=True)
        (doc / "versions" / vid / "01_input" / "a.pdf").write_text("x", encoding="utf-8")
        fc = findings.get(vid)
        if fc is not None:
            _wj(doc / "versions" / vid / "03_analysis" / "latest" / "03_findings.json",
                {"findings": [{"severity": "Критическое"}] * fc})


@pytest.fixture
def v2tree(tmp_path, monkeypatch):
    v2 = tmp_path / "projects_v2"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": "0b540226", "display_name": "213", "folder_name": OBJF})
    _doc(v2, "AI", "doc-complete", statuses={"v001": "complete"}, findings={"v001": 7})
    _doc(v2, "AI", "doc-partial", statuses={"v001": "partial"}, findings={"v001": 2})
    _doc(v2, "OV", "doc-none", statuses={"v001": "none"})
    _doc(v2, "ITP", "doc-source", migration_kind="legacy_findings_preserve",
         statuses={"v001": "source_only"})
    _doc(v2, "EOM", "doc-legacy", migration_kind="legacy_findings_preserve",
         statuses={"v001": "legacy_partial"}, findings={"v001": 3})
    _doc(v2, "AR", "doc-versioned", kind="container",
         versions=[{"version_id": "v001", "version_no": 1},
                   {"version_id": "v002", "version_no": 2}], current="v002",
         statuses={"v001": "partial", "v002": "complete"},
         findings={"v001": 4, "v002": 9})
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    return v2


def _on(monkeypatch):
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", "true")


def _off(monkeypatch):
    monkeypatch.delenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", raising=False)


def _tree_hash(v2: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(v2.rglob("*")):
        if f.is_file():
            h.update(str(f.relative_to(v2)).encode())
            h.update(f.read_bytes())
    return h.hexdigest()


# --- unit: helper logic -----------------------------------------------------

def test_opt_in_detection():
    assert RC.opt_in_requested("projects_v2", None) is True
    assert RC.opt_in_requested(None, "projects_v2") is True
    assert RC.opt_in_requested("PROJECTS_V2", None) is True
    assert RC.opt_in_requested(None, None) is False
    assert RC.opt_in_requested("legacy", None) is False


def test_flag_default_off(monkeypatch):
    _off(monkeypatch)
    assert RC.canary_flag_enabled() is False
    _on(monkeypatch)
    assert RC.canary_flag_enabled() is True


# --- projects list endpoint -------------------------------------------------

def test_projects_list_no_optin_is_legacy(monkeypatch, v2tree):
    """Без opt-in → legacy: НЕТ canary-маркера (production path не меняется)."""
    _on(monkeypatch)  # даже при включённом флаге, без opt-in → legacy
    r = client.get("/api/projects")
    assert r.status_code == 200
    assert r.json().get("storage_backend") != "projects_v2"
    assert "canary" not in r.json()


def test_projects_list_optin_flag_off_403(monkeypatch, v2tree):
    _off(monkeypatch)
    r = client.get("/api/projects?storage=projects_v2")
    assert r.status_code == 403


def test_projects_list_optin_flag_on_serves_v2(monkeypatch, v2tree):
    _on(monkeypatch)
    # /api/projects strictly scoped к текущему объекту → делаем тестовый текущим
    monkeypatch.setattr(object_service, "get_current_object",
                        lambda: {"id": "0b540226", "name": "213",
                                 "projects_dir": "/tmp/none"})
    r = client.get("/api/projects?storage=projects_v2")
    assert r.status_code == 200
    body = r.json()
    assert body["storage_backend"] == "projects_v2"
    assert body["canary"] is True
    # LEGACY-форма: projects (массив), НЕ v2-native documents/count
    assert isinstance(body["projects"], list) and len(body["projects"]) == 6
    assert {p["project_id"] for p in body["projects"]} >= {
        "doc-complete", "doc-versioned", "doc-source", "doc-legacy"}


def test_projects_list_optin_via_header(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get("/api/projects", headers={"X-Audit-Storage": "projects_v2"})
    assert r.status_code == 200
    assert r.json()["storage_backend"] == "projects_v2"


# --- findings endpoint ------------------------------------------------------

def test_findings_no_optin_is_legacy_not_v2(monkeypatch, v2tree):
    """Без opt-in вызывается legacy findings_service (НЕ v2 canary), даже при флаге ON."""
    _on(monkeypatch)  # флаг включён, но без opt-in → legacy path

    class _Fake:
        def model_dump(self):
            return {"legacy_path": True, "findings": []}

    called = {"legacy": False}

    def _fake_get_findings(project_id, **kw):
        called["legacy"] = True
        return _Fake()

    import backend.app.api.routers.findings as F
    monkeypatch.setattr(F.findings_service, "get_findings", _fake_get_findings)
    r = client.get("/api/findings/doc-complete")
    assert r.status_code == 200
    assert called["legacy"] is True                       # legacy реально вызван
    assert r.json() == {"legacy_path": True, "findings": []}
    assert r.json().get("storage_backend") != "projects_v2"  # НЕ v2 canary


def test_findings_optin_flag_off_403(monkeypatch, v2tree):
    _off(monkeypatch)
    r = client.get("/api/findings/doc-complete?storage=projects_v2")
    assert r.status_code == 403


def test_findings_optin_flag_on_serves_v2(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get("/api/findings/doc-complete?storage=projects_v2")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    assert b["canary"] is True
    assert b["document_code"] == "doc-complete"
    assert b["analysis_status"] == "complete"
    assert b["findings_count"] == 7          # findings count не теряется
    assert b["version_count"] == 1
    assert len(b["findings"]) == 7
    assert b["findings_by_severity"].get("Критическое") == 7


def test_findings_version_count_preserved(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get("/api/findings/doc-versioned?storage=projects_v2")
    assert r.status_code == 200
    b = r.json()
    assert b["version_count"] == 2           # version count не теряется
    assert b["findings_count"] == 9          # current=v002 findings
    assert b["version_id"] == "v002"


def test_findings_source_only_ok(monkeypatch, v2tree):
    """source_only документ не падает, findings=0."""
    _on(monkeypatch)
    r = client.get("/api/findings/doc-source?storage=projects_v2")
    assert r.status_code == 200
    b = r.json()
    assert b["analysis_status"] == "source_only"
    assert b["findings_count"] == 0


def test_findings_legacy_partial_ok(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get("/api/findings/doc-legacy?storage=projects_v2")
    assert r.status_code == 200
    b = r.json()
    assert b["analysis_status"] == "legacy_partial"
    assert b["findings_count"] == 3


def test_findings_optin_header(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get("/api/findings/doc-partial", headers={"X-Audit-Storage": "projects_v2"})
    assert r.status_code == 200
    assert r.json()["findings_count"] == 2


def test_findings_v2_missing_is_canary_404_not_legacy(monkeypatch, v2tree):
    """v2 не нашёл документ → 404 canary-error, НЕ silent fallback в legacy."""
    _on(monkeypatch)
    r = client.get("/api/findings/no-such-doc?storage=projects_v2")
    assert r.status_code == 404
    assert "projects_v2 canary" in r.json()["detail"]


# --- safety: read-only + backend independence -------------------------------

def test_canary_is_read_only(monkeypatch, v2tree):
    _on(monkeypatch)
    before = _tree_hash(v2tree)
    client.get("/api/projects?storage=projects_v2")
    client.get("/api/findings/doc-complete?storage=projects_v2")
    client.get("/api/findings/doc-versioned?storage=projects_v2")
    assert _tree_hash(v2tree) == before      # дерево projects_v2 не изменилось


def test_canary_independent_of_storage_backend(monkeypatch, v2tree):
    """Canary работает по своему флагу; AUDIT_STORAGE_BACKEND остаётся legacy."""
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "legacy")
    _on(monkeypatch)
    r = client.get("/api/projects?storage=projects_v2")
    assert r.status_code == 200
    assert r.json()["storage_backend"] == "projects_v2"
    # сам флаг backend не трогается модулем
    import os
    assert os.environ.get("AUDIT_STORAGE_BACKEND") == "legacy"


def test_prefixed_document_code_resolves_before_basename_dupe(monkeypatch, v2tree, tmp_path):
    """Рецидив ОВ2-К4 (2026-07-02): document_code с дисциплинным префиксом
    («OV/<код> V1») при наличии СТЕЙЛ-ДУБЛЯ с чистым basename-кодом резолвился в
    дубль (basename-срез в _resolve_doc_or_404) → в UI видна только V1 дубля,
    решения эксперта «пропадали». Полный pid должен выигрывать у basename."""
    v2 = v2tree
    # стейл-дубль: чистый basename-код, ОДНА версия
    _doc(v2, "OV", "dup-code V1", statuses={"v001": "complete"}, findings={"v001": 4})
    # живой документ: код с префиксом (папка с U+2215, как пишет мигратор), ДВЕ версии
    _doc(v2, "OV", "OV∕dup-code V1", kind="container",
         versions=[{"version_id": "v001", "version_no": 1},
                   {"version_id": "v002", "version_no": 2}], current="v002",
         statuses={"v001": "complete", "v002": "complete"},
         findings={"v001": 4, "v002": 9})
    # document_code в document.json должен быть С «/» (как в реале), папка — с ∕
    doc_dir = v2 / "objects" / OBJF / "disciplines" / "OV" / "documents" / "OV∕dup-code V1"
    dj = json.loads((doc_dir / "document.json").read_text(encoding="utf-8"))
    dj["document_code"] = "OV/dup-code V1"
    _wj(doc_dir / "document.json", dj)

    _on(monkeypatch)
    # полный префиксный pid → живой документ (обе версии)
    r = client.get("/api/projects/OV%2Fdup-code%20V1/versions",
                   headers={"X-Audit-Storage": "projects_v2"})
    assert r.status_code == 200
    data = r.json()
    assert data["project_id"] == "OV/dup-code V1"
    assert [v["version_id"] for v in data["versions"]] == ["v1", "v2"]
    # basename-pid по-прежнему находит свой (стейл) документ — поведение не менялось
    r2 = client.get("/api/projects/dup-code%20V1/versions",
                    headers={"X-Audit-Storage": "projects_v2"})
    assert r2.status_code == 200
    assert [v["version_id"] for v in r2.json()["versions"]] == ["v1"]
