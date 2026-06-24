"""
v2 list hiding (Step 9.1/10): default-v2 `/api/projects` должен скрывать проекты
так же, как legacy iter_project_dirs:
  * `_`-prefix (документ/дисциплина) — скрыт;
  * project_id из hidden_projects.json — скрыт;
  * обычные проекты видны; object-scope сохранён; `?storage=legacy` работает.

Инцидент 2026-06-17: `_smoke_dualwrite_*` был скрыт в legacy `/api/projects`, но
ВИДЕН в v2 `/api/projects` (v2_projects_list не учитывал `_`/hidden_projects).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app.main import app
import backend.app.services.common.object_service as object_service
import backend.app.services.common.project_service as project_service
from backend.app.services.storage import read_canary as RC

client = TestClient(app, raise_server_exceptions=False)

OBJID = "hideobj001"
OBJF = "999_HideTest"


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _doc(v2, disc, code):
    d = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    _wj(d / "document.json", {"document_code": code, "object_id": OBJID,
                              "discipline": disc, "kind": "plain",
                              "versions": [{"version_id": "v001", "version_no": 1}],
                              "current_version": "v001"})
    (d / "current_version.txt").write_text("v001\n", encoding="utf-8")
    vd = d / "versions" / "v001"
    _wj(vd / "version.json", {"version_id": "v001", "version_no": 1,
                              "label": "V1", "analysis_status": "source_only"})
    (vd / "01_input").mkdir(parents=True, exist_ok=True)
    (vd / "01_input" / f"{code}.pdf").write_text("%PDF-1.4\n", encoding="utf-8")
    return d


@pytest.fixture
def v2env(tmp_path, monkeypatch):
    data = tmp_path
    v2 = data / "projects_v2"
    (data / "projects").mkdir(parents=True, exist_ok=True)
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OBJID, "display_name": "999 Hide", "folder_name": OBJF})
    _doc(v2, "EOM", "NORM-1")
    _doc(v2, "EOM", "NORM-2")
    _doc(v2, "EOM", "_smoke_dualwrite_x")     # `_`-prefix doc → должен скрыться
    _doc(v2, "EOM", "HIDE-ME")                # через hidden_projects.json
    _doc(v2, "_tmpdisc", "INDISC")            # `_`-prefix discipline → скрыт
    monkeypatch.setenv("AUDIT_DATA_DIR", str(data))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", "true")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    monkeypatch.setattr(object_service, "get_current_object",
                        lambda: {"id": OBJID, "name": "999 Hide",
                                 "projects_dir": str(data / "projects")})
    # hidden_projects по умолчанию пуст (тест hidden_projects ставит свой)
    monkeypatch.setattr(project_service, "_load_hidden_projects", lambda: set())
    return v2


# ── unit ──────────────────────────────────────────────────────────────────

def test_unit_underscore_doc_hidden():
    assert RC._v2_doc_hidden({"document_code": "_smoke_x", "discipline": "EOM"}, set()) is True


def test_unit_underscore_discipline_hidden():
    assert RC._v2_doc_hidden({"document_code": "X", "discipline": "_tmp"}, set()) is True


def test_unit_hidden_projects_entry_hidden():
    assert RC._v2_doc_hidden({"document_code": "HIDE-ME", "discipline": "EOM"},
                             {"HIDE-ME"}) is True
    assert RC._v2_doc_hidden({"document_code": "HIDE-ME", "discipline": "EOM"},
                             {"EOM/HIDE-ME"}) is True


def test_unit_normal_visible():
    assert RC._v2_doc_hidden({"document_code": "13АВ-РД-АР0.1-ПА", "discipline": "AR"},
                             {"OTHER"}) is False


# ── integration: /api/projects ─────────────────────────────────────────────

def test_v2_list_hides_underscore_and_disc(v2env):
    # hidden_projects пуст → скрываются только `_`-prefix doc и `_`-discipline.
    # HIDE-ME (без `_`) виден, т.к. не в hidden_projects.
    ids = [p["project_id"] for p in client.get("/api/projects").json()["projects"]]
    assert "_smoke_dualwrite_x" not in ids
    assert "INDISC" not in ids                          # `_`-discipline
    assert set(ids) == {"NORM-1", "NORM-2", "HIDE-ME"}


def test_v2_list_honors_hidden_projects(v2env, monkeypatch):
    monkeypatch.setattr(project_service, "_load_hidden_projects", lambda: {"HIDE-ME"})
    ids = [p["project_id"] for p in client.get("/api/projects").json()["projects"]]
    assert "HIDE-ME" not in ids                         # скрыт через hidden_projects
    assert "_smoke_dualwrite_x" not in ids
    assert set(ids) == {"NORM-1", "NORM-2"}


def test_v2_list_normal_projects_visible(v2env):
    ids = [p["project_id"] for p in client.get("/api/projects").json()["projects"]]
    assert "NORM-1" in ids and "NORM-2" in ids


def test_storage_legacy_still_works(v2env):
    # legacy projects/ пуст в фикстуре → 0 проектов, но shape/200 корректны
    r = client.get("/api/projects?storage=legacy")
    assert r.status_code == 200
    b = r.json()
    assert isinstance(b.get("projects"), list)
    assert "object_name" in b
    assert not any(p["project_id"].startswith("_") for p in b["projects"])
