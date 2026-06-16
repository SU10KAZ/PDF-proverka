"""
Тесты limited default read cutover: флаг AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED.

Когда флаг OFF (default) — поведение прежнее: обычный запрос legacy, opt-in v2.
Когда флаг ON — 6 approved GET-endpoint'ов читают projects_v2 ПО УМОЛЧАНИЮ (без
opt-in); `?storage=legacy` форсит legacy; endpoints вне approved list — legacy;
write-endpoints не трогаются; AUDIT_STORAGE_BACKEND не читается.

Изоляция через AUDIT_PROJECTS_V2_DIR. conftest выключает PORTAL_AUTH_ENABLED и
НЕ задаёт READ_DEFAULT/READ_CANARY (тест выставляет сам).
"""

from __future__ import annotations

import hashlib
import json
import sys
import urllib.parse
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
from backend.app.main import app  # noqa: E402
from backend.app.services.storage import read_canary as RC  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"
client = TestClient(app)
Q = lambda s: urllib.parse.quote(s, safe="")


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _doc(v2, disc, code, *, kind="plain", versions=None, current=None,
         migration_kind=None, statuses=None, findings=None, blocks=None):
    versions = versions or [{"version_id": "v001", "version_no": 1}]
    statuses = statuses or {}
    findings = findings or {}
    blocks = blocks or {}
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
        latest = doc / "versions" / vid / "03_analysis" / "latest"
        fc = findings.get(vid)
        if fc is not None:
            _wj(latest / "03_findings.json",
                {"findings": [{"id": f"F-{i+1:03d}", "severity": "Критическое"} for i in range(fc)]})
        bc = blocks.get(vid)
        if bc is not None:
            _wj(latest / "02_blocks_analysis.json",
                {"block_analyses": [{"block_id": f"block_{i}"} for i in range(bc)]})
    return doc


@pytest.fixture
def v2tree(tmp_path, monkeypatch):
    v2 = tmp_path / "projects_v2"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": "0b540226", "display_name": "213", "folder_name": OBJF})
    _doc(v2, "AI", "doc-complete", statuses={"v001": "complete"},
         findings={"v001": 7}, blocks={"v001": 5})
    _doc(v2, "ITP", "doc-source", migration_kind="legacy_findings_preserve",
         statuses={"v001": "source_only"})
    _doc(v2, "EOM", "doc-legacy", migration_kind="legacy_findings_preserve",
         statuses={"v001": "legacy_partial"}, findings={"v001": 3})
    _doc(v2, "AR", "doc-versioned", kind="container",
         versions=[{"version_id": "v001", "version_no": 1},
                   {"version_id": "v002", "version_no": 2}], current="v002",
         statuses={"v001": "partial", "v002": "complete"},
         findings={"v001": 4, "v002": 9}, blocks={"v002": 8})
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    return v2


def _default_on(mp): mp.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
def _default_off(mp): mp.delenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", raising=False)
def _canary_on(mp): mp.setenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", "true")
def _canary_off(mp): mp.delenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", raising=False)


def _tree_hash(v2: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(v2.rglob("*")):
        if f.is_file():
            h.update(str(f.relative_to(v2)).encode()); h.update(f.read_bytes())
    return h.hexdigest()


# --- unit: resolve matrix ---------------------------------------------------

class _Req:
    def __init__(self, q=None, h=None):
        from starlette.datastructures import QueryParams, Headers
        self.query_params = QueryParams({"storage": q} if q else {})
        self.headers = Headers({"x-audit-storage": h} if h else {})


def test_resolve_matrix(monkeypatch):
    _default_off(monkeypatch); _canary_off(monkeypatch)
    assert RC.resolve_read_backend(_Req()) == "legacy"                       # no pref, both off
    _canary_on(monkeypatch)
    assert RC.resolve_read_backend(_Req(q="projects_v2")) == "projects_v2"   # opt-in + canary
    _canary_off(monkeypatch)
    import pytest as _p
    from fastapi import HTTPException
    with _p.raises(HTTPException):
        RC.resolve_read_backend(_Req(q="projects_v2"))                       # opt-in + canary off → 403
    _default_on(monkeypatch)
    assert RC.resolve_read_backend(_Req()) == "projects_v2"                  # default on, no pref
    assert RC.resolve_read_backend(_Req(q="legacy")) == "legacy"            # force-legacy beats default
    assert RC.resolve_read_backend(_Req(h="legacy")) == "legacy"           # force-legacy via header


def test_default_flag_default_off(monkeypatch):
    _default_off(monkeypatch)
    assert RC.default_read_enabled() is False
    _default_on(monkeypatch)
    assert RC.default_read_enabled() is True


# --- default OFF: behavior unchanged ---------------------------------------

def test_default_off_list_is_legacy(monkeypatch, v2tree):
    _default_off(monkeypatch); _canary_on(monkeypatch)
    r = client.get("/api/projects")
    assert r.status_code == 200 and r.json().get("storage_backend") != "projects_v2"

def test_default_off_optin_still_v2(monkeypatch, v2tree):
    _default_off(monkeypatch); _canary_on(monkeypatch)
    r = client.get("/api/projects?storage=projects_v2")
    assert r.status_code == 200 and r.json()["storage_backend"] == "projects_v2"


# --- default ON: approved endpoints read v2 by default ----------------------

def test_default_on_list_is_v2(monkeypatch, v2tree):
    _default_on(monkeypatch)
    r = client.get("/api/projects")
    assert r.status_code == 200
    assert r.json()["storage_backend"] == "projects_v2"
    assert r.json()["count"] == 4

def test_default_on_findings_is_v2(monkeypatch, v2tree):
    _default_on(monkeypatch)
    r = client.get("/api/findings/doc-complete")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    assert b["findings_count"] == 7      # findings count не теряется

def test_default_on_details_is_v2(monkeypatch, v2tree):
    _default_on(monkeypatch)
    b = client.get("/api/projects/doc-complete").json()
    assert b["storage_backend"] == "projects_v2"
    assert b["analysis_status"] == "complete"

def test_default_on_versions_is_v2(monkeypatch, v2tree):
    _default_on(monkeypatch)
    b = client.get("/api/projects/doc-versioned/versions").json()
    assert b["storage_backend"] == "projects_v2"
    assert b["version_count"] == 2       # version count не теряется

def test_default_on_finding_by_id_is_v2(monkeypatch, v2tree):
    _default_on(monkeypatch)
    b = client.get("/api/findings/doc-complete/finding/F-002").json()
    assert b["storage_backend"] == "projects_v2"
    assert b["finding"]["id"] == "F-002"

def test_default_on_blocks_analysis_is_v2(monkeypatch, v2tree):
    _default_on(monkeypatch)
    b = client.get("/api/tiles/doc-complete/blocks/analysis").json()
    assert b["storage_backend"] == "projects_v2"
    assert b["block_count"] == 5


# --- force-legacy override --------------------------------------------------

def test_default_on_force_legacy_query(monkeypatch, v2tree):
    """?storage=legacy при default ON форсит legacy (для отката/сравнения)."""
    _default_on(monkeypatch)
    r = client.get("/api/projects?storage=legacy")
    assert r.status_code == 200
    assert r.json().get("storage_backend") != "projects_v2"   # legacy shape

def test_default_on_force_legacy_header(monkeypatch, v2tree):
    _default_on(monkeypatch)
    r = client.get("/api/projects", headers={"X-Audit-Storage": "legacy"})
    assert r.json().get("storage_backend") != "projects_v2"

def test_default_on_force_legacy_findings(monkeypatch, v2tree):
    """findings force-legacy → legacy path (doc-complete нет в legacy → 404 legacy)."""
    _default_on(monkeypatch)
    import backend.app.api.routers.findings as F

    class _Fake:
        def model_dump(self): return {"legacy_path": True}
    monkeypatch.setattr(F.findings_service, "get_findings", lambda *a, **k: _Fake())
    r = client.get("/api/findings/doc-complete?storage=legacy")
    assert r.status_code == 200
    assert r.json() == {"legacy_path": True}   # legacy service вызван


# --- non-approved endpoints stay legacy even with default ON ---------------

def test_default_on_objects_stays_legacy(monkeypatch, v2tree):
    """/api/objects не в approved list → legacy даже при default ON."""
    _default_on(monkeypatch)
    r = client.get("/api/objects")
    assert r.status_code == 200
    assert r.json().get("storage_backend") != "projects_v2"

def test_default_on_disciplines_stays_legacy(monkeypatch, v2tree):
    """/api/projects/disciplines не зовёт resolve_read_backend → legacy."""
    _default_on(monkeypatch)
    r = client.get("/api/projects/disciplines")
    assert r.status_code == 200
    assert (r.json() if isinstance(r.json(), dict) else {}).get("storage_backend") != "projects_v2"


# --- type coverage + read-only ---------------------------------------------

@pytest.mark.parametrize("code,status", [
    ("doc-complete", "complete"), ("doc-source", "source_only"),
    ("doc-legacy", "legacy_partial"), ("doc-versioned", "complete"),
])
def test_default_on_all_types_ok(monkeypatch, v2tree, code, status):
    _default_on(monkeypatch)
    r = client.get(f"/api/projects/{code}")
    assert r.status_code == 200
    assert r.json()["analysis_status"] == status

def test_default_on_read_only(monkeypatch, v2tree):
    _default_on(monkeypatch)
    before = _tree_hash(v2tree)
    for ep in ("/api/projects", "/api/findings/doc-complete",
               "/api/projects/doc-complete", "/api/projects/doc-versioned/versions",
               "/api/findings/doc-complete/finding/F-001",
               "/api/tiles/doc-complete/blocks/analysis"):
        client.get(ep)
    assert _tree_hash(v2tree) == before

def test_default_on_does_not_touch_storage_backend(monkeypatch, v2tree):
    import os
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "legacy")
    _default_on(monkeypatch)
    client.get("/api/projects")
    assert os.environ.get("AUDIT_STORAGE_BACKEND") == "legacy"
