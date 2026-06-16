"""
Тесты РАСШИРЕННОГО opt-in read-only canary projects_v2 на дополнительных GET-endpoint'ах:
  * GET /api/projects/{project_id}                       (детали)
  * GET /api/projects/{project_id}/versions             (версии)
  * GET /api/projects/{project_id}/config               (document.json)
  * GET /api/findings/{project_id}/finding/{finding_id} (одно замечание)
  * GET /api/tiles/{project_id}/blocks/analysis         (анализ блоков)

Каждый: без opt-in → legacy; opt-in + флаг ON → v2; opt-in + флаг OFF → 403.
+ source_only/legacy_partial/complete/partial/none; findings/version не теряются;
block-analysis наличие; read-only.

Изоляция через AUDIT_PROJECTS_V2_DIR. conftest выключает PORTAL_AUTH_ENABLED.
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
    _doc(v2, "AI", "doc-partial", statuses={"v001": "partial"},
         findings={"v001": 2}, blocks={"v001": 3})
    _doc(v2, "OV", "doc-none", statuses={"v001": "none"})
    _doc(v2, "ITP", "doc-source", migration_kind="legacy_findings_preserve",
         statuses={"v001": "source_only"})
    _doc(v2, "EOM", "doc-legacy", migration_kind="legacy_findings_preserve",
         statuses={"v001": "legacy_partial"}, findings={"v001": 3}, blocks={"v001": 2})
    _doc(v2, "AR", "doc-versioned", kind="container",
         versions=[{"version_id": "v001", "version_no": 1},
                   {"version_id": "v002", "version_no": 2}], current="v002",
         statuses={"v001": "partial", "v002": "complete"},
         findings={"v001": 4, "v002": 9}, blocks={"v002": 8})
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    return v2


def _on(mp): mp.setenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", "true")
def _off(mp): mp.delenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", raising=False)
OPTIN = "?storage=projects_v2"


def _tree_hash(v2: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(v2.rglob("*")):
        if f.is_file():
            h.update(str(f.relative_to(v2)).encode()); h.update(f.read_bytes())
    return h.hexdigest()


# === project details: GET /api/projects/{id} ===============================

def test_details_no_optin_legacy(monkeypatch, v2tree):
    _on(monkeypatch)
    import backend.app.api.routers.projects as P

    class _S:
        def model_dump(self): return {"legacy_path": True}
    monkeypatch.setattr(P.project_service, "get_project_status", lambda *a, **k: _S())
    r = client.get("/api/projects/doc-complete")
    assert r.status_code == 200
    assert r.json().get("storage_backend") != "projects_v2"


def test_details_optin_v2(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get(f"/api/projects/doc-complete{OPTIN}")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2" and b["canary"] is True
    # LEGACY ProjectStatus-форма: pipeline-объект обязателен (нет краша шаблона)
    assert isinstance(b["pipeline"], dict) and "gemma_enrichment" in b["pipeline"]
    assert b["findings_count"] == 7
    assert b["pipeline"]["blocks_analysis"] == "done"  # has_02 → pipeline.blocks_analysis
    assert b["version_count"] == 1 and b["project_id"] == "doc-complete"


def test_details_optin_flag_off_403(monkeypatch, v2tree):
    _off(monkeypatch)
    assert client.get(f"/api/projects/doc-complete{OPTIN}").status_code == 403


def test_details_versioned(monkeypatch, v2tree):
    _on(monkeypatch)
    b = client.get(f"/api/projects/doc-versioned{OPTIN}").json()
    # LEGACY ProjectStatus: latest_version_id (denorm) вместо v2-native current_version
    assert b["version_count"] == 2 and b["latest_version_id"] == "v2"
    assert b["findings_count"] == 9


# === versions: GET /api/projects/{id}/versions =============================

def test_versions_no_optin_legacy(monkeypatch, v2tree):
    _on(monkeypatch)
    import backend.app.api.routers.projects as P
    monkeypatch.setattr(P.project_service, "resolve_project_dir",
                        lambda pid: Path("/nonexistent"))
    r = client.get("/api/projects/doc-complete/versions")
    assert r.status_code == 404  # legacy путь (проект не найден), не v2

def test_versions_optin_v2(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get(f"/api/projects/doc-versioned/versions{OPTIN}")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    assert b["version_count"] == 2
    # LEGACY-форма version_id: denorm v00N → vN (frontend сравнивает с 'v1')
    assert {v["version_id"] for v in b["versions"]} == {"v1", "v2"}
    assert b["latest_version_id"] == "v2"

def test_versions_flag_off_403(monkeypatch, v2tree):
    _off(monkeypatch)
    assert client.get(f"/api/projects/doc-complete/versions{OPTIN}").status_code == 403


# === config: NOT wired — route shadowed by catch-all /{project_id:path} ===
# GET /api/projects/{id}/config регистрируется ПОСЛЕ catch-all /{project_id:path},
# поэтому недостижим как отдельный маршрут. Помечен not_ready в отчёте.


def test_config_route_shadowed_by_catchall(monkeypatch, v2tree):
    """opt-in на /config попадает в catch-all get_project → 'config' не документ → 404.

    Документирует, ПОЧЕМУ /config не подключён (а не молчаливый пропуск)."""
    _on(monkeypatch)
    r = client.get(f"/api/projects/doc-complete/config{OPTIN}")
    assert r.status_code == 404


# === single finding: GET /api/findings/{id}/finding/{fid} =================

def test_finding_by_id_optin_v2(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get(f"/api/findings/doc-complete/finding/F-003{OPTIN}")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    # LEGACY: поля замечания на верхнем уровне (не вложены под finding)
    assert b["id"] == "F-003"

def test_finding_by_id_missing_404(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get(f"/api/findings/doc-complete/finding/F-999{OPTIN}")
    assert r.status_code == 404
    assert "projects_v2 canary" in r.json()["detail"]

def test_finding_by_id_flag_off_403(monkeypatch, v2tree):
    _off(monkeypatch)
    assert client.get(f"/api/findings/doc-complete/finding/F-001{OPTIN}").status_code == 403


# === block analysis: GET /api/tiles/{id}/blocks/analysis ==================

def test_blocks_analysis_optin_v2(monkeypatch, v2tree):
    _on(monkeypatch)
    r = client.get(f"/api/tiles/doc-complete/blocks/analysis{OPTIN}")
    assert r.status_code == 200
    b = r.json()
    assert b["storage_backend"] == "projects_v2"
    # LEGACY: классифицированный dict blocks (frontend → Object.entries(data.blocks))
    assert isinstance(b["blocks"], dict) and len(b["blocks"]) == 5
    assert b["total_analyzed"] == 5 and isinstance(b["counts"], dict)

def test_blocks_analysis_none_doc(monkeypatch, v2tree):
    """doc-none без 02_blocks_analysis не падает (пустой dict, не 500)."""
    _on(monkeypatch)
    r = client.get(f"/api/tiles/doc-none/blocks/analysis{OPTIN}")
    assert r.status_code == 200
    b = r.json()
    assert isinstance(b["blocks"], dict) and b["blocks"] == {}
    assert b["total_analyzed"] == 0

def test_blocks_analysis_flag_off_403(monkeypatch, v2tree):
    _off(monkeypatch)
    assert client.get(f"/api/tiles/doc-complete/blocks/analysis{OPTIN}").status_code == 403


# === type coverage + read-only ============================================

@pytest.mark.parametrize("code,status", [
    ("doc-complete", "complete"), ("doc-partial", "partial"), ("doc-none", "none"),
    ("doc-source", "source_only"), ("doc-legacy", "legacy_partial"),
])
def test_details_all_types_ok(monkeypatch, v2tree, code, status):
    _on(monkeypatch)
    r = client.get(f"/api/projects/{code}{OPTIN}")
    assert r.status_code == 200
    # LEGACY ProjectStatus-форма (no 500 для всех типов); pipeline всегда есть
    b = r.json()
    assert isinstance(b.get("pipeline"), dict) and b["project_id"] == code

def test_expanded_canary_read_only(monkeypatch, v2tree):
    _on(monkeypatch)
    before = _tree_hash(v2tree)
    for ep in (f"/api/projects/doc-complete{OPTIN}",
               f"/api/projects/doc-versioned/versions{OPTIN}",
               f"/api/projects/doc-complete/config{OPTIN}",
               f"/api/findings/doc-complete/finding/F-001{OPTIN}",
               f"/api/tiles/doc-complete/blocks/analysis{OPTIN}"):
        client.get(ep)
    assert _tree_hash(v2tree) == before
