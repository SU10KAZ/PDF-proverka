"""
Тесты read-only shadow API над projects_v2 (projects_v2_shadow router) +
core CLI-проверки (check_shadow_api). Изолированы от реального projects_v2 через
AUDIT_PROJECTS_V2_DIR на синтетическое дерево в tmp_path.

conftest.py выставляет PORTAL_AUTH_ENABLED=false до импорта app.
"""

from __future__ import annotations

import json
import sys
import urllib.parse
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "scripts" / "projects_v2"))
from backend.app.main import app  # noqa: E402
from backend.app.services.storage import projects_v2_adapter as ADP  # noqa: E402
import check_shadow_api as CS  # noqa: E402

SHADOW = "/api/projects-v2-shadow"
OBJF = "213_Mosfilmovskaya_31A_KingSons"
client = TestClient(app)


# ---------------------------------------------------------------------------
# synthetic projects_v2 tree
# ---------------------------------------------------------------------------


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
                {"findings": [{"severity": "x"}] * fc})


@pytest.fixture
def v2tree(tmp_path, monkeypatch):
    v2 = tmp_path / "projects_v2"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": "0b540226", "display_name": "213", "folder_name": OBJF})
    _doc(v2, "AI", "doc-complete", statuses={"v001": "complete"}, findings={"v001": 7})
    _doc(v2, "AI", "doc-partial", statuses={"v001": "partial"}, findings={"v001": 2})
    _doc(v2, "OV", "doc-none", statuses={"v001": "none"})
    # King&Sons legacy preserve: source_only / legacy_partial — это version-level
    # статусы ВНУТРИ типа king_sons_legacy_preserve (как в реальном parity report)
    _doc(v2, "ITP", "doc-source", migration_kind="legacy_findings_preserve",
         statuses={"v001": "source_only"})
    _doc(v2, "EOM", "doc-legacy", migration_kind="legacy_findings_preserve",
         statuses={"v001": "legacy_partial"}, findings={"v001": 3})
    # non-King&Sons legacy_partial (аналог СОТ V1: preserve_reason содержит legacy)
    _doc(v2, "SS", "doc-sot", statuses={"v001": "legacy_partial"}, findings={"v001": 1})
    _doc(v2, "AR", "doc-versioned", kind="container",
         versions=[{"version_id": "v001", "version_no": 1},
                   {"version_id": "v002", "version_no": 2}], current="v002",
         statuses={"v001": "partial", "v002": "complete"},
         findings={"v001": 4, "v002": 9})
    # parity report (для /parity/sample и _pick_samples в CLI) — типы как в реальном
    _wj(v2 / "_system" / "backend_parity_report.json", {
        "generated_at": "t", "documents_checked": 7, "by_type": {},
        "passed": 7, "failed": 0, "parity_ok": True, "findings_no_loss_overall": True,
        "total_v2_findings": 26, "total_legacy_findings": 26,
        "results": [
            {"document_code": "doc-complete", "type": "complete", "ok": True},
            {"document_code": "doc-partial", "type": "partial", "ok": True},
            {"document_code": "doc-none", "type": "none", "ok": True},
            {"document_code": "doc-source", "type": "king_sons_legacy_preserve", "ok": True},
            {"document_code": "doc-legacy", "type": "king_sons_legacy_preserve", "ok": True},
            {"document_code": "doc-sot", "type": "legacy_partial", "ok": True},
            {"document_code": "doc-versioned", "type": "versioned", "ok": True},
        ]})
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    return v2


def _enable(monkeypatch):
    monkeypatch.setenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", "true")


def _disable(monkeypatch):
    monkeypatch.delenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", raising=False)


def _snap(code):
    return client.get(f"{SHADOW}/documents/{urllib.parse.quote(code, safe='')}/snapshot")


# ---------------------------------------------------------------------------
# disabled by default
# ---------------------------------------------------------------------------


def test_router_disabled_by_default(monkeypatch, v2tree):
    _disable(monkeypatch)
    assert client.get(f"{SHADOW}/health").status_code == 404
    assert client.get(f"{SHADOW}/objects").status_code == 404
    assert client.get(f"{SHADOW}/documents").status_code == 404
    assert _snap("doc-complete").status_code == 404


def test_disabled_value_is_404(monkeypatch, v2tree):
    monkeypatch.setenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", "false")
    assert client.get(f"{SHADOW}/health").status_code == 404


# ---------------------------------------------------------------------------
# enabled
# ---------------------------------------------------------------------------


def test_health_when_enabled(monkeypatch, v2tree):
    _enable(monkeypatch)
    r = client.get(f"{SHADOW}/health")
    assert r.status_code == 200
    j = r.json()
    assert j["shadow_api_enabled"] is True
    assert j["read_only"] is True
    assert j["storage_backend_default"] == "legacy"
    assert j["object_count"] == 1 and j["document_count"] == 7


def test_objects_list(monkeypatch, v2tree):
    _enable(monkeypatch)
    j = client.get(f"{SHADOW}/objects").json()
    assert j["count"] == 1 and j["objects"][0]["folder_name"] == OBJF


def test_documents_list(monkeypatch, v2tree):
    _enable(monkeypatch)
    j = client.get(f"{SHADOW}/documents").json()
    assert j["total"] == 7
    codes = {d["document_code"] for d in j["documents"]}
    assert {"doc-complete", "doc-versioned"} <= codes
    # filter by status
    jp = client.get(f"{SHADOW}/documents?analysis_status=complete").json()
    assert {d["document_code"] for d in jp["documents"]} == {"doc-complete", "doc-versioned"}


def test_snapshot_has_analysis_status(monkeypatch, v2tree):
    _enable(monkeypatch)
    j = _snap("doc-complete").json()
    assert j["current_version"] == "v001"
    assert j["versions"][0]["analysis_status"] == "complete"
    assert j["versions"][0]["findings_count"] == 7


def test_versioned_snapshot(monkeypatch, v2tree):
    _enable(monkeypatch)
    j = _snap("doc-versioned").json()
    assert j["version_count"] == 2 and j["current_version"] == "v002"
    statuses = {v["version_id"]: v["analysis_status"] for v in j["versions"]}
    assert statuses == {"v001": "partial", "v002": "complete"}


def test_source_only_does_not_crash(monkeypatch, v2tree):
    _enable(monkeypatch)
    r = _snap("doc-source")
    assert r.status_code == 200
    v = r.json()["versions"][0]
    assert v["analysis_status"] == "source_only" and v["findings_count"] == 0


def test_legacy_partial_does_not_crash(monkeypatch, v2tree):
    _enable(monkeypatch)
    r = _snap("doc-legacy")
    assert r.status_code == 200
    v = r.json()["versions"][0]
    assert v["analysis_status"] == "legacy_partial"
    assert r.json()["migration_kind"] == "legacy_findings_preserve"


def test_versions_endpoint(monkeypatch, v2tree):
    _enable(monkeypatch)
    j = client.get(f"{SHADOW}/documents/doc-versioned/versions").json()
    assert j["version_count"] == 2
    assert j["versions"][0]["is_legacy_preserve"] is False


def test_parity_sample(monkeypatch, v2tree):
    _enable(monkeypatch)
    j = client.get(f"{SHADOW}/parity/sample").json()
    assert j["available"] is True and j["parity_ok"] is True
    assert j["documents_checked"] == 7


def test_missing_document_404(monkeypatch, v2tree):
    _enable(monkeypatch)
    assert _snap("NOPE").status_code == 404


# ---------------------------------------------------------------------------
# read-only invariant + production unaffected
# ---------------------------------------------------------------------------


def test_adapter_read_only_via_api(monkeypatch, v2tree):
    _enable(monkeypatch)
    before = {p: (p.stat().st_mtime_ns, p.read_bytes())
              for p in (v2tree / "objects").rglob("*") if p.is_file()}
    client.get(f"{SHADOW}/health")
    client.get(f"{SHADOW}/objects")
    client.get(f"{SHADOW}/documents")
    for code in ("doc-complete", "doc-source", "doc-legacy", "doc-versioned"):
        _snap(code)
        client.get(f"{SHADOW}/documents/{code}/versions")
        client.get(f"{SHADOW}/documents/{code}")
    after = {p: (p.stat().st_mtime_ns, p.read_bytes())
             for p in (v2tree / "objects").rglob("*") if p.is_file()}
    assert before == after, "shadow API must not modify projects_v2 files"


def test_storage_backend_default_legacy(monkeypatch):
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    assert ADP.get_storage_backend() == "legacy"


def test_production_endpoint_unaffected_by_flag(monkeypatch, v2tree):
    # существующий endpoint работает одинаково при любом значении флага
    _disable(monkeypatch)
    off = client.get("/api/objects").status_code
    _enable(monkeypatch)
    on = client.get("/api/objects").status_code
    assert off == on == 200


# ---------------------------------------------------------------------------
# CLI core (check_shadow_api.check_shadow) через TestClient-fetch
# ---------------------------------------------------------------------------


def _fetch(path):
    r = client.get(path)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, None


def test_cli_core_disabled(monkeypatch, v2tree):
    _disable(monkeypatch)
    rep = CS.check_shadow(_fetch)
    assert rep["shadow_enabled"] is False
    assert rep["ok"] is False


def test_cli_core_enabled_covers_types(monkeypatch, v2tree):
    _enable(monkeypatch)
    rep = CS.check_shadow(_fetch, per_type=2)
    assert rep["shadow_enabled"] is True
    assert rep["ok"] is True
    assert set(CS.REQUIRED_TYPES).issubset(set(rep["covered_types"]))
    assert rep["summary"]["documents_ok"] == rep["summary"]["documents_checked"]


def test_cli_writes_report(monkeypatch, v2tree, tmp_path):
    _enable(monkeypatch)
    rep = CS.check_shadow(_fetch)
    rep["generated_at"] = "t"
    jp, mp = CS.write_reports(rep, v2tree)
    assert jp.exists() and mp.exists()
    assert "Shadow API check" in mp.read_text(encoding="utf-8")
