"""
Тесты controlled HTTP smoke (http_smoke_shadow_api): реальный uvicorn-сокет,
минимальный app без lifespan. Изоляция через AUDIT_PROJECTS_V2_DIR на синтетику.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "scripts" / "projects_v2"))
import http_smoke_shadow_api as HS  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"


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


def _build_tree(tmp_path) -> Path:
    v2 = tmp_path / "projects_v2"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": "0b540226", "display_name": "213", "folder_name": OBJF})
    _doc(v2, "AI", "doc-complete", statuses={"v001": "complete"}, findings={"v001": 5})
    _doc(v2, "AI", "doc-partial", statuses={"v001": "partial"}, findings={"v001": 2})
    _doc(v2, "OV", "doc-none", statuses={"v001": "none"})
    _doc(v2, "ITP", "doc-source", migration_kind="legacy_findings_preserve",
         statuses={"v001": "source_only"})
    _doc(v2, "EOM", "doc-legacy", migration_kind="legacy_findings_preserve",
         statuses={"v001": "legacy_partial"}, findings={"v001": 3})
    _doc(v2, "SS", "doc-sot", statuses={"v001": "legacy_partial"}, findings={"v001": 1})
    _doc(v2, "AR", "doc-versioned", kind="container",
         versions=[{"version_id": "v001", "version_no": 1},
                   {"version_id": "v002", "version_no": 2}], current="v002",
         statuses={"v001": "partial", "v002": "complete"}, findings={"v001": 4, "v002": 9})
    _wj(v2 / "_system" / "backend_parity_report.json", {
        "generated_at": "t", "parity_ok": True, "results": [
            {"document_code": "doc-complete", "type": "complete", "ok": True},
            {"document_code": "doc-partial", "type": "partial", "ok": True},
            {"document_code": "doc-none", "type": "none", "ok": True},
            {"document_code": "doc-source", "type": "king_sons_legacy_preserve", "ok": True},
            {"document_code": "doc-legacy", "type": "king_sons_legacy_preserve", "ok": True},
            {"document_code": "doc-sot", "type": "legacy_partial", "ok": True},
            {"document_code": "doc-versioned", "type": "versioned", "ok": True},
        ]})
    return v2


# ---------------------------------------------------------------------------
# unit (без сервера)
# ---------------------------------------------------------------------------


def test_build_smoke_app_has_routes():
    app = HS.build_smoke_app()
    paths = {r.path for r in app.routes}
    assert any(p.startswith("/api/projects-v2-shadow") for p in paths)
    assert any(p.startswith("/api/objects") for p in paths)


def test_pick_sample_codes_covers_kingsons_widely():
    parity = {"results": [
        {"document_code": "a", "type": "complete"},
        {"document_code": "k1", "type": "king_sons_legacy_preserve"},
        {"document_code": "k2", "type": "king_sons_legacy_preserve"},
        {"document_code": "k3", "type": "king_sons_legacy_preserve"},
    ]}
    picked = HS._pick_sample_codes(parity)
    kk = [p for p in picked if p["type"] == "king_sons_legacy_preserve"]
    assert len(kk) == 3  # king_sons берётся шире (до 3)
    assert any(p["type"] == "complete" for p in picked)


def test_snapshot_detects_no_change(tmp_path):
    (tmp_path / "a.txt").write_text("x")
    snap1 = HS._snapshot(tmp_path)
    snap2 = HS._snapshot(tmp_path)
    assert snap1 == snap2 and len(snap1) == 1


def test_render_md_smoke():
    rep = {"generated_at": "t", "ok": True,
           "summary": {"checks_passed": 1, "checks_total": 1,
                       "documents_ok": 0, "documents_checked": 0},
           "checks": [{"check": "x", "ok": True, "detail": "d"}], "documents": []}
    md = HS.render_md(rep)
    assert "controlled HTTP smoke" in md and "production backend" in md


# ---------------------------------------------------------------------------
# integration (реальный uvicorn-сокет)
# ---------------------------------------------------------------------------


def test_run_smoke_http_real_socket(tmp_path, monkeypatch):
    v2 = _build_tree(tmp_path)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    rep = HS.run_smoke(v2, port=0)

    assert rep["transport"] == "http"
    assert rep["base_url"].startswith("http://127.0.0.1:")
    assert ":8081" not in rep["base_url"]  # не production-порт
    by = {c["check"]: c["ok"] for c in rep["checks"]}
    assert by["health_404_without_flag"] is True
    assert by["shadow_health_200"] is True
    assert by["health_backend_default_legacy"] is True
    assert by["shadow_objects_200"] is True
    assert by["shadow_documents_200"] is True
    assert by["shadow_snapshots_200_with_status"] is True
    assert by["status_variety_covered"] is True
    assert by["health_404_after_disable"] is True
    assert by["read_only_objects_unchanged"] is True
    assert by["adapter_backend_default_legacy"] is True
    assert rep["ok"] is True
    # флаг не оставлен включённым
    import os
    assert os.environ.get("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED") in (None, "false")


def test_run_smoke_writes_reports(tmp_path, monkeypatch):
    v2 = _build_tree(tmp_path)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    rep = HS.run_smoke(v2, port=0)
    jp, mp = HS.write_reports(rep, v2)
    assert jp.exists() and mp.exists()
    data = json.loads(jp.read_text(encoding="utf-8"))
    assert data["transport"] == "http"
