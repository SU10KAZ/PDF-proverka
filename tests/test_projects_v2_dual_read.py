"""
Тесты dual-read слоя (projects_v2_dual_read) + shadow dual-read endpoints.
Гермётичны: legacy + projects_v2 + old_to_new_map в tmp_path.
"""

from __future__ import annotations

import json
import sys
import urllib.parse
from pathlib import Path

from fastapi.testclient import TestClient

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
from backend.app.services.storage import projects_v2_dual_read as DR  # noqa: E402
from backend.app.services.storage.projects_v2_dual_read import DualReadService  # noqa: E402
from backend.app.main import app  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"
OBJ_DISPLAY = '213. Мосфильмовская 31А "King&Sons"'
OID = "0b540226"
SHADOW = "/api/projects-v2-shadow"
client = TestClient(app)


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _legacy_out(legacy_dir: Path, *, has01, has02, findings, pipeline=True):
    out = legacy_dir / "_output"
    out.mkdir(parents=True, exist_ok=True)
    if has01:
        _wj(out / "02_text_analysis.json", {"x": 1})
    if has02:
        _wj(out / "01_blocks_analysis.json", {"blocks": []})
    if findings is not None:
        _wj(out / "03_findings.json", {"findings": [{"severity": "Критическое"}] * findings})
    if pipeline and findings is not None:
        _wj(out / "pipeline_log.json", {"stages": {"findings": {"status": "done"}}})
    return out


def _build(tmp_path):
    v2 = tmp_path / "projects_v2"
    legacy = tmp_path / "projects"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OID, "display_name": OBJ_DISPLAY, "folder_name": OBJF})
    migs = []

    def doc(disc, code, *, status, has01, has02, findings, legacy_findings=None,
            migration_kind=None, kb=None, with_legacy=True, with_map=True,
            with_lp=True, pipeline_in="99_service"):
        legacy_findings = findings if legacy_findings is None else legacy_findings
        d = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
        legacy_dir = legacy / OBJ_DISPLAY / disc / code
        dj = {"document_code": code, "object_id": OID, "discipline": disc, "kind": "plain",
              "versions": [{"version_id": "v001", "version_no": 1}], "current_version": "v001"}
        if with_lp:
            dj["legacy_project_path"] = str(legacy_dir)
        if migration_kind:
            dj["migration_kind"] = migration_kind
        _wj(d / "document.json", dj)
        (d / "current_version.txt").write_text("v001\n", encoding="utf-8")
        vj = {"version_id": "v001", "version_no": 1, "analysis_status": status}
        if migration_kind:
            vj["migration_kind"] = migration_kind
        _wj(d / "versions/v001/version.json", vj)
        (d / "versions/v001/01_input").mkdir(parents=True, exist_ok=True)
        (d / "versions/v001/01_input/a.pdf").write_text("x", encoding="utf-8")
        latest = d / "versions/v001/03_analysis/latest"
        if has01:
            _wj(latest / "02_text_analysis.json", {"x": 1})
        if has02:
            _wj(latest / "01_blocks_analysis.json", {"blocks": []})
        if findings is not None:
            _wj(latest / "03_findings.json", {"findings": [{"severity": "Критическое"}] * findings})
            if pipeline_in == "99_service":
                _wj(d / "versions/v001/99_service/pipeline_log.json", {"stages": {"f": {}}})
            elif pipeline_in == "latest":
                _wj(latest / "pipeline_log.json", {"stages": {"f": {}}})
        if kb is not None:
            _wj(d / "versions/v001/04_review/kb_decisions_link.json",
                {"entry_count": kb, "entries": []})
        files = []
        if with_legacy:
            lout = _legacy_out(legacy_dir, has01=has01, has02=has02, findings=legacy_findings)
            if legacy_findings is not None:
                files.append({"old_path": str(lout / "03_findings.json"),
                              "new_path": str(latest / "03_findings.json")})
        if with_map:
            migs.append({"object_id": OID, "document_code": code, "version_id": "v001",
                         "legacy_folder_path": str(legacy_dir), "files": files})

    doc("AI", "doc-complete", status="complete", has01=True, has02=True, findings=10)
    doc("AI", "doc-partial", status="partial", has01=True, has02=False, findings=3)
    doc("OV", "doc-none", status="none", has01=False, has02=False, findings=None)
    doc("ITP", "doc-source", status="source_only", has01=False, has02=False,
        findings=None, migration_kind="legacy_findings_preserve")
    doc("SS", "doc-ak", status="legacy_partial", has01=True, has02=True, findings=8,
        migration_kind="legacy_findings_preserve", kb=4, pipeline_in="latest")
    doc("EOM", "doc-loss", status="complete", has01=True, has02=True,
        findings=3, legacy_findings=9)
    # missing_legacy: нет lp и нет map → legacy не находится, без findings
    doc("KM", "doc-nolegacy", status="none", has01=False, has02=False, findings=None,
        with_legacy=False, with_map=False, with_lp=False)

    _wj(v2 / "_system" / "old_to_new_map.json", {"schema_version": 1, "migrations": migs})
    decisions = [{"source_project": "doc-ak", "item_id": f"F-{i}"} for i in range(1, 5)]
    _wj(tmp_path / "knowledge_base" / "decisions_log.json", {"entries": decisions})
    return v2, legacy


# ---------------------------------------------------------------------------
# service
# ---------------------------------------------------------------------------


def test_dual_read_match(tmp_path):
    v2, _ = _build(tmp_path)
    r = DualReadService(v2).compare_document("doc-complete")
    assert r["status"] == DR.MATCH
    assert r["findings_legacy"] == r["findings_v2"] == 10
    assert r["findings_loss"] is False


def test_dual_read_expected_difference_kingsons(tmp_path):
    v2, _ = _build(tmp_path)
    r = DualReadService(v2).compare_document("doc-ak")
    assert r["status"] == DR.EXPECTED
    assert r["is_kingsons_preserve"] is True
    assert r["findings_legacy"] == r["findings_v2"] == 8
    kb = next(f for f in r["fields"] if f["field"] == "kb_link_entry_count")
    assert kb["status"] == DR.MATCH and kb["legacy"] == 4 and kb["v2"] == 4


def test_dual_read_source_only_no_crash(tmp_path):
    v2, _ = _build(tmp_path)
    r = DualReadService(v2).compare_document("doc-source")
    assert r["status"] in (DR.MATCH, DR.EXPECTED)
    assert r["findings_loss"] is False


def test_dual_read_mismatch_detection(tmp_path):
    v2, _ = _build(tmp_path)
    r = DualReadService(v2).compare_document("doc-loss")
    assert r["status"] == DR.MISMATCH
    assert r["findings_loss"] is True
    fc = next(f for f in r["fields"] if f["field"] == "findings_count")
    assert fc["status"] == DR.MISMATCH and fc["legacy"] == 9 and fc["v2"] == 3


def test_dual_read_missing_legacy(tmp_path):
    v2, _ = _build(tmp_path)
    r = DualReadService(v2).compare_document("doc-nolegacy")
    assert r["status"] == DR.MISSING_LEGACY


def test_dual_read_missing_v2(tmp_path):
    v2, _ = _build(tmp_path)
    r = DualReadService(v2).compare_document("does-not-exist")
    assert r["status"] == DR.MISSING_V2


def test_dual_read_sample(tmp_path):
    v2, _ = _build(tmp_path)
    s = DualReadService(v2).sample(per_type=3)
    assert s["documents_checked"] >= 6
    assert DR.MISMATCH in s["status_counts"]  # doc-loss присутствует
    assert "doc-loss" in s["mismatches"]
    assert "doc-loss" in s["findings_losses"]


def test_service_read_only(tmp_path):
    v2, legacy = _build(tmp_path)
    before = {p: p.read_bytes() for p in (v2 / "objects").rglob("*") if p.is_file()}
    lbefore = {p: p.read_bytes() for p in legacy.rglob("*") if p.is_file()}
    DualReadService(v2).sample(per_type=3)
    after = {p: p.read_bytes() for p in (v2 / "objects").rglob("*") if p.is_file()}
    lafter = {p: p.read_bytes() for p in legacy.rglob("*") if p.is_file()}
    assert before == after and lbefore == lafter


# ---------------------------------------------------------------------------
# endpoints
# ---------------------------------------------------------------------------


def test_dual_read_endpoints_disabled_by_default(tmp_path, monkeypatch):
    v2, _ = _build(tmp_path)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.delenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", raising=False)
    assert client.get(f"{SHADOW}/dual-read/sample").status_code == 404
    assert client.get(f"{SHADOW}/dual-read/document/doc-complete").status_code == 404
    assert client.get(f"{SHADOW}/cutover-readiness").status_code == 404


def test_dual_read_endpoints_enabled(tmp_path, monkeypatch):
    v2, _ = _build(tmp_path)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", "true")
    s = client.get(f"{SHADOW}/dual-read/sample?per_type=2")
    assert s.status_code == 200 and "doc-loss" in s.json()["mismatches"]
    d = client.get(f"{SHADOW}/dual-read/document/{urllib.parse.quote('doc-complete')}")
    assert d.status_code == 200 and d.json()["status"] == DR.MATCH
    cr = client.get(f"{SHADOW}/cutover-readiness")
    assert cr.status_code == 200 and "recommendation" in cr.json()
    assert client.get(f"{SHADOW}/dual-read/document/NOPE").status_code == 404
