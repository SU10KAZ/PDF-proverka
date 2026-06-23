"""
Тесты cutover readiness (projects_v2_dual_read.cutover_readiness + CLI build) +
endpoint /cutover-readiness. Синтетика: legacy+v2+map+_system reports в tmp_path.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from fastapi.testclient import TestClient

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "scripts" / "projects_v2"))
from backend.app.services.storage.projects_v2_dual_read import (  # noqa: E402
    cutover_readiness, REC_NOT_READY, REC_SHADOW_PROD, REC_CANARY,
)
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter  # noqa: E402
from backend.app.main import app  # noqa: E402
import check_cutover_readiness as CC  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"
OBJ_DISPLAY = '213. Мосфильмовская 31А "King&Sons"'
OID = "0b540226"
SHADOW = "/api/projects-v2-shadow"
client = TestClient(app)


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _matched_doc(v2, legacy, migs, disc, code, *, status, findings):
    d = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    legacy_dir = legacy / OBJ_DISPLAY / disc / code
    _wj(d / "document.json", {"document_code": code, "object_id": OID, "discipline": disc,
                              "kind": "plain", "versions": [{"version_id": "v001", "version_no": 1}],
                              "current_version": "v001", "legacy_project_path": str(legacy_dir)})
    (d / "current_version.txt").write_text("v001\n", encoding="utf-8")
    _wj(d / "versions/v001/version.json",
        {"version_id": "v001", "version_no": 1, "analysis_status": status})
    (d / "versions/v001/01_input").mkdir(parents=True, exist_ok=True)
    (d / "versions/v001/01_input/a.pdf").write_text("x", encoding="utf-8")
    latest = d / "versions/v001/03_analysis/latest"
    out = legacy_dir / "_output"
    out.mkdir(parents=True, exist_ok=True)
    files = []
    # для complete пишем 01/02/03 в ОБЕ стороны, чтобы legacy-derived статус
    # совпал с v2 (иначе legacy=partial vs v2=complete → ложный mismatch)
    if status == "complete":
        for n in ("01_text_analysis.json", "02_blocks_analysis.json"):
            _wj(latest / n, {"x": 1})
            _wj(out / n, {"x": 1})
    if findings is not None:
        _wj(latest / "03_findings.json", {"findings": [{"severity": "x"}] * findings})
        _wj(out / "03_findings.json", {"findings": [{"severity": "x"}] * findings})
        files.append({"old_path": str(out / "03_findings.json"),
                      "new_path": str(latest / "03_findings.json")})
    migs.append({"object_id": OID, "document_code": code, "version_id": "v001",
                 "legacy_folder_path": str(legacy_dir), "files": files})


def _build(tmp_path, *, drift_docs=0, contract_ok=True, contract_mismatch=0,
           contract_checked=2, bparity_ok=True, bparity_noloss=True):
    v2 = tmp_path / "projects_v2"
    legacy = tmp_path / "projects"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OID, "display_name": OBJ_DISPLAY, "folder_name": OBJF})
    migs = []
    _matched_doc(v2, legacy, migs, "AI", "doc-complete", status="complete", findings=10)
    _matched_doc(v2, legacy, migs, "OV", "doc-none", status="none", findings=None)
    _wj(v2 / "_system" / "old_to_new_map.json", {"schema_version": 1, "migrations": migs})
    _wj(v2 / "_system" / "migrated_drift_scan_report.json",
        {"summary": {"drift_documents": drift_docs, "unstable": 0, "stable": drift_docs}})
    _wj(v2 / "_system" / "ui_contract_parity_report.json",
        {"contract_ok": contract_ok, "documents_checked": contract_checked,
         "doc_status_counts": {"MATCH": contract_checked - contract_mismatch,
                               "MISMATCH": contract_mismatch}})
    _wj(v2 / "_system" / "backend_parity_report.json",
        {"parity_ok": bparity_ok, "findings_no_loss_overall": bparity_noloss})
    _wj(tmp_path / "knowledge_base" / "decisions_log.json", {"entries": []})
    return v2


# ---------------------------------------------------------------------------


def test_canary_when_full_corpus_green(tmp_path):
    # contract_checked (2) >= v2 docs (2) → full corpus
    v2 = _build(tmp_path, contract_checked=2)
    rep = cutover_readiness(ProjectsV2Adapter(v2), validate_status="PASS", per_type=3)
    assert rep["recommendation"] == REC_CANARY
    assert rep["dual_read_sample"]["ok"] is True
    assert rep["total_mismatches"] == 0


def test_shadow_prod_when_sampled_only(tmp_path):
    # contract_checked (1) < v2 docs (2) → не full corpus
    v2 = _build(tmp_path, contract_checked=1)
    rep = cutover_readiness(ProjectsV2Adapter(v2), validate_status="PASS", per_type=3)
    assert rep["recommendation"] == REC_SHADOW_PROD


def test_not_ready_validate_fail(tmp_path):
    v2 = _build(tmp_path)
    rep = cutover_readiness(ProjectsV2Adapter(v2), validate_status="FAIL", per_type=3)
    assert rep["recommendation"] == REC_NOT_READY


def test_not_ready_drift(tmp_path):
    v2 = _build(tmp_path, drift_docs=3)
    rep = cutover_readiness(ProjectsV2Adapter(v2), validate_status="PASS", per_type=3)
    assert rep["recommendation"] == REC_NOT_READY
    assert rep["drift"]["ok"] is False


def test_not_ready_contract_mismatch(tmp_path):
    v2 = _build(tmp_path, contract_ok=False, contract_mismatch=1)
    rep = cutover_readiness(ProjectsV2Adapter(v2), validate_status="PASS", per_type=3)
    assert rep["recommendation"] == REC_NOT_READY


def test_not_ready_when_validate_unknown(tmp_path):
    v2 = _build(tmp_path)
    rep = cutover_readiness(ProjectsV2Adapter(v2), validate_status=None, per_type=3)
    assert rep["recommendation"] == REC_NOT_READY  # validate неизвестен → консервативно


def test_cli_build_writes_report(tmp_path):
    v2 = _build(tmp_path, contract_checked=2)
    rep = CC.build(v2, per_type=3, run_validate_flag=False)  # без subprocess
    jp, mp = CC.write_reports(rep, v2)
    assert jp.exists() and mp.exists()
    assert "Cutover readiness" in mp.read_text(encoding="utf-8")
    # validate не запускался → status None → not_ready (консервативно)
    assert rep["recommendation"] == REC_NOT_READY


def test_endpoint_cutover_readiness(tmp_path, monkeypatch):
    v2 = _build(tmp_path, contract_checked=2)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.delenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", raising=False)
    assert client.get(f"{SHADOW}/cutover-readiness").status_code == 404
    monkeypatch.setenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", "true")
    r = client.get(f"{SHADOW}/cutover-readiness")
    assert r.status_code == 200
    assert "recommendation" in r.json()
    assert r.json()["storage_backend_default"] == "legacy"


def test_readiness_read_only(tmp_path):
    v2 = _build(tmp_path, contract_checked=2)
    before = {p: p.read_bytes() for p in (v2 / "objects").rglob("*") if p.is_file()}
    cutover_readiness(ProjectsV2Adapter(v2), validate_status="PASS", per_type=3)
    after = {p: p.read_bytes() for p in (v2 / "objects").rglob("*") if p.is_file()}
    assert before == after
