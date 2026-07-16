"""
Тесты parity-CLI (check_backend_parity): сравнение legacy ↔ projects_v2 через
read-only adapter. Гермётичны (tmp_path): строим v2 + legacy + old_to_new_map.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "scripts" / "projects_v2"))
import check_backend_parity as P  # noqa: E402
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"
OID = "0b540226"


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _make_pair(tmp_path, code, disc, *, v2_findings, legacy_findings,
               latest_extra=("02_text_analysis.json", "01_blocks_analysis.json"),
               status="complete"):
    """Создаёт документ в v2 + соответствующий legacy + запись в map."""
    v2 = tmp_path / "projects_v2"
    legacy = tmp_path / "projects"
    # v2 doc
    doc = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    _wj(doc / "document.json", {
        "document_code": code, "object_id": OID, "discipline": disc, "kind": "plain",
        "versions": [{"version_id": "v001", "version_no": 1}],
        "current_version": "v001",
        "legacy_project_path": str(legacy / disc / code)})
    (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
    _wj(doc / "versions/v001/version.json",
        {"version_id": "v001", "version_no": 1, "analysis_status": status})
    (doc / "versions/v001/01_input").mkdir(parents=True, exist_ok=True)
    (doc / "versions/v001/01_input/a.pdf").write_text("x", encoding="utf-8")
    latest = doc / "versions/v001/03_analysis/latest"
    for n in latest_extra:
        _wj(latest / n, {"x": 1})
    files = []
    if v2_findings is not None:
        _wj(latest / "03_findings.json", {"findings": [{"severity": "x"}] * v2_findings})
        _wj(doc / "versions/v001/99_service/pipeline_log.json", {"stages": {}})
    # legacy _output
    legout = legacy / disc / code / "_output"
    legout.mkdir(parents=True, exist_ok=True)
    for n in latest_extra:
        _wj(legout / n, {"x": 1})
    if legacy_findings is not None:
        _wj(legout / "03_findings.json", {"findings": [{"severity": "x"}] * legacy_findings})
        files.append({"old_path": str(legout / "03_findings.json"),
                      "new_path": str(latest / "03_findings.json")})
    return {"object_id": OID, "document_code": code, "version_id": "v001",
            "legacy_folder_path": str(legacy / disc / code), "files": files}


def _build(tmp_path, *, loss=False):
    v2 = tmp_path / "projects_v2"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OID, "display_name": "213", "folder_name": OBJF})
    migs = []
    migs.append(_make_pair(tmp_path, "doc-complete", "AI", v2_findings=10, legacy_findings=10))
    migs.append(_make_pair(tmp_path, "doc-none", "OV", v2_findings=None, legacy_findings=None,
                           latest_extra=(), status="none"))
    if loss:
        # v2 потеряла часть findings относительно legacy
        migs.append(_make_pair(tmp_path, "doc-loss", "EOM", v2_findings=3, legacy_findings=8))
    _wj(v2 / "_system" / "old_to_new_map.json", {"schema_version": 1, "migrations": migs})
    return v2


# ---------------------------------------------------------------------------


def test_parity_ok_when_matching(tmp_path):
    v2 = _build(tmp_path, loss=False)
    a = ProjectsV2Adapter(v2)
    rep = P.run_parity(a, explicit_codes=["doc-complete", "doc-none"])
    assert rep["parity_ok"] is True
    assert rep["documents_checked"] == 2
    assert rep["failed"] == 0
    assert rep["total_v2_findings"] == 10 and rep["total_legacy_findings"] == 10
    assert rep["findings_no_loss_overall"] is True


def test_parity_detects_findings_loss(tmp_path):
    v2 = _build(tmp_path, loss=True)
    a = ProjectsV2Adapter(v2)
    rep = P.run_parity(a, explicit_codes=["doc-complete", "doc-loss"])
    assert rep["parity_ok"] is False
    loss = next(r for r in rep["results"] if r["document_code"] == "doc-loss")
    assert loss["ok"] is False
    assert "findings_no_loss" in loss["hard_failures"]
    # симметричный подсчёт увидел расхождение
    vr = loss["versions"][0]
    assert vr["v2_findings"] == 3 and vr["legacy_findings"] == 8


def test_findings_count_in_dir_priority(tmp_path):
    d = tmp_path / "_output"
    d.mkdir()
    _wj(d / "03_findings.json", {"findings": [1, 2, 3]})
    assert P.findings_count_in_dir(d) == 3
    # 03a_norms_verified.json имеет приоритет
    _wj(d / "03a_norms_verified.json", {"findings": [1, 2, 3, 4, 5]})
    assert P.findings_count_in_dir(d) == 5
    assert P.findings_count_in_dir(tmp_path / "missing") == 0


def test_report_files_written(tmp_path):
    v2 = _build(tmp_path, loss=False)
    a = ProjectsV2Adapter(v2)
    rep = P.run_parity(a, explicit_codes=["doc-complete", "doc-none"])
    jp, mp = P.write_reports(rep, v2)
    assert jp.exists() and mp.exists()
    assert (v2 / "_system" / "backend_parity_report.csv").exists()
    md = mp.read_text(encoding="utf-8")
    assert "Backend parity report" in md
    data = json.loads(jp.read_text())
    assert data["documents_checked"] == 2


def test_none_documents_zero_findings_parity(tmp_path):
    v2 = _build(tmp_path, loss=False)
    a = ProjectsV2Adapter(v2)
    rep = P.run_parity(a, explicit_codes=["doc-none"])
    r = rep["results"][0]
    assert r["type"] == "none" and r["ok"] is True
    assert r["versions"][0]["v2_findings"] == 0 and r["versions"][0]["legacy_findings"] == 0


def test_auto_select_covers_types(tmp_path):
    v2 = _build(tmp_path, loss=True)
    a = ProjectsV2Adapter(v2)
    rep = P.run_parity(a, per_type=3)
    types = set(rep["by_type"])
    assert {"complete", "none"} <= types
