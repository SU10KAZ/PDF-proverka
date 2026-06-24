"""
Тесты UI/API contract parity (check_ui_contract_parity) + shadow endpoint
/ui-contract/sample. Гермётичны: строим legacy + projects_v2 + old_to_new_map в
tmp_path, projects_root прокидываем явно.
"""

from __future__ import annotations

import json
import sys
import urllib.parse
from pathlib import Path

from fastapi.testclient import TestClient

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "scripts" / "projects_v2"))
import check_ui_contract_parity as UC  # noqa: E402
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter  # noqa: E402
from backend.app.main import app  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"
OBJ_DISPLAY = '213. Мосфильмовская 31А "King&Sons"'
OID = "0b540226"
client = TestClient(app)


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _legacy_output(legacy_dir: Path, *, has01, has02, findings, pipeline=True):
    out = legacy_dir / "_output"
    out.mkdir(parents=True, exist_ok=True)
    if has01:
        _wj(out / "01_text_analysis.json", {"x": 1})
    if has02:
        _wj(out / "02_blocks_analysis.json", {"blocks": []})
    if findings is not None:
        _wj(out / "03_findings.json", {"findings": [{"severity": "Критическое"}] * findings})
    if pipeline:
        _wj(out / "pipeline_log.json", {"stages": {"findings": {"status": "done"}}})
    return out


def _v2_version(doc: Path, vid, vno, *, status, has01, has02, findings,
                migration_kind=None, kb=None, pipeline_in="99_service"):
    vroot = doc / "versions" / vid
    vj = {"version_id": vid, "version_no": vno, "analysis_status": status}
    if migration_kind:
        vj["migration_kind"] = migration_kind
    _wj(vroot / "version.json", vj)
    (vroot / "01_input").mkdir(parents=True, exist_ok=True)
    (vroot / "01_input" / "a.pdf").write_text("x", encoding="utf-8")
    latest = vroot / "03_analysis" / "latest"
    if has01:
        _wj(latest / "01_text_analysis.json", {"x": 1})
    if has02:
        _wj(latest / "02_blocks_analysis.json", {"blocks": []})
    if findings is not None:
        _wj(latest / "03_findings.json", {"findings": [{"severity": "Критическое"}] * findings})
        if pipeline_in == "99_service":
            _wj(vroot / "99_service" / "pipeline_log.json", {"stages": {"findings": {"status": "done"}}})
        elif pipeline_in == "latest":
            _wj(latest / "pipeline_log.json", {"stages": {"findings": {"status": "done"}}})
    if kb is not None:
        _wj(vroot / "04_review" / "kb_decisions_link.json", {"entry_count": kb, "entries": []})


def _build(tmp_path, *, findings_loss=False):
    v2 = tmp_path / "projects_v2"
    legacy = tmp_path / "projects"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OID, "display_name": OBJ_DISPLAY, "folder_name": OBJF})
    migs = []

    def plain(disc, code, *, status, has01, has02, findings, migration_kind=None,
              kb=None, legacy_findings=None, pipeline_in="99_service"):
        legacy_findings = findings if legacy_findings is None else legacy_findings
        doc = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
        legacy_dir = legacy / OBJ_DISPLAY / disc / code
        dj = {"document_code": code, "object_id": OID, "discipline": disc, "kind": "plain",
              "versions": [{"version_id": "v001", "version_no": 1}],
              "current_version": "v001", "legacy_project_path": str(legacy_dir)}
        if migration_kind:
            dj["migration_kind"] = migration_kind
        _wj(doc / "document.json", dj)
        (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
        _v2_version(doc, "v001", 1, status=status, has01=has01, has02=has02,
                    findings=findings, migration_kind=migration_kind, kb=kb,
                    pipeline_in=pipeline_in)
        lout = _legacy_output(legacy_dir, has01=has01, has02=has02,
                              findings=legacy_findings, pipeline=(legacy_findings is not None))
        files = []
        if legacy_findings is not None:
            files.append({"old_path": str(lout / "03_findings.json"),
                          "new_path": str(doc / "versions/v001/03_analysis/latest/03_findings.json")})
        migs.append({"object_id": OID, "document_code": code, "version_id": "v001",
                     "legacy_folder_path": str(legacy_dir), "files": files})

    # complete (legacy==v2)
    plain("AI", "doc-complete", status="complete", has01=True, has02=True, findings=10)
    # partial
    plain("AI", "doc-partial", status="partial", has01=True, has02=False, findings=3)
    # none
    plain("OV", "doc-none", status="none", has01=False, has02=False, findings=None)
    # source_only (King&Sons, no analysis)
    plain("ITP", "doc-source", status="source_only", has01=False, has02=False,
          findings=None, migration_kind="legacy_findings_preserve")
    # legacy_partial King&Sons with findings + KB link (АК-like)
    decisions = [{"source_project": "doc-ak", "item_id": f"F-{i}"} for i in range(1, 5)]
    plain("SS", "doc-ak", status="legacy_partial", has01=True, has02=True, findings=8,
          migration_kind="legacy_findings_preserve", kb=4, pipeline_in="latest")
    if findings_loss:
        # v2 теряет часть findings относительно legacy
        plain("EOM", "doc-loss", status="complete", has01=True, has02=True,
              findings=3, legacy_findings=9)

    _wj(v2 / "_system" / "old_to_new_map.json", {"schema_version": 1, "migrations": migs})
    _wj(tmp_path / "knowledge_base" / "decisions_log.json", {"entries": decisions})
    return v2, legacy


def _run(tmp_path, **kw):
    v2, legacy = _build(tmp_path, **kw)
    a = ProjectsV2Adapter(v2)
    rep = UC.run_contract_parity(a, projects_root=legacy,
                                 explicit_codes=kw.get("codes"))
    return rep, v2, legacy


# ---------------------------------------------------------------------------


def test_complete_document_match(tmp_path):
    v2, legacy = _build(tmp_path)
    a = ProjectsV2Adapter(v2)
    rep = UC.run_contract_parity(a, projects_root=legacy, explicit_codes=["doc-complete"])
    r = rep["results"][0]
    assert r["doc_status"] == UC.MATCH
    fc = next(f for f in r["fields"] if f["field"] == "findings_count")
    assert fc["status"] == UC.MATCH and fc["legacy"] == fc["v2"] == 10
    assert r["findings_loss"] is False


def test_partial_document_handled(tmp_path):
    v2, legacy = _build(tmp_path)
    a = ProjectsV2Adapter(v2)
    rep = UC.run_contract_parity(a, projects_root=legacy, explicit_codes=["doc-partial"])
    r = rep["results"][0]
    assert r["doc_status"] in (UC.MATCH, UC.EXPECTED)  # не MISMATCH
    assert r["findings_loss"] is False


def test_none_and_source_only_no_crash(tmp_path):
    v2, legacy = _build(tmp_path)
    a = ProjectsV2Adapter(v2)
    rep = UC.run_contract_parity(a, projects_root=legacy,
                                 explicit_codes=["doc-none", "doc-source"])
    statuses = {r["document_code"]: r["doc_status"] for r in rep["results"]}
    assert statuses["doc-none"] == UC.MATCH
    # source_only — King&Sons → expected difference, не mismatch
    assert statuses["doc-source"] in (UC.MATCH, UC.EXPECTED)
    assert all(not r["findings_loss"] for r in rep["results"])


def test_kingsons_legacy_partial_preserves_findings_and_kb(tmp_path):
    v2, legacy = _build(tmp_path)
    a = ProjectsV2Adapter(v2)
    rep = UC.run_contract_parity(a, projects_root=legacy, explicit_codes=["doc-ak"])
    r = rep["results"][0]
    assert r["is_kingsons_preserve"] is True
    assert r["doc_status"] == UC.EXPECTED          # из-за legacy_partial-статуса
    assert r["findings_loss"] is False
    fc = next(f for f in r["fields"] if f["field"] == "findings_count")
    assert fc["status"] == UC.MATCH and fc["v2"] == 8
    kb = next(f for f in r["fields"] if f["field"] == "kb_link_entry_count")
    assert kb["status"] == UC.MATCH and kb["legacy"] == 4 and kb["v2"] == 4
    # analysis_status — expected difference (legacy derived complete, v2 legacy_partial)
    st = next(f for f in r["fields"] if f["field"] == "analysis_status")
    assert st["status"] == UC.EXPECTED


def test_findings_mismatch_caught(tmp_path):
    v2, legacy = _build(tmp_path, findings_loss=True)
    a = ProjectsV2Adapter(v2)
    rep = UC.run_contract_parity(a, projects_root=legacy, explicit_codes=["doc-loss"])
    r = rep["results"][0]
    assert r["doc_status"] == UC.MISMATCH
    assert r["findings_loss"] is True
    assert rep["contract_ok"] is False
    assert "doc-loss" in rep["findings_losses"]
    fc = next(f for f in r["fields"] if f["field"] == "findings_count")
    assert fc["status"] == UC.MISMATCH and fc["legacy"] == 9 and fc["v2"] == 3


def test_report_written(tmp_path):
    v2, legacy = _build(tmp_path)
    a = ProjectsV2Adapter(v2)
    rep = UC.run_contract_parity(a, projects_root=legacy)
    jp, mp, cp = UC.write_reports(rep, v2)
    assert jp.exists() and mp.exists() and cp.exists()
    assert "UI/API contract parity" in mp.read_text(encoding="utf-8")
    data = json.loads(jp.read_text(encoding="utf-8"))
    assert data["documents_checked"] >= 5
    assert "doc_status_counts" in data and "field_status_counts" in data


def test_full_run_no_loss_contract_ok(tmp_path):
    v2, legacy = _build(tmp_path)
    a = ProjectsV2Adapter(v2)
    rep = UC.run_contract_parity(a, projects_root=legacy)
    assert rep["contract_ok"] is True
    assert rep["any_findings_loss"] is False
    assert rep["any_version_loss"] is False
    assert rep["doc_status_counts"][UC.MISMATCH] == 0


def test_all_docs_mode_checks_whole_corpus(tmp_path):
    v2, legacy = _build(tmp_path)
    a = ProjectsV2Adapter(v2)
    total = len(a.list_documents())
    rep = UC.run_contract_parity(a, projects_root=legacy, all_docs=True)
    assert rep["documents_checked"] == total  # весь корпус, не выборка
    assert rep["doc_status_counts"][UC.MISMATCH] == 0
    assert rep["any_findings_loss"] is False and rep["any_version_loss"] is False


def test_write_reports_custom_stem(tmp_path):
    v2, legacy = _build(tmp_path)
    a = ProjectsV2Adapter(v2)
    rep = UC.run_contract_parity(a, projects_root=legacy, all_docs=True)
    jp, mp, cp = UC.write_reports(rep, v2, stem="full_corpus_parity_report")
    assert jp.name == "full_corpus_parity_report.json"
    assert jp.exists() and mp.exists() and cp.exists()


def test_read_only_projects_v2_unchanged(tmp_path):
    v2, legacy = _build(tmp_path)
    before = {p: (p.stat().st_mtime_ns, p.read_bytes())
              for p in (v2 / "objects").rglob("*") if p.is_file()}
    legacy_before = {p: (p.stat().st_mtime_ns, p.read_bytes())
                     for p in legacy.rglob("*") if p.is_file()}
    a = ProjectsV2Adapter(v2)
    UC.run_contract_parity(a, projects_root=legacy)  # no write_reports here
    after = {p: (p.stat().st_mtime_ns, p.read_bytes())
             for p in (v2 / "objects").rglob("*") if p.is_file()}
    legacy_after = {p: (p.stat().st_mtime_ns, p.read_bytes())
                    for p in legacy.rglob("*") if p.is_file()}
    assert before == after, "projects_v2/objects must be unchanged"
    assert legacy_before == legacy_after, "legacy projects/ must be unchanged"


# ---------------------------------------------------------------------------
# shadow endpoint /ui-contract/sample
# ---------------------------------------------------------------------------


def test_ui_contract_endpoint_disabled_by_default(tmp_path, monkeypatch):
    v2, _ = _build(tmp_path)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.delenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", raising=False)
    assert client.get("/api/projects-v2-shadow/ui-contract/sample").status_code == 404


def test_ui_contract_endpoint_enabled(tmp_path, monkeypatch):
    v2, _ = _build(tmp_path)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_SHADOW_API_ENABLED", "true")
    r = client.get("/api/projects-v2-shadow/ui-contract/sample?per_type=2")
    assert r.status_code == 200
    j = r.json()
    assert j["count"] >= 5
    codes = {s["document_code"]: s for s in j["sample"]}
    assert codes["doc-complete"]["findings_count"] == 10
    assert codes["doc-complete"]["analysis_status"] == "complete"
    ak = codes["doc-ak"]
    assert ak["is_legacy_preserve"] is True and ak["kb_link_entry_count"] == 4


# ---------------------------------------------------------------------------
# tooling-fix (2026-06-16): robust legacy matcher — naming artifacts больше не
# дают ложный MISSING_IN_LEGACY/MISMATCH; реальные потери всё ещё ловятся.
# ---------------------------------------------------------------------------


def _single(tmp_path, *, disc, code, legacy_folder_name=None, in_map=True,
            doc_legacy_path=None, make_legacy_dir=True, findings=None,
            v2_versions=None, vg_versions=None):
    """Гермётично: один v2-документ + (опц.) legacy-папка с произвольным именем +
    old_to_new_map. Возвращает (result, report). legacy_folder_name может отличаться
    от code (`.pdf` / `(main)` / ` V2`). vg_versions → version_group.json контейнера.
    """
    v2 = tmp_path / "projects_v2"
    legacy = tmp_path / "projects"
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OID, "display_name": OBJ_DISPLAY, "folder_name": OBJF})
    legacy_folder_name = legacy_folder_name or code
    v2_versions = v2_versions or [{"version_id": "v001", "version_no": 1}]
    doc = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    legacy_dir = legacy / OBJ_DISPLAY / disc / legacy_folder_name
    dj = {"document_code": code, "object_id": OID, "discipline": disc, "kind": "plain",
          "versions": v2_versions, "current_version": v2_versions[-1]["version_id"]}
    dj["legacy_project_path"] = (doc_legacy_path if doc_legacy_path is not None
                                 else str(legacy_dir))
    _wj(doc / "document.json", dj)
    (doc / "current_version.txt").write_text(
        v2_versions[-1]["version_id"] + "\n", encoding="utf-8")
    st = "complete" if findings is not None else "none"
    for v in v2_versions:
        _v2_version(doc, v["version_id"], v["version_no"], status=st,
                    has01=findings is not None, has02=findings is not None,
                    findings=findings)
    if make_legacy_dir:
        legacy_dir.mkdir(parents=True, exist_ok=True)
        if vg_versions is not None:
            _wj(legacy_dir / "version_group.json",
                {"logical_project_id": code,
                 "latest_version_id": f"v{len(vg_versions)}", "versions": vg_versions})
        if findings is not None:
            _legacy_output(legacy_dir, has01=True, has02=True, findings=findings)
    migs = []
    if in_map:
        migs.append({"object_id": OID, "object_name": OBJ_DISPLAY, "discipline": disc,
                     "document_code": code, "version_id": v2_versions[-1]["version_id"],
                     "version_no": v2_versions[-1]["version_no"],
                     "legacy_folder_name": legacy_folder_name,
                     "legacy_folder_path": str(legacy_dir), "files": []})
    _wj(v2 / "_system" / "old_to_new_map.json", {"schema_version": 1, "migrations": migs})
    _wj(tmp_path / "knowledge_base" / "decisions_log.json", {"entries": []})
    rep = UC.run_contract_parity(ProjectsV2Adapter(v2), projects_root=legacy,
                                 explicit_codes=[code])
    return rep["results"][0], rep


def test_pdf_suffix_legacy_id_no_false_missing(tmp_path):
    """`.pdf` в имени legacy-папки → EXPECTED_NAMING_DIFFERENCE, не MISSING/MISMATCH."""
    r, rep = _single(tmp_path, disc="VK", code="13АВ-РД-ВК1-К2",
                     legacy_folder_name="13АВ-РД-ВК1-К2.pdf", findings=None)
    assert r["doc_status"] == UC.EXPECTED_NAMING
    assert r["naming_diff"] is True and r["legacy_exists"] is True
    assert rep["doc_status_counts"][UC.MISSING_LEGACY] == 0
    assert rep["doc_status_counts"][UC.MISMATCH] == 0
    nm = next(f for f in r["fields"] if f["field"] == "legacy_folder_naming")
    assert nm["status"] == UC.EXPECTED_NAMING and nm["legacy"] == "13АВ-РД-ВК1-К2.pdf"
    dc = next(f for f in r["fields"] if f["field"] == "document_code")
    assert dc["status"] == UC.MATCH  # коды связаны через old_to_new_map


def test_version_suffix_legacy_folder_no_false_mismatch(tmp_path):
    """legacy `<base> V2` folder ↔ чистый v2 document_code → EXPECTED_NAMING, не MISMATCH."""
    r, rep = _single(tmp_path, disc="AR", code="133_23-ГК-АР2",
                     legacy_folder_name="133_23-ГК-АР2 V2", findings=5)
    assert r["doc_status"] == UC.EXPECTED_NAMING
    assert r["findings_loss"] is False and rep["contract_ok"] is True
    dc = next(f for f in r["fields"] if f["field"] == "document_code")
    assert dc["status"] == UC.MATCH


def test_main_container_matched(tmp_path):
    """legacy `(main)` контейнер сопоставляется (document_code_for → logical_project_id)."""
    r, rep = _single(tmp_path, disc="EOM", code="133_23-ГК-ЭМ1",
                     legacy_folder_name="133_23-ГК-ЭМ1(main)",
                     vg_versions=[{"version_id": "v1", "version_no": 1}], findings=None)
    assert r["doc_status"] in (UC.MATCH, UC.EXPECTED_NAMING)
    assert r["legacy_exists"] is True
    assert rep["doc_status_counts"][UC.MISSING_LEGACY] == 0
    assert rep["doc_status_counts"][UC.MISMATCH] == 0


def test_legacy_folder_name_used_when_doc_path_empty(tmp_path):
    """document.json без legacy_project_path → резолв через map (object/discipline/folder)."""
    r, rep = _single(tmp_path, disc="OV", code="doc-mapname", doc_legacy_path="",
                     legacy_folder_name="doc-mapname", findings=4)
    assert r["legacy_match_via"] == "old_to_new_map"
    assert r["doc_status"] == UC.MATCH and r["legacy_exists"] is True
    obj = next(f for f in r["fields"] if f["field"] == "object_display_name")
    assert obj["status"] == UC.MATCH and obj["legacy"] == OBJ_DISPLAY


def test_old_to_new_map_has_priority_over_bogus_doc_path(tmp_path):
    """document.json указывает на несуществующий путь, но map верный → резолв по map."""
    r, rep = _single(tmp_path, disc="OV", code="doc-prio",
                     doc_legacy_path="/nonexistent/wrong/path",
                     legacy_folder_name="doc-prio", findings=2)
    assert r["legacy_match_via"] == "old_to_new_map"
    assert r["legacy_exists"] is True and r["doc_status"] == UC.MATCH


def test_real_missing_legacy_still_caught(tmp_path):
    """Нет map-записи, legacy_project_path битый, папки нет → MISSING_IN_LEGACY (ловится)."""
    r, rep = _single(tmp_path, disc="OV", code="doc-ghost", in_map=False,
                     doc_legacy_path="/nonexistent/ghost", make_legacy_dir=False,
                     findings=None)
    assert r["doc_status"] == UC.MISSING_LEGACY
    assert r["legacy_exists"] is False
    assert "doc-ghost" in rep["missing_in_legacy_real"]


def test_findings_loss_still_caught_after_fix(tmp_path):
    """Реальная потеря findings (v2 < legacy) всё ещё → MISMATCH + findings_loss."""
    v2, legacy = _build(tmp_path, findings_loss=True)
    rep = UC.run_contract_parity(ProjectsV2Adapter(v2), projects_root=legacy,
                                 explicit_codes=["doc-loss"])
    r = rep["results"][0]
    assert r["doc_status"] == UC.MISMATCH and r["findings_loss"] is True
    assert "doc-loss" in rep["findings_losses"] and rep["contract_ok"] is False


def test_version_loss_still_caught_after_fix(tmp_path):
    """legacy (main) с 3 версиями, v2 — 1 (не King&Sons) → version_count MISMATCH + version_loss."""
    r, rep = _single(tmp_path, disc="AR", code="doc-vloss",
                     legacy_folder_name="doc-vloss(main)",
                     vg_versions=[{"version_id": f"v{i}", "version_no": i} for i in (1, 2, 3)],
                     v2_versions=[{"version_id": "v001", "version_no": 1}], findings=2)
    assert r["version_loss"] is True and r["doc_status"] == UC.MISMATCH
    assert "doc-vloss" in rep["version_losses"]
    vc = next(f for f in r["fields"] if f["field"] == "version_count")
    assert vc["status"] == UC.MISMATCH and vc["legacy"] == 3 and vc["v2"] == 1
