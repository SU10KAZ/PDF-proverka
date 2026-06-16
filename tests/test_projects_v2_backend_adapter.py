"""
Тесты read-only adapter `projects_v2` (ProjectsV2Adapter). Гермётичны (tmp_path),
проверяют чтение metadata/артефактов, feature flag и инвариант «ничего не пишет».
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root
from backend.app.services.storage import projects_v2_adapter as A  # noqa: E402
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter  # noqa: E402

OBJF = "213_Mosfilmovskaya_31A_KingSons"


def _w(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False) if not isinstance(data, str) else data,
                 encoding="utf-8")


def _mkdoc(v2: Path, disc: str, code: str, *, kind="plain", versions=None,
           current=None, migration_kind=None):
    versions = versions or [{"version_id": "v001", "version_no": 1}]
    doc = v2 / "objects" / OBJF / "disciplines" / disc / "documents" / code
    dj = {"schema_version": 1, "document_code": code, "object_id": "0b540226",
          "discipline": disc, "kind": kind, "versions": versions,
          "current_version": current or versions[-1]["version_id"],
          "legacy_project_path": f"/legacy/{code}"}
    if migration_kind:
        dj["migration_kind"] = migration_kind
    _w(doc / "document.json", dj)
    _w(doc / "current_version.txt", (current or versions[-1]["version_id"]) + "\n")
    return doc


def _mkversion(doc: Path, vid, *, analysis_status, latest=None, inputs=("a.pdf",),
               pipeline_log_in="99_service", migration_kind=None, preserve_reason=None,
               findings=None):
    vroot = doc / "versions" / vid
    vj = {"schema_version": 1, "version_id": vid, "version_no": int(vid[-1]),
          "analysis_status": analysis_status}
    if migration_kind:
        vj["migration_kind"] = migration_kind
    if preserve_reason:
        vj["preserve_reason"] = preserve_reason
    _w(vroot / "version.json", vj)
    for f in inputs:
        _w(vroot / "01_input" / f, "x")
    for name in (latest or []):
        if name == "03_findings.json" and findings is not None:
            _w(vroot / "03_analysis" / "latest" / name, {"findings": findings})
        else:
            _w(vroot / "03_analysis" / "latest" / name, {"x": 1})
    if pipeline_log_in == "99_service":
        _w(vroot / "99_service" / "pipeline_log.json", {"stages": {}})
    elif pipeline_log_in == "latest":
        _w(vroot / "03_analysis" / "latest" / "pipeline_log.json", {"stages": {}})
    return vroot


def _build(tmp_path) -> Path:
    v2 = tmp_path / "projects_v2"
    _w(v2 / "objects" / OBJF / "object.json",
       {"object_id": "0b540226", "display_name": "213 KingSons", "folder_name": OBJF,
        "legacy_path": "/legacy/213"})
    # complete
    d = _mkdoc(v2, "AI", "doc-complete")
    _mkversion(d, "v001", analysis_status="complete",
               latest=["01_text_analysis.json", "02_blocks_analysis.json", "03_findings.json",
                       "03a_norms_verified.json"],
               findings=[{"severity": "Критическое"}, {"severity": "Рекомендательное"}])
    # partial
    d = _mkdoc(v2, "AI", "doc-partial")
    _mkversion(d, "v001", analysis_status="partial",
               latest=["01_text_analysis.json", "03_findings.json"],
               findings=[{"severity": "Критическое"}])
    # none
    d = _mkdoc(v2, "OV", "doc-none")
    _mkversion(d, "v001", analysis_status="none", latest=[], pipeline_log_in=None)
    # source_only (legacy preserve)
    d = _mkdoc(v2, "ITP", "doc-source", migration_kind="legacy_findings_preserve")
    _mkversion(d, "v001", analysis_status="source_only", latest=[], pipeline_log_in=None,
               migration_kind="legacy_findings_preserve",
               preserve_reason="king_sons_source_only_legacy_bundle",
               inputs=("a.pdf", "b.pdf", "c.pdf"))
    # legacy_partial (legacy preserve, pipeline_log in latest)
    d = _mkdoc(v2, "EOM", "doc-legacy", migration_kind="legacy_findings_preserve")
    _mkversion(d, "v001", analysis_status="legacy_partial",
               latest=["01_text_analysis.json", "02_blocks_analysis.json", "03_findings.json"],
               pipeline_log_in="latest", migration_kind="legacy_findings_preserve",
               preserve_reason="king_sons_legacy_findings_preserve",
               findings=[{"severity": "Экономическое"}] * 3)
    # versioned v001 (partial) + v002 (complete)
    d = _mkdoc(v2, "AR", "doc-versioned", kind="container",
               versions=[{"version_id": "v001", "version_no": 1},
                         {"version_id": "v002", "version_no": 2}], current="v002")
    _mkversion(d, "v001", analysis_status="partial", latest=["03_findings.json"],
               pipeline_log_in=None, findings=[{"severity": "x"}] * 5)
    _mkversion(d, "v002", analysis_status="complete",
               latest=["01_text_analysis.json", "02_blocks_analysis.json", "03_findings.json"],
               findings=[{"severity": "x"}] * 9)
    return v2


# ---------------------------------------------------------------------------
# feature flag
# ---------------------------------------------------------------------------


def test_flag_default_legacy(monkeypatch):
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    assert A.get_storage_backend() == "legacy"
    assert A.is_v2_backend_enabled() is False


def test_flag_unknown_value_is_legacy(monkeypatch):
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "something")
    assert A.get_storage_backend() == "legacy"


def test_flag_explicit_v2(monkeypatch):
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    assert A.get_storage_backend() == "projects_v2"
    assert A.is_v2_backend_enabled() is True


def test_default_v2_root_uses_config_data_dir(monkeypatch):
    # без AUDIT_PROJECTS_V2_DIR берём config.DATA_DIR/projects_v2 (а не code-relative),
    # чтобы в production (код в -deploy, данные через AUDIT_DATA_DIR) путь был верным
    monkeypatch.delenv("AUDIT_PROJECTS_V2_DIR", raising=False)
    from backend.app.core import config
    assert A._default_v2_root() == Path(config.DATA_DIR) / "projects_v2"


def test_default_v2_root_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(tmp_path / "custom_v2"))
    assert A._default_v2_root() == (tmp_path / "custom_v2").resolve()


# ---------------------------------------------------------------------------
# navigation + metadata
# ---------------------------------------------------------------------------


def test_list_objects_disciplines_documents(tmp_path):
    a = ProjectsV2Adapter(_build(tmp_path))
    objs = a.list_objects()
    assert len(objs) == 1 and objs[0]["folder_name"] == OBJF
    assert objs[0]["object_id"] == "0b540226"
    assert set(a.list_disciplines(OBJF)) == {"AI", "OV", "ITP", "EOM", "AR"}
    docs = a.list_documents()
    assert len(docs) == 6
    codes = {d["document_code"] for d in docs}
    assert "doc-complete" in codes and "doc-versioned" in codes


def test_document_and_version_metadata(tmp_path):
    a = ProjectsV2Adapter(_build(tmp_path))
    doc = a.get_document(OBJF, "AR", "doc-versioned")
    assert doc["kind"] == "container"
    assert doc["current_version"] == "v002"
    assert doc["version_count"] == 2
    meta = a.version_metadata(Path(doc["doc_dir"]), "v002")
    assert meta["analysis_status"] == "complete"
    assert meta["is_legacy_preserve"] is False


def test_legacy_preserve_and_source_only_metadata(tmp_path):
    a = ProjectsV2Adapter(_build(tmp_path))
    doc = a.get_document(OBJF, "ITP", "doc-source")
    meta = a.version_metadata(Path(doc["doc_dir"]), "v001")
    assert meta["is_legacy_preserve"] is True
    assert meta["is_source_only"] is True
    assert meta["preserve_reason"] == "king_sons_source_only_legacy_bundle"
    leg = a.get_document(OBJF, "EOM", "doc-legacy")
    lmeta = a.version_metadata(Path(leg["doc_dir"]), "v001")
    assert lmeta["is_legacy_partial"] is True and lmeta["is_legacy_preserve"] is True


# ---------------------------------------------------------------------------
# analysis artifacts
# ---------------------------------------------------------------------------


def test_complete_artifacts_and_findings(tmp_path):
    a = ProjectsV2Adapter(_build(tmp_path))
    doc = a.get_document(OBJF, "AI", "doc-complete")
    dd = Path(doc["doc_dir"])
    art = a.latest_analysis_files(dd, "v001")
    assert art["has_01_text_analysis"] and art["has_02_blocks_analysis"] and art["has_03_findings"]
    # findings priority: 03a_norms_verified.json wins; it has no findings key -> 0
    assert a.findings_path(dd, "v001").name == "03a_norms_verified.json"
    # text/blocks readable
    assert a.read_text_analysis(dd, "v001") is not None
    assert a.read_blocks_analysis(dd, "v001") is not None
    assert a.has_pipeline_log(dd, "v001") is True


def test_findings_count_uses_03_findings_when_no_norms(tmp_path):
    a = ProjectsV2Adapter(_build(tmp_path))
    doc = a.get_document(OBJF, "AI", "doc-partial")
    dd = Path(doc["doc_dir"])
    assert a.findings_path(dd, "v001").name == "03_findings.json"
    assert a.findings_count(dd, "v001") == 1
    assert a.findings_by_severity(dd, "v001") == {"Критическое": 1}


def test_partial_none_source_only(tmp_path):
    a = ProjectsV2Adapter(_build(tmp_path))
    # none
    dn = Path(a.get_document(OBJF, "OV", "doc-none")["doc_dir"])
    assert a.analysis_status(dn, "v001") == "none"
    assert a.latest_analysis_files(dn, "v001")["present"] == []
    assert a.findings_count(dn, "v001") == 0
    assert a.has_pipeline_log(dn, "v001") is False
    # source_only inputs preserved, no analysis, no fake findings
    ds = Path(a.get_document(OBJF, "ITP", "doc-source")["doc_dir"])
    assert a.findings_count(ds, "v001") == 0
    assert len(a.input_files(ds, "v001")) == 3
    # legacy_partial pipeline_log lives in latest
    dl = Path(a.get_document(OBJF, "EOM", "doc-legacy")["doc_dir"])
    assert a.has_pipeline_log(dl, "v001") is True
    assert a.findings_count(dl, "v001") == 3


def test_versioned_per_version_findings(tmp_path):
    a = ProjectsV2Adapter(_build(tmp_path))
    dd = Path(a.get_document(OBJF, "AR", "doc-versioned")["doc_dir"])
    assert a.findings_count(dd, "v001") == 5
    assert a.findings_count(dd, "v002") == 9
    assert a.current_version_id(dd) == "v002"


def test_document_snapshot(tmp_path):
    a = ProjectsV2Adapter(_build(tmp_path))
    snap = a.document_snapshot(OBJF, "EOM", "doc-legacy")
    assert snap["migration_kind"] == "legacy_findings_preserve"
    v = snap["versions"][0]
    assert v["analysis_status"] == "legacy_partial" and v["findings_count"] == 3


def test_find_document(tmp_path):
    a = ProjectsV2Adapter(_build(tmp_path))
    assert a.find_document("doc-complete")["discipline"] == "AI"
    assert a.find_document("nope") is None


# ---------------------------------------------------------------------------
# read-only invariant
# ---------------------------------------------------------------------------


def test_adapter_writes_nothing(tmp_path):
    v2 = _build(tmp_path)
    before_files = {p: (p.stat().st_mtime_ns, p.read_bytes())
                    for p in v2.rglob("*") if p.is_file()}
    a = ProjectsV2Adapter(v2)
    # exercise the full read surface
    for d in a.list_documents():
        dd = Path(d["doc_dir"])
        for v in a.list_versions(dd):
            vid = v["version_id"]
            a.version_metadata(dd, vid)
            a.input_files(dd, vid)
            a.latest_analysis_files(dd, vid)
            a.read_text_analysis(dd, vid)
            a.read_blocks_analysis(dd, vid)
            a.read_findings(dd, vid)
            a.findings_count(dd, vid)
            a.findings_by_severity(dd, vid)
            a.has_pipeline_log(dd, vid)
            a.read_pipeline_log(dd, vid)
        a.document_snapshot(d["object_folder"], d["discipline"], d["document_code"])
    after_files = {p: (p.stat().st_mtime_ns, p.read_bytes())
                   for p in v2.rglob("*") if p.is_file()}
    assert before_files == after_files, "adapter must not modify/create files"
    assert set(before_files) == set(after_files), "adapter must not create/delete files"
