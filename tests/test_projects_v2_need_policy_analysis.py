"""
Тесты разбора WARNINGS_NEED_POLICY (read-only policy analysis). Гермётичны.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import analyze_need_policy_projects as anp  # noqa: E402


def _sig(**over) -> dict:
    base = dict(
        has_pdf=True, has_document_md=True, has_result_json=True, has_ocr_html=True,
        has_project_info=True, has_01=True, has_02=True, has_03=True, has_pipeline_log=True,
        pdf_named=False, has_version_group=False,
        multiple_pdf=False, multiple_document_md=False, multiple_result_json=False,
        document_code_conflict=False, sibling_count=1, sibling_unambiguous=False,
        legacy_generation=False, kb_linked=False,
    )
    base.update(over)
    return base


# ---------------------------------------------------------------------------
# classify_need_policy
# ---------------------------------------------------------------------------


def test_missing_ocr_html():
    v = anp.classify_need_policy(_sig(has_ocr_html=False))
    assert v["subgroup"] == anp.POLICY_READY_MISSING_OCR_HTML
    assert v["can_migrate_auto"] is True
    assert v["proposed_next_class"] == "WARNINGS_AUTO_CANDIDATE"


def test_no_analysis():
    v = anp.classify_need_policy(_sig(has_01=False, has_02=False, has_03=False))
    assert v["subgroup"] == anp.POLICY_READY_NO_ANALYSIS
    assert v["proposed_analysis_status"] == "none"


def test_partial_analysis():
    v = anp.classify_need_policy(_sig(has_01=True, has_02=False, has_03=False))
    assert v["subgroup"] == anp.POLICY_READY_PARTIAL_ANALYSIS
    assert v["proposed_analysis_status"] == "partial"


def test_single_pdf_named_folder():
    v = anp.classify_need_policy(_sig(pdf_named=True, has_version_group=False, sibling_count=1))
    assert v["subgroup"] == anp.POLICY_READY_SINGLE_PDF_NAMED_FOLDER
    assert v["can_migrate_auto"] is True


def test_grouped_versions_without_main():
    v = anp.classify_need_policy(_sig(pdf_named=True, has_version_group=False,
                                      sibling_count=3, sibling_unambiguous=True))
    assert v["subgroup"] == anp.POLICY_READY_GROUPED_VERSIONS_WITHOUT_MAIN


def test_manual_version_grouping_ambiguous():
    v = anp.classify_need_policy(_sig(pdf_named=True, has_version_group=False,
                                      sibling_count=2, sibling_unambiguous=False))
    assert v["subgroup"] == anp.POLICY_NEEDS_MANUAL_VERSION_GROUPING
    assert v["can_migrate_auto"] is False


def test_recheck_as_blocked_multiple_pdf():
    v = anp.classify_need_policy(_sig(multiple_pdf=True))
    assert v["subgroup"] == anp.POLICY_RECHECK_AS_BLOCKED
    assert v["proposed_next_class"] == "WARNINGS_BLOCKED"


def test_recheck_as_blocked_incomplete_quad():
    v = anp.classify_need_policy(_sig(has_result_json=False))
    assert v["subgroup"] == anp.POLICY_RECHECK_AS_BLOCKED


def test_legacy_kb_preserve():
    # King&Sons legacy, KB-связь, анализ неполный
    v = anp.classify_need_policy(_sig(legacy_generation=True, kb_linked=True,
                                      has_01=False, has_02=False, has_03=False,
                                      pdf_named=True))  # даже pdf_named -> preserve приоритетнее
    assert v["subgroup"] == anp.POLICY_READY_LEGACY_KB_PRESERVE
    assert v["proposed_analysis_status"] == "legacy_partial"
    assert v["legacy_kb_preserve"] is True


def test_legacy_kb_preserve_not_when_complete():
    # анализ полный -> не legacy_partial, обычный путь (тут pdf_named -> single)
    v = anp.classify_need_policy(_sig(legacy_generation=True, kb_linked=True,
                                      has_01=True, has_02=True, has_03=True, pdf_named=True))
    assert v["subgroup"] != anp.POLICY_READY_LEGACY_KB_PRESERVE


def test_blocker_priority_over_kb_preserve():
    v = anp.classify_need_policy(_sig(legacy_generation=True, kb_linked=True,
                                      has_01=False, has_02=False, has_03=False,
                                      multiple_pdf=True))
    assert v["subgroup"] == anp.POLICY_RECHECK_AS_BLOCKED


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def test_logical_base():
    assert anp.logical_base("X V1.pdf") == ("X", 1)
    assert anp.logical_base("13АВ-РД-АР3-К7 V2.pdf") == ("13АВ-РД-АР3-К7", 2)
    assert anp.logical_base("133_23-ГК-СОТ V1") == ("133_23-ГК-СОТ", 1)
    assert anp.logical_base("plain.pdf") == ("plain", None)


def test_detect_version_siblings(tmp_path):
    disc = tmp_path / "AR"
    disc.mkdir()
    for n in ("DOC V1.pdf", "DOC V2.pdf", "DOC V3.pdf"):
        (disc / n).mkdir()
    cnt, unamb, base = anp.detect_version_siblings(disc / "DOC V1.pdf")
    assert cnt == 3 and unamb is True and base == "DOC"
    # одиночная
    solo = tmp_path / "SS"; solo.mkdir(); (solo / "ONE V1.pdf").mkdir()
    cnt2, unamb2, _ = anp.detect_version_siblings(solo / "ONE V1.pdf")
    assert cnt2 == 1 and unamb2 is False


def test_load_kb_source_projects(tmp_path):
    (tmp_path / "knowledge_base").mkdir()
    (tmp_path / "knowledge_base" / "decisions_log.json").write_text(json.dumps({
        "entries": [{"source_project": "133_23-ГК-СОТ"}, {"source_project": "X"}]
    }), encoding="utf-8")
    s = anp.load_kb_source_projects(tmp_path)
    assert "133_23-ГК-СОТ" in s and "X" in s


# ---------------------------------------------------------------------------
# analyze() read-only + legacy не меняется
# ---------------------------------------------------------------------------


def _build_reports(tmp_path):
    legacy = tmp_path / "projects"
    proj = legacy / "OBJ" / "AR" / "DOC V1.pdf"
    (proj / "_output").mkdir(parents=True)
    (proj / "DOC V1.pdf").mkdir()  # noise dir, ignored
    v2 = tmp_path / "projects_v2"
    (v2 / "_system").mkdir(parents=True)
    rd = {"projects": [{
        "object_id": "o1", "object": "214. Alia (ASTERUS)", "discipline": "AR",
        "document_code": "DOC V1", "kind": "plain", "version_count": 1,
        "legacy_path": str(proj), "has_pdf": True, "has_document_md": True,
        "has_ocr_html": True, "has_result_json": True, "has_project_info": True,
        "has_output": True, "has_01_text_analysis": False, "has_02_blocks_analysis": False,
        "has_03_findings": False, "has_pipeline_log": False,
        "pdf_named_version_folder": True, "has_version_group": False,
        "multiple_pdf": False, "multiple_document_md": False, "multiple_result_json": False,
        "document_code_conflict": False,
    }]}
    wp = {"projects": [{
        "policy_group": "WARNINGS_NEED_POLICY", "recommendation": "needs_policy",
        "object_id": "o1", "object": "214. Alia (ASTERUS)", "discipline": "AR",
        "document_code": "DOC V1", "kind": "plain",
        "warning_tags": ["pdf_in_version_folder_name", "no_analysis"], "blockers": [],
    }]}
    (v2 / "_system" / "migration_readiness_report.json").write_text(json.dumps(rd), encoding="utf-8")
    (v2 / "_system" / "migration_warning_policy_report.json").write_text(json.dumps(wp), encoding="utf-8")
    return legacy, v2


def test_analyze_read_only(tmp_path):
    legacy, v2 = _build_reports(tmp_path)
    before = {str(p.relative_to(legacy)): p.stat().st_mtime
              for p in legacy.rglob("*") if p.is_file()}
    res = anp.analyze(legacy, v2)
    assert len(res["rows"]) == 1
    assert res["rows"][0]["subgroup"] == anp.POLICY_READY_SINGLE_PDF_NAMED_FOLDER
    after = {str(p.relative_to(legacy)): p.stat().st_mtime
             for p in legacy.rglob("*") if p.is_file()}
    assert before == after  # legacy не изменён
