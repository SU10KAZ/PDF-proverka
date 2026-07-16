"""
Тесты разбора blocked/manual King&Sons legacy проектов
(POLICY_READY_LEGACY_FINDINGS_PRESERVE). Read-only, гермётичны.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import analyze_blocked_manual_projects as abm  # noqa: E402


# ---------------------------------------------------------------------------
# classify
# ---------------------------------------------------------------------------


def test_classify_king_sons_preserve():
    assert abm.classify_blocked_manual('213. Мосфильмовская 31А "King&Sons"') == \
        abm.POLICY_READY_LEGACY_FINDINGS_PRESERVE


def test_classify_non_king_sons_stays_manual():
    assert abm.classify_blocked_manual("214. Alia (ASTERUS)") == abm.POLICY_STILL_MANUAL


def test_proposed_version_json_fields():
    vj = abm.proposed_version_json()
    assert vj["analysis_status"] == "legacy_partial"
    assert vj["analysis_generation"] == "legacy"
    assert vj["preserve_reason"] == "king_sons_legacy_findings_preserve"
    assert vj["source_files_strategy"] == "legacy_bundle"
    assert vj["primary_goal"] == "preserve_findings_and_kb_links"


# ---------------------------------------------------------------------------
# kb lookup
# ---------------------------------------------------------------------------


def test_kb_entries_for():
    decisions = [
        {"source_project": "133_23-ГК-АК", "item_id": "F-1", "summary": "x", "expert_decision": "accept"},
        {"source_project": "133_23-ГК-АК", "item_id": "F-2", "summary": "y", "expert_decision": "reject"},
        {"source_project": "OTHER", "item_id": "F-9", "summary": "z"},
    ]
    items = abm.kb_entries_for(decisions, "133_23-ГК-АК")
    assert len(items) == 2
    assert {i["item_id"] for i in items} == {"F-1", "F-2"}


# ---------------------------------------------------------------------------
# gather_legacy_inventory
# ---------------------------------------------------------------------------


def _mk(p: Path, text="x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def test_inventory_collects_sources_and_ambiguous(tmp_path):
    proj = tmp_path / "133_23-ГК-АК(main)"
    # две версии с дублирующими ролями (multiple)
    for v in ("133_23-ГК-АК", "133_23-ГК-АК V2.pdf"):
        _mk(proj / v / f"{v}.pdf")
        _mk(proj / v / f"{v}_document.md")
        _mk(proj / v / f"{v}_ocr.html")
        _mk(proj / v / f"{v}_result.json")
    # анализ в _output
    _mk(proj / "133_23-ГК-АК" / "_output" / "03_findings.json")
    _mk(proj / "133_23-ГК-АК" / "_output" / "02_text_analysis.json")
    # backup-каталог НЕ должен попадать
    _mk(proj / "_bench_backup_1" / "junk.pdf")
    # неклассифицируемый файл
    _mk(proj / "133_23-ГК-АК" / "weird.dat")

    inv = abm.gather_legacy_inventory(proj)
    assert len(inv["by_role"]["pdf"]) == 2          # multiple pdf
    assert len(inv["by_role"]["document_md"]) == 2
    assert inv["analysis_present"]["03_findings.json"] is True
    assert inv["analysis_present"]["02_text_analysis.json"] is True
    assert inv["analysis_present"]["01_blocks_analysis.json"] is False
    assert any("_output" in d for d in inv["output_dirs"])
    # backup junk не попал в bundle
    assert not any("_bench_backup" in f for f in inv["source_files"])
    # weird.dat — unclassified, но сохранён в bundle
    assert any(f.endswith("weird.dat") for f in inv["unclassified_files"])
    assert any(f.endswith("weird.dat") for f in inv["source_files"])


def test_inventory_incomplete_single_pdf(tmp_path):
    proj = tmp_path / "133_23-ГК-ИТП.ТМ"
    _mk(proj / "133_23-ГК-ИТП.ТМ.pdf")
    _mk(proj / "_output" / "extracted.txt")  # _output без анализа
    inv = abm.gather_legacy_inventory(proj)
    assert len(inv["by_role"]["pdf"]) == 1
    assert inv["analysis_present"]["03_findings.json"] is False
    assert inv["output_dirs"]  # _output сохранится в legacy_output


# ---------------------------------------------------------------------------
# analyze() read-only
# ---------------------------------------------------------------------------


def test_analyze_read_only(tmp_path):
    legacy = tmp_path / "projects"
    proj = legacy / '213. Мосфильмовская 31А "King&Sons"' / "SS" / "133_23-ГК-АК(main)"
    _mk(proj / "133_23-ГК-АК" / "133_23-ГК-АК.pdf")
    _mk(proj / "133_23-ГК-АК" / "_output" / "03_findings.json")

    v2 = tmp_path / "projects_v2"
    (v2 / "_system").mkdir(parents=True)
    (tmp_path / "knowledge_base").mkdir()
    (tmp_path / "knowledge_base" / "decisions_log.json").write_text(json.dumps({
        "entries": [{"source_project": "133_23-ГК-АК", "item_id": "F-1",
                     "summary": "leak", "expert_decision": "accept"}]
    }), encoding="utf-8")
    (v2 / "_system" / "migration_warning_policy_report.json").write_text(json.dumps({
        "projects": [{
            "policy_group": "WARNINGS_BLOCKED", "object": '213. Мосфильмовская 31А "King&Sons"',
            "discipline": "SS", "document_code": "133_23-ГК-АК", "kind": "container",
            "legacy_path": str(proj), "blockers": ["multiple_pdf"],
        }]
    }), encoding="utf-8")

    before = {str(p.relative_to(legacy)): p.stat().st_mtime
              for p in legacy.rglob("*") if p.is_file()}
    res = abm.analyze(v2)
    assert len(res["rows"]) == 1
    r = res["rows"][0]
    assert r["proposed_policy"] == abm.POLICY_READY_LEGACY_FINDINGS_PRESERVE
    assert r["has_03_findings"] is True
    assert r["kb_linked"] is True and r["kb_entries"] == 1
    assert r["migrate_now"] is False
    assert r["proposed_version_json"]["preserve_reason"] == "king_sons_legacy_findings_preserve"
    after = {str(p.relative_to(legacy)): p.stat().st_mtime
             for p in legacy.rglob("*") if p.is_file()}
    assert before == after  # legacy не изменён
