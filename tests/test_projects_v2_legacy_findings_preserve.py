"""
Тесты миграции King&Sons blocked/manual проектов как legacy snapshot
(POLICY_READY_LEGACY_FINDINGS_PRESERVE). Гермётичны (tmp_path), legacy read-only.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import migrate_legacy_findings_preserve as mig  # noqa: E402
import v2lib  # noqa: E402
import validate_migration as vm  # noqa: E402

KING = '213. Мосфильмовская 31А "King&Sons"'


def _mk(p: Path, text="x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def _container_project(root: Path) -> Path:
    """Контейнер с двумя версиями (multiple pdf/md/result) + анализ в _output V1."""
    proj = root / "projects" / KING / "EOM" / "133_23-ГК-ЭМ2(main)"
    _mk(proj / "version_group.json", json.dumps({"logical_project_id": "133_23-ГК-ЭМ2"}))
    # V1 с двумя частями + project_info + _output с анализом
    v1 = proj / "133_23-ГК-ЭМ2"
    for part in ("Часть 1", "Часть 2"):
        _mk(v1 / f"{part}.pdf")
        _mk(v1 / f"{part}_document.md")
        _mk(v1 / f"{part}_ocr.html")
        _mk(v1 / f"{part}_result.json")
    _mk(v1 / "project_info.json")
    _mk(v1 / "_output" / "03_findings.json", json.dumps({"findings": [{"id": "F-1"}]}))
    _mk(v1 / "_output" / "01_text_analysis.json")
    _mk(v1 / "_output" / "02_blocks_analysis.json")
    _mk(v1 / "_output" / "pipeline_log.json")
    _mk(v1 / "_output" / "blocks" / "block_001.png", "PNG")
    _mk(v1 / "_output" / "_bench_backup_123" / "old.json")  # бэкап внутри _output
    # V2 (папка с .pdf на конце) — свой комплект, пустой _output
    v2 = proj / "133_23-ГК-ЭМ2 V2.pdf"
    _mk(v2 / "133_23-ГК-ЭМ2 V2.pdf")
    _mk(v2 / "133_23-ГК-ЭМ2 V2_document.md")
    _mk(v2 / "133_23-ГК-ЭМ2 V2_result.json")
    (v2 / "_output").mkdir(parents=True, exist_ok=True)
    return proj


def _source_only_project(root: Path) -> Path:
    """Plain-проект только с PDF (без анализа)."""
    proj = root / "projects" / KING / "ITP" / "133_23-ГК-ИТП.ТМ"
    _mk(proj / "133_23-ГК-ИТП.ТМ (1) (3).pdf")
    _mk(proj / "client.log", "log")
    _mk(proj / "project_info.json")
    (proj / "_output").mkdir(parents=True, exist_ok=True)  # пустой _output
    return proj


def _kb(root: Path, entries):
    _mk(root / "knowledge_base" / "decisions_log.json",
        json.dumps({"entries": entries}, ensure_ascii=False))


def _run(proj: Path, root: Path, decisions=None):
    v2 = root / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    return mig.migrate_one(proj, v2, objects_map={"by_name": {}, "by_path": {}, "by_id": {}},
                           decisions=decisions or [], execute=True), v2


# ---------------------------------------------------------------------------


def test_refuses_non_king_sons(tmp_path):
    proj = tmp_path / "projects" / "214. Alia (ASTERUS)" / "EOM" / "doc(main)"
    _mk(proj / "version_group.json", json.dumps({"logical_project_id": "doc"}))
    _mk(proj / "doc" / "a.pdf")
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    import pytest
    with pytest.raises(SystemExit):
        mig.migrate_one(proj, v2, objects_map={"by_name": {}, "by_path": {}, "by_id": {}},
                        decisions=[], execute=True)


def test_legacy_bundle_keeps_multiple_sources(tmp_path):
    proj = _container_project(tmp_path)
    res, v2 = _run(proj, tmp_path)
    bundle = Path(res["legacy_bundle_dir"])
    pdfs = [p for p in bundle.rglob("*.pdf") if p.is_file()]
    mds = [p for p in bundle.rglob("*_document.md") if p.is_file()]
    results = [p for p in bundle.rglob("*_result.json") if p.is_file()]
    assert len(pdfs) == 3        # 2 части V1 + 1 V2
    assert len(mds) == 3
    assert len(results) == 3
    # структура версий сохранена внутри bundle
    assert (bundle / "133_23-ГК-ЭМ2" / "Часть 1.pdf").exists()
    assert (bundle / "133_23-ГК-ЭМ2 V2.pdf" / "133_23-ГК-ЭМ2 V2.pdf").exists()


def test_03_findings_goes_to_latest(tmp_path):
    proj = _container_project(tmp_path)
    res, v2 = _run(proj, tmp_path)
    vroot = Path(res["v2_document_dir"]) / "versions" / "v001"
    assert (vroot / "03_analysis" / "latest" / "03_findings.json").exists()
    assert (vroot / "03_analysis" / "latest" / "01_text_analysis.json").exists()
    assert (vroot / "03_analysis" / "latest" / "02_blocks_analysis.json").exists()
    assert res["analysis_status"] == "legacy_partial"
    # содержимое findings не потеряно
    data = json.loads((vroot / "03_analysis" / "latest" / "03_findings.json").read_text())
    assert data["findings"][0]["id"] == "F-1"


def test_full_output_preserved_in_legacy_output(tmp_path):
    proj = _container_project(tmp_path)
    res, v2 = _run(proj, tmp_path)
    lo = Path(res["legacy_output_dir"])
    # полная копия _output, включая png-блоки и бэкап внутри _output
    assert (lo / "133_23-ГК-ЭМ2" / "_output" / "03_findings.json").exists()
    assert (lo / "133_23-ГК-ЭМ2" / "_output" / "blocks" / "block_001.png").exists()
    assert (lo / "133_23-ГК-ЭМ2" / "_output" / "_bench_backup_123" / "old.json").exists()


def test_kb_link_saved_as_metadata_without_touching_kb(tmp_path):
    proj = _container_project(tmp_path)
    entries = [
        {"source_project": "133_23-ГК-ЭМ2", "item_id": "F-1", "summary": "leak", "expert_decision": "accept"},
        {"source_project": "133_23-ГК-ЭМ2", "item_id": "F-2", "summary": "gap", "expert_decision": "reject"},
        {"source_project": "OTHER", "item_id": "F-9"},
    ]
    _kb(tmp_path, entries)
    kb_path = tmp_path / "knowledge_base" / "decisions_log.json"
    before = kb_path.read_text(encoding="utf-8")
    res, v2 = _run(proj, tmp_path, decisions=entries)
    vroot = Path(res["v2_document_dir"]) / "versions" / "v001"
    link = vroot / "04_review" / "kb_decisions_link.json"
    assert link.exists()
    payload = json.loads(link.read_text())
    assert payload["entry_count"] == 2
    assert {e["item_id"] for e in payload["entries"]} == {"F-1", "F-2"}
    assert res["kb_linked"] is True and res["kb_entries"] == 2
    # база знаний не изменилась
    assert kb_path.read_text(encoding="utf-8") == before


def test_source_only_no_fake_files(tmp_path):
    proj = _source_only_project(tmp_path)
    res, v2 = _run(proj, tmp_path)
    vroot = Path(res["v2_document_dir"]) / "versions" / "v001"
    assert res["analysis_status"] == "source_only"
    assert res["version_json"]["preserve_reason"] == "king_sons_source_only_legacy_bundle"
    assert "primary_goal" not in res["version_json"]
    # никаких фейковых артефактов
    assert not (vroot / "03_analysis" / "latest" / "03_findings.json").exists()
    assert not list((vroot / "03_analysis" / "latest").glob("*"))
    # bundle хранит PDF + служебные, ocr/md не выдуманы
    bundle = vroot / "01_input" / "legacy_bundle"
    assert list(bundle.rglob("*.pdf"))
    assert not list(bundle.rglob("*_ocr.html"))


def test_old_to_new_map_updated_and_validate_pass(tmp_path):
    proj = _container_project(tmp_path)
    src = _source_only_project(tmp_path)
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    map_path = v2 / "_system" / "old_to_new_map.json"
    map_obj = v2lib.load_old_to_new_map(map_path)
    om = {"by_name": {}, "by_path": {}, "by_id": {}}
    for p in (proj, src):
        res = mig.migrate_one(p, v2, objects_map=om, decisions=[], execute=True)
        v2lib.upsert_migration(map_obj, res["map_record"])
        assert res["checksum_errors"] == []
        assert res["checksum_checked"] > 0
    v2lib.save_old_to_new_map(map_obj, map_path)

    reloaded = v2lib.load_old_to_new_map(map_path)
    assert len(reloaded["migrations"]) == 2
    for m in reloaded["migrations"]:
        assert m["migration_kind"] == "legacy_findings_preserve"
        assert m["version_id"] == "v001"

    # validate проходит для legacy snapshot (в т.ч. source-only без quad)
    errors, notes = vm.validate_map(reloaded)
    assert errors == [], errors


def test_legacy_projects_untouched(tmp_path):
    proj = _container_project(tmp_path)
    legacy = tmp_path / "projects"
    before = {str(p.relative_to(legacy)): (p.stat().st_mtime, p.stat().st_size)
              for p in legacy.rglob("*") if p.is_file()}
    _run(proj, tmp_path)
    after = {str(p.relative_to(legacy)): (p.stat().st_mtime, p.stat().st_size)
             for p in legacy.rglob("*") if p.is_file()}
    assert before == after


def test_dry_run_copies_nothing(tmp_path):
    proj = _container_project(tmp_path)
    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    res = mig.migrate_one(proj, v2, objects_map={"by_name": {}, "by_path": {}, "by_id": {}},
                          decisions=[], execute=False)
    assert res["executed"] is False
    # никаких version-папок не создано
    assert not (Path(res["v2_document_dir"]) / "versions").exists()
