"""
Тесты генератора финальной приёмки миграции projects_v2. Гермётичны (tmp_path),
read-only, без subprocess (do_validate=False).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import generate_final_acceptance_report as fa  # noqa: E402


def _doc(obj_root: Path, folder: str, disc: str, code: str, *, kind="plain",
         versions=None, statuses=None, migration_kind=None, findings=False,
         kb_entries=0):
    """Создаёт document.json + version.json(ы)."""
    versions = versions or ["v001"]
    statuses = statuses or {}
    doc = obj_root / folder / "disciplines" / disc / "documents" / code
    doc.mkdir(parents=True, exist_ok=True)
    dj = {"schema_version": 1, "document_code": code, "discipline": disc,
          "kind": kind, "versions": [{"version_id": v, "version_no": i + 1}
                                     for i, v in enumerate(versions)]}
    if migration_kind:
        dj["migration_kind"] = migration_kind
    (doc / "document.json").write_text(json.dumps(dj, ensure_ascii=False), encoding="utf-8")
    for v in versions:
        vroot = doc / "versions" / v
        vroot.mkdir(parents=True, exist_ok=True)
        vj = {"schema_version": 1, "version_id": v}
        st = statuses.get(v, "(omit)")
        if st != "(omit)":
            vj["analysis_status"] = st
        (vroot / "version.json").write_text(json.dumps(vj, ensure_ascii=False), encoding="utf-8")
        if findings:
            latest = vroot / "03_analysis" / "latest"
            latest.mkdir(parents=True, exist_ok=True)
            (latest / "03_findings.json").write_text("{}", encoding="utf-8")
        if kb_entries:
            rev = vroot / "04_review"
            rev.mkdir(parents=True, exist_ok=True)
            (rev / "kb_decisions_link.json").write_text(
                json.dumps({"entry_count": kb_entries, "entries": []}), encoding="utf-8")
    return doc


def _system(v2: Path):
    sd = v2 / "_system"
    sd.mkdir(parents=True, exist_ok=True)
    migs = []
    # отражаем 184 уникальных документа? для теста — небольшое число.
    return sd


def _build_v2(tmp_path) -> Path:
    v2 = tmp_path / "projects_v2"
    obj = v2 / "objects"
    KS = fa.KING_SONS_OBJECT_FOLDER
    # 4 King&Sons preserve
    _doc(obj, KS, "EOM", "133_23-ГК-ЭМ2", kind="container",
         statuses={"v001": "legacy_partial"}, migration_kind="legacy_findings_preserve",
         findings=True)
    _doc(obj, KS, "SS", "133_23-ГК-АК", kind="container",
         statuses={"v001": "legacy_partial"}, migration_kind="legacy_findings_preserve",
         findings=True, kb_entries=4)
    _doc(obj, KS, "EOM", "Фасадное освещение", kind="plain",
         statuses={"v001": "source_only"}, migration_kind="legacy_findings_preserve")
    _doc(obj, KS, "ITP", "133_23-ГК-ИТП.ТМ", kind="plain",
         statuses={"v001": "source_only"}, migration_kind="legacy_findings_preserve")
    # один обычный многоверсионный + один с пропущенным статусом (legacy schema)
    _doc(obj, "214_Alia_ASTERUS", "EOM", "doc-a", kind="container",
         versions=["v001", "v002"], statuses={"v001": "complete", "v002": "partial"})
    _doc(obj, "214_Alia_ASTERUS", "OV", "doc-b", kind="plain",
         statuses={"v001": "(omit)"})  # старая схема без analysis_status

    sd = _system(v2)
    # old_to_new_map: одна запись на версию (7 версий по 6 документам)
    migs = []
    for dj in sorted(obj.rglob("document.json")):
        d = json.loads(dj.read_text())
        for v in d["versions"]:
            migs.append({"object_id": dj.parents[3].name, "document_code": d["document_code"],
                         "version_id": v["version_id"]})
    (sd / "old_to_new_map.json").write_text(json.dumps({"migrations": migs}), encoding="utf-8")
    (sd / "migration_readiness_report.json").write_text(json.dumps({"summary": {
        "total_projects": 6, "group_counts": {
            "ALREADY_MIGRATED": 6, "AUTO_SAFE": 0, "CAN_MIGRATE_WITH_WARNINGS": 0,
            "MANUAL_REVIEW_REQUIRED": 0, "SKIP_EMPTY_OR_INVALID": 0},
        "already_migrated_count": 6, "pdf_named_version_folders": 1, "no_analysis": 1,
        "multiple_pdf_md_json": 0, "incomplete_input_quad": 0}}), encoding="utf-8")
    (sd / "migration_warning_policy_report.json").write_text(json.dumps({"summary": {
        "total_legacy_projects": 6, "policy_counts": {}}}), encoding="utf-8")
    (sd / "migrated_drift_scan_report.json").write_text(json.dumps({"summary": {
        "drift_documents": 0, "stable": 0, "unstable": 0}}), encoding="utf-8")
    return v2


def test_gather_counts_and_statuses(tmp_path):
    v2 = _build_v2(tmp_path)
    rep = fa.gather(v2, tmp_path / "projects", do_validate=False)
    t = rep["totals"]
    assert t["documents_in_v2"] == 6
    assert t["migrated_documents"] == 6
    assert t["version_level_migration_records"] == 7  # doc-a имеет 2 версии
    assert t["versions_total"] == 7
    assert t["plain_documents"] == 3
    assert t["container_documents"] == 3
    assert t["legacy_findings_preserve_documents"] == 4
    assert t["source_only_versions"] == 2
    dist = rep["analysis_status_distribution"]
    assert dist["legacy_partial"] == 2
    assert dist["source_only"] == 2
    assert dist["complete"] == 1
    assert dist["partial"] == 1
    assert dist["(no_status_field_legacy_schema)"] == 1


def test_no_obj_star_and_readable_objects(tmp_path):
    v2 = _build_v2(tmp_path)
    rep = fa.gather(v2, tmp_path / "projects", do_validate=False)
    assert rep["objects"]["obj_star_folders"] == []
    assert fa.KING_SONS_OBJECT_FOLDER in rep["objects"]["readable_folders"]
    assert "214_Alia_ASTERUS" in rep["objects"]["readable_folders"]


def test_king_sons_checks_pass(tmp_path):
    v2 = _build_v2(tmp_path)
    rep = fa.gather(v2, tmp_path / "projects", do_validate=False)
    ks = {c["document"]: c for c in rep["king_sons_legacy_preserve"]}
    assert ks["EOM/133_23-ГК-ЭМ2"]["findings_present"] is True
    assert ks["EOM/133_23-ГК-ЭМ2"]["ok"] is True
    ak = ks["SS/133_23-ГК-АК"]
    assert ak["findings_present"] is True and ak["kb_linked"] is True and ak["kb_entries"] == 4
    assert ak["ok"] is True
    assert ks["EOM/Фасадное освещение"]["analysis_status"] == "source_only"
    assert ks["EOM/Фасадное освещение"]["findings_present"] is False
    assert ks["EOM/Фасадное освещение"]["ok"] is True
    assert ks["ITP/133_23-ГК-ИТП.ТМ"]["ok"] is True


def test_acceptance_ok_requires_184_in_prod_but_flags_in_fixture(tmp_path):
    """Фикстура имеет 6 документов → acceptance_ok=False (порог 184), но все
    под-проверки (validate/drift/obj_star/king_sons) проходят."""
    v2 = _build_v2(tmp_path)
    rep = fa.gather(v2, tmp_path / "projects", do_validate=False)
    assert rep["drift"]["result"] == "PASS"
    assert rep["readiness_group_counts"]["MANUAL_REVIEW_REQUIRED"] == 0
    assert all(c["ok"] for c in rep["king_sons_legacy_preserve"])
    # 184-порог не выполнен в фикстуре
    assert rep["acceptance_ok"] is False


def test_render_md_smoke(tmp_path):
    v2 = _build_v2(tmp_path)
    rep = fa.gather(v2, tmp_path / "projects", do_validate=False)
    md = fa.render_md(rep)
    assert "Финальная приёмка миграции projects_v2" in md
    assert "King&Sons legacy preserve" in md
    assert "Остаточные риски" in md


def test_risks_include_legacy_schema_and_backend(tmp_path):
    v2 = _build_v2(tmp_path)
    rep = fa.gather(v2, tmp_path / "projects", do_validate=False)
    joined = " ".join(rep["remaining_risks"])
    assert "analysis_status" in joined  # legacy schema risk
    assert "backend" in joined.lower()
