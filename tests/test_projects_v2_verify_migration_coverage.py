"""Тесты read-only верификатора покрытия legacy → projects_v2 (Шаг 4B/10).

Все фикстуры — во временных tmp-каталогах; реальный projects_v2 не трогается.
"""
from __future__ import annotations

import importlib.util
import hashlib
import json
from pathlib import Path

import pytest

# Импорт скрипта по пути (он в scripts/, не пакет).
_SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts" / "projects_v2" / "verify_migration_coverage.py"
)
_spec = importlib.util.spec_from_file_location("verify_migration_coverage", _SCRIPT)
vmc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vmc)


# ── helpers для построения фикстур ──────────────────────────────────────────

def _make_legacy_project(legacy_root: Path, rel: str, *, doc_code: str,
                         section: str, findings_bytes: int = 0) -> Path:
    d = legacy_root / rel
    d.mkdir(parents=True, exist_ok=True)
    (d / "project_info.json").write_text(
        json.dumps({"name": doc_code, "document_code": doc_code,
                    "section": section, "project_id": doc_code}),
        encoding="utf-8",
    )
    if findings_bytes > 0:
        out = d / "_output"
        out.mkdir(exist_ok=True)
        (out / "03_findings.json").write_text("x" * findings_bytes, encoding="utf-8")
    return d


def _make_v2_doc(v2_root: Path, obj_folder: str, disc: str, doc_code: str,
                 *, findings_bytes: int = 0, version: str = "v001") -> Path:
    doc = v2_root / "objects" / obj_folder / "disciplines" / disc / "documents" / doc_code
    doc.mkdir(parents=True, exist_ok=True)
    (doc / "document.json").write_text(json.dumps({"document_code": doc_code}), encoding="utf-8")
    if findings_bytes > 0:
        latest = doc / "versions" / version / "03_analysis" / "latest"
        latest.mkdir(parents=True, exist_ok=True)
        (latest / "03_findings.json").write_text("y" * findings_bytes, encoding="utf-8")
    return doc


def _write_map(v2_root: Path, entries: list[dict]) -> Path:
    sysdir = v2_root / "_system"
    sysdir.mkdir(parents=True, exist_ok=True)
    mf = sysdir / "old_to_new_map.json"
    mf.write_text(json.dumps({"schema_version": 1, "migrations": entries}), encoding="utf-8")
    return mf


@pytest.fixture
def roots(tmp_path):
    legacy = tmp_path / "projects"
    v2 = tmp_path / "projects_v2"
    legacy.mkdir()
    (v2 / "objects").mkdir(parents=True)
    return legacy, v2


# ── 1. mapped ───────────────────────────────────────────────────────────────

def test_mapped_project(roots):
    legacy, v2 = roots
    d = _make_legacy_project(legacy, "obj/AR/DOC-1", doc_code="DOC-1", section="AR")
    _make_v2_doc(v2, "obj_slug", "AR", "DOC-1")
    mf = _write_map(v2, [{"document_code": "DOC-1", "legacy_folder_path": str(d)}])
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert rep["counts"]["mapped"] == 1
    assert rep["counts"]["missing_v2_real_backlog"] == 0
    assert rep["real_backlog"] is False


# ── 2. dual-write present, not in ledger ────────────────────────────────────

def test_dual_write_present_not_in_ledger(roots):
    legacy, v2 = roots
    _make_legacy_project(legacy, "obj/KM/DOC-2", doc_code="DOC-2", section="KM")
    _make_v2_doc(v2, "obj_slug", "KM", "DOC-2")
    mf = _write_map(v2, [])  # пустой ledger
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert rep["counts"]["dual_write_present_but_not_in_ledger"] == 1
    assert rep["counts"]["missing_v2_real_backlog"] == 0
    assert rep["real_backlog"] is False


# ── 3. experiment sandbox excluded ──────────────────────────────────────────

def test_experiment_sandbox_excluded(roots):
    legacy, v2 = roots
    _make_legacy_project(
        legacy, "obj/KJ/REAL/_experiments/run1/shadow",
        doc_code="shadow", section="KJ",
    )
    mf = _write_map(v2, [])
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert rep["counts"]["experiment_sandbox_junk"] == 1
    # junk не попадает в реальный backlog
    assert rep["counts"]["missing_v2_real_backlog"] == 0
    assert rep["real_backlog"] is False


def test_smoke_project_itself_is_excluded(roots):
    legacy, v2 = roots
    _make_legacy_project(
        legacy, "obj/EOM/_smoke_dualwrite_20260617",
        doc_code="EOM/_smoke_dualwrite_20260617", section="EOM",
    )
    mf = _write_map(v2, [])
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert rep["counts"]["experiment_sandbox_junk"] == 1
    assert rep["real_backlog"] is False


def test_stale_pdf_suffix_and_wrong_section_resolve_unique_v2_doc(roots):
    legacy, v2 = roots
    _make_legacy_project(
        legacy, "obj/SS/DOC-PDF", doc_code="DOC-PDF.pdf", section="EOM",
    )
    _make_v2_doc(v2, "obj_slug", "SS", "DOC-PDF")
    mf = _write_map(v2, [])
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert rep["counts"]["dual_write_present_but_not_in_ledger"] == 1
    assert rep["counts"]["missing_v2_real_backlog"] == 0


def test_test_object_is_not_accepted_as_production_match(roots):
    legacy, v2 = roots
    _make_legacy_project(legacy, "obj/AR/DOC-T", doc_code="DOC-T", section="AR")
    _make_v2_doc(v2, "test_synthetic", "AR", "DOC-T")
    mf = _write_map(v2, [])
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert rep["counts"]["missing_v2_real_backlog"] == 1


def test_verified_archived_exclusion_is_not_backlog(roots, tmp_path):
    legacy, v2 = roots
    _make_legacy_project(
        legacy, "obj/AR/AR7_norms_pilot", doc_code="AR7_norms_pilot", section="AR",
    )
    archive = tmp_path / "AR7.tgz"
    archive.write_bytes(b"verified archive")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    _write_map(v2, [])
    (v2 / "_system" / "legacy_deletion_exclusions.json").write_text(
        json.dumps({"exclusions": [{
            "legacy_relative_path": "obj/AR/AR7_norms_pilot",
            "archive_path": str(archive),
            "archive_sha256": digest,
        }]}),
        encoding="utf-8",
    )
    rep = vmc.classify_coverage(legacy, v2, v2 / "_system" / "old_to_new_map.json")
    assert rep["counts"]["archived_intentional_exclusion"] == 1
    assert rep["counts"]["missing_v2_real_backlog"] == 0
    assert rep["exclusion_checks"][0]["verified"] is True


def test_unverified_archived_exclusion_remains_backlog(roots, tmp_path):
    legacy, v2 = roots
    _make_legacy_project(
        legacy, "obj/AR/AR7_norms_pilot", doc_code="AR7_norms_pilot", section="AR",
    )
    archive = tmp_path / "AR7.tgz"
    archive.write_bytes(b"changed")
    _write_map(v2, [])
    (v2 / "_system" / "legacy_deletion_exclusions.json").write_text(
        json.dumps({"exclusions": [{
            "legacy_relative_path": "obj/AR/AR7_norms_pilot",
            "archive_path": str(archive),
            "archive_sha256": "0" * 64,
        }]}),
        encoding="utf-8",
    )
    rep = vmc.classify_coverage(legacy, v2, v2 / "_system" / "old_to_new_map.json")
    assert rep["counts"]["archived_intentional_exclusion"] == 0
    assert rep["counts"]["missing_v2_real_backlog"] == 1
    assert rep["exclusion_checks"][0]["verified"] is False


# ── 4. orphan .pdf-named folder ─────────────────────────────────────────────

def test_orphan_pdf_named_folder(roots):
    legacy, v2 = roots
    _make_legacy_project(legacy, "obj/EOM/DOC-3(main)/DOC-3 V2.pdf",
                         doc_code="DOC-3 V2.pdf", section="EOM")
    mf = _write_map(v2, [])
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert rep["counts"]["orphan_pdf_named_folder"] == 1
    assert rep["real_backlog"] is False


def test_pdf_named_folder_that_is_mapped_is_not_orphan(roots):
    """`.pdf`-именованная папка, которая ЕСТЬ в map, → mapped (не orphan).

    Ловит ordering-баг: orphan-классификация не должна перебивать mapped.
    """
    legacy, v2 = roots
    d = _make_legacy_project(legacy, "obj/KJ/DOC-X(main)/DOC-X (1).pdf",
                             doc_code="DOC-X", section="KJ")
    _make_v2_doc(v2, "obj_slug", "KJ", "DOC-X")
    mf = _write_map(v2, [{"document_code": "DOC-X", "legacy_folder_path": str(d)}])
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert rep["counts"]["mapped"] == 1
    assert rep["counts"]["orphan_pdf_named_folder"] == 0


# ── 5. real missing backlog → exit 1 ────────────────────────────────────────

def test_real_missing_backlog_exit_1(roots, capsys):
    legacy, v2 = roots
    _make_legacy_project(legacy, "obj/VK/DOC-4", doc_code="DOC-4", section="VK")
    # ни map-записи, ни v2-документа
    _write_map(v2, [])
    code = vmc.main(["--legacy-root", str(legacy), "--v2-root", str(v2)])
    assert code == 1
    rep_out = capsys.readouterr().out
    assert "missing_v2_real_backlog" in rep_out


# ── 6. output → custom /tmp path, not _system ───────────────────────────────

def test_output_to_custom_path(roots, tmp_path):
    legacy, v2 = roots
    _make_legacy_project(legacy, "obj/AR/DOC-5", doc_code="DOC-5", section="AR")
    _make_v2_doc(v2, "obj_slug", "AR", "DOC-5")
    _write_map(v2, [])
    out = tmp_path / "custom" / "coverage.json"
    code = vmc.main(["--legacy-root", str(legacy), "--v2-root", str(v2),
                     "--output", str(out)])
    assert code == 0
    assert out.is_file()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["counts"]["dual_write_present_but_not_in_ledger"] == 1


def test_output_refuses_system_dir(roots):
    legacy, v2 = roots
    _make_legacy_project(legacy, "obj/AR/DOC-6", doc_code="DOC-6", section="AR")
    _write_map(v2, [])
    bad = v2 / "_system" / "coverage.json"
    code = vmc.main(["--legacy-root", str(legacy), "--v2-root", str(v2),
                     "--output", str(bad)])
    assert code == 2
    assert not bad.exists()


# ── 7. default mode не мутирует v2 root ─────────────────────────────────────

def test_default_mode_does_not_mutate_v2(roots):
    legacy, v2 = roots
    _make_legacy_project(legacy, "obj/AR/DOC-7", doc_code="DOC-7", section="AR")
    _make_v2_doc(v2, "obj_slug", "AR", "DOC-7")
    _write_map(v2, [])

    before = {str(p): p.stat().st_mtime_ns for p in v2.rglob("*") if p.is_file()}
    code = vmc.main(["--legacy-root", str(legacy), "--v2-root", str(v2)])
    after = {str(p): p.stat().st_mtime_ns for p in v2.rglob("*") if p.is_file()}

    assert code == 0
    assert before == after  # ни одного нового/изменённого файла в v2


# ── 8. snapshot drift = warning, не hard fail ───────────────────────────────

def test_snapshot_drift_is_warning_not_fail(roots):
    legacy, v2 = roots
    d = _make_legacy_project(legacy, "obj/EOM/DOC-8", doc_code="DOC-8",
                             section="EOM", findings_bytes=200_000)
    _make_v2_doc(v2, "obj_slug", "EOM", "DOC-8", findings_bytes=120_000)
    mf = _write_map(v2, [{"document_code": "DOC-8", "legacy_folder_path": str(d)}])
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert rep["counts"]["mapped"] == 1
    assert len(rep["snapshot_drift_candidates"]) == 1
    assert rep["real_backlog"] is False  # drift не делает backlog


def test_snapshot_drift_within_tolerance_not_flagged(roots):
    legacy, v2 = roots
    d = _make_legacy_project(legacy, "obj/EOM/DOC-9", doc_code="DOC-9",
                             section="EOM", findings_bytes=100_000)
    _make_v2_doc(v2, "obj_slug", "EOM", "DOC-9", findings_bytes=100_200)  # +200B < 2%
    mf = _write_map(v2, [{"document_code": "DOC-9", "legacy_folder_path": str(d)}])
    rep = vmc.classify_coverage(legacy, v2, mf)
    assert len(rep["snapshot_drift_candidates"]) == 0
