"""
Тесты пилотной миграции одобренных WARNINGS_NEED_POLICY проектов
(`--need-policy-approved`). Гермётичны (tmp_path).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"
sys.path.insert(0, str(_SCRIPTS))
import v2lib                              # noqa: E402
import batch_migrate_projects_v2 as batch  # noqa: E402

CWW = "CAN_MIGRATE_WITH_WARNINGS"
OBJECTS_MAP = {"by_name": {"OBJ": "o1"}, "by_path": {}, "by_id": {"o1": "OBJ"}}


def _mk(proj: Path, base: str, *, ocr=True, analysis=False, project_info=True):
    proj.mkdir(parents=True)
    (proj / f"{base}.pdf").write_text("%PDF " + base, encoding="utf-8")
    (proj / f"{base}_document.md").write_text("# " + base, encoding="utf-8")
    if ocr:
        (proj / f"{base}_ocr.html").write_text("<html>", encoding="utf-8")
    (proj / f"{base}_result.json").write_text("{}", encoding="utf-8")
    if project_info:
        (proj / "project_info.json").write_text("{}", encoding="utf-8")
    out = proj / "_output"; out.mkdir()
    if analysis:
        (out / "01_text_analysis.json").write_text("{}", encoding="utf-8")
        (out / "02_blocks_analysis.json").write_text("{}", encoding="utf-8")
        (out / "03_findings.json").write_text("{}", encoding="utf-8")


def _row(subgroup, legacy_path, document_code, can_migrate, *, discipline="AR"):
    return {
        "subgroup": subgroup, "can_migrate_auto_after_policy": can_migrate,
        "object": "OBJ", "discipline": discipline, "document_code": document_code,
        "legacy_path": str(legacy_path),
    }


@pytest.fixture
def env(tmp_path):
    legacy = tmp_path / "projects"
    disc = legacy / "OBJ" / "AR"
    # SINGLE .pdf-named folder, no analysis
    _mk(disc / "SNG V1.pdf", "SNG V1", analysis=False)
    # MISSING OCR (no ocr), with analysis
    _mk(disc / "NOOCR", "NOOCR", ocr=False, analysis=True)
    # LEGACY KB preserve (no analysis)
    _mk(disc / "KB V1.pdf", "KB V1", analysis=False)
    # MANUAL (not approved)
    _mk(disc / "MAN", "MAN")
    # BLOCKED (not approved)
    _mk(disc / "BLK", "BLK")

    v2 = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2)
    rows = [
        _row("POLICY_READY_SINGLE_PDF_NAMED_FOLDER", disc / "SNG V1.pdf", "SNG V1", True),
        _row("POLICY_READY_MISSING_OCR_HTML", disc / "NOOCR", "NOOCR", True),
        _row("POLICY_READY_LEGACY_KB_PRESERVE", disc / "KB V1.pdf", "KB V1", True),
        _row("POLICY_NEEDS_MANUAL_VERSION_GROUPING", disc / "MAN", "MAN", False),
        _row("POLICY_RECHECK_AS_BLOCKED", disc / "BLK", "BLK", False),
    ]
    (v2 / "_system" / "need_policy_analysis_report.json").write_text(
        json.dumps({"projects": rows}, ensure_ascii=False), encoding="utf-8")
    report_path = v2 / "_system" / "need_policy_analysis_report.json"
    return legacy, v2, report_path


def snapshot(root: Path) -> dict:
    return {str(p.relative_to(root)): (v2lib.sha256_file(p), int(p.stat().st_mtime))
            for p in sorted(root.rglob("*")) if p.is_file()}


# ---------------------------------------------------------------------------
# validate_request
# ---------------------------------------------------------------------------


def test_need_policy_requires_warnings_class():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request("AUTO_SAFE", execute=True, dry_run=False,
                               allow_warnings=False, force=False, need_policy_approved=True)


def test_need_policy_conflicts_with_warning_policy():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(CWW, execute=True, dry_run=False, allow_warnings=False,
                               force=False, warning_policy="WARNINGS_AUTO_CANDIDATE",
                               need_policy_approved=True)


def test_need_policy_ok_without_allow_warnings():
    batch.validate_request(CWW, execute=True, dry_run=False, allow_warnings=False,
                           force=False, need_policy_approved=True)


def test_warnings_class_without_any_mode_errors():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(CWW, execute=False, dry_run=True, allow_warnings=True,
                               force=False)


# ---------------------------------------------------------------------------
# selection
# ---------------------------------------------------------------------------


def test_dry_run_selects_only_approved(env):
    legacy, v2, rp = env
    before = snapshot(legacy)
    res = batch.run_batch(report_path=rp, v2_root=v2, klass=CWW, limit=10,
                          skip_already_migrated=True, execute=False,
                          objects_map=OBJECTS_MAP, need_policy_approved=True)
    codes = {r["document_code"] for r in res["rows"]}
    assert codes == {"SNG V1", "NOOCR", "KB V1"}        # approved only
    assert "MAN" not in codes and "BLK" not in codes    # manual/blocked excluded
    assert res["summary"]["copied_files_total"] == 0
    assert res["summary"]["errors"] == 0
    assert snapshot(legacy) == before                   # dry-run no-op


def test_limit_respected(env):
    legacy, v2, rp = env
    res = batch.run_batch(report_path=rp, v2_root=v2, klass=CWW, limit=2,
                          skip_already_migrated=True, execute=False,
                          objects_map=OBJECTS_MAP, need_policy_approved=True)
    assert res["summary"]["selected"] == 2


# ---------------------------------------------------------------------------
# execute + per-policy rules
# ---------------------------------------------------------------------------


def test_execute_applies_policy_rules(env):
    legacy, v2, rp = env
    before = snapshot(legacy)
    res = batch.run_batch(report_path=rp, v2_root=v2, klass=CWW, limit=10,
                          skip_already_migrated=True, execute=True,
                          objects_map=OBJECTS_MAP, need_policy_approved=True)
    assert res["summary"]["migrated"] == 3
    assert res["summary"]["errors"] == 0

    docroot = v2 / "objects" / "obj_o1" / "disciplines" / "AR" / "documents"
    # fallback: object folder name may be readable; resolve by scanning
    def doc_dir(code):
        for d in (v2 / "objects").rglob("document.json"):
            if d.parent.name == code:
                return d.parent
        return None

    # MISSING_OCR: нет fake ocr.html, manifest содержит missing_optional_files
    noocr = doc_dir("NOOCR")
    assert noocr is not None
    v1 = noocr / "versions" / "v001"
    assert not (v1 / "02_work" / "ocr.html").exists()
    man = json.loads((v1 / "01_input" / "input_manifest.json").read_text(encoding="utf-8"))
    assert man["missing_optional_files"] == ["ocr_html"]

    # SINGLE .pdf folder -> versions/v001, старое имя в metadata
    sng = doc_dir("SNG V1")
    assert sng is not None and (sng / "versions" / "v001").is_dir()
    vj = json.loads((sng / "versions" / "v001" / "version.json").read_text(encoding="utf-8"))
    assert vj["legacy_folder_name"] == "SNG V1.pdf"

    # LEGACY_KB_PRESERVE -> analysis_status=legacy_partial + поля
    kb = doc_dir("KB V1")
    vjk = json.loads((kb / "versions" / "v001" / "version.json").read_text(encoding="utf-8"))
    assert vjk["analysis_status"] == "legacy_partial"
    assert vjk["analysis_generation"] == "legacy"
    assert vjk["preserve_reason"] == "legacy_algorithm_with_kb_findings"

    # legacy не изменён
    assert snapshot(legacy) == before


def test_manual_blocked_never_migrated(env):
    legacy, v2, rp = env
    batch.run_batch(report_path=rp, v2_root=v2, klass=CWW, limit=10,
                    skip_already_migrated=True, execute=True,
                    objects_map=OBJECTS_MAP, need_policy_approved=True)
    docs = {d.parent.name for d in (v2 / "objects").rglob("document.json")}
    assert "MAN" not in docs and "BLK" not in docs


def test_execute_then_skip(env):
    legacy, v2, rp = env
    batch.run_batch(report_path=rp, v2_root=v2, klass=CWW, limit=10,
                    skip_already_migrated=True, execute=True,
                    objects_map=OBJECTS_MAP, need_policy_approved=True)
    res2 = batch.run_batch(report_path=rp, v2_root=v2, klass=CWW, limit=10,
                           skip_already_migrated=True, execute=True,
                           objects_map=OBJECTS_MAP, need_policy_approved=True)
    assert res2["summary"]["selected"] == 0
    assert res2["summary"]["skipped_already_migrated"] == 3
