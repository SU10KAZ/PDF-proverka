"""v2-only read / export / destructive-contract совместимость.

Всё в tmp_path; legacy `projects/` отсутствует. Проверяем, что read/export
работают из projects_v2 без legacy, а destructive-op требует backup+confirmation.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.storage import v2_primary_wiring as wiring
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
from backend.app.services.storage.storage_write_facade import DestructiveWriteBlocked

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"


def _make_v2_full(v2_root: Path, obj="OBJ_F", disc="KJ", code="DOC-6C",
                  *, object_id="obj6c", vid="v001", findings_n=3) -> Path:
    """Полный v2-документ: input PDF + metadata + findings + optimization + log."""
    doc = v2_root / "objects" / obj / "disciplines" / disc / "documents" / code
    doc.mkdir(parents=True, exist_ok=True)
    (doc / "document.json").write_text(json.dumps({
        "schema_version": 1, "document_code": code, "object_id": object_id,
        "versions": [{"version_id": vid, "version_no": 1}],
    }), encoding="utf-8")
    vdir = doc / "versions" / vid
    (vdir / "01_input").mkdir(parents=True, exist_ok=True)
    (vdir / "01_input" / f"{code}.pdf").write_bytes(b"%PDF-1.4 fake")
    (vdir / "01_input" / f"{code}_document.md").write_bytes(b"# md")
    latest = vdir / "03_analysis" / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    (latest / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": f"F-{i}", "category": "Критическое"}
                                 for i in range(findings_n)]}), encoding="utf-8")
    (latest / "02_blocks_analysis.json").write_text(json.dumps({"blocks": [{"block_id": "b1"}]}), encoding="utf-8")
    (latest / "optimization.json").write_text(json.dumps({"items": [{"id": "o1"}]}), encoding="utf-8")
    (vdir / "03_analysis" / "runs" / "r1").mkdir(parents=True, exist_ok=True)
    (vdir / "03_analysis" / "runs" / "r1" / "pipeline_log.json").write_text(
        json.dumps({"stages": ["done"]}), encoding="utf-8")
    return doc


# ── read v2-only ────────────────────────────────────────────────────────────

def test_v2_only_read_does_not_require_legacy(tmp_path):
    v2 = tmp_path / "projects_v2"
    doc = _make_v2_full(v2, findings_n=5)
    assert not (tmp_path / "projects").exists()  # legacy отсутствует

    adapter = ProjectsV2Adapter(v2)
    assert adapter.findings_count(doc, "v001") == 5
    assert adapter.read_blocks_analysis(doc, "v001") is not None
    assert adapter.has_pipeline_log(doc, "v001") is True
    d = adapter.find_document("DOC-6C")
    assert d is not None and d["object_folder"] == "OBJ_F"


def test_v2_only_findings_status_optimization_consistent(tmp_path):
    v2 = tmp_path / "projects_v2"
    doc = _make_v2_full(v2, findings_n=4)
    adapter = ProjectsV2Adapter(v2)
    # findings
    assert adapter.findings_count(doc, "v001") == 4
    # status (analysis present)
    af = adapter.latest_analysis_files(doc, "v001")
    assert af["has_03_findings"] is True
    # optimization — читается напрямую из latest
    opt = json.loads((adapter.latest_dir(doc, "v001") / "optimization.json").read_text(encoding="utf-8"))
    assert opt["items"][0]["id"] == "o1"


def test_v2_only_finding_by_id_via_list(tmp_path):
    v2 = tmp_path / "projects_v2"
    doc = _make_v2_full(v2, findings_n=3)
    adapter = ProjectsV2Adapter(v2)
    flist = adapter.findings_list(doc, "v001")
    by_id = {f["id"]: f for f in flist}
    assert "F-1" in by_id and by_id["F-1"]["category"] == "Критическое"


# ── export source lookup ─────────────────────────────────────────────────────

def test_v2_only_export_can_find_pdf_or_reports_blocker(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    _make_v2_full(v2)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    # helper находит исходный PDF из v2 01_input без legacy
    pdf = wiring.v2_source_pdf("DOC-6C", "v001", v2_root=v2)
    assert pdf is not None
    assert pdf.name == "DOC-6C.pdf"
    assert pdf.resolve().is_relative_to(v2.resolve())
    assert pdf.read_bytes().startswith(b"%PDF")
    # NOTE blocker: полный export.py ZIP всё ещё через resolve_project_dir(legacy)
    # + _resolve_version_pdf смотрит в version_dir root (не 01_input). Переписать
    # export под v2 — отдельный шаг (документировано в playbook 8/10).


# ── destructive guards ───────────────────────────────────────────────────────

def test_guard_noop_in_legacy_and_dual_shadow(monkeypatch):
    for mode in ("legacy", "dual_write_shadow"):
        monkeypatch.setenv(_WMODE, mode)
        # no-op: не бросает
        wiring.guard_destructive_v2_primary("clean_project_data")
        wiring.guard_destructive_v2_primary("rename_project")
        wiring.guard_destructive_v2_primary("delete_project")


def test_clean_guard_requires_backup_confirmation_in_v2_primary(monkeypatch):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    with pytest.raises(DestructiveWriteBlocked):
        wiring.guard_destructive_v2_primary("clean_project_data")
    # После создания backup и записи confirmation log caller передаёт контекст —
    # guard больше не блокирует конкретный clean.
    wiring.guard_destructive_v2_primary(
        "clean_project_data", confirmed=True, backup_id="backup-1",
    )


def test_rename_blocked_or_guarded_in_v2_primary(monkeypatch):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    with pytest.raises(DestructiveWriteBlocked):
        wiring.guard_destructive_v2_primary("rename_project")


def test_delete_blocked_in_v2_primary(monkeypatch):
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    with pytest.raises(DestructiveWriteBlocked):
        wiring.guard_destructive_v2_primary("delete_project")


def test_clean_project_data_requires_confirmation_in_v2_primary_real(monkeypatch, tmp_path):
    """Реальная clean_project_data в v2-primary требует явное подтверждение
    ДО резолва target и любых файловых операций."""
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    from backend.app.services.common import project_service
    with pytest.raises(ValueError, match="_confirmed"):
        project_service.clean_project_data("any/project")
