"""
Регрессия: projects_v2 read/write консистентность для findings/статуса после
ЖИВОГО аудита.

Системный баг: при `dual_write_shadow` v2-снимок обрывался на block_analysis
(late artifacts — 03_findings/optimization/нормы + финальный pipeline_log —
попадали только в legacy `_output`). При включённом read-cutover findings/статус
читались из v2 → UI показывал «аудит не проводился» / неполный конвейер, хотя
legacy содержит полный аудит.

Фикс — две стороны:
  WRITE: re-mirror проекта в projects_v2 ПОСЛЕ завершения всего конвейера
         (единая точка `_run_batch_queue` → `_shadow_mirror_completed_audit`),
         где legacy `_output` уже содержит late artifacts.
  READ:  защитный fallback на legacy в read_canary, если v2-снимок неполный
         (нет findings-артефакта в v2), а legacy-аудит завершён.

Гермётичны: tmp_path + AUDIT_PROJECTS_V2_DIR, никаких живых данных.

Run: python -m pytest tests/test_projects_v2_incomplete_snapshot_fallback.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.app.services.storage import read_canary as RC  # noqa: E402
from backend.app.services.storage import storage_write_facade as swf  # noqa: E402
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter  # noqa: E402


OBJF = "214_Alia_ASTERUS"
OBJID = "73a0e59a"
DISC = "KM"
CODE = "Mockup 2 V1"


class _Req:
    """Минимальный stub Request: только query_params.get(...)."""

    def __init__(self, params: dict | None = None):
        self.query_params = params or {}


def _wj(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _build(tmp_path, *, v2_findings: bool, legacy_findings: bool,
           legacy_pipeline_late: bool = True):
    """Синтетический v2-документ + (опц.) legacy `_output`.

    v2_findings — есть ли 03_findings.json в v2 `03_analysis/latest`.
    legacy_findings — есть ли 03_findings.json в legacy `_output`.
    Возвращает (v2_root, legacy_project_dir).
    """
    v2 = tmp_path / "projects_v2"
    legacy_root = tmp_path / "projects" / "214. Alia (ASTERUS)"
    legacy_proj = legacy_root / DISC / CODE
    # object.json c legacy_path (его читает _legacy_output_dir_for_doc)
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OBJID, "display_name": "214", "folder_name": OBJF,
         "legacy_path": str(legacy_root)})
    doc = v2 / "objects" / OBJF / "disciplines" / DISC / "documents" / CODE
    _wj(doc / "document.json",
        {"document_code": CODE, "object_id": OBJID, "discipline": DISC,
         "kind": "plain", "current_version": "v001",
         "versions": [{"version_id": "v001", "version_no": 1}]})
    (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
    latest = doc / "versions" / "v001" / "03_analysis" / "latest"
    _wj(doc / "versions" / "v001" / "version.json",
        {"version_id": "v001", "version_no": 1, "analysis_status": "complete"})
    # v2 всегда имеет ранние артефакты (как снимок, замерший на block_analysis)
    _wj(latest / "02_blocks_analysis.json", {"block_analyses": []})
    if v2_findings:
        _wj(latest / "03_findings.json",
            {"findings": [{"id": f"V2-{i}", "severity": "Критическое"} for i in range(3)]})
    # legacy _output
    if legacy_findings:
        out = legacy_proj / "_output"
        _wj(out / "03_findings.json",
            {"findings": [{"id": f"F-{i:03d}", "severity": "Критическое"} for i in range(38)]})
        stages = {"block_analysis": {"status": "done"}}
        if legacy_pipeline_late:
            stages.update({"findings_merge": {"status": "done"},
                           "optimization": {"status": "done"},
                           "excel": {"status": "done"}})
        _wj(out / "pipeline_log.json", {"version": 1, "stages": stages})
        _wj(legacy_proj / "project_info.json",
            {"project_id": f"{DISC}/{CODE}", "name": CODE, "section": DISC,
             "object_id": OBJID})
    return v2, doc


# ---------------------------------------------------------------------------
# helpers: сигнал неполноты снимка
# ---------------------------------------------------------------------------

def test_snapshot_incomplete_when_v2_findings_missing_legacy_present(tmp_path, monkeypatch):
    v2, doc = _build(tmp_path, v2_findings=False, legacy_findings=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    a = ProjectsV2Adapter()
    out_dir = RC._v2_snapshot_incomplete(a, doc, "v001")
    assert out_dir is not None
    assert (out_dir / "03_findings.json").is_file()


def test_snapshot_complete_when_v2_has_findings(tmp_path, monkeypatch):
    v2, doc = _build(tmp_path, v2_findings=True, legacy_findings=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    a = ProjectsV2Adapter()
    # v2 findings есть → снимок достаточно полный → НЕ fallback
    assert RC._v2_snapshot_incomplete(a, doc, "v001") is None


def test_snapshot_not_incomplete_when_legacy_also_missing(tmp_path, monkeypatch):
    v2, doc = _build(tmp_path, v2_findings=False, legacy_findings=False)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    a = ProjectsV2Adapter()
    # ни v2, ни legacy findings → нет смысла в fallback (поведение прежнее)
    assert RC._v2_snapshot_incomplete(a, doc, "v001") is None


# ---------------------------------------------------------------------------
# Test 3: findings endpoint → fallback на legacy при неполном v2
# ---------------------------------------------------------------------------

def test_v2_findings_falls_back_to_legacy_when_incomplete(tmp_path, monkeypatch):
    v2, doc = _build(tmp_path, v2_findings=False, legacy_findings=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))

    # legacy findings_service резолвится через PROJECTS_DIR — мокаем его, чтобы
    # тест был гермётичен (проверяем именно ветку fallback read_canary).
    fake = {"findings": [{"id": f"F-{i:03d}"} for i in range(38)], "total": 38}

    def _fake_get_findings(project_id, version_id=None):
        class _M:
            def model_dump(self_inner):
                return dict(fake)
        return _M()

    import backend.app.services.findings.findings_service as fs
    monkeypatch.setattr(fs, "get_findings", _fake_get_findings)

    res = RC.v2_findings(_Req(), f"{DISC}/{CODE}")
    assert res.get("storage_backend") == "legacy_fallback"
    assert res.get("v2_snapshot_incomplete") is True
    assert len(res["findings"]) == 38  # не пустой «аудит не проводился»


def test_v2_findings_prefers_v2_when_complete(tmp_path, monkeypatch):
    v2, doc = _build(tmp_path, v2_findings=True, legacy_findings=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))

    def _boom(*a, **k):  # fallback не должен вызываться
        raise AssertionError("legacy fallback must NOT run when v2 is complete")

    monkeypatch.setattr(RC, "_legacy_findings_fallback", _boom)
    res = RC.v2_findings(_Req(), f"{DISC}/{CODE}")
    assert res.get("storage_backend") == RC.BACKEND_V2
    assert "v2_snapshot_incomplete" not in res
    assert res["findings_count"] == 3


def test_v2_findings_both_missing_keeps_v2_empty(tmp_path, monkeypatch):
    v2, doc = _build(tmp_path, v2_findings=False, legacy_findings=False)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    res = RC.v2_findings(_Req(), f"{DISC}/{CODE}")
    # поведение прежнее: v2-ответ, пустые findings, без fallback-маркера
    assert res.get("storage_backend") == RC.BACKEND_V2
    assert res["findings_count"] == 0
    assert "v2_snapshot_incomplete" not in res


# ---------------------------------------------------------------------------
# Test 4: project status endpoint → fallback на legacy pipeline_log
# ---------------------------------------------------------------------------

def test_v2_project_details_falls_back_to_legacy_status(tmp_path, monkeypatch):
    v2, doc = _build(tmp_path, v2_findings=False, legacy_findings=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))

    sentinel = {"project_id": f"{DISC}/{CODE}", "pipeline": {"excel": "done"}}

    def _fake_status(project_id, version_id=None):
        class _M:
            def model_dump(self_inner):
                return dict(sentinel)
        return _M()

    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "get_project_status", _fake_status)

    res = RC.v2_project_details(_Req(), f"{DISC}/{CODE}")
    assert res.get("storage_backend") == "legacy_fallback"
    assert res.get("v2_snapshot_incomplete") is True
    assert res["pipeline"]["excel"] == "done"


def test_v2_project_details_prefers_v2_when_complete(tmp_path, monkeypatch):
    v2, doc = _build(tmp_path, v2_findings=True, legacy_findings=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))

    def _boom(*a, **k):
        raise AssertionError("legacy status fallback must NOT run when v2 complete")

    monkeypatch.setattr(RC, "_legacy_project_status_fallback", _boom)
    res = RC.v2_project_details(_Req(), f"{DISC}/{CODE}")
    assert res.get("storage_backend") == RC.BACKEND_V2


# ---------------------------------------------------------------------------
# Test 1 + 2: WRITE — re-mirror после полного аудита переносит late artifacts
# ---------------------------------------------------------------------------

def _make_legacy_project(tmp_path, *, with_late: bool):
    """legacy проект в раскладке <objects>/<obj>/<disc>/<doc> с _output."""
    legacy_root = tmp_path / "projects"
    proj = legacy_root / "OBJX" / DISC / "DOCX"
    out = proj / "_output"
    out.mkdir(parents=True)
    _wj(proj / "project_info.json",
        {"project_id": f"{DISC}/DOCX", "name": "DOCX", "section": DISC,
         "object_id": "objhashX"})
    _wj(out / "01_text_analysis.json", {"x": 1})
    _wj(out / "02_blocks_analysis.json", {"block_analyses": []})
    stages = {"block_analysis": {"status": "done"}}
    if with_late:
        _wj(out / "03_findings.json",
            {"findings": [{"id": "F-001"}, {"id": "F-002"}]})
        _wj(out / "optimization.json", {"meta": {"total_items": 1}})
        stages.update({"findings_merge": {"status": "done"},
                       "optimization": {"status": "done"},
                       "excel": {"status": "done"}})
    _wj(out / "pipeline_log.json", {"version": 1, "stages": stages})
    return proj


def test_full_mirror_copies_late_artifacts_and_complete_pipeline(tmp_path, monkeypatch):
    proj = _make_legacy_project(tmp_path, with_late=True)
    v2 = tmp_path / "projects_v2"
    v2.mkdir()
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "dual_write_shadow")

    res = swf.StorageWriteFacade().shadow_mirror_project(proj)
    assert res.v2_ok is True

    doc_dir = Path(res.v2_paths[0])
    a = ProjectsV2Adapter()
    # Test 1: 03_findings.json попал в v2 latest
    assert a.findings_path(doc_dir, "v001") is not None
    assert a.findings_count(doc_dir, "v001") == 2
    # Test 2: pipeline_log в v2 содержит late stages (не замер на block_analysis)
    plog = a.read_pipeline_log(doc_dir, "v001") or {}
    stages = plog.get("stages", {})
    assert stages.get("findings_merge", {}).get("status") == "done"
    assert stages.get("excel", {}).get("status") == "done"
    # Test 8 (эквивалент): после полного mirror read-side НЕ видит снимок неполным
    assert RC._v2_snapshot_incomplete(a, doc_dir, "v001") is None


def test_partial_mirror_then_full_mirror_overwrites_incomplete(tmp_path, monkeypatch):
    """Идемпотентность: ранний (block_analysis) снимок, затем полный — late
    artifacts появляются (повторный mirror перезаписывает latest)."""
    v2 = tmp_path / "projects_v2"
    v2.mkdir()
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "dual_write_shadow")
    f = swf.StorageWriteFacade()

    # ранний снимок: без late artifacts
    proj = _make_legacy_project(tmp_path, with_late=False)
    res1 = f.shadow_mirror_project(proj)
    a = ProjectsV2Adapter()
    doc_dir = Path(res1.v2_paths[0])
    assert a.findings_path(doc_dir, "v001") is None  # findings ещё нет

    # дописываем late artifacts в legacy (как делает аудит после block_analysis)
    out = proj / "_output"
    _wj(out / "03_findings.json", {"findings": [{"id": "F-001"}]})
    _wj(out / "pipeline_log.json", {"version": 1, "stages": {
        "block_analysis": {"status": "done"}, "excel": {"status": "done"}}})

    # полный снимок (как _run_batch_queue по завершении)
    f.shadow_mirror_project(proj)
    assert a.findings_path(doc_dir, "v001") is not None
    assert a.findings_count(doc_dir, "v001") == 1


# ---------------------------------------------------------------------------
# WRITE — manager helper wiring + fail-soft
# ---------------------------------------------------------------------------

def test_manager_completion_helper_calls_mirror(monkeypatch):
    from backend.app.pipeline.manager import PipelineManager
    calls = []
    monkeypatch.setattr(
        swf, "shadow_mirror_project_id_safe",
        lambda pid, run_id=None: calls.append((pid, run_id)),
    )
    mgr = PipelineManager.__new__(PipelineManager)  # без тяжёлого __init__

    class _Job:
        job_id = "job-123"

    mgr._shadow_mirror_completed_audit("KM/DOCX", _Job())
    assert calls == [("KM/DOCX", "job-123")]


def test_manager_completion_helper_fail_soft(monkeypatch):
    from backend.app.pipeline.manager import PipelineManager

    def _boom(pid, run_id=None):
        raise RuntimeError("v2 mirror exploded")

    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", _boom)
    mgr = PipelineManager.__new__(PipelineManager)
    # не должно поднять исключение (fail-soft — legacy авторитетна)
    mgr._shadow_mirror_completed_audit("KM/DOCX", None)
