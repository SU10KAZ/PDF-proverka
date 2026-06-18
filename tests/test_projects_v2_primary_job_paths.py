"""Тесты Шага 6B/10 — manager._resolve_job_paths / pipeline-write для V2_PRIMARY.

Всё в tmp_path; production projects_v2 / projects не трогаются. Ветка v2-primary
flag-gated и в проде (dual_write_shadow) не исполняется.
"""
from __future__ import annotations

import json
import types
from pathlib import Path

import pytest

from backend.app.services.storage import v2_primary_wiring as wiring

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"
_V2DIR = "AUDIT_PROJECTS_V2_DIR"


def _job(project_id="DOC-6B", version_id="v001", job_id="job_6b_1"):
    return types.SimpleNamespace(project_id=project_id, version_id=version_id, job_id=job_id)


def _make_v2_doc(v2_root: Path, obj_folder: str, disc: str, doc_code: str,
                 *, object_id="73a0e59a", versions=("v001",)) -> Path:
    doc = v2_root / "objects" / obj_folder / "disciplines" / disc / "documents" / doc_code
    doc.mkdir(parents=True, exist_ok=True)
    (doc / "document.json").write_text(json.dumps({
        "schema_version": 1, "document_code": doc_code, "object_id": object_id,
        "versions": [{"version_id": v, "version_no": i + 1} for i, v in enumerate(versions)],
    }), encoding="utf-8")
    for v in versions:
        (doc / "versions" / v).mkdir(parents=True, exist_ok=True)
    return doc


def _resolve_via_manager(job):
    """Вызвать PipelineManager._resolve_job_paths без тяжёлого __init__."""
    from backend.app.pipeline.manager import PipelineManager
    dummy = object.__new__(PipelineManager)
    return PipelineManager._resolve_job_paths(dummy, job)


# ── Test 1 & 2: legacy / dual_shadow unchanged ──────────────────────────────

def _legacy_check(monkeypatch, tmp_path, mode):
    monkeypatch.setenv(_WMODE, mode)
    legacy_root = tmp_path / "projects" / "OBJ" / "KJ" / "DOC-6B"
    legacy_root.mkdir(parents=True)
    import backend.app.pipeline.manager as mgr
    monkeypatch.setattr(mgr, "resolve_project_dir", lambda pid, **kw: legacy_root)
    root, version_dir, output_dir = _resolve_via_manager(_job())
    # legacy-форма: output == version_dir/_output, под legacy-root, НЕ v2
    assert output_dir == version_dir / "_output"
    assert output_dir.resolve().is_relative_to(legacy_root.resolve())
    assert "projects_v2" not in str(output_dir)
    assert "03_analysis" not in str(output_dir)


def test_legacy_mode_unchanged(monkeypatch, tmp_path):
    _legacy_check(monkeypatch, tmp_path, "legacy")


def test_dual_shadow_mode_unchanged(monkeypatch, tmp_path):
    _legacy_check(monkeypatch, tmp_path, "dual_write_shadow")


# ── Test 3: V2_PRIMARY резолвит v2 version dir ──────────────────────────────

def test_v2_primary_resolves_v2_version_dir(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    _make_v2_doc(v2, "OBJ_FOLDER", "KJ", "DOC-6B", versions=("v001", "v002"))
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))
    import backend.app.pipeline.manager as mgr
    # legacy может отсутствовать — резолв v2-native
    monkeypatch.setattr(mgr, "resolve_project_dir",
                        lambda pid, **kw: (_ for _ in ()).throw(FileNotFoundError()))

    doc_dir, version_dir, output_dir = _resolve_via_manager(_job(version_id="v002"))
    assert "projects_v2" in str(version_dir)
    assert version_dir.name == "v002"
    assert output_dir == version_dir / "03_analysis" / "runs" / "job_6b_1"
    assert "_output" not in output_dir.parent.name  # не legacy flat _output


# ── Test 4: V2_PRIMARY output artifacts уходят в v2 ──────────────────────────

def test_v2_primary_output_artifacts_go_to_v2(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    _make_v2_doc(v2, "OBJ_FOLDER", "KJ", "DOC-6B")
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))
    import backend.app.pipeline.manager as mgr
    monkeypatch.setattr(mgr, "resolve_project_dir",
                        lambda pid, **kw: (_ for _ in ()).throw(FileNotFoundError()))

    _doc, _vdir, output_dir = _resolve_via_manager(_job())
    # fake stage write
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "01_text_analysis.json").write_text("{}", encoding="utf-8")
    assert (output_dir / "01_text_analysis.json").is_file()
    assert output_dir.resolve().is_relative_to(v2.resolve())
    # legacy отсутствует
    assert not (tmp_path / "projects").exists()


# ── Test 5: completed audit artifacts → v2 latest ───────────────────────────

def test_v2_primary_completed_audit_artifacts_in_latest(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    _make_v2_doc(v2, "OBJ_FOLDER", "KJ", "DOC-6B")
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    from backend.app.services.storage import v2_primary_prototype as proto
    from backend.app.services.storage.storage_write_facade import StorageWriteFacade

    target = wiring.resolve_v2_target_by_id("DOC-6B", "v001", v2_root=v2)
    assert target is not None
    out = tmp_path / "out"
    out.mkdir()
    for name in proto.LATE_AUDIT_ARTIFACTS:
        (out / name).write_text(json.dumps({"k": name}), encoding="utf-8")

    facade = StorageWriteFacade(v2_root=v2)
    res = proto.write_completed_audit_artifacts_v2(facade, target, out, run_id="job_6b_1")
    assert set(res) == set(proto.LATE_AUDIT_ARTIFACTS)
    latest = target.version_dir(v2) / "03_analysis" / "latest"
    for name in ("03_findings.json", "03_findings_review.json", "norm_checks.json",
                 "optimization.json", "optimization_review.json", "pipeline_log.json"):
        assert (latest / name).is_file(), name


# ── Test 6: legacy недоступен в V2_PRIMARY ──────────────────────────────────

def test_v2_primary_works_without_legacy(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    _make_v2_doc(v2, "OBJ_FOLDER", "AR", "DOC-NOLEG")
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))
    # legacy полностью отсутствует
    assert not (tmp_path / "projects").exists()

    paths = wiring.resolve_v2_job_paths("DOC-NOLEG", "v001", run_id="r1", v2_root=v2,
                                        legacy_project_dir=None)
    assert paths is not None
    _doc, version_dir, output_dir = paths
    assert version_dir.resolve().is_relative_to(v2.resolve())
    assert output_dir == version_dir / "03_analysis" / "runs" / "r1"


# ── Test 7: невалидный target / run_id → явная ошибка, без записи ────────────

def test_v2_primary_rejects_unresolvable(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    (v2 / "objects").mkdir(parents=True)
    # документа нет в v2, legacy не передан → None
    assert wiring.resolve_v2_job_paths("NOPE", "v001", run_id="r1", v2_root=v2) is None
    _make_v2_doc(v2, "OBJ_FOLDER", "KJ", "DOC-6B")
    # пустой / `..` run_id → None
    assert wiring.resolve_v2_job_paths("DOC-6B", "v001", run_id="", v2_root=v2) is None
    assert wiring.resolve_v2_job_paths("DOC-6B", "v001", run_id="..", v2_root=v2) is None
    # path-traversal в run_id НЕЙТРАЛИЗУЕТСЯ basename → путь остаётся под version_dir
    p = wiring.resolve_v2_job_paths("DOC-6B", "v001", run_id="../escape", v2_root=v2)
    assert p is not None
    _doc, _vd, out = p
    assert out.name == "escape"  # traversal снят
    assert out.resolve().is_relative_to(v2.resolve())  # не вышли за v2-root

    # manager-уровень: unresolvable → RuntimeError (не молчаливый legacy)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))
    import backend.app.pipeline.manager as mgr
    monkeypatch.setattr(mgr, "resolve_project_dir",
                        lambda pid, **kw: (_ for _ in ()).throw(FileNotFoundError()))
    with pytest.raises(RuntimeError):
        _resolve_via_manager(_job(project_id="NOPE"))


# ── Test 7b: версия отсутствует в v2-документе → None (drift, без фабрикации) ─

def test_v2_primary_missing_version_returns_none(tmp_path):
    v2 = tmp_path / "projects_v2"
    _make_v2_doc(v2, "OBJ_FOLDER", "KJ", "DOC-DRIFT", versions=("v001",))
    # запрошена v002, которой нет в v2-документе → None (а не versions/v002 путь)
    assert wiring.resolve_v2_job_paths("DOC-DRIFT", "v002", run_id="r1", v2_root=v2) is None
    # v001 существует → резолвится
    assert wiring.resolve_v2_job_paths("DOC-DRIFT", "v001", run_id="r1", v2_root=v2) is not None


def test_v2_primary_legacy_format_vid_normalizes(tmp_path):
    v2 = tmp_path / "projects_v2"
    _make_v2_doc(v2, "OBJ_FOLDER", "KJ", "DOC-NORM", versions=("v001", "v002"))
    # legacy-формат 'v2' нормализуется в v002 и матчится
    p = wiring.resolve_v2_job_paths("DOC-NORM", "v2", run_id="r1", v2_root=v2)
    assert p is not None
    _doc, version_dir, _out = p
    assert version_dir.name == "v002"


# ── Test 7c: object_id скоупит резолв при общем document_code ────────────────

def test_v2_primary_object_id_scopes_resolution(tmp_path):
    v2 = tmp_path / "projects_v2"
    # два объекта с ОДНИМ document_code
    _make_v2_doc(v2, "OBJ_A", "KJ", "SHARED", object_id="aaaa1111")
    _make_v2_doc(v2, "OBJ_B", "KJ", "SHARED", object_id="bbbb2222")
    ta = wiring.resolve_v2_target_by_id("SHARED", "v001", v2_root=v2, object_id="bbbb2222")
    assert ta is not None and ta.object_folder == "OBJ_B"
    tb = wiring.resolve_v2_target_by_id("SHARED", "v001", v2_root=v2, object_id="aaaa1111")
    assert tb is not None and tb.object_folder == "OBJ_A"


# ── Test 8: нет записи вне tmp_path ─────────────────────────────────────────

def test_v2_primary_no_write_outside_tmp(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    _make_v2_doc(v2, "OBJ_FOLDER", "KJ", "DOC-6B")
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))

    paths = wiring.resolve_v2_job_paths("DOC-6B", "v001", run_id="r1", v2_root=v2)
    assert paths is not None
    for p in paths:
        assert Path(p).resolve().is_relative_to(tmp_path.resolve())
