"""
test_clean_projects_v2.py
-------------------------
Регрессия: кнопка «Очистить» (clean_project_data) должна чистить projects_v2,
когда UI читает статус из v2 (AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=true).

Покрытие (по спецификации задачи):
  1. flag off → v2 не трогается (legacy-only поведение сохранено).
  2. v2-read on → pipeline status (99_service/pipeline_log.json) удаляется.
  3. v2-read on → generated артефакты удалены, source/metadata сохранены.
  4. clean не трогает соседнюю версию.
  5. malformed project_id → safe (никакого удаления, без исключения).
  6. production-like end-to-end → clean НЕ no-op, v2-состояние изменилось.

Изоляция: синтетический projects_v2 в tmp, config.DATA_DIR монкипатчится,
никаких живых данных.

Run: python -m pytest tests/test_clean_projects_v2.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.core.config as config  # noqa: E402
import backend.app.services.common.project_service as ps  # noqa: E402


# ─── helpers ──────────────────────────────────────────────────────────────────


def _make_v2_doc(v2root: Path, *, object_folder="OBJ_F", discipline="SS",
                 code="DOC1", object_id="OBJ1",
                 versions=("v001",), current="v001") -> Path:
    """Построить минимальный валидный v2-документ с заданными версиями.

    Каждая версия содержит: 01_input (source), version.json (metadata),
    02_work/03_analysis/04_review/05_export/99_service (generated).
    """
    docdir = (v2root / "objects" / object_folder / "disciplines" / discipline
              / "documents" / code)
    docdir.mkdir(parents=True)
    (docdir / "object.json").parent  # no-op
    obj_dir = v2root / "objects" / object_folder
    (obj_dir / "object.json").write_text(
        json.dumps({"object_id": object_id, "display_name": "Obj"}), encoding="utf-8")
    (docdir / "document.json").write_text(json.dumps({
        "object_id": object_id, "document_code": code, "current_version": current,
        "versions": [{"version_id": v} for v in versions],
    }), encoding="utf-8")
    (docdir / "current_version.txt").write_text(current, encoding="utf-8")
    for v in versions:
        vd = docdir / "versions" / v
        for sub in ("01_input", "02_work", "03_analysis", "04_review", "05_export", "99_service"):
            (vd / sub).mkdir(parents=True)
        (vd / "version.json").write_text(json.dumps({"version_id": v}), encoding="utf-8")
        (vd / "01_input" / "document.pdf").write_text("PDFDATA", encoding="utf-8")
        (vd / "01_input" / f"{code}_document.md").write_text("MD", encoding="utf-8")
        (vd / "99_service" / "pipeline_log.json").write_text(json.dumps({
            "stages": {"gemma_enrichment": {"status": "done"},
                       "text_analysis": {"status": "error", "error": "Exit code -9"}}
        }), encoding="utf-8")
        (vd / "99_service" / "gemma_enrichment_summary.json").write_text("{}", encoding="utf-8")
        (vd / "02_work" / "blocks_gemma_100").mkdir()
        (vd / "02_work" / "blocks_gemma_100" / "index.json").write_text("{}", encoding="utf-8")
        (vd / "03_analysis" / "03_findings.json").write_text("{}", encoding="utf-8")
    return docdir


@pytest.fixture
def v2env(tmp_path, monkeypatch):
    """projects_v2 в tmp + read-default ON. Возвращает (v2root, builder)."""
    v2root = tmp_path / "projects_v2"
    (v2root / "objects").mkdir(parents=True)
    monkeypatch.setattr(config, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
    return v2root


def _vdir(docdir: Path, vid="v001") -> Path:
    return docdir / "versions" / vid


# ─── 1. flag off → v2 не трогается ──────────────────────────────────────────


def test_flag_off_v2_untouched(tmp_path, monkeypatch):
    v2root = tmp_path / "projects_v2"
    (v2root / "objects").mkdir(parents=True)
    monkeypatch.setattr(config, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "false")
    docdir = _make_v2_doc(v2root)

    out = ps._clean_projects_v2_artifacts("DOC1", "v1", object_id="OBJ1")

    assert out["v2_attempted"] is False, "при flag off v2-arm не активируется"
    assert out["v2_cleaned"] is False
    # generated артефакты на месте
    assert (_vdir(docdir) / "99_service" / "pipeline_log.json").exists()
    assert (_vdir(docdir) / "02_work" / "blocks_gemma_100").exists()


# ─── 2. v2-read on → pipeline status удаляется ──────────────────────────────


def test_v2_clean_removes_pipeline_status(v2env):
    docdir = _make_v2_doc(v2env)
    pl = _vdir(docdir) / "99_service" / "pipeline_log.json"
    assert pl.exists()

    out = ps._clean_projects_v2_artifacts("DOC1", "v1", object_id="OBJ1")

    assert out["v2_attempted"] is True
    assert out["v2_cleaned"] is True
    assert "99_service" in out["v2_removed"]
    assert not pl.exists(), "pipeline_log.json (источник статуса UI) должен исчезнуть"


# ─── 3. generated удалены, source/metadata сохранены ────────────────────────


def test_v2_clean_removes_generated_preserves_source(v2env):
    docdir = _make_v2_doc(v2env)
    vd = _vdir(docdir)

    out = ps._clean_projects_v2_artifacts("DOC1", "v1", object_id="OBJ1")

    assert out["v2_cleaned"] is True
    # generated — удалены
    for name in ("02_work", "03_analysis", "04_review", "05_export", "99_service"):
        assert not (vd / name).exists(), f"{name} должен быть удалён"
        assert name in out["v2_removed"]
    # source + metadata — сохранены
    assert (vd / "01_input" / "document.pdf").exists(), "source PDF сохранён"
    assert (vd / "01_input" / "DOC1_document.md").exists(), "source MD сохранён"
    assert (vd / "version.json").exists(), "version metadata сохранён"
    assert (docdir / "document.json").exists(), "doc metadata сохранён"
    assert (docdir / "current_version.txt").exists(), "current_version сохранён"
    assert (docdir / "versions").is_dir(), "структура versions сохранена"


def test_v2_clean_backup_created_reversible(v2env):
    docdir = _make_v2_doc(v2env)
    out = ps._clean_projects_v2_artifacts("DOC1", "v1", object_id="OBJ1")
    # backup существует и содержит перемещённые артефакты (обратимо)
    assert out["v2_backup"]
    backup = Path(out["v2_backup"])
    assert backup.is_dir()
    assert (backup / "99_service" / "pipeline_log.json").exists(), "артефакты сохранены в backup"
    # backup лежит в projects_v2/_system (не в дереве objects)
    assert "_system" in str(backup) and "clean_backups" in str(backup)


# ─── 4. соседняя версия не тронута ──────────────────────────────────────────


def test_v2_clean_preserves_sibling_version(v2env):
    docdir = _make_v2_doc(v2env, versions=("v001", "v002"), current="v002")
    v1, v2 = _vdir(docdir, "v001"), _vdir(docdir, "v002")

    # чистим v1 (legacy v1 → v001)
    out = ps._clean_projects_v2_artifacts("DOC1", "v1", object_id="OBJ1")

    assert out["v2_version_id"] == "v001"
    assert out["v2_cleaned"] is True
    # v001 — generated удалены
    assert not (v1 / "99_service").exists()
    assert not (v1 / "03_analysis").exists()
    # v002 — ПОЛНОСТЬЮ нетронута
    assert (v2 / "99_service" / "pipeline_log.json").exists()
    assert (v2 / "02_work" / "blocks_gemma_100").exists()
    assert (v2 / "03_analysis" / "03_findings.json").exists()
    assert (v2 / "01_input" / "document.pdf").exists()


# ─── 5. malformed project_id → safe no-op ───────────────────────────────────


def test_v2_clean_malformed_project_id_safe(v2env):
    docdir = _make_v2_doc(v2env)
    sentinel = docdir / "versions" / "v001" / "99_service" / "pipeline_log.json"
    assert sentinel.exists()

    # path-traversal в project_id → basename нейтрализует, документ не найден
    out = ps._clean_projects_v2_artifacts("../../../../etc/passwd", "v1", object_id="OBJ1")

    assert out["v2_cleaned"] is False
    assert out["v2_removed"] == []
    assert any("не найден" in w for w in out["warnings"])
    # ничего реального не удалено
    assert sentinel.exists()


def test_v2_clean_unknown_document_warns(v2env):
    _make_v2_doc(v2env)
    out = ps._clean_projects_v2_artifacts("NOPE-DOC", "v1", object_id="OBJ1")
    assert out["v2_attempted"] is True
    assert out["v2_cleaned"] is False
    assert any("не найден" in w for w in out["warnings"])


# ─── 6. production-like end-to-end через clean_project_data ──────────────────


@pytest.fixture
def legacy_project(tmp_path, monkeypatch):
    """Минимальный legacy-проект DOC1 с (пустым) _output + изоляция projects_dir."""
    projects = tmp_path / "projects"
    pdir = projects / "DOC1"
    (pdir / "_output").mkdir(parents=True)
    (pdir / "project_info.json").write_text(json.dumps({
        "project_id": "DOC1", "name": "DOC1", "section": "SS", "pdf_file": "document.pdf",
    }), encoding="utf-8")
    (pdir / "document.pdf").write_bytes(b"%PDF-1.4 fake")
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})
    return projects, pdir


def test_clean_project_data_end_to_end_v2_not_noop(v2env, legacy_project):
    """Production-like: read-default ON, legacy _output пустой, v2 содержит
    pipeline_log → clean_project_data НЕ no-op, чистит v2."""
    _projects, pdir = legacy_project
    docdir = _make_v2_doc(v2env, code="DOC1", discipline="SS")
    pl = _vdir(docdir) / "99_service" / "pipeline_log.json"
    assert pl.exists()

    result = ps.clean_project_data("DOC1", version_id="v1")

    assert result["legacy_cleaned"] is True
    assert result["v2_attempted"] is True
    assert result["v2_cleaned"] is True
    assert "99_service" in result.get("v2_removed", [])
    assert not pl.exists(), "v2 pipeline status удалён → UI больше не покажет старый статус"
    # legacy _output пересоздан пустым
    assert (pdir / "_output").is_dir()
    assert not any((pdir / "_output").iterdir())
    # source сохранён в legacy и v2
    assert (pdir / "document.pdf").exists()
    assert (_vdir(docdir) / "01_input" / "document.pdf").exists()


def test_clean_project_data_legacy_only_when_flag_off(tmp_path, monkeypatch, legacy_project):
    """Flag off → старое поведение: legacy чистится, v2 не трогается."""
    _projects, pdir = legacy_project
    # legacy _output с артефактом
    (pdir / "_output" / "03_findings.json").write_text("{}", encoding="utf-8")
    v2root = tmp_path / "projects_v2"
    (v2root / "objects").mkdir(parents=True)
    monkeypatch.setattr(config, "DATA_DIR", tmp_path, raising=False)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "false")
    docdir = _make_v2_doc(v2root, code="DOC1", discipline="SS")
    pl = _vdir(docdir) / "99_service" / "pipeline_log.json"

    result = ps.clean_project_data("DOC1", version_id="v1")

    assert result["legacy_cleaned"] is True
    assert result["v2_attempted"] is False
    assert result["v2_cleaned"] is False
    # legacy очищен (findings удалён, _output пересоздан пустым)
    assert (pdir / "_output").is_dir()
    assert not any((pdir / "_output").iterdir())
    # v2 НЕ тронут
    assert pl.exists()
