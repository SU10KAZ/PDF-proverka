"""
test_backend_main_v2_audit_md_resolution.py
-------------------------------------------
Регресс-тесты для bug найденного 2026-05-14 в pre-cutover smoke:
backend.app.pipeline.manager._run_smart_pipeline / _run_ocr_pipeline /
_run_resumed_pipeline / _run_tile_audit / _run_main_audit раньше использовали
`resolve_project_dir(pid)` без `version_id`, поэтому MD-check для V2 audit
смотрел в V1 root и падал «MD-файл не найден» даже когда MD загружен в V2.

Эти тесты фиксируют контракт нового helper'а `_resolve_job_paths`:
- V1 (или version_id=None) → root project_dir.
- V2+ → _versions/v{N}/.
- неизвестная версия → fallback root (legacy V1).

Также проверяем, что:
- find_project_markdown по V2 проекту находит MD из _versions/v2/.
- V1 поведение остаётся прежним (regression V1).

Run:
    python -m pytest tests/test_backend_main_v2_audit_md_resolution.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─── Fixtures ───────────────────────────────────────────────────────────


def _write_pdf(p: Path) -> None:
    p.write_bytes(b"%PDF-1.4\nfake\n%%EOF\n")


def _write_md(p: Path, body: str = "## STR 1\n\n[TEXT b1]\n\nMD body.\n") -> None:
    p.write_text(body, encoding="utf-8")


@pytest.fixture
def v1_v2_project(tmp_path, monkeypatch):
    """V1 root с PDF + V1_document.md, V2 с PDF + V2_document.md."""
    projects_dir = tmp_path / "projects"
    projects_dir.mkdir()
    pdir = projects_dir / "M31A"
    (pdir / "_output").mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({
            "project_id": "M31A", "name": "M31A", "section": "EOM",
            "pdf_file": "doc.pdf", "md_file": "v1_document.md",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_pdf(pdir / "doc.pdf")
    _write_md(pdir / "v1_document.md", "V1 MD content")

    # Подменяем backend project_service projects_dir → tmp_path.
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects_dir)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})

    # Создаём V2 через тот же version_service, что использует production.
    from backend.app.services.common import version_service
    version_service.create_next_version(
        pdir, "M31A", source="manual", status="new", comment="V2",
    )
    v2_dir = pdir / "_versions" / "v2"
    _write_pdf(v2_dir / "v2.pdf")
    _write_md(v2_dir / "v2_document.md", "V2 MD content")
    # Update V2 project_info.json md_file
    v2_info_path = v2_dir / "project_info.json"
    v2_info = json.loads(v2_info_path.read_text(encoding="utf-8"))
    v2_info["md_file"] = "v2_document.md"
    v2_info["pdf_file"] = "v2.pdf"
    v2_info_path.write_text(json.dumps(v2_info, ensure_ascii=False), encoding="utf-8")

    return projects_dir, pdir, v2_dir


def _make_job(version_id):
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    return AuditJob(
        job_id="test-job",
        project_id="M31A",
        version_id=version_id,
        stage=AuditStage.PREPARE,
        status=JobStatus.RUNNING,
    )


# ─── 1. _resolve_job_paths контракт ─────────────────────────────────────


def test_resolve_job_paths_v1_returns_root(v1_v2_project):
    projects_dir, pdir, _v2_dir = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    root, version_dir, output_dir = pm._resolve_job_paths(_make_job("v1"))
    assert root == pdir
    assert version_dir == pdir  # V1 = root
    assert output_dir == pdir / "_output"


def test_resolve_job_paths_v2_returns_versions_subdir(v1_v2_project):
    projects_dir, pdir, v2_dir = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    root, version_dir, output_dir = pm._resolve_job_paths(_make_job("v2"))
    assert root == pdir
    assert version_dir == v2_dir
    assert output_dir == v2_dir / "_output"


def test_resolve_job_paths_none_version_resolves_to_latest(v1_v2_project):
    """Без явного version_id — резолв на latest version (контракт version_service).
    Запуск jobs без version_id должны быть редки: API endpoints validate version_id
    и проставляют его перед enqueue. Но если уж дошло до job без vid, версионность
    должна быть detrministic."""
    projects_dir, pdir, v2_dir = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    root, version_dir, output_dir = pm._resolve_job_paths(_make_job(None))
    assert root == pdir
    # Latest = V2 (создана в fixture).
    assert version_dir == v2_dir
    assert output_dir == v2_dir / "_output"


def test_resolve_job_paths_unknown_version_falls_back_to_root(v1_v2_project):
    """Если version_id невалидный (не в manifest) — fallback на root (legacy)."""
    projects_dir, pdir, _ = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    root, version_dir, output_dir = pm._resolve_job_paths(_make_job("v999"))
    assert version_dir == pdir
    assert output_dir == pdir / "_output"


# ─── 2. find_project_markdown работает с version-aware project_dir ──────


def test_find_project_markdown_picks_v2_md_for_v2_job(v1_v2_project):
    """Bug 2026-05-14: V2 audit падал на MD-check, потому что искал MD в V1.
    После fix: с version-aware project_dir функция находит V2 MD."""
    projects_dir, pdir, v2_dir = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import find_project_markdown

    pm = PipelineManager()
    _root, project_dir, _output = pm._resolve_job_paths(_make_job("v2"))
    project_info = json.loads((project_dir / "project_info.json").read_text(encoding="utf-8"))
    md = find_project_markdown(project_dir, project_info)
    assert md is not None, "V2 MD должен быть найден через version-aware project_dir"
    assert md.parent == v2_dir
    # Содержимое — именно V2-овое, не V1
    assert "V2 MD content" in md.read_text(encoding="utf-8")


def test_find_project_markdown_picks_v1_md_for_v1_job(v1_v2_project):
    """Regression V1: V1 MD остаётся доступен из V1 root через legacy путь."""
    _, pdir, _ = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import find_project_markdown

    pm = PipelineManager()
    _root, project_dir, _output = pm._resolve_job_paths(_make_job("v1"))
    project_info = json.loads((project_dir / "project_info.json").read_text(encoding="utf-8"))
    md = find_project_markdown(project_dir, project_info)
    assert md is not None
    assert md.parent == pdir
    assert "V1 MD content" in md.read_text(encoding="utf-8")


# ─── 3. V2 без MD по-прежнему даёт понятную ошибку ─────────────────────


def test_v2_without_md_returns_none(v1_v2_project):
    """Если в V2 нет ни одного *_document.md и project_info.md_file не указывает
    на существующий файл — find_project_markdown возвращает None (caller бросит
    «MD не найден»). V1 fallback не работает — V2 изолирована."""
    projects_dir, pdir, v2_dir = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import find_project_markdown

    # Удаляем MD из V2 и обнуляем project_info.md_file.
    (v2_dir / "v2_document.md").unlink()
    info_path = v2_dir / "project_info.json"
    info = json.loads(info_path.read_text(encoding="utf-8"))
    info["md_file"] = ""
    info_path.write_text(json.dumps(info, ensure_ascii=False), encoding="utf-8")

    pm = PipelineManager()
    _root, project_dir, _output = pm._resolve_job_paths(_make_job("v2"))
    project_info = json.loads((project_dir / "project_info.json").read_text(encoding="utf-8"))
    md = find_project_markdown(project_dir, project_info)
    assert md is None, "V2 без MD не должен fallback'нуться на V1 MD"


# ─── 4. _make_stage_context (regression — ctx.project_dir теперь V2-aware) ──


def test_make_stage_context_v2_project_dir_is_v2(v1_v2_project):
    projects_dir, pdir, v2_dir = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    ctx = pm._make_stage_context(_make_job("v2"))
    assert ctx.project_dir == v2_dir
    assert ctx.output_dir == v2_dir / "_output"
    assert ctx.version_id == "v2"


def test_make_stage_context_v1_project_dir_is_root(v1_v2_project):
    """Regression V1: ctx.project_dir для V1 = root, как раньше."""
    _, pdir, _ = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    ctx = pm._make_stage_context(_make_job("v1"))
    assert ctx.project_dir == pdir
    assert ctx.output_dir == pdir / "_output"


# ─── 5. Grep-regression: критичные _run_* сайты больше не используют
#         resolve_project_dir(pid) для MD-check ─────────────────────────


def test_md_check_sites_use_helper_not_raw_resolve():
    """Защита от регрессии: ровно те места, где раньше был V2 leak,
    больше не должны вызывать resolve_project_dir(pid) в обход
    _resolve_job_paths."""
    manager_path = _ROOT / "backend" / "app" / "pipeline" / "manager.py"
    src = manager_path.read_text(encoding="utf-8")

    # Каждый из этих фрагментов раньше указывал на bug. Теперь их быть не должно.
    bad_patterns = [
        # _run_smart_pipeline старый MD-check
        'project_dir = resolve_project_dir(pid)\n            md_candidates',
        # _run_ocr_pipeline старый MD-check
        'project_dir = resolve_project_dir(pid)\n            if find_project_markdown',
    ]
    for pat in bad_patterns:
        assert pat not in src, f"V2-leak регрессия — найден старый паттерн:\n{pat}"
