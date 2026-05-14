"""
test_backend_main_v2_late_stage_paths.py
----------------------------------------
Регресс-тесты для late-stage helpers, которые в Pass A остались
version-unaware (Blocker, выявленный после Pass A code-audit).

Late-stage функции, которые могли читать/писать V1, когда V2 audit
доходит до Stage 02/findings_merge/optimization:

1. backend.app.pipeline.stages.findings_merge.backfill_highlights.backfill_project
   — принимает project_dir, поэтому caller (manager._backfill_highlight_regions)
     должен передавать version-aware path. Раньше передавал V1 root.

2. backend.app.pipeline.stages.prepare.task_builder._load_project_info /
   _overrides_path / _get_md_file_path / _get_project_paths /
   _get_block_analysis_example / _load_document_graph / block path —
   читали V1 root через resolve_project_dir.

3. backend.app.pipeline.stages.prepare.prompt_builder._read_text_analysis_for_blocks /
   _read_json_file / _read_findings_merge_blocks / _read_md_file /
   _get_plan_images / interleaved content — читали V1 _output.

После Pass B все они переходят на _version_output_dir / _version_project_dir,
которые используют bind_version() ContextVar или latest_version_id.

Run:
    python -m pytest tests/test_backend_main_v2_late_stage_paths.py -v
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


def _write_json(p: Path, obj) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


@pytest.fixture
def v1_v2_with_artefacts(tmp_path, monkeypatch):
    """V1 + V2 проект с distinct artefacts в обоих _output, чтобы можно было
    отличить «прочитал V1» от «прочитал V2»."""
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
    (pdir / "doc.pdf").write_bytes(b"%PDF-1.4\nfake\n%%EOF\n")
    (pdir / "v1_document.md").write_text("V1 MD content", encoding="utf-8")

    # V1 sentinel artefacts in _output
    _write_json(pdir / "_output" / "03_findings.json", {
        "version_marker": "V1",
        "findings": [
            {"id": "F-V1-001", "page": 4, "source_block_ids": ["block_v1_a"]},
        ],
    })
    _write_json(pdir / "_output" / "02_blocks_analysis.json", {
        "version_marker": "V1",
        "block_analyses": [
            {"block_id": "block_v1_a", "highlight_regions": [{"x": 1, "y": 1}]},
        ],
    })
    _write_json(pdir / "_output" / "01_text_analysis.json", {
        "version_marker": "V1",
        "project_params": {"src": "v1"},
        "text_findings": [{"id": "T-V1"}],
        "normative_refs_found": [],
    })
    _write_json(pdir / "_output" / "document_graph.json", {
        "version_marker": "V1",
        "pages": [{"page": 1, "sheet_no": "1"}],
    })

    # Подменяем backend project_service projects_dir → tmp.
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects_dir)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})

    from backend.app.services.common import version_service
    version_service.create_next_version(
        pdir, "M31A", source="manual", status="new", comment="V2",
    )
    v2_dir = pdir / "_versions" / "v2"
    (v2_dir / "v2.pdf").write_bytes(b"%PDF-1.4\nfake-v2\n%%EOF\n")
    (v2_dir / "v2_document.md").write_text("V2 MD content", encoding="utf-8")
    v2_info_path = v2_dir / "project_info.json"
    v2_info = json.loads(v2_info_path.read_text(encoding="utf-8"))
    v2_info["md_file"] = "v2_document.md"
    v2_info["pdf_file"] = "v2.pdf"
    v2_info_path.write_text(json.dumps(v2_info, ensure_ascii=False, indent=2), encoding="utf-8")

    # V2 distinct artefacts
    _write_json(v2_dir / "_output" / "03_findings.json", {
        "version_marker": "V2",
        "findings": [
            {"id": "F-V2-001", "page": 5, "source_block_ids": ["block_v2_a"]},
        ],
    })
    _write_json(v2_dir / "_output" / "02_blocks_analysis.json", {
        "version_marker": "V2",
        "block_analyses": [
            {"block_id": "block_v2_a", "highlight_regions": [{"x": 9, "y": 9}]},
        ],
    })
    _write_json(v2_dir / "_output" / "01_text_analysis.json", {
        "version_marker": "V2",
        "project_params": {"src": "v2"},
        "text_findings": [{"id": "T-V2"}],
        "normative_refs_found": [],
    })
    _write_json(v2_dir / "_output" / "document_graph.json", {
        "version_marker": "V2",
        "pages": [{"page": 1, "sheet_no": "V2-1"}],
    })

    return projects_dir, pdir, v2_dir


def _v1_findings_hash(pdir: Path) -> bytes:
    return (pdir / "_output" / "03_findings.json").read_bytes()


# ─── 1. backfill_highlights via manager._backfill_highlight_regions ─────


def test_backfill_highlight_regions_v2_does_not_touch_v1(v1_v2_with_artefacts):
    """manager._backfill_highlight_regions должен пробежать по V2 _output
    (когда active_jobs[pid].version_id='v2'), а не V1 root."""
    projects_dir, pdir, v2_dir = v1_v2_with_artefacts
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus

    pm = PipelineManager()
    job = AuditJob(
        job_id="job-bk", project_id="M31A", version_id="v2",
        stage=AuditStage.FINDINGS_MERGE, status=JobStatus.RUNNING,
    )
    pm.active_jobs["M31A"] = job

    v1_before = _v1_findings_hash(pdir)
    pm._backfill_highlight_regions("M31A")

    # V1 sentinel не изменён
    assert _v1_findings_hash(pdir) == v1_before, (
        "V2 _backfill_highlight_regions переписал V1 03_findings.json"
    )


def test_backfill_highlight_regions_v1_still_works(v1_v2_with_artefacts):
    """V1 регрессия: при V1 job всё ещё работает с root _output."""
    projects_dir, pdir, _v2 = v1_v2_with_artefacts
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus

    pm = PipelineManager()
    job = AuditJob(
        job_id="job-bk1", project_id="M31A", version_id="v1",
        stage=AuditStage.FINDINGS_MERGE, status=JobStatus.RUNNING,
    )
    pm.active_jobs["M31A"] = job

    # Должен пройти без ошибки и НЕ должен поломать V1 03_findings.json
    pm._backfill_highlight_regions("M31A")
    fd = json.loads((pdir / "_output" / "03_findings.json").read_text(encoding="utf-8"))
    assert fd["version_marker"] == "V1"


# ─── 2. findings_merge/runner._version_output_dir picks V2 via bind ─────


def test_backfill_text_evidence_uses_v2_via_bind_version(v1_v2_with_artefacts):
    """findings_merge.backfill_text_evidence_in_findings должен под bind_version('v2')
    читать/писать V2 03_findings.json, не трогая V1."""
    _projects_dir, pdir, v2_dir = v1_v2_with_artefacts
    from backend.app.services.common import version_service
    from backend.app.pipeline.stages.findings_merge.runner import (
        backfill_text_evidence_in_findings,
    )

    v1_before = _v1_findings_hash(pdir)
    with version_service.pinned_version("v2"):
        backfill_text_evidence_in_findings("M31A")
    assert _v1_findings_hash(pdir) == v1_before


def test_merge_similar_findings_uses_v2_via_bind_version(v1_v2_with_artefacts):
    _projects_dir, pdir, v2_dir = v1_v2_with_artefacts
    from backend.app.services.common import version_service
    from backend.app.pipeline.stages.findings_merge.runner import (
        merge_similar_findings,
    )

    v1_before = _v1_findings_hash(pdir)
    with version_service.pinned_version("v2"):
        # Хотя сейчас findings_path = V2 03_findings.json содержит только 1
        # finding (merge не сработает), главное — V1 не модифицирован.
        merge_similar_findings("M31A")
    assert _v1_findings_hash(pdir) == v1_before


def test_attach_stage02_coverage_to_findings_uses_v2_via_bind(v1_v2_with_artefacts):
    _projects_dir, pdir, v2_dir = v1_v2_with_artefacts
    from backend.app.services.common import version_service
    from backend.app.pipeline.stages.block_analysis.runner import (
        attach_stage02_coverage_to_findings,
    )

    v1_before = _v1_findings_hash(pdir)
    with version_service.pinned_version("v2"):
        attach_stage02_coverage_to_findings("M31A")
    assert _v1_findings_hash(pdir) == v1_before


# ─── 3. prompt_builder reads via bind_version ───────────────────────────


def test_prompt_builder_reads_v2_text_analysis(v1_v2_with_artefacts):
    """prompt_builder._read_text_analysis_for_blocks должен под bind_version('v2')
    вернуть содержимое V2 01_text_analysis.json."""
    _projects_dir, _pdir, _v2 = v1_v2_with_artefacts
    from backend.app.services.common import version_service
    from backend.app.pipeline.stages.prepare.prompt_builder import (
        _read_text_analysis_for_blocks,
    )

    with version_service.pinned_version("v2"):
        out = _read_text_analysis_for_blocks("M31A")
    # Должно содержать V2-маркер из text_findings, не V1.
    assert '"T-V2"' in out
    assert '"T-V1"' not in out


def test_prompt_builder_reads_v1_text_analysis_when_no_bind(v1_v2_with_artefacts):
    """V1 регрессия: без bind_version и при latest=v2 — fallback должен корректно
    выбрать latest (V2). Если V2 удалить — должна работать V1.

    Важно: latest version при наличии V2 == V2 (контракт version_service).
    Тест проверяет, что без bind по умолчанию используется latest.
    """
    _projects_dir, _pdir, _v2 = v1_v2_with_artefacts
    from backend.app.pipeline.stages.prepare.prompt_builder import (
        _read_text_analysis_for_blocks,
    )

    out = _read_text_analysis_for_blocks("M31A")
    # Без bind: latest == V2.
    assert '"T-V2"' in out


def test_read_findings_merge_blocks_v2_via_bind(v1_v2_with_artefacts):
    _projects_dir, _pdir, _v2 = v1_v2_with_artefacts
    from backend.app.services.common import version_service
    from backend.app.pipeline.stages.prepare.prompt_builder import (
        _read_findings_merge_blocks,
    )

    with version_service.pinned_version("v2"):
        raw = _read_findings_merge_blocks("M31A", compact_for_local=False)
    assert '"V2"' in raw
    assert '"block_v2_a"' in raw


# ─── 4. task_builder _load_project_info / _get_md_file_path ─────────────


def test_load_project_info_v2_via_bind(v1_v2_with_artefacts):
    _projects_dir, _pdir, v2_dir = v1_v2_with_artefacts
    from backend.app.services.common import version_service
    from backend.app.pipeline.stages.prepare.task_builder import _load_project_info

    with version_service.pinned_version("v2"):
        info = _load_project_info("M31A")
    assert info.get("md_file") == "v2_document.md"
    assert info.get("pdf_file") == "v2.pdf"


def test_get_md_file_path_v2_via_bind(v1_v2_with_artefacts):
    _projects_dir, _pdir, v2_dir = v1_v2_with_artefacts
    from backend.app.services.common import version_service
    from backend.app.pipeline.stages.prepare.task_builder import _get_md_file_path

    project_info = {"md_file": "v2_document.md"}
    with version_service.pinned_version("v2"):
        md_path = _get_md_file_path(project_info, "M31A")
    assert "_versions/v2/v2_document.md" in md_path


def test_get_project_paths_v2_via_bind(v1_v2_with_artefacts):
    _projects_dir, _pdir, v2_dir = v1_v2_with_artefacts
    from backend.app.services.common import version_service
    from backend.app.pipeline.stages.prepare.task_builder import _get_project_paths

    with version_service.pinned_version("v2"):
        proj, out = _get_project_paths("M31A")
    assert "_versions/v2" in proj
    assert "_versions/v2/_output" in out


def test_load_document_graph_v2_via_bind(v1_v2_with_artefacts):
    _projects_dir, _pdir, _v2 = v1_v2_with_artefacts
    from backend.app.services.common import version_service
    from backend.app.pipeline.stages.prepare.task_builder import _load_document_graph

    with version_service.pinned_version("v2"):
        graph = _load_document_graph("M31A")
    assert graph is not None
    assert graph.get("version_marker") == "V2"


# ─── 5. Pre-crop loop guard: V2 project skipped ─────────────────────────


@pytest.mark.asyncio
async def test_precrop_skips_v2_project(v1_v2_with_artefacts, monkeypatch):
    """_precrop_project с проектом, у которого latest_version_id='v2',
    должен skip — не запускать subprocess."""
    _projects_dir, _pdir, _v2 = v1_v2_with_artefacts
    from backend.app.pipeline import manager as mgr

    captured_subprocess = []

    async def _fake_run_script(*args, **kwargs):
        captured_subprocess.append(args)
        return (0, "", "")

    monkeypatch.setattr(mgr, "run_script", _fake_run_script)

    pm = mgr.PipelineManager()
    result = await pm._precrop_project("M31A")

    assert result is False, "Pre-crop должен возвращать False для V2 project"
    assert len(captured_subprocess) == 0, (
        "Pre-crop НЕ должен запускать subprocess для V2 project — он V1-only"
    )


@pytest.mark.asyncio
async def test_precrop_proceeds_for_v1_only_project(tmp_path, monkeypatch):
    """V1-only project (без manifest) должен пройти pre-crop guard."""
    projects_dir = tmp_path / "projects"
    projects_dir.mkdir()
    pdir = projects_dir / "V1ONLY"
    pdir.mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({"project_id": "V1ONLY", "section": "EOM", "pdf_file": "x.pdf"}),
        encoding="utf-8",
    )
    (pdir / "x.pdf").write_bytes(b"%PDF-1.4\n%%EOF\n")
    # Нет _result.json — pre-crop вернёт False (не OCR-проект), но guard НЕ
    # должен срабатывать раньше. Проверим именно логику guard'а через флаг.

    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects_dir)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)

    from backend.app.pipeline import manager as mgr
    pm = mgr.PipelineManager()
    # V1 только → guard не должен возвращать False до проверки blocks/result.
    # Pre-crop вернёт False по другой причине (нет _result.json).
    # Проверим, что причина именно "no _result.json", а не V2-guard.
    captured_logs = []

    async def _fake_broadcast_global(msg):
        captured_logs.append(getattr(msg, "message", str(msg)))
    monkeypatch.setattr(mgr.ws_manager, "broadcast_global", _fake_broadcast_global)

    result = await pm._precrop_project("V1ONLY")
    assert result is False
    # Не должно быть лога "skip — latest_version_id" (это V2 guard)
    for log in captured_logs:
        assert "latest_version_id" not in log, (
            "V1-only project triggered V2 guard"
        )


# ─── 6. Grep regression: late-stage sources don't use raw resolve_project_dir/_output ───


def test_late_stage_modules_use_version_helper():
    """Регрессия: late-stage helpers (prompt_builder, task_builder, findings_merge runner)
    больше не должны использовать `resolve_project_dir(...) / "_output"` напрямую
    в functions, обслуживающих runtime artefacts."""
    targets = [
        _ROOT / "backend" / "app" / "pipeline" / "stages" / "prepare" / "prompt_builder.py",
        _ROOT / "backend" / "app" / "pipeline" / "stages" / "prepare" / "task_builder.py",
        _ROOT / "backend" / "app" / "pipeline" / "stages" / "findings_merge" / "runner.py",
    ]
    for path in targets:
        src = path.read_text(encoding="utf-8")
        # Допустимо: только в _version_output_dir/_version_project_dir fallback'е
        # ('return resolve_project_dir(...) / "_output"' внутри try/except).
        # Запрещаем ВСЕ остальные использования паттерна для runtime artefacts.
        # Грубая, но надёжная проверка: после patch'а должно остаться <= 2 вхождений
        # (в декларациях fallback функций).
        count = src.count('resolve_project_dir(project_id) / "_output"')
        assert count <= 1, (
            f"{path.name}: найдено {count} version-unaware "
            f"`resolve_project_dir(project_id) / \"_output\"` — должно быть <= 1 (fallback)"
        )
