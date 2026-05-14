"""
test_backend_main_v2_subprocess_paths.py
----------------------------------------
Регресс-тесты для bug найденного 2026-05-14 в pre-cutover smoke (Pass A):
backend.app.pipeline.manager._run_smart_pipeline и другие job-aware стадии
вызывали subprocess scripts (process_project.py / blocks.py) с
`_project_path(pid)`, который version-unaware и всегда возвращал V1 root.
В результате V2 audit перезаписывал V1 `_output/`.

Эти тесты фиксируют контракт нового helper'а `_project_path_for_job(job)` и
обёртки `_run_script_for_job(job, ...)`:

- V1 job → subprocess получает path к root project_dir.
- V2 job → subprocess получает path к `_versions/v2/`.
- AUDIT_* env (PROJECT_ID/VERSION_ID/VERSION_DIR/OUTPUT_DIR) уезжает в env_overrides.
- V1 `_output/` не модифицируется при V2 mocked run (sentinel-проверка).

Также проверяем grep-регрессию: в manager.py больше не должно быть
паттерна `_run_script(... _project_path(pid) ...)` без _for_job-обёртки
в job-aware местах (исключение: pre-crop loop, явно V1-only).

Run:
    python -m pytest tests/test_backend_main_v2_subprocess_paths.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

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
    """V1 root + V2 version, оба с PDF/MD. Sentinel V1 _output/03_findings.json."""
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

    # V1 sentinel — критичный artefact, который НЕ должен мутировать при V2 audit.
    sentinel = pdir / "_output" / "03_findings.json"
    sentinel.write_text(
        json.dumps({"v1_marker": "must_not_be_overwritten", "findings": [{"id": "F-001"}]}, ensure_ascii=False),
        encoding="utf-8",
    )

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
    _write_pdf(v2_dir / "v2.pdf")
    _write_md(v2_dir / "v2_document.md", "V2 MD content")
    v2_info_path = v2_dir / "project_info.json"
    v2_info = json.loads(v2_info_path.read_text(encoding="utf-8"))
    v2_info["md_file"] = "v2_document.md"
    v2_info["pdf_file"] = "v2.pdf"
    v2_info_path.write_text(json.dumps(v2_info, ensure_ascii=False), encoding="utf-8")

    return projects_dir, pdir, v2_dir, sentinel


def _make_job(version_id):
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    return AuditJob(
        job_id="test-job",
        project_id="M31A",
        version_id=version_id,
        stage=AuditStage.PREPARE,
        status=JobStatus.RUNNING,
    )


# ─── 1. _project_path(pid, version_id) контракт ─────────────────────────


def test_project_path_v1_returns_root(v1_v2_project):
    projects_dir, pdir, _v2, _ = v1_v2_project
    from backend.app.pipeline.manager import _project_path

    p = _project_path("M31A", "v1")
    # V1 → root project_dir (legacy behaviour)
    assert Path(p).resolve() == pdir.resolve()


def test_project_path_v2_returns_versions_subdir(v1_v2_project):
    projects_dir, _pdir, v2_dir, _ = v1_v2_project
    from backend.app.pipeline.manager import _project_path

    p = _project_path("M31A", "v2")
    assert Path(p).resolve() == v2_dir.resolve()


def test_project_path_no_version_keeps_root(v1_v2_project):
    """Без version_id → старое V1 поведение (root). Long-running job-ы должны
    передавать version_id явно через _project_path_for_job(job)."""
    projects_dir, pdir, _v2, _ = v1_v2_project
    from backend.app.pipeline.manager import _project_path

    p = _project_path("M31A")
    assert Path(p).resolve() == pdir.resolve()


def test_project_path_unknown_version_falls_back_to_root(v1_v2_project):
    """Невалидный version_id → fallback root, не stack trace."""
    projects_dir, pdir, _v2, _ = v1_v2_project
    from backend.app.pipeline.manager import _project_path

    p = _project_path("M31A", "v999")
    assert Path(p).resolve() == pdir.resolve()


# ─── 2. _project_path_for_job контракт ──────────────────────────────────


def test_project_path_for_job_v1(v1_v2_project):
    projects_dir, pdir, _v2, _ = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    p = pm._project_path_for_job(_make_job("v1"))
    assert Path(p).resolve() == pdir.resolve()


def test_project_path_for_job_v2(v1_v2_project):
    projects_dir, _pdir, v2_dir, _ = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    p = pm._project_path_for_job(_make_job("v2"))
    assert Path(p).resolve() == v2_dir.resolve()


# ─── 3. _make_audit_env_for_job контракт ────────────────────────────────


def test_make_audit_env_v2(v1_v2_project):
    projects_dir, pdir, v2_dir, _ = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    env = pm._make_audit_env_for_job(_make_job("v2"))
    assert env["AUDIT_PROJECT_ID"] == "M31A"
    assert env["AUDIT_VERSION_ID"] == "v2"
    assert Path(env["AUDIT_VERSION_DIR"]).resolve() == v2_dir.resolve()
    assert Path(env["AUDIT_OUTPUT_DIR"]).resolve() == (v2_dir / "_output").resolve()


def test_make_audit_env_v1(v1_v2_project):
    """V1 регрессия: env должно быть консистентным (vid="v1", version_dir=root)."""
    projects_dir, pdir, _v2, _ = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    env = pm._make_audit_env_for_job(_make_job("v1"))
    assert env["AUDIT_PROJECT_ID"] == "M31A"
    assert env["AUDIT_VERSION_ID"] == "v1"
    assert Path(env["AUDIT_VERSION_DIR"]).resolve() == pdir.resolve()
    assert Path(env["AUDIT_OUTPUT_DIR"]).resolve() == (pdir / "_output").resolve()


def test_make_audit_env_none_version_id_fallback_v1(v1_v2_project):
    """job.version_id=None → AUDIT_VERSION_ID должен быть строкой 'v1'."""
    projects_dir, pdir, v2_dir, _ = v1_v2_project
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    env = pm._make_audit_env_for_job(_make_job(None))
    # None → "v1" в env (не пустая строка, не "None")
    assert env["AUDIT_VERSION_ID"] == "v1"
    # version_dir для None → latest (V2 в этом проекте)
    assert Path(env["AUDIT_VERSION_DIR"]).resolve() == v2_dir.resolve()


# ─── 4. _run_script_for_job передаёт env_overrides ──────────────────────


@pytest.mark.asyncio
async def test_run_script_for_job_v2_injects_env(v1_v2_project, monkeypatch):
    projects_dir, pdir, v2_dir, _ = v1_v2_project
    from backend.app.pipeline import manager as mgr

    captured = {}

    async def _fake_run_script(*args, **kwargs):
        captured["args"] = args
        captured["env_overrides"] = kwargs.get("env_overrides", {})
        return (0, "", "")

    # Подменяем именно run_script (то, что _run_script делегирует).
    monkeypatch.setattr(mgr, "run_script", _fake_run_script)

    pm = mgr.PipelineManager()
    job = _make_job("v2")
    await pm._run_script_for_job(job, "fake_script.py", ["arg1"])

    env = captured["env_overrides"]
    assert env["AUDIT_PROJECT_ID"] == "M31A"
    assert env["AUDIT_VERSION_ID"] == "v2"
    assert Path(env["AUDIT_VERSION_DIR"]).resolve() == v2_dir.resolve()
    assert Path(env["AUDIT_OUTPUT_DIR"]).resolve() == (v2_dir / "_output").resolve()


@pytest.mark.asyncio
async def test_run_script_for_job_v2_caller_overrides_win(v1_v2_project, monkeypatch):
    """Caller-supplied env_overrides побеждают AUDIT_* при коллизии."""
    projects_dir, _pdir, _v2, _ = v1_v2_project
    from backend.app.pipeline import manager as mgr

    captured = {}

    async def _fake_run_script(*args, **kwargs):
        captured["env_overrides"] = kwargs.get("env_overrides", {})
        return (0, "", "")

    monkeypatch.setattr(mgr, "run_script", _fake_run_script)

    pm = mgr.PipelineManager()
    await pm._run_script_for_job(
        _make_job("v2"),
        "fake_script.py",
        [],
        env_overrides={"AUDIT_VERSION_ID": "FORCED", "MY_FLAG": "1"},
    )
    assert captured["env_overrides"]["AUDIT_VERSION_ID"] == "FORCED"
    assert captured["env_overrides"]["MY_FLAG"] == "1"
    # AUDIT_PROJECT_ID не override'ился — остался от helper'а.
    assert captured["env_overrides"]["AUDIT_PROJECT_ID"] == "M31A"


# ─── 5. V1 _output sentinel: не модифицируется при mocked V2 run ────────


@pytest.mark.asyncio
async def test_v2_subprocess_does_not_touch_v1_output_sentinel(v1_v2_project, monkeypatch):
    """Mocked end-to-end: PipelineManager собирает argv для PROCESS_PROJECT_SCRIPT
    с V2 path, env с AUDIT_VERSION_DIR=V2; V1 sentinel _output/03_findings.json
    остаётся неизменным."""
    projects_dir, pdir, v2_dir, sentinel = v1_v2_project
    from backend.app.pipeline import manager as mgr

    sentinel_before = sentinel.read_bytes()
    captured_argv = []

    async def _fake_run_script(*args, **kwargs):
        # args[0] = script, args[1] = list[str] argv, args[2] = on_output
        captured_argv.append(list(args[1]) if len(args) >= 2 and isinstance(args[1], list) else [])
        return (0, "", "")

    monkeypatch.setattr(mgr, "run_script", _fake_run_script)

    pm = mgr.PipelineManager()
    job = _make_job("v2")
    # Имитируем именно тот вызов, который раньше разрушал V1.
    await pm._run_script_for_job(
        job,
        str(mgr.PROCESS_PROJECT_SCRIPT),
        [pm._project_path_for_job(job)],
    )

    # 1. Subprocess получил V2 path, не V1 root.
    assert len(captured_argv) == 1
    received_path = Path(captured_argv[0][0]).resolve()
    assert received_path == v2_dir.resolve(), (
        f"V2 audit передал V1 root в subprocess! Получено: {received_path}, "
        f"ожидалось: {v2_dir.resolve()}"
    )
    # 2. V1 sentinel не тронут.
    assert sentinel.read_bytes() == sentinel_before


# ─── 6. Grep-регрессия: критичные сайты используют _for_job ─────────────


def test_pipeline_manager_no_legacy_run_script_with_pid_in_job_aware_sites():
    """Регрессия: ровно те сайты, где раньше был V2-leak в subprocess argv,
    больше не должны вызывать _run_script(pid, ..., _project_path(pid)).
    Single исключение — pre-crop loop (явно V1-only с комментарием)."""
    src = (_ROOT / "backend" / "app" / "pipeline" / "manager.py").read_text(encoding="utf-8")

    # Эти паттерны раньше указывали на bug.
    bad_patterns = [
        # PROCESS_PROJECT_SCRIPT в smart pipeline с V1 root
        'str(PROCESS_PROJECT_SCRIPT),\n                [_project_path(pid)]',
        # BLOCKS_SCRIPT merge с V1 root
        '["merge", _project_path(pid)]',
        '["merge", _project_path(job.project_id)]',
    ]
    for pat in bad_patterns:
        assert pat not in src, (
            f"V2-leak регрессия — найден старый паттерн в manager.py:\n{pat}"
        )

    # Сайты должны теперь вызывать _run_script_for_job с _project_path_for_job.
    # Минимум 5 вхождений _run_script_for_job ожидаем.
    assert src.count("_run_script_for_job(") >= 5, (
        "Ожидаем минимум 5 вызовов _run_script_for_job в job-aware местах"
    )
    assert src.count("_project_path_for_job(job)") >= 5, (
        "Ожидаем минимум 5 вызовов _project_path_for_job(job) в job-aware местах"
    )
