"""Гейт «index.json есть, PNG нет» и fail-soft хука эвакуации.

Состояние «индекс на месте, картинок нет» достижимо и БЕЗ эвакуации: resume
засевает run-папку одним index.json. Раньше все проверки готовности смотрели
только на существование индекса и рапортовали «кропы готовы», после чего анализ
блоков шёл вслепую и возвращал `PNG missing` по каждому блоку — при этом прогон
выглядел успешным. Эти тесты фиксируют, что так больше не бывает.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from backend.app.pipeline.stages.block_context.contract import crops_materialized


def _make_blocks_dir(tmp_path, *, with_pngs=True, n=3):
    bd = tmp_path / "blocks_stage02_100"
    bd.mkdir(parents=True)
    entries = []
    for i in range(n):
        name = f"block_b{i}.png"
        if with_pngs:
            (bd / name).write_bytes(b"\x89PNG\r\n\x1a\n" + b"x" * 4096)
        entries.append({"block_id": f"b{i}", "file": name, "page": 1})
    (bd / "index.json").write_text(
        json.dumps(
            {
                "total_blocks": n,
                "profile": "stage02_100",
                "dpi": 100,
                "min_long_side": 800,
                "compact": False,
                "skip_small": False,
                "blocks": entries,
            }
        ),
        encoding="utf-8",
    )
    return bd


def test_index_present_but_pngs_gone_is_not_ready(tmp_path):
    bd = _make_blocks_dir(tmp_path, with_pngs=False)
    ok, missing = crops_materialized(bd)
    assert ok is False
    assert sorted(missing) == ["b0", "b1", "b2"]


def test_all_present_is_ready(tmp_path):
    bd = _make_blocks_dir(tmp_path, with_pngs=True)
    assert crops_materialized(bd) == (True, [])


def test_missing_blocks_dir_reports_everything_missing(tmp_path):
    bd = _make_blocks_dir(tmp_path, with_pngs=True)
    index = json.loads((bd / "index.json").read_text(encoding="utf-8"))
    for p in bd.glob("block_*.png"):
        p.unlink()
    bd.rename(tmp_path / "moved_away")
    (tmp_path / "blocks_stage02_100").mkdir()
    (tmp_path / "blocks_stage02_100" / "index.json").write_text(json.dumps(index))
    ok, missing = crops_materialized(tmp_path / "blocks_stage02_100")
    assert ok is False and len(missing) == 3


def test_no_index_is_not_our_case(tmp_path):
    """Индекса нет — решает вызывающий (это ветка «кроп ещё не делался»)."""
    empty = tmp_path / "blocks_stage02_100"
    empty.mkdir()
    assert crops_materialized(empty) == (True, [])


def test_resume_detector_requires_materialized_crops(tmp_path, monkeypatch):
    """resume не должен считать блоки готовыми по одному index.json."""
    from backend.app.pipeline import resume_detector

    bd = _make_blocks_dir(tmp_path, with_pngs=False)
    assert (bd / "index.json").is_file()
    ok, missing = resume_detector.crops_materialized(bd)
    assert ok is False and missing


def test_resolved_blocks_dirs_mirrors_reader_fallthrough(tmp_path):
    """Защита обязана повторять ПОВЕДЕНИЕ читателя, а не только его «лидера».

    Регресс боевого прогона 2026-08-03: `blocks_dir()` идёт по прогонам по
    порядку и берёт ПЕРВЫЙ с index.json, поэтому прогон-лидер без индекса
    (например, run_refresh_*, где кроп ещё не делался) заставляет читателя
    провалиться на следующий. `resolved_blocks_dirs()` проверял только двух
    лидеров, возвращал ПУСТОЙ набор — и живой путь чтения уезжал под эвакуацию.
    """
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    doc_dir = tmp_path / "documents" / "DOC"
    version_id = "v001"
    runs = doc_dir / "versions" / version_id / "03_analysis" / "runs"

    # Лидер по mtime и по имени — БЕЗ index.json.
    leader = runs / "run_zzz_newer"
    (leader / "blocks_stage02_100").mkdir(parents=True)

    # Более старый прогон — С индексом: именно его вернёт читатель.
    older = runs / "run_aaa_older"
    bd = older / "blocks_stage02_100"
    bd.mkdir(parents=True)
    _make_index_only(bd)

    import os
    import time

    old_t = time.time() - 10_000
    os.utime(older, (old_t, old_t))

    adapter = ProjectsV2Adapter()
    reader_choice = adapter.blocks_dir(doc_dir, version_id)
    protected = adapter.resolved_blocks_dirs(doc_dir, version_id)

    assert reader_choice is not None
    assert reader_choice.resolve() == bd.resolve()
    assert bd.resolve() in protected, (
        "папка, которую реально читает blocks_dir(), обязана быть защищена"
    )


def _make_index_only(bd):
    (bd / "index.json").write_text(
        json.dumps(
            {
                "total_blocks": 1,
                "profile": "stage02_100",
                "dpi": 100,
                "min_long_side": 800,
                "compact": False,
                "skip_small": False,
                "blocks": [{"block_id": "b0", "file": "block_b0.png", "page": 1}],
            }
        ),
        encoding="utf-8",
    )


# ───────────────────────── хук эвакуации: fail-soft ───────────────────────


@pytest.mark.asyncio
async def test_eviction_hook_is_noop_when_flag_off(monkeypatch):
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.models.audit import JobStatus

    monkeypatch.setattr(
        "backend.app.core.config.BLOCK_CROP_EVICTION_ENABLED", False, raising=False
    )
    mgr = PipelineManager.__new__(PipelineManager)
    called = []
    job = SimpleNamespace(status=JobStatus.COMPLETED, project_id="p", version_id="v")
    mgr.is_running = lambda *_a, **_k: called.append("is_running") or False
    await PipelineManager._maybe_evict_block_crops(mgr, job, SimpleNamespace(items=[]))
    assert called == [], "при выключенном флаге хук не должен ничего делать"


@pytest.mark.asyncio
async def test_eviction_hook_skips_when_same_version_still_queued(monkeypatch):
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.models.audit import JobStatus

    monkeypatch.setattr(
        "backend.app.core.config.BLOCK_CROP_EVICTION_ENABLED", True, raising=False
    )
    mgr = PipelineManager.__new__(PipelineManager)
    mgr.is_running = lambda *_a, **_k: False
    resolved = []
    mgr._resolve_job_paths = lambda job: resolved.append(job) or (None, None, None)

    job = SimpleNamespace(status=JobStatus.COMPLETED, project_id="p", version_id="v")
    queue = SimpleNamespace(
        items=[SimpleNamespace(project_id="p", version_id="v", status="pending")]
    )
    await PipelineManager._maybe_evict_block_crops(mgr, job, queue)
    assert resolved == [], "при незавершённой работе по версии эвакуация невозможна"


@pytest.mark.asyncio
async def test_eviction_hook_never_raises(monkeypatch):
    """Любой сбой эвакуации не смеет ронять завершённый аудит."""
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.models.audit import JobStatus

    monkeypatch.setattr(
        "backend.app.core.config.BLOCK_CROP_EVICTION_ENABLED", True, raising=False
    )
    mgr = PipelineManager.__new__(PipelineManager)
    mgr.is_running = lambda *_a, **_k: False

    def _boom(_job):
        raise RuntimeError("резолв путей упал")

    mgr._resolve_job_paths = _boom
    logged = []

    async def _log(_job, msg, level="info"):
        logged.append((level, msg))

    mgr._log = _log

    job = SimpleNamespace(status=JobStatus.COMPLETED, project_id="p", version_id="v")
    await PipelineManager._maybe_evict_block_crops(mgr, job, SimpleNamespace(items=[]))
    assert job.status == JobStatus.COMPLETED
    assert logged and logged[-1][0] == "warn"
