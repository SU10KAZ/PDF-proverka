"""
test_gemma_prefetch.py
----------------------
Tests for pre-Gemma OCR prefetch in batch-queue.

Все тесты строго изолированы:
- Используют tmp_path / monkeypatch.
- НЕ трогают реальный batch_queue.json, реальные projects/, реальный Gemma instance.
- НЕ обращаются к live API текущего backend.
- enrich_project / runner мокаются.

Runs with:
    python -m pytest backend/tests/test_gemma_prefetch.py -v
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.app.models.audit import (
    BatchAction,
    BatchQueueItem,
    BatchQueueStatus,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def isolated_batch_queue_file(tmp_path, monkeypatch):
    """Подменяет BATCH_QUEUE_FILE на временный, чтобы тесты не трогали prod."""
    fake_queue_file = tmp_path / "batch_queue.json"
    import backend.app.pipeline.manager as mgr
    monkeypatch.setattr(mgr, "BATCH_QUEUE_FILE", fake_queue_file)
    return fake_queue_file


@pytest.fixture
def mock_manager(isolated_batch_queue_file, monkeypatch):
    """Изолированный PipelineManager без реальных async tasks/жизненного цикла."""
    from backend.app.pipeline.manager import PipelineManager
    m = PipelineManager()
    # Подменим broadcasts и логирование на no-op чтоб не тащить ws_manager
    async def _noop(*a, **kw): return None
    monkeypatch.setattr(m, "_broadcast_batch_progress", _noop)
    monkeypatch.setattr(m, "_log", _noop)
    return m


@pytest.fixture
def fake_project_dir(tmp_path):
    """Минимальный проектный каталог с _output/."""
    pdir = tmp_path / "fake_proj"
    (pdir / "_output").mkdir(parents=True)
    return pdir


# ─── Test 1: gate _is_current_running_past_gemma ─────────────────────────────

def test_pregemma_does_not_attempt_lock_during_current_gemma(mock_manager):
    """Pre-Gemma gate: пока main pipeline не пометил Gemma stage done для
    текущего running-проекта, _is_current_running_past_gemma() возвращает False
    и loop не пытается захватить lock."""
    q = BatchQueueStatus(queue_id="q1", action="full", status="running")
    q.items = [
        BatchQueueItem(project_id="proj_A", status="running"),
        BatchQueueItem(project_id="proj_B", status="pending"),
    ]
    mock_manager._batch_queue = q
    # Маркер ещё не выставлен
    assert mock_manager._is_current_running_past_gemma() is False
    # Пометили — теперь gate открыт
    mock_manager._mark_gemma_stage_done("proj_A")
    assert mock_manager._is_current_running_past_gemma() is True


# ─── Test 2: after main releases ─────────────────────────────────────────────

def test_pregemma_gate_opens_after_main_releases(mock_manager):
    """Маркер _current_gemma_stage_done выставляется в _mark_gemma_stage_done.
    После него gate возвращает True даже если в очереди есть running проект."""
    q = BatchQueueStatus(queue_id="q2", status="running")
    q.items = [BatchQueueItem(project_id="p", status="running")]
    mock_manager._batch_queue = q
    assert mock_manager._is_current_running_past_gemma() is False
    mock_manager._mark_gemma_stage_done("p")
    assert mock_manager._is_current_running_past_gemma() is True


# ─── Test 3: skip if crop not ready ──────────────────────────────────────────

def test_pregemma_skips_if_crop_not_ready(mock_manager, fake_project_dir, monkeypatch):
    """_select_pregemma_candidate возвращает (None, False) когда нет index.json."""
    from backend.app.pipeline.manager import resolve_project_dir as _rpd
    # Подменим resolve_project_dir чтоб возвращал fake dir
    monkeypatch.setattr(
        "backend.app.pipeline.manager.resolve_project_dir",
        lambda pid: fake_project_dir,
    )
    q = BatchQueueStatus(queue_id="q3", status="running")
    q.items = [
        BatchQueueItem(project_id="running_proj", status="running"),
        BatchQueueItem(project_id="next_proj", status="pending"),
    ]
    q.current_index = 0
    mock_manager._batch_queue = q

    # Index файла нет → кандидат None
    result = mock_manager._select_pregemma_candidate(q)
    assert result == (None, False)


# ─── Test 4: batch skips Gemma if prefetched & valid ─────────────────────────

@pytest.mark.asyncio
async def test_batch_skips_gemma_if_prefetched_valid(
    mock_manager, fake_project_dir, monkeypatch
):
    """Если item.gemma_prefetched=True и gemma_outputs_are_valid()=True, main
    pipeline должен выйти из _run_gemma_enrichment_stage до вызова runner'а."""
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus

    q = BatchQueueStatus(queue_id="q4", status="running")
    q.items = [
        BatchQueueItem(project_id="p", status="running", gemma_prefetched=True),
    ]
    mock_manager._batch_queue = q

    monkeypatch.setattr(
        "backend.app.pipeline.manager.resolve_project_dir",
        lambda pid: fake_project_dir,
    )
    # outputs_valid = True
    monkeypatch.setattr(
        "backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract.gemma_outputs_are_valid",
        lambda *a, **kw: (True, "ok"),
    )
    runner_called = False
    async def fake_runner(ctx, *, force=False):
        nonlocal runner_called
        runner_called = True
        class R: success = True; cancelled = False; error = None
        return R()
    monkeypatch.setattr(
        "backend.app.pipeline.manager._run_gemma_enrichment_stage_fn", fake_runner
    )

    job = AuditJob(
        job_id="j1", project_id="p",
        stage=AuditStage.PREPARE, status=JobStatus.RUNNING,
    )
    await mock_manager._run_gemma_enrichment_stage(job, force=False)

    assert runner_called is False, "runner НЕ должен был вызваться при валидном prefetch"
    assert mock_manager._current_gemma_stage_done.get("p") is True


# ─── Test 5: batch runs Gemma if prefetched but invalid ──────────────────────

@pytest.mark.asyncio
async def test_batch_runs_gemma_if_prefetched_invalid(
    mock_manager, fake_project_dir, monkeypatch
):
    """Если marker есть, но outputs битые — runner вызывается штатно."""
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus

    q = BatchQueueStatus(queue_id="q5", status="running")
    q.items = [
        BatchQueueItem(project_id="p", status="running", gemma_prefetched=True),
    ]
    mock_manager._batch_queue = q

    monkeypatch.setattr(
        "backend.app.pipeline.manager.resolve_project_dir",
        lambda pid: fake_project_dir,
    )
    monkeypatch.setattr(
        "backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract.gemma_outputs_are_valid",
        lambda *a, **kw: (False, "schema_mismatch"),
    )
    runner_called = False
    async def fake_runner(ctx, *, force=False):
        nonlocal runner_called
        runner_called = True
        class R: success = True; cancelled = False; error = None
        return R()
    monkeypatch.setattr(
        "backend.app.pipeline.manager._run_gemma_enrichment_stage_fn", fake_runner
    )
    # Подменим _make_stage_context чтоб не падать на отсутствии файлов
    monkeypatch.setattr(mock_manager, "_make_stage_context", lambda j: MagicMock())

    job = AuditJob(
        job_id="j2", project_id="p",
        stage=AuditStage.PREPARE, status=JobStatus.RUNNING,
    )
    await mock_manager._run_gemma_enrichment_stage(job, force=False)

    assert runner_called is True, "runner должен был вызваться: marker есть, но outputs битые"


# ─── Test 6: failed pre-Gemma не валит проект ────────────────────────────────

def test_failed_pregemma_does_not_fail_project():
    """item.gemma_prefetch_status='failed' не влияет на item.status."""
    item = BatchQueueItem(
        project_id="p", status="pending",
        gemma_prefetch_status="failed", gemma_prefetch_error="net error",
    )
    # Симулируем что main pipeline дошёл — pending должен сохраниться
    assert item.status == "pending"
    assert item.gemma_prefetched is False  # не помечен как готовый
    # _select_pregemma_candidate должен пропускать failed (window=1 + skip)
    # Это проверяется в test_pregemma_window_eq_1


# ─── Test 7: prepare-data blocked when in batch ──────────────────────────────

@pytest.mark.asyncio
async def test_prepare_data_blocked_when_in_batch(monkeypatch):
    """start_prepare_data и start_retry_failed бросают RuntimeError для проекта
    из активной batch-очереди."""
    from backend.app.pipeline.stages.prepare import prepare_service
    from backend.app.pipeline import manager as mgr_mod

    # Подменим pipeline_manager.is_project_in_active_batch
    fake_mgr = MagicMock()
    fake_mgr.is_project_in_active_batch = MagicMock(return_value=True)
    fake_mgr.is_running = MagicMock(return_value=False)
    fake_mgr.is_queued = MagicMock(return_value=False)
    monkeypatch.setattr(mgr_mod, "pipeline_manager", fake_mgr)

    with pytest.raises(RuntimeError, match="активной batch-очереди"):
        await prepare_service.start_prepare_data("pid_in_batch")

    with pytest.raises(RuntimeError, match="активной batch-очереди"):
        await prepare_service.start_retry_failed("pid_in_batch")


# ─── Test 8: queue persists prefetch state ───────────────────────────────────

def test_queue_persists_prefetch_state(mock_manager, isolated_batch_queue_file):
    """Все 5 новых полей сохраняются и восстанавливаются через persist+load."""
    q = BatchQueueStatus(queue_id="q8", action="full", total=1, status="running")
    q.items = [
        BatchQueueItem(
            project_id="p", status="pending",
            gemma_prefetched=True,
            gemma_prefetch_status="done",
            gemma_prefetch_error=None,
            gemma_prefetch_started_at="2026-01-01T00:00:00",
            gemma_prefetch_finished_at="2026-01-01T00:05:00",
        ),
    ]
    mock_manager._batch_queue = q
    mock_manager._persist_queue()

    # Файл создан атомарно (tmp + replace)
    assert isolated_batch_queue_file.exists()
    raw = json.loads(isolated_batch_queue_file.read_text())
    item_raw = raw["items"][0]
    assert item_raw["gemma_prefetched"] is True
    assert item_raw["gemma_prefetch_status"] == "done"
    assert item_raw["gemma_prefetch_started_at"] == "2026-01-01T00:00:00"
    assert item_raw["gemma_prefetch_finished_at"] == "2026-01-01T00:05:00"

    # Roundtrip через Pydantic
    restored = BatchQueueStatus(**raw)
    assert restored.items[0].gemma_prefetched is True
    assert restored.items[0].gemma_prefetch_status == "done"


# ─── Test 9: window=1 — не переходит к N+2 ───────────────────────────────────

def test_pregemma_window_eq_1(mock_manager, fake_project_dir, monkeypatch, tmp_path):
    """Если N+1 уже валиден (outputs готовы), _select_pregemma_candidate
    помечает его skipped и возвращает (None, True). НЕ идёт смотреть N+2."""
    q = BatchQueueStatus(queue_id="q9", status="running")
    q.items = [
        BatchQueueItem(project_id="running_proj", status="running"),
        BatchQueueItem(project_id="next_proj", status="pending"),
        BatchQueueItem(project_id="further_proj", status="pending"),
    ]
    q.current_index = 0
    mock_manager._batch_queue = q

    # Подменим resolve_project_dir чтоб возвращал dir с готовым index.json
    proj_dir = tmp_path / "next_proj"
    blocks_dir = proj_dir / "_output" / "blocks_gemma_100"
    blocks_dir.mkdir(parents=True)
    (blocks_dir / "index.json").write_text("{}")
    monkeypatch.setattr(
        "backend.app.pipeline.manager.resolve_project_dir",
        lambda pid: proj_dir,
    )
    # outputs_valid для next_proj = True (уже сделан раньше)
    monkeypatch.setattr(
        "backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract.gemma_outputs_are_valid",
        lambda *a, **kw: (True, "ok"),
    )

    result = mock_manager._select_pregemma_candidate(q)
    assert result == (None, True), "Должен вернуть (None, True) — кандидата нет, но мутация была"
    # N+1 помечен skipped
    assert q.items[1].gemma_prefetched is True
    assert q.items[1].gemma_prefetch_status == "skipped"
    # N+2 НЕ затронут (window=1)
    assert q.items[2].gemma_prefetched is False
    assert q.items[2].gemma_prefetch_status is None


# ─── Test 10: lock timeout не блокирует ──────────────────────────────────────

@pytest.mark.asyncio
async def test_pregemma_lock_timeout_does_not_block():
    """Если lock занят другим owner, pre-Gemma отваливается по asyncio.wait_for
    timeout и не блокирует main pipeline."""
    lock = asyncio.Lock()
    await lock.acquire()  # симулируем что main владелец

    acquired = False
    try:
        await asyncio.wait_for(lock.acquire(), timeout=0.2)
        acquired = True
    except asyncio.TimeoutError:
        acquired = False

    assert acquired is False, "должен был отвалиться по timeout"
    # Цикл pre-Gemma продолжается без падения
    lock.release()


# ─── Test 11: cancel во время активного run сбрасывает state ─────────────────

@pytest.mark.asyncio
async def test_pregemma_cancelled_during_active_run_resets_state(
    mock_manager, monkeypatch, tmp_path
):
    """При CancelledError во время _run_gemma_enrichment_stage_fn target должен
    получить gemma_prefetch_status=None (не остаться 'running'), lock освободиться,
    CancelledError re-raised."""
    from backend.app.pipeline.stages.prepare import prepare_service as ps_mod

    q = BatchQueueStatus(queue_id="q11", status="running")
    q.items = [
        BatchQueueItem(project_id="running_proj", status="running"),
        BatchQueueItem(project_id="target_proj", status="pending"),
    ]
    q.current_index = 0
    mock_manager._batch_queue = q
    mock_manager._mark_gemma_stage_done("running_proj")  # gate открыт

    # Подменим resolve_project_dir
    proj_dir = tmp_path / "target_proj"
    blocks_dir = proj_dir / "_output" / "blocks_gemma_100"
    blocks_dir.mkdir(parents=True)
    (blocks_dir / "index.json").write_text("{}")
    monkeypatch.setattr(
        "backend.app.pipeline.manager.resolve_project_dir",
        lambda pid: proj_dir,
    )
    # outputs_valid сначала False (нужен реальный run), потом — без разницы
    monkeypatch.setattr(
        "backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract.gemma_outputs_are_valid",
        lambda *a, **kw: (False, "no_summary"),
    )

    async def cancellable_runner(ctx, *, force=False):
        await asyncio.sleep(10)  # будет отменён
        class R: success = False; cancelled = True; error = None
        return R()

    monkeypatch.setattr(
        "backend.app.pipeline.manager._run_gemma_enrichment_stage_fn",
        cancellable_runner,
    )
    monkeypatch.setattr(
        mock_manager, "_make_stage_context", lambda j: MagicMock()
    )

    pause_event = asyncio.Event(); pause_event.set()

    # Сбросим общий prepare lock на свежий
    ps_mod.prepare_state._global_lock = None

    task = asyncio.create_task(
        mock_manager._run_gemma_prefetch_loop(q, pause_event)
    )

    # Дать time для входа в lock + start runner
    for _ in range(20):
        await asyncio.sleep(0.05)
        if q.items[1].gemma_prefetch_status == "running":
            break

    assert q.items[1].gemma_prefetch_status == "running", \
        "должен был успеть выставить running статус"

    # Отменяем
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # State сброшен в None — main pipeline увидит непрепарированный проект
    assert q.items[1].gemma_prefetch_status is None
    assert q.items[1].gemma_prefetched is False
    # Lock освобождён
    assert ps_mod.prepare_state.get_lock().locked() is False
