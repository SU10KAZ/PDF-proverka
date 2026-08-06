"""Параллельная обработка очереди проектов (BATCH_MAX_PARALLEL).

Очередь исторически строго последовательна: `_run_batch_queue` крутил while-цикл
и вёл ровно один проект. Здесь проверяется режим слотов — N проектов
одновременно — и, главное, что при значении по умолчанию (1) поведение
осталось прежним до детали.

Покрытие:
  1. Дефолт = 1: слот один, проекты идут строго по очереди.
  2. BATCH_MAX_PARALLEL=5: пять проектов реально выполняются ОДНОВРЕМЕННО.
  3. Число слотов не превышает длину очереди (2 проекта ≠ 5 задач).
  4. Один project_id не захватывается двумя слотами (реестры ключуются по pid).
  5. Изоляция версий: каждый слот видит СВОЙ version_id через ContextVar.
  6. Падение одного проекта не роняет соседние слоты.
  7. Отмена одного проекта не убивает очередь; остальные доезжают.
  8. Все item'ы получают терминальный статус, очередь завершается.
  9. Защита живых проектов от cleanup_zombies покрывает ВСЕ слоты.

Run: python -m pytest tests/test_batch_queue_parallel.py -v
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.pipeline.manager as mgr_mod  # noqa: E402
from backend.app.pipeline.manager import PipelineManager  # noqa: E402
from backend.app.models.audit import (  # noqa: E402
    AuditJob,
    AuditStage,
    BatchQueueItem,
    BatchQueueStatus,
    JobStatus,
)


async def _anoop(*a, **k):
    return None


def _meta_job() -> AuditJob:
    return AuditJob(
        job_id="meta", project_id="__BATCH__", stage=AuditStage.PREPARE,
        status=JobStatus.RUNNING, started_at="2026-08-06T00:00:00",
    )


def _mgr(dispatch) -> PipelineManager:
    mgr = PipelineManager()
    mgr._persist_queue = lambda: None
    mgr._broadcast_batch_progress = _anoop
    mgr._log = _anoop
    mgr._stop_heartbeat = lambda pid: None
    mgr._shadow_mirror_completed_audit = lambda pid, job: None
    mgr._maybe_evict_block_crops = _anoop
    mgr._cleanup_batch_worker = lambda meta_job: None
    mgr._run_precrop_loop = _anoop
    mgr._resolve_object_id_for_project = lambda *a, **k: None
    mgr.is_running = lambda pid: False
    mgr._dispatch_action = dispatch
    return mgr


def _queue(items, action="audit+optimization") -> BatchQueueStatus:
    return BatchQueueStatus(
        queue_id="q", action=action, status="running",
        total=len(items), current_index=0, items=items,
    )


def _item(pid, status="pending", **kw) -> BatchQueueItem:
    return BatchQueueItem(
        project_id=pid, action=kw.pop("action", "audit+optimization"),
        status=status, job_id=f"job-{pid}", **kw,
    )


class _ConcurrencyProbe:
    """Считает, сколько проектов выполнялось одновременно."""

    def __init__(self, hold: float = 0.05):
        self.hold = hold
        self.now = 0
        self.peak = 0
        self.order: list[str] = []

    async def dispatch(self, item, job, default_action="full", action_override=None):
        self.now += 1
        self.peak = max(self.peak, self.now)
        self.order.append(item.project_id)
        try:
            await asyncio.sleep(self.hold)
            job.status = JobStatus.COMPLETED
        finally:
            self.now -= 1


@pytest.mark.asyncio
async def test_default_is_sequential(monkeypatch):
    """Без флага — ровно прежнее поведение: один проект за раз."""
    monkeypatch.delenv("BATCH_MAX_PARALLEL", raising=False)
    probe = _ConcurrencyProbe()
    mgr = _mgr(probe.dispatch)
    queue = _queue([_item(p) for p in ("A", "B", "C")])
    mgr._batch_queue = queue

    await mgr._run_batch_queue(queue, _meta_job())

    assert probe.peak == 1, f"дефолт должен быть последовательным, а было {probe.peak}"
    assert probe.order == ["A", "B", "C"]
    assert queue.status == "completed"
    assert queue.completed == 3


@pytest.mark.asyncio
async def test_five_projects_run_concurrently(monkeypatch):
    """Главное свойство: пять проектов действительно идут одновременно."""
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "5")
    probe = _ConcurrencyProbe(hold=0.08)
    mgr = _mgr(probe.dispatch)
    pids = [f"P{i}" for i in range(5)]
    queue = _queue([_item(p) for p in pids])
    mgr._batch_queue = queue

    await mgr._run_batch_queue(queue, _meta_job())

    assert probe.peak == 5, f"ожидали 5 одновременных проектов, было {probe.peak}"
    assert sorted(probe.order) == sorted(pids)
    assert queue.completed == 5
    assert queue.status == "completed"
    assert all(it.status == "completed" for it in queue.items)


@pytest.mark.asyncio
async def test_slots_capped_by_queue_length(monkeypatch):
    """Пять слотов на два проекта — лишние задачи не создаются."""
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "5")
    probe = _ConcurrencyProbe(hold=0.03)
    mgr = _mgr(probe.dispatch)
    queue = _queue([_item("A"), _item("B")])
    mgr._batch_queue = queue

    await mgr._run_batch_queue(queue, _meta_job())

    assert probe.peak <= 2
    assert queue.completed == 2


@pytest.mark.asyncio
async def test_same_project_not_taken_by_two_slots(monkeypatch):
    """Один project_id не может выполняться в двух слотах сразу.

    Реестры active_jobs/_tasks/_heartbeat_tasks и kill_all_processes ключуются
    голым project_id — два одновременных item'а одного проекта затёрли бы друг друга.
    """
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "4")
    seen_together: list[int] = []
    running: set[str] = set()

    async def dispatch(item, job, default_action="full", action_override=None):
        assert item.project_id not in running, "один проект захвачен дважды"
        running.add(item.project_id)
        seen_together.append(len(running))
        try:
            await asyncio.sleep(0.05)
            job.status = JobStatus.COMPLETED
        finally:
            running.discard(item.project_id)

    mgr = _mgr(dispatch)
    # Один и тот же проект дважды в очереди (разные версии/действия).
    queue = _queue([_item("DUP"), _item("DUP"), _item("OTHER")])
    mgr._batch_queue = queue

    await mgr._run_batch_queue(queue, _meta_job())

    assert all(it.status == "completed" for it in queue.items)
    assert queue.completed == 3


@pytest.mark.asyncio
async def test_version_scope_isolated_between_slots(monkeypatch):
    """Каждый слот видит свой version_id — иначе аудит писался бы в чужую версию."""
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "3")
    from backend.app.services.common import version_service

    seen: dict[str, str] = {}

    async def dispatch(item, job, default_action="full", action_override=None):
        # Пауза — чтобы соседние слоты гарантированно успели выставить своё.
        await asyncio.sleep(0.03)
        seen[item.project_id] = version_service.get_bound_version_id()
        job.status = JobStatus.COMPLETED

    mgr = _mgr(dispatch)
    items = [_item(f"P{i}", version_id=f"v00{i}") for i in range(3)]
    queue = _queue(items)
    mgr._batch_queue = queue

    await mgr._run_batch_queue(queue, _meta_job())

    assert seen == {"P0": "v000", "P1": "v001", "P2": "v002"}


@pytest.mark.asyncio
async def test_one_failure_does_not_kill_neighbours(monkeypatch):
    """Сбой одного проекта не должен ронять соседние слоты."""
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "3")

    async def dispatch(item, job, default_action="full", action_override=None):
        await asyncio.sleep(0.02)
        if item.project_id == "BAD":
            raise RuntimeError("подопытный сбой этапа")
        job.status = JobStatus.COMPLETED

    mgr = _mgr(dispatch)
    queue = _queue([_item("OK1"), _item("BAD"), _item("OK2")])
    mgr._batch_queue = queue

    await mgr._run_batch_queue(queue, _meta_job())

    by_pid = {it.project_id: it for it in queue.items}
    assert by_pid["OK1"].status == "completed"
    assert by_pid["OK2"].status == "completed"
    assert by_pid["BAD"].status == "failed"
    assert "подопытный сбой" in (by_pid["BAD"].error or "")
    assert queue.completed == 2
    assert queue.failed == 1


@pytest.mark.asyncio
async def test_cancelled_item_marked_and_queue_continues(monkeypatch):
    """Отмена одного проекта не оставляет фантом 'running' и не рвёт очередь."""
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "3")

    async def dispatch(item, job, default_action="full", action_override=None):
        if item.project_id == "CANCEL_ME":
            raise asyncio.CancelledError()
        await asyncio.sleep(0.02)
        job.status = JobStatus.COMPLETED

    mgr = _mgr(dispatch)
    queue = _queue([_item("A"), _item("CANCEL_ME"), _item("B")])
    mgr._batch_queue = queue

    await mgr._run_batch_queue(queue, _meta_job())

    by_pid = {it.project_id: it for it in queue.items}
    assert by_pid["CANCEL_ME"].status == "cancelled", "отменённый item завис в running"
    assert by_pid["A"].status == "completed"
    assert by_pid["B"].status == "completed"
    # Ни один item не должен остаться незавершённым.
    assert all(it.status in ("completed", "failed", "cancelled", "skipped")
               for it in queue.items)


def test_protected_pids_cover_all_running_slots():
    """cleanup_zombies не имеет права снимать НИ ОДИН из живых слотов."""
    mgr = PipelineManager()
    queue = _queue([_item("A", status="running"),
                    _item("B", status="running"),
                    _item("C", status="pending")])
    mgr._batch_queue = queue
    mgr._tasks["__BATCH__"] = _AliveTask()

    protected = mgr._protected_pids()
    assert {"A", "B", "__BATCH__"} <= protected
    assert "C" not in protected  # ещё не запускался — защищать нечего


def test_current_batch_item_pids_falls_back_to_index():
    """До проставления статусов опираемся на current_index (прежнее поведение)."""
    mgr = PipelineManager()
    queue = _queue([_item("A"), _item("B")])
    queue.current_index = 1
    mgr._batch_queue = queue
    assert mgr._current_batch_item_pids() == {"B"}


def test_batch_max_parallel_reads_env(monkeypatch):
    monkeypatch.delenv("BATCH_MAX_PARALLEL", raising=False)
    assert mgr_mod.batch_max_parallel() == 1

    monkeypatch.setenv("BATCH_MAX_PARALLEL", "5")
    assert mgr_mod.batch_max_parallel() == 5

    # Мусор и выход за границы не должны ронять запуск очереди.
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "не-число")
    assert mgr_mod.batch_max_parallel() == 1
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "999")
    assert mgr_mod.batch_max_parallel() == mgr_mod.BATCH_MAX_PARALLEL_CAP
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "0")
    assert mgr_mod.batch_max_parallel() == 1


class _AliveTask:
    def done(self) -> bool:
        return False
