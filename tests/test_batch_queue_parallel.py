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


# ─── Дозагрузка очереди: вопрос Андрея Ивановича от 06.08.2026 ─────────
# «Если запущено 10 проектов и 5 пошли сразу — после завершения одного
#  шестой автоматически пойдёт, чтобы постоянно было 5 в работе?»


class _TimelineProbe:
    """Пишет, сколько проектов работало в момент старта каждого следующего."""

    def __init__(self, holds: dict[str, float], default: float = 0.05):
        self.holds = holds
        self.default = default
        self.now = 0
        self.peak = 0
        self.at_start: list[tuple[str, int]] = []
        self.samples: list[int] = []

    async def dispatch(self, item, job, default_action="full", action_override=None):
        self.now += 1
        self.peak = max(self.peak, self.now)
        self.at_start.append((item.project_id, self.now))
        try:
            await asyncio.sleep(self.holds.get(item.project_id, self.default))
            job.status = JobStatus.COMPLETED
        finally:
            self.now -= 1

    async def sample(self, period: float, stop: asyncio.Event):
        while not stop.is_set():
            self.samples.append(self.now)
            await asyncio.sleep(period)


@pytest.mark.asyncio
async def test_ten_projects_keep_five_slots_busy(monkeypatch):
    """10 проектов при 5 слотах: пул доливается, а не ждёт всю пятёрку.

    Ключевое свойство — слот берёт следующий item СРАЗУ, как освободился, не
    дожидаясь соседей. Разные длительности специально: если бы дозагрузка шла
    «волнами по пять», шестой стартовал бы только после самого долгого.
    """
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "5")
    pids = [f"P{i:02d}" for i in range(10)]
    # Первые пять — резко разной длительности, остальные ровные.
    holds = {"P00": 0.02, "P01": 0.20, "P02": 0.20, "P03": 0.20, "P04": 0.20}
    probe = _TimelineProbe(holds, default=0.03)
    mgr = _mgr(probe.dispatch)
    queue = _queue([_item(p) for p in pids])
    mgr._batch_queue = queue

    stop = asyncio.Event()
    sampler = asyncio.create_task(probe.sample(0.005, stop))
    await mgr._run_batch_queue(queue, _meta_job())
    stop.set()
    await sampler

    assert probe.peak == 5, f"потолок должен быть ровно 5, был {probe.peak}"
    assert max(probe.samples) <= 5, "пул не имеет права превышать BATCH_MAX_PARALLEL"
    assert queue.completed == 10 and queue.status == "completed"
    assert all(it.status == "completed" for it in queue.items)

    # Шестой обязан стартовать, пока четверо ещё работают — то есть на входе
    # он видит 5 занятых слотов (свой + четыре чужих).
    sixth_pid, sixth_now = probe.at_start[5]
    assert sixth_now == 5, (
        f"шестой ({sixth_pid}) стартовал при {sixth_now} занятых слотах — "
        "значит пул опустел и дозагрузка идёт волнами, а не по мере освобождения"
    )
    # И это не разовая удача: пул держится полным почти весь прогон.
    full = sum(1 for _, n in probe.at_start if n == 5)
    assert full >= 6, f"полным пул был лишь при {full} стартах из 10"


@pytest.mark.asyncio
async def test_first_free_slot_takes_the_next_item_not_a_fixed_one(monkeypatch):
    """Освободившийся слот берёт ПЕРВЫЙ незавершённый сверху очереди.

    От этого зависит перестановка строк в интерфейсе: оператор двигает
    приоритет — и следующий свободный слот берёт то, что подняли наверх.
    """
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "2")
    probe = _TimelineProbe({"A": 0.02, "B": 0.30}, default=0.02)
    mgr = _mgr(probe.dispatch)
    queue = _queue([_item(p) for p in ("A", "B", "C", "D")])
    mgr._batch_queue = queue

    await mgr._run_batch_queue(queue, _meta_job())

    # A закончился первым → его слот берёт C (первый pending), не D.
    assert probe.at_start[2][0] == "C", f"третьим взяли {probe.at_start[2][0]}, а не C"
    assert queue.completed == 4


@pytest.mark.asyncio
async def test_slots_grow_when_projects_are_added_to_a_running_queue(monkeypatch):
    """Дозагрузка поднимает слоты (запрос Андрея Ивановича 06.08.2026).

    Раньше число слотов считалось ОДИН раз на старте как
    `min(потолок, длина очереди)`. Запустил очередь одним проектом — получил
    один слот навсегда, и дослать в неё девять не помогало: они шли по
    одному. Теперь супервизор пересматривает набор слотов, а enqueue его
    будит через `_wake_batch_slots`.
    """
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "5")
    probe = _TimelineProbe({"A": 0.30}, default=0.05)
    mgr = _mgr(probe.dispatch)
    queue = _queue([_item("A")])          # очередь СТАРТУЕТ с одним проектом
    mgr._batch_queue = queue

    async def _add_later():
        await asyncio.sleep(0.05)         # супервизор уже поднял свой слот
        for p in ("B", "C", "D", "E", "F", "G"):
            queue.items.append(_item(p))
        queue.total = len(queue.items)
        mgr._wake_batch_slots()           # то же, что делает start_batch

    await asyncio.gather(mgr._run_batch_queue(queue, _meta_job()), _add_later())

    assert queue.completed == 7
    assert probe.peak == 5, (
        f"после дозагрузки пул должен вырасти до потолка, а вырос до {probe.peak}"
    )
    # Долитые проекты обязаны пойти ПАРАЛЛЕЛЬНО с ещё работающим A, а не после.
    started_while_a_runs = [pid for pid, n in probe.at_start if n >= 2]
    assert len(started_while_a_runs) >= 4, (
        "долитые проекты дождались завершения первого — доливка не сработала"
    )


@pytest.mark.asyncio
async def test_single_retry_enqueue_wakes_running_slot_pool(monkeypatch, tmp_path):
    """Поштучный retry будит пул так же, как массовый start_batch.

    UI добавляет retry каждого упавшего проекта отдельным HTTP-запросом. Если
    первый запрос уже успел запустить очередь из одного item, последующие
    должны немедленно поднять остальные слоты, а не ждать завершения первого.
    """
    from backend.app.pipeline.stages.prepare import prepare_service
    from backend.app.services.common import version_service

    monkeypatch.setattr(mgr_mod, "resolve_project_dir", lambda _pid: tmp_path)
    monkeypatch.setattr(
        version_service, "resolve_effective_version_id",
        lambda *_a, **_k: "v001",
    )
    monkeypatch.setattr(
        version_service, "get_version_entry", lambda *_a, **_k: {},
    )
    monkeypatch.setattr(
        prepare_service, "is_prepare_active_or_queued", lambda _pid: False,
    )

    mgr = PipelineManager()
    queue = _queue([_item("A", status="running")], action="retry_stage")
    mgr._batch_queue = queue
    mgr._ensure_batch_worker = lambda action_for_label="full": queue
    mgr._resolve_object_id_for_project = lambda *_a, **_k: None
    mgr._broadcast_batch_progress = _anoop
    wake_calls = 0

    def _wake():
        nonlocal wake_calls
        wake_calls += 1

    mgr._wake_batch_slots = _wake

    job = await mgr._enqueue_single(
        "B", action="retry_stage", retry_stage="block_analysis",
        version_id="v001",
    )

    assert job.project_id == "B"
    assert queue.items[-1].project_id == "B"
    assert queue.items[-1].retry_stage == "block_analysis"
    assert wake_calls == 1


@pytest.mark.asyncio
async def test_pool_never_exceeds_ceiling_after_adds(monkeypatch):
    """Доливка не имеет права перескочить BATCH_MAX_PARALLEL."""
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "3")
    probe = _TimelineProbe({}, default=0.04)
    mgr = _mgr(probe.dispatch)
    queue = _queue([_item("A")])
    mgr._batch_queue = queue

    async def _flood():
        for wave in range(3):
            await asyncio.sleep(0.02)
            for i in range(5):
                queue.items.append(_item(f"W{wave}_{i}"))
            queue.total = len(queue.items)
            mgr._wake_batch_slots()

    await asyncio.gather(mgr._run_batch_queue(queue, _meta_job()), _flood())

    assert probe.peak <= 3, f"пул превысил потолок: {probe.peak}"
    assert queue.completed == 16


@pytest.mark.asyncio
async def test_single_item_with_high_ceiling_still_cancels_cleanly(monkeypatch):
    """Один проект при потолке 5 теперь идёт в дочерней задаче, не в __BATCH__.

    Это ИЗМЕНЕНИЕ: прежняя формула `min(потолок, длина)` давала для одного
    item'а parallel=1 и исполняла его прямо в корутине __BATCH__. Ради
    доливки пришлось перейти на пул и здесь — значит отмена обязана
    по-прежнему доходить до item'а, а очередь не должна зависать.
    """
    monkeypatch.setenv("BATCH_MAX_PARALLEL", "5")
    started = asyncio.Event()

    async def _slow(item, job, default_action="full", action_override=None):
        started.set()
        await asyncio.sleep(10)
        job.status = JobStatus.COMPLETED

    mgr = _mgr(_slow)
    queue = _queue([_item("SOLO")])
    mgr._batch_queue = queue

    task = asyncio.create_task(mgr._run_batch_queue(queue, _meta_job()))
    await asyncio.wait_for(started.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert queue.items[0].status != "running", (
        "после отмены item не должен остаться running — иначе cleanup_zombies "
        "посчитает его живым, а resume снесёт артефакты"
    )
