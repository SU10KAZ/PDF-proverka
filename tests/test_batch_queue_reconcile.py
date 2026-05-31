"""Тесты reconcile зависшей batch-очереди.

Воспроизводит баг: после отмены / исключения воркера очередь остаётся с
'running'-item'ом без живого worker'а → UI вечно показывает «Выполняется».
`_reconcile_stale_queue()` должен демотировать такие item'ы.
"""
from backend.app.pipeline.manager import PipelineManager
from backend.app.models.audit import BatchQueueStatus, BatchQueueItem


class _FakeTask:
    """Заглушка asyncio.Task: done() возвращает заранее заданное значение."""

    def __init__(self, done: bool):
        self._done = done

    def done(self) -> bool:
        return self._done


def _mgr() -> PipelineManager:
    mgr = PipelineManager()
    # Не писать на диск во время теста.
    mgr._persist_queue = lambda: None
    return mgr


def test_reconcile_demotes_running_item_when_worker_dead():
    mgr = _mgr()
    mgr._batch_queue = BatchQueueStatus(
        queue_id="q-stale",
        action="resume",
        status="completed",  # очередь завершилась, но item залип в running
        items=[BatchQueueItem(project_id="p1", action="resume", status="running")],
        total=1,
        current_index=0,
    )
    # Воркер не зарегистрирован (мёртв) → _tasks["__BATCH__"] отсутствует.
    assert mgr._reconcile_stale_queue() is True
    assert mgr._batch_queue.items[0].status == "interrupted"
    # get_batch_queue тоже возвращает уже консистентную очередь.
    q = mgr.get_batch_queue()
    assert all(it.status != "running" for it in q.items)


def test_reconcile_running_queue_without_worker_becomes_interrupted():
    mgr = _mgr()
    mgr._batch_queue = BatchQueueStatus(
        queue_id="q-orphan",
        action="full",
        status="running",
        items=[BatchQueueItem(project_id="p1", action="full", status="running")],
        total=1,
        current_index=0,
    )
    assert mgr._reconcile_stale_queue() is True
    assert mgr._batch_queue.status == "interrupted"
    assert mgr._batch_queue.items[0].status == "interrupted"


def test_reconcile_noop_when_worker_alive():
    mgr = _mgr()
    mgr._batch_queue = BatchQueueStatus(
        queue_id="q-live",
        action="full",
        status="running",
        items=[BatchQueueItem(project_id="p1", action="full", status="running")],
        total=1,
        current_index=0,
    )
    mgr._tasks["__BATCH__"] = _FakeTask(done=False)  # воркер жив
    assert mgr._reconcile_stale_queue() is False
    assert mgr._batch_queue.status == "running"
    assert mgr._batch_queue.items[0].status == "running"


def test_reconcile_noop_when_no_queue():
    mgr = _mgr()
    assert mgr._batch_queue is None
    assert mgr._reconcile_stale_queue() is False
