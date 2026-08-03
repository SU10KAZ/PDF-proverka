"""
test_batch_queue_hide_finished.py
---------------------------------
«Убрать завершённые» из панели очереди — косметика, но с жёстким требованием:
элементы НЕ удаляются из списка.

Почему: worker батча (`_run_batch_queue`) обходит очередь по позиционному
индексу — локальный `idx`, инкрементируемый по ходу. Если физически удалить
записи ПЕРЕД текущей, хвост сдвинется на столько же позиций, и worker молча
перепрыгнет через часть pending-проектов (03.08.2026: в очереди из 22 позиций
удаление 6 сгоревших записей пропустило бы 5 ждущих проектов).

Поэтому hide_finished_batch_items только проставляет hidden=True.

Run: python -m pytest tests/test_batch_queue_hide_finished.py -v
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
from backend.app.models.audit import BatchQueueStatus, BatchQueueItem  # noqa: E402


def _make_queue() -> BatchQueueStatus:
    statuses = ["failed", "failed", "completed", "running", "pending", "pending"]
    items = [
        BatchQueueItem(project_id=f"P{i}", action="audit+optimization", status=st)
        for i, st in enumerate(statuses)
    ]
    return BatchQueueStatus(
        queue_id="q-test",
        action="audit+optimization",
        items=items,
        total=len(items),
        current_index=3,
        status="running",
    )


def _make_manager(monkeypatch) -> PipelineManager:
    m = PipelineManager.__new__(PipelineManager)
    m._batch_queue = _make_queue()
    monkeypatch.setattr(m, "_persist_queue", lambda: None)

    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(mgr_mod.ws_manager, "broadcast_global", _noop)
    monkeypatch.setattr(PipelineManager, "_broadcast_batch_progress", _noop)
    return m


def test_hidden_items_are_not_removed(monkeypatch):
    """Главная гарантия: длина списка и позиции неизменны."""
    m = _make_manager(monkeypatch)
    before = [it.project_id for it in m._batch_queue.items]

    queue = asyncio.run(m.hide_finished_batch_items())

    assert [it.project_id for it in queue.items] == before, (
        "элементы очереди сдвинулись — worker пропустит pending-проекты"
    )
    assert queue.total == len(before)
    assert queue.current_index == 3


def test_only_terminal_statuses_hidden(monkeypatch):
    m = _make_manager(monkeypatch)
    queue = asyncio.run(m.hide_finished_batch_items())

    hidden = {it.project_id for it in queue.items if it.hidden}
    assert hidden == {"P0", "P1", "P2"}, f"скрыто не то: {hidden}"
    for it in queue.items:
        if it.status in ("running", "pending"):
            assert not it.hidden, f"{it.project_id} ({it.status}) не должен скрываться"


def test_status_filter_narrows_selection(monkeypatch):
    m = _make_manager(monkeypatch)
    queue = asyncio.run(m.hide_finished_batch_items(["failed"]))

    hidden = {it.project_id for it in queue.items if it.hidden}
    assert hidden == {"P0", "P1"}, f"фильтр по статусу не сработал: {hidden}"


def test_unknown_status_rejected(monkeypatch):
    m = _make_manager(monkeypatch)
    with pytest.raises(RuntimeError):
        asyncio.run(m.hide_finished_batch_items(["running"]))


def test_no_queue_rejected(monkeypatch):
    m = _make_manager(monkeypatch)
    m._batch_queue = None
    with pytest.raises(RuntimeError):
        asyncio.run(m.hide_finished_batch_items())
