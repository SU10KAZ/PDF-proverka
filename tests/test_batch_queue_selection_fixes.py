"""
test_batch_queue_selection_fixes.py
-----------------------------------
Регресс на три бага выбора элемента в worker'е очереди (инцидент 04.08.2026).

  A. «Мёртвый слот»: элемент, оказавшийся в позиции ВЫШЕ текущей (перестановка
     в UI или поздний add), не выполнялся никогда — worker шёл по монотонному
     idx и назад не возвращался.
  B. Пауза замораживала выбор: item захватывался ДО гейта паузы, поэтому
     перестановка очереди во время паузы на него не действовала — после
     «продолжить» запускался старый проект, а не поднятый оператором.
  C. Прерванный рестартом элемент гнался с нуля: авто-resume брал item целиком
     (`audit+optimization`), и проект с готовыми блоками/текстом/сводом
     пересчитывался с crop_blocks.

Run: python -m pytest tests/test_batch_queue_selection_fixes.py -v
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

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
        status=JobStatus.RUNNING, started_at="2026-08-04T00:00:00",
    )


def _mgr(dispatch) -> PipelineManager:
    """Manager со всей внешней обвязкой, заглушенной под тест."""
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


def _queue(items) -> BatchQueueStatus:
    return BatchQueueStatus(
        queue_id="q", action="audit+optimization", status="running",
        total=len(items), current_index=0, items=items,
    )


def _item(pid, status="pending", **kw) -> BatchQueueItem:
    return BatchQueueItem(
        project_id=pid, action=kw.pop("action", "audit+optimization"),
        status=status, job_id=f"job-{pid}", **kw,
    )


# ─── A. элемент выше текущей позиции не теряется ─────────────────────────────


def test_item_moved_above_current_position_still_runs(monkeypatch):
    """Перестановка pending-элемента ВВЕРХ (в позицию ниже уже пройденных)
    не делает его невыполнимым: worker пересматривает список заново."""
    order = []

    async def dispatch(item, job, default_action="full", action_override=None):
        order.append(item.project_id)
        job.status = JobStatus.COMPLETED
        # После первого же проекта поднимаем C в самое начало очереди —
        # позиция 0 уже «пройдена» старым монотонным idx.
        if item.project_id == "A":
            q.items.insert(0, q.items.pop(2))

    q = _queue([_item("A"), _item("B"), _item("C")])
    mgr = _mgr(dispatch)
    asyncio.run(mgr._run_batch_queue(q, _meta_job()))

    assert order == ["A", "C", "B"], order
    assert [it.status for it in q.items] == ["completed"] * 3
    assert q.status == "completed"


# ─── B. перестановка во время паузы действует ────────────────────────────────


def test_reorder_during_pause_takes_effect():
    """Пока очередь на паузе, поднятый вверх проект должен пойти ПЕРВЫМ:
    выбор элемента происходит после снятия паузы, а не до неё."""
    order = []

    async def dispatch(item, job, default_action="full", action_override=None):
        order.append(item.project_id)
        job.status = JobStatus.COMPLETED

    q = _queue([_item("A"), _item("B")])
    mgr = _mgr(dispatch)

    async def _run():
        mgr._paused = True
        mgr._pause_event.clear()
        worker = asyncio.create_task(mgr._run_batch_queue(q, _meta_job()))
        await asyncio.sleep(0)          # worker дошёл до гейта паузы
        q.items.reverse()               # оператор поднял B наверх
        mgr._paused = False
        mgr._pause_event.set()
        await asyncio.wait_for(worker, timeout=5)

    asyncio.run(_run())
    assert order == ["B", "A"], order


# ─── C. прерванный рестартом элемент продолжается, а не гонится с нуля ───────


def test_interrupted_item_resumes_instead_of_full_restart():
    """Элемент в статусе interrupted диспетчеризуется как resume:
    полный action заново не запускается."""
    seen = []

    async def dispatch(item, job, default_action="full", action_override=None):
        seen.append((item.project_id, item.status, action_override))
        job.status = JobStatus.COMPLETED

    q = _queue([_item("A", status="interrupted"), _item("B")])
    mgr = _mgr(dispatch)
    asyncio.run(mgr._run_batch_queue(q, _meta_job()))

    assert seen[0][0] == "A" and seen[0][2] == "resume", seen
    # Обычный pending-элемент продолжает идти своим action'ом.
    assert seen[1][0] == "B" and seen[1][2] is None, seen


def test_interrupted_retry_stage_item_keeps_its_stage():
    """Точечный retry_stage-элемент уже адресный — подменять его на resume
    нельзя, иначе повтор одного этапа превратится в общий каскад."""
    seen = []

    async def dispatch(item, job, default_action="full", action_override=None):
        seen.append((item.retry_stage, action_override))
        job.status = JobStatus.COMPLETED

    q = _queue([_item("A", status="interrupted", action="retry_stage",
                      retry_stage="findings_merge")])
    mgr = _mgr(dispatch)
    asyncio.run(mgr._run_batch_queue(q, _meta_job()))

    assert seen == [("findings_merge", None)], seen


# ─── D. элемент, дописанный в очередь во время работы, подхватывается ────────


def test_item_appended_while_running_is_picked_up():
    """Поздний add в работающую очередь не теряется."""
    order = []

    async def dispatch(item, job, default_action="full", action_override=None):
        order.append(item.project_id)
        job.status = JobStatus.COMPLETED
        if item.project_id == "A":
            q.items.append(_item("Z"))
            q.total = len(q.items)

    q = _queue([_item("A")])
    mgr = _mgr(dispatch)
    asyncio.run(mgr._run_batch_queue(q, _meta_job()))

    assert order == ["A", "Z"], order
    assert q.status == "completed"
