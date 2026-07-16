"""Тесты устойчивости batch-очереди при живом текущем проекте.

Воспроизводит инцидент: batch-worker __BATCH__ исполняет текущий project audit
ВНУТРИ своей же корутины (self._tasks[pid] = asyncio.current_task()). У meta-job
__BATCH__ нет собственного heartbeat-цикла, поэтому через ZOMBIE_TIMEOUT_SEC
cleanup_zombies ложно признавал его зомби и снимал из self._tasks — после чего
_reconcile_stale_queue демотировал ВСЮ очередь в interrupted, хотя проект ещё
реально выполнялся. Фоновые prefetch/pre-crop циклы (gated на status=="running")
после этого умирали, а UI показывал «сервер перезапущен — очередь прервана».

Покрытие:
  1. cleanup_zombies НЕ снимает __BATCH__/текущий проект, пока worker жив.
  2. cleanup_zombies всё ещё убирает настоящего зомби (регрессия не сломана).
  3. _reconcile_stale_queue НЕ демотирует, пока жив текущий аудит.
  4. _reconcile_stale_queue демотирует, когда нет ни worker'а, ни живого аудита.
  5. resume → BatchResumeBlockedError, пока текущий проект жив.
  6. resume идемпотентен при живом worker'е (без дубля).
  7. resume разрешён после завершения проекта и поднимает ровно один worker.
  8. prefetch не залипает на неподготовленном (V2/no-crops) проекте — окно.
  9. get_batch_diagnostics: batch_worker_lost + current_project_running.

Run: python -m pytest tests/test_batch_queue_resilience.py -v
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
from backend.app.pipeline.manager import (  # noqa: E402
    PipelineManager,
    BatchResumeBlockedError,
)
from backend.app.models.audit import (  # noqa: E402
    BatchQueueStatus,
    BatchQueueItem,
    AuditJob,
    JobStatus,
)

_OLD_TS = "2000-01-01T00:00:00"  # heartbeat сильно протух → кандидат в зомби


class _FakeTask:
    """Заглушка asyncio.Task: done() возвращает заранее заданное значение."""

    def __init__(self, done: bool):
        self._done = done

    def done(self) -> bool:
        return self._done


def _mgr() -> PipelineManager:
    mgr = PipelineManager()
    mgr._persist_queue = lambda: None  # не писать на диск

    async def _anoop(*a, **kw):
        return None

    mgr._broadcast_batch_progress = _anoop

    # Безопасный _cleanup без побочного lmstudio-планировщика — нам важна только
    # семантика «убрать из active_jobs/_tasks».
    def _safe_cleanup(pid: str):
        mgr.active_jobs.pop(pid, None)
        mgr._tasks.pop(pid, None)

    mgr._cleanup = _safe_cleanup
    return mgr


def _running_job(pid: str, started_at: str = _OLD_TS) -> AuditJob:
    return AuditJob(
        job_id=f"job-{pid}",
        project_id=pid,
        status=JobStatus.RUNNING,
        started_at=started_at,
    )


def _queue(status="running", items=None, current_index=0) -> BatchQueueStatus:
    items = items or [BatchQueueItem(project_id="p1", action="full", status="running")]
    return BatchQueueStatus(
        queue_id="q-test",
        action="full",
        status=status,
        items=items,
        total=len(items),
        current_index=current_index,
    )


# ─── 1. cleanup_zombies защищает живой worker + текущий проект ────────────────

def test_cleanup_zombies_protects_live_batch_and_current_project():
    mgr = _mgr()
    mgr._batch_queue = _queue(
        items=[BatchQueueItem(project_id="p1", action="full", status="running")]
    )
    # worker жив, но meta-job __BATCH__ и project-job протухли по heartbeat
    mgr._tasks["__BATCH__"] = _FakeTask(done=False)
    mgr.active_jobs["__BATCH__"] = AuditJob(
        job_id="b", project_id="__BATCH__", status=JobStatus.RUNNING, started_at=_OLD_TS
    )
    mgr.active_jobs["p1"] = _running_job("p1")

    mgr.cleanup_zombies()

    # Ни worker, ни текущий проект не сняты, очередь не демотирована.
    assert "__BATCH__" in mgr.active_jobs
    assert "__BATCH__" in mgr._tasks
    assert "p1" in mgr.active_jobs
    assert mgr._batch_queue.status == "running"
    assert mgr._batch_queue.items[0].status == "running"


# ─── 2. cleanup_zombies всё ещё убирает настоящего зомби ──────────────────────

def test_cleanup_zombies_still_removes_real_zombie():
    mgr = _mgr()
    # Нет очереди, нет worker'а, нет живых процессов — протухший job это зомби.
    mgr.active_jobs["pX"] = _running_job("pX")
    mgr.cleanup_zombies()
    assert "pX" not in mgr.active_jobs


# ─── 3. reconcile НЕ демотирует, пока жив текущий аудит ───────────────────────

def test_reconcile_noop_when_worker_lost_but_audit_alive():
    mgr = _mgr()
    mgr._batch_queue = _queue()
    # worker-регистрация потеряна (нет _tasks["__BATCH__"]), но есть живой
    # project-job в active_jobs.
    mgr.active_jobs["p1"] = _running_job("p1")
    assert mgr._reconcile_stale_queue() is False
    assert mgr._batch_queue.status == "running"
    assert mgr._batch_queue.items[0].status == "running"


# ─── 4. reconcile демотирует, когда нет worker'а и нет живого аудита ──────────

def test_reconcile_demotes_when_no_worker_and_no_audit():
    mgr = _mgr()
    mgr._batch_queue = _queue()
    # active_jobs пуст, живых процессов нет → реально мёртвая очередь.
    assert mgr._reconcile_stale_queue() is True
    assert mgr._batch_queue.status == "interrupted"
    assert mgr._batch_queue.items[0].status == "interrupted"


# ─── 5. resume заблокирован, пока текущий проект жив ──────────────────────────

def test_resume_blocked_while_current_project_running():
    mgr = _mgr()
    mgr._batch_queue = _queue(status="interrupted")
    mgr.active_jobs["p1"] = _running_job("p1")  # живой аудит

    async def _run():
        with pytest.raises(BatchResumeBlockedError):
            await mgr.resume_interrupted_batch()

    asyncio.run(_run())
    # Очередь не тронута, дубль-worker не создан.
    assert mgr._batch_queue.status == "interrupted"
    assert "__BATCH__" not in mgr._tasks


# ─── 6. resume идемпотентен при живом worker'е (без дубля) ────────────────────

def test_resume_idempotent_when_worker_alive():
    mgr = _mgr()
    mgr._batch_queue = _queue(status="running")
    live = _FakeTask(done=False)
    mgr._tasks["__BATCH__"] = live

    async def _run():
        return await mgr.resume_interrupted_batch()

    q = asyncio.run(_run())
    assert q is mgr._batch_queue
    assert mgr._tasks["__BATCH__"] is live  # тот же worker, не пересоздан


# ─── 7. resume разрешён после завершения проекта — ровно один worker ──────────

def test_resume_spawns_single_worker_when_idle():
    mgr = _mgr()
    mgr._batch_queue = _queue(
        status="interrupted",
        items=[BatchQueueItem(project_id="p1", action="full", status="interrupted")],
    )

    started = {"n": 0}

    async def _fake_worker(queue, meta_job):
        started["n"] += 1
        await asyncio.sleep(60)  # держим worker «живым»

    mgr._run_batch_queue = _fake_worker

    async def _run():
        q = await mgr.resume_interrupted_batch()
        await asyncio.sleep(0)  # дать worker-таску стартовать
        # Повторный resume не создаёт второй worker (worker жив).
        q2 = await mgr.resume_interrupted_batch()
        assert q2 is q
        task = mgr._tasks.get("__BATCH__")
        assert task is not None and not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return q

    q = asyncio.run(_run())
    assert q.status == "running"
    assert q.items[0].status == "pending"  # interrupted → pending для прохода
    assert started["n"] == 1  # ровно один worker


# ─── 8. prefetch не залипает на неподготовленном проекте (окно) ───────────────

def _patch_gemma(monkeypatch, ready_names: set[str], valid_names: set[str], tmp_path):
    """resolve_project_dir → tmp_path/<pid>; index.exists() и outputs_are_valid
    управляются множествами имён."""
    import backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract as gec

    monkeypatch.setattr(mgr_mod, "resolve_project_dir", lambda pid: tmp_path / pid)

    class _P:
        def __init__(self, exists: bool):
            self._e = exists

        def exists(self) -> bool:
            return self._e

    monkeypatch.setattr(
        gec, "gemma_blocks_index_path", lambda proj_dir: _P(proj_dir.name in ready_names)
    )
    monkeypatch.setattr(
        gec, "gemma_outputs_are_valid", lambda proj_dir: (proj_dir.name in valid_names, "x")
    )


def test_model_prefetch_is_absent_from_queue_manager():
    mgr = _mgr()

    assert not hasattr(mgr, "_select_pregemma_candidate")
    assert not hasattr(mgr, "_run_gemma_prefetch_loop")


def test_diagnostics_degraded_but_current_running():
    mgr = _mgr()
    mgr._batch_queue = _queue(status="running")
    mgr.active_jobs["p1"] = _running_job("p1")  # живой проект, worker «потерян»
    d = mgr.get_batch_diagnostics()
    assert d["worker_alive"] is False
    assert d["current_project_running"] is True
    assert d["batch_worker_lost"] is True
    assert d["resume_available"] is False
    assert d["display_status"] == "degraded_but_current_running"
    assert d["current_project_id"] == "p1"


def test_diagnostics_resume_available_when_interrupted_and_idle():
    mgr = _mgr()
    mgr._batch_queue = _queue(status="interrupted")
    d = mgr.get_batch_diagnostics()
    assert d["worker_alive"] is False
    assert d["current_project_running"] is False
    assert d["resume_available"] is True
    assert d["display_status"] == "interrupted"


def test_diagnostics_running_normally_when_worker_alive():
    mgr = _mgr()
    mgr._batch_queue = _queue(status="running")
    mgr._tasks["__BATCH__"] = _FakeTask(done=False)
    d = mgr.get_batch_diagnostics()
    assert d["worker_alive"] is True
    assert d["current_project_running"] is True
    assert d["batch_worker_lost"] is False
    assert d["resume_available"] is False
    assert d["display_status"] == "running"
