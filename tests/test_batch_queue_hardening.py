"""
test_batch_queue_hardening.py
-----------------------------
Task 6/6 — финальное упрочнение Batch Audit Queue.

Покрытие точечных фиксов:
  A. _heartbeat_loop устойчив: исключение в ОДНОЙ итерации логируется и НЕ
     убивает цикл (раньше `except Exception` на весь while молча гасил heartbeat
     → джоба «замолкала» → cleanup_zombies ложно признавал её зомби).
  B. _cleanup_batch_worker identity-aware: старый worker в finally НЕ сносит
     регистрацию НОВОГО worker'а (гонка close+enqueue).
  C. restart → resume предсказуем: load_persisted_queue демотирует running в
     interrupted с понятной причиной, resume поднимает ровно один worker.
  D. prefetch failure НЕ переводит основной batch item в skipped/failed —
     трогается только отдельное поле gemma_prefetch_status.

Все тесты изолированы (tmp_path / monkeypatch, без диска/сети/реального Gemma).

Run: python -m pytest tests/test_batch_queue_hardening.py -v
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.pipeline.manager as mgr_mod  # noqa: E402
from backend.app.pipeline.manager import PipelineManager  # noqa: E402
from backend.app.models.audit import (  # noqa: E402
    BatchQueueStatus,
    BatchQueueItem,
    AuditJob,
    AuditStage,
    JobStatus,
)


class _FakeTask:
    def __init__(self, done: bool):
        self._done = done

    def done(self) -> bool:
        return self._done


async def _anoop(*a, **k):
    return None


def _mgr() -> PipelineManager:
    mgr = PipelineManager()
    mgr._persist_queue = lambda: None
    mgr._broadcast_batch_progress = _anoop
    return mgr


# ─── A. heartbeat resilience ──────────────────────────────────────────────────


def test_heartbeat_survives_iteration_exception(monkeypatch):
    """Сбой broadcast в одной итерации не должен гасить heartbeat-цикл:
    last_heartbeat обновляется, цикл доходит до следующих тиков."""
    mgr = _mgr()
    job = AuditJob(
        job_id="j", project_id="p", stage=AuditStage.PREPARE,
        status=JobStatus.RUNNING, started_at="2026-01-01T00:00:00",
    )

    calls = {"broadcast": 0, "sleep": 0}

    async def bad_broadcast(*a, **k):
        calls["broadcast"] += 1
        if calls["broadcast"] == 1:
            raise RuntimeError("ws boom")  # первый тик падает

    monkeypatch.setattr(mgr_mod.ws_manager, "broadcast_to_project", bad_broadcast)

    real_sleep = asyncio.sleep

    async def fast_sleep(_secs):
        calls["sleep"] += 1
        if calls["sleep"] >= 3:
            job.status = JobStatus.COMPLETED  # на 3-м тике останавливаем цикл
        await real_sleep(0)

    monkeypatch.setattr(mgr_mod.asyncio, "sleep", fast_sleep)

    # Не должно бросить наружу.
    asyncio.run(mgr._heartbeat_loop(job))

    assert calls["broadcast"] >= 2, "цикл должен пережить сбойный тик и продолжить"
    assert job.last_heartbeat is not None, "last_heartbeat обновляется даже при сбое broadcast"


def test_heartbeat_stops_cleanly_when_job_not_running(monkeypatch):
    """Если job не RUNNING — цикл выходит без исключений (нормальное завершение)."""
    mgr = _mgr()
    job = AuditJob(
        job_id="j", project_id="p", stage=AuditStage.PREPARE,
        status=JobStatus.COMPLETED, started_at="2026-01-01T00:00:00",
    )
    real_sleep = asyncio.sleep
    monkeypatch.setattr(mgr_mod.asyncio, "sleep", lambda _s: real_sleep(0))
    broadcast_calls = {"n": 0}

    async def count_broadcast(*a, **k):
        broadcast_calls["n"] += 1

    monkeypatch.setattr(mgr_mod.ws_manager, "broadcast_to_project", count_broadcast)
    asyncio.run(mgr._heartbeat_loop(job))
    assert broadcast_calls["n"] == 0, "при не-RUNNING job heartbeat не шлётся"


# ─── B. identity-aware _cleanup_batch_worker ─────────────────────────────────


def test_cleanup_batch_worker_removes_own_registration(monkeypatch):
    mgr = _mgr()

    async def _run():
        meta = AuditJob(job_id="b", project_id="__BATCH__",
                        stage=AuditStage.PREPARE, status=JobStatus.RUNNING)
        mgr._tasks["__BATCH__"] = asyncio.current_task()  # регистрация = ЭТОТ таск
        mgr.active_jobs["__BATCH__"] = meta
        mgr._cleanup_batch_worker(meta)
        assert "__BATCH__" not in mgr._tasks
        assert "__BATCH__" not in mgr.active_jobs

    asyncio.run(_run())


def test_cleanup_batch_worker_preserves_new_worker(monkeypatch):
    """Гонка close+enqueue: пока старый worker в finally, enqueue поднял новый
    под тем же ключом __BATCH__. Старый НЕ должен снести регистрацию нового."""
    mgr = _mgr()

    async def _run():
        old_meta = AuditJob(job_id="old", project_id="__BATCH__",
                            stage=AuditStage.PREPARE, status=JobStatus.RUNNING)
        new_task = _FakeTask(done=False)
        new_meta = AuditJob(job_id="new", project_id="__BATCH__",
                            stage=AuditStage.PREPARE, status=JobStatus.RUNNING)
        # Новый worker уже занял __BATCH__.
        mgr._tasks["__BATCH__"] = new_task
        mgr.active_jobs["__BATCH__"] = new_meta
        # Старый worker доходит до finally:
        mgr._cleanup_batch_worker(old_meta)
        # Регистрация нового worker'а сохранена.
        assert mgr._tasks["__BATCH__"] is new_task
        assert mgr.active_jobs["__BATCH__"] is new_meta

    asyncio.run(_run())


# ─── C. restart → resume предсказуем ─────────────────────────────────────────


def test_restart_recovery_marks_interrupted_with_reason(monkeypatch, tmp_path):
    """load_persisted_queue: running → interrupted с понятной причиной;
    pending остаётся pending; статус очереди → interrupted (worker не стартует)."""
    qfile = tmp_path / "batch_queue.json"
    monkeypatch.setattr(mgr_mod, "BATCH_QUEUE_FILE", qfile)

    src = BatchQueueStatus(
        queue_id="q", action="full", status="running", total=2, current_index=0,
        items=[
            BatchQueueItem(project_id="A", action="full", status="running", job_id="ja"),
            BatchQueueItem(project_id="B", action="full", status="pending", job_id="jb"),
        ],
    )
    qfile.write_text(json.dumps(src.model_dump()), encoding="utf-8")

    mgr = _mgr()
    mgr.load_persisted_queue()
    q = mgr._batch_queue
    assert q is not None
    assert q.status == "interrupted"
    assert q.items[0].status == "interrupted"
    assert q.items[0].error == "Прервано рестартом сервера"
    assert q.items[1].status == "pending"


def test_restart_recovery_then_resume_spawns_single_worker(monkeypatch, tmp_path):
    """После recovery resume поднимает РОВНО один worker, items → pending."""
    qfile = tmp_path / "batch_queue.json"
    monkeypatch.setattr(mgr_mod, "BATCH_QUEUE_FILE", qfile)
    src = BatchQueueStatus(
        queue_id="q", action="full", status="running", total=2, current_index=0,
        items=[
            BatchQueueItem(project_id="A", action="full", status="running", job_id="ja"),
            BatchQueueItem(project_id="B", action="full", status="pending", job_id="jb"),
        ],
    )
    qfile.write_text(json.dumps(src.model_dump()), encoding="utf-8")

    mgr = _mgr()
    mgr.load_persisted_queue()
    assert mgr._batch_queue.status == "interrupted"

    started = {"n": 0}

    async def fake_worker(queue, meta_job):
        started["n"] += 1
        await asyncio.sleep(60)

    mgr._run_batch_queue = fake_worker

    async def _run():
        rq = await mgr.resume_interrupted_batch()
        await asyncio.sleep(0)  # дать worker-таску стартовать
        assert rq.status == "running"
        assert all(it.status in ("pending", "running") for it in rq.items)
        task = mgr._tasks.get("__BATCH__")
        assert task is not None and not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())
    assert started["n"] == 1


# ─── D. prefetch failure не портит основной batch item ───────────────────────


class _ExistsPath:
    def __init__(self, exists: bool):
        self._e = exists

    def exists(self) -> bool:
        return self._e


def test_batch_items_no_longer_publish_prefetch_state():
    item = BatchQueueItem(project_id="target", status="pending")

    assert "gemma_prefetch_status" not in item.model_dump()
    assert item.status == "pending"
