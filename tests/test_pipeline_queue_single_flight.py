"""reserc.md #67 — single-flight guard на старт Qwen→Opus pipeline.

find_active_pipeline_job возвращает активный (running/queued с ЖИВОЙ Task) job
сессии → роутер отклоняет второй параллельный старт (один LM Studio инстанс).
Stale-job (running без живой Task после рестарта) активным НЕ считается.
"""
from __future__ import annotations

import json

from backend.app.services.stage_comparison import pipeline_queue as pq


class _FakeTask:
    def __init__(self, done: bool):
        self._done = done

    def done(self) -> bool:
        return self._done


def _write_job(d, job_id, status):
    (d / f"{job_id}.json").write_text(
        json.dumps({"job_id": job_id, "status": status}), encoding="utf-8"
    )


def test_no_jobs_no_active(tmp_path, monkeypatch):
    monkeypatch.setattr(pq, "_jobs_dir", lambda sid: tmp_path)
    monkeypatch.setattr(pq, "_active_tasks", {})
    assert pq.find_active_pipeline_job("s") is None


def test_live_task_is_active(tmp_path, monkeypatch):
    _write_job(tmp_path, "j1", "running")
    monkeypatch.setattr(pq, "_jobs_dir", lambda sid: tmp_path)
    monkeypatch.setattr(pq, "_active_tasks", {"s": {"j1": _FakeTask(False)}})
    res = pq.find_active_pipeline_job("s")
    assert res is not None and res["job_id"] == "j1"


def test_stale_running_without_task_not_active(tmp_path, monkeypatch):
    # running, но нет живой Task (рестарт uvicorn) → не блокирует новый старт.
    _write_job(tmp_path, "j1", "running")
    monkeypatch.setattr(pq, "_jobs_dir", lambda sid: tmp_path)
    monkeypatch.setattr(pq, "_active_tasks", {})
    assert pq.find_active_pipeline_job("s") is None


def test_done_task_not_active(tmp_path, monkeypatch):
    _write_job(tmp_path, "j1", "running")
    monkeypatch.setattr(pq, "_jobs_dir", lambda sid: tmp_path)
    monkeypatch.setattr(pq, "_active_tasks", {"s": {"j1": _FakeTask(True)}})
    assert pq.find_active_pipeline_job("s") is None


def test_finished_job_not_active(tmp_path, monkeypatch):
    _write_job(tmp_path, "j1", "done")
    monkeypatch.setattr(pq, "_jobs_dir", lambda sid: tmp_path)
    monkeypatch.setattr(pq, "_active_tasks", {"s": {"j1": _FakeTask(False)}})
    assert pq.find_active_pipeline_job("s") is None
