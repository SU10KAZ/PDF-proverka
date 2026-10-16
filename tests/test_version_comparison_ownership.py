"""A second portal must not interrupt comparisons merely by reading them."""
import json

import pytest

from backend.app.services.common.atomic_json import atomic_write_json
from backend.app.services.version_comparison import jobs, paths, recovery, series, work_queue


@pytest.mark.parametrize("executor", ["worker", "process"])
@pytest.mark.parametrize("state", ["queued", "running", "cancelling"])
@pytest.mark.parametrize("live", [True, False])
def test_reading_foreign_series_respects_process_identity(tmp_path, monkeypatch, executor, state, live):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    monkeypatch.setenv("VERSION_COMPARISON_EXECUTOR", executor)
    monkeypatch.setattr(work_queue, "heartbeat", lambda: {"online": True, "active": 1})
    monkeypatch.setattr(work_queue, "cancel_run", lambda _: None)
    owner = recovery.identity() if live else {"pid": 99999999, "start": "0", "boot": "old"}
    run = {"run_id": "run-foreign", "state": state, "owner": "other-portal", "owner_process": owner,
           "items": [{"kind": "compare", "status": "running"}], "progress": {"total": 1, "done": 0}}
    data = {"series_id": "series-foreign", "state": state, "owner": "other-portal", "owner_process": owner,
            "pairs": [{"run_id": run["run_id"], "state": state}]}
    run_path = jobs._run_path("obj", run["run_id"])
    series_path = paths.series_path("obj", data["series_id"])
    atomic_write_json(run_path, run)
    atomic_write_json(series_path, data)

    result = series.get("obj", data["series_id"])
    expected = state if live else ("cancelled" if state == "cancelling" else "interrupted")
    assert result["state"] == expected
    assert result["pairs"][0]["run_state"] == expected
    if live:
        # GET must preserve both the persisted state and in-flight sheet status.
        assert json.loads(run_path.read_text()) == run
        assert json.loads(series_path.read_text()) == data


def test_portal_resume_marks_owner_so_series_server_does_not_take_over(tmp_path, monkeypatch):
    """16.10.2026: портал (без режима исполнителя) продолжил серию, не отметив себя владельцем;
    сервер серий счёл владельца мёртвым и повёл ту же серию параллельно — два прогона пары."""
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    monkeypatch.delenv("VERSION_COMPARISON_EXECUTOR", raising=False)
    import threading
    monkeypatch.setattr(series, "_cancel", {})
    monkeypatch.setattr(series, "_start", lambda oid, sid: series._cancel.setdefault(sid, threading.Event()))
    dead = {"pid": 99999999, "start": "0", "boot": "old"}
    data = {"series_id": "s1", "object_id": "obj", "project_id": "p", "state": "cancelled",
            "owner": "old-server", "owner_process": dead, "auto_resume": True,
            "pairs": [{"run_id": None, "state": "queued"}]}
    path = paths.series_path("obj", "s1")
    atomic_write_json(path, data)
    series.resume("obj", "s1")                                   # портал, обычный режим
    saved = json.loads(path.read_text())
    assert saved["owner_process"] == recovery.identity() and saved["state"] == "queued"
    # Другой процесс (иной BOOT_ID) видит живого владельца: не прерывает и не перехватывает.
    monkeypatch.setattr(jobs, "BOOT_ID", "series-server")
    assert series.get("obj", "s1")["state"] == "queued"
    with pytest.raises(Exception, match="уже выполняется"):
        series.resume("obj", "s1")


def test_run_resume_is_claimed_once(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    monkeypatch.delenv("VERSION_COMPARISON_EXECUTOR", raising=False)
    started = []
    import threading
    monkeypatch.setattr(jobs, "_cancel_events", {})

    def fake_start(oid, rid):
        started.append(rid)
        jobs._cancel_events.setdefault(rid, threading.Event())       # поток этого процесса «жив»
    monkeypatch.setattr(jobs, "_start", fake_start)
    run = {"run_id": "r1", "object_id": "obj", "state": "interrupted", "owner": "gone",
           "owner_process": {"pid": 99999999, "start": "0", "boot": "old"},
           "items": [{"kind": "compare", "status": "queued"}], "progress": {"total": 1, "done": 0}}
    atomic_write_json(jobs._run_path("obj", "r1"), run)
    jobs.resume_run("obj", "r1")
    assert json.loads(jobs._run_path("obj", "r1").read_text())["owner_process"] == recovery.identity()
    with pytest.raises(Exception, match="уже выполняется"):
        jobs.resume_run("obj", "r1")
    assert len(started) == 1
