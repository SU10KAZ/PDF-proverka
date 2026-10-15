"""Complete synthetic PDF flow through the durable compute service."""
from __future__ import annotations

import json
import threading

import pytest

from backend.app.services.common.atomic_json import atomic_write_json
from backend.app.services.version_comparison import jobs, paths, recovery, resources, work_queue as q, worker
from tests.test_version_comparison_api import client, _setup_project, _wait, OBJ


@pytest.fixture
def compute(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_EXECUTOR", "worker")
    monkeypatch.setenv("VERSION_COMPARISON_QUEUE_DIR", str(tmp_path / "work"))
    monkeypatch.setenv("VERSION_COMPARISON_MEMORY_GB", "4")
    monkeypatch.setenv("VERSION_COMPARISON_WORKERS", "2")
    monkeypatch.setenv("VERSION_COMPARISON_DPI", "72")
    monkeypatch.setattr(resources, "snapshot", lambda: {
        "headroom_bytes": 8 * resources.GIB, "pressure_avg10": 0, "limits": []})
    q.STOP_WAITERS.clear()
    supervisor = worker.Supervisor()
    thread = threading.Thread(target=supervisor.serve, daemon=True)
    thread.start()
    _wait(q.heartbeat, lambda d: d.get("online"), timeout=10)
    yield supervisor
    supervisor.stop.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    q.STOP_WAITERS.clear()


def test_full_api_uses_external_workers_and_recovers_completed_sheets(client, compute, tmp_path, monkeypatch):
    pid, vids, _ = _setup_project(client, tmp_path)
    base = f"/api/version-comparison/objects/{OBJ}"
    _wait(lambda: client.get(f"{base}/projects/{pid}/matrix").json(), lambda d: d["state"] == "ready")
    status = client.get("/api/version-comparison/worker-status").json()
    assert status["enabled"] and status["online"]
    run = client.post(f"{base}/projects/{pid}/runs", json={"a_version": vids[0], "b_version": vids[1]}).json()
    rid = run["run_id"]
    run = _wait(lambda: client.get(f"{base}/runs/{rid}").json(), lambda d: d["state"] in ("done", "failed"))
    assert run["state"] == "done", run
    assert run["progress"]["failed"] == 0
    assert run["auto_resume"]
    with q.database() as db:
        completed = dict(db.execute("SELECT id, attempts FROM work WHERE operation='sheet'").fetchall())
    assert completed and all(attempt == 1 for attempt in completed.values())
    # Emulate a web-server restart after the final sheets, before final run status was persisted.
    run.update(state="running", owner="old-process", owner_process={"pid": 99999999})
    atomic_write_json(jobs._run_path(OBJ, rid), run)
    assert recovery.recover_once() == [rid]
    run = _wait(lambda: client.get(f"{base}/runs/{rid}").json(), lambda d: d["state"] == "done")
    with q.database() as db:
        after = dict(db.execute("SELECT id, attempts FROM work WHERE operation='sheet'").fetchall())
    assert after == completed


def test_recovery_never_launches_legacy_cancelled_or_other_pipeline_work(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    monkeypatch.setenv("VERSION_COMPARISON_EXECUTOR", "worker")
    monkeypatch.setattr(q, "heartbeat", lambda: {"online": True})
    for rid, state, opted in [("legacy", "running", False), ("cancelled", "cancelled", True), ("failed", "failed", True)]:
        atomic_write_json(jobs._run_path(OBJ, rid), {
            "object_id": OBJ, "run_id": rid, "state": state, "auto_resume": opted,
            "owner": "old", "items": [], "schema": "version_comparison.run/v1"})
    monkeypatch.setattr(jobs, "resume_run", lambda *a: pytest.fail("unexpected resume"))
    assert recovery.recover_once() == []


def test_live_other_process_is_not_marked_interrupted(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    monkeypatch.setenv("VERSION_COMPARISON_EXECUTOR", "worker")
    atomic_write_json(jobs._run_path(OBJ, "live"), {
        "run_id": "live", "state": "running", "owner": "another-web-process",
        "owner_process": recovery.identity(), "items": []})
    assert jobs.get_run(OBJ, "live")["state"] == "running"


def test_cancel_survives_web_restart_and_cancels_durable_children(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("VERSION_COMPARISON_QUEUE_DIR", str(tmp_path / "queue"))
    monkeypatch.setenv("VERSION_COMPARISON_EXECUTOR", "worker")
    identifier = q.submit("probe", [{}], key="cancelled-run:sheet:0")
    other = q.submit("probe", [{}], key="cancelled-run-other:sheet:0")
    atomic_write_json(jobs._run_path(OBJ, "cancelled-run"), {
        "run_id": "cancelled-run", "state": "cancelling", "owner": "dead", "auto_resume": True,
        "items": [{"status": "running"}]})
    assert jobs.get_run(OBJ, "cancelled-run")["state"] == "cancelled"
    assert q.get(identifier)["state"] == "cancelled"
    assert q.get(other)["state"] == "queued"
