"""Resource admission, real child failures, durable restart and cancellation."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from backend.app.services.version_comparison import resources, work_queue as q, worker


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_QUEUE_DIR", str(tmp_path / "queue"))
    monkeypatch.setenv("VERSION_COMPARISON_MEMORY_GB", "1")
    monkeypatch.setenv("VERSION_COMPARISON_WORKERS", "2")
    monkeypatch.setenv("VERSION_COMPARISON_TASK_TIMEOUT", "10")
    q.STOP_WAITERS.clear()
    monkeypatch.setattr(resources, "snapshot", lambda: {
        "headroom_bytes": 8 * resources.GIB, "pressure_avg10": 0, "limits": []})
    supervisors = []
    def make():
        s = worker.Supervisor()
        supervisors.append(s)
        return s
    yield make
    for s in supervisors:
        for active in s.running.values():
            worker._terminate(active["process"])
            active["log"].close()
    q.STOP_WAITERS.clear()


def until(supervisor, predicate, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        supervisor.tick()
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("worker did not reach expected state")


def test_ancestor_high_limit_counts_even_with_free_host_memory(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_HOST_RESERVE_GB", "4")
    proc, cg = tmp_path / "proc", tmp_path / "cgroup"
    (proc / "self").mkdir(parents=True)
    (proc / "meminfo").write_text("MemAvailable: 33554432 kB\n")
    (proc / "self/cgroup").write_text("0::/user.slice/user-1001.slice/worker\n")
    child = cg / "user.slice/user-1001.slice/worker"
    child.mkdir(parents=True)
    (child / "memory.current").write_text(str(2 * resources.GIB))
    (child / "memory.max").write_text(str(32 * resources.GIB))
    parent = child.parent
    (parent / "memory.current").write_text(str(15 * resources.GIB))
    (parent / "memory.high").write_text(str(16 * resources.GIB))
    state = resources.snapshot(proc, cg)
    assert state["headroom_bytes"] == resources.GIB // 2
    assert not resources.can_start(resources.GIB, [], 20 * resources.GIB, state)


def test_admission_reserves_future_growth_and_stops_under_pressure():
    gib = resources.GIB
    state = {"headroom_bytes": 4 * gib, "pressure_avg10": 0}
    assert not resources.can_start(2 * gib, [(4 * gib, gib)], 10 * gib, state)
    assert resources.can_start(gib, [(4 * gib, gib)], 10 * gib, state)
    assert not resources.can_start(gib, [], 10 * gib, state | {"pressure_avg10": 20})


def sheet_args(width, height):
    return [{"params": {"dpi": 200},
             "a": {"profile": {"width": width, "height": height}},
             "b": {"profile": {"width": width, "height": height}}}]


def test_sheet_budget_admits_five_a1_or_three_a0_without_oversubscription():
    budget = 18 * resources.GIB
    state = {"headroom_bytes": budget, "pressure_avg10": 0}
    for dims, count in [((1684, 2384), 5), ((2384, 3370), 3)]:
        cost = resources.estimate("sheet", sheet_args(*dims))
        assert resources.can_start(cost, [(cost, 0)] * (count - 1), budget, state)
        assert not resources.can_start(cost, [(cost, 0)] * count, budget, state)


def test_sheet_estimate_covers_cross_orientation_and_large_source_files(tmp_path):
    args = sheet_args(1684, 2384)
    normal = resources.estimate("sheet", args)
    args[0]["b"]["profile"].update(width=2384, height=1684)
    assert resources.estimate("sheet", args) > normal
    pdf = tmp_path / "large.pdf"
    with pdf.open("wb") as stream:
        stream.truncate(200 * resources.MIB)
    args[0]["a"]["pdf"] = str(pdf)
    assert resources.estimate("sheet", args) >= normal + 800 * resources.MIB
    for side in ("a", "b"):
        args[0][side]["profile"]["content_hash"] = "same-page-content"
    assert resources.estimate("sheet", args) == 384 * resources.MIB


def costed_probe(key, mib, seconds=3):
    identifier = q.submit("probe", [{"seconds": seconds}], key=key)
    with q.database() as db:
        db.execute("UPDATE work SET cost=? WHERE id=?", (mib * resources.MIB, identifier))
    return identifier


def test_backfill_uses_remaining_budget_without_starving_old_work(runtime, monkeypatch):
    monkeypatch.setattr(worker, "BACKFILL_LIMIT", 1)
    s = runtime()
    old = costed_probe("active", 600)
    s.tick()
    big = costed_probe("waiting-large", 700)
    small = costed_probe("fits", 250, 0.1)
    s.tick()
    assert q.get(big)["state"] == "queued"
    assert q.get(small)["state"] == "running"
    assert sum(t["reservation"] for t in s.running.values()) <= s.budget
    until(s, lambda: small not in s.running)
    later = costed_probe("cannot-overtake-again", 250, 0.1)
    s.tick()
    assert q.get(later)["state"] == "queued"
    q.cancel(old)
    s.tick()
    assert q.get(big)["state"] == "running"


@pytest.mark.parametrize("exclusive_retry", [False, True])
def test_backfill_drains_after_wait_limit_and_before_exclusive_retry(runtime, exclusive_retry):
    s = runtime()
    costed_probe("active", 600)
    s.tick()
    big = costed_probe("large", 700)
    if exclusive_retry:
        with q.database() as db:
            db.execute("UPDATE work SET attempts=1 WHERE id=?", (big,))
    else:
        s.blocked[big] = {"since": time.monotonic() - worker.BACKFILL_SECONDS - 1, "bypasses": 0}
    small = costed_probe("small", 250)
    s.tick()
    assert q.get(big)["state"] == "queued"
    assert q.get(small)["state"] == "queued"


def test_underestimated_memory_retries_alone_with_full_budget(runtime):
    s = runtime()
    underestimated = costed_probe("underestimated", 1, 0.2)
    healthy = costed_probe("healthy-neighbour", 256, 0.5)
    until(s, lambda: underestimated in s.running and q.get(underestimated)["attempts"] == 2)
    assert q.get(healthy)["state"] == "done"
    assert len(s.running) == 1
    assert s.running[underestimated]["reservation"] == s.budget
    until(s, lambda: q.get(underestimated)["state"] == "done")
    assert q.get(underestimated)["attempts"] == 2
    assert q.get(healthy)["attempts"] == 1


def test_retry_uses_available_headroom_and_remains_exclusive(runtime, monkeypatch):
    # Idle page cache makes cgroup headroom slightly smaller than the budget.
    # The old full-budget reservation waited forever with no active children.
    headroom = 768 * resources.MIB
    monkeypatch.setattr(resources, "snapshot", lambda: {
        "headroom_bytes": headroom, "pressure_avg10": 0, "limits": []})
    s = runtime()
    retry = costed_probe("retry-below-budget", 256, 0.5)
    with q.database() as db:
        db.execute("UPDATE work SET attempts=1 WHERE id=?", (retry,))
    later = costed_probe("would-fit-budget-spare", 128, 0.1)
    s.tick()
    assert q.get(retry)["state"] == "running"
    assert q.get(retry)["attempts"] == 2
    assert s.running[retry]["reservation"] == headroom < s.budget
    assert q.get(later)["state"] == "queued"
    s.tick()
    assert list(s.running) == [retry]
    assert q.get(later)["state"] == "queued"
    until(s, lambda: q.get(retry)["state"] == "done" and q.get(later)["state"] == "done")


@pytest.mark.parametrize("headroom,pressure", [(128, 0), (768, 20)])
def test_retry_still_obeys_minimum_estimate_and_memory_pressure(runtime, monkeypatch, headroom, pressure):
    monkeypatch.setattr(resources, "snapshot", lambda: {
        "headroom_bytes": headroom * resources.MIB, "pressure_avg10": pressure, "limits": []})
    s = runtime()
    retry = costed_probe("unsafe-retry", 256)
    with q.database() as db:
        db.execute("UPDATE work SET attempts=1 WHERE id=?", (retry,))
    later = costed_probe("must-not-overtake-retry", 64)
    s.tick()
    assert not s.running
    assert q.get(retry)["state"] == q.get(later)["state"] == "queued"
    assert q.get(retry)["attempts"] == 1


def test_real_processes_run_parallel_and_use_one_native_thread(runtime):
    s = runtime()
    identifiers = [q.submit("probe", [{"seconds": 0.2}], key=str(i)) for i in range(2)]
    until(s, lambda: all(q.get(i)["state"] == "done" for i in identifiers))
    results = [json.loads(q.get(i)["result"]) for i in identifiers]
    assert len({r["pid"] for r in results}) == 2
    assert all(r["pid"] != os.getpid() and r["native_threads"] == 1 for r in results)
    assert all(q.get(i)["attempts"] == 1 for i in identifiers)


def test_oversize_task_fails_without_starting_and_small_task_still_finishes(runtime):
    s = runtime()
    big = q.submit("probe", [{}], key="big")
    with q.database() as db:
        db.execute("UPDATE work SET cost=? WHERE id=?", (2 * resources.GIB, big))
    small = q.submit("probe", [{}], key="small")
    until(s, lambda: q.get(small)["state"] == "done")
    assert q.get(big)["state"] == "failed"
    assert q.get(big)["attempts"] == 0


def test_cancellation_terminates_only_cancelled_child(runtime):
    s = runtime()
    cancelled = q.submit("probe", [{"seconds": 5}], key="cancel")
    healthy = q.submit("probe", [{"seconds": 0.2}], key="healthy")
    s.tick()
    proc = s.running[cancelled]["process"]
    q.cancel(cancelled)
    until(s, lambda: q.get(healthy)["state"] == "done" and proc.poll() is not None)
    assert q.get(cancelled)["state"] == "cancelled"
    assert q.get(cancelled)["attempts"] == 1


def test_killed_task_retries_once_then_fails_without_poisoning_queue(runtime):
    s = runtime()
    identifier = q.submit("probe", [{"seconds": 5}])
    for attempt in (1, 2):
        until(s, lambda: identifier in s.running and q.get(identifier)["attempts"] == attempt)
        proc = s.running[identifier]["process"]
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()
        s.tick()
    assert q.get(identifier)["state"] == "failed"
    assert q.get(identifier)["attempts"] == 2
    healthy = q.submit("probe", [{}], key="after-crash")
    until(s, lambda: q.get(healthy)["state"] == "done")


def test_timeout_is_bounded_and_does_not_retry_forever(runtime, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_TASK_TIMEOUT", "0.2")
    s = runtime()
    identifier = q.submit("probe", [{"seconds": 5}])
    until(s, lambda: q.get(identifier)["state"] == "failed")
    assert q.get(identifier)["attempts"] == 2
    assert "время" in q.get(identifier)["error"]


def test_recovery_preserves_done_and_cancelled_and_bounds_crash_attempts(runtime):
    s = runtime()
    identifiers = [q.submit("probe", [{}], key=str(i)) for i in range(4)]
    with q.database() as db:
        for i, state, attempts in zip(identifiers, ["done", "cancelled", "running", "running"], [1, 1, 1, 2]):
            db.execute("UPDATE work SET state=?,attempts=? WHERE id=?", (state, attempts, i))
    s.recover()
    assert [q.get(i)["state"] for i in identifiers] == ["done", "cancelled", "queued", "failed"]


def test_submission_is_idempotent_and_rejects_arbitrary_operations(runtime):
    assert q.submit("probe", [{}]) == q.submit("probe", [{}])
    with pytest.raises(ValueError):
        q.submit("arbitrary.module.function", [])


def test_waiter_shutdown_keeps_durable_work(runtime):
    q.STOP_WAITERS.set()
    with pytest.raises(q.WorkUnavailable):
        q.execute("probe", [{}])
    identifier = q.submit("probe", [{}])
    assert q.get(identifier)["state"] == "queued"


def test_supervisor_process_restart_recovers_durable_task(runtime):
    # Carry the fixture's controlled memory snapshot into the child process.
    # This tests durable recovery; admission limits have separate tests above.
    command = [sys.executable, "-c",
               "from backend.app.services.version_comparison import resources, worker; "
               "resources.snapshot = lambda: dict(headroom_bytes=8*resources.GIB, "
               "pressure_avg10=0, limits=[]); worker.main()"]
    processes = []
    identifier = q.submit("probe", [{"seconds": 1}], key="restart")
    def wait_for(predicate):
        end = time.monotonic() + 15
        while time.monotonic() < end:
            if predicate():
                return
            time.sleep(0.05)
        raise AssertionError("supervisor restart timed out")
    try:
        first = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(first)
        wait_for(lambda: q.get(identifier)["state"] == "running")
        first.kill()
        first.wait(timeout=3)
        second = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(second)
        wait_for(lambda: q.get(identifier)["state"] in q.TERMINAL)
        assert q.get(identifier)["state"] == "done", q.get(identifier)
        assert q.get(identifier)["attempts"] == 2
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
