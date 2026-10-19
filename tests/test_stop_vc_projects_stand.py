"""Real process groups: stop workers even after their server exits, safely."""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

import pytest

from scripts.server.stop_vc_projects_stand import _members, _process, stop


APP = '''
import multiprocessing
import os
import signal
import time
from pathlib import Path

def worker(index):
    if os.environ.get("STUBBORN") == "1":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    Path(f"../run/worker{index}.pid").write_text(str(os.getpid()))
    while True:
        time.sleep(0.1)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    ctx = multiprocessing.get_context("spawn")
    for i in range(2):
        ctx.Process(target=worker, args=(i,)).start()
    while True:
        time.sleep(0.1)
'''


@pytest.fixture
def stand(tmp_path):
    for directory in ("code", "app", "run"):
        (tmp_path / directory).mkdir()
    (tmp_path / "code/uvicorn.py").write_text(APP)
    processes = []

    def launch(stubborn=False):
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "stand_app:app", "--app-dir", str(tmp_path / "app")],
            cwd=tmp_path / "code", start_new_session=True,
            env={**os.environ, "STUBBORN": "1" if stubborn else "0"},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        processes.append(proc)
        (tmp_path / "run/stand.pid").write_text(str(proc.pid))
        deadline = time.monotonic() + 10
        while not all((tmp_path / f"run/worker{i}.pid").exists() for i in range(2)):
            assert proc.poll() is None, "fixture server exited"
            assert time.monotonic() < deadline, "fixture workers did not start"
            time.sleep(0.02)
        return proc

    yield tmp_path, launch
    for proc in processes:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=5)


@pytest.mark.parametrize("stubborn", [False, True])
def test_stop_removes_multiprocessing_workers_and_tracker(stand, stubborn):
    root, launch = stand
    proc = launch(stubborn)
    before = _members(proc.pid)
    assert len(before) == 4  # server, resource tracker, two spawn workers
    stopped = stop(root, timeout=0.3)
    proc.wait(timeout=3)
    assert set(stopped) == set(before)
    assert not _members(proc.pid)
    assert not (root / "run/stand.pid").exists()
    assert stop(root) == []


def test_stale_pid_file_does_not_signal_another_stand(stand, tmp_path):
    root, launch = stand
    proc = launch()
    wrong = tmp_path / "another-stand"
    (wrong / "run").mkdir(parents=True)
    (wrong / "run/stand.pid").write_text(str(proc.pid))
    before = set(_members(proc.pid))
    with pytest.raises(RuntimeError, match="another process"):
        stop(wrong, timeout=0)
    assert proc.poll() is None
    assert set(_members(proc.pid)) == before
    assert (wrong / "run/stand.pid").exists()


def test_stop_cleans_workers_after_server_has_already_exited(stand):
    root, launch = stand
    proc = launch(stubborn=True)
    proc.terminate()
    proc.wait(timeout=3)
    assert len(_members(proc.pid)) == 3
    assert len(stop(root, timeout=0.3)) == 3
    assert not _members(proc.pid)
    assert not (root / "run/stand.pid").exists()


def test_orphan_workers_from_another_stand_are_not_signalled(stand, tmp_path):
    root, launch = stand
    proc = launch(stubborn=True)
    proc.terminate()
    proc.wait(timeout=3)
    wrong = tmp_path / "another-stand"
    (wrong / "run").mkdir(parents=True)
    (wrong / "run/stand.pid").write_text(str(proc.pid))
    with pytest.raises(RuntimeError, match="another process"):
        stop(wrong, timeout=0)
    assert len(_members(proc.pid)) == 3


def test_refuses_callers_process_group(stand):
    root, _ = stand
    (root / "run/stand.pid").write_text(str(os.getpgrp()))
    with pytest.raises(RuntimeError, match="unsafe process group"):
        stop(root)


def test_absent_server_is_idempotent(stand):
    root, _ = stand
    proc = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
    proc.wait(timeout=5)
    assert _process(proc.pid) is None
    (root / "run/stand.pid").write_text(str(proc.pid))
    assert stop(root) == []
    assert not (root / "run/stand.pid").exists()
