"""Resume only explicitly opted-in deterministic runs; never other pipelines."""
from __future__ import annotations

import fcntl
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from . import paths, work_queue

log = logging.getLogger(__name__)


def identity(pid: int | None = None) -> dict:
    pid = pid or os.getpid()
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        return {"pid": pid, "start": stat[stat.rfind(")") + 2:].split()[19],
                "boot": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}
    except OSError:
        return {}


def owner_alive(owner: dict | None) -> bool:
    return bool(owner and owner == identity(owner.get("pid")))


def recover_once() -> list[str]:
    if not work_queue.enabled() or not work_queue.heartbeat().get("online"):
        return []
    from . import jobs, series
    recovered = []
    referenced = set()
    for path in sorted((paths.root() / "objects").glob("*/series/*/series.json")):
        data = json.loads(path.read_text())
        if not data.get("auto_resume") or data.get("state") not in ("queued", "running", "interrupted"):
            continue
        referenced.update(p.get("run_id") for p in data["pairs"])
        if owner_alive(data.get("owner_process")) and data.get("owner") != jobs.BOOT_ID:
            continue
        current = series.get(data["object_id"], data["series_id"])
        if current["state"] == "interrupted":
            series.resume(data["object_id"], data["series_id"])
            recovered.append(data["series_id"])
    for path in sorted((paths.root() / "objects").glob("*/runs/*/run.json")):
        data = json.loads(path.read_text())
        if data.get("auto_resume") and data.get("state") == "cancelling":
            jobs.get_run(data["object_id"], data["run_id"])
            continue
        if (not data.get("auto_resume") or data["run_id"] in referenced
                or data.get("state") not in ("queued", "running", "interrupted")):
            continue
        if owner_alive(data.get("owner_process")) and data.get("owner") != jobs.BOOT_ID:
            continue
        current = jobs.get_run(data["object_id"], data["run_id"])
        if current["state"] == "interrupted":
            jobs.resume_run(data["object_id"], data["run_id"])
            recovered.append(data["run_id"])
    return recovered


def _loop(stop: threading.Event) -> None:
    paths.root().mkdir(parents=True, exist_ok=True)
    with (paths.root() / "recovery.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        while not stop.is_set():
            try:
                resumed = recover_once()
                if resumed:
                    log.info("Recovered deterministic comparisons: %s", resumed)
            except Exception:
                log.exception("Comparison recovery failed; will retry on the next check")
            stop.wait(10)


@asynccontextmanager
async def lifespan(_app):
    stop = threading.Event()
    thread = None
    from . import batches
    batch_thread = threading.Thread(target=batches.run, args=(stop,), name="vc-batches", daemon=True)
    work_queue.STOP_WAITERS.clear()
    batch_thread.start()
    if work_queue.enabled():
        thread = threading.Thread(target=_loop, args=(stop,), name="vc-recovery", daemon=True)
        thread.start()
    try:
        yield
    finally:
        stop.set()
        if work_queue.enabled():
            work_queue.STOP_WAITERS.set()
        if thread:
            thread.join(timeout=2)
        batch_thread.join(timeout=2)
