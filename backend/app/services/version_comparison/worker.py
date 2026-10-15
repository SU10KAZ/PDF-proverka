"""Separate, restartable supervisor for deterministic PDF computations only.

Run as a system service outside the interactive user's slice. Each task has a
fresh process, a memory reservation, a deadline and at most two attempts.
"""
from __future__ import annotations

import argparse
import ctypes
import fcntl
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from backend.app.services.common.atomic_json import atomic_write_json
from . import resources, work_queue as queue


def _native_environment() -> None:
    for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                "NUMEXPR_NUM_THREADS", "OPENCV_FOR_THREADS_NUM"):
        os.environ[key] = "1"


def perform(operation: str, args: list):
    if operation == "probe":
        import cv2
        import hashlib
        data = args[0]
        end = time.monotonic() + min(30, max(0, float(data.get("seconds", 0.1))))
        block = bytearray(min(32, max(1, int(data.get("mib", 1)))) * resources.MIB)
        count = 0
        while time.monotonic() < end:
            hashlib.sha256(block).digest()
            count += 1
        return {"pid": os.getpid(), "native_threads": cv2.getNumThreads(), "iterations": count}
    if operation == "profile":
        from . import profile
        root, object_id, sha = args
        os.environ["VERSION_COMPARISON_ROOT"] = root
        return profile.build(object_id, sha)["page_count"]
    if operation == "spec":
        from . import spec
        return spec.build(*args)
    if operation == "sheet":
        from . import engine
        task = args[0]
        out = Path(task["out_dir"])
        out.mkdir(parents=True, exist_ok=True)
        # Two web processes or recovered runs may request the same cached pair.
        with (out / "compute.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            result_path = out / "result.json"
            if result_path.exists():
                try:
                    result = json.loads(result_path.read_text())
                    regions = result.get("regions", [])
                    shifts = sum(r.get("class") in engine.SHIFT_CLASSES for r in regions)
                    return {"status": result["status"], "regions": len(regions) - shifts,
                            "style_regions": sum(r.get("class") == "style" for r in regions),
                            "shift_regions": shifts}
                except (ValueError, KeyError):
                    result_path.unlink()  # incomplete file left by a killed computation
            return engine.compute_sheet_pair(task)
    raise ValueError("unsupported computation")


def child(identifier: str, lease: str, parent: int) -> None:
    # Do not leave a compute process behind if the supervisor dies before systemd acts.
    if sys.platform == "linux":
        ctypes.CDLL(None).prctl(1, signal.SIGKILL, 0, 0, 0)
        if os.getppid() != parent:
            return
    _native_environment()
    from backend.app.services.common.cpu_pool import configure_worker_threads
    configure_worker_threads()
    task = queue.get(identifier)
    if task["state"] != "running" or task["lease"] != lease:
        return
    try:
        result = perform(task["operation"], json.loads(task["args"]))
        encoded = json.dumps(result, ensure_ascii=False, allow_nan=False)
        state, error = "done", None
    except Exception as exc:
        state, encoded, error = "failed", None, f"{type(exc).__name__}: {exc}"[:2000]
    with queue.database() as db:
        db.execute("UPDATE work SET state=?, result=?, error=?, updated=? "
                   "WHERE id=? AND lease=? AND state='running'",
                   (state, encoded, error, time.time(), identifier, lease))


def _terminate(process: subprocess.Popen) -> None:
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)
        except ProcessLookupError:
            process.wait(timeout=2)


class Supervisor:
    def __init__(self):
        self.budget = int(resources.number("VERSION_COMPARISON_MEMORY_GB", 4, 0.25) * resources.GIB)
        self.workers = int(resources.number("VERSION_COMPARISON_WORKERS", 12, 1))
        try:
            self.workers = min(self.workers, len(os.sched_getaffinity(0)))
        except AttributeError:
            self.workers = min(self.workers, os.cpu_count() or 1)
        self.deadline = resources.number("VERSION_COMPARISON_TASK_TIMEOUT", 900, 0.1)
        self.running: dict[str, dict] = {}
        self.owner = uuid.uuid4().hex
        self.stop = threading.Event()
        self.last_status = 0.0
        self.last_sample = 0.0

    def recover(self) -> None:
        with queue.database() as db:
            db.execute("UPDATE work SET state=CASE WHEN attempts<2 THEN 'queued' ELSE 'failed' END, "
                       "error='Вычислитель прерван; повтор ограничен двумя попытками', updated=? "
                       "WHERE state='running'", (time.time(),))

    def _failure(self, identifier: str, error: str) -> None:
        with queue.database() as db:
            db.execute("UPDATE work SET state=CASE WHEN attempts<2 THEN 'queued' ELSE 'failed' END, "
                       "error=?, updated=? WHERE id=? AND state='running'", (error, time.time(), identifier))

    def tick(self) -> None:
        state = resources.snapshot()
        for identifier, active in list(self.running.items()):
            proc = active["process"]
            task = queue.get(identifier)
            rss = resources.rss_bytes(proc.pid)
            active["rss"] = rss
            reason = None
            if task["state"] == "cancelled":
                reason = "Расчёт отменён"
            elif time.monotonic() - active["started"] > self.deadline:
                reason = "Превышено время расчёта листа"
            elif rss > active["reservation"]:
                reason = "Лист превысил резерв памяти; повтор выполняется отдельно"
            elif state["headroom_bytes"] < 128 * resources.MIB and len(self.running):
                # Stop admission AND reduce the active load before the ancestor limit is exhausted.
                largest = max(self.running, key=lambda k: self.running[k].get("rss", 0))
                if identifier == largest:
                    reason = "Недостаточно памяти в системной группе; расчёт отложен"
            if reason:
                _terminate(proc)
                self._failure(identifier, reason)
            if reason or proc.poll() is not None:
                if not reason and task["state"] not in queue.TERMINAL:
                    self._failure(identifier, f"Процесс вычисления завершился с кодом {proc.returncode}")
                proc.wait()
                active["log"].close()
                del self.running[identifier]

        with queue.database() as db:
            pending = [dict(r) for r in db.execute("SELECT * FROM work WHERE state='queued' ORDER BY created LIMIT 200")]
        blocked = ""
        for task in pending:
            if len(self.running) >= self.workers or self.stop.is_set():
                break
            if task["cost"] > self.budget:
                with queue.database() as db:
                    db.execute("UPDATE work SET state='failed', error=?, updated=? WHERE id=? AND state='queued'",
                               ("Лист требует больше памяти, чем бюджет вычислителя; увеличьте бюджет или обработайте лист частями",
                                time.time(), task["id"]))
                continue
            # Retry a crashed / oversized task on its own, never in a memory stampede.
            cost = self.budget if task["attempts"] else task["cost"]
            active_usage = [(x["reservation"], x.get("rss", 0)) for x in self.running.values()]
            if not resources.can_start(cost, active_usage, self.budget, resources.snapshot()):
                blocked = "Ожидание свободной памяти"
                break  # drain active work; a large old task must not starve behind small new tasks
            lease = uuid.uuid4().hex
            with queue.database() as db:
                changed = db.execute("UPDATE work SET state='running', attempts=attempts+1, lease=?, updated=? "
                                     "WHERE id=? AND state='queued'", (lease, time.time(), task["id"])).rowcount
            if not changed:
                continue
            log = (queue.directory() / f"{task['id']}.log").open("ab")
            try:
                proc = subprocess.Popen([sys.executable, "-m", "backend.app.services.version_comparison.worker", "--task", task["id"],
                                         "--lease", lease, "--parent", str(os.getpid())],
                                        stdout=log, stderr=log, start_new_session=True)
            except OSError as exc:
                log.close()
                self._failure(task["id"], f"Не удалось запустить вычислитель: {exc}")
                continue
            self.running[task["id"]] = {"process": proc, "log": log, "started": time.monotonic(),
                                         "reservation": cost, "rss": 0}
        if time.monotonic() - self.last_status > 1:
            self.last_status = time.monotonic()
            with queue.database() as db:
                counts = dict(db.execute("SELECT state, COUNT(*) FROM work GROUP BY state").fetchall())
            status = {
                "online": True, "updated": time.time(), "pid": os.getpid(), "owner": self.owner,
                "workers": self.workers, "active": len(self.running), "memory_budget_bytes": self.budget,
                "reserved_bytes": sum(x["reservation"] for x in self.running.values()),
                "resources": state, "counts": counts, "waiting_reason": blocked,
                "active_tasks": [{"id": identifier, "pid": active["process"].pid,
                                  "rss_bytes": active.get("rss", 0), "reserved_bytes": active["reservation"]}
                                 for identifier, active in self.running.items()],
            }
            atomic_write_json(queue.directory() / "status.json", status)
            if time.monotonic() - self.last_sample > 10:
                self.last_sample = time.monotonic()
                target = queue.directory() / time.strftime("metrics-%Y-%m-%d.jsonl", time.gmtime())
                with target.open("a") as output:
                    output.write(json.dumps(status, ensure_ascii=False) + "\n")
                    output.flush()
                    os.fsync(output.fileno())
                for old in queue.directory().glob("metrics-*.jsonl"):
                    if old.stat().st_mtime < time.time() - 14 * 86400:
                        old.unlink()

    def serve(self) -> None:
        _native_environment()
        queue.directory().mkdir(parents=True, exist_ok=True, mode=0o700)
        with (queue.directory() / "supervisor.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.recover()
            try:
                while not self.stop.is_set():
                    self.tick()
                    self.stop.wait(0.2)
            finally:
                for identifier, active in self.running.items():
                    _terminate(active["process"])
                    active["log"].close()
                    with queue.database() as db:
                        db.execute("UPDATE work SET state='queued', attempts=MAX(0,attempts-1), updated=? "
                                   "WHERE id=? AND state='running'", (time.time(), identifier))
                atomic_write_json(queue.directory() / "status.json", {"online": False, "updated": time.time()})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task")
    parser.add_argument("--lease")
    parser.add_argument("--parent", type=int)
    args = parser.parse_args()
    if args.task:
        child(args.task, args.lease, args.parent)
        return
    supervisor = Supervisor()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: supervisor.stop.set())
    supervisor.serve()


if __name__ == "__main__":
    main()
