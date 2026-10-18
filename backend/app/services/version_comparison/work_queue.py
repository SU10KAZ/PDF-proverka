"""Durable local-only work queue. JSON operations, no arbitrary serialized callables."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path

OPERATIONS = frozenset({"sheet", "profile", "spec", "probe"})
TERMINAL = frozenset({"done", "failed", "cancelled"})
STOP_WAITERS = threading.Event()


class WorkError(RuntimeError):
    pass


class WorkCancelled(WorkError):
    pass


class WorkUnavailable(WorkError):
    pass


class WorkPaused(WorkError):
    """Пауза: задача ещё не начиналась и снята; идущая досчитывается."""


def enabled() -> bool:
    return os.environ.get("VERSION_COMPARISON_EXECUTOR", "").strip() == "worker"


def directory() -> Path:
    raw = os.environ.get("VERSION_COMPARISON_QUEUE_DIR", "").strip()
    if not raw:
        raise WorkError("Не настроена отдельная служба сравнения: VERSION_COMPARISON_QUEUE_DIR")
    return Path(raw).expanduser().resolve()


@contextmanager
def database():
    root = directory()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect(root / "queue.sqlite3", timeout=15)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("""CREATE TABLE IF NOT EXISTS work (
            id TEXT PRIMARY KEY, operation TEXT NOT NULL, args TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'queued', cost INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL,
            updated REAL NOT NULL, lease TEXT, result TEXT, error TEXT,
            request_key TEXT NOT NULL DEFAULT '')""")
        if "request_key" not in {row[1] for row in db.execute("PRAGMA table_info(work)")}:
            db.execute("ALTER TABLE work ADD COLUMN request_key TEXT NOT NULL DEFAULT ''")
        db.execute("CREATE INDEX IF NOT EXISTS work_pending ON work(state, created)")
        yield db
        db.commit()
    finally:
        db.close()


def submit(operation: str, args: list, *, key: str = "") -> str:
    if operation not in OPERATIONS:
        raise ValueError("unsupported computation")
    from .resources import estimate
    from .settings import ALGO_VERSION, PROFILE_VERSION
    payload = json.dumps(args, ensure_ascii=False, sort_keys=True, allow_nan=False)
    identifier = hashlib.sha256(json.dumps([ALGO_VERSION, PROFILE_VERSION, operation, payload, key]).encode()).hexdigest()
    now = time.time()
    with database() as db:
        db.execute("INSERT OR IGNORE INTO work(id, operation, args, cost, created, updated, request_key) VALUES(?,?,?,?,?,?,?)",
                   (identifier, operation, payload, estimate(operation, args), now, now, key))
    return identifier


def get(identifier: str) -> dict:
    with database() as db:
        row = db.execute("SELECT * FROM work WHERE id=?", (identifier,)).fetchone()
    if row is None:
        raise WorkError("Задача вычисления не найдена")
    return dict(row)


def cancel(identifier: str) -> None:
    with database() as db:
        db.execute("UPDATE work SET state='cancelled', updated=? WHERE id=? AND state IN ('queued','running')",
                   (time.time(), identifier))


def withdraw(identifier: str) -> bool:
    """Снять задачу, если она ещё ждёт в очереди (пауза); идущую не трогать. True — снята."""
    with database() as db:
        cur = db.execute("UPDATE work SET state='cancelled', updated=? WHERE id=? AND state='queued'",
                         (time.time(), identifier))
        return cur.rowcount > 0


def cancel_run(run_id: str) -> None:
    prefix = run_id + ":"
    with database() as db:
        db.execute("UPDATE work SET state='cancelled', updated=? WHERE substr(request_key,1,?)=? "
                   "AND state IN ('queued','running')", (time.time(), len(prefix), prefix))


def retry_failed(operation: str, args: list) -> None:
    identifier = submit(operation, args)
    with database() as db:
        db.execute("UPDATE work SET state='queued', attempts=0, error=NULL, updated=? "
                   "WHERE id=? AND state IN ('failed','cancelled')", (time.time(), identifier))


def heartbeat() -> dict:
    try:
        data = json.loads((directory() / "status.json").read_text())
        data["online"] = time.time() - data.get("updated", 0) < 15 and data.get("online", False)
        return data
    except (OSError, ValueError):
        return {"online": False}


def execute(operation: str, args: list, *, key: str = "", cancel_event: threading.Event | None = None,
            pause_event: threading.Event | None = None):
    identifier = submit(operation, args, key=key)
    withdrawn = False
    # No implicit execution inside the HTTP server if the separate worker is unavailable.
    unavailable_since = None
    while True:
        if STOP_WAITERS.is_set():
            raise WorkUnavailable("Веб-сервер перезапускается; вычисление сохранено")
        if cancel_event is not None and cancel_event.is_set():
            cancel(identifier)
            raise WorkCancelled("Расчёт отменён")
        if pause_event is not None and pause_event.is_set() and not withdrawn:
            if withdraw(identifier):
                raise WorkPaused("Пауза: расчёт листа не начинался")
            withdrawn = True  # уже идёт — дождаться результата
        task = get(identifier)
        if task["state"] == "done":
            return json.loads(task["result"])
        if task["state"] in ("failed", "cancelled"):
            error = WorkCancelled if task["state"] == "cancelled" else WorkError
            raise error(task["error"] or "Расчёт отменён")
        if heartbeat().get("online"):
            unavailable_since = None
        else:
            unavailable_since = unavailable_since or time.monotonic()
            if time.monotonic() - unavailable_since > 60:
                # Keep queued work intact; startup recovery can reconnect to its stable ID.
                raise WorkUnavailable("Служба сравнения недоступна; задача сохранена в очереди")
        if cancel_event is not None:
            cancel_event.wait(0.25)
        else:
            time.sleep(0.25)
