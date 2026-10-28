"""Durable reserve queues. Additive v1 schema, same SQLite conventions as accounts."""
from __future__ import annotations

import os
import fcntl
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from backend.app.core.config import APP_DATA_DIR

SCHEMA = """
CREATE TABLE IF NOT EXISTS reserve_meta (
  id INTEGER PRIMARY KEY CHECK(id=1), schema_version INTEGER NOT NULL,
  revision INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO reserve_meta VALUES (1, 1, 0);
CREATE TABLE IF NOT EXISTS reserve_tasks (
  id TEXT PRIMARY KEY, engineer_id TEXT NOT NULL, position INTEGER NOT NULL,
  title TEXT NOT NULL, object_id TEXT, project_id TEXT, version_id TEXT,
  forecast_date TEXT,
  check_type TEXT NOT NULL DEFAULT 'expert_review',
  state TEXT NOT NULL CHECK(state IN ('preparation','ready','in_progress','completed','cancelled')),
  started_at TEXT, completed_at TEXT, created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL, created_by TEXT NOT NULL,
  UNIQUE(object_id, project_id, version_id, check_type)
);
CREATE INDEX IF NOT EXISTS reserve_queue ON reserve_tasks(engineer_id, position);
CREATE TABLE IF NOT EXISTS reserve_operations (
  id TEXT PRIMARY KEY, actor TEXT NOT NULL, request_hash TEXT NOT NULL,
  kind TEXT NOT NULL, before_json TEXT NOT NULL, after_json TEXT NOT NULL,
  result_json TEXT NOT NULL, created_at TEXT NOT NULL, undone_by TEXT
);
"""


def db_path() -> Path:
    return Path(os.environ.get('AUDIT_RESERVE_DB') or APP_DATA_DIR / 'reserve_calendar.sqlite3')


@contextmanager
def transaction():
    path = db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # Serialize first-time WAL/schema initialization across threads and workers.
    lock = open(str(path) + '.lock', 'a+b')
    conn = None
    try:
        fcntl.flock(lock, fcntl.LOCK_EX)
        conn = sqlite3.connect(path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA busy_timeout=15000')
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA foreign_keys=ON')
        conn.executescript(SCHEMA)
        columns = {row[1] for row in conn.execute('PRAGMA table_info(reserve_tasks)')}
        if 'forecast_date' not in columns:
            conn.execute('ALTER TABLE reserve_tasks ADD COLUMN forecast_date TEXT')
        if conn.execute('SELECT schema_version FROM reserve_meta').fetchone()[0] != 1:
            raise RuntimeError('Unsupported reserve calendar schema')
        conn.execute('BEGIN IMMEDIATE')
        try:
            yield conn
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()
    finally:
        if conn is not None:
            conn.close()
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


def tasks(conn) -> list[dict]:
    return [dict(r) for r in conn.execute('SELECT * FROM reserve_tasks ORDER BY engineer_id, position, id')]


def revision(conn) -> int:
    return conn.execute('SELECT revision FROM reserve_meta WHERE id=1').fetchone()[0]


def bump(conn):
    conn.execute('UPDATE reserve_meta SET revision=revision+1 WHERE id=1')


def save_task(conn, task: dict):
    keys = list(task)
    conn.execute(
        f"INSERT INTO reserve_tasks ({','.join(keys)}) VALUES ({','.join('?' for _ in keys)}) "
        f"ON CONFLICT(id) DO UPDATE SET {','.join(k+'=excluded.'+k for k in keys if k != 'id')}",
        list(task.values()),
    )
