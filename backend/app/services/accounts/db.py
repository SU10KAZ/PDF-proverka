"""SQLite-файл аккаунтов портала.

Один файл вне каталогов релизов (по умолчанию ``APP_DATA_DIR/
portal_accounts.sqlite3``, переопределяется ``PORTAL_ACCOUNTS_DB``). WAL,
``busy_timeout``, внешние ключи — на каждом соединении, как в
``distributed_workers/database.py``. Соединение открывается на операцию:
10–15 пользователей, запись редкая, а соединение на поток с разделением между
потоками пула — лишний источник ошибок.

Синхронные функции этого модуля нельзя вызывать из event loop напрямую —
только через ``asyncio.to_thread`` (см. ``portal_auth``).
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

SCHEMA_VERSION = "1"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
  id               INTEGER PRIMARY KEY,
  login            TEXT NOT NULL,
  login_norm       TEXT NOT NULL UNIQUE,
  display_name     TEXT NOT NULL,
  contact          TEXT NOT NULL DEFAULT '',
  note             TEXT NOT NULL DEFAULT '',
  password_hash    TEXT NOT NULL,
  role             TEXT NOT NULL CHECK (role IN ('admin','viewer')),
  status           TEXT NOT NULL CHECK (status IN ('pending','active','rejected','blocked')),
  staff_id         TEXT UNIQUE,
  session_version  INTEGER NOT NULL DEFAULT 1,
  must_change_password INTEGER NOT NULL DEFAULT 0 CHECK (must_change_password IN (0,1)),
  status_reason    TEXT NOT NULL DEFAULT '',
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  decided_at       TEXT,
  decided_by       INTEGER REFERENCES accounts(id),
  last_login_at    TEXT,
  registration_ip  TEXT,
  legacy_login     TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS account_sections (
  account_id   INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  section      TEXT NOT NULL CHECK (section IN
               ('audit_results','version_comparison','stage_comparison','knowledge_base','schedule')),
  can_view     INTEGER NOT NULL CHECK (can_view IN (0,1)),
  can_download INTEGER NOT NULL CHECK (can_download IN (0,1)),
  CHECK (can_download = 0 OR can_view = 1),
  CHECK (can_download = 0 OR section NOT IN ('knowledge_base','schedule')),
  PRIMARY KEY (account_id, section)
);
-- Additive v1 extension: older releases safely ignore this permission table.
CREATE TABLE IF NOT EXISTS account_capabilities (
  account_id INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
  upload_projects INTEGER NOT NULL DEFAULT 0 CHECK (upload_projects IN (0,1))
);
-- Separate additive table: older releases may clear account_capabilities when
-- audit access is revoked; that must not affect calendar editing permissions.
CREATE TABLE IF NOT EXISTS account_schedule_permissions (
  account_id INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
  edit_schedule INTEGER NOT NULL DEFAULT 0 CHECK (edit_schedule IN (0,1))
);
CREATE TABLE IF NOT EXISTS auth_events (
  id INTEGER PRIMARY KEY,
  at TEXT NOT NULL,
  actor_account_id INTEGER,
  target_account_id INTEGER,
  event TEXT NOT NULL,
  details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS auth_throttle (
  key TEXT PRIMARY KEY,
  failures INTEGER NOT NULL,
  window_start REAL NOT NULL,
  locked_until REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""

_init_lock = threading.Lock()
_initialized: set[str] = set()


def db_path() -> Path:
    raw = (os.environ.get("PORTAL_ACCOUNTS_DB") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    from backend.app.core.config import APP_DATA_DIR

    return Path(APP_DATA_DIR) / "portal_accounts.sqlite3"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _open(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def exists() -> bool:
    return db_path().is_file()


def ensure_ready(*, create: bool = True) -> Path:
    """Создать/проверить схему. ``create=False`` — не создавать файл."""
    path = db_path()
    key = str(path)
    if key in _initialized and path.is_file():
        return path
    with _init_lock:
        if not path.is_file():
            if not create:
                raise FileNotFoundError(str(path))
            path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(str(path), os.O_CREAT | os.O_WRONLY, 0o600)
            os.close(fd)
        conn = _open(path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            row = None
            try:
                row = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
            except sqlite3.OperationalError:
                row = None
            if row is not None and row["value"] != SCHEMA_VERSION:
                raise RuntimeError(
                    f"portal_accounts: схема {row['value']}, код ожидает {SCHEMA_VERSION}"
                )
            conn.executescript(_SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,),
            )
        finally:
            conn.close()
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        _initialized.add(key)
    return path


@contextmanager
def read_conn() -> Iterator[sqlite3.Connection]:
    conn = _open(ensure_ready(create=False))
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_txn() -> Iterator[sqlite3.Connection]:
    """Транзакция записи: BEGIN IMMEDIATE, откат при исключении."""
    conn = _open(ensure_ready())
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
    finally:
        conn.close()


def backup(dest_dir: Path) -> Path:
    """Онлайн-копия базы (sqlite3 backup API) в ``dest_dir``."""
    src_path = ensure_ready(create=False)
    dest_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = dest_dir / f"portal_accounts.{stamp}.sqlite3"
    src = _open(src_path)
    try:
        out = sqlite3.connect(str(dest))
        try:
            src.backup(out)
        finally:
            out.close()
    finally:
        src.close()
    os.chmod(dest, 0o600)
    return dest


def reset_for_tests() -> None:
    _initialized.clear()


__all__ = ["SCHEMA_VERSION", "backup", "db_path", "ensure_ready", "exists",
           "read_conn", "reset_for_tests", "utc_now", "write_txn"]
