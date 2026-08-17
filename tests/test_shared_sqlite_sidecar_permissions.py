"""Regression: shared-state SQLite sidecars remain group-writable."""
from __future__ import annotations

import os
import sqlite3
import stat
from types import SimpleNamespace

from backend.app.services.distributed_workers import database
from backend.app.services.distributed_workers.state_permissions import SHARED_FILE_MODE


def test_shared_mode_normalizes_runtime_sqlite_sidecars(tmp_path):
    data_dir = tmp_path / "shared"
    data_dir.mkdir(mode=0o2770)
    db_path = data_dir / "workers.db"
    previous = os.umask(0o077)
    try:
        conn = sqlite3.connect(str(db_path), isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE probe(value INTEGER)")
        conn.execute("INSERT INTO probe VALUES (1)")
        conn.commit()
    finally:
        os.umask(previous)

    wal = db_path.with_name(db_path.name + "-wal")
    assert wal.exists()
    assert stat.S_IMODE(wal.stat().st_mode) == 0o600

    st = SimpleNamespace(db_path=db_path)
    database._normalize_shared_sqlite_files(st)
    assert stat.S_IMODE(db_path.stat().st_mode) == SHARED_FILE_MODE
    assert stat.S_IMODE(wal.stat().st_mode) == SHARED_FILE_MODE

    conn2 = sqlite3.connect(str(db_path), timeout=1.0, isolation_level=None)
    try:
        assert conn2.execute("SELECT COUNT(*) FROM probe").fetchone()[0] == 1
    finally:
        conn2.close()
