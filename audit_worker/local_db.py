"""Локальная база воркера: точка стыка сетевого агента и исполнителя.

Зачем появилась. На этапе 0 агент и работа жили в одном процессе: сеть, опрос
центра и запуск тестового процесса делал один и тот же питон. Из этого
следовало ровно то, что и должно было: перезапуск СЕТЕВОГО агента убивал
РАБОТУ. Разделить их можно, только если у них есть общий транзакционный стык —
файлы через `os.replace` для «атомарно захватить задание» не годятся: два
исполнителя одновременно прочитают одно и то же и запустят два процесса.

Отсюда SQLite WAL и та же дисциплина, что на центре (ADR-007):
  * один писатель под threading.Lock, читатели — свои соединения;
  * захват задания — `BEGIN IMMEDIATE` + условный UPDATE, а не «прочитал и
    записал»;
  * миграции нумерованные, вперёд-только, каждая одной транзакцией.

Что здесь НЕ хранится: worker-токен, bootstrap-секрет, любые учётные данные.
База доступна исполнителю, а исполнителю сетевые секреты не положены (§8.2).
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

SCHEMA_VERSION = 1

PRAGMAS = (
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA busy_timeout = 10000",
    "PRAGMA foreign_keys = ON",
)

# Состояния локальной очереди. `interrupted` — диагностическое: процесс исчез
# без маркера завершения, и автоматически перезапускать его НЕЛЬЗЯ (§8.6).
QUEUE_QUEUED = "queued"
QUEUE_CLAIMED = "claimed"
QUEUE_RUNNING = "running"
QUEUE_FINISHED = "finished"
QUEUE_FAILED = "failed"
QUEUE_CANCELLED = "cancelled"
QUEUE_INTERRUPTED = "executor_interrupted"

TERMINAL_QUEUE_STATES = frozenset(
    {QUEUE_FINISHED, QUEUE_FAILED, QUEUE_CANCELLED, QUEUE_INTERRUPTED}
)

LEASE_SEC = 120.0

_MIGRATION_1 = """
CREATE TABLE IF NOT EXISTS executor_instances (
    executor_instance_id   TEXT PRIMARY KEY,
    process_pid            INTEGER NOT NULL,
    -- Тик старта из /proc: голый pid переиспользуется системой, и без метки
    -- времени чужой процесс легко принять за свой (I-17).
    process_start_identity REAL,
    version                TEXT,
    started_at             REAL NOT NULL,
    last_heartbeat_at      REAL NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'online'
);

CREATE TABLE IF NOT EXISTS execution_queue (
    attempt_id          TEXT PRIMARY KEY,
    job_id              TEXT NOT NULL,
    job_type            TEXT NOT NULL,
    state               TEXT NOT NULL DEFAULT 'queued',
    claimed_by_executor TEXT,
    claim_generation    INTEGER NOT NULL DEFAULT 0,
    claimed_at          REAL,
    lease_expires_at    REAL,
    created_at          REAL NOT NULL,
    updated_at          REAL NOT NULL,
    params_json         TEXT NOT NULL DEFAULT '{}',
    result_json         TEXT
);
CREATE INDEX IF NOT EXISTS ix_queue_state ON execution_queue(state);

CREATE TABLE IF NOT EXISTS local_commands (
    local_command_id   TEXT PRIMARY KEY,
    command_type       TEXT NOT NULL,
    job_id             TEXT,
    attempt_id         TEXT,
    payload_json       TEXT NOT NULL DEFAULT '{}',
    status             TEXT NOT NULL DEFAULT 'pending',
    created_at         REAL NOT NULL,
    claimed_at         REAL,
    completed_at       REAL,
    result_json        TEXT,
    -- Идемпотентность сквозная: одна команда центра → одна локальная.
    central_command_id TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_local_cmd_status ON local_commands(status);

CREATE TABLE IF NOT EXISTS process_registry (
    attempt_id             TEXT PRIMARY KEY,
    job_id                 TEXT NOT NULL,
    executor_instance_id   TEXT,
    pid                    INTEGER,
    process_start_identity REAL,
    command_fingerprint    TEXT,
    process_group_id       INTEGER,
    started_at             REAL,
    last_observed_at       REAL,
    status                 TEXT NOT NULL DEFAULT 'running',
    exit_code              INTEGER
);
"""

MIGRATIONS: dict[int, str] = {1: _MIGRATION_1}


class LocalDB:
    """Соединение к worker.db. Один экземпляр на процесс (agent или executor)."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._local = threading.local()
        self._migrate()

    # ─── Соединения ──────────────────────────────────────────────────────────
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=15.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        for pragma in PRAGMAS:
            conn.execute(pragma)
        return conn

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
        return conn

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            conn = self._conn()
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    def read(self) -> sqlite3.Connection:
        return self._conn()

    def _migrate(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)"
            )
            row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
            version = int(row[0]) if row and row[0] is not None else 0
            for target in sorted(MIGRATIONS):
                if target <= version:
                    continue
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for statement in MIGRATIONS[target].split(";"):
                        if statement.strip():
                            conn.execute(statement)
                    conn.execute(
                        "INSERT INTO schema_migrations (version, applied_at) VALUES (?,?)",
                        (target, time.time()),
                    )
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")
        finally:
            conn.close()

    # ─── Исполнители ─────────────────────────────────────────────────────────
    def register_executor(self, *, version: str) -> str:
        from audit_worker import process_registry as procinfo

        instance_id = f"exe_{uuid.uuid4().hex[:12]}"
        now = time.time()
        pid = os.getpid()
        with self.write() as conn:
            conn.execute(
                "INSERT INTO executor_instances (executor_instance_id, process_pid, "
                "process_start_identity, version, started_at, last_heartbeat_at, status) "
                "VALUES (?,?,?,?,?,?,?)",
                (instance_id, pid, procinfo.process_start_time(pid), version,
                 now, now, "online"),
            )
        return instance_id

    def executor_heartbeat(self, instance_id: str, *, status: str = "online") -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE executor_instances SET last_heartbeat_at = ?, status = ? "
                "WHERE executor_instance_id = ?",
                (time.time(), status, instance_id),
            )

    def executor_stopped(self, instance_id: str) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE executor_instances SET status = 'offline', last_heartbeat_at = ? "
                "WHERE executor_instance_id = ?",
                (time.time(), instance_id),
            )

    def latest_executor(self) -> Optional[dict[str, Any]]:
        """Самый свежий исполнитель. Живость решает вызывающий, а не флаг в БД."""
        row = self.read().execute(
            "SELECT * FROM executor_instances ORDER BY last_heartbeat_at DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def executor_snapshot(self, *, stale_sec: float = 90.0) -> dict[str, Any]:
        """Состояние исполнителя для heartbeat агента.

        Статус вычисляется по СВЕЖЕСТИ отметки и живости pid, а не по колонке
        `status`: упавший процесс свою строку в 'offline' не переведёт.
        """
        from audit_worker import process_registry as procinfo

        row = self.latest_executor()
        if row is None:
            return {"status": "offline", "running_processes": 0, "ambiguous_processes": 0}
        alive = procinfo.is_alive(
            int(row["process_pid"]), row["process_start_identity"]
        )
        age = time.time() - float(row["last_heartbeat_at"])
        if not alive:
            status = "offline"
        elif age <= stale_sec:
            status = "online"
        else:
            status = "stale"
        if row["status"] == "offline" and not alive:
            status = "offline"
        procs = self.list_processes()
        return {
            "executor_instance_id": row["executor_instance_id"],
            "status": status,
            "last_heartbeat_at": row["last_heartbeat_at"],
            "version": row["version"],
            "running_processes": sum(1 for p in procs if p["status"] == "running"),
            "ambiguous_processes": sum(1 for p in procs if p["status"] == "ambiguous"),
        }

    # ─── Очередь исполнения ──────────────────────────────────────────────────
    def enqueue(
        self, *, job_id: str, attempt_id: str, job_type: str, params: dict[str, Any]
    ) -> bool:
        """Поставить попытку в локальную очередь. Повтор безопасен."""
        now = time.time()
        with self.write() as conn:
            existing = conn.execute(
                "SELECT state FROM execution_queue WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
            if existing is not None:
                return False
            conn.execute(
                "INSERT INTO execution_queue (attempt_id, job_id, job_type, state, "
                "created_at, updated_at, params_json) VALUES (?,?,?,?,?,?,?)",
                (attempt_id, job_id, job_type, QUEUE_QUEUED, now, now,
                 json.dumps(params, ensure_ascii=False)),
            )
        return True

    def claim_next(self, executor_instance_id: str) -> Optional[dict[str, Any]]:
        """Атомарно захватить одну попытку.

        Захват — условный UPDATE под BEGIN IMMEDIATE. Два одновременно
        запущенных исполнителя не могут получить одну строку: проигравший
        увидит rowcount == 0 (§8.4).
        """
        now = time.time()
        with self.write() as conn:
            row = conn.execute(
                "SELECT * FROM execution_queue WHERE state = ? "
                "ORDER BY created_at ASC LIMIT 1",
                (QUEUE_QUEUED,),
            ).fetchone()
            if row is None:
                return None
            cur = conn.execute(
                "UPDATE execution_queue SET state = ?, claimed_by_executor = ?, "
                "claim_generation = claim_generation + 1, claimed_at = ?, "
                "lease_expires_at = ?, updated_at = ? "
                "WHERE attempt_id = ? AND state = ?",
                (QUEUE_CLAIMED, executor_instance_id, now, now + LEASE_SEC, now,
                 row["attempt_id"], QUEUE_QUEUED),
            )
            if cur.rowcount != 1:
                return None
            claimed = dict(row)
            claimed.update(
                {
                    "state": QUEUE_CLAIMED,
                    "claimed_by_executor": executor_instance_id,
                    "claim_generation": int(row["claim_generation"]) + 1,
                }
            )
        return claimed

    def renew_lease(self, attempt_id: str, executor_instance_id: str) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE execution_queue SET lease_expires_at = ?, updated_at = ? "
                "WHERE attempt_id = ? AND claimed_by_executor = ?",
                (time.time() + LEASE_SEC, time.time(), attempt_id, executor_instance_id),
            )

    def set_queue_state(
        self,
        attempt_id: str,
        state: str,
        *,
        executor_instance_id: Optional[str] = None,
        result: Optional[dict[str, Any]] = None,
    ) -> None:
        fields = ["state = ?", "updated_at = ?"]
        args: list[Any] = [state, time.time()]
        if result is not None:
            fields.append("result_json = ?")
            args.append(json.dumps(result, ensure_ascii=False))
        sql = f"UPDATE execution_queue SET {', '.join(fields)} WHERE attempt_id = ?"
        args.append(attempt_id)
        if executor_instance_id:
            sql += " AND claimed_by_executor = ?"
            args.append(executor_instance_id)
        with self.write() as conn:
            conn.execute(sql, args)

    def queue_item(self, attempt_id: str) -> Optional[dict[str, Any]]:
        row = self.read().execute(
            "SELECT * FROM execution_queue WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_queue(self, *, states: Optional[tuple[str, ...]] = None) -> list[dict[str, Any]]:
        if states:
            placeholders = ",".join("?" for _ in states)
            rows = self.read().execute(
                f"SELECT * FROM execution_queue WHERE state IN ({placeholders}) "
                "ORDER BY created_at ASC",
                states,
            ).fetchall()
        else:
            rows = self.read().execute(
                "SELECT * FROM execution_queue ORDER BY created_at ASC"
            ).fetchall()
        return [dict(r) for r in rows]

    # ─── Локальные команды ───────────────────────────────────────────────────
    def enqueue_local_command(
        self,
        *,
        command_type: str,
        job_id: Optional[str],
        attempt_id: Optional[str],
        payload: dict[str, Any],
        central_command_id: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """Положить команду для исполнителя. Повтор по central_command_id — no-op."""
        local_id = f"lcmd_{uuid.uuid4().hex[:12]}"
        now = time.time()
        try:
            with self.write() as conn:
                conn.execute(
                    "INSERT INTO local_commands (local_command_id, command_type, "
                    "job_id, attempt_id, payload_json, status, created_at, "
                    "central_command_id) VALUES (?,?,?,?,?,?,?,?)",
                    (local_id, command_type, job_id, attempt_id,
                     json.dumps(payload, ensure_ascii=False), "pending", now,
                     central_command_id),
                )
        except sqlite3.IntegrityError:
            return self.local_command_by_central(central_command_id or "")
        return self.local_command(local_id)

    def local_command(self, local_command_id: str) -> Optional[dict[str, Any]]:
        row = self.read().execute(
            "SELECT * FROM local_commands WHERE local_command_id = ?", (local_command_id,)
        ).fetchone()
        return dict(row) if row else None

    def local_command_by_central(self, central_command_id: str) -> Optional[dict[str, Any]]:
        row = self.read().execute(
            "SELECT * FROM local_commands WHERE central_command_id = ?",
            (central_command_id,),
        ).fetchone()
        return dict(row) if row else None

    def claim_local_command(self) -> Optional[dict[str, Any]]:
        """Взять одну команду в работу. Второй исполнитель её уже не получит."""
        with self.write() as conn:
            row = conn.execute(
                "SELECT * FROM local_commands WHERE status = 'pending' "
                "ORDER BY created_at ASC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            cur = conn.execute(
                "UPDATE local_commands SET status = 'processing', claimed_at = ? "
                "WHERE local_command_id = ? AND status = 'pending'",
                (time.time(), row["local_command_id"]),
            )
            if cur.rowcount != 1:
                return None
        item = dict(row)
        item["status"] = "processing"
        return item

    def complete_local_command(
        self, local_command_id: str, result: dict[str, Any], *, status: str = "done"
    ) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE local_commands SET status = ?, completed_at = ?, result_json = ? "
                "WHERE local_command_id = ?",
                (status, time.time(), json.dumps(result, ensure_ascii=False),
                 local_command_id),
            )

    def finished_local_commands(self) -> list[dict[str, Any]]:
        """Исполненные команды, чей результат ещё не подтверждён центру."""
        rows = self.read().execute(
            "SELECT * FROM local_commands WHERE status = 'done' "
            "AND central_command_id IS NOT NULL ORDER BY completed_at ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_command_reported(self, local_command_id: str) -> None:
        with self.write() as conn:
            conn.execute(
                "UPDATE local_commands SET status = 'reported' WHERE local_command_id = ?",
                (local_command_id,),
            )

    def pending_local_command_count(self) -> int:
        row = self.read().execute(
            "SELECT COUNT(*) AS n FROM local_commands WHERE status IN "
            "('pending','processing')"
        ).fetchone()
        return int(row["n"]) if row else 0

    # ─── Реестр процессов ────────────────────────────────────────────────────
    def register_process(
        self,
        *,
        job_id: str,
        attempt_id: str,
        executor_instance_id: str,
        pid: int,
        process_start_identity: Optional[float],
        command_fingerprint: str,
        process_group_id: int,
    ) -> None:
        now = time.time()
        with self.write() as conn:
            conn.execute(
                "INSERT INTO process_registry (attempt_id, job_id, executor_instance_id, "
                "pid, process_start_identity, command_fingerprint, process_group_id, "
                "started_at, last_observed_at, status) VALUES (?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(attempt_id) DO UPDATE SET job_id = excluded.job_id, "
                "executor_instance_id = excluded.executor_instance_id, "
                "pid = excluded.pid, "
                "process_start_identity = excluded.process_start_identity, "
                "command_fingerprint = excluded.command_fingerprint, "
                "process_group_id = excluded.process_group_id, "
                "started_at = excluded.started_at, "
                "last_observed_at = excluded.last_observed_at, "
                "status = excluded.status, exit_code = NULL",
                (attempt_id, job_id, executor_instance_id, pid, process_start_identity,
                 command_fingerprint, process_group_id, now, now, "running"),
            )

    def update_process(self, attempt_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["last_observed_at"] = time.time()
        assignments = ", ".join(f"{k} = ?" for k in fields)
        with self.write() as conn:
            conn.execute(
                f"UPDATE process_registry SET {assignments} WHERE attempt_id = ?",
                (*fields.values(), attempt_id),
            )

    def process_row(self, attempt_id: str) -> Optional[dict[str, Any]]:
        row = self.read().execute(
            "SELECT * FROM process_registry WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_processes(self) -> list[dict[str, Any]]:
        rows = self.read().execute("SELECT * FROM process_registry").fetchall()
        return [dict(r) for r in rows]
