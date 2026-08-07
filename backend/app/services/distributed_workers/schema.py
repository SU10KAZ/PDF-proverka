"""DDL и миграции SQLite-хранилища подсистемы воркеров.

Решение по хранилищу — ADR-007 техпроекта: SQLite в режиме WAL, один файл,
доступ только через asyncio.to_thread. Причина не производительность, а
транзакционность: «вставить события + сдвинуть курсор + сменить состояние»
обязано быть атомарным, а atomic_write_json в этом репозитории пишет БЕЗ
fcntl.flock (см. первый аудит §5.1) — строить на этом новую подсистему с
конкурентной записью значило бы повторить известный дефект.

Три инварианта обеспечивает САМА схема, а не дисциплина кода:
  * UNIQUE(job_id, attempt_id, sequence)  → идемпотентность событий (I-04);
  * ux_jobs_active_project (частичный)    → одно активное задание на проект (I-05);
  * worker_tokens.token_sha256 UNIQUE и отсутствие колонки с самим токеном
    → токен физически негде хранить в открытом виде (§20.3).

Миграции нумерованные, вперёд-только. Каждая — идемпотентный шаг, применяется
под одной транзакцией вместе с записью в schema_migrations.
"""
from __future__ import annotations

import sqlite3
import time

SCHEMA_VERSION = 2

# Порядок PRAGMA важен: journal_mode должен быть выставлен до первой записи.
PRAGMAS = (
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA busy_timeout = 5000",
    "PRAGMA foreign_keys = ON",
)

_MIGRATION_1 = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id             TEXT PRIMARY KEY,
    display_name          TEXT NOT NULL,
    instance_id           TEXT,
    registration_status   TEXT NOT NULL DEFAULT 'pending',
    connection_status     TEXT NOT NULL DEFAULT 'offline',
    last_seen_at          REAL,
    worker_version        TEXT,
    protocol_version      INTEGER NOT NULL DEFAULT 1,
    pipeline_revision     TEXT,
    capabilities          TEXT NOT NULL DEFAULT '{}',
    configured_max_slots  INTEGER NOT NULL DEFAULT 1,
    calculated_free_slots INTEGER NOT NULL DEFAULT 0,
    active_jobs           TEXT NOT NULL DEFAULT '[]',
    resource_snapshot     TEXT,
    worker_state          TEXT NOT NULL DEFAULT 'unregistered',
    notes                 TEXT,
    created_at            REAL NOT NULL,
    updated_at            REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_tokens (
    token_id     TEXT PRIMARY KEY,
    worker_id    TEXT NOT NULL REFERENCES workers(worker_id) ON DELETE CASCADE,
    -- ХЭШ, не токен. Колонки с самим токеном в схеме нет намеренно.
    token_sha256 TEXT NOT NULL UNIQUE,
    label        TEXT,
    created_at   REAL NOT NULL,
    expires_at   REAL,
    revoked_at   REAL
);
CREATE INDEX IF NOT EXISTS ix_tokens_worker
    ON worker_tokens(worker_id) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS remote_jobs (
    job_id                  TEXT PRIMARY KEY,
    job_type                TEXT NOT NULL,
    project_id              TEXT NOT NULL,
    version_id              TEXT,
    attempt_id              TEXT NOT NULL,
    attempt_no              INTEGER NOT NULL DEFAULT 1,
    execution_token_sha256  TEXT,
    assigned_worker_id      TEXT REFERENCES workers(worker_id),
    execution_mode          TEXT NOT NULL DEFAULT 'remote',
    state                   TEXT NOT NULL,
    connectivity_state      TEXT NOT NULL DEFAULT 'online',
    retention_state         TEXT NOT NULL DEFAULT 'retained',
    payload                 TEXT NOT NULL DEFAULT '{}',
    package_id              TEXT,
    source_package_hash     TEXT,
    result_package_hash     TEXT,
    created_at              REAL NOT NULL,
    assigned_at             REAL,
    started_at              REAL,
    completed_locally_at    REAL,
    returned_at             REAL,
    validated_at            REAL,
    retention_until         REAL,
    last_event_seq          INTEGER NOT NULL DEFAULT 0,
    error                   TEXT,
    progress_snapshot       TEXT,
    superseded_by_attempt   TEXT
);
CREATE INDEX IF NOT EXISTS ix_jobs_worker_state
    ON remote_jobs(assigned_worker_id, state);
-- Одно активное задание на (project_id, version_id): физический запрет
-- двойного запуска (I-05), не зависящий от корректности кода.
CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_active_project
    ON remote_jobs(project_id, IFNULL(version_id, ''))
    WHERE state NOT IN ('completed', 'failed', 'cancelled', 'superseded_result_received');

CREATE TABLE IF NOT EXISTS job_state_transitions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    from_state TEXT,
    to_state   TEXT NOT NULL,
    actor      TEXT NOT NULL,          -- worker | center | operator:<login>
    reason     TEXT,
    at         REAL NOT NULL,
    event_seq  INTEGER
);
CREATE INDEX IF NOT EXISTS ix_transitions_job ON job_state_transitions(job_id, id);

CREATE TABLE IF NOT EXISTS job_cursors (
    job_id        TEXT NOT NULL,
    attempt_id    TEXT NOT NULL,
    last_seen_seq INTEGER NOT NULL DEFAULT 0,
    updated_at    REAL NOT NULL,
    PRIMARY KEY (job_id, attempt_id)
);

CREATE TABLE IF NOT EXISTS worker_events (
    job_id         TEXT NOT NULL,
    attempt_id     TEXT NOT NULL,
    sequence       INTEGER NOT NULL,
    event_id       TEXT NOT NULL,
    worker_id      TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    occurred_at    REAL NOT NULL,
    received_at    REAL NOT NULL,
    payload        TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (job_id, attempt_id, sequence)
);
CREATE INDEX IF NOT EXISTS ix_events_type
    ON worker_events(job_id, event_type, sequence);

CREATE TABLE IF NOT EXISTS upload_sessions (
    upload_id     TEXT PRIMARY KEY,
    job_id        TEXT NOT NULL,
    attempt_id    TEXT NOT NULL,
    package_type  TEXT NOT NULL,
    expected_size INTEGER NOT NULL,
    chunk_size    INTEGER NOT NULL,
    expected_hash TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'open',
    created_at    REAL NOT NULL,
    expires_at    REAL NOT NULL,
    finalized_at  REAL,
    error         TEXT
);
CREATE INDEX IF NOT EXISTS ix_uploads_job ON upload_sessions(job_id, attempt_id);

CREATE TABLE IF NOT EXISTS upload_chunks (
    upload_id   TEXT NOT NULL REFERENCES upload_sessions(upload_id) ON DELETE CASCADE,
    idx         INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    received_at REAL NOT NULL,
    PRIMARY KEY (upload_id, idx)
);

CREATE TABLE IF NOT EXISTS worker_commands (
    command_id      TEXT PRIMARY KEY,
    worker_id       TEXT NOT NULL,
    command_type    TEXT NOT NULL,
    payload         TEXT NOT NULL DEFAULT '{}',
    created_at      REAL NOT NULL,
    delivered_at    REAL,
    acknowledged_at REAL,
    result          TEXT,
    idempotency_key TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_cmd_pending
    ON worker_commands(worker_id) WHERE acknowledged_at IS NULL;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key            TEXT PRIMARY KEY,
    worker_id      TEXT NOT NULL,
    endpoint       TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json  TEXT NOT NULL,
    status_code    INTEGER NOT NULL,
    created_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS resource_snapshots (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id TEXT NOT NULL,
    at        REAL NOT NULL,
    snapshot  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_res_worker ON resource_snapshots(worker_id, at DESC);
"""

# Миграция 2: двухэтапная выдача токена (регистрация → одобрение → claim).
# Раньше worker_token выдавался прямо в ответе на /register, то есть ДО
# одобрения оператором: секрет существовал у неодобренного воркера. Теперь
# на регистрацию выдаётся одноразовый claim-secret, а сам токен — только
# после одобрения и только один раз.
#
# Миграция аддитивная: ALTER TABLE ADD COLUMN не трогает существующие строки,
# поэтому обновление идёт без удаления базы.
_MIGRATION_2 = """
ALTER TABLE workers ADD COLUMN claim_secret_sha256 TEXT;
ALTER TABLE workers ADD COLUMN claim_issued_at REAL;
ALTER TABLE workers ADD COLUMN claim_used_at REAL;
ALTER TABLE workers ADD COLUMN rejected_at REAL;
CREATE INDEX IF NOT EXISTS ix_workers_claim
    ON workers(claim_secret_sha256) WHERE claim_secret_sha256 IS NOT NULL;
"""

MIGRATIONS: dict[int, str] = {1: _MIGRATION_1, 2: _MIGRATION_2}


def apply_pragmas(conn: sqlite3.Connection) -> None:
    for pragma in PRAGMAS:
        conn.execute(pragma)


def current_version(conn: sqlite3.Connection) -> int:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)"
    )
    row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def migrate(conn: sqlite3.Connection) -> int:
    """Применить недостающие миграции. Возвращает итоговую версию схемы."""
    version = current_version(conn)
    for target in sorted(MIGRATIONS):
        if target <= version:
            continue
        conn.executescript(MIGRATIONS[target])
        conn.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            (target, time.time()),
        )
        conn.commit()
        version = target
    return version
