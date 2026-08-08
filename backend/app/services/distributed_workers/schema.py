"""DDL и миграции SQLite-хранилища подсистемы воркеров.

Решение по хранилищу — ADR-007 техпроекта: SQLite в режиме WAL, один файл,
доступ только через asyncio.to_thread. Причина не производительность, а
транзакционность: «вставить события + сдвинуть курсор + сменить состояние»
обязано быть атомарным, а atomic_write_json в этом репозитории пишет БЕЗ
fcntl.flock (см. первый аудит §5.1) — строить на этом новую подсистему с
конкурентной записью значило бы повторить известный дефект.

Инварианты, которые обеспечивает САМА схема, а не дисциплина кода:
  * UNIQUE(job_id, attempt_id, sequence)   → идемпотентность событий (I-04);
  * ux_attempts_one_active (частичный)     → одна активная попытка на задание;
  * UNIQUE(job_id, attempt_number)         → номера попыток не переиспользуются;
  * ux_logical_jobs_active_project         → одно активное задание на проект;
  * worker_tokens.token_sha256 UNIQUE и отсутствие колонки с самим токеном
    → токен физически негде хранить в открытом виде (§20.3).

Миграции нумерованные, вперёд-только. Каждая применяется ПООПЕРАТОРНО внутри
одной явной транзакции вместе с записью в schema_migrations: частично
мигрированной базы после сбоя не остаётся. `executescript` для этого не
годится — он коммитит текущую транзакцию перед запуском.
"""
from __future__ import annotations

import sqlite3
import time

SCHEMA_VERSION = 4

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
    actor      TEXT NOT NULL,
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

# ─────────────────────────────────────────────────────────────────────────────
# Миграция 3 (этап 3.5): логическое задание отделено от ПОПЫТКИ.
#
# Зачем. В схеме этапа 0 попытка была колонками одной строки remote_jobs.
# Из этого следовало: создать вторую попытку = затереть первую. Ни истории, ни
# отдельного результата старой попытки, ни признака «оператор признал попытку
# потерянной, но процесс на VPS может работать» выразить было негде.
#
# Что делает миграция:
#   1. заводит logical_jobs (что делаем) и job_attempts (кто и когда делал);
#   2. переносит каждую существующую строку remote_jobs в пару
#      «логическое задание + попытка №N» без потери полей и результатов;
#   3. заменяет таблицу remote_jobs ПРЕДСТАВЛЕНИЕМ «текущая попытка задания» —
#      весь читающий код этапа 0 продолжает работать без изменений, а писать
#      в неё нельзя физически (SQLite не даёт UPDATE по view), что и требуется:
#      единственный писатель состояния — job_service.transition.
#
# Ось disposition ортогональна execution_state (§4.2 задания):
#   execution_state = 'running' + disposition = 'operator_declared_lost' значит
#   «центр больше не считает попытку текущей», но НЕ значит «процесс на VPS
#   остановлен» (I-06). Смешивать их запрещено.
_MIGRATION_3 = """
CREATE TABLE IF NOT EXISTS logical_jobs (
    job_id               TEXT PRIMARY KEY,
    -- Пользовательский код проекта: кириллица, пробелы и «/» допустимы.
    -- ЭТО МЕТАДАННЫЕ. Компонентом файлового пути не является никогда (I-11).
    project_external_id  TEXT NOT NULL,
    project_display_name TEXT NOT NULL DEFAULT '',
    project_version_id   TEXT,
    job_type             TEXT NOT NULL,
    payload              TEXT NOT NULL DEFAULT '{}',
    current_attempt_id   TEXT,
    overall_state        TEXT NOT NULL DEFAULT 'active',
    created_at           REAL NOT NULL,
    created_by           TEXT NOT NULL DEFAULT 'center',
    updated_at           REAL NOT NULL
);
-- Одно активное логическое задание на (проект, версия). Новая ПОПЫТКА того же
-- задания сюда не попадает — она меняет job_attempts, а не logical_jobs.
CREATE UNIQUE INDEX IF NOT EXISTS ux_logical_jobs_active_project
    ON logical_jobs(project_external_id, IFNULL(project_version_id, ''))
    WHERE overall_state = 'active';

CREATE TABLE IF NOT EXISTS job_attempts (
    attempt_id              TEXT PRIMARY KEY,
    job_id                  TEXT NOT NULL REFERENCES logical_jobs(job_id) ON DELETE CASCADE,
    attempt_number          INTEGER NOT NULL,
    assignment_generation   INTEGER NOT NULL DEFAULT 1,
    assigned_worker_id      TEXT REFERENCES workers(worker_id),
    -- ХЭШ токена попытки. Сам токен не хранится нигде.
    execution_token_hash    TEXT,
    execution_state         TEXT NOT NULL,
    attempt_disposition     TEXT NOT NULL DEFAULT 'active',
    connectivity_state      TEXT NOT NULL DEFAULT 'online',
    retention_state         TEXT NOT NULL DEFAULT 'retained',
    package_id              TEXT,
    source_package_path     TEXT,
    source_package_hash     TEXT,
    result_package_path     TEXT,
    result_package_hash     TEXT,
    -- none | validated | superseded | rejected: КУДА лёг результат попытки.
    result_storage_class    TEXT NOT NULL DEFAULT 'none',
    progress_json           TEXT,
    error_json              TEXT,
    last_event_seq          INTEGER NOT NULL DEFAULT 0,
    superseded_by_attempt   TEXT,
    cancel_reason           TEXT,
    lost_reason             TEXT,
    operator_note           TEXT,
    observed_worker_state   TEXT,
    created_at              REAL NOT NULL,
    assigned_at             REAL,
    accepted_at             REAL,
    started_at              REAL,
    completed_locally_at    REAL,
    result_received_at      REAL,
    validated_at            REAL,
    cancel_requested_at     REAL,
    cancelled_at            REAL,
    declared_lost_at        REAL,
    superseded_at           REAL,
    result_acknowledged_at  REAL,
    deleted_from_worker_at  REAL,
    retention_until         REAL,
    UNIQUE(job_id, attempt_number)
);
-- Ровно одна активная попытка на логическое задание (I-04). Частичный
-- уникальный индекс, потому что обычный UNIQUE запретил бы и вторую
-- ЗАВЕРШЁННУЮ попытку, а её сохранять обязательно.
CREATE UNIQUE INDEX IF NOT EXISTS ux_attempts_one_active
    ON job_attempts(job_id) WHERE attempt_disposition = 'active';
CREATE INDEX IF NOT EXISTS ix_attempts_worker_state
    ON job_attempts(assigned_worker_id, execution_state);
CREATE INDEX IF NOT EXISTS ix_attempts_job
    ON job_attempts(job_id, attempt_number);
CREATE INDEX IF NOT EXISTS ix_attempts_token
    ON job_attempts(execution_token_hash) WHERE execution_token_hash IS NOT NULL;

-- Журнал операторских действий. Append-only: эндпоинта удаления записей нет
-- ни здесь, ни в административном API (§6 задания, I-15).
CREATE TABLE IF NOT EXISTS worker_admin_actions (
    action_id            TEXT PRIMARY KEY,
    actor_id             TEXT NOT NULL,
    actor_display_name   TEXT NOT NULL DEFAULT '',
    action_type          TEXT NOT NULL,
    worker_id            TEXT,
    job_id               TEXT,
    attempt_id           TEXT,
    previous_state_json  TEXT,
    requested_state_json TEXT,
    reason               TEXT NOT NULL DEFAULT '',
    idempotency_key      TEXT,
    request_id           TEXT,
    source_ip            TEXT,
    user_agent           TEXT,
    created_at           REAL NOT NULL,
    result_status        TEXT NOT NULL DEFAULT 'ok',
    result_json          TEXT
);
CREATE INDEX IF NOT EXISTS ix_admin_actions_at
    ON worker_admin_actions(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_admin_actions_job
    ON worker_admin_actions(job_id, created_at DESC);
-- Повтор идемпотентного действия не создаёт вторую запись «изменения».
CREATE UNIQUE INDEX IF NOT EXISTS ux_admin_actions_idem
    ON worker_admin_actions(action_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Команды центра становятся адресными (задание + попытка) и получают
-- собственный жизненный цикл: pending → delivered → acknowledged/expired.
ALTER TABLE worker_commands ADD COLUMN job_id TEXT;
ALTER TABLE worker_commands ADD COLUMN attempt_id TEXT;
ALTER TABLE worker_commands ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE worker_commands ADD COLUMN expires_at REAL;
CREATE INDEX IF NOT EXISTS ix_cmd_attempt ON worker_commands(attempt_id);

-- Аренда на сборку архива. Раньше сессия, чей сборщик умер (упал бэкенд,
-- вотчдог, необработанное исключение), навсегда оставалась в 'assembling':
-- повторный complete получал 409 «сборка уже идёт», и готовый результат
-- было не сдать. try/finally тут не помог бы — процесс может умереть целиком.
ALTER TABLE upload_sessions ADD COLUMN assembly_started_at REAL;

INSERT INTO logical_jobs
    (job_id, project_external_id, project_display_name, project_version_id,
     job_type, payload, current_attempt_id, overall_state,
     created_at, created_by, updated_at)
SELECT
    job_id, project_id, project_id, version_id, job_type, payload, attempt_id,
    CASE
        WHEN state = 'completed' THEN 'completed'
        WHEN state IN ('failed', 'cancelled', 'superseded_result_received')
            THEN 'needs_operator'
        ELSE 'active'
    END,
    created_at, 'center:migration_3', created_at
FROM remote_jobs;

INSERT INTO job_attempts
    (attempt_id, job_id, attempt_number, assignment_generation,
     assigned_worker_id, execution_token_hash, execution_state,
     attempt_disposition, connectivity_state, retention_state, package_id,
     source_package_hash, result_package_hash, result_storage_class,
     progress_json, error_json, last_event_seq, superseded_by_attempt,
     created_at, assigned_at, started_at, completed_locally_at,
     result_received_at, validated_at, retention_until)
SELECT
    attempt_id, job_id, attempt_no, 1,
    assigned_worker_id, execution_token_sha256, state,
    CASE
        WHEN state IN ('completed', 'failed') THEN 'completed'
        WHEN state = 'cancelled' THEN 'cancelled'
        WHEN state = 'superseded_result_received' THEN 'superseded'
        ELSE 'active'
    END,
    connectivity_state, retention_state, package_id,
    source_package_hash, result_package_hash,
    CASE
        WHEN state = 'completed' THEN 'validated'
        WHEN state = 'superseded_result_received' THEN 'superseded'
        ELSE 'none'
    END,
    progress_snapshot, error, last_event_seq, superseded_by_attempt,
    created_at, assigned_at, started_at, completed_locally_at,
    returned_at, validated_at, retention_until
FROM remote_jobs;

DROP TABLE remote_jobs;

-- remote_jobs остаётся ЧИТАЕМЫМ именем «текущая попытка задания». Записи в
-- него нет и быть не может: SQLite отвергает UPDATE по представлению, и это
-- ровно та гарантия, которая нужна — писать разрешено только через
-- job_service.transition поверх job_attempts.
CREATE VIEW remote_jobs AS
SELECT
    a.job_id                 AS job_id,
    j.job_type               AS job_type,
    j.project_external_id    AS project_id,
    j.project_external_id    AS project_external_id,
    j.project_display_name   AS project_display_name,
    j.project_version_id     AS version_id,
    a.attempt_id             AS attempt_id,
    a.attempt_number         AS attempt_no,
    a.assignment_generation  AS assignment_generation,
    a.execution_token_hash   AS execution_token_sha256,
    a.assigned_worker_id     AS assigned_worker_id,
    'remote'                 AS execution_mode,
    a.execution_state        AS state,
    a.attempt_disposition    AS attempt_disposition,
    a.connectivity_state     AS connectivity_state,
    a.retention_state        AS retention_state,
    j.payload                AS payload,
    a.package_id             AS package_id,
    a.source_package_hash    AS source_package_hash,
    a.result_package_hash    AS result_package_hash,
    a.result_storage_class   AS result_storage_class,
    a.created_at             AS created_at,
    j.created_at             AS job_created_at,
    a.assigned_at            AS assigned_at,
    a.accepted_at            AS accepted_at,
    a.started_at             AS started_at,
    a.completed_locally_at   AS completed_locally_at,
    a.result_received_at     AS returned_at,
    a.validated_at           AS validated_at,
    a.cancel_requested_at    AS cancel_requested_at,
    a.cancelled_at           AS cancelled_at,
    a.declared_lost_at       AS declared_lost_at,
    a.superseded_at          AS superseded_at,
    a.result_acknowledged_at AS result_acknowledged_at,
    a.deleted_from_worker_at AS deleted_from_worker_at,
    a.retention_until        AS retention_until,
    a.last_event_seq         AS last_event_seq,
    a.error_json             AS error,
    a.progress_json          AS progress_snapshot,
    a.superseded_by_attempt  AS superseded_by_attempt,
    j.overall_state          AS overall_state,
    j.current_attempt_id     AS current_attempt_id
FROM logical_jobs j
JOIN job_attempts a ON a.attempt_id = j.current_attempt_id;
"""


# ─────────────────────────────────────────────────────────────────────────────
# Миграция 4 (пред-пайплайновый гейт): раздельный учёт слотов.
#
# Проблема, которую она закрывает. Колонка `configured_max_slots` служила
# ДВУМ хозяевам сразу: её выставлял оператор при одобрении воркера и её же
# перезаписывал каждый heartbeat значением, которое воркер сообщал о себе.
# Пока слот был один, совпадение было случайно-верным; на двух слотах это
# означало бы, что настройку оператора молча затирает сам воркер — то есть
# лимит задаёт та сторона, которую он и должен ограничивать.
#
# Теперь:
#   configured_max_slots       — настройка ОПЕРАТОРА (центр), пишется approve;
#   worker_reported_max_slots  — что воркер сообщает о себе (heartbeat);
#   max_verified_slots         — capability его СБОРКИ (сколько проверено);
#   active_local_jobs / running_processes / locally_reserved_slots — что воркер
#     насчитал у себя. Это ДИАГНОСТИКА: решение центр принимает по своей базе,
#     а расхождение показывает как slot_count_mismatch (S-15).
#
# Аддитивная миграция: ALTER TABLE ADD COLUMN не трогает существующие строки.
_MIGRATION_4 = """
ALTER TABLE workers ADD COLUMN worker_reported_max_slots INTEGER;
ALTER TABLE workers ADD COLUMN max_verified_slots INTEGER NOT NULL DEFAULT 1;
ALTER TABLE workers ADD COLUMN active_local_jobs INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN running_processes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workers ADD COLUMN locally_reserved_slots INTEGER NOT NULL DEFAULT 0;
UPDATE workers SET worker_reported_max_slots = configured_max_slots;

-- Журнал операторских решений получает РОЛЬ и РАЗРЕШЕНИЕ, по которому действие
-- было пропущено. Без них запись отвечает на «кто», но не на «на каком
-- основании» — а при разборе инцидента нужно именно второе (§13 задания).
ALTER TABLE worker_admin_actions ADD COLUMN actor_role TEXT;
ALTER TABLE worker_admin_actions ADD COLUMN permission TEXT;
"""


def _statements(script: str) -> tuple[str, ...]:
    """Разбить SQL-скрипт на отдельные операторы.

    Нужен потому, что `executescript` коммитит текущую транзакцию перед
    выполнением: миграция под ним не может быть атомарной. Разбор учитывает
    строковые литералы и комментарии `--`, чтобы `;` внутри них не разрезал
    оператор пополам.
    """
    out: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    i = 0
    length = len(script)
    while i < length:
        ch = script[i]
        if quote is not None:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
            buf.append(ch)
        elif ch == "-" and script[i : i + 2] == "--":
            end = script.find("\n", i)
            if end == -1:
                break
            buf.append(script[i : end + 1])
            i = end
        elif ch == ";":
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf)
    if tail.strip():
        out.append(tail)
    return tuple(s.strip() for s in out if s.strip())


MIGRATIONS: dict[int, tuple[str, ...]] = {
    1: _statements(_MIGRATION_1),
    2: _statements(_MIGRATION_2),
    3: _statements(_MIGRATION_3),
    4: _statements(_MIGRATION_4),
}


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


def pending_migrations(conn: sqlite3.Connection) -> list[int]:
    version = current_version(conn)
    return [target for target in sorted(MIGRATIONS) if target > version]


def migrate(conn: sqlite3.Connection) -> int:
    """Применить недостающие миграции. Возвращает итоговую версию схемы.

    Каждая миграция идёт ОДНОЙ транзакцией вместе с отметкой в
    schema_migrations. Сбой на любом операторе откатывает шаг целиком: базы,
    где половина миграции применена, а отметки нет, не существует.
    """
    version = current_version(conn)
    for target in sorted(MIGRATIONS):
        if target <= version:
            continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            for statement in MIGRATIONS[target]:
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (target, time.time()),
            )
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        version = target
    return version
