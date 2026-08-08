"""Доступ к таблицам подсистемы воркеров.

Все функции СИНХРОННЫЕ — из async-обработчиков их зовут через
`database.run_db(...)`. Транзакционные операции, где нужна атомарность
нескольких изменений (события + курсор + состояние), собраны в одну функцию,
а не размазаны по вызывающему коду.
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from typing import Any, Iterable, Optional

from backend.app.models.distributed_workers import (
    TERMINAL_JOB_STATES,
    ConnectivityState,
    JobState,
    RegistrationStatus,
    WorkerState,
)
from backend.app.services.distributed_workers import database
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings


# ─── Утилиты ─────────────────────────────────────────────────────────────────
def new_id(prefix: str, size: int = 8) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:size]}"


def _loads(raw: Optional[str], default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default


def row_to_dict(row: sqlite3.Row | None) -> Optional[dict[str, Any]]:
    return dict(row) if row is not None else None


# ─── Воркеры ─────────────────────────────────────────────────────────────────
def create_worker(
    *,
    display_name: str,
    instance_id: str,
    worker_version: str,
    protocol_version: int,
    pipeline_revision: Optional[str],
    capabilities: dict[str, Any],
    configured_max_slots: int,
    settings: DistributedWorkersSettings | None = None,
) -> dict[str, Any]:
    now = time.time()
    worker_id = new_id("wrk")
    with database.write_txn(settings) as conn:
        conn.execute(
            "INSERT INTO workers (worker_id, display_name, instance_id, "
            "registration_status, connection_status, worker_state, last_seen_at, "
            "worker_version, protocol_version, pipeline_revision, capabilities, "
            "configured_max_slots, calculated_free_slots, active_jobs, "
            "created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                worker_id, display_name, instance_id,
                RegistrationStatus.PENDING.value, ConnectivityState.ONLINE.value,
                WorkerState.PENDING_APPROVAL.value, now,
                worker_version, protocol_version, pipeline_revision,
                json.dumps(capabilities, ensure_ascii=False),
                configured_max_slots, 0, "[]", now, now,
            ),
        )
    return get_worker(worker_id, settings=settings)  # type: ignore[return-value]


def get_worker(
    worker_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM workers WHERE worker_id = ?", (worker_id,)
        ).fetchone()
    return row_to_dict(row)


def find_worker_by_instance(
    instance_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM workers WHERE instance_id = ? ORDER BY created_at DESC LIMIT 1",
            (instance_id,),
        ).fetchone()
    return row_to_dict(row)


def list_workers(*, settings: DistributedWorkersSettings | None = None) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM workers ORDER BY created_at ASC"
        ).fetchall()
    return [dict(r) for r in rows]


def update_worker_fields(
    worker_id: str,
    fields: dict[str, Any],
    *,
    settings: DistributedWorkersSettings | None = None,
) -> None:
    if not fields:
        return
    fields = dict(fields)
    fields["updated_at"] = time.time()
    assignments = ", ".join(f"{k} = ?" for k in fields)
    with database.write_txn(settings) as conn:
        conn.execute(
            f"UPDATE workers SET {assignments} WHERE worker_id = ?",
            (*fields.values(), worker_id),
        )


def record_resource_snapshot(
    worker_id: str,
    snapshot: dict[str, Any],
    *,
    keep_last: int = 200,
    settings: DistributedWorkersSettings | None = None,
) -> None:
    now = time.time()
    with database.write_txn(settings) as conn:
        conn.execute(
            "INSERT INTO resource_snapshots (worker_id, at, snapshot) VALUES (?,?,?)",
            (worker_id, now, json.dumps(snapshot, ensure_ascii=False)),
        )
        conn.execute(
            "DELETE FROM resource_snapshots WHERE worker_id = ? AND id NOT IN "
            "(SELECT id FROM resource_snapshots WHERE worker_id = ? "
            " ORDER BY at DESC LIMIT ?)",
            (worker_id, worker_id, keep_last),
        )


# ─── Токены ──────────────────────────────────────────────────────────────────
def insert_token(
    worker_id: str,
    token_sha256: str,
    *,
    label: str = "primary",
    settings: DistributedWorkersSettings | None = None,
) -> str:
    token_id = new_id("tok")
    with database.write_txn(settings) as conn:
        conn.execute(
            "INSERT INTO worker_tokens (token_id, worker_id, token_sha256, label, created_at) "
            "VALUES (?,?,?,?,?)",
            (token_id, worker_id, token_sha256, label, time.time()),
        )
    return token_id


def find_worker_by_token_hash(
    token_sha256: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT w.* FROM worker_tokens t JOIN workers w ON w.worker_id = t.worker_id "
            "WHERE t.token_sha256 = ? AND t.revoked_at IS NULL",
            (token_sha256,),
        ).fetchone()
    return row_to_dict(row)


def revoke_tokens(worker_id: str, *, settings: DistributedWorkersSettings | None = None) -> int:
    with database.write_txn(settings) as conn:
        cur = conn.execute(
            "UPDATE worker_tokens SET revoked_at = ? "
            "WHERE worker_id = ? AND revoked_at IS NULL",
            (time.time(), worker_id),
        )
        return cur.rowcount


# ─── Задания и попытки ───────────────────────────────────────────────────────
class ActiveJobExists(RuntimeError):
    """Уже есть активное задание на (project_id, version_id) — индекс не дал вставить."""


class ActiveAttemptExists(RuntimeError):
    """У задания уже есть активная попытка — частичный уникальный индекс не дал вставить."""


# Проекция строки попытки в форму, привычную остальному коду (та же, что даёт
# представление remote_jobs). Единственное отличие: сюда попадает ЛЮБАЯ
# попытка, а не только текущая — это нужно вернувшемуся старому воркеру.
ATTEMPT_PROJECTION = """
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
FROM job_attempts a
JOIN logical_jobs j ON j.job_id = a.job_id
"""

# Исторические имена колонок остались во всём коде этапа 0 (fields={"returned_at":…}).
# Переименовывать их по всему репозиторию — лишний риск ради косметики, поэтому
# трансляция сосредоточена в одном месте.
_ATTEMPT_FIELD_ALIASES = {
    "state": "execution_state",
    "error": "error_json",
    "progress_snapshot": "progress_json",
    "returned_at": "result_received_at",
    "execution_token_sha256": "execution_token_hash",
    "attempt_no": "attempt_number",
    "project_id": None,          # поле логического задания: попытке не принадлежит
    "version_id": None,
    "job_type": None,
    "payload": None,
}


def _attempt_columns(fields: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in fields.items():
        column = _ATTEMPT_FIELD_ALIASES.get(key, key)
        if column is None:
            raise ValueError(
                f"Поле {key!r} принадлежит логическому заданию, а не попытке"
            )
        out[column] = value
    return out


def create_job(
    *,
    job_type: str,
    project_id: str,
    version_id: Optional[str],
    payload: dict[str, Any],
    display_name: str = "",
    created_by: str = "center",
    settings: DistributedWorkersSettings | None = None,
) -> dict[str, Any]:
    """Создать логическое задание и его ПЕРВУЮ попытку.

    `project_id` здесь — внешний код проекта: кириллица, пробелы и «/»
    допустимы. Идентификаторы хранения (`job_id`, `attempt_id`) — UUID, и
    только они попадают в файловые пути (I-11).
    """
    now = time.time()
    job_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    try:
        with database.write_txn(settings) as conn:
            conn.execute(
                "INSERT INTO logical_jobs (job_id, project_external_id, "
                "project_display_name, project_version_id, job_type, payload, "
                "current_attempt_id, overall_state, created_at, created_by, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    job_id, project_id, display_name or project_id, version_id,
                    job_type, json.dumps(payload, ensure_ascii=False),
                    attempt_id, "active", now, created_by, now,
                ),
            )
            conn.execute(
                "INSERT INTO job_attempts (attempt_id, job_id, attempt_number, "
                "assignment_generation, execution_state, attempt_disposition, "
                "connectivity_state, retention_state, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    attempt_id, job_id, 1, 1, JobState.CREATED.value, "active",
                    ConnectivityState.ONLINE.value, "retained", now,
                ),
            )
            conn.execute(
                "INSERT INTO job_state_transitions "
                "(job_id, attempt_id, from_state, to_state, actor, reason, at) "
                "VALUES (?,?,?,?,?,?,?)",
                (job_id, attempt_id, None, JobState.CREATED.value, "center", "created", now),
            )
    except sqlite3.IntegrityError as exc:
        raise ActiveJobExists(
            f"На «{project_id}»/{version_id or '-'} уже есть активное задание"
        ) from exc
    return get_job(job_id, settings=settings)  # type: ignore[return-value]


def create_next_attempt(
    *,
    job_id: str,
    worker_id: str,
    settings: DistributedWorkersSettings | None = None,
) -> dict[str, Any]:
    """Завести НОВУЮ попытку логического задания.

    Старая попытка не перезаписывается и не удаляется: у неё остаются свои
    результат, события, журнал и disposition. Новая получает следующий номер,
    новое поколение назначения и собственный execution_token (выдаётся выше).

    Отказ по частичному уникальному индексу означает, что активная попытка ещё
    есть, — это I-05, и обходить его нельзя.
    """
    now = time.time()
    attempt_id = str(uuid.uuid4())
    try:
        with database.write_txn(settings) as conn:
            row = conn.execute(
                "SELECT MAX(attempt_number) AS n, MAX(assignment_generation) AS g "
                "FROM job_attempts WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            number = int((row["n"] if row else 0) or 0) + 1
            generation = int((row["g"] if row else 0) or 0) + 1
            conn.execute(
                "INSERT INTO job_attempts (attempt_id, job_id, attempt_number, "
                "assignment_generation, assigned_worker_id, execution_state, "
                "attempt_disposition, connectivity_state, retention_state, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    attempt_id, job_id, number, generation, worker_id,
                    JobState.CREATED.value, "active",
                    ConnectivityState.ONLINE.value, "retained", now,
                ),
            )
            conn.execute(
                "UPDATE logical_jobs SET current_attempt_id = ?, overall_state = 'active', "
                "updated_at = ? WHERE job_id = ?",
                (attempt_id, now, job_id),
            )
            conn.execute(
                "INSERT INTO job_state_transitions "
                "(job_id, attempt_id, from_state, to_state, actor, reason, at) "
                "VALUES (?,?,?,?,?,?,?)",
                (job_id, attempt_id, None, JobState.CREATED.value, "center",
                 f"новая попытка №{number}", now),
            )
    except sqlite3.IntegrityError as exc:
        raise ActiveAttemptExists(
            "У задания уже есть активная попытка: сначала отмените её либо "
            "признайте потерянной"
        ) from exc
    return get_attempt(attempt_id, settings=settings)  # type: ignore[return-value]


def get_job(
    job_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    """Текущая попытка задания в исторической форме строки remote_jobs."""
    with database.read_conn(settings) as conn:
        row = conn.execute("SELECT * FROM remote_jobs WHERE job_id = ?", (job_id,)).fetchone()
    return row_to_dict(row)


def get_logical_job(
    job_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM logical_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
    return row_to_dict(row)


def get_attempt(
    attempt_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    """Любая попытка (в т. ч. отозванная) в той же форме, что и get_job."""
    with database.read_conn(settings) as conn:
        row = conn.execute(
            f"{ATTEMPT_PROJECTION} WHERE a.attempt_id = ?", (attempt_id,)
        ).fetchone()
    return row_to_dict(row)


def list_attempts(
    job_id: str, *, settings: DistributedWorkersSettings | None = None
) -> list[dict[str, Any]]:
    """История попыток задания, от первой к последней."""
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            f"{ATTEMPT_PROJECTION} WHERE a.job_id = ? ORDER BY a.attempt_number ASC",
            (job_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def find_attempt_by_token_hash(
    job_id: str,
    token_sha256: str,
    *,
    settings: DistributedWorkersSettings | None = None,
) -> Optional[dict[str, Any]]:
    """Найти попытку по хэшу её execution-токена.

    Так вернувшийся старый воркер попадает в контур СВОЕЙ попытки, а не
    получает 409 «попытка отозвана» и не трогает актуальную (I-07).
    """
    with database.read_conn(settings) as conn:
        row = conn.execute(
            f"{ATTEMPT_PROJECTION} WHERE a.job_id = ? AND a.execution_token_hash = ?",
            (job_id, token_sha256),
        ).fetchone()
    return row_to_dict(row)


def update_logical_job(
    job_id: str,
    fields: dict[str, Any],
    *,
    settings: DistributedWorkersSettings | None = None,
) -> None:
    if not fields:
        return
    fields = dict(fields)
    fields["updated_at"] = time.time()
    assignments = ", ".join(f"{k} = ?" for k in fields)
    with database.write_txn(settings) as conn:
        conn.execute(
            f"UPDATE logical_jobs SET {assignments} WHERE job_id = ?",
            (*fields.values(), job_id),
        )


def list_jobs(
    *,
    worker_id: Optional[str] = None,
    state: Optional[str] = None,
    limit: int = 200,
    settings: DistributedWorkersSettings | None = None,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM remote_jobs"
    args: list[Any] = []
    where: list[str] = []
    if worker_id:
        where.append("assigned_worker_id = ?")
        args.append(worker_id)
    if state:
        where.append("state = ?")
        args.append(state)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC LIMIT ?"
    args.append(limit)
    with database.read_conn(settings) as conn:
        rows = conn.execute(sql, args).fetchall()
    return [dict(r) for r in rows]


def claim_next_job_for_worker(
    worker_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    """Атомарно взять одно задание, предназначенное этому воркеру.

    Задание уже закреплено оператором (assigned_worker_id проставлен при
    создании), поэтому «взять» = перевести assigned → source_uploading и
    отметить факт выдачи. BEGIN IMMEDIATE не даёт двум запросам забрать одно.
    """
    now = time.time()
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM remote_jobs WHERE assigned_worker_id = ? AND state = ? "
            "ORDER BY created_at ASC LIMIT 1",
            (worker_id, JobState.ASSIGNED.value),
        ).fetchone()
        if row is None:
            return None
        job = dict(row)
        conn.execute(
            "UPDATE job_attempts SET execution_state = ? WHERE attempt_id = ?",
            (JobState.SOURCE_UPLOADING.value, job["attempt_id"]),
        )
        conn.execute(
            "INSERT INTO job_state_transitions "
            "(job_id, attempt_id, from_state, to_state, actor, reason, at) "
            "VALUES (?,?,?,?,?,?,?)",
            (job["job_id"], job["attempt_id"], JobState.ASSIGNED.value,
             JobState.SOURCE_UPLOADING.value, "worker", "jobs/next", now),
        )
        job["state"] = JobState.SOURCE_UPLOADING.value
    return job


def has_assigned_job(
    worker_id: str, *, settings: DistributedWorkersSettings | None = None
) -> bool:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT 1 FROM remote_jobs WHERE assigned_worker_id = ? AND state = ? LIMIT 1",
            (worker_id, JobState.ASSIGNED.value),
        ).fetchone()
    return row is not None


def update_job_fields(
    job_id: str,
    fields: dict[str, Any],
    *,
    settings: DistributedWorkersSettings | None = None,
) -> None:
    """Обновить поля ТЕКУЩЕЙ попытки задания (историческое имя функции)."""
    if not fields:
        return
    columns = _attempt_columns(fields)
    assignments = ", ".join(f"{k} = ?" for k in columns)
    with database.write_txn(settings) as conn:
        conn.execute(
            f"UPDATE job_attempts SET {assignments} WHERE attempt_id = "
            "(SELECT current_attempt_id FROM logical_jobs WHERE job_id = ?)",
            (*columns.values(), job_id),
        )


def update_attempt_fields(
    attempt_id: str,
    fields: dict[str, Any],
    *,
    settings: DistributedWorkersSettings | None = None,
) -> None:
    """Обновить поля КОНКРЕТНОЙ попытки — в том числе уже отозванной."""
    if not fields:
        return
    columns = _attempt_columns(fields)
    assignments = ", ".join(f"{k} = ?" for k in columns)
    with database.write_txn(settings) as conn:
        conn.execute(
            f"UPDATE job_attempts SET {assignments} WHERE attempt_id = ?",
            (*columns.values(), attempt_id),
        )


def insert_transition(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    attempt_id: str,
    from_state: Optional[str],
    to_state: str,
    actor: str,
    reason: str = "",
    event_seq: Optional[int] = None,
) -> None:
    conn.execute(
        "INSERT INTO job_state_transitions "
        "(job_id, attempt_id, from_state, to_state, actor, reason, at, event_seq) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (job_id, attempt_id, from_state, to_state, actor, reason, time.time(), event_seq),
    )


def list_transitions(
    job_id: str, *, settings: DistributedWorkersSettings | None = None
) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM job_state_transitions WHERE job_id = ? ORDER BY id ASC",
            (job_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def jobs_for_worker_nonterminal(
    worker_id: str, *, settings: DistributedWorkersSettings | None = None
) -> list[dict[str, Any]]:
    terminal = tuple(s.value for s in TERMINAL_JOB_STATES)
    placeholders = ",".join("?" for _ in terminal)
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            f"SELECT * FROM remote_jobs WHERE assigned_worker_id = ? "
            f"AND state NOT IN ({placeholders})",
            (worker_id, *terminal),
        ).fetchall()
    return [dict(r) for r in rows]


def claim_upload_for_assembly(
    upload_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[str]:
    """Занять сессию под сборку архива. Возвращает прежний статус или None.

    Без этого захвата два одновременных `complete` входили в сборку оба,
    писали в один tmp-файл и портили друг другу архив, а проигравший затирал
    уже выставленный `verified` статусом `failed` — задание после этого было
    не доставить вообще.
    """
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT status FROM upload_sessions WHERE upload_id = ?", (upload_id,)
        ).fetchone()
        if row is None:
            return None
        status = row["status"]
        if status == "assembling":
            return None          # уже собирает кто-то другой
        conn.execute(
            "UPDATE upload_sessions SET status = 'assembling' WHERE upload_id = ?",
            (upload_id,),
        )
    return status


def jobs_with_retention(
    worker_id: str, *, limit: int = 200, settings: DistributedWorkersSettings | None = None
) -> list[dict[str, Any]]:
    """Задания воркера, у которых центр УЖЕ проставил срок хранения.

    Такие задания всегда терминальны (`retention_until` появляется вместе с
    `completed`/`superseded_result_received`), поэтому выборка «нетерминальных»
    их не видела и канал retention_updates в heartbeat всегда был пуст —
    воркер узнавал срок только из ответа на complete или из reconcile.

    Выборка идёт по ПОПЫТКАМ, а не по текущим заданиям: у отозванной попытки
    тоже есть свой срок хранения, и без него её пакет остался бы на воркере
    навсегда как retention_unconfirmed.
    """
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            f"{ATTEMPT_PROJECTION} WHERE a.assigned_worker_id = ?"
            " AND a.retention_until IS NOT NULL"
            " ORDER BY a.validated_at DESC, a.result_received_at DESC LIMIT ?",
            (worker_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def attempts_for_worker_nonterminal(
    worker_id: str, *, settings: DistributedWorkersSettings | None = None
) -> list[dict[str, Any]]:
    """Все НЕзавершённые попытки воркера, включая уже не текущие."""
    terminal = tuple(s.value for s in TERMINAL_JOB_STATES)
    placeholders = ",".join("?" for _ in terminal)
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            f"{ATTEMPT_PROJECTION} WHERE a.assigned_worker_id = ? "
            f"AND a.execution_state NOT IN ({placeholders})",
            (worker_id, *terminal),
        ).fetchall()
    return [dict(r) for r in rows]


# ─── Курсоры и события ───────────────────────────────────────────────────────
def get_cursor(
    job_id: str, attempt_id: str, *, settings: DistributedWorkersSettings | None = None
) -> int:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT last_seen_seq FROM job_cursors WHERE job_id = ? AND attempt_id = ?",
            (job_id, attempt_id),
        ).fetchone()
    return int(row["last_seen_seq"]) if row else 0


def cursors_for_worker(
    worker_id: str, *, settings: DistributedWorkersSettings | None = None
) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT c.job_id, c.attempt_id, c.last_seen_seq FROM job_cursors c "
            "JOIN remote_jobs j ON j.job_id = c.job_id AND j.attempt_id = c.attempt_id "
            "WHERE j.assigned_worker_id = ?",
            (worker_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def apply_event_batch(
    *,
    job_id: str,
    attempt_id: str,
    worker_id: str,
    events: Iterable[dict[str, Any]],
    advance_to: int = 0,
    settings: DistributedWorkersSettings | None = None,
) -> tuple[int, int, int]:
    """Вставить непрерывный батч событий и сдвинуть курсор ОДНОЙ транзакцией.

    `advance_to` — номер, до которого курсор обязан дойти, даже если часть
    событий батча в таблицу не пишется (строки логов уходят в файл, но
    нумерация у них общая с остальными: курсор один на оба потока).

    Возвращает (last_seen_seq, accepted, skipped_duplicates).
    Дубли отбиваются как логикой курсора, так и первичным ключом таблицы —
    двойная защита I-04.
    """
    now = time.time()
    accepted = 0
    skipped = 0
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT last_seen_seq FROM job_cursors WHERE job_id = ? AND attempt_id = ?",
            (job_id, attempt_id),
        ).fetchone()
        last_seen = int(row["last_seen_seq"]) if row else 0

        for ev in events:
            seq = int(ev["sequence"])
            if seq <= last_seen:
                skipped += 1
                continue
            try:
                conn.execute(
                    "INSERT INTO worker_events (job_id, attempt_id, sequence, event_id, "
                    "worker_id, event_type, occurred_at, received_at, payload, schema_version) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        job_id, attempt_id, seq, ev["event_id"], worker_id,
                        ev["event_type"], float(ev["occurred_at"]), now,
                        json.dumps(ev.get("payload") or {}, ensure_ascii=False),
                        int(ev.get("schema_version") or 1),
                    ),
                )
                accepted += 1
            except sqlite3.IntegrityError:
                skipped += 1
            last_seen = max(last_seen, seq)

        last_seen = max(last_seen, int(advance_to or 0))

        conn.execute(
            "INSERT INTO job_cursors (job_id, attempt_id, last_seen_seq, updated_at) "
            "VALUES (?,?,?,?) ON CONFLICT(job_id, attempt_id) DO UPDATE SET "
            "last_seen_seq = excluded.last_seen_seq, updated_at = excluded.updated_at",
            (job_id, attempt_id, last_seen, now),
        )
        conn.execute(
            "UPDATE job_attempts SET last_event_seq = ? WHERE attempt_id = ?",
            (last_seen, attempt_id),
        )
    return last_seen, accepted, skipped


def list_events(
    job_id: str,
    *,
    attempt_id: Optional[str] = None,
    after_seq: int = 0,
    limit: int = 500,
    settings: DistributedWorkersSettings | None = None,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM worker_events WHERE job_id = ? AND sequence > ?"
    args: list[Any] = [job_id, after_seq]
    if attempt_id:
        sql += " AND attempt_id = ?"
        args.append(attempt_id)
    sql += " ORDER BY sequence ASC LIMIT ?"
    args.append(limit)
    with database.read_conn(settings) as conn:
        rows = conn.execute(sql, args).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["payload"] = _loads(d.get("payload"), {})
        out.append(d)
    return out


# ─── Upload-сессии ───────────────────────────────────────────────────────────
def create_upload_session(
    *,
    upload_id: str,
    job_id: str,
    attempt_id: str,
    package_type: str,
    expected_size: int,
    chunk_size: int,
    expected_hash: str,
    ttl_sec: int,
    settings: DistributedWorkersSettings | None = None,
) -> dict[str, Any]:
    now = time.time()
    with database.write_txn(settings) as conn:
        conn.execute(
            "INSERT INTO upload_sessions (upload_id, job_id, attempt_id, package_type, "
            "expected_size, chunk_size, expected_hash, status, created_at, expires_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (upload_id, job_id, attempt_id, package_type, expected_size, chunk_size,
             expected_hash, "open", now, now + ttl_sec),
        )
    return get_upload_session(upload_id, settings=settings)  # type: ignore[return-value]


def get_upload_session(
    upload_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM upload_sessions WHERE upload_id = ?", (upload_id,)
        ).fetchone()
    return row_to_dict(row)


def find_open_upload(
    job_id: str,
    attempt_id: str,
    expected_hash: str,
    *,
    settings: DistributedWorkersSettings | None = None,
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM upload_sessions WHERE job_id = ? AND attempt_id = ? "
            "AND expected_hash = ? AND status IN ('open','assembling','verified') "
            "ORDER BY created_at DESC LIMIT 1",
            (job_id, attempt_id, expected_hash),
        ).fetchone()
    return row_to_dict(row)


def update_upload_session(
    upload_id: str,
    fields: dict[str, Any],
    *,
    settings: DistributedWorkersSettings | None = None,
) -> None:
    if not fields:
        return
    assignments = ", ".join(f"{k} = ?" for k in fields)
    with database.write_txn(settings) as conn:
        conn.execute(
            f"UPDATE upload_sessions SET {assignments} WHERE upload_id = ?",
            (*fields.values(), upload_id),
        )


def record_chunk(
    *,
    upload_id: str,
    idx: int,
    sha256: str,
    size: int,
    settings: DistributedWorkersSettings | None = None,
) -> str:
    """Записать факт приёма чанка. Возвращает 'inserted' | 'replayed' | 'conflict'."""
    now = time.time()
    with database.write_txn(settings) as conn:
        existing = conn.execute(
            "SELECT sha256 FROM upload_chunks WHERE upload_id = ? AND idx = ?",
            (upload_id, idx),
        ).fetchone()
        if existing is not None:
            return "replayed" if existing["sha256"] == sha256 else "conflict"
        conn.execute(
            "INSERT INTO upload_chunks (upload_id, idx, sha256, size, received_at) "
            "VALUES (?,?,?,?,?)",
            (upload_id, idx, sha256, size, now),
        )
        conn.execute(
            "UPDATE upload_sessions SET expires_at = ? WHERE upload_id = ?",
            (now + 86400, upload_id),
        )
    return "inserted"


def chunk_hash(
    upload_id: str, idx: int, *, settings: DistributedWorkersSettings | None = None
) -> Optional[str]:
    """sha256 уже принятого чанка или None."""
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT sha256 FROM upload_chunks WHERE upload_id = ? AND idx = ?",
            (upload_id, idx),
        ).fetchone()
    return row["sha256"] if row else None


def received_chunks(
    upload_id: str, *, settings: DistributedWorkersSettings | None = None
) -> list[int]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT idx FROM upload_chunks WHERE upload_id = ? ORDER BY idx ASC",
            (upload_id,),
        ).fetchall()
    return [int(r["idx"]) for r in rows]


def received_size(
    upload_id: str, *, settings: DistributedWorkersSettings | None = None
) -> int:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(size), 0) AS total FROM upload_chunks WHERE upload_id = ?",
            (upload_id,),
        ).fetchone()
    return int(row["total"]) if row else 0


# ─── Команды ─────────────────────────────────────────────────────────────────
COMMAND_TTL_SEC = 7 * 86400


class CommandAckConflict(RuntimeError):
    """Повторный ACK с ДРУГИМ результатом. Первый ответ остаётся в силе."""


def enqueue_command(
    *,
    worker_id: str,
    command_type: str,
    payload: dict[str, Any],
    idempotency_key: str,
    job_id: Optional[str] = None,
    attempt_id: Optional[str] = None,
    ttl_sec: int = COMMAND_TTL_SEC,
    settings: DistributedWorkersSettings | None = None,
) -> dict[str, Any]:
    """Поставить команду в очередь воркера. Персистентно, переживает рестарт.

    Команда адресна: она относится к конкретной ПОПЫТКЕ, и после смены попытки
    исполнять её уже нельзя (I-07). Повтор по idempotency_key возвращает
    существующую команду, а не создаёт вторую.
    """
    now = time.time()
    command_id = new_id("cmd")
    try:
        with database.write_txn(settings) as conn:
            conn.execute(
                "INSERT INTO worker_commands (command_id, worker_id, command_type, "
                "payload, created_at, idempotency_key, job_id, attempt_id, status, "
                "expires_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (command_id, worker_id, command_type,
                 json.dumps(payload, ensure_ascii=False), now, idempotency_key,
                 job_id, attempt_id, "pending", now + ttl_sec),
            )
    except sqlite3.IntegrityError:
        # Идемпотентность по ключу: повтор возвращает уже существующую команду.
        with database.read_conn(settings) as conn:
            row = conn.execute(
                "SELECT * FROM worker_commands WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        return dict(row) if row else {}
    return get_command(command_id, settings=settings)  # type: ignore[return-value]


def get_command(
    command_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_commands WHERE command_id = ?", (command_id,)
        ).fetchone()
    return row_to_dict(row)


def commands_for_job(
    job_id: str, *, settings: DistributedWorkersSettings | None = None
) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM worker_commands WHERE job_id = ? ORDER BY created_at ASC",
            (job_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def expire_stale_commands(
    *, now: Optional[float] = None,
    settings: DistributedWorkersSettings | None = None,
) -> int:
    """Пометить просроченные команды. Истёкшая команда не исполняется."""
    stamp = now if now is not None else time.time()
    with database.write_txn(settings) as conn:
        cur = conn.execute(
            "UPDATE worker_commands SET status = 'expired' "
            "WHERE acknowledged_at IS NULL AND status != 'expired' "
            "AND expires_at IS NOT NULL AND expires_at < ?",
            (stamp,),
        )
        return cur.rowcount


def pending_commands(
    worker_id: str, *, mark_delivered: bool = False,
    settings: DistributedWorkersSettings | None = None,
) -> list[dict[str, Any]]:
    """Неподтверждённые и НЕ просроченные команды воркера.

    Уже доставленные (`delivered`) остаются в выборке намеренно: доставка не
    равна исполнению, ответ мог не дойти, и повторная выдача обязана быть
    безопасной (§7, I-09). Выпадает команда только по ACK или по сроку.
    """
    expire_stale_commands(settings=settings)
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM worker_commands WHERE worker_id = ? AND acknowledged_at IS NULL "
            "AND status != 'expired' ORDER BY created_at ASC",
            (worker_id,),
        ).fetchall()
    items = [dict(r) for r in rows]
    if mark_delivered and items:
        now = time.time()
        with database.write_txn(settings) as conn:
            for item in items:
                if item.get("delivered_at") is None:
                    conn.execute(
                        "UPDATE worker_commands SET delivered_at = ?, status = 'delivered' "
                        "WHERE command_id = ?",
                        (now, item["command_id"]),
                    )
                    item["delivered_at"] = now
                    item["status"] = "delivered"
    return items


def ack_command(
    command_id: str,
    result: dict[str, Any],
    *,
    settings: DistributedWorkersSettings | None = None,
) -> tuple[dict[str, Any], bool]:
    """Подтвердить команду. Возвращает (команда, replayed).

    Повторный ACK с тем же результатом безопасен (I-09). Повторный ACK с
    ДРУГИМ результатом — конфликт: переписать историю исполнения нельзя,
    иначе «отменено» задним числом превратилось бы в «уже завершено».
    """
    canonical = json.dumps(result, ensure_ascii=False, sort_keys=True)
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_commands WHERE command_id = ?", (command_id,)
        ).fetchone()
        if row is None:
            return {}, False
        item = dict(row)
        if item.get("acknowledged_at") is not None:
            prior = item.get("result") or "{}"
            try:
                same = json.loads(prior) == json.loads(canonical)
            except ValueError:
                same = prior == canonical
            if not same:
                raise CommandAckConflict(
                    "Команда уже подтверждена другим результатом — "
                    "перезаписать историю исполнения нельзя"
                )
            return item, True
        now = time.time()
        conn.execute(
            "UPDATE worker_commands SET acknowledged_at = ?, result = ?, "
            "status = 'acknowledged' WHERE command_id = ?",
            (now, canonical, command_id),
        )
        item["acknowledged_at"] = now
        item["result"] = canonical
        item["status"] = "acknowledged"
    return item, False


# ─── Журнал операторских действий (append-only) ──────────────────────────────
def record_admin_action(
    *,
    actor_id: str,
    actor_display_name: str = "",
    action_type: str,
    worker_id: Optional[str] = None,
    job_id: Optional[str] = None,
    attempt_id: Optional[str] = None,
    previous_state: Optional[dict[str, Any]] = None,
    requested_state: Optional[dict[str, Any]] = None,
    reason: str = "",
    idempotency_key: Optional[str] = None,
    request_id: Optional[str] = None,
    source_ip: Optional[str] = None,
    user_agent: Optional[str] = None,
    result_status: str = "ok",
    result: Optional[dict[str, Any]] = None,
    settings: DistributedWorkersSettings | None = None,
) -> dict[str, Any]:
    """Записать операторское действие. Функции удаления записей нет намеренно.

    Повтор идемпотентного действия (тот же action_type + idempotency_key) не
    создаёт вторую запись: иначе журнал показывал бы два разных изменения
    состояния там, где было одно.
    """
    action_id = new_id("act", 12)
    now = time.time()
    try:
        with database.write_txn(settings) as conn:
            conn.execute(
                "INSERT INTO worker_admin_actions (action_id, actor_id, "
                "actor_display_name, action_type, worker_id, job_id, attempt_id, "
                "previous_state_json, requested_state_json, reason, idempotency_key, "
                "request_id, source_ip, user_agent, created_at, result_status, "
                "result_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    action_id, actor_id, actor_display_name, action_type, worker_id,
                    job_id, attempt_id,
                    json.dumps(previous_state, ensure_ascii=False) if previous_state else None,
                    json.dumps(requested_state, ensure_ascii=False) if requested_state else None,
                    reason, idempotency_key, request_id, source_ip, user_agent, now,
                    result_status,
                    json.dumps(result, ensure_ascii=False) if result else None,
                ),
            )
    except sqlite3.IntegrityError:
        with database.read_conn(settings) as conn:
            row = conn.execute(
                "SELECT * FROM worker_admin_actions WHERE action_type = ? "
                "AND idempotency_key = ?",
                (action_type, idempotency_key),
            ).fetchone()
        return dict(row) if row else {}
    return {"action_id": action_id, "created_at": now}


def list_admin_actions(
    *,
    job_id: Optional[str] = None,
    worker_id: Optional[str] = None,
    limit: int = 200,
    settings: DistributedWorkersSettings | None = None,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM worker_admin_actions"
    where: list[str] = []
    args: list[Any] = []
    if job_id:
        where.append("job_id = ?")
        args.append(job_id)
    if worker_id:
        where.append("worker_id = ?")
        args.append(worker_id)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC LIMIT ?"
    args.append(limit)
    with database.read_conn(settings) as conn:
        rows = conn.execute(sql, args).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["previous_state"] = _loads(item.pop("previous_state_json", None), None)
        item["requested_state"] = _loads(item.pop("requested_state_json", None), None)
        item["result"] = _loads(item.pop("result_json", None), None)
        out.append(item)
    return out


# ─── Идемпотентность HTTP ────────────────────────────────────────────────────
def get_idempotent_response(
    key: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM idempotency_keys WHERE key = ?", (key,)
        ).fetchone()
    return row_to_dict(row)


def save_idempotent_response(
    *,
    key: str,
    worker_id: str,
    endpoint: str,
    request_sha256: str,
    response_json: str,
    status_code: int,
    settings: DistributedWorkersSettings | None = None,
) -> None:
    with database.write_txn(settings) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO idempotency_keys (key, worker_id, endpoint, "
            "request_sha256, response_json, status_code, created_at) VALUES (?,?,?,?,?,?,?)",
            (key, worker_id, endpoint, request_sha256, response_json, status_code, time.time()),
        )
