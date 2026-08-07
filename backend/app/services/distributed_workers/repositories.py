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


# ─── Задания ─────────────────────────────────────────────────────────────────
class ActiveJobExists(RuntimeError):
    """Уже есть активное задание на (project_id, version_id) — индекс не дал вставить."""


def create_job(
    *,
    job_type: str,
    project_id: str,
    version_id: Optional[str],
    payload: dict[str, Any],
    settings: DistributedWorkersSettings | None = None,
) -> dict[str, Any]:
    now = time.time()
    job_id = str(uuid.uuid4())
    attempt_id = new_id("att")
    try:
        with database.write_txn(settings) as conn:
            conn.execute(
                "INSERT INTO remote_jobs (job_id, job_type, project_id, version_id, "
                "attempt_id, attempt_no, state, connectivity_state, retention_state, "
                "payload, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    job_id, job_type, project_id, version_id, attempt_id, 1,
                    JobState.CREATED.value, ConnectivityState.ONLINE.value,
                    "retained", json.dumps(payload, ensure_ascii=False), now,
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
            f"На {project_id}/{version_id or '-'} уже есть активное задание"
        ) from exc
    return get_job(job_id, settings=settings)  # type: ignore[return-value]


def get_job(
    job_id: str, *, settings: DistributedWorkersSettings | None = None
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute("SELECT * FROM remote_jobs WHERE job_id = ?", (job_id,)).fetchone()
    return row_to_dict(row)


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
            "UPDATE remote_jobs SET state = ? WHERE job_id = ?",
            (JobState.SOURCE_UPLOADING.value, job["job_id"]),
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
    if not fields:
        return
    assignments = ", ".join(f"{k} = ?" for k in fields)
    with database.write_txn(settings) as conn:
        conn.execute(
            f"UPDATE remote_jobs SET {assignments} WHERE job_id = ?",
            (*fields.values(), job_id),
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
    """
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM remote_jobs WHERE assigned_worker_id = ?"
            " AND retention_until IS NOT NULL"
            " ORDER BY validated_at DESC, returned_at DESC LIMIT ?",
            (worker_id, limit),
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
            "UPDATE remote_jobs SET last_event_seq = ? WHERE job_id = ?",
            (last_seen, job_id),
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
def enqueue_command(
    *,
    worker_id: str,
    command_type: str,
    payload: dict[str, Any],
    idempotency_key: str,
    settings: DistributedWorkersSettings | None = None,
) -> dict[str, Any]:
    now = time.time()
    command_id = new_id("cmd")
    try:
        with database.write_txn(settings) as conn:
            conn.execute(
                "INSERT INTO worker_commands (command_id, worker_id, command_type, "
                "payload, created_at, idempotency_key) VALUES (?,?,?,?,?,?)",
                (command_id, worker_id, command_type,
                 json.dumps(payload, ensure_ascii=False), now, idempotency_key),
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


def pending_commands(
    worker_id: str, *, mark_delivered: bool = False,
    settings: DistributedWorkersSettings | None = None,
) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM worker_commands WHERE worker_id = ? AND acknowledged_at IS NULL "
            "ORDER BY created_at ASC",
            (worker_id,),
        ).fetchall()
    items = [dict(r) for r in rows]
    if mark_delivered and items:
        now = time.time()
        with database.write_txn(settings) as conn:
            for item in items:
                if item.get("delivered_at") is None:
                    conn.execute(
                        "UPDATE worker_commands SET delivered_at = ? WHERE command_id = ?",
                        (now, item["command_id"]),
                    )
                    item["delivered_at"] = now
    return items


def ack_command(
    command_id: str,
    result: dict[str, Any],
    *,
    settings: DistributedWorkersSettings | None = None,
) -> tuple[dict[str, Any], bool]:
    """Подтвердить команду. Возвращает (команда, replayed)."""
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_commands WHERE command_id = ?", (command_id,)
        ).fetchone()
        if row is None:
            return {}, False
        item = dict(row)
        if item.get("acknowledged_at") is not None:
            return item, True
        conn.execute(
            "UPDATE worker_commands SET acknowledged_at = ?, result = ? WHERE command_id = ?",
            (time.time(), json.dumps(result, ensure_ascii=False), command_id),
        )
        item["acknowledged_at"] = time.time()
        item["result"] = json.dumps(result, ensure_ascii=False)
    return item, False


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
