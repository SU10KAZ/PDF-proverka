"""Durable network-delivery metadata for the 12B Agent Gateway.

Jobs, events, commands and results remain in their existing authoritative
tables/services.  This module stores only connection fencing and facts needed
to repeat an offer/ACK after a gateway process crash.
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional

from backend.app.models.distributed_workers import JobState, RegistrationStatus
from backend.app.services.distributed_workers import database, repositories
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings


class GatewayConnectionRejected(RuntimeError):
    pass


def accept_connection(
    *,
    worker_id: str,
    instance_id: str,
    connection_id: str,
    connection_epoch: int,
    protocol_version: int,
    settings: DistributedWorkersSettings,
) -> tuple[dict[str, Any], Optional[str]]:
    """Atomically fence an older stream and persist GRPC_STREAM ownership."""
    now = time.time()
    with database.write_txn(settings) as conn:
        worker_row = conn.execute(
            "SELECT * FROM workers WHERE worker_id = ?", (worker_id,)
        ).fetchone()
        if worker_row is None:
            raise GatewayConnectionRejected("unknown worker identity")
        worker = dict(worker_row)
        if worker.get("registration_status") != RegistrationStatus.APPROVED.value:
            raise GatewayConnectionRejected("worker is not approved")
        stored_instance = str(worker.get("instance_id") or "")
        if stored_instance and stored_instance != instance_id:
            raise GatewayConnectionRejected("worker instance identity mismatch")

        prior_row = conn.execute(
            "SELECT * FROM worker_transport_sessions WHERE worker_id = ?",
            (worker_id,),
        ).fetchone()
        prior = dict(prior_row) if prior_row is not None else None
        last_epoch = int((prior or {}).get("last_connection_epoch") or 0)
        if int(connection_epoch) <= last_epoch:
            raise GatewayConnectionRejected(
                f"stale connection epoch: received {connection_epoch}, last accepted {last_epoch}"
            )
        old_connection_id = (prior or {}).get("active_connection_id")
        conn.execute(
            "INSERT INTO worker_transport_sessions "
            "(worker_id, transport_mode, last_connection_epoch, active_connection_id, "
            "protocol_version, connected_at, last_message_at, last_heartbeat_at, "
            "disconnected_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(worker_id) DO UPDATE SET "
            "transport_mode=excluded.transport_mode, "
            "last_connection_epoch=excluded.last_connection_epoch, "
            "active_connection_id=excluded.active_connection_id, "
            "protocol_version=excluded.protocol_version, connected_at=excluded.connected_at, "
            "last_message_at=excluded.last_message_at, disconnected_at=NULL, "
            "updated_at=excluded.updated_at",
            (
                worker_id,
                "grpc_stream",
                int(connection_epoch),
                connection_id,
                int(protocol_version),
                now,
                now,
                None,
                None,
                now,
            ),
        )
    return worker, str(old_connection_id) if old_connection_id else None


def touch_connection(
    worker_id: str,
    connection_id: str,
    *,
    heartbeat: bool = False,
    settings: DistributedWorkersSettings,
) -> bool:
    now = time.time()
    fields = "last_message_at = ?, updated_at = ?"
    values: list[Any] = [now, now]
    if heartbeat:
        fields += ", last_heartbeat_at = ?"
        values.append(now)
    with database.write_txn(settings) as conn:
        cur = conn.execute(
            f"UPDATE worker_transport_sessions SET {fields} "
            "WHERE worker_id = ? AND active_connection_id = ?",
            (*values, worker_id, connection_id),
        )
    return cur.rowcount == 1


def disconnect_connection(
    worker_id: str,
    connection_id: str,
    *,
    settings: DistributedWorkersSettings,
) -> bool:
    """Clear only this ephemeral connection; retain grpc transport ownership."""
    now = time.time()
    with database.write_txn(settings) as conn:
        cur = conn.execute(
            "UPDATE worker_transport_sessions SET active_connection_id=NULL, "
            "disconnected_at=?, updated_at=? "
            "WHERE worker_id=? AND active_connection_id=?",
            (now, now, worker_id, connection_id),
        )
        if cur.rowcount:
            conn.execute(
                "UPDATE workers SET connection_status='offline', updated_at=? "
                "WHERE worker_id=?",
                (now, worker_id),
            )
    return cur.rowcount == 1


def get_transport_session(
    worker_id: str, *, settings: DistributedWorkersSettings
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_transport_sessions WHERE worker_id=?", (worker_id,)
        ).fetchone()
    return dict(row) if row is not None else None


def mark_offer_delivered(attempt_id: str, *, settings: DistributedWorkersSettings) -> None:
    now = time.time()
    with database.write_txn(settings) as conn:
        conn.execute(
            "UPDATE gateway_job_offers SET delivered_at=COALESCE(delivered_at, ?), "
            "updated_at=? WHERE attempt_id=? AND status='offered'",
            (now, now, attempt_id),
        )


def mark_offer_accepted(attempt_id: str, *, settings: DistributedWorkersSettings) -> None:
    now = time.time()
    with database.write_txn(settings) as conn:
        conn.execute(
            "UPDATE gateway_job_offers SET status='accepted', accepted_at=?, updated_at=? "
            "WHERE attempt_id=? AND status IN ('offered','accepted')",
            (now, now, attempt_id),
        )


def decline_offer(
    *,
    attempt_id: str,
    worker_id: str,
    reason: str,
    requeue: bool,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Persist decline and optionally return an unaccepted attempt to assigned."""
    now = time.time()
    with database.write_txn(settings) as conn:
        row = conn.execute(
            repositories.ATTEMPT_PROJECTION + " WHERE a.attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise GatewayConnectionRejected("attempt not found")
        attempt = dict(row)
        if attempt.get("assigned_worker_id") != worker_id:
            raise GatewayConnectionRejected("attempt belongs to another worker")
        conn.execute(
            "UPDATE gateway_job_offers SET status='declined', declined_at=?, "
            "decline_reason=?, updated_at=? WHERE attempt_id=?",
            (now, reason[:128], now, attempt_id),
        )
        if requeue and attempt.get("state") == JobState.SOURCE_UPLOADING.value:
            conn.execute(
                "UPDATE job_attempts SET execution_state=? WHERE attempt_id=? "
                "AND execution_state=?",
                (JobState.ASSIGNED.value, attempt_id, JobState.SOURCE_UPLOADING.value),
            )
            repositories.insert_transition(
                conn,
                job_id=attempt["job_id"],
                attempt_id=attempt_id,
                from_state=JobState.SOURCE_UPLOADING.value,
                to_state=JobState.ASSIGNED.value,
                actor="center",
                reason=f"gateway decline: {reason[:128]}",
            )
            attempt["state"] = JobState.ASSIGNED.value
    return attempt


def pending_offers(
    worker_id: str, *, settings: DistributedWorkersSettings
) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM gateway_job_offers WHERE worker_id=? AND status='offered' "
            "ORDER BY offered_at ASC",
            (worker_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def recover_expired_offers(
    *, now: Optional[float] = None, settings: DistributedWorkersSettings
) -> int:
    stamp = float(now if now is not None else time.time())
    recovered = 0
    with database.write_txn(settings) as conn:
        rows = conn.execute(
            "SELECT o.*, a.execution_state FROM gateway_job_offers o "
            "JOIN job_attempts a ON a.attempt_id=o.attempt_id "
            "WHERE o.status='offered' AND o.expires_at<=?",
            (stamp,),
        ).fetchall()
        for raw in rows:
            item = dict(raw)
            if item["execution_state"] == JobState.SOURCE_UPLOADING.value:
                cur = conn.execute(
                    "UPDATE job_attempts SET execution_state=? WHERE attempt_id=? "
                    "AND execution_state=?",
                    (
                        JobState.ASSIGNED.value,
                        item["attempt_id"],
                        JobState.SOURCE_UPLOADING.value,
                    ),
                )
                if cur.rowcount:
                    repositories.insert_transition(
                        conn,
                        job_id=item["job_id"],
                        attempt_id=item["attempt_id"],
                        from_state=JobState.SOURCE_UPLOADING.value,
                        to_state=JobState.ASSIGNED.value,
                        actor="center",
                        reason="gateway offer lease expired before accept",
                    )
                    recovered += 1
            conn.execute(
                "UPDATE gateway_job_offers SET status='expired', updated_at=? "
                "WHERE attempt_id=?",
                (stamp, item["attempt_id"]),
            )
    return recovered


def authorize_transfer(
    *,
    transfer_id: str,
    worker_id: str,
    job_id: str,
    attempt_id: str,
    direction: str,
    settings: DistributedWorkersSettings,
) -> bool:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT 1 FROM gateway_transfers WHERE transfer_id=? AND worker_id=? "
            "AND job_id=? AND attempt_id=? AND direction=? AND expires_at>?",
            (transfer_id, worker_id, job_id, attempt_id, direction, time.time()),
        ).fetchone()
    return row is not None


def record_result_ready(
    *,
    worker_id: str,
    job_id: str,
    attempt_id: str,
    transfer_id: str,
    result_sha256: str,
    routing_plan_hash: str,
    execution_revision: str,
    ready_at: float,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    now = time.time()
    with database.write_txn(settings) as conn:
        row = conn.execute(
            repositories.ATTEMPT_PROJECTION + " WHERE a.attempt_id=?", (attempt_id,)
        ).fetchone()
        if row is None:
            raise GatewayConnectionRejected("attempt not found")
        attempt = dict(row)
        if attempt["job_id"] != job_id or attempt.get("assigned_worker_id") != worker_id:
            raise GatewayConnectionRejected("result attempt identity mismatch")
        existing = conn.execute(
            "SELECT * FROM gateway_result_notifications WHERE attempt_id=?", (attempt_id,)
        ).fetchone()
        if existing is not None and str(existing["result_sha256"]) != result_sha256:
            raise GatewayConnectionRejected("conflicting result identity")
        conn.execute(
            "INSERT INTO gateway_result_notifications "
            "(attempt_id,job_id,worker_id,transfer_id,result_sha256,routing_plan_hash,"
            "execution_revision,status,ready_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(attempt_id) DO UPDATE SET updated_at=excluded.updated_at",
            (
                attempt_id,
                job_id,
                worker_id,
                transfer_id,
                result_sha256,
                routing_plan_hash,
                execution_revision,
                "pending_validation",
                ready_at,
                now,
            ),
        )
        conn.execute(
            "INSERT OR REPLACE INTO gateway_transfers "
            "(transfer_id,worker_id,job_id,attempt_id,direction,expires_at,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                transfer_id,
                worker_id,
                job_id,
                attempt_id,
                "agent_to_center",
                now + 86400,
                now,
            ),
        )
    return attempt


def pending_result_notifications(
    worker_id: str, *, settings: DistributedWorkersSettings
) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT n.*, a.execution_state, a.result_package_hash, a.retention_until, "
            "a.validated_at, a.error_json AS error FROM gateway_result_notifications n "
            "JOIN job_attempts a ON a.attempt_id=n.attempt_id "
            "WHERE n.worker_id=? ORDER BY n.ready_at ASC",
            (worker_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def record_progress(
    *, attempt_id: str, worker_id: str, snapshot: dict[str, Any],
    settings: DistributedWorkersSettings,
) -> None:
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT assigned_worker_id FROM job_attempts WHERE attempt_id=?", (attempt_id,)
        ).fetchone()
        if row is None or row["assigned_worker_id"] != worker_id:
            raise GatewayConnectionRejected("progress attempt identity mismatch")
        conn.execute(
            "UPDATE job_attempts SET progress_json=? WHERE attempt_id=?",
            (json.dumps(snapshot, ensure_ascii=False), attempt_id),
        )
