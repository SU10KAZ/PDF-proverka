"""Persistent resumable state and one-time registration tokens."""
from __future__ import annotations

import hashlib
import json
import secrets
import time
import uuid
from typing import Any

from backend.app.services.distributed_workers import auth, database
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

from .models import BootstrapOperation, BootstrapRequest, BootstrapState
from .security import redact


TOKEN_PREFIX = "wbt_"
_REQUEST_FIELDS = {
    "host",
    "port",
    "ssh_user",
    "ssh_auth_ref",
    "expected_host_fingerprint",
    "install_root",
    "center_url",
    "display_name",
    "max_slots",
    "providers",
    "provider_setup",
    "release_id",
    "bundle_path",
    "bundle_sha256",
    "bootstrap_instance_id",
    "worker_id",
}


class SessionNotFound(KeyError):
    pass


class RegistrationTokenRejected(RuntimeError):
    """Неверный, просроченный, чужой или повторно использованный token."""


class SessionUpdateConflict(RuntimeError):
    """The persisted workflow is currently executing and cannot be mutated."""


def _request_payload(request: BootstrapRequest) -> dict[str, Any]:
    raw = request.model_dump(mode="json")
    # Allowlist важнее redaction: секретоподобное новое поле не попадёт в БД,
    # пока разработчик явно не рассмотрит его здесь.
    return redact({key: raw[key] for key in _REQUEST_FIELDS if key in raw})


def _stable_instance_id(request: BootstrapRequest) -> str:
    """One installation root maps to one center worker across new sessions.

    An idempotency key protects retries of one API request. This stable,
    non-secret installation identity additionally protects an operator who
    starts INSTALL again with a fresh key after an interrupted session.
    Fingerprint, display name, provider selection and SSH credential are not
    identity: each may legitimately change while the installation stays the
    same.
    """
    target = {
        "center_url": request.center_url.lower(),
        "host": request.host.lower(),
        "install_root": request.install_root,
        "port": request.port,
        "ssh_user": request.ssh_user,
    }
    canonical = json.dumps(target, sort_keys=True, separators=(",", ":"))
    return "inst_boot_" + hashlib.sha256(canonical.encode()).hexdigest()[:32]


def create_session(
    *,
    operation: BootstrapOperation,
    request: BootstrapRequest,
    idempotency_key: str | None,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    now = time.time()
    session_id = "wbs_" + uuid.uuid4().hex
    requested_payload = _request_payload(request)
    instance_id = request.bootstrap_instance_id or _stable_instance_id(request)
    payload = dict(requested_payload)
    payload["bootstrap_instance_id"] = instance_id
    with database.write_txn(settings) as conn:
        if idempotency_key:
            prior = conn.execute(
                "SELECT * FROM worker_bootstrap_sessions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if prior is not None:
                prior_dict = dict(prior)
                prior_request = json.loads(prior_dict["request_json"])
                if request.bootstrap_instance_id is None:
                    prior_request.pop("bootstrap_instance_id", None)
                    requested_payload.pop("bootstrap_instance_id", None)
                if prior_dict["operation"] != operation.value or prior_request != requested_payload:
                    raise ValueError("idempotency_key уже использован с другим запросом")
                return _session(dict(prior), conn=conn)
        conn.execute(
            """INSERT INTO worker_bootstrap_sessions
               (session_id,idempotency_key,operation,state,step,request_json,
                result_json,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                session_id,
                idempotency_key,
                operation.value,
                BootstrapState.QUEUED.value,
                "created",
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                "{}",
                now,
                now,
            ),
        )
        conn.execute(
            """INSERT INTO worker_bootstrap_events
               (session_id,at,step,state,code,detail_json) VALUES (?,?,?,?,?,?)""",
            (session_id, now, "created", BootstrapState.QUEUED.value, None, "{}"),
        )
        row = conn.execute(
            "SELECT * FROM worker_bootstrap_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        return _session(dict(row), conn=conn)


def get_session(
    session_id: str, *, settings: DistributedWorkersSettings
) -> dict[str, Any]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_bootstrap_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if row is None:
            raise SessionNotFound(session_id)
        return _session(dict(row), conn=conn)


def list_sessions(
    *, limit: int = 100, settings: DistributedWorkersSettings
) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM worker_bootstrap_sessions ORDER BY updated_at DESC LIMIT ?",
            (max(1, min(limit, 500)),),
        ).fetchall()
        return [_session(dict(row), conn=conn, with_events=False) for row in rows]


def update_center_url(
    session_id: str,
    *,
    center_url: str,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Replace a temporary center endpoint without creating a new session.

    The installation identity and successful immutable steps remain pinned to
    the session. Only configuration is invalidated so the next normal resume
    rewrites ``worker.env`` before registration/heartbeat. Any outstanding
    one-time registration token is closed as part of the same transaction.
    """
    now = time.time()
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_bootstrap_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if row is None:
            raise SessionNotFound(session_id)
        current = dict(row)
        if current["state"] == BootstrapState.RUNNING.value:
            raise SessionUpdateConflict("running bootstrap session cannot be updated")

        request_payload = json.loads(current["request_json"])
        previous_url = str(request_payload.get("center_url") or "")
        validated = BootstrapRequest.model_validate(
            {**request_payload, "center_url": center_url}
        )
        if validated.center_url == previous_url:
            return _session(current, conn=conn)

        # model_validate preserves the explicit bootstrap_instance_id already
        # stored for this installation; a tunnel hostname is transport, not
        # worker identity.
        request_payload = _request_payload(validated)
        result = json.loads(current.get("result_json") or "{}")
        result.pop("configured", None)
        conn.execute(
            """UPDATE worker_bootstrap_sessions
               SET state = ?, step = ?, request_json = ?, result_json = ?,
                   error_code = NULL, error_detail = NULL, updated_at = ?
               WHERE session_id = ?""",
            (
                BootstrapState.QUEUED.value,
                "center_url_updated",
                json.dumps(request_payload, ensure_ascii=False, sort_keys=True),
                json.dumps(result, ensure_ascii=False, sort_keys=True),
                now,
                session_id,
            ),
        )
        conn.execute(
            """UPDATE worker_bootstrap_registration_tokens
               SET used_at = COALESCE(used_at, ?) WHERE session_id = ?""",
            (now, session_id),
        )
        conn.execute(
            """INSERT INTO worker_bootstrap_events
               (session_id,at,step,state,code,detail_json) VALUES (?,?,?,?,?,?)""",
            (
                session_id,
                now,
                "center_url_updated",
                BootstrapState.QUEUED.value,
                "center_endpoint_replaced",
                json.dumps(
                    redact(
                        {
                            "previous_center_url": previous_url,
                            "center_url": validated.center_url,
                            "resume_required": True,
                        }
                    ),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ),
        )
        updated = conn.execute(
            "SELECT * FROM worker_bootstrap_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        return _session(dict(updated), conn=conn)


def transition(
    session_id: str,
    *,
    state: BootstrapState,
    step: str,
    code: str | None = None,
    detail: dict[str, Any] | None = None,
    result_patch: dict[str, Any] | None = None,
    fields: dict[str, Any] | None = None,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    now = time.time()
    safe_detail = redact(detail or {})
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_bootstrap_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if row is None:
            raise SessionNotFound(session_id)
        current = dict(row)
        result = json.loads(current.get("result_json") or "{}")
        result.update(redact(result_patch or {}))
        allowed = {"worker_id", "release_id", "previous_release_id", "error_code", "error_detail"}
        updates = {key: redact(value) for key, value in (fields or {}).items() if key in allowed}
        updates.update(
            {
                "state": state.value,
                "step": step,
                "result_json": json.dumps(result, ensure_ascii=False, sort_keys=True),
                "updated_at": now,
            }
        )
        columns = ", ".join(f"{key} = ?" for key in updates)
        conn.execute(
            f"UPDATE worker_bootstrap_sessions SET {columns} WHERE session_id = ?",
            (*updates.values(), session_id),
        )
        conn.execute(
            """INSERT INTO worker_bootstrap_events
               (session_id,at,step,state,code,detail_json) VALUES (?,?,?,?,?,?)""",
            (
                session_id,
                now,
                step,
                state.value,
                code,
                json.dumps(safe_detail, ensure_ascii=False, sort_keys=True),
            ),
        )
        updated = conn.execute(
            "SELECT * FROM worker_bootstrap_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        return _session(dict(updated), conn=conn)


def issue_registration_token(
    session_id: str,
    *,
    expected_instance_id: str,
    ttl_sec: int,
    settings: DistributedWorkersSettings,
    now: float | None = None,
) -> str:
    """Вернуть plain token ровно вызывающему; сохранить только SHA-256."""
    if ttl_sec < 30 or ttl_sec > 3600:
        raise ValueError("TTL registration token должен быть 30..3600 секунд")
    at = time.time() if now is None else now
    token = TOKEN_PREFIX + secrets.token_urlsafe(32)
    with database.write_txn(settings) as conn:
        exists = conn.execute(
            "SELECT 1 FROM worker_bootstrap_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if exists is None:
            raise SessionNotFound(session_id)
        # У предыдущих токенов сессии убираем возможность использования. Это
        # делает resume однозначным: жив только последний выпущенный token.
        conn.execute(
            """UPDATE worker_bootstrap_registration_tokens
               SET used_at = COALESCE(used_at, ?) WHERE session_id = ?""",
            (at, session_id),
        )
        conn.execute(
            """INSERT INTO worker_bootstrap_registration_tokens
               (token_hash,session_id,expected_instance_id,expires_at,created_at)
               VALUES (?,?,?,?,?)""",
            (auth.hash_token(token), session_id, expected_instance_id, at + ttl_sec, at),
        )
    return token


def consume_registration_token(
    token: str | None,
    *,
    instance_id: str,
    settings: DistributedWorkersSettings,
    now: float | None = None,
) -> str:
    """Атомарно consume token; вернуть session_id.

    Все отказы имеют один публичный тип, чтобы endpoint не был oracle по
    существованию/состоянию токена.
    """
    if not token or not token.startswith(TOKEN_PREFIX):
        raise RegistrationTokenRejected("registration token rejected")
    at = time.time() if now is None else now
    token_hash = auth.hash_token(token)
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_bootstrap_registration_tokens WHERE token_hash = ?",
            (token_hash,),
        ).fetchone()
        if (
            row is None
            or row["used_at"] is not None
            or float(row["expires_at"]) < at
            or row["expected_instance_id"] != instance_id
        ):
            raise RegistrationTokenRejected("registration token rejected")
        changed = conn.execute(
            """UPDATE worker_bootstrap_registration_tokens SET used_at = ?
               WHERE token_hash = ? AND used_at IS NULL AND expires_at >= ?
                 AND expected_instance_id = ?""",
            (at, token_hash, at, instance_id),
        ).rowcount
        if changed != 1:
            raise RegistrationTokenRejected("registration token rejected")
        return str(row["session_id"])


def invalidate_registration_tokens(
    session_id: str,
    *,
    settings: DistributedWorkersSettings,
    now: float | None = None,
) -> None:
    """Close every still-live token after a successful remote register call."""
    at = time.time() if now is None else now
    with database.write_txn(settings) as conn:
        conn.execute(
            """UPDATE worker_bootstrap_registration_tokens
               SET used_at = COALESCE(used_at, ?) WHERE session_id = ?""",
            (at, session_id),
        )


def _session(
    row: dict[str, Any], *, conn, with_events: bool = True
) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    if with_events:
        raw_events = conn.execute(
            """SELECT at,step,state,code,detail_json FROM worker_bootstrap_events
               WHERE session_id = ? ORDER BY event_id""",
            (row["session_id"],),
        ).fetchall()
        events = [
            {
                "at": item["at"],
                "step": item["step"],
                "state": item["state"],
                "code": item["code"],
                "detail": json.loads(item["detail_json"] or "{}"),
            }
            for item in raw_events
        ]
    return {
        "session_id": row["session_id"],
        "operation": row["operation"],
        "state": row["state"],
        "step": row["step"],
        "request": json.loads(row["request_json"]),
        "result": json.loads(row["result_json"] or "{}"),
        "error_code": row.get("error_code"),
        "error_detail": row.get("error_detail"),
        "worker_id": row.get("worker_id"),
        "release_id": row.get("release_id"),
        "previous_release_id": row.get("previous_release_id"),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "events": events,
    }
