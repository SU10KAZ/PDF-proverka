"""Identity-preserving enrollment of one pre-authorized historical worker.

This is intentionally not an option on generic registration.  The worker ID
comes from an admin-created authorization stored by the Center; the machine
request can only prove the one-time token and repeat that exact ID pair.

Secrets have deliberately asymmetric retry semantics.  The first successful
completion returns a newly generated runtime token and stores only its SHA-256.
An exact retry after a lost response returns a deterministic completed state,
but never replays or mints a credential.  Recovery uses the existing admin-only
``rotate-token`` operation with a new idempotency key.
"""
from __future__ import annotations

import hashlib
import json
import re
import secrets
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from backend.app.models.distributed_workers import (
    ConnectivityState,
    RegistrationStatus,
    WorkerState,
)
from backend.app.services.distributed_workers import auth, database, identifiers
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings


AUTHORIZATION_TOKEN_PREFIX = "ren_"
MIN_TTL_SEC = 30
MAX_TTL_SEC = 3600

_WORKER_ID_RE = re.compile(r"^wrk_[0-9a-f]{8,32}$")
_INSTANCE_ID_RE = re.compile(r"^inst_[A-Za-z0-9][A-Za-z0-9._-]{2,122}$")


class AuthorizationStatus(str, Enum):
    PENDING = "PENDING"
    CONSUMED = "CONSUMED"
    EXPIRED = "EXPIRED"
    REVOKED = "REVOKED"


class ReasonCode(str, Enum):
    AUTH_CREATED = "AUTH_CREATED"
    AUTH_REVOKED = "AUTH_REVOKED"
    COMPLETED = "COMPLETED"
    AUTH_NOT_FOUND = "AUTH_NOT_FOUND"
    AUTH_EXPIRED = "AUTH_EXPIRED"
    AUTH_CONSUMED = "AUTH_CONSUMED"
    AUTH_REVOKED_STATE = "AUTH_REVOKED"
    TOKEN_INVALID = "TOKEN_INVALID"
    WORKER_ID_MISMATCH = "WORKER_ID_MISMATCH"
    INSTANCE_ID_MISMATCH = "INSTANCE_ID_MISMATCH"
    WORKER_ALREADY_BOUND_OTHER_INSTANCE = "WORKER_ALREADY_BOUND_OTHER_INSTANCE"
    INSTANCE_ALREADY_BOUND_OTHER_WORKER = "INSTANCE_ALREADY_BOUND_OTHER_WORKER"
    WORKER_ALREADY_EXISTS = "WORKER_ALREADY_EXISTS"
    INVALID_WORKER_ID = "INVALID_WORKER_ID"
    INVALID_INSTANCE_ID = "INVALID_INSTANCE_ID"
    INVALID_TTL = "INVALID_TTL"
    IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED"
    IDEMPOTENT_COMPLETED = "IDEMPOTENT_COMPLETED"
    REGISTRY_INCONSISTENT = "REGISTRY_INCONSISTENT"


class ReenrollmentRejected(RuntimeError):
    """Typed internal rejection; the public machine API remains non-oracular."""

    def __init__(self, reason: ReasonCode):
        super().__init__(reason.value)
        self.reason = reason


@dataclass(frozen=True)
class AuthorizationResult:
    authorization: dict[str, Any]
    authorization_token: Optional[str]
    idempotent: bool
    token_recovery_required: bool


@dataclass(frozen=True)
class CompletionResult:
    worker: dict[str, Any]
    runtime_token: Optional[str]
    reason: ReasonCode
    credential_issued: bool
    recovery_required: bool


def _new_id(prefix: str, length: int = 20) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:length]}"


def _canonical_sha256(value: dict[str, Any]) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _validate_worker_id(value: str) -> str:
    text = str(value or "").strip()
    if not _WORKER_ID_RE.fullmatch(text):
        raise ReenrollmentRejected(ReasonCode.INVALID_WORKER_ID)
    return text


def _validate_instance_id(value: str) -> str:
    text = str(value or "").strip()
    if not _INSTANCE_ID_RE.fullmatch(text):
        raise ReenrollmentRejected(ReasonCode.INVALID_INSTANCE_ID)
    return text


def _effective_ttl(settings: DistributedWorkersSettings, requested: Optional[int]) -> int:
    ttl = settings.identity_reenrollment_ttl_sec if requested is None else int(requested)
    if ttl < MIN_TTL_SEC or ttl > MAX_TTL_SEC:
        raise ReenrollmentRejected(ReasonCode.INVALID_TTL)
    return ttl


def _safe_authorization(row: Any) -> dict[str, Any]:
    result = dict(row)
    result.pop("token_sha256", None)
    result.pop("admin_request_sha256", None)
    result.pop("completion_request_sha256", None)
    result.pop("runtime_token_id", None)
    return result


def _insert_event(
    conn: Any,
    *,
    authorization_id: Optional[str],
    event_type: str,
    reason: ReasonCode,
    worker_id: Optional[str],
    instance_id: Optional[str],
    actor: str,
    request_id: Optional[str],
    now: float,
) -> None:
    conn.execute(
        "INSERT INTO worker_identity_reenrollment_events "
        "(authorization_id,event_type,reason_code,worker_id,instance_id,actor,"
        "request_id,occurred_at,detail_json) VALUES (?,?,?,?,?,?,?,?,?)",
        (
            authorization_id,
            event_type,
            reason.value,
            worker_id,
            instance_id,
            actor,
            request_id,
            now,
            "{}",
        ),
    )


def create_authorization(
    *,
    expected_worker_id: str,
    expected_instance_id: str,
    created_by: str,
    idempotency_key: str,
    settings: DistributedWorkersSettings,
    ttl_sec: Optional[int] = None,
    request_id: Optional[str] = None,
    now: Optional[float] = None,
) -> AuthorizationResult:
    """Create one exact-pair authorization and reveal its token once."""
    worker_id = _validate_worker_id(expected_worker_id)
    instance_id = _validate_instance_id(expected_instance_id)
    actor = str(created_by or "").strip()
    key = str(idempotency_key or "").strip()[:128]
    if not actor or not key:
        raise ReenrollmentRejected(ReasonCode.IDEMPOTENCY_KEY_REUSED)
    ttl = _effective_ttl(settings, ttl_sec)
    moment = time.time() if now is None else float(now)
    request_sha = _canonical_sha256(
        {"worker_id": worker_id, "instance_id": instance_id, "ttl_sec": ttl}
    )

    raw_token = AUTHORIZATION_TOKEN_PREFIX + secrets.token_urlsafe(32)
    token_sha = auth.hash_token(raw_token)
    authorization_id = _new_id("rea")
    with database.write_txn(settings) as conn:
        prior = conn.execute(
            "SELECT * FROM worker_identity_reenrollment_authorizations "
            "WHERE created_by=? AND admin_idempotency_key=?",
            (actor, key),
        ).fetchone()
        if prior is not None:
            if not auth.constant_time_equals(prior["admin_request_sha256"], request_sha):
                raise ReenrollmentRejected(ReasonCode.IDEMPOTENCY_KEY_REUSED)
            return AuthorizationResult(
                authorization=_safe_authorization(prior),
                authorization_token=None,
                idempotent=True,
                token_recovery_required=True,
            )
        conn.execute(
            "INSERT INTO worker_identity_reenrollment_authorizations "
            "(authorization_id,expected_worker_id,expected_instance_id,token_sha256,"
            "status,created_by,created_at,expires_at,admin_idempotency_key,"
            "admin_request_sha256) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                authorization_id,
                worker_id,
                instance_id,
                token_sha,
                AuthorizationStatus.PENDING.value,
                actor,
                moment,
                moment + ttl,
                key,
                request_sha,
            ),
        )
        _insert_event(
            conn,
            authorization_id=authorization_id,
            event_type="IDENTITY_REENROLLMENT_AUTH_CREATED",
            reason=ReasonCode.AUTH_CREATED,
            worker_id=worker_id,
            instance_id=instance_id,
            actor=actor,
            request_id=request_id,
            now=moment,
        )
        row = conn.execute(
            "SELECT * FROM worker_identity_reenrollment_authorizations "
            "WHERE authorization_id=?",
            (authorization_id,),
        ).fetchone()
    return AuthorizationResult(
        authorization=_safe_authorization(row),
        authorization_token=raw_token,
        idempotent=False,
        token_recovery_required=False,
    )


def revoke_authorization(
    *,
    authorization_id: str,
    actor: str,
    settings: DistributedWorkersSettings,
    request_id: Optional[str] = None,
    now: Optional[float] = None,
) -> dict[str, Any]:
    moment = time.time() if now is None else float(now)
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_identity_reenrollment_authorizations "
            "WHERE authorization_id=?",
            (authorization_id,),
        ).fetchone()
        if row is None:
            raise ReenrollmentRejected(ReasonCode.AUTH_NOT_FOUND)
        status = AuthorizationStatus(row["status"])
        if status is AuthorizationStatus.CONSUMED:
            raise ReenrollmentRejected(ReasonCode.AUTH_CONSUMED)
        if status is not AuthorizationStatus.REVOKED:
            conn.execute(
                "UPDATE worker_identity_reenrollment_authorizations "
                "SET status=?, revoked_at=? WHERE authorization_id=?",
                (AuthorizationStatus.REVOKED.value, moment, authorization_id),
            )
            _insert_event(
                conn,
                authorization_id=authorization_id,
                event_type="IDENTITY_REENROLLMENT_AUTH_REVOKED",
                reason=ReasonCode.AUTH_REVOKED,
                worker_id=row["expected_worker_id"],
                instance_id=row["expected_instance_id"],
                actor=actor,
                request_id=request_id,
                now=moment,
            )
        current = conn.execute(
            "SELECT * FROM worker_identity_reenrollment_authorizations "
            "WHERE authorization_id=?",
            (authorization_id,),
        ).fetchone()
    return _safe_authorization(current)


def _transaction_checkpoint(_name: str) -> None:
    """No-op seam used to prove rollback at each transactional boundary."""


def _completion_fingerprint(
    *,
    authorization_id: str,
    worker_id: str,
    instance_id: str,
    display_name_hint: str,
    worker_version: str,
    protocol_version: int,
    pipeline_revision: Optional[str],
    capabilities: dict[str, Any],
    configured_max_slots: int,
) -> str:
    return _canonical_sha256(
        {
            "authorization_id": authorization_id,
            "worker_id": worker_id,
            "instance_id": instance_id,
            "display_name_hint": display_name_hint,
            "worker_version": worker_version,
            "protocol_version": protocol_version,
            "pipeline_revision": pipeline_revision,
            "capabilities": capabilities,
            "configured_max_slots": configured_max_slots,
        }
    )


def _record_rejection(
    *,
    authorization_id: str,
    worker_id: str,
    instance_id: str,
    reason: ReasonCode,
    request_id: Optional[str],
    settings: DistributedWorkersSettings,
    now: float,
) -> None:
    with database.write_txn(settings) as conn:
        if reason is ReasonCode.AUTH_EXPIRED:
            conn.execute(
                "UPDATE worker_identity_reenrollment_authorizations "
                "SET status=? WHERE authorization_id=? AND status=?",
                (
                    AuthorizationStatus.EXPIRED.value,
                    authorization_id,
                    AuthorizationStatus.PENDING.value,
                ),
            )
        _insert_event(
            conn,
            authorization_id=authorization_id or None,
            event_type="IDENTITY_REENROLLMENT_REJECTED",
            reason=reason,
            worker_id=worker_id or None,
            instance_id=instance_id or None,
            actor="worker-machine",
            request_id=request_id,
            now=now,
        )


def complete_identity_reenrollment(
    *,
    authorization_id: str,
    provided_token: Optional[str],
    worker_id: str,
    instance_id: str,
    display_name_hint: str,
    worker_version: str,
    protocol_version: int,
    pipeline_revision: Optional[str],
    capabilities: dict[str, Any],
    configured_max_slots: int,
    idempotency_key: str,
    settings: DistributedWorkersSettings,
    request_id: Optional[str] = None,
    now: Optional[float] = None,
) -> CompletionResult:
    """Atomically create the exact identity, new token, consumed auth and audit."""
    moment = time.time() if now is None else float(now)
    try:
        normalized_worker_id = _validate_worker_id(worker_id)
        normalized_instance_id = _validate_instance_id(instance_id)
        key = str(idempotency_key or "").strip()[:128]
        if not key:
            raise ReenrollmentRejected(ReasonCode.IDEMPOTENCY_KEY_REUSED)
        request_sha = _completion_fingerprint(
            authorization_id=authorization_id,
            worker_id=normalized_worker_id,
            instance_id=normalized_instance_id,
            display_name_hint=display_name_hint,
            worker_version=worker_version,
            protocol_version=protocol_version,
            pipeline_revision=pipeline_revision,
            capabilities=capabilities,
            configured_max_slots=configured_max_slots,
        )
        supplied_sha = auth.hash_token(provided_token or "")

        with database.write_txn(settings) as conn:
            authorization = conn.execute(
                "SELECT * FROM worker_identity_reenrollment_authorizations "
                "WHERE authorization_id=?",
                (authorization_id,),
            ).fetchone()
            if authorization is None:
                raise ReenrollmentRejected(ReasonCode.AUTH_NOT_FOUND)
            if not auth.constant_time_equals(supplied_sha, authorization["token_sha256"]):
                raise ReenrollmentRejected(ReasonCode.TOKEN_INVALID)

            status = AuthorizationStatus(authorization["status"])
            if status is AuthorizationStatus.CONSUMED:
                same_retry = (
                    authorization["completion_idempotency_key"] == key
                    and authorization["completion_request_sha256"] == request_sha
                )
                if not same_retry:
                    raise ReenrollmentRejected(ReasonCode.AUTH_CONSUMED)
                worker = conn.execute(
                    "SELECT * FROM workers WHERE worker_id=? AND instance_id=?",
                    (normalized_worker_id, normalized_instance_id),
                ).fetchone()
                token_row = conn.execute(
                    "SELECT token_id FROM worker_tokens WHERE token_id=? "
                    "AND worker_id=? AND revoked_at IS NULL",
                    (authorization["runtime_token_id"], normalized_worker_id),
                ).fetchone()
                if worker is None or token_row is None:
                    raise ReenrollmentRejected(ReasonCode.REGISTRY_INCONSISTENT)
                return CompletionResult(
                    worker=dict(worker),
                    runtime_token=None,
                    reason=ReasonCode.IDEMPOTENT_COMPLETED,
                    credential_issued=False,
                    recovery_required=True,
                )
            if status is AuthorizationStatus.EXPIRED or moment >= float(
                authorization["expires_at"]
            ):
                raise ReenrollmentRejected(ReasonCode.AUTH_EXPIRED)
            if status is AuthorizationStatus.REVOKED:
                raise ReenrollmentRejected(ReasonCode.AUTH_REVOKED_STATE)
            if status is not AuthorizationStatus.PENDING:
                raise ReenrollmentRejected(ReasonCode.REGISTRY_INCONSISTENT)
            if authorization["expected_worker_id"] != normalized_worker_id:
                raise ReenrollmentRejected(ReasonCode.WORKER_ID_MISMATCH)
            if authorization["expected_instance_id"] != normalized_instance_id:
                raise ReenrollmentRejected(ReasonCode.INSTANCE_ID_MISMATCH)

            by_worker = conn.execute(
                "SELECT worker_id,instance_id FROM workers WHERE worker_id=?",
                (normalized_worker_id,),
            ).fetchone()
            if by_worker is not None:
                reason = (
                    ReasonCode.WORKER_ALREADY_BOUND_OTHER_INSTANCE
                    if by_worker["instance_id"] != normalized_instance_id
                    else ReasonCode.WORKER_ALREADY_EXISTS
                )
                raise ReenrollmentRejected(reason)
            by_instance = conn.execute(
                "SELECT worker_id,instance_id FROM workers WHERE instance_id=? "
                "ORDER BY created_at DESC LIMIT 1",
                (normalized_instance_id,),
            ).fetchone()
            if by_instance is not None:
                raise ReenrollmentRejected(ReasonCode.INSTANCE_ALREADY_BOUND_OTHER_WORKER)

            _transaction_checkpoint("before_worker_insert")
            display_name = identifiers.normalize_display_name(
                display_name_hint, fallback=normalized_worker_id
            )
            conn.execute(
                "INSERT INTO workers (worker_id,display_name,instance_id,"
                "registration_status,connection_status,worker_state,last_seen_at,"
                "worker_version,protocol_version,pipeline_revision,capabilities,"
                "configured_max_slots,calculated_free_slots,active_jobs,created_at,"
                "updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    normalized_worker_id,
                    display_name,
                    normalized_instance_id,
                    RegistrationStatus.APPROVED.value,
                    ConnectivityState.OFFLINE.value,
                    WorkerState.IDLE.value,
                    None,
                    worker_version,
                    protocol_version,
                    pipeline_revision,
                    json.dumps(capabilities, ensure_ascii=False, sort_keys=True),
                    # Re-enrollment restores identity, not operational
                    # capacity authority.  The machine hint may participate
                    # in the idempotency fingerprint but cannot configure the
                    # Center; start at the conservative one-slot default.
                    1,
                    0,
                    "[]",
                    moment,
                    moment,
                ),
            )
            _transaction_checkpoint("after_worker_insert")

            runtime_token = auth.generate_token()
            runtime_token_id = _new_id("tok")
            conn.execute(
                "INSERT INTO worker_tokens "
                "(token_id,worker_id,token_sha256,label,created_at,expires_at,revoked_at) "
                "VALUES (?,?,?,?,?,NULL,NULL)",
                (
                    runtime_token_id,
                    normalized_worker_id,
                    auth.hash_token(runtime_token),
                    "identity_reenrollment",
                    moment,
                ),
            )
            _transaction_checkpoint("after_runtime_token_insert")
            conn.execute(
                "INSERT INTO worker_transport_sessions "
                "(worker_id,transport_mode,last_connection_epoch,updated_at) "
                "VALUES (?,?,0,?)",
                (normalized_worker_id, "polling", moment),
            )
            updated = conn.execute(
                "UPDATE worker_identity_reenrollment_authorizations SET "
                "status=?,consumed_at=?,completion_idempotency_key=?,"
                "completion_request_sha256=?,runtime_token_id=? "
                "WHERE authorization_id=? AND status=?",
                (
                    AuthorizationStatus.CONSUMED.value,
                    moment,
                    key,
                    request_sha,
                    runtime_token_id,
                    authorization_id,
                    AuthorizationStatus.PENDING.value,
                ),
            )
            if updated.rowcount != 1:
                raise ReenrollmentRejected(ReasonCode.REGISTRY_INCONSISTENT)
            _insert_event(
                conn,
                authorization_id=authorization_id,
                event_type="IDENTITY_REENROLLMENT_COMPLETED",
                reason=ReasonCode.COMPLETED,
                worker_id=normalized_worker_id,
                instance_id=normalized_instance_id,
                actor="worker-machine",
                request_id=request_id,
                now=moment,
            )
            worker = conn.execute(
                "SELECT * FROM workers WHERE worker_id=?",
                (normalized_worker_id,),
            ).fetchone()
        return CompletionResult(
            worker=dict(worker),
            runtime_token=runtime_token,
            reason=ReasonCode.COMPLETED,
            credential_issued=True,
            recovery_required=False,
        )
    except ReenrollmentRejected as exc:
        _record_rejection(
            authorization_id=str(authorization_id or "")[:64],
            worker_id=str(worker_id or "")[:64],
            instance_id=str(instance_id or "")[:128],
            reason=exc.reason,
            request_id=request_id,
            settings=settings,
            now=moment,
        )
        raise


def get_authorization(
    authorization_id: str, *, settings: DistributedWorkersSettings
) -> Optional[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM worker_identity_reenrollment_authorizations "
            "WHERE authorization_id=?",
            (authorization_id,),
        ).fetchone()
    return _safe_authorization(row) if row is not None else None


def list_security_events(*, settings: DistributedWorkersSettings) -> list[dict[str, Any]]:
    with database.read_conn(settings) as conn:
        rows = conn.execute(
            "SELECT * FROM worker_identity_reenrollment_events ORDER BY event_id"
        ).fetchall()
    return [dict(row) for row in rows]
