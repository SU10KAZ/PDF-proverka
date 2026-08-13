"""12F.1E security contract for exact-pair identity re-enrollment.

All tests use temporary Center databases and an in-process ASGI transport.
No production service, database, worker, provider, or network listener is used.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.distributed_workers_helpers import (
    ADMIN_USER,
    VIEWER_USER,
    SyncASGITransport,
    enable_portal_roles,
    make_center_app,
    portal_client,
)


EXACT_WORKER_ID = "wrk_19c87718"
EXACT_INSTANCE_ID = "inst_boot_e129036dddf5c59049080ddd15624e72"
OTHER_WORKER_ID = "wrk_aaaaaaaa"
OTHER_INSTANCE_ID = "inst_other_aaaaaaaa"
INTENT = {"X-Requested-With": "audit-workers"}


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_LONG_POLL_SEC", "1")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "100")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", "200")
    enable_portal_roles(monkeypatch)

    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    settings = get_settings()
    database.ensure_ready(settings)

    # SyncASGITransport creates a short-lived event loop for each request.
    # A Python 3.12 default-executor shutdown can wait indefinitely while a
    # thread-local SQLite connection survives in its worker.  Execute the
    # same synchronous repository operations inline in this in-process ASGI
    # fixture; production keeps database.run_db/to_thread unchanged, and all
    # transaction/locking behavior under test still belongs to SQLite.
    async def inline_run_db(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(database, "run_db", inline_run_db)
    app = make_center_app()
    transport = SyncASGITransport(app)
    result = {
        "settings": settings,
        "app": app,
        "transport": transport,
        "admin": portal_client(app, username=ADMIN_USER),
        "viewer": portal_client(app, username=VIEWER_USER),
        "machine": httpx.Client(transport=transport, base_url="http://center"),
    }
    yield result
    for key in ("admin", "viewer", "machine"):
        result[key].close()
    database.reset_state_for_tests()


def _admin_create(
    env: dict[str, Any],
    *,
    worker_id: str = EXACT_WORKER_ID,
    instance_id: str = EXACT_INSTANCE_ID,
    ttl_sec: int = 300,
    key: str = "auth-create-1",
) -> dict[str, Any]:
    response = env["admin"].post(
        "/api/workers/identity-reenrollment/authorizations",
        json={
            "expected_worker_id": worker_id,
            "expected_instance_id": instance_id,
            "ttl_sec": ttl_sec,
        },
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _completion_payload(
    *, worker_id: str = EXACT_WORKER_ID, instance_id: str = EXACT_INSTANCE_ID
) -> dict[str, Any]:
    return {
        "worker_id": worker_id,
        "instance_id": instance_id,
        "display_name_hint": "physical-31",
        "worker_version": "12f1e-test",
        "protocol_version": 1,
        "pipeline_revision": "isolated-test",
        "capabilities": {"providers": [], "job_types": ["test_pipeline_v1"]},
        "configured_max_slots_hint": 1,
    }


def _machine_complete(
    env: dict[str, Any],
    created: dict[str, Any],
    *,
    token: str | None = None,
    worker_id: str = EXACT_WORKER_ID,
    instance_id: str = EXACT_INSTANCE_ID,
    key: str = "complete-1",
) -> httpx.Response:
    payload = _completion_payload(worker_id=worker_id, instance_id=instance_id)
    payload["authorization_id"] = created["authorization"]["authorization_id"]
    return env["machine"].post(
        "/api/v1/worker/identity-reenrollment",
        json=payload,
        headers={
            "Authorization": f"Bearer {token or created['authorization_token']}",
            "Idempotency-Key": key,
        },
    )


def _service_create(env: dict[str, Any], *, now: float = 1000.0, ttl: int = 300):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    return service.create_authorization(
        expected_worker_id=EXACT_WORKER_ID,
        expected_instance_id=EXACT_INSTANCE_ID,
        created_by="operator:test-admin",
        idempotency_key=f"create-{now}-{ttl}",
        ttl_sec=ttl,
        now=now,
        settings=env["settings"],
    )


def _service_complete(
    env: dict[str, Any],
    created,
    *,
    now: float = 1001.0,
    worker_id: str = EXACT_WORKER_ID,
    instance_id: str = EXACT_INSTANCE_ID,
    token: str | None = None,
    key: str = "complete-service-1",
):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    return service.complete_identity_reenrollment(
        authorization_id=created.authorization["authorization_id"],
        provided_token=token or created.authorization_token,
        worker_id=worker_id,
        instance_id=instance_id,
        display_name_hint="physical-31",
        worker_version="12f1e-test",
        protocol_version=1,
        pipeline_revision="isolated-test",
        capabilities={"providers": [], "job_types": ["test_pipeline_v1"]},
        configured_max_slots=1,
        idempotency_key=key,
        now=now,
        settings=env["settings"],
    )


def _insert_other_worker(env: dict[str, Any], *, worker_id=OTHER_WORKER_ID, instance_id=OTHER_INSTANCE_ID):
    from backend.app.services.distributed_workers import database

    now = time.time()
    with database.write_txn(env["settings"]) as conn:
        conn.execute(
            "INSERT INTO workers (worker_id,display_name,instance_id,"
            "registration_status,connection_status,worker_state,protocol_version,"
            "capabilities,configured_max_slots,calculated_free_slots,active_jobs,"
            "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                worker_id,
                "other",
                instance_id,
                "approved",
                "offline",
                "idle",
                1,
                "{}",
                1,
                0,
                "[]",
                now,
                now,
            ),
        )


# A, B: the generic registration model and service remain Center-ID-owned.
def test_generic_registration_ignores_requested_worker_id_and_generates_random(isolated):
    from backend.app.models.distributed_workers import RegisterRequest
    from backend.app.services.distributed_workers import registration_service

    parsed = RegisterRequest.model_validate(
        {
            "instance_id": "inst_generic_aaaaaaaa",
            "protocol_version": 1,
            "worker_id": EXACT_WORKER_ID,
            "requested_worker_id": EXACT_WORKER_ID,
        }
    )
    assert not hasattr(parsed, "worker_id")
    worker, _claim, created = registration_service.register_worker(
        instance_id=parsed.instance_id,
        display_name_hint="generic",
        worker_version=parsed.worker_version,
        protocol_version=parsed.protocol_version,
        pipeline_revision=None,
        capabilities=parsed.capabilities.model_dump(),
        configured_max_slots_hint=1,
        settings=isolated["settings"],
    )
    assert created is True
    assert worker["worker_id"].startswith("wrk_")
    assert worker["worker_id"] != EXACT_WORKER_ID


# C-E: only the portal admin contour creates historical identity authority.
def test_admin_can_create_exact_pair_authorization(isolated):
    created = _admin_create(isolated)
    assert created["authorization"]["expected_worker_id"] == EXACT_WORKER_ID
    assert created["authorization"]["expected_instance_id"] == EXACT_INSTANCE_ID
    assert created["authorization_token"].startswith("ren_")


@pytest.mark.parametrize("principal", ["viewer", "machine"])
def test_non_admin_cannot_create_authorization(isolated, principal):
    client = isolated[principal]
    headers = {**INTENT, "Idempotency-Key": "denied"}
    if principal == "machine":
        headers["Authorization"] = "Bearer wtk_machine_principal"
    response = client.post(
        "/api/workers/identity-reenrollment/authorizations",
        json={
            "expected_worker_id": EXACT_WORKER_ID,
            "expected_instance_id": EXACT_INSTANCE_ID,
        },
        headers=headers,
    )
    assert response.status_code in (401, 403)


def test_operator_role_cannot_create_authorization(isolated):
    from tests.distributed_workers_helpers import OPERATOR_USER

    operator = portal_client(isolated["app"], username=OPERATOR_USER)
    response = operator.post(
        "/api/workers/identity-reenrollment/authorizations",
        json={
            "expected_worker_id": EXACT_WORKER_ID,
            "expected_instance_id": EXACT_INSTANCE_ID,
        },
        headers={"Idempotency-Key": "operator-denied"},
    )
    assert response.status_code == 403


# F-H, W, Z: exact identity and a new hash-only runtime credential.
def test_valid_exact_pair_preserves_ids_and_issues_new_runtime_token(isolated):
    created = _admin_create(isolated)
    response = _machine_complete(isolated, created)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["worker_id"] == EXACT_WORKER_ID
    assert body["instance_id"] == EXACT_INSTANCE_ID
    assert body["registration_status"] == "approved"
    assert body["transport_mode"] == "polling"
    assert body["credential_issued"] is True
    assert body["worker_token"].startswith("wtk_")

    from backend.app.services.distributed_workers import auth, database

    with database.read_conn(isolated["settings"]) as conn:
        token_row = conn.execute(
            "SELECT token_sha256 FROM worker_tokens WHERE worker_id=?",
            (EXACT_WORKER_ID,),
        ).fetchone()
    assert token_row["token_sha256"] == auth.hash_token(body["worker_token"])


def test_machine_capacity_hint_cannot_configure_reenrolled_identity(isolated):
    created = _service_create(isolated)
    from backend.app.services.distributed_workers import identity_reenrollment as service

    result = service.complete_identity_reenrollment(
        authorization_id=created.authorization["authorization_id"],
        provided_token=created.authorization_token,
        worker_id=EXACT_WORKER_ID,
        instance_id=EXACT_INSTANCE_ID,
        display_name_hint="physical-31",
        worker_version="12f1e-test",
        protocol_version=1,
        pipeline_revision="isolated-test",
        capabilities={},
        configured_max_slots=64,
        idempotency_key="capacity-is-not-identity-authority",
        now=1001.0,
        settings=isolated["settings"],
    )
    assert result.worker["configured_max_slots"] == 1


# I-K, AJ: typed reasons internally; one bounded public response externally.
@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"worker_id": "wrk_bbbbbbbb"}, "WORKER_ID_MISMATCH"),
        ({"instance_id": "inst_wrong_bbbbbbbb"}, "INSTANCE_ID_MISMATCH"),
        ({"token": "ren_" + "x" * 43}, "TOKEN_INVALID"),
    ],
)
def test_pair_and_token_mismatches_have_typed_reason(isolated, change, reason):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    created = _service_create(isolated)
    with pytest.raises(service.ReenrollmentRejected) as rejected:
        _service_complete(isolated, created, **change)
    assert rejected.value.reason.value == reason
    events = service.list_security_events(settings=isolated["settings"])
    assert events[-1]["reason_code"] == reason


def test_machine_api_failure_is_non_oracular(isolated):
    created = _admin_create(isolated)
    details = []
    for kwargs in (
        {"token": "ren_" + "x" * 43},
        {"worker_id": "wrk_bbbbbbbb"},
        {"instance_id": "inst_wrong_bbbbbbbb"},
    ):
        response = _machine_complete(isolated, created, **kwargs)
        assert response.status_code == 401
        details.append(response.json()["detail"])
    assert details[0] == details[1] == details[2]


# L-O: expiration, revocation, consumed state and one-use behavior.
def test_expired_authorization_rejected_and_persisted_expired(isolated):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    created = _service_create(isolated, now=1000.0, ttl=30)
    with pytest.raises(service.ReenrollmentRejected) as rejected:
        _service_complete(isolated, created, now=1030.0)
    assert rejected.value.reason is service.ReasonCode.AUTH_EXPIRED
    row = service.get_authorization(
        created.authorization["authorization_id"], settings=isolated["settings"]
    )
    assert row["status"] == "EXPIRED"


def test_revoked_authorization_rejected(isolated):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    created = _service_create(isolated)
    service.revoke_authorization(
        authorization_id=created.authorization["authorization_id"],
        actor="operator:test-admin",
        settings=isolated["settings"],
        now=1001.0,
    )
    with pytest.raises(service.ReenrollmentRejected) as rejected:
        _service_complete(isolated, created, now=1002.0)
    assert rejected.value.reason is service.ReasonCode.AUTH_REVOKED_STATE


def test_consumed_authorization_cannot_change_request_or_mint_again(isolated):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    created = _service_create(isolated)
    first = _service_complete(isolated, created)
    with pytest.raises(service.ReenrollmentRejected) as rejected:
        _service_complete(isolated, created, key="different-key")
    assert rejected.value.reason is service.ReasonCode.AUTH_CONSUMED
    assert first.runtime_token is not None


# P-Q: registry conflicts are fail-closed.
def test_worker_id_already_bound_to_other_instance_rejected(isolated):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    _insert_other_worker(isolated, worker_id=EXACT_WORKER_ID)
    created = _service_create(isolated)
    with pytest.raises(service.ReenrollmentRejected) as rejected:
        _service_complete(isolated, created)
    assert rejected.value.reason is service.ReasonCode.WORKER_ALREADY_BOUND_OTHER_INSTANCE


def test_instance_already_bound_to_other_worker_rejected(isolated):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    _insert_other_worker(isolated, instance_id=EXACT_INSTANCE_ID)
    created = _service_create(isolated)
    with pytest.raises(service.ReenrollmentRejected) as rejected:
        _service_complete(isolated, created)
    assert rejected.value.reason is service.ReasonCode.INSTANCE_ALREADY_BOUND_OTHER_WORKER


# R: an empty registry still requires a real authorization and token.
def test_empty_registry_does_not_bypass_authorization(isolated):
    payload = _completion_payload()
    payload["authorization_id"] = "rea_doesnotexist000000"
    response = isolated["machine"].post(
        "/api/v1/worker/identity-reenrollment",
        json=payload,
        headers={
            "Authorization": "Bearer ren_" + "x" * 43,
            "Idempotency-Key": "empty-bypass",
        },
    )
    assert response.status_code == 401


# S-V, X-Y, AQ: neither raw secret has a persistence/log/audit representation.
def test_authorization_and_runtime_secrets_absent_from_db_logs_and_events(
    isolated, caplog
):
    from backend.app.services.distributed_workers import database

    caplog.set_level(logging.DEBUG)
    created = _admin_create(isolated)
    enrollment_token = created["authorization_token"]
    completed = _machine_complete(isolated, created).json()
    runtime_token = completed["worker_token"]
    fake_old_runtime_token = "wtk_old_production_token_must_never_be_read"

    with database.read_conn(isolated["settings"]) as conn:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        ]
        dump = "".join(
            json.dumps([dict(row) for row in conn.execute(f'SELECT * FROM "{table}"')])
            for table in tables
            if table != "sqlite_sequence"
        )
    event_text = json.dumps(
        [record.getMessage() for record in caplog.records], ensure_ascii=False
    )
    for secret in (enrollment_token, runtime_token, fake_old_runtime_token):
        assert secret not in dump
        assert secret not in event_text
    assert fake_old_runtime_token not in json.dumps(completed)


# AA-AC: all crash seams roll back worker, token and authorization consumption.
@pytest.mark.parametrize(
    "checkpoint",
    ["before_worker_insert", "after_worker_insert", "after_runtime_token_insert"],
)
def test_transaction_fault_rolls_back_every_partial_state(isolated, monkeypatch, checkpoint):
    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers import identity_reenrollment as service

    created = _service_create(isolated)

    def fail(name: str) -> None:
        if name == checkpoint:
            raise RuntimeError(f"fault:{name}")

    monkeypatch.setattr(service, "_transaction_checkpoint", fail)
    with pytest.raises(RuntimeError, match="fault"):
        _service_complete(isolated, created)
    with database.read_conn(isolated["settings"]) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM workers WHERE worker_id=?", (EXACT_WORKER_ID,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM worker_tokens WHERE worker_id=?", (EXACT_WORKER_ID,)
        ).fetchone()[0] == 0
        status = conn.execute(
            "SELECT status FROM worker_identity_reenrollment_authorizations "
            "WHERE authorization_id=?",
            (created.authorization["authorization_id"],),
        ).fetchone()[0]
    assert status == "PENDING"


# AD-AH: exact retry returns stable state and never a second secret.
def test_duplicate_success_is_safe_and_documents_lost_response_recovery(isolated):
    from backend.app.services.distributed_workers import database

    created = _service_create(isolated)
    first = _service_complete(isolated, created)
    second = _service_complete(isolated, created)
    assert first.credential_issued is True
    assert second.reason.value == "IDEMPOTENT_COMPLETED"
    assert second.runtime_token is None
    assert second.credential_issued is False
    assert second.recovery_required is True
    with database.read_conn(isolated["settings"]) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM workers WHERE worker_id=?", (EXACT_WORKER_ID,)
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM worker_tokens WHERE worker_id=?", (EXACT_WORKER_ID,)
        ).fetchone()[0] == 1


def test_admin_authorization_retry_does_not_reveal_token_twice(isolated):
    first = _admin_create(isolated, key="same-create")
    second = _admin_create(isolated, key="same-create")
    assert first["authorization_token"].startswith("ren_")
    assert second["authorization_token"] is None
    assert second["idempotent"] is True
    assert second["token_recovery_required"] is True


def test_admin_idempotency_key_reuse_with_changed_pair_is_rejected(isolated):
    first = _admin_create(isolated, key="changed-create")
    response = isolated["admin"].post(
        "/api/workers/identity-reenrollment/authorizations",
        json={
            "expected_worker_id": "wrk_bbbbbbbb",
            "expected_instance_id": "inst_changed_bbbbbbbb",
            "ttl_sec": 300,
        },
        headers={"Idempotency-Key": "changed-create"},
    )
    assert first["authorization"]["expected_worker_id"] == EXACT_WORKER_ID
    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "IDEMPOTENCY_KEY_REUSED"


def test_machine_endpoint_uses_durable_registration_rate_limit(
    isolated, monkeypatch
):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "1")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", "1")
    created = _admin_create(isolated)
    first = _machine_complete(isolated, created, token="ren_" + "x" * 43)
    second = _machine_complete(isolated, created)
    assert first.status_code == 401
    assert second.status_code == 429
    assert "Retry-After" in second.headers


# AI: immutable security events include creation, completion and safe rejection.
def test_security_audit_events_are_complete_and_secret_free(isolated):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    created = _service_create(isolated)
    with pytest.raises(service.ReenrollmentRejected):
        _service_complete(isolated, created, token="ren_" + "x" * 43)
    _service_complete(isolated, created)
    events = service.list_security_events(settings=isolated["settings"])
    assert [event["event_type"] for event in events] == [
        "IDENTITY_REENROLLMENT_AUTH_CREATED",
        "IDENTITY_REENROLLMENT_REJECTED",
        "IDENTITY_REENROLLMENT_COMPLETED",
    ]
    assert all(
        set(event).isdisjoint(
            {"token", "authorization_token", "runtime_token", "token_sha256"}
        )
        for event in events
    )


# AK: the issued token authenticates existing polling heartbeat unchanged.
def test_new_runtime_token_authenticates_polling_heartbeat(isolated):
    created = _admin_create(isolated)
    completed = _machine_complete(isolated, created).json()
    heartbeat = isolated["machine"].post(
        "/api/v1/worker/heartbeat",
        json={
            "instance_id": EXACT_INSTANCE_ID,
            "sent_at": time.time(),
            "worker_state": "idle",
            "configured_max_slots": 1,
            "calculated_free_slots": 1,
            "active_jobs": [],
            "max_verified_slots": 1,
        },
        headers={
            "Authorization": f"Bearer {completed['worker_token']}",
            "X-Worker-Id": EXACT_WORKER_ID,
            "X-Instance-Id": EXACT_INSTANCE_ID,
            "X-Protocol-Version": "1",
        },
    )
    assert heartbeat.status_code == 200, heartbeat.text
    assert heartbeat.json()["connection_status"] == "online"
    from backend.app.services.distributed_workers import database

    with database.read_conn(isolated["settings"]) as conn:
        transport = conn.execute(
            "SELECT transport_mode FROM worker_transport_sessions WHERE worker_id=?",
            (EXACT_WORKER_ID,),
        ).fetchone()
    assert transport["transport_mode"] == "polling"


# Non-empty registry proof: unrelated identities and credentials remain intact.
def test_nonempty_registry_reenrollment_preserves_unrelated_worker(isolated):
    from backend.app.services.distributed_workers import auth, database

    _insert_other_worker(isolated)
    other_token = "wtk_other_existing_credential"
    with database.write_txn(isolated["settings"]) as conn:
        conn.execute(
            "INSERT INTO worker_tokens (token_id,worker_id,token_sha256,label,created_at) "
            "VALUES (?,?,?,?,?)",
            ("tok_other", OTHER_WORKER_ID, auth.hash_token(other_token), "existing", 1.0),
        )
    completed = _service_complete(isolated, _service_create(isolated))
    assert completed.worker["worker_id"] == EXACT_WORKER_ID
    with database.read_conn(isolated["settings"]) as conn:
        rows = conn.execute(
            "SELECT worker_id,instance_id FROM workers ORDER BY worker_id"
        ).fetchall()
        existing_hash = conn.execute(
            "SELECT token_sha256 FROM worker_tokens WHERE token_id='tok_other'"
        ).fetchone()[0]
    assert {(row["worker_id"], row["instance_id"]) for row in rows} == {
        (OTHER_WORKER_ID, OTHER_INSTANCE_ID),
        (EXACT_WORKER_ID, EXACT_INSTANCE_ID),
    }
    assert existing_hash == auth.hash_token(other_token)


# AN: fresh canonical database reaches the actual schema version.
def test_schema_migration_fresh_database(isolated):
    from backend.app.services.distributed_workers import database, schema

    with database.read_conn(isolated["settings"]) as conn:
        assert schema.current_version(conn) == schema.SCHEMA_VERSION == 13
        columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(worker_identity_reenrollment_authorizations)"
            )
        }
    assert "token_sha256" in columns
    assert "runtime_token" not in columns


# AO-AP: upgrading a populated v12 copy is additive and preserves old rows.
def test_schema_migration_populated_v12_database_is_additive(tmp_path):
    from backend.app.services.distributed_workers import schema

    path = tmp_path / "populated-v12.db"
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    schema.apply_pragmas(conn)
    conn.execute(
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)"
    )
    for version in range(1, 13):
        conn.execute("BEGIN IMMEDIATE")
        for statement in schema.MIGRATIONS[version]:
            conn.execute(statement)
        conn.execute(
            "INSERT INTO schema_migrations (version,applied_at) VALUES (?,?)",
            (version, float(version)),
        )
        conn.execute("COMMIT")
    conn.execute(
        "INSERT INTO workers (worker_id,display_name,instance_id,"
        "registration_status,connection_status,worker_state,protocol_version,"
        "capabilities,configured_max_slots,calculated_free_slots,active_jobs,"
        "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            OTHER_WORKER_ID,
            "preserved",
            OTHER_INSTANCE_ID,
            "approved",
            "offline",
            "idle",
            1,
            "{}",
            1,
            0,
            "[]",
            1.0,
            1.0,
        ),
    )
    assert schema.migrate(conn) == 13
    row = conn.execute(
        "SELECT display_name,instance_id FROM workers WHERE worker_id=?",
        (OTHER_WORKER_ID,),
    ).fetchone()
    assert dict(row) == {"display_name": "preserved", "instance_id": OTHER_INSTANCE_ID}
    assert conn.execute(
        "SELECT COUNT(*) FROM worker_identity_reenrollment_authorizations"
    ).fetchone()[0] == 0
    conn.close()


def test_ttl_configuration_default_and_bounds(isolated, monkeypatch):
    from backend.app.services.distributed_workers import identity_reenrollment as service
    from backend.app.services.distributed_workers.settings import get_settings

    assert isolated["settings"].identity_reenrollment_ttl_sec == 300
    monkeypatch.setenv("DISTRIBUTED_WORKERS_IDENTITY_REENROLLMENT_TTL_SEC", "29")
    with pytest.raises(service.ReenrollmentRejected) as rejected:
        service.create_authorization(
            expected_worker_id=EXACT_WORKER_ID,
            expected_instance_id=EXACT_INSTANCE_ID,
            created_by="operator:test-admin",
            idempotency_key="bad-default-ttl",
            settings=get_settings(),
        )
    assert rejected.value.reason is service.ReasonCode.INVALID_TTL


@pytest.mark.parametrize(
    ("worker_id", "instance_id", "reason"),
    [
        ("../worker", EXACT_INSTANCE_ID, "INVALID_WORKER_ID"),
        (EXACT_WORKER_ID, "../../instance", "INVALID_INSTANCE_ID"),
    ],
)
def test_invalid_identity_formats_are_typed_and_rejected(
    isolated, worker_id, instance_id, reason
):
    from backend.app.services.distributed_workers import identity_reenrollment as service

    with pytest.raises(service.ReenrollmentRejected) as rejected:
        service.create_authorization(
            expected_worker_id=worker_id,
            expected_instance_id=instance_id,
            created_by="operator:test-admin",
            idempotency_key="invalid-format",
            settings=isolated["settings"],
        )
    assert rejected.value.reason.value == reason


def test_worker_cli_reenrollment_never_reads_old_token_and_writes_new_atomically(
    isolated, tmp_path, monkeypatch
):
    from audit_worker.config import WorkerConfig
    from audit_worker.local_store import WorkerStateStore
    from audit_worker.registration import complete_identity_reenrollment

    config = WorkerConfig(
        dispatcher_url="http://center",
        root=tmp_path / "worker",
        display_name="physical-31",
        transport=isolated["transport"],
    )
    config.ensure_dirs()
    store = WorkerStateStore(config.state_path, config.token_path)
    store.save({"worker_id": EXACT_WORKER_ID, "last_instance_id": EXACT_INSTANCE_ID})
    store.write_token("wtk_old_runtime_must_not_be_sent")
    created = _service_create(isolated, now=time.time())

    original_read_token = WorkerStateStore.read_token

    def forbidden_read(_self):
        raise AssertionError("old runtime token was read")

    monkeypatch.setattr(WorkerStateStore, "read_token", forbidden_read)
    result = complete_identity_reenrollment(
        config,
        authorization_id=created.authorization["authorization_id"],
        authorization_token=created.authorization_token,
    )
    monkeypatch.setattr(WorkerStateStore, "read_token", original_read_token)
    assert result["worker_id"] == EXACT_WORKER_ID
    assert result["credential_stored"] is True
    assert original_read_token(store).startswith("wtk_")
    assert original_read_token(store) != "wtk_old_runtime_must_not_be_sent"
    assert oct(config.token_path.stat().st_mode)[-3:] == "600"
