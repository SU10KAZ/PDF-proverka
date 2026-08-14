from __future__ import annotations

import asyncio
import grp
import json
import os
import socket
import sqlite3
import stat
import struct
import subprocess
import sys
import time
import uuid
from argparse import Namespace
from pathlib import Path

import httpx
import pytest

from backend.app.services.distributed_workers import auth, database, repositories
from backend.app.services.distributed_workers import state_permissions
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersConfigError,
    get_settings,
)
from scripts import manage_distributed_worker_state as state_tool
from tests.distributed_workers_helpers import make_center_app


def _seed_private_state(monkeypatch, data_dir: Path) -> None:
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(data_dir))
    for name in (
        "DISTRIBUTED_WORKERS_SHARED_STATE",
        "DISTRIBUTED_WORKERS_SHARED_OWNER_UID",
        "DISTRIBUTED_WORKERS_SHARED_GID",
        "DISTRIBUTED_WORKERS_SHARED_RECEIPT",
    ):
        monkeypatch.delenv(name, raising=False)
    database.ensure_ready(get_settings())
    database.reset_state_for_tests()


def _prepare(monkeypatch, data_dir: Path):
    _seed_private_state(monkeypatch, data_dir)
    receipt_dir = data_dir.parent / "trusted-receipt"
    receipt = receipt_dir / "shared-state.json"
    group_name = grp.getgrgid(os.getgid()).gr_name
    monkeypatch.setattr(state_tool, "TRUSTED_RECEIPT_OWNER_UID", os.getuid())
    monkeypatch.setattr(state_tool, "TRUSTED_RECEIPT_OWNER_GID", os.getgid())
    # Unit fixtures cannot create a root-owned file.  The real systemd GID-984
    # regression supplies the actual root boundary; here only receipt contents
    # and object checks are exercised.
    monkeypatch.setattr(
        state_permissions,
        "_validate_receipt_boundary",
        lambda path: path.lstat(),
    )
    args = Namespace(
        data_dir=data_dir,
        owner_uid=os.getuid(),
        shared_gid=os.getgid(),
        shared_group=group_name,
        service_uid=[],
        receipt=receipt,
        backend_host="127.0.0.1",
        backend_port=1,
    )
    previous_umask = os.umask(0o077)
    try:
        state_tool.prepare(args)
    finally:
        os.umask(previous_umask)
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_OWNER_UID", str(os.getuid()))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_GID", str(os.getgid()))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_RECEIPT", str(receipt))
    return get_settings(), args


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    for name in (
        "DISTRIBUTED_WORKERS_SHARED_STATE",
        "DISTRIBUTED_WORKERS_SHARED_OWNER_UID",
        "DISTRIBUTED_WORKERS_SHARED_GID",
        "DISTRIBUTED_WORKERS_SHARED_RECEIPT",
    ):
        monkeypatch.delenv(name, raising=False)
    database.reset_state_for_tests()
    yield
    database.reset_state_for_tests()


def test_privileged_bootstrap_is_exact_receipted_and_idempotent(monkeypatch, tmp_path):
    settings, args = _prepare(monkeypatch, tmp_path / "state")
    before = args.receipt.read_bytes()
    state_tool.prepare(args)
    state_tool.validate(args)
    assert args.receipt.read_bytes() != before  # timestamp is renewed
    assert stat.S_IMODE(args.receipt.parent.stat().st_mode) == 0o755
    for path in database._state_directories(settings):
        info = path.stat()
        assert (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == (
            os.getuid(), os.getgid(), 0o2770,
        )
    assert stat.S_IMODE(settings.db_path.stat().st_mode) == 0o660
    assert state_permissions.validate_runtime_shared_state(
        data_dir=settings.data_dir,
        owner_uid=os.getuid(),
        shared_gid=os.getgid(),
        receipt_path=args.receipt,
    )["runtime_mutations"] == []


def test_runtime_never_chmods_or_chowns_shared_state(monkeypatch, tmp_path):
    settings, _args = _prepare(monkeypatch, tmp_path / "prepared")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("shared runtime permission mutation is forbidden")

    monkeypatch.setattr(state_permissions.os, "chmod", forbidden, raising=False)
    monkeypatch.setattr(database.os, "chmod", forbidden)
    monkeypatch.setattr(database.os, "chown", forbidden, raising=False)
    database.ensure_ready(settings)
    with database.read_conn(settings) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() is not None


def test_receipt_survives_normal_database_writes_and_process_restart(monkeypatch, tmp_path):
    settings, _args = _prepare(monkeypatch, tmp_path / "restart")
    database.ensure_ready(settings)
    repositories.create_worker(
        display_name="12F durable receipt",
        instance_id="inst_" + uuid.uuid4().hex,
        worker_version="12f-namespace",
        protocol_version=1,
        pipeline_revision="isolated",
        capabilities={"job_types": ["test_pipeline_v1"], "compressions": ["gzip"]},
        configured_max_slots=1,
        settings=settings,
    )
    database.reset_state_for_tests()
    assert database.ensure_ready(settings) == settings.db_path


def test_overflow_gid_is_never_a_source_of_trust(monkeypatch):
    monkeypatch.setattr(
        state_permissions, "_read_id_map",
        lambda kind: (state_permissions.IdMapRange(1001, 1001, 1),),
    )
    monkeypatch.setattr(state_permissions, "_overflow_id", lambda kind: 65534)
    with pytest.raises(DistributedWorkersConfigError, match="not trusted"):
        state_permissions._validate_namespace_id(
            actual=65534, expected_host=984, kind="gid", receipt_backed=False
        )
    assert state_permissions._validate_namespace_id(
        actual=65534, expected_host=984, kind="gid", receipt_backed=True
    ) == "trusted_receipt_plus_unmapped_overflow"
    with pytest.raises(DistributedWorkersConfigError, match="unverifiable"):
        state_permissions._validate_namespace_id(
            actual=65533, expected_host=984, kind="gid", receipt_backed=True
        )


def test_current_user_owned_receipt_is_rejected_as_forgeable(tmp_path):
    receipt_dir = tmp_path / "receipt"
    receipt_dir.mkdir(mode=0o700)
    receipt = receipt_dir / "state.json"
    receipt.write_text("{}", encoding="utf-8")
    receipt.chmod(0o444)
    with pytest.raises(DistributedWorkersConfigError):
        state_permissions._validate_receipt_boundary(receipt)


def test_wrong_mode_fails_before_database_open(monkeypatch, tmp_path):
    settings, _args = _prepare(monkeypatch, tmp_path / "wrong-mode")
    settings.data_dir.chmod(0o2777)
    with pytest.raises(DistributedWorkersConfigError, match="mode mismatch"):
        database.ensure_ready(settings)


def test_wrong_owner_configuration_fails_without_chown(monkeypatch, tmp_path):
    settings, _args = _prepare(monkeypatch, tmp_path / "wrong-owner")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_OWNER_UID", str(os.getuid() + 1))
    with pytest.raises(DistributedWorkersConfigError, match="owner_uid mismatch"):
        database.ensure_ready(get_settings())
    assert settings.db_path.exists()


def test_wrong_host_gid_is_rejected_by_authoritative_validator(monkeypatch, tmp_path):
    _settings, args = _prepare(monkeypatch, tmp_path / "wrong-gid")
    args.shared_gid = os.getgid() + 1
    with pytest.raises(SystemExit, match="group identity mismatch"):
        state_tool.validate_host(args)


def test_named_access_acl_is_rejected(monkeypatch, tmp_path):
    settings, _args = _prepare(monkeypatch, tmp_path / "unsafe-acl")
    undefined = 0xFFFFFFFF
    entries = (
        (0x01, 0o7, undefined),
        (0x02, 0o7, os.getuid()),
        (0x04, 0o7, undefined),
        (0x10, 0o7, undefined),
        (0x20, 0o0, undefined),
    )
    raw = bytearray(struct.pack("<I", 2))
    for entry in entries:
        raw.extend(struct.pack("<HHI", *entry))
    os.setxattr(settings.incoming_dir, "system.posix_acl_access", bytes(raw))
    with pytest.raises(DistributedWorkersConfigError, match="named access ACL"):
        database.ensure_ready(settings)


def test_inherited_sqlite_access_acl_uses_mask_for_group_mode(monkeypatch, tmp_path):
    settings, _args = _prepare(monkeypatch, tmp_path / "inherited-acl")
    sidecar = settings.db_path.with_name(settings.db_path.name + "-wal")
    previous_umask = os.umask(0o077)
    try:
        descriptor = os.open(sidecar, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        os.close(descriptor)
    finally:
        os.umask(previous_umask)

    raw = os.getxattr(sidecar, "system.posix_acl_access")
    entries = state_permissions._decode_acl(raw, sidecar, "access")
    undefined = 0xFFFFFFFF
    assert entries[(0x04, undefined)] == 0o7  # inherited GROUP_OBJ
    assert entries[(0x10, undefined)] == 0o6  # mode group bits / effective mask
    assert stat.S_IMODE(sidecar.stat().st_mode) == 0o660
    state_permissions.validate_shared_file(
        sidecar,
        shared_gid=os.getgid(),
        allowed_owner_uids={os.getuid()},
    )


def test_prepare_recovers_e601_wal_preserves_data_and_mints_receipt_last(
    monkeypatch, tmp_path
):
    settings, args = _prepare(monkeypatch, tmp_path / "e601-redeploy")
    connection = sqlite3.connect(settings.db_path, isolation_level=None)
    try:
        assert connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
        connection.execute("CREATE TABLE redeploy_probe(value TEXT NOT NULL)")
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()

    # Exact legacy e601 policy: directories and the main DB become private.
    for directory in database._state_directories(settings):
        directory.chmod(0o700)
    settings.db_path.chmod(0o600)

    # Model the e601 process-level lifecycle: a committed WAL transaction is
    # followed by process exit without sqlite3_close() for thread-local conns.
    # This leaves recoverable durable WAL state despite a clean systemd stop.
    subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import os,sqlite3,sys; "
                "c=sqlite3.connect(sys.argv[1],isolation_level=None); "
                "c.execute('PRAGMA journal_mode=WAL'); "
                "c.execute('PRAGMA wal_autocheckpoint=0'); "
                "c.execute(\"INSERT INTO redeploy_probe VALUES "
                "('committed-in-wal')\"); os._exit(0)"
            ),
            str(settings.db_path),
        ],
        check=True,
    )
    wal = settings.db_path.with_name(settings.db_path.name + "-wal")
    shm = settings.db_path.with_name(settings.db_path.name + "-shm")
    assert wal.exists()
    assert shm.exists()
    assert stat.S_IMODE(settings.data_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(settings.db_path.stat().st_mode) == 0o600

    # An immutable main-file read deliberately ignores WAL: the committed row
    # is not checkpointed yet, so deleting WAL here would lose it.
    immutable = sqlite3.connect(
        f"{settings.db_path.as_uri()}?mode=ro&immutable=1", uri=True
    )
    try:
        assert immutable.execute("SELECT COUNT(*) FROM redeploy_probe").fetchone()[0] == 0
    finally:
        immutable.close()

    state_tool.prepare(args)
    state_tool.validate(args)
    receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
    assert receipt["objects"]
    for path in state_tool._known_database_files(settings.data_dir):
        info = path.stat()
        assert info.st_gid == os.getgid()
        assert stat.S_IMODE(info.st_mode) == 0o660

    recovered = sqlite3.connect(settings.db_path)
    try:
        assert recovered.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert recovered.execute("SELECT value FROM redeploy_probe").fetchall() == [
            ("committed-in-wal",)
        ]
    finally:
        recovered.close()

    # The new hardened runtime accepts the receipt and opens the recovered DB.
    monkeypatch.setattr(
        state_permissions,
        "_read_id_map",
        lambda kind: (
            state_permissions.IdMapRange(
                os.getuid() if kind == "uid" else os.getgid(),
                os.getuid() if kind == "uid" else os.getgid(),
                1,
            ),
        ),
    )
    assert database.ensure_ready(settings) == settings.db_path
    with database.read_conn(settings) as runtime_connection:
        assert runtime_connection.execute("SELECT value FROM redeploy_probe").fetchone()[0] == (
            "committed-in-wal"
        )


def test_prepare_fails_closed_while_sqlite_writer_is_active(monkeypatch, tmp_path):
    settings, args = _prepare(monkeypatch, tmp_path / "active-writer")
    receipt_before = args.receipt.read_bytes()
    settings.data_dir.chmod(0o700)
    settings.db_path.chmod(0o600)
    writer = sqlite3.connect(settings.db_path, isolation_level=None)
    try:
        writer.execute("PRAGMA journal_mode = WAL")
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("CREATE TABLE active_writer_probe(value INTEGER)")
        with pytest.raises(SystemExit, match="SQLite is busy|non-quiescent"):
            state_tool.prepare(args)
        assert args.receipt.read_bytes() == receipt_before
        assert stat.S_IMODE(settings.data_dir.stat().st_mode) == 0o700
        assert stat.S_IMODE(settings.db_path.stat().st_mode) == 0o600
    finally:
        writer.execute("ROLLBACK")
        writer.close()


def test_prepare_normalizes_legacy_rollback_journal(monkeypatch, tmp_path):
    settings, args = _prepare(monkeypatch, tmp_path / "rollback-journal")
    journal = settings.db_path.with_name(settings.db_path.name + "-journal")
    journal.write_bytes(b"non-hot legacy journal fixture")
    journal.chmod(0o600)
    monkeypatch.setattr(state_tool, "_require_database_quiescent", lambda _path: (0, -1, -1))

    state_tool.prepare(args)

    assert journal in state_tool._known_database_files(settings.data_dir)
    assert stat.S_IMODE(journal.stat().st_mode) == 0o660
    receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
    assert "workers.db-journal" in {item["path"] for item in receipt["objects"]}


def test_object_replacement_is_detected(monkeypatch, tmp_path):
    settings, _args = _prepare(monkeypatch, tmp_path / "replacement")
    target = settings.incoming_dir
    target.rmdir()
    target.mkdir(mode=0o2770)
    target.chmod(0o2770)
    os.setxattr(
        target, "system.posix_acl_default", state_permissions.encode_default_acl()
    )
    with pytest.raises(DistributedWorkersConfigError, match="object identity"):
        database.ensure_ready(settings)


def test_missing_receipt_configuration_is_typed(monkeypatch, tmp_path):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "missing"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_OWNER_UID", str(os.getuid()))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_GID", str(os.getgid()))
    with pytest.raises(DistributedWorkersConfigError, match="SHARED_RECEIPT"):
        get_settings()


def test_preparation_refuses_active_backend(monkeypatch, tmp_path):
    _settings, args = _prepare(monkeypatch, tmp_path / "active-backend")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        args.backend_port = listener.getsockname()[1]
        with pytest.raises(SystemExit, match="backend listener is active"):
            state_tool.prepare(args)


def test_authenticated_polling_request_path_does_not_revalidate_or_mutate(
    monkeypatch, tmp_path
):
    settings, _args = _prepare(monkeypatch, tmp_path / "polling")
    database.ensure_ready(settings)
    worker = repositories.create_worker(
        display_name="12F namespace polling",
        instance_id="inst_" + uuid.uuid4().hex,
        worker_version="12f-namespace",
        protocol_version=1,
        pipeline_revision="isolated",
        capabilities={"job_types": ["test_pipeline_v1"], "compressions": ["gzip"]},
        configured_max_slots=1,
        settings=settings,
    )
    repositories.update_worker_fields(
        worker["worker_id"],
        {"registration_status": "approved", "worker_state": "idle"},
        settings=settings,
    )
    token = auth.generate_token()
    repositories.insert_token(worker["worker_id"], auth.hash_token(token), settings=settings)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("request path must not revalidate filesystem contract")

    monkeypatch.setattr(database, "validate_runtime_shared_state", forbidden)
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Worker-Id": worker["worker_id"],
        "X-Instance-Id": worker["instance_id"],
        "X-Protocol-Version": "1",
    }

    async def poll():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=make_center_app()),
            base_url="http://center.test",
        ) as client:
            heartbeat = await client.post(
                "/api/v1/worker/heartbeat",
                headers=headers,
                json={
                    "instance_id": worker["instance_id"],
                    "sent_at": time.time(),
                    "worker_state": "idle",
                    "configured_max_slots": 1,
                    "calculated_free_slots": 1,
                    "active_jobs": [],
                    "max_verified_slots": 1,
                },
            )
            commands = await client.get("/api/v1/worker/commands", headers=headers)
            return heartbeat, commands

    heartbeat, commands = asyncio.run(poll())
    assert heartbeat.status_code == 200
    assert commands.status_code == 200
    assert commands.json() == {"commands": []}
