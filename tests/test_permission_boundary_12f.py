from __future__ import annotations

import asyncio
import os
import stat
import uuid
from argparse import Namespace

import httpx
import pytest

from backend.app.services.distributed_workers import auth, database, repositories
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersConfigError,
    get_settings,
)
from backend.app.services.distributed_workers import state_permissions
from scripts.manage_distributed_worker_state import prepare as prepare_shared_state
from tests.distributed_workers_helpers import make_center_app


def _prepare(monkeypatch, data_dir):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(data_dir))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_OWNER_UID", str(os.getuid()))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_GID", str(os.getgid()))
    args = Namespace(
        data_dir=data_dir,
        owner_uid=os.getuid(),
        shared_gid=os.getgid(),
        service_uid=[],
    )
    prepare_shared_state(args)
    return get_settings(), args


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    for name in (
        "DISTRIBUTED_WORKERS_SHARED_STATE",
        "DISTRIBUTED_WORKERS_SHARED_OWNER_UID",
        "DISTRIBUTED_WORKERS_SHARED_GID",
    ):
        monkeypatch.delenv(name, raising=False)
    database.reset_state_for_tests()
    yield
    database.reset_state_for_tests()


def test_privileged_bootstrap_is_exact_and_idempotent(monkeypatch, tmp_path):
    settings, args = _prepare(monkeypatch, tmp_path / "state")
    database.ensure_ready(settings)
    before = {
        path: (path.stat().st_uid, path.stat().st_gid, stat.S_IMODE(path.stat().st_mode))
        for path in database._state_directories(settings)
    }
    prepare_shared_state(args)
    after = {
        path: (path.stat().st_uid, path.stat().st_gid, stat.S_IMODE(path.stat().st_mode))
        for path in database._state_directories(settings)
    }
    assert after == before
    assert set(after.values()) == {(os.getuid(), os.getgid(), 0o2770)}
    assert stat.S_IMODE(settings.db_path.stat().st_mode) == 0o660


def test_prepared_shared_state_never_runtime_chmods_a_directory(
    monkeypatch, tmp_path
):
    settings, _args = _prepare(monkeypatch, tmp_path / "prepared")
    database.ensure_ready(settings)
    database.reset_state_for_tests()
    real_chmod = state_permissions.os.chmod
    calls: list[tuple[str, int]] = []

    def guarded(path, mode, *args, **kwargs):
        calls.append((str(path), mode))
        if stat.S_ISDIR(os.lstat(path).st_mode):
            raise AssertionError("runtime directory chmod is forbidden")
        return real_chmod(path, mode, *args, **kwargs)

    monkeypatch.setattr(state_permissions.os, "chmod", guarded)
    database.ensure_ready(settings)
    with database.read_conn(settings) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() is not None
    assert all(mode == 0o660 for _path, mode in calls)


def test_wrong_shared_directory_mode_fails_closed_before_database_open(
    monkeypatch, tmp_path
):
    settings, _args = _prepare(monkeypatch, tmp_path / "wrong-mode")
    settings.data_dir.chmod(0o770)
    with pytest.raises(DistributedWorkersConfigError, match="metadata mismatch"):
        database.ensure_ready(settings)
    assert not settings.db_path.exists()


def test_wrong_shared_directory_owner_fails_closed_without_chown(
    monkeypatch, tmp_path
):
    settings, _args = _prepare(monkeypatch, tmp_path / "wrong-owner")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_OWNER_UID", str(os.getuid() + 1))

    def forbidden(*_args, **_kwargs):
        raise AssertionError("runtime chown is forbidden")

    monkeypatch.setattr(database.os, "chown", forbidden, raising=False)
    with pytest.raises(DistributedWorkersConfigError, match="metadata mismatch"):
        database.ensure_ready(get_settings())
    assert not settings.db_path.exists()


def test_missing_shared_identity_configuration_is_typed(monkeypatch, tmp_path):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "missing"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "true")
    with pytest.raises(DistributedWorkersConfigError, match="requires exact"):
        get_settings()


def test_authenticated_commands_poll_has_no_runtime_sgid_chmod(monkeypatch, tmp_path):
    settings, _args = _prepare(monkeypatch, tmp_path / "polling")
    worker = repositories.create_worker(
        display_name="12F hardened polling",
        instance_id="inst_" + uuid.uuid4().hex,
        worker_version="12f-permission",
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
    database.reset_state_for_tests()

    real_chmod = state_permissions.os.chmod
    calls: list[tuple[str, int]] = []

    def guarded(path, mode, *args, **kwargs):
        calls.append((str(path), mode))
        if mode & stat.S_ISGID or stat.S_ISDIR(os.lstat(path).st_mode):
            raise PermissionError("RestrictSUIDSGID regression guard")
        return real_chmod(path, mode, *args, **kwargs)

    monkeypatch.setattr(state_permissions.os, "chmod", guarded)
    # This unit test is about the request-path permission call graph.  The
    # separate real-systemd proof exercises the production to_thread boundary.
    async def inline_run_db(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(database, "run_db", inline_run_db)
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
            return await client.get("/api/v1/worker/commands", headers=headers)

    response = asyncio.run(poll())
    assert response.status_code == 200
    assert response.json() == {"commands": []}
    assert not any(mode & stat.S_ISGID for _path, mode in calls)
    assert not any(stat.S_ISDIR(os.lstat(path).st_mode) for path, _mode in calls)
