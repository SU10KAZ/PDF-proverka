from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from audit_worker.mtls_identity import make_csr
from backend.app.security.ca_factory import create_issuing_ca, create_root_ca
from backend.app.security.certificate_profiles import (
    CertificateIssuer,
    validate_worker_certificate,
)
from backend.app.security.issuer_rpc import UnixSocketEnrollmentAuthority
from backend.app.services.distributed_workers import database, repositories
from backend.app.services.distributed_workers.certificate_registry import (
    CertificateRegistry,
)
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersConfigError,
    get_settings,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _key_pem(key) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _write(path: Path, value: bytes, mode: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    path.chmod(mode)
    return path


def _material(root: Path) -> dict[str, Path]:
    root_key, root_cert = create_root_ca()
    issuer_key, issuer_cert = create_issuing_ca(root_key, root_cert)
    root_pem = root_cert.public_bytes(serialization.Encoding.PEM)
    issuer_pem = issuer_cert.public_bytes(serialization.Encoding.PEM)
    issuer_chain = issuer_pem + root_pem
    server_key = ec.generate_private_key(ec.SECP256R1())
    server = CertificateIssuer(
        issuer_cert, issuer_key, chain_pem=issuer_chain
    ).issue_server(
        server_key.public_key(), identity="127.0.0.1", lifetime=timedelta(days=30)
    )
    return {
        "root_key": _write(root / "offline-root/root-key.pem", _key_pem(root_key), 0o600),
        "root_cert": _write(root / "offline-root/root-ca.pem", root_pem, 0o644),
        "issuer_key": _write(root / "issuer/issuing-ca-key.pem", _key_pem(issuer_key), 0o600),
        "issuer_cert": _write(root / "issuer/issuing-ca.pem", issuer_pem, 0o640),
        "issuer_chain": _write(root / "issuer/issuing-chain.pem", issuer_chain, 0o640),
        "server_key": _write(root / "gateway/server-key.pem", _key_pem(server_key), 0o600),
        "server_cert": _write(
            root / "gateway/server-chain.pem",
            server.certificate_pem + issuer_chain,
            0o640,
        ),
        "worker_trust": _write(
            root / "worker-trust/worker-ca-bundle.pem", issuer_chain, 0o640
        ),
    }


def _sha(paths: dict[str, Path]) -> dict[str, str]:
    return {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in paths.items()
    }


def _validate_command(paths: dict[str, Path]) -> list[str]:
    command = [sys.executable, str(REPO_ROOT / "scripts/manage_worker_pki.py"), "validate-set"]
    for name, path in paths.items():
        command.extend((f"--{name.replace('_', '-')}", str(path)))
    command.extend(("--identity", "127.0.0.1"))
    return command


@pytest.fixture(autouse=True)
def _reset_database_state(monkeypatch):
    monkeypatch.delenv("DISTRIBUTED_WORKERS_SHARED_STATE", raising=False)
    monkeypatch.delenv("DISTRIBUTED_WORKERS_SHARED_GID", raising=False)
    database.reset_state_for_tests()
    yield
    database.reset_state_for_tests()


def test_shared_gid_setting_is_typed_and_fail_closed(monkeypatch, tmp_path):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_GID", str(os.getgid()))
    assert get_settings().shared_state_gid == os.getgid()
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_GID", "gateway-group")
    with pytest.raises(DistributedWorkersConfigError, match="numeric POSIX GID"):
        get_settings()
    monkeypatch.delenv("DISTRIBUTED_WORKERS_SHARED_GID")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "perhaps")
    with pytest.raises(DistributedWorkersConfigError, match="explicit boolean"):
        get_settings()


def test_default_state_permissions_remain_private(monkeypatch, tmp_path):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "private"))
    settings = get_settings()
    database.ensure_ready(settings)
    assert stat.S_IMODE(settings.data_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(settings.db_path.stat().st_mode) == 0o600


def test_shared_state_permissions_are_exact_and_idempotent(monkeypatch, tmp_path):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "shared"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_GID", str(os.getgid()))
    settings = get_settings()
    database.ensure_ready(settings)
    settings.data_dir.chmod(0o700)
    settings.db_path.chmod(0o600)
    database.ensure_ready(settings)
    for directory in (
        settings.data_dir,
        settings.source_packages_dir,
        settings.incoming_dir,
        settings.result_staging_dir,
        settings.validated_results_dir,
        settings.rejected_results_dir,
        settings.superseded_results_dir,
        settings.job_logs_dir,
    ):
        info = directory.stat()
        assert info.st_gid == os.getgid()
        assert stat.S_IMODE(info.st_mode) == 0o2770
    info = settings.db_path.stat()
    assert info.st_gid == os.getgid()
    assert stat.S_IMODE(info.st_mode) == 0o660


def test_acl_shared_mode_repairs_mask_without_requiring_group_change(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "acl-shared"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "true")
    settings = get_settings()
    database.ensure_ready(settings)
    original_gid = settings.data_dir.stat().st_gid
    settings.data_dir.chmod(0o700)
    settings.db_path.chmod(0o600)
    database.ensure_ready(settings)
    assert settings.data_dir.stat().st_gid == original_gid
    assert stat.S_IMODE(settings.data_dir.stat().st_mode) == 0o2770
    assert stat.S_IMODE(settings.db_path.stat().st_mode) == 0o660


def test_unrepairable_shared_gid_mismatch_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "wrong-gid"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_GID", str(os.getgid() + 1))

    def denied(*_args, **_kwargs):
        raise PermissionError("test denies chgrp")

    monkeypatch.setattr(database.os, "chown", denied)
    with pytest.raises(RuntimeError, match="expected configured GID"):
        database.ensure_ready(get_settings())


def test_complete_partial_run_is_validation_only_and_preserves_ca(tmp_path):
    paths = _material(tmp_path / "partial-production-like-pki")
    before = _sha(paths)
    first = subprocess.run(
        _validate_command(paths),
        cwd=tmp_path,
        env={
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": str(REPO_ROOT),
            "PYTHONDONTWRITEBYTECODE": "1",
            "AUDIT_DISABLE_DOTENV": "1",
        },
        text=True,
        capture_output=True,
        check=True,
    )
    second = subprocess.run(
        _validate_command(paths),
        cwd=tmp_path,
        env={
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": str(REPO_ROOT),
            "PYTHONDONTWRITEBYTECODE": "1",
            "AUDIT_DISABLE_DOTENV": "1",
        },
        text=True,
        capture_output=True,
        check=True,
    )
    assert _sha(paths) == before
    assert json.loads(first.stdout) == json.loads(second.stdout)
    assert json.loads(second.stdout)["status"] == "valid_existing_pki_preserved"
    assert json.loads(second.stdout)["private_key_contents_exposed"] is False


def test_launcher_from_immutable_copy_recovers_after_failed_partial_run_and_signs(
    monkeypatch, tmp_path
):
    paths = _material(tmp_path / "pki")
    before = _sha(paths)
    data_dir = tmp_path / "center"
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(data_dir))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_STATE", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_SHARED_GID", str(os.getgid()))
    settings = get_settings()
    worker = repositories.create_worker(
        display_name="12F recovery isolated Worker",
        instance_id="inst_" + uuid.uuid4().hex,
        worker_version="12f-recovery",
        protocol_version=1,
        pipeline_revision="isolated",
        capabilities={"job_types": ["test_pipeline_v1"], "compressions": ["gzip"]},
        configured_max_slots=1,
        settings=settings,
    )
    repositories.update_worker_fields(
        worker["worker_id"],
        {
            "registration_status": "approved",
            "worker_state": "idle",
            "max_verified_slots": 1,
            "calculated_free_slots": 1,
        },
        settings=settings,
    )
    database.reset_state_for_tests()

    release_app = tmp_path / "immutable-release/app"
    for package in ("backend", "contracts"):
        shutil.copytree(
            REPO_ROOT / package,
            release_app / package,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
    for path in sorted(release_app.rglob("*"), reverse=True):
        path.chmod(0o555 if path.is_dir() else 0o444)
    release_app.chmod(0o555)
    unrelated_cwd = tmp_path / "caller-cwd"
    unrelated_cwd.mkdir()
    socket_path = tmp_path / "run/issuer.sock"
    base_env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONPATH": str(release_app),
        "PYTHONDONTWRITEBYTECODE": "1",
        "AUDIT_DISABLE_DOTENV": "1",
        "AUDIT_ROOT_DIR": str(release_app),
        "DISTRIBUTED_WORKERS_ENABLED": "true",
        "DISTRIBUTED_WORKERS_DATA_DIR": str(data_dir),
        "DISTRIBUTED_WORKERS_SHARED_STATE": "true",
        "DISTRIBUTED_WORKERS_SHARED_GID": str(os.getgid()),
        "AUDIT_WORKER_ISSUER_SOCKET": str(socket_path),
        "AUDIT_WORKER_ISSUER_KEY": str(paths["issuer_key"]),
        "AUDIT_WORKER_ISSUER_CERT": str(paths["issuer_cert"]),
        "AUDIT_WORKER_ISSUER_CHAIN": str(paths["issuer_chain"]),
    }
    command = [sys.executable, "-m", "backend.app.security.issuer_service"]

    failed = subprocess.run(
        command,
        cwd=unrelated_cwd,
        env={**base_env, "AUDIT_WORKER_ISSUER_ALLOWED_UIDS": ""},
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert failed.returncode != 0
    assert "issuer allowed UID set is empty" in failed.stderr
    assert _sha(paths) == before

    process = subprocess.Popen(
        command,
        cwd=unrelated_cwd,
        env={**base_env, "AUDIT_WORKER_ISSUER_ALLOWED_UIDS": str(os.getuid())},
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not socket_path.is_socket():
            if process.poll() is not None:
                break
            time.sleep(0.05)
        if not socket_path.is_socket():
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=15)
            pytest.fail(
                f"isolated issuer socket absent (rc={process.returncode}); "
                f"stdout={stdout!r}; stderr={stderr!r}"
            )
        assert process.poll() is None
        key = ec.generate_private_key(ec.SECP256R1())
        csr_pem = make_csr(_key_pem(key), worker["worker_id"])
        client = UnixSocketEnrollmentAuthority(socket_path)
        first = client.enroll(
            worker_id=worker["worker_id"],
            instance_id=worker["instance_id"],
            csr_pem=csr_pem,
            request_id="12f-recovery-isolated-request",
        )
        second = client.enroll(
            worker_id=worker["worker_id"],
            instance_id=worker["instance_id"],
            csr_pem=csr_pem,
            request_id="12f-recovery-isolated-request",
        )
        assert second.serial_hex == first.serial_hex
        leaf = x509.load_pem_x509_certificate(first.certificate_chain_pem)
        assert validate_worker_certificate(
            leaf, expected_worker_id=worker["worker_id"]
        ) == worker["worker_id"]
        database.reset_state_for_tests()
        registry = CertificateRegistry(settings)
        assert registry.by_serial(first.serial_hex)["status"] == "ACTIVE"
        with database.read_conn(settings) as connection:
            assert connection.execute(
                "SELECT COUNT(*) FROM worker_certificates"
            ).fetchone()[0] == 1
        assert _sha(paths) == before
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=15)
    assert process.returncode == 0, stdout + stderr


def test_systemd_sources_pin_module_root_and_shared_umask():
    issuer = (REPO_ROOT / "deploy/systemd/web-ocr-worker-cert-issuer.service").read_text()
    gateway = (REPO_ROOT / "deploy/systemd/web-ocr-agent-gateway.service").read_text()
    for unit in (issuer, gateway):
        assert "WorkingDirectory=/opt/auditmanager/current/app" in unit
        assert "Environment=PYTHONPATH=/opt/auditmanager/current/app" in unit
        assert "/opt/auditmanager/current/venv/bin/python -m backend.app." in unit
        assert "UMask=0007" in unit
        assert "ReadWritePaths=/var/lib/auditmanager/distributed_workers" in unit
        assert "/var/lib/auditmanager/distributed-workers" not in unit
    assert "pki/issuer" not in gateway
    assert "AUDIT_WORKER_ISSUER_KEY" not in gateway
