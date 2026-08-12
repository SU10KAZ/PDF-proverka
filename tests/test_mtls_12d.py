from __future__ import annotations

import asyncio
import os
import stat
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import grpc
import pytest
import pytest_asyncio
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from audit_worker.certificate_renewal import WorkerCertificateRotator
from audit_worker.config import InsecureTransportError, WorkerConfig, validate_control_transport
from audit_worker.key_store import (
    KeyStoreError,
    LinuxPermissionKeyStore,
    WindowsDpapiKeyStore,
)
from audit_worker.mtls_identity import install_public_identity, make_csr
from backend.app.agent_gateway.config import GatewayConfig, GatewayConfigError
from backend.app.agent_gateway.server import GatewayServer
from backend.app.security.ca_factory import create_issuing_ca, create_root_ca
from backend.app.security.issuer_rpc import (
    UnixSocketEnrollmentAuthority,
    UnixSocketRenewalAuthority,
)
from backend.app.security.issuer_service import IssuerServer
from backend.app.security.certificate_profiles import (
    CertificateIssuer,
    CertificateProfileError,
    certificate_fingerprint,
    cert_not_after,
    cert_not_before,
    csr_sha256,
    serial_hex,
    validate_server_certificate,
    validate_worker_certificate,
)
from backend.app.services.distributed_workers import database, repositories
from backend.app.services.distributed_workers import registration_service
from backend.app.services.distributed_workers.certificate_lifecycle import (
    CertificateLifecycleAuthority,
    CertificateLifecycleError,
)
from backend.app.services.distributed_workers.certificate_registry import (
    CertificateRegistry,
    CertificateRegistryError,
    PresentedCertificate,
    RevocationReason,
)
from backend.app.services.distributed_workers.settings import get_settings
from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb
from contracts.agent_stream.v1 import agent_stream_pb2_grpc as stream_grpc


def _pem_key(key) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _write(path: Path, data: bytes, mode=0o644):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    os.chmod(path, mode)
    return path


def _worker(settings, worker_id_hint=""):
    row = repositories.create_worker(
        display_name="12d test worker", instance_id="inst_" + uuid.uuid4().hex,
        worker_version="12d", protocol_version=1, pipeline_revision="test",
        capabilities={"job_types": ["test_pipeline_v1"], "compressions": ["gzip"]},
        configured_max_slots=1, settings=settings,
    )
    repositories.update_worker_fields(
        row["worker_id"],
        {"registration_status": "approved", "worker_state": "idle",
         "max_verified_slots": 1, "calculated_free_slots": 1},
        settings=settings,
    )
    return repositories.get_worker(row["worker_id"], settings=settings)


class Pki:
    def __init__(self, root: Path):
        self.root_key, self.root_cert = create_root_ca()
        self.issuing_key, self.issuing_cert = create_issuing_ca(
            self.root_key, self.root_cert
        )
        self.root_pem = self.root_cert.public_bytes(serialization.Encoding.PEM)
        self.chain_pem = (
            self.issuing_cert.public_bytes(serialization.Encoding.PEM) + self.root_pem
        )
        self.issuer = CertificateIssuer(
            self.issuing_cert, self.issuing_key, chain_pem=self.chain_pem
        )
        self.server_key = ec.generate_private_key(ec.SECP256R1())
        self.server = self.issuer.issue_server(
            self.server_key.public_key(), identity="127.0.0.1",
            lifetime=timedelta(days=30),
        )
        self.server_key_path = _write(root / "server.key", _pem_key(self.server_key), 0o600)
        self.server_cert_path = _write(
            root / "server.pem", self.server.certificate_pem + self.chain_pem
        )
        self.ca_path = _write(root / "root.pem", self.root_pem)

    def issue_worker(self, worker_id: str, *, now=None, lifetime=timedelta(days=30)):
        key = ec.generate_private_key(ec.SECP256R1())
        csr = x509.load_pem_x509_csr(make_csr(_pem_key(key), worker_id))
        issued = self.issuer.issue_worker(
            csr, worker_id=worker_id, lifetime=lifetime, now=now
        )
        return key, csr, issued


@pytest.fixture()
def center(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", "s" * 32)
    database.reset_state_for_tests()
    settings = get_settings()
    database.ensure_ready(settings)
    yield settings
    database.reset_state_for_tests()


def _record(registry, worker, csr, issued, request="crq_initial"):
    cert = issued.certificate
    return registry.record_issuance(
        PresentedCertificate(
            serial_hex=serial_hex(cert),
            fingerprint_sha256=certificate_fingerprint(cert),
            worker_id=worker["worker_id"],
            not_before=cert_not_before(cert).timestamp(),
            not_after=cert_not_after(cert).timestamp(),
            issuer_id=issued.issuer_id,
            certificate_pem=issued.certificate_pem + issued.chain_pem,
        ),
        csr_sha256=csr_sha256(csr), request_id=request,
        instance_id=worker["instance_id"],
    )


@pytest.fixture()
def identity(tmp_path, center):
    pki = Pki(tmp_path / "pki")
    worker = _worker(center)
    key, csr, issued = pki.issue_worker(worker["worker_id"])
    registry = CertificateRegistry(center)
    row = _record(registry, worker, csr, issued)
    key_dir = tmp_path / "identity"
    store = LinuxPermissionKeyStore(key_dir)
    store.store_private_key(_pem_key(key))
    cert_path = key_dir / "client-cert.pem"
    ca_path = key_dir / "ca-bundle.pem"
    install_public_identity(
        certificate_path=cert_path, trust_bundle_path=ca_path,
        certificate_chain_pem=issued.certificate_pem + issued.chain_pem,
        trust_bundle_pem=pki.root_pem,
    )
    return pki, worker, registry, store, cert_path, ca_path, row


def _gateway_config(pki: Pki, *, identity="127.0.0.1", interval=.1):
    return GatewayConfig(
        host="127.0.0.1", port=0, security_mode="mtls",
        server_certificate_path=pki.server_cert_path,
        server_private_key_path=pki.server_key_path,
        client_ca_bundle_path=pki.ca_path,
        server_identity=identity,
        certificate_check_interval_sec=interval,
        graceful_shutdown_sec=.1,
    )


def _credentials(pki, key, issued):
    return grpc.ssl_channel_credentials(
        root_certificates=pki.root_pem,
        private_key=_pem_key(key),
        certificate_chain=issued.certificate_pem + issued.chain_pem,
    )


def _hello(worker, hello_worker=None, epoch=1):
    wid = hello_worker or worker["worker_id"]
    return stream_pb.AgentToCenter(
        protocol_version=1, message_id="hello_" + uuid.uuid4().hex,
        worker_id=wid, stream_sequence=1,
        hello=stream_pb.AgentHello(
            worker_id=wid, worker_instance_id=worker["instance_id"],
            supported_protocol_versions=[1], worker_software_version="12d",
            execution_revision="test", max_slots=1, connection_epoch=epoch,
        ),
    )


async def _connect(port, creds, worker, hello_worker=None, epoch=1):
    channel = grpc.aio.secure_channel(f"127.0.0.1:{port}", creds)
    call = stream_grpc.AgentStreamServiceStub(channel).Connect()
    await call.write(_hello(worker, hello_worker, epoch=epoch))
    return channel, call


def test_profiles_are_strict_and_uri_san(identity):
    pki, worker, _, _, _, _, row = identity
    cert = x509.load_pem_x509_certificate(row["certificate_pem"])
    assert validate_worker_certificate(cert) == worker["worker_id"]
    validate_server_certificate(pki.server.certificate, expected_identity="127.0.0.1")
    with pytest.raises(CertificateProfileError):
        validate_server_certificate(pki.server.certificate, expected_identity="wrong.invalid")


def test_registry_persistence_idempotency_and_no_private_key(identity, center):
    _, worker, registry, _, _, _, row = identity
    restarted = CertificateRegistry(center)
    assert restarted.by_serial(row["serial_hex"])["worker_id"] == worker["worker_id"]
    assert "private" not in " ".join(restarted.by_serial(row["serial_hex"]).keys()).lower()
    assert restarted.by_request("crq_initial")["serial_hex"] == row["serial_hex"]


def test_linux_key_store_permissions_and_symlink_guard(tmp_path):
    store = LinuxPermissionKeyStore(tmp_path / "identity")
    store.generate()
    assert stat.S_IMODE(store.root.stat().st_mode) == 0o700
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600
    bad_root = tmp_path / "link"
    bad_root.symlink_to(store.root, target_is_directory=True)
    with pytest.raises(KeyStoreError):
        LinuxPermissionKeyStore(bad_root).load_private_key()


def test_windows_dpapi_platform_guard(tmp_path):
    if os.name != "nt":
        with pytest.raises(KeyStoreError, match="Windows"):
            WindowsDpapiKeyStore(tmp_path / "dpapi").store_private_key(b"secret")


def test_worker_mtls_config_fail_closed(identity, tmp_path):
    _, _, _, store, cert_path, ca_path, _ = identity
    cfg = WorkerConfig(
        dispatcher_url="https://center.invalid", root=tmp_path / "w", display_name="w",
        control_transport="grpc", grpc_target="127.0.0.1:8443",
        grpc_security_mode="mtls", grpc_ca_bundle_path=ca_path,
        grpc_client_certificate_path=cert_path, grpc_key_store_dir=store.root,
        grpc_server_identity="127.0.0.1",
    )
    validate_control_transport(cfg)
    cfg.grpc_server_identity = "wrong.invalid"
    with pytest.raises(InsecureTransportError):
        validate_control_transport(cfg)
    assert WorkerConfig(
        dispatcher_url="https://x", root=tmp_path, display_name="x"
    ).control_transport == "polling"


@pytest.mark.asyncio
async def test_real_grpc_hello_over_mtls(identity, center):
    pki, worker, _, _, _, _, _ = identity
    key, _, issued = pki.issue_worker(worker["worker_id"])
    # Use the enrolled leaf from fixture, not this extra key.
    row = CertificateRegistry(center).by_request("crq_initial")
    cert = bytes(row["certificate_pem"])
    store_key = identity[3].load_private_key()
    creds = grpc.ssl_channel_credentials(
        root_certificates=pki.root_pem, private_key=store_key, certificate_chain=cert
    )
    server = GatewayServer(_gateway_config(pki), worker_settings=center)
    port = await server.start()
    channel, call = await _connect(port, creds, worker)
    response = await asyncio.wait_for(call.read(), 3)
    assert response.WhichOneof("payload") == "hello"
    assert server.metrics.snapshot()["mtls_handshakes_total"] == 1
    call.cancel(); await channel.close(); await server.stop()
    assert await server.registry.count() == 0


@pytest.mark.asyncio
async def test_certificate_a_hello_b_rejected_before_domain(identity, center):
    pki, worker, _, store, _, _, row = identity
    other = _worker(center)
    creds = grpc.ssl_channel_credentials(
        root_certificates=pki.root_pem, private_key=store.load_private_key(),
        certificate_chain=bytes(row["certificate_pem"]),
    )
    server = GatewayServer(_gateway_config(pki), worker_settings=center)
    port = await server.start()
    channel, call = await _connect(port, creds, worker, other["worker_id"])
    with pytest.raises(grpc.aio.AioRpcError) as found:
        await asyncio.wait_for(call.read(), 3)
    assert found.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert await server.registry.count() == 0
    assert server.metrics.snapshot()["cert_identity_mismatches"] == 1
    await channel.close(); await server.stop()


@pytest.mark.asyncio
async def test_missing_and_untrusted_client_cert_rejected_at_tls(identity, center, tmp_path):
    pki, worker, _, _, _, _, _ = identity
    server = GatewayServer(_gateway_config(pki), worker_settings=center)
    port = await server.start()
    no_client = grpc.aio.secure_channel(
        f"127.0.0.1:{port}", grpc.ssl_channel_credentials(root_certificates=pki.root_pem)
    )
    call = stream_grpc.AgentStreamServiceStub(no_client).Connect()
    with pytest.raises(grpc.aio.AioRpcError):
        await call.write(_hello(worker))
    await no_client.close()
    evil = Pki(tmp_path / "evil")
    evil_key, _, evil_cert = evil.issue_worker(worker["worker_id"])
    channel = grpc.aio.secure_channel(
        f"127.0.0.1:{port}", _credentials(pki, evil_key, evil_cert)
    )
    call = stream_grpc.AgentStreamServiceStub(channel).Connect()
    with pytest.raises(grpc.aio.AioRpcError):
        await call.write(_hello(worker))
    await channel.close(); await server.stop()


@pytest.mark.asyncio
async def test_worker_rejects_untrusted_or_wrong_san_server(identity, center, tmp_path):
    pki, worker, _, store, _, _, row = identity
    server = GatewayServer(_gateway_config(pki), worker_settings=center)
    port = await server.start()
    evil = Pki(tmp_path / "evil-server")
    creds = grpc.ssl_channel_credentials(
        root_certificates=evil.root_pem, private_key=store.load_private_key(),
        certificate_chain=bytes(row["certificate_pem"]),
    )
    channel = grpc.aio.secure_channel(f"127.0.0.1:{port}", creds)
    call = stream_grpc.AgentStreamServiceStub(channel).Connect()
    with pytest.raises(grpc.aio.AioRpcError):
        await call.write(_hello(worker))
    await channel.close(); await server.stop()

    wrong_key = ec.generate_private_key(ec.SECP256R1())
    wrong_leaf = pki.issuer.issue_server(
        wrong_key.public_key(), identity="wrong.invalid", lifetime=timedelta(days=2)
    )
    wrong_key_path = _write(tmp_path / "wrong.key", _pem_key(wrong_key), 0o600)
    wrong_cert_path = _write(tmp_path / "wrong.pem", wrong_leaf.certificate_pem + pki.chain_pem)
    wrong_cfg = GatewayConfig(
        host="127.0.0.1", security_mode="mtls",
        server_certificate_path=wrong_cert_path, server_private_key_path=wrong_key_path,
        client_ca_bundle_path=pki.ca_path, server_identity="wrong.invalid",
    )
    wrong_server = GatewayServer(wrong_cfg, worker_settings=center)
    wrong_port = await wrong_server.start()
    good_creds = grpc.ssl_channel_credentials(
        root_certificates=pki.root_pem, private_key=store.load_private_key(),
        certificate_chain=bytes(row["certificate_pem"]),
    )
    channel = grpc.aio.secure_channel(f"127.0.0.1:{wrong_port}", good_creds)
    call = stream_grpc.AgentStreamServiceStub(channel).Connect()
    with pytest.raises(grpc.aio.AioRpcError):
        await call.write(_hello(worker))
    await channel.close(); await wrong_server.stop()


@pytest.mark.asyncio
async def test_revocation_closes_active_stream_and_denies_reconnect(identity, center):
    pki, worker, registry, store, _, _, row = identity
    creds = grpc.ssl_channel_credentials(
        root_certificates=pki.root_pem, private_key=store.load_private_key(),
        certificate_chain=bytes(row["certificate_pem"]),
    )
    server = GatewayServer(_gateway_config(pki, interval=.05), worker_settings=center)
    port = await server.start()
    channel, call = await _connect(port, creds, worker)
    assert (await asyncio.wait_for(call.read(), 3)).WhichOneof("payload") == "hello"
    assert registry.revoke_serial(row["serial_hex"], RevocationReason.COMPROMISED)
    terminal = await asyncio.wait_for(call.read(), 2)
    assert terminal is grpc.aio.EOF
    await channel.close()
    channel, call = await _connect(port, creds, worker)
    with pytest.raises(grpc.aio.AioRpcError):
        await asyncio.wait_for(call.read(), 3)
    await channel.close(); await server.stop()


def test_renewal_idempotency_identity_gate_and_rotation(identity, center):
    pki, worker, registry, store, cert_path, ca_path, row = identity
    authority = CertificateLifecycleAuthority(
        issuer=pki.issuer, registry=registry, worker_lifetime=timedelta(days=30)
    )
    from backend.app.agent_gateway.auth import AuthenticatedPeer
    peer = AuthenticatedPeer(
        worker_id=worker["worker_id"], serial_hex=row["serial_hex"],
        fingerprint_sha256=row["fingerprint_sha256"], issuer_id=row["issuer_id"],
        not_before=row["not_before"], not_after=row["not_after"], peer="test",
        certificate_pem=bytes(row["certificate_pem"]),
    )
    new_key = ec.generate_private_key(ec.SECP256R1())
    csr = make_csr(_pem_key(new_key), worker["worker_id"])
    first = authority.renew(peer=peer, csr_pem=csr, request_id="crq_retry")
    second = authority.renew(peer=peer, csr_pem=csr, request_id="crq_retry")
    assert first.serial_hex == second.serial_hex
    other = _worker(center)
    wrong_csr = make_csr(_pem_key(ec.generate_private_key(ec.SECP256R1())), other["worker_id"])
    with pytest.raises(CertificateLifecycleError, match="identity"):
        authority.renew(peer=peer, csr_pem=wrong_csr, request_id="crq_escalate")
    authority.activate_rotation(old_serial=row["serial_hex"], new_serial=first.serial_hex)
    with pytest.raises(CertificateRegistryError, match="REPLACED"):
        registry.validate_presented(
            serial_hex=row["serial_hex"], fingerprint_sha256=row["fingerprint_sha256"],
            worker_id=worker["worker_id"],
        )
    assert registry.by_serial(first.serial_hex)["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_new_key_rotation_over_authenticated_mtls(identity, center, tmp_path):
    pki, worker, registry, store, cert_path, ca_path, row = identity
    authority = CertificateLifecycleAuthority(
        issuer=pki.issuer, registry=registry, worker_lifetime=timedelta(days=30)
    )
    server = GatewayServer(
        _gateway_config(pki), worker_settings=center, renewal_authority=authority
    )
    port = await server.start()
    cfg = WorkerConfig(
        dispatcher_url="https://center.invalid", root=tmp_path / "worker",
        display_name="worker", control_transport="grpc",
        grpc_target=f"127.0.0.1:{port}", grpc_security_mode="mtls",
        grpc_ca_bundle_path=ca_path, grpc_client_certificate_path=cert_path,
        grpc_key_store_dir=store.root, grpc_server_identity="127.0.0.1",
        grpc_connect_timeout_sec=5,
    )
    result = await asyncio.to_thread(
        WorkerCertificateRotator(config=cfg, worker_id=worker["worker_id"]).renew
    )
    assert result["phase"] == "active"
    assert result["old_serial"] == row["serial_hex"]
    assert result["new_serial"] != row["serial_hex"]
    assert registry.by_serial(row["serial_hex"])["status"] == "REPLACED"
    assert registry.by_serial(result["new_serial"])["status"] == "ACTIVE"
    await server.stop()


@pytest.mark.asyncio
async def test_gateway_renews_through_separate_unix_issuer(
    identity, center, tmp_path, monkeypatch
):
    pki, worker, registry, store, cert_path, ca_path, row = identity
    issuer_dir = tmp_path / "issuer"
    issuer_cert = _write(
        issuer_dir / "issuing.pem",
        pki.issuing_cert.public_bytes(serialization.Encoding.PEM),
    )
    issuer_key = _write(issuer_dir / "issuing.key", _pem_key(pki.issuing_key), 0o600)
    issuer_chain = _write(issuer_dir / "chain.pem", pki.chain_pem)
    socket_path = tmp_path / "run" / "issuer.sock"
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_SOCKET", str(socket_path))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_CERT", str(issuer_cert))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_KEY", str(issuer_key))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_CHAIN", str(issuer_chain))
    monkeypatch.setenv("AUDIT_WORKER_GATEWAY_UID", str(os.getuid()))
    issuer_service = IssuerServer()
    await issuer_service.start()
    server = GatewayServer(
        _gateway_config(pki), worker_settings=center,
        renewal_authority=UnixSocketRenewalAuthority(socket_path),
    )
    port = await server.start()
    cfg = WorkerConfig(
        dispatcher_url="https://center.invalid", root=tmp_path / "worker-socket",
        display_name="worker", control_transport="grpc",
        grpc_target=f"127.0.0.1:{port}", grpc_security_mode="mtls",
        grpc_ca_bundle_path=ca_path, grpc_client_certificate_path=cert_path,
        grpc_key_store_dir=store.root, grpc_server_identity="127.0.0.1",
        grpc_connect_timeout_sec=5,
    )
    result = await asyncio.to_thread(
        WorkerCertificateRotator(config=cfg, worker_id=worker["worker_id"]).renew
    )
    assert result["phase"] == "active"
    assert registry.by_serial(row["serial_hex"])["status"] == "REPLACED"
    await server.stop()
    await issuer_service.stop()


@pytest.mark.asyncio
async def test_initial_enrollment_uses_protected_issuer_socket(
    center, tmp_path, monkeypatch
):
    pki = Pki(tmp_path / "enroll-pki")
    worker = _worker(center)
    issuer_dir = tmp_path / "enroll-issuer"
    issuer_cert = _write(
        issuer_dir / "issuing.pem",
        pki.issuing_cert.public_bytes(serialization.Encoding.PEM),
    )
    issuer_key = _write(issuer_dir / "issuing.key", _pem_key(pki.issuing_key), 0o600)
    issuer_chain = _write(issuer_dir / "chain.pem", pki.chain_pem)
    socket_path = tmp_path / "enroll-run" / "issuer.sock"
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_SOCKET", str(socket_path))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_CERT", str(issuer_cert))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_KEY", str(issuer_key))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_CHAIN", str(issuer_chain))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_ALLOWED_UIDS", str(os.getuid()))
    service = IssuerServer()
    await service.start()
    key = ec.generate_private_key(ec.SECP256R1())
    response = await asyncio.to_thread(
        UnixSocketEnrollmentAuthority(socket_path).enroll,
        worker_id=worker["worker_id"], instance_id=worker["instance_id"],
        csr_pem=make_csr(_pem_key(key), worker["worker_id"]),
        request_id="bootstrap-cert-test",
    )
    assert CertificateRegistry(center).by_serial(response.serial_hex)["status"] == "ACTIVE"
    with pytest.raises(Exception):
        await asyncio.to_thread(
            UnixSocketEnrollmentAuthority(socket_path).enroll,
            worker_id=worker["worker_id"], instance_id="wrong-instance",
            csr_pem=make_csr(_pem_key(key), worker["worker_id"]),
            request_id="bootstrap-cert-wrong-instance",
        )
    await service.stop()


@pytest.mark.asyncio
async def test_issuer_startup_migrates_fresh_registry(tmp_path, monkeypatch):
    pki = Pki(tmp_path / "fresh-pki")
    issuer_dir = tmp_path / "fresh-issuer"
    issuer_cert = _write(
        issuer_dir / "issuing.pem",
        pki.issuing_cert.public_bytes(serialization.Encoding.PEM),
    )
    issuer_key = _write(issuer_dir / "issuing.key", _pem_key(pki.issuing_key), 0o600)
    issuer_chain = _write(issuer_dir / "chain.pem", pki.chain_pem)
    data_dir = tmp_path / "never-started-center"
    socket_path = tmp_path / "fresh-run" / "issuer.sock"
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(data_dir))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_SOCKET", str(socket_path))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_CERT", str(issuer_cert))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_KEY", str(issuer_key))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_CHAIN", str(issuer_chain))
    monkeypatch.setenv("AUDIT_WORKER_ISSUER_ALLOWED_UIDS", str(os.getuid()))
    database.reset_state_for_tests()
    assert not (data_dir / "workers.db").exists()
    service = IssuerServer()
    await service.start()
    with database.read_conn(service.authority.registry.settings) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_certificates'"
        ).fetchone()
    await service.stop()
    database.reset_state_for_tests()


def test_worker_decommission_revokes_all_active_certificates(identity, center):
    _, worker, registry, _, _, _, row = identity
    registration_service.revoke_worker(worker_id=worker["worker_id"], settings=center)
    assert registry.by_serial(row["serial_hex"])["status"] == "REVOKED"
    assert registry.by_serial(row["serial_hex"])["revocation_reason"] == "DECOMMISSIONED"


def test_server_startup_rejects_key_mismatch_and_public_insecure(identity, tmp_path):
    pki, *_ = identity
    wrong = _write(tmp_path / "mismatch.key", _pem_key(ec.generate_private_key(ec.SECP256R1())), 0o600)
    with pytest.raises(GatewayConfigError, match="do not match"):
        GatewayConfig(
            security_mode="mtls", server_certificate_path=pki.server_cert_path,
            server_private_key_path=wrong, client_ca_bundle_path=pki.ca_path,
            server_identity="127.0.0.1",
        ).validated()
    with pytest.raises(GatewayConfigError):
        GatewayConfig(host="0.0.0.0", port=8443, security_mode="test_insecure").validated()


def test_issuer_startup_rejects_certificate_key_mismatch(identity):
    pki, *_ = identity
    with pytest.raises(CertificateProfileError, match="do not match"):
        CertificateIssuer(
            pki.issuing_cert,
            ec.generate_private_key(ec.SECP256R1()),
            chain_pem=pki.chain_pem,
        )


def test_multiple_ca_bundle_parses(identity, tmp_path):
    pki, *_ = identity
    second_root_key, second_root = create_root_ca(common_name="rotation root")
    bundle = _write(
        tmp_path / "overlap.pem", pki.root_pem + second_root.public_bytes(serialization.Encoding.PEM)
    )
    cfg = _gateway_config(pki)
    object.__setattr__(cfg, "client_ca_bundle_path", bundle)
    cfg.validated()


@pytest.mark.asyncio
@pytest.mark.parametrize("offset,lifetime", [(-7200, 3600), (86400, 3600)])
async def test_expired_and_not_yet_valid_client_rejected(identity, center, offset, lifetime):
    pki, worker, registry, _, _, _, _ = identity
    issued_at = datetime.now(timezone.utc) + timedelta(seconds=offset)
    key, csr, issued = pki.issue_worker(
        worker["worker_id"], now=issued_at, lifetime=timedelta(seconds=lifetime)
    )
    _record(registry, worker, csr, issued, request=f"crq_time_{offset}")
    server = GatewayServer(_gateway_config(pki), worker_settings=center)
    port = await server.start()
    channel = grpc.aio.secure_channel(
        f"127.0.0.1:{port}", _credentials(pki, key, issued)
    )
    call = stream_grpc.AgentStreamServiceStub(channel).Connect()
    with pytest.raises(grpc.aio.AioRpcError):
        await call.write(_hello(worker))
    await channel.close(); await server.stop()


@pytest.mark.asyncio
async def test_wrong_eku_client_rejected_by_tls(identity, center):
    pki, worker, *_ = identity
    key = ec.generate_private_key(ec.SECP256R1())
    server_only = pki.issuer.issue_server(
        key.public_key(), identity="127.0.0.1", lifetime=timedelta(days=2)
    )
    creds = grpc.ssl_channel_credentials(
        root_certificates=pki.root_pem, private_key=_pem_key(key),
        certificate_chain=server_only.certificate_pem + pki.chain_pem,
    )
    server = GatewayServer(_gateway_config(pki), worker_settings=center)
    port = await server.start()
    channel = grpc.aio.secure_channel(f"127.0.0.1:{port}", creds)
    call = stream_grpc.AgentStreamServiceStub(channel).Connect()
    with pytest.raises(grpc.aio.AioRpcError):
        await call.write(_hello(worker))
    await channel.close(); await server.stop()


@pytest.mark.asyncio
async def test_connection_expires_even_when_stream_stays_open(tmp_path, center):
    pki = Pki(tmp_path / "short-pki")
    worker = _worker(center)
    key, csr, issued = pki.issue_worker(
        worker["worker_id"], lifetime=timedelta(seconds=3)
    )
    _record(CertificateRegistry(center), worker, csr, issued, request="crq_short")
    server = GatewayServer(_gateway_config(pki, interval=.05), worker_settings=center)
    port = await server.start()
    channel, call = await _connect(port, _credentials(pki, key, issued), worker)
    assert (await asyncio.wait_for(call.read(), 2)).WhichOneof("payload") == "hello"
    assert await asyncio.wait_for(call.read(), 5) is grpc.aio.EOF
    await channel.close(); await server.stop()


@pytest.mark.asyncio
async def test_server_leaf_rotation_same_ca_reconnects(identity, center, tmp_path):
    pki, worker, _, store, _, _, row = identity
    creds = grpc.ssl_channel_credentials(
        root_certificates=pki.root_pem, private_key=store.load_private_key(),
        certificate_chain=bytes(row["certificate_pem"]),
    )
    first = GatewayServer(_gateway_config(pki), worker_settings=center)
    port = await first.start()
    channel, call = await _connect(port, creds, worker)
    assert (await asyncio.wait_for(call.read(), 3)).WhichOneof("payload") == "hello"
    call.cancel(); await channel.close(); await first.stop()
    new_key = ec.generate_private_key(ec.SECP256R1())
    new_leaf = pki.issuer.issue_server(
        new_key.public_key(), identity="127.0.0.1", lifetime=timedelta(days=30)
    )
    pki.server_key_path = _write(tmp_path / "rotated-server.key", _pem_key(new_key), 0o600)
    pki.server_cert_path = _write(
        tmp_path / "rotated-server.pem", new_leaf.certificate_pem + pki.chain_pem
    )
    second = GatewayServer(_gateway_config(pki), worker_settings=center)
    new_port = await second.start()
    channel, call = await _connect(new_port, creds, worker, epoch=2)
    assert (await asyncio.wait_for(call.read(), 3)).WhichOneof("payload") == "hello"
    call.cancel(); await channel.close(); await second.stop()


def test_revoked_certificate_cannot_renew(identity, center):
    pki, worker, registry, _, _, _, row = identity
    authority = CertificateLifecycleAuthority(issuer=pki.issuer, registry=registry)
    from backend.app.agent_gateway.auth import AuthenticatedPeer
    peer = AuthenticatedPeer(
        worker_id=worker["worker_id"], serial_hex=row["serial_hex"],
        fingerprint_sha256=row["fingerprint_sha256"], issuer_id=row["issuer_id"],
        not_before=row["not_before"], not_after=row["not_after"], peer="test",
        certificate_pem=bytes(row["certificate_pem"]),
    )
    registry.revoke_serial(row["serial_hex"], RevocationReason.ADMIN_REVOKED)
    csr = make_csr(_pem_key(ec.generate_private_key(ec.SECP256R1())), worker["worker_id"])
    with pytest.raises(CertificateRegistryError, match="REVOKED"):
        authority.renew(peer=peer, csr_pem=csr, request_id="crq_revoked")


def test_no_private_key_material_in_repository_paths():
    roots = [Path("backend/app/agent_gateway"), Path("docs/distributed_audit_workers/12d"), Path("contracts/worker_certificate")]
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_file():
                assert b"-----BEGIN PRIVATE KEY-----" not in path.read_bytes(), path
