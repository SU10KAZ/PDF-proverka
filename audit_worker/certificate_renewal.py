"""Resumable new-key Worker certificate rotation over current mTLS."""
from __future__ import annotations

import hashlib
import threading
import time
import uuid
from pathlib import Path

import grpc

from audit_worker.key_store import WorkerKeyStore, platform_key_store
from audit_worker.local_store import atomic_write_json, read_json
from audit_worker.mtls_identity import (
    install_public_identity,
    load_identity,
    make_csr,
)
from contracts.worker_certificate.v1 import worker_certificate_pb2 as cert_pb
from contracts.worker_certificate.v1 import worker_certificate_pb2_grpc as cert_grpc


class CertificateRenewalError(RuntimeError):
    pass


class WorkerCertificateRotator:
    def __init__(self, *, config, worker_id: str) -> None:
        self.config = config
        self.worker_id = worker_id
        self.identity_root = Path(config.grpc_key_store_dir)
        self.state_path = self.identity_root / "renewal-state.json"
        self.current_store = platform_key_store(
            self.identity_root, config.grpc_key_store_backend
        )

    def renew_if_due(self, *, now: float | None = None) -> dict:
        """Rotate automatically inside the configured, serial-jittered window."""
        current = load_identity(
            key_store=self.current_store,
            certificate_path=self.config.grpc_client_certificate_path,
            trust_bundle_path=self.config.grpc_ca_bundle_path,
            worker_id=self.worker_id,
        )
        current_time = time.time() if now is None else float(now)
        base_window = float(self.config.grpc_renew_before_sec)
        jitter = float(self.config.grpc_renew_jitter)
        fraction = int.from_bytes(
            hashlib.sha256(current.serial_hex.encode("ascii")).digest()[:8], "big"
        ) / float(2**64 - 1)
        renew_window = base_window * (1.0 + jitter * (2.0 * fraction - 1.0))
        renew_at = current.not_after - max(60.0, renew_window)
        if current_time < renew_at:
            return {
                "phase": "not_due", "serial": current.serial_hex,
                "not_after": current.not_after, "renew_at": renew_at,
            }
        state = read_json(self.state_path, {}) or {}
        if (
            state.get("phase") == "active"
            and state.get("new_serial") == current.serial_hex
        ):
            # A completed prior transaction remains an idempotency result for
            # renew(), but a newly due leaf starts a fresh durable request.
            atomic_write_json(self.state_path, {})
        return self.renew()

    def renew(self) -> dict:
        current = load_identity(
            key_store=self.current_store,
            certificate_path=self.config.grpc_client_certificate_path,
            trust_bundle_path=self.config.grpc_ca_bundle_path,
            worker_id=self.worker_id,
        )
        state = read_json(self.state_path, {}) or {}
        if state.get("phase") == "active" and state.get("new_serial"):
            return state
        if not state.get("request_id"):
            request_id = "crq_" + uuid.uuid4().hex
            stage_root = self.identity_root / ("staging-" + request_id)
            stage_store = platform_key_store(stage_root, self.config.grpc_key_store_backend)
            key_pem = stage_store.generate()
            csr_pem = make_csr(key_pem, self.worker_id)
            stage_root.mkdir(parents=True, exist_ok=True)
            csr_path = stage_root / "request.csr.pem"
            csr_path.write_bytes(csr_pem)
            state = {
                "phase": "prepared", "request_id": request_id,
                "stage_root": str(stage_root), "old_serial": current.serial_hex,
                "prepared_at": time.time(),
            }
            atomic_write_json(self.state_path, state)
        stage_root = Path(state["stage_root"])
        if stage_root.parent != self.identity_root or not stage_root.name.startswith("staging-crq_"):
            raise CertificateRenewalError("unsafe persisted rotation staging path")
        stage_store = platform_key_store(stage_root, self.config.grpc_key_store_backend)
        csr_pem = (stage_root / "request.csr.pem").read_bytes()
        old_channel = self._channel(current)
        try:
            stub = cert_grpc.WorkerCertificateServiceStub(old_channel)
            issued = stub.RenewCertificate(
                cert_pb.RenewCertificateRequest(
                    request_id=state["request_id"], csr_pem=csr_pem
                ),
                timeout=float(self.config.grpc_connect_timeout_sec),
            )
        except grpc.RpcError as exc:
            raise CertificateRenewalError(f"certificate renewal RPC failed: {exc.code().name}") from exc
        finally:
            old_channel.close()
        if issued.request_id != state["request_id"]:
            raise CertificateRenewalError("renewal response request identity mismatch")
        stage_cert = stage_root / "client-cert.pem"
        stage_trust = stage_root / "ca-bundle.pem"
        install_public_identity(
            certificate_path=stage_cert, trust_bundle_path=stage_trust,
            certificate_chain_pem=bytes(issued.certificate_chain_pem),
            trust_bundle_pem=bytes(issued.trust_bundle_pem),
        )
        candidate = load_identity(
            key_store=stage_store, certificate_path=stage_cert,
            trust_bundle_path=stage_trust, worker_id=self.worker_id,
        )
        if candidate.serial_hex != issued.serial_hex:
            raise CertificateRenewalError("renewal leaf serial mismatch")
        state.update({
            "phase": "issued", "new_serial": candidate.serial_hex,
            "new_fingerprint": candidate.fingerprint_sha256,
        })
        atomic_write_json(self.state_path, state)

        # Prove TLS + hostname + registry acceptance before changing live files.
        new_channel = self._channel(candidate)
        try:
            validated = cert_grpc.WorkerCertificateServiceStub(new_channel).ValidateIdentity(
                cert_pb.ValidateIdentityRequest(),
                timeout=float(self.config.grpc_connect_timeout_sec),
            )
            if validated.worker_id != self.worker_id or validated.serial_hex != candidate.serial_hex:
                raise CertificateRenewalError("Gateway validated a different candidate identity")
        except grpc.RpcError as exc:
            raise CertificateRenewalError(f"candidate mTLS proof failed: {exc.code().name}") from exc
        finally:
            new_channel.close()

        # Atomic writes per file; if the second write fails, restore the old
        # in-memory identity.  The only private-key writes are KeyStore calls.
        try:
            self.current_store.store_private_key(candidate.private_key_pem)
            install_public_identity(
                certificate_path=self.config.grpc_client_certificate_path,
                trust_bundle_path=self.config.grpc_ca_bundle_path,
                certificate_chain_pem=candidate.certificate_chain_pem,
                trust_bundle_pem=candidate.trust_bundle_pem,
            )
        except BaseException:
            self.current_store.store_private_key(current.private_key_pem)
            install_public_identity(
                certificate_path=self.config.grpc_client_certificate_path,
                trust_bundle_path=self.config.grpc_ca_bundle_path,
                certificate_chain_pem=current.certificate_chain_pem,
                trust_bundle_pem=current.trust_bundle_pem,
            )
            raise
        committed = load_identity(
            key_store=self.current_store,
            certificate_path=self.config.grpc_client_certificate_path,
            trust_bundle_path=self.config.grpc_ca_bundle_path,
            worker_id=self.worker_id,
        )
        activation_channel = self._channel(committed)
        try:
            cert_grpc.WorkerCertificateServiceStub(activation_channel).ActivateCertificate(
                cert_pb.ActivateCertificateRequest(
                    predecessor_serial_hex=state["old_serial"]
                ),
                timeout=float(self.config.grpc_connect_timeout_sec),
            )
        except grpc.RpcError as exc:
            # New identity remains valid; persisted phase makes operator-visible
            # recovery deterministic and does not destroy the old local backup.
            state["phase"] = "activation_pending"
            atomic_write_json(self.state_path, state)
            raise CertificateRenewalError(f"rotation activation failed: {exc.code().name}") from exc
        finally:
            activation_channel.close()
        state.update({"phase": "active", "activated_at": time.time()})
        atomic_write_json(self.state_path, state)
        return state

    def _channel(self, identity):
        credentials = grpc.ssl_channel_credentials(
            root_certificates=identity.trust_bundle_pem,
            private_key=identity.private_key_pem,
            certificate_chain=identity.certificate_chain_pem,
        )
        return grpc.secure_channel(self.config.grpc_target, credentials)


class AutomaticCertificateRenewal:
    """Agent-owned scheduler; certificate work never blocks heartbeat paths."""

    def __init__(self, *, config, worker_id: str, log=print, rotator=None) -> None:
        self.config = config
        self.log = log
        self.rotator = rotator or WorkerCertificateRotator(
            config=config, worker_id=worker_id
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._loop, name="certificate-renewal", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)

    def _loop(self) -> None:
        delay = 0.0
        while not self._stop.wait(delay):
            try:
                result = self.rotator.renew_if_due()
                if result.get("phase") == "not_due":
                    delay = max(
                        60.0,
                        min(3600.0, float(result["renew_at"]) - time.time()),
                    )
                else:
                    delay = 60.0
            except Exception as exc:  # noqa: BLE001 - bounded retry is required
                self.log(
                    "certificate renewal deferred "
                    f"error={type(exc).__name__}"
                )
                delay = max(
                    60.0,
                    min(900.0, float(self.config.grpc_reconnect_max_delay_sec) * 4),
                )
