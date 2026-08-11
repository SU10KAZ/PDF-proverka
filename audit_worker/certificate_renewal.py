"""Resumable new-key Worker certificate rotation over current mTLS."""
from __future__ import annotations

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
