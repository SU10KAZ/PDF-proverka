"""Certificate enrollment/renewal policy, independent from transport and issuer process."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from backend.app.agent_gateway.auth import AuthenticatedPeer
from backend.app.security.certificate_profiles import (
    CertificateIssuer,
    CertificateProfileError,
    certificate_fingerprint,
    cert_not_after,
    cert_not_before,
    csr_sha256,
    serial_hex,
)
from backend.app.services.distributed_workers.certificate_registry import (
    CertificateRegistry,
    CertificateRegistryError,
    PresentedCertificate,
)


class CertificateLifecycleError(RuntimeError):
    pass


@dataclass(frozen=True)
class CertificateResponse:
    certificate_chain_pem: bytes
    trust_chain_pem: bytes
    serial_hex: str
    fingerprint_sha256: str
    not_before: float
    not_after: float
    request_id: str


class CertificateLifecycleAuthority:
    """Runs only in the protected issuer boundary, never in Gateway process."""

    def __init__(
        self, *, issuer: CertificateIssuer, registry: CertificateRegistry,
        worker_lifetime: timedelta = timedelta(days=30),
    ) -> None:
        if not timedelta(hours=1) <= worker_lifetime <= timedelta(days=397):
            raise CertificateLifecycleError("Worker certificate lifetime outside policy")
        self.issuer = issuer
        self.registry = registry
        self.worker_lifetime = worker_lifetime

    def enroll(
        self, *, authorized_worker_id: str, instance_id: str,
        csr_pem: bytes, request_id: str,
    ) -> CertificateResponse:
        return self._issue(
            worker_id=authorized_worker_id, instance_id=instance_id,
            csr_pem=csr_pem, request_id=request_id,
            predecessor_serial=None, renewed=False,
        )

    def renew(
        self, *, peer: AuthenticatedPeer, csr_pem: bytes, request_id: str,
    ) -> CertificateResponse:
        # Current leaf must still be ACTIVE and unexpired.  Revoked/expired
        # identities recover only through the bootstrap/admin flow.
        self.registry.validate_presented(
            serial_hex=peer.serial_hex,
            fingerprint_sha256=peer.fingerprint_sha256,
            worker_id=peer.worker_id,
        )
        return self._issue(
            worker_id=peer.worker_id, instance_id="", csr_pem=csr_pem,
            request_id=request_id, predecessor_serial=peer.serial_hex,
            renewed=True,
        )

    def _issue(
        self, *, worker_id: str, instance_id: str, csr_pem: bytes,
        request_id: str, predecessor_serial: str | None, renewed: bool,
    ) -> CertificateResponse:
        if not request_id or len(request_id) > 200:
            raise CertificateLifecycleError("invalid certificate request id")
        try:
            csr = x509.load_pem_x509_csr(csr_pem)
        except ValueError as exc:
            raise CertificateLifecycleError("invalid CSR encoding") from exc
        digest = csr_sha256(csr)
        existing = self.registry.by_request(request_id)
        if existing is not None:
            if existing["worker_id"] != worker_id or existing["csr_sha256"] != digest:
                raise CertificateLifecycleError("request id reused with different CSR/worker")
            return self._from_row(existing)
        try:
            issued = self.issuer.issue_worker(
                csr, worker_id=worker_id, lifetime=self.worker_lifetime
            )
        except CertificateProfileError as exc:
            raise CertificateLifecycleError(str(exc)) from exc
        cert = issued.certificate
        row = self.registry.record_issuance(
            PresentedCertificate(
                serial_hex=serial_hex(cert),
                fingerprint_sha256=certificate_fingerprint(cert),
                worker_id=worker_id,
                not_before=cert_not_before(cert).timestamp(),
                not_after=cert_not_after(cert).timestamp(),
                issuer_id=issued.issuer_id,
                certificate_pem=issued.certificate_pem + issued.chain_pem,
            ),
            csr_sha256=digest, request_id=request_id,
            instance_id=instance_id or None,
            predecessor_serial=predecessor_serial, renewed=renewed,
        )
        return self._from_row(row)

    def activate_rotation(self, *, old_serial: str, new_serial: str) -> None:
        """Called after the Worker proves a TLS handshake with the new leaf."""
        self.registry.replace(old_serial, new_serial)

    def _from_row(self, row) -> CertificateResponse:
        return CertificateResponse(
            certificate_chain_pem=bytes(row["certificate_pem"]),
            trust_chain_pem=self.issuer.chain_pem,
            serial_hex=row["serial_hex"],
            fingerprint_sha256=row["fingerprint_sha256"],
            not_before=float(row["not_before"]),
            not_after=float(row["not_after"]),
            request_id=row["request_id"],
        )
