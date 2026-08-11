"""Load, validate and create Worker mTLS identity without backend imports."""
from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from audit_worker.key_store import WorkerKeyStore, _atomic_write


WORKER_URI_PREFIX = "urn:auditmanager:worker:"


class WorkerIdentityError(RuntimeError):
    pass


@dataclass(frozen=True)
class WorkerTlsIdentity:
    worker_id: str
    private_key_pem: bytes
    certificate_chain_pem: bytes
    trust_bundle_pem: bytes
    serial_hex: str
    fingerprint_sha256: str
    not_after: float


def identity_uri(worker_id: str) -> str:
    return WORKER_URI_PREFIX + quote(worker_id, safe="._-")


def make_csr(private_key_pem: bytes, worker_id: str) -> bytes:
    key = serialization.load_pem_private_key(private_key_pem, password=None)
    csr = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, "AuditManager Worker")]))
        .add_extension(
            x509.SubjectAlternativeName([x509.UniformResourceIdentifier(identity_uri(worker_id))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    return csr.public_bytes(serialization.Encoding.PEM)


def load_identity(
    *, key_store: WorkerKeyStore, certificate_path: Path, trust_bundle_path: Path,
    worker_id: str,
) -> WorkerTlsIdentity:
    key_pem = key_store.load_private_key()
    cert_chain = certificate_path.read_bytes()
    trust = trust_bundle_path.read_bytes()
    cert = x509.load_pem_x509_certificate(cert_chain)
    key = serialization.load_pem_private_key(key_pem, password=None)
    cert_pub = cert.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    key_pub = key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    if cert_pub != key_pub:
        raise WorkerIdentityError("client certificate and private key mismatch")
    san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    if san.get_values_for_type(x509.UniformResourceIdentifier) != [identity_uri(worker_id)]:
        raise WorkerIdentityError("client certificate SAN does not match worker_id")
    eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    if list(eku) != [ExtendedKeyUsageOID.CLIENT_AUTH]:
        raise WorkerIdentityError("client certificate lacks strict clientAuth EKU")
    not_before_dt = getattr(cert, "not_valid_before_utc", cert.not_valid_before.replace(tzinfo=timezone.utc))
    not_after_dt = getattr(cert, "not_valid_after_utc", cert.not_valid_after.replace(tzinfo=timezone.utc))
    now = datetime.now(timezone.utc)
    if now < not_before_dt or now >= not_after_dt:
        raise WorkerIdentityError("client certificate is outside validity")
    if b"BEGIN CERTIFICATE" not in trust:
        raise WorkerIdentityError("configured trust bundle has no CA certificate")
    return WorkerTlsIdentity(
        worker_id=worker_id, private_key_pem=key_pem,
        certificate_chain_pem=cert_chain, trust_bundle_pem=trust,
        serial_hex=format(cert.serial_number, "x"),
        fingerprint_sha256=cert.fingerprint(hashes.SHA256()).hex(),
        not_after=not_after_dt.timestamp(),
    )


def install_public_identity(
    *, certificate_path: Path, trust_bundle_path: Path,
    certificate_chain_pem: bytes, trust_bundle_pem: bytes,
) -> None:
    _atomic_write(certificate_path, certificate_chain_pem, 0o644)
    _atomic_write(trust_bundle_path, trust_bundle_pem, 0o644)
