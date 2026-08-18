"""Verified TLS peer identity boundary for Agent Gateway RPCs."""
from __future__ import annotations

from dataclasses import dataclass

from cryptography import x509

from backend.app.security.certificate_profiles import (
    certificate_fingerprint,
    cert_not_after,
    cert_not_before,
    serial_hex,
    validate_worker_certificate,
    worker_identity_uri,
)


class PeerAuthenticationError(RuntimeError):
    pass


@dataclass(frozen=True)
class AuthenticatedPeer:
    worker_id: str
    serial_hex: str
    fingerprint_sha256: str
    issuer_id: str
    not_before: float
    not_after: float
    peer: str
    certificate_pem: bytes


def authenticated_peer(context) -> AuthenticatedPeer:
    auth = context.auth_context() or {}
    transport = list(auth.get("transport_security_type", ()))
    if not transport or transport[0] not in {b"ssl", b"tls"}:
        raise PeerAuthenticationError("RPC has no verified TLS transport")
    identity_key = context.peer_identity_key()
    if identity_key != "x509_subject_alternative_name":
        raise PeerAuthenticationError("TLS peer identity is not sourced from certificate SAN")
    identities = list(context.peer_identities() or ())
    pem_values = list(auth.get("x509_pem_cert", ()))
    if len(pem_values) != 1:
        raise PeerAuthenticationError("verified leaf certificate is unavailable")
    try:
        cert = x509.load_pem_x509_certificate(pem_values[0])
        worker_id = validate_worker_certificate(cert)
    except Exception as exc:  # normalized safe error; raw certificate is never logged
        raise PeerAuthenticationError(f"invalid Worker certificate profile: {exc}") from exc
    expected = worker_identity_uri(worker_id).encode("utf-8")
    if identities.count(expected) != 1:
        raise PeerAuthenticationError("verified SAN identity is ambiguous or mismatched")
    issuer_id = cert.issuer.rfc4514_string()
    return AuthenticatedPeer(
        worker_id=worker_id,
        serial_hex=serial_hex(cert),
        fingerprint_sha256=certificate_fingerprint(cert),
        issuer_id=issuer_id,
        not_before=cert_not_before(cert).timestamp(),
        not_after=cert_not_after(cert).timestamp(),
        peer=context.peer(),
        certificate_pem=pem_values[0],
    )
