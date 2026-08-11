"""Strict X.509 profiles for the 12D private PKI.

Production CA keys are loaded only by an explicit issuer command/process.
Gateway modules import the validation helpers, never :class:`CertificateIssuer`.
"""
from __future__ import annotations

import hashlib
import ipaddress
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, unquote

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


PROFILE_VERSION = 1
WORKER_URI_PREFIX = "urn:auditmanager:worker:"
_WORKER_ID = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


class CertificateProfileError(RuntimeError):
    pass


def worker_identity_uri(worker_id: str) -> str:
    if not _WORKER_ID.fullmatch(worker_id):
        raise CertificateProfileError("invalid worker_id for certificate identity")
    return WORKER_URI_PREFIX + quote(worker_id, safe="._-")


def worker_id_from_uri(uri: str) -> str:
    if not uri.startswith(WORKER_URI_PREFIX):
        raise CertificateProfileError("certificate SAN is not a Worker identity")
    worker_id = unquote(uri[len(WORKER_URI_PREFIX):])
    if worker_identity_uri(worker_id) != uri:
        raise CertificateProfileError("non-canonical Worker certificate identity")
    return worker_id


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def cert_not_before(cert: x509.Certificate) -> datetime:
    return _utc(getattr(cert, "not_valid_before_utc", cert.not_valid_before))


def cert_not_after(cert: x509.Certificate) -> datetime:
    return _utc(getattr(cert, "not_valid_after_utc", cert.not_valid_after))


def certificate_fingerprint(cert: x509.Certificate) -> str:
    return cert.fingerprint(hashes.SHA256()).hex()


def serial_hex(cert: x509.Certificate) -> str:
    return format(cert.serial_number, "x")


def public_key_bytes(key) -> bytes:
    return key.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def assert_key_matches(cert: x509.Certificate, private_key) -> None:
    if public_key_bytes(cert.public_key()) != public_key_bytes(private_key.public_key()):
        raise CertificateProfileError("certificate and private key do not match")


def worker_id_from_certificate(cert: x509.Certificate) -> str:
    try:
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound as exc:
        raise CertificateProfileError("Worker certificate has no SAN") from exc
    identities = []
    for uri in san.get_values_for_type(x509.UniformResourceIdentifier):
        if uri.startswith(WORKER_URI_PREFIX):
            identities.append(worker_id_from_uri(uri))
    if len(identities) != 1:
        raise CertificateProfileError("Worker certificate must contain exactly one canonical URI SAN")
    return identities[0]


def validate_worker_csr(csr: x509.CertificateSigningRequest, expected_worker_id: str) -> None:
    if not csr.is_signature_valid:
        raise CertificateProfileError("CSR signature is invalid")
    try:
        san = csr.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound as exc:
        raise CertificateProfileError("Worker CSR has no SAN") from exc
    uris = san.get_values_for_type(x509.UniformResourceIdentifier)
    if uris != [worker_identity_uri(expected_worker_id)]:
        raise CertificateProfileError("CSR identity does not match authenticated worker")
    if not isinstance(csr.public_key(), ec.EllipticCurvePublicKey) or not isinstance(
        csr.public_key().curve, ec.SECP256R1
    ):
        raise CertificateProfileError("Worker CSR must use ECDSA P-256")


def validate_worker_certificate(
    cert: x509.Certificate, *, expected_worker_id: str | None = None,
    now: datetime | None = None,
) -> str:
    worker_id = worker_id_from_certificate(cert)
    if expected_worker_id is not None and worker_id != expected_worker_id:
        raise CertificateProfileError("certificate worker identity mismatch")
    current = _utc(now or datetime.now(timezone.utc))
    if current < cert_not_before(cert):
        raise CertificateProfileError("Worker certificate is not yet valid")
    if current >= cert_not_after(cert):
        raise CertificateProfileError("Worker certificate is expired")
    eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    if list(eku) != [ExtendedKeyUsageOID.CLIENT_AUTH]:
        raise CertificateProfileError("Worker certificate must have clientAuth EKU only")
    usage = cert.extensions.get_extension_for_class(x509.KeyUsage).value
    if not usage.digital_signature or usage.key_encipherment or usage.key_cert_sign:
        raise CertificateProfileError("Worker certificate keyUsage is invalid")
    if cert.extensions.get_extension_for_class(x509.BasicConstraints).value.ca:
        raise CertificateProfileError("Worker leaf cannot be a CA")
    return worker_id


def validate_server_certificate(
    cert: x509.Certificate, *, expected_identity: str,
    now: datetime | None = None,
) -> None:
    current = _utc(now or datetime.now(timezone.utc))
    if current < cert_not_before(cert) or current >= cert_not_after(cert):
        raise CertificateProfileError("Gateway server certificate is outside validity")
    eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    if list(eku) != [ExtendedKeyUsageOID.SERVER_AUTH]:
        raise CertificateProfileError("Gateway certificate must have serverAuth EKU only")
    usage = cert.extensions.get_extension_for_class(x509.KeyUsage).value
    if not usage.digital_signature or usage.key_cert_sign:
        raise CertificateProfileError("Gateway certificate keyUsage is invalid")
    san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    try:
        expected_ip = ipaddress.ip_address(expected_identity)
    except ValueError:
        if expected_identity not in san.get_values_for_type(x509.DNSName):
            raise CertificateProfileError("Gateway certificate DNS SAN mismatch")
    else:
        if expected_ip not in san.get_values_for_type(x509.IPAddress):
            raise CertificateProfileError("Gateway certificate IP SAN mismatch")


def create_worker_csr(private_key, worker_id: str) -> x509.CertificateSigningRequest:
    return (
        x509.CertificateSigningRequestBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, "AuditManager Worker")]))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.UniformResourceIdentifier(worker_identity_uri(worker_id))
            ]),
            critical=False,
        )
        .sign(private_key, hashes.SHA256())
    )


@dataclass(frozen=True)
class IssuedCertificate:
    certificate: x509.Certificate
    chain_pem: bytes
    issuer_id: str

    @property
    def certificate_pem(self) -> bytes:
        return self.certificate.public_bytes(serialization.Encoding.PEM)


class CertificateIssuer:
    """Explicit issuer boundary; never instantiate inside Gateway runtime."""

    def __init__(self, issuer_cert: x509.Certificate, issuer_key, *, chain_pem: bytes) -> None:
        self.issuer_cert = issuer_cert
        self._issuer_key = issuer_key
        self.chain_pem = chain_pem
        self.issuer_id = certificate_fingerprint(issuer_cert)
        constraints = issuer_cert.extensions.get_extension_for_class(x509.BasicConstraints).value
        if not constraints.ca:
            raise CertificateProfileError("issuer certificate is not a CA")

    @classmethod
    def from_files(cls, cert_path: Path, key_path: Path, chain_path: Path) -> "CertificateIssuer":
        return cls(
            x509.load_pem_x509_certificate(cert_path.read_bytes()),
            serialization.load_pem_private_key(key_path.read_bytes(), password=None),
            chain_pem=chain_path.read_bytes(),
        )

    def issue_worker(
        self, csr: x509.CertificateSigningRequest, *, worker_id: str,
        lifetime: timedelta, now: datetime | None = None,
    ) -> IssuedCertificate:
        validate_worker_csr(csr, worker_id)
        current = _utc(now or datetime.now(timezone.utc))
        cert = (
            x509.CertificateBuilder()
            .subject_name(csr.subject)
            .issuer_name(self.issuer_cert.subject)
            .public_key(csr.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(current - timedelta(minutes=5))
            .not_valid_after(current + lifetime)
            .add_extension(
                csr.extensions.get_extension_for_class(x509.SubjectAlternativeName).value,
                critical=False,
            )
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True, content_commitment=False, key_encipherment=False,
                    data_encipherment=False, key_agreement=False, key_cert_sign=False,
                    crl_sign=False, encipher_only=False, decipher_only=False,
                ), critical=True,
            )
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(csr.public_key()), critical=False)
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(self.issuer_cert.public_key()),
                critical=False,
            )
            .sign(self._issuer_key, hashes.SHA256())
        )
        validate_worker_certificate(cert, expected_worker_id=worker_id, now=current)
        return IssuedCertificate(cert, self.chain_pem, self.issuer_id)

    def issue_server(
        self, public_key, *, identity: str, lifetime: timedelta,
        now: datetime | None = None,
    ) -> IssuedCertificate:
        current = _utc(now or datetime.now(timezone.utc))
        try:
            san_value = x509.IPAddress(ipaddress.ip_address(identity))
        except ValueError:
            san_value = x509.DNSName(identity)
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, identity)]))
            .issuer_name(self.issuer_cert.subject)
            .public_key(public_key)
            .serial_number(x509.random_serial_number())
            .not_valid_before(current - timedelta(minutes=5))
            .not_valid_after(current + lifetime)
            .add_extension(x509.SubjectAlternativeName([san_value]), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True, content_commitment=False, key_encipherment=False,
                    data_encipherment=False, key_agreement=True, key_cert_sign=False,
                    crl_sign=False, encipher_only=False, decipher_only=False,
                ), critical=True,
            )
            .sign(self._issuer_key, hashes.SHA256())
        )
        validate_server_certificate(cert, expected_identity=identity, now=current)
        return IssuedCertificate(cert, self.chain_pem, self.issuer_id)


def csr_sha256(csr: x509.CertificateSigningRequest) -> str:
    return hashlib.sha256(csr.public_bytes(serialization.Encoding.DER)).hexdigest()
