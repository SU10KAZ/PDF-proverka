"""Explicit CA creation helpers used by the offline/admin issuer tool and tests."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


def _ca_builder(*, subject: x509.Name, issuer: x509.Name, public_key, now: datetime, lifetime: timedelta, path_length: int):
    return (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + lifetime)
        .add_extension(x509.BasicConstraints(ca=True, path_length=path_length), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False,
                key_encipherment=False, data_encipherment=False,
                key_agreement=False, key_cert_sign=True, crl_sign=True,
                encipher_only=False, decipher_only=False,
            ), critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False)
    )


def create_root_ca(*, common_name: str = "AuditManager Offline Root CA", lifetime: timedelta = timedelta(days=3650), now: datetime | None = None):
    current = now or datetime.now(timezone.utc)
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    cert = (
        _ca_builder(
            subject=name, issuer=name, public_key=key.public_key(), now=current,
            lifetime=lifetime, path_length=1,
        )
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    return key, cert


def create_issuing_ca(
    root_key, root_cert: x509.Certificate, *,
    common_name: str = "AuditManager Worker Issuing CA",
    lifetime: timedelta = timedelta(days=1825), now: datetime | None = None,
):
    current = now or datetime.now(timezone.utc)
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    cert = (
        _ca_builder(
            subject=name, issuer=root_cert.subject, public_key=key.public_key(),
            now=current, lifetime=lifetime, path_length=0,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(root_key.public_key()),
            critical=False,
        )
        .sign(root_key, hashes.SHA256())
    )
    return key, cert
