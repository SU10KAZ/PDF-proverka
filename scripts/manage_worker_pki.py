#!/usr/bin/env python3
"""Offline/admin PKI tool.  It is not imported by Agent Gateway runtime."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
from datetime import timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from backend.app.security.ca_factory import create_issuing_ca, create_root_ca
from backend.app.security.certificate_profiles import (
    CertificateIssuer,
    assert_key_matches,
    certificate_fingerprint,
    validate_server_certificate,
)


def _write(path: Path, data: bytes, mode: int) -> None:
    if path.exists() and path.is_symlink():
        raise SystemExit(f"refusing symlink output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


def _key_pem(key) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def init_root(args) -> None:
    key, cert = create_root_ca(lifetime=timedelta(days=args.days))
    _write(args.key, _key_pem(key), 0o600)
    _write(args.cert, cert.public_bytes(serialization.Encoding.PEM), 0o644)


def init_issuer(args) -> None:
    root_key = serialization.load_pem_private_key(args.root_key.read_bytes(), password=None)
    root_cert = x509.load_pem_x509_certificate(args.root_cert.read_bytes())
    key, cert = create_issuing_ca(
        root_key, root_cert, lifetime=timedelta(days=args.days)
    )
    _write(args.key, _key_pem(key), 0o600)
    _write(args.cert, cert.public_bytes(serialization.Encoding.PEM), 0o644)
    _write(
        args.chain,
        cert.public_bytes(serialization.Encoding.PEM)
        + root_cert.public_bytes(serialization.Encoding.PEM),
        0o644,
    )


def issue_server(args) -> None:
    issuer = CertificateIssuer.from_files(args.issuer_cert, args.issuer_key, args.chain)
    key = ec.generate_private_key(ec.SECP256R1())
    issued = issuer.issue_server(
        key.public_key(), identity=args.identity, lifetime=timedelta(days=args.days)
    )
    _write(args.key, _key_pem(key), 0o600)
    _write(args.cert, issued.certificate_pem + issued.chain_pem, 0o644)


def issue_worker(args) -> None:
    issuer = CertificateIssuer.from_files(args.issuer_cert, args.issuer_key, args.chain)
    csr = x509.load_pem_x509_csr(args.csr.read_bytes())
    issued = issuer.issue_worker(
        csr, worker_id=args.worker_id, lifetime=timedelta(days=args.days)
    )
    _write(args.cert, issued.certificate_pem + issued.chain_pem, 0o644)


def _certificates(path: Path) -> list[x509.Certificate]:
    marker = b"-----END CERTIFICATE-----"
    chunks = []
    for part in path.read_bytes().split(marker):
        if b"-----BEGIN CERTIFICATE-----" in part:
            chunks.append(x509.load_pem_x509_certificate(part + marker + b"\n"))
    if not chunks:
        raise ValueError(f"certificate file is empty: {path}")
    return chunks


def _plain_file(path: Path, *, private: bool = False) -> None:
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ValueError(f"PKI path is not a plain file: {path}")
    if private and stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError(f"private key is accessible outside its owner: {path}")


def _verify_signature(cert: x509.Certificate, issuer: x509.Certificate) -> None:
    public_key = issuer.public_key()
    if not isinstance(public_key, ec.EllipticCurvePublicKey):
        raise ValueError("production PKI CA key must be ECDSA")
    public_key.verify(
        cert.signature,
        cert.tbs_certificate_bytes,
        ec.ECDSA(cert.signature_hash_algorithm),
    )


def validate_pki_set(args) -> dict[str, object]:
    """Validate a complete existing PKI set without ever regenerating it."""
    private_paths = (args.root_key, args.issuer_key, args.server_key)
    public_paths = (
        args.root_cert,
        args.issuer_cert,
        args.issuer_chain,
        args.server_cert,
        args.worker_trust,
    )
    for path in private_paths:
        _plain_file(path, private=True)
    for path in public_paths:
        _plain_file(path)

    root_key = serialization.load_pem_private_key(args.root_key.read_bytes(), password=None)
    issuer_key = serialization.load_pem_private_key(
        args.issuer_key.read_bytes(), password=None
    )
    server_key = serialization.load_pem_private_key(
        args.server_key.read_bytes(), password=None
    )
    root = x509.load_pem_x509_certificate(args.root_cert.read_bytes())
    issuer = x509.load_pem_x509_certificate(args.issuer_cert.read_bytes())
    issuer_chain = _certificates(args.issuer_chain)
    server_chain = _certificates(args.server_cert)
    trust_chain = _certificates(args.worker_trust)

    assert_key_matches(root, root_key)
    assert_key_matches(issuer, issuer_key)
    assert_key_matches(server_chain[0], server_key)
    if root.subject != root.issuer:
        raise ValueError("root CA is not self-issued")
    if issuer.issuer != root.subject or server_chain[0].issuer != issuer.subject:
        raise ValueError("production PKI issuer chain names do not match")
    _verify_signature(root, root)
    _verify_signature(issuer, root)
    _verify_signature(server_chain[0], issuer)

    root_constraints = root.extensions.get_extension_for_class(
        x509.BasicConstraints
    ).value
    issuer_constraints = issuer.extensions.get_extension_for_class(
        x509.BasicConstraints
    ).value
    if not root_constraints.ca or root_constraints.path_length != 1:
        raise ValueError("root CA basic constraints do not match profile")
    if not issuer_constraints.ca or issuer_constraints.path_length != 0:
        raise ValueError("issuing CA basic constraints do not match profile")
    for name, cert in (("root", root), ("issuer", issuer)):
        usage = cert.extensions.get_extension_for_class(x509.KeyUsage).value
        if not usage.key_cert_sign or not usage.crl_sign:
            raise ValueError(f"{name} CA key usage does not match profile")
    validate_server_certificate(server_chain[0], expected_identity=args.identity)

    expected_chain = [certificate_fingerprint(issuer), certificate_fingerprint(root)]
    if [certificate_fingerprint(cert) for cert in issuer_chain] != expected_chain:
        raise ValueError("issuing chain does not contain exactly issuer + root")
    if [certificate_fingerprint(cert) for cert in trust_chain] != expected_chain:
        raise ValueError("Worker trust bundle does not match issuing chain")
    if [certificate_fingerprint(cert) for cert in server_chain[1:]] != expected_chain:
        raise ValueError("Gateway server chain does not contain issuer + root")

    return {
        "status": "valid_existing_pki_preserved",
        "root_certificate_sha256": certificate_fingerprint(root),
        "issuer_certificate_sha256": certificate_fingerprint(issuer),
        "server_certificate_sha256": certificate_fingerprint(server_chain[0]),
        "root_subject": root.subject.rfc4514_string(),
        "issuer_subject": issuer.subject.rfc4514_string(),
        "server_identity": args.identity,
        "file_sha256": {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (*private_paths, *public_paths)
        },
        "private_key_contents_exposed": False,
    }


def validate_set(args) -> None:
    print(json.dumps(validate_pki_set(args), sort_keys=True, separators=(",", ":")))


def _add_validate_set(sub) -> None:
    validate = sub.add_parser(
        "validate-set",
        help="validate and fingerprint a complete existing PKI set; never writes",
    )
    for name in (
        "root-key", "root-cert", "issuer-key", "issuer-cert", "issuer-chain",
        "server-key", "server-cert", "worker-trust",
    ):
        validate.add_argument(f"--{name}", type=Path, required=True)
    validate.add_argument("--identity", required=True)
    validate.set_defaults(func=validate_set)


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(required=True)
    root = sub.add_parser("init-root")
    root.add_argument("--key", type=Path, required=True)
    root.add_argument("--cert", type=Path, required=True)
    root.add_argument("--days", type=int, default=3650)
    root.set_defaults(func=init_root)
    inter = sub.add_parser("init-issuer")
    inter.add_argument("--root-key", type=Path, required=True)
    inter.add_argument("--root-cert", type=Path, required=True)
    inter.add_argument("--key", type=Path, required=True)
    inter.add_argument("--cert", type=Path, required=True)
    inter.add_argument("--chain", type=Path, required=True)
    inter.add_argument("--days", type=int, default=1825)
    inter.set_defaults(func=init_issuer)
    server = sub.add_parser("issue-server")
    server.add_argument("--issuer-key", type=Path, required=True)
    server.add_argument("--issuer-cert", type=Path, required=True)
    server.add_argument("--chain", type=Path, required=True)
    server.add_argument("--identity", required=True)
    server.add_argument("--key", type=Path, required=True)
    server.add_argument("--cert", type=Path, required=True)
    server.add_argument("--days", type=int, default=90)
    server.set_defaults(func=issue_server)
    worker = sub.add_parser("issue-worker")
    worker.add_argument("--issuer-key", type=Path, required=True)
    worker.add_argument("--issuer-cert", type=Path, required=True)
    worker.add_argument("--chain", type=Path, required=True)
    worker.add_argument("--worker-id", required=True)
    worker.add_argument("--csr", type=Path, required=True)
    worker.add_argument("--cert", type=Path, required=True)
    worker.add_argument("--days", type=int, default=30)
    worker.set_defaults(func=issue_worker)
    _add_validate_set(sub)
    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
