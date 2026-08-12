#!/usr/bin/env python3
"""Offline/admin PKI tool.  It is not imported by Agent Gateway runtime."""
from __future__ import annotations

import argparse
import os
from datetime import timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from backend.app.security.ca_factory import create_issuing_ca, create_root_ca
from backend.app.security.certificate_profiles import CertificateIssuer


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
    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
