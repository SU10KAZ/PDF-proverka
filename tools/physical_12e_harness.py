"""Narrow setup helper for the isolated physical 12E topology.

It deliberately has no production defaults.  Every command requires an
explicit new ``--root`` and operates only below it.  The helper creates an
isolated Center DB/PKI, an approved synthetic Worker identity and safe
``test_pipeline_v1`` jobs; it never calls a provider or touches :8081.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import stat
import sys
import time
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any


def _write(path: Path, data: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(data)
    os.chmod(path, mode)


def _write_json(path: Path, value: dict[str, Any], mode: int = 0o600) -> None:
    _write(path, (json.dumps(value, sort_keys=True, indent=2) + "\n").encode(), mode)


def _safe_root(raw: str, *, must_be_new: bool = False) -> Path:
    root = Path(raw).expanduser().resolve()
    if root == Path("/") or len(root.parts) < 3:
        raise SystemExit("12E root must be a specific non-root directory")
    if must_be_new and root.exists():
        raise SystemExit(f"refusing to reuse existing 12E root: {root}")
    return root


def _center_env(root: Path) -> None:
    os.environ["DISTRIBUTED_WORKERS_ENABLED"] = "true"
    os.environ["DISTRIBUTED_WORKERS_DATA_DIR"] = str(root / "workers")
    # It is never emitted and registration is not used by this helper.  The
    # setting only satisfies the isolated subsystem configuration boundary.
    os.environ["DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET"] = "12e-" + "x" * 40
    os.environ["DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES"] = "4096"


def _pem_key(key: Any) -> bytes:
    from cryptography.hazmat.primitives import serialization

    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _context(root: Path) -> dict[str, Any]:
    return json.loads((root / "context.json").read_text(encoding="utf-8"))


def prepare_center(args: argparse.Namespace) -> int:
    root = _safe_root(args.root, must_be_new=True)
    root.mkdir(mode=0o700)
    _center_env(root)

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from backend.app.security.ca_factory import create_issuing_ca, create_root_ca
    from backend.app.security.certificate_profiles import CertificateIssuer
    from backend.app.services.distributed_workers import database, registration_service, repositories
    from backend.app.services.distributed_workers.settings import get_settings

    root_key, root_cert = create_root_ca(common_name="12E isolated root")
    issuing_key, issuing_cert = create_issuing_ca(root_key, root_cert)
    root_pem = root_cert.public_bytes(serialization.Encoding.PEM)
    chain_pem = issuing_cert.public_bytes(serialization.Encoding.PEM) + root_pem
    issuer = CertificateIssuer(issuing_cert, issuing_key, chain_pem=chain_pem)
    server_key = ec.generate_private_key(ec.SECP256R1())
    server = issuer.issue_server(
        server_key.public_key(), identity=args.server_identity, lifetime=timedelta(days=30)
    )
    pki = root / "pki"
    _write(pki / "server.key", _pem_key(server_key), 0o600)
    _write(pki / "server.pem", server.certificate_pem + chain_pem, 0o644)
    _write(pki / "root.pem", root_pem, 0o644)
    _write(pki / "issuing.key", _pem_key(issuing_key), 0o600)
    _write(pki / "issuing.pem", issuing_cert.public_bytes(serialization.Encoding.PEM), 0o644)
    _write(pki / "chain.pem", chain_pem, 0o644)

    settings = get_settings()
    database.ensure_ready(settings)
    instance_id = "inst_12e_" + uuid.uuid4().hex[:16]
    worker = repositories.create_worker(
        display_name="12E isolated physical worker",
        instance_id=instance_id,
        worker_version="12e-physical",
        protocol_version=1,
        pipeline_revision="12e-physical",
        capabilities={"job_types": ["test_pipeline_v1"], "compressions": ["gzip"]},
        configured_max_slots=2,
        settings=settings,
    )
    worker = registration_service.approve_worker(
        worker_id=worker["worker_id"], display_name="12E isolated physical worker",
        configured_max_slots=2, settings=settings,
    )
    _worker, token = registration_service.rotate_token(worker_id=worker["worker_id"], settings=settings)
    context = {
        "schema": 1,
        "root": str(root),
        "worker_id": worker["worker_id"],
        "initial_instance_id": instance_id,
        "server_identity": args.server_identity,
        "created_at": time.time(),
        "provider_inference": {"claude": 0, "codex": 0, "openrouter": 0},
    }
    _write_json(root / "context.json", context, 0o600)
    _write_json(root / "worker-handoff.json", {**context, "worker_token": token}, 0o600)
    print(json.dumps({k: context[k] for k in ("schema", "root", "worker_id", "initial_instance_id", "server_identity")}, sort_keys=True))
    return 0


def issue_worker(args: argparse.Namespace) -> int:
    root = _safe_root(args.root)
    _center_env(root)
    context = _context(root)
    from cryptography import x509
    from backend.app.security.certificate_profiles import (
        CertificateIssuer, certificate_fingerprint, cert_not_after, cert_not_before,
        csr_sha256, serial_hex,
    )
    from backend.app.services.distributed_workers.certificate_registry import CertificateRegistry, PresentedCertificate
    from backend.app.services.distributed_workers.settings import get_settings

    issuer = CertificateIssuer.from_files(
        root / "pki" / "issuing.pem", root / "pki" / "issuing.key", root / "pki" / "chain.pem"
    )
    csr = x509.load_pem_x509_csr(Path(args.csr).read_bytes())
    issued = issuer.issue_worker(
        csr, worker_id=context["worker_id"], lifetime=timedelta(days=args.lifetime_days)
    )
    registry = CertificateRegistry(get_settings())
    record = registry.record_issuance(
        PresentedCertificate(
            serial_hex=serial_hex(issued.certificate),
            fingerprint_sha256=certificate_fingerprint(issued.certificate),
            worker_id=context["worker_id"],
            not_before=cert_not_before(issued.certificate).timestamp(),
            not_after=cert_not_after(issued.certificate).timestamp(),
            issuer_id=issued.issuer_id,
            certificate_pem=issued.certificate_pem + issued.chain_pem,
        ),
        csr_sha256=csr_sha256(csr), request_id="crq_12e_initial_" + secrets.token_hex(8),
        instance_id=context["initial_instance_id"],
    )
    _write(root / "pki" / "worker-cert.pem", issued.certificate_pem + issued.chain_pem, 0o644)
    _write_json(root / "certificate.json", {
        "worker_id": context["worker_id"], "serial_hex": record["serial_hex"],
        "fingerprint_sha256": record["fingerprint_sha256"], "not_after": record["not_after"],
    })
    print(json.dumps({"worker_id": context["worker_id"], "serial_hex": record["serial_hex"], "not_after": record["not_after"]}, sort_keys=True))
    return 0


def create_job(args: argparse.Namespace) -> int:
    root = _safe_root(args.root)
    _center_env(root)
    context = _context(root)
    from backend.app.models.distributed_workers import TestJobParams
    from backend.app.services.distributed_workers import job_service
    from backend.app.services.distributed_workers.settings import get_settings

    job = job_service.create_test_job(
        worker_id=context["worker_id"], project_id=args.project_id, version_id=None,
        params=TestJobParams(label=args.label, steps=args.steps, step_seconds=args.step_seconds),
        actor="12e-isolated-harness", settings=get_settings(),
    )
    result = {key: job.get(key) for key in ("job_id", "attempt_id", "state", "assigned_worker_id", "package_id")}
    _write_json(root / "jobs" / (str(job["job_id"]) + ".json"), result)
    print(json.dumps(result, sort_keys=True))
    return 0


def worker_init(args: argparse.Namespace) -> int:
    root = _safe_root(args.root, must_be_new=True)
    handoff = json.loads(Path(args.handoff).read_text(encoding="utf-8"))
    from audit_worker.key_store import LinuxPermissionKeyStore
    from audit_worker.local_store import WorkerStateStore
    from audit_worker.mtls_identity import make_csr

    root.mkdir(mode=0o700)
    store = WorkerStateStore(root / "worker_state.json", root / "token")
    store.save({
        "worker_id": handoff["worker_id"], "instance_id": handoff["initial_instance_id"],
        "registration_status": "approved", "created_by": "12e-isolated-harness",
    })
    store.write_token(handoff["worker_token"])
    key_store = LinuxPermissionKeyStore(root / "identity")
    csr = make_csr(key_store.generate(), handoff["worker_id"])
    _write(Path(args.csr_out), csr, 0o644)
    print(json.dumps({"worker_id": handoff["worker_id"], "root": str(root), "key_store_mode": oct(stat.S_IMODE(key_store.root.stat().st_mode)), "key_mode": oct(stat.S_IMODE(key_store.path.stat().st_mode))}, sort_keys=True))
    return 0


def worker_install_cert(args: argparse.Namespace) -> int:
    root = _safe_root(args.root)
    from audit_worker.mtls_identity import install_public_identity

    identity = root / "identity"
    install_public_identity(
        certificate_path=identity / "client-cert.pem", trust_bundle_path=identity / "ca-bundle.pem",
        certificate_chain_pem=Path(args.cert).read_bytes(), trust_bundle_pem=Path(args.ca).read_bytes(),
    )
    print(json.dumps({"root": str(root), "certificate_installed": True}, sort_keys=True))
    return 0


def parser() -> argparse.ArgumentParser:
    parsed = argparse.ArgumentParser(description=__doc__)
    commands = parsed.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare-center")
    prepare.add_argument("--root", required=True)
    prepare.add_argument("--server-identity", required=True)
    prepare.set_defaults(func=prepare_center)
    issue = commands.add_parser("issue-worker")
    issue.add_argument("--root", required=True)
    issue.add_argument("--csr", required=True)
    issue.add_argument("--lifetime-days", type=int, default=30)
    issue.set_defaults(func=issue_worker)
    job = commands.add_parser("create-job")
    job.add_argument("--root", required=True)
    job.add_argument("--project-id", required=True)
    job.add_argument("--label", required=True)
    job.add_argument("--steps", type=int, default=10)
    job.add_argument("--step-seconds", type=float, default=0.1)
    job.set_defaults(func=create_job)
    worker = commands.add_parser("worker-init")
    worker.add_argument("--root", required=True)
    worker.add_argument("--handoff", required=True)
    worker.add_argument("--csr-out", required=True)
    worker.set_defaults(func=worker_init)
    install = commands.add_parser("worker-install-cert")
    install.add_argument("--root", required=True)
    install.add_argument("--cert", required=True)
    install.add_argument("--ca", required=True)
    install.set_defaults(func=worker_install_cert)
    return parsed


if __name__ == "__main__":
    arguments = parser().parse_args()
    raise SystemExit(arguments.func(arguments))
