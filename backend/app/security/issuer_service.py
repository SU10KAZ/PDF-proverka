"""Protected online issuing-CA service; listens on a local Unix socket only."""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import signal
import socket
import struct
from datetime import timedelta
from pathlib import Path

from backend.app.agent_gateway.auth import AuthenticatedPeer
from backend.app.security.certificate_profiles import CertificateIssuer
from backend.app.services.distributed_workers.certificate_lifecycle import (
    CertificateLifecycleAuthority,
)
from backend.app.services.distributed_workers.certificate_registry import CertificateRegistry
from backend.app.services.distributed_workers.settings import get_settings


logger = logging.getLogger("worker-certificate-issuer")
MAX_REQUEST = 256 * 1024


class IssuerServer:
    def __init__(self) -> None:
        self.socket_path = Path(os.environ["AUDIT_WORKER_ISSUER_SOCKET"])
        key_path = Path(os.environ["AUDIT_WORKER_ISSUER_KEY"])
        cert_path = Path(os.environ["AUDIT_WORKER_ISSUER_CERT"])
        chain_path = Path(os.environ["AUDIT_WORKER_ISSUER_CHAIN"])
        if key_path.is_symlink() or key_path.stat().st_mode & 0o077:
            raise RuntimeError("issuing CA key must be a non-symlink mode-0600 file")
        self.allowed_uid = int(os.environ["AUDIT_WORKER_GATEWAY_UID"])
        self.authority = CertificateLifecycleAuthority(
            issuer=CertificateIssuer.from_files(cert_path, key_path, chain_path),
            registry=CertificateRegistry(get_settings()),
            worker_lifetime=timedelta(
                days=max(1, min(90, int(os.environ.get("AUDIT_WORKER_CERT_LIFETIME_DAYS", "30"))))
            ),
        )
        self.server = None

    async def start(self) -> None:
        self.socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
        if self.socket_path.exists():
            if not self.socket_path.is_socket():
                raise RuntimeError("issuer socket path exists and is not a socket")
            self.socket_path.unlink()
        self.server = await asyncio.start_unix_server(self._handle, path=str(self.socket_path))
        os.chmod(self.socket_path, 0o660)

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _handle(self, reader, writer) -> None:
        try:
            sock = writer.get_extra_info("socket")
            if sock is None or not hasattr(socket, "SO_PEERCRED"):
                raise RuntimeError("Unix peer credentials unavailable")
            _pid, uid, _gid = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
            if uid != self.allowed_uid:
                raise PermissionError("issuer caller uid rejected")
            size = struct.unpack("!I", await reader.readexactly(4))[0]
            if size <= 0 or size > MAX_REQUEST:
                raise ValueError("issuer request size rejected")
            request = json.loads((await reader.readexactly(size)).decode())
            if request.get("operation") != "renew":
                raise ValueError("issuer operation rejected")
            item = request["peer"]
            peer = AuthenticatedPeer(
                worker_id=item["worker_id"], serial_hex=item["serial_hex"],
                fingerprint_sha256=item["fingerprint_sha256"], issuer_id=item["issuer_id"],
                not_before=float(item["not_before"]), not_after=float(item["not_after"]),
                peer=item["peer"],
                certificate_pem=base64.b64decode(item["certificate_pem"], validate=True),
            )
            result = self.authority.renew(
                peer=peer,
                csr_pem=base64.b64decode(request["csr_pem"], validate=True),
                request_id=request["request_id"],
            )
            response = {
                "ok": True,
                "certificate_chain_pem": base64.b64encode(result.certificate_chain_pem).decode(),
                "trust_chain_pem": base64.b64encode(result.trust_chain_pem).decode(),
                "serial_hex": result.serial_hex,
                "fingerprint_sha256": result.fingerprint_sha256,
                "not_before": result.not_before,
                "not_after": result.not_after,
                "request_id": result.request_id,
            }
        except Exception as exc:
            logger.warning("issuer request rejected: %s", type(exc).__name__)
            response = {"ok": False, "error": "issuer request rejected"}
        raw = json.dumps(response, sort_keys=True, separators=(",", ":")).encode()
        writer.write(struct.pack("!I", len(raw)) + raw)
        await writer.drain()
        writer.close()
        await writer.wait_closed()


async def _main() -> None:
    logging.basicConfig(level=logging.INFO)
    service = IssuerServer()
    await service.start()
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    await stopped.wait()
    await service.stop()


if __name__ == "__main__":
    asyncio.run(_main())
