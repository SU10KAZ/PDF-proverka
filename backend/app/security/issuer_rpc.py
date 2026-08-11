"""Narrow Unix-socket client for Gateway → protected certificate issuer."""
from __future__ import annotations

import asyncio
import base64
import json
import struct
from pathlib import Path

from backend.app.agent_gateway.auth import AuthenticatedPeer
from backend.app.services.distributed_workers.certificate_lifecycle import CertificateResponse


class IssuerRpcError(RuntimeError):
    pass


class UnixSocketRenewalAuthority:
    def __init__(self, path: Path, *, timeout: float = 10.0) -> None:
        self.path = path
        self.timeout = timeout

    async def renew(
        self, *, peer: AuthenticatedPeer, csr_pem: bytes, request_id: str
    ) -> CertificateResponse:
        payload = {
            "operation": "renew",
            "request_id": request_id,
            "csr_pem": base64.b64encode(csr_pem).decode("ascii"),
            "peer": {
                "worker_id": peer.worker_id,
                "serial_hex": peer.serial_hex,
                "fingerprint_sha256": peer.fingerprint_sha256,
                "issuer_id": peer.issuer_id,
                "not_before": peer.not_before,
                "not_after": peer.not_after,
                "peer": peer.peer,
                "certificate_pem": base64.b64encode(peer.certificate_pem).decode("ascii"),
            },
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        if len(raw) > 256 * 1024:
            raise IssuerRpcError("issuer request is too large")

        async def exchange():
            reader, writer = await asyncio.open_unix_connection(str(self.path))
            try:
                writer.write(struct.pack("!I", len(raw)) + raw)
                await writer.drain()
                size = struct.unpack("!I", await reader.readexactly(4))[0]
                if size > 512 * 1024:
                    raise IssuerRpcError("issuer response is too large")
                return json.loads((await reader.readexactly(size)).decode())
            finally:
                writer.close()
                await writer.wait_closed()

        try:
            response = await asyncio.wait_for(exchange(), timeout=self.timeout)
        except (OSError, asyncio.TimeoutError, ValueError) as exc:
            raise IssuerRpcError("protected issuer unavailable") from exc
        if not response.get("ok"):
            raise IssuerRpcError(str(response.get("error") or "issuer rejected request")[:200])
        return CertificateResponse(
            certificate_chain_pem=base64.b64decode(response["certificate_chain_pem"], validate=True),
            trust_chain_pem=base64.b64decode(response["trust_chain_pem"], validate=True),
            serial_hex=response["serial_hex"],
            fingerprint_sha256=response["fingerprint_sha256"],
            not_before=float(response["not_before"]),
            not_after=float(response["not_after"]),
            request_id=response["request_id"],
        )
