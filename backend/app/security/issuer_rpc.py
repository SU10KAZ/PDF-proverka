"""Narrow Unix-socket client for Gateway → protected certificate issuer."""
from __future__ import annotations

import asyncio
import base64
import json
import struct
import socket
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


class UnixSocketEnrollmentAuthority:
    """Synchronous bootstrap/admin client; only public CSR crosses the socket."""

    def __init__(self, path: Path, *, timeout: float = 10.0) -> None:
        self.path = path
        self.timeout = timeout

    def enroll(
        self, *, worker_id: str, instance_id: str, csr_pem: bytes,
        request_id: str, settings=None,
    ) -> CertificateResponse:
        request = json.dumps({
            "operation": "enroll", "worker_id": worker_id,
            "instance_id": instance_id, "request_id": request_id,
            "csr_pem": base64.b64encode(csr_pem).decode("ascii"),
        }, sort_keys=True, separators=(",", ":")).encode()
        if len(request) > 256 * 1024:
            raise IssuerRpcError("issuer enrollment request is too large")
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(self.timeout)
                client.connect(str(self.path))
                client.sendall(struct.pack("!I", len(request)) + request)
                header = self._read_exact(client, 4)
                size = struct.unpack("!I", header)[0]
                if size > 512 * 1024:
                    raise IssuerRpcError("issuer response is too large")
                response = json.loads(self._read_exact(client, size).decode())
        except (OSError, ValueError) as exc:
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

    @staticmethod
    def _read_exact(client: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            part = client.recv(size - len(chunks))
            if not part:
                raise IssuerRpcError("protected issuer closed response")
            chunks.extend(part)
        return bytes(chunks)
