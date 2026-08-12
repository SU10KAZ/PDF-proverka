"""mTLS-authenticated certificate renewal RPC boundary."""
from __future__ import annotations

import inspect
from typing import Protocol

import grpc

from backend.app.agent_gateway.auth import PeerAuthenticationError, authenticated_peer
from backend.app.agent_gateway.metrics import GatewayMetrics
from backend.app.services.distributed_workers.certificate_lifecycle import (
    CertificateLifecycleError,
    CertificateResponse,
)
from backend.app.services.distributed_workers.certificate_registry import (
    CertificateRegistry,
    CertificateRegistryError,
)
from contracts.worker_certificate.v1 import worker_certificate_pb2 as cert_pb
from contracts.worker_certificate.v1 import worker_certificate_pb2_grpc as cert_grpc


class RenewalAuthority(Protocol):
    def renew(self, *, peer, csr_pem: bytes, request_id: str) -> CertificateResponse: ...


class WorkerCertificateService(cert_grpc.WorkerCertificateServiceServicer):
    def __init__(
        self, *, authority: RenewalAuthority, registry: CertificateRegistry,
        metrics: GatewayMetrics,
    ) -> None:
        self.authority = authority
        self.registry = registry
        self.metrics = metrics

    async def RenewCertificate(self, request, context):
        try:
            peer = authenticated_peer(context)
            self.registry.validate_presented(
                serial_hex=peer.serial_hex,
                fingerprint_sha256=peer.fingerprint_sha256,
                worker_id=peer.worker_id,
            )
            response = self.authority.renew(
                peer=peer, csr_pem=bytes(request.csr_pem),
                request_id=str(request.request_id),
            )
            if inspect.isawaitable(response):
                response = await response
        except PeerAuthenticationError:
            self.metrics.inc("certificate_renewal_failures")
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "mTLS peer rejected")
        except CertificateRegistryError:
            self.metrics.inc("certificate_renewal_failures")
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "current certificate is not active")
        except CertificateLifecycleError as exc:
            self.metrics.inc("certificate_renewal_failures")
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc)[:200])
        self.metrics.inc("certificate_renewals")
        return cert_pb.CertificateIdentity(
            certificate_chain_pem=response.certificate_chain_pem,
            trust_bundle_pem=response.trust_chain_pem,
            serial_hex=response.serial_hex,
            fingerprint_sha256=response.fingerprint_sha256,
            not_before_unix_seconds=int(response.not_before),
            not_after_unix_seconds=int(response.not_after),
            request_id=response.request_id,
        )

    async def ValidateIdentity(self, request, context):
        try:
            peer = authenticated_peer(context)
            self.registry.validate_presented(
                serial_hex=peer.serial_hex,
                fingerprint_sha256=peer.fingerprint_sha256,
                worker_id=peer.worker_id,
            )
        except (PeerAuthenticationError, CertificateRegistryError):
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "mTLS identity rejected")
        return cert_pb.ValidatedIdentity(
            worker_id=peer.worker_id,
            serial_hex=peer.serial_hex,
            fingerprint_sha256=peer.fingerprint_sha256,
            not_after_unix_seconds=int(peer.not_after),
        )

    async def ActivateCertificate(self, request, context):
        try:
            peer = authenticated_peer(context)
            self.registry.validate_presented(
                serial_hex=peer.serial_hex,
                fingerprint_sha256=peer.fingerprint_sha256,
                worker_id=peer.worker_id,
            )
            self.registry.replace(
                str(request.predecessor_serial_hex), peer.serial_hex
            )
        except PeerAuthenticationError:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "mTLS peer rejected")
        except CertificateRegistryError as exc:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc)[:200])
        self.metrics.inc("certificate_rotations")
        return cert_pb.ValidatedIdentity(
            worker_id=peer.worker_id,
            serial_hex=peer.serial_hex,
            fingerprint_sha256=peer.fingerprint_sha256,
            not_after_unix_seconds=int(peer.not_after),
        )
