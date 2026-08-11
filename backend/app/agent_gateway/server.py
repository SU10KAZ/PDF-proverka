"""Lifecycle of the separate localhost-only 12B gRPC server."""
from __future__ import annotations

import logging

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from backend.app.agent_gateway.config import GatewayConfig, GatewayConfigError
from backend.app.agent_gateway.domain import GatewayDomainAdapter
from backend.app.agent_gateway.metrics import GatewayMetrics
from backend.app.agent_gateway.registry import GatewayConnectionRegistry
from backend.app.agent_gateway.service import AgentStreamService
from backend.app.agent_gateway.certificate_service import (
    RenewalAuthority,
    WorkerCertificateService,
)
from backend.app.services.distributed_workers import database
from backend.app.services.distributed_workers.certificate_registry import CertificateRegistry
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersSettings,
    get_settings,
)
from contracts.agent_stream.v1 import agent_stream_pb2_grpc as stream_grpc
from contracts.worker_certificate.v1 import worker_certificate_pb2_grpc as cert_grpc


logger = logging.getLogger("agent-gateway")
SERVICE_NAME = "auditmanager.agent_stream.v1.AgentStreamService"


class GatewayServer:
    def __init__(
        self,
        config: GatewayConfig,
        *,
        worker_settings: DistributedWorkersSettings | None = None,
        renewal_authority: RenewalAuthority | None = None,
    ) -> None:
        self.config = config.validated()
        self.worker_settings = worker_settings or get_settings()
        self.metrics = GatewayMetrics()
        self.registry = GatewayConnectionRegistry(self.metrics)
        self.domain = GatewayDomainAdapter(self.worker_settings)
        self.certificate_registry = (
            CertificateRegistry(self.worker_settings)
            if self.config.security_mode == "mtls" else None
        )
        self.service = AgentStreamService(
            config=self.config,
            domain=self.domain,
            registry=self.registry,
            metrics=self.metrics,
            certificate_registry=self.certificate_registry,
        )
        self.certificate_service = (
            WorkerCertificateService(
                authority=renewal_authority,
                registry=self.certificate_registry,
                metrics=self.metrics,
            )
            if renewal_authority is not None and self.certificate_registry is not None
            else None
        )
        self.grpc_server: grpc.aio.Server | None = None
        self.health_servicer = health.aio.HealthServicer()
        self.bound_port: int | None = None

    async def start(self) -> int:
        self.worker_settings.require_enabled()
        await database.run_db(database.ensure_ready, self.worker_settings)
        self.grpc_server = grpc.aio.server(
            options=(
                ("grpc.max_receive_message_length", self.config.max_inbound_message_bytes),
                ("grpc.max_send_message_length", self.config.max_outbound_message_bytes),
            ),
            maximum_concurrent_rpcs=None,
        )
        stream_grpc.add_AgentStreamServiceServicer_to_server(
            self.service, self.grpc_server
        )
        if self.certificate_service is not None:
            cert_grpc.add_WorkerCertificateServiceServicer_to_server(
                self.certificate_service, self.grpc_server
            )
        if self.config.health_enabled:
            health_pb2_grpc.add_HealthServicer_to_server(
                self.health_servicer, self.grpc_server
            )
        host = f"[{self.config.host}]" if ":" in self.config.host else self.config.host
        address = f"{host}:{self.config.port}"
        if self.config.security_mode == "mtls":
            assert self.config.server_certificate_path is not None
            assert self.config.server_private_key_path is not None
            assert self.config.client_ca_bundle_path is not None
            credentials = grpc.ssl_server_credentials(
                ((
                    self.config.server_private_key_path.read_bytes(),
                    self.config.server_certificate_path.read_bytes(),
                ),),
                root_certificates=self.config.client_ca_bundle_path.read_bytes(),
                require_client_auth=True,
            )
            port = self.grpc_server.add_secure_port(address, credentials)
        else:
            port = self.grpc_server.add_insecure_port(address)
        if not port:
            raise GatewayConfigError(f"could not bind Agent Gateway to {address}")
        self.bound_port = port
        await self.grpc_server.start()
        if self.config.health_enabled:
            await self.health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
            await self.health_servicer.set(
                SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING
            )
        logger.info(
            "agent-gateway started",
            extra={
                "bind_host": self.config.host,
                "bind_port": port,
                "security_mode": self.config.security_mode,
            },
        )
        return port

    async def stop(self, grace: float | None = None) -> None:
        if self.grpc_server is None:
            return
        if self.config.health_enabled:
            await self.health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
            await self.health_servicer.set(
                SERVICE_NAME, health_pb2.HealthCheckResponse.NOT_SERVING
            )
        await self.service.drain()
        await self.grpc_server.stop(
            self.config.graceful_shutdown_sec if grace is None else max(0.0, grace)
        )
        self.grpc_server = None
        logger.info("agent-gateway stopped")

    async def wait_for_termination(self) -> None:
        if self.grpc_server is not None:
            await self.grpc_server.wait_for_termination()
