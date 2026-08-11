"""Single typed configuration boundary for the separate Agent Gateway."""
from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass


class GatewayConfigError(RuntimeError):
    pass


def _integer(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise GatewayConfigError(f"{name} must be an integer") from exc


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise GatewayConfigError(f"{name} must be numeric") from exc


def _boolean(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    return default if raw is None else raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class GatewayConfig:
    host: str = "127.0.0.1"
    port: int = 0
    environment: str = "test"
    security_mode: str = "test_insecure"
    supported_protocol_versions: tuple[int, ...] = (1,)
    heartbeat_timeout_sec: float = 90.0
    idle_timeout_sec: float = 120.0
    max_inbound_message_bytes: int = 1024 * 1024
    max_outbound_message_bytes: int = 1024 * 1024
    max_event_batch_count: int = 256
    max_unacked_event_window: int = 1024
    max_outbound_queue: int = 128
    offer_timeout_sec: float = 30.0
    offer_poll_interval_sec: float = 0.1
    graceful_shutdown_sec: float = 5.0
    connection_policy: str = "newer_epoch_supersedes"
    health_enabled: bool = True
    metrics_enabled: bool = True
    reflection_enabled: bool = False

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        versions = tuple(
            int(item.strip())
            for item in os.environ.get("AGENT_GATEWAY_PROTOCOL_VERSIONS", "1").split(",")
            if item.strip()
        )
        return cls(
            host=os.environ.get("AGENT_GATEWAY_HOST", "127.0.0.1").strip(),
            port=_integer("AGENT_GATEWAY_PORT", 0),
            environment=os.environ.get("AGENT_GATEWAY_ENVIRONMENT", "test").strip().lower(),
            security_mode=os.environ.get("AGENT_GATEWAY_SECURITY_MODE", "test_insecure").strip().lower(),
            supported_protocol_versions=versions,
            heartbeat_timeout_sec=_float("AGENT_GATEWAY_HEARTBEAT_TIMEOUT_SEC", 90.0),
            idle_timeout_sec=_float("AGENT_GATEWAY_IDLE_TIMEOUT_SEC", 120.0),
            max_inbound_message_bytes=_integer("AGENT_GATEWAY_MAX_INBOUND_BYTES", 1024 * 1024),
            max_outbound_message_bytes=_integer("AGENT_GATEWAY_MAX_OUTBOUND_BYTES", 1024 * 1024),
            max_event_batch_count=_integer("AGENT_GATEWAY_MAX_EVENT_BATCH_COUNT", 256),
            max_unacked_event_window=_integer("AGENT_GATEWAY_MAX_UNACKED_EVENTS", 1024),
            max_outbound_queue=_integer("AGENT_GATEWAY_MAX_OUTBOUND_QUEUE", 128),
            offer_timeout_sec=_float("AGENT_GATEWAY_OFFER_TIMEOUT_SEC", 30.0),
            offer_poll_interval_sec=_float("AGENT_GATEWAY_OFFER_POLL_SEC", 0.1),
            graceful_shutdown_sec=_float("AGENT_GATEWAY_SHUTDOWN_SEC", 5.0),
            connection_policy=os.environ.get("AGENT_GATEWAY_CONNECTION_POLICY", "newer_epoch_supersedes").strip(),
            health_enabled=_boolean("AGENT_GATEWAY_HEALTH_ENABLED", True),
            metrics_enabled=_boolean("AGENT_GATEWAY_METRICS_ENABLED", True),
            reflection_enabled=_boolean("AGENT_GATEWAY_REFLECTION_ENABLED", False),
        ).validated()

    def validated(self) -> "GatewayConfig":
        try:
            address = ipaddress.ip_address(self.host)
        except ValueError as exc:
            raise GatewayConfigError("AGENT_GATEWAY_HOST must be an IP literal") from exc
        if self.security_mode not in {"test_insecure", "mtls"}:
            raise GatewayConfigError("unsupported gateway security mode")
        if self.environment == "production" and self.security_mode != "mtls":
            raise GatewayConfigError("production gateway requires mTLS")
        if self.security_mode == "test_insecure" and not address.is_loopback:
            raise GatewayConfigError("test_insecure gateway may bind only loopback")
        if self.security_mode == "test_insecure" and self.port == 8443:
            raise GatewayConfigError("test_insecure gateway must not bind production port 8443")
        if self.security_mode == "mtls":
            raise GatewayConfigError("mTLS runtime is intentionally deferred to 12D")
        if not (0 <= self.port <= 65535):
            raise GatewayConfigError("gateway port outside valid range")
        if self.supported_protocol_versions != (1,):
            raise GatewayConfigError("12B gateway supports protocol major 1 only")
        positive = {
            "heartbeat_timeout_sec": self.heartbeat_timeout_sec,
            "idle_timeout_sec": self.idle_timeout_sec,
            "max_inbound_message_bytes": self.max_inbound_message_bytes,
            "max_outbound_message_bytes": self.max_outbound_message_bytes,
            "max_event_batch_count": self.max_event_batch_count,
            "max_unacked_event_window": self.max_unacked_event_window,
            "max_outbound_queue": self.max_outbound_queue,
            "offer_timeout_sec": self.offer_timeout_sec,
            "offer_poll_interval_sec": self.offer_poll_interval_sec,
            "graceful_shutdown_sec": self.graceful_shutdown_sec,
        }
        if any(float(value) <= 0 for value in positive.values()):
            raise GatewayConfigError("gateway limits and timeouts must be positive")
        if self.connection_policy != "newer_epoch_supersedes":
            raise GatewayConfigError("unsupported duplicate connection policy")
        if self.environment == "production" and self.reflection_enabled:
            raise GatewayConfigError("gRPC reflection is disabled in production")
        return self
