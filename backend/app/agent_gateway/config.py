"""Single typed configuration boundary for the separate Agent Gateway."""
from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from backend.app.security.certificate_profiles import (
    CertificateProfileError,
    assert_key_matches,
    validate_server_certificate,
)


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
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise GatewayConfigError(f"{name} must be a boolean")


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
    log_level: str = "INFO"
    server_certificate_path: Path | None = None
    server_private_key_path: Path | None = None
    client_ca_bundle_path: Path | None = None
    server_identity: str | None = None
    certificate_check_interval_sec: float = 2.0
    issuer_socket_path: Path | None = None

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        try:
            versions = tuple(
                int(item.strip())
                for item in os.environ.get("AGENT_GATEWAY_PROTOCOL_VERSIONS", "1").split(",")
                if item.strip()
            )
        except ValueError as exc:
            raise GatewayConfigError(
                "AGENT_GATEWAY_PROTOCOL_VERSIONS must contain integers"
            ) from exc
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
            log_level=os.environ.get("AGENT_GATEWAY_LOG_LEVEL", "INFO").strip().upper(),
            server_certificate_path=(
                Path(os.environ["AGENT_GATEWAY_SERVER_CERT"]).expanduser().resolve()
                if os.environ.get("AGENT_GATEWAY_SERVER_CERT", "").strip() else None
            ),
            server_private_key_path=(
                Path(os.environ["AGENT_GATEWAY_SERVER_KEY"]).expanduser().resolve()
                if os.environ.get("AGENT_GATEWAY_SERVER_KEY", "").strip() else None
            ),
            client_ca_bundle_path=(
                Path(os.environ["AGENT_GATEWAY_CLIENT_CA_BUNDLE"]).expanduser().resolve()
                if os.environ.get("AGENT_GATEWAY_CLIENT_CA_BUNDLE", "").strip() else None
            ),
            server_identity=(
                os.environ.get("AGENT_GATEWAY_SERVER_IDENTITY", "").strip() or None
            ),
            certificate_check_interval_sec=_float(
                "AGENT_GATEWAY_CERT_CHECK_INTERVAL_SEC", 2.0
            ),
            issuer_socket_path=(
                Path(os.environ["AGENT_GATEWAY_ISSUER_SOCKET"]).expanduser().resolve()
                if os.environ.get("AGENT_GATEWAY_ISSUER_SOCKET", "").strip() else None
            ),
        ).validated()

    def validated(self) -> "GatewayConfig":
        try:
            address = ipaddress.ip_address(self.host)
        except ValueError as exc:
            raise GatewayConfigError("AGENT_GATEWAY_HOST must be an IP literal") from exc
        if self.security_mode not in {"test_insecure", "mtls"}:
            raise GatewayConfigError("unsupported gateway security mode")
        if self.environment not in {"test", "development", "production"}:
            raise GatewayConfigError("unsupported gateway environment")
        if self.environment == "production" and self.security_mode != "mtls":
            raise GatewayConfigError("production gateway requires mTLS")
        if self.security_mode == "test_insecure" and not address.is_loopback:
            raise GatewayConfigError("test_insecure gateway may bind only loopback")
        if self.security_mode == "test_insecure" and self.port == 8443:
            raise GatewayConfigError("test_insecure gateway must not bind production port 8443")
        if self.security_mode == "mtls":
            self._validate_mtls_material()
            if self.environment == "production" and (
                self.issuer_socket_path is None or not self.issuer_socket_path.exists()
            ):
                raise GatewayConfigError("production mTLS requires available protected issuer socket")
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
            "certificate_check_interval_sec": self.certificate_check_interval_sec,
        }
        if any(float(value) <= 0 for value in positive.values()):
            raise GatewayConfigError("gateway limits and timeouts must be positive")
        if self.connection_policy != "newer_epoch_supersedes":
            raise GatewayConfigError("unsupported duplicate connection policy")
        if not self.health_enabled or not self.metrics_enabled:
            raise GatewayConfigError("12B gateway health and metrics must remain enabled")
        if self.reflection_enabled:
            raise GatewayConfigError("gRPC reflection is not enabled in 12B")
        if self.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise GatewayConfigError("unsupported gateway log level")
        return self

    def _validate_mtls_material(self) -> None:
        required = {
            "AGENT_GATEWAY_SERVER_CERT": self.server_certificate_path,
            "AGENT_GATEWAY_SERVER_KEY": self.server_private_key_path,
            "AGENT_GATEWAY_CLIENT_CA_BUNDLE": self.client_ca_bundle_path,
            "AGENT_GATEWAY_SERVER_IDENTITY": self.server_identity,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise GatewayConfigError("12D mTLS config missing: " + ", ".join(missing))
        cert_path = self.server_certificate_path
        key_path = self.server_private_key_path
        ca_path = self.client_ca_bundle_path
        assert cert_path is not None and key_path is not None and ca_path is not None
        for label, path in (("server certificate", cert_path), ("server key", key_path), ("client CA bundle", ca_path)):
            if not path.is_file() or path.is_symlink():
                raise GatewayConfigError(f"mTLS {label} must be a regular non-symlink file")
        if key_path.stat().st_mode & 0o077:
            raise GatewayConfigError("Gateway server private key must be mode 0600")
        if hasattr(os, "geteuid") and key_path.stat().st_uid != os.geteuid():
            raise GatewayConfigError("Gateway server private key owner must match service uid")
        try:
            cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
            key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
            assert_key_matches(cert, key)
            validate_server_certificate(cert, expected_identity=str(self.server_identity))
            ca_blob = ca_path.read_bytes()
            blocks = ca_blob.count(b"-----BEGIN CERTIFICATE-----")
            if blocks < 1:
                raise CertificateProfileError("client CA bundle is empty")
            # Parse every element so malformed overlap bundles fail startup.
            for item in ca_blob.split(b"-----END CERTIFICATE-----"):
                if b"-----BEGIN CERTIFICATE-----" in item:
                    ca = x509.load_pem_x509_certificate(item + b"-----END CERTIFICATE-----\n")
                    if not ca.extensions.get_extension_for_class(x509.BasicConstraints).value.ca:
                        raise CertificateProfileError("client trust bundle contains a non-CA leaf")
        except (ValueError, OSError, CertificateProfileError, x509.ExtensionNotFound) as exc:
            raise GatewayConfigError(f"invalid Gateway mTLS material: {exc}") from exc
