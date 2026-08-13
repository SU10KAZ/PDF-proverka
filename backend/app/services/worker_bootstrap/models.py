"""Контракт one-click bootstrap, общий для CLI, API и тестового executor."""
from __future__ import annotations

import re
from enum import Enum
from pathlib import PurePosixPath
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class BootstrapOperation(str, Enum):
    INSTALL = "install"
    UPDATE = "update"
    REPAIR = "repair"
    STATUS = "status"
    VALIDATE = "validate"
    ROLLBACK = "rollback"
    UNINSTALL = "uninstall"
    DEREGISTER = "deregister"


class BootstrapState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    ACTION_REQUIRED = "action_required"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class BootstrapStep(str, Enum):
    CREATED = "created"
    SSH_ENROLLMENT = "ssh_enrollment"
    PREFLIGHT = "preflight"
    RELEASE_INSTALL = "release_install"
    CONFIGURATION = "configuration"
    PROVIDERS = "providers"
    REGISTRATION = "registration"
    STARTING = "starting"
    SELF_TEST = "self_test"
    CERTIFICATE_ENROLLMENT = "certificate_enrollment"
    READY = "ready"
    ROLLING_BACK = "rolling_back"
    FAILED = "failed"


class ProviderChoice(str, Enum):
    CLAUDE = "claude"
    CODEX = "codex"
    OPENROUTER = "openrouter"


_SAFE_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,254}$")
_FINGERPRINT = re.compile(r"^SHA256:[A-Za-z0-9+/]{20,80}={0,2}$")


def validate_install_root(value: str) -> str:
    """Разрешить отдельный абсолютный каталог, но не системный корень.

    Значение передаётся удалённым shell-скриптам позиционным аргументом. Даже
    при таком безопасном транспорте запрещаем управляющие символы и `..`, чтобы
    audit trail всегда однозначно называл цель.
    """
    if not value or value != value.strip() or any(ch.isspace() for ch in value):
        raise ValueError("install_root содержит пробельные/управляющие символы")
    if not re.fullmatch(r"/[A-Za-z0-9._+@/-]+", value):
        raise ValueError("install_root содержит небезопасные для systemd символы")
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("install_root должен быть нормальным абсолютным путём")
    blocked = {
        "/", "/bin", "/boot", "/dev", "/etc", "/home", "/lib", "/lib64",
        "/media", "/mnt", "/opt", "/proc", "/root", "/run", "/sbin",
        "/srv", "/sys", "/tmp", "/usr", "/var",
    }
    normalized = str(path)
    if normalized in blocked:
        raise ValueError("install_root не может быть системным корнем")
    return normalized


class BootstrapRequest(BaseModel):
    """Несекретная спецификация целевой машины.

    `ssh_auth_ref` — идентификатор в центральном resolver, не путь к ключу и
    тем более не его содержимое. Provider credentials через API не принимаются.
    """

    model_config = ConfigDict(extra="forbid")

    host: str = Field(min_length=1, max_length=255)
    port: int = Field(default=22, ge=1, le=65535)
    ssh_user: str = Field(min_length=1, max_length=64)
    ssh_auth_ref: str = Field(min_length=1, max_length=255)
    expected_host_fingerprint: str
    install_root: str
    center_url: str
    display_name: str = Field(min_length=1, max_length=120)
    max_slots: int = Field(default=1, ge=1, le=64)
    providers: list[ProviderChoice] = Field(default_factory=lambda: list(ProviderChoice))
    provider_setup: Literal["preserve", "install_missing"] = "install_missing"
    release_id: str | None = None
    bundle_path: str | None = None
    bundle_sha256: str | None = None
    bootstrap_instance_id: str | None = None
    worker_id: str | None = None
    # 12C prepares the explicit transport contract but keeps every existing
    # session on polling unless the operator selects grpc_stream.
    transport_mode: Literal["polling", "grpc_stream"] = "polling"
    gateway_target: str | None = None
    protocol_versions: list[int] = Field(default_factory=lambda: [1], min_length=1)
    gateway_security_mode: Literal["test_insecure", "mtls"] = "test_insecure"

    @field_validator("host", "ssh_user", "ssh_auth_ref")
    @classmethod
    def safe_refs(cls, value: str) -> str:
        if not _SAFE_REF.fullmatch(value):
            raise ValueError("значение содержит недопустимые символы")
        return value

    @field_validator("display_name")
    @classmethod
    def safe_display_name(cls, value: str) -> str:
        if value != value.strip() or any(ord(ch) < 32 for ch in value):
            raise ValueError("display_name содержит управляющие символы")
        return value

    @field_validator("expected_host_fingerprint")
    @classmethod
    def fingerprint_format(cls, value: str) -> str:
        if not _FINGERPRINT.fullmatch(value):
            raise ValueError("ожидается OpenSSH fingerprint вида SHA256:...")
        return value

    @field_validator("install_root")
    @classmethod
    def safe_root(cls, value: str) -> str:
        return validate_install_root(value)

    @field_validator("center_url")
    @classmethod
    def https_center(cls, value: str) -> str:
        value = value.rstrip("/")
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("center_url обязан использовать HTTPS")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("center_url не может содержать credentials, query или fragment")
        return value

    @field_validator("bundle_sha256")
    @classmethod
    def sha_format(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("bundle_sha256 должен быть lowercase SHA-256")
        return value

    @model_validator(mode="after")
    def root_is_not_runtime_users_home(self) -> "BootstrapRequest":
        if self.install_root == f"/home/{self.ssh_user}":
            raise ValueError("install_root не может быть HOME SSH user")
        if self.protocol_versions != [1]:
            raise ValueError("12C поддерживает только Agent Stream protocol version 1")
        if self.transport_mode == "polling":
            if self.gateway_target is not None:
                raise ValueError("gateway_target допустим только для grpc_stream")
            if self.gateway_security_mode != "test_insecure":
                raise ValueError("gateway_security_mode применим только к grpc_stream")
            return self
        target = (self.gateway_target or "").strip()
        match = re.fullmatch(
            r"(?P<host>[A-Za-z0-9.-]+|\[[0-9A-Fa-f:]+\]):(?P<port>[0-9]{1,5})",
            target,
        )
        if not match or not 1 <= int(match.group("port")) <= 65535:
            raise ValueError("gateway_target обязан иметь вид host:port")
        host = match.group("host").strip("[]").lower()
        if self.gateway_security_mode == "test_insecure" and host not in {
            "localhost", "127.0.0.1", "::1"
        }:
            raise ValueError("test_insecure gateway_target обязан быть loopback host:port")
        if self.gateway_security_mode == "mtls" and not target:
            raise ValueError("mTLS gateway_target обязателен")
        self.gateway_target = target
        return self


class CreateBootstrapSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation: BootstrapOperation
    request: BootstrapRequest
    idempotency_key: str | None = Field(default=None, max_length=200)


class UpdateBootstrapSession(BaseModel):
    """Mutable, non-secret fields of an existing resumable session."""

    model_config = ConfigDict(extra="forbid")

    center_url: str

    @field_validator("center_url")
    @classmethod
    def https_center(cls, value: str) -> str:
        return BootstrapRequest.https_center(value)


class BootstrapSessionView(BaseModel):
    session_id: str
    operation: BootstrapOperation
    state: BootstrapState
    step: str
    request: dict
    result: dict
    error_code: str | None = None
    error_detail: str | None = None
    worker_id: str | None = None
    release_id: str | None = None
    previous_release_id: str | None = None
    created_at: float
    updated_at: float
    events: list[dict] = Field(default_factory=list)


class BootstrapActionRequired(BaseModel):
    code: Literal[
        "claude_login_required",
        "codex_login_required",
        "openrouter_secret_required",
        "provider_auth_required",
        "provider_cli_required",
        "cli_artifact_required",
    ]
    providers: list[ProviderChoice] = Field(default_factory=list)
    session_id: str
    resume_command: str


class BootstrapResult(BaseModel):
    ready: bool
    core_ready: bool
    worker_id: str | None = None
    release_id: str | None = None
    bootstrap_version: str
    policy_version: int
    no_inference: bool = True
