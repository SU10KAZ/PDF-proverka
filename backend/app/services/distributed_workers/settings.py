"""Настройки подсистемы распределённых воркеров.

Значения по умолчанию живут в core/config.py (стиль проекта), а этот модуль
перечитывает окружение на каждый вызов get_settings() — так же, как это делает
core/portal_auth.get_settings(). Причина: тестам нужно менять флаги через
monkeypatch без перезагрузки модуля config. Registration credentials здесь
намеренно нет: их выдаёт persistent one-time token store.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from backend.app.core import config


class DistributedWorkersConfigError(RuntimeError):
    """Ошибка конфигурации подсистемы (не 500, а понятное сообщение оператору)."""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_optional_id(name: str, kind: str) -> int | None:
    """Read an opt-in POSIX id without silently accepting typos.

    Most integer settings predate security-sensitive multi-service state and
    intentionally fall back to their default on malformed input.  A wrong GID
    would either lock a service out of the registry or grant state access to
    the wrong group, so this setting is deliberately fail-closed.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    value = raw.strip()
    if not value.isascii() or not value.isdecimal():
        raise DistributedWorkersConfigError(f"{name} must be a numeric POSIX {kind}")
    identifier = int(value)
    if identifier < 0:
        raise DistributedWorkersConfigError(
            f"{name} must be a non-negative POSIX {kind}"
        )
    return identifier


def _env_optional_path(name: str) -> Path | None:
    raw = os.environ.get(name, "").strip()
    return Path(raw) if raw else None


def _env_strict_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise DistributedWorkersConfigError(f"{name} must be an explicit boolean")


@dataclass(frozen=True)
class DistributedWorkersSettings:
    enabled: bool
    data_dir: Path
    heartbeat_stale_sec: int
    heartbeat_offline_sec: int
    max_package_bytes: int
    upload_chunk_bytes: int
    long_poll_sec: int
    test_job_max_sec: int
    protocol_version: int
    manifest_version: int
    allow_insecure_admin: bool = False
    # Opt-in production shared-service boundary.  2770/0660 deliberately keeps
    # POSIX ACL masks effective; an optional exact GID supports deployments
    # that use one dedicated group instead of named service-user ACL entries.
    # Disabled by default, preserving historical single-owner 0700/0600.
    shared_state_enabled: bool = False
    shared_state_owner_uid: int | None = None
    shared_state_gid: int | None = None
    shared_state_receipt_path: Path | None = None
    # Лимит частоты заявок на регистрацию. 0 в любом из порогов = этот порог
    # выключен явной настройкой (обе нули — ограничителя нет вовсе).
    registration_rate_window_sec: int = 3600
    registration_rate_max_per_instance: int = 10
    registration_rate_max_per_ip: int = 30
    identity_reenrollment_ttl_sec: int = 300
    # ─── Provider gate (этап 11) ────────────────────────────────────────────
    # Порог `low`. 0 или отрицательное = состояние `low` не вычисляется вовсе.
    # Это не «выключено по недосмотру»: §12 прямо запрещает вычислять `low`
    # без настроенного порога, и ноль здесь означает осознанный отказ.
    quota_low_threshold_pct: int = 25
    quota_stale_sec: int = 3600
    quota_history_retention_days: int = 120
    quota_history_max_rows_per_account: int = 5000
    quota_history_min_interval_sec: int = 900
    # ─── Диагностика выкатки ────────────────────────────────────────────────
    # Манифесты релизов центра и шлюза. Это ФАКТ РАЗВЁРТЫВАНИЯ, а не телеметрия
    # воркера: шлюз — отдельный процесс под другим пользователем, о себе он в
    # общую базу ничего не пишет, а угадывать его релиз по каталогам значило бы
    # показывать оператору догадку. Не задано — экран честно скажет «нет
    # данных», а не выдумает совпадение версий.
    center_release_manifest: Path | None = None
    gateway_release_manifest: Path | None = None

    # ─── Производные пути ───────────────────────────────────────────────────
    @property
    def db_path(self) -> Path:
        return self.data_dir / "workers.db"

    @property
    def source_packages_dir(self) -> Path:
        return self.data_dir / "source_packages"

    @property
    def incoming_dir(self) -> Path:
        """Staging принимаемых чанков. Вне зоны чтения остального кода."""
        return self.data_dir / "incoming"

    @property
    def result_staging_dir(self) -> Path:
        return self.data_dir / "result_staging"

    @property
    def validated_results_dir(self) -> Path:
        return self.data_dir / "validated_results"

    @property
    def rejected_results_dir(self) -> Path:
        return self.data_dir / "rejected_results"

    @property
    def superseded_results_dir(self) -> Path:
        """Результаты ОТОЗВАННЫХ попыток. Отдельный корень — не «архив ошибок».

        Такой результат может быть совершенно корректным: попытку отозвал
        оператор, а воркер честно доработал. Держать его вперемешку с
        отвергнутой валидацией значило бы стереть это различие.
        """
        return self.data_dir / "superseded_results"

    @property
    def job_logs_dir(self) -> Path:
        return self.data_dir / "job_logs"

    def require_enabled(self) -> None:
        if not self.enabled:
            raise DistributedWorkersConfigError(
                "Подсистема распределённых воркеров выключена "
                "(DISTRIBUTED_WORKERS_ENABLED=false)."
            )

def get_settings() -> DistributedWorkersSettings:
    """Снимок настроек. Env перечитывается каждый раз (тесты, hot-reload)."""
    raw_dir = os.environ.get("DISTRIBUTED_WORKERS_DATA_DIR", "").strip()
    data_dir = Path(raw_dir).resolve() if raw_dir else config.DISTRIBUTED_WORKERS_DATA_DIR
    shared_state_enabled = _env_strict_bool("DISTRIBUTED_WORKERS_SHARED_STATE")
    shared_state_owner_uid = _env_optional_id(
        "DISTRIBUTED_WORKERS_SHARED_OWNER_UID", "UID"
    )
    shared_state_gid = _env_optional_id("DISTRIBUTED_WORKERS_SHARED_GID", "GID")
    raw_receipt = os.environ.get("DISTRIBUTED_WORKERS_SHARED_RECEIPT", "").strip()
    shared_state_receipt_path = Path(raw_receipt) if raw_receipt else None
    if (shared_state_owner_uid is not None or shared_state_gid is not None) and not shared_state_enabled:
        raise DistributedWorkersConfigError(
            "DISTRIBUTED_WORKERS_SHARED_OWNER_UID/SHARED_GID require "
            "DISTRIBUTED_WORKERS_SHARED_STATE=true"
        )
    if shared_state_enabled and (
        shared_state_owner_uid is None
        or shared_state_gid is None
        or shared_state_receipt_path is None
    ):
        raise DistributedWorkersConfigError(
            "DISTRIBUTED_WORKERS_SHARED_STATE=true requires exact "
            "DISTRIBUTED_WORKERS_SHARED_OWNER_UID, DISTRIBUTED_WORKERS_SHARED_GID "
            "and DISTRIBUTED_WORKERS_SHARED_RECEIPT"
        )
    if shared_state_receipt_path is not None:
        if not shared_state_enabled:
            raise DistributedWorkersConfigError(
                "DISTRIBUTED_WORKERS_SHARED_RECEIPT requires "
                "DISTRIBUTED_WORKERS_SHARED_STATE=true"
            )
        if not shared_state_receipt_path.is_absolute():
            raise DistributedWorkersConfigError(
                "DISTRIBUTED_WORKERS_SHARED_RECEIPT must be an absolute path"
            )
    return DistributedWorkersSettings(
        enabled=_env_bool("DISTRIBUTED_WORKERS_ENABLED", config.DISTRIBUTED_WORKERS_ENABLED),
        allow_insecure_admin=_env_bool(
            "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN",
            config.DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN,
        ),
        shared_state_enabled=shared_state_enabled,
        shared_state_owner_uid=shared_state_owner_uid,
        shared_state_gid=shared_state_gid,
        shared_state_receipt_path=shared_state_receipt_path,
        data_dir=data_dir,
        heartbeat_stale_sec=_env_int(
            "DISTRIBUTED_WORKERS_HEARTBEAT_STALE_SEC",
            config.DISTRIBUTED_WORKERS_HEARTBEAT_STALE_SEC,
        ),
        heartbeat_offline_sec=_env_int(
            "DISTRIBUTED_WORKERS_HEARTBEAT_OFFLINE_SEC",
            config.DISTRIBUTED_WORKERS_HEARTBEAT_OFFLINE_SEC,
        ),
        max_package_bytes=_env_int(
            "DISTRIBUTED_WORKERS_MAX_PACKAGE_BYTES",
            config.DISTRIBUTED_WORKERS_MAX_PACKAGE_BYTES,
        ),
        upload_chunk_bytes=_env_int(
            "DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES",
            config.DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES,
        ),
        long_poll_sec=_env_int(
            "DISTRIBUTED_WORKERS_LONG_POLL_SEC", config.DISTRIBUTED_WORKERS_LONG_POLL_SEC
        ),
        test_job_max_sec=_env_int(
            "DISTRIBUTED_WORKERS_TEST_JOB_MAX_SEC",
            config.DISTRIBUTED_WORKERS_TEST_JOB_MAX_SEC,
        ),
        protocol_version=config.DISTRIBUTED_WORKERS_PROTOCOL_VERSION,
        manifest_version=config.DISTRIBUTED_WORKERS_MANIFEST_VERSION,
        registration_rate_window_sec=_env_int(
            "DISTRIBUTED_WORKERS_REGISTRATION_RATE_WINDOW_SEC",
            config.DISTRIBUTED_WORKERS_REGISTRATION_RATE_WINDOW_SEC,
        ),
        registration_rate_max_per_instance=_env_int(
            "DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE",
            config.DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE,
        ),
        registration_rate_max_per_ip=_env_int(
            "DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP",
            config.DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP,
        ),
        identity_reenrollment_ttl_sec=_env_int(
            "DISTRIBUTED_WORKERS_IDENTITY_REENROLLMENT_TTL_SEC",
            config.DISTRIBUTED_WORKERS_IDENTITY_REENROLLMENT_TTL_SEC,
        ),
        quota_low_threshold_pct=_env_int(
            "DISTRIBUTED_WORKERS_QUOTA_LOW_THRESHOLD_PCT",
            config.DISTRIBUTED_WORKERS_QUOTA_LOW_THRESHOLD_PCT,
        ),
        quota_stale_sec=_env_int(
            "DISTRIBUTED_WORKERS_QUOTA_STALE_SEC",
            config.DISTRIBUTED_WORKERS_QUOTA_STALE_SEC,
        ),
        quota_history_retention_days=_env_int(
            "DISTRIBUTED_WORKERS_QUOTA_HISTORY_RETENTION_DAYS",
            config.DISTRIBUTED_WORKERS_QUOTA_HISTORY_RETENTION_DAYS,
        ),
        quota_history_max_rows_per_account=_env_int(
            "DISTRIBUTED_WORKERS_QUOTA_HISTORY_MAX_ROWS_PER_ACCOUNT",
            config.DISTRIBUTED_WORKERS_QUOTA_HISTORY_MAX_ROWS_PER_ACCOUNT,
        ),
        quota_history_min_interval_sec=_env_int(
            "DISTRIBUTED_WORKERS_QUOTA_HISTORY_MIN_INTERVAL_SEC",
            config.DISTRIBUTED_WORKERS_QUOTA_HISTORY_MIN_INTERVAL_SEC,
        ),
        center_release_manifest=_env_optional_path("DISTRIBUTED_WORKERS_CENTER_RELEASE_MANIFEST"),
        gateway_release_manifest=_env_optional_path("DISTRIBUTED_WORKERS_GATEWAY_RELEASE_MANIFEST"),
    )
