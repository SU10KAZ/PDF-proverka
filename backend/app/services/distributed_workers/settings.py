"""Настройки подсистемы распределённых воркеров.

Значения по умолчанию живут в core/config.py (стиль проекта), а этот модуль
перечитывает окружение на каждый вызов get_settings() — так же, как это делает
core/portal_auth.get_settings(). Причина: тестам нужно менять флаги через
monkeypatch без перезагрузки модуля config, а fail-fast по bootstrap-секрету
должен срабатывать на РАБОТАЮЩЕМ процессе, а не только на импорте.
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


@dataclass(frozen=True)
class DistributedWorkersSettings:
    enabled: bool
    data_dir: Path
    bootstrap_secret: str
    heartbeat_stale_sec: int
    heartbeat_offline_sec: int
    max_package_bytes: int
    upload_chunk_bytes: int
    long_poll_sec: int
    test_job_max_sec: int
    protocol_version: int
    manifest_version: int
    allow_insecure_admin: bool = False

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
    def job_logs_dir(self) -> Path:
        return self.data_dir / "job_logs"

    def require_enabled(self) -> None:
        if not self.enabled:
            raise DistributedWorkersConfigError(
                "Подсистема распределённых воркеров выключена "
                "(DISTRIBUTED_WORKERS_ENABLED=false)."
            )

    def require_bootstrap_secret(self) -> str:
        """Секрет регистрации. Пустой при включённой подсистеме — ошибка конфигурации.

        Небезопасного значения по умолчанию быть не должно: иначе публичный
        эндпоинт регистрации примет кого угодно.
        """
        if not self.bootstrap_secret:
            raise DistributedWorkersConfigError(
                "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET не задан. "
                "При DISTRIBUTED_WORKERS_ENABLED=true секрет регистрации "
                "обязателен — значения по умолчанию нет намеренно."
            )
        if len(self.bootstrap_secret) < 16:
            raise DistributedWorkersConfigError(
                "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET короче 16 символов. "
                "Используйте длинный случайный секрет "
                "(python -c \"import secrets; print(secrets.token_urlsafe(32))\")."
            )
        return self.bootstrap_secret


def get_settings() -> DistributedWorkersSettings:
    """Снимок настроек. Env перечитывается каждый раз (тесты, hot-reload)."""
    raw_dir = os.environ.get("DISTRIBUTED_WORKERS_DATA_DIR", "").strip()
    data_dir = Path(raw_dir).resolve() if raw_dir else config.DISTRIBUTED_WORKERS_DATA_DIR
    return DistributedWorkersSettings(
        enabled=_env_bool("DISTRIBUTED_WORKERS_ENABLED", config.DISTRIBUTED_WORKERS_ENABLED),
        allow_insecure_admin=_env_bool(
            "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN",
            config.DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN,
        ),
        data_dir=data_dir,
        bootstrap_secret=os.environ.get(
            "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET",
            config.DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET,
        ).strip(),
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
    )
