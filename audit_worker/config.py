"""Конфигурация агента: только env + файлы состояния, никаких зависимостей от backend."""
from __future__ import annotations

import os
import platform
import sys
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from audit_worker import PROTOCOL_VERSION, __version__

DEFAULT_ROOT = "/var/lib/audit-worker"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass
class WorkerConfig:
    dispatcher_url: str
    root: Path
    display_name: str
    heartbeat_interval_sec: float = 30.0
    poll_wait_sec: int = 25
    event_flush_interval_sec: float = 1.0
    event_batch_max: int = 500
    max_slots: int = 1
    request_timeout_sec: float = 60.0
    # Потолки тестового задания. Воркер зажимает параметры ПОВТОРНО, даже если
    # центр прислал что-то большее — доверять входу нельзя (§4 задания).
    test_max_total_sec: float = 300.0
    test_max_steps: int = 100
    test_max_result_bytes: int = 8 * 1024 * 1024
    verify_tls: bool = True
    # Dev-режим: разрешает http:// ТОЛЬКО для localhost. Глобального
    # отключения проверки TLS не существует — см. validate_transport_security.
    allow_insecure_localhost: bool = False
    extra_capabilities: dict = field(default_factory=dict)
    # ─── Хранение локальных данных (этап 3.5) ───────────────────────────────
    # Сухой прогон по умолчанию: менеджер считает кандидатов и показывает
    # ожидаемый выигрыш, но НИЧЕГО не стирает, пока удаление не включено явно.
    retention_enabled: bool = True
    retention_delete_enabled: bool = False
    retention_days: int = 30
    retention_scan_interval_sec: int = 3600
    disk_warning_free_bytes: int = 5 * 1024 * 1024 * 1024
    disk_critical_free_bytes: int = 1 * 1024 * 1024 * 1024
    # Автозапуск локального исполнителя вместе с агентом. Только для dev:
    # в проде это два systemd-юнита, и рестарт агента не трогает исполнителя.
    dev_spawn_executor: bool = False
    # ─── Реальный аудит (этап ExecutionBackend) ─────────────────────────────
    # Отпечаток кода конвейера на этом VPS. Центр сверяет его со своим и не
    # показывает несовместимый воркер как доступный: разные ревизии дают
    # разные артефакты при одинаковых входных данных.
    pipeline_revision: str | None = None
    # Каталог с установленным кодом платформы (корень репозитория). Из него
    # запускается фиксированный internal runner. Задаётся АДМИНИСТРАТОРОМ VPS,
    # не центром: путь к исполняемому коду не может приходить из задания.
    pipeline_root: Path | None = None
    # Приём заданий типа audit_pipeline_v1. Включение подсистемы воркеров этого
    # НЕ включает: реальный аудит требует отдельного осознанного решения.
    audit_pipeline_enabled: bool = False
    # Разрешение запускать НАСТОЯЩИЕ Claude/Codex. По умолчанию запрещено, и
    # это независимо от audit_pipeline_enabled: тестовый прогон реального
    # конвейера не должен тратить подписку.
    allow_real_llm: bool = False
    # Каталог поддельных CLI-провайдеров. Задаётся конфигурацией ВОРКЕРА и
    # никогда полем задания (§17 задания).
    fake_provider_dir: Path | None = None
    # Жёсткий предел одновременных audit_pipeline_v1. Доказанный максимум — 1.
    real_audit_max_slots: int = 1
    # Подмена сетевого слоя httpx. Только для end-to-end тестов (ASGITransport):
    # настоящий агент против настоящего приложения без сокетов. В проде None.
    transport: object | None = None

    @property
    def state_path(self) -> Path:
        return self.root / "worker_state.json"

    @property
    def token_path(self) -> Path:
        return self.root / "token"

    @property
    def jobs_dir(self) -> Path:
        return self.root / "jobs"

    @property
    def runtime_dir(self) -> Path:
        return self.root / "runtime"

    @property
    def local_db_path(self) -> Path:
        """worker.db — общий транзакционный стык агента и исполнителя."""
        return self.root / "worker.db"

    @property
    def trash_dir(self) -> Path:
        """Локальная корзина: сюда каталог переезжает ДО стирания содержимого."""
        return self.root / "trash"

    @property
    def tombstones_dir(self) -> Path:
        """Следы удалённых попыток: hash и сроки остаются, данных нет."""
        return self.runtime_dir / "tombstones"

    def job_dir(self, job_id: str, attempt_id: str) -> Path:
        """Путь строится ТОЛЬКО из UUID (I-11): внешний код проекта сюда не попадает."""
        from audit_worker.paths import attempt_dir

        return attempt_dir(self.jobs_dir, job_id, attempt_id)

    def ensure_dirs(self) -> None:
        for path in (
            self.root, self.jobs_dir, self.runtime_dir, self.trash_dir,
            self.tombstones_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.root, 0o750)
        except OSError:
            pass

    def capabilities(self) -> dict:
        from audit_worker import slots as _slots

        job_types = ["test_pipeline_v1"]
        if self.audit_pipeline_enabled:
            job_types.append("audit_pipeline_v1")
        caps = {
            # Что воркер УМЕЕТ запускать. В тестовом режиме это поддельные CLI:
            # центр обязан видеть разницу, иначе «аудит прошёл» ничего не значит.
            "providers": (
                ["claude_cli", "codex_cli"] if self.allow_real_llm
                else ["fake_claude_cli", "fake_codex_cli"]
            ),
            "provider_mode": "real" if self.allow_real_llm else "fake",
            "real_llm_enabled": self.allow_real_llm,
            "audit_pipeline_enabled": self.audit_pipeline_enabled,
            "real_audit_max_slots": self.real_audit_max_slots,
            "pipeline_revision": self.pipeline_revision,
            "compressions": ["gzip", "none"],
            "job_types": job_types,
            # Сколько одновременных попыток ПРОВЕРЕНО этой сборкой воркера.
            # Не «сколько хочет оператор» — центр берёт минимум из обоих.
            "max_verified_slots": _slots.MAX_VERIFIED_SLOTS,
            "python": platform.python_version(),
            "os": f"{platform.system()} {platform.release()}",
            "cores": os.cpu_count() or 1,
            "max_package_bytes": 2 * 1024 * 1024 * 1024,
            "worker_package": __version__,
        }
        caps.update(self.extra_capabilities)
        return caps


def _max_slots_from_env() -> int:
    """Число слотов из окружения, зажатое доказанным максимумом этапа.

    Предупреждение печатается: молчаливое «5 → 2» оставило бы оператора в
    уверенности, что у него пять слотов, и он списал бы простой на что угодно,
    кроме настройки.
    """
    from audit_worker import slots as _slots

    limit = _slots.normalize_max_slots(os.environ.get("AUDIT_WORKER_MAX_SLOTS"))
    if limit.notice:
        print(f"[audit-worker] ВНИМАНИЕ: {limit.notice}", file=sys.stderr)
    return limit.value


def load_config(
    argv_root: str | None = None, *, require_dispatcher: bool = True
) -> WorkerConfig:
    """Собрать конфигурацию.

    `require_dispatcher=False` — для ИСПОЛНИТЕЛЯ: он к центру не ходит, и
    требовать от него адрес центра значило бы намекать, что когда-нибудь
    сходит. Проверка транспорта в этом случае тоже не выполняется.
    """
    root = Path(
        argv_root or os.environ.get("AUDIT_WORKER_ROOT") or DEFAULT_ROOT
    ).expanduser().resolve()
    url = os.environ.get("AUDIT_WORKER_DISPATCHER_URL", "").strip().rstrip("/")
    if not url and require_dispatcher:
        raise SystemExit(
            "AUDIT_WORKER_DISPATCHER_URL не задан. Пример:\n"
            "  export AUDIT_WORKER_DISPATCHER_URL=https://auditmanager.app"
        )
    config = WorkerConfig(
        dispatcher_url=url,
        root=root,
        display_name=os.environ.get("AUDIT_WORKER_NAME", "").strip()
        or f"{platform.node()}",
        heartbeat_interval_sec=_env_float("AUDIT_WORKER_HEARTBEAT_SEC", 30.0),
        poll_wait_sec=_env_int("AUDIT_WORKER_POLL_WAIT_SEC", 25),
        max_slots=_max_slots_from_env(),
        request_timeout_sec=_env_float("AUDIT_WORKER_TIMEOUT_SEC", 60.0),
        test_max_total_sec=_env_float("AUDIT_WORKER_TEST_MAX_SEC", 300.0),
        retention_enabled=_env_bool("AUDIT_WORKER_RETENTION_ENABLED", True),
        # По умолчанию ВЫКЛЮЧЕНО. Включать отдельно и осознанно: неверное
        # правило удаления стоит дороже, чем занятое место.
        retention_delete_enabled=_env_bool(
            "AUDIT_WORKER_RETENTION_DELETE_ENABLED", False
        ),
        retention_days=max(1, _env_int("AUDIT_WORKER_RETENTION_DAYS", 30)),
        retention_scan_interval_sec=max(
            60, _env_int("AUDIT_WORKER_RETENTION_SCAN_INTERVAL_SEC", 3600)
        ),
        disk_warning_free_bytes=_env_int(
            "AUDIT_WORKER_DISK_WARNING_FREE_BYTES", 5 * 1024 * 1024 * 1024
        ),
        disk_critical_free_bytes=_env_int(
            "AUDIT_WORKER_DISK_CRITICAL_FREE_BYTES", 1 * 1024 * 1024 * 1024
        ),
        dev_spawn_executor=_env_bool("AUDIT_WORKER_DEV_SPAWN_EXECUTOR", False),
        pipeline_revision=(
            os.environ.get("AUDIT_WORKER_PIPELINE_REVISION", "").strip() or None
        ),
        pipeline_root=(
            Path(os.environ["AUDIT_WORKER_PIPELINE_ROOT"]).expanduser().resolve()
            if os.environ.get("AUDIT_WORKER_PIPELINE_ROOT", "").strip()
            else None
        ),
        audit_pipeline_enabled=_env_bool("AUDIT_WORKER_AUDIT_PIPELINE_ENABLED", False),
        allow_real_llm=_env_bool("AUDIT_WORKER_ALLOW_REAL_LLM", False),
        fake_provider_dir=(
            Path(os.environ["AUDIT_WORKER_FAKE_PROVIDER_DIR"]).expanduser().resolve()
            if os.environ.get("AUDIT_WORKER_FAKE_PROVIDER_DIR", "").strip()
            else None
        ),
        # Единица — не настройка, а доказанный предел этапа. Значение больше
        # зажимается: заявлять больше без прогона нельзя.
        real_audit_max_slots=min(
            1, max(0, _env_int("AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS", 1))
        ),
        # verify_tls намеренно НЕ управляется переменной окружения: глобальный
        # verify=false — это тихое отключение защиты канала. Единственная
        # послабляющая настройка — dev-флаг для localhost (проверяется отдельно).
        verify_tls=True,
        allow_insecure_localhost=os.environ.get(
            "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST", "false"
        ).lower() in {"1", "true", "yes", "on"},
    )
    if require_dispatcher:
        validate_transport_security(config)
    return config


class InsecureTransportError(SystemExit):
    """Небезопасный транспорт: агент не стартует, а не работает молча по HTTP."""


LOCALHOST_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}


def validate_transport_security(config: "WorkerConfig") -> None:
    """Прод обязан быть HTTPS. HTTP допустим только к localhost и только с флагом.

    Ошибка отображается явным текстом при старте, а не превращается в тихую
    работу открытым текстом по сети.
    """
    parsed = urlparse(config.dispatcher_url)
    scheme = (parsed.scheme or "").lower()
    host = (parsed.hostname or "").lower()

    if scheme == "https":
        return
    if scheme != "http":
        raise InsecureTransportError(
            f"AUDIT_WORKER_DISPATCHER_URL: ожидается https:// (получено {scheme or 'без схемы'}://)"
        )
    if host not in LOCALHOST_HOSTS:
        raise InsecureTransportError(
            f"AUDIT_WORKER_DISPATCHER_URL={config.dispatcher_url}: HTTP запрещён для "
            f"внешнего хоста {host!r}. Используйте https://. "
            "HTTP допустим только к localhost и только с "
            "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST=true."
        )
    if not config.allow_insecure_localhost:
        raise InsecureTransportError(
            "HTTP к localhost требует явного AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST=true "
            "(dev-режим). В проде используйте https://."
        )


def python_executable() -> str:
    return sys.executable or "python3"
