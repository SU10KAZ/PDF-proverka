"""Конфигурация агента: только env + файлы состояния, никаких зависимостей от backend."""
from __future__ import annotations

import os
import platform
import sys
from dataclasses import dataclass, field
from pathlib import Path

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
    extra_capabilities: dict = field(default_factory=dict)
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

    def job_dir(self, job_id: str, attempt_id: str) -> Path:
        return self.jobs_dir / job_id / attempt_id

    def ensure_dirs(self) -> None:
        for path in (self.root, self.jobs_dir, self.runtime_dir):
            path.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.root, 0o750)
        except OSError:
            pass

    def capabilities(self) -> dict:
        caps = {
            "providers": [],           # этап 0: LLM не подключены намеренно
            "compressions": ["gzip", "none"],
            "job_types": ["test_pipeline_v1"],
            "python": platform.python_version(),
            "os": f"{platform.system()} {platform.release()}",
            "cores": os.cpu_count() or 1,
            "max_package_bytes": 2 * 1024 * 1024 * 1024,
            "worker_package": __version__,
        }
        caps.update(self.extra_capabilities)
        return caps


def load_config(argv_root: str | None = None) -> WorkerConfig:
    root = Path(
        argv_root or os.environ.get("AUDIT_WORKER_ROOT") or DEFAULT_ROOT
    ).expanduser().resolve()
    url = os.environ.get("AUDIT_WORKER_DISPATCHER_URL", "").strip().rstrip("/")
    if not url:
        raise SystemExit(
            "AUDIT_WORKER_DISPATCHER_URL не задан. Пример:\n"
            "  export AUDIT_WORKER_DISPATCHER_URL=https://auditmanager.app"
        )
    return WorkerConfig(
        dispatcher_url=url,
        root=root,
        display_name=os.environ.get("AUDIT_WORKER_NAME", "").strip()
        or f"{platform.node()}",
        heartbeat_interval_sec=_env_float("AUDIT_WORKER_HEARTBEAT_SEC", 30.0),
        poll_wait_sec=_env_int("AUDIT_WORKER_POLL_WAIT_SEC", 25),
        max_slots=max(1, min(5, _env_int("AUDIT_WORKER_MAX_SLOTS", 1))),
        request_timeout_sec=_env_float("AUDIT_WORKER_TIMEOUT_SEC", 60.0),
        test_max_total_sec=_env_float("AUDIT_WORKER_TEST_MAX_SEC", 300.0),
        verify_tls=os.environ.get("AUDIT_WORKER_VERIFY_TLS", "true").lower()
        not in {"0", "false", "no", "off"},
    )


def python_executable() -> str:
    return sys.executable or "python3"
