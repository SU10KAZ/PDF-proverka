"""Backend test harness isolation from production storage cutover flags."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

# Журнал действий — в песочницу процесса (см. tests/conftest.py): базовое
# значение config.ACTION_LOG_DIR не должно указывать на прод logs/actions.
_ACTION_LOG_SANDBOX = tempfile.TemporaryDirectory(prefix="pdf-proverka-pytest-actionlog-")
os.environ.setdefault(
    "AUDIT_ACTION_LOG_DIR", str(Path(_ACTION_LOG_SANDBOX.name) / "actions_log")
)

_DEFAULT_STORAGE_ENV = {
    "AUDIT_STORAGE_BACKEND": "legacy",
    "AUDIT_PROJECTS_V2_WRITE_MODE": "dual_write_shadow",
    "AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED": "false",
}
for _name, _value in _DEFAULT_STORAGE_ENV.items():
    os.environ[_name] = _value


@pytest.fixture(autouse=True)
def _isolate_storage_cutover_env(monkeypatch):
    """Every backend test starts from legacy storage unless it opts into v2."""
    for name, value in _DEFAULT_STORAGE_ENV.items():
        monkeypatch.setenv(name, value)


@pytest.fixture(autouse=True)
def _isolate_action_log(tmp_path, monkeypatch):
    """Никакой backend-тест не пишет в живой logs/actions/ (журнал действий)."""
    try:
        from backend.app.core import config as _cfg
    except Exception:
        return
    monkeypatch.setattr(
        _cfg, "ACTION_LOG_DIR", tmp_path / "actions_log", raising=False
    )


@pytest.fixture(autouse=True)
def _isolate_schedule_completion_file(tmp_path, monkeypatch):
    """Никакой backend-тест не пишет в живой knowledge_base/schedule_completion.json.

    save_expert_review штампует «день завершения» проекта через
    schedule_service.set_completion_once — изолируем стор графика в per-test tmp.
    """
    try:
        import backend.app.services.common.schedule_service as _sched
    except Exception:
        return
    monkeypatch.setattr(
        _sched, "SCHEDULE_COMPLETION_FILE",
        tmp_path / "schedule_completion.json", raising=False,
    )
