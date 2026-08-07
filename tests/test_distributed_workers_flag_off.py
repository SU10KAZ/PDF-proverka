"""
test_distributed_workers_flag_off.py
------------------------------------
Регресс: при DISTRIBUTED_WORKERS_ENABLED=false (значение по умолчанию)
существующая платформа работает как раньше.

Проверяется буквально то, что обещано в §5 задания:
  * роутеры воркеров не отвечают (404);
  * SQLite-база НЕ создаётся на диске;
  * фоновых задач не запускается;
  * экран отдаёт признак «функция отключена», а не пустоту;
  * существующий локальный аудит не затронут — точек врезки в PipelineManager
    на этом этапе нет вовсе (проверяется грепом по исходникам).

Run: python -m pytest tests/test_distributed_workers_flag_off.py -v
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def test_default_is_off():
    """Флаг по умолчанию выключен — включение всегда осознанное."""
    import importlib

    from backend.app.core import config

    importlib.reload(config)
    assert config.DISTRIBUTED_WORKERS_ENABLED is False


def test_no_database_created_when_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "false")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "off"))

    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import (
        DistributedWorkersConfigError,
        get_settings,
    )

    database.reset_state_for_tests()
    settings = get_settings()
    assert settings.enabled is False
    with pytest.raises(DistributedWorkersConfigError):
        database.ensure_ready(settings)
    assert not settings.db_path.exists()
    assert not settings.data_dir.exists()


def test_worker_api_absent_when_disabled(tmp_path, monkeypatch):
    """Роутер воркеров не регистрируется — путей нет вовсе."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "false")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "off"))
    httpx = pytest.importorskip("httpx")

    from tests.distributed_workers_helpers import (
        SyncASGITransport,
        make_disabled_center_app,
    )

    client = httpx.Client(
        transport=SyncASGITransport(make_disabled_center_app()), base_url="http://center"
    )
    # Ни воркерского контура, ни операторского — маршрутов нет вовсе.
    assert client.post("/api/v1/worker/heartbeat", json={}).status_code == 404
    assert client.post("/api/v1/worker/register", json={}).status_code == 404
    assert client.get("/api/workers").status_code == 404
    assert client.post("/api/workers/jobs", json={}).status_code == 404
    assert client.get("/api/workers/jobs/list").status_code == 404


def test_status_endpoint_reports_disabled(tmp_path, monkeypatch):
    """Фронт должен честно показать «отключено», а не пустой экран."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "false")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "off"))
    httpx = pytest.importorskip("httpx")

    from tests.distributed_workers_helpers import (
        SyncASGITransport,
        make_disabled_center_app,
    )

    client = httpx.Client(
        transport=SyncASGITransport(make_disabled_center_app()), base_url="http://center"
    )
    response = client.get("/api/workers/status")
    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is False
    assert "DISTRIBUTED_WORKERS_ENABLED" in body["reason"]


def test_status_reports_config_error_when_secret_missing(tmp_path, monkeypatch):
    """Включено, но секрета нет → понятная ошибка конфигурации, а не дефолт."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "on"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", "")
    httpx = pytest.importorskip("httpx")

    from tests.distributed_workers_helpers import SyncASGITransport, make_center_app

    client = httpx.Client(
        transport=SyncASGITransport(make_center_app()), base_url="http://center"
    )
    body = client.get("/api/workers/status").json()
    assert body["enabled"] is True
    assert body["config_error"]
    assert "BOOTSTRAP_SECRET" in body["config_error"]


# ─── Существующий конвейер не затронут ───────────────────────────────────────
def test_pipeline_manager_untouched():
    """Точек врезки в PipelineManager на этапе 0 быть не должно.

    Задача §3.2 прямо запрещает трогать _dispatch_action, _batch_slot_worker,
    cleanup_zombies и resume. Проверяем грепом, а не на слово.
    """
    source = (_ROOT / "backend/app/pipeline/manager.py").read_text(encoding="utf-8")
    for marker in (
        "distributed_workers",
        "audit_worker",
        "ExecutionBackend",
        "RemoteWorkerExecutionBackend",
        "DISTRIBUTED_WORKERS",
    ):
        assert marker not in source, f"manager.py не должен знать о {marker}"


def test_no_llm_invocation_in_worker_package():
    """Ни Claude Code, ни Codex на этом этапе не запускаются.

    Проверяется по существу, а не по вхождению слова: имя бинаря не должно
    встречаться в строковом литерале, из которого может собраться argv, и
    ни один модуль LLM-раннеров не импортируется.
    """
    package = _ROOT / "audit_worker"
    binary_literal = re.compile(r"""["'](claude|codex)(\s|["'])""", re.IGNORECASE)
    runner_import = re.compile(r"(claude_runner|codex_runner|anthropic|openai)")
    offenders = []
    for path in package.rglob("*.py"):
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if binary_literal.search(line) or runner_import.search(line):
                offenders.append(f"{path.name}:{line_no}: {line.strip()[:80]}")
    assert not offenders, "агент не должен запускать LLM:\n" + "\n".join(offenders)


def test_only_one_subprocess_spawn_point():
    """Единственная точка порождения процессов — фиксированный argv тест-раннера."""
    package = _ROOT / "audit_worker"
    spawners = []
    for path in package.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "subprocess.Popen(" in text or "subprocess.run(" in text:
            spawners.append(path.name)
    assert spawners == ["test_runner.py"], spawners

    source = (package / "test_runner.py").read_text(encoding="utf-8")
    assert "argv = build_argv(params_path)" in source
    assert "subprocess.Popen(  # noqa: S603" in source
    assert "shell=False" in source


def test_no_arbitrary_command_execution_in_agent():
    """Ни одной ветки, где команда/argv приходят из задания."""
    package = _ROOT / "audit_worker"
    banned = ("shell=True", "os.system(", "eval(", "exec(", "subprocess.run(cmd")
    offenders = []
    for path in package.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for marker in banned:
            if marker in text:
                offenders.append(f"{path.name}: {marker}")
    assert not offenders, "опасные конструкции в агенте:\n" + "\n".join(offenders)


def test_agent_does_not_import_backend():
    """Агент самодостаточен: ставится на голый VPS без кода платформы."""
    package = _ROOT / "audit_worker"
    offenders = []
    for path in package.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if re.search(r"^\s*(from|import)\s+backend", text, re.MULTILINE):
            offenders.append(path.name)
    assert not offenders, f"агент импортирует backend: {offenders}"


def test_portal_auth_exempts_only_worker_prefix():
    """Исключение из портальной авторизации — ровно один префикс."""
    from backend.app.core import portal_auth

    assert portal_auth.EXEMPT_PREFIXES == ("/api/v1/worker/",)
    assert portal_auth.is_path_exempt("/api/v1/worker/heartbeat") is True
    # Операторский контур остаётся под портальной авторизацией.
    assert portal_auth.is_path_exempt("/api/workers") is False
    assert portal_auth.is_path_exempt("/api/workers/jobs") is False
    assert portal_auth.is_path_exempt("/api/projects") is False
