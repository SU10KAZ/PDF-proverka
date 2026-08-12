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

import json
import os
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


def test_status_does_not_require_reusable_bootstrap_secret(tmp_path, monkeypatch):
    """Enabled Center has no reusable shared-bootstrap configuration gate."""
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
    assert "BOOTSTRAP_SECRET" not in (body["config_error"] or "")


# ─── Существующий конвейер не затронут ───────────────────────────────────────
def test_pipeline_manager_knows_nothing_about_the_worker_subsystem():
    """Граница врезки: менеджер знает про `pipeline.execution` и больше ни про что.

    До этапа ExecutionBackend проверка была строже — «manager.py не должен
    упоминать ExecutionBackend вовсе», потому что врезки не существовало. Теперь
    она есть, и запрет переехал туда, где он по-прежнему содержателен:

      * менеджер НЕ импортирует подсистему воркеров напрямую — иначе
        локальный конвейер начал бы зависеть от её включённости;
      * менеджер НЕ импортирует пакет `audit_worker` — это код чужого VPS;
      * менеджер не строит команд и argv для удалённого исполнения.

    Слово «ExecutionBackend» в комментарии точки врезки допустимо и полезно:
    именно оно объясняет, почему вызов идёт через `_execute_item`.
    """
    source = (_ROOT / "backend/app/pipeline/manager.py").read_text(encoding="utf-8")
    for marker in (
        "distributed_workers",
        "audit_worker",
        "DISTRIBUTED_WORKERS",
        "RemoteWorkerExecutionBackend",
    ):
        assert marker not in source, f"manager.py не должен знать о {marker}"
    # Врезка существует и идёт через абстракцию, а не через прямой вызов.
    assert "_execute_item" in source
    assert "backend.app.pipeline.execution" in source


def test_no_llm_invocation_in_worker_package():
    """В пакете воркера нет обращений ни к Claude Code, ни к Codex.

    Ищем имя бинаря в ЛЮБОМ строковом литерале, а не только вплотную к
    кавычке: путь вида "/usr/local/bin/claude" прежний шаблон пропускал.
    """
    import ast

    banned = {"claude", "codex", "claude-code", "anthropic", "openrouter"}
    offenders = []
    for path in sorted((_ROOT / "audit_worker").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = {
            id(ast.get_docstring(n, clean=False))
            for n in ast.walk(tree)
            if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef))
        }
        literals = [
            n for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
            and id(n.value) not in docstrings
        ]
        for node in literals:
            text = node.value.strip().lower()
            # Только исполняемые формы: имя бинаря целиком или хвост пути к нему.
            candidate = text.rsplit("/", 1)[-1] if "/" in text else text
            if candidate in banned:
                offenders.append(f"{path.name}:{node.lineno} {node.value[:80]}")
    assert not offenders, offenders


def test_provider_layer_is_the_only_place_naming_real_clis():
    """Граница после этапа 11: имена настоящих CLI живут ТОЛЬКО в providers/.

    Инвариант проверки выше не отменён, а уточнён. Он защищал одно: путь
    КОНВЕЙЕРА не должен уметь дотянуться до настоящего Claude/Codex, иначе
    воркер, рапортующий `provider_mode="fake"`, тихо позвал бы настоящую
    модель. С этапа 11 появился ВТОРОЙ путь — наблюдение за провайдерами, — и
    он обязан называть CLI по имени, потому что именно их и опрашивает.

    Разделение сохраняет исходную защиту:
      * модули верхнего уровня `audit_worker/*.py` (через них идёт конвейер)
        по-прежнему не содержат исполняемых литералов настоящих CLI;
      * пакет `audit_worker/providers/` их содержит, но НЕ импортируется ни
        из `audit_runner`, ни из пути выполнения задания — это и проверяется
        здесь. Пока импорта нет, `enforce_fake_providers` обойти нечем.
    """
    import ast

    provider_dir = _ROOT / "audit_worker" / "providers"
    assert provider_dir.is_dir(), "пакет providers/ должен существовать"

    # Ни один модуль, участвующий в ВЫПОЛНЕНИИ задания, не тянет providers/.
    pipeline_modules = ("audit_runner.py", "test_runner.py", "test_process.py")
    importers: list[str] = []
    for name in pipeline_modules:
        path = _ROOT / "audit_worker" / name
        if not path.is_file():
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "audit_worker.providers"
            ):
                importers.append(f"{name}:{node.lineno}")
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("audit_worker.providers"):
                        importers.append(f"{name}:{node.lineno}")
    assert not importers, (
        "путь выполнения задания импортирует провайдерский слой — "
        f"изоляция поддельных провайдеров под угрозой: {importers}"
    )


def test_only_one_subprocess_spawn_point():
    """Порождение процесса — ровно одно место и без shell.

    Проверяем ДЕРЕВО РАЗБОРА, а не текст: переименование переменной или
    перенос строки тест не ломают, а вторая точка запуска (Popen, run, call,
    check_output, os.system, os.popen, exec*) — ломает.
    """
    import ast

    spawners = {
        "Popen", "run", "call", "check_call", "check_output",
        "system", "popen", "execv", "execve", "execvp", "spawnv",
    }
    found: list[str] = []
    for path in sorted((_ROOT / "audit_worker").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            module = getattr(getattr(func, "value", None), "id", None)
            if name in spawners and module in ("subprocess", "os"):
                found.append(f"{path.name}:{node.lineno} {module}.{name}")
                for kw in node.keywords:
                    if kw.arg == "shell":
                        assert isinstance(kw.value, ast.Constant) and kw.value.value is False, (
                            f"shell=True в {path.name}:{node.lineno}"
                        )
    # Точек ровно три, и все известны поимённо:
    #   test_runner.py  — запуск ТЕСТОВОГО процесса;
    #   audit_runner.py — запуск РЕАЛЬНОГО аудита (этап ExecutionBackend);
    #   __main__.py     — dev-режим `run`, поднимающий собственного исполнителя
    #                     фиксированным argv `python -m audit_worker executor`.
    # Третья добавлена осознанно: реальный аудит не мог использовать точку
    # тестового процесса — у него другой argv и другое окружение. Гарантия при
    # этом сохранена: обе точки строят argv САМИ, из констант своего модуля.
    files = sorted({entry.split(":", 1)[0] for entry in found})
    assert files == ["__main__.py", "audit_runner.py", "test_runner.py"], found
    assert sum(1 for e in found if e.startswith("test_runner.py")) == 1, found
    assert sum(1 for e in found if e.startswith("audit_runner.py")) == 1, found

    # Самозапуск исполнителя не должен уметь принимать чужой argv: элементы
    # списка — только sys.executable и строковые литералы (I-10).
    main_tree = ast.parse((_ROOT / "audit_worker" / "__main__.py").read_text("utf-8"))
    spawn_calls = [
        node for node in ast.walk(main_tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "attr", None) == "Popen"
    ]
    assert len(spawn_calls) == 1, "в __main__ должен быть один самозапуск"
    argv = spawn_calls[0].args[0]
    assert isinstance(argv, ast.List), "argv обязан быть литеральным списком"
    literals = [e.value for e in argv.elts if isinstance(e, ast.Constant)]
    assert "-m" in literals and "audit_worker" in literals and "executor" in literals


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


# ─── Проверка НАСТОЯЩЕГО backend/app/main.py ─────────────────────────────────
# Тесты выше работают с самодельной сборкой приложения из helpers — она по
# построению не содержит спорных маршрутов, поэтому их ассерты «404» не могут
# упасть и решение в main.py не проверяют. Ниже — проверка самого main.py:
# он импортируется в отдельном процессе, потому что читает флаг НА ИМПОРТЕ и
# кэшируется в sys.modules на весь прогон pytest.
_ROUTE_PROBE = r'''
import json, os, sys
sys.path.insert(0, %(root)r)
from backend.app.main import app
paths = sorted({getattr(r, "path", "") for r in app.routes})
print(json.dumps({
    "worker_api": [p for p in paths if p.startswith("/api/v1/worker")],
    "admin_api": [p for p in paths if p.startswith("/api/workers")],
    "page": [p for p in paths if p == "/audit-workers"],
    "total": len(paths),
}, ensure_ascii=False))
'''


def _probe_main(env: dict) -> dict:
    """Импортировать реальный main.py в отдельном процессе и вернуть маршруты."""
    import subprocess

    root = str(_ROOT)
    child_env = {**os.environ, **env}
    child_env.pop("PYTEST_CURRENT_TEST", None)
    result = subprocess.run(
        [sys.executable, "-c", _ROUTE_PROBE % {"root": root}],
        capture_output=True, text=True, env=child_env, cwd=root, timeout=180,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_real_main_registers_nothing_when_flag_off(tmp_path):
    """При выключенном флаге в НАСТОЯЩЕМ приложении нет ни одной ручки воркеров."""
    routes = _probe_main({
        "DISTRIBUTED_WORKERS_ENABLED": "false",
        "DISTRIBUTED_WORKERS_DATA_DIR": str(tmp_path / "off"),
    })
    assert routes["worker_api"] == []
    # Исключения — статус и «кто я». Оба обязаны отвечать всегда: без них экран
    # не может ни сказать «функция отключена», ни честно показать «прав нет».
    # Ни один из них ничего не меняет и ничего чужого не раскрывает.
    assert sorted(routes["admin_api"]) == ["/api/workers/me", "/api/workers/status"]
    assert routes["page"] == ["/audit-workers"]
    assert routes["total"] > 100          # остальное приложение на месте
    assert not (tmp_path / "off").exists()


def test_real_main_registers_both_contours_when_flag_on(tmp_path):
    routes = _probe_main({
        "DISTRIBUTED_WORKERS_ENABLED": "true",
        "DISTRIBUTED_WORKERS_DATA_DIR": str(tmp_path / "on"),
        "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": "x" * 32,
        "PORTAL_AUTH_ENABLED": "true",
    })
    assert "/api/v1/worker/register" in routes["worker_api"]
    assert "/api/v1/worker/claim" in routes["worker_api"]
    assert "/api/workers" in routes["admin_api"]
    assert len(routes["worker_api"]) >= 15


def test_admin_contour_not_exposed_without_portal_auth(tmp_path):
    """Операторский API не поднимается, если портальная защита выключена.

    У него нет собственной аутентификации, а rotate-token отдаёт живой токен
    воркера открытым текстом.
    """
    routes = _probe_main({
        "DISTRIBUTED_WORKERS_ENABLED": "true",
        "DISTRIBUTED_WORKERS_DATA_DIR": str(tmp_path / "insecure"),
        "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": "x" * 32,
        "PORTAL_AUTH_ENABLED": "false",
        "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN": "false",
    })
    assert sorted(routes["admin_api"]) == ["/api/workers/me", "/api/workers/status"]
    assert "/api/workers/jobs" not in routes["admin_api"]
    # Контур воркеров при этом работает: у него своя аутентификация по токену.
    assert "/api/v1/worker/register" in routes["worker_api"]


def test_admin_contour_available_with_explicit_dev_optin(tmp_path):
    routes = _probe_main({
        "DISTRIBUTED_WORKERS_ENABLED": "true",
        "DISTRIBUTED_WORKERS_DATA_DIR": str(tmp_path / "dev"),
        "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": "x" * 32,
        "PORTAL_AUTH_ENABLED": "false",
        "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN": "true",
    })
    assert "/api/workers" in routes["admin_api"]
