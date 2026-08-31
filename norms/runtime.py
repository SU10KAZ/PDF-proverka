"""Единый контракт расположения и здоровья нормативного runtime.

Код нормативных инструментов принадлежит релизу, а тяжёлое окружение и данные
живут отдельно. В development это по-прежнему ``norms/tools`` репозитория. В
иммутабельном release-layout ``.../releases/<id>/app`` автоматически выбирается
``.../shared/norms/tools``. Явный ``NORMS_TOOLS_PATH`` имеет высший приоритет.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Iterable

NORMS_TOOLS_ENV = "NORMS_TOOLS_PATH"
NORMS_STATUS_INDEX_ENV = "NORMS_STATUS_INDEX_PATH"
NORMS_VAULT_ENV = "NORMS_VAULT_PATH"
NORMS_MCP_PYTHON_ENV = "NORMS_MCP_PYTHON"

class NormsRuntimeUnavailableError(RuntimeError):
    """Нормативная база не готова к authoritative-проверке."""


def release_code_tools_path() -> Path:
    """Каталог кода tools, принадлежащий текущему checkout/release."""
    return Path(__file__).resolve().parent / "tools"


def default_runtime_tools_path(*, module_file: str | Path | None = None) -> Path:
    """Дефолт runtime для development и иммутабельного центра.

    Проверка имени ``releases`` описывает установленный layout, а не конкретный
    домашний каталог. Поэтому одинаково работают ``/home/coder/auditmanager`` и
    системная установка ``/opt/auditmanager``.
    """
    norms_dir = (
        Path(module_file).resolve().parent
        if module_file is not None
        else Path(__file__).resolve().parent
    )
    for parent in norms_dir.parents:
        if parent.name == "releases":
            return parent.parent / "shared" / "norms" / "tools"
    return norms_dir / "tools"


def configured_runtime_tools_path() -> Path:
    raw = (os.environ.get(NORMS_TOOLS_ENV) or "").strip()
    return Path(raw).expanduser() if raw else default_runtime_tools_path()


def configured_status_index_path(tools_path: Path | None = None) -> Path:
    raw = (os.environ.get(NORMS_STATUS_INDEX_ENV) or "").strip()
    tools = tools_path or configured_runtime_tools_path()
    return Path(raw).expanduser() if raw else tools / "status_index.json"


def configured_vault_path(tools_path: Path | None = None) -> Path:
    raw = (os.environ.get(NORMS_VAULT_ENV) or "").strip()
    tools = tools_path or configured_runtime_tools_path()
    return Path(raw).expanduser() if raw else tools.parent / "vault"


def configured_mcp_python_path(tools_path: Path | None = None) -> Path:
    raw = (os.environ.get(NORMS_MCP_PYTHON_ENV) or "").strip()
    tools = tools_path or configured_runtime_tools_path()
    return Path(raw).expanduser() if raw else tools / "venv" / "bin" / "python"


def configured_paragraph_embeddings_path(tools_path: Path | None = None) -> Path:
    tools = tools_path or configured_runtime_tools_path()
    return tools / "paragraphs_embeddings.npz"


def status_index_problems(path: Path | None = None) -> list[str]:
    """Проверить, что индекс существует и не является пустым fallback."""
    target = path or configured_status_index_path()
    if not target.is_file():
        return [f"нет authoritative status_index.json: {target}"]
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"status_index.json не читается: {target}: {exc}"]
    norms = payload.get("norms")
    if not isinstance(norms, list) or not norms:
        return [f"status_index.json пуст: {target}"]
    total = (payload.get("meta") or {}).get("total")
    if total is not None and total != len(norms):
        return [
            f"status_index.json несогласован: meta.total={total}, записей={len(norms)}"
        ]
    return []


def assert_status_index_available(path: Path | None = None) -> None:
    problems = status_index_problems(path)
    if problems:
        raise NormsRuntimeUnavailableError(
            "Нормативная база недоступна: " + "; ".join(problems)
        )


def _missing_files(paths: Iterable[tuple[str, Path]]) -> list[str]:
    return [f"нет {label}: {path}" for label, path in paths if not path.is_file()]


def runtime_problems(
    *,
    code_tools_path: Path | None = None,
    runtime_tools_path: Path | None = None,
    python_path: Path | None = None,
    timeout_sec: int = 20,
) -> list[str]:
    """Полный preflight MCP-runtime без загрузки многогигабайтных моделей."""
    code_tools = code_tools_path or release_code_tools_path()
    runtime_tools = runtime_tools_path or configured_runtime_tools_path()
    python = python_path or configured_mcp_python_path(runtime_tools)
    status_index = configured_status_index_path(runtime_tools)
    vault = configured_vault_path(runtime_tools)
    embeddings = configured_paragraph_embeddings_path(runtime_tools)
    launcher = code_tools / "mcp_launcher.py"

    problems: list[str] = []
    problems.extend(
        _missing_files(
            (
                ("MCP server", code_tools / "mcp_server.py"),
                ("MCP launcher", launcher),
                ("norms_api", code_tools / "norms_api.py"),
                ("интерпретатор runtime", python),
                ("индекс semantic search", embeddings),
            )
        )
    )
    problems.extend(status_index_problems(status_index))
    if not vault.is_dir():
        problems.append(f"нет vault нормативных документов: {vault}")
    elif not any(vault.glob("*.md")):
        problems.append(f"vault не содержит нормативных документов: {vault}")
    if python.is_file() and not os.access(python, os.X_OK):
        problems.append(f"интерпретатор runtime не исполняем: {python}")
    if launcher.is_file() and not os.access(launcher, os.X_OK):
        problems.append(f"MCP launcher не исполняем: {launcher}")
    if embeddings.is_file() and embeddings.stat().st_size < 1024:
        problems.append(f"индекс semantic search пуст: {embeddings}")
    if problems:
        return problems

    probe = (
        "import sys;"
        f"sys.path.insert(0,{str(code_tools)!r});"
        "import mcp,numpy,yaml,sentence_transformers;"
        "import mcp_server,norms_api;"
        "data=norms_api.load_status_index(force_reload=True);"
        "assert data.get('norms'), 'empty status index';"
        "print('norms-runtime-ok')"
    )
    env = dict(os.environ)
    env.update(
        {
            NORMS_TOOLS_ENV: str(runtime_tools),
            NORMS_STATUS_INDEX_ENV: str(status_index),
            NORMS_VAULT_ENV: str(vault),
            NORMS_MCP_PYTHON_ENV: str(python),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    try:
        completed = subprocess.run(
            [str(python), "-c", probe],
            cwd=str(code_tools),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return [f"не удалось выполнить smoke-test runtime: {exc}"]
    if completed.returncode != 0:
        output = (completed.stdout + "\n" + completed.stderr).strip().splitlines()
        tail = output[-1] if output else f"exit {completed.returncode}"
        return [f"smoke-test runtime не пройден: {tail}"]
    return []


def assert_runtime_available(**kwargs) -> None:
    problems = runtime_problems(**kwargs)
    if problems:
        raise NormsRuntimeUnavailableError(
            "Сервер норм недоступен: " + "; ".join(problems)
        )
