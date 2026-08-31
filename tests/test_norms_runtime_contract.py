"""Контракт общего нормативного runtime для immutable releases."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from norms import runtime


def _valid_runtime(tmp_path: Path) -> tuple[Path, Path]:
    code_tools = tmp_path / "app" / "norms" / "tools"
    code_tools.mkdir(parents=True)
    for name in ("mcp_server.py", "norms_api.py", "mcp_launcher.py"):
        (code_tools / name).write_text("# probe fixture\n", encoding="utf-8")
    (code_tools / "mcp_launcher.py").chmod(0o755)

    tools = tmp_path / "shared" / "norms" / "tools"
    tools.mkdir(parents=True)
    (tools / "venv/bin").mkdir(parents=True)
    (tools / "venv/bin/python").symlink_to(sys.executable)
    (tools / "paragraphs_embeddings.npz").write_bytes(b"x" * 2048)
    (tools / "status_index.json").write_text(
        json.dumps({"meta": {"total": 1}, "norms": [{"code": "СП 1"}]}),
        encoding="utf-8",
    )
    vault = tools.parent / "vault"
    vault.mkdir()
    (vault / "СП 1_document.md").write_text("1 Текст", encoding="utf-8")
    return code_tools, tools


def test_checkout_defaults_to_inrepo_tools():
    expected = Path(runtime.__file__).resolve().parent / "tools"
    assert runtime.default_runtime_tools_path() == expected


def test_release_layout_defaults_to_shared_runtime():
    module = Path("/opt/auditmanager/releases/ui-real-deadbeef/app/norms/runtime.py")
    assert runtime.default_runtime_tools_path(module_file=module) == Path(
        "/opt/auditmanager/shared/norms/tools"
    )


def test_explicit_runtime_environment_wins(monkeypatch, tmp_path):
    tools = tmp_path / "custom-tools"
    # Point overrides have higher priority by design.  Clear any values left by
    # lazy norms_api bootstrap so this test measures the tools-root contract,
    # independently of suite order.
    monkeypatch.delenv(runtime.NORMS_STATUS_INDEX_ENV, raising=False)
    monkeypatch.delenv(runtime.NORMS_VAULT_ENV, raising=False)
    monkeypatch.delenv(runtime.NORMS_MCP_PYTHON_ENV, raising=False)
    monkeypatch.setenv("NORMS_TOOLS_PATH", str(tools))
    assert runtime.configured_runtime_tools_path() == tools
    assert runtime.configured_status_index_path() == tools / "status_index.json"
    assert runtime.configured_vault_path() == tools.parent / "vault"
    assert runtime.configured_mcp_python_path() == tools / "venv/bin/python"


@pytest.mark.parametrize("payload", [{}, {"meta": {"total": 0}, "norms": []}])
def test_empty_status_index_is_not_accepted(tmp_path, payload):
    path = tmp_path / "status_index.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert runtime.status_index_problems(path)
    with pytest.raises(runtime.NormsRuntimeUnavailableError):
        runtime.assert_status_index_available(path)


def test_runtime_preflight_checks_python_data_and_vault(tmp_path):
    code_tools, tools = _valid_runtime(tmp_path)
    (tools / "venv/bin/python").unlink()
    problems = runtime.runtime_problems(
        code_tools_path=code_tools,
        runtime_tools_path=tools,
    )
    assert any("интерпретатор" in item for item in problems)


def test_runtime_preflight_runs_import_smoke(monkeypatch, tmp_path):
    code_tools, tools = _valid_runtime(tmp_path)
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return subprocess.CompletedProcess(command, 0, "norms-runtime-ok\n", "")

    monkeypatch.setattr(runtime.subprocess, "run", fake_run)
    assert runtime.runtime_problems(
        code_tools_path=code_tools,
        runtime_tools_path=tools,
    ) == []
    assert captured["env"]["NORMS_TOOLS_PATH"] == str(tools)
    assert captured["command"][0] == str(tools / "venv/bin/python")


def test_runtime_preflight_reports_import_failure(monkeypatch, tmp_path):
    code_tools, tools = _valid_runtime(tmp_path)

    monkeypatch.setattr(
        runtime.subprocess,
        "run",
        lambda command, **kwargs: subprocess.CompletedProcess(
            command, 1, "", "ModuleNotFoundError: No module named 'mcp'\n"
        ),
    )
    problems = runtime.runtime_problems(
        code_tools_path=code_tools,
        runtime_tools_path=tools,
    )
    assert problems == [
        "smoke-test runtime не пройден: ModuleNotFoundError: No module named 'mcp'"
    ]


def test_claude_mcp_config_uses_release_owned_launcher():
    config = json.loads((Path(__file__).parents[1] / ".mcp.json").read_text())
    norms = config["mcpServers"]["norms"]
    assert norms == {"command": "./norms/tools/mcp_launcher.py", "args": []}
    launcher = Path(__file__).parents[1] / "norms/tools/mcp_launcher.py"
    assert launcher.stat().st_mode & 0o111


def test_setup_refuses_to_replace_existing_runtime(tmp_path):
    from scripts import setup_norms_runtime

    target = tmp_path / "shared/norms"
    target.mkdir(parents=True)
    with pytest.raises(SystemExit, match="не будет изменён"):
        setup_norms_runtime.install(
            source_norms=tmp_path / "source",
            target=target,
            python=Path(sys.executable),
        )


def test_paragraphs_cache_stays_in_repo_for_checkout():
    expected = Path(runtime.__file__).resolve().parent / "norms_paragraphs.json"
    assert runtime.configured_paragraphs_cache_path() == expected


def test_paragraphs_cache_leaves_readonly_release():
    """Кеш цитат ДОПИСЫВАЕТСЯ прогоном, а каталог кода релиза только на чтение.

    Регресс 31.08.2026: запись падала PermissionError уже ПОСЛЕ того, как цитаты
    были проверены, и роняла весь этап норм.
    """
    module = Path("/opt/auditmanager/releases/ui-real-deadbeef/app/norms/runtime.py")
    resolved = runtime.configured_paragraphs_cache_path(module_file=module)
    assert resolved == Path("/opt/auditmanager/shared/norms/norms_paragraphs.json")
    assert "releases" not in resolved.parts


def test_paragraphs_cache_env_override(monkeypatch, tmp_path):
    target = tmp_path / "custom" / "norms_paragraphs.json"
    monkeypatch.setenv(runtime.NORMS_PARAGRAPHS_ENV, str(target))
    assert runtime.configured_paragraphs_cache_path() == target


def test_norms_api_gets_runtime_data_paths(monkeypatch):
    """norms_api берёт данные из env, иначе — из КАТАЛОГА КОДА, где их нет.

    Регресс 31.08.2026: в release-layout индекс не грузился, и вызывающий код
    читал это как «документа нет в базе нормативов», отклоняя каждый пункт и
    каждую цитату.
    """
    import os

    from norms import _native_verify

    for var in (
        runtime.NORMS_TOOLS_ENV,
        runtime.NORMS_STATUS_INDEX_ENV,
        runtime.NORMS_VAULT_ENV,
    ):
        monkeypatch.delenv(var, raising=False)

    _native_verify._ensure_runtime_env()

    tools = Path(os.environ[runtime.NORMS_TOOLS_ENV])
    assert tools == _native_verify.NORMS_TOOLS_PATH
    assert Path(os.environ[runtime.NORMS_STATUS_INDEX_ENV]) == tools / "status_index.json"
    assert Path(os.environ[runtime.NORMS_VAULT_ENV]) == tools.parent / "vault"
