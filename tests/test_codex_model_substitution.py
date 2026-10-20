import json
from pathlib import Path

import pytest

import backend.app.services.llm.codex_model_substitution as substitution


@pytest.fixture
def settings_file(monkeypatch, tmp_path):
    path = tmp_path / "codex_model_substitution.json"
    monkeypatch.setattr(substitution, "SETTINGS_FILE", path)
    return path


def test_no_file_means_no_substitution(settings_file):
    assert substitution.substitute_codex_model("gpt-5.6-sol") == "gpt-5.6-sol"


@pytest.mark.parametrize("content", [
    "not json",
    json.dumps([1, 2]),
    json.dumps({"substitutions": "gpt-6-sol"}),
    json.dumps({"substitutions": {"gpt-5.6-sol": ""}}),
    json.dumps({"substitutions": {"gpt-5.6-sol": "gpt-6-sol --sandbox x"}}),
    json.dumps({"substitutions": {"gpt-5.6-sol": "gpt-5.6-sol"}}),
])
def test_invalid_file_means_no_substitution(settings_file, content):
    settings_file.write_text(content, encoding="utf-8")
    assert substitution.substitute_codex_model("gpt-5.6-sol") == "gpt-5.6-sol"


def test_only_listed_model_is_substituted(settings_file):
    settings_file.write_text(
        json.dumps({"substitutions": {"gpt-5.6-sol": "gpt-6-sol"}}), encoding="utf-8",
    )
    assert substitution.substitute_codex_model("gpt-5.6-sol") == "gpt-6-sol"
    assert substitution.substitute_codex_model("gpt-6-astra") == "gpt-6-astra"


@pytest.mark.asyncio
async def test_json_runner_runs_substitute_but_keeps_requested_label(monkeypatch, settings_file):
    import backend.app.services.llm.codex_runner as codex_runner

    settings_file.write_text(
        json.dumps({"substitutions": {"gpt-5.6-sol": "gpt-6-sol"}}), encoding="utf-8",
    )
    captured = {}
    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/usr/bin/codex")

    async def fake_run_command(cmd, **kwargs):
        captured["cmd"] = cmd
        Path(cmd[cmd.index("-o") + 1]).write_text('{"items": []}', encoding="utf-8")
        return 0, "", ""

    monkeypatch.setattr(codex_runner, "run_command", fake_run_command)

    result = await codex_runner.run_codex_json_messages(
        [{"role": "user", "content": "Return JSON."}],
        timeout=10,
        stage="block_analysis",
        project_id="DOC-1",
        model="codex/gpt-5.6-sol",
    )

    cmd = captured["cmd"]
    assert cmd[cmd.index("--model") + 1] == "gpt-6-sol"
    assert result.model == "codex/gpt-5.6-sol"
    assert result.is_error is False


@pytest.mark.asyncio
async def test_agentic_runner_runs_substitute(monkeypatch, settings_file):
    import backend.app.services.llm.codex_runner as codex_runner

    settings_file.write_text(
        json.dumps({"substitutions": {"gpt-5.6-sol": "gpt-6-sol"}}), encoding="utf-8",
    )
    captured = {}
    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/usr/bin/codex")

    async def fake_run_command(cmd, **kwargs):
        captured["cmd"] = cmd
        Path(cmd[cmd.index("-o") + 1]).write_text("done", encoding="utf-8")
        return 0, "", ""

    monkeypatch.setattr(codex_runner, "run_command", fake_run_command)

    exit_code, _output, _result = await codex_runner.run_codex_exec(
        "task", timeout=10, stage="optimization", project_id="DOC-1",
        model="codex/gpt-5.6-sol",
    )

    assert exit_code == 0
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "gpt-6-sol"
