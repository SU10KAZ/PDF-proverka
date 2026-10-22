"""Усилие Claude-этапов задаётся конвейером, а не личным settings.json CLI."""

from backend.app.core import config
from backend.app.services.llm import claude_runner


def test_default_effort_is_medium():
    # 22.10.2026: замеры режима «только Claude» сделаны на medium.
    assert config.CLAUDE_STAGE_EFFORT == "medium"
    assert config.claude_effort_args() == ["--effort", "medium"]


def test_explicit_effort_wins_and_unknown_falls_back(monkeypatch):
    assert config.claude_effort_args("low") == ["--effort", "low"]
    assert config.claude_effort_args("XHIGH") == ["--effort", "xhigh"]
    assert config.claude_effort_args("bogus") == ["--effort", "medium"]


def test_empty_setting_leaves_effort_to_cli(monkeypatch):
    monkeypatch.setattr(config, "CLAUDE_STAGE_EFFORT", "")
    assert config.claude_effort_args() == []


def test_stage_command_carries_effort(monkeypatch):
    monkeypatch.setattr(claude_runner, "get_claude_cli", lambda: "claude")
    cmd = claude_runner._build_cmd("Read,Write", "claude-opus-5-5")

    assert cmd[cmd.index("--effort") + 1] == "medium"
    assert claude_runner._build_cmd("Read", "claude-opus-5-5", "high")[
        cmd.index("--effort") + 1
    ] == "high"


def test_json_runner_falls_back_to_stage_effort(monkeypatch):
    from backend.app.services.llm import claude_json_runner

    assert claude_json_runner._effort(None) == "medium"
    assert claude_json_runner._effort("high") == "high"
    monkeypatch.setattr(config, "CLAUDE_STAGE_EFFORT", "")
    assert claude_json_runner._effort("") is None
