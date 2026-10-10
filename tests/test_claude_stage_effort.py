"""Усилие Claude-этапов задаётся конвейером, а не личным settings.json CLI."""

from backend.app.core import config
from backend.app.services.llm import claude_runner


def test_default_effort_is_high():
    assert config.CLAUDE_STAGE_EFFORT == "high"
    assert config.claude_effort_args() == ["--effort", "high"]


def test_explicit_effort_wins_and_unknown_falls_back(monkeypatch):
    assert config.claude_effort_args("low") == ["--effort", "low"]
    assert config.claude_effort_args("XHIGH") == ["--effort", "xhigh"]
    assert config.claude_effort_args("bogus") == ["--effort", "high"]


def test_empty_setting_leaves_effort_to_cli(monkeypatch):
    monkeypatch.setattr(config, "CLAUDE_STAGE_EFFORT", "")
    assert config.claude_effort_args() == []


def test_stage_command_carries_effort(monkeypatch):
    monkeypatch.setattr(claude_runner, "get_claude_cli", lambda: "claude")
    cmd = claude_runner._build_cmd("Read,Write", "claude-opus-5-5")

    assert cmd[cmd.index("--effort") + 1] == "high"
    assert claude_runner._build_cmd("Read", "claude-opus-5-5", "medium")[
        cmd.index("--effort") + 1
    ] == "medium"
