"""claude_json_runner: `claude -p` как JSON-модель с изображениями (без живой модели)."""
from __future__ import annotations

import asyncio
import base64
import io
import json
import stat
import sys
from pathlib import Path

import pytest
from PIL import Image

import backend.app.services.llm.claude_json_runner as runner_mod
import backend.scripts.audit_rejected_findings_codex as audit_cli
from backend.app.services.findings.rejected_audit_service import _looks_like_subscription_limit


def _fake_cli(tmp_path: Path, *, result: dict, exit_code: int = 0) -> Path:
    """Фальшивый `claude`: пишет argv и stdin в файлы, печатает NDJSON с result."""
    script = tmp_path / "claude"
    script.write_text(
        f"#!{sys.executable}\n"
        "import json, sys\n"
        f"open({str(tmp_path / 'argv.json')!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
        f"open({str(tmp_path / 'stdin.jsonl')!r}, 'w').write(sys.stdin.read())\n"
        "print(json.dumps({'type': 'system', 'subtype': 'init'}))\n"
        f"print({json.dumps(json.dumps(result))})\n"
        f"sys.exit({exit_code})\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script


def _png(path: Path, size: tuple[int, int]) -> Path:
    Image.new("RGB", size, "white").save(path, format="PNG")
    return path


def _run(**kwargs):
    messages = [
        {"role": "system", "content": "СИСТЕМНЫЙ ПРОМПТ"},
        {"role": "user", "content": "ЗАДАЧА"},
    ]
    return asyncio.run(runner_mod.run_claude_json_messages(messages, timeout=60, **kwargs))


def _stdin_content(tmp_path: Path) -> list[dict]:
    line = (tmp_path / "stdin.jsonl").read_text(encoding="utf-8").strip()
    return json.loads(line)["message"]["content"]


def test_success_reads_structured_output_and_passes_images_in_order(tmp_path, monkeypatch):
    cli = _fake_cli(tmp_path, result={
        "type": "result", "is_error": False, "session_id": "sess-1", "result": "",
        "structured_output": {"reviews": [{"case_id": "RF-1"}]},
        "usage": {"input_tokens": 5, "cache_creation_input_tokens": 100,
                  "cache_read_input_tokens": 20, "output_tokens": 7,
                  "output_tokens_details": {"thinking_tokens": 3}},
    })
    monkeypatch.setattr(runner_mod, "get_claude_cli", lambda: str(cli))
    first = _png(tmp_path / "a.png", (40, 20))
    second = _png(tmp_path / "b.png", (30, 30))

    result = _run(
        model="claude/claude-opus-5",
        reasoning_effort="high",
        image_paths=[first, second],
        output_schema={"type": "object"},
        allowed_tools="",
    )

    assert not result.is_error, result.error_message
    assert result.json_data == {"reviews": [{"case_id": "RF-1"}]}
    assert result.model == "claude/claude-opus-5"
    assert result.input_tokens == 125 and result.cached_tokens == 20
    assert result.output_tokens == 7 and result.reasoning_tokens == 3
    assert result.response_id == "sess-1" and result.cost_source == "subscription"

    argv = json.loads((tmp_path / "argv.json").read_text())
    assert argv[argv.index("--model") + 1] == "claude-opus-5"
    assert argv[argv.index("--effort") + 1] == "high"
    assert argv[argv.index("--system-prompt") + 1] == "СИСТЕМНЫЙ ПРОМПТ"
    assert json.loads(argv[argv.index("--json-schema") + 1]) == {"type": "object"}
    for flag in ("--tools=", "--setting-sources=", "--strict-mcp-config", "--no-session-persistence"):
        assert flag in argv
    assert argv[argv.index("--input-format") + 1] == "stream-json"

    content = _stdin_content(tmp_path)
    kinds = [block["type"] for block in content]
    assert kinds == ["text", "image", "text", "image", "text"]
    assert content[0]["text"] == "Изображение 1 из 2"
    assert base64.b64decode(content[1]["source"]["data"]) == first.read_bytes()
    assert base64.b64decode(content[3]["source"]["data"]) == second.read_bytes()
    assert content[-1]["text"] == "ЗАДАЧА"


def test_oversized_image_is_shrunk_in_memory_only(tmp_path, monkeypatch):
    cli = _fake_cli(tmp_path, result={
        "type": "result", "is_error": False, "result": "{\"ok\": true}", "usage": {},
    })
    monkeypatch.setattr(runner_mod, "get_claude_cli", lambda: str(cli))
    big = _png(tmp_path / "big.png", (9000, 300))
    before = big.read_bytes()

    result = _run(image_paths=[big])

    assert not result.is_error and result.json_data == {"ok": True}
    assert result.finish_reason == "images_resized=1"
    assert big.read_bytes() == before
    sent = _stdin_content(tmp_path)[1]["source"]["data"]
    with Image.open(io.BytesIO(base64.b64decode(sent))) as image:
        assert max(image.size) == runner_mod.MAX_IMAGE_EDGE


def test_subscription_limit_is_reported_so_the_audit_halts(tmp_path, monkeypatch):
    cli = _fake_cli(tmp_path, exit_code=1, result={
        "type": "result", "is_error": True,
        "result": "You've hit your limit · resets 5pm (Europe/Moscow)", "usage": {},
    })
    monkeypatch.setattr(runner_mod, "get_claude_cli", lambda: str(cli))

    result = _run()

    assert result.is_error
    assert _looks_like_subscription_limit(result.error_message)


def test_declared_tools_are_refused_not_silently_dropped(tmp_path, monkeypatch):
    monkeypatch.setattr(runner_mod, "get_claude_cli", lambda: "/nonexistent")
    result = _run(allowed_tools="mcp__norms__get_norm")
    assert result.is_error and "tools_unsupported" in result.error_message


def test_missing_image_fails_the_call(tmp_path, monkeypatch):
    cli = _fake_cli(tmp_path, result={"type": "result", "is_error": False, "result": "{}"})
    monkeypatch.setattr(runner_mod, "get_claude_cli", lambda: str(cli))
    result = _run(image_paths=[tmp_path / "нет.png"])
    assert result.is_error and "claude_image_missing" in result.error_message
    assert not (tmp_path / "stdin.jsonl").exists()


@pytest.mark.parametrize("model,expected", [
    ("claude/claude-opus-5", "claude-opus-5"),
    ("claude-sonnet-5", "claude-sonnet-5"),
    ("", runner_mod.DEFAULT_CLAUDE_JSON_MODEL),
])
def test_model_prefix_is_stripped(model, expected):
    assert runner_mod.resolve_claude_json_model(model) == expected


def test_cli_picks_claude_runner_by_model_prefix(monkeypatch):
    monkeypatch.setattr("backend.app.core.config.get_claude_cli", lambda: sys.executable)
    runner, cli = audit_cli._resolve_runner("claude/claude-opus-5")
    assert runner is runner_mod.run_claude_json_messages
    assert cli == sys.executable
    assert audit_cli._external_processor("claude/claude-opus-5") == "subscription Claude Code"
    assert audit_cli._external_processor(audit_cli.CODEX_MODEL) == "subscription Codex"


def test_disclosure_confirmed_for_codex_does_not_cover_claude(tmp_path):
    manifest = tmp_path / "manifest.jsonl"
    inventory = tmp_path / "inventory.json"
    manifest.write_text("", encoding="utf-8")
    inventory.write_text("{}", encoding="utf-8")
    disclosure = audit_cli._build_external_disclosure(
        [], manifest_path=manifest, inventory_path=inventory,
    )
    assert disclosure["model"] == audit_cli.CODEX_MODEL
    disclosure_path = tmp_path / "external_codex_disclosure.json"
    disclosure_path.write_text(json.dumps(disclosure), encoding="utf-8")

    with pytest.raises(ValueError, match="подтверждён для"):
        audit_cli._verify_external_disclosure(
            output_dir=tmp_path,
            disclosure_path=disclosure_path,
            batch_size=1,
            requested_max_batch_images=1,
            model=audit_cli.CLAUDE_MODEL,
        )
