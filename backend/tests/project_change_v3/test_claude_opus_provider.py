"""ProjectChange V3 / Opus: every model stage on Claude Opus — zero model calls.

Engine 3.5.0 moved Mapper, Miner and Dedupe from ``gpt-6-astra`` (Codex CLI) to
``claude-opus-5`` (Claude Code CLI, stream-json).  The Claude CLI is replaced
here by ``fake_claude.py``, which logs what a model would have seen and can
misbehave the way the real CLI can: answer with another model after a refusal,
ignore ``--effort``, report a quota error, keep a tool.

What must hold:

* the model-visible text is the SAME payload as before, byte for byte, and the
  images are the same files in label order — nothing is cut, chunked or dropped;
* a call the CLI would silently trim (too many / too heavy / too large images)
  is refused BEFORE the provider is contacted;
* an answer is accepted only from the requested model; there is no fallback
  model, no Codex, no legacy;
* provenance names the provider, the model, the thinking level and the
  transport — never ``gpt-6-astra``.
"""
from __future__ import annotations

import hashlib
import json
import stat
import struct
import threading
import time
import zlib
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import transport
from backend.app.services.project_change_v3.provider import (
    ClaudeOpusProvider,
    CodexProvider,
    ProviderError,
    build_codex_payload,
    get_provider,
    normalized_claude_usage,
)
from backend.tests.project_change_v3 import generic_fixture as gf

FAKE = Path(__file__).with_name("fake_claude.py")
SCHEMA = {"type": "object", "required": ["pair", "ok"], "additionalProperties": False,
          "properties": {"pair": {"type": "string"}, "ok": {"type": "boolean"}}}
ANSWER = {"pair": "P", "ok": True}
HAIKU = "claude-haiku-4-5-20251001"


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def png(width: int, height: int, filler: bytes = b"\0") -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(b"IDAT", zlib.compress(filler)) + chunk(b"IEND", b"")


def image_rows(tmp_path: Path, count: int = 3, size=(1800, 1273)) -> list[dict]:
    rows = []
    for i in range(count):
        path = tmp_path / f"crop_{i}.png"
        path.write_bytes(png(*size, filler=b"crop %d" % (i % 2)))  # two identical crops: one model image
        rows.append({"path": str(path), "label": {"block": f"B{i}"}})
    return rows


def prompt_of(chars: int) -> str:
    # Multi-byte on purpose, plus the line separators a line-based reader would split on.
    unit = "Щит ВРУ-1 → 400 В; 配电; 𝛑=3,14159     | "
    return (unit * (chars // len(unit) + 1))[:chars]


@pytest.fixture
def schema_validator(monkeypatch):
    import sys
    import types

    try:
        import jsonschema  # noqa: F401
    except ImportError:
        def validate(value, schema):
            missing = [k for k in schema.get("required", []) if k not in value]
            if missing:
                raise ValueError(f"missing {missing}")

        monkeypatch.setitem(sys.modules, "jsonschema", types.SimpleNamespace(validate=validate))


@pytest.fixture
def fake_cli(tmp_path, monkeypatch, schema_validator):
    log, answer = tmp_path / "claude.log", tmp_path / "answer.json"
    answer.write_text(json.dumps(ANSWER), encoding="utf-8")
    monkeypatch.setenv("STAGE_COMPARISON_AI_CLAUDE_BIN", str(FAKE))
    # Any use of the previous provider's CLI is a test failure, not a fallback.
    monkeypatch.setenv("STAGE_COMPARISON_AI_CODEX_BIN", str(tmp_path / "no-codex-here"))
    monkeypatch.setenv("STAGE_COMPARISON_AI_ENV_ALLOWLIST", "FAKE_CLAUDE_LOG,FAKE_CLAUDE_RESPONSE,FAKE_CLAUDE_MODE")
    monkeypatch.setenv("FAKE_CLAUDE_LOG", str(log))
    monkeypatch.setenv("FAKE_CLAUDE_RESPONSE", str(answer))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-must-never-reach-the-cli")
    monkeypatch.delenv("FAKE_CLAUDE_MODE", raising=False)

    def records() -> list[dict]:
        return [json.loads(line) for line in log.read_text(encoding="utf-8").split("\n") if line] if log.exists() else []

    records.answer = answer
    return records


def complete(provider, prompt, rows, **kwargs):
    return provider.complete(stage="MINING", call_id="c1", pair_id="P", prompt=prompt, data={"pair": "P"},
                             schema=SCHEMA, images=rows, **kwargs)


# ── What the model sees ─────────────────────────────────────────────────────

def test_the_same_payload_and_images_reach_the_model(tmp_path, fake_cli):
    rows, prompt = image_rows(tmp_path), prompt_of(50_000)
    payload, paths, _ = build_codex_payload(prompt, {"pair": "P"}, rows)
    provider = ClaudeOpusProvider()
    assert complete(provider, prompt, rows) == ANSWER
    (seen,) = fake_cli()
    # Text: the unchanged V3 payload, exactly, in ONE message.
    assert seen["text"] == payload and seen["text_sha256"] == sha(payload)
    assert seen["stdin_lines"] == 1 and seen["stdin_is_ascii"] and seen["role"] == "user"
    # Images: the unique files, in label order, each under its ordinal, then the text.
    assert seen["image_sha256"] == [hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in paths]
    assert len(paths) == 2 and seen["labels"] == ["Image 1:", "Image 2:"]
    assert seen["block_types"] == ["text", "image", "text", "image", "text"]
    assert seen["image_media_types"] == ["image/png", "image/png"]

    receipt = provider.last_transport
    assert receipt["model_visible_payload_sha256"] == sha(payload) == receipt["chunk_sha256"][0]
    assert receipt["image_sha256"] == seen["image_sha256"] and receipt["sent_equals_planned"] is True
    assert (receipt["evidence_dropped"], receipt["evidence_truncated"]) == (0, 0)
    assert receipt["provider_transport_version"] == "projectchange_v3_claude_cli_transport/1"
    assert receipt["transport"] == "claude_cli_stream_json_single_user_message"
    assert (receipt["provider"], receipt["model"], receipt["reasoning"]) == (
        "claude_code_cli_subscription", "claude-opus-5", "xhigh")


def test_the_cli_session_is_isolated_and_has_no_fallback_model(tmp_path, fake_cli):
    provider = ClaudeOpusProvider()
    complete(provider, prompt_of(2_000), image_rows(tmp_path, 1))
    (seen,) = fake_cli()
    argv = seen["argv"]

    def value(name):
        return argv[argv.index(name) + 1]

    assert argv[0] == "-p" and value("--model") == "claude-opus-5" and value("--effort") == "xhigh"
    assert (value("--input-format"), value("--output-format")) == ("stream-json", "stream-json")
    assert value("--tools") == "" and value("--setting-sources") == ""
    for switch in ("--verbose", "--strict-mcp-config", "--no-session-persistence", "--disable-slash-commands"):
        assert switch in argv
    assert json.loads(value("--json-schema")) == SCHEMA
    assert "--fallback-model" not in argv and not any("gpt" in part.lower() for part in argv)
    assert seen["env"] == {"DISABLE_AUTO_COMPACT": "1", "DISABLE_AUTOUPDATER": "1",
                           "CLAUDE_CODE_MAX_OUTPUT_TOKENS": "128000", "ANTHROPIC_API_KEY": None,
                           "STAGE_COMPARISON_AI_RUN": "c1"}
    assert "sc_ai_claude_mm_" in seen["cwd"]
    wire = provider.last_transport["wire"]
    assert wire["tools"] == ["StructuredOutput"] and wire["mcp_servers"] == [] and wire["api_key_source"] == "none"
    assert wire["fallback_model_flag"] is False and wire["max_output_tokens_requested"] == 128_000


def test_text_above_one_codex_turn_is_one_message_with_native_context(tmp_path, fake_cli):
    prompt = prompt_of(2_345_678)  # the Codex transport needed three chunks for this
    payload = build_codex_payload(prompt, {"pair": "P"}, [])[0]
    assert len(payload) > transport.CODEX_TURN_MAX_CHARS
    provider = ClaudeOpusProvider()
    assert complete(provider, prompt, []) == ANSWER
    (seen,) = fake_cli()
    assert seen["text"] == payload and seen["stdin_lines"] == 1 and seen["block_types"] == ["text"]
    receipt = provider.last_transport
    assert receipt["oversize_transport_used"] is False and receipt["injected_history_items"] == 0
    assert receipt["oversize_strategy"] == "native_context_single_user_message"
    assert receipt["chunk_chars"] == [len(payload)] and receipt["model_visible_payload_size"] == len(payload)


def test_receipt_carries_the_model_the_context_and_the_usage(tmp_path, fake_cli):
    provider = ClaudeOpusProvider()
    complete(provider, prompt_of(3_000), image_rows(tmp_path))
    receipt = provider.last_transport
    assert receipt["provider_ok"] is True
    assert receipt["usage"] == {"input_tokens": 6_220, "cached_input_tokens": 3_100,
                                "output_tokens": 450, "reasoning_output_tokens": 300}
    wire = receipt["wire"]
    assert wire["cli_version"] == "9.9.9" and wire["assistant_models"] == ["claude-opus-5"]
    assert (wire["context_window"], wire["max_output_tokens"]) == (1_000_000, 128_000)
    assert wire["auxiliary_models"] == [HAIKU] and wire["num_turns"] == 2
    assert wire["assistant_content_types"] == {"thinking": 1, "tool_use": 1}
    assert normalized_claude_usage({}) is None


def test_images_the_cli_would_reencode_are_named_not_hidden(tmp_path, fake_cli):
    heavy = tmp_path / "heavy.png"
    heavy.write_bytes(png(1800, 1273, filler=bytes(range(256)) * 4_000)[:600_000].ljust(600_000, b"\0"))
    light = tmp_path / "light.png"
    light.write_bytes(png(900, 600))
    provider = ClaudeOpusProvider()
    complete(provider, "p", [{"path": str(heavy), "label": {}}, {"path": str(light), "label": {}}])
    receipt = provider.last_transport
    assert receipt["images"] == 2 and receipt["images_reencoded_by_provider_cli"] == 1
    assert receipt["images_reencoded_by_provider_cli_sha256"] == [hashlib.sha256(heavy.read_bytes()).hexdigest()]
    assert receipt["image_px"] == [[1800, 1273], [900, 600]]
    # Our side sent the original bytes of both.
    assert fake_cli()[0]["image_sha256"] == receipt["image_sha256"]


# ── A call the CLI would trim is never made ─────────────────────────────────

def test_too_many_images_are_refused_before_the_provider(tmp_path, fake_cli):
    rows = []
    for i in range(transport.CLAUDE_MEDIA_COUNT_MAX + 1):
        path = tmp_path / f"i{i}.png"
        path.write_bytes(png(64, 64, filler=b"%d" % i))
        rows.append({"path": str(path), "label": {}})
    provider = ClaudeOpusProvider()
    with pytest.raises(ProviderError) as error:
        complete(provider, "p", rows)
    assert error.value.code == "transport_integrity" and "silently removes" in error.value.message
    assert fake_cli() == [] and provider.call_count == 0 and provider.last_transport is None


def test_too_heavy_images_are_refused_before_the_provider(tmp_path, fake_cli, monkeypatch):
    rows = image_rows(tmp_path)
    monkeypatch.setattr(transport, "CLAUDE_MEDIA_BASE64_BYTES_MAX", 100)
    with pytest.raises(ProviderError) as error:
        complete(ClaudeOpusProvider(), "p", rows)
    assert error.value.code == "transport_integrity" and "silently removes" in error.value.message
    assert fake_cli() == []


@pytest.mark.parametrize("data,needle", [
    (png(2001, 100), "downscales"),
    (b"not an image at all", "unreadable"),
])
def test_an_image_the_cli_would_alter_or_stub_is_refused(tmp_path, fake_cli, data, needle):
    path = tmp_path / "x.png"
    path.write_bytes(data)
    with pytest.raises(ProviderError) as error:
        complete(ClaudeOpusProvider(), "p", [{"path": str(path), "label": {}}])
    assert error.value.code == "transport_integrity" and needle in error.value.message
    assert fake_cli() == []


def test_the_production_source_packaging_fits_the_envelope():
    """Source prep caps a crop at 1800 px; the CLI alters images above 2000 px."""
    import inspect

    from backend.app.services.project_change_v3 import source_prep

    default = inspect.signature(source_prep._crop_pixmap).parameters["max_dimension"].default
    assert default <= transport.CLAUDE_IMAGE_MAX_PX


def test_header_sizes_of_every_accepted_format():
    assert transport.image_size_px(png(1438, 1801)) == (1438, 1801)
    assert transport.image_size_px(b"GIF89a" + struct.pack("<HH", 320, 200) + b"\0" * 8) == (320, 200)
    jpeg = b"\xff\xd8\xff\xe0\x00\x04\x00\x00" + b"\xff\xc0\x00\x11\x08" + struct.pack(">HH", 600, 800) + b"\0" * 12
    assert transport.image_size_px(jpeg) == (800, 600)
    assert transport.image_size_px(b"BM" + b"\0" * 40) is None


# ── An answer is accepted only from the requested model ─────────────────────

@pytest.mark.parametrize("mode,code", [
    ("foreign_model", "provider_model_mismatch"),   # the CLI's own refusal fallback answered
    ("foreign_usage", "provider_model_mismatch"),
    ("tools_leak", "provider_isolation_breach"),
    ("no_structured", "provider_no_structured_output"),
    ("effort_warning", "provider_effort_ignored"),
    ("quota", "PERMANENT"),
    ("max_tokens", "ERROR_DURING_EXECUTION"),
    ("limit_as_success", "PROVIDER_ERROR"),         # never reported under the word "success"
    ("no_result", "TRANSIENT"),
])
def test_provider_side_failures_fail_the_call(tmp_path, fake_cli, monkeypatch, mode, code):
    monkeypatch.setenv("FAKE_CLAUDE_MODE", mode)
    provider = ClaudeOpusProvider()
    with pytest.raises(ProviderError) as error:
        complete(provider, prompt_of(1_000), image_rows(tmp_path, 1))
    assert error.value.code == code
    assert len(fake_cli()) == 1 and provider.call_count == 1  # one attempt, never a second model
    assert provider.last_transport["provider_ok"] is False


def test_a_dated_id_of_the_same_model_is_the_same_model(tmp_path, fake_cli, monkeypatch):
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "dated_model")
    provider = ClaudeOpusProvider()
    assert complete(provider, "p", []) == ANSWER
    assert provider.last_transport["wire"]["assistant_models"] == ["claude-opus-5-20260801"]


def test_an_unknown_effort_level_is_refused_not_silently_defaulted(tmp_path, fake_cli):
    provider = ClaudeOpusProvider(reasoning="ultra")
    with pytest.raises(ProviderError) as error:
        complete(provider, "p", [])
    assert error.value.code == "provider_gateway_error" and "ultra" in error.value.message
    assert fake_cli() == []


def test_an_answer_outside_the_schema_is_rejected(tmp_path, fake_cli):
    fake_cli.answer.write_text(json.dumps({"pair": "P"}), encoding="utf-8")
    with pytest.raises(ProviderError) as error:
        complete(ClaudeOpusProvider(), "p", [])
    assert error.value.code == "schema_invalid"


def test_production_has_one_provider_and_no_switch_to_another(monkeypatch):
    for name in ("PROJECT_COMPARISON_V3_PROVIDER", "PROJECT_COMPARISON_V3_MODEL", "AUDIT_CODEX_MODEL"):
        monkeypatch.setenv(name, "gpt-6-astra")
    provider = get_provider()
    assert isinstance(provider, ClaudeOpusProvider) and not isinstance(provider, CodexProvider)
    assert (provider.provider, provider.model, provider.reasoning) == (
        "claude_code_cli_subscription", "claude-opus-5", "xhigh")


# ── Offline readiness of the provider ───────────────────────────────────────

def test_gate_checks_the_claude_cli_without_a_model_call(fake_cli, monkeypatch):
    from backend.app.services.project_change_v3.provider_gate import check_provider_readiness

    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "1")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "0")
    monkeypatch.delenv("PROJECT_COMPARISON_V3_PROVIDER_READY", raising=False)
    gate = check_provider_readiness()
    assert gate["available"] is True and gate["reason"] == "provider_ready" and gate["model_calls"] == 0
    assert gate["model"] == "claude-opus-5" and fake_cli() == []  # --version / --help only


def test_gate_is_closed_when_the_backend_cannot_reach_the_cli(tmp_path, monkeypatch):
    from backend.app.services.project_change_v3.provider_gate import check_provider_readiness

    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "1")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "0")
    monkeypatch.delenv("PROJECT_COMPARISON_V3_PROVIDER_READY", raising=False)
    monkeypatch.setenv("STAGE_COMPARISON_AI_CLAUDE_BIN", str(tmp_path / "missing" / "claude"))
    gate = check_provider_readiness()
    assert gate["available"] is False and gate["reason"] == "provider_not_ready"
    assert "STAGE_COMPARISON_AI_CLAUDE_BIN" in gate["gateway_detail"]


def test_gate_is_closed_when_the_cli_cannot_take_images(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison.ai import gateway

    old = tmp_path / "old_claude.py"
    old.write_text("#!/usr/bin/env python3\nimport sys\n"
                   "print('1.0.0' if '--version' in sys.argv else '--json-schema --tools --setting-sources "
                   "--strict-mcp-config --system-prompt --output-format (text, json) --effort --verbose "
                   "--no-session-persistence')\n", encoding="utf-8")
    old.chmod(old.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("STAGE_COMPARISON_AI_CLAUDE_BIN", str(old))
    report = gateway.validate_claude_runtime(reasoning_level="xhigh")
    assert report["ok"] is False
    assert any("--input-format" in p for p in report["problems"]) and any("stream-json" in p for p in report["problems"])


# ── The whole engine on the provider ────────────────────────────────────────

@pytest.fixture
def env(tmp_path, monkeypatch, fake_cli):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    built = gf.build_comparison(tmp_path)
    from backend.app.services.project_change_v3 import scope
    from backend.app.services.stage_comparison import production_orchestrator as orch

    monkeypatch.setattr(scope, "_object_stage_paths", lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})

    def _legacy(*_a, **_k):
        raise AssertionError("legacy comparison invoked")

    monkeypatch.setattr(orch, "_run_production_comparison_locked", _legacy)
    monkeypatch.setattr(orch, "_run_production_comparison_impl", _legacy)
    built["records"] = fake_cli
    yield built
    from backend.app.services.project_change_v3.provider import reset_test_provider

    reset_test_provider()


class ScriptedOpus(ClaudeOpusProvider):
    """The production provider; the fake CLI answers what the fixture scripted for the stage."""

    def __init__(self, answer_file: Path, handlers: dict):
        super().__init__()
        self._answer_file, self._handlers = answer_file, handlers

    def complete(self, **kwargs):
        scripted = self._handlers[kwargs["stage"]](**{k: kwargs[k] for k in ("call_id", "pair_id", "data",
                                                                             "schema", "images")})
        self._answer_file.write_text(json.dumps(scripted, ensure_ascii=False), encoding="utf-8")
        return super().complete(**kwargs)


def _run(built, provider):
    from backend.app.services.project_change_v3.provider import set_test_provider
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison

    set_test_provider(provider)
    return run_production_comparison(built["session_id"], gf.PAIR_ID, input_mode="DOCUMENT")


def _stored(built, name="state"):
    from backend.app.services.stage_comparison import production_store

    return production_store.load_artifact(built["session_id"], gf.PAIR_ID, name, include_domain_keys=True)


def test_mapper_miner_and_dedupe_all_run_on_opus(env):
    state = _run(env, ScriptedOpus(env["records"].answer, gf.fake_handlers()))
    assert state["status"] in {"REVIEW", "COMPLETED"}, state
    result = _stored(env, "project_change_v3_result")
    prov = result["provenance"]
    calls = prov["transport_calls"]
    stages = [c["stage"] for c in calls]
    assert stages[0] == "MAPPING" and "MINING" in stages and stages[-1] == "DEDUPE"
    # Every stage, one configuration, one transport — and the CLI saw exactly these calls.
    assert {(c["provider"], c["model"], c["reasoning"]) for c in calls} == {
        ("claude_code_cli_subscription", "claude-opus-5", "xhigh")}
    assert {c["provider_transport_version"] for c in calls} == {"projectchange_v3_claude_cli_transport/1"}
    assert all(c["wire"]["assistant_models"] == ["claude-opus-5"] and c["sent_equals_planned"] for c in calls)
    seen = env["records"]()
    assert len(seen) == len(calls)
    assert [r["text_sha256"] for r in seen] == [c["model_visible_payload_sha256"] for c in calls]
    assert [r["image_sha256"] for r in seen] == [c["image_sha256"] for c in calls]
    assert all(r["argv"][r["argv"].index("--model") + 1] == "claude-opus-5" for r in seen)
    assert (prov["engine_version"], prov["engine_variant"]) == ("3.5.0", "ProjectChange V3 / Opus")
    assert (prov["provider"], prov["model"], prov["thinking"]) == (
        "claude_code_cli_subscription", "claude-opus-5", {"type": "adaptive", "effort": "xhigh"})
    assert prov["usage_total"]["calls_with_usage"] == len(calls) and result["legacy_invoked"] is False
    assert "astra" not in json.dumps(result, ensure_ascii=False).lower()


def test_the_frozen_prompts_are_byte_for_byte_those_of_the_previous_engine():
    from backend.app.services.project_change_v3 import contracts

    assert contracts.MAPPER_PROMPT_SHA256.startswith("324696ce")
    assert contracts.MINER_PROMPT_SHA256.startswith("7837a504")
    assert contracts.DEDUPE_PROMPT_SHA256.startswith("b31304ff")
    assert contracts.SOURCE_PACKAGING_VERSION == "projectchange_v3_source_pack/2"


def test_a_run_that_mixed_models_is_never_persisted(env):
    handlers = gf.fake_handlers()

    class Mixing(ScriptedOpus):
        def complete(self, **kwargs):
            answer = super().complete(**kwargs)
            if kwargs["stage"] == "DEDUPE":
                self.last_transport["model"] = "gpt-6-astra"
            return answer

    state = _run(env, Mixing(env["records"].answer, handlers))
    assert state["status"] == "FAILED" and state["reason_code"] == "v3_model_mixing"
    assert _stored(env, "project_change_v3_result") is None and state["legacy_invoked"] is False


def test_a_foreign_model_answer_fails_the_run_and_publishes_nothing(env, monkeypatch):
    monkeypatch.setenv("FAKE_CLAUDE_MODE", "foreign_model")
    state = _run(env, ScriptedOpus(env["records"].answer, gf.fake_handlers()))
    assert state["status"] == "FAILED" and state["reason_code"] == "provider_model_mismatch"
    assert len(env["records"]()) == 1  # the Mapper call; nothing was retried on another model
    assert _stored(env, "project_change_v3_result") is None and state["legacy_invoked"] is False


def test_cancel_kills_the_claude_session_in_flight(env, monkeypatch):
    from backend.app.services.stage_comparison.ai import gateway
    from backend.app.services.stage_comparison.production_orchestrator import (
        active_run_control,
        cancel_production_comparison,
    )

    monkeypatch.setenv("FAKE_CLAUDE_MODE", "slow")
    outcome: dict = {}
    provider = ScriptedOpus(env["records"].answer, gf.fake_handlers())
    worker = threading.Thread(target=lambda: outcome.update(state=_run(env, provider)), daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 30
        while gateway.live_process_count() == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        assert gateway.live_process_count() == 1, "the Mapper CLI session never started"
        assert active_run_control(env["session_id"], gf.PAIR_ID) is not None
        started = time.monotonic()
        answer = cancel_production_comparison(env["session_id"], gf.PAIR_ID, requested_by="engineer")
        worker.join(timeout=30)
        assert not worker.is_alive(), "cancel did not stop the run"
    finally:
        gateway.kill_live_processes()
        worker.join(timeout=30)
    assert time.monotonic() - started < 15 and answer["cancelled"] is True
    state = outcome["state"]
    assert state["status"] == "FAILED" and state["reason_code"] == "v3_cancelled"
    assert gateway.live_process_count() == 0 and _stored(env, "project_change_v3_result") is None
    last = state["provenance"]["transport_calls"][-1]
    assert last["stage"] == "MAPPING" and last["provider_ok"] is False and last["usage"] is None
