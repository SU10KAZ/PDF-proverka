"""Oversize V3 payloads: lossless, versioned transport through the real provider adapter.

Zero model calls: the Codex CLI is replaced by ``fake_codex.py``, which enforces
the real per-turn limit and logs what a model would have seen.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import transport
from backend.app.services.project_change_v3.provider import CodexProvider, ProviderError, build_codex_payload

FAKE = Path(__file__).with_name("fake_codex.py")
SCHEMA = {"type": "object", "required": ["pair", "ok"], "additionalProperties": False,
          "properties": {"pair": {"type": "string"}, "ok": {"type": "boolean"}}}
ANSWER = {"pair": "P", "ok": True}


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@pytest.fixture
def schema_validator(monkeypatch):
    """The dev interpreter has no jsonschema; transport tests need only a required-keys check."""
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
    log, answer = tmp_path / "codex.log", tmp_path / "answer.json"
    answer.write_text(json.dumps(ANSWER), encoding="utf-8")
    monkeypatch.setenv("STAGE_COMPARISON_AI_CODEX_BIN", str(FAKE))
    monkeypatch.setenv("STAGE_COMPARISON_AI_ENV_ALLOWLIST", "FAKE_CODEX_LOG,FAKE_CODEX_RESPONSE,FAKE_CODEX_MODE")
    monkeypatch.setenv("FAKE_CODEX_LOG", str(log))
    monkeypatch.setenv("FAKE_CODEX_RESPONSE", str(answer))
    monkeypatch.delenv("FAKE_CODEX_MODE", raising=False)

    def records() -> list[dict]:
        return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()] if log.exists() else []

    return records


def images(tmp_path, count=3):
    rows = []
    for i in range(count):
        path = tmp_path / f"crop_{i}.png"
        path.write_bytes(b"\x89PNG fake crop %d" % (i % 2))  # two identical crops: one model image
        rows.append({"path": str(path), "label": {"block": f"B{i}"}})
    return rows


def synthetic_prompt(chars: int) -> str:
    # Multi-byte on purpose: Cyrillic, CJK and an astral-plane symbol.
    unit = "Щит ВРУ-1 → 400 В; 配电; 𝛑=3,14159 | "
    return (unit * (chars // len(unit) + 1))[:chars]


def payload_for(prompt: str, rows: list[dict]) -> str:
    return build_codex_payload(prompt, {"pair": "P"}, rows)[0]


def test_plan_is_lossless_and_versioned():
    small = transport.plan("x" * transport.CODEX_TURN_MAX_CHARS)
    assert not small.oversize and small.chunks == ("x" * transport.CODEX_TURN_MAX_CHARS,)
    text = synthetic_prompt(2_345_678)
    big = transport.plan(text)
    assert big.oversize and "".join(big.chunks) == text
    assert all(len(c) <= transport.CHUNK_CHARS for c in big.chunks) and len(big.chunks) == 3
    receipt = big.receipt()
    assert receipt["provider_transport_version"] == transport.PROVIDER_TRANSPORT_VERSION
    assert receipt["model_visible_payload_sha256"] == sha(text) and receipt["model_visible_payload_size"] == len(text)
    assert receipt["model_visible_payload_bytes"] == len(text.encode("utf-8")) and receipt["injected_history_items"] == 2
    with pytest.raises(transport.TransportIntegrityError):
        transport.verify(big.chunks[:-1], big)
    with pytest.raises(transport.TransportIntegrityError):
        transport.plan("")


def test_standard_path_sends_exact_payload_through_codex_exec(tmp_path, fake_cli):
    rows = images(tmp_path)
    prompt = synthetic_prompt(50_000)
    provider = CodexProvider()
    assert provider.complete(stage="MINING", call_id="c1", pair_id="P", prompt=prompt, data={"pair": "P"},
                             schema=SCHEMA, images=rows) == ANSWER
    (record,) = fake_cli()
    payload = payload_for(prompt, rows)
    assert record["mode"] == "exec" and record["stdin_sha256"] == sha(payload)
    assert len(record["images"]) == 2 and record["output_schema"]
    receipt = provider.last_transport
    assert receipt["oversize_transport_used"] is False and receipt["transport"] == transport.STANDARD_TRANSPORT
    assert receipt["model_visible_payload_sha256"] == sha(payload) and receipt["images"] == 2


@pytest.mark.parametrize("chars", [1_048_577 - 0, 1_700_000])
def test_oversize_path_is_lossless_through_app_server(tmp_path, fake_cli, chars):
    rows = images(tmp_path)
    prompt = synthetic_prompt(chars)
    payload = payload_for(prompt, rows)
    assert len(payload) > transport.CODEX_TURN_MAX_CHARS
    provider = CodexProvider()
    assert provider.complete(stage="MINING", call_id="c2", pair_id="P", prompt=prompt, data={"pair": "P"},
                             schema=SCHEMA, images=rows) == ANSWER
    (record,) = fake_cli()
    plan = transport.plan(payload)
    assert record["mode"] == "app-server" and "rejected_turn_chars" not in record
    assert record["model_visible_sha256"] == sha(payload) and record["model_visible_chars"] == len(payload)
    assert record["history_items"] == len(plan.chunks) - 1 and record["turn_text_chars"] == len(plan.chunks[-1])
    assert record["requests"] == ["initialize", "initialized", "thread/start", "thread/inject_items", "turn/start"]
    assert record["thread_start"] == {"model": provider.model, "sandbox": "read-only", "ephemeral": True,
                                      "approvalPolicy": "never"}
    assert record["effort"] == provider.reasoning and record["output_schema"]
    assert record["cyberAccessProgram"] == transport.OVERSIZE_CYBER_ACCESS_PROGRAM
    assert len(record["images"]) == 2 and all(Path(p).is_absolute() for p in record["images"])
    receipt = provider.last_transport
    assert receipt["oversize_transport_used"] is True and receipt["transport"] == transport.OVERSIZE_TRANSPORT
    assert receipt["model_visible_payload_sha256"] == sha(payload) == receipt["wire"]["wire_text_sha256"]
    assert receipt["model_visible_payload_size"] == len(payload) == receipt["wire"]["wire_text_chars"]
    assert sum(receipt["chunk_chars"]) == len(payload)


def test_exec_alone_would_reject_the_same_payload(tmp_path, fake_cli):
    """Why the transport exists: one Codex turn refuses the text before inference."""
    from backend.app.services.stage_comparison.ai.gateway import call_codex

    payload = payload_for(synthetic_prompt(1_200_000), images(tmp_path))
    result = call_codex(payload, model="m", schema=SCHEMA, retries=0)
    assert not result.ok and "input_too_large" in result.raw_excerpt


@pytest.mark.parametrize("mode,code", [("reject_inject", None), ("turn_failed", "PERMANENT"), ("tool_item", "PERMANENT")])
def test_oversize_failures_fail_closed_with_receipt(tmp_path, fake_cli, monkeypatch, mode, code):
    monkeypatch.setenv("FAKE_CODEX_MODE", mode)
    provider = CodexProvider()
    with pytest.raises(ProviderError) as info:
        provider.complete(stage="MINING", call_id="c3", pair_id="P", prompt=synthetic_prompt(1_300_000),
                          data={"pair": "P"}, schema=SCHEMA, images=images(tmp_path))
    if code:
        assert info.value.code == code
    assert provider.last_transport["oversize_transport_used"] is True
    assert provider.last_transport["model_visible_payload_size"] > transport.CODEX_TURN_MAX_CHARS


def test_mismatched_wire_is_never_sent(tmp_path, fake_cli):
    from backend.app.services.stage_comparison.ai.gateway import GatewayError, call_codex_app_server

    with pytest.raises(GatewayError):
        call_codex_app_server(["a" * 10], "b", model="m", expected_text_sha256=sha("a" * 9 + "b"))
    assert fake_cli() == []


def test_engine_receipts_every_call_in_provenance(tmp_path, monkeypatch):
    from backend.tests.project_change_v3 import generic_fixture as gf
    from backend.app.services.project_change_v3 import scope
    from backend.app.services.project_change_v3.provider import FakeProvider, reset_test_provider, set_test_provider
    from backend.app.services.stage_comparison import production_store
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison

    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    built = gf.build_comparison(tmp_path)
    monkeypatch.setattr(scope, "_object_stage_paths", lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})

    class ReceiptingFake(FakeProvider):
        last_transport = None

        def complete(self, **kwargs):
            self.last_transport = {"provider_transport_version": transport.PROVIDER_TRANSPORT_VERSION,
                                   "oversize_transport_used": kwargs["stage"] == "MINING",
                                   "model_visible_payload_sha256": "0" * 64, "model_visible_payload_size": 1}
            return super().complete(**kwargs)

    provider = ReceiptingFake(handlers=gf.fake_handlers())
    set_test_provider(provider)
    try:
        state = run_production_comparison(built["session_id"], gf.PAIR_ID, input_mode="DOCUMENT")
    finally:
        reset_test_provider()
    assert state["status"] in {"REVIEW", "COMPLETED"}
    result = production_store.load_artifact(built["session_id"], gf.PAIR_ID, "project_change_v3_result",
                                            include_domain_keys=True)
    for provenance in (state["provenance"], result["provenance"]):
        assert provenance["provider_transport_version"] == transport.PROVIDER_TRANSPORT_VERSION
        calls = provenance["transport_calls"]
        assert [c["stage"] for c in calls] == [c["stage"] for c in provider.calls]
        assert calls[0]["stage"] == "MAPPING" and any(c["oversize_transport_used"] for c in calls)


def test_missing_schema_validator_refuses_before_any_model_call(tmp_path, fake_cli, monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "jsonschema", None)  # import raises ImportError
    provider = CodexProvider()
    with pytest.raises(ProviderError) as info:
        provider.complete(stage="MINING", call_id="c4", pair_id="P", prompt=synthetic_prompt(1_300_000),
                          data={"pair": "P"}, schema=SCHEMA, images=images(tmp_path))
    assert info.value.code == "schema_validator_unavailable"
    assert fake_cli() == [] and provider.last_transport is None and provider.call_count == 0
