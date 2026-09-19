"""One bounded Miner retry after an evidence-traceability rejection — zero model calls.

Research A-R017: a complete Miner answer was rejected with
``Untraceable evidence: PCA-A-R017-C001`` and the next call with the SAME
model-visible input was accepted.  Production answers such a rejection with
exactly one more identical call (2 attempts in total) and receipts both; any
other failure stays final.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from backend.tests.project_change_v3 import generic_fixture as gf

FAKE = Path(__file__).with_name("fake_codex.py")
R1 = f"{gf.PAIR_ID}_R-001"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    built = gf.build_comparison(tmp_path)
    from backend.app.services.project_change_v3 import scope

    monkeypatch.setattr(scope, "_object_stage_paths", lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})
    from backend.app.services.stage_comparison import production_orchestrator as orch

    def _legacy(*_a, **_k):
        raise AssertionError("legacy comparison invoked")

    monkeypatch.setattr(orch, "_run_production_comparison_locked", _legacy)
    monkeypatch.setattr(orch, "_run_production_comparison_impl", _legacy)
    yield built
    from backend.app.services.project_change_v3.provider import reset_test_provider

    reset_test_provider()


def _sha(value) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


class RecordingFake:
    """FakeProvider that records exactly what each call would show the model."""

    def __init__(self, handlers, *, mining_answers=None):
        from backend.app.services.project_change_v3.provider import FakeProvider

        self.inner = FakeProvider(handlers=handlers)
        self.seen: list[dict] = []
        self.mining_answers = mining_answers or {}

    @property
    def calls(self):
        return self.inner.calls

    def complete(self, **kwargs):
        from backend.app.services.project_change_v3.provider import build_codex_payload
        from backend.app.services.project_change_v3.transport import sha256_text

        payload, image_paths, _ = build_codex_payload(kwargs["prompt"], kwargs["data"], kwargs["images"])
        self.seen.append({
            "stage": kwargs["stage"], "call_id": kwargs["call_id"],
            "payload_sha256": sha256_text(payload),
            "prompt_sha256": sha256_text(kwargs["prompt"]),
            "data_sha256": _sha(kwargs["data"]),
            "schema_sha256": _sha(kwargs["schema"]),
            "images": [hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in image_paths],
        })
        answer = self.inner.complete(**kwargs)
        if kwargs["stage"] == "MINING":
            region = kwargs["data"]["frozen_region"]["region_id"]
            script = self.mining_answers.get(region)
            if script:
                attempt = sum(1 for s in self.seen if s["stage"] == "MINING"
                              and s["call_id"].startswith(f"{gf.PAIR_ID}_{region}"))
                answer = script(copy.deepcopy(answer), attempt, kwargs)
        return answer


def untraceable_first(bad_attempts=(1,)):
    def script(answer, attempt, _kwargs):
        if attempt in bad_attempts:  # a syntactically valid answer with a bbox no source block has
            answer["projectchanges"][0]["evidence_items"][0]["bbox"] = [0.0, 0.0, 1.0, 1.0]
        return answer
    return script


def _run(built, provider):
    from backend.app.services.project_change_v3.provider import set_test_provider
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison

    set_test_provider(provider)
    return run_production_comparison(built["session_id"], gf.PAIR_ID, input_mode="DOCUMENT")


def _artifact(built, name):
    from backend.app.services.stage_comparison import production_store

    return production_store.load_artifact(built["session_id"], gf.PAIR_ID, name, include_domain_keys=True)


def _r1(provider):
    return [s for s in provider.seen if s["call_id"].startswith(R1)]


def test_one_retry_after_provenance_rejection_continues_the_pipeline(env):
    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": untraceable_first()})
    state = _run(env, provider)

    assert state["status"] == "REVIEW" and state["reason_code"] == "v3_completed"
    first, second = _r1(provider)  # exactly two calls for the rejected region
    assert [first["call_id"], second["call_id"]] == [R1, f"{R1}_RETRY_1"]
    for key in ("payload_sha256", "prompt_sha256", "data_sha256", "schema_sha256", "images"):
        assert first[key] == second[key], key  # same prompt, data, images, schema: no repair
    assert [s["stage"] for s in provider.seen] == ["MAPPING", "MINING", "MINING", "MINING", "DEDUPE"]

    result = _artifact(env, "project_change_v3_result")
    assert [c["projectchange_id"] for c in result["projectchanges"]] == ["PC-R-001-C001"]
    for provenance in (state["provenance"], result["provenance"]):
        attempts = [a for a in provenance["miner_attempts"] if a["region_id"] == "R-001"]
        assert [(a["attempt"], a["validation"], a["accepted"]) for a in attempts] == [
            (1, "REJECTED", False), (2, "ACCEPTED", True)]
        assert attempts[0]["rejection_kind"] == "untraceable_evidence"
        assert attempts[0]["rejection_reason"] == "Untraceable evidence: PC-R-001-C001"
        assert attempts[0]["model_visible_payload_sha256"] == attempts[1]["model_visible_payload_sha256"] \
            == first["payload_sha256"]
        assert attempts[0]["model_visible_input_sha256"] == attempts[1]["model_visible_input_sha256"]
        assert provenance["miner_retry_policy"]["max_attempts"] == 2
    assert state["model_calls"] == 0  # FakeProvider: nothing real was called


def test_double_provenance_failure_is_failed_after_exactly_two_calls(env):
    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": untraceable_first((1, 2))})
    state = _run(env, provider)

    assert state["status"] == "FAILED" and state["reason_code"] == "miner_provenance_rejected"
    assert len(_r1(provider)) == 2
    assert [s["stage"] for s in provider.seen] == ["MAPPING", "MINING", "MINING"]  # nothing after the failure
    assert _artifact(env, "project_change_v3_result") is None
    attempts = state["provenance"]["miner_attempts"]
    assert [(a["attempt"], a["validation"]) for a in attempts] == [(1, "REJECTED"), (2, "REJECTED")]
    assert len({a["model_visible_payload_sha256"] for a in attempts}) == 1


def test_graphic_crop_mismatch_is_the_same_traceability_rejection(env):
    def script(answer, attempt, _kwargs):
        if attempt == 1:
            graphic = next(e for e in answer["projectchanges"][0]["evidence_items"] if e["block_type"] == "GRAPHIC")
            graphic["crop_ref"] = "not/the/block/crop.png"
        return answer

    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": script})
    state = _run(env, provider)
    assert state["status"] == "REVIEW" and len(_r1(provider)) == 2
    assert state["provenance"]["miner_attempts"][0]["rejection_kind"] == "graphic_crop_mismatch"


@pytest.mark.parametrize("breaker, reason", [
    (lambda a: a["projectchanges"][0].update(evidence_items=[
        e for e in a["projectchanges"][0]["evidence_items"] if e["side"] == "OLD"]), "One-sided evidence"),
    (lambda a: a.update(region_id="R-999"), "Miner identity mismatch"),
])
def test_other_validation_failures_are_not_retried(env, breaker, reason):
    def script(answer, _attempt, _kwargs):
        breaker(answer)
        return answer

    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": script})
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "miner_validation_failed"
    assert reason in state["message"] and len(_r1(provider)) == 1
    assert state["provenance"]["miner_attempts"][0]["rejection_kind"] == "not_retryable"


@pytest.mark.parametrize("code", [
    "PERMANENT", "provider_gateway_error", "transport_integrity", "schema_validator_unavailable",
    "provider_cancelled", "schema_invalid",
])
def test_provider_failures_are_not_retried(env, code):
    from backend.app.services.project_change_v3.provider import ProviderError

    def script(_answer, _attempt, _kwargs):
        raise ProviderError(code, f"simulated {code}")

    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": script})
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == code and len(_r1(provider)) == 1


def test_a_retry_never_runs_on_a_changed_input(env):
    """If anything the model sees changed after attempt 1, attempt 2 is not sent at all."""
    def script(answer, attempt, kwargs):
        if attempt == 1:
            kwargs["data"]["frozen_region"]["scope"] = "mutated after the first call"
            answer["projectchanges"][0]["evidence_items"][0]["bbox"] = [0.0, 0.0, 1.0, 1.0]
        return answer

    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": script})
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "miner_retry_input_drift"
    assert len(_r1(provider)) == 1


# ── The real provider adapter through the gateway (fake Codex CLI) ─────────

@pytest.fixture
def fake_cli(tmp_path, monkeypatch):
    import sys
    import types

    try:
        import jsonschema  # noqa: F401
    except ImportError:  # dev interpreter: required-keys check; the release venv validates for real
        def validate(value, schema):
            missing = [k for k in schema.get("required", []) if k not in value]
            if missing:
                raise ValueError(f"missing {missing}")

        monkeypatch.setitem(sys.modules, "jsonschema", types.SimpleNamespace(validate=validate))
    log, answer = tmp_path / "codex.log", tmp_path / "answer.json"
    monkeypatch.setenv("STAGE_COMPARISON_AI_CODEX_BIN", str(FAKE))
    monkeypatch.setenv("STAGE_COMPARISON_AI_ENV_ALLOWLIST", "FAKE_CODEX_LOG,FAKE_CODEX_RESPONSE,FAKE_CODEX_MODE")
    monkeypatch.setenv("FAKE_CODEX_LOG", str(log))
    monkeypatch.setenv("FAKE_CODEX_RESPONSE", str(answer))
    monkeypatch.delenv("FAKE_CODEX_MODE", raising=False)
    return log, answer


def test_codex_provider_receipts_both_attempts_with_usage(env, fake_cli):
    """MINING goes through CodexProvider + gateway + fake CLI; receipts carry usage of both attempts."""
    from backend.app.services.project_change_v3.provider import CodexProvider

    log, answer_file = fake_cli
    handlers = gf.fake_handlers()
    codex = CodexProvider()

    class Hybrid:
        model, reasoning = codex.model, codex.reasoning
        last_transport = None
        mining = 0

        def complete(self, **kwargs):
            if kwargs["stage"] != "MINING":
                self.last_transport = None
                return handlers[kwargs["stage"]](**{k: kwargs[k] for k in ("call_id", "pair_id", "data",
                                                                           "schema", "images")})
            answer = handlers["MINING"](**{k: kwargs[k] for k in ("call_id", "pair_id", "data", "schema", "images")})
            if kwargs["call_id"] == R1:
                answer["projectchanges"][0]["evidence_items"][0]["bbox"] = [0.0, 0.0, 1.0, 1.0]
            answer_file.write_text(json.dumps(answer, ensure_ascii=False), encoding="utf-8")
            try:
                return codex.complete(**kwargs)
            finally:
                self.last_transport = codex.last_transport

    state = _run(env, Hybrid())
    assert state["status"] == "REVIEW"
    records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 3 and all(r["mode"] == "exec" and r["json_events"] for r in records)
    calls = state["provenance"]["transport_calls"]
    r1 = [c for c in calls if c["call_id"].startswith(R1)]
    assert [c["call_id"] for c in r1] == [R1, f"{R1}_RETRY_1"]
    assert r1[0]["model_visible_payload_sha256"] == r1[1]["model_visible_payload_sha256"] == records[0]["stdin_sha256"] \
        == records[1]["stdin_sha256"]
    assert all(c["transport"] == "codex_exec_stdin" and c["usage"]["output_tokens"] == 10 for c in calls)
    attempts = [a for a in state["provenance"]["miner_attempts"] if a["region_id"] == "R-001"]
    assert [a["usage"] for a in attempts] == [r1[0]["usage"], r1[1]["usage"]]
    assert all(a["transport"] == "codex_exec_stdin" and a["transport_payload_sha256"] == a["model_visible_payload_sha256"]
               for a in attempts)
    total = state["provenance"]["usage_total"]
    assert total["calls"] == total["calls_with_usage"] == 3
    assert total["input_tokens"] == sum(c["usage"]["input_tokens"] for c in calls)
    assert total["output_tokens"] == 30 and total["reasoning_output_tokens"] == 15


def test_json_events_answer_is_read_only_from_the_output_file(tmp_path, fake_cli, monkeypatch):
    from backend.app.services.stage_comparison.ai.gateway import call_codex, exec_event_usage

    log, answer_file = fake_cli
    answer_file.write_text(json.dumps({"ok": True}), encoding="utf-8")
    ok = call_codex("hello", model="m", retries=0, json_events=True)
    assert ok.ok and ok.parsed == {"ok": True} and ok.usage["output_tokens"] == 10
    plain = call_codex("hello", model="m", retries=0)
    assert plain.ok and plain.usage == {}
    assert [json.loads(line)["json_events"] for line in log.read_text(encoding="utf-8").splitlines()] == [True, False]
    monkeypatch.setenv("FAKE_CODEX_MODE", "exec_no_output")
    events_only = call_codex("hello", model="m", retries=0, json_events=True)
    assert not events_only.ok and events_only.parsed is None  # stdout events are never an answer
    assert exec_event_usage('{"type":"turn.completed","usage":{"input_tokens":7}}\nnoise\n') == {"input_tokens": 7}
