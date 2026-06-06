"""Tests for Qwen graphic-extraction resilience to transient ngrok/LM Studio
transport failures (incident 2026-06-06, pair ПОС).

Covers:
  * classify_describe_error — ngrok HTML 404 / ReadError / 5xx → transport;
    valid 400 → model; invalid_json (2xx) → content.
  * per-block transport retry recovers after 1–2 transient errors.
  * exhausted transport retry → final result tagged error_class="transport".
  * model/schema failure is NOT retried as transport (no infinite loop).
  * pair validation: transport errors → transient_llm_transport_failed,
    NOT error_blocks_over_threshold; recovered blocks don't trip the threshold;
    content errors over the threshold still fail as content.
  * _fail_qwen marks transient failures retryable with a human hint.
  * health gate: blocks start when LLM/ngrok unavailable; preflight advisory.
"""
import asyncio
import json

import pytest

import backend.app.services.stage_comparison.graphic_llm_local as g
import backend.app.services.stage_comparison.pipeline_queue as pq

DR = g.DescribeResult
NGROK_404 = (
    "<!DOCTYPE html><html class='h-full' lang='en-US'>"
    "<head><link href='https://assets.ngrok.com/fonts/...'></head>"
    "<body>Tunnel not found</body></html>"
)


def _cfg(**over):
    base = dict(
        provider="local_openai_compatible", base_url="https://t.ngrok-free.dev",
        model="qwen/qwen3.6-35b-a3b", fallback_model="qwen3.6-35b-a3b-mtp",
        temperature=0.0, max_tokens=1800, max_continuations=0, timeout_sec=60,
        image_long_side=1100, auth="basic", enable_model_load=True,
        load_context_length=16000, basic_user="u", basic_pass="p",
    )
    base.update(over)
    return g.LocalGraphicLLMConfig(**base)


# ─── 1. classification ──────────────────────────────────────────────────────
@pytest.mark.parametrize("result,expected", [
    (DR(status="error", error="http_404", full_raw_response=NGROK_404), "transport"),
    (DR(status="error", error="http_error:ReadError:"), "transport"),
    (DR(status="timeout", error="timeout:ReadTimeout"), "transport"),
    (DR(status="error", error="http_503", full_raw_response="Service Unavailable"), "transport"),
    (DR(status="error", error="http_502", full_raw_response="<html>502 Bad Gateway</html>"), "transport"),
    (DR(status="error", error="http_400", full_raw_response=""), "transport"),         # empty body
    (DR(status="error", error="http_400", full_raw_response='{"error":"bad param"}'), "model"),
    (DR(status="invalid_json", error="json_parse_failed", full_raw_response="1. Analyze..."), "content"),
    (DR(status="invalid_json", error="json_parse_failed", full_raw_response=NGROK_404), "transport"),
    (DR(status="done"), "ok"),
    (DR(status="partial"), "ok"),
    (DR(status="provider_unavailable", error="x"), "transport"),
    (None, "transport"),
])
def test_classify_describe_error(result, expected):
    assert g.classify_describe_error(result) == expected


def test_ngrok_html_detector():
    assert g._looks_like_transport_html(NGROK_404)
    assert g._looks_like_transport_html("<html>504 Gateway Timeout</html>")
    assert not g._looks_like_transport_html('{"choices":[]}')
    assert not g._looks_like_transport_html("")


# ─── 2. retry recovers after transient errors ───────────────────────────────
@pytest.mark.asyncio
async def test_retry_recovers_after_transient(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_RETRIES", "3")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_BACKOFF", "0")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_JITTER", "0")
    calls = {"n": 0}

    async def fake_once(**kw):
        calls["n"] += 1
        if calls["n"] <= 2:
            return DR(status="error", error="http_404", full_raw_response=NGROK_404), NGROK_404
        return DR(status="done", parsed={"ok": True}), '{"ok": true}'

    monkeypatch.setattr(g, "_describe_image_once", fake_once)
    res = await g._describe_with_retry_and_fallback(
        img_url="x", prompt="p", cfg=_cfg(), primary_model="qwen/qwen3.6-35b-a3b",
        fallback_used_hint=False, allow_fallback=True, pinned_model=None, stream=False,
    )
    assert res.status == "done"
    assert calls["n"] == 3  # 2 transient + 1 success


# ─── 3. exhausted transport retry → tagged transport ────────────────────────
@pytest.mark.asyncio
async def test_retry_exhausted_marks_transport(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_RETRIES", "2")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_BACKOFF", "0")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_JITTER", "0")
    calls = {"n": 0}

    async def fake_once(**kw):
        calls["n"] += 1
        return DR(status="error", error="http_404", full_raw_response=NGROK_404), NGROK_404

    monkeypatch.setattr(g, "_describe_image_once", fake_once)
    res = await g._describe_with_retry_and_fallback(
        img_url="x", prompt="p", cfg=_cfg(), primary_model="qwen/qwen3.6-35b-a3b",
        fallback_used_hint=False, allow_fallback=True, pinned_model=None, stream=False,
    )
    assert res.status == "error"
    assert res.error_class == "transport"
    assert calls["n"] == 3  # 1 + 2 retries (no fallback for transport)


# ─── 4. model/schema error is NOT transport-retried ─────────────────────────
@pytest.mark.asyncio
async def test_model_error_not_retried(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_RETRIES", "3")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TRANSPORT_BACKOFF", "0")
    calls = {"n": 0}

    async def fake_once(**kw):
        calls["n"] += 1
        return (DR(status="error", error="http_400",
                   full_raw_response='{"error":"invalid request"}'),
                '{"error":"invalid request"}')

    monkeypatch.setattr(g, "_describe_image_once", fake_once)
    # No fallback so we isolate the transport-retry decision (fallback only fires
    # on invalid_json anyway, not on a real 400).
    res = await g._describe_with_retry_and_fallback(
        img_url="x", prompt="p", cfg=_cfg(fallback_model=""),
        primary_model="qwen/qwen3.6-35b-a3b",
        fallback_used_hint=False, allow_fallback=False, pinned_model=None, stream=False,
    )
    assert res.status == "error"
    assert res.error_class == "model"
    assert calls["n"] == 1  # called exactly once — no transport retry


# ─── 5. pair validation transport-awareness ─────────────────────────────────
def _write_side(base, sid, pid, side, items):
    te = base / "pairs" / pid / "text_enrichment"
    te.mkdir(parents=True, exist_ok=True)
    (te / f"{side}_image_descriptions.json").write_text(
        json.dumps({"items": items}), encoding="utf-8")


@pytest.fixture
def _pq_paths(tmp_path, monkeypatch):
    base = tmp_path / "sess"
    monkeypatch.setattr(pq.paths_mod, "pair_dir", lambda s, pid: base / "pairs" / pid)
    return "s1", base


def test_validation_transport_errors_are_transient(_pq_paths):
    sid, base = _pq_paths
    # 4 blocks: 3 transport-failed (75% > 25%), 1 ok. Content errors = 0.
    items = [
        {"status": "error", "error_class": "transport", "transport_error": True},
        {"status": "error", "error": "http_404", "parse_error_detail": "http_error"},
        {"status": "error", "error": "http_error:ReadError:"},
        {"status": "done", "block_type": "photo_or_general"},
    ]
    _write_side(base, sid, "p1", "left", items)
    _write_side(base, sid, "p1", "right", [])
    ok, reason, metrics = pq._validate_qwen_pair(sid, "p1")
    assert not ok
    assert reason == "transient_llm_transport_failed"
    assert metrics["transport_error_blocks"] == 3
    assert metrics["content_error_blocks"] == 0


def test_validation_content_errors_still_fail(_pq_paths):
    sid, base = _pq_paths
    # 2 content errors out of 4 (50% > 25%) → genuine content fail.
    items = [
        {"status": "error", "error_class": "content", "parse_error_detail": "markdown_reasoning"},
        {"status": "error", "error_class": "content", "parse_error_detail": "empty_content"},
        {"status": "done"}, {"status": "done"},
    ]
    _write_side(base, sid, "p1", "left", items)
    _write_side(base, sid, "p1", "right", [])
    ok, reason, _ = pq._validate_qwen_pair(sid, "p1")
    assert not ok
    assert reason == "error_blocks_over_threshold"


def test_validation_recovered_blocks_pass(_pq_paths):
    sid, base = _pq_paths
    # All blocks recovered (done) → no error, passes even though they once flaked.
    items = [{"status": "done", "block_type": "photo_or_general"} for _ in range(5)]
    _write_side(base, sid, "p1", "left", items)
    _write_side(base, sid, "p1", "right", [])
    ok, reason, _ = pq._validate_qwen_pair(sid, "p1")
    assert ok and reason == "ok"


def test_is_transport_error_block_heuristic():
    assert pq._is_transport_error_block({"transport_error": True})
    assert pq._is_transport_error_block({"error_class": "transport"})
    assert pq._is_transport_error_block({"error": "http_error:ReadError:"})
    assert pq._is_transport_error_block({"parse_error_detail": "http_error"})
    assert not pq._is_transport_error_block({"error_class": "content"})
    assert not pq._is_transport_error_block({"parse_error_detail": "markdown_reasoning"})


# ─── 6. _fail_qwen marks transient retryable ────────────────────────────────
def test_fail_qwen_marks_transport_retryable():
    job = {"qwen_worker": {"failed": 0}, "queues": {"failed": []}}
    it = {"pair_id": "p1", "warnings": []}
    pq._fail_qwen(job, it, "validation:transient_llm_transport_failed")
    assert it["retryable"] is True
    assert it["failure_class"] == "transport"
    assert "safe to retry" in it["problem_hint"].lower()


def test_fail_qwen_content_not_retryable():
    job = {"qwen_worker": {"failed": 0}, "queues": {"failed": []}}
    it = {"pair_id": "p1"}
    pq._fail_qwen(job, it, "validation:error_blocks_over_threshold")
    assert it["retryable"] is False
    assert it["failure_class"] == "content"


# ─── 7. health gate ─────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_health_gate_ok():
    async def probe():
        return {"ok": True, "reason": "ok", "details": {"loaded_models_count": 1}}
    res = await pq.qwen_health_gate(probe_fn=probe)
    assert res["ok"] is True


@pytest.mark.asyncio
async def test_health_gate_no_model():
    async def probe():
        return {"ok": False, "reason": "no_model_loaded", "details": {}}
    res = await pq.qwen_health_gate(probe_fn=probe)
    assert res["ok"] is False
    assert res["reason"] == "no_model_loaded"


@pytest.mark.asyncio
async def test_health_gate_probe_raises_is_soft():
    async def probe():
        raise RuntimeError("boom")
    res = await pq.qwen_health_gate(probe_fn=probe)
    assert res["ok"] is False
    assert "probe_error" in res["reason"]


@pytest.mark.asyncio
async def test_probe_qwen_health_no_model(monkeypatch):
    # endpoint up, but no model loaded → not ok.
    async def fake_diag(cfg=None):
        return {"endpoint_available": True, "loaded_models": [], "ctx_ok": False,
                "fast_profile_ok": False, "primary_loaded_ctx": None}
    monkeypatch.setattr(g, "loaded_models_diagnostics", fake_diag)
    monkeypatch.setattr(g, "check_local_graphic_llm_available", lambda cfg: (True, None))
    res = await g.probe_qwen_health(cfg=_cfg(), do_live_test=False)
    assert res["ok"] is False
    assert res["reason"] == "no_model_loaded"


@pytest.mark.asyncio
async def test_probe_qwen_health_ok_with_live(monkeypatch):
    async def fake_diag(cfg=None):
        return {"endpoint_available": True, "loaded_models": [{"model_key": "qwen"}],
                "ctx_ok": True, "fast_profile_ok": True, "primary_loaded_ctx": 16000}

    async def fake_live(cfg):
        return {"ok": True, "reason": "ok", "status_code": 200}

    monkeypatch.setattr(g, "loaded_models_diagnostics", fake_diag)
    monkeypatch.setattr(g, "check_local_graphic_llm_available", lambda cfg: (True, None))
    monkeypatch.setattr(g, "_live_completion_probe", fake_live)
    res = await g.probe_qwen_health(cfg=_cfg(), do_live_test=True)
    assert res["ok"] is True


@pytest.mark.asyncio
async def test_probe_qwen_health_live_html_fails(monkeypatch):
    async def fake_diag(cfg=None):
        return {"endpoint_available": True, "loaded_models": [{"model_key": "qwen"}],
                "ctx_ok": True, "fast_profile_ok": True, "primary_loaded_ctx": 16000}

    async def fake_live(cfg):
        return {"ok": False, "reason": "http_404_html", "status_code": 404}

    monkeypatch.setattr(g, "loaded_models_diagnostics", fake_diag)
    monkeypatch.setattr(g, "check_local_graphic_llm_available", lambda cfg: (True, None))
    monkeypatch.setattr(g, "_live_completion_probe", fake_live)
    res = await g.probe_qwen_health(cfg=_cfg(), do_live_test=True)
    assert res["ok"] is False
    assert "live_test_failed" in res["reason"]
