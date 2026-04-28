"""
Unit tests for OpenRouter stage 02 experimental path.

Tests cover:
- model pricing / _MODEL_PRICES includes gemini-2.5-flash
- strict schema loading + response_format construction
- response-healing / provider.require_parameters wiring (via extra_body)
- completeness checks (missing / duplicates / extra)
- single-block block_id inference (allowed only 1-in-1-out)
- usage/cost extraction from OpenRouter response (actual vs estimated)
- byte-cap splitter (deterministic)
- Phase A / B / C winner rules
- production defaults unchanged when experiment flags absent

No real OpenRouter calls; all I/O mocked.
"""
from __future__ import annotations

import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# ──────────────────────────────────────────────────────────────────────────
# 1. Model pricing: gemini-2.5-flash supported
# ──────────────────────────────────────────────────────────────────────────

def test_model_prices_has_gemini_flash():
    from webapp.services.llm_runner import _MODEL_PRICES, _estimate_cost
    assert "google/gemini-2.5-flash" in _MODEL_PRICES
    prices = _MODEL_PRICES["google/gemini-2.5-flash"]
    # Flash должен быть существенно дешевле Pro
    pro = _MODEL_PRICES["google/gemini-3.1-pro-preview"]
    assert prices["input"] < pro["input"]
    assert prices["output"] < pro["output"]


def test_estimate_cost_flash_matches_table():
    from webapp.services.llm_runner import _estimate_cost
    # 1M input + 1M output → input_price + output_price USD
    cost = _estimate_cost("google/gemini-2.5-flash", 1_000_000, 1_000_000)
    assert cost == pytest.approx(0.30 + 2.50, rel=0.01)


def test_estimate_cost_unknown_model_returns_zero():
    from webapp.services.llm_runner import _estimate_cost
    assert _estimate_cost("unknown/model", 1000, 1000) == 0.0


def test_available_models_includes_openrouter_flash():
    from webapp.config import AVAILABLE_MODELS
    ids = [m["id"] for m in AVAILABLE_MODELS]
    assert "google/gemini-2.5-flash" in ids
    flash_entry = next(m for m in AVAILABLE_MODELS if m["id"] == "google/gemini-2.5-flash")
    assert flash_entry["provider"] == "openrouter"


def test_stage_model_restrictions_includes_openrouter_flash():
    from webapp.config import STAGE_MODEL_RESTRICTIONS
    assert "google/gemini-2.5-flash" in STAGE_MODEL_RESTRICTIONS["block_batch"]


# ──────────────────────────────────────────────────────────────────────────
# 2. Strict schema loading
# ──────────────────────────────────────────────────────────────────────────

def test_openrouter_strict_schema_loads():
    from webapp.services.openrouter_block_batch import load_openrouter_block_batch_schema
    schema = load_openrouter_block_batch_schema()
    assert schema["type"] == "object"
    # Top-level required fields
    assert "block_analyses" in schema["required"]
    # No $schema key (provider-incompatible)
    assert "$schema" not in schema
    # additionalProperties strictly false
    assert schema.get("additionalProperties") is False


def test_openrouter_strict_schema_block_items_structure():
    from webapp.services.openrouter_block_batch import load_openrouter_block_batch_schema
    schema = load_openrouter_block_batch_schema()
    items = schema["properties"]["block_analyses"]["items"]
    req = set(items["required"])
    # Semantic contract
    for fname in ("block_id", "page", "label", "summary", "key_values_read", "findings"):
        assert fname in req
    assert items.get("additionalProperties") is False


def test_openrouter_strict_schema_no_format_date_time():
    """Gemini strict mode rejects format:date-time; OpenRouter routes to providers."""
    from webapp.services.openrouter_block_batch import load_openrouter_block_batch_schema
    schema = load_openrouter_block_batch_schema()

    def _scan(node):
        if isinstance(node, dict):
            if node.get("format") == "date-time":
                return True
            return any(_scan(v) for v in node.values())
        if isinstance(node, list):
            return any(_scan(v) for v in node)
        return False

    assert not _scan(schema), "strict schema must NOT contain format:date-time"


# ──────────────────────────────────────────────────────────────────────────
# 3. run_llm extra_body wiring (response_format, plugins, provider)
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_llm_strict_schema_builds_json_schema_format():
    """When strict_schema is passed, response_format uses json_schema with strict=true."""
    from webapp.services import llm_runner

    class _FakeMsg:
        def __init__(self, content):
            self.content = content

    class _FakeChoice:
        def __init__(self):
            self.message = _FakeMsg('{"block_analyses": []}')
            self.finish_reason = "stop"

    class _FakeUsage:
        prompt_tokens = 100
        completion_tokens = 50
        prompt_tokens_details = types.SimpleNamespace(cached_tokens=0, cache_write_tokens=0)
        completion_tokens_details = types.SimpleNamespace(reasoning_tokens=0)
        cost = 0.01

    class _FakeResp:
        id = "resp-1"
        def __init__(self):
            self.choices = [_FakeChoice()]
            self.usage = _FakeUsage()

    captured = {}

    async def _fake_create(**kwargs):
        captured.update(kwargs)
        return _FakeResp()

    fake_client = MagicMock()
    fake_client.chat.completions.create = _fake_create

    with patch.object(llm_runner, "_get_client", return_value=fake_client):
        r = await llm_runner.run_llm(
            "block_batch",
            messages=[{"role": "user", "content": "hi"}],
            model_override="google/gemini-2.5-flash",
            strict_schema={"type": "object"},
            schema_name="block_batch",
            response_healing=True,
            require_parameters=True,
        )

    assert captured["response_format"]["type"] == "json_schema"
    assert captured["response_format"]["json_schema"]["name"] == "block_batch"
    assert captured["response_format"]["json_schema"]["strict"] is True
    assert captured["response_format"]["json_schema"]["schema"] == {"type": "object"}

    eb = captured["extra_body"]
    assert {"id": "response-healing"} in eb["plugins"]
    assert eb["provider"]["require_parameters"] is True

    assert r.cost_source == "actual"
    assert r.cost_usd == pytest.approx(0.01)
    assert r.response_id == "resp-1"
    assert r.finish_reason == "stop"


@pytest.mark.asyncio
async def test_run_llm_provider_data_collection_opt_in():
    """provider.data_collection is NOT set by default; only when explicitly requested."""
    from webapp.services import llm_runner

    class _FakeMsg:
        def __init__(self, content):
            self.content = content

    class _FakeResp:
        id = "r"
        choices = [types.SimpleNamespace(message=_FakeMsg("{}"), finish_reason="stop")]
        usage = None

    captured = {}
    async def _fake_create(**kwargs):
        captured.update(kwargs)
        return _FakeResp()

    fake_client = MagicMock()
    fake_client.chat.completions.create = _fake_create

    # Default: no data_collection present
    with patch.object(llm_runner, "_get_client", return_value=fake_client):
        await llm_runner.run_llm(
            "block_batch",
            messages=[{"role": "user", "content": "x"}],
            model_override="google/gemini-2.5-flash",
            require_parameters=True,
        )
    eb = captured.get("extra_body", {})
    assert "data_collection" not in eb.get("provider", {})

    # Explicit deny: should be present
    captured.clear()
    with patch.object(llm_runner, "_get_client", return_value=fake_client):
        await llm_runner.run_llm(
            "block_batch",
            messages=[{"role": "user", "content": "x"}],
            model_override="google/gemini-2.5-flash",
            provider_data_collection="deny",
        )
    assert captured["extra_body"]["provider"]["data_collection"] == "deny"


@pytest.mark.asyncio
async def test_run_llm_actual_cost_preferred_over_estimated():
    from webapp.services import llm_runner

    class _FakeUsage:
        prompt_tokens = 1000
        completion_tokens = 500
        prompt_tokens_details = types.SimpleNamespace(cached_tokens=0, cache_write_tokens=0)
        completion_tokens_details = types.SimpleNamespace(reasoning_tokens=100)
        cost = 0.001234

    class _FakeResp:
        id = "r"
        choices = [types.SimpleNamespace(
            message=types.SimpleNamespace(content='{"ok": 1}'),
            finish_reason="stop",
        )]
        usage = _FakeUsage()

    async def _fake_create(**kw):
        return _FakeResp()

    fake_client = MagicMock()
    fake_client.chat.completions.create = _fake_create

    with patch.object(llm_runner, "_get_client", return_value=fake_client):
        r = await llm_runner.run_llm(
            "block_batch",
            messages=[{"role": "user", "content": "x"}],
            model_override="google/gemini-2.5-flash",
        )

    assert r.cost_source == "actual"
    assert r.cost_usd == pytest.approx(0.001234)
    assert r.reasoning_tokens == 100


@pytest.mark.asyncio
async def test_run_llm_falls_back_to_estimated_when_no_cost():
    from webapp.services import llm_runner

    class _FakeUsage:
        prompt_tokens = 1_000_000  # 1M
        completion_tokens = 0
        prompt_tokens_details = None
        completion_tokens_details = None
        cost = None

    class _FakeResp:
        id = "r"
        choices = [types.SimpleNamespace(
            message=types.SimpleNamespace(content='{}'),
            finish_reason="stop",
        )]
        usage = _FakeUsage()

    async def _fake_create(**kw):
        return _FakeResp()

    fake_client = MagicMock()
    fake_client.chat.completions.create = _fake_create

    with patch.object(llm_runner, "_get_client", return_value=fake_client):
        r = await llm_runner.run_llm(
            "block_batch",
            messages=[{"role": "user", "content": "x"}],
            model_override="google/gemini-2.5-flash",
        )

    # 1M prompt tokens @ 0.30/1M = 0.30
    assert r.cost_source == "estimated"
    assert r.cost_usd == pytest.approx(0.30, rel=0.01)


# ──────────────────────────────────────────────────────────────────────────
# 4. Completeness checks
# ──────────────────────────────────────────────────────────────────────────

def test_completeness_all_present():
    from webapp.services.openrouter_block_batch import check_completeness
    r = check_completeness(
        ["A", "B", "C"],
        {"block_analyses": [
            {"block_id": "A"}, {"block_id": "B"}, {"block_id": "C"},
        ]},
    )
    assert r.ok
    assert r.missing == []
    assert r.duplicates == []
    assert r.extra == []


def test_completeness_missing_detected():
    from webapp.services.openrouter_block_batch import check_completeness
    r = check_completeness(
        ["A", "B", "C"],
        {"block_analyses": [{"block_id": "A"}]},
    )
    assert set(r.missing) == {"B", "C"}
    assert not r.ok


def test_completeness_duplicate_detected():
    from webapp.services.openrouter_block_batch import check_completeness
    r = check_completeness(
        ["A", "B"],
        {"block_analyses": [
            {"block_id": "A"}, {"block_id": "A"}, {"block_id": "B"},
        ]},
    )
    assert r.duplicates == ["A"]
    assert not r.ok


def test_completeness_extra_detected():
    from webapp.services.openrouter_block_batch import check_completeness
    r = check_completeness(
        ["A"],
        {"block_analyses": [
            {"block_id": "A"}, {"block_id": "Z"},
        ]},
    )
    assert r.extra == ["Z"]
    assert not r.ok


def test_completeness_no_block_analyses_means_all_missing():
    from webapp.services.openrouter_block_batch import check_completeness
    r = check_completeness(["A", "B"], {})
    assert set(r.missing) == {"A", "B"}


# ──────────────────────────────────────────────────────────────────────────
# 5. Single-block block_id inference
# ──────────────────────────────────────────────────────────────────────────

def test_single_block_inference_allowed_when_1_in_1_out_empty_bid():
    from webapp.services.openrouter_block_batch import check_completeness
    data = {"block_analyses": [
        {
            # block_id missing
            "page": 4,
            "label": "Plan",
            "summary": "ok",
            "key_values_read": [],
            "findings": [],
        }
    ]}
    r = check_completeness(["A"], data, single_block_inference=True)
    assert r.block_id_inferred_from_input is True
    assert r.inferred_block_id_count == 1
    assert r.ok
    assert data["block_analyses"][0]["block_id"] == "A"


def test_single_block_inference_disabled_in_multi_block_request():
    """2+ input blocks => no inference even if response has 1 empty id."""
    from webapp.services.openrouter_block_batch import check_completeness
    data = {"block_analyses": [
        {"page": 4, "label": "L", "summary": "s", "key_values_read": [], "findings": []}
    ]}
    r = check_completeness(["A", "B"], data, single_block_inference=True)
    assert r.block_id_inferred_from_input is False
    assert set(r.missing) == {"A", "B"}


def test_single_block_inference_skipped_if_fields_invalid():
    """If summary is empty or fields missing → no inference."""
    from webapp.services.openrouter_block_batch import check_completeness
    data = {"block_analyses": [
        {"page": 4, "label": "L", "summary": "", "key_values_read": [], "findings": []}
    ]}
    r = check_completeness(["A"], data, single_block_inference=True)
    assert r.block_id_inferred_from_input is False


def test_single_block_inference_skipped_when_flag_false():
    from webapp.services.openrouter_block_batch import check_completeness
    data = {"block_analyses": [
        {"page": 4, "label": "L", "summary": "ok", "key_values_read": [], "findings": []}
    ]}
    r = check_completeness(["A"], data, single_block_inference=False)
    assert r.block_id_inferred_from_input is False
    assert r.missing == ["A"]


# ──────────────────────────────────────────────────────────────────────────
# 6. Byte-cap splitter
# ──────────────────────────────────────────────────────────────────────────

def test_splitter_small_batch_unchanged():
    from webapp.services.openrouter_block_batch import split_batch_by_byte_cap
    blocks = [{"block_id": f"B{i}", "size_kb": 100} for i in range(3)]
    out = split_batch_by_byte_cap(blocks, byte_cap_kb=9000, hard_cap=12)
    assert out == [blocks]


def test_splitter_over_byte_cap_splits_deterministically():
    from webapp.services.openrouter_block_batch import split_batch_by_byte_cap
    blocks = [{"block_id": f"B{i}", "size_kb": 3000} for i in range(4)]  # 12000 KB total
    out = split_batch_by_byte_cap(blocks, byte_cap_kb=9000, hard_cap=12)
    # 3 blocks fit into 9000 KB (3×3000=9000), 4th opens new batch
    assert len(out) == 2
    assert [b["block_id"] for b in out[0]] == ["B0", "B1", "B2"]
    assert [b["block_id"] for b in out[1]] == ["B3"]


def test_splitter_respects_hard_cap():
    from webapp.services.openrouter_block_batch import split_batch_by_byte_cap
    blocks = [{"block_id": f"B{i}", "size_kb": 10} for i in range(15)]  # tiny
    out = split_batch_by_byte_cap(blocks, byte_cap_kb=9000, hard_cap=12)
    assert len(out) == 2
    assert len(out[0]) == 12
    assert len(out[1]) == 3


def test_splitter_empty_input_returns_empty():
    from webapp.services.openrouter_block_batch import split_batch_by_byte_cap
    assert split_batch_by_byte_cap([], byte_cap_kb=9000) == []


def test_splitter_default_limits_from_config():
    from webapp.services.openrouter_block_batch import split_batch_by_byte_cap
    from webapp.config import OPENROUTER_STAGE02_HARD_CAP_BLOCKS
    assert OPENROUTER_STAGE02_HARD_CAP_BLOCKS == 12


# ──────────────────────────────────────────────────────────────────────────
# 7. Phase A quality gate
# ──────────────────────────────────────────────────────────────────────────

def _make_metrics(**kw):
    from scripts.run_gemini_openrouter_stage02_experiment import RunMetrics
    m = RunMetrics()
    for k, v in kw.items():
        setattr(m, k, v)
    return m


def test_phase_a_gate_flash_passes_when_quality_ok_and_cost_lower():
    from scripts.run_gemini_openrouter_stage02_experiment import apply_phase_a_gate, MODEL_FLASH, MODEL_PRO
    flash = _make_metrics(
        model_id=MODEL_FLASH,
        coverage_pct=100.0, missing_count=0, duplicate_count=0, extra_count=0,
        unreadable_count=2, blocks_with_findings=20, total_findings=30,
        median_key_values=9.0, cost_per_valid_block=0.0010,
    )
    pro = _make_metrics(
        model_id=MODEL_PRO,
        coverage_pct=100.0, missing_count=0, unreadable_count=3,
        blocks_with_findings=20, total_findings=30,
        median_key_values=9.0, cost_per_valid_block=0.0050,
    )
    mainline, fallback, md = apply_phase_a_gate(flash, pro)
    assert mainline == MODEL_FLASH
    assert fallback == MODEL_PRO
    assert "PASSED" in md


def test_phase_a_gate_flash_fails_on_findings_deficit():
    from scripts.run_gemini_openrouter_stage02_experiment import apply_phase_a_gate, MODEL_FLASH, MODEL_PRO
    flash = _make_metrics(
        coverage_pct=100.0, unreadable_count=2,
        blocks_with_findings=10, total_findings=12,  # <95% of pro
        median_key_values=9.0, cost_per_valid_block=0.001,
    )
    pro = _make_metrics(
        coverage_pct=100.0, unreadable_count=2,
        blocks_with_findings=20, total_findings=30,
        median_key_values=9.0, cost_per_valid_block=0.005,
    )
    mainline, fallback, md = apply_phase_a_gate(flash, pro)
    assert mainline == MODEL_PRO
    assert "FAILED" in md


def test_phase_a_gate_flash_fails_on_coverage():
    from scripts.run_gemini_openrouter_stage02_experiment import apply_phase_a_gate, MODEL_PRO
    flash = _make_metrics(
        coverage_pct=98.0, missing_count=1, unreadable_count=0,
        blocks_with_findings=20, total_findings=30, median_key_values=9.0,
        cost_per_valid_block=0.001,
    )
    pro = _make_metrics(
        coverage_pct=100.0, blocks_with_findings=20, total_findings=30,
        median_key_values=9.0, cost_per_valid_block=0.005,
    )
    mainline, _, md = apply_phase_a_gate(flash, pro)
    assert mainline == MODEL_PRO


def test_phase_a_gate_flash_fails_when_cost_not_substantially_lower():
    from scripts.run_gemini_openrouter_stage02_experiment import apply_phase_a_gate, MODEL_PRO
    flash = _make_metrics(
        coverage_pct=100.0, unreadable_count=2,
        blocks_with_findings=20, total_findings=30, median_key_values=9.0,
        cost_per_valid_block=0.0045,  # only 10% cheaper than pro
    )
    pro = _make_metrics(
        coverage_pct=100.0, unreadable_count=2,
        blocks_with_findings=20, total_findings=30, median_key_values=9.0,
        cost_per_valid_block=0.0050,
    )
    mainline, _, md = apply_phase_a_gate(flash, pro)
    assert mainline == MODEL_PRO


# ──────────────────────────────────────────────────────────────────────────
# 8. Phase B / C winner rules
# ──────────────────────────────────────────────────────────────────────────

def test_phase_b_winner_picks_lowest_cost_with_full_coverage():
    from scripts.run_gemini_openrouter_stage02_experiment import select_batch_profile_winner
    a = _make_metrics(batch_profile="b6",  coverage_pct=100, cost_per_valid_block=0.004, elapsed_s=100)
    b = _make_metrics(batch_profile="b8",  coverage_pct=100, cost_per_valid_block=0.003, elapsed_s=120)
    c = _make_metrics(batch_profile="b10", coverage_pct=100, cost_per_valid_block=0.005, elapsed_s=80)
    w, r = select_batch_profile_winner([a, b, c])
    assert w == "b8"  # lowest cost/block with 100% coverage


def test_phase_b_winner_skips_incomplete_coverage():
    from scripts.run_gemini_openrouter_stage02_experiment import select_batch_profile_winner
    a = _make_metrics(batch_profile="b6", coverage_pct=90, missing_count=5, cost_per_valid_block=0.001, elapsed_s=50)
    b = _make_metrics(batch_profile="b8", coverage_pct=100, cost_per_valid_block=0.003, elapsed_s=120)
    w, _ = select_batch_profile_winner([a, b])
    assert w == "b8"


def test_phase_c_parallelism_winner_prefers_lowest_elapsed_at_full_coverage():
    from scripts.run_gemini_openrouter_stage02_experiment import select_parallelism_winner
    x = _make_metrics(parallelism=2, batch_profile="b8", coverage_pct=100, failed_batches=0, elapsed_s=200, cost_per_valid_block=0.003)
    y = _make_metrics(parallelism=3, batch_profile="b8", coverage_pct=100, failed_batches=0, elapsed_s=120, cost_per_valid_block=0.003)
    z = _make_metrics(parallelism=4, batch_profile="b8", coverage_pct=100, failed_batches=1, elapsed_s=110, cost_per_valid_block=0.003)
    para, prof = select_parallelism_winner([x, y, z])
    assert para == 3  # lower failed_batches ties-broken to 3


def test_phase_c_parallelism_winner_rejects_high_failure_rate():
    from scripts.run_gemini_openrouter_stage02_experiment import select_parallelism_winner
    x = _make_metrics(parallelism=3, batch_profile="b8", coverage_pct=100, failed_batches=0, elapsed_s=150, cost_per_valid_block=0.003)
    y = _make_metrics(parallelism=4, batch_profile="b8", coverage_pct=95, failed_batches=3, elapsed_s=120, cost_per_valid_block=0.003)
    para, _ = select_parallelism_winner([x, y])
    assert para == 3  # rejects higher-failure option


# ──────────────────────────────────────────────────────────────────────────
# 9. Production defaults unchanged
# ──────────────────────────────────────────────────────────────────────────

def test_production_stage_model_block_batch_not_auto_switched():
    """The persisted stage_model for block_batch must NOT be auto-set to Flash/Pro OpenRouter by config import."""
    import importlib
    from webapp import config as cfg
    importlib.reload(cfg)
    model = cfg.get_stage_model("block_batch")
    # Production winner is claude-opus-4-7; must NOT have been flipped by import.
    # (If someone manually set something else, just assert it wasn't auto-flipped to flash.)
    assert model != "google/gemini-2.5-flash", (
        "block_batch stage model was auto-switched to Flash — this should only happen manually."
    )


def test_openrouter_flash_price_entry_unchanged_by_experiment_run():
    """Experiment run must not mutate _MODEL_PRICES."""
    from webapp.services.llm_runner import _MODEL_PRICES
    before = dict(_MODEL_PRICES["google/gemini-2.5-flash"])
    # Simulate module re-import (happens in CI sometimes)
    import importlib, webapp.services.llm_runner as lr
    importlib.reload(lr)
    after = dict(lr._MODEL_PRICES["google/gemini-2.5-flash"])
    assert before == after


def test_config_stage02_byte_cap_default():
    from webapp.config import OPENROUTER_STAGE02_RAW_BYTE_CAP_KB
    assert OPENROUTER_STAGE02_RAW_BYTE_CAP_KB == 9000


def test_config_stage02_hard_cap_default():
    from webapp.config import OPENROUTER_STAGE02_HARD_CAP_BLOCKS
    assert OPENROUTER_STAGE02_HARD_CAP_BLOCKS == 12


# ──────────────────────────────────────────────────────────────────────────
# 10. Runner: error classification
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_openrouter_block_batch_handles_llm_error():
    """When run_llm returns is_error=True, result should mark all input_block_ids as missing."""
    from webapp.services import openrouter_block_batch as ob
    from webapp.models.usage import LLMResult

    async def _fake_run_llm(*a, **kw):
        return LLMResult(
            is_error=True,
            error_message="provider down",
            model="google/gemini-2.5-flash",
        )

    with patch.object(ob, "run_llm", _fake_run_llm):
        res = await ob.run_openrouter_block_batch(
            messages=[{"role": "user", "content": "x"}],
            input_block_ids=["A", "B"],
            model="google/gemini-2.5-flash",
        )
    assert res.is_error
    assert set(res.missing) == {"A", "B"}


@pytest.mark.asyncio
async def test_run_openrouter_block_batch_passes_strict_schema_by_default():
    """By default (strict_schema=True), run_llm should get the loaded schema."""
    from webapp.services import openrouter_block_batch as ob
    from webapp.models.usage import LLMResult

    captured = {}
    async def _fake_run_llm(*a, **kw):
        captured.update(kw)
        return LLMResult(
            text='{"block_analyses":[]}',
            json_data={"block_analyses": []},
            model="google/gemini-2.5-flash",
        )

    with patch.object(ob, "run_llm", _fake_run_llm):
        await ob.run_openrouter_block_batch(
            messages=[{"role": "user", "content": "x"}],
            input_block_ids=["A"],
            model="google/gemini-2.5-flash",
        )
    # strict_schema should be passed as a dict (not None, not False)
    assert captured.get("strict_schema") is not None
    assert isinstance(captured["strict_schema"], dict)
    assert captured.get("response_healing") is True
    assert captured.get("require_parameters") is True
