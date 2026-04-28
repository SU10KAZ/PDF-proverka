"""Focused tests for single-block runner: ranking + projection + diff."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))


def _make_envelope(bid_analyses: dict, input_ids: list[str] | None = None, inferred: int = 0):
    """Build a BatchResultEnvelope with given block analyses keyed by block_id."""
    from run_gemini_openrouter_stage02_experiment import BatchResultEnvelope
    return BatchResultEnvelope(
        batch_id=1,
        model_id="google/gemini-2.5-flash",
        input_block_ids=input_ids or list(bid_analyses.keys()),
        parsed_data={"block_analyses": list(bid_analyses.values())},
        inferred_block_id_count=inferred,
    )


# ────────────────────────── Weak-block scoring ────────────────────────────────

def test_score_weakness_missing_analysis_is_max():
    from run_gemini_openrouter_stage02_singleblock import score_weakness
    score, reasons = score_weakness({"block_id": "A"}, None, inferred=False)
    assert score == 100
    assert "block_missing_from_output" in reasons


def test_score_weakness_strong_block_scores_zero():
    from run_gemini_openrouter_stage02_singleblock import score_weakness
    a = {
        "summary": "A strong and detailed description " * 3,
        "key_values_read": ["v1", "v2", "v3", "v4", "v5"],
        "findings": [{"id": "G-001"}],
    }
    score, reasons = score_weakness({"block_id": "A", "size_kb": 50}, a, inferred=False)
    assert score == 0
    assert reasons == []


def test_score_weakness_no_findings_weighs_highest_alone():
    from run_gemini_openrouter_stage02_singleblock import (
        score_weakness, WEAK_WEIGHTS,
    )
    a = {
        "summary": "A strong and detailed description " * 3,
        "key_values_read": ["v1", "v2", "v3", "v4", "v5"],
        "findings": [],
    }
    score, reasons = score_weakness({"block_id": "A", "size_kb": 50}, a, inferred=False)
    assert score == WEAK_WEIGHTS["no_findings"]
    assert "no_findings" in reasons


def test_score_weakness_combines_multiple_signals():
    from run_gemini_openrouter_stage02_singleblock import score_weakness
    a = {"summary": "", "key_values_read": [], "findings": []}
    # no findings + empty summary + empty kv = 4 + 3 + 3 = 10
    score, reasons = score_weakness({"block_id": "A", "size_kb": 50}, a, inferred=False)
    assert score == 10
    assert set(["no_findings", "empty_summary", "empty_key_values"]).issubset(set(reasons))


def test_score_weakness_heavy_weak_bonus():
    from run_gemini_openrouter_stage02_singleblock import score_weakness
    a = {"summary": "", "key_values_read": [], "findings": []}
    # heavy block w/ weak output: extra +2
    block = {"block_id": "A", "size_kb": 5000, "is_full_page": True}
    score, reasons = score_weakness(block, a, inferred=False)
    assert "heavy_weak" in reasons
    # 4 + 3 + 3 + 2 = 12
    assert score == 12


def test_score_weakness_inferred_block_id_adds_point():
    from run_gemini_openrouter_stage02_singleblock import score_weakness
    a = {
        "summary": "strong summary" * 5,
        "key_values_read": ["a", "b", "c"],
        "findings": [{"id": "G-001"}],
    }
    score, reasons = score_weakness({"block_id": "A", "size_kb": 50}, a, inferred=True)
    assert "inferred_block_id" in reasons
    assert score == 1


# ────────────────────────── Rank ordering ─────────────────────────────────────

def test_rank_weak_blocks_orders_by_score_desc():
    from run_gemini_openrouter_stage02_singleblock import rank_weak_blocks
    all_blocks = [
        {"block_id": "strong", "page": 1, "size_kb": 50},
        {"block_id": "weak",   "page": 2, "size_kb": 50},
        {"block_id": "mid",    "page": 3, "size_kb": 50},
    ]
    envelope = _make_envelope({
        "strong": {"summary": "s" * 80, "key_values_read": ["a", "b", "c"],
                   "findings": [{"id": "G-1"}], "block_id": "strong"},
        "weak":   {"summary": "", "key_values_read": [], "findings": [],
                   "block_id": "weak"},
        "mid":    {"summary": "medium desc" * 3, "key_values_read": ["x"],
                   "findings": [], "block_id": "mid"},
    })
    ranked = rank_weak_blocks([envelope], all_blocks)
    order = [r["block_id"] for r in ranked]
    # weak > mid > strong
    assert order == ["weak", "mid", "strong"]


def test_rank_weak_blocks_missing_analysis_first():
    from run_gemini_openrouter_stage02_singleblock import rank_weak_blocks
    all_blocks = [
        {"block_id": "present", "page": 1, "size_kb": 50},
        {"block_id": "missing", "page": 2, "size_kb": 50},
    ]
    envelope = _make_envelope({
        "present": {"summary": "hello" * 20, "key_values_read": ["a"],
                    "findings": [{"id": "G-1"}], "block_id": "present"},
    })
    ranked = rank_weak_blocks([envelope], all_blocks)
    assert ranked[0]["block_id"] == "missing"
    assert "block_missing_from_output" in ranked[0]["reasons"]


# ────────────────────────── Pro vs Flash diff ─────────────────────────────────

def test_diff_improvement_counted():
    from run_gemini_openrouter_stage02_singleblock import diff_pro_vs_flash
    from run_gemini_openrouter_stage02_experiment import BatchResultEnvelope

    def _env(analyses):
        return BatchResultEnvelope(
            batch_id=1, model_id="m",
            input_block_ids=list(analyses.keys()),
            parsed_data={"block_analyses": list(analyses.values())},
        )

    flash = _env({
        "A": {"block_id": "A", "summary": "", "key_values_read": [], "findings": []},
    })
    pro = _env({
        "A": {"block_id": "A", "summary": "found",
              "key_values_read": ["x", "y"],
              "findings": [{"id": "G-1"}, {"id": "G-2"}]},
    })
    d = diff_pro_vs_flash([pro], [flash], ["A"])
    assert d["improved"] == 1
    assert d["degraded"] == 0
    assert d["unchanged"] == 0
    assert d["added_findings"] == 2
    assert d["added_kv"] == 2


def test_diff_degradation_counted():
    from run_gemini_openrouter_stage02_singleblock import diff_pro_vs_flash
    from run_gemini_openrouter_stage02_experiment import BatchResultEnvelope

    def _env(analyses):
        return BatchResultEnvelope(
            batch_id=1, model_id="m",
            input_block_ids=list(analyses.keys()),
            parsed_data={"block_analyses": list(analyses.values())},
        )

    flash = _env({
        "A": {"block_id": "A", "summary": "found",
              "key_values_read": ["x", "y"],
              "findings": [{"id": "G-1"}, {"id": "G-2"}]},
    })
    pro = _env({
        "A": {"block_id": "A", "summary": "found",
              "key_values_read": ["x"],
              "findings": []},
    })
    d = diff_pro_vs_flash([pro], [flash], ["A"])
    assert d["degraded"] == 1
    assert d["improved"] == 0


def test_diff_unchanged_counted():
    from run_gemini_openrouter_stage02_singleblock import diff_pro_vs_flash
    from run_gemini_openrouter_stage02_experiment import BatchResultEnvelope

    def _env(bid):
        return BatchResultEnvelope(
            batch_id=1, model_id="m",
            input_block_ids=[bid],
            parsed_data={"block_analyses": [{
                "block_id": bid, "summary": "ok",
                "key_values_read": ["x"],
                "findings": [{"id": "G-1"}],
            }]},
        )

    d = diff_pro_vs_flash([_env("A")], [_env("A")], ["A"])
    assert d["unchanged"] == 1


# ────────────────────────── Hybrid projection ─────────────────────────────────

def test_project_hybrid_basic_scales_linearly():
    from run_gemini_openrouter_stage02_singleblock import project_hybrid_policy
    from run_gemini_openrouter_stage02_experiment import RunMetrics

    flash_m = RunMetrics(
        total_input_blocks=100, cost_per_valid_block=0.003,
    )
    pro_m = RunMetrics(
        total_input_blocks=10, total_cost_usd=0.70,
    )
    # 15 blocks would trigger: 10 w/ findings=0 + kv<=2
    ranked_all = [
        {"block_id": f"W{i}", "findings_count": 0, "kv_count": 1,
         "analysis_present": True, "reasons": ["no_findings"]}
        for i in range(15)
    ] + [
        {"block_id": f"S{i}", "findings_count": 2, "kv_count": 5,
         "analysis_present": True, "reasons": []}
        for i in range(85)
    ]
    diff = {"sample_size": 10, "improved": 6, "unchanged": 3, "degraded": 1,
            "added_findings": 12, "added_kv": 10}
    proj = project_hybrid_policy(
        flash_full_m=flash_m, sample_diff=diff, sample_pro_m=pro_m,
        ranked_all=ranked_all, total_blocks=100,
    )
    assert proj["policy_triggered_count"] == 15
    assert proj["projected_flash_full_cost"] == pytest.approx(0.30)
    assert proj["projected_pro_escalation_cost"] == pytest.approx(1.05, abs=1e-3)  # 0.07 * 15
    assert proj["projected_hybrid_total_cost"] == pytest.approx(1.35, abs=1e-3)
    # 60% improve rate → 9 improved on 15
    assert proj["projected_improved_blocks"] == 9


def test_project_hybrid_zero_improvements():
    from run_gemini_openrouter_stage02_singleblock import project_hybrid_policy
    from run_gemini_openrouter_stage02_experiment import RunMetrics

    flash_m = RunMetrics(total_input_blocks=50, cost_per_valid_block=0.002)
    pro_m = RunMetrics(total_input_blocks=10, total_cost_usd=0.50)
    ranked = [{"block_id": "A", "findings_count": 1, "kv_count": 5,
               "analysis_present": True, "reasons": []}]
    diff = {"sample_size": 10, "improved": 0, "unchanged": 10, "degraded": 0,
            "added_findings": 0, "added_kv": 0}
    proj = project_hybrid_policy(
        flash_full_m=flash_m, sample_diff=diff, sample_pro_m=pro_m,
        ranked_all=ranked, total_blocks=50,
    )
    assert proj["projected_improved_blocks"] == 0
    assert proj["projected_extra_cost_per_improved"] is None


# ────────────────────────── Final recommendation gate ─────────────────────────

def test_recommendation_pass_gate():
    from run_gemini_openrouter_stage02_singleblock import build_final_recommendation
    from run_gemini_openrouter_stage02_experiment import RunMetrics
    from run_gemini_openrouter_stage02_budget import BudgetTracker

    flash_m = RunMetrics(
        total_input_blocks=215, coverage_pct=100.0,
        missing_count=0, duplicate_count=0, extra_count=0,
        total_findings=40, blocks_with_findings=20,
        total_key_values=3000, median_key_values=12,
        total_cost_usd=0.50, cost_per_valid_block=0.00233,
        elapsed_s=500, p95_batch_duration_s=30,
        model_id="google/gemini-2.5-flash",
    )
    pro_m = RunMetrics(
        total_input_blocks=15, coverage_pct=100,
        missing_count=0, duplicate_count=0, extra_count=0,
        total_cost_usd=1.1, total_findings=20,
    )
    diff = {"sample_size": 15, "improved": 10, "unchanged": 4, "degraded": 0,
            "unreadable_recovery": 1, "added_findings": 20, "added_kv": 30,
            "flash_total_findings": 0, "flash_total_kv": 50,
            "pro_total_findings": 20, "pro_total_kv": 80, "per_block_diff": []}
    projection = {
        "trigger_rule": "x", "projected_flash_full_cost": 0.5,
        "projected_pro_escalation_cost": 1.0, "projected_hybrid_total_cost": 1.5,
        "policy_triggered_count": 30, "total_blocks": 215, "triggered_pct": 14.0,
        "flash_cost_per_block": 0.002, "pro_cost_per_block_sample": 0.07,
        "projected_improved_blocks": 20, "projected_added_findings": 40,
        "projected_extra_cost_per_improved": 0.05,
        "sample_improved": 10, "sample_unchanged": 4, "sample_degraded": 0,
    }
    bt = BudgetTracker(cap_usd=2.5)
    md = build_final_recommendation(
        flash_m=flash_m, pro_m=pro_m, diff=diff, projection=projection,
        budget=bt, sample_size=15, total_blocks=215,
    )
    assert "Flash single-block + selective Pro escalation" in md
    assert "PASS" in md


def test_recommendation_flash_incomplete_rejects_mainline():
    from run_gemini_openrouter_stage02_singleblock import build_final_recommendation
    from run_gemini_openrouter_stage02_experiment import RunMetrics
    from run_gemini_openrouter_stage02_budget import BudgetTracker

    flash_m = RunMetrics(
        total_input_blocks=215, coverage_pct=92.0,
        missing_count=17, duplicate_count=0, extra_count=0,
        total_findings=10, total_key_values=500, median_key_values=5,
        total_cost_usd=0.5, cost_per_valid_block=0.003,
        elapsed_s=300, p95_batch_duration_s=20,
    )
    bt = BudgetTracker(cap_usd=2.5)
    md = build_final_recommendation(
        flash_m=flash_m, pro_m=None, diff=None, projection=None,
        budget=bt, sample_size=0, total_blocks=215,
    )
    assert "Do NOT adopt Gemini/OpenRouter" in md
