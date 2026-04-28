"""
Targeted tests for budget wrapper: BudgetTracker + weak-block selector.

No e2e, no real OpenRouter calls.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))


def test_budget_preflight_approves_within_cap():
    from run_gemini_openrouter_stage02_budget import BudgetTracker
    t = BudgetTracker(cap_usd=1.0)
    ev = t.preflight(phase="X", run_id="r1", planned_blocks=100,
                     planned_batches=10, per_block_usd=0.005)
    assert ev.approved
    assert ev.estimated_usd == pytest.approx(0.5)
    assert t.stopped is False


def test_budget_preflight_rejects_when_over_cap():
    from run_gemini_openrouter_stage02_budget import BudgetTracker
    t = BudgetTracker(cap_usd=0.3)
    ev = t.preflight(phase="X", run_id="r", planned_blocks=100,
                     planned_batches=10, per_block_usd=0.01)
    assert ev.approved is False
    assert t.stopped is True
    assert "exceeds remaining" in t.stop_reason


def test_budget_commit_deducts_actual_not_estimated():
    from run_gemini_openrouter_stage02_budget import BudgetTracker
    t = BudgetTracker(cap_usd=1.0)
    ev = t.preflight(phase="X", run_id="r1", planned_blocks=100,
                     planned_batches=10, per_block_usd=0.005)
    t.commit(ev, actual_usd=0.42)
    assert t.spent_usd == pytest.approx(0.42)
    assert ev.actual_usd == pytest.approx(0.42)
    assert ev.remaining_after == pytest.approx(0.58)


def test_budget_timeline_dict_roundtrip():
    from run_gemini_openrouter_stage02_budget import BudgetTracker
    t = BudgetTracker(cap_usd=2.0)
    ev = t.preflight(phase="B-lite", run_id="r", planned_blocks=60,
                     planned_batches=6, per_block_usd=0.003)
    t.commit(ev, actual_usd=0.17)
    d = t.to_dict()
    assert d["cap_usd"] == 2.0
    assert d["spent_usd"] == pytest.approx(0.17)
    assert d["remaining_usd"] == pytest.approx(1.83)
    assert len(d["events"]) == 1


def test_per_block_est_flash_cheaper_than_pro():
    from run_gemini_openrouter_stage02_budget import per_block_est
    assert per_block_est("google/gemini-2.5-flash", "batch") < per_block_est("google/gemini-3.1-pro-preview", "batch")
    assert per_block_est("google/gemini-2.5-flash", "single") < per_block_est("google/gemini-3.1-pro-preview", "single")


def test_weak_block_selector_picks_unreadable_first():
    from run_gemini_openrouter_stage02_budget import select_weak_blocks
    from run_gemini_openrouter_stage02_experiment import BatchResultEnvelope

    all_blocks = [
        {"block_id": "A", "page": 1, "size_kb": 100},
        {"block_id": "B", "page": 1, "size_kb": 100},
        {"block_id": "C", "page": 1, "size_kb": 100},
    ]
    # A: unreadable (score 3); B: empty KV + no findings (score 4); C: strong (score 0)
    res_AB = BatchResultEnvelope(
        batch_id=1, model_id="flash", input_block_ids=["A", "B"],
        parsed_data={
            "block_analyses": [
                {"block_id": "A", "unreadable_text": True,
                 "key_values_read": ["x", "y"], "findings": [{"id": "G-001"}], "summary": "ok"*20},
                {"block_id": "B", "unreadable_text": False,
                 "key_values_read": [], "findings": [], "summary": "ok"*20},
            ],
        },
    )
    res_C = BatchResultEnvelope(
        batch_id=2, model_id="flash", input_block_ids=["C"],
        parsed_data={
            "block_analyses": [
                {"block_id": "C", "unreadable_text": False,
                 "key_values_read": ["a", "b", "c", "d", "e"],
                 "findings": [{"id": "G-001"}, {"id": "G-002"}],
                 "summary": "A very long and detailed description" * 2},
            ],
        },
    )
    ids, blocks = select_weak_blocks([res_AB, res_C], all_blocks, sample_size=2)
    # A score=4 (unreadable 3 + short KV 1). B score=4 (empty KV 2 + no findings 2).
    # Both tied above C's low score; C must be excluded.
    assert set(ids) == {"A", "B"}
    assert "C" not in ids


def test_weak_block_selector_treats_errors_as_weakest():
    from run_gemini_openrouter_stage02_budget import select_weak_blocks
    from run_gemini_openrouter_stage02_experiment import BatchResultEnvelope

    all_blocks = [
        {"block_id": "A", "page": 1, "size_kb": 100},
        {"block_id": "B", "page": 1, "size_kb": 100},
    ]
    res_err = BatchResultEnvelope(
        batch_id=1, model_id="flash", input_block_ids=["A"],
        is_error=True, error_type="provider",
    )
    res_ok = BatchResultEnvelope(
        batch_id=2, model_id="flash", input_block_ids=["B"],
        parsed_data={
            "block_analyses": [{"block_id": "B", "unreadable_text": False,
                                "key_values_read": ["x"], "findings": [{"id": "G-1"}],
                                "summary": "good summary here" * 3}],
        },
    )
    ids, _ = select_weak_blocks([res_err, res_ok], all_blocks, sample_size=1)
    assert ids == ["A"]


def test_weak_block_selector_respects_sample_size():
    from run_gemini_openrouter_stage02_budget import select_weak_blocks
    from run_gemini_openrouter_stage02_experiment import BatchResultEnvelope

    all_blocks = [{"block_id": f"B{i}", "page": 1, "size_kb": 50} for i in range(20)]
    res = BatchResultEnvelope(
        batch_id=1, model_id="flash", input_block_ids=[f"B{i}" for i in range(20)],
        parsed_data={"block_analyses": [
            {"block_id": f"B{i}", "unreadable_text": False,
             "key_values_read": [], "findings": [], "summary": ""}
            for i in range(20)
        ]},
    )
    ids, _ = select_weak_blocks([res], all_blocks, sample_size=15)
    assert len(ids) == 15
