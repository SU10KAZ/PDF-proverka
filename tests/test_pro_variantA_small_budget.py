from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


def _block(block_id: str, risk: str, size_kb: float, ocr_len: int) -> dict:
    render_long = 2600 if risk == "heavy" else 900
    crop_long = 3200 if risk == "heavy" else 1800
    is_full_page = risk == "heavy"
    return {
        "block_id": block_id,
        "size_kb": size_kb,
        "ocr_text_len": ocr_len,
        "render_size": [render_long, 1000],
        "crop_px": [0, 0, crop_long, 1000],
        "is_full_page": is_full_page,
        "quadrant": None,
        "merged_block_ids": [],
        "ocr_label": f"label {block_id}",
        "page": 1,
    }


def test_select_stress_set_includes_problematic_and_quota_buckets():
    from run_pro_variantA_small_budget import select_stress_set

    blocks = [
        _block("H1", "heavy", 2000, 1400),
        _block("H2", "heavy", 1500, 1300),
        _block("H3", "heavy", 1200, 1200),
        _block("H4", "heavy", 1000, 1100),
        _block("H5", "heavy", 900, 1000),
        _block("H6", "heavy", 800, 900),
        _block("N1", "normal", 70, 1500),
        _block("N2", "normal", 60, 1400),
        _block("N3", "normal", 50, 1300),
        _block("P1", "normal", 40, 1200),
        _block("P2", "heavy", 950, 1250),
    ]

    selected, manifest = select_stress_set(
        blocks,
        preferred_problematic_ids=["P1", "P2"],
        extra_problematic_ids=["N3"],
    )

    assert len(selected) == 8
    assert "P1" in selected and "P2" in selected
    risks = {row["block_id"]: row["risk"] for row in manifest}
    assert sum(1 for bid in selected if risks[bid] == "heavy") >= 4
    assert sum(1 for bid in selected if risks[bid] == "normal") >= 2


def test_select_stress_set_prefers_nonheavy_fill_after_base_quotas():
    from run_pro_variantA_small_budget import select_stress_set

    blocks = [
        _block("H1", "heavy", 2000, 1400),
        _block("H2", "heavy", 1800, 1300),
        _block("H3", "heavy", 1600, 1200),
        _block("H4", "heavy", 1400, 1100),
        _block("H5", "heavy", 1200, 1000),
        _block("N1", "normal", 80, 1500),
        _block("N2", "normal", 70, 1400),
        _block("N3", "normal", 60, 1300),
        _block("N4", "normal", 50, 1200),
    ]

    selected, manifest = select_stress_set(
        blocks,
        preferred_problematic_ids=["H1", "N1"],
        extra_problematic_ids=["N4"],
    )

    risks = {row["block_id"]: row["risk"] for row in manifest}
    assert len(selected) == 8
    assert sum(1 for bid in selected if risks[bid] == "normal") >= 4
    assert "H5" not in selected


def test_select_resolution_set_picks_top_six_heavy():
    from run_pro_variantA_small_budget import select_resolution_set

    blocks = [
        _block("H1", "heavy", 2100, 1000),
        _block("H2", "heavy", 1800, 1000),
        _block("H3", "heavy", 1600, 1000),
        _block("H4", "heavy", 1400, 1000),
        _block("H5", "heavy", 1200, 1000),
        _block("H6", "heavy", 1000, 1000),
        _block("H7", "heavy", 800, 1000),
        _block("N1", "normal", 90, 900),
    ]
    selected, _ = select_resolution_set(blocks)
    assert selected == ["H1", "H2", "H3", "H4", "H5", "H6"]


def test_batch_early_stop_rules_gate_b4_and_b6():
    from run_pro_variantA_small_budget import should_continue_batch_study

    ok, _ = should_continue_batch_study([], "b2")
    assert ok is True

    ok, reason = should_continue_batch_study([], "b4")
    assert ok is False
    assert "b2" in reason

    rows = [{"profile_id": "b2", "survived": True}, {"profile_id": "b4", "survived": False}]
    ok, reason = should_continue_batch_study(rows, "b6")
    assert ok is False
    assert "b4" in reason


def test_quality_preservation_gate_rejects_findings_collapse_and_noisy_kv():
    from run_pro_variantA_small_budget import evaluate_quality_preservation

    side = [
        {
            "block_id": "A",
            "classification": "likely_degraded",
            "findings_presence_collapse": True,
            "kv_collapse": False,
            "generic_summary": False,
            "noisy_kv_inflation": False,
            "summary_specificity_improved": False,
            "kv_adequacy_improved": False,
            "reasons": ["findings_presence_collapse"],
        },
        {
            "block_id": "B",
            "classification": "likely_degraded",
            "findings_presence_collapse": False,
            "kv_collapse": False,
            "generic_summary": False,
            "noisy_kv_inflation": True,
            "summary_specificity_improved": False,
            "kv_adequacy_improved": False,
            "reasons": ["noisy_kv_inflation"],
        },
    ]
    gate = evaluate_quality_preservation(side)
    assert gate["quality_gate_passed"] is False
    assert "findings_presence_collapse" in gate["hard_fail_reasons"]


def test_budget_stop_logic_blocks_over_cap_step():
    from run_pro_variantA_small_budget import BudgetTracker

    budget = BudgetTracker(cap_usd=1.0)
    ev1 = budget.preflight(step_id="a", label="step a", predicted_usd=0.4)
    assert ev1.approved is True
    budget.commit(ev1, 0.3)

    ev2 = budget.preflight(step_id="b", label="step b", predicted_usd=0.8)
    assert ev2.approved is False
    assert budget.stopped is True


def test_winner_recommendation_falls_back_to_single_if_batch_confirmatory_fails():
    from run_pro_variantA_small_budget import recommend_final_config

    batch_rows = [
        {"profile_id": "b2", "survived": True},
        {"profile_id": "b4", "survived": True},
    ]
    confirmatory = {
        "coverage_pct": 92.0,
        "missing_count": 2,
        "duplicate_count": 0,
        "extra_count": 0,
        "fail_mode_distribution": {"missing": 1},
    }

    rec = recommend_final_config(
        best_resolution="r800",
        batch_rows=batch_rows,
        confirmatory_summary=confirmatory,
    )
    assert rec["candidate_before_confirmatory"]["batch_profile"] == "b4"
    assert rec["final_config"]["mode"] == "single_block"
