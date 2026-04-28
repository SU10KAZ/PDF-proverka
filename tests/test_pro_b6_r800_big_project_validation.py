from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


def test_audit_set_selection_enforces_6_heavy_4_normal_2_risky():
    from run_pro_b6_r800_big_project_validation import select_audit_set

    blocks = []
    for idx in range(8):
        blocks.append(
            {
                "block_id": f"H{idx}",
                "is_full_page": True,
                "size_kb": 500 - idx,
                "ocr_text_len": 1200 - idx,
                "page": idx + 1,
            }
        )
    for idx in range(8):
        blocks.append(
            {
                "block_id": f"N{idx}",
                "size_kb": 650 - idx,
                "ocr_text_len": 1100 - idx,
                "page": idx + 20,
            }
        )
    for idx in range(3):
        blocks.append(
            {
                "block_id": f"L{idx}",
                "size_kb": 50 - idx,
                "ocr_text_len": 150 - idx,
                "page": idx + 40,
            }
        )

    audit_ids, rows = select_audit_set(
        blocks,
        historical_problematic_ids=["N6", "L1", "H1"],
    )

    assert len(audit_ids) == 12
    assert len({row["block_id"] for row in rows if row["selection_bucket"] == "heavy"}) == 6
    assert len({row["block_id"] for row in rows if row["selection_bucket"] == "normal_dense"}) == 4
    assert len({row["block_id"] for row in rows if row["selection_bucket"] == "historical_risky"}) == 2


def test_smoke_early_stop_blocks_full_run_after_quality_failure():
    from run_pro_b6_r800_big_project_validation import should_continue_after_smoke

    ok, reason = should_continue_after_smoke(
        {"hard_gate_passed": True, "quality_gate_passed": False}
    )
    assert ok is False
    assert "quality-preservation" in reason


def test_budget_stop_logic_blocks_over_cap_step():
    from run_pro_b6_r800_big_project_validation import BudgetTracker

    budget = BudgetTracker(cap_usd=1.0)
    event = budget.preflight(step_id="reference", label="reference", predicted_usd=0.7)
    assert event.approved is True
    budget.commit(event, 0.65)

    next_event = budget.preflight(step_id="full", label="full", predicted_usd=0.5)
    assert next_event.approved is False
    assert budget.stopped is True
    assert "exceeds remaining" in budget.stop_reason


def test_recommendation_logic_requires_smoke_and_full_survival():
    from run_pro_b6_r800_big_project_validation import BudgetTracker, recommend_validation

    budget = BudgetTracker(cap_usd=9.0)
    rec = recommend_validation(
        smoke_result={"survived": True},
        full_result={
            "performed": True,
            "hard_gate_passed": True,
            "audit_quality_gate_passed": True,
        },
        budget=budget,
    )
    assert rec["practical_big_project_config"] is True
    assert rec["recommended_profile"] == "b6 + r800"

    rec2 = recommend_validation(
        smoke_result={"survived": False},
        full_result=None,
        budget=budget,
    )
    assert rec2["practical_big_project_config"] is False
    assert rec2["next_step_needed"] is True


def test_reference_reuse_reads_prior_exact_config_artifact(tmp_path: Path):
    from run_pro_b6_r800_big_project_validation import (
        EXP_NAME,
        PDF_NAME,
        find_reusable_reference_outputs,
    )

    project_dir = tmp_path / "projects" / "214. Alia (ASTERUS)" / "KJ" / PDF_NAME
    run_dir = project_dir / "_experiments" / EXP_NAME / "20260423_120000"
    run_dir.mkdir(parents=True)

    summary = {
        "model_id": "google/gemini-3.1-pro-preview",
        "mode": "single_block",
        "reasoning_effort": "high",
        "parallelism": 2,
        "response_healing_initial": True,
        "resolution": "r800",
    }
    analyses = {
        "B1": {"block_id": "B1", "summary": "s1"},
        "B2": {"block_id": "B2", "summary": "s2"},
    }
    (run_dir / "reference_run_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (run_dir / "reference_analyses.json").write_text(json.dumps(analyses), encoding="utf-8")

    reused, sources, inspected = find_reusable_reference_outputs(project_dir, ["B1", "B3"])
    assert reused == {"B1": {"block_id": "B1", "summary": "s1"}}
    assert sources["B1"] == str(run_dir)
    assert inspected == [str(run_dir)]
