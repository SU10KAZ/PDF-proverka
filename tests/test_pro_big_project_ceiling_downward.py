from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


def test_reuse_of_audit_set_comes_from_existing_big_project_validation_artifact(tmp_path: Path):
    from run_pro_big_project_ceiling_downward import (
        PDF_NAME,
        REUSE_EXP_NAME,
        load_reused_audit_context,
    )

    project_dir = tmp_path / "projects" / "214. Alia (ASTERUS)" / "KJ" / PDF_NAME
    reuse_dir = project_dir / "_experiments" / REUSE_EXP_NAME / "20260422_210243"
    reuse_dir.mkdir(parents=True)

    audit_ids = [f"B{i}" for i in range(12)]
    manifest = {
        "timestamp": "20260422_210243",
        "pdf": PDF_NAME,
        "project_dir": str(project_dir),
        "provider": "OpenRouter",
        "model_id": "google/gemini-3.1-pro-preview",
        "resolution": "r800",
        "parallelism": 2,
        "response_healing_initial": True,
        "full_run_performed": False,
    }
    reference_summary = {
        "model_id": "google/gemini-3.1-pro-preview",
        "mode": "single_block",
        "reasoning_effort": "high",
        "parallelism": 2,
        "response_healing_initial": True,
        "resolution": "r800",
        "coverage_pct": 100.0,
        "missing_block_ids": [],
        "requested_block_ids": audit_ids,
    }
    reference_analyses = {block_id: {"block_id": block_id, "summary": f"s-{block_id}"} for block_id in audit_ids}
    smoke_payload = {"summary_row": {"total_cost_usd": 0.24, "survived": False}}

    (reuse_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (reuse_dir / "audit_set_block_ids.json").write_text(json.dumps(audit_ids), encoding="utf-8")
    (reuse_dir / "audit_set_manifest.md").write_text("# Audit\n", encoding="utf-8")
    (reuse_dir / "reference_run_summary.json").write_text(json.dumps(reference_summary), encoding="utf-8")
    (reuse_dir / "reference_analyses.json").write_text(json.dumps(reference_analyses), encoding="utf-8")
    (reuse_dir / "audit_smoke_summary.json").write_text(json.dumps(smoke_payload), encoding="utf-8")

    reused_dir, reused_manifest, reused_ids, audit_manifest_md, reference_map, reused_ref_summary, smoke = load_reused_audit_context(project_dir)
    assert reused_dir == reuse_dir
    assert reused_manifest["timestamp"] == "20260422_210243"
    assert reused_ids == audit_ids
    assert audit_manifest_md.strip() == "# Audit"
    assert list(reference_map) == audit_ids
    assert reused_ref_summary["requested_block_ids"] == audit_ids
    assert smoke["summary_row"]["total_cost_usd"] == 0.24


def test_early_stop_logic_blocks_b2_after_survived_b4():
    from run_pro_big_project_ceiling_downward import should_launch_profile

    ok, reason = should_launch_profile([{"profile_id": "b4", "survived": True}], "b2")
    assert ok is False
    assert "stops at b4" in reason


def test_budget_stop_logic_blocks_over_cap_step():
    from run_pro_big_project_ceiling_downward import BudgetTracker

    budget = BudgetTracker(cap_usd=0.50)
    event = budget.preflight(step_id="b4", label="Run b4", predicted_usd=0.30)
    assert event.approved is True
    budget.commit(event, 0.28)

    next_event = budget.preflight(step_id="b2", label="Run b2", predicted_usd=0.30)
    assert next_event.approved is False
    assert budget.stopped is True
    assert "exceeds remaining" in budget.stop_reason


def test_budget_estimate_for_b2_uses_observed_b4_actual_cost():
    from run_pro_big_project_ceiling_downward import estimate_profile_cost

    predicted = estimate_profile_cost(
        "b2",
        0.24,
        [{"profile_id": "b4", "launched": True, "actual_cost_usd": 0.50}],
    )
    assert predicted == 0.8


def test_recommendation_logic_prefers_b4_then_b2_then_single_block_only():
    from run_pro_big_project_ceiling_downward import BudgetTracker, recommend_ceiling

    budget = BudgetTracker(cap_usd=1.0)

    rec_b4 = recommend_ceiling(
        [{"profile_id": "b4", "launched": True, "survived": True}],
        budget,
    )
    assert rec_b4["survived_b4"] is True
    assert rec_b4["practical_ceiling"] == "b4"

    rec_b2 = recommend_ceiling(
        [
            {"profile_id": "b4", "launched": True, "survived": False},
            {"profile_id": "b2", "launched": True, "survived": True},
        ],
        budget,
    )
    assert rec_b2["launched_b2"] is True
    assert rec_b2["survived_b2"] is True
    assert rec_b2["practical_ceiling"] == "b2"

    rec_single = recommend_ceiling(
        [
            {"profile_id": "b4", "launched": True, "survived": False},
            {"profile_id": "b2", "launched": True, "survived": False},
        ],
        budget,
    )
    assert rec_single["practical_ceiling"] == "single-block only"
    assert rec_single["next_step_needed"] is False
