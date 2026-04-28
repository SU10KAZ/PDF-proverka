from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))


def test_early_stop_logic_blocks_b10_after_failed_b8():
    from run_pro_batch_upper_bound_small import should_launch_profile

    ok, reason = should_launch_profile(
        [{"profile_id": "b6", "survived": True}],
        "b8",
    )
    assert ok is True

    ok, reason = should_launch_profile(
        [
            {"profile_id": "b6", "survived": True},
            {"profile_id": "b8", "survived": False},
        ],
        "b10",
    )
    assert ok is False
    assert "b8" in reason


def test_budget_stop_logic_blocks_over_cap_step():
    from run_pro_batch_upper_bound_small import BudgetTracker

    budget = BudgetTracker(cap_usd=0.50)
    ev1 = budget.preflight(step_id="b8", label="Run b8", predicted_usd=0.30)
    assert ev1.approved is True
    budget.commit(ev1, 0.25)

    ev2 = budget.preflight(step_id="b10", label="Run b10", predicted_usd=0.30)
    assert ev2.approved is False
    assert budget.stopped is True
    assert "exceeds remaining" in budget.stop_reason


def test_recommendation_logic_prefers_highest_survived_profile():
    from run_pro_batch_upper_bound_small import recommend_upper_bound

    rec = recommend_upper_bound(
        [
            {"profile_id": "b6", "survived": True, "launched": False},
            {"profile_id": "b8", "survived": True, "launched": True},
            {"profile_id": "b10", "survived": False, "launched": True},
        ]
    )
    assert rec["survived_b8"] is True
    assert rec["launched_b10"] is True
    assert rec["survived_b10"] is False
    assert rec["degradation_starts_at"] == "b10"
    assert rec["practical_upper_bound"] == "b8"

    rec2 = recommend_upper_bound(
        [
            {"profile_id": "b6", "survived": True, "launched": False},
            {"profile_id": "b8", "survived": False, "launched": True},
        ]
    )
    assert rec2["practical_upper_bound"] == "b6"
    assert rec2["degradation_starts_at"] == "b8"


def test_reuse_of_stress_set_comes_from_existing_variant_a_artifact(tmp_path: Path):
    from run_pro_batch_upper_bound_small import PDF_NAME, load_reused_stress_set

    project_dir = tmp_path / "projects" / "214. Alia (ASTERUS)" / "KJ" / PDF_NAME
    variant_dir = project_dir / "_experiments" / "pro_variantA_small_budget" / "20260422_115144"
    variant_dir.mkdir(parents=True)

    manifest = {
        "timestamp": "20260422_115144",
        "pdf": PDF_NAME,
        "project_dir": str(project_dir),
        "openrouter_only": True,
        "production_defaults_changed": False,
        "claude_touched": False,
        "flash_touched": False,
        "stress_set_size": 8,
    }
    stress_ids = [f"B{i}" for i in range(8)]

    (variant_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (variant_dir / "stress_set_block_ids.json").write_text(json.dumps(stress_ids), encoding="utf-8")
    (variant_dir / "stress_set_manifest.md").write_text("# Stress\n", encoding="utf-8")
    (variant_dir / "b6_batch_results.json").write_text("[]", encoding="utf-8")

    reused_dir, reused_manifest, reused_ids, source_manifest_md = load_reused_stress_set(project_dir)
    assert reused_dir == variant_dir
    assert reused_manifest["timestamp"] == "20260422_115144"
    assert reused_ids == stress_ids
    assert source_manifest_md.strip() == "# Stress"
