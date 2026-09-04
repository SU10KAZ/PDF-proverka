"""Contracts and recorded outcome of the v2.7 tiered acceptance evaluation."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import acceptance, acceptance_metrics


@pytest.fixture(scope="module")
def metrics() -> dict:
    return acceptance_metrics.build()


@pytest.fixture(scope="module")
def verdict(metrics: dict) -> dict:
    return acceptance_metrics.gates(metrics)


# --- contracts ---------------------------------------------------------------


def test_capacity_is_never_accounted_per_batch(metrics: dict) -> None:
    assert metrics["capacity_stage"] == "POST_CONSENSUS_GLOBAL"
    assert acceptance.CAPACITY_VIEWS["PRIMARY_PER_TIER"]["decides_gate"] is True
    assert acceptance.CAPACITY_VIEWS["SECONDARY_CROSS_TIER"]["decides_gate"] is False


def test_gates_match_the_frozen_disclosure(metrics: dict) -> None:
    assert metrics["acceptance_gates"] == acceptance.ACCEPTANCE_GATES
    assert metrics["acceptance_gates"]["stable_3_of_3_min"] == 0.90
    assert metrics["acceptance_gates"]["cross_cold_exact_consistency_min"] == 0.85
    assert metrics["acceptance_gates"]["majority_override"] is False


def test_hard_set_never_decides(verdict: dict) -> None:
    assert verdict["hard_set_never_decides"] is True
    assert "HARD_DIAGNOSTIC" not in verdict["tiers"]
    assert set(verdict["tiers"]) == {"AUTO_ONE_TO_ONE", "AUTO_MERGED"}


def test_need_more_evidence_is_never_counted_as_an_auto_match(metrics: dict) -> None:
    for tier in acceptance.TIERS:
        row = metrics["tier_metrics"][tier]
        assert row["stable_published_matches"] + row["stable_need_more_evidence"] <= (
            row["reproducible_tasks"]
        )
    for row in metrics["tasks"]:
        if row["stable_outcome"] == lineage.NEED_MORE_EVIDENCE:
            assert row["stable_outcome_kind"] == acceptance_metrics.OUTCOME_NME


# --- recorded outcome --------------------------------------------------------


def test_the_consented_run_completed_exactly_as_disclosed(metrics: dict) -> None:
    cost = metrics["cost"]

    assert cost["planned_requests"] == 194
    assert cost["request_records"] == 194
    assert cost["successful_inference_requests"] == 194
    assert cost["stopped_early"] is False
    assert metrics["experiment_valid"] is True
    assert metrics["consent"]["ok"] is True
    assert metrics["consent"]["drifted_artifacts"] == []


def test_every_safety_property_holds(metrics: dict) -> None:
    safety = metrics["safety"]

    assert safety["unsupported_accepted_match_count"] == 0
    assert safety["verifier_rejection_task_count"] == 0
    assert safety["technical_failure_task_count"] == 0
    assert safety["RIGHT_MAP_CONFLICT"] == 0
    assert safety["false_conflicts"] == 0
    assert safety["true_conflicts"] == safety["capacity_error_count"] == 29


def test_capacity_is_invariant_under_permutation(metrics: dict) -> None:
    permutation = metrics["permutation_invariance"]

    assert permutation["changes"] == 0
    assert permutation["groups_checked"] > 0


def test_recorded_tier_results(metrics: dict) -> None:
    one_to_one = metrics["tier_metrics"]["AUTO_ONE_TO_ONE"]
    merged = metrics["tier_metrics"]["AUTO_MERGED"]

    assert one_to_one["tasks"] == 33
    assert one_to_one["reproducible_tasks"] == 22
    assert one_to_one["stable_published_matches"] == 14
    assert merged["tasks"] == 54
    assert merged["reproducible_tasks"] == 46
    assert merged["stable_published_matches"] == 8
    # AUTO_MERGED reproducibility is dominated by reproducible NON-answers, so
    # the headline rate must never be read as coverage.
    assert merged["stable_published_match_rate"] < merged["stable_3_of_3_rate"]


def test_neither_tier_earned_auto_eligibility(verdict: dict) -> None:
    assert verdict["relations_earning_auto_eligibility"] == []
    assert verdict["any_tier_passed"] is False
    assert verdict["verdict"] == "NOT_READY"
    assert lineage.AUTO_ELIGIBLE_RELATIONS == frozenset()


def test_failed_gates_are_reported_without_softening(verdict: dict) -> None:
    assert verdict["tiers"]["AUTO_ONE_TO_ONE"]["failed_gates"] == [
        "cross_cold_consistency_threshold",
        "sentinels_do_not_regress",
        "stable_3_of_3_threshold",
    ]
    assert verdict["tiers"]["AUTO_MERGED"]["failed_gates"] == [
        "sentinels_do_not_regress",
        "stable_3_of_3_threshold",
    ]


def test_sentinel_regression_is_reported(metrics: dict) -> None:
    sentinels = metrics["sentinels"]

    assert sentinels["regression"] is True
    changed = {row["sentinel"] for row in sentinels["changed"]}
    assert changed == {"LEFT20 METERING"}
    assert sentinels["reference_use"] == (
        "REGRESSION_REFERENCE_ONLY_NEVER_A_MAPPING_RULE"
    )


def test_runner_refuses_to_repeat_the_consented_run() -> None:
    with pytest.raises(RuntimeError, match="refusing to repeat"):
        acceptance.experiment(
            acceptance.DEFAULT_OUTPUT,
            consent_granted=True,
            consented_sha256={},
        )
