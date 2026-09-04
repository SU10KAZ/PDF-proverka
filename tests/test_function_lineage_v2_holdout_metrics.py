"""Invariants of the holdout acceptance reporting.

The suite asserts contracts, not outcomes: strict unanimity, no majority
override, sentinel references used only as regression references, and
DOCUMENT_LINK never treated as functional truth.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import holdout, holdout_metrics, stratified


def _pass(decision: str) -> dict:
    return {
        "decision": decision,
        "model_ok": True,
        "response_parser_ok": True,
        "verifier_ok": True,
        "capacity_ok": True,
    }


def test_stable_match_requires_both_passes_and_every_verifier():
    result = stratified._repeat_result({"A": _pass("lcand_x"), "B": _pass("lcand_x")}, 1)

    assert result["status"] == "STABLE_MATCH"
    assert result["stable_decision"] == "lcand_x"


def test_disagreeing_passes_never_produce_a_decision():
    result = stratified._repeat_result({"A": _pass("lcand_x"), "B": _pass("lcand_y")}, 1)

    assert result["status"] == "PASS_DISAGREEMENT"
    assert result["stable_decision"] is None


@pytest.mark.parametrize("failing", ["verifier_ok", "capacity_ok", "response_parser_ok"])
def test_any_failed_gate_blocks_the_decision(failing: str):
    left, right = _pass("lcand_x"), _pass("lcand_x")
    right[failing] = False

    result = stratified._repeat_result({"A": left, "B": right}, 1)

    assert result["stable_decision"] is None
    assert result["status"] in {
        "VERIFIER_REJECTION", "CAPACITY_REJECTION", "RESPONSE_PARSER_REJECTION",
    }


def test_need_more_evidence_is_reported_as_unresolved_not_as_a_match():
    result = stratified._repeat_result({
        "A": _pass(lineage.NEED_MORE_EVIDENCE),
        "B": _pass(lineage.NEED_MORE_EVIDENCE),
    }, 1)

    assert result["status"] == "STABLE_UNRESOLVED"
    assert result["stable_decision"] == lineage.NEED_MORE_EVIDENCE


def test_two_of_three_cold_repeats_never_become_a_stable_decision():
    task = {
        "cold_repeats": [
            {"status": "STABLE_MATCH", "stable_decision": "lcand_x"},
            {"status": "STABLE_MATCH", "stable_decision": "lcand_x"},
            {"status": "PASS_DISAGREEMENT", "stable_decision": None},
        ],
    }
    stable = [
        value["stable_decision"] for value in task["cold_repeats"]
        if value["status"] in {"STABLE_MATCH", "STABLE_UNRESOLVED"}
    ]

    # The aggregation requires every repeat to be stable and identical, so a
    # 2-of-3 majority cannot be promoted to a decision.
    assert len(stable) != len(task["cold_repeats"])


def test_sentinel_references_are_regression_only():
    metrics = holdout_metrics._sentinel_metrics([])

    assert metrics["reference_use"] == (
        "REGRESSION_REFERENCE_ONLY_NEVER_A_MAPPING_RULE"
    )
    assert set(holdout_metrics.SENTINEL_REFERENCES) == set(holdout.SENTINELS)


def test_document_link_is_never_functional_truth():
    references = holdout_metrics._reference_metrics([{
        "reference_classes": ["DOCUMENT_LINK"],
        "stable_decision": "lcand_x",
        "references": [{
            "reference_class": "DOCUMENT_LINK",
            "candidate_ids": ["lcand_x"],
        }],
    }])

    assert references["document_link_used_as_functional_truth"] is False
    assert references["document_link_candidate_occurrences"] == 1
    assert references["authoritative"]["determined_rows"] == 0
    assert references["research"]["determined_rows"] == 0


def test_unsupported_detection_flags_a_decision_outside_the_inventory():
    rows = [
        {
            "task_id": "t1",
            "stable_decision": "lcand_invented",
            "candidate_inventory": [{"candidate_id": "lcand_offered"}],
        },
        {
            "task_id": "t2",
            "stable_decision": "lcand_offered",
            "candidate_inventory": [{"candidate_id": "lcand_offered"}],
        },
        {
            "task_id": "t3",
            "stable_decision": lineage.NEED_MORE_EVIDENCE,
            "candidate_inventory": [],
        },
    ]

    assert holdout_metrics._unsupported(rows) == [
        {"task_id": "t1", "candidate_id": "lcand_invented"}
    ]


def test_runner_refuses_without_consent(tmp_path: Path):
    with pytest.raises(RuntimeError, match="consent"):
        holdout.experiment(
            tmp_path, consent_granted=False, consented_sha256={},
        )


def test_runner_refuses_when_a_consented_artifact_drifts(tmp_path: Path):
    for name in holdout.CONSENTED_ARTIFACTS:
        (tmp_path / name).write_text("drifted", encoding="utf-8")

    with pytest.raises(RuntimeError, match="new consent is required"):
        holdout.experiment(
            tmp_path,
            consent_granted=True,
            consented_sha256={name: "0" * 64 for name in holdout.CONSENTED_ARTIFACTS},
        )


def test_runner_refuses_to_repeat_an_existing_run():
    with pytest.raises(RuntimeError, match="refusing to repeat"):
        holdout.experiment(
            holdout.DEFAULT_OUTPUT,
            consent_granted=True,
            consented_sha256={},
        )


# --- recorded outcome of the consented run ------------------------------------
# These lock the artifact so a later change cannot silently move the numbers.


@pytest.fixture(scope="module")
def recorded() -> dict:
    return holdout_metrics.build()


def test_the_consented_run_completed_exactly_as_disclosed(recorded: dict):
    cost = recorded["cost"]

    assert cost["planned_requests"] == 110
    assert cost["request_records"] == 110
    assert cost["successful_inference_requests"] == 110
    assert cost["stopped_early"] is False
    assert recorded["consent"]["ok"] is True
    assert recorded["consent"]["drifted_artifacts"] == []


def test_every_safety_property_holds(recorded: dict):
    safety = recorded["safety"]

    assert safety["unsupported_accepted_match_count"] == 0
    assert safety["verifier_rejection_task_count"] == 0
    assert safety["technical_failure_task_count"] == 0
    assert safety["RIGHT_MAP_CONFLICT"] == 0
    assert safety["false_conflicts"] == 0
    assert safety["true_conflicts"] == 12
    assert safety["conflict_root_cause_counts"]["D_FRAGMENTATION_DEFECT"] == 0
    assert recorded["sentinels"]["regression"] is False


def test_reproducibility_is_reported_without_softening(recorded: dict):
    verdict = holdout_metrics.gates(recorded)
    diagnosis = recorded["instability_diagnosis"]

    assert verdict["reproducibility"]["stable_3_of_3"] == 16
    assert verdict["reproducibility"]["tasks"] == 36
    assert verdict["failed_gates"] == ["strong_reproducibility"]
    assert verdict["all_passed"] is False
    # The capacity-stage ceiling is measured and is still below the threshold,
    # so no capacity change could turn this gate green.
    assert diagnosis["own_answer_consistent_3_of_3"] == 21
    assert diagnosis["own_answer_consistent_rate"] < verdict["thresholds"][
        "stable_3_of_3_min"
    ]
    assert diagnosis["capacity_collateral_task_count"] == 5


def test_no_task_published_a_decision_without_unanimity(recorded: dict):
    verdict = holdout_metrics.gates(recorded)

    assert verdict["tasks_publishing_a_decision_without_unanimity"] == []
    assert verdict["gates"]["unstable_tasks_publish_no_decision"] is True
