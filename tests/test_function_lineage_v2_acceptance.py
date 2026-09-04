from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import acceptance, holdout, stratified


@pytest.fixture(scope="module")
def objects() -> dict:
    return acceptance.build_frozen_objects()


def test_preparation_makes_no_model_calls(objects: dict) -> None:
    assert objects["preflight"]["model_calls"] == 0
    manifest = acceptance.input_manifest(objects)

    assert manifest["model_calls_made_so_far"] == 0
    gate = acceptance.disclosure(objects, manifest)
    assert gate["consent_granted"] is False
    assert gate["consent_required"] is True


def test_selection_replays_byte_identically() -> None:
    first = acceptance.build_frozen_objects()
    second = acceptance.build_frozen_objects()

    for name in ("population", "sample"):
        assert acceptance._json_bytes(first[name]) == acceptance._json_bytes(second[name])
    assert acceptance._jsonl_bytes(first["shards"]) == acceptance._jsonl_bytes(
        second["shards"]
    )


def test_no_already_evaluated_task_can_enter_the_sample(objects: dict) -> None:
    population = objects["population"]
    selected = set(objects["sample"]["selected_task_ids"])
    consumed = population["excluded"]["task_ids"]

    assert len(consumed["V2_5_DIAGNOSTIC"]) == 36
    assert len(consumed["V2_6_HOLDOUT"]) == 36
    assert len(consumed["V2_4_2_SENTINEL"]) == 7
    for values in consumed.values():
        assert not selected & set(values)
    assert population["acceptance_eligible_size"] == 134


def test_tier_follows_only_from_the_task_own_inventory(objects: dict) -> None:
    """Tier must be computable before any model call."""
    for row in objects["sample"]["selected_tasks"]:
        definition = acceptance.TIERS[row["tier"]]
        relations = set(row["relation_types"])
        if definition["relations"] is None:
            continue
        assert relations <= definition["relations"], row["task_id"]
        assert relations & definition["requires"], row["task_id"]


def test_auto_tiers_are_taken_whole_so_there_is_no_sampling_freedom(objects: dict) -> None:
    population = objects["population"]
    sample = objects["sample"]

    for tier in ("AUTO_ONE_TO_ONE", "AUTO_MERGED"):
        assert acceptance.TIER_SAMPLE_SIZE[tier] is None
        assert sample["tier_sizes"][tier] == population["tier_sizes"][tier]
    assert sample["go_deciding_tiers"] == ["AUTO_ONE_TO_ONE", "AUTO_MERGED"]
    assert acceptance.TIERS["HARD_DIAGNOSTIC"]["decides_go"] is False


def test_an_auto_tier_task_can_never_produce_a_relation_outside_its_family(
    objects: dict,
) -> None:
    """This is what makes the tier immune to post-hoc revision."""
    rows = {
        str(value["task_id"]): value for value in objects["population"]["tasks"]
    }
    for tier in ("AUTO_ONE_TO_ONE", "AUTO_MERGED"):
        allowed = acceptance.TIERS[tier]["relations"]
        for task_id in objects["sample"]["selected_task_ids_by_tier"][tier]:
            for candidate in rows[task_id]["candidates"]:
                assert candidate["relation_type"] in allowed


def test_hard_tier_never_decides_the_product_question(objects: dict) -> None:
    hard = objects["sample"]["selected_task_ids_by_tier"]["HARD_DIAGNOSTIC"]

    assert len(hard) == acceptance.TIER_SAMPLE_SIZE["HARD_DIAGNOSTIC"]
    assert "HARD_DIAGNOSTIC" not in objects["sample"]["go_deciding_tiers"]


def test_gates_are_fixed_before_inference_and_not_relaxed(objects: dict) -> None:
    gates = objects["sample"]["acceptance_gates"]

    assert gates["stable_3_of_3_min"] == (
        stratified.VERDICT_THRESHOLDS["a_overall_stable_3_of_3_min"]
    )
    assert gates["cross_cold_exact_consistency_min"] == (
        stratified.VERDICT_THRESHOLDS["a_cross_cold_exact_consistency_min"]
    )
    assert gates["unsupported_accepted_max"] == 0
    assert gates["false_capacity_conflicts_max"] == 0
    assert gates["right_map_conflict_max"] == 0
    assert gates["technical_failures_max"] == 0
    assert gates["sentinel_regression_max"] == 0
    assert gates["batch_permutation_changes_max"] == 0
    assert gates["majority_override"] is False
    assert gates["accepted_match_requires"] == [
        "PASS_A_EQUALS_PASS_B", "PARSER_PASS", "VERIFIER_PASS", "CAPACITY_PASS",
    ]


def test_sentinel_prompts_stay_identical_to_the_frozen_v2_5_shards(objects: dict) -> None:
    preflight = objects["preflight"]

    assert preflight["sentinel_prompts_identical_to_v2_5"] is True
    assert preflight["failures"] == []
    assert preflight["ok"] is True


def test_transport_stays_bounded_and_provider_safe(objects: dict) -> None:
    for shard in objects["shards"]:
        assert shard["provider_safe_schema_problems"] == []
        assert shard["prompt_characters"] > 0


def test_disclosure_states_the_exact_planned_cost(objects: dict) -> None:
    manifest = acceptance.input_manifest(objects)
    gate = acceptance.disclosure(objects, manifest)
    breakdown = gate["planned_request_breakdown"]

    assert gate["planned_requests"] == (
        breakdown["tiered_shards"]
        * breakdown["tiered_cold_repeats"]
        * breakdown["passes_per_repeat"]
        + breakdown["sentinel_shards"]
        * breakdown["sentinel_cold_repeats"]
        * breakdown["passes_per_repeat"]
    )
    assert gate["vision"] is False
    assert gate["writes_production_state"] is False
    assert gate["enables_shadow"] is False
    assert gate["materializes_output"] is False
    assert gate["tier_change_after_inference_forbidden"] is True


def test_inference_is_reachable_only_through_the_consent_gate() -> None:
    """Before consent the module had no runner; after it, the runner is gated."""
    import inspect

    signature = inspect.signature(acceptance.experiment)

    assert set(signature.parameters) == {
        "output", "consent_granted", "consented_sha256",
    }
    for name in ("consent_granted", "consented_sha256"):
        assert signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert signature.parameters[name].default is inspect.Parameter.empty
    assert acceptance.CONSENTED_ARTIFACTS == (
        "model_inputs.jsonl", "acceptance_population.json", "acceptance_sample.json",
    )
