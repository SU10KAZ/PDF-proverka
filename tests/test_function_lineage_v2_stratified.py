from __future__ import annotations

import copy

import pytest

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import stratified


@pytest.fixture(scope="module")
def frozen() -> dict:
    return stratified.build_frozen_objects()


def test_population_and_sample_replay_are_deterministic(frozen: dict) -> None:
    replay = stratified.build_frozen_objects()

    assert frozen["population"]["population_size"] == 213
    assert frozen["population"]["new_sample_eligible_size"] == 206
    assert stratified._json_bytes(frozen["population"]) == stratified._json_bytes(replay["population"])
    assert stratified._json_bytes(frozen["sample"]) == stratified._json_bytes(replay["sample"])
    assert stratified._jsonl_bytes(frozen["shards"]) == stratified._jsonl_bytes(replay["shards"])


def test_sample_is_36_new_tasks_with_equal_corpus_quotas(frozen: dict) -> None:
    sample = frozen["sample"]
    selected = set(sample["selected_task_ids"])

    assert sample["sample_size"] == 36
    assert sample["sample_size_by_corpus"] == {
        "IOS1.1": 12,
        "IOS2.1": 12,
        "IOS3.1": 12,
    }
    assert len(selected) == 36
    assert not selected.intersection(stratified.SENTINEL_IDS)
    assert sample["redistribution"] is None
    assert sample["selection_algorithm"]["page_or_filename_selection"] is False
    assert sample["selection_algorithm"]["manual_selection"] is False


def test_every_available_stratum_is_covered_and_e_is_sentinel_only(frozen: dict) -> None:
    coverage = frozen["sample"]["stratum_coverage"]

    for stratum, value in coverage.items():
        assert value["covered"] is (value["eligible_population"] > 0), stratum
    assert coverage["E"] == {
        "eligible_population": 0,
        "selected_tasks": 0,
        "covered": False,
    }


def test_each_corpus_has_a_closed_exact_child_union_bundle(frozen: dict) -> None:
    sample = frozen["sample"]
    selected = set(sample["selected_task_ids"])

    assert len(sample["exact_child_union_closed_bundles"]) == 3
    assert {
        next(value["corpus"] for value in sample["selected_tasks"] if value["task_id"] == bundle["parent_task_id"])
        for bundle in sample["exact_child_union_closed_bundles"]
    } == set(stratified.CORPUS_ORDER)
    for bundle in sample["exact_child_union_closed_bundles"]:
        assert set(bundle["task_ids"]).issubset(selected)
        assert set(bundle["child_task_ids"]).issubset(selected)
        assert bundle["parent_task_id"] in selected
        assert not set(bundle["task_ids"]).intersection(stratified.SENTINEL_IDS)


def test_model_inputs_are_scoped_provider_safe_and_vision_free(frozen: dict) -> None:
    shards = frozen["shards"]
    new_task_ids = {
        task_id
        for shard in shards if shard["evaluation_set"] == "NEW_SAMPLE"
        for task_id in shard["task_ids"]
    }
    sentinel_ids = {
        task_id
        for shard in shards if shard["evaluation_set"] == "SENTINEL"
        for task_id in shard["task_ids"]
    }

    assert new_task_ids == set(frozen["sample"]["selected_task_ids"])
    assert sentinel_ids == stratified.SENTINEL_IDS
    assert frozen["preflight"]["ok"] is True
    assert frozen["preflight"]["unknown_candidate_rejection_tests"] == len(shards)
    assert frozen["preflight"]["provider_schema_problem_count"] == 0
    assert frozen["preflight"]["provider_schema_contains_oneOf"] is False
    for shard in shards:
        assert shard["prompt_characters"] <= stratified.scoped_transport.HARD_CHARACTERS
        assert shard["provider_safe_schema_problems"] == []
        for context in shard["model_payload"]["task_contexts"]:
            assert lineage.FUNCTION_REMOVED not in context["allowed_decisions"]
            assert all(
                candidate["scope_relation"] == "EXACT_SCOPE"
                for candidate in context["functional_candidates"]
            )


def test_recall_and_scope_safety_are_frozen(frozen: dict) -> None:
    preflight = frozen["preflight"]

    assert preflight["recall"] == stratified.EXPECTED_RECALL
    assert preflight["deterministic_safety"] == {
        "raw_candidate_count": 1461,
        "forensically_preserved_raw_candidate_count": 1461,
        "cross_granularity_selectable_competition": 0,
        "RIGHT_MAP_CONFLICT": 0,
        "capacity_defect_count": 0,
        "candidate_partition_defect_count": 0,
        "unknown_scope_policy": "FAIL_CLOSED",
        "raw_candidates_preserved": True,
    }


def test_nme_capacity_preflight_has_no_conflict(frozen: dict) -> None:
    population = frozen["population"]
    datasets = stratified._datasets(frozen["sources"], frozen["shards"])
    records = []
    for shard in frozen["shards"]:
        if shard["evaluation_set"] != "NEW_SAMPLE":
            continue
        response = {"results": [
            {"task_id": task_id, "decision": lineage.NEED_MORE_EVIDENCE}
            for task_id in shard["task_ids"]
        ]}
        records.append({
            "evaluation_set": "NEW_SAMPLE",
            "pair_id": shard["pair_id"],
            "task_ids": list(shard["task_ids"]),
            "model_call": {"ok": True},
            "transport_verification": {"ok": True},
            "response": response,
            "capacity_verification": None,
        })

    errors = stratified._apply_capacity(
        records,
        evaluation_set="NEW_SAMPLE",
        datasets=datasets,
        population=population,
    )

    assert errors == []
    assert all(record["capacity_verification"]["ok"] for record in records)


def test_pass_disagreement_has_no_majority_override() -> None:
    by_pass = {
        "A": {
            "model_ok": True,
            "response_parser_ok": True,
            "verifier_ok": True,
            "capacity_ok": True,
            "decision": "candidate_a",
        },
        "B": {
            "model_ok": True,
            "response_parser_ok": True,
            "verifier_ok": True,
            "capacity_ok": True,
            "decision": "candidate_b",
        },
    }

    result = stratified._repeat_result(copy.deepcopy(by_pass), 1)

    assert result["status"] == "PASS_DISAGREEMENT"
    assert result["stable_decision"] is None
