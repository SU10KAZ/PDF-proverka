from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import holdout


@pytest.fixture(scope="module")
def objects() -> dict:
    return holdout.build_frozen_objects()


def test_preparation_makes_no_model_calls(objects: dict) -> None:
    assert objects["preflight"]["model_calls"] == 0
    manifest = holdout.input_manifest(objects)

    assert manifest["model_calls_made_so_far"] == 0
    assert holdout.disclosure(objects, manifest)["consent_granted"] is False


def test_selection_replays_byte_identically() -> None:
    first = holdout.build_frozen_objects()
    second = holdout.build_frozen_objects()

    assert holdout._json_bytes(first["population"]) == holdout._json_bytes(second["population"])
    assert holdout._json_bytes(first["sample"]) == holdout._json_bytes(second["sample"])
    assert holdout._jsonl_bytes(first["shards"]) == holdout._jsonl_bytes(second["shards"])


def test_holdout_is_independent_of_the_v2_5_diagnostic_set(objects: dict) -> None:
    population = objects["population"]
    selected = set(objects["sample"]["selected_task_ids"])
    diagnostic = set(population["excluded"]["v2_5_diagnostic_task_ids"])
    sentinels = set(population["excluded"]["sentinel_task_ids"])

    assert len(diagnostic) == 36
    assert len(sentinels) == 7
    assert population["population_size"] == 213
    assert population["holdout_eligible_size"] == 170
    assert not selected & diagnostic
    assert not selected & sentinels


def test_sample_is_deterministic_and_never_page_or_file_driven(objects: dict) -> None:
    sample = objects["sample"]
    algorithm = sample["selection_algorithm"]

    assert sample["sample_size"] == 36
    assert sample["sample_size_by_corpus"] == {"IOS1.1": 12, "IOS2.1": 12, "IOS3.1": 12}
    assert algorithm["page_or_filename_selection"] is False
    assert algorithm["manual_selection"] is False
    assert algorithm["salt"] == holdout.SELECTION_SALT
    assert holdout.SELECTION_SALT != "function-lineage-v2.5-stratified-sample-v1"


def test_every_available_stratum_is_covered(objects: dict) -> None:
    coverage = objects["sample"]["stratum_coverage"]

    for stratum, row in coverage.items():
        assert row["covered"] is (row["eligible_population"] > 0), stratum
    assert objects["sample"]["uncoverable_strata"] == ["E"]
    assert coverage["E"]["eligible_population"] == 0


def test_sentinel_prompts_are_identical_to_the_frozen_v2_5_shards(objects: dict) -> None:
    preflight = objects["preflight"]

    assert preflight["sentinel_prompts_identical_to_v2_5"] is True
    assert preflight["sentinel_shard_count"] == 4
    assert preflight["failures"] == []
    assert preflight["ok"] is True


def test_transport_stays_bounded_and_provider_safe(objects: dict) -> None:
    for shard in objects["shards"]:
        assert shard["provider_safe_schema_problems"] == []
        assert shard["prompt_characters"] > 0
        assert shard["task_ids"]


def test_disclosure_states_the_exact_planned_cost_and_data_classes(objects: dict) -> None:
    manifest = holdout.input_manifest(objects)
    gate = holdout.disclosure(objects, manifest)

    assert gate["consent_required"] is True
    assert gate["vision"] is False
    assert gate["images"] == []
    assert gate["writes_production_state"] is False
    assert gate["enables_shadow"] is False
    assert gate["materializes_output"] is False
    assert gate["planned_requests"] == (
        gate["planned_request_breakdown"]["holdout_shards"]
        * gate["planned_request_breakdown"]["holdout_cold_repeats"]
        * gate["planned_request_breakdown"]["passes_per_repeat"]
        + gate["planned_request_breakdown"]["sentinel_shards"]
        * gate["planned_request_breakdown"]["sentinel_cold_repeats"]
        * gate["planned_request_breakdown"]["passes_per_repeat"]
    )
    assert gate["transmitted_data_classes"]
    assert any("images" in value for value in gate["not_transmitted"])
    assert gate["model_inputs_sha256"] == manifest["model_inputs_sha256"]


def test_inference_is_reachable_only_through_the_consent_gate() -> None:
    """Phase 5 had no runner at all; after consent it exists but is gated."""
    import inspect

    signature = inspect.signature(holdout.experiment)

    assert set(signature.parameters) == {
        "output", "consent_granted", "consented_sha256",
    }
    for name in ("consent_granted", "consented_sha256"):
        assert signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert signature.parameters[name].default is inspect.Parameter.empty
    assert holdout.CONSENTED_ARTIFACTS == (
        "model_inputs.jsonl", "holdout_population.json", "holdout_sample.json",
    )
