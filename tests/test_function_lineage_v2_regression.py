from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import regression


@pytest.fixture(scope="module")
def artifact() -> dict:
    return regression.build()


def test_regression_makes_no_model_calls(artifact: dict) -> None:
    assert artifact["model_calls"] == 0
    assert artifact["schema_version"] == regression.SCHEMA_VERSION


def test_two_independent_replays_are_byte_identical() -> None:
    first = regression.build()
    second = regression.build()

    assert regression._json_bytes(first) == regression._json_bytes(second)


def test_candidate_generation_is_unchanged(artifact: dict) -> None:
    equivalence = artifact["candidate_regeneration_equivalence"]

    assert equivalence["byte_identical"] is True
    assert equivalence["frozen_artifact_sha256"] == equivalence["regenerated_artifact_sha256"]


def test_scoped_layer_replays_byte_identically(artifact: dict) -> None:
    replay = artifact["deterministic_replay"]

    assert replay["scope_graph_replay_identical"] is True
    assert replay["scope_graph_matches_frozen"] is True
    assert replay["scope_graph_replay_mismatches"] == []
    assert replay["scope_graph_frozen_mismatches"] == []
    assert replay["scoped_transport_matches_frozen"] is True


def test_recall_baselines_do_not_regress(artifact: dict) -> None:
    recall = artifact["recall"]

    assert recall["unchanged"] is True
    assert recall["observed"]["raw_candidate_recall"] == {
        "recall_at_1": 0.578947,
        "recall_at_3": 0.684211,
        "recall_at_5": 0.842105,
        "recall_at_10": 0.947368,
    }
    assert recall["observed"]["scope_eligible_recall"] == {
        "recall_at_1": 0.789474,
        "recall_at_3": 0.842105,
        "recall_at_5": 0.894737,
        "recall_at_10": 0.947368,
    }


def test_scope_safety_matches_the_frozen_baseline(artifact: dict) -> None:
    safety = artifact["scope_safety"]

    assert safety["matches_frozen_baseline"] is True
    assert safety["raw_candidates_preserved"] is True
    assert safety["unknown_scope_policy"] == "FAIL_CLOSED"
    assert safety["cross_granularity_competition"]["after"] == {
        "candidate_pair_count": 0,
        "task_count": 0,
    }
    assert safety["observed"]["RIGHT_MAP_CONFLICT"] == 0
    assert safety["observed"]["page_global_exclusivity"] is False
    assert safety["observed"]["search_failure_count"] == 0


def test_true_conflicts_stay_rejected_and_no_false_conflict_remains(artifact: dict) -> None:
    capacity = artifact["capacity_replay"]

    assert capacity["true_conflicts_total"] == 9
    assert capacity["true_conflicts_still_rejected"] == 9
    assert capacity["true_conflicts_lost"] == []
    assert capacity["false_conflicts_after"] == 0
    assert capacity["introduced_conflicts"] == []


def test_licences_are_confined_to_proven_kinds(artifact: dict) -> None:
    sweep = artifact["population_capacity_sweep"]

    assert set(sweep["licence_kinds"]) <= {
        "SAME_ATOMIC_OWNERSHIP",
        "DERIVED_COMPOSITE_OWNERSHIP",
        "DERIVED_EXACT_CHILD_UNION",
    }
    assert sweep["licence_kinds"]["DERIVED_EXACT_CHILD_UNION"] == 117
    assert sweep["rejected"] + sweep["licensed"] == sweep["classified_collisions"]
    # Licensing stays a small, proven minority of the collision surface.
    assert sweep["licensed"] < sweep["rejected"]


def test_sentinel_deterministic_inputs_are_unchanged(artifact: dict) -> None:
    sentinels = artifact["sentinels"]

    assert len(sentinels["sentinels"]) == 7
    assert sentinels["all_references_selectable"] is True
    assert sentinels["all_scope_ids_match"] is True
    assert sentinels["all_present_in_transport"] is True
    assert sentinels["reference_use"] == (
        "REGRESSION_REFERENCE_ONLY_NEVER_A_MAPPING_RULE"
    )
    for row in sentinels["sentinels"]:
        assert row["unknown_is_selectable"] is False
        assert row["candidate_count"] >= 1


def test_all_gates_pass(artifact: dict) -> None:
    failed = [name for name, value in artifact["gates"].items() if not value]

    assert failed == []
    assert artifact["all_gates_passed"] is True
