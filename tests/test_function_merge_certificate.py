"""Deterministic merge certificate.

Synthetic passports only — no project, page or file name of any corpus.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import merge_certificate as merge


def _passport(function_id: str, side: str, **fields) -> dict:
    return {
        "function_id": function_id,
        "side": side,
        "function_class": fields.pop("function_class", "HOT_WATER"),
        "component_role": fields.pop("component_role", "ROLE_A"),
        "document_role": "GRAPHIC_SHEET",
        "neighboring_function_context": fields.pop("neighbours", []),
        **fields,
    }


def _candidate(**overrides) -> dict:
    key = "RIGHT:9:frag_target"
    candidate = {
        "candidate_id": "lcand_test",
        "pair_id": "pair_test",
        "relation_type": "MERGED_N_TO_1",
        "left_function_ids": ["func_a", "func_b"],
        "right_function_ids": ["func_r"],
        "left_fragment_ids": ["frag_a", "frag_b"],
        "right_fragment_ids": ["frag_target"],
        "left_pages": [4, 5],
        "right_pages": [9],
        "right_capacity_keys": [key],
        "component_map": [
            {
                "left_fragment_id": "frag_a", "left_function_id": "func_a",
                "right_fragment_id": "frag_target", "right_function_id": "func_r",
                "capacity_key": key, "component_role": "ROLE_A",
                "left_physical_page": 4, "right_physical_page": 9,
            },
            {
                "left_fragment_id": "frag_b", "left_function_id": "func_b",
                "right_fragment_id": "frag_target", "right_function_id": "func_r",
                "capacity_key": key, "component_role": "ROLE_A",
                "left_physical_page": 5, "right_physical_page": 9,
            },
        ],
        "evidence_refs": ["flev_1"],
    }
    candidate.update(overrides)
    return candidate


CATALOG = {"flev_1": {"evidence_id": "flev_1", "provenance_type": "FRAGMENT_OWNED_EVIDENCE"}}


def _certify(left_a: dict, left_b: dict, right: dict, **overrides) -> dict:
    passports = {"func_a": left_a, "func_b": left_b, "func_r": right}
    return merge.certify(_candidate(**overrides), passports, CATALOG)


# --- the certificate can certify: 0 in the corpora is a data fact ------------


def test_a_fully_documented_merge_is_certified():
    """Positive control: proves the certificate is not vacuously impossible."""
    result = _certify(
        _passport("func_a", "LEFT", serviced_object=["Стояк 1"], zone=["Секция A"]),
        _passport("func_b", "LEFT", serviced_object=["Стояк 2"], zone=["Секция A"]),
        _passport(
            "func_r", "RIGHT",
            serviced_object=["Стояк 1", "Стояк 2"], zone=["Секция A"],
        ),
    )

    assert result["status"] == "CERTIFIED"
    assert result["contradicted_dimensions"] == []
    assert set(result["proven_required_dimensions"]) == set(merge.REQUIRED_DIMENSIONS)


# --- negative controls -------------------------------------------------------


def test_sources_serving_different_objects_cannot_merge():
    result = _certify(
        _passport("func_a", "LEFT", serviced_object=["Корпус 1"], zone=["Корпус 1"]),
        _passport("func_b", "LEFT", serviced_object=["Корпус 2"], zone=["Корпус 2"]),
        _passport("func_r", "RIGHT", serviced_object=["Корпус 1"], zone=["Корпус 1"]),
    )

    assert result["status"] == "CONTRADICTORY"
    assert "SERVICED_OBJECT_COMPATIBILITY" in result["contradicted_dimensions"]


def test_a_target_covering_only_one_source_cannot_merge():
    result = _certify(
        _passport("func_a", "LEFT", serviced_object=["Стояк 1"]),
        _passport("func_b", "LEFT", serviced_object=["Стояк 2"]),
        _passport("func_r", "RIGHT", serviced_object=["Стояк 9"]),
    )

    assert result["status"] == "CONTRADICTORY"
    assert "TARGET_CONSOLIDATION" in result["contradicted_dimensions"]


def test_incompatible_engineering_classes_cannot_merge():
    result = _certify(
        _passport("func_a", "LEFT", function_class="HOT_WATER",
                  serviced_object=["Стояк 1"]),
        _passport("func_b", "LEFT", function_class="GROUNDING_LIGHTNING",
                  serviced_object=["Стояк 2"]),
        _passport("func_r", "RIGHT", function_class="HOT_WATER",
                  serviced_object=["Стояк 1", "Стояк 2"]),
    )

    assert result["status"] == "CONTRADICTORY"
    assert "FUNCTION_COMPATIBILITY" in result["contradicted_dimensions"]


def test_a_lost_source_component_cannot_be_certified():
    result = _certify(
        _passport("func_a", "LEFT", serviced_object=["Стояк 1"]),
        _passport("func_b", "LEFT", serviced_object=["Стояк 2"]),
        _passport("func_r", "RIGHT", serviced_object=["Стояк 1", "Стояк 2"]),
        left_fragment_ids=["frag_a", "frag_b", "frag_missing"],
    )

    assert result["status"] == "CONTRADICTORY"
    assert "NO_UNEXPLAINED_LEFT_COMPONENT" in result["contradicted_dimensions"]
    assert result["dimensions"]["NO_UNEXPLAINED_LEFT_COMPONENT"]["unexplained"] == [
        "frag_missing"
    ]


# --- unknown is never false --------------------------------------------------


def test_a_missing_scope_fact_is_unknown_not_a_contradiction():
    result = _certify(
        _passport("func_a", "LEFT"),
        _passport("func_b", "LEFT"),
        _passport("func_r", "RIGHT"),
    )

    assert result["status"] != "CONTRADICTORY"
    assert result["contradicted_dimensions"] == []
    assert "TARGET_CONSOLIDATION" in result["missing_required_dimensions"]
    assert "SERVICED_OBJECT_COMPATIBILITY" in result["missing_required_dimensions"]


def test_absent_topology_is_missing_not_contradicted():
    result = _certify(
        _passport("func_a", "LEFT", serviced_object=["Стояк 1"]),
        _passport("func_b", "LEFT", serviced_object=["Стояк 2"]),
        _passport("func_r", "RIGHT", serviced_object=["Стояк 1", "Стояк 2"]),
    )

    assert result["dimensions"]["TOPOLOGY_CONVERGENCE"]["state"] == "MISSING"
    assert "TOPOLOGY_CONVERGENCE" not in result["contradicted_dimensions"]


# --- forbidden evidence ------------------------------------------------------


def test_page_adjacency_never_certifies_a_merge():
    adjacent = _certify(
        _passport("func_a", "LEFT"),
        _passport("func_b", "LEFT"),
        _passport("func_r", "RIGHT"),
        left_pages=[4, 5],
    )
    distant = _certify(
        _passport("func_a", "LEFT"),
        _passport("func_b", "LEFT"),
        _passport("func_r", "RIGHT"),
        left_pages=[4, 40],
    )

    assert adjacent["status"] == distant["status"]
    assert all(
        value is False for value in adjacent["forbidden_evidence_used"].values()
    )


def test_a_lone_candidate_does_not_imply_a_merge():
    result = _certify(
        _passport("func_a", "LEFT"),
        _passport("func_b", "LEFT"),
        _passport("func_r", "RIGHT"),
    )

    assert result["status"] != "CERTIFIED"
    assert result["forbidden_evidence_used"]["single_candidate_implies_merge"] is False


def test_tokenised_systems_field_is_never_used():
    assert "systems" in merge.EXCLUDED_FACTS
    assert "systems" not in merge.SCOPE_FACTS
    assert "systems" not in merge.CONTINUITY_FACTS


def test_capacity_identity_stays_fragment_exact():
    result = _certify(
        _passport("func_a", "LEFT", serviced_object=["Стояк 1"]),
        _passport("func_b", "LEFT", serviced_object=["Стояк 2"]),
        _passport("func_r", "RIGHT", serviced_object=["Стояк 1", "Стояк 2"]),
    )

    assert result["dimensions"]["CAPACITY"]["state"] == "PROVEN"
    assert result["capacity_keys"] == ["RIGHT:9:frag_target"]


# --- corpus audit ------------------------------------------------------------


@pytest.fixture(scope="module")
def audit() -> dict:
    return merge.build()


def test_audit_makes_no_model_calls(audit: dict) -> None:
    assert audit["model_calls"] == 0
    assert audit["safety"]["candidate_recall_loss"] == 0
    assert audit["safety"]["page_global_exclusivity"] is False


def test_audit_replay_is_byte_identical() -> None:
    first = merge.build()
    second = merge.build()

    assert merge.stratified._json_bytes(first) == merge.stratified._json_bytes(second)


def test_every_merge_hypothesis_comes_from_page_adjacency(audit: dict) -> None:
    """The generator's own trigger is a forbidden merge signal."""
    counts = audit["candidate_generation"]["counts"]

    assert counts.get("non_adjacent_pages", 0) == 0
    assert counts["adjacent_pages"] == 125


def test_recorded_corpus_outcome(audit: dict) -> None:
    assert audit["status_counts_overall"] == {
        "CONTRADICTORY": 56,
        "PARTIAL": 69,
    }
    assert audit["auto_merged_certified_tier"]["candidate_count"] == 0
    assert audit["auto_merged_certified_tier"]["eligible"] is False


def test_the_single_missing_evidence_is_object_scope(audit: dict) -> None:
    """Every PARTIAL certificate fails for the same, single reason."""
    partial = [
        value for value in audit["certificates"] if value["status"] == "PARTIAL"
    ]

    assert len(partial) == 69
    for value in partial:
        assert sorted(value["missing_required_dimensions"]) == [
            "SERVICED_OBJECT_COMPATIBILITY",
            "TARGET_CONSOLIDATION",
        ]
        states = set(value["dimensions"]["TARGET_CONSOLIDATION"]["per_field"].values())
        assert states == {"MISSING"}


def test_the_stable_refusals_were_correct(audit: dict) -> None:
    replay = audit["stable_nme_replay"]

    assert replay["stable_need_more_evidence_tasks"] == 18
    assert replay["single_candidate_tasks"] == 14
    assert replay["best_status_counts"].get("CERTIFIED", 0) == 0
    assert replay["usable_as_acceptance_evidence"] is False
