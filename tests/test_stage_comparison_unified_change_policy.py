"""G2.4.5 closed contract and deterministic policy rules."""
from __future__ import annotations

import importlib

import pytest

from backend.app.services.stage_comparison.unified_change_policy import (
    CONFIDENCE_BASES,
    CONFIDENCE_LEVELS,
    DIRECTIONS,
    DIMENSIONS,
    GRAPHIC_COVERAGE_DIMENSION_MAP,
    LIVE_DECISION_GATES,
    OBSERVATION_ONLY_GATES,
    OUTCOMES,
    RELATION_STATUSES,
    UNKNOWN_DIMENSION,
    GateState,
    canonical_identity_cell,
    check_cardinality_safety,
    check_direction_non_contradiction,
    check_entity_link_strength,
    check_outcome_compatibility,
    check_same_dimension,
    check_scope_compatibility,
    check_source_validity,
    check_subject_identity,
    confidence_policy,
    content_signature,
    contradiction_is_proven,
    evaluate_candidate_gates,
    evaluate_source_relation,
    normalize_confidence,
    normalize_evidence_atom,
    reinforce_confidence,
    review_evidence_id,
    resolve_dimension,
    should_surface_atom,
    stable_change_id,
)
from backend.app.services.stage_comparison.unified_entity_bridge.graphic_coverage_policy import (
    MODE2_OBSERVABLE_DIMENSIONS,
    UNSUPPORTED_SEMANTIC_DIMENSIONS,
)


CHECKED_BOTH = {"LEFT": "CHECKED", "RIGHT": "CHECKED"}


def _candidate(**overrides):
    values = {
        "left_scope_ref": "scope-1",
        "right_scope_ref": "scope-1",
        "subject_relation": "SAME_ENTITY",
        "links_by_side": {
            "LEFT": {"relation": "SAME_ENTITY", "confidence": "HIGH"},
            "RIGHT": {"relation": "SAME_ENTITY", "confidence": "HIGH"},
        },
        "left_dimension": "STRUCTURE",
        "right_dimension": "STRUCTURE",
        "left_direction": "INCREASED",
        "right_direction": "INCREASED",
        "left_outcome": "MATERIAL_CHANGE",
        "right_outcome": "MATERIAL_CHANGE",
        "source_valid": True,
        "coverage_by_side": CHECKED_BOTH,
        "document_binding_state": "DOCUMENT_BINDING_PROVEN",
        "text_count": 1,
        "graphic_count": 1,
    }
    values.update(overrides)
    return evaluate_candidate_gates(**values)


def test_closed_policy_enums_and_graphic_coverage_mapping():
    assert DIMENSIONS == (
        "PRINCIPLE",
        "METHOD",
        "OPERATION",
        "STRUCTURE",
        "CONNECTION",
        "TYPE",
        "PARAMETER",
        "QUANTITY",
        "SPACE",
    )
    assert OUTCOMES == ("MATERIAL_CHANGE", "DETAIL_ONLY", "REVIEW_REQUIRED")
    assert DIRECTIONS == (
        "ADDED",
        "REMOVED",
        "REPLACED",
        "INCREASED",
        "DECREASED",
        "ALTERED",
    )
    assert RELATION_STATUSES == (
        "CORROBORATING",
        "COMPLEMENTARY",
        "CONTRADICTORY",
        "UNRELATED",
        "SINGLE_SOURCE",
    )
    assert CONFIDENCE_LEVELS == ("HIGH", "MEDIUM", "LOW", "UNKNOWN")
    assert CONFIDENCE_BASES == (
        "SINGLE_SOURCE",
        "CORROBORATED",
        "COMPLEMENTARY",
        "CONTESTED",
    )
    assert set(GRAPHIC_COVERAGE_DIMENSION_MAP) == set(DIMENSIONS)
    assert GRAPHIC_COVERAGE_DIMENSION_MAP["OPERATION"] is None
    assert {
        dimension
        for dimension, mapped in GRAPHIC_COVERAGE_DIMENSION_MAP.items()
        if mapped is None
    } == {"OPERATION", *UNSUPPORTED_SEMANTIC_DIMENSIONS}
    assert {
        value for value in GRAPHIC_COVERAGE_DIMENSION_MAP.values() if value is not None
    } == set(MODE2_OBSERVABLE_DIMENSIONS)


def test_dimension_is_explicit_on_each_atom_and_unknown_is_not_guessed_from_text():
    assert resolve_dimension(None) == UNKNOWN_DIMENSION
    assert resolve_dimension(["PARAMETER", "QUANTITY"]) == UNKNOWN_DIMENSION
    assert resolve_dimension({"text": "увеличен параметр"}) == UNKNOWN_DIMENSION
    atom = normalize_evidence_atom(
        {
            "atom_id": "atom-1",
            "source": "TEXT",
            "scope_ref": "scope-1",
            "subject_ref": "panel-1",
            "project_entity_ref": "panel-1",
            "dimension": ["PARAMETER", "QUANTITY"],
            "direction": "ALTERED",
            "outcome": "REVIEW_REQUIRED",
            "confidence": "medium",
        }
    )
    assert atom["dimension"] == UNKNOWN_DIMENSION
    assert atom["confidence"] == "MEDIUM"


@pytest.mark.parametrize("dimension", DIMENSIONS)
def test_each_of_the_nine_dimensions_round_trips(dimension):
    assert resolve_dimension(dimension) == dimension


def test_identity_cell_and_change_id_ignore_evidence_order_and_modalities():
    cell = canonical_identity_cell(
        "scope-1", "panel-1", "PARAMETER", "INCREASED"
    )
    change_id = stable_change_id(cell)
    evidence = [
        {"atom_id": "text", "source": "TEXT"},
        {"atom_id": "graphic", "source": "GRAPHIC"},
    ]

    assert stable_change_id(cell) == change_id
    assert content_signature(evidence) == content_signature(list(reversed(evidence)))
    assert content_signature(evidence[:1]) != content_signature(evidence)
    assert stable_change_id(cell) == change_id


def test_same_entity_with_different_dimensions_is_not_the_same_change():
    parameter = canonical_identity_cell(
        "scope-1", "panel-1", "PARAMETER", "ALTERED"
    )
    connection = canonical_identity_cell(
        "scope-1", "panel-1", "CONNECTION", "ALTERED"
    )
    evaluation = evaluate_source_relation(
        text_state="VALID",
        graphic_state="VALID",
        scope_compatible=True,
        subject_relation="SAME_ENTITY",
        document_binding_state="DOCUMENT_BINDING_PROVEN",
        text_dimension="PARAMETER",
        graphic_dimension="CONNECTION",
    )

    assert stable_change_id(parameter) != stable_change_id(connection)
    assert evaluation["relation_status"] == "COMPLEMENTARY"
    assert "merge" not in evaluation


def test_unknown_dimension_atoms_do_not_receive_one_unified_change_id():
    first = canonical_identity_cell(
        "scope-1", "panel-1", UNKNOWN_DIMENSION, "ALTERED"
    )
    second = canonical_identity_cell(
        "scope-1", "panel-1", UNKNOWN_DIMENSION, "ALTERED"
    )

    with pytest.raises(
        ValueError, match="resolved dimension required for unified change_id"
    ):
        stable_change_id(first)
    with pytest.raises(
        ValueError, match="resolved dimension required for unified change_id"
    ):
        stable_change_id(second)
    assert review_evidence_id(first, "atom-1") != review_evidence_id(
        second, "atom-2"
    )


@pytest.mark.parametrize(
    ("coverage", "expected_state"),
    [
        ({"LEFT": "CHECKED", "RIGHT": "NOT_CHECKED"}, GateState.FAIL.value),
        ({"LEFT": "CHECKED", "RIGHT": "NOT_APPLICABLE"}, GateState.FAIL.value),
        ({"LEFT": "CHECKED", "RIGHT": "CHECK_BLOCKED"}, GateState.REVIEW_REQUIRED.value),
        (CHECKED_BOTH, GateState.PASS.value),
    ],
)
def test_coverage_is_side_specific_and_check_blocked_never_passes(coverage, expected_state):
    gate = check_source_validity(
        source_valid=True,
        coverage_by_side=coverage,
        document_binding_state="DOCUMENT_BINDING_PROVEN",
        graphic_dimension="STRUCTURE",
    )
    assert gate["state"] == expected_state


def test_non_proven_binding_blocks_cross_source_graphic_evidence():
    gate = check_source_validity(
        source_valid=True,
        coverage_by_side=CHECKED_BOTH,
        document_binding_state="DOCUMENT_BINDING_UNPROVEN",
        graphic_dimension="STRUCTURE",
    )
    assert gate["state"] == GateState.FAIL.value
    assert gate["reason_codes"] == ["document_binding_not_proven"]


@pytest.mark.parametrize("right_state", ["NOT_CHECKED", "NOT_APPLICABLE"])
def test_not_checked_and_not_applicable_are_not_contradictions(right_state):
    gates = _candidate(
        right_direction="DECREASED",
        coverage_by_side={"LEFT": "CHECKED", "RIGHT": right_state},
    )
    assert gates["gates"]["M5"]["state"] == GateState.FAIL.value
    assert gates["gates"]["M7"]["state"] == GateState.FAIL.value
    assert contradiction_is_proven(gates) is False


def test_contradiction_requires_scope_subject_dimension_and_checked_coverage():
    gates = _candidate(right_direction="DECREASED")
    assert contradiction_is_proven(gates) is True


def test_all_m1_to_m8_are_reported_but_only_four_are_live_decision_gates():
    gates = _candidate()
    assert tuple(gates["gates"]) == tuple(f"M{index}" for index in range(1, 9))
    assert tuple(gates["live_decision_gates"]) == LIVE_DECISION_GATES
    assert tuple(gates["observation_only_gates"]) == OBSERVATION_ONLY_GATES
    assert all(item["state"] == GateState.PASS.value for item in gates["gates"].values())


def test_individual_gate_failures_are_structured():
    assert check_scope_compatibility("a", "b")["state"] == GateState.FAIL.value
    assert check_subject_identity("DIFFERENT_ENTITY")["state"] == GateState.FAIL.value
    assert check_entity_link_strength(
        {
            "LEFT": {"relation": "UNKNOWN", "confidence": "UNKNOWN"},
            "RIGHT": {"relation": "SAME_ENTITY", "confidence": "HIGH"},
        }
    )["state"] == GateState.FAIL.value
    assert check_same_dimension("TYPE", "CONNECTION")["state"] == GateState.FAIL.value
    assert check_direction_non_contradiction("ADDED", "REMOVED")["state"] == GateState.FAIL.value
    outcome_gate = check_outcome_compatibility("MATERIAL_CHANGE", "DETAIL_ONLY")
    assert outcome_gate["state"] == GateState.FAIL.value
    assert check_source_validity(
        source_valid=False,
        coverage_by_side=CHECKED_BOTH,
        document_binding_state="DOCUMENT_BINDING_PROVEN",
        graphic_dimension="STRUCTURE",
    )["state"] == GateState.REVIEW_REQUIRED.value
    assert check_cardinality_safety(1, 2)["state"] == GateState.FAIL.value


@pytest.mark.parametrize(
    ("text_count", "graphic_count", "reason"),
    [(1, 3, "one_to_many_cardinality_ambiguous"), (4, 1, "many_to_one_cardinality_ambiguous")],
)
def test_m8_rejects_one_to_many_and_many_to_one(text_count, graphic_count, reason):
    result = check_cardinality_safety(text_count, graphic_count)
    assert result["state"] == GateState.FAIL.value
    assert result["reason_codes"] == [reason]


def test_confidence_is_normalized_and_reinforced_by_at_most_one_level():
    assert normalize_confidence("high") == "HIGH"
    assert normalize_confidence("MEDIUM") == "MEDIUM"
    assert reinforce_confidence("LOW", strict_corroboration=True) == "MEDIUM"
    assert reinforce_confidence("MEDIUM", strict_corroboration=True) == "HIGH"
    assert reinforce_confidence("HIGH", strict_corroboration=True) == "HIGH"
    assert confidence_policy(
        "MEDIUM", "CORROBORATING", strict_corroboration=True
    ) == {"level": "HIGH", "basis": "CORROBORATED"}


def test_silence_does_not_lower_confidence_and_contradiction_never_raises_it():
    assert confidence_policy("HIGH", "SINGLE_SOURCE") == {
        "level": "HIGH",
        "basis": "SINGLE_SOURCE",
    }
    assert confidence_policy(
        "MEDIUM", "CONTRADICTORY", strict_corroboration=True
    ) == {"level": "MEDIUM", "basis": "CONTESTED"}


def test_surface_rule_uses_only_prepared_project_entity_reference():
    assert should_surface_atom("panel:VRU-1") is True
    assert should_surface_atom(None) is False
    assert should_surface_atom("") is False


def test_policy_package_exposes_no_merge_executor():
    module = importlib.import_module(
        "backend.app.services.stage_comparison.unified_change_policy"
    )
    assert not any("merge" in name.casefold() for name in module.__all__)


@pytest.mark.parametrize(
    "dimension", ["OPERATION", "PARAMETER", "METHOD", "PRINCIPLE", "SPACE"]
)
def test_graphic_checked_claim_cannot_override_unobservable_dimension(dimension):
    gate = check_source_validity(
        source_valid=True,
        coverage_by_side=CHECKED_BOTH,
        document_binding_state="DOCUMENT_BINDING_PROVEN",
        graphic_dimension=dimension,
    )

    assert gate == {
        "gate": "M7",
        "state": GateState.NOT_APPLICABLE.value,
        "reason_codes": ["graphic_route_cannot_observe_dimension"],
    }


@pytest.mark.parametrize(
    ("text_direction", "graphic_direction"),
    [("ALTERED", "ALTERED"), ("ADDED", "REMOVED")],
)
def test_operation_graphic_evidence_cannot_corroborate_or_contradict(
    text_direction, graphic_direction
):
    result = evaluate_source_relation(
        text_state="VALID",
        graphic_state="VALID",
        scope_compatible=True,
        subject_relation="SAME_ENTITY",
        document_binding_state="DOCUMENT_BINDING_PROVEN",
        text_dimension="OPERATION",
        graphic_dimension="OPERATION",
        text_direction=text_direction,
        graphic_direction=graphic_direction,
        coverage_by_side=CHECKED_BOTH,
    )

    assert result["relation_status"] is None
    assert result["outcome"] == "REVIEW_REQUIRED"
    assert result["reason_codes"] == ["graphic_route_cannot_observe_dimension"]


@pytest.mark.parametrize(
    "omitted", ["scope_compatible", "subject_relation", "document_binding_state"]
)
def test_source_relation_requires_provenance_and_identity_inputs(omitted):
    facts = {
        "text_state": "VALID",
        "graphic_state": "VALID",
        "scope_compatible": True,
        "subject_relation": "SAME_ENTITY",
        "document_binding_state": "DOCUMENT_BINDING_PROVEN",
    }
    facts.pop(omitted)

    with pytest.raises(TypeError):
        evaluate_source_relation(**facts)


def test_m3_observes_links_but_does_not_upgrade_m2_subject_identity():
    gates = _candidate(subject_relation="UNKNOWN")

    assert gates["gates"]["M2"]["state"] == GateState.REVIEW_REQUIRED.value
    assert gates["gates"]["M3"]["state"] == GateState.PASS.value
    assert "M2" in gates["live_decision_gates"]
    assert "M3" in gates["observation_only_gates"]
