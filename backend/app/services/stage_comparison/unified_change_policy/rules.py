"""Pure G2.4.5 gates and source-relation policy facts.

This module deliberately has no merge executor.  It reports M1–M8 and a
relation/outcome; a later stage may consume those facts under a separate
contract.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Mapping

from .confidence import normalize_confidence
from .contract import (
    UNKNOWN_DIMENSION,
    Direction,
    Outcome,
    PolicyValidationError,
    SourceFactState,
    SourceRelationStatus,
    SubjectRelation,
    resolve_dimension,
)


LIVE_DECISION_GATES = ("M1", "M2", "M7", "M8")
OBSERVATION_ONLY_GATES = ("M3", "M4", "M5", "M6")


class GateState(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


def _gate(gate: str, state: GateState, *reason_codes: str) -> dict[str, Any]:
    if gate not in {f"M{index}" for index in range(1, 9)}:
        raise PolicyValidationError("gate: M1..M8 required")
    if not reason_codes:
        raise PolicyValidationError(f"{gate}: reason code required")
    return {
        "gate": gate,
        "state": state.value,
        "reason_codes": sorted(set(reason_codes)),
    }


def _reference(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _enum(value: Any, enum_type: type[Enum], where: str) -> Enum:
    raw = value.value if isinstance(value, enum_type) else value
    try:
        return enum_type(raw)
    except (TypeError, ValueError) as error:
        raise PolicyValidationError(f"{where}: unsupported") from error


def check_scope_compatibility(left_scope_ref: Any, right_scope_ref: Any) -> dict[str, Any]:
    left = _reference(left_scope_ref)
    right = _reference(right_scope_ref)
    if left is None or right is None:
        return _gate("M1", GateState.REVIEW_REQUIRED, "scope_identity_incomplete")
    if left != right:
        return _gate("M1", GateState.FAIL, "scope_incompatible")
    return _gate("M1", GateState.PASS, "scope_identity_equal")


def check_subject_identity(subject_relation: Any) -> dict[str, Any]:
    relation = _enum(subject_relation, SubjectRelation, "subject_relation")
    if relation == SubjectRelation.SAME_ENTITY:
        return _gate("M2", GateState.PASS, "subject_identity_equal")
    if relation == SubjectRelation.UNKNOWN:
        return _gate("M2", GateState.REVIEW_REQUIRED, "subject_identity_unknown")
    reason = (
        "subjects_related_but_not_identical"
        if relation == SubjectRelation.RELATED_ENTITY
        else "subject_identity_differs"
    )
    return _gate("M2", GateState.FAIL, reason)


def check_entity_link_strength(links_by_side: Any) -> dict[str, Any]:
    """M3 requires SAME_ENTITY/HIGH independently on LEFT and RIGHT."""
    if not isinstance(links_by_side, Mapping):
        return _gate("M3", GateState.REVIEW_REQUIRED, "entity_links_absent")
    missing = []
    weak = []
    conflicting = []
    for side in ("LEFT", "RIGHT"):
        link = links_by_side.get(side)
        if not isinstance(link, Mapping):
            missing.append(side.lower())
            continue
        relation = link.get("relation")
        try:
            confidence = normalize_confidence(link.get("confidence"))
        except PolicyValidationError:
            confidence = "UNKNOWN"
        if relation == "UNKNOWN":
            conflicting.append(side.lower())
        elif relation != "SAME_ENTITY" or confidence != "HIGH":
            weak.append(side.lower())
    if conflicting:
        return _gate(
            "M3", GateState.FAIL, *[f"{side}_entity_link_unknown" for side in conflicting]
        )
    if missing or weak:
        return _gate(
            "M3",
            GateState.REVIEW_REQUIRED,
            *[f"{side}_entity_link_absent" for side in missing],
            *[f"{side}_entity_link_not_same_high" for side in weak],
        )
    return _gate("M3", GateState.PASS, "same_entity_high_on_both_sides")


def check_same_dimension(left_dimension: Any, right_dimension: Any) -> dict[str, Any]:
    left = resolve_dimension(left_dimension)
    right = resolve_dimension(right_dimension)
    if UNKNOWN_DIMENSION in {left, right}:
        return _gate("M4", GateState.REVIEW_REQUIRED, "dimension_unknown")
    if left != right:
        return _gate("M4", GateState.FAIL, "dimensions_differ")
    return _gate("M4", GateState.PASS, "dimension_equal")


_CONTRADICTORY_DIRECTIONS = {
    frozenset({Direction.ADDED.value, Direction.REMOVED.value}),
    frozenset({Direction.INCREASED.value, Direction.DECREASED.value}),
}


def directions_contradict(left_direction: Any, right_direction: Any) -> bool:
    left = _enum(left_direction, Direction, "left_direction").value
    right = _enum(right_direction, Direction, "right_direction").value
    return frozenset({left, right}) in _CONTRADICTORY_DIRECTIONS


def check_direction_non_contradiction(
    left_direction: Any, right_direction: Any
) -> dict[str, Any]:
    if directions_contradict(left_direction, right_direction):
        return _gate("M5", GateState.FAIL, "directions_contradict")
    return _gate("M5", GateState.PASS, "directions_do_not_contradict")


def check_outcome_compatibility(left_outcome: Any, right_outcome: Any) -> dict[str, Any]:
    left = _enum(left_outcome, Outcome, "left_outcome")
    right = _enum(right_outcome, Outcome, "right_outcome")
    if Outcome.REVIEW_REQUIRED in {left, right}:
        return _gate("M6", GateState.REVIEW_REQUIRED, "outcome_requires_review")
    if left != right:
        return _gate("M6", GateState.FAIL, "material_and_detail_outcomes_differ")
    return _gate("M6", GateState.PASS, "outcomes_equal")


def check_source_validity(
    *,
    source_valid: bool,
    coverage_by_side: Any,
    document_binding_state: Any,
) -> dict[str, Any]:
    """M7: cross-source GRAPHIC use requires binding and per-side CHECKED."""
    if source_valid is not True:
        return _gate("M7", GateState.REVIEW_REQUIRED, "source_self_reports_review")
    if document_binding_state != "DOCUMENT_BINDING_PROVEN":
        return _gate("M7", GateState.FAIL, "document_binding_not_proven")
    if not isinstance(coverage_by_side, Mapping):
        return _gate("M7", GateState.FAIL, "coverage_by_side_absent")
    states = {side: coverage_by_side.get(side) for side in ("LEFT", "RIGHT")}
    if any(state == "CHECK_BLOCKED" for state in states.values()):
        return _gate("M7", GateState.REVIEW_REQUIRED, "coverage_check_blocked")
    if states != {"LEFT": "CHECKED", "RIGHT": "CHECKED"}:
        reasons = []
        for side, state in states.items():
            if state == "NOT_APPLICABLE":
                reasons.append(f"{side.lower()}_coverage_not_applicable")
            elif state == "NOT_CHECKED":
                reasons.append(f"{side.lower()}_coverage_not_checked")
            elif state != "CHECKED":
                reasons.append(f"{side.lower()}_coverage_absent")
        return _gate("M7", GateState.FAIL, *reasons)
    return _gate("M7", GateState.PASS, "both_sides_checked_under_proven_binding")


def check_cardinality_safety(text_count: Any, graphic_count: Any) -> dict[str, Any]:
    counts = (text_count, graphic_count)
    if any(
        not isinstance(item, int) or isinstance(item, bool) or item < 0
        for item in counts
    ):
        raise PolicyValidationError("cardinality: non-negative integers required")
    if counts == (1, 1):
        return _gate("M8", GateState.PASS, "one_to_one_cardinality")
    if 0 in counts:
        return _gate("M8", GateState.REVIEW_REQUIRED, "candidate_side_absent")
    if text_count == 1:
        direction = "one_to_many"
    elif graphic_count == 1:
        direction = "many_to_one"
    else:
        direction = "many_to_many"
    return _gate("M8", GateState.FAIL, f"{direction}_cardinality_ambiguous")


def evaluate_candidate_gates(
    *,
    left_scope_ref: Any,
    right_scope_ref: Any,
    subject_relation: Any,
    links_by_side: Any,
    left_dimension: Any,
    right_dimension: Any,
    left_direction: Any,
    right_direction: Any,
    left_outcome: Any,
    right_outcome: Any,
    source_valid: bool,
    coverage_by_side: Any,
    document_binding_state: Any,
    text_count: int,
    graphic_count: int,
) -> dict[str, Any]:
    gates = [
        check_scope_compatibility(left_scope_ref, right_scope_ref),
        check_subject_identity(subject_relation),
        check_entity_link_strength(links_by_side),
        check_same_dimension(left_dimension, right_dimension),
        check_direction_non_contradiction(left_direction, right_direction),
        check_outcome_compatibility(left_outcome, right_outcome),
        check_source_validity(
            source_valid=source_valid,
            coverage_by_side=coverage_by_side,
            document_binding_state=document_binding_state,
        ),
        check_cardinality_safety(text_count, graphic_count),
    ]
    return {
        "gates": {item["gate"]: item for item in gates},
        "live_decision_gates": list(LIVE_DECISION_GATES),
        "observation_only_gates": list(OBSERVATION_ONLY_GATES),
    }


def contradiction_is_proven(gates: Any) -> bool:
    """Silence/N/A/unchecked coverage can never become a contradiction."""
    values = gates.get("gates") if isinstance(gates, Mapping) else None
    if not isinstance(values, Mapping):
        return False
    required_pass = ("M1", "M2", "M4", "M7", "M8")
    return all(
        isinstance(values.get(gate), Mapping)
        and values[gate].get("state") == GateState.PASS.value
        for gate in required_pass
    ) and isinstance(values.get("M5"), Mapping) and values["M5"].get(
        "state"
    ) == GateState.FAIL.value


def _combined_outcome(left: Outcome, right: Outcome) -> Outcome:
    if Outcome.REVIEW_REQUIRED in {left, right}:
        return Outcome.REVIEW_REQUIRED
    if Outcome.MATERIAL_CHANGE in {left, right}:
        return Outcome.MATERIAL_CHANGE
    return Outcome.DETAIL_ONLY


def _result(
    relation: SourceRelationStatus | None,
    outcome: Outcome,
    *reason_codes: str,
) -> dict[str, Any]:
    return {
        "relation_status": relation.value if relation is not None else None,
        "outcome": outcome.value,
        "reason_codes": sorted(set(reason_codes)),
        "live_decision_gates": list(LIVE_DECISION_GATES),
        "observation_only_gates": list(OBSERVATION_ONLY_GATES),
    }


def evaluate_source_relation(
    *,
    text_state: Any,
    graphic_state: Any,
    text_outcome: Any = Outcome.MATERIAL_CHANGE,
    graphic_outcome: Any = Outcome.MATERIAL_CHANGE,
    scope_compatible: bool | None = True,
    subject_relation: Any = SubjectRelation.SAME_ENTITY,
    text_dimension: Any = UNKNOWN_DIMENSION,
    graphic_dimension: Any = UNKNOWN_DIMENSION,
    text_direction: Any = Direction.ALTERED,
    graphic_direction: Any = Direction.ALTERED,
    coverage_by_side: Any = None,
    document_binding_state: Any = "DOCUMENT_BINDING_PROVEN",
    text_count: int = 1,
    graphic_count: int = 1,
) -> dict[str, Any]:
    """Classify only structured policy facts; never execute a merge."""
    text = _enum(text_state, SourceFactState, "text_state")
    graphic = _enum(graphic_state, SourceFactState, "graphic_state")
    text_result = _enum(text_outcome, Outcome, "text_outcome")
    graphic_result = _enum(graphic_outcome, Outcome, "graphic_outcome")
    subject = _enum(subject_relation, SubjectRelation, "subject_relation")

    valid = [text == SourceFactState.VALID, graphic == SourceFactState.VALID]
    review_states = {SourceFactState.REVIEW_REQUIRED, SourceFactState.CHECK_BLOCKED}
    silent_states = {
        SourceFactState.ABSENT,
        SourceFactState.NOT_CHECKED,
        SourceFactState.NOT_APPLICABLE,
    }
    if sum(valid) == 1 and (graphic if valid[0] else text) in silent_states:
        outcome = text_result if valid[0] else graphic_result
        return _result(
            SourceRelationStatus.SINGLE_SOURCE,
            outcome,
            "second_source_has_no_valid_evidence",
        )
    if sum(valid) == 1 and (graphic if valid[0] else text) in review_states:
        return _result(None, Outcome.REVIEW_REQUIRED, "source_fact_requires_review")
    if sum(valid) == 0:
        if text in review_states or graphic in review_states:
            return _result(None, Outcome.REVIEW_REQUIRED, "source_fact_requires_review")
        raise PolicyValidationError("source relation: at least one evidence source required")

    combined = _combined_outcome(text_result, graphic_result)
    if scope_compatible is False:
        return _result(SourceRelationStatus.UNRELATED, combined, "scope_incompatible")
    if scope_compatible is None:
        return _result(None, Outcome.REVIEW_REQUIRED, "scope_compatibility_unknown")
    if subject == SubjectRelation.DIFFERENT_ENTITY:
        return _result(SourceRelationStatus.UNRELATED, combined, "subjects_unrelated")
    if subject == SubjectRelation.UNKNOWN:
        return _result(None, Outcome.REVIEW_REQUIRED, "subject_identity_unknown")
    if subject == SubjectRelation.RELATED_ENTITY:
        return _result(
            SourceRelationStatus.COMPLEMENTARY,
            combined,
            "subjects_related_but_not_identical",
        )

    cardinality = check_cardinality_safety(text_count, graphic_count)
    if cardinality["state"] != GateState.PASS.value:
        return _result(None, Outcome.REVIEW_REQUIRED, *cardinality["reason_codes"])

    left_dimension = resolve_dimension(text_dimension)
    right_dimension = resolve_dimension(graphic_dimension)
    if UNKNOWN_DIMENSION in {left_dimension, right_dimension}:
        return _result(None, Outcome.REVIEW_REQUIRED, "dimension_unknown")
    if left_dimension != right_dimension:
        return _result(
            SourceRelationStatus.COMPLEMENTARY,
            combined,
            "same_entity_different_dimensions",
        )
    if text_result != graphic_result:
        return _result(
            SourceRelationStatus.COMPLEMENTARY,
            combined,
            "material_and_detail_outcomes_differ",
        )

    coverage_gate = check_source_validity(
        source_valid=True,
        coverage_by_side=coverage_by_side,
        document_binding_state=document_binding_state,
    )
    if coverage_gate["state"] != GateState.PASS.value:
        return _result(None, Outcome.REVIEW_REQUIRED, *coverage_gate["reason_codes"])
    if directions_contradict(text_direction, graphic_direction):
        return _result(
            SourceRelationStatus.CONTRADICTORY,
            Outcome.REVIEW_REQUIRED,
            "same_scope_subject_dimension_checked_but_directions_contradict",
        )
    return _result(
        SourceRelationStatus.CORROBORATING,
        combined,
        "same_scope_subject_dimension_with_compatible_directions",
    )


def should_surface_atom(project_entity_ref: Any) -> bool:
    """Surface structured project entities; document-only atoms stay folded."""
    return _reference(project_entity_ref) is not None


__all__ = [
    "LIVE_DECISION_GATES",
    "OBSERVATION_ONLY_GATES",
    "GateState",
    "check_cardinality_safety",
    "check_direction_non_contradiction",
    "check_entity_link_strength",
    "check_outcome_compatibility",
    "check_same_dimension",
    "check_scope_compatibility",
    "check_source_validity",
    "check_subject_identity",
    "contradiction_is_proven",
    "directions_contradict",
    "evaluate_candidate_gates",
    "evaluate_source_relation",
    "should_surface_atom",
]
