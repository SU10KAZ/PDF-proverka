"""Closed G2.4.5 enums and structured evidence-atom contract.

The policy consumes fields already produced by upstream stages.  It never
derives a dimension, entity, direction, or outcome from free text.
"""
from __future__ import annotations

from enum import Enum
from typing import Any


POLICY_VERSION = "unified-change-policy-v1"


class _PolicyEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class Dimension(_PolicyEnum):
    PRINCIPLE = "PRINCIPLE"
    METHOD = "METHOD"
    OPERATION = "OPERATION"
    STRUCTURE = "STRUCTURE"
    CONNECTION = "CONNECTION"
    TYPE = "TYPE"
    PARAMETER = "PARAMETER"
    QUANTITY = "QUANTITY"
    SPACE = "SPACE"


UNKNOWN_DIMENSION = "UNKNOWN_DIMENSION"
DIMENSIONS = tuple(item.value for item in Dimension)
EVIDENCE_DIMENSIONS = (*DIMENSIONS, UNKNOWN_DIMENSION)


class Outcome(_PolicyEnum):
    MATERIAL_CHANGE = "MATERIAL_CHANGE"
    DETAIL_ONLY = "DETAIL_ONLY"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class Direction(_PolicyEnum):
    ADDED = "ADDED"
    REMOVED = "REMOVED"
    REPLACED = "REPLACED"
    INCREASED = "INCREASED"
    DECREASED = "DECREASED"
    ALTERED = "ALTERED"


class SourceRelationStatus(_PolicyEnum):
    CORROBORATING = "CORROBORATING"
    COMPLEMENTARY = "COMPLEMENTARY"
    CONTRADICTORY = "CONTRADICTORY"
    UNRELATED = "UNRELATED"
    SINGLE_SOURCE = "SINGLE_SOURCE"


class ConfidenceLevel(_PolicyEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    UNKNOWN = "UNKNOWN"


class ConfidenceBasis(_PolicyEnum):
    SINGLE_SOURCE = "SINGLE_SOURCE"
    CORROBORATED = "CORROBORATED"
    COMPLEMENTARY = "COMPLEMENTARY"
    CONTESTED = "CONTESTED"


class SourceFactState(_PolicyEnum):
    """Whether one modality contributes a usable evidence atom."""

    VALID = "VALID"
    ABSENT = "ABSENT"
    NOT_CHECKED = "NOT_CHECKED"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    CHECK_BLOCKED = "CHECK_BLOCKED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class SubjectRelation(_PolicyEnum):
    SAME_ENTITY = "SAME_ENTITY"
    RELATED_ENTITY = "RELATED_ENTITY"
    DIFFERENT_ENTITY = "DIFFERENT_ENTITY"
    UNKNOWN = "UNKNOWN"


OUTCOMES = tuple(item.value for item in Outcome)
DIRECTIONS = tuple(item.value for item in Direction)
RELATION_STATUSES = tuple(item.value for item in SourceRelationStatus)
CONFIDENCE_LEVELS = tuple(item.value for item in ConfidenceLevel)
CONFIDENCE_BASES = tuple(item.value for item in ConfidenceBasis)


# Keep the G2.4.5 dimensions explicitly mapped to what the current GRAPHIC
# semantic route can actually observe.  A shared name is not evidence of
# support: graphic-coverage-policy-v2 observes only structural topology, type,
# and repeated-group quantity.  A future coverage producer may replace a null
# mapping only when it emits coverage for that dimension.
GRAPHIC_COVERAGE_DIMENSION_MAP = {
    Dimension.PRINCIPLE.value: None,
    Dimension.METHOD.value: None,
    Dimension.OPERATION.value: None,
    Dimension.STRUCTURE.value: Dimension.STRUCTURE.value,
    Dimension.CONNECTION.value: Dimension.CONNECTION.value,
    Dimension.TYPE.value: Dimension.TYPE.value,
    Dimension.PARAMETER.value: None,
    Dimension.QUANTITY.value: Dimension.QUANTITY.value,
    Dimension.SPACE.value: None,
}


class PolicyValidationError(ValueError):
    """Structured policy input violates the closed contract."""


def _enum_value(value: Any, enum_type: type[_PolicyEnum], where: str) -> str:
    raw = value.value if isinstance(value, enum_type) else value
    if not isinstance(raw, str):
        raise PolicyValidationError(f"{where}: string enum required")
    try:
        return enum_type(raw).value
    except ValueError as error:
        allowed = [item.value for item in enum_type]
        raise PolicyValidationError(f"{where}: one of {allowed} required") from error


def resolve_dimension(structured_value: Any) -> str:
    """Resolve only an explicit structured dimension, never free text.

    Multiple explicit candidates and missing data stay UNKNOWN rather than
    splitting an atom or guessing from its sentence.
    """
    value = (
        structured_value.get("dimension")
        if isinstance(structured_value, dict)
        else structured_value
    )
    if value is None or value == UNKNOWN_DIMENSION:
        return UNKNOWN_DIMENSION
    if isinstance(value, (list, tuple, set, frozenset)):
        candidates = {
            _enum_value(item, Dimension, "dimension") for item in value
        }
        return next(iter(candidates)) if len(candidates) == 1 else UNKNOWN_DIMENSION
    return _enum_value(value, Dimension, "dimension")


def normalize_evidence_atom(value: Any) -> dict[str, Any]:
    """Validate one atom; dimension belongs here, not on an aggregate header."""
    expected = {
        "atom_id",
        "source",
        "scope_ref",
        "subject_ref",
        "project_entity_ref",
        "dimension",
        "direction",
        "outcome",
        "confidence",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise PolicyValidationError("evidence atom: invalid fields")
    if not isinstance(value["atom_id"], str) or not value["atom_id"].strip():
        raise PolicyValidationError("evidence atom.atom_id: non-empty string required")
    if value["source"] not in {"TEXT", "GRAPHIC"}:
        raise PolicyValidationError("evidence atom.source: TEXT or GRAPHIC required")
    for key in ("scope_ref", "subject_ref", "project_entity_ref"):
        if value[key] is not None and (
            not isinstance(value[key], str) or not value[key].strip()
        ):
            raise PolicyValidationError(
                f"evidence atom.{key}: non-empty string or null required"
            )
    from .confidence import normalize_confidence

    dimension = resolve_dimension(value["dimension"])
    outcome = _enum_value(value["outcome"], Outcome, "outcome")
    if dimension == UNKNOWN_DIMENSION:
        outcome = Outcome.REVIEW_REQUIRED.value
    return {
        **value,
        "dimension": dimension,
        "direction": _enum_value(value["direction"], Direction, "direction"),
        "outcome": outcome,
        "confidence": normalize_confidence(value["confidence"]),
    }


__all__ = [
    "CONFIDENCE_BASES",
    "CONFIDENCE_LEVELS",
    "DIRECTIONS",
    "DIMENSIONS",
    "EVIDENCE_DIMENSIONS",
    "GRAPHIC_COVERAGE_DIMENSION_MAP",
    "OUTCOMES",
    "POLICY_VERSION",
    "RELATION_STATUSES",
    "UNKNOWN_DIMENSION",
    "ConfidenceBasis",
    "ConfidenceLevel",
    "Direction",
    "Dimension",
    "Outcome",
    "PolicyValidationError",
    "SourceFactState",
    "SourceRelationStatus",
    "SubjectRelation",
    "normalize_evidence_atom",
    "resolve_dimension",
]
