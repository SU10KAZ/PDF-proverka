"""Ordinal confidence rules; no arithmetic combination is permitted."""
from __future__ import annotations

from typing import Any

from .contract import (
    ConfidenceBasis,
    ConfidenceLevel,
    PolicyValidationError,
    SourceRelationStatus,
)


_REINFORCEMENT = {
    ConfidenceLevel.UNKNOWN.value: ConfidenceLevel.UNKNOWN.value,
    ConfidenceLevel.LOW.value: ConfidenceLevel.MEDIUM.value,
    ConfidenceLevel.MEDIUM.value: ConfidenceLevel.HIGH.value,
    ConfidenceLevel.HIGH.value: ConfidenceLevel.HIGH.value,
}


def normalize_confidence(value: Any) -> str:
    raw = value.value if isinstance(value, ConfidenceLevel) else value
    if not isinstance(raw, str):
        raise PolicyValidationError("confidence: ordinal string required")
    normalized = raw.strip().upper()
    try:
        return ConfidenceLevel(normalized).value
    except ValueError as error:
        raise PolicyValidationError(
            "confidence: HIGH, MEDIUM, LOW, or UNKNOWN required"
        ) from error


def reinforce_confidence(value: Any, *, strict_corroboration: bool) -> str:
    normalized = normalize_confidence(value)
    return _REINFORCEMENT[normalized] if strict_corroboration else normalized


def confidence_policy(
    primary: Any,
    relation_status: Any,
    *,
    strict_corroboration: bool = False,
) -> dict[str, str]:
    """Return an ordinal level and its basis without averaging sources."""
    level = normalize_confidence(primary)
    raw_relation = (
        relation_status.value
        if isinstance(relation_status, SourceRelationStatus)
        else relation_status
    )
    try:
        relation = SourceRelationStatus(raw_relation)
    except (TypeError, ValueError) as error:
        raise PolicyValidationError("relation_status: unsupported") from error

    if relation == SourceRelationStatus.CORROBORATING:
        level = reinforce_confidence(
            level, strict_corroboration=strict_corroboration
        )
        basis = ConfidenceBasis.CORROBORATED
    elif relation == SourceRelationStatus.COMPLEMENTARY:
        basis = ConfidenceBasis.COMPLEMENTARY
    elif relation == SourceRelationStatus.CONTRADICTORY:
        basis = ConfidenceBasis.CONTESTED
    else:
        # UNRELATED evidence and silence never modify the primary source.
        basis = ConfidenceBasis.SINGLE_SOURCE
    return {"level": level, "basis": basis.value}


__all__ = ["confidence_policy", "normalize_confidence", "reinforce_confidence"]
