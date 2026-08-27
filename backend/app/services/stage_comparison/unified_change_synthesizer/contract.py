"""Versioned G2.4.6 contracts for deterministic unified change synthesis.

The contract accepts facts already structured by upstream producers.  It does
not contain free-text fields from which a dimension, entity, direction, or
outcome could be inferred.
"""
from __future__ import annotations

import json
from typing import Any, Iterable, Mapping

from ..unified_change_policy import (
    UNKNOWN_DIMENSION,
    Outcome,
    PolicyValidationError,
    normalize_evidence_atom,
)
from ..unified_change_policy.contract import POLICY_VERSION


INPUT_VERSION = "unified-change-synthesis-input.v1"
SYNTHESIS_VERSION = "unified-change-synthesis.v1"
IDENTITY_VERSION = "unified-change-identity.v1"
KIND = "stage_comparison_unified_changes"
DIRECTION = "LEFT_TO_RIGHT"

SOURCE_MODES = frozenset({"TEXT", "GRAPHIC", "BOTH"})
REVIEW_STATUSES = frozenset({"CONFIRMED", "REVIEW_REQUIRED"})
RELATION_STATUSES = frozenset(
    {
        "SINGLE_SOURCE",
        "CORROBORATING",
        "CONTRADICTORY",
    }
)

_ATOM_REQUIRED = {
    "atom_id",
    "source",
    "scope_ref",
    "subject_ref",
    "project_entity_ref",
    "dimension",
    "direction",
    "outcome",
    "confidence",
    "evidence_ref",
    "source_artifact",
    "provenance",
}
_ATOM_OPTIONAL = {
    "facet_ref",
    "before_value",
    "after_value",
    "review_status",
}
_SOURCE_ARTIFACT_KEYS = {"kind", "schema_version", "artifact_ref"}


class SynthesisValidationError(ValueError):
    """A synthesis input or output violates the G2.4.6 contract."""


def _json_compatible(value: Any, where: str) -> None:
    try:
        json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise SynthesisValidationError(f"{where}: JSON-compatible value required") from error


def _reference(value: Any, where: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value.strip():
        suffix = " or null" if nullable else ""
        raise SynthesisValidationError(
            f"{where}: non-empty string{suffix} required"
        )
    return value.strip()


def normalize_source_artifact(value: Any) -> dict[str, str]:
    """Return a closed, stable reference to one immutable source artifact."""
    if not isinstance(value, Mapping) or set(value) != _SOURCE_ARTIFACT_KEYS:
        raise SynthesisValidationError("source_artifact: invalid fields")
    return {
        key: _reference(value[key], f"source_artifact.{key}")  # type: ignore[dict-item]
        for key in ("kind", "schema_version", "artifact_ref")
    }


def normalize_synthesis_atom(value: Any) -> dict[str, Any]:
    """Validate one already-structured TEXT or GRAPHIC evidence atom."""
    if not isinstance(value, Mapping):
        raise SynthesisValidationError("atom: object required")
    fields = set(value)
    if not _ATOM_REQUIRED <= fields or not fields <= _ATOM_REQUIRED | _ATOM_OPTIONAL:
        raise SynthesisValidationError("atom: invalid fields")

    core = {key: value[key] for key in (
        "atom_id",
        "source",
        "scope_ref",
        "subject_ref",
        "project_entity_ref",
        "dimension",
        "direction",
        "outcome",
        "confidence",
    )}
    try:
        normalized = normalize_evidence_atom(core)
    except PolicyValidationError as error:
        raise SynthesisValidationError(str(error)) from error

    normalized["scope_ref"] = _reference(normalized["scope_ref"], "atom.scope_ref")
    normalized["subject_ref"] = _reference(
        normalized["subject_ref"], "atom.subject_ref"
    )
    normalized["project_entity_ref"] = _reference(
        normalized["project_entity_ref"],
        "atom.project_entity_ref",
        nullable=True,
    )
    normalized["facet_ref"] = _reference(
        value.get("facet_ref"), "atom.facet_ref", nullable=True
    )
    normalized["before_value"] = value.get("before_value")
    normalized["after_value"] = value.get("after_value")
    normalized["evidence_ref"] = _reference(
        value["evidence_ref"], "atom.evidence_ref"
    )
    normalized["source_artifact"] = normalize_source_artifact(
        value["source_artifact"]
    )
    if not isinstance(value["provenance"], Mapping) or not value["provenance"]:
        raise SynthesisValidationError("atom.provenance: non-empty object required")
    normalized["provenance"] = dict(value["provenance"])
    _json_compatible(normalized["provenance"], "atom.provenance")
    _json_compatible(normalized["before_value"], "atom.before_value")
    _json_compatible(normalized["after_value"], "atom.after_value")

    default_review = (
        "REVIEW_REQUIRED"
        if normalized["outcome"] == Outcome.REVIEW_REQUIRED.value
        or normalized["dimension"] == UNKNOWN_DIMENSION
        else "CONFIRMED"
    )
    review_status = value.get("review_status", default_review)
    if review_status not in REVIEW_STATUSES:
        raise SynthesisValidationError("atom.review_status: unsupported")
    if normalized["dimension"] == UNKNOWN_DIMENSION:
        review_status = "REVIEW_REQUIRED"
    normalized["review_status"] = review_status
    return normalized


def normalize_candidate(value: Any) -> dict[str, Any]:
    """Validate an explicit upstream TEXT↔GRAPHIC candidate, without matching."""
    required = {
        "candidate_id",
        "text_atom_id",
        "graphic_atom_id",
        "subject_relation",
        "links_by_side",
        "source_valid",
        "coverage_by_side",
        "document_binding_state",
        "text_count",
        "graphic_count",
        "subject_identity_provenance",
    }
    if not isinstance(value, Mapping) or set(value) != required:
        raise SynthesisValidationError("candidate: invalid fields")
    normalized = dict(value)
    for key in ("candidate_id", "text_atom_id", "graphic_atom_id"):
        normalized[key] = _reference(value[key], f"candidate.{key}")
    if not isinstance(value["source_valid"], bool):
        raise SynthesisValidationError("candidate.source_valid: boolean required")
    for key in ("text_count", "graphic_count"):
        count = value[key]
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise SynthesisValidationError(
                f"candidate.{key}: non-negative integer required"
            )
    if not isinstance(value["subject_identity_provenance"], Mapping) or not value[
        "subject_identity_provenance"
    ]:
        raise SynthesisValidationError(
            "candidate.subject_identity_provenance: non-empty object required"
        )
    for key in ("links_by_side", "coverage_by_side", "subject_identity_provenance"):
        _json_compatible(value[key], f"candidate.{key}")
    return normalized


def canonical_source_artifacts(
    atoms: Iterable[Mapping[str, Any]],
) -> list[dict[str, str]]:
    artifacts = {
        json.dumps(
            normalize_source_artifact(atom["source_artifact"]),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ): normalize_source_artifact(atom["source_artifact"])
        for atom in atoms
    }
    return [artifacts[key] for key in sorted(artifacts)]


__all__ = [
    "DIRECTION",
    "IDENTITY_VERSION",
    "INPUT_VERSION",
    "KIND",
    "POLICY_VERSION",
    "RELATION_STATUSES",
    "REVIEW_STATUSES",
    "SOURCE_MODES",
    "SYNTHESIS_VERSION",
    "SynthesisValidationError",
    "canonical_source_artifacts",
    "normalize_candidate",
    "normalize_source_artifact",
    "normalize_synthesis_atom",
]
