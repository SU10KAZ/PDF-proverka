"""Independent final validation for ``unified-change-synthesis.v1``."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from ..unified_change_policy import (
    CONFIDENCE_BASES,
    CONFIDENCE_LEVELS,
    DIRECTIONS,
    DIMENSIONS,
    EVIDENCE_DIMENSIONS,
    OUTCOMES,
    UNKNOWN_DIMENSION,
)
from ..unified_change_policy.contract import POLICY_VERSION
from .contract import (
    DIRECTION,
    IDENTITY_VERSION,
    INPUT_VERSION,
    KIND,
    RELATION_STATUSES,
    REVIEW_STATUSES,
    SOURCE_MODES,
    SYNTHESIS_VERSION,
    SynthesisValidationError,
    normalize_source_artifact,
)
from .identity import (
    content_signature,
    stable_atomic_change_id,
    stable_group_id,
    stable_review_item_id,
)
from .presentation import PRESENTATION_VERSION


_TOP_LEVEL_KEYS = {
    "synthesis_version",
    "kind",
    "direction",
    "policy_version",
    "identity_version",
    "changes",
    "review_items",
    "contested_groups",
    "presentation_groups",
    "diagnostics",
    "source_artifacts",
    "provenance",
    "validation",
}
_CHANGE_KEYS = {
    "change_id",
    "scope_ref",
    "subject_ref",
    "project_entity_ref",
    "facet_ref",
    "dimension",
    "direction",
    "outcome",
    "source_mode",
    "evidence_refs",
    "relation_status",
    "confidence",
    "before_value",
    "after_value",
    "review_status",
    "content_signature",
    "provenance",
}
_REVIEW_KEYS = {
    "review_evidence_id",
    "atom_id",
    "source",
    "scope_ref",
    "subject_ref",
    "project_entity_ref",
    "facet_ref",
    "dimension",
    "direction",
    "outcome",
    "confidence",
    "before_value",
    "after_value",
    "evidence_refs",
    "review_status",
    "reason_codes",
    "content_signature",
    "provenance",
}
_EVIDENCE_KEYS = {"evidence_ref", "atom_id", "source", "source_artifact"}
_CONTEST_KEYS = {
    "group_id",
    "change_ids",
    "evidence_refs",
    "relation_status",
    "review_status",
    "reason_codes",
    "provenance",
}
_PRESENTATION_KEYS = {
    "group_id",
    "scope_ref",
    "subject_ref",
    "family",
    "change_ids",
    "title",
    "provenance",
}


def _fields(value: Any, expected: set[str], where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise SynthesisValidationError(f"{where}: invalid fields")
    return value


def _ref(value: Any, where: str, *, nullable: bool = False) -> str | None:
    if nullable and value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise SynthesisValidationError(f"{where}: non-empty string required")
    return value


def _unique_refs(values: Any, where: str, *, minimum: int = 1) -> list[str]:
    if (
        not isinstance(values, list)
        or len(values) < minimum
        or any(not isinstance(value, str) or not value for value in values)
        or len(values) != len(set(values))
    ):
        raise SynthesisValidationError(f"{where}: unique string array required")
    return values


def _confidence(value: Any, where: str) -> None:
    item = _fields(value, {"level", "basis"}, where)
    if item["level"] not in CONFIDENCE_LEVELS or item["basis"] not in CONFIDENCE_BASES:
        raise SynthesisValidationError(f"{where}: unsupported confidence")


def _evidence(values: Any, where: str) -> list[dict[str, Any]]:
    if not isinstance(values, list) or not values:
        raise SynthesisValidationError(f"{where}: non-empty array required")
    output: list[dict[str, Any]] = []
    links: set[tuple[str, str]] = set()
    for index, value in enumerate(values):
        item_where = f"{where}[{index}]"
        item = _fields(value, _EVIDENCE_KEYS, item_where)
        evidence_ref = _ref(item["evidence_ref"], f"{item_where}.evidence_ref")
        atom_id = _ref(item["atom_id"], f"{item_where}.atom_id")
        source = item["source"]
        if source not in {"TEXT", "GRAPHIC"}:
            raise SynthesisValidationError(f"{item_where}.source: unsupported")
        link = (source, str(evidence_ref))
        if link in links:
            raise SynthesisValidationError(f"{item_where}: duplicate evidence link")
        links.add(link)
        output.append(
            {
                "evidence_ref": evidence_ref,
                "atom_id": atom_id,
                "source": source,
                "source_artifact": normalize_source_artifact(
                    item["source_artifact"]
                ),
            }
        )
    expected = sorted(
        output,
        key=lambda item: (
            {"TEXT": 0, "GRAPHIC": 1}[item["source"]],
            item["evidence_ref"],
            item["atom_id"],
        ),
    )
    if output != expected:
        raise SynthesisValidationError(f"{where}: deterministic order required")
    return output


def _validate_change(change: Any, index: int) -> dict[str, Any]:
    where = f"changes[{index}]"
    item = _fields(change, _CHANGE_KEYS, where)
    _ref(item["change_id"], f"{where}.change_id")
    _ref(item["scope_ref"], f"{where}.scope_ref")
    _ref(item["subject_ref"], f"{where}.subject_ref")
    _ref(item["project_entity_ref"], f"{where}.project_entity_ref", nullable=True)
    _ref(item["facet_ref"], f"{where}.facet_ref", nullable=True)
    if item["dimension"] not in DIMENSIONS:
        raise SynthesisValidationError(f"{where}.dimension: resolved dimension required")
    if item["direction"] not in DIRECTIONS or item["outcome"] not in OUTCOMES:
        raise SynthesisValidationError(f"{where}: unsupported direction/outcome")
    if item["source_mode"] not in SOURCE_MODES:
        raise SynthesisValidationError(f"{where}.source_mode: unsupported")
    if item["relation_status"] not in RELATION_STATUSES:
        raise SynthesisValidationError(f"{where}.relation_status: unsupported")
    if item["review_status"] not in REVIEW_STATUSES:
        raise SynthesisValidationError(f"{where}.review_status: unsupported")
    evidence = _evidence(item["evidence_refs"], f"{where}.evidence_refs")
    evidence_sources = {value["source"] for value in evidence}
    expected_mode = "BOTH" if len(evidence_sources) == 2 else next(iter(evidence_sources))
    if item["source_mode"] != expected_mode:
        raise SynthesisValidationError(f"{where}.source_mode: evidence mismatch")
    if item["source_mode"] == "BOTH" and item["relation_status"] != "CORROBORATING":
        raise SynthesisValidationError(f"{where}: BOTH must be corroborating")
    if item["relation_status"] == "CORROBORATING" and item["source_mode"] != "BOTH":
        raise SynthesisValidationError(f"{where}: corroboration needs both sources")
    _confidence(item["confidence"], f"{where}.confidence")
    identity = (item.get("provenance") or {}).get("identity")
    if not isinstance(identity, Mapping) or stable_atomic_change_id(identity) != item[
        "change_id"
    ]:
        raise SynthesisValidationError(f"{where}.change_id: identity mismatch")
    if content_signature(evidence) != item["content_signature"]:
        raise SynthesisValidationError(f"{where}.content_signature: mismatch")
    return dict(item)


def _validate_review(item: Any, index: int) -> dict[str, Any]:
    where = f"review_items[{index}]"
    value = _fields(item, _REVIEW_KEYS, where)
    atom_id = _ref(value["atom_id"], f"{where}.atom_id")
    evidence = _evidence(value["evidence_refs"], f"{where}.evidence_refs")
    if len(evidence) != 1 or evidence[0]["atom_id"] != atom_id:
        raise SynthesisValidationError(f"{where}.evidence_refs: atom mismatch")
    expected_id = stable_review_item_id(
        {"atom_id": atom_id, "evidence_ref": evidence[0]["evidence_ref"]}
    )
    if value["review_evidence_id"] != expected_id:
        raise SynthesisValidationError(f"{where}.review_evidence_id: mismatch")
    source = value["source"]
    if (
        source not in {"TEXT", "GRAPHIC"}
        or source != evidence[0]["source"]
        or value["dimension"] not in EVIDENCE_DIMENSIONS
        or value["outcome"] != "REVIEW_REQUIRED"
        or value["review_status"] != "REVIEW_REQUIRED"
    ):
        raise SynthesisValidationError(f"{where}: invalid review state")
    _ref(value["scope_ref"], f"{where}.scope_ref")
    _ref(value["subject_ref"], f"{where}.subject_ref", nullable=True)
    project_entity_ref = _ref(
        value["project_entity_ref"],
        f"{where}.project_entity_ref",
        nullable=True,
    )
    _ref(value["facet_ref"], f"{where}.facet_ref", nullable=True)
    if value["direction"] not in DIRECTIONS:
        raise SynthesisValidationError(f"{where}.direction: unsupported")
    _confidence(value["confidence"], f"{where}.confidence")
    reason_codes = _unique_refs(value["reason_codes"], f"{where}.reason_codes")
    unknown_dimension = value["dimension"] == UNKNOWN_DIMENSION
    engineering_scope_unresolved = source == "TEXT" and project_entity_ref is None
    if not (unknown_dimension or engineering_scope_unresolved):
        raise SynthesisValidationError(f"{where}: review reason not established")
    required_reasons = {
        reason
        for applies, reason in (
            (unknown_dimension, "dimension_unknown"),
            (engineering_scope_unresolved, "engineering_scope_unresolved"),
        )
        if applies
    }
    if not required_reasons <= set(reason_codes):
        raise SynthesisValidationError(f"{where}.reason_codes: incomplete")
    if content_signature(evidence) != value["content_signature"]:
        raise SynthesisValidationError(f"{where}.content_signature: mismatch")
    return dict(value)


def _validate_contests(
    values: Any,
    changes: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        raise SynthesisValidationError("contested_groups: array required")
    output = []
    seen_changes: set[str] = set()
    for index, value in enumerate(values):
        where = f"contested_groups[{index}]"
        item = _fields(value, _CONTEST_KEYS, where)
        ids = _unique_refs(item["change_ids"], f"{where}.change_ids", minimum=2)
        if len(ids) != 2 or ids != sorted(ids) or any(change_id not in changes for change_id in ids):
            raise SynthesisValidationError(f"{where}.change_ids: invalid")
        if seen_changes & set(ids):
            raise SynthesisValidationError(f"{where}.change_ids: already contested")
        seen_changes.update(ids)
        if any(
            changes[change_id]["relation_status"] != "CONTRADICTORY"
            or changes[change_id]["review_status"] != "REVIEW_REQUIRED"
            for change_id in ids
        ):
            raise SynthesisValidationError(f"{where}: change state mismatch")
        if (
            item["relation_status"] != "CONTRADICTORY"
            or item["review_status"] != "REVIEW_REQUIRED"
        ):
            raise SynthesisValidationError(f"{where}: invalid contest state")
        _evidence(item["evidence_refs"], f"{where}.evidence_refs")
        _unique_refs(item["reason_codes"], f"{where}.reason_codes")
        output.append(dict(item))
    return output


def _validate_presentations(
    values: Any,
    changes: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        raise SynthesisValidationError("presentation_groups: array required")
    output = []
    represented: set[str] = set()
    for index, value in enumerate(values):
        where = f"presentation_groups[{index}]"
        item = _fields(value, _PRESENTATION_KEYS, where)
        ids = _unique_refs(item["change_ids"], f"{where}.change_ids", minimum=2)
        if ids != sorted(ids) or any(change_id not in changes for change_id in ids):
            raise SynthesisValidationError(f"{where}.change_ids: invalid")
        if represented & set(ids):
            raise SynthesisValidationError(f"{where}.change_ids: duplicate grouping")
        represented.update(ids)
        if item["family"] != "PARAMETER" or any(
            changes[change_id]["dimension"] != "PARAMETER"
            or changes[change_id]["scope_ref"] != item["scope_ref"]
            or changes[change_id]["subject_ref"] != item["subject_ref"]
            or changes[change_id]["relation_status"] == "CONTRADICTORY"
            for change_id in ids
        ):
            raise SynthesisValidationError(f"{where}: incompatible members")
        identity = {
            "presentation_version": PRESENTATION_VERSION,
            "scope_ref": item["scope_ref"],
            "subject_ref": item["subject_ref"],
            "family": item["family"],
        }
        if item["group_id"] != stable_group_id("pgroup_", identity):
            raise SynthesisValidationError(f"{where}.group_id: mismatch")
        _ref(item["title"], f"{where}.title")
        output.append(dict(item))
    return output


def validate_synthesis(payload: Any) -> dict[str, Any]:
    """Validate the complete synthesis independently of its producer."""
    value = _fields(payload, _TOP_LEVEL_KEYS, "synthesis")
    if (
        value["synthesis_version"] != SYNTHESIS_VERSION
        or value["kind"] != KIND
        or value["direction"] != DIRECTION
        or value["policy_version"] != POLICY_VERSION
        or value["identity_version"] != IDENTITY_VERSION
    ):
        raise SynthesisValidationError("synthesis: unsupported contract")
    for name in ("changes", "review_items", "source_artifacts"):
        if not isinstance(value[name], list):
            raise SynthesisValidationError(f"synthesis.{name}: array required")
    changes_list = [
        _validate_change(change, index) for index, change in enumerate(value["changes"])
    ]
    if [item["change_id"] for item in changes_list] != sorted(
        item["change_id"] for item in changes_list
    ) or len({item["change_id"] for item in changes_list}) != len(changes_list):
        raise SynthesisValidationError("changes: unique deterministic order required")
    reviews = [
        _validate_review(item, index)
        for index, item in enumerate(value["review_items"])
    ]
    if [item["review_evidence_id"] for item in reviews] != sorted(
        item["review_evidence_id"] for item in reviews
    ) or len({item["review_evidence_id"] for item in reviews}) != len(reviews):
        raise SynthesisValidationError("review_items: unique deterministic order required")
    changes = {item["change_id"]: item for item in changes_list}
    contests = _validate_contests(value["contested_groups"], changes)
    presentations = _validate_presentations(value["presentation_groups"], changes)
    if [item["group_id"] for item in contests] != sorted(item["group_id"] for item in contests):
        raise SynthesisValidationError("contested_groups: deterministic order required")
    if [item["group_id"] for item in presentations] != sorted(item["group_id"] for item in presentations):
        raise SynthesisValidationError("presentation_groups: deterministic order required")

    artifacts = [normalize_source_artifact(item) for item in value["source_artifacts"]]
    serialized_artifacts = [json.dumps(item, sort_keys=True) for item in artifacts]
    if serialized_artifacts != sorted(set(serialized_artifacts)):
        raise SynthesisValidationError("source_artifacts: unique deterministic order required")
    if not isinstance(value["diagnostics"], Mapping) or not isinstance(
        value["provenance"], Mapping
    ):
        raise SynthesisValidationError("synthesis: diagnostics/provenance objects required")
    if (
        value["provenance"].get("input_contract") != INPUT_VERSION
        or value["provenance"].get("uses_llm") is not False
    ):
        raise SynthesisValidationError("synthesis.provenance: invalid")
    if value["validation"] != {
        "contract": SYNTHESIS_VERSION,
        "valid": True,
        "errors": [],
    }:
        raise SynthesisValidationError("synthesis.validation: invalid")
    try:
        json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise SynthesisValidationError("synthesis: not JSON-compatible") from error
    return dict(value)


def schema_path() -> Path:
    return Path(__file__).with_name("unified_change_synthesis_v1.schema.json")


__all__ = ["schema_path", "validate_synthesis"]
