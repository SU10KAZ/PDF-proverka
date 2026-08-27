"""Closed Stage 4 artifact for explicit semantic facts over Stage 3 deltas.

This stage validates structured facts supplied by an upstream deterministic
producer or a separately governed semantic hint.  It never infers a facet,
dimension or entity from similar free text, and it performs no model call.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from .production_artifacts import content_signature, stable_id, utc_now
from .text_differences import KIND as STAGE3_KIND, VERSION as STAGE3_VERSION
from .unified_change_policy import (
    CONFIDENCE_LEVELS,
    DIRECTIONS,
    EVIDENCE_DIMENSIONS,
    OUTCOMES,
)


KIND = "stage_comparison_text_semantic_validation"
SCHEMA_VERSION = "text-semantic-validation.v1"
PRODUCER_VERSION = "text-semantic-validation-v1"
STAGE3_DIGEST_VERSION = "stage3-semantic-content-v1"

_OPTIONAL_FACT_FIELDS = {
    "fact_id",
    "scope_ref",
    "subject_ref",
    "project_entity_ref",
    "facet_ref",
    "before_value",
    "after_value",
    "provenance",
}
_REQUIRED_FACT_FIELDS = {
    "source_evidence_ref",
    "dimension",
    "direction",
    "outcome",
    "confidence",
}


def stage3_evidence_ref(group: Mapping[str, Any], bucket: str, item: Mapping[str, Any]) -> str:
    """Stable source identity independent of Stage 3 array ordering."""
    identity = {
        "group_id": str(group.get("id") or ""),
        "bucket": bucket,
        "left_fragment_ids": sorted(str(value) for value in item.get("left_fragment_ids") or []),
        "right_fragment_ids": sorted(str(value) for value in item.get("right_fragment_ids") or []),
        "left_pages": sorted(int(value) for value in item.get("left_pages") or []),
        "right_pages": sorted(int(value) for value in item.get("right_pages") or []),
    }
    # Legacy/imported Stage 3 fixtures may not carry fragment ids.  Values are
    # then the only honest source identity; they are evidence, not a guessed
    # engineering facet.
    if not identity["left_fragment_ids"] and not identity["right_fragment_ids"]:
        identity["before"] = item.get("before")
        identity["after"] = item.get("after")
    return stable_id("tde_", identity)


def iter_stage3_evidence(text_differences: Mapping[str, Any]):
    if (
        text_differences.get("kind") != STAGE3_KIND
        or text_differences.get("version") != STAGE3_VERSION
    ):
        raise ValueError("Stage 3 text differences artifact required")
    for group in text_differences.get("sheet_groups") or []:
        if not isinstance(group, Mapping):
            raise ValueError("Stage 3 sheet group must be an object")
        for bucket in ("changed", "removed", "added"):
            values = group.get(bucket) or []
            if not isinstance(values, list):
                raise ValueError(f"Stage 3 {bucket} must be an array")
            for item in values:
                if not isinstance(item, Mapping):
                    raise ValueError(f"Stage 3 {bucket} item must be an object")
                yield stage3_evidence_ref(group, bucket, item), group, bucket, item


def stage3_content_signature(text_differences: Mapping[str, Any]) -> str:
    """Bind Stage 4 to canonical Stage 3 evidence, not a caller-owned label."""
    evidence = []
    references = []
    for evidence_ref, group, bucket, item in iter_stage3_evidence(text_differences):
        references.append(evidence_ref)
        evidence.append({
            "evidence_ref": evidence_ref,
            "bucket": bucket,
            "group": {
                "id": group.get("id"),
                "left_pages": sorted(int(value) for value in group.get("left_pages") or []),
                "right_pages": sorted(int(value) for value in group.get("right_pages") or []),
                "relation_type": group.get("relation_type"),
                "relation_status": group.get("relation_status"),
            },
            "item": dict(item),
        })
    if len(references) != len(set(references)):
        raise ValueError("Stage 3 contains duplicate evidence identity")
    evidence.sort(key=lambda value: (value["evidence_ref"], content_signature(value)))
    return content_signature({
        "digest_version": STAGE3_DIGEST_VERSION,
        "kind": text_differences.get("kind"),
        "version": text_differences.get("version"),
        "pair_id": text_differences.get("pair_id"),
        "algorithm": text_differences.get("algorithm"),
        "source_signature": text_differences.get("source_signature"),
        "evidence": evidence,
    })


def _reference(value: Any, where: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}: non-empty string required")
    return value.strip()


def _normalize_fact(
    fact: Mapping[str, Any],
    *,
    available_evidence: Mapping[str, str],
) -> dict[str, Any]:
    fields = set(fact)
    if not _REQUIRED_FACT_FIELDS <= fields or not fields <= (
        _REQUIRED_FACT_FIELDS | _OPTIONAL_FACT_FIELDS
    ):
        raise ValueError("semantic fact: invalid fields")
    source_ref = _reference(fact.get("source_evidence_ref"), "source_evidence_ref")
    if source_ref not in available_evidence:
        raise ValueError("semantic fact references unknown Stage 3 evidence")
    if fact.get("dimension") not in EVIDENCE_DIMENSIONS:
        raise ValueError("semantic fact.dimension: unsupported")
    if fact.get("direction") not in DIRECTIONS:
        raise ValueError("semantic fact.direction: unsupported")
    if fact.get("outcome") not in OUTCOMES:
        raise ValueError("semantic fact.outcome: unsupported")
    if fact.get("confidence") not in CONFIDENCE_LEVELS:
        raise ValueError("semantic fact.confidence: unsupported")
    normalized = {
        "source_evidence_ref": source_ref,
        "source_evidence_signature": available_evidence[source_ref],
        "scope_ref": _reference(fact.get("scope_ref"), "scope_ref", nullable=True),
        "subject_ref": _reference(fact.get("subject_ref"), "subject_ref", nullable=True),
        "project_entity_ref": _reference(
            fact.get("project_entity_ref"), "project_entity_ref", nullable=True
        ),
        "facet_ref": _reference(fact.get("facet_ref"), "facet_ref", nullable=True),
        "dimension": fact["dimension"],
        "direction": fact["direction"],
        "outcome": fact["outcome"],
        "confidence": fact["confidence"],
        "before_value": fact.get("before_value"),
        "after_value": fact.get("after_value"),
        "provenance": dict(fact.get("provenance") or {}),
    }
    fact_identity = {
        "source_evidence_ref": source_ref,
        "subject_ref": normalized["subject_ref"],
        "project_entity_ref": normalized["project_entity_ref"],
        "facet_ref": normalized["facet_ref"],
        "dimension": normalized["dimension"],
        "direction": normalized["direction"],
    }
    explicit_id = fact.get("fact_id")
    normalized["fact_id"] = (
        _reference(explicit_id, "fact_id")
        if explicit_id is not None
        else stable_id("tsf_", fact_identity)
    )
    return normalized


def build_semantic_validation(
    text_differences: Mapping[str, Any],
    facts: Iterable[Mapping[str, Any]] = (),
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    evidence = list(iter_stage3_evidence(text_differences))
    available = {
        evidence_ref: content_signature({"bucket": bucket, "item": dict(item)})
        for evidence_ref, _group, bucket, item in evidence
    }
    if len(available) != len(evidence):
        raise ValueError("Stage 3 contains duplicate evidence identity")
    normalized = sorted(
        (_normalize_fact(fact, available_evidence=available) for fact in facts),
        key=lambda item: item["fact_id"],
    )
    ids = [item["fact_id"] for item in normalized]
    if len(ids) != len(set(ids)):
        raise ValueError("semantic facts contain duplicate fact_id")
    stage3_signature = stage3_content_signature(text_differences)
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "generated_at": generated_at or utc_now(),
        "input_signature": content_signature({
            "producer": PRODUCER_VERSION,
            "stage3_signature": stage3_signature,
            "stage3_source_signature": text_differences.get("source_signature"),
            "facts": normalized,
        }),
        "stage3_signature": stage3_signature,
        "stage3_source_signature": text_differences.get("source_signature"),
        "facts": normalized,
        "unresolved_source_evidence": sorted(
            set(available) - {item["source_evidence_ref"] for item in normalized}
        ),
        "provenance": {
            "producer": PRODUCER_VERSION,
            "uses_model": False,
            "infers_from_free_text": False,
        },
    }


__all__ = [
    "KIND",
    "PRODUCER_VERSION",
    "SCHEMA_VERSION",
    "STAGE3_DIGEST_VERSION",
    "build_semantic_validation",
    "iter_stage3_evidence",
    "stage3_content_signature",
    "stage3_evidence_ref",
]
