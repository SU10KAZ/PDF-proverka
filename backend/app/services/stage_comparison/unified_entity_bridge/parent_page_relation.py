"""Strict contract for externally proven excerpt-to-parent page relations.

This module validates evidence produced elsewhere.  It performs no discovery,
filename matching, PDF search, resource fingerprinting, or parent inference.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .document_binding import (
    document_identity_is_complete,
    normalize_document_descriptor,
)
from .page_identity import graphic_page_index_0based_to_canonical_index


RELATION_VERSION = "parent-page-relation.v1"
RELATION_PROVEN = "PROVEN"
RELATION_AMBIGUOUS = "AMBIGUOUS"
RELATION_UNPROVEN = "UNPROVEN"
RELATION_MISMATCH = "MISMATCH"
RELATION_STATES = frozenset(
    {RELATION_PROVEN, RELATION_AMBIGUOUS, RELATION_UNPROVEN, RELATION_MISMATCH}
)


class ParentPageRelationValidationError(ValueError):
    """An external parent-page assertion is malformed or unsafe to consume."""


def normalize_page_reference(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"document", "page_index_0based"}:
        raise ParentPageRelationValidationError(f"{where}: invalid page reference")
    document = normalize_document_descriptor(value["document"], f"{where}.document")
    if not document_identity_is_complete(document):
        raise ParentPageRelationValidationError(
            f"{where}.document: complete document/version identity required"
        )
    page_index = graphic_page_index_0based_to_canonical_index(
        value["page_index_0based"]
    )
    return {"document": document, "page_index_0based": page_index}


def validate_parent_page_relation(payload: Any) -> dict[str, Any]:
    expected = {
        "relation_version",
        "state",
        "reason_codes",
        "excerpt",
        "parent",
        "evidence",
    }
    if not isinstance(payload, dict) or set(payload) != expected:
        raise ParentPageRelationValidationError("parent page relation: invalid envelope")
    if payload["relation_version"] != RELATION_VERSION:
        raise ParentPageRelationValidationError("parent page relation: unsupported version")
    if payload["state"] not in RELATION_STATES:
        raise ParentPageRelationValidationError("parent page relation.state: unsupported")
    if (
        not isinstance(payload["reason_codes"], list)
        or not payload["reason_codes"]
        or any(not isinstance(item, str) or not item for item in payload["reason_codes"])
        or payload["reason_codes"] != sorted(set(payload["reason_codes"]))
    ):
        raise ParentPageRelationValidationError(
            "parent page relation.reason_codes: sorted unique strings required"
        )
    excerpt = normalize_page_reference(payload["excerpt"], "parent relation.excerpt")
    parent = (
        normalize_page_reference(payload["parent"], "parent relation.parent")
        if payload["parent"] is not None
        else None
    )
    evidence = payload["evidence"]
    if evidence is not None and (
        not isinstance(evidence, dict)
        or set(evidence) != {"producer_id", "evidence_id"}
        or any(not isinstance(item, str) or not item for item in evidence.values())
    ):
        raise ParentPageRelationValidationError(
            "parent page relation.evidence: producer_id/evidence_id required"
        )
    if payload["state"] == RELATION_PROVEN and (parent is None or evidence is None):
        raise ParentPageRelationValidationError(
            "PROVEN parent page relation requires parent and external evidence"
        )
    return {
        **payload,
        "excerpt": excerpt,
        "parent": parent,
        "reason_codes": sorted(set(payload["reason_codes"])),
    }


def normalize_parent_page_relations(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ParentPageRelationValidationError("parent_page_relations: array required")
    normalized = [validate_parent_page_relation(item) for item in value]
    normalized.sort(
        key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True)
    )
    canonical = [
        json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for item in normalized
    ]
    if len(canonical) != len(set(canonical)):
        raise ParentPageRelationValidationError("parent_page_relations: duplicate relation")
    return normalized


def make_parent_page_relation(
    *,
    state: str,
    excerpt: Any,
    parent: Any = None,
    reason_codes: list[str],
    evidence: Any = None,
) -> dict[str, Any]:
    """Build the strict envelope from an external producer's assertion."""
    return validate_parent_page_relation(
        {
            "relation_version": RELATION_VERSION,
            "state": state,
            "reason_codes": sorted(set(reason_codes)),
            "excerpt": excerpt,
            "parent": parent,
            "evidence": evidence,
        }
    )


def schema_path() -> Path:
    return Path(__file__).with_name("parent_page_relation.schema.json")


__all__ = [
    "ParentPageRelationValidationError",
    "RELATION_AMBIGUOUS",
    "RELATION_MISMATCH",
    "RELATION_PROVEN",
    "RELATION_STATES",
    "RELATION_UNPROVEN",
    "RELATION_VERSION",
    "make_parent_page_relation",
    "normalize_page_reference",
    "normalize_parent_page_relations",
    "schema_path",
    "validate_parent_page_relation",
]
