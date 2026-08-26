"""Deterministic proof that graphic blocks belong to the pair's documents.

Stage 5.3 identifies a pair of documents.  The graphic route identifies blocks.
Nothing in the G2.4.x artifacts used to record that both describe the *same*
PDFs, so a drawing from another object could be joined to this pair unnoticed.

This module closes that gap with data, not with the fact of the CLI call.  It
compares the version-aware document reference carried by every graphic block
pair side against the reference of the corresponding selected source and
reports a fail-closed state.  ``UNPROVEN`` and ``MISMATCH`` are deliberately
different: absence of evidence is not evidence of absence, so a missing
descriptor never produces a contradiction verdict.

No fuzzy matching, no filename similarity, no ordering dependence: document
and version identities are compared for exact equality and every list is
sorted.  ``source_path`` is deliberately excluded from semantic identity.
"""
from __future__ import annotations

import json
from typing import Any


BINDING_VERSION = "document-binding-v2"

BINDING_PROVEN = "DOCUMENT_BINDING_PROVEN"
BINDING_MISMATCH = "DOCUMENT_BINDING_MISMATCH"
BINDING_UNPROVEN = "DOCUMENT_BINDING_UNPROVEN"
BINDING_AMBIGUOUS = "DOCUMENT_BINDING_AMBIGUOUS"
BINDING_ERROR = "DOCUMENT_BINDING_ERROR"

BINDING_STATES = frozenset(
    {
        BINDING_PROVEN,
        BINDING_MISMATCH,
        BINDING_UNPROVEN,
        BINDING_AMBIGUOUS,
        BINDING_ERROR,
    }
)

#: Where a document descriptor came from.  ``ARTIFACT`` means it was read from a
#: stored artifact keyed by the pair itself (strongest); ``CLI_ARGUMENT`` means a
#: caller supplied it out of band (weaker, but still recorded); ``ABSENT`` means
#: no descriptor exists at all.
PROVENANCE_ARTIFACT = "ARTIFACT"
PROVENANCE_CLI_ARGUMENT = "CLI_ARGUMENT"
PROVENANCE_ABSENT = "ABSENT"

PROVENANCE_SOURCES = frozenset(
    {PROVENANCE_ARTIFACT, PROVENANCE_CLI_ARGUMENT, PROVENANCE_ABSENT}
)

SIDES = ("LEFT", "RIGHT")

ABSENT_DESCRIPTOR: dict[str, Any] = {
    "document_code": None,
    "version_id": None,
    "storage_identity": None,
    "source_path": None,
    "provenance": PROVENANCE_ABSENT,
}


class DocumentBindingValidationError(ValueError):
    """A document descriptor is malformed and cannot be used as evidence."""


def _optional_text(value: Any, where: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise DocumentBindingValidationError(f"{where}: non-empty string or null required")
    return value


def normalize_document_descriptor(value: Any, where: str) -> dict[str, Any]:
    """Validate one version-aware document reference and return its canonical form."""
    if value is None:
        return dict(ABSENT_DESCRIPTOR)
    if not isinstance(value, dict):
        raise DocumentBindingValidationError(f"{where}: object or null required")
    unknown = set(value) - {
        "document_code",
        "version_id",
        "storage_identity",
        "source_path",
        "provenance",
    }
    if unknown:
        raise DocumentBindingValidationError(
            f"{where}: unknown fields {sorted(unknown)}"
        )
    document_code = _optional_text(value.get("document_code"), f"{where}.document_code")
    version_id = _optional_text(value.get("version_id"), f"{where}.version_id")
    storage_identity = _optional_text(
        value.get("storage_identity"), f"{where}.storage_identity"
    )
    source_path = _optional_text(value.get("source_path"), f"{where}.source_path")
    provenance = value.get("provenance")
    identity_present = document_code is not None or storage_identity is not None
    if provenance is None:
        provenance = (
            PROVENANCE_CLI_ARGUMENT if identity_present or version_id is not None
            else PROVENANCE_ABSENT
        )
    if provenance not in PROVENANCE_SOURCES:
        raise DocumentBindingValidationError(
            f"{where}.provenance: one of {sorted(PROVENANCE_SOURCES)} required"
        )
    if not identity_present and version_id is None and provenance != PROVENANCE_ABSENT:
        raise DocumentBindingValidationError(
            f"{where}: provenance without identity must be {PROVENANCE_ABSENT}"
        )
    if (identity_present or version_id is not None) and provenance == PROVENANCE_ABSENT:
        raise DocumentBindingValidationError(
            f"{where}: identity present but provenance is {PROVENANCE_ABSENT}"
        )
    return {
        "document_code": document_code,
        "version_id": version_id,
        "storage_identity": storage_identity,
        "source_path": source_path,
        "provenance": provenance,
    }


def document_identity_is_complete(value: Any) -> bool:
    """Return whether a reference contains document *and* version identity."""
    try:
        descriptor = normalize_document_descriptor(value, "document reference")
    except DocumentBindingValidationError:
        return False
    return bool(
        descriptor["version_id"]
        and (descriptor["storage_identity"] or descriptor["document_code"])
    )


def compare_document_identity(expected: Any, observed: Any) -> tuple[str, list[str]]:
    """Compare two references without treating their locator as identity.

    A concrete contradiction produces ``MISMATCH``.  Missing document or
    version identity produces ``UNPROVEN``.  Exact document/version identity
    produces ``PROVEN``.
    """
    left = normalize_document_descriptor(expected, "expected document")
    right = normalize_document_descriptor(observed, "observed document")
    if not document_identity_is_complete(left) or not document_identity_is_complete(right):
        reasons = []
        if not left["version_id"]:
            reasons.append("expected_version_id_absent")
        if not right["version_id"]:
            reasons.append("observed_version_id_absent")
        if not (left["storage_identity"] or left["document_code"]):
            reasons.append("expected_document_identity_absent")
        if not (right["storage_identity"] or right["document_code"]):
            reasons.append("observed_document_identity_absent")
        return BINDING_UNPROVEN, sorted(set(reasons))

    comparable_document_identity = bool(
        (left["storage_identity"] and right["storage_identity"])
        or (left["document_code"] and right["document_code"])
    )
    if not comparable_document_identity:
        return BINDING_UNPROVEN, ["document_identity_not_comparable"]

    contradictions = []
    if (
        left["storage_identity"] is not None
        and right["storage_identity"] is not None
        and left["storage_identity"] != right["storage_identity"]
    ):
        contradictions.append("storage_identity_differs")
    if (
        left["document_code"] is not None
        and right["document_code"] is not None
        and left["document_code"] != right["document_code"]
    ):
        contradictions.append("document_code_differs")
    if left["version_id"] != right["version_id"]:
        contradictions.append("version_id_differs")
    if contradictions:
        return BINDING_MISMATCH, contradictions
    return BINDING_PROVEN, ["document_and_version_identity_equal"]


def normalize_pair_documents(value: Any) -> dict[str, Any] | None:
    """Validate the LEFT/RIGHT document descriptors of the Stage 5.3 pair."""
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != set(SIDES):
        raise DocumentBindingValidationError(
            "pair_documents: object with LEFT and RIGHT required"
        )
    return {
        side: normalize_document_descriptor(value[side], f"pair_documents.{side}")
        for side in SIDES
    }


def pair_documents_from_pair_artifact(
    pair_artifact: Any, stage53: Any = None
) -> dict[str, Any]:
    """Read LEFT/RIGHT document descriptors from a stored ``pair.json``.

    The pair artifact is keyed by ``pair_id``; when ``stage53`` is supplied the
    two are cross-checked so the descriptors cannot silently describe a
    different pair than the one being joined.
    """
    if not isinstance(pair_artifact, dict):
        raise DocumentBindingValidationError("pair artifact: object required")
    pair_id = pair_artifact.get("id")
    if not isinstance(pair_id, str) or not pair_id.strip():
        raise DocumentBindingValidationError("pair artifact.id: non-empty string required")
    if stage53 is not None:
        expected = (stage53 or {}).get("pair_id") if isinstance(stage53, dict) else None
        if expected != pair_id:
            raise DocumentBindingValidationError(
                "pair artifact.id does not match Stage 5.3 pair_id"
            )
    documents: dict[str, Any] = {}
    for side in SIDES:
        raw = pair_artifact.get(side.lower())
        if not isinstance(raw, dict):
            documents[side] = dict(ABSENT_DESCRIPTOR)
            continue
        code = raw.get("document_code")
        version_id = raw.get("version_id")
        storage_identity = raw.get("storage_identity") or raw.get("session_document_id")
        path = raw.get("pdf_path") or raw.get("relative") or raw.get("filename")
        identity_present = any(
            isinstance(item, str) and item.strip()
            for item in (code, version_id, storage_identity)
        )
        documents[side] = normalize_document_descriptor(
            {
                "document_code": code if isinstance(code, str) and code.strip() else None,
                "version_id": (
                    version_id
                    if isinstance(version_id, str) and version_id.strip()
                    else None
                ),
                "storage_identity": (
                    storage_identity
                    if isinstance(storage_identity, str) and storage_identity.strip()
                    else None
                ),
                "source_path": path if isinstance(path, str) and path.strip() else None,
                "provenance": (
                    PROVENANCE_ARTIFACT
                    if identity_present
                    else PROVENANCE_ABSENT
                ),
            },
            f"pair artifact.{side.lower()}",
        )
    return documents


def document_descriptor_for_block(
    blocks_payload: Any,
    block_id: Any,
    *,
    document_code: Any,
    version_id: Any = None,
    storage_identity: Any = None,
    source_path: Any = None,
    provenance: str = PROVENANCE_ARTIFACT,
) -> dict[str, Any]:
    """Bind one block id to the document whose ``blocks.json`` contains it.

    This is the data-level proof of ownership: the block must actually appear
    in that document's block index.  A block absent from the index is refused
    rather than described, so a wrong descriptor cannot be minted.
    """
    if not isinstance(blocks_payload, dict) or not isinstance(
        blocks_payload.get("blocks"), list
    ):
        raise DocumentBindingValidationError("blocks index: object with blocks[] required")
    if not isinstance(block_id, str) or not block_id.strip():
        raise DocumentBindingValidationError("blocks index: non-empty block id required")
    known = {
        str(record.get("block_id") or record.get("id") or "")
        for record in blocks_payload["blocks"]
        if isinstance(record, dict)
    }
    if block_id not in known:
        raise DocumentBindingValidationError(
            f"blocks index does not contain block {block_id}"
        )
    return normalize_document_descriptor(
        {
            "document_code": document_code,
            "version_id": version_id,
            "storage_identity": storage_identity,
            "source_path": source_path,
            "provenance": provenance,
        },
        f"block {block_id} document",
    )


def _observed_documents(
    graphic_scope_groups: Any, side: str
) -> tuple[list[dict[str, Any]], bool]:
    """Collect the document descriptors observed on one side of every block pair."""
    observed: list[dict[str, Any]] = []
    side_key = side.lower()
    any_pair = False
    for group in graphic_scope_groups or []:
        for pair in group.get("block_pairs") or []:
            any_pair = True
            pair_side = pair.get(side_key) or {}
            descriptor = pair_side.get("document")
            observed.append(
                {
                    "document": normalize_document_descriptor(
                        descriptor, f"block pair.{side_key}.document"
                    ),
                    "page_index_0based": pair_side.get("canonical_page_index"),
                }
            )
    return observed, any_pair


def _proven_parent_document(
    expected: dict[str, Any], observation: dict[str, Any], relations: list[dict[str, Any]]
) -> bool:
    for relation in relations:
        if relation.get("state") != "PROVEN":
            continue
        excerpt = relation.get("excerpt") or {}
        parent = relation.get("parent") or {}
        excerpt_state, _ = compare_document_identity(
            excerpt.get("document"), observation["document"]
        )
        parent_state, _ = compare_document_identity(
            expected, parent.get("document")
        )
        if (
            excerpt_state == BINDING_PROVEN
            and parent_state == BINDING_PROVEN
            and excerpt.get("page_index_0based") == observation["page_index_0based"]
        ):
            return True
    return False


def _verify_side(
    expected: dict[str, Any] | None,
    observed: list[dict[str, Any]],
    parent_page_relations: list[dict[str, Any]],
) -> dict[str, Any]:
    expected_reference = normalize_document_descriptor(expected, "expected document")
    observed_documents = [item["document"] for item in observed]
    observed_references = sorted(
        observed_documents,
        key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
    )
    missing = [
        item for item in observed if not document_identity_is_complete(item["document"])
    ]
    complete = [
        item for item in observed if document_identity_is_complete(item["document"])
    ]
    reasons: list[str] = []
    if not document_identity_is_complete(expected_reference):
        reasons.append("selected_document_version_identity_incomplete")
    if missing:
        reasons.append("block_document_version_identity_incomplete")

    verdicts = []
    for observation in complete:
        verdict = compare_document_identity(
            expected_reference, observation["document"]
        )
        if verdict[0] != BINDING_PROVEN and _proven_parent_document(
            expected_reference, observation, parent_page_relations
        ):
            verdict = (BINDING_PROVEN, ["proven_parent_page_relation_binds_document"])
        verdicts.append(verdict)
    for _, item_reasons in verdicts:
        reasons.extend(item_reasons)
    if any(state == BINDING_MISMATCH for state, _ in verdicts):
        state = BINDING_MISMATCH
    elif not document_identity_is_complete(expected_reference):
        distinct = {
            json.dumps(item["document"], sort_keys=True, separators=(",", ":"))
            for item in complete
        }
        state = BINDING_AMBIGUOUS if len(distinct) > 1 else BINDING_UNPROVEN
        if state == BINDING_AMBIGUOUS:
            reasons.append("multiple_observed_document_version_identities")
    elif missing or not complete:
        if not complete and not missing:
            reasons.append("no_graphic_block_pairs_on_side")
        state = BINDING_UNPROVEN
    else:
        reasons.append("every_block_document_version_equals_selected_source")
        state = BINDING_PROVEN
    return {
        "state": state,
        "reason_codes": sorted(set(reasons)),
        "expected_document": expected_reference,
        "observed_documents": observed_references,
        "unbound_block_pairs": len(missing),
    }


def verify_document_binding(
    pair_documents: Any,
    graphic_scope_groups: Any,
    parent_page_relations: Any = None,
) -> dict[str, Any]:
    """Prove — or refuse to prove — that graphic blocks belong to the pair.

    Returns one of ``DOCUMENT_BINDING_PROVEN`` / ``DOCUMENT_BINDING_MISMATCH`` /
    ``DOCUMENT_BINDING_UNPROVEN`` with the reason codes behind the verdict.

    ``MISMATCH`` is reported only when a block's document code is known *and*
    differs from the pair's document code on the same side.  Every other
    incomplete situation is ``UNPROVEN``.
    """
    documents = normalize_pair_documents(pair_documents)
    relations = parent_page_relations if isinstance(parent_page_relations, list) else []
    sides: dict[str, Any] = {}
    any_pair = False
    for side in SIDES:
        observed, seen = _observed_documents(graphic_scope_groups, side)
        any_pair = any_pair or seen
        sides[side] = _verify_side(
            (documents or {}).get(side) if documents else None, observed, relations
        )

    reasons: list[str] = []
    if not any_pair:
        state = BINDING_UNPROVEN
        reasons.append("no_graphic_scope_groups")
    elif any(sides[side]["state"] == BINDING_ERROR for side in SIDES):
        state = BINDING_ERROR
        reasons.append("document_binding_verification_error")
    elif any(sides[side]["state"] == BINDING_MISMATCH for side in SIDES):
        state = BINDING_MISMATCH
        reasons.append("document_binding_contradicted_by_data")
    elif any(sides[side]["state"] == BINDING_AMBIGUOUS for side in SIDES):
        state = BINDING_AMBIGUOUS
        reasons.append("document_binding_ambiguous")
    elif all(sides[side]["state"] == BINDING_PROVEN for side in SIDES):
        state = BINDING_PROVEN
        reasons.append("both_sides_bound_to_pair_documents")
    else:
        state = BINDING_UNPROVEN
        reasons.append("document_binding_not_provable_from_available_data")
    for side in SIDES:
        reasons.extend(f"{side.lower()}:{code}" for code in sides[side]["reason_codes"])

    return {
        "binding_version": BINDING_VERSION,
        "state": state,
        "reason_codes": sorted(set(reasons)),
        "pair_documents": documents,
        "sides": sides,
    }


def validate_document_binding(payload: Any) -> dict[str, Any]:
    """Validate a stored document binding block."""
    if not isinstance(payload, dict) or set(payload) != {
        "binding_version",
        "state",
        "reason_codes",
        "pair_documents",
        "sides",
    }:
        raise DocumentBindingValidationError("document binding: invalid envelope")
    if payload["binding_version"] != BINDING_VERSION:
        raise DocumentBindingValidationError("document binding: unsupported version")
    if payload["state"] not in BINDING_STATES:
        raise DocumentBindingValidationError("document binding.state: unsupported")
    if not isinstance(payload["reason_codes"], list) or not payload["reason_codes"]:
        raise DocumentBindingValidationError(
            "document binding.reason_codes: non-empty array required"
        )
    normalize_pair_documents(payload["pair_documents"])
    sides = payload["sides"]
    if not isinstance(sides, dict) or set(sides) != set(SIDES):
        raise DocumentBindingValidationError("document binding.sides: LEFT and RIGHT required")
    for side in SIDES:
        item = sides[side]
        if not isinstance(item, dict) or set(item) != {
            "state",
            "reason_codes",
            "expected_document",
            "observed_documents",
            "unbound_block_pairs",
        }:
            raise DocumentBindingValidationError(
                f"document binding.sides.{side}: invalid fields"
            )
        if item["state"] not in BINDING_STATES:
            raise DocumentBindingValidationError(
                f"document binding.sides.{side}.state: unsupported"
            )
        normalize_document_descriptor(
            item["expected_document"], f"document binding.sides.{side}.expected_document"
        )
        if not isinstance(item["observed_documents"], list):
            raise DocumentBindingValidationError(
                f"document binding.sides.{side}.observed_documents: array required"
            )
        normalized_observed = [
            normalize_document_descriptor(
                value, f"document binding.sides.{side}.observed_documents"
            )
            for value in item["observed_documents"]
        ]
        if normalized_observed != sorted(
            normalized_observed,
            key=lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")),
        ):
            raise DocumentBindingValidationError(
                f"document binding.sides.{side}.observed_documents: sorted array required"
            )
        if not isinstance(item["unbound_block_pairs"], int) or item["unbound_block_pairs"] < 0:
            raise DocumentBindingValidationError(
                f"document binding.sides.{side}.unbound_block_pairs: non-negative int required"
            )
    return payload


__all__ = [
    "ABSENT_DESCRIPTOR",
    "BINDING_AMBIGUOUS",
    "BINDING_ERROR",
    "BINDING_MISMATCH",
    "BINDING_PROVEN",
    "BINDING_STATES",
    "BINDING_UNPROVEN",
    "BINDING_VERSION",
    "DocumentBindingValidationError",
    "PROVENANCE_ABSENT",
    "PROVENANCE_ARTIFACT",
    "PROVENANCE_CLI_ARGUMENT",
    "PROVENANCE_SOURCES",
    "document_descriptor_for_block",
    "document_identity_is_complete",
    "compare_document_identity",
    "normalize_document_descriptor",
    "normalize_pair_documents",
    "pair_documents_from_pair_artifact",
    "validate_document_binding",
    "verify_document_binding",
]
