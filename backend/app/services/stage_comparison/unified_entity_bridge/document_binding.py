"""Deterministic proof that graphic blocks belong to the pair's documents.

Stage 5.3 identifies a pair of documents.  The graphic route identifies blocks.
Nothing in the G2.4.x artifacts used to record that both describe the *same*
PDFs, so a drawing from another object could be joined to this pair unnoticed.

This module closes that gap with data, not with the fact of the CLI call.  It
compares the document descriptor carried by every graphic block pair side
against the document descriptor of the corresponding pair side and reports one
of three states.  ``UNPROVEN`` and ``MISMATCH`` are deliberately different:
absence of evidence is not evidence of absence, so a missing descriptor never
produces a contradiction verdict.

No fuzzy matching, no filename similarity, no ordering dependence: document
codes are compared for exact equality and every list is sorted.
"""
from __future__ import annotations

from typing import Any


BINDING_VERSION = "document-binding-v1"

BINDING_PROVEN = "DOCUMENT_BINDING_PROVEN"
BINDING_MISMATCH = "DOCUMENT_BINDING_MISMATCH"
BINDING_UNPROVEN = "DOCUMENT_BINDING_UNPROVEN"

BINDING_STATES = frozenset({BINDING_PROVEN, BINDING_MISMATCH, BINDING_UNPROVEN})

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
    """Validate one document descriptor and return its canonical form."""
    if value is None:
        return dict(ABSENT_DESCRIPTOR)
    if not isinstance(value, dict):
        raise DocumentBindingValidationError(f"{where}: object or null required")
    unknown = set(value) - {"document_code", "source_path", "provenance"}
    if unknown:
        raise DocumentBindingValidationError(
            f"{where}: unknown fields {sorted(unknown)}"
        )
    document_code = _optional_text(value.get("document_code"), f"{where}.document_code")
    source_path = _optional_text(value.get("source_path"), f"{where}.source_path")
    provenance = value.get("provenance")
    if provenance is None:
        provenance = PROVENANCE_ABSENT if document_code is None else PROVENANCE_CLI_ARGUMENT
    if provenance not in PROVENANCE_SOURCES:
        raise DocumentBindingValidationError(
            f"{where}.provenance: one of {sorted(PROVENANCE_SOURCES)} required"
        )
    if document_code is None and provenance != PROVENANCE_ABSENT:
        raise DocumentBindingValidationError(
            f"{where}: provenance without document_code must be {PROVENANCE_ABSENT}"
        )
    if document_code is not None and provenance == PROVENANCE_ABSENT:
        raise DocumentBindingValidationError(
            f"{where}: document_code present but provenance is {PROVENANCE_ABSENT}"
        )
    return {
        "document_code": document_code,
        "source_path": source_path,
        "provenance": provenance,
    }


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
        path = raw.get("pdf_path") or raw.get("relative") or raw.get("filename")
        documents[side] = normalize_document_descriptor(
            {
                "document_code": code if isinstance(code, str) and code.strip() else None,
                "source_path": path if isinstance(path, str) and path.strip() else None,
                "provenance": (
                    PROVENANCE_ARTIFACT
                    if isinstance(code, str) and code.strip()
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
            descriptor = (pair.get(side_key) or {}).get("document")
            observed.append(
                normalize_document_descriptor(descriptor, f"block pair.{side_key}.document")
            )
    return observed, any_pair


def _verify_side(
    expected: dict[str, Any] | None, observed: list[dict[str, Any]]
) -> dict[str, Any]:
    expected_code = (expected or {}).get("document_code")
    observed_codes = sorted(
        {item["document_code"] for item in observed if item["document_code"] is not None}
    )
    missing = [item for item in observed if item["document_code"] is None]
    reasons: list[str] = []
    if expected_code is None:
        reasons.append("pair_document_code_absent")
    if missing:
        reasons.append("block_document_code_absent")
    differing = sorted(code for code in observed_codes if code != expected_code)
    if expected_code is not None and differing:
        reasons.append("block_document_code_differs_from_pair_document")
        state = BINDING_MISMATCH
    elif expected_code is None or missing or not observed_codes:
        if not observed_codes and not missing:
            reasons.append("no_graphic_block_pairs_on_side")
        state = BINDING_UNPROVEN
    else:
        reasons.append("every_block_document_code_equals_pair_document")
        state = BINDING_PROVEN
    return {
        "state": state,
        "reason_codes": sorted(set(reasons)),
        "expected_document_code": expected_code,
        "expected_source_path": (expected or {}).get("source_path"),
        "expected_provenance": (expected or {}).get("provenance", PROVENANCE_ABSENT),
        "observed_document_codes": observed_codes,
        "unbound_block_pairs": len(missing),
    }


def verify_document_binding(
    pair_documents: Any, graphic_scope_groups: Any
) -> dict[str, Any]:
    """Prove — or refuse to prove — that graphic blocks belong to the pair.

    Returns one of ``DOCUMENT_BINDING_PROVEN`` / ``DOCUMENT_BINDING_MISMATCH`` /
    ``DOCUMENT_BINDING_UNPROVEN`` with the reason codes behind the verdict.

    ``MISMATCH`` is reported only when a block's document code is known *and*
    differs from the pair's document code on the same side.  Every other
    incomplete situation is ``UNPROVEN``.
    """
    documents = normalize_pair_documents(pair_documents)
    sides: dict[str, Any] = {}
    any_pair = False
    for side in SIDES:
        observed, seen = _observed_documents(graphic_scope_groups, side)
        any_pair = any_pair or seen
        sides[side] = _verify_side(
            (documents or {}).get(side) if documents else None, observed
        )

    reasons: list[str] = []
    if not any_pair:
        state = BINDING_UNPROVEN
        reasons.append("no_graphic_scope_groups")
    elif any(sides[side]["state"] == BINDING_MISMATCH for side in SIDES):
        state = BINDING_MISMATCH
        reasons.append("document_binding_contradicted_by_data")
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
            "expected_document_code",
            "expected_source_path",
            "expected_provenance",
            "observed_document_codes",
            "unbound_block_pairs",
        }:
            raise DocumentBindingValidationError(
                f"document binding.sides.{side}: invalid fields"
            )
        if item["state"] not in BINDING_STATES:
            raise DocumentBindingValidationError(
                f"document binding.sides.{side}.state: unsupported"
            )
        if not isinstance(item["observed_document_codes"], list) or item[
            "observed_document_codes"
        ] != sorted(item["observed_document_codes"]):
            raise DocumentBindingValidationError(
                f"document binding.sides.{side}.observed_document_codes: sorted array required"
            )
        if not isinstance(item["unbound_block_pairs"], int) or item["unbound_block_pairs"] < 0:
            raise DocumentBindingValidationError(
                f"document binding.sides.{side}.unbound_block_pairs: non-negative int required"
            )
    return payload


__all__ = [
    "ABSENT_DESCRIPTOR",
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
    "normalize_document_descriptor",
    "normalize_pair_documents",
    "pair_documents_from_pair_artifact",
    "validate_document_binding",
    "verify_document_binding",
]
