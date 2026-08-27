"""G2.4.6 identity extension for atomic changes and presentation records."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, Mapping

from ..unified_change_policy import UNKNOWN_DIMENSION
from .contract import IDENTITY_VERSION, SynthesisValidationError


CANONICAL_SYNTHESIS_DIGEST_VERSION = "canonical-synthesis-digest.v1"


def digest(value: Any) -> str:
    try:
        raw = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise SynthesisValidationError("identity: JSON-compatible value required") from error
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def canonical_synthesis_digest(payload: Mapping[str, Any]) -> str:
    """Return the versioned canonical digest of a complete synthesis.

    The digest input is the entire payload, without excluded fields, wrapped
    with a domain/version marker. Serialization is UTF-8 JSON with Unicode
    preserved, keys sorted recursively, compact comma/colon separators, and
    NaN/Infinity rejected. The lowercase SHA-256 hex digest is returned.
    """
    if (
        not isinstance(payload, Mapping)
        or payload.get("synthesis_version") != "unified-change-synthesis.v1"
    ):
        raise SynthesisValidationError(
            "canonical synthesis digest: unified-change-synthesis.v1 required"
        )
    return digest(
        {
            "digest_version": CANONICAL_SYNTHESIS_DIGEST_VERSION,
            "synthesis": dict(payload),
        }
    )


def canonical_atomic_identity(
    atom: Mapping[str, Any],
    *,
    evidence_scoped: bool = True,
) -> dict[str, Any]:
    """Build the G2.4.6 identity cell without changing G2.4.5.

    ``facet_ref`` is an explicit upstream property reference.  The production
    synthesizer evidence-scopes every atomic fact from its first appearance;
    this optional switch remains available only for callers that need the
    non-unique base cell for grouping or diagnostics.
    """
    if atom.get("dimension") == UNKNOWN_DIMENSION:
        raise SynthesisValidationError(
            "atomic identity: UNKNOWN_DIMENSION belongs to review_items"
        )
    required = ("scope_ref", "subject_ref", "dimension", "direction", "atom_id")
    if any(not isinstance(atom.get(key), str) or not atom[key].strip() for key in required):
        raise SynthesisValidationError("atomic identity: incomplete atom")
    facet_ref = atom.get("facet_ref")
    if facet_ref is not None and (
        not isinstance(facet_ref, str) or not facet_ref.strip()
    ):
        raise SynthesisValidationError("atomic identity.facet_ref: invalid")
    use_evidence_scope = evidence_scoped or (
        atom["dimension"] == "PARAMETER" and facet_ref is None
    )
    return {
        "identity_version": IDENTITY_VERSION,
        "scope_ref": atom["scope_ref"],
        "subject_ref": atom["subject_ref"],
        "dimension": atom["dimension"],
        "direction_class": atom["direction"],
        "facet_ref": facet_ref,
        "evidence_scope": atom["atom_id"] if use_evidence_scope else None,
    }


def stable_atomic_change_id(identity: Mapping[str, Any]) -> str:
    expected = {
        "identity_version",
        "scope_ref",
        "subject_ref",
        "dimension",
        "direction_class",
        "facet_ref",
        "evidence_scope",
    }
    evidence_scope = identity.get("evidence_scope")
    if (
        set(identity) != expected
        or identity.get("identity_version") != IDENTITY_VERSION
        or not isinstance(evidence_scope, str)
        or not evidence_scope.strip()
    ):
        raise SynthesisValidationError("atomic identity: invalid fields or version")
    return "uchg_" + digest(dict(identity))[:20]


def stable_review_item_id(atom: Mapping[str, Any]) -> str:
    return "ureview_" + digest(
        {
            "identity_version": IDENTITY_VERSION,
            "atom_id": atom.get("atom_id"),
            "evidence_ref": atom.get("evidence_ref"),
        }
    )[:20]


def stable_group_id(
    prefix: str,
    identity: Mapping[str, Any],
) -> str:
    return prefix + digest(dict(identity))[:20]


def content_signature(evidence: Iterable[Mapping[str, Any]]) -> str:
    canonical = sorted(
        json.dumps(
            dict(item),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        for item in evidence
    )
    return digest(canonical)


__all__ = [
    "CANONICAL_SYNTHESIS_DIGEST_VERSION",
    "canonical_atomic_identity",
    "canonical_synthesis_digest",
    "content_signature",
    "digest",
    "stable_atomic_change_id",
    "stable_group_id",
    "stable_review_item_id",
]
