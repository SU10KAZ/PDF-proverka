"""Stable G2.4.5 change identity, independent of attached evidence."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

from .contract import Direction, PolicyValidationError, resolve_dimension


def _digest(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _reference(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PolicyValidationError(f"{where}: non-empty string required")
    return value.strip()


def canonical_identity_cell(
    scope_ref: Any,
    subject_ref: Any,
    dimension: Any,
    direction_class: Any,
) -> dict[str, str]:
    """Build ``(scope, subject, dimension, direction_class)`` canonically."""
    raw_direction = (
        direction_class.value
        if isinstance(direction_class, Direction)
        else direction_class
    )
    try:
        direction = Direction(raw_direction).value
    except (TypeError, ValueError) as error:
        raise PolicyValidationError("direction_class: unsupported") from error
    return {
        "scope_ref": _reference(scope_ref, "scope_ref"),
        "subject_ref": _reference(subject_ref, "subject_ref"),
        "dimension": resolve_dimension(dimension),
        "direction_class": direction,
    }


def stable_change_id(identity_cell: Any) -> str:
    if not isinstance(identity_cell, dict):
        raise PolicyValidationError("identity_cell: object required")
    canonical = canonical_identity_cell(
        identity_cell.get("scope_ref"),
        identity_cell.get("subject_ref"),
        identity_cell.get("dimension"),
        identity_cell.get("direction_class"),
    )
    if set(identity_cell) != set(canonical):
        raise PolicyValidationError("identity_cell: invalid fields")
    return "change_" + _digest(canonical)[:20]


def content_signature(evidence_atoms: Iterable[Any]) -> str:
    """Order-independent content signature; additions intentionally change it."""
    canonical = [
        json.dumps(
            item,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        for item in evidence_atoms
    ]
    return _digest(sorted(canonical))


__all__ = ["canonical_identity_cell", "content_signature", "stable_change_id"]
