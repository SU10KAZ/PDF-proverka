"""Small shared primitives for versioned Stage Comparison artifacts.

The production comparison flow persists logically separate JSON artifacts.  A
canonical signature is used for staleness checks and stable identities; no
artifact identity depends on list order or a UI row number.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


def canonical_json(value: Any) -> str:
    """Serialize JSON data deterministically and reject NaN/Infinity."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def content_signature(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def stable_id(prefix: str, *parts: Any, length: int = 20) -> str:
    return prefix + content_signature(parts)[:length]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonical_strings(values: Iterable[Any] | None) -> list[str]:
    """Return a stable, case-insensitive set of non-empty string facts."""
    normalized = {
        " ".join(str(value).casefold().replace("ё", "е").split())
        for value in (values or ())
        if str(value or "").strip()
    }
    return sorted(normalized)


def artifact_is_stale(
    artifact: Mapping[str, Any] | None,
    current_input_signature: str,
) -> bool:
    return not artifact or artifact.get("input_signature") != current_input_signature


__all__ = [
    "artifact_is_stale",
    "canonical_json",
    "canonical_strings",
    "content_signature",
    "stable_id",
    "utc_now",
]
