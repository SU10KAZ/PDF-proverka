"""Small shared primitives for versioned Stage Comparison artifacts.

The production comparison flow persists logically separate JSON artifacts.  A
canonical signature is used for staleness checks and stable identities; no
artifact identity depends on list order or a UI row number.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
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


@lru_cache(maxsize=256)
def _file_sha256(
    resolved_path: str,
    size: int,
    mtime_ns: int,
    ctime_ns: int,
) -> str:
    del size, mtime_ns, ctime_ns  # cache-key fields; content is read below
    digest = hashlib.sha256()
    with Path(resolved_path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_content_identity(path: str | Path) -> dict[str, Any]:
    """Return a private source identity that detects same-size/mtime rewrites."""
    source = Path(path)
    try:
        stat = source.stat()
        resolved = str(source.resolve())
        return {
            "path": resolved,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "ctime_ns": stat.st_ctime_ns,
            "sha256": _file_sha256(
                resolved,
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
            ),
        }
    except OSError:
        return {
            "path": str(source),
            "size": None,
            "mtime_ns": None,
            "ctime_ns": None,
            "sha256": None,
        }


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
    "file_content_identity",
    "stable_id",
    "utc_now",
]
