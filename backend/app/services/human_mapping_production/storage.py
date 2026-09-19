"""Append-only JSONL stores with file locking, scoped by object/pair."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Iterable

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore

REPO = Path(__file__).resolve().parents[4]
DEFAULT_ROOT = REPO / "comparison" / "human_mapping"


def comparison_root() -> Path:
    env = os.environ.get("COMPARISON_ROOT", "").strip()
    if env:
        return Path(env) / "human_mapping"
    return DEFAULT_ROOT


# Strict subset of the project safe-ID alphabet (``stage_comparison.paths._safe_id``
# keeps alnum/-/_ by stripping; here anything else is REJECTED, never stripped,
# so two different unsafe IDs can never collapse into one directory).  A leading
# "_" is reserved for internal namespaces such as ``_smoke``.
_SAFE_SCOPE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}")


class InvalidScopeId(ValueError):
    """An object/pair/comparison identifier is not a safe storage segment."""

    def __init__(self, kind: str, value: object):
        super().__init__(f"invalid {kind} id")
        self.kind = kind
        self.value = value


def require_safe_id(value: object, kind: str) -> str:
    """Validate one storage path segment BEFORE any filesystem access."""
    if not isinstance(value, str) or not _SAFE_SCOPE_ID.fullmatch(value):
        raise InvalidScopeId(kind, value)
    return value


def pair_dir(object_id: str, pair_id: str, *, smoke: bool = False) -> Path:
    object_id = require_safe_id(object_id, "object")
    pair_id = require_safe_id(pair_id, "pair")
    root = comparison_root()
    if smoke:
        return root / "_smoke" / object_id / pair_id
    return root / object_id / pair_id


def _lock_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".lock")


class _FileLock:
    def __init__(self, path: Path):
        self.path = path
        self._fh = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a+", encoding="utf-8")
        if fcntl is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._fh is not None:
            if fcntl is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
        return False


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def append_jsonl(path: Path, row: dict[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _FileLock(_lock_path(path)):
        with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return row


def reviews_path(object_id: str, pair_id: str, *, smoke: bool = False) -> Path:
    return pair_dir(object_id, pair_id, smoke=smoke) / "reviews.jsonl"


def block_links_path(object_id: str, pair_id: str, *, smoke: bool = False) -> Path:
    return pair_dir(object_id, pair_id, smoke=smoke) / "human_block_link_edits.jsonl"


def list_reviews(object_id: str, pair_id: str, *, smoke: bool = False) -> list[dict[str, Any]]:
    return read_jsonl(reviews_path(object_id, pair_id, smoke=smoke))


def list_block_links(object_id: str, pair_id: str, *, smoke: bool = False) -> list[dict[str, Any]]:
    return read_jsonl(block_links_path(object_id, pair_id, smoke=smoke))


def append_review(object_id: str, pair_id: str, row: dict[str, Any], *, smoke: bool = False) -> dict[str, Any]:
    return append_jsonl(reviews_path(object_id, pair_id, smoke=smoke), row)


def append_block_link(object_id: str, pair_id: str, row: dict[str, Any], *, smoke: bool = False) -> dict[str, Any]:
    return append_jsonl(block_links_path(object_id, pair_id, smoke=smoke), row)
