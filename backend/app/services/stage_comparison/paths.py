"""Filesystem paths for comparison sessions, suggestions and user links."""
from __future__ import annotations

import os
from pathlib import Path

from backend.app.core.config import ROOT_DIR


def _safe_id(value: str) -> str:
    safe = "".join(char for char in str(value or "") if char.isalnum() or char in "-_")
    if not safe:
        raise ValueError("invalid id")
    return safe


def comparison_root_path() -> Path:
    raw = os.environ.get("COMPARISON_ROOT", "").strip()
    return Path(raw).expanduser().resolve() if raw else ROOT_DIR / "comparison"


def comparison_root() -> Path:
    root = comparison_root_path()
    root.mkdir(parents=True, exist_ok=True)
    return root


def sessions_root_path() -> Path:
    return comparison_root_path() / "sessions"


def session_dir(session_id: str) -> Path:
    return sessions_root_path() / _safe_id(session_id)


def session_json_path(session_id: str) -> Path:
    return session_dir(session_id) / "session.json"


def pairs_root(session_id: str) -> Path:
    return session_dir(session_id) / "pairs"


def pair_dir(session_id: str, pair_id: str) -> Path:
    return pairs_root(session_id) / _safe_id(pair_id)


def pair_json_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "pair.json"


def sheet_match_suggestions_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "sheet_match_suggestions.json"


def sheet_links_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "sheet_links.json"


def text_comparison_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_comparison.json"


def text_exclusions_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_exclusions.json"


def text_differences_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_differences.json"


def text_ai_review_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_ai_review.json"


def text_final_comparison_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_final_comparison.json"


def index_json_path() -> Path:
    return comparison_root_path() / "index.json"


__all__ = [
    "comparison_root_path",
    "comparison_root",
    "sessions_root_path",
    "session_dir",
    "session_json_path",
    "pairs_root",
    "pair_dir",
    "pair_json_path",
    "sheet_match_suggestions_path",
    "sheet_links_path",
    "text_comparison_path",
    "text_exclusions_path",
    "text_differences_path",
    "text_ai_review_path",
    "text_final_comparison_path",
    "index_json_path",
]
