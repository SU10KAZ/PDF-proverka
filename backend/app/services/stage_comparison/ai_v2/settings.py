"""Runtime policy for the experimental whole-document analyst.

Nothing in this module enables the feature implicitly.  A caller must set
``STAGE_COMPARISON_AI_ANALYST_V2=true`` for every experimental run.  This is
kept distinct from ``STAGE_COMPARISON_AI_MODE`` so STANDARD cannot silently
start using an unaccepted implementation.
"""
from __future__ import annotations

import os

FEATURE_FLAG = "STAGE_COMPARISON_AI_ANALYST_V2"
MODEL = "gpt-5.6-sol"
ALLOWED_EFFORTS = ("low", "medium")
DEFAULT_EFFORTS = ("low",)


def _bool(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _int(name: str, default: int, low: int, high: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    try:
        return max(low, min(high, int(raw))) if raw else default
    except ValueError:
        return default


def enabled() -> bool:
    return _bool(FEATURE_FLAG)


def require_enabled() -> None:
    if not enabled():
        raise RuntimeError(
            f"AI Analyst v2 is experimental; set {FEATURE_FLAG}=true explicitly"
        )


def effort(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in ALLOWED_EFFORTS:
        raise ValueError(f"unsupported v2 reasoning effort: {value!r}")
    return normalized


def max_sessions() -> int:
    return _int("STAGE_COMPARISON_AI_V2_MAX_SESSIONS", 4, 1, 4)


def max_expansions() -> int:
    return _int("STAGE_COMPARISON_AI_V2_MAX_EXPANSIONS", 2, 0, 2)


def timeout_seconds() -> int:
    return _int("STAGE_COMPARISON_AI_V2_TIMEOUT_SECONDS", 600, 30, 1800)


__all__ = [
    "ALLOWED_EFFORTS",
    "DEFAULT_EFFORTS",
    "FEATURE_FLAG",
    "MODEL",
    "effort",
    "enabled",
    "max_expansions",
    "max_sessions",
    "require_enabled",
    "timeout_seconds",
]
