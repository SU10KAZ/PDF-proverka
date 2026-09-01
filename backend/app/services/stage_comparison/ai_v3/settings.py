"""Fail-closed runtime policy for the experimental v3 selector."""
from __future__ import annotations

import os

FEATURE_FLAG = "STAGE_COMPARISON_AI_ANALYST_V3"
MODEL = "gpt-5.6-sol"
REASONING_EFFORT = "low"


def _bool(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    return default if not raw else raw in {"1", "true", "yes", "on"}


def _int(name: str, default: int, low: int, high: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    try:
        return max(low, min(high, int(raw))) if raw else default
    except ValueError:
        return default


def enabled() -> bool:
    return _bool(FEATURE_FLAG, False)


def require_enabled() -> None:
    if not enabled():
        raise RuntimeError(
            f"AI Analyst v3 is experimental; set {FEATURE_FLAG}=true explicitly"
        )


def timeout_seconds() -> int:
    return _int("STAGE_COMPARISON_AI_V3_TIMEOUT_SECONDS", 600, 30, 1800)


__all__ = [
    "FEATURE_FLAG",
    "MODEL",
    "REASONING_EFFORT",
    "enabled",
    "require_enabled",
    "timeout_seconds",
]
