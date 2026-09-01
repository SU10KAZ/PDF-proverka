"""Independent production policy for the HRO Question Closure layer."""
from __future__ import annotations

import os


FEATURE_FLAG = "STAGE_COMPARISON_AI_QUESTION_CLOSURE"
CACHE_FLAG = "STAGE_COMPARISON_AI_QUESTION_CLOSURE_CACHE_ENABLED"
CLOSURE_LAYER_VERSION = "stage-comparison-question-closure-production.v1"


def _bool(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    return default if not raw else raw in {"1", "true", "yes", "on"}


def enabled() -> bool:
    return _bool(FEATURE_FLAG, False)


def cache_enabled() -> bool:
    return _bool(CACHE_FLAG, True)


def require_enabled() -> None:
    if not enabled():
        raise RuntimeError(f"Question Closure is disabled; set {FEATURE_FLAG}=true")


__all__ = [
    "CACHE_FLAG",
    "CLOSURE_LAYER_VERSION",
    "FEATURE_FLAG",
    "cache_enabled",
    "enabled",
    "require_enabled",
]
