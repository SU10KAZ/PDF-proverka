"""Shared normalisation helpers used by class_dedup and fuzzy_dedup.

The two dedup modules each carry their own copy of these helpers on purpose
(see `class_dedup.py` and `fuzzy_dedup.py` module docstrings) so they can be
vendored independently. This file is a thin shim that re-exports the class_dedup
implementations for callers who want a single import path. **The shim is NOT
imported from the dedup modules themselves** — that would defeat the
independence invariant.
"""
from __future__ import annotations

from .class_dedup import (
    SEVERITY_WEIGHT,
    _is_critical,
    _normalise,
    _severity_weight,
    _short_signature,
)

__all__ = [
    "SEVERITY_WEIGHT",
    "_is_critical",
    "_normalise",
    "_severity_weight",
    "_short_signature",
]
