"""One normalization, used by every join in this layer.

Kept in its own module for a boring reason that has cost this project before:
when two comparisons fold text differently, they disagree about facts and the
disagreement looks like a finding.  Everything here folds case, ``ё``, and
punctuation, and nothing here removes digits or engineering separators.
"""
from __future__ import annotations

import re

_NORMALIZE_RE = re.compile(r"[^0-9a-zа-яё]+")
_WORD_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё]+")

#: Below this length a normalized string matches by accident.  Used wherever a
#: "does the other layer contain this" question is asked.
MIN_COMPARABLE = 4


def normalize(value: object) -> str:
    return _NORMALIZE_RE.sub(" ", str(value).lower().replace("ё", "е")).strip()


def words(value: object) -> list[str]:
    return _WORD_RE.findall(str(value))


def comparable(value: object) -> str:
    """Normalized form, empty when the string is too short to compare."""
    folded = normalize(value)
    return folded if len(folded) >= MIN_COMPARABLE else ""


__all__ = ["MIN_COMPARABLE", "comparable", "normalize", "words"]
