"""Deterministic entity normalization for the TEXT/GRAPHIC bridge.

Normalization is deliberately lossless: callers receive both the source value
and a derived canonical value.  It does not mutate source artifacts, perform
fuzzy matching, or infer a functional role from free text.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any


NORMALIZER_VERSION = "entity-normalizer-v1"

_DASHES_RE = re.compile(r"[\u2010-\u2015\u2212]")
_NON_ALNUM_RE = re.compile(r"[^0-9A-ZА-ЯЁ]+")
_BOUNDARY_RE = re.compile(r"(?<=[A-ZА-ЯЁ])(?=\d)|(?<=\d)(?=[A-ZА-ЯЁ])")

# These are explicit engineering aliases, not general transliteration.  The
# longest prefixes are checked first so that a designation is not partially
# consumed by a shorter alias.
_PREFIX_ALIASES = (
    ("ПОМЕЩЕНИЕ", "ROOM"),
    ("POMESHCHENIE", "ROOM"),
    ("PANEL", "PANEL"),
    ("ROOM", "ROOM"),
    ("ГРЩ", "MSB"),
    ("MSB", "MSB"),
    ("ВРУ", "VRU"),
    ("VRU", "VRU"),
    ("ЩР", "PANEL"),
    ("SHR", "PANEL"),
)

_DESIGNATION_SUFFIX_HOMOGLYPHS = str.maketrans(
    {
        "А": "A",
        "В": "B",
        "С": "C",
        "Е": "E",
        "Н": "H",
        "К": "K",
        "М": "M",
        "О": "O",
        "Р": "P",
        "Т": "T",
        "Х": "X",
        "У": "Y",
    }
)

_ROLE_ALIASES = {
    "FIRE_PUMP": "FIRE_PUMP",
    "FIREPUMP": "FIRE_PUMP",
    "ПОЖАРНЫЙ_НАСОС": "FIRE_PUMP",
    "НАСОС_ПОЖАРНЫЙ": "FIRE_PUMP",
    "ПОЖАРНОГО_НАСОСА": "FIRE_PUMP",
}


def _source_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _upper_nfkc(value: Any) -> str:
    text = unicodedata.normalize("NFKC", _source_string(value))
    text = _DASHES_RE.sub("-", text).replace("ё", "е").replace("Ё", "Е")
    return text.upper()


def _canonical_tokens(value: Any) -> list[str]:
    text = _NON_ALNUM_RE.sub("_", _upper_nfkc(value)).strip("_")
    if not text:
        return []
    return [part for part in text.split("_") if part]


def _canonical_designation(tokens: list[str]) -> str | None:
    if not tokens:
        return None
    compact = "".join(tokens)
    for source_prefix, canonical_prefix in _PREFIX_ALIASES:
        if not compact.startswith(source_prefix):
            continue
        suffix = compact[len(source_prefix) :]
        if source_prefix not in {"ROOM", "ПОМЕЩЕНИЕ", "POMESHCHENIE"} and not suffix:
            return canonical_prefix
        if suffix:
            suffix = suffix.translate(_DESIGNATION_SUFFIX_HOMOGLYPHS)
            suffix = "_".join(_BOUNDARY_RE.sub("_", suffix).split("_"))
            compact_suffix = suffix.replace("_", "")
            if canonical_prefix == "ROOM" and not compact_suffix[0].isdigit():
                continue
            if canonical_prefix == "PANEL" and not any(
                character.isdigit() for character in compact_suffix
            ):
                continue
            if canonical_prefix in {"VRU", "MSB"} and not (
                any(character.isdigit() for character in compact_suffix)
                or 1 <= len(compact_suffix) <= 3
            ):
                continue
            return f"{canonical_prefix}_{suffix}"
        return canonical_prefix
    return None


def canonical_entity_name(value: Any) -> str:
    """Return a stable canonical spelling without changing ``value`` itself."""
    tokens = _canonical_tokens(value)
    designation = _canonical_designation(tokens)
    if designation is not None:
        return designation
    canonical = "_".join(tokens)
    return _BOUNDARY_RE.sub("_", canonical)


def normalize_entity_name(value: Any) -> dict[str, str]:
    """Return the original spelling together with its deterministic canonical form."""
    original = _source_string(value)
    return {
        "original": original,
        "canonical": canonical_entity_name(original),
    }


def normalize_functional_role(value: Any) -> dict[str, str]:
    """Normalize an explicitly supplied role; never derive one from an entity name."""
    normalized = normalize_entity_name(value)
    normalized["canonical"] = _ROLE_ALIASES.get(
        normalized["canonical"], normalized["canonical"]
    )
    return normalized


__all__ = [
    "NORMALIZER_VERSION",
    "canonical_entity_name",
    "normalize_entity_name",
    "normalize_functional_role",
]
