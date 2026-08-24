"""Compact deterministic fingerprints for already extracted sheet text.

The module consumes only structured Markdown summaries/entities that already
exist in Stage Comparison.  It deliberately does not retain page text and does
not invoke OCR, parsers, embeddings, or a language model.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any, Iterable


VERSION = 1
MAX_RARE_TERMS = 96
MAX_STRUCTURAL_TOKENS = 96

_SPACE_RE = re.compile(r"\s+")
_EMAIL_RE = re.compile(r"\b[^\s@]+@[^\s@]+\b", re.IGNORECASE)
_URL_RE = re.compile(r"\b(?:https?://|www\.)\S+", re.IGNORECASE)
_DATE_RE = re.compile(r"(?<!\d)\d{1,2}[./-]\d{1,2}[./-](?:19|20)\d{2}(?!\d)")
_TOKEN_RE = re.compile(
    r"[a-zа-яё]+(?:[-./][a-zа-яё0-9]+)*|"
    r"(?=[a-zа-яё0-9./-]*[a-zа-яё])(?=[a-zа-яё0-9./-]*\d)"
    r"[a-zа-яё0-9]+(?:[-./][a-zа-яё0-9]+)*|"
    r"\d+(?:[.,xх×]\d+)+",
    re.IGNORECASE,
)
_RAW_ACRONYM_RE = re.compile(
    r"(?<![A-ZА-ЯЁ0-9])(?:[A-ZА-ЯЁ]{2,8}(?:[-./][A-ZА-ЯЁ0-9]{1,12})*|"
    r"[A-ZА-ЯЁ]{1,8}\d+[A-ZА-ЯЁ0-9.-]*)(?![A-ZА-ЯЁ0-9])"
)

_GENERIC_TERMS = {
    "автомат", "адрес", "данные", "документ", "здание", "лист", "листа",
    "наименование", "оборудование", "общие", "организация", "помещение",
    "проект", "проектировщик", "система", "страница", "таблица", "часть",
    "этаж", "элементы", "фрагмент", "изображение", "изображены", "показаны",
    "приведены", "содержит", "отображены", "включает", "указаны", "схема",
    "план", "узел", "деталь", "разрез", "заказчик", "корректировка",
}
_STOP_WORDS = _GENERIC_TERMS | {
    "без", "более", "был", "были", "быть", "вдоль", "включая", "для", "до",
    "его", "ему", "если", "или", "из", "как", "между", "над", "на", "не",
    "них", "об", "один", "она", "они", "от", "по", "под", "при", "со", "так",
    "также", "через", "что", "это", "этот", "эта", "эти", "and", "for", "from",
    "into", "the", "with", "sheet", "page", "summary", "entities",
}
_SERVICE_NOISE_RE = re.compile(
    r"(?:заказчик|проектировщик|организац|\bсро\b|\bгип\b|\bгап\b|фамили|"
    r"подпис|телефон|e-?mail|корректировк|адрес\s+объект|номер\s+документ|"
    r"типов\w*\s+строк\w*\s+штамп)",
    re.IGNORECASE,
)
_SERVICE_FIELD_RE = re.compile(
    r"(?:организация|заказчик|проектировщик|\bсро\b|\bгип\b|\bгап\b|"
    r"фамилия|подпись|телефон|e-?mail|адрес\s+объекта?)\s*[:=-]?\s*[^.;\n]{0,180}",
    re.IGNORECASE,
)
_PURPOSE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("single_line", re.compile(r"однолинейн|single[- ]?line")),
    ("plan", re.compile(r"\bплан\w*\b|\bplan\b")),
    ("scheme", re.compile(r"\bсхем\w*\b|\bschematic\b")),
    ("node", re.compile(r"\bуз(?:ел|ла|лы|лов|ле|лом)\b|\bдетал\w*\b|\bnode\b")),
    ("explication", re.compile(r"экспликац|schedule")),
    ("specification", re.compile(r"спецификац|ведомост\w*\s+элемент|bill of")),
    ("section", re.compile(r"\bразрез\w*\b|\bсечени\w*\b|\bsection\b")),
    ("facade", re.compile(r"\bфасад\w*\b|\belevation\b")),
    ("calculation", re.compile(r"\bрасчет\w*\b|\bcalculation\b")),
    ("diagram", re.compile(r"\bдиаграм\w*\b|\bdiagram\b")),
)
_NODE_STEMS = ("узел", "узла", "стойк", "креплен", "примыкан", "детал")
_SECTION_STEMS = (
    "схем", "план", "разрез", "фасад", "спецификац", "экспликац", "ведомост",
)


def normalize_content(value: str) -> str:
    """Normalize comparison text while removing known service-only noise."""
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = _URL_RE.sub(" ", _EMAIL_RE.sub(" ", _DATE_RE.sub(" ", text)))
    text = _SERVICE_FIELD_RE.sub(" ", text)
    text = text.replace("Ё", "Е").replace("ё", "е")
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"(?<=\d)[х×](?=\d)", "x", text, flags=re.IGNORECASE)
    return _SPACE_RE.sub(" ", text).strip().casefold()


def purpose_terms(value: str) -> list[str]:
    normalized = normalize_content(value)
    return [name for name, pattern in _PURPOSE_PATTERNS if pattern.search(normalized)]


def _ordered_unique(values: Iterable[str], limit: int) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = normalize_content(raw).strip("-./")
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
        if len(output) >= limit:
            break
    return output


def _tokens(value: str) -> list[str]:
    return [match.group(0).strip("-./") for match in _TOKEN_RE.finditer(value)]


def _informative(token: str) -> bool:
    if not token or token in _STOP_WORDS or _SERVICE_NOISE_RE.search(token):
        return False
    if re.fullmatch(r"(?:19|20)\d{2}", token):
        return False
    if token.isalpha() and len(token) < 4:
        return False
    if token.isdigit():
        return False
    return True


def _is_designation(token: str) -> bool:
    has_letter = bool(re.search(r"[a-zа-я]", token))
    has_digit = bool(re.search(r"\d", token))
    pure_dimension = bool(re.fullmatch(r"\d+(?:[x.,]\d+)+", token))
    grid_axis = bool(
        re.fullmatch(r"\d+[.-][a-zа-я](?:-\d+[.-][a-zа-я])?", token)
    )
    return has_letter and has_digit and not pure_dimension and not grid_axis and len(token) >= 3


def _ngrams(tokens: list[str], sizes: tuple[int, ...]) -> Iterable[str]:
    for size in sizes:
        for index in range(0, len(tokens) - size + 1):
            yield " ".join(tokens[index:index + size])


def build_sheet_content_fingerprint(
    semantic_text: str, *, title: str | None = None,
) -> dict[str, Any]:
    """Build a bounded explainable fingerprint without retaining source text."""
    normalized = normalize_content(semantic_text)
    normalized_title = normalize_content(title or "")
    tokens = _tokens(normalized)
    informative = [token for token in tokens if _informative(token)]
    word_terms = [token for token in informative if token.isalpha()]
    designations = _ordered_unique(
        (token for token in informative if _is_designation(token)), 64,
    )
    dimensions = _ordered_unique(
        (token for token in informative if re.fullmatch(r"\d+(?:[x.,]\d+)+", token)), 24,
    )
    grid_axes = _ordered_unique(
        (
            token for token in informative
            if re.fullmatch(r"\d+[.-][a-zа-я](?:-\d+[.-][a-zа-я])?", token)
        ),
        32,
    )

    raw_acronyms = _ordered_unique(
        (match.group(0) for match in _RAW_ACRONYM_RE.finditer(str(semantic_text or ""))), 64,
    )
    system_names = _ordered_unique(
        (
            token for token in [*raw_acronyms, *designations]
            if not re.fullmatch(r"(?:гост|сп|ip)\d.*", token)
        ),
        64,
    )
    equipment_codes = _ordered_unique(
        (
            token for token in designations
            if re.match(r"^[a-zа-я]{1,10}(?:[-.]?[a-zа-я]{0,5})?\d", token)
        ),
        64,
    )

    structural = _ordered_unique(
        [*_ngrams(word_terms, (2, 3)), *dimensions, *grid_axes], MAX_STRUCTURAL_TOKENS,
    )
    node_names = _ordered_unique(
        (
            gram for gram in _ngrams(word_terms, (2, 3, 4))
            if any(stem in gram for stem in _NODE_STEMS)
        ),
        32,
    )
    section_names = _ordered_unique(
        (
            gram for gram in _ngrams(word_terms, (2, 3, 4))
            if any(stem in gram for stem in _SECTION_STEMS)
        ),
        32,
    )
    rare_terms = _ordered_unique(
        [*designations, *system_names, *word_terms, *_ngrams(word_terms, (2,))],
        MAX_RARE_TERMS,
    )
    source_sha256 = hashlib.sha256(normalized.encode()).hexdigest() if normalized else ""
    return {
        "version": VERSION,
        "source_sha256": source_sha256,
        "purpose_terms": purpose_terms(f"{normalized_title} {normalized}"),
        "system_names": system_names,
        "unique_designations": designations,
        "equipment_codes": equipment_codes,
        "node_names": node_names,
        "section_names": section_names,
        "rare_terms": rare_terms,
        "structural_tokens": structural,
    }


def has_meaningful_content(fingerprint: dict[str, Any] | None) -> bool:
    if not isinstance(fingerprint, dict) or fingerprint.get("version") != VERSION:
        return False
    return any(
        fingerprint.get(key)
        for key in (
            "system_names", "unique_designations", "equipment_codes", "node_names",
            "section_names", "rare_terms", "structural_tokens",
        )
    )


__all__ = [
    "MAX_RARE_TERMS", "MAX_STRUCTURAL_TOKENS", "VERSION",
    "build_sheet_content_fingerprint", "has_meaningful_content", "normalize_content",
    "purpose_terms",
]
