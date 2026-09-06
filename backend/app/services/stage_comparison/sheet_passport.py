"""Паспорт листа из текста, который у страницы уже есть.

Sheet Matcher v3 строил факты листа только из строк ``**Summary:**`` и
``**Entities:**`` Markdown — их печатает описание графического блока, и они
есть у 847 страниц корпуса из 3 039.  У остальных ~72 % страниц тело
Markdown лежит рядом (таблицы, заголовки, текст листа; медиана 2 138
символов), но в паспорт не попадало, и лист оставался ``UNKNOWN`` при любом
кандидате.

Этот модуль добавляет факты из тела страницы Markdown (и, по выбору, из
нативного текстового слоя PDF) через тот же ограниченный отпечаток
``build_sheet_content_fingerprint``.  Контракт остаётся асимметричным: факт
может лишь *добавить* положительное наблюдение; отсутствие факта ничего не
утверждает.  Термин, стоящий на большой доле страниц документа (штамп, шапка
таблицы, название объекта), не идентифицирует страницу и удаляется по
частоте в документе — это общее правило, не список исключений.

Ни модели, ни OCR, ни сети.  Детерминированно.
"""
from __future__ import annotations

import re
from collections import Counter
from math import ceil
from typing import Any, Iterable, Mapping

from .sheet_content_fingerprint import (
    build_sheet_content_fingerprint,
    has_meaningful_content,
)


PASSPORT_VERSION = "sheet-passport.v1"
SOURCES = frozenset({"MARKDOWN_BODY", "NATIVE_PDF_TEXT"})
MODES = frozenset({"FALLBACK", "MERGE"})

#: Термин, встречающийся не менее чем на такой доле страниц документа (и не
#: менее чем на DOC_FREQUENCY_MIN_PAGES страницах), считается общедокументным.
DOC_FREQUENCY_MAX_SHARE = 0.20
DOC_FREQUENCY_MIN_PAGES = 3
MAX_PAGE_TEXT_CHARS = 20000

_PAGE_RE = re.compile(r"(?m)^##\s+Page\s+(\d+)\s*$")
#: Служебные строки Markdown, которые описывают блок, а не лист: идентификаторы
#: блоков, ссылки на кропы, время создания, штамп-метаданные, советы проверки.
_SERVICE_LINE_RE = re.compile(
    r"(?m)^(?:###\s+BLOCK\b.*|>\s*\*\*(?:Created|Crop|Stamp):\*\*.*|"
    r"\*\*(?:Verification|Проверка):\*\*.*)$"
)
_IMAGE_MARK_RE = re.compile(r"\*\*\[IMAGE\]\*\*|\[IMAGE\]")
_LIST_FIELDS = (
    "system_names",
    "unique_designations",
    "equipment_codes",
    "node_names",
    "section_names",
    "rare_terms",
    "structural_tokens",
)


def page_bodies_from_markdown(markdown: str) -> dict[int, str]:
    """Весь текст страницы между заголовками ``## Page N`` без служебных строк."""
    matches = list(_PAGE_RE.finditer(markdown or ""))
    bodies: dict[int, str] = {}
    for index, match in enumerate(matches):
        page = int(match.group(1))
        end = matches[index + 1].start() if index + 1 < len(matches) else len(markdown)
        body = markdown[match.end():end]
        body = _SERVICE_LINE_RE.sub(" ", body)
        body = _IMAGE_MARK_RE.sub(" ", body)
        body = body.strip()
        if body:
            bodies[page] = body[:MAX_PAGE_TEXT_CHARS]
    return bodies


def page_texts_from_pdf(pdf_path: str) -> dict[int, str]:
    """Нативный текстовый слой каждой страницы.  Только чтение, без OCR."""
    import fitz  # ленивый импорт: разбор Markdown должен работать без PyMuPDF

    texts: dict[int, str] = {}
    with fitz.open(str(pdf_path)) as document:
        for number in range(1, document.page_count + 1):
            text = document[number - 1].get_text("text")
            text = " ".join(str(text or "").split())
            if text:
                texts[number] = text[:MAX_PAGE_TEXT_CHARS]
    return texts


def document_frequency_limit(page_count: int) -> int:
    return max(DOC_FREQUENCY_MIN_PAGES, ceil(page_count * DOC_FREQUENCY_MAX_SHARE))


def build_passports(
    page_texts: Mapping[int, str],
    *,
    titles: Mapping[int, str | None] | None = None,
) -> dict[int, dict[str, Any]]:
    """Отпечатки страниц с удалением общедокументных терминов.

    Возвращает только страницы, у которых после фильтра остаётся хотя бы один
    содержательный термин.  ``purpose_terms`` — закрытый словарь назначения
    листа, к нему частотный фильтр не применяется.
    """
    titles = titles or {}
    raw: dict[int, dict[str, Any]] = {}
    for page, text in page_texts.items():
        if not str(text or "").strip():
            continue
        raw[int(page)] = build_sheet_content_fingerprint(
            str(text), title=str(titles.get(int(page)) or ""),
        )
    if not raw:
        return {}
    frequency: Counter[str] = Counter()
    for fingerprint in raw.values():
        seen: set[str] = set()
        for field in _LIST_FIELDS:
            seen.update(fingerprint.get(field) or [])
        frequency.update(seen)
    limit = document_frequency_limit(len(raw))
    passports: dict[int, dict[str, Any]] = {}
    for page, fingerprint in raw.items():
        filtered = dict(fingerprint)
        for field in _LIST_FIELDS:
            filtered[field] = [
                term for term in fingerprint.get(field) or []
                if frequency[term] < limit
            ]
        filtered["passport"] = {
            "version": PASSPORT_VERSION,
            "document_frequency_limit": limit,
            "pages_with_text": len(raw),
        }
        if has_meaningful_content(filtered):
            passports[page] = filtered
    return passports


def _merge_lists(first: Iterable[str], second: Iterable[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in [*first, *second]:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def extend_sheet_index(
    records: Iterable[dict[str, Any]],
    passports: Mapping[int, Mapping[str, Any]],
    *,
    source: str,
    mode: str = "FALLBACK",
) -> dict[str, int]:
    """Дописать паспорт в записи индекса листов.  Возвращает счётчики.

    ``FALLBACK`` — только страницам без ``content_fingerprint``;
    ``MERGE`` — всем страницам, списки объединяются (существующие термины
    первыми, лимиты отпечатка не расширяются).
    """
    if source not in SOURCES:
        raise ValueError("unsupported sheet passport source")
    if mode not in MODES:
        raise ValueError("unsupported sheet passport mode")
    counts = {"added": 0, "merged": 0, "unchanged": 0}
    for record in records:
        page = int(record.get("pdf_page") or record.get("page") or 0)
        passport = passports.get(page)
        existing = record.get("content_fingerprint")
        if passport is None:
            counts["unchanged"] += 1
            continue
        if not isinstance(existing, dict):
            record["content_fingerprint"] = {
                **dict(passport),
                "passport": {**dict(passport.get("passport") or {}), "source": source, "mode": mode},
            }
            counts["added"] += 1
            continue
        if mode == "FALLBACK":
            counts["unchanged"] += 1
            continue
        merged = dict(existing)
        for field in _LIST_FIELDS:
            merged[field] = _merge_lists(existing.get(field) or [], passport.get(field) or [])
        merged["purpose_terms"] = _merge_lists(
            existing.get("purpose_terms") or [], passport.get("purpose_terms") or [],
        )
        merged["passport"] = {**dict(passport.get("passport") or {}), "source": source, "mode": mode, "merged_with": "MARKDOWN_SUMMARY"}
        record["content_fingerprint"] = merged
        counts["merged"] += 1
    return counts


__all__ = [
    "DOC_FREQUENCY_MAX_SHARE",
    "DOC_FREQUENCY_MIN_PAGES",
    "MODES",
    "PASSPORT_VERSION",
    "SOURCES",
    "build_passports",
    "document_frequency_limit",
    "extend_sheet_index",
    "page_bodies_from_markdown",
    "page_texts_from_pdf",
]
