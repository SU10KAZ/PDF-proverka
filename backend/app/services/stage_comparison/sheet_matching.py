"""Simple sheet matching based only on the Page/Sheet index in results HTML."""
from __future__ import annotations

import re
from collections import Counter
from difflib import SequenceMatcher
from html.parser import HTMLParser
from typing import Any


_PAGE_HREF_RE = re.compile(r"^#page-(\d+)$", re.IGNORECASE)
_SHEET_LABEL_RE = re.compile(r"^(?:sheet|лист)\s+(.+)$", re.IGNORECASE)
_PAGE_LABEL_RE = re.compile(r"^(?:page|страница)\s+\d+$", re.IGNORECASE)
_TITLE_SEPARATOR_RE = re.compile(r"\s+[\-–—]\s+")
_DASH_RE = re.compile(r"[–—]")
_SPACE_RE = re.compile(r"\s+")


class _ResultsSheetIndexParser(HTMLParser):
    """Collect only anchors from the ready-made #page-N contents list."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._page_index: int | None = None
        self._text: list[str] = []
        self.labels: list[tuple[int, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a" or self._page_index is not None:
            return
        href = next((value for key, value in attrs if key.casefold() == "href"), None)
        match = _PAGE_HREF_RE.fullmatch(str(href or "").strip())
        if match:
            self._page_index = int(match.group(1))
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._page_index is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() != "a" or self._page_index is None:
            return
        label = _SPACE_RE.sub(" ", "".join(self._text)).strip()
        self.labels.append((self._page_index, label))
        self._page_index = None
        self._text = []


def _display_title(value: str | None) -> str | None:
    title = _SPACE_RE.sub(" ", str(value or "")).strip().rstrip(".").strip()
    return title or None


def _sheet_record(pdf_page: int, sheet_number: str | None, title: str | None) -> dict[str, Any]:
    sheet = _SPACE_RE.sub(" ", str(sheet_number or "")).strip() or None
    clean_title = _display_title(title)
    if sheet:
        display = f"Sheet {sheet}" + (f" — {clean_title}" if clean_title else "")
    else:
        display = f"Page {pdf_page}"
    return {
        "pdf_page": pdf_page,
        "sheet_number": sheet,
        "title": clean_title,
        "display": display,
    }


def extract_sheet_index_from_results_html(html: str) -> list[dict[str, Any]]:
    """Extract PDF page, Sheet and title from the existing results HTML index."""
    parser = _ResultsSheetIndexParser()
    parser.feed(html or "")
    parser.close()
    by_page: dict[int, dict[str, Any]] = {}
    for zero_based_page, raw_label in parser.labels:
        label = _SPACE_RE.sub(" ", raw_label).strip()
        pdf_page = zero_based_page + 1
        sheet_match = _SHEET_LABEL_RE.fullmatch(label)
        if sheet_match:
            remainder = sheet_match.group(1).strip()
            parts = _TITLE_SEPARATOR_RE.split(remainder, maxsplit=1)
            sheet_number = parts[0].strip()
            title = parts[1].strip() if len(parts) == 2 else None
            record = _sheet_record(pdf_page, sheet_number, title)
        elif _PAGE_LABEL_RE.fullmatch(label):
            record = _sheet_record(pdf_page, None, None)
        else:
            continue
        current = by_page.get(pdf_page)
        if current is None or (record["sheet_number"] and not current["sheet_number"]):
            by_page[pdf_page] = record
    return [by_page[page] for page in sorted(by_page)]


def placeholder_sheet_index(page_count: int) -> list[dict[str, Any]]:
    """Keep PDF pages available to the manual editor when the HTML index is absent."""
    return [_sheet_record(page, None, None) for page in range(1, max(0, page_count) + 1)]


def canonicalize_sheet_title(title: str | None) -> str | None:
    """Apply cosmetic-only normalization suitable for exact/fuzzy comparison."""
    value = str(title or "").casefold().replace("ё", "е").strip()
    if not value:
        return None
    value = _DASH_RE.sub("-", value)
    value = _SPACE_RE.sub(" ", value)
    value = re.sub(r"\s*-\s*", "-", value)
    value = re.sub(r"\bм\s+(\d+)\s*[_:]\s*(\d+)\b", r"м \1:\2", value)
    value = value.rstrip(".").strip()
    return value or None


def _canonical_sheet_number(sheet_number: str | None) -> str | None:
    value = _SPACE_RE.sub(" ", str(sheet_number or "")).strip().casefold()
    return value or None


def _prepared(record: dict[str, Any]) -> dict[str, Any]:
    return {
        **record,
        "canonical_title": canonicalize_sheet_title(record.get("title")),
        "canonical_sheet": _canonical_sheet_number(record.get("sheet_number")),
    }


def _candidate(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    left_title_counts: Counter[str],
    right_title_counts: Counter[str],
) -> dict[str, Any] | None:
    left_title = left["canonical_title"]
    right_title = right["canonical_title"]
    if not left_title or not right_title:
        return None
    similarity = SequenceMatcher(None, left_title, right_title).ratio()
    same_title = left_title == right_title
    same_sheet = bool(
        left["canonical_sheet"]
        and right["canonical_sheet"]
        and left["canonical_sheet"] == right["canonical_sheet"]
    )
    confidence = "low"
    reason = "title_candidate"
    eligible = False
    if same_title and same_sheet:
        confidence = "high"
        reason = "same_sheet_number_and_title"
        eligible = True
    elif same_title and left_title_counts[left_title] == right_title_counts[right_title] == 1:
        confidence = "high"
        reason = "same_unique_title"
        eligible = True
    elif not same_title and similarity >= 0.92:
        confidence = "high" if same_sheet else "medium"
        reason = "similar_title"
        eligible = True
    return {
        "right_page": int(right["pdf_page"]),
        "right_sheet_number": right.get("sheet_number"),
        "title_similarity": round(similarity, 4),
        "same_sheet_number": same_sheet,
        "confidence": confidence,
        "reason": [reason],
        "eligible": eligible,
    }


def match_sheet_indexes(
    left_sheet_index: list[dict[str, Any]],
    right_sheet_index: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build primary suggestions and TOP-3 using only title similarity and Sheet."""
    left = [_prepared(record) for record in left_sheet_index]
    right = [_prepared(record) for record in right_sheet_index]
    left_counts = Counter(item["canonical_title"] for item in left if item["canonical_title"])
    right_counts = Counter(item["canonical_title"] for item in right if item["canonical_title"])
    suggestions: list[dict[str, Any]] = []
    matched_left: set[int] = set()
    used_right: set[int] = set()
    confidence_rank = {"high": 2, "medium": 1, "low": 0}

    for left_record in left:
        candidates = [
            candidate
            for right_record in right
            if (candidate := _candidate(
                left_record,
                right_record,
                left_title_counts=left_counts,
                right_title_counts=right_counts,
            )) is not None
        ]
        candidates.sort(
            key=lambda item: (
                not item["eligible"],
                -confidence_rank[item["confidence"]],
                -item["title_similarity"],
                not item["same_sheet_number"],
                item["right_page"],
            )
        )
        primary = next((item for item in candidates if item["eligible"]), None)
        if primary:
            matched_left.add(int(left_record["pdf_page"]))
            used_right.add(int(primary["right_page"]))
        alternatives = [item for item in candidates if item is not primary][:3]
        suggestions.append({
            "left_page": int(left_record["pdf_page"]),
            "left_sheet_number": left_record.get("sheet_number"),
            "primary_right_page": int(primary["right_page"]) if primary else None,
            "primary_right_sheet_number": primary.get("right_sheet_number") if primary else None,
            "confidence": primary["confidence"] if primary else "unmatched",
            "reason": list(primary["reason"]) if primary else [],
            "title_similarity": primary["title_similarity"] if primary else None,
            "alternatives": alternatives,
        })

    return {
        "status": "ok",
        "left_sheet_index": left_sheet_index,
        "right_sheet_index": right_sheet_index,
        "suggestions": suggestions,
        "unmatched_left_pages": sorted(
            int(record["pdf_page"]) for record in left_sheet_index
            if int(record["pdf_page"]) not in matched_left
        ),
        "unmatched_right_pages": sorted(
            int(record["pdf_page"]) for record in right_sheet_index
            if int(record["pdf_page"]) not in used_right
        ),
    }


__all__ = [
    "canonicalize_sheet_title",
    "extract_sheet_index_from_results_html",
    "match_sheet_indexes",
    "placeholder_sheet_index",
]
