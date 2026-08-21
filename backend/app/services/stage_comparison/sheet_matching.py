"""Simple sheet matching based only on the Page/Sheet index in results HTML."""
from __future__ import annotations

import re
from collections import Counter
from difflib import SequenceMatcher
from html.parser import HTMLParser
from math import ceil
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


_SEQUENCE_MIN_PAIRS = 3
_SEQUENCE_GAP_PENALTY = -2.0


def _numeric_sheet(record: dict[str, Any]) -> int | None:
    value = str(record.get("canonical_sheet") or "").strip()
    return int(value) if value.isdigit() else None


def _numeric_sheet_runs(records: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Split numbered pages into monotone, physically contiguous runs.

    A volume can contain several independent sequences with the same Sheet
    numbers (contents, explanatory notes and the actual drawings).  A reset
    from a larger number back to 1 starts a new candidate run.  Equal numbers
    stay in one run because OCR occasionally reads ``..., 8, 8, 10, ...``.
    """
    numbered = [record for record in records if _numeric_sheet(record) is not None]
    runs: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for record in numbered:
        if current:
            previous = current[-1]
            page_is_next = int(record["pdf_page"]) == int(previous["pdf_page"]) + 1
            current_sheet = _numeric_sheet(record)
            previous_sheet = _numeric_sheet(previous)
            assert current_sheet is not None and previous_sheet is not None
            sheet_reset = current_sheet < previous_sheet
            if not page_is_next or sheet_reset:
                runs.append(current)
                current = []
        current.append(record)
    if current:
        runs.append(current)
    return [
        run for run in runs
        if len(run) >= _SEQUENCE_MIN_PAIRS
        and len({_numeric_sheet(record) for record in run}) >= _SEQUENCE_MIN_PAIRS
    ]


def _sequence_title_similarity(left: dict[str, Any], right: dict[str, Any]) -> float:
    left_title = left.get("canonical_title")
    right_title = right.get("canonical_title")
    if not left_title or not right_title:
        return 0.0
    return SequenceMatcher(None, left_title, right_title).ratio()


def _sequence_match_score(left: dict[str, Any], right: dict[str, Any]) -> float:
    """Score one diagonal in a global sheet-sequence alignment.

    Sheet equality is deliberately stronger than title text: title extraction
    often returns the common volume name instead of the lower title-block row.
    A one-off OCR number still receives a small score so strong neighbours can
    repair an isolated ``8, 8, 10`` sequence, while a gap is preferred over a
    cascade of shifted sheet numbers.
    """
    left_sheet = _numeric_sheet(left)
    right_sheet = _numeric_sheet(right)
    assert left_sheet is not None and right_sheet is not None
    difference = abs(left_sheet - right_sheet)
    sheet_score = 6.0 if difference == 0 else float(max(-4, -difference))
    return sheet_score + 2.0 * _sequence_title_similarity(left, right)


def _align_sheet_runs(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
) -> tuple[list[tuple[dict[str, Any], dict[str, Any], float]], float]:
    """Needleman-Wunsch alignment for two monotone drawing runs."""
    left_count = len(left)
    right_count = len(right)
    scores = [[0.0] * (right_count + 1) for _ in range(left_count + 1)]
    moves: list[list[str | None]] = [
        [None] * (right_count + 1) for _ in range(left_count + 1)
    ]
    for left_index in range(1, left_count + 1):
        scores[left_index][0] = scores[left_index - 1][0] + _SEQUENCE_GAP_PENALTY
        moves[left_index][0] = "left_gap"
    for right_index in range(1, right_count + 1):
        scores[0][right_index] = scores[0][right_index - 1] + _SEQUENCE_GAP_PENALTY
        moves[0][right_index] = "right_gap"

    for left_index in range(1, left_count + 1):
        for right_index in range(1, right_count + 1):
            options = (
                (
                    scores[left_index - 1][right_index - 1]
                    + _sequence_match_score(left[left_index - 1], right[right_index - 1]),
                    "match",
                ),
                (
                    scores[left_index - 1][right_index] + _SEQUENCE_GAP_PENALTY,
                    "left_gap",
                ),
                (
                    scores[left_index][right_index - 1] + _SEQUENCE_GAP_PENALTY,
                    "right_gap",
                ),
            )
            # Stable option order intentionally prefers a diagonal on an exact
            # tie; local acceptance below still rejects unsupported diagonals.
            scores[left_index][right_index], moves[left_index][right_index] = max(
                options, key=lambda option: option[0]
            )

    alignment: list[tuple[dict[str, Any], dict[str, Any], float]] = []
    left_index = left_count
    right_index = right_count
    while left_index or right_index:
        move = moves[left_index][right_index]
        if move == "match":
            left_record = left[left_index - 1]
            right_record = right[right_index - 1]
            alignment.append(
                (left_record, right_record, _sequence_match_score(left_record, right_record))
            )
            left_index -= 1
            right_index -= 1
        elif move == "left_gap":
            left_index -= 1
        else:
            right_index -= 1
    alignment.reverse()
    return alignment, scores[left_count][right_count]


def _best_sheet_sequence_alignment(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any], float]]:
    """Choose the strongest compatible drawing run pair in both documents."""
    candidates: list[
        tuple[float, int, float, list[tuple[dict[str, Any], dict[str, Any], float]]]
    ] = []
    for left_run in _numeric_sheet_runs(left):
        for right_run in _numeric_sheet_runs(right):
            alignment, score = _align_sheet_runs(left_run, right_run)
            if len(alignment) < _SEQUENCE_MIN_PAIRS:
                continue
            supported = sum(
                _numeric_sheet(left_record) == _numeric_sheet(right_record)
                or _sequence_title_similarity(left_record, right_record) >= 0.92
                for left_record, right_record, _ in alignment
            )
            required_support = max(_SEQUENCE_MIN_PAIRS, ceil(len(alignment) * 0.6))
            coverage = len(alignment) / max(1, min(len(left_run), len(right_run)))
            if supported < required_support or coverage < 0.7:
                continue
            candidates.append((score, supported, coverage, alignment))
    if not candidates:
        return []
    return max(candidates, key=lambda candidate: candidate[:3])[3]


def _sequence_pair_is_supported(
    alignment: list[tuple[dict[str, Any], dict[str, Any], float]],
    index: int,
) -> bool:
    left_record, right_record, _ = alignment[index]
    if _numeric_sheet(left_record) == _numeric_sheet(right_record):
        return True
    # An isolated OCR error is safe to bridge only when both adjacent aligned
    # pairs have exact sheet numbers and preserve the same physical page offset.
    if index == 0 or index + 1 >= len(alignment):
        return False
    previous_left, previous_right, _ = alignment[index - 1]
    next_left, next_right, _ = alignment[index + 1]
    neighbours_match = (
        _numeric_sheet(previous_left) == _numeric_sheet(previous_right)
        and _numeric_sheet(next_left) == _numeric_sheet(next_right)
    )
    offsets_match = (
        int(previous_right["pdf_page"]) - int(previous_left["pdf_page"])
        == int(right_record["pdf_page"]) - int(left_record["pdf_page"])
        == int(next_right["pdf_page"]) - int(next_left["pdf_page"])
    )
    return neighbours_match and offsets_match


def match_sheet_indexes(
    left_sheet_index: list[dict[str, Any]],
    right_sheet_index: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build deterministic suggestions from titles, Sheet and global order."""
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

    # The exact matcher above remains authoritative.  Global alignment only
    # fills its holes, so unique-title matches and deliberate page reordering
    # are never overwritten by the sequential fallback.
    suggestion_by_left = {int(item["left_page"]): item for item in suggestions}
    alignment = _best_sheet_sequence_alignment(left, right)
    for index, (left_record, right_record, _) in enumerate(alignment):
        left_page = int(left_record["pdf_page"])
        right_page = int(right_record["pdf_page"])
        suggestion = suggestion_by_left[left_page]
        if suggestion["primary_right_page"] is not None or right_page in used_right:
            continue
        if not _sequence_pair_is_supported(alignment, index):
            continue
        same_sheet = _numeric_sheet(left_record) == _numeric_sheet(right_record)
        similarity = round(_sequence_title_similarity(left_record, right_record), 4)
        suggestion.update({
            "primary_right_page": right_page,
            "primary_right_sheet_number": right_record.get("sheet_number"),
            "confidence": "high",
            "reason": [
                "same_sheet_number_and_sequence"
                if same_sheet else "sequence_repaired_sheet_number"
            ],
            "title_similarity": similarity,
            "alternatives": [
                alternative for alternative in suggestion["alternatives"]
                if int(alternative["right_page"]) != right_page
            ],
        })
        matched_left.add(left_page)
        used_right.add(right_page)

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
