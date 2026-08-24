"""Deterministic sheet matching from the HTML index and compact page semantics."""
from __future__ import annotations

import re
from collections import Counter
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from html.parser import HTMLParser
from math import ceil
from typing import Any

from .sheet_content_fingerprint import (
    build_sheet_content_fingerprint,
    has_meaningful_content,
)


_PAGE_HREF_RE = re.compile(r"^#page-(\d+)$", re.IGNORECASE)
_SHEET_LABEL_RE = re.compile(r"^(?:sheet|лист)\s+(.+)$", re.IGNORECASE)
_PAGE_LABEL_RE = re.compile(r"^(?:page|страница)\s+\d+$", re.IGNORECASE)
_TITLE_SEPARATOR_RE = re.compile(r"\s+[\-–—]\s+")
_DASH_RE = re.compile(r"[–—]")
_SPACE_RE = re.compile(r"\s+")
_MARKDOWN_PAGE_RE = re.compile(r"(?m)^##\s+Page\s+(\d+)\s*$")
_MARKDOWN_FACT_RE = re.compile(
    r"(?m)^\*\*(?:Summary|Entities):\*\*\s*(.+)$", re.IGNORECASE,
)
_FRACTIONAL_SHEET_RE = re.compile(r"^(\d+)\.(\d+)$")
_EQUIPMENT_RE = re.compile(
    r"(?<![а-яa-z0-9])"
    r"(грщ|вру|щао|щр|що|як)"
    r"\s*[-.]?\s*"
    r"([0-9]+[аa]?|итп|[аa])?"
    r"(?![а-яa-z0-9])",
    re.IGNORECASE,
)


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


def extract_page_semantics_from_markdown(markdown: str) -> dict[int, str]:
    """Return compact per-page meaning without changing the displayed title.

    The HTML contents index is still the source of Sheet and title.  A short
    summary from the already generated Markdown is only a fallback
    for pages whose repeated volume title hides an otherwise obvious topic.
    """
    matches = list(_MARKDOWN_PAGE_RE.finditer(markdown or ""))
    semantics: dict[int, str] = {}
    for index, match in enumerate(matches):
        page = int(match.group(1))
        end = matches[index + 1].start() if index + 1 < len(matches) else len(markdown)
        body = markdown[match.end():end]
        parts: list[str] = []
        for fact_match in _MARKDOWN_FACT_RE.finditer(body):
            fact = _SPACE_RE.sub(" ", fact_match.group(1)).strip()
            if fact:
                parts.append(fact)
        if parts:
            # This value is transient: match_sheet_indexes immediately converts
            # it into a bounded fingerprint and does not expose the source text.
            semantics[page] = " ".join(dict.fromkeys(parts))[:8000]
    return semantics


def _public_sheet_index_record(record: dict[str, Any]) -> dict[str, Any]:
    """Expose display metadata plus a compact fingerprint, never page text."""
    public = {key: value for key, value in record.items() if not key.startswith("_")}
    semantic_text = str(record.get("_semantic_text") or "")
    if semantic_text:
        fingerprint = build_sheet_content_fingerprint(
            semantic_text, title=str(record.get("title") or ""),
        )
        if has_meaningful_content(fingerprint):
            public["content_fingerprint"] = fingerprint
    return public


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


def _equipment_identifier(title: str | None) -> str | None:
    """Extract the stable engineering mark hidden by cosmetic title changes."""
    value = canonicalize_sheet_title(title)
    if not value:
        return None
    match = _EQUIPMENT_RE.search(value)
    if not match:
        return None
    family = match.group(1).casefold()
    suffix = (match.group(2) or "").casefold().replace("a", "а")
    if family == "грщ":
        return "equipment:грщ"
    if not suffix:
        return None
    # A frequent OCR ambiguity in this exact kind of mark is ЯКЗ vs ЯК3.
    if family == "як" and suffix == "з":
        suffix = "3"
    return f"equipment:{family}:{suffix}"


def _topic_identifier(semantic_text: str | None) -> str | None:
    """Recognise a few strong two-token topics in generic repeated titles."""
    value = canonicalize_sheet_title(semantic_text)
    if not value:
        return None
    if re.search(r"\bбву\b", value) and re.search(r"\bбпи\b", value):
        return "topic:бву-бпи"
    if "молниезащ" in value and (
        "заземлен" in value or "уравниван" in value and "потенциал" in value
    ):
        return "topic:молниезащита-заземление"
    return None


def _prepared(record: dict[str, Any]) -> dict[str, Any]:
    equipment = _equipment_identifier(record.get("title"))
    topic = _topic_identifier(record.get("_semantic_text"))
    return {
        **record,
        "canonical_title": canonicalize_sheet_title(record.get("title")),
        "canonical_sheet": _canonical_sheet_number(record.get("sheet_number")),
        "semantic_key": equipment or topic,
        "semantic_key_source": "title" if equipment else (
            "markdown_topic" if topic else None
        ),
    }


def _inherit_fractional_semantic_keys(records: list[dict[str, Any]]) -> None:
    """Treat 1.1/1.2/1.3 as continuation pages when only one has a title."""
    current: list[dict[str, Any]] = []
    current_base: str | None = None

    def flush() -> None:
        if len(current) < 2:
            return
        keys = {record.get("semantic_key") for record in current if record.get("semantic_key")}
        if len(keys) != 1:
            return
        key = next(iter(keys))
        for record in current:
            if record.get("semantic_key") is None and not record.get("canonical_title"):
                record["semantic_key"] = key
                record["semantic_key_source"] = "fractional_continuation"

    for record in records:
        match = _FRACTIONAL_SHEET_RE.fullmatch(str(record.get("canonical_sheet") or ""))
        base = match.group(1) if match else None
        page_is_next = bool(
            current and int(record["pdf_page"]) == int(current[-1]["pdf_page"]) + 1
        )
        if not base or base != current_base or not page_is_next:
            flush()
            current = []
            current_base = base
        if base:
            current.append(record)
    flush()


def _panel_mark(record: dict[str, Any]) -> tuple[str, int, bool] | None:
    key = str(record.get("semantic_key") or "")
    match = re.fullmatch(r"equipment:(щр|що|щао):(\d+)(а)?", key)
    if not match:
        return None
    family = match.group(1)
    number = int(match.group(2))
    emergency = bool(match.group(3)) or family == "щао"
    return family, number, emergency


def _infer_alternating_panel_holes(records: list[dict[str, Any]]) -> None:
    """Recover missing ЩР-N/ЩР-Nа titles from a repeated alternating series."""
    bases: Counter[int] = Counter()
    max_number = 0
    anchor_pages: list[int] = []
    for record in records:
        mark = _panel_mark(record)
        sheet = _numeric_sheet(record)
        if not mark or mark[0] != "щр" or sheet is None or sheet != sheet.to_integral_value():
            continue
        _, number, emergency = mark
        bases[int(sheet) - 2 * (number - 1) - int(emergency)] += 1
        max_number = max(max_number, number)
        anchor_pages.append(int(record["pdf_page"]))
    if not bases:
        return
    base, support = bases.most_common(1)[0]
    if support < 4 or max_number < 3:
        return
    first_anchor_page = min(anchor_pages)
    last_anchor_page = max(anchor_pages)
    for record in records:
        if record.get("semantic_key") is not None:
            continue
        sheet = _numeric_sheet(record)
        if sheet is None or sheet != sheet.to_integral_value():
            continue
        page = int(record["pdf_page"])
        if page < first_anchor_page or page > last_anchor_page + 1:
            continue
        offset = int(sheet) - base
        if offset < 0 or offset >= 2 * max_number:
            continue
        number = offset // 2 + 1
        suffix = "а" if offset % 2 else ""
        record["semantic_key"] = f"equipment:щр:{number}{suffix}"
        record["semantic_key_source"] = "inferred_panel_series_hole"


def _allocate_semantic_group(
    left_group: list[dict[str, Any]],
    right_group: list[dict[str, Any]],
    *,
    assignments: dict[int, dict[str, Any]],
    assigned_left: set[int],
    assigned_right: set[int],
    confidence: str,
    reason: str,
    group_key: str,
) -> None:
    """Cover equal and split/merged sheet groups without losing right pages."""
    left_group = sorted(
        (record for record in left_group if int(record["pdf_page"]) not in assigned_left),
        key=lambda record: int(record["pdf_page"]),
    )
    right_group = sorted(
        (record for record in right_group if int(record["pdf_page"]) not in assigned_right),
        key=lambda record: int(record["pdf_page"]),
    )
    if not left_group or not right_group:
        return

    allocations: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
    if len(left_group) == len(right_group):
        allocations = [
            (left_record, [right_record])
            for left_record, right_record in zip(left_group, right_group)
        ]
    elif len(left_group) == 1:
        allocations = [(left_group[0], right_group)]
    elif len(right_group) == 1:
        allocations = [(left_record, right_group) for left_record in left_group]
    elif len(left_group) < len(right_group):
        for index, left_record in enumerate(left_group):
            start = index * len(right_group) // len(left_group)
            end = (index + 1) * len(right_group) // len(left_group)
            allocations.append((left_record, right_group[start:max(start + 1, end)]))
    else:
        for index, left_record in enumerate(left_group):
            right_index = min(len(right_group) - 1, index * len(right_group) // len(left_group))
            allocations.append((left_record, [right_group[right_index]]))

    for left_record, right_records in allocations:
        left_page = int(left_record["pdf_page"])
        right_pages = [int(record["pdf_page"]) for record in right_records]
        assignments[left_page] = {
            "right_records": right_records,
            "confidence": confidence,
            "reason": [reason],
            "group_key": group_key,
        }
        assigned_left.add(left_page)
        assigned_right.update(right_pages)


def _panel_groups(
    records: list[dict[str, Any]], family: str, emergency: bool,
) -> dict[int, list[dict[str, Any]]]:
    groups: dict[int, list[dict[str, Any]]] = {}
    for record in records:
        mark = _panel_mark(record)
        if not mark or mark[0] != family or mark[2] != emergency:
            continue
        groups.setdefault(mark[1], []).append(record)
    return groups


def _semantic_assignments(
    left: list[dict[str, Any]], right: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    """Match stable marks first, then infer a repeated ЩР→ЩО/ЩАО rename."""
    _inherit_fractional_semantic_keys(left)
    _inherit_fractional_semantic_keys(right)
    _infer_alternating_panel_holes(left)
    _infer_alternating_panel_holes(right)

    assignments: dict[int, dict[str, Any]] = {}
    assigned_left: set[int] = set()
    assigned_right: set[int] = set()
    left_by_key: dict[str, list[dict[str, Any]]] = {}
    right_by_key: dict[str, list[dict[str, Any]]] = {}
    for record in left:
        if record.get("semantic_key"):
            left_by_key.setdefault(str(record["semantic_key"]), []).append(record)
    for record in right:
        if record.get("semantic_key"):
            right_by_key.setdefault(str(record["semantic_key"]), []).append(record)

    common_keys = sorted(
        set(left_by_key) & set(right_by_key),
        key=lambda key: min(int(record["pdf_page"]) for record in left_by_key[key]),
    )
    for key in common_keys:
        left_group = left_by_key[key]
        right_group = right_by_key[key]
        if key.startswith("topic:") and (len(left_group) != 1 or len(right_group) != 1):
            continue
        inherited = any(
            record.get("semantic_key_source") in {
                "fractional_continuation", "inferred_panel_series_hole",
            }
            for record in [*left_group, *right_group]
        )
        split_or_merged = len(left_group) != len(right_group)
        _allocate_semantic_group(
            left_group,
            right_group,
            assignments=assignments,
            assigned_left=assigned_left,
            assigned_right=assigned_right,
            confidence="medium" if inherited or split_or_merged or key.startswith("topic:") else "high",
            reason=(
                "same_semantic_topic" if key.startswith("topic:")
                else "same_equipment_identifier_group" if split_or_merged or inherited
                else "same_equipment_identifier"
            ),
            group_key=key,
        )

    # ЩР-N/ЩР-Nа and ЩО-N/ЩАО-N are not universally synonymous.  Treat the
    # rename as a document-level pattern only when at least three numbers
    # support the same transformation in this pair of volumes.
    alias_specs = (
        ("щр", False, "що", False, "normal"),
        ("щр", True, "щао", True, "emergency"),
        ("що", False, "щр", False, "normal"),
        ("щао", True, "щр", True, "emergency"),
    )
    for left_family, left_emergency, right_family, right_emergency, alias_kind in alias_specs:
        left_groups = _panel_groups(left, left_family, left_emergency)
        right_groups = _panel_groups(right, right_family, right_emergency)
        common_numbers = sorted(set(left_groups) & set(right_groups))
        if len(common_numbers) < 3:
            continue
        for number in common_numbers:
            _allocate_semantic_group(
                left_groups[number],
                right_groups[number],
                assignments=assignments,
                assigned_left=assigned_left,
                assigned_right=assigned_right,
                confidence="medium",
                reason="inferred_panel_series_rename",
                group_key=f"panel-series:{alias_kind}:{number}",
            )
    return assignments


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


def _numeric_sheet(record: dict[str, Any]) -> Decimal | None:
    value = str(record.get("canonical_sheet") or "").strip()
    if not re.fullmatch(r"\d+(?:[.,]\d+)?", value):
        return None
    try:
        return Decimal(value.replace(",", "."))
    except InvalidOperation:
        return None


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
    sheet_score = 6.0 if difference == 0 else float(max(Decimal(-4), -difference))
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


def _sequence_pair_has_support(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_key = left.get("semantic_key")
    right_key = right.get("semantic_key")
    semantic_conflict = bool(left_key and right_key and left_key != right_key)
    return bool(
        _sequence_title_similarity(left, right) >= 0.92
        or _numeric_sheet(left) == _numeric_sheet(right) and not semantic_conflict
    )


def _sheet_sequence_alignments(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
) -> list[list[tuple[dict[str, Any], dict[str, Any], float]]]:
    """Choose every compatible non-overlapping run pair in both documents."""
    left_runs = _numeric_sheet_runs(left)
    right_runs = _numeric_sheet_runs(right)
    candidates: list[
        tuple[
            float, int, float, float, int, int,
            list[tuple[dict[str, Any], dict[str, Any], float]],
        ]
    ] = []
    for left_run_index, left_run in enumerate(left_runs):
        for right_run_index, right_run in enumerate(right_runs):
            alignment, score = _align_sheet_runs(left_run, right_run)
            if len(alignment) < _SEQUENCE_MIN_PAIRS:
                continue
            supported = sum(
                _sequence_pair_has_support(left_record, right_record)
                for left_record, right_record, _ in alignment
            )
            required_support = max(_SEQUENCE_MIN_PAIRS, ceil(len(alignment) * 0.6))
            coverage = len(alignment) / max(1, min(len(left_run), len(right_run)))
            if supported < required_support or coverage < 0.7:
                continue
            left_position = left_run_index / max(1, len(left_runs) - 1)
            right_position = right_run_index / max(1, len(right_runs) - 1)
            candidates.append((
                score,
                supported,
                coverage,
                -abs(left_position - right_position),
                left_run_index,
                right_run_index,
                alignment,
            ))

    selected: list[list[tuple[dict[str, Any], dict[str, Any], float]]] = []
    used_left_runs: set[int] = set()
    used_right_runs: set[int] = set()
    for candidate in sorted(candidates, key=lambda item: item[:4], reverse=True):
        left_run_index, right_run_index = candidate[4], candidate[5]
        if left_run_index in used_left_runs or right_run_index in used_right_runs:
            continue
        selected.append(candidate[6])
        used_left_runs.add(left_run_index)
        used_right_runs.add(right_run_index)
    return selected


def _sequence_pair_is_supported(
    alignment: list[tuple[dict[str, Any], dict[str, Any], float]],
    index: int,
) -> bool:
    left_record, right_record, _ = alignment[index]
    if _numeric_sheet(left_record) == _numeric_sheet(right_record):
        return _sequence_pair_has_support(left_record, right_record)
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
    semantic_assignments = _semantic_assignments(left, right)
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
        semantic = semantic_assignments.get(int(left_record["pdf_page"]))
        if semantic:
            right_records = semantic["right_records"]
            first_right = right_records[0]
            primary = {
                "right_page": int(first_right["pdf_page"]),
                "right_sheet_number": first_right.get("sheet_number"),
                "title_similarity": round(
                    _sequence_title_similarity(left_record, first_right), 4
                ),
                "same_sheet_number": (
                    left_record.get("canonical_sheet")
                    == first_right.get("canonical_sheet")
                ),
                "confidence": semantic["confidence"],
                "reason": semantic["reason"],
                "eligible": True,
            }
            primary_right_pages = [int(record["pdf_page"]) for record in right_records]
        else:
            primary = next((item for item in candidates if item["eligible"]), None)
            primary_right_pages = [int(primary["right_page"])] if primary else []
        if primary:
            matched_left.add(int(left_record["pdf_page"]))
            used_right.update(primary_right_pages)
        alternatives = [
            item for item in candidates
            if int(item["right_page"]) not in primary_right_pages
        ][:3]
        suggestions.append({
            "left_page": int(left_record["pdf_page"]),
            "left_sheet_number": left_record.get("sheet_number"),
            "primary_right_page": int(primary["right_page"]) if primary else None,
            "primary_right_pages": primary_right_pages,
            "primary_right_sheet_number": primary.get("right_sheet_number") if primary else None,
            "confidence": primary["confidence"] if primary else "unmatched",
            "reason": list(primary["reason"]) if primary else [],
            "title_similarity": primary["title_similarity"] if primary else None,
            "alternatives": alternatives,
            "match_group": semantic.get("group_key") if semantic else None,
        })

    # The exact matcher above remains authoritative.  Global alignment only
    # fills its holes, so unique-title matches and deliberate page reordering
    # are never overwritten by the sequential fallback.
    suggestion_by_left = {int(item["left_page"]): item for item in suggestions}
    for alignment in _sheet_sequence_alignments(left, right):
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
                "primary_right_pages": [right_page],
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
                "match_group": None,
            })
            matched_left.add(left_page)
            used_right.add(right_page)

    return {
        "status": "ok",
        "left_sheet_index": [_public_sheet_index_record(record) for record in left_sheet_index],
        "right_sheet_index": [_public_sheet_index_record(record) for record in right_sheet_index],
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
    "extract_page_semantics_from_markdown",
    "extract_sheet_index_from_results_html",
    "match_sheet_indexes",
    "placeholder_sheet_index",
]
