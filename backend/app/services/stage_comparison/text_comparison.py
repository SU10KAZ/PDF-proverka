"""Deterministic exact-text exclusion for accepted P <-> RD sheet links.

Markdown supplies the structured text units.  PyMuPDF is used only to locate
those units on the original PDF page so the UI can draw an overlay.  This
module never writes a PDF and deliberately contains no OCR, semantic or vector
graphics comparison.
"""
from __future__ import annotations

import hashlib
import html
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


_PAGE_RE = re.compile(r"(?m)^##\s+Page\s+(\d+)\s*$")
_BLOCK_RE = re.compile(
    r"(?m)^###\s+BLOCK\s+#\d+\s+\[([^]]+)]\s*:\s*([^\s]+)\s*$"
)
_TABLE_DIVIDER_RE = re.compile(r"^\s*\|?(?:\s*:?-{3,}:?\s*\|)+\s*$")
_MARKDOWN_LINK_RE = re.compile(r"!?\[([^]]*)]\([^)]*\)")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MARKUP_RE = re.compile(r"(?<!\\)[*_`~]+")
_SPACE_RE = re.compile(r"\s+")
_DASH_RE = re.compile(r"[‐‑‒–—−]")
_DECIMAL_RE = re.compile(r"(?<=\d)[,.](?=\d)")
_SENTENCE_BREAK_RE = re.compile(r"(?<=[.!?;])\s+(?=[A-ZА-ЯЁ0-9])")
_SPECIFIC_ENGINEERING_RE = re.compile(
    r"(?:\b(?:сп|гост|ту|вру|грщ|щр|що|ар|кр|ов|вк|эом)\s*[-./]?\s*\d"
    r"|\b\d+(?:[.:/-]\d+){1,}\b|\b[а-яa-z]{2,}[-/]\d+[а-яa-z0-9./-]*)",
    re.IGNORECASE,
)


def canonicalize_text(text: str | None) -> str:
    """Conservative formatting-only normalization; engineering values survive."""
    value = html.unescape(str(text or ""))
    value = _MARKDOWN_LINK_RE.sub(r"\1", value)
    value = _HTML_TAG_RE.sub(" ", value)
    value = _MARKUP_RE.sub("", value)
    value = value.casefold().replace("ё", "е").replace("\u00a0", " ")
    value = _DASH_RE.sub("-", value)
    value = _DECIMAL_RE.sub(".", value)
    # Pipes are structural Markdown table separators, not document content.
    value = value.replace("|", " ")
    value = _SPACE_RE.sub(" ", value).strip()
    value = re.sub(r"\s*/\s*", "/", value)
    value = re.sub(r"\s*-\s*", "-", value)
    return value


def _display_text(text: str) -> str:
    value = _MARKDOWN_LINK_RE.sub(r"\1", text)
    value = _HTML_TAG_RE.sub("", value)
    value = _MARKUP_RE.sub("", value)
    value = re.sub(r"^\s{0,3}#{1,6}\s+", "", value)
    value = re.sub(r"^\s*[-+*]\s+", "", value)
    return _SPACE_RE.sub(" ", value.replace("|", " ")).strip()


def _split_long_paragraph(text: str, maximum: int = 520) -> list[str]:
    clean = _SPACE_RE.sub(" ", text).strip()
    if len(clean) <= maximum:
        return [clean] if clean else []
    sentences = _SENTENCE_BREAK_RE.split(clean)
    result: list[str] = []
    current = ""
    for sentence in sentences:
        candidate = f"{current} {sentence}".strip()
        if current and len(candidate) > maximum:
            result.append(current)
            current = sentence
        else:
            current = candidate
    if current:
        result.append(current)
    return result


def parse_markdown_fragments(markdown: str, stage: str) -> list[dict[str, Any]]:
    """Read only real TEXT blocks and retain rows/paragraphs as separate units."""
    pages = list(_PAGE_RE.finditer(markdown or ""))
    raw_fragments: list[dict[str, Any]] = []
    for page_index, page_match in enumerate(pages):
        pdf_page = int(page_match.group(1))
        page_end = pages[page_index + 1].start() if page_index + 1 < len(pages) else len(markdown)
        page_body = markdown[page_match.end():page_end]
        blocks = list(_BLOCK_RE.finditer(page_body))
        for block_index, block_match in enumerate(blocks):
            if block_match.group(1).strip().casefold() != "text":
                continue
            block_id = block_match.group(2).strip()
            block_end = blocks[block_index + 1].start() if block_index + 1 < len(blocks) else len(page_body)
            body = page_body[block_match.end():block_end]
            paragraph: list[str] = []
            table_index = 0

            def emit(
                value: str, kind: str, group: str,
                location_parts: list[str] | None = None,
            ) -> None:
                for part in _split_long_paragraph(value):
                    text = _display_text(part)
                    canonical = canonicalize_text(text)
                    if not canonical:
                        continue
                    raw_fragments.append({
                        "stage": stage,
                        "pdf_page": pdf_page,
                        "sheet_number": None,
                        "text": text,
                        "canonical_text": canonical,
                        "source_block_id": block_id,
                        "source_kind": kind,
                        "source_group": group,
                        "location_parts": [
                            canonicalize_text(item) for item in (location_parts or [])
                            if canonicalize_text(item)
                        ],
                    })

            def flush_paragraph() -> None:
                if paragraph:
                    emit(" ".join(paragraph), "paragraph", block_id)
                    paragraph.clear()

            for raw_line in body.splitlines():
                line = raw_line.strip()
                if line.startswith("> **Created:**") or line.startswith("> **Crop:**") or line.startswith("> **Stamp:**"):
                    continue
                if not line:
                    flush_paragraph()
                    continue
                if _TABLE_DIVIDER_RE.fullmatch(line):
                    flush_paragraph()
                    continue
                if line.startswith("|") and line.endswith("|"):
                    flush_paragraph()
                    table_index += 1
                    cells = [cell.strip() for cell in line.strip("|").split("|")]
                    emit(
                        " | ".join(cells), "table_row", f"{block_id}:table",
                        location_parts=cells,
                    )
                    continue
                if re.match(r"^#{1,6}\s+", line):
                    flush_paragraph()
                    emit(line, "heading", block_id)
                    continue
                if re.match(r"^[-+*]\s+", line):
                    flush_paragraph()
                    emit(line, "list_item", block_id)
                    continue
                if line.startswith(">"):
                    # Other blockquotes are generated metadata, not sheet text.
                    continue
                paragraph.append(line)
            flush_paragraph()

    occurrence: Counter[tuple[int, str, str]] = Counter()
    for order, fragment in enumerate(raw_fragments):
        key = (
            int(fragment["pdf_page"]),
            str(fragment["source_block_id"]),
            str(fragment["canonical_text"]),
        )
        occurrence[key] += 1
        identity = "|".join((
            stage,
            str(fragment["pdf_page"]),
            str(fragment["source_block_id"]),
            str(occurrence[key]),
            str(fragment["canonical_text"]),
        ))
        fragment["id"] = "txt_" + hashlib.sha1(identity.encode("utf-8")).hexdigest()[:16]
        fragment["order"] = order
        fragment["char_count"] = len(str(fragment["canonical_text"]))
        fragment["bboxes"] = []
        fragment["source_location"] = None
    return raw_fragments


def _normalized_word_stream(words: list[tuple]) -> tuple[str, list[dict[str, Any]]]:
    pieces: list[str] = []
    indexed: list[dict[str, Any]] = []
    cursor = 0
    for word in words:
        canonical = canonicalize_text(str(word[4]))
        if not canonical:
            continue
        if pieces:
            pieces.append(" ")
            cursor += 1
        start = cursor
        pieces.append(canonical)
        cursor += len(canonical)
        indexed.append({
            "start": start,
            "end": cursor,
            "rect": tuple(float(value) for value in word[:4]),
            "block": int(word[5]),
            "line": int(word[6]),
        })
    return "".join(pieces), indexed


def _normalized_bbox(page: Any, rect: Any, fitz: Any) -> dict[str, float] | None:
    page_rect = page.rect
    area = fitz.Rect(rect)
    if page.rotation:
        area = area * page.rotation_matrix
    if not page_rect.width or not page_rect.height:
        return None
    x = max(0.0, min(1.0, (area.x0 - page_rect.x0) / page_rect.width))
    y = max(0.0, min(1.0, (area.y0 - page_rect.y0) / page_rect.height))
    right = max(x, min(1.0, (area.x1 - page_rect.x0) / page_rect.width))
    bottom = max(y, min(1.0, (area.y1 - page_rect.y0) / page_rect.height))
    if right <= x or bottom <= y:
        return None
    return {
        "x": round(x, 6),
        "y": round(y, 6),
        "width": round(right - x, 6),
        "height": round(bottom - y, 6),
    }


def _span_boxes(
    page: Any, words: list[dict[str, Any]], start: int, end: int, fitz: Any
) -> list[dict[str, float]]:
    selected = [word for word in words if word["end"] > start and word["start"] < end]
    by_line: dict[tuple[int, int], list[Any]] = defaultdict(list)
    for word in selected:
        by_line[(word["block"], word["line"])].append(fitz.Rect(word["rect"]))
    boxes = []
    for key in sorted(by_line):
        rects = by_line[key]
        united = rects[0]
        for rect in rects[1:]:
            united |= rect
        if box := _normalized_bbox(page, united, fitz):
            boxes.append(box)
    return boxes


def _all_spans(stream: str, value: str, maximum: int = 40) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    cursor = 0
    while len(spans) < maximum:
        start = stream.find(value, cursor)
        if start < 0:
            break
        spans.append((start, start + len(value)))
        cursor = start + max(1, len(value))
    return spans


def _locate_table_parts(
    page: Any, stream: str, words: list[dict[str, Any]], parts: list[str], fitz: Any
) -> tuple[list[dict[str, float]], tuple[int, int] | None]:
    candidates: list[tuple[str, list[tuple[int, int]]]] = []
    for part in dict.fromkeys(item for item in parts if len(item) >= 2):
        spans = _all_spans(stream, part)
        if spans:
            candidates.append((part, spans))
    if not candidates:
        return [], None
    # A row must have an unambiguous anchor cell.  This prevents a repeated
    # value such as "Коридор" from being placed on an arbitrary table row.
    anchors = [item for item in candidates if len(item[1]) == 1]
    if not anchors:
        return [], None
    _, anchor_spans = max(anchors, key=lambda item: len(item[0]))
    anchor = anchor_spans[0]
    anchor_boxes = _span_boxes(page, words, anchor[0], anchor[1], fitz)
    if not anchor_boxes:
        return [], None
    anchor_y = sum(box["y"] + box["height"] / 2 for box in anchor_boxes) / len(anchor_boxes)
    output = list(anchor_boxes)
    for _, spans in candidates:
        if spans == anchor_spans:
            continue
        located = []
        for start, end in spans:
            boxes = _span_boxes(page, words, start, end, fitz)
            if boxes:
                y = sum(box["y"] + box["height"] / 2 for box in boxes) / len(boxes)
                located.append((abs(y - anchor_y), boxes))
        if located:
            nearest = min(located, key=lambda item: item[0])
            if nearest[0] <= 0.04:
                output.extend(nearest[1])
    unique = {
        (box["x"], box["y"], box["width"], box["height"]): box for box in output
    }
    return sorted(unique.values(), key=lambda box: (box["y"], box["x"])), anchor


def attach_pdf_locations(
    fragments: list[dict[str, Any]], pdf_path: Path, fitz: Any
) -> None:
    """Attach vector-text word boxes without changing fragment contents."""
    by_page: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for fragment in fragments:
        by_page[int(fragment["pdf_page"])].append(fragment)
    with fitz.open(str(pdf_path)) as document:
        for pdf_page, page_fragments in by_page.items():
            if pdf_page < 1 or pdf_page > document.page_count:
                continue
            page = document[pdf_page - 1]
            stream, words = _normalized_word_stream(page.get_text("words", sort=True))
            cursors: dict[str, int] = defaultdict(int)
            for fragment in page_fragments:
                canonical = str(fragment["canonical_text"])
                start = stream.find(canonical, cursors[canonical])
                if start < 0 and cursors[canonical]:
                    start = stream.find(canonical)
                end = start + len(canonical) if start >= 0 else -1
                if start >= 0:
                    cursors[canonical] = end
                boxes = _span_boxes(page, words, start, end, fitz) if start >= 0 else []
                if not boxes and fragment.get("source_kind") == "table_row":
                    boxes, table_span = _locate_table_parts(
                        page, stream, words, fragment.get("location_parts") or [], fitz
                    )
                    if table_span:
                        start, end = table_span
                if not boxes:
                    continue
                fragment["bboxes"] = boxes
                fragment["source_location"] = {
                    "pdf_page": pdf_page,
                    "word_start": start,
                    "word_end": end,
                }


def extract_document_fragments(
    *, stage: str, markdown_path: Path, pdf_path: Path,
    sheet_index: list[dict[str, Any]], fitz: Any,
) -> list[dict[str, Any]]:
    markdown = markdown_path.read_text(encoding="utf-8")
    fragments = parse_markdown_fragments(markdown, stage)
    sheet_numbers = {
        int(item["pdf_page"]): item.get("sheet_number")
        for item in sheet_index if item.get("pdf_page") is not None
    }
    for fragment in fragments:
        fragment["sheet_number"] = sheet_numbers.get(int(fragment["pdf_page"]))
    attach_pdf_locations(fragments, pdf_path, fitz)
    return fragments


def _relative_position(fragment: dict[str, Any]) -> tuple[float, float]:
    boxes = fragment.get("bboxes") or []
    if not boxes:
        return 0.5, 0.5
    return (
        sum(float(box["x"]) + float(box["width"]) / 2 for box in boxes) / len(boxes),
        sum(float(box["y"]) + float(box["height"]) / 2 for box in boxes) / len(boxes),
    )


def _ordered(fragments: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(fragments, key=lambda item: (
        int(item["pdf_page"]), _relative_position(item)[1],
        _relative_position(item)[0], int(item.get("order") or 0), str(item["id"]),
    ))


def _safe_exact_pairs(
    left: list[dict[str, Any]], right: list[dict[str, Any]]
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    if len(left) == len(right) == 1:
        return [(left[0], right[0])]
    if len(left) != len(right) or not left:
        return []
    left_ordered, right_ordered = _ordered(left), _ordered(right)
    canonical = str(left_ordered[0]["canonical_text"])
    pairs = list(zip(left_ordered, right_ordered))
    for a, b in pairs:
        if not a.get("bboxes") or not b.get("bboxes"):
            return []
        ax, ay = _relative_position(a)
        bx, by = _relative_position(b)
        maximum_distance = 0.24 if len(canonical) >= 30 else 0.12
        if a.get("source_kind") != b.get("source_kind") or abs(ax - bx) + abs(ay - by) > maximum_distance:
            return []
    return pairs


def _match_id(left_id: str, right_id: str, status: str) -> str:
    raw = f"{status}|{left_id}|{right_id}"
    return "match_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _match_record(
    left: dict[str, Any], right: dict[str, Any], status: str,
    link: dict[str, Any] | None, origin_side: str | None = None,
) -> dict[str, Any]:
    return {
        "id": _match_id(str(left["id"]), str(right["id"]), status),
        "left_fragment_id": left["id"],
        "right_fragment_id": right["id"],
        "left_page": int(left["pdf_page"]),
        "right_page": int(right["pdf_page"]),
        "left_bboxes": left.get("bboxes") or [],
        "right_bboxes": right.get("bboxes") or [],
        "status": status,
        "canonical_text": left["canonical_text"],
        "confidence": "exact",
        "link_id": (link or {}).get("id"),
        "expected_left_pages": [int(p) for p in (link or {}).get("left_pages") or []],
        "expected_right_pages": [int(p) for p in (link or {}).get("right_pages") or []],
        "origin_side": origin_side,
    }


def _cross_sheet_eligible(canonical: str) -> bool:
    compact = canonical.strip()
    return len(compact) >= 15 or bool(_SPECIFIC_ENGINEERING_RE.search(compact))


def _fragment_maps(fragments: list[dict[str, Any]]) -> tuple[dict[str, dict], dict[int, list[dict]]]:
    by_id = {str(fragment["id"]): fragment for fragment in fragments}
    by_page: dict[int, list[dict]] = defaultdict(list)
    for fragment in fragments:
        by_page[int(fragment["pdf_page"])].append(fragment)
    return by_id, by_page


def compare_fragments(
    left_fragments: list[dict[str, Any]], right_fragments: list[dict[str, Any]],
    links: list[dict[str, Any]], *, left_page_count: int | None = None,
    right_page_count: int | None = None,
) -> dict[str, Any]:
    """Run linked exact matching, then conservative exact cross-sheet lookup."""
    left_by_id, left_by_page = _fragment_maps(left_fragments)
    right_by_id, right_by_page = _fragment_maps(right_fragments)
    used_left: set[str] = set()
    used_right: set[str] = set()
    matches: list[dict[str, Any]] = []
    normalized_links = sorted(links, key=lambda item: (
        min(item.get("left_pages") or [10**9]), min(item.get("right_pages") or [10**9]),
        str(item.get("id") or ""),
    ))

    for link in normalized_links:
        left_group = [fragment for page in link.get("left_pages") or [] for fragment in left_by_page.get(int(page), []) if fragment["id"] not in used_left]
        right_group = [fragment for page in link.get("right_pages") or [] for fragment in right_by_page.get(int(page), []) if fragment["id"] not in used_right]
        left_exact: dict[str, list[dict]] = defaultdict(list)
        right_exact: dict[str, list[dict]] = defaultdict(list)
        for fragment in left_group:
            left_exact[str(fragment["canonical_text"])].append(fragment)
        for fragment in right_group:
            right_exact[str(fragment["canonical_text"])].append(fragment)
        for canonical in sorted(set(left_exact) & set(right_exact)):
            for left, right in _safe_exact_pairs(left_exact[canonical], right_exact[canonical]):
                used_left.add(str(left["id"]))
                used_right.add(str(right["id"]))
                matches.append(_match_record(left, right, "same_on_linked_sheet", link))

    # Only fragments belonging to an accepted relation are pass-2 sources.
    linked_left_pages = {int(page) for link in normalized_links for page in link.get("left_pages") or []}
    linked_right_pages = {int(page) for link in normalized_links for page in link.get("right_pages") or []}
    left_expected = {
        int(page): link for link in normalized_links for page in link.get("left_pages") or []
    }
    right_expected = {
        int(page): link for link in normalized_links for page in link.get("right_pages") or []
    }
    right_index: dict[str, list[dict]] = defaultdict(list)
    left_index: dict[str, list[dict]] = defaultdict(list)
    for fragment in right_fragments:
        right_index[str(fragment["canonical_text"])].append(fragment)
    for fragment in left_fragments:
        left_index[str(fragment["canonical_text"])].append(fragment)

    def is_frequent(candidates: list[dict], page_count: int) -> bool:
        pages = {int(item["pdf_page"]) for item in candidates}
        return len(candidates) >= 8 or len(pages) > max(4, int(max(1, page_count) * 0.18))

    def find_elsewhere(source: dict, side: str) -> tuple[dict, dict] | None:
        canonical = str(source["canonical_text"])
        if not _cross_sheet_eligible(canonical):
            return None
        if side == "left":
            link = left_expected.get(int(source["pdf_page"]))
            if not link:
                return None
            expected = {int(page) for page in link.get("right_pages") or []}
            all_candidates = right_index.get(canonical, [])
            candidates = [item for item in all_candidates if int(item["pdf_page"]) not in expected and item["id"] not in used_right]
            total = right_page_count or max(right_by_page or {1: []})
        else:
            link = right_expected.get(int(source["pdf_page"]))
            if not link:
                return None
            expected = {int(page) for page in link.get("left_pages") or []}
            all_candidates = left_index.get(canonical, [])
            candidates = [item for item in all_candidates if int(item["pdf_page"]) not in expected and item["id"] not in used_left]
            total = left_page_count or max(left_by_page or {1: []})
        if not candidates or is_frequent(all_candidates, total):
            return None
        # A unique actual page is enough; repeated occurrences on different
        # pages are ambiguous and therefore remain for later comparison.
        pages = {int(item["pdf_page"]) for item in candidates}
        if len(pages) != 1:
            return None
        if len(candidates) == 1:
            return candidates[0], link
        if not source.get("bboxes") or any(not item.get("bboxes") for item in candidates):
            return None
        sx, sy = _relative_position(source)
        ranked = sorted(
            (
                abs(sx - _relative_position(item)[0])
                + abs(sy - _relative_position(item)[1]),
                str(item["id"]), item,
            )
            for item in candidates
        )
        if ranked[0][0] > 0.3 or (
            len(ranked) > 1 and ranked[1][0] - ranked[0][0] < 0.02
        ):
            return None
        return ranked[0][2], link

    for source in _ordered(fragment for fragment in left_fragments if int(fragment["pdf_page"]) in linked_left_pages and fragment["id"] not in used_left):
        found = find_elsewhere(source, "left")
        if not found:
            continue
        target, link = found
        used_left.add(str(source["id"]))
        used_right.add(str(target["id"]))
        matches.append(_match_record(source, target, "found_on_other_sheet", link, "left"))

    for source in _ordered(fragment for fragment in right_fragments if int(fragment["pdf_page"]) in linked_right_pages and fragment["id"] not in used_right):
        found = find_elsewhere(source, "right")
        if not found:
            continue
        target, link = found
        used_right.add(str(source["id"]))
        used_left.add(str(target["id"]))
        matches.append(_match_record(target, source, "found_on_other_sheet", link, "right"))

    matches.sort(key=lambda item: (
        item["left_page"], item["right_page"], item["status"], item["id"]
    ))
    remaining_left = [fragment["id"] for fragment in left_fragments if int(fragment["pdf_page"]) in linked_left_pages and fragment["id"] not in used_left]
    remaining_right = [fragment["id"] for fragment in right_fragments if int(fragment["pdf_page"]) in linked_right_pages and fragment["id"] not in used_right]
    return {
        "matches": matches,
        "remaining": {"left": remaining_left, "right": remaining_right},
        "used_left": used_left,
        "used_right": used_right,
        "left_by_id": left_by_id,
        "right_by_id": right_by_id,
    }


def _sum_chars(fragments: Iterable[dict[str, Any]]) -> int:
    return sum(int(fragment.get("char_count") or len(str(fragment.get("canonical_text") or ""))) for fragment in fragments)


def build_metrics_and_hints(
    comparison: dict[str, Any], left_fragments: list[dict[str, Any]],
    right_fragments: list[dict[str, Any]], links: list[dict[str, Any]],
    left_labels: dict[int, str] | None = None, right_labels: dict[int, str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    left_labels, right_labels = left_labels or {}, right_labels or {}
    left_by_id, left_by_page = _fragment_maps(left_fragments)
    right_by_id, right_by_page = _fragment_maps(right_fragments)
    matches = comparison["matches"]
    metrics: list[dict[str, Any]] = []
    hints: list[dict[str, Any]] = []
    all_relevant_left: set[str] = set()
    all_relevant_right: set[str] = set()

    for link in links:
        left_pages = {int(page) for page in link.get("left_pages") or []}
        right_pages = {int(page) for page in link.get("right_pages") or []}
        left_items = [item for page in left_pages for item in left_by_page.get(page, [])]
        right_items = [item for page in right_pages for item in right_by_page.get(page, [])]
        left_ids = {str(item["id"]) for item in left_items}
        right_ids = {str(item["id"]) for item in right_items}
        all_relevant_left.update(left_ids)
        all_relevant_right.update(right_ids)
        linked = [item for item in matches if item["status"] == "same_on_linked_sheet" and item["left_fragment_id"] in left_ids and item["right_fragment_id"] in right_ids]
        elsewhere = [item for item in matches if item["status"] == "found_on_other_sheet" and (item["left_fragment_id"] in left_ids or item["right_fragment_id"] in right_ids)]
        linked_left_ids = {item["left_fragment_id"] for item in linked}
        linked_right_ids = {item["right_fragment_id"] for item in linked}
        elsewhere_left_ids = {item["left_fragment_id"] for item in elsewhere if item["left_fragment_id"] in left_ids}
        elsewhere_right_ids = {item["right_fragment_id"] for item in elsewhere if item["right_fragment_id"] in right_ids}
        left_chars, right_chars = _sum_chars(left_items), _sum_chars(right_items)
        linked_left_chars = _sum_chars(left_by_id[item] for item in linked_left_ids)
        linked_right_chars = _sum_chars(right_by_id[item] for item in linked_right_ids)
        elsewhere_left_chars = _sum_chars(left_by_id[item] for item in elsewhere_left_ids)
        elsewhere_right_chars = _sum_chars(right_by_id[item] for item in elsewhere_right_ids)
        remaining_left_chars = max(0, left_chars - linked_left_chars - elsewhere_left_chars)
        remaining_right_chars = max(0, right_chars - linked_right_chars - elsewhere_right_chars)
        total_chars = left_chars + right_chars
        combined_linked = linked_left_chars + linked_right_chars
        combined_elsewhere = elsewhere_left_chars + elsewhere_right_chars
        metric = {
            "link_id": link.get("id"),
            "left_pages": sorted(left_pages),
            "right_pages": sorted(right_pages),
            "left_fragments": len(left_items),
            "right_fragments": len(right_items),
            "left_text_chars": left_chars,
            "right_text_chars": right_chars,
            "exact_linked_matches": len(linked),
            "matched_linked_left_chars": linked_left_chars,
            "matched_linked_right_chars": linked_right_chars,
            "matched_elsewhere_left_chars": elsewhere_left_chars,
            "matched_elsewhere_right_chars": elsewhere_right_chars,
            "remaining_left_chars": remaining_left_chars,
            "remaining_right_chars": remaining_right_chars,
            "left_linked_share": round(linked_left_chars / left_chars, 4) if left_chars else 0,
            "right_linked_share": round(linked_right_chars / right_chars, 4) if right_chars else 0,
            "combined": {
                "linked_percent": round(100 * combined_linked / total_chars, 1) if total_chars else 0,
                "elsewhere_percent": round(100 * combined_elsewhere / total_chars, 1) if total_chars else 0,
                "remaining_percent": round(100 * max(0, total_chars - combined_linked - combined_elsewhere) / total_chars, 1) if total_chars else 0,
            },
        }
        metrics.append(metric)

        aggregates: dict[tuple[str, int, int], dict[str, Any]] = {}
        pass1_remaining_by_source: dict[tuple[str, int], int] = defaultdict(int)
        for fragment in left_items:
            if fragment["id"] not in linked_left_ids:
                pass1_remaining_by_source[("left", int(fragment["pdf_page"]))] += int(fragment["char_count"])
        for fragment in right_items:
            if fragment["id"] not in linked_right_ids:
                pass1_remaining_by_source[("right", int(fragment["pdf_page"]))] += int(fragment["char_count"])
        for match in elsewhere:
            source_side = str(match.get("origin_side") or "left")
            if source_side == "left" and match["left_fragment_id"] in left_ids:
                source_id, source_page, target_page = match["left_fragment_id"], int(match["left_page"]), int(match["right_page"])
                fragment = left_by_id[source_id]
            elif source_side == "right" and match["right_fragment_id"] in right_ids:
                source_id, source_page, target_page = match["right_fragment_id"], int(match["right_page"]), int(match["left_page"])
                fragment = right_by_id[source_id]
            else:
                continue
            key = (source_side, source_page, target_page)
            aggregate = aggregates.setdefault(key, {"fragment_ids": [], "matched_chars": 0})
            aggregate["fragment_ids"].append(source_id)
            aggregate["matched_chars"] += int(fragment["char_count"])
        for (source_side, source_page, target_page), aggregate in sorted(aggregates.items()):
            remaining_chars = pass1_remaining_by_source[(source_side, source_page)]
            share_remaining = aggregate["matched_chars"] / remaining_chars if remaining_chars else 0
            if aggregate["matched_chars"] < 40 and len(aggregate["fragment_ids"]) < 2:
                continue
            linked_share = metric["left_linked_share"] if source_side == "left" else metric["right_linked_share"]
            source_total = _sum_chars(left_by_page[source_page]) if source_side == "left" else _sum_chars(right_by_page[source_page])
            share_total = aggregate["matched_chars"] / source_total if source_total else 0
            strength = "likely_incorrect" if linked_share < 0.18 and share_total >= 0.55 else "additional"
            target_side = "right" if source_side == "left" else "left"
            current_target_pages = sorted(right_pages if source_side == "left" else left_pages)
            labels = right_labels if target_side == "right" else left_labels
            hint_id = "hint_" + hashlib.sha1(
                f"{link.get('id')}|{source_side}|{source_page}|{target_page}".encode("utf-8")
            ).hexdigest()[:14]
            hints.append({
                "id": hint_id,
                "link_id": link.get("id"),
                "kind": strength,
                "source_side": source_side,
                "source_page": source_page,
                "target_side": target_side,
                "current_target_pages": current_target_pages,
                "actual_page": target_page,
                "actual_label": labels.get(target_page) or f"Страница {target_page}",
                "matched_fragments": len(aggregate["fragment_ids"]),
                "matched_chars": aggregate["matched_chars"],
                "share_of_remaining_text": round(share_remaining, 4),
                "share_of_source_text": round(share_total, 4),
                "linked_share": linked_share,
            })

    relevant_left = [left_by_id[item] for item in all_relevant_left]
    relevant_right = [right_by_id[item] for item in all_relevant_right]
    linked_matches = [item for item in matches if item["status"] == "same_on_linked_sheet"]
    elsewhere_matches = [item for item in matches if item["status"] == "found_on_other_sheet"]
    linked_chars = sum(
        int(left_by_id[item["left_fragment_id"]]["char_count"]) + int(right_by_id[item["right_fragment_id"]]["char_count"])
        for item in linked_matches
    )
    elsewhere_chars = sum(
        (int(left_by_id[item["left_fragment_id"]]["char_count"]) if item["left_fragment_id"] in all_relevant_left else 0)
        + (int(right_by_id[item["right_fragment_id"]]["char_count"]) if item["right_fragment_id"] in all_relevant_right else 0)
        for item in elsewhere_matches
    )
    total_chars = _sum_chars(relevant_left) + _sum_chars(relevant_right)
    summary = {
        "links": len(links),
        "left_fragments": len(relevant_left),
        "right_fragments": len(relevant_right),
        "left_text_chars": _sum_chars(relevant_left),
        "right_text_chars": _sum_chars(relevant_right),
        "linked_matches": len(linked_matches),
        "found_elsewhere_matches": len(elsewhere_matches),
        "linked_chars": linked_chars,
        "found_elsewhere_chars": elsewhere_chars,
        "remaining_chars": max(0, total_chars - linked_chars - elsewhere_chars),
        "linked_percent": round(100 * linked_chars / total_chars, 1) if total_chars else 0,
        "found_elsewhere_percent": round(100 * elsewhere_chars / total_chars, 1) if total_chars else 0,
        "remaining_percent": round(100 * max(0, total_chars - linked_chars - elsewhere_chars) / total_chars, 1) if total_chars else 0,
        "hints": len(hints),
    }
    metrics.sort(key=lambda item: (item["left_pages"], item["right_pages"], str(item["link_id"])))
    hints.sort(key=lambda item: (item["source_side"], item["source_page"], item["actual_page"]))
    return metrics, hints, summary


def build_overlays(matches: list[dict[str, Any]], labels: dict[str, dict[int, str]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    overlays: dict[str, dict[str, list[dict[str, Any]]]] = {"left": {}, "right": {}}
    for match in matches:
        for side in ("left", "right"):
            page = int(match[f"{side}_page"])
            other_side = "right" if side == "left" else "left"
            other_page = int(match[f"{other_side}_page"])
            status = str(match["status"])
            if status == "found_on_other_sheet":
                stage_label = "РД" if other_side == "right" else "П"
                other_label = labels.get(other_side, {}).get(other_page) or f"Страница {other_page}"
                title = f"Найдено на другом листе {stage_label}: {other_label}"
            else:
                title = "Совпало на связанном листе"
            bucket = overlays[side].setdefault(str(page), [])
            for box_index, box in enumerate(match.get(f"{side}_bboxes") or []):
                bucket.append({
                    "id": f"{match['id']}_{side}_{box_index}",
                    **box,
                    "status": status,
                    "title": title,
                    "counterpart_page": other_page,
                })
    for side in overlays:
        for page in overlays[side]:
            overlays[side][page].sort(key=lambda item: (item["y"], item["x"], item["id"]))
    return overlays


def public_view(payload: dict[str, Any] | None, *, stale: bool = False) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or payload.get("version") != 1:
        return None
    return {
        "version": 1,
        "pair_id": payload.get("pair_id"),
        "generated_at": payload.get("generated_at"),
        "source_signature": payload.get("source_signature"),
        "stale": bool(stale),
        "summary": payload.get("summary") or {},
        "link_metrics": payload.get("link_metrics") or [],
        "sheet_link_hints": payload.get("sheet_link_hints") or [],
        "overlays": payload.get("overlays") or {"left": {}, "right": {}},
    }


__all__ = [
    "canonicalize_text", "parse_markdown_fragments", "attach_pdf_locations",
    "extract_document_fragments", "compare_fragments", "build_metrics_and_hints",
    "build_overlays", "public_view",
]
