"""Small, text-only sheet passports and rule-based P-to-RD suggestions.

This module deliberately operates on the current Markdown files only.  It has
no dependency on prepared documents, OCR, images, blocks, geometry or models.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable


_PAGE_RE = re.compile(r"(?m)^##\s+Page\s+(\d+)\s*$")
_STAMP_SHEET_RE = re.compile(r"\|\s*Sheet:\s*([^|\r\n]*)", re.IGNORECASE)
_STAMP_NAME_RE = re.compile(r"\|\s*Name:\s*([^|\r\n]*)", re.IGNORECASE)
_BUILDING_RE = re.compile(
    r"\bкорпус(?:а|ов)?\s*[:№]?\s*"
    r"((?:\d+(?:[.,]\d+)?)(?:\s*(?:,|;|\sи\s)\s*\d+(?:[.,]\d+)?){0,8})",
    re.IGNORECASE,
)
_FLOOR_RE = re.compile(
    r"\b(\d{1,2}(?:\s*[-–—]\s*\d{1,2})?)\s*"
    r"(?:[-‐‑–—]?\s*(?:го|й|ого))?\s*этаж(?:а|ей|и)?\b",
    re.IGNORECASE,
)
_LEVEL_RE = re.compile(r"\bLevel\s*:\s*([+-]?\d+(?:[.,]\d+)?)", re.IGNORECASE)
_CONTENTS_ROW_RE = re.compile(
    r"^\s*\|\s*([^|]{1,24}?)\s*\|\s*([^|]{4,500}?)\s*\|",
    re.MULTILINE,
)
_UNDERGROUND_WORDS = {
    "первый": 1,
    "первого": 1,
    "1": 1,
    "второй": 2,
    "второго": 2,
    "2": 2,
    "третий": 3,
    "третьего": 3,
    "3": 3,
}


@dataclass(frozen=True)
class PageSection:
    pdf_page: int
    text: str


@dataclass(frozen=True)
class SheetPassport:
    pdf_page: int
    sheet_number: str | None
    kind: str
    buildings: tuple[str, ...]
    floor: str | None
    level: str | None
    roof: bool
    underground: bool
    underground_level: int | None
    title_hint: str
    search_text: str
    source: dict

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["buildings"] = list(self.buildings)
        return payload


def split_markdown_pages(markdown: str) -> list[PageSection]:
    """Split a document by exact ``## Page N`` headings."""
    matches = list(_PAGE_RE.finditer(markdown or ""))
    return [
        PageSection(
            pdf_page=int(match.group(1)),
            text=(markdown[match.end(): matches[index + 1].start()] if index + 1 < len(matches)
                  else markdown[match.end():]).strip(),
        )
        for index, match in enumerate(matches)
    ]


def normalize_level(raw: str | None) -> str | None:
    value = str(raw or "").strip().replace(",", ".")
    if not value:
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        return None
    sign = "+" if value.startswith("+") else ""
    return f"{sign}{number.quantize(Decimal('0.001'))}"


def extract_buildings(text: str) -> tuple[str, ...]:
    """Return the most title-like explicit ``Корпус(а) ...`` occurrence."""
    candidates: list[tuple[int, int, tuple[str, ...]]] = []
    for index, line in enumerate((text or "").splitlines()):
        for match in _BUILDING_RE.finditer(line):
            values = tuple(sorted(
                {token.replace(",", ".") for token in re.findall(r"\d+(?:[.,]\d+)?", match.group(1))},
                key=lambda token: tuple(int(part) for part in token.split(".")),
            ))
            if not values:
                continue
            low = line.casefold()
            score = 0
            if any(token in low for token in ("план", "кров", "этаж", "level", "type:")):
                score += 4
            if len(line.strip()) <= 180:
                score += 2
            if line.lstrip().startswith(("#", "**", "> **")):
                score += 1
            candidates.append((score, -index, values))
    return max(candidates, default=(0, 0, ()))[2]


def extract_floor(text: str) -> str | None:
    best: tuple[int, int, str] | None = None
    for index, line in enumerate((text or "").splitlines()):
        for match in _FLOOR_RE.finditer(line):
            value = re.sub(r"\s*[-–—]\s*", "-", match.group(1))
            low = line.casefold()
            score = (4 if "план" in low else 0) + (2 if len(line.strip()) <= 180 else 0)
            candidate = (score, -index, value)
            if best is None or candidate > best:
                best = candidate
    return best[2] if best else None


def _semantic_text(page_text: str) -> str:
    # The stamp's Object field is repeated on every page and must not turn the
    # whole document into the same building/underground passport.  Name and
    # Sheet are parsed separately before these lines are removed.
    lines = [line for line in page_text.splitlines() if "**Stamp:**" not in line]
    return "\n".join(lines)


def _sheet_number(page_text: str) -> str | None:
    match = _STAMP_SHEET_RE.search(page_text)
    value = re.sub(r"\s+", " ", match.group(1)).strip() if match else ""
    return value or None


def _stamp_names(page_text: str) -> list[str]:
    return [
        re.sub(r"[*_`]+", "", match.group(1)).strip()
        for match in _STAMP_NAME_RE.finditer(page_text)
        if match.group(1).strip()
    ]


def _kind(text: str) -> str:
    low = text.casefold()
    if re.search(r"\b(содержание\s+тома|содержание|ведомость\s+рабочих\s+чертежей)\b", low):
        return "contents"
    if re.search(r"\b(план\s+кровли|кровл[яиюе])\b", low):
        return "roof"
    if re.search(r"type\s*:\s*план\b", low):
        return "plan"
    if re.search(r"type\s*:\s*разрез\b|\bразрез\b", low):
        return "section"
    if re.search(r"type\s*:\s*фасад\b|\bфасад\b", low):
        return "facade"
    if re.search(r"\bсхем[аы]\b", low):
        return "scheme"
    if re.search(r"\b(пояснительная\s+записка|общие\s+указания|п\.?\s*з\.?)\b", low):
        return "note"
    if re.search(r"\bплан(?:ы|а|ом)?\b", low):
        return "plan"
    if re.search(r"\bспецификац", low):
        return "specification"
    if re.search(r"\bтаблиц[аы]\b", low):
        return "table"
    return "other"


def _classification_text(text: str, names: Iterable[str], contents_title: str) -> str:
    lines = [*names]
    if contents_title:
        lines.append(contents_title)
    for raw in text.splitlines():
        line = raw.strip()
        low = line.casefold()
        if "**[image]**" in low or line.startswith("#"):
            lines.append(line)
        elif len(line) <= 280 and any(token in low for token in (
            "план", "кров", "разрез", "фасад", "схем", "спецификац",
            "пояснительная записка", "общие указания", "содержание",
            "ведомость рабочих чертежей",
        )):
            lines.append(line)
    return "\n".join(lines)


def _underground(text: str, level: str | None) -> tuple[bool, int | None]:
    match = re.search(
        r"\b(первый|первого|второй|второго|третий|третьего|[123])"
        r"(?:\s*[-‐‑–—]?\s*(?:й|го))?\s+подземн\w*\s+этаж\w*",
        text,
        re.IGNORECASE,
    )
    underground_level = _UNDERGROUND_WORDS.get(match.group(1).casefold()) if match else None
    negative_level = bool(level and level.startswith("-"))
    return bool(match or negative_level), underground_level


def _title_hint(text: str, names: Iterable[str], fallback: str = "") -> str:
    candidates: list[tuple[int, int, str]] = []
    raw_lines = [*names, *(text or "").splitlines(), fallback]
    for index, raw in enumerate(raw_lines):
        line = re.sub(r"^[>|#\s]+|[*_`]", "", raw).strip(" |")
        line = re.sub(r"\s+", " ", line)
        if not line or len(line) > 280 or line.startswith(("Created:", "Crop:")):
            continue
        low = line.casefold()
        score = 0
        if "план" in low or "кров" in low:
            score += 5
        if "корпус" in low:
            score += 3
        if "этаж" in low or "level:" in low:
            score += 3
        if raw in names:
            score += 2
        if fallback and raw == fallback:
            score += 2
        if score:
            candidates.append((score, -index, line))
    if candidates:
        return max(candidates)[2][:280]
    return fallback[:280] if fallback else "Лист без распознанного названия"


def _compact_search_text(text: str, names: Iterable[str], contents_title: str) -> str:
    useful = list(names)
    if contents_title:
        useful.append(contents_title)
    for line in text.splitlines():
        low = line.casefold()
        if any(token in low for token in (
            "корпус", "этаж", "кров", "type:", "level:", "zone:", "summary:", "подземн",
        )):
            useful.append(line)
    compact = re.sub(r"\s+", " ", " ".join(useful)).strip()
    return compact[:4000]


def _contents_catalog(sections: Iterable[PageSection]) -> dict[str, str]:
    catalog: dict[str, str] = {}
    for section in sections:
        low = section.text.casefold()
        if "содержание" not in low and "ведомость рабочих чертежей" not in low:
            continue
        for match in _CONTENTS_ROW_RE.finditer(section.text):
            raw_number = re.sub(r"[*_`]", "", match.group(1)).strip()
            title = re.sub(r"[*_`]", "", match.group(2)).strip()
            if not re.fullmatch(r"[\w.-]{1,16}", raw_number, re.UNICODE):
                continue
            if any(token in title.casefold() for token in ("наименование", "содержание", "---")):
                continue
            catalog.setdefault(raw_number, title)
    return catalog


def extract_sheet_passport(
    pdf_page: int,
    page_text: str,
    *,
    contents_title: str = "",
) -> SheetPassport:
    sheet_number = _sheet_number(page_text)
    names = _stamp_names(page_text)
    semantic = _semantic_text(page_text)
    feature_text = "\n".join([*names, semantic, contents_title])
    classification_text = _classification_text(semantic, names, contents_title)
    level_match = _LEVEL_RE.search(feature_text)
    level = normalize_level(level_match.group(1)) if level_match else None
    roof = bool(re.search(
        r"\b(план\s+кровли|кровл[яиюе])\b", classification_text, re.IGNORECASE,
    ))
    underground, underground_level = _underground(feature_text, level)
    source = {"md_page": pdf_page}
    if sheet_number:
        source["sheet_number"] = "stamp"
    if contents_title:
        source["contents_title"] = contents_title
    return SheetPassport(
        pdf_page=pdf_page,
        sheet_number=sheet_number,
        kind=_kind(classification_text),
        buildings=extract_buildings(feature_text),
        floor=extract_floor(feature_text),
        level=level,
        roof=roof,
        underground=underground,
        underground_level=underground_level,
        title_hint=_title_hint(semantic, names, contents_title),
        search_text=_compact_search_text(semantic, names, contents_title),
        source=source,
    )


def build_sheet_passports(markdown: str) -> list[SheetPassport]:
    sections = split_markdown_pages(markdown)
    catalog = _contents_catalog(sections)
    passports: list[SheetPassport] = []
    for section in sections:
        number = _sheet_number(section.text)
        contents_title = catalog.get(number or "", "")
        # A contents page describes many other sheets; never enrich it with a
        # row that happens to share its own sheet number.
        if _kind(_semantic_text(section.text)) == "contents":
            contents_title = ""
        passports.append(extract_sheet_passport(
            section.pdf_page,
            section.text,
            contents_title=contents_title,
        ))
    return passports


def _same_buildings(left: SheetPassport, right: SheetPassport) -> bool:
    return bool(left.buildings and right.buildings and left.buildings == right.buildings)


def _same_kind(left: SheetPassport, right: SheetPassport) -> bool:
    return left.kind == right.kind and left.kind != "other"


def _contradictions(left: SheetPassport, right: SheetPassport) -> list[str]:
    out = []
    if left.buildings and right.buildings and left.buildings != right.buildings:
        out.append("different_buildings")
    if left.floor and right.floor and left.floor != right.floor:
        out.append("different_floor")
    if left.level and right.level and left.level != right.level:
        out.append("different_level")
    if left.roof != right.roof and (left.roof or right.roof):
        out.append("different_roof")
    meaningful = {"plan", "roof", "section", "facade", "scheme"}
    if left.kind in meaningful and right.kind in meaningful and left.kind != right.kind:
        out.append("different_kind")
    return out


def _high_confidence(left: SheetPassport, right: SheetPassport) -> bool:
    if not _same_kind(left, right):
        return False
    if left.underground and right.underground and left.level and left.level == right.level:
        return True
    if not _same_buildings(left, right):
        return False
    if left.roof and right.roof:
        return True
    return bool(left.floor and left.floor == right.floor)


def _candidate(
    left: SheetPassport,
    right: SheetPassport,
    *,
    left_position: int,
    right_position: int,
    left_total: int,
    right_total: int,
) -> dict:
    score = 0
    reason: list[str] = []
    if _same_buildings(left, right):
        score += 4
        reason.append("same_buildings")
    if left.floor and left.floor == right.floor:
        score += 4
        reason.append("same_floor")
    if left.level and left.level == right.level:
        score += 3
        reason.append("same_level")
    if _same_kind(left, right):
        score += 2
        reason.append("same_kind")
    if left.sheet_number and left.sheet_number == right.sheet_number:
        score += 1
        reason.append("same_sheet_number")
    left_ratio = left_position / max(1, left_total - 1)
    right_ratio = right_position / max(1, right_total - 1)
    order_distance = abs(left_ratio - right_ratio)
    if order_distance <= 0.10:
        score += 1
        reason.append("order_neighbor")
    contradictions = _contradictions(left, right)
    high = _high_confidence(left, right) and not contradictions
    eligible = not contradictions and (high or score >= 7)
    return {
        "right_page": right.pdf_page,
        "right_passport": right.to_dict(),
        "confidence": "high" if high else ("medium" if eligible else "low"),
        "reason": reason,
        "score": score,
        "contradictions": contradictions,
        "eligible": eligible,
        "order_distance": order_distance,
    }


def suggest_sheet_matches(
    left_passports: list[SheetPassport],
    right_passports: list[SheetPassport],
) -> dict:
    suggestions = []
    used_right_pages: set[int] = set()
    for left_position, left in enumerate(left_passports):
        ranked = [
            _candidate(
                left,
                right,
                left_position=left_position,
                right_position=right_position,
                left_total=len(left_passports),
                right_total=len(right_passports),
            )
            for right_position, right in enumerate(right_passports)
        ]
        ranked.sort(key=lambda item: (
            0 if item["confidence"] == "high" else 1 if item["confidence"] == "medium" else 2,
            -item["score"],
            len(item["contradictions"]),
            item["order_distance"],
            item["right_page"],
        ))
        primary = next((item for item in ranked if item["eligible"]), None)
        if primary:
            used_right_pages.add(primary["right_page"])
        alternatives = [item for item in ranked if item is not primary][:3]
        suggestions.append({
            "left_page": left.pdf_page,
            "left_passport": left.to_dict(),
            "primary_right_page": primary["right_page"] if primary else None,
            "primary_right_passport": primary["right_passport"] if primary else None,
            "confidence": primary["confidence"] if primary else "unmatched",
            "reason": primary["reason"] if primary else [],
            "alternatives": [{key: item[key] for key in (
                "right_page", "right_passport", "confidence", "reason",
            )} for item in alternatives],
            "source": "auto",
        })
    matched_left = {item["left_page"] for item in suggestions if item["primary_right_page"] is not None}
    return {
        "left_passports": [passport.to_dict() for passport in left_passports],
        "right_passports": [passport.to_dict() for passport in right_passports],
        "suggestions": suggestions,
        "unmatched_left_pages": [
            passport.pdf_page for passport in left_passports if passport.pdf_page not in matched_left
        ],
        "unmatched_right_pages": [
            passport.pdf_page for passport in right_passports if passport.pdf_page not in used_right_pages
        ],
    }


__all__ = [
    "PageSection",
    "SheetPassport",
    "split_markdown_pages",
    "normalize_level",
    "extract_buildings",
    "extract_floor",
    "extract_sheet_passport",
    "build_sheet_passports",
    "suggest_sheet_matches",
]
