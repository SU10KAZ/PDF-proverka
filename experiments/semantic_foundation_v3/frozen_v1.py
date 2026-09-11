"""Offline deterministic Comparison Unit Classifier / materializer V1.

Audit-only code. It has no imports from matcher implementations and performs
no network or model calls. Source Markdown is primary; blocks.json supplies
geometry and block provenance. PDF/native text is never read globally.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
import hashlib
import json
import re
import statistics
import unicodedata
from pathlib import Path
from typing import Any, Iterable


PRODUCER_VERSION = "comparison-unit-classifier-v1.0.0"
SCHEMA_VERSIONS = {
    "comparison_units": "comparison-units.v1",
    "text_sections": "text-sections.v1",
    "table_identities": "table-identities.v1",
}
THRESHOLDS = {
    "landscape_ratio": 1.10,
    "graphic_dominant_ratio": 0.50,
    "full_page_graphic_ratio": 0.75,
    "table_dense_ratio": 0.75,
    "table_neighbor_ratio": 0.40,
}
GRAPHIC_LEAF_TERMS = (
    "план", "схем", "чертеж", "ведомост", "экспликац",
    "разрез", "фасад", "узел", "генплан",
)

PAGE_RE = re.compile(r"^## Page\s+(\d+)\s*$")
BLOCK_RE = re.compile(r"^### BLOCK #([^ ]+) \[([^]]+)\]:\s*(\S+)\s*$")
HEADING_RE = re.compile(r"^(#{3,6})\s+(.+?)\s*$")
NUMBERED_HEADING_RE = re.compile(
    r"^(?:\*\*)?\s*((?:\d+[.)]?)(?:\.\d+[.)]?){0,5})\s+(.{2,})(?:\*\*)?$"
)
TABLE_ROW_RE = re.compile(r"^\s*\|.*\|\s*$")
TABLE_SEPARATOR_RE = re.compile(r"^\s*\|(?:\s*:?-{2,}:?\s*\|)+\s*$")
LIST_RE = re.compile(r"^\s*(?:[-*+]\s+|\d+[.)]\s+)")


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def digest(value: Any) -> str:
    raw = value if isinstance(value, bytes) else canonical_bytes(value)
    return hashlib.sha256(raw).hexdigest()


def file_sha(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def normalize(value: str) -> str:
    value = unicodedata.normalize("NFC", str(value or "")).casefold()
    value = value.replace("ё", "е")
    value = re.sub(r"[*_`#]", "", value)
    value = re.sub(r"[^\w\d]+", " ", value, flags=re.UNICODE)
    return " ".join(value.split())


def normalize_table_title(value: str) -> str:
    key = normalize(value)
    # Continuation markers describe pagination, not table identity.
    key = re.sub(r"^(?:продолжение|окончание)\s+(?:таблиц[аыи]?\s*)?", "", key).strip()
    key = re.sub(r"^таблиц[аыи]?(?:\s+номер\s*\d+|\s+\d+)?\s*", "", key).strip()
    key = re.sub(r"\s+(?:продолжение|окончание)$", "", key).strip()
    return key


def strip_markup(value: str) -> str:
    value = re.sub(r"^#{1,6}\s+", "", value.strip())
    value = re.sub(r"^(?:\*\*|__)(.*)(?:\*\*|__)$", r"\1", value)
    return value.strip()


def semantic_key(namespace: str, payload: dict) -> str:
    encoded = {
        "namespace": namespace,
        "producer_version": PRODUCER_VERSION,
        "payload": payload,
    }
    return f"{namespace}_v1_{digest(encoded)[:32]}"


def rect_area(coords: list[float] | None) -> float:
    if not coords or len(coords) != 4:
        return 0.0
    x0, y0, x1, y1 = coords
    return max(0.0, x1 - x0) * max(0.0, y1 - y0)


def parse_stamp(text: str) -> dict:
    out = {}
    for item in text.split("|"):
        if ":" not in item:
            continue
        key, value = item.split(":", 1)
        key = normalize(key)
        if key in {"code", "stage", "sheet", "object", "name", "organization", "revisions"}:
            out[key] = value.strip()
    return out


@dataclass
class SourceLine:
    text: str
    page: int
    block_id: str
    block_type: str
    markdown_line: int
    within_block_line: int

    def ref(self) -> dict:
        return {
            "page": self.page,
            "block_id": self.block_id,
            "block_type": self.block_type,
            "markdown_line": self.markdown_line,
            "within_block_line": self.within_block_line,
            "line_sha256": hashlib.sha256(self.text.encode("utf-8")).hexdigest(),
        }


@dataclass
class ParsedBlock:
    page: int
    block_id: str
    block_type: str
    ordinal: str
    header_line: int
    stamp: dict = field(default_factory=dict)
    lines: list[SourceLine] = field(default_factory=list)


@dataclass
class ParsedPage:
    number: int
    blocks: list[ParsedBlock] = field(default_factory=list)

    @property
    def content_lines(self) -> list[SourceLine]:
        return [line for block in self.blocks for line in block.lines if line.text.strip()]


def parse_markdown(path: Path) -> tuple[dict[int, ParsedPage], dict]:
    """Parse deterministic page/block boundaries from the exported Markdown."""
    raw = path.read_text(encoding="utf-8", errors="replace")
    pages: dict[int, ParsedPage] = {}
    document_stamp: dict = {}
    page: ParsedPage | None = None
    block: ParsedBlock | None = None
    content_started = False
    block_line = 0
    for line_no, line in enumerate(raw.splitlines(), 1):
        match = PAGE_RE.match(line)
        if match:
            page = pages.setdefault(int(match.group(1)), ParsedPage(int(match.group(1))))
            block = None
            continue
        match = BLOCK_RE.match(line)
        if match and page:
            block = ParsedBlock(page.number, match.group(3), match.group(2).lower(), match.group(1), line_no)
            page.blocks.append(block)
            content_started = False
            block_line = 0
            continue
        if line.startswith("**Stamp:**") and page is None:
            document_stamp = parse_stamp(line.split("**Stamp:**", 1)[1])
            continue
        if block is None:
            continue
        if line.startswith("> **Stamp:**"):
            block.stamp = parse_stamp(line.split("**Stamp:**", 1)[1])
            continue
        if line.startswith("> **Created:**") or line.startswith("> **Crop:**"):
            continue
        if line.startswith(">") and not content_started:
            continue
        if not line.strip() and not content_started:
            continue
        content_started = True
        block_line += 1
        block.lines.append(SourceLine(line, page.number, block.block_id, block.block_type, line_no, block_line))
    return pages, {"document_stamp": document_stamp, "markdown_sha256": file_sha(path), "bytes": path.stat().st_size}


def front_matter_types(page: ParsedPage) -> list[str]:
    lines = [normalize(strip_markup(x.text)) for x in page.content_lines]
    joined = "\n".join(lines)
    types = []
    if re.search(r"\bсодержание(?:\s+тома)?\b|\bоглавление\b", joined) or (
        re.search(r"\bсостав\s+(?:тома|раздела|проектной документации)\b", joined)
        and sum(TABLE_ROW_RE.match(x.text) is not None for x in page.content_lines) >= 3
    ):
        types.append("CONTENTS")
    if re.search(r"\bзаверени[ея]\s+(?:проектной|рабочей)\s+документации\b|\bудостоверяю\b", joined):
        types.append("CERTIFICATION")
    if re.search(r"\b(?:ведомость|таблица|регистрация)\s+(?:регистрации\s+)?изменен", joined) or re.search(r"\bразрешение\s+на\s+внесение\s+изменен", joined):
        types.append("REVISION_REGISTER")
    cover_signal = re.search(r"\b(?:проектная|рабочая)\s+документация\b", joined)
    cover_detail = re.search(r"\bтом\s+\d|\bраздел\s+\d|\bкнига\s+\d|\bглавн(?:ый|ого)\s+(?:инженер|архитектор)\b", joined)
    if page.number <= 3 and cover_signal and cover_detail and not any(x in types for x in ("CONTENTS", "REVISION_REGISTER")):
        types.append("COVER_OR_TITLE")
    return sorted(set(types))


def heading_info(line: SourceLine) -> dict | None:
    match = HEADING_RE.match(line.text.strip())
    if not match:
        return None
    title = strip_markup(match.group(2))
    numbered = NUMBERED_HEADING_RE.match(title)
    number = None
    if numbered:
        number = numbered.group(1).rstrip(".)")
        title = numbered.group(2).strip()
    if number:
        level = min(6, number.count(".") + 1)
    else:
        level = max(1, len(match.group(1)) - 2)
    return {"level": level, "markdown_level": len(match.group(1)), "number": number, "title": title, "raw": line.text}


def table_stats(page: ParsedPage) -> dict:
    lines = [x for x in page.content_lines if x.text.strip()]
    table = [x for x in lines if TABLE_ROW_RE.match(x.text)]
    return {
        "content_lines": len(lines),
        "table_lines": len(table),
        "table_line_ratio": round(len(table) / len(lines), 6) if lines else 0.0,
    }


def page_evidence(page: ParsedPage, raw_page: dict, raw_blocks: list[dict], neighbors: dict[int, ParsedPage]) -> dict:
    blocks = [b for b in raw_blocks if int(b.get("page_index", -1)) + 1 == page.number]
    counts = Counter(str(b.get("block_type", "unknown")) for b in blocks)
    areas = defaultdict(float)
    for b in blocks:
        areas[str(b.get("block_type", "unknown"))] += rect_area(b.get("coords_norm"))
    graphic = areas.get("image", 0.0) + areas.get("graphic", 0.0)
    total = sum(areas.values())
    graphic_ratio = graphic / total if total else 0.0
    width = int(raw_page.get("width_px") or 0)
    height = int(raw_page.get("height_px") or 0)
    aspect = width / height if height else 0.0
    stats = table_stats(page)
    headings = [heading_info(x) for x in page.content_lines]
    headings = [x for x in headings if x]
    texts = [strip_markup(x.text) for x in page.content_lines]
    normalized_text = "\n".join(normalize(x) for x in texts)
    terms = [term for term in GRAPHIC_LEAF_TERMS if term in normalized_text]
    stamps = [b.stamp for b in page.blocks if b.stamp]
    stamp = max(stamps, key=lambda x: sum(bool(v) for v in x.values()), default={})
    adjacent = []
    for number in (page.number - 1, page.number + 1):
        if number in neighbors:
            other = table_stats(neighbors[number])
            adjacent.append({"physical_page": number, **other, "front_matter_types": front_matter_types(neighbors[number])})
    return {
        "physical_page": page.number,
        "source_available": True,
        "front_matter_types": front_matter_types(page),
        "has_stamp_block": bool(counts.get("stamp")),
        "title_block": stamp,
        "page_geometry": {
            "width_px": width,
            "height_px": height,
            "rotation": int(raw_page.get("rotation") or 0),
            "aspect_ratio": round(aspect, 6),
            "landscape": aspect > THRESHOLDS["landscape_ratio"],
        },
        "block_composition": {
            "counts": dict(sorted(counts.items())),
            "normalized_rectangle_area": {k: round(v, 6) for k, v in sorted(areas.items())},
            "graphic_area_ratio": round(graphic_ratio, 6),
        },
        "markdown_structure": {
            **stats,
            "headings": [x["title"] for x in headings],
            "first_content": texts[:3],
            "last_content": texts[-3:],
        },
        "graphic_leaf_terms": terms,
        "adjacent_page_signals": adjacent,
    }


def classify_evidence(evidence: dict) -> dict:
    """Frozen deterministic classifier. It cannot inspect expected labels."""
    front = evidence.get("front_matter_types") or []
    if front:
        return {
            "unit_type": "MIXED", "status": "DETERMINISTIC", "confidence": "HIGH",
            "reason_codes": ["FRONT_MATTER_EXCLUDED", *front],
        }
    geometry = evidence.get("page_geometry") or {}
    composition = evidence.get("block_composition") or {}
    graphic = float(composition.get("graphic_area_ratio") or 0.0)
    terms = evidence.get("graphic_leaf_terms") or []
    stamp = bool(evidence.get("has_stamp_block"))
    landscape = bool(geometry.get("landscape")) or float(geometry.get("aspect_ratio") or 0.0) > THRESHOLDS["landscape_ratio"]
    if stamp and (landscape or graphic >= THRESHOLDS["graphic_dominant_ratio"]):
        return {
            "unit_type": "SHEET", "status": "DETERMINISTIC", "confidence": "HIGH",
            "reason_codes": ["TITLE_BLOCK_BLOCK_PRESENT", "LANDSCAPE_GEOMETRY" if landscape else "GRAPHIC_DOMINANT"],
        }
    if landscape and graphic >= 0.25 and terms:
        return {
            "unit_type": "SHEET", "status": "DETERMINISTIC", "confidence": "MEDIUM",
            "reason_codes": ["LANDSCAPE_GRAPHIC_ENGINEERING_LEAF", "GRAPHIC_LEAF_TITLE_SIGNAL"],
        }
    if graphic >= THRESHOLDS["full_page_graphic_ratio"] and terms:
        return {
            "unit_type": "SHEET", "status": "DETERMINISTIC", "confidence": "MEDIUM",
            "reason_codes": ["FULL_PAGE_GRAPHIC", "GRAPHIC_LEAF_TITLE_SIGNAL"],
        }
    md = evidence.get("markdown_structure") or {}
    table_lines = int(md.get("table_lines") or 0)
    ratio = float(md.get("table_line_ratio") or 0.0)
    neighbor = any(
        int(x.get("table_lines") or 0) >= 3
        and float(x.get("table_line_ratio") or 0.0) >= THRESHOLDS["table_neighbor_ratio"]
        and not (x.get("front_matter_types") or [])
        for x in evidence.get("adjacent_page_signals") or []
    )
    if table_lines >= 3 and ratio >= THRESHOLDS["table_dense_ratio"] and neighbor:
        return {
            "unit_type": "TABLE", "status": "DETERMINISTIC", "confidence": "MEDIUM",
            "reason_codes": ["TABLE_DENSE", "ADJACENT_TABLE_CONTINUATION"],
        }
    if int(md.get("content_lines") or 0) >= 2:
        return {
            "unit_type": "TEXT_SECTION", "status": "DETERMINISTIC", "confidence": "MEDIUM",
            "reason_codes": ["ORDERED_NARRATIVE_OR_CALCULATION_BLOCKS", "PAGE_NOT_SHEET_OR_LOGICAL_TABLE"],
        }
    return {
        "unit_type": "UNKNOWN", "status": "UNKNOWN", "confidence": "LOW",
        "reason_codes": ["INSUFFICIENT_DETERMINISTIC_EVIDENCE", "FAIL_CLOSED"],
    }


def table_groups(page: ParsedPage) -> list[list[SourceLine]]:
    groups, current = [], []
    for line in page.content_lines:
        if TABLE_ROW_RE.match(line.text):
            current.append(line)
        elif current:
            if len(current) >= 2:
                groups.append(current)
            current = []
    if len(current) >= 2:
        groups.append(current)
    return groups


def parse_cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def normalize_header(value: str, ordinal: int) -> str:
    key = normalize(value)
    if key:
        return key
    if "№" in value:
        return "row_number"
    return f"column_{ordinal}"


def table_schema(group: list[SourceLine]) -> tuple[list[str], list[str], str]:
    rows = [parse_cells(x.text) for x in group]
    separator = next((i for i, line in enumerate(group) if TABLE_SEPARATOR_RE.match(line.text)), None)
    if separator is None or separator == 0:
        width = max((len(x) for x in rows), default=0)
        headers = [f"column_{i+1}" for i in range(width)]
        return headers, [normalize_header(x, i + 1) for i, x in enumerate(headers)], "PARTIAL"
    header_rows = rows[:separator]
    width = max((len(x) for x in header_rows), default=0)
    headers = []
    for idx in range(width):
        parts = [row[idx] for row in header_rows if idx < len(row) and row[idx].strip()]
        headers.append(" / ".join(parts) or f"column_{idx+1}")
    header_keys = [normalize_header(x, i + 1) for i, x in enumerate(headers)]
    status = "DETERMINISTIC" if len(header_rows) == 1 and all(header_keys) else "PARTIAL"
    return headers, header_keys, status


def nearest_title(page: ParsedPage, group: list[SourceLine], hierarchy: list[dict]) -> tuple[str, str]:
    first = group[0].markdown_line
    candidates = [x for x in page.content_lines if x.markdown_line < first and first - x.markdown_line <= 12]
    for line in reversed(candidates):
        info = heading_info(line)
        if info and re.search(r"таблиц|ведомост|экспликац|перечень|спецификац|расчет", normalize(info["title"])):
            return info["title"], "EXPLICIT_HEADING"
        clean = strip_markup(line.text)
        if 3 <= len(clean) <= 180 and re.search(r"таблиц|ведомост|экспликац|перечень|спецификац|расчет", normalize(clean)):
            return clean, "EXPLICIT_NEARBY_TEXT"
    if hierarchy:
        return hierarchy[-1]["title"], "SECTION_CONTAINER"
    return "", "MISSING"


def repeated_furniture(pages: dict[int, ParsedPage]) -> set[tuple[int, str, int]]:
    occurrences: dict[str, list[SourceLine]] = defaultdict(list)
    for page in pages.values():
        content = page.content_lines
        edge = content[:4] + content[-4:]
        for line in edge:
            key = normalize(strip_markup(line.text))
            if 8 <= len(key) <= 220:
                occurrences[key].append(line)
    threshold = max(3, int(len(pages) * 0.15))
    out = set()
    for lines in occurrences.values():
        if len({x.page for x in lines}) >= threshold:
            out.update((x.page, x.block_id, x.markdown_line) for x in lines)
    return out


def build_tables(document_version: str, pages: dict[int, ParsedPage], classifications: dict[int, dict]) -> tuple[list[dict], dict[int, list[dict]], list[dict]]:
    """Build logical tables. Adjacency is necessary but never sufficient."""
    segment_rows = []
    page_to_segments: dict[int, list[dict]] = defaultdict(list)
    hierarchy: list[dict] = []
    for number in sorted(pages):
        page = pages[number]
        if classifications[number]["unit_type"] == "SHEET":
            continue
        # Hierarchy context is reconstructed before each table group.
        for line in page.content_lines:
            info = heading_info(line)
            if info:
                hierarchy = [h for h in hierarchy if h["level"] < info["level"]]
                hierarchy.append(info)
        for group_index, group in enumerate(table_groups(page), 1):
            headers, header_keys, schema_status = table_schema(group)
            title, title_source = nearest_title(page, group, hierarchy)
            rows = [parse_cells(x.text) for x in group if not TABLE_SEPARATOR_RE.match(x.text)]
            data_rows = rows[1:] if schema_status == "DETERMINISTIC" else rows
            stats = table_stats(page)
            standalone = (
                classifications[number]["unit_type"] == "TABLE"
                or stats["table_line_ratio"] >= 0.55
                or title_source.startswith("EXPLICIT")
            )
            seg = {
                "page": number,
                "segment_index_on_page": group_index,
                "title": title,
                "title_source": title_source,
                "normalized_title": normalize_table_title(title),
                "container_path": [normalize(h["title"]) for h in hierarchy],
                "headers": headers,
                "header_keys": header_keys,
                "schema_status": schema_status,
                "header_present": any(TABLE_SEPARATOR_RE.match(x.text) for x in group),
                "column_count": len(headers),
                "group": group,
                "data_rows": data_rows,
                "standalone": standalone,
            }
            segment_rows.append(seg)
            page_to_segments[number].append(seg)

    logical: list[list[dict]] = []
    ambiguous = []
    for seg in segment_rows:
        merged = False
        if logical:
            previous = logical[-1][-1]
            adjacent = seg["page"] == previous["page"] + 1
            # A guarded header-less row continuation may inherit the previous
            # schema only when the semantic title/container already agrees.
            preliminary_title_equal = bool(seg["normalized_title"] and seg["normalized_title"] == previous["normalized_title"])
            preliminary_container_equal = bool(seg["container_path"] and seg["container_path"] == previous["container_path"])
            preliminary_semantic_guard = preliminary_title_equal or (
                preliminary_container_equal and previous["title_source"] != "MISSING" and seg["title_source"] != "MISSING"
            )
            if (
                adjacent and preliminary_semantic_guard and not seg["header_present"]
                and seg["schema_status"] == "PARTIAL" and previous["schema_status"] == "DETERMINISTIC"
                and seg["column_count"] == previous["column_count"]
            ):
                seg["headers"] = list(previous["headers"])
                seg["header_keys"] = list(previous["header_keys"])
                seg["schema_status"] = "DETERMINISTIC"
                seg["header_inherited_from_previous_segment"] = True
            schema_equal = seg["header_keys"] == previous["header_keys"] and bool(seg["header_keys"])
            title_equal = bool(seg["normalized_title"] and seg["normalized_title"] == previous["normalized_title"])
            container_equal = bool(seg["container_path"] and seg["container_path"] == previous["container_path"])
            semantic_guard = title_equal or (container_equal and previous["title_source"] != "MISSING" and seg["title_source"] != "MISSING")
            if adjacent and schema_equal and semantic_guard and seg["schema_status"] == previous["schema_status"] == "DETERMINISTIC":
                logical[-1].append(seg)
                merged = True
            elif adjacent and schema_equal and not semantic_guard:
                ambiguous.append({
                    "left_page": previous["page"], "right_page": seg["page"],
                    "reason_code": "HEADERS_MATCH_WITHOUT_SEMANTIC_CONTAINER",
                    "disposition": "NOT_MERGED_FAIL_CLOSED",
                })
            elif adjacent and semantic_guard and not schema_equal:
                ambiguous.append({
                    "left_page": previous["page"], "right_page": seg["page"],
                    "reason_code": "SEMANTIC_CONTAINER_WITH_INCOMPATIBLE_SCHEMA",
                    "disposition": "NOT_MERGED_FAIL_CLOSED",
                })
        if not merged:
            logical.append([seg])

    tables = []
    occurrence = Counter()
    for group in logical:
        first = group[0]
        context = first["container_path"]
        title_key = first["normalized_title"] or "untitled"
        schema_key = first["header_keys"]
        collision_key = json.dumps([title_key, schema_key, context], ensure_ascii=False, sort_keys=True)
        occurrence[collision_key] += 1
        table_key = semantic_key("table", {
            "document_version": document_version,
            "semantic_title": title_key,
            "schema": schema_key,
            "container_context": context,
            "semantic_occurrence": occurrence[collision_key],
        })
        row_sequence, cell_refs = [], []
        label_counts = Counter()
        raw_rows = []
        for segment_index, seg in enumerate(group, 1):
            source_data_lines = [x for x in seg["group"] if not TABLE_SEPARATOR_RE.match(x.text)]
            if seg["header_present"] and source_data_lines:
                source_data_lines = source_data_lines[1:]
            for source_line in source_data_lines:
                cells = parse_cells(source_line.text)
                raw_rows.append((segment_index, source_line, cells))
                if cells:
                    label_counts[normalize(cells[0])] += 1
        for ordinal, (segment_index, source_line, cells) in enumerate(raw_rows, 1):
            first_key = normalize(cells[0]) if cells else ""
            if first_key and re.fullmatch(r"\d+(?:[.,]\d+)?", first_key):
                row_key, key_type, stable = first_key, "EXPLICIT_ROW_NUMBER", True
            elif first_key and label_counts[first_key] == 1:
                row_key, key_type, stable = first_key, "STABLE_LABEL_COLUMN", True
            else:
                compound = " | ".join(normalize(x) for x in cells[:2] if normalize(x))
                if compound and sum(1 for _, _, other in raw_rows if " | ".join(normalize(x) for x in other[:2] if normalize(x)) == compound) == 1:
                    row_key, key_type, stable = compound, "SEMANTIC_COMPOUND_KEY", True
                else:
                    row_key, key_type, stable = f"row_{ordinal}", "POSITION_FALLBACK", False
            row_ref = {**source_line.ref(), "segment_index": segment_index, "row_ordinal": ordinal}
            row_sequence.append({"row_key": row_key, "key_type": key_type, "stable": stable, "cells": cells, "source_ref": row_ref})
            for column, value in enumerate(cells, 1):
                cell_refs.append({
                    "row_key": row_key,
                    "column_semantic_key": first["header_keys"][column - 1] if column <= len(first["header_keys"]) else f"column_{column}",
                    "value": value,
                    "page": source_line.page,
                    "block_id": source_line.block_id,
                    "markdown_line": source_line.markdown_line,
                    "segment_index": segment_index,
                    "column_ordinal": column,
                })
        pages_span = sorted({seg["page"] for seg in group})
        status = "PARTIAL" if any(seg["schema_status"] != "DETERMINISTIC" for seg in group) else "DETERMINISTIC"
        tables.append({
            "table_key": table_key,
            "document_version": document_version,
            "table_title": first["title"],
            "normalized_title": first["normalized_title"],
            "schema": {"headers": first["headers"], "normalized_headers": first["header_keys"], "status": status},
            "column_semantic_keys": first["header_keys"],
            "row_sequence": row_sequence,
            "physical_page_span": pages_span,
            "continuation_segments": [
                {
                    "segment_index": i,
                    "page": seg["page"],
                    "block_ids": sorted({x.block_id for x in seg["group"]}),
                    "markdown_line_span": [seg["group"][0].markdown_line, seg["group"][-1].markdown_line],
                    "repeated_header": i > 1 and seg["header_present"] and seg["header_keys"] == first["header_keys"],
                    "header_inherited_from_previous_segment": bool(seg.get("header_inherited_from_previous_segment")),
                    "title_source": seg["title_source"],
                }
                for i, seg in enumerate(group, 1)
            ],
            "cell_refs": cell_refs,
            "ownership": "STANDALONE" if any(seg["standalone"] for seg in group) else "SUBORDINATE_TO_TEXT_SECTION",
            "status": status,
            "reason_codes": ["LOGICAL_TABLE_MATERIALIZED", "SEMANTIC_CONTAINER_GUARD"] + (["MULTIPAGE_CONTINUATION"] if len(pages_span) > 1 else []),
            "provenance": {
                "source_pages": pages_span,
                "source_block_ids": sorted({x.block_id for seg in group for x in seg["group"]}),
                "source_kind": "MARKDOWN_PRIMARY",
            },
            "producer_version": PRODUCER_VERSION,
        })
    return tables, page_to_segments, ambiguous


def build_sections(document_version: str, pages: dict[int, ParsedPage], classifications: dict[int, dict], tables: list[dict]) -> tuple[list[dict], dict]:
    table_lines = {
        (cell["page"], cell["block_id"], cell["markdown_line"])
        for table in tables if table["ownership"] == "STANDALONE"
        for cell in table["cell_refs"]
    }
    # Include header and separator lines of standalone tables as excluded table evidence.
    for table in tables:
        if table["ownership"] == "STANDALONE":
            for seg in table["continuation_segments"]:
                page = pages[seg["page"]]
                lo, hi = seg["markdown_line_span"]
                table_lines.update((x.page, x.block_id, x.markdown_line) for x in page.content_lines if lo <= x.markdown_line <= hi)
    furniture = repeated_furniture(pages)
    hierarchy: list[dict] = []
    current: dict | None = None
    drafts = []
    orphan_lines = []

    def open_section(info: dict | None, line: SourceLine | None, synthetic: str | None = None):
        nonlocal current, hierarchy
        if info:
            hierarchy = [h for h in hierarchy if h["level"] < info["level"]]
            node = {"level": info["level"], "number": info["number"], "title": info["title"], "normalized_title": normalize(info["title"])}
            hierarchy.append(node)
            title, number, explicit = info["title"], info["number"], True
        else:
            title, number, explicit = synthetic or "Document narrative", None, False
            if not hierarchy:
                hierarchy = [{"level": 1, "number": None, "title": title, "normalized_title": normalize(title)}]
        current = {
            "section_hierarchy": [dict(x) for x in hierarchy],
            "section_number": number,
            "section_title": title,
            "normalized_title": normalize(title),
            "explicit_heading": explicit,
            "heading_ref": line.ref() if line else None,
            "fragments": [],
            "table_references": [],
        }
        drafts.append(current)
        if line:
            current["fragments"].append({"kind": "HEADING", "text": line.text, "source_ref": line.ref()})

    table_by_page = defaultdict(list)
    for table in tables:
        for page in table["physical_page_span"]:
            table_by_page[page].append(table)

    for number in sorted(pages):
        unit = classifications[number]["unit_type"]
        if unit in {"SHEET", "UNKNOWN"} or (unit == "MIXED" and not any(table_by_page[number])):
            current = None
            hierarchy = []
            continue
        for line in pages[number].content_lines:
            locator = (line.page, line.block_id, line.markdown_line)
            if locator in furniture:
                continue
            if locator in table_lines:
                if current:
                    for table in table_by_page[number]:
                        if table["table_key"] not in current["table_references"]:
                            current["table_references"].append(table["table_key"])
                continue
            info = heading_info(line)
            if info:
                open_section(info, line)
                continue
            clean = line.text.strip()
            if not clean:
                continue
            if current is None:
                # A synthetic semantic container owns heading-less narrative; page is provenance only.
                open_section(None, None, "Unheaded document narrative")
                orphan_lines.append(line.ref())
            kind = "LIST_ITEM" if LIST_RE.match(clean) else "PARAGRAPH"
            current["fragments"].append({"kind": kind, "text": line.text, "source_ref": line.ref()})

    occurrence = Counter()
    sections = []
    for draft in drafts:
        if not draft["fragments"]:
            continue
        path = [x["normalized_title"] for x in draft["section_hierarchy"]]
        collision = json.dumps(path, ensure_ascii=False)
        occurrence[collision] += 1
        key = semantic_key("text_section", {
            "document_version": document_version,
            "normalized_heading_path": path,
            "semantic_sibling_occurrence": occurrence[collision],
        })
        refs = [x["source_ref"] for x in draft["fragments"]]
        pages_span = sorted({x["page"] for x in refs})
        parent = draft["section_hierarchy"][-2] if len(draft["section_hierarchy"]) > 1 else None
        sections.append({
            "text_section_key": key,
            "document_version": document_version,
            "section_hierarchy": draft["section_hierarchy"],
            "section_number": draft["section_number"],
            "section_title": draft["section_title"],
            "normalized_title": draft["normalized_title"],
            "parent_section": parent,
            "ordered_text_blocks": draft["fragments"],
            "table_references": draft["table_references"],
            "physical_page_span": pages_span,
            "fragment_refs": refs,
            "native_fallback_refs": [],
            "status": "DETERMINISTIC" if draft["explicit_heading"] else "PARTIAL",
            "reason_codes": ["EXPLICIT_HEADING_HIERARCHY" if draft["explicit_heading"] else "UNHEADED_NARRATIVE_CONTAINER"] + (["CROSSES_PHYSICAL_PAGE_BOUNDARY"] if len(pages_span) > 1 else []),
            "provenance": {
                "source_pages": pages_span,
                "source_block_ids": sorted({x["block_id"] for x in refs}),
                "source_kind": "MARKDOWN_PRIMARY",
                "heading_ref": draft["heading_ref"],
            },
            "producer_version": PRODUCER_VERSION,
        })

    all_fragment_lines = [normalize(x["text"]) for s in sections for x in s["ordered_text_blocks"] if x["kind"] != "HEADING" and normalize(x["text"])]
    counts = Counter(all_fragment_lines)
    quality = {
        "furniture_lines_excluded": len(furniture),
        "orphan_paragraphs": len(orphan_lines),
        "orphan_refs": orphan_lines,
        "duplicate_paragraphs": sum(v - 1 for v in counts.values() if v > 1),
    }
    return sections, quality


def sheet_identity(document_version: str, page: ParsedPage, evidence: dict) -> str:
    title = evidence.get("title_block") or {}
    payload = {
        "document_version": document_version,
        "code": normalize(title.get("code", "")),
        "sheet_number": normalize(title.get("sheet", "")),
        "sheet_name": normalize(title.get("name", "") or title.get("object", "")),
    }
    if not any(payload[k] for k in ("code", "sheet_number", "sheet_name")):
        payload["evidence_digest"] = digest([x.text for x in page.content_lines])
    return semantic_key("sheet", payload)


def materialize_document(document: dict) -> dict:
    md_path = Path(document["artifacts"]["work_md"]["path"])
    blocks_path = Path(document["artifacts"]["blocks"]["path"])
    pages, md_meta = parse_markdown(md_path)
    raw = json.loads(blocks_path.read_text(encoding="utf-8"))
    raw_pages = {int(x["page_index"]) + 1: x for x in raw.get("pages", [])}
    # Keep pages found in either source. Missing Markdown becomes UNKNOWN.
    for number in raw_pages:
        pages.setdefault(number, ParsedPage(number))
    evidence, classifications = {}, {}
    for number in sorted(pages):
        ev = page_evidence(pages[number], raw_pages.get(number, {}), raw.get("blocks", []), pages)
        evidence[number] = ev
        classifications[number] = classify_evidence(ev)

    tables, page_table_segments, ambiguous = build_tables(document["document_version"], pages, classifications)
    sections, section_quality = build_sections(document["document_version"], pages, classifications, tables)
    sections_by_page = defaultdict(list)
    for section in sections:
        for page in section["physical_page_span"]:
            sections_by_page[page].append(section["text_section_key"])
    tables_by_page = defaultdict(list)
    for table in tables:
        if table["ownership"] == "STANDALONE":
            for page in table["physical_page_span"]:
                tables_by_page[page].append(table["table_key"])

    page_records, leaf_units = [], []
    for number in sorted(pages):
        cls = classifications[number]
        child_ids = sorted(set(sections_by_page[number] + tables_by_page[number]))
        effective_type = "MIXED" if sections_by_page[number] and tables_by_page[number] else cls["unit_type"]
        page_record = {
            "unit_id": semantic_key("page_route", {"document_version": document["document_version"], "source_page": number}),
            "unit_type": effective_type,
            "page_classifier_type": cls["unit_type"],
            "scope": "PHYSICAL_PAGE_ROUTING_SURFACE",
            "source_pages": [number],
            "blocks": [b.block_id for b in pages[number].blocks],
            "child_unit_ids": child_ids,
            "confidence": cls["confidence"],
            "status": cls["status"],
            "reason_codes": cls["reason_codes"] + (["MULTIPLE_LEGITIMATE_SEMANTIC_CHILDREN"] if effective_type == "MIXED" and cls["unit_type"] != "MIXED" else []),
            "evidence_refs": {
                "markdown": str(md_path), "blocks": str(blocks_path), "physical_page": number,
                "evidence_digest": digest(evidence[number]),
            },
            "page_document_provenance": {
                "document_code": document["document_code"], "document_version": document["document_version"],
                "version_id": document["version_id"], "physical_page": number,
            },
            "producer_version": PRODUCER_VERSION,
        }
        page_records.append(page_record)
        if cls["unit_type"] == "SHEET":
            leaf_units.append({
                "unit_id": sheet_identity(document["document_version"], pages[number], evidence[number]),
                "unit_type": "SHEET", "scope": "SHEET_BOUNDED_ARTIFACT", "source_pages": [number],
                "blocks": page_record["blocks"], "confidence": cls["confidence"], "status": cls["status"],
                "reason_codes": cls["reason_codes"], "evidence": evidence[number],
                "provenance": page_record["page_document_provenance"], "producer_version": PRODUCER_VERSION,
            })
        elif cls["unit_type"] in {"MIXED", "UNKNOWN"} and not child_ids:
            leaf_units.append({
                "unit_id": semantic_key(cls["unit_type"].lower(), {"document_version": document["document_version"], "source_page": number}),
                "unit_type": cls["unit_type"], "scope": "REVIEW_OR_FAIL_CLOSED", "source_pages": [number],
                "blocks": page_record["blocks"], "confidence": cls["confidence"], "status": cls["status"],
                "reason_codes": cls["reason_codes"], "evidence": evidence[number],
                "provenance": page_record["page_document_provenance"], "producer_version": PRODUCER_VERSION,
            })
    leaf_units.extend({
        "unit_id": s["text_section_key"], "unit_type": "TEXT_SECTION", "scope": "SEMANTIC_SECTION",
        "source_pages": s["physical_page_span"], "blocks": s["provenance"]["source_block_ids"],
        "confidence": "MEDIUM" if s["status"] == "DETERMINISTIC" else "LOW", "status": s["status"],
        "reason_codes": s["reason_codes"], "evidence": {"section_hierarchy": s["section_hierarchy"]},
        "provenance": s["provenance"], "producer_version": PRODUCER_VERSION,
    } for s in sections)
    leaf_units.extend({
        "unit_id": t["table_key"], "unit_type": "TABLE", "scope": "LOGICAL_TABLE",
        "source_pages": t["physical_page_span"], "blocks": t["provenance"]["source_block_ids"],
        "confidence": "MEDIUM" if t["status"] == "DETERMINISTIC" else "LOW", "status": t["status"],
        "reason_codes": t["reason_codes"], "evidence": {"title": t["table_title"], "schema": t["schema"], "ownership": t["ownership"]},
        "provenance": t["provenance"], "producer_version": PRODUCER_VERSION,
    } for t in tables if t["ownership"] == "STANDALONE")

    source_text_blocks = {
        b.block_id for page in pages.values() for b in page.blocks
        if b.block_type == "text" and b.lines and classifications[page.number]["unit_type"] not in {"SHEET", "MIXED", "UNKNOWN"}
    }
    represented_blocks = {ref["block_id"] for s in sections for ref in s["fragment_refs"]}
    table_blocks = {x for t in tables for x in t["provenance"]["source_block_ids"]}
    missing_blocks = sorted(source_text_blocks - represented_blocks - table_blocks)
    quality = {
        **section_quality,
        "source_text_blocks_in_materializable_pages": len(source_text_blocks),
        "represented_text_blocks": len(represented_blocks | table_blocks),
        "missing_blocks": missing_blocks,
        "ambiguous_table_continuations": ambiguous,
    }
    return {
        "document": document,
        "markdown_meta": md_meta,
        "page_evidence": evidence,
        "page_classifications": classifications,
        "comparison_units": {
            "schema": SCHEMA_VERSIONS["comparison_units"], "producer_version": PRODUCER_VERSION,
            "document_version": document["document_version"], "source": document["source_refs"],
            "page_routing": page_records, "units": sorted(leaf_units, key=lambda x: x["unit_id"]),
        },
        "text_sections": {
            "schema": SCHEMA_VERSIONS["text_sections"], "producer_version": PRODUCER_VERSION,
            "document_version": document["document_version"], "sections": sorted(sections, key=lambda x: x["text_section_key"]),
        },
        "table_identities": {
            "schema": SCHEMA_VERSIONS["table_identities"], "producer_version": PRODUCER_VERSION,
            "document_version": document["document_version"], "tables": sorted(tables, key=lambda x: x["table_key"]),
        },
        "quality": quality,
    }


def summary_stats(values: Iterable[int]) -> dict:
    values = list(values)
    if not values:
        return {"count": 0, "mean": None, "median": None}
    return {"count": len(values), "mean": round(statistics.mean(values), 3), "median": statistics.median(values)}
