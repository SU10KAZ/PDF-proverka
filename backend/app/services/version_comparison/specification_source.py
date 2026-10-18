"""Prepare authoritative ``results.md`` blocks for whole-spec comparison.

This module deliberately reads only the prepared Markdown.  In particular, it
does not consult the originating PDF or attempt to repair recognized text.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from backend.app.services.common import results_md


FIELDS = ("pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note")
IGNORE_REASONS = ("header", "separator", "section", "note", "empty")
_BLOCK_PREFIX_RE = re.compile(r"^###\s+BLOCK\b")
_PAGE_PREFIX_RE = re.compile(r"^##\s+Page\b")
_TITLE_HEADER_RE = re.compile(r"^(?:спецификация|ведомость|перечень)(?:\s|№|$)", re.IGNORECASE)
_HEADER_ALIASES = {
    "pos": ("поз", "позиция", "поз позиция", "п п", "номер позиции"),
    "name": ("наименование",),
    "mark": ("тип", "марка", "обозначение", "тип марка обозначение"),
    "code": ("код",),
    "supplier": ("поставщик", "завод изготовитель", "завод изготовитель поставщик", "поставщик завод изготовитель"),
    "unit": ("ед изм", "единица измерения", "единица изм"),
    "qty": ("количество", "кол", "кол во", "колво", "количество кол"),
    "mass": ("масса",),
    "note": ("примечание",),
}


def extraction_schema() -> dict[str, Any]:
    """Return the strict JSON schema for block-local extraction."""
    evidence = {
        "type": "object",
        "additionalProperties": False,
        "required": ["line", "quote"],
        "properties": {"line": {"type": "integer"}, "quote": {"type": "string", "minLength": 1}},
    }
    row = {
        "type": "object",
        "additionalProperties": False,
        "required": ["section", "parent", "fields", "evidence"],
        "properties": {
            "section": {"type": "string"},
            "parent": {"type": "string"},
            "fields": {
                "type": "object",
                "additionalProperties": False,
                "required": list(FIELDS),
                "properties": {field: {"type": "string"} for field in FIELDS},
            },
            "evidence": {"type": "array", "items": evidence, "minItems": 1},
        },
    }
    ignored = {
        "type": "object",
        "additionalProperties": False,
        "required": ["line", "reason"],
        "properties": {
            "line": {"type": "integer"},
            "reason": {"type": "string", "enum": list(IGNORE_REASONS)},
        },
    }
    block = {
        "type": "object",
        "additionalProperties": False,
        "required": ["block_id", "rows", "ignored", "issues"],
        "properties": {
            "block_id": {"type": "string"},
            "rows": {"type": "array", "items": row},
            "ignored": {"type": "array", "items": ignored},
            "issues": {"type": "array", "items": {"type": "string"}},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["blocks"],
        "properties": {"blocks": {"type": "array", "items": block}},
    }


def load_source(
    path: Path,
    *,
    label: str,
    pages: list[int] | None = None,
    block_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Load selected blocks, preserving original absolute Markdown line numbers.

    If neither selector is supplied, all parsed blocks define the scope. When
    both are supplied their intersection is selected.
    """
    raw = Path(path).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    issues: list[str] = []
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        text = raw.decode("utf-8", errors="replace")
        issues.append(f"Source is not valid UTF-8: {exc}")

    parsed = results_md.parse_results_md(text)
    raw_lines = text.splitlines()
    parsed_block_headers = {b.header_line for b in parsed.blocks}
    parsed_page_starts = {p.start_line for p in parsed.pages}
    for number, line in enumerate(raw_lines, 1):
        if _BLOCK_PREFIX_RE.match(line) and number not in parsed_block_headers:
            issues.append(f"Malformed or unparsed BLOCK header at line {number}")
        if _PAGE_PREFIX_RE.match(line) and number not in parsed_page_starts:
            issues.append(f"Malformed or unparsed PAGE header at line {number}")

    if not parsed.blocks:
        issues.append("No parseable blocks found")
    ids: dict[str, int] = {}
    for block in parsed.blocks:
        ids[block.block_id] = ids.get(block.block_id, 0) + 1
    for block_id, count in ids.items():
        if count > 1:
            issues.append(f"Duplicate block id in source: {block_id}")
    page_counts: dict[int, int] = {}
    for page in parsed.pages:
        page_counts[page.number] = page_counts.get(page.number, 0) + 1
    for page, count in page_counts.items():
        if count > 1:
            issues.append(f"Duplicate page header in source: {page}")
    if any(block.page <= 0 for block in parsed.blocks):
        issues.append("One or more blocks are outside a parsed page")

    wanted_pages = None
    if pages is not None:
        if not isinstance(pages, list) or any(type(page) is not int or page < 1 for page in pages):
            issues.append("Requested pages must be a list of positive integers")
            pages = []
        if len(pages) != len(set(pages)):
            issues.append("Requested pages contain duplicates")
        wanted_pages = set(pages)
    wanted_ids = None
    if block_ids is not None:
        if not isinstance(block_ids, list) or any(not isinstance(block_id, str) or not block_id for block_id in block_ids):
            issues.append("Requested block ids must be a list of nonempty strings")
            block_ids = []
        if len(block_ids) != len(set(block_ids)):
            issues.append("Requested block ids contain duplicates")
        wanted_ids = set(block_ids)
    if pages is not None:
        for page in sorted(wanted_pages or set()):
            if page not in page_counts:
                issues.append(f"Requested page not found: {page}")
            elif not any(block.page == page for block in parsed.blocks):
                issues.append(f"Requested page has no parsed blocks: {page}")
    if block_ids is not None:
        for block_id in block_ids:
            if block_id not in ids:
                issues.append(f"Requested block id not found: {block_id}")

    selected = [
        b for b in parsed.blocks
        if (wanted_pages is None or b.page in wanted_pages)
        and (wanted_ids is None or b.block_id in wanted_ids)
    ]
    if (pages is not None or block_ids is not None) and not selected:
        issues.append("The requested selection contains no blocks")

    # The canonical parser intentionally ignores text outside BLOCK bodies.
    # Fail closed on unparsed text in selected pages and across all-block scope.
    boundaries = sorted(
        [(b.header_line, "block", b) for b in parsed.blocks]
        + [(p.start_line, "page", p) for p in parsed.pages]
    )
    block_spans: list[tuple[int, int]] = []
    for idx, (start, kind, obj) in enumerate(boundaries):
        if kind != "block":
            continue
        end = boundaries[idx + 1][0] - 1 if idx + 1 < len(boundaries) else len(raw_lines)
        block_spans.append((start, end))

    if wanted_pages is not None:
        for page_index, page_obj in enumerate(parsed.pages):
            if page_obj.number not in wanted_pages:
                continue
            page_start = page_obj.start_line
            next_page = next((p.start_line for p in parsed.pages if p.start_line > page_start), len(raw_lines) + 1)
            content_lines = range(page_start + 1, next_page)
            block_lines = {line for start, end in block_spans for line in range(start, end + 1)}
            orphaned = [n for n in content_lines if n not in block_lines and raw_lines[n - 1].strip()]
            if orphaned:
                issues.append(f"Unparsed content on selected page {page_obj.number} at line(s): {', '.join(map(str, orphaned[:8]))}")
    elif pages is None and block_ids is None:
        for page_obj in parsed.pages:
            if not any(block.page == page_obj.number for block in parsed.blocks):
                issues.append(f"Page in all-block scope has no parsed blocks: {page_obj.number}")
        page_header_lines = {page.start_line for page in parsed.pages}
        block_line_set = {line for start, end in block_spans for line in range(start, end + 1)}
        allowed_preamble = re.compile(r"^(?:#\s+Document:|Path:|Generated:|\*\*Stamp:\*\*|---\s*$)")
        orphaned = [
            number for number, line in enumerate(raw_lines, 1)
            if line.strip() and number not in block_line_set and number not in page_header_lines
            and not allowed_preamble.match(line)
        ]
        if orphaned:
            issues.append(f"Unparsed content in all-block scope at line(s): {', '.join(map(str, orphaned[:8]))}")

    blocks: list[dict[str, Any]] = []
    for block in selected:
        end_line = block.body_start_line + len(block.body.splitlines())
        source_lines = raw_lines[block.body_start_line - 1 : end_line - 1]
        lines = [
            {"line": block.body_start_line + offset, "text": line}
            for offset, line in enumerate(source_lines)
            if line.strip()
        ]
        if not lines:
            issues.append(f"Selected block is empty: {block.block_id}")
        if block.block_type not in ("text", "table"):
            issues.append(f"Selected block has unsupported image/non-text evidence: {block.block_id} ({block.block_type})")
        blocks.append({
            "block_id": block.block_id,
            "page": block.page,
            "kind": block.block_type,
            "lines": lines,
            "sheet": block.sheet or "",
            "name": block.sheet_name or "",
        })

    return {
        "label": label,
        "document": parsed.document_name or Path(path).name,
        "source_sha256": digest,
        "scope": "selected" if pages is not None or block_ids is not None else "all blocks",
        "selection": {
            "pages": sorted({block["page"] for block in blocks}),
            "block_ids": [block["block_id"] for block in blocks],
            "mode": "pages" if pages is not None else ("block_ids" if block_ids is not None else "all_blocks"),
        },
        "complete": not issues,
        "issues": issues,
        "blocks": blocks,
    }


def _norm(value: str) -> str:
    return " ".join(value.split())


def _header_norm(value: str) -> str:
    value = value.lower().replace("ё", "е")
    value = re.sub(r"[^\w]+", " ", value, flags=re.UNICODE)
    return _norm(value)


def _pipe_cells(line: str) -> list[str] | None:
    stripped = line.strip()
    if "|" not in stripped:
        return None
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    return [cell.strip() for cell in stripped.split("|")]


def _table_header(cells: list[str] | None) -> dict[int, str] | None:
    if not cells:
        return None
    columns: dict[int, str] = {}
    for index, cell in enumerate(cells):
        normalized = _header_norm(cell)
        for field, aliases in _HEADER_ALIASES.items():
            if normalized in aliases:
                columns[index] = field
                break
    # A recognized header must map every nonempty column exactly once. Partial
    # mapping could silently discard an unrecognized but substantive field.
    mapped = set(columns.values())
    nonempty = {i for i, cell in enumerate(cells) if cell.strip()}
    if len(columns) == len(nonempty) and len(mapped) == len(columns) and len(columns) >= 2 and mapped.intersection({"pos", "name", "qty"}):
        return columns
    return None


def _separator_row(cells: list[str] | None) -> bool:
    return bool(cells) and all(re.fullmatch(r"[:\- =]+", cell or " ") for cell in cells)


def _plain_separator(line: str) -> bool:
    stripped = line.strip()
    return bool(re.fullmatch(r"(?:[-=*_]\s*){3,}", stripped))


def _section_norm(value: str) -> str:
    return _norm(re.sub(r"^[#>*`\s]+|[#>*`\s]+$", "", value))


def _contains_cell(value: str, cell: str, *, numeric_safe: bool = False) -> bool:
    value_n, cell_n = _norm(value), _norm(cell)
    if not cell_n:
        return True
    start = 0
    while True:
        found = value_n.find(cell_n, start)
        if found < 0:
            return False
        end = found + len(cell_n)
        numeric_neighbors = "0123456789.,/"
        left_char = value_n[found - 1] if found else ""
        right_char = value_n[end] if end < len(value_n) else ""
        left_ok = found == 0 or not (left_char.isalnum() or left_char == "_" or (numeric_safe and left_char in numeric_neighbors))
        right_ok = end == len(value_n) or not (right_char.isalnum() or right_char == "_" or (numeric_safe and right_char in numeric_neighbors))
        if not numeric_safe or (left_ok and right_ok):
            return True
        start = found + 1


def _has_pipe_row(line: str) -> bool:
    """A small fail-closed guard against hiding data rows as headers."""
    parts = [part.strip() for part in line.strip().strip("|").split("|")]
    if len(parts) < 3:
        return False
    has_number = any(re.search(r"\d", part) for part in parts)
    separator = all(re.fullmatch(r"[:\- =]+", part or " ") for part in parts)
    return has_number and not separator


def validate_extraction(source: dict[str, Any], payload: Any) -> dict[str, Any]:
    """Validate extraction against all blocks in one complete specification.

    Rows belong to the block containing their first cited line, while evidence
    may cite any block in the source. This permits exact, evidence-backed
    continuation across page and block boundaries.
    """
    issues: list[str] = list(source.get("issues", [])) if isinstance(source, dict) and isinstance(source.get("issues"), list) else []
    src_blocks = source.get("blocks") if isinstance(source, dict) else None
    if not isinstance(src_blocks, list):
        src_blocks = []
        issues.append("Source has no block list")
    if not isinstance(payload, dict) or not isinstance(payload.get("blocks"), list):
        issues.append("Extraction payload must contain a blocks array")
        payload_blocks: list[Any] = []
    else:
        payload_blocks = payload["blocks"]
        if set(payload) != {"blocks"}:
            issues.append("Extraction payload has unexpected top-level keys")

    expected_ids = [b.get("block_id") for b in src_blocks if isinstance(b, dict)]
    got_ids = [b.get("block_id") if isinstance(b, dict) else None for b in payload_blocks]
    valid_got_ids = [value for value in got_ids if isinstance(value, str)]
    if len(valid_got_ids) != len(got_ids):
        issues.append("Extraction block ids must be strings")
    if len(valid_got_ids) != len(set(valid_got_ids)):
        issues.append("Extraction contains duplicate block ids")
    if set(valid_got_ids) != set(expected_ids) or len(valid_got_ids) != len(expected_ids):
        issues.append("Extraction block ids do not exactly match the assigned source blocks")
    payload_by_id = {b.get("block_id"): b for b in payload_blocks if isinstance(b, dict) and isinstance(b.get("block_id"), str)}

    line_map: dict[int, tuple[dict[str, Any], str]] = {}
    for src_block in src_blocks:
        if not isinstance(src_block, dict):
            issues.append("Malformed source block")
            continue
        for line in src_block.get("lines", []):
            if not isinstance(line, dict) or not isinstance(line.get("line"), int) or not isinstance(line.get("text"), str):
                issues.append(f"Malformed source line in {src_block.get('block_id', '?')}")
                continue
            number = line["line"]
            if number in line_map:
                issues.append(f"Duplicate absolute source line number: {number}")
            line_map[number] = (src_block, line["text"])

    table_cells_by_line: dict[int, dict[str, str]] = {}
    table_header: dict[int, str] | None = None
    table_width = 0
    table_header_lines: set[int] = set()
    table_separator_lines: set[int] = set()
    ordered_lines = list(line_map.items())
    for line_index, (number, (_, text)) in enumerate(ordered_lines):
        cells = _pipe_cells(text)
        recognized = _table_header(cells)
        if recognized is not None:
            table_header = recognized
            table_width = len(cells or [])
            table_header_lines.add(number)
            continue
        next_cells = _pipe_cells(ordered_lines[line_index + 1][1][1]) if line_index + 1 < len(ordered_lines) else None
        if cells and _separator_row(next_cells):
            issues.append(f"Unrecognized Markdown table header at line {number}")
            table_header = None
            table_width = 0
            continue
        if table_header and cells:
            if len(cells) != table_width:
                issues.append(f"Markdown table row width differs from its header at line {number}")
                continue
            if _separator_row(cells):
                table_separator_lines.add(number)
                continue
            if any(cell.strip() for cell in cells):
                table_cells_by_line[number] = {
                    field: cells[index]
                    for index, field in table_header.items()
                    if index < len(cells)
                }

    all_text = " ".join(_norm(text) for _, text in line_map.values())
    classified: set[int] = set()
    evidence_claims: dict[int, set[str]] = {}
    ignored_claims: dict[int, list[tuple[str, str]]] = {}
    section_ignored_lines: set[int] = set()
    rows_out: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    for src_block in src_blocks:
        if not isinstance(src_block, dict):
            continue
        block_id = src_block.get("block_id")
        model_block = payload_by_id.get(block_id)
        if not isinstance(model_block, dict):
            continue
        if set(model_block) != {"block_id", "rows", "ignored", "issues"}:
            issues.append(f"Malformed extraction block shape: {block_id}")
        if not isinstance(model_block.get("issues"), list) or any(not isinstance(x, str) for x in model_block.get("issues", [])):
            issues.append(f"Malformed block issues: {block_id}")
        elif model_block["issues"]:
            issues.extend(f"{block_id}: {x}" for x in model_block["issues"])

        rows = model_block.get("rows")
        ignored = model_block.get("ignored")
        if not isinstance(rows, list) or not isinstance(ignored, list):
            issues.append(f"Malformed rows/ignored arrays: {block_id}")
            continue
        local_rows: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
        duplicate_signatures: set[tuple[Any, ...]] = set()
        for index, row in enumerate(rows):
            if not isinstance(row, dict) or set(row) != {"section", "parent", "fields", "evidence"}:
                issues.append(f"Malformed row {index + 1} in {block_id}")
                continue
            fields = row.get("fields")
            evidence = row.get("evidence")
            if not isinstance(fields, dict) or set(fields) != set(FIELDS) or any(not isinstance(fields.get(f), str) for f in FIELDS):
                issues.append(f"Malformed fields in row {index + 1} of {block_id}")
                continue
            if not any(fields.values()):
                issues.append(f"Empty extracted row in {block_id}, row {index + 1}")
            if not isinstance(evidence, list) or not evidence:
                issues.append(f"Row {index + 1} in {block_id} has no evidence")
                continue
            cites: list[dict[str, Any]] = []
            seen_lines: set[int] = set()
            for citation in evidence:
                if not isinstance(citation, dict) or set(citation) != {"line", "quote"}:
                    issues.append(f"Malformed evidence in row {index + 1} of {block_id}")
                    continue
                line_no, quote = citation.get("line"), citation.get("quote")
                if not isinstance(line_no, int) or not isinstance(quote, str) or not quote.strip() or line_no not in line_map:
                    issues.append(f"Evidence references an invalid source line in {block_id}")
                    continue
                if not quote or quote not in line_map[line_no][1]:
                    issues.append(f"Evidence quote is not an exact source substring in {block_id}, line {line_no}")
                    continue
                if line_no in seen_lines:
                    issues.append(f"Repeated evidence line in a row of {block_id}: {line_no}")
                    continue
                seen_lines.add(line_no)
                classified.add(line_no)
                evidence_claims.setdefault(line_no, set()).add(f"{block_id}:{index + 1}")
                if len(cites) == 0 and line_map[line_no][0].get("block_id") != block_id:
                    issues.append(f"Row in {block_id} begins with evidence from another block (continuation ownership mismatch)")
                cites.append({"line": line_no, "quote": quote})
            combined = _norm(" ".join(c["quote"] for c in cites))
            for field in FIELDS:
                value = fields[field]
                numeric_safe = field in ("pos", "code", "qty", "mass")
                grounded = _contains_cell(combined, value, numeric_safe=numeric_safe) if value else True
                if value and not grounded:
                    # Pipe-delimited table continuations may place the continued
                    # name in a later row/block. Ground against corresponding
                    # cells in evidence order, without intervening columns.
                    cell_parts = [table_cells_by_line[c["line"]].get(field, "") for c in cites if c["line"] in table_cells_by_line]
                    cell_grounding = _norm(" ".join(part for part in cell_parts if part))
                    if not cell_grounding or not _contains_cell(cell_grounding, value, numeric_safe=numeric_safe):
                        issues.append(f"Fabricated or ungrounded {field} in {block_id}, row {index + 1}")
            for context_key in ("section", "parent"):
                value = row.get(context_key)
                if not isinstance(value, str):
                    issues.append(f"Malformed {context_key} in {block_id}, row {index + 1}")
                elif value and _norm(value) not in all_text:
                    issues.append(f"Ungrounded {context_key} in {block_id}, row {index + 1}")
            signature = (tuple((f, _norm(fields[f])) for f in FIELDS), tuple(sorted(seen_lines)))
            if signature in duplicate_signatures:
                issues.append(f"Duplicate extraction in {block_id}, row {index + 1}")
            duplicate_signatures.add(signature)
            local_rows.append((row, cites))

        for item in ignored:
            if not isinstance(item, dict) or set(item) != {"line", "reason"}:
                issues.append(f"Malformed ignored-line entry in {block_id}")
                continue
            line_no, reason = item.get("line"), item.get("reason")
            if not isinstance(line_no, int) or line_no not in line_map or reason not in IGNORE_REASONS:
                issues.append(f"Invalid ignored-line entry in {block_id}")
                continue
            if line_map[line_no][0].get("block_id") != block_id:
                issues.append(f"Ignored line assigned to the wrong block in {block_id}: {line_no}")
            if reason == "empty":
                issues.append(f"A nonblank source line cannot be ignored as empty in {block_id}, line {line_no}")
            ignored_claims.setdefault(line_no, []).append((block_id, reason))
            classified.add(line_no)
            if reason == "note":
                issues.append(f"Substantive note may not be discarded in {block_id}, line {line_no}")
            if reason in ("header", "separator", "section") and _has_pipe_row(line_map[line_no][1]):
                issues.append(f"Data-like line hidden as {reason} in {block_id}, line {line_no}")
            ignored_text = line_map[line_no][1]
            if reason == "header" and line_no not in table_header_lines and not _TITLE_HEADER_RE.match(_section_norm(ignored_text).lower()):
                issues.append(f"Unrecognized line hidden as header in {block_id}, line {line_no}")
            if reason == "separator" and line_no not in table_separator_lines and not _plain_separator(ignored_text):
                issues.append(f"Non-separator line hidden as separator in {block_id}, line {line_no}")
            if reason == "section":
                section_ignored_lines.add(line_no)

        for index, (row, cites) in enumerate(local_rows, 1):
            grouped: dict[str, list[dict[str, Any]]] = {}
            for cite in cites:
                cited_block = line_map[cite["line"]][0]
                grouped.setdefault(cited_block["block_id"], []).append(cite)
            sources = []
            for cited_id, block_cites in grouped.items():
                cited_block = next(b for b in src_blocks if isinstance(b, dict) and b.get("block_id") == cited_id)
                for cite in block_cites:
                    sources.append({
                        "document": source.get("document", ""),
                        "page": cited_block.get("page"),
                        "block_id": cited_id,
                        "line_start": cite["line"],
                        "line_end": cite["line"],
                        "quote": cite["quote"],
                    })
            rows_out.append({
                "id": f"{block_id}:{index}",
                "section": row.get("section", ""),
                "parent": row.get("parent", ""),
                "fields": row.get("fields", {}),
                "sources": sources,
            })

    for line_no, claims in evidence_claims.items():
        if len(claims) > 1:
            issues.append(f"Source line claimed by multiple rows: {line_no}")
        if line_no in ignored_claims:
            issues.append(f"Source line classified as both evidence and ignored: {line_no}")
    for line_no, claims in ignored_claims.items():
        if len(claims) > 1:
            issues.append(f"Source line ignored more than once: {line_no}")

    row_context = {
        _section_norm(value).casefold()
        for row in rows_out
        for value in (row.get("section", ""), row.get("parent", ""))
        if isinstance(value, str) and _section_norm(value)
    }
    for line_no in section_ignored_lines:
        normalized = _section_norm(line_map[line_no][1]).casefold()
        if not normalized or normalized not in row_context:
            issues.append(f"Unconfirmed section line hidden as section at line {line_no}")

    for line_no, expected_cells in table_cells_by_line.items():
        if line_no not in evidence_claims:
            issues.append(f"Markdown table data line is not extracted as a row: {line_no}")
            continue
        for field, cell in expected_cells.items():
            if not cell:
                continue
            matching = [
                row for row in rows_out
                if any(source_ref["line_start"] == line_no for source_ref in row["sources"])
            ]
            numeric_safe = field in ("pos", "code", "qty", "mass")
            if not any(_contains_cell(row["fields"].get(field, ""), cell, numeric_safe=numeric_safe) for row in matching):
                issues.append(f"Markdown table cell is missing or altered in {field}, line {line_no}")

    missing = set(line_map) - classified
    if missing:
        issues.append(f"Unclassified source lines: {', '.join(map(str, sorted(missing)))}")
    for src_block in src_blocks:
        if not isinstance(src_block, dict):
            continue
        block_lines = {line["line"] for line in src_block.get("lines", []) if isinstance(line, dict) and isinstance(line.get("line"), int)}
        missing_here = sorted(block_lines - classified)
        coverage.append({
            "block_id": src_block.get("block_id"),
            "source_nonblank_lines": len(block_lines),
            "classified_lines": len(block_lines - set(missing_here)),
            "unclassified_lines": missing_here,
        })

    complete = bool(source.get("complete", False)) and not issues
    return {
        "label": source.get("label", ""),
        "document": source.get("document", ""),
        "source_sha256": source.get("source_sha256", ""),
        "scope": source.get("scope", ""),
        "selection": source.get("selection", {}),
        "complete": complete,
        "issues": issues,
        "rows": rows_out if complete else [],
        "source_coverage": coverage,
    }
