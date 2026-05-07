"""
table_context.py
-----------------
Extract table context rows when a finding relates to tabular data.

Table context includes: header row, row before target, target row, row after.

Read-only. Never modifies artifacts.
"""
from __future__ import annotations

import re

from .context_models import TableContextRow

# Keywords suggesting a finding references table data
_TABLE_KEYWORDS = re.compile(
    r"таблиц[аеиу]|ведомост[ьи]|спецификаци[яи]|table|schedule|specification|"
    r"позиц[ияи]\s*[\d№]|позиция\s|строк[аеи]|column|ряд\s|row\s",
    re.IGNORECASE,
)

# Simple table row detector: 3+ pipe/tab/spaces-separated values, or rows with digit columns
_ROW_SEP_RE = re.compile(r"\t|  {3,}|\|")
_UNITS_RE = re.compile(r"кг|мм|м[²³]?|шт|п\.?\s?м|погонн|тонн|kg|mm|m[²³]?|pcs", re.IGNORECASE)


def _split_into_rows(text: str) -> list[str]:
    """Split block text into row candidates."""
    lines = text.splitlines()
    # Filter empty lines and very short noise
    return [ln.strip() for ln in lines if len(ln.strip()) >= 3]


def _looks_like_table_row(line: str) -> bool:
    """Heuristic: line has multiple spaced/tabbed columns or pipe separators."""
    return bool(_ROW_SEP_RE.search(line)) or line.count("  ") >= 2


def _parse_cells(line: str) -> list[str]:
    """Split a row line into cells."""
    if "|" in line:
        return [c.strip() for c in line.split("|") if c.strip()]
    # Split on 3+ spaces or tabs
    cells = re.split(r"\t|  {3,}", line)
    return [c.strip() for c in cells if c.strip()]


def _find_target_row_index(rows: list[str], finding: dict) -> int:
    """
    Find which row index is most relevant to the finding.
    Matches norm references, position numbers, or key values from problem/description.
    """
    problem_text = (
        (finding.get("problem") or "") + " " +
        (finding.get("description") or "")
    ).lower()

    # Try to find position numbers / spec item references
    pos_matches = re.findall(r"(?:поз|pos|позиц)[\.:\s]*(\d+)", problem_text, re.IGNORECASE)
    numbers = re.findall(r"\b(\d{1,4})\b", problem_text)

    best_idx = 0
    best_score = 0
    for i, row in enumerate(rows):
        row_lower = row.lower()
        score = 0
        for pos in pos_matches:
            if pos in row_lower:
                score += 5
        for num in numbers[:6]:
            if f" {num} " in f" {row_lower} " or row_lower.startswith(num):
                score += 2
        if score > best_score:
            best_score = score
            best_idx = i

    return best_idx


def get_table_context(
    finding: dict,
    document_graph: dict,
    blocks_analysis: Optional[dict] = None,
    max_rows: int = 6,
) -> list[TableContextRow]:
    """
    Extract table context rows relevant to this finding.

    Returns up to max_rows rows: header(s), preceding row, target row, following row.
    Returns empty list if finding does not appear to reference a table.

    Args:
        finding: raw finding dict
        document_graph: parsed document_graph.json
        blocks_analysis: optional parsed 02_blocks_analysis.json for richer labels
        max_rows: max rows to return

    Returns:
        List of TableContextRow
    """
    # Only extract table context if finding seems to reference table data
    problem_text = (
        (finding.get("problem") or "") + " " +
        (finding.get("description") or "")
    )
    if not _TABLE_KEYWORDS.search(problem_text):
        return []

    # Collect evidence block texts
    evidence = finding.get("evidence") or []
    evidence_ids: set[str] = {
        str(e.get("block_id") or e.get("id") or "")
        for e in evidence
        if e.get("block_id") or e.get("id")
    }

    # Build block text map
    block_texts: dict[str, tuple[str, int]] = {}  # id → (text, page)
    pages = document_graph.get("pages", [])
    for pg in pages:
        pnum = pg.get("page") or pg.get("page_index") or 0
        try:
            pnum = int(pnum)
        except (TypeError, ValueError):
            pnum = 0
        for block in (pg.get("text_blocks") or []) + (pg.get("image_blocks") or []):
            bid = str(block.get("id") or block.get("block_id") or "")
            text = (block.get("text") or "").strip()
            if bid and text:
                block_texts[bid] = (text, pnum)

    # Look through evidence blocks for table-like content
    for ev in evidence:
        bid = str(ev.get("block_id") or ev.get("id") or "")
        if bid not in block_texts:
            continue
        text, page = block_texts[bid]

        rows = _split_into_rows(text)
        table_rows = [r for r in rows if _looks_like_table_row(r)]

        if len(table_rows) < 2:
            continue

        # Find units row and header rows
        result: list[TableContextRow] = []
        units_rows = [r for r in table_rows if _UNITS_RE.search(r)]
        header_candidates = table_rows[:2]

        for hr in header_candidates[:1]:
            result.append(TableContextRow(
                row_type="header",
                cells=_parse_cells(hr),
                raw_text=hr,
            ))

        if units_rows:
            result.append(TableContextRow(
                row_type="units",
                cells=_parse_cells(units_rows[0]),
                raw_text=units_rows[0],
            ))

        target_idx = _find_target_row_index(table_rows, finding)

        if target_idx > 0:
            result.append(TableContextRow(
                row_type="prev",
                cells=_parse_cells(table_rows[target_idx - 1]),
                raw_text=table_rows[target_idx - 1],
            ))

        result.append(TableContextRow(
            row_type="target",
            cells=_parse_cells(table_rows[target_idx]),
            raw_text=table_rows[target_idx],
        ))

        if target_idx + 1 < len(table_rows):
            result.append(TableContextRow(
                row_type="next",
                cells=_parse_cells(table_rows[target_idx + 1]),
                raw_text=table_rows[target_idx + 1],
            ))

        return result[:max_rows]

    return []


# Fix missing Optional import
from typing import Optional
