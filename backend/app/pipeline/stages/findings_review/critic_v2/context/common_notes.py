"""
common_notes.py
----------------
Retrieve general-notes / legend / common-instructions blocks from document.

Priority:
  1. Same sheet as the finding
  2. Same document (any page)
  3. Referenced sheet (if cross-reference found)

Read-only. Never modifies artifacts.
"""
from __future__ import annotations

import re

from .context_models import BlockSnippet

# Keywords identifying general-notes / legend blocks
_NOTES_PATTERNS = [
    r"общие\s+указани",
    r"общие\s+положения",
    r"примечани",
    r"условные\s+обозначени",
    r"легенда",
    r"general\s+notes?",
    r"legend",
    r"notes?:",
    r"примечание\s*[:\.\d]",
    r"^\s*примечание\s*$",
    r"перечень\s+нормативных",
    r"нормативные\s+ссылки",
    r"применяемые\s+нормы",
    r"перечень\s+документов",
]

_NOTES_RE = re.compile("|".join(_NOTES_PATTERNS), re.IGNORECASE)

# Minimum text length for a block to be considered as general notes
_MIN_NOTES_LEN = 100
# Maximum number of blocks to return
_MAX_NOTES_BLOCKS = 4


def _is_notes_block(block: dict, text: str) -> bool:
    """Return True if this block looks like general notes / legend."""
    if len(text) < _MIN_NOTES_LEN:
        return False
    label = str(block.get("label") or block.get("ocr_label") or "").lower()
    if _NOTES_RE.search(label):
        return True
    # Check first 200 chars of text for section heading patterns
    head = text[:200]
    if _NOTES_RE.search(head):
        return True
    return False


def get_common_notes(
    finding: dict,
    document_graph: dict,
    max_blocks: int = _MAX_NOTES_BLOCKS,
) -> list[BlockSnippet]:
    """
    Find general-notes / legend blocks most relevant to this finding.

    Args:
        finding: raw finding dict
        document_graph: parsed document_graph.json
        max_blocks: max notes blocks to return

    Returns:
        List of BlockSnippet, same-sheet blocks first
    """
    # Determine finding's sheet/page
    finding_sheet = str(finding.get("sheet") or "").lower()
    finding_pages: set[int] = set()
    fp = finding.get("page")
    if isinstance(fp, (int, float)):
        finding_pages.add(int(fp))
    elif isinstance(fp, list):
        for p in fp:
            try:
                finding_pages.add(int(p))
            except (TypeError, ValueError):
                pass

    same_sheet_results: list[BlockSnippet] = []
    other_results: list[BlockSnippet] = []
    seen_ids: set[str] = set()

    pages = document_graph.get("pages", [])
    for pg in pages:
        pnum = pg.get("page") or pg.get("page_index")
        try:
            pnum = int(pnum)
        except (TypeError, ValueError):
            continue

        page_sheet = str(
            pg.get("sheet_no_normalized") or pg.get("sheet_name") or ""
        ).lower()
        is_same_sheet = (
            pnum in finding_pages
            or (finding_sheet and finding_sheet in page_sheet)
            or (page_sheet and page_sheet in finding_sheet)
        )

        text_blocks = pg.get("text_blocks") or []
        for block in text_blocks:
            bid = str(block.get("id") or block.get("block_id") or "")
            if not bid or bid in seen_ids:
                continue
            text = (block.get("text") or "").strip()
            if _is_notes_block(block, text):
                label = str(block.get("label") or "").strip()
                snippet = BlockSnippet(
                    block_id=bid,
                    page=pnum,
                    text=text,
                    block_type="text",
                    sheet=page_sheet,
                    label=label,
                )
                seen_ids.add(bid)
                if is_same_sheet:
                    same_sheet_results.append(snippet)
                else:
                    other_results.append(snippet)

    # Combine: same-sheet first, then other pages (prefer early pages → General Notes on p1)
    other_results.sort(key=lambda b: b.page)
    combined = same_sheet_results + other_results
    return combined[:max_blocks]
