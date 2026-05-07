"""
cross_references.py
--------------------
Extract cross-references from evidence blocks and resolve them to actual block text.

Patterns detected:
  - "см. лист N" / "see leaf N"
  - "узел N-N" / "detail N-N"
  - "разрез N-N" / "section N-N"
  - "лист N" standalone
  - "ведомость деталей"
  - "комплект КЖ..."
  - "спецификация"

Read-only. Never modifies artifacts.
"""
from __future__ import annotations

import re
from typing import Optional

from .context_models import CrossReference

# Patterns that identify cross-references in block text
_XREF_PATTERNS = [
    re.compile(r"(?:см\.?|see)\s+(?:лист|leaf|л\.?)\s*(\d+[-–\d]*)", re.IGNORECASE),
    re.compile(r"(?:узел|detail|узл\.?)\s+([\w\d][-–\w\d]+)", re.IGNORECASE),
    re.compile(r"(?:разрез|section)\s+([\w\d][-–\w\d]+)", re.IGNORECASE),
    re.compile(r"(?:лист|leaf|л\.)\s*(\d+)\b", re.IGNORECASE),
    re.compile(r"комплект\s+(КЖ[\w\d\./-]+)", re.IGNORECASE),
    re.compile(r"(?:ведомость|schedule|statement)\s+деталей", re.IGNORECASE),
    re.compile(r"спецификац\w+", re.IGNORECASE),
    re.compile(r"(?:по\s+)?(?:ГОСТ|СП|СНиП)\s+[\d\w\.-]+", re.IGNORECASE),
]

_MAX_XREFS = 6
_MAX_XREF_TEXT_LEN = 200


def _extract_refs_from_text(text: str) -> list[tuple[str, str, str]]:
    """
    Return list of (ref_text, target_type, target_id) tuples.
    target_type: "sheet" | "detail" | "section" | "document" | "spec"
    """
    refs = []
    seen = set()
    for pat in _XREF_PATTERNS:
        for m in pat.finditer(text):
            ref_text = m.group(0).strip()
            if ref_text in seen or len(ref_text) < 3:
                continue
            seen.add(ref_text)
            # Classify
            rt = ref_text.lower()
            if any(w in rt for w in ("лист", "leaf", "л.")):
                ttype = "sheet"
            elif any(w in rt for w in ("узел", "detail")):
                ttype = "detail"
            elif any(w in rt for w in ("разрез", "section")):
                ttype = "section"
            elif any(w in rt for w in ("спецификац", "ведомость", "schedule")):
                ttype = "spec"
            elif any(w in rt for w in ("комплект", "кж")):
                ttype = "document"
            else:
                ttype = "other"
            target_id = m.group(1) if m.lastindex and m.lastindex >= 1 else ""
            refs.append((ref_text, ttype, target_id))
    return refs[:_MAX_XREFS]


def _find_resolved_block(
    target_type: str,
    target_id: str,
    document_graph: dict,
) -> Optional[str]:
    """
    Try to find the block in document_graph that corresponds to this cross-reference.
    Returns first 200 chars of the resolved block text, or None.
    """
    if not target_id:
        return None

    pages = document_graph.get("pages", [])
    target_lower = target_id.lower().strip()

    for pg in pages:
        # Match sheet references by sheet number
        if target_type == "sheet":
            sheet_raw = str(pg.get("sheet_no_raw") or pg.get("sheet_no_normalized") or "")
            if target_lower in sheet_raw.lower():
                # Return text of first block on this page
                for block in (pg.get("text_blocks") or []):
                    text = (block.get("text") or "").strip()
                    if len(text) > 30:
                        return text[:_MAX_XREF_TEXT_LEN]

        # Match detail/section references in block text
        if target_type in ("detail", "section"):
            for block in (pg.get("text_blocks") or []) + (pg.get("image_blocks") or []):
                label = str(block.get("label") or "").lower()
                if target_lower in label:
                    text = block.get("text") or ""
                    if len(text.strip()) > 20:
                        return text.strip()[:_MAX_XREF_TEXT_LEN]

    return None


def get_cross_references(
    finding: dict,
    document_graph: dict,
    max_refs: int = _MAX_XREFS,
) -> list[CrossReference]:
    """
    Extract and partially resolve cross-references from finding's evidence blocks.

    Args:
        finding: raw finding dict
        document_graph: parsed document_graph.json
        max_refs: max cross-references to return

    Returns:
        List of CrossReference (resolved_text filled when target found)
    """
    # Build block text map
    block_texts: dict[str, str] = {}
    pages = document_graph.get("pages", [])
    for pg in pages:
        for block in (pg.get("text_blocks") or []) + (pg.get("image_blocks") or []):
            bid = str(block.get("id") or block.get("block_id") or "")
            text = (block.get("text") or "").strip()
            if bid and text:
                block_texts[bid] = text

    # Collect evidence block IDs
    evidence = finding.get("evidence") or []
    ev_ids = [
        str(e.get("block_id") or e.get("id") or "")
        for e in evidence
        if e.get("block_id") or e.get("id")
    ]
    ev_ids = [bid for bid in ev_ids if bid]

    # Also scan problem/description text
    problem_text = (
        (finding.get("problem") or "") + " " + (finding.get("description") or "")
    )

    all_refs: list[CrossReference] = []
    seen_refs: set[str] = set()

    # Scan evidence blocks + problem text
    texts_to_scan = [block_texts.get(bid, "") for bid in ev_ids] + [problem_text]
    for text in texts_to_scan:
        if not text:
            continue
        raw_refs = _extract_refs_from_text(text)
        for ref_text, ttype, target_id in raw_refs:
            if ref_text in seen_refs:
                continue
            seen_refs.add(ref_text)
            resolved = _find_resolved_block(ttype, target_id, document_graph)
            all_refs.append(CrossReference(
                ref_text=ref_text,
                target_sheet="" if ttype != "sheet" else target_id,
                target_leaf=target_id if ttype in ("detail", "section") else "",
                resolved_text=resolved or "",
            ))

    return all_refs[:max_refs]
