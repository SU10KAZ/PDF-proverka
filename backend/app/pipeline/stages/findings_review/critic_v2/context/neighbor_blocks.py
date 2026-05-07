"""
neighbor_blocks.py
-------------------
Retrieve neighboring blocks for a finding's evidence blocks.

Neighbors are:
- same-page blocks within ±N index positions (by block sequence on page)
- adjacent-page blocks (page ± 1) if evidence spans page boundary
- blocks with nearby coordinates (spatial proximity)

Read-only. Never modifies artifacts.
"""
from __future__ import annotations

import re
from typing import Optional

from .context_models import BlockSnippet

# Max vertical/horizontal distance in normalized coords to count as "neighbor"
_SPATIAL_PROXIMITY_THRESHOLD = 0.25


def _parse_coords(coords) -> Optional[list[float]]:
    """Parse coords_norm from various storage formats."""
    if not coords:
        return None
    if isinstance(coords, list) and len(coords) == 4:
        try:
            return [float(x) for x in coords]
        except (TypeError, ValueError):
            return None
    if isinstance(coords, str):
        nums = re.findall(r"[-+]?\d*\.?\d+", coords)
        if len(nums) == 4:
            return [float(x) for x in nums]
    return None


def _vertical_distance(c1: list[float], c2: list[float]) -> float:
    """Vertical centre-to-centre distance in normalized coords."""
    cy1 = (c1[1] + c1[3]) / 2
    cy2 = (c2[1] + c2[3]) / 2
    return abs(cy1 - cy2)


def _is_spatially_close(c1: Optional[list[float]], c2: Optional[list[float]]) -> bool:
    if c1 is None or c2 is None:
        return False
    return _vertical_distance(c1, c2) <= _SPATIAL_PROXIMITY_THRESHOLD


def _block_to_snippet(block: dict, page: int, rank: int = 0) -> Optional[BlockSnippet]:
    """Convert a raw block dict to BlockSnippet. Returns None if no useful text."""
    bid = block.get("id") or block.get("block_id") or ""
    if not bid:
        return None
    text = (
        block.get("text") or block.get("gemma_text") or block.get("summary") or ""
    ).strip()
    if not text or len(text) < 5:
        return None
    label = (
        block.get("label") or block.get("ocr_label") or block.get("sheet_type") or ""
    ).strip()
    coords = _parse_coords(block.get("coords_norm"))
    return BlockSnippet(
        block_id=bid,
        page=page,
        text=text,
        block_type=block.get("type", "text"),
        sheet=str(block.get("sheet") or block.get("sheet_no_normalized") or ""),
        label=label,
        coords_norm=coords,
        distance_rank=rank,
    )


def get_neighbor_blocks(
    finding: dict,
    document_graph: dict,
    max_neighbors: int = 6,
    index_window: int = 4,
) -> list[BlockSnippet]:
    """
    Retrieve blocks neighboring the finding's evidence blocks.

    Strategy:
    1. Collect evidence block IDs and their page numbers.
    2. For each evidence page, gather all blocks on that page (text + image).
    3. Find the index position of each evidence block in the page block list.
    4. Return blocks within ±index_window positions OR spatial proximity.
    5. Also include blocks from adjacent pages (page ± 1).
    6. Deduplicate; skip blocks already in evidence.

    Args:
        finding: raw finding dict with evidence field
        document_graph: parsed document_graph.json
        max_neighbors: cap on total neighbor blocks returned
        index_window: ±N block positions on same page to include

    Returns:
        List of BlockSnippet, ordered by distance_rank ascending
    """
    # Build evidence set
    evidence = finding.get("evidence") or []
    evidence_ids: set[str] = {
        str(e.get("block_id") or e.get("id") or "")
        for e in evidence
        if e.get("block_id") or e.get("id")
    }
    evidence_ids.update(str(bid) for bid in (finding.get("related_block_ids") or []))
    evidence_ids.discard("")

    # Also use source_block_ids
    evidence_ids.update(str(bid) for bid in (finding.get("source_block_ids") or []))

    # Map page → list of all blocks (text + image)
    page_blocks: dict[int, list[dict]] = {}
    pages = document_graph.get("pages", [])
    for pg in pages:
        pnum = pg.get("page") or pg.get("page_index")
        try:
            pnum = int(pnum)
        except (TypeError, ValueError):
            continue
        all_blocks = list(pg.get("text_blocks", []) or []) + list(pg.get("image_blocks", []) or [])
        page_blocks[pnum] = all_blocks

    # Determine evidence pages
    evidence_pages: set[int] = set()
    for e in evidence:
        pg = e.get("page")
        try:
            evidence_pages.add(int(pg))
        except (TypeError, ValueError):
            pass
    # Also look at finding page field
    finding_page = finding.get("page")
    if isinstance(finding_page, (int, float)):
        evidence_pages.add(int(finding_page))
    elif isinstance(finding_page, list):
        for p in finding_page:
            try:
                evidence_pages.add(int(p))
            except (TypeError, ValueError):
                pass

    # Build id→coords map for evidence blocks (for spatial matching)
    evidence_coords: dict[str, Optional[list[float]]] = {}
    for pg in pages:
        for block in (pg.get("text_blocks") or []) + (pg.get("image_blocks") or []):
            bid = str(block.get("id") or block.get("block_id") or "")
            if bid in evidence_ids:
                evidence_coords[bid] = _parse_coords(block.get("coords_norm"))

    neighbor_snippets: list[BlockSnippet] = []
    seen_ids: set[str] = set(evidence_ids)

    # Search pages: evidence pages + adjacent pages
    search_pages: set[int] = set()
    for p in evidence_pages:
        search_pages.update([p - 1, p, p + 1])
    search_pages = {p for p in search_pages if p in page_blocks}

    for search_page in sorted(search_pages):
        blocks = page_blocks[search_page]
        is_evidence_page = search_page in evidence_pages

        # Find index positions of evidence blocks on this page
        ev_indices: list[int] = []
        for i, b in enumerate(blocks):
            bid = str(b.get("id") or b.get("block_id") or "")
            if bid in evidence_ids:
                ev_indices.append(i)

        for i, block in enumerate(blocks):
            bid = str(block.get("id") or block.get("block_id") or "")
            if bid in seen_ids:
                continue

            # Compute distance rank
            rank = 999
            if ev_indices:
                # Index-based distance
                min_idx_dist = min(abs(i - ei) for ei in ev_indices)
                if min_idx_dist <= index_window:
                    rank = min_idx_dist
                else:
                    # Try spatial proximity
                    b_coords = _parse_coords(block.get("coords_norm"))
                    for ev_id, ev_coords in evidence_coords.items():
                        if _is_spatially_close(b_coords, ev_coords):
                            rank = index_window  # spatial match gets boundary rank
                            break
            elif not is_evidence_page:
                # Adjacent page with no evidence blocks: all blocks are candidates
                rank = index_window + 1

            if rank <= index_window or (not is_evidence_page and rank <= index_window + 2):
                snippet = _block_to_snippet(block, search_page, rank=rank)
                if snippet:
                    seen_ids.add(bid)
                    neighbor_snippets.append(snippet)

    # Sort by (page_distance, distance_rank) and cap
    def sort_key(s: BlockSnippet) -> tuple:
        page_dist = 0 if s.page in evidence_pages else 1
        return (page_dist, s.distance_rank, s.page)

    neighbor_snippets.sort(key=sort_key)
    return neighbor_snippets[:max_neighbors]
