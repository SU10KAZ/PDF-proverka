"""Bounded review batches for meaningful pages left unmatched by Mapper."""
from __future__ import annotations

import re
from typing import Any


def _tokens(page: dict[str, Any]) -> set[str]:
    text = str(page.get("native_page_text") or "") + " " + " ".join(
        str(block.get("structured_md") or "") for block in page.get("blocks") or []
    )
    return {
        token for token in re.findall(r"[0-9A-Za-zА-Яа-яЁё_.-]+", text.casefold())
        if len(token) >= 3
    }


def _candidate_pages(
    primary: dict[str, Any],
    opposite: list[dict[str, Any]],
    *,
    limit: int,
) -> list[int]:
    source = _tokens(primary)
    scored = []
    for candidate in opposite:
        target = _tokens(candidate)
        overlap = len(source & target)
        union = len(source | target) or 1
        scored.append((overlap / union, overlap, -int(candidate["physical_page"]), int(candidate["physical_page"])))
    scored.sort(reverse=True)
    return [page for _ratio, overlap, _neg, page in scored[:limit] if overlap > 0]


def build_supplemental_batches(
    *,
    semantic_map: dict[str, Any],
    page_records: dict[tuple[str, int], dict[str, Any]],
    max_primary_pages: int = 8,
    candidate_pages_per_primary: int = 2,
    max_batches: int = 4,
) -> list[dict[str, Any]]:
    """Group unmatched content pages and add lexical counterpart candidates."""
    if max_primary_pages < 1 or candidate_pages_per_primary < 0 or max_batches < 0:
        raise ValueError("invalid supplemental review limits")
    batches = []
    for side, field, opposite in (
        ("OLD", "unmatched_old", "NEW"),
        ("NEW", "unmatched_new", "OLD"),
    ):
        primary_pages = [
            int(page) for page in semantic_map.get(field) or []
            if _tokens(page_records[(side, int(page))])
        ]
        opposite_records = [record for (record_side, _page), record in page_records.items() if record_side == opposite]
        for offset in range(0, len(primary_pages), max_primary_pages):
            if len(batches) >= max_batches:
                break
            primary_chunk = primary_pages[offset:offset + max_primary_pages]
            candidate_pages = sorted({
                candidate
                for page in primary_chunk
                for candidate in _candidate_pages(
                    page_records[(side, page)], opposite_records, limit=candidate_pages_per_primary,
                )
            })
            ordinal = len(batches) + 1
            region_id = f"U-R{ordinal:03d}"
            region = {
                "region_id": region_id,
                "old_pages": primary_chunk if side == "OLD" else candidate_pages,
                "new_pages": primary_chunk if side == "NEW" else candidate_pages,
                "engineering_domain": "Unmatched content review",
                "scope": f"Bounded review of unmatched {side} pages",
                "locations": [],
                "reason_for_correspondence": "Opposite pages are lexical candidates, not established counterparts.",
                "important_text_blocks": [],
                "important_table_blocks": [],
                "important_graphic_blocks": [],
                "confidence": 0,
            }
            # The old Miner validator requires a bilateral region.  With no
            # candidate, the page remains pending instead of inventing a side.
            if not region["old_pages"] or not region["new_pages"]:
                continue
            batches.append({
                "batch_id": region_id,
                "primary_side": side,
                "primary_pages": primary_chunk,
                "primary_keys": [(side, page) for page in primary_chunk],
                "candidate_side": opposite,
                "candidate_pages": candidate_pages,
                "region": region,
            })
    return batches


def validate_supplemental_answer(batch: dict[str, Any], answer: dict[str, Any]) -> None:
    """Every primary page must be explicitly addressed by the accepted answer."""
    addressed: set[tuple[str, int]] = set()
    for change in answer.get("projectchanges") or []:
        addressed.update(
            (str(item.get("side")), int(item.get("physical_page") or 0))
            for item in change.get("evidence_items") or []
        )
    for hint in answer.get("unresolved_hints") or []:
        addressed.update(
            (str(item.get("side")), int(item.get("physical_page") or 0))
            for item in hint.get("evidence_items") or []
        )
        addressed.update(("OLD", int(page)) for page in hint.get("old_pages") or [])
        addressed.update(("NEW", int(page)) for page in hint.get("new_pages") or [])
    missing = sorted(set(batch["primary_keys"]) - addressed)
    if missing:
        raise ValueError(f"supplemental answer did not address primary pages: {missing}")
