"""Build Human Mapping UI data from persisted V3 source + semantic map.

PAGE CONTEXT ≠ SEMANTIC MEMBERSHIP.

* ``region.old_blocks`` / ``region.new_blocks`` are the Mapper's explicit
  membership: ``important_text_blocks + important_table_blocks +
  important_graphic_blocks`` of that side, in that order (the sealed V1.2.4
  fixture contract, ``scripts/human_mapping_verification_ui_v1.py``).
* ``region.pages`` is visual context: every page that hosts a member block,
  with ALL blocks of that page.

A side with no member block is never filled with page blocks.  Its
membership stays EMPTY, the region is marked for review, and the mapped
pages of that side become its visual context so a human can link manually.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

MAPPED = "MAPPED"
EMPTY = "EMPTY"
REVIEW_INSUFFICIENT_MAPPING = "REVIEW_INSUFFICIENT_MAPPING"
REVIEW_INVALID_MEMBERSHIP_REFS = "REVIEW_INVALID_MEMBERSHIP_REFS"
_IMPORTANT_KEYS = ("important_text_blocks", "important_table_blocks", "important_graphic_blocks")


def _page_record(work_dir: Path, side: str, page_no: int) -> dict[str, Any] | None:
    path = work_dir / "source" / side.lower() / f"p{int(page_no):03d}" / "page.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _crop_rel(side: str, page_no: int, block: dict[str, Any]) -> str:
    if not block.get("graphic_crop_ref"):
        return ""
    return f"assets/{side.lower()}/p{int(page_no):03d}/{block['block_id']}.png"


def _page_block(side: str, page_no: int, block: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": block["block_id"],
        "type": block["modality"],
        "page": int(page_no),
        "bbox": block["bbox"],
        "structured_md": block.get("structured_md") or "",
        "tables": block.get("tables") or [],
        "crop": _crop_rel(side, page_no, block),
    }


def _context_page(work_dir: Path, side: str, page_no: int) -> dict[str, Any] | None:
    record = _page_record(work_dir, side, page_no)
    if record is None:
        return None
    return {
        "page": int(page_no),
        "image": f"assets/{side.lower()}/p{int(page_no):03d}/full_page.png",
        "blocks": [_page_block(side, page_no, b) for b in record.get("blocks") or []],
    }


def build_region(region: dict[str, Any], work_dir: Path) -> dict[str, Any]:
    refs = [ref for key in _IMPORTANT_KEYS for ref in (region.get(key) or [])]
    members: dict[str, list[dict[str, Any]]] = {"OLD": [], "NEW": []}
    invalid: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for ref in refs:
        side = ref.get("side")
        page_no = ref.get("physical_page")
        block_id = ref.get("block_id")
        record = (
            _page_record(work_dir, side, page_no)
            if side in members and isinstance(page_no, int)
            else None
        )
        block = next(
            (b for b in (record or {}).get("blocks") or [] if b.get("block_id") == block_id),
            None,
        )
        if block is None:
            invalid.append({
                "side": side,
                "physical_page": page_no,
                "block_id": block_id,
                "reason": "BLOCK_NOT_ON_CITED_PAGE",
            })
            continue
        if (side, block_id) in seen:
            continue
        seen.add((side, block_id))
        members[side].append({
            **_page_block(side, page_no, block),
            "side": side,
            "relevance": ref.get("relevance") or "",
        })

    membership_state: dict[str, str] = {}
    pages: dict[str, list[dict[str, Any]]] = {"OLD": [], "NEW": []}
    for side, pages_key in (("OLD", "old_pages"), ("NEW", "new_pages")):
        if members[side]:
            membership_state[side] = MAPPED
            page_numbers = sorted({b["page"] for b in members[side]})
        else:
            membership_state[side] = EMPTY
            page_numbers = sorted({int(p) for p in region.get(pages_key) or []})
        for page_no in page_numbers:
            page = _context_page(work_dir, side, page_no)
            if page is not None:
                pages[side].append(page)

    if invalid:
        mapping_state = REVIEW_INVALID_MEMBERSHIP_REFS
    elif EMPTY in membership_state.values():
        mapping_state = REVIEW_INSUFFICIENT_MAPPING
    else:
        mapping_state = MAPPED
    return {
        "id": region["region_id"],
        "domain": region.get("engineering_domain") or "",
        "scope": region.get("scope") or "",
        "reason": region.get("reason_for_correspondence") or "",
        "confidence": region.get("confidence"),
        "old_blocks": members["OLD"],
        "new_blocks": members["NEW"],
        "pages": pages,
        "membership_state": membership_state,
        "mapping_state": mapping_state,
        "invalid_membership_refs": invalid,
    }


def build_human_mapping_ui_data(
    *,
    pair_id: str,
    object_id: str,
    semantic_map: dict[str, Any],
    work_dir: Path,
    label: str | None = None,
) -> dict[str, Any]:
    """Convert V3 semantic map + prepared pages into the HM UI payload."""
    work_dir = Path(work_dir)
    regions = [build_region(region, work_dir) for region in semantic_map.get("regions") or []]
    canonical_map = json.dumps(semantic_map, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return {
        "schema": "human-mapping-ui-data/1",
        "object_id": object_id,
        "pair": pair_id,
        "pair_key": pair_id,
        "label": label or pair_id,
        "source_semantic_map": "project_change_v3_semantic_map",
        "source_sha256": hashlib.sha256(canonical_map).hexdigest(),
        "review_region_count": sum(r["mapping_state"] != MAPPED for r in regions),
        "regions": regions,
    }


def materialize_hm_assets(work_dir: Path, dest_assets: Path) -> None:
    """Copy prepared page rasters/crops into an HM assets tree (errors propagate)."""
    src = Path(work_dir) / "source"
    if not src.is_dir():
        raise FileNotFoundError(f"V3 prepared source missing: {src}")
    dest_assets = Path(dest_assets)
    for side_dir in sorted(src.iterdir()):
        if not side_dir.is_dir():
            continue
        for page_dir in sorted(side_dir.iterdir()):
            if not page_dir.is_dir():
                continue
            target = dest_assets / side_dir.name / page_dir.name
            target.mkdir(parents=True, exist_ok=True)
            for f in page_dir.glob("*.png"):
                shutil.copy2(f, target / f.name)
