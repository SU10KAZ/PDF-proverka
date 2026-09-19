"""Build Human Mapping UI data from persisted V3 source + semantic map."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _page_image_rel(work_dir: Path, side: str, page_no: int) -> str:
    return f"assets/{side.lower()}/p{page_no:03d}/full_page.png"


def build_human_mapping_ui_data(
    *,
    pair_id: str,
    object_id: str,
    semantic_map: dict[str, Any],
    work_dir: Path,
    label: str | None = None,
) -> dict[str, Any]:
    """Convert V3 semantic map + prepared pages into HM UI payload."""
    work_dir = Path(work_dir)
    regions_out = []
    for region in semantic_map.get("regions") or []:
        pages = {"OLD": [], "NEW": []}
        old_blocks = []
        new_blocks = []
        for side_key, side in (("old_pages", "OLD"), ("new_pages", "NEW")):
            for page_no in region.get(side_key) or []:
                page_path = (
                    work_dir
                    / "source"
                    / side.lower()
                    / f"p{int(page_no):03d}"
                    / "page.json"
                )
                page = json.loads(page_path.read_text(encoding="utf-8"))
                blocks = []
                for b in page.get("blocks") or []:
                    item = {
                        "id": b["block_id"],
                        "type": b["modality"],
                        "page": page_no,
                        "bbox": b["bbox"],
                        "structured_md": b.get("structured_md") or "",
                        "tables": b.get("tables") or [],
                        "crop": (
                            f"assets/{side.lower()}/p{int(page_no):03d}/{b['block_id']}.png"
                            if b.get("graphic_crop_ref")
                            else None
                        ),
                    }
                    blocks.append(item)
                    if side == "OLD":
                        old_blocks.append({"id": item["id"], "type": item["type"]})
                    else:
                        new_blocks.append({"id": item["id"], "type": item["type"]})
                pages[side].append(
                    {
                        "page": page_no,
                        "image": _page_image_rel(work_dir, side, int(page_no)),
                        "blocks": blocks,
                    }
                )
        regions_out.append(
            {
                "id": region["region_id"],
                "domain": region.get("engineering_domain") or "",
                "scope": region.get("scope") or "",
                "reason": region.get("reason_for_correspondence") or "",
                "confidence": region.get("confidence"),
                "old_blocks": old_blocks,
                "new_blocks": new_blocks,
                "pages": pages,
            }
        )
    return {
        "schema": "human-mapping-ui-data/1",
        "object_id": object_id,
        "pair": pair_id,
        "pair_key": pair_id,
        "label": label or pair_id,
        "source_semantic_map": "project_change_v3",
        "regions": regions_out,
    }


def materialize_hm_assets(work_dir: Path, dest_assets: Path) -> None:
    """Copy prepared page rasters/crops into an HM assets tree."""
    import shutil

    src = Path(work_dir) / "source"
    if not src.is_dir():
        return
    dest_assets = Path(dest_assets)
    for side_dir in src.iterdir():
        if not side_dir.is_dir():
            continue
        for page_dir in side_dir.iterdir():
            if not page_dir.is_dir():
                continue
            target = dest_assets / side_dir.name / page_dir.name
            target.mkdir(parents=True, exist_ok=True)
            for f in page_dir.glob("*.png"):
                shutil.copy2(f, target / f.name)
