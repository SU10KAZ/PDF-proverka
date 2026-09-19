"""Prepare generic OLD/NEW structured source packages for ProjectChange V3."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .contracts import SOURCE_PACKAGING_VERSION

_BLOCK_HDR = re.compile(r"^### BLOCK #(\d+) \[([^]]+)\]: (\S+)\s*$", re.M)
_TABLE_RE = re.compile(r"(?:^\s*\|.*\|\s*$\n?)+", re.M)


def sha256_file(path: Path | str) -> str:
    data = Path(path).read_bytes()
    return hashlib.sha256(data).hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_md_blocks(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    matches = list(_BLOCK_HDR.finditer(text))
    result: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        body = text[match.end() : end]
        marker = re.search(r"^## Page \d+\s*$", body, re.M)
        if marker:
            body = body[: marker.start()]
        result[match.group(3)] = "\n".join(
            line
            for line in body.splitlines()
            if not line.startswith(("> **Created:", "> **Crop:", "> **Stamp:"))
        ).strip()
    return result


class SourcePreparationError(ValueError):
    """Source package violates the frozen V3 packaging contract (fail closed)."""


# Frozen V3 contract (ai_first_semantic_mapping_projectchange_v3.prepare_pair):
# a block type outside this table is an error, never a silent TEXT.
_MODALITY_BY_BLOCK_TYPE = {"text": "TEXT", "image": "GRAPHIC", "stamp": "TEXT"}


def _modality(block_type: Any, tables: list[str]) -> str:
    modality = _MODALITY_BY_BLOCK_TYPE.get(block_type) if isinstance(block_type, str) else None
    if modality is None:
        raise SourcePreparationError(f"unknown block_type {block_type!r}")
    if tables and modality == "TEXT":
        return "TABLE"
    return modality


def _block_bbox(block: dict[str, Any]) -> list[Any]:
    bbox = block.get("coords_norm")
    if not isinstance(bbox, list) or len(bbox) != 4 or not all(
        isinstance(value, (int, float)) and not isinstance(value, bool) for value in bbox
    ):
        raise SourcePreparationError(
            f"block {block.get('block_id')!r} has no valid coords_norm bbox"
        )
    return bbox


def _block_page(block: dict[str, Any]) -> int:
    page_index = block.get("page_index")
    if not isinstance(page_index, int) or isinstance(page_index, bool) or page_index < 0:
        raise SourcePreparationError(
            f"block {block.get('block_id')!r} has no valid page_index"
        )
    return page_index + 1


def _crop_pixmap(page: Any, coords: list[float], output: Path, max_dimension: int = 1800) -> None:
    import fitz

    rect = page.rect
    clip = (
        fitz.Rect(
            coords[0] * rect.width,
            coords[1] * rect.height,
            coords[2] * rect.width,
            coords[3] * rect.height,
        )
        & rect
    )
    if clip.is_empty or clip.width <= 0 or clip.height <= 0:
        raise SourcePreparationError(f"graphic crop region is empty: {coords!r}")
    scale = min(max_dimension / max(clip.width, clip.height), 3.0)
    page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False).save(output)


def prepare_side(
    *,
    side: str,
    pdf_path: Path,
    blocks_path: Path,
    md_path: Path,
    out_dir: Path,
) -> list[dict[str, Any]]:
    import fitz

    if not pdf_path.is_file():
        raise SourcePreparationError(f"{side} pdf missing: {pdf_path}")
    if not blocks_path.is_file():
        raise SourcePreparationError(f"{side} blocks missing: {blocks_path}")
    # Markdown is mandatory (project contract and frozen V3): no empty-text fallback.
    if not md_path.is_file():
        raise SourcePreparationError(f"{side} markdown missing: {md_path}")
    blocks_payload = json.loads(blocks_path.read_text(encoding="utf-8"))
    blocks = blocks_payload.get("blocks") if isinstance(blocks_payload, dict) else None
    if not isinstance(blocks, list):
        raise SourcePreparationError(f"{side} blocks.json must be an object with a blocks list")
    try:
        md = parse_md_blocks(md_path)
    except UnicodeDecodeError as exc:
        raise SourcePreparationError(f"{side} markdown is not UTF-8: {md_path}") from exc
    doc = fitz.open(str(pdf_path))
    by_page: dict[int, list[dict[str, Any]]] = {}
    for block in blocks:
        by_page.setdefault(_block_page(block), []).append(block)

    structure: list[dict[str, Any]] = []
    pdf_sha = sha256_file(pdf_path)
    for page_no in range(1, len(doc) + 1):
        page = doc[page_no - 1]
        page_dir = out_dir / side.lower() / f"p{page_no:03d}"
        page_dir.mkdir(parents=True, exist_ok=True)
        full_page = page_dir / "full_page.png"
        scale = 1800 / max(page.rect.width, page.rect.height)
        page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False).save(full_page)
        rows = []
        summary = []
        for block in by_page.get(page_no, []):
            body = md.get(block["block_id"], "")
            tables = [m.group().strip() for m in _TABLE_RE.finditer(body)]
            modality = _modality(block.get("block_type"), tables)
            bbox = _block_bbox(block)
            crop_ref = ""
            if modality == "GRAPHIC":
                crop = page_dir / f"{block['block_id']}.png"
                _crop_pixmap(page, bbox, crop)
                crop_ref = str(crop)
            row = {
                "block_id": block["block_id"],
                "modality": modality,
                "source_block_type": block.get("block_type"),
                "bbox": bbox,
                "structured_md": body,
                "tables": tables,
                "existing_description": body if modality == "GRAPHIC" else "",
                "graphic_crop_ref": crop_ref,
                "graphic_crop_sha256": sha256_file(crop_ref) if crop_ref else "",
            }
            rows.append(row)
            summary.append(
                {
                    "block_id": row["block_id"],
                    "modality": modality,
                    "bbox": row["bbox"],
                    "structured_md": body,
                    "tables": tables,
                    "graphic_crop_ref": crop_ref,
                }
            )
        record = {
            "side": side,
            "source_pdf": str(pdf_path),
            "source_pdf_sha256": pdf_sha,
            "physical_page": page_no,
            "blocks": rows,
            "native_page_text": page.get_text(sort=True),
            "full_page_ref": str(full_page),
            "full_page_sha256": sha256_file(full_page),
        }
        page_json = page_dir / "page.json"
        page_json.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        structure.append({"side": side, "physical_page": page_no, "blocks": summary})
    doc.close()
    return structure


def prepare_comparison_sources(
    *,
    pair_id: str,
    old_paths: dict[str, Path],
    new_paths: dict[str, Path],
    work_dir: Path,
    object_id: str | None = None,
) -> dict[str, Any]:
    """Build generic OLD/NEW structure for an arbitrary comparison pair."""
    work_dir.mkdir(parents=True, exist_ok=True)
    source_dir = work_dir / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    structure: list[dict[str, Any]] = []
    structure.extend(
        prepare_side(
            side="OLD",
            pdf_path=Path(old_paths["pdf"]),
            blocks_path=Path(old_paths["blocks"]),
            md_path=Path(old_paths.get("markdown") or old_paths.get("md") or ""),
            out_dir=source_dir,
        )
    )
    structure.extend(
        prepare_side(
            side="NEW",
            pdf_path=Path(new_paths["pdf"]),
            blocks_path=Path(new_paths["blocks"]),
            md_path=Path(new_paths.get("markdown") or new_paths.get("md") or ""),
            out_dir=source_dir,
        )
    )
    structure_path = work_dir / "DOCUMENT_STRUCTURE.json"
    structure_path.write_text(
        json.dumps(structure, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    manifest = {
        "schema": "projectchange_v3_source_manifest/1",
        "pair_id": pair_id,
        "object_id": object_id,
        "source_packaging_version": SOURCE_PACKAGING_VERSION,
        "structure_path": str(structure_path),
        "structure_sha256": sha256_file(structure_path),
        "old_pdf_sha256": sha256_file(old_paths["pdf"]),
        "new_pdf_sha256": sha256_file(new_paths["pdf"]),
        "pages": {
            f"{page['side']}:{page['physical_page']}": len(page["blocks"])
            for page in structure
        },
    }
    manifest_path = work_dir / "SOURCE_MANIFEST.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return {
        "pair_id": pair_id,
        "object_id": object_id,
        "structure": structure,
        "structure_path": str(structure_path),
        "structure_sha256": sha256_file(structure_path),
        "manifest": manifest,
        "manifest_path": str(manifest_path),
        "work_dir": str(work_dir),
        "source_packaging_version": SOURCE_PACKAGING_VERSION,
    }


def mapping_images(structure: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for page in structure:
        for block in page["blocks"]:
            if block.get("graphic_crop_ref"):
                result.append(
                    {
                        "path": block["graphic_crop_ref"],
                        "label": {
                            "side": page["side"],
                            "physical_page": page["physical_page"],
                            "kind": "GRAPHIC_CROP",
                            "block_id": block["block_id"],
                            "bbox": block["bbox"],
                            "ref": block["graphic_crop_ref"],
                        },
                    }
                )
    return result


def load_page_record(work_dir: Path, side: str, page_no: int) -> dict[str, Any]:
    path = Path(work_dir) / "source" / side.lower() / f"p{page_no:03d}" / "page.json"
    return json.loads(path.read_text(encoding="utf-8"))


def optimized_region_bundle(
    *,
    pair_id: str,
    region: dict[str, Any],
    work_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Assemble Miner input with frozen full-page raster optimization rules."""
    pages: list[dict[str, Any]] = []
    images: list[dict[str, Any]] = []
    for side_key, side in (("old_pages", "OLD"), ("new_pages", "NEW")):
        for page_no in region.get(side_key) or []:
            page = load_page_record(work_dir, side, int(page_no))
            pages.append(page)
            graphic_blocks = [b for b in page["blocks"] if b["modality"] == "GRAPHIC"]
            for block in page["blocks"]:
                if block.get("graphic_crop_ref"):
                    images.append(
                        {
                            "path": block["graphic_crop_ref"],
                            "label": {
                                "side": page["side"],
                                "physical_page": page_no,
                                "kind": "GRAPHIC_CROP",
                                "block_id": block["block_id"],
                                "bbox": block["bbox"],
                                "ref": block["graphic_crop_ref"],
                            },
                        }
                    )
            if graphic_blocks:
                images.append(
                    {
                        "path": page["full_page_ref"],
                        "label": {
                            "side": page["side"],
                            "physical_page": page_no,
                            "kind": "FULL_PAGE_CONTEXT",
                            "block_id": "",
                            "bbox": [0, 0, 1, 1],
                            "ref": page["full_page_ref"],
                        },
                    }
                )
    data = {"pair": pair_id, "frozen_region": region, "pages": pages}
    return data, images
