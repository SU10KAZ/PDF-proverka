from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import fitz


@dataclass
class RenderResult:
    png_bytes: bytes
    width_px: int
    height_px: int
    dpi: float
    note: str = ""


def _page_and_clip(pdf_path: str, page_no: int, block: dict):
    doc = fitz.open(pdf_path)
    page = doc[page_no - 1]
    rect = page.rect
    bbox = block.get("bbox")
    if bbox:
        clip = fitz.Rect(*bbox)
    else:
        x0, y0, x1, y1 = block.get("bbox_norm") or [0.0, 0.0, 1.0, 1.0]
        clip = fitz.Rect(
            rect.x0 + rect.width * float(x0),
            rect.y0 + rect.height * float(y0),
            rect.x0 + rect.width * float(x1),
            rect.y0 + rect.height * float(y1),
        )
    return doc, page, clip & rect


def _render(page, clip, dpi: float, note: str = "") -> RenderResult:
    matrix = fitz.Matrix(dpi / 72.0, dpi / 72.0)
    pix = page.get_pixmap(matrix=matrix, clip=clip, alpha=False)
    return RenderResult(pix.tobytes("png"), pix.width, pix.height, float(dpi), note)


def render_block_at_dpi(pdf_path: str, page_no: int, block: dict, dpi: float, *, max_long_side_px: Optional[int] = None) -> RenderResult:
    doc, page, clip = _page_and_clip(pdf_path, page_no, block)
    try:
        note = ""
        if max_long_side_px:
            long_px = max(clip.width, clip.height) * dpi / 72.0
            if long_px > max_long_side_px:
                dpi = dpi * (max_long_side_px / long_px)
                note = "dpi_capped"
        return _render(page, clip, dpi, note)
    finally:
        doc.close()


def render_block_resized_long_side(pdf_path: str, page_no: int, block: dict, target_long_side_px: int, *, max_scale: float = 6.0) -> RenderResult:
    doc, page, clip = _page_and_clip(pdf_path, page_no, block)
    try:
        long_pt = max(clip.width, clip.height)
        scale = target_long_side_px / long_pt if long_pt else 1.0
        note = "resized_long_side"
        if scale > max_scale:
            scale = max_scale
            note = "scale_clamped_6x"
        return _render(page, clip, scale * 72.0, note)
    finally:
        doc.close()


def extract_block_pdf_bytes(pdf_path: str, page_no: int, block: dict) -> bytes:
    src, _page, clip = _page_and_clip(pdf_path, page_no, block)
    out = fitz.open()
    try:
        page = out.new_page(width=clip.width, height=clip.height)
        page.show_pdf_page(fitz.Rect(0, 0, clip.width, clip.height), src, page_no - 1, clip=clip)
        return out.tobytes()
    finally:
        out.close()
        src.close()


def tile_block(pdf_path: str, page_no: int, block: dict, dpi: float, *, max_tile_long_side_px: int = 1200, max_tiles: int = 8):
    # The offline contract only needs bounded valid image tiles. Start with one
    # capped render; production tiling experiments can split further if needed.
    return [render_block_at_dpi(pdf_path, page_no, block, dpi, max_long_side_px=max_tile_long_side_px)][:max_tiles]
