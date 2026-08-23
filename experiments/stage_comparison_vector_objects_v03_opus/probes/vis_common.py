# -*- coding: utf-8 -*-
"""`visprep` probe — shared helpers for the pointwise-Vision experiment (§18/§19).

Blocks are read ONLY through v03_foundation, objects only through v03_objects,
counterfactuals only through v03_counterfactual.  This file adds no extraction of
its own: it converts PDF-point rectangles into the coords_px space of result.json
so that `F.render_block` can rasterise exactly that sub-region of a prepared block,
and it prices the resulting PNG with the vision-token formula measured in v0.2.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
EXP = Path(__file__).resolve().parents[1]
ART = EXP / "artifacts"
sys.path.insert(0, str(EXP / "probes"))

import fitz                       # noqa: E402
import v03_foundation as F        # noqa: E402

CASES_DIR = ART / "vis_cases"
CAND_DIR = ART / "vis_cand"

# v0.2, measured; NOT re-measured here
TOK_CAP = 3051


def image_tokens(w: int, h: int) -> float:
    return min(1.2014 * math.ceil(w / 32) * math.ceil(h / 32) + 48.67, TOK_CAP)


def abspath(p: str) -> str:
    q = Path(p)
    return str(q if q.is_absolute() else (ROOT / q))


def page_scale(pdf: str, page_index: int, page_px_w: float, page_px_h: float):
    doc = F.open_doc(abspath(pdf))
    r = doc[page_index].rect
    return r.width / float(page_px_w), r.height / float(page_px_h)


def pt_to_px(bbox_pt, sx: float, sy: float):
    x0, y0, x1, y1 = bbox_pt
    return [x0 / sx, y0 / sy, x1 / sx, y1 / sy]


def pad_rect(bbox_pt, pad_pt: float, min_side_pt: float = 0.0):
    x0, y0, x1, y1 = bbox_pt
    x0, x1 = min(x0, x1), max(x0, x1)
    y0, y1 = min(y0, y1), max(y0, y1)
    x0 -= pad_pt; x1 += pad_pt; y0 -= pad_pt; y1 += pad_pt
    if min_side_pt:
        cx, cy = (x0 + x1) / 2.0, (y0 + y1) / 2.0
        if x1 - x0 < min_side_pt:
            x0, x1 = cx - min_side_pt / 2, cx + min_side_pt / 2
        if y1 - y0 < min_side_pt:
            y0, y1 = cy - min_side_pt / 2, cy + min_side_pt / 2
    return [x0, y0, x1, y1]


def square_rect(bbox_pt):
    """Make the window square so both sides get identical pixel dimensions."""
    x0, y0, x1, y1 = bbox_pt
    cx, cy = (x0 + x1) / 2.0, (y0 + y1) / 2.0
    s = max(x1 - x0, y1 - y0) / 2.0
    return [cx - s, cy - s, cx + s, cy + s]


def render_region(side: dict, bbox_pt, out_png: Path, target_px: int = 768):
    """Rasterise the PDF-point rectangle `bbox_pt` of one side of a pair.

    Everything goes through F.render_block (the only allowed reader): the rectangle is
    expressed back in the coords_px space of result.json, exactly the space the
    prepared block's own coords live in.
    """
    pdf = abspath(side["pdf"])
    sx, sy = page_scale(pdf, side["page_index"], side["page_px"][0], side["page_px"][1])
    coords_px = pt_to_px(bbox_pt, sx, sy)
    pix = F.render_block(
        pdf, side["page_index"], coords_px,
        side["page_px"][0], side["page_px"][1],
        target_px=target_px, out_png=str(out_png),
    )
    return pix.width, pix.height


def montage(left_png: Path, right_png: Path, out_png: Path, gap: int = 14):
    """Side-by-side contact image for MY OWN eyes only (never shipped to a blind agent)."""
    a = fitz.Pixmap(str(left_png))
    b = fitz.Pixmap(str(right_png))
    W, H = a.width + gap + b.width, max(a.height, b.height)
    out = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, W, H), False)
    out.clear_with(200)
    a.set_origin(0, 0)
    b.set_origin(a.width + gap, 0)
    out.copy(a, a.irect)
    out.copy(b, b.irect)
    out.save(str(out_png))
    return W, H


def load(name: str):
    with open(ART / name, encoding="utf-8") as fh:
        return json.load(fh)


def mine_pairs() -> dict:
    d = load("mine_pairs.json")
    return {p["pair_id"]: p for p in d["pairs"]}


def loc_pairs() -> dict:
    d = load("loc_real_pairs.json")
    return {p["pair_id"]: p for p in d["pairs"]}
