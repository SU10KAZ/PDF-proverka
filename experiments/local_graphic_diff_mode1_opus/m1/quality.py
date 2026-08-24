"""Dual extraction metric — precision AND completeness.

The previous audit's sharpest lesson: extraction precision can be 1.0 while a
third of the drawing is missing.  So every block is measured twice, against the
rendered page itself:

* PRECISION  — how much of the extracted geometry actually lies on visible ink;
* RECALL     — how much of the visible ink is explained by extracted geometry.

Text is excluded from both (glyphs are not vector primitives here and text is
compared by another pipeline), embedded raster images are excluded and flagged.
"""
from __future__ import annotations

import math
from typing import Any

import cv2
import fitz
import numpy as np

from .core import PAGES, Block, extract_ink, rasterize, render_gray, text_spans


def _boxes_mask(boxes, frame, cell, pad=0.0):
    x0, y0, x1, y1 = frame
    w = max(1, int(math.ceil((x1 - x0) / cell)))
    h = max(1, int(math.ceil((y1 - y0) / cell)))
    m = np.zeros((h, w), np.uint8)
    for b in boxes:
        a = max(0, int(math.floor((b[0] - pad - x0) / cell)))
        c = max(0, int(math.floor((b[1] - pad - y0) / cell)))
        d = min(w, int(math.ceil((b[2] + pad - x0) / cell)))
        e = min(h, int(math.ceil((b[3] + pad - y0) / cell)))
        if d > a and e > c:
            m[c:e, a:d] = 1
    return m


def image_rects(block: Block) -> list[list[float]]:
    prec = PAGES.page(block.pdf, block.page_index)
    page, rm = prec["page"], prec["rot_matrix"]
    out = []
    for info in page.get_images(full=True):
        try:
            for r in page.get_image_rects(info[0]):
                rv = fitz.Rect(r) * rm
                out.append([min(rv.x0, rv.x1), min(rv.y0, rv.y1), max(rv.x0, rv.x1), max(rv.y0, rv.y1)])
        except Exception:
            continue
    return out


def extraction_quality(block: Block, cell_pt: float = 0.4, tol_pt: float = 1.0,
                       dark_thr: int = 200) -> dict[str, Any]:
    ink = extract_ink(block)
    frame = list(block.bbox_vis)
    gray = render_gray(block, cell_pt)
    h, w = gray.shape
    vis = (gray < dark_thr).astype(np.uint8)

    pred = rasterize(ink["segments"], ink["widths"], frame, cell_pt, min_width_pt=0.35, fills=ink["fills"])
    ph, pw = pred.shape
    if (ph, pw) != (h, w):
        hh, ww = min(h, ph), min(w, pw)
        vis, pred = vis[:hh, :ww], pred[:hh, :ww]
        h, w = hh, ww

    # a stroke sitting exactly on the crop edge is half outside the rect: the
    # renderer still shows it, the clipped vector does not.  That is a framing
    # artefact, not lost geometry, so the outermost ring is not measured.
    edge = max(1, int(math.ceil(1.0 / cell_pt)))
    ring = np.zeros_like(vis)
    ring[edge:-edge or None, edge:-edge or None] = 1

    tspans = text_spans(block)
    tmask = _boxes_mask([t["bbox"] for t in tspans], frame, cell_pt, pad=0.8)[:h, :w]
    imask = _boxes_mask(image_rects(block), frame, cell_pt)[:h, :w]
    excl = ((tmask | imask) > 0) | (ring == 0)
    vis_g = (vis & ~excl).astype(np.uint8)
    pred_g = (pred & ~excl).astype(np.uint8)

    k = max(1, int(round(tol_pt / cell_pt)))
    ker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * k + 1, 2 * k + 1))
    vis_d = cv2.dilate(vis_g, ker)
    pred_d = cv2.dilate(pred_g, ker)

    nv, npd = int(vis_g.sum()), int(pred_g.sum())
    tp_p = int((pred_g & vis_d).sum())
    tp_r = int((vis_g & pred_d).sum())
    missed = (vis_g & ~pred_d).astype(np.uint8)

    # how the missed ink is distributed: one big lost area is much worse than
    # a uniform hairline bias
    n_lab, lab, stats, _ = cv2.connectedComponentsWithStats(missed, connectivity=8)
    comps = sorted((int(stats[i, cv2.CC_STAT_AREA]) for i in range(1, n_lab)), reverse=True)
    big = [c for c in comps if c >= 200]

    return {
        "block_id": block.block_id,
        "pdf": block.pdf,
        "page_index": block.page_index,
        "cell_pt": cell_pt,
        "tol_pt": tol_pt,
        "visible_cells": nv,
        "predicted_cells": npd,
        "precision": round(tp_p / npd, 4) if npd else None,
        "recall": round(tp_r / nv, 4) if nv else None,
        "missed_cells": int(missed.sum()),
        "missed_largest_component": comps[0] if comps else 0,
        "missed_big_components": len(big),
        "missed_share_in_big": round(sum(big) / max(1, int(missed.sum())), 4),
        "excluded_text_cells": int(tmask.sum()),
        "edge_ring_cells": int((ring == 0).sum()),
        "excluded_image_cells": int(imask.sum()),
        "text_spans": len(tspans),
        "page_rotation": ink["page_rotation"],
        "segments": int(len(ink["segments"])),
        "fill_polygons": len(ink["fills"]),
        "invisible_paths": ink["n_invisible_paths"],
        "segments_dropped_invisible": ink["segments_dropped_invisible"],
        "text_as_curves_suspected": bool(len(tspans) == 0 and len(ink["segments"]) > 500),
        "raster_backed": bool(int(imask.sum()) > 0.2 * h * w),
    }
