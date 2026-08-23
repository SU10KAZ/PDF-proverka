# -*- coding: utf-8 -*-
"""VERIFY track: independent minimal extraction, written from scratch.

Deliberately does NOT import v03_foundation.  Implements the production formula
(crop_blocks/blocks.py::crop_from_pdf) plus page.derotation_matrix, and nothing else.
"""
from __future__ import annotations
import math
from typing import Optional, Sequence

import fitz
import numpy as np

WHITE = 0.98


def frame(page: fitz.Page, coords_px: Sequence[float], pw: float, ph: float):
    """(clip_display, clip_page, page->display matrix, display->page matrix)."""
    pr = page.rect
    sx = pr.width / float(pw)
    sy = pr.height / float(ph)
    x1, y1, x2, y2 = [float(v) for v in coords_px[:4]]
    clip_d = fitz.Rect(min(x1, x2) * sx, min(y1, y2) * sy, max(x1, x2) * sx, max(y1, y2) * sy)
    derot = page.derotation_matrix
    clip_p = fitz.Rect(clip_d) * derot
    clip_p.normalize()
    return clip_d, clip_p, page.rotation_matrix, derot, sx, sy


def _overlaps(r: fitz.Rect, box: fitz.Rect, eps: float = 1e-6) -> bool:
    """Degenerate-safe overlap.  fitz.Rect.intersects() returns False for EMPTY rects
    (zero width or zero height) -- i.e. for every axis-aligned single-line path."""
    return (r.x0 <= box.x1 + eps and box.x0 <= r.x1 + eps
            and r.y0 <= box.y1 + eps and box.y0 <= r.y1 + eps)


def is_white(c) -> bool:
    return c is not None and len(c) >= 3 and all(float(v) >= WHITE for v in c[:3])


def ink_rule(g: dict) -> Optional[str]:
    """Replica of the fnd ink rules, minus the outside_clip test."""
    fill, color = g.get("fill"), g.get("color")
    fo, so = g.get("fill_opacity"), g.get("stroke_opacity")
    fo0 = fo is not None and float(fo) <= 0.01
    so0 = so is not None and float(so) <= 0.01
    hf, hs = fill is not None, color is not None
    if (not hf or fo0) and (not hs or so0):
        if fo0 or so0:
            return "zero_opacity"
    if hf and hs and is_white(fill) and is_white(color):
        return "white_fill_white_stroke"
    if hf and not hs and is_white(fill):
        return "white_fill_no_stroke"
    if hs and not hf and is_white(color):
        return "white_stroke_no_fill"
    return None


def _cubic(p0, p1, p2, p3, n=6):
    out = []
    for i in range(n + 1):
        t = i / n; u = 1 - t
        out.append((u**3*p0[0] + 3*u*u*t*p1[0] + 3*u*t*t*p2[0] + t**3*p3[0],
                    u**3*p0[1] + 3*u*u*t*p1[1] + 3*u*t*t*p2[1] + t**3*p3[1]))
    return out


def _pt(v):
    return (float(v.x), float(v.y)) if isinstance(v, fitz.Point) else (float(v[0]), float(v[1]))


def _clip_seg(a, b, box):
    x0, y0, x1, y1 = box
    px, py = a; qx, qy = b
    dx, dy = qx - px, qy - py
    t0, t1 = 0.0, 1.0
    for p, q in ((-dx, px - x0), (dx, x1 - px), (-dy, py - y0), (dy, y1 - py)):
        if p == 0:
            if q < 0: return None
            continue
        t = q / p
        if p < 0:
            if t > t1: return None
            t0 = max(t0, t)
        else:
            if t < t0: return None
            t1 = min(t1, t)
    if t0 > t1: return None
    return ((px + t0*dx, py + t0*dy), (px + t1*dx, py + t1*dy))


def segments(page, clip_d, clip_p, fwd, *, path_gate="overlap", drop_invisible=True,
             naive=False, drawings=None):
    """path_gate: 'overlap' (mine, degenerate-safe) | 'intersects' (module replica).

    naive=True feeds the display clip straight to the page space (the known-bad path).
    Returns (kept, dropped, stats).
    """
    read = fitz.Rect(clip_d) if naive else fitz.Rect(clip_p)
    m = fitz.Identity if naive else fwd
    box = [clip_d.x0, clip_d.y0, clip_d.x1, clip_d.y1]
    dr = page.get_drawings() if drawings is None else drawings
    kept, dropped = [], []
    st = {"paths": len(dr), "gated_out": 0, "invisible": 0, "empty_rect_paths": 0,
          "empty_rect_in_clip": 0}
    for pi, g in enumerate(dr):
        r = g.get("rect")
        if r is not None:
            rr = fitz.Rect(r)
            if rr.is_empty:
                st["empty_rect_paths"] += 1
                if _overlaps(rr, read):
                    st["empty_rect_in_clip"] += 1
            ok = _overlaps(rr, read) if path_gate == "overlap" else rr.intersects(read)
            if not ok:
                st["gated_out"] += 1
                continue
        rule = ink_rule(g)
        if rule:
            st["invisible"] += 1
        sink = dropped if (rule and drop_invisible) else kept
        w = float(g.get("width") or 0.0)
        for it in g.get("items") or []:
            op = it[0]
            pts = []
            if op == "l":
                pts = [(_pt(it[1]), _pt(it[2]))]
            elif op == "re":
                q = fitz.Rect(it[1]); c = [(q.x0,q.y0),(q.x1,q.y0),(q.x1,q.y1),(q.x0,q.y1)]
                pts = [(c[k], c[(k+1) % 4]) for k in range(4)]
            elif op == "qu":
                q = it[1]; c = [_pt(q.ul), _pt(q.ur), _pt(q.lr), _pt(q.ll)]
                pts = [(c[k], c[(k+1) % 4]) for k in range(4)]
            elif op == "c":
                s = _cubic(_pt(it[1]), _pt(it[2]), _pt(it[3]), _pt(it[4]))
                pts = [(s[k], s[k+1]) for k in range(len(s)-1)]
            for a, b in pts:
                A = fitz.Point(*a) * m; B = fitz.Point(*b) * m
                res = _clip_seg((A.x, A.y), (B.x, B.y), box)
                if res is None: continue
                (sx, sy), (ex, ey) = res
                L = math.hypot(ex-sx, ey-sy)
                if L <= 1e-6: continue
                sink.append({"p0": (sx, sy), "p1": (ex, ey), "len": L, "w": w,
                             "path": pi, "rule": rule, "op": op})
    return kept, dropped, st


def render(page, clip_d, dpi=150):
    s = dpi / 72.0
    pix = page.get_pixmap(matrix=fitz.Matrix(s, s), clip=clip_d, alpha=False)
    a = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    g = a[:, :, :3].mean(axis=2) if pix.n >= 3 else a[:, :, 0].astype(float)
    return g, s, pix


def seg_mask(segs, clip_d, s, shape):
    """Rasterise segments into a boolean mask of the given shape."""
    from PIL import Image, ImageDraw
    im = Image.new("1", (shape[1], shape[0]), 0)
    d = ImageDraw.Draw(im)
    for sg in segs:
        x0 = (sg["p0"][0] - clip_d.x0) * s; y0 = (sg["p0"][1] - clip_d.y0) * s
        x1 = (sg["p1"][0] - clip_d.x0) * s; y1 = (sg["p1"][1] - clip_d.y0) * s
        d.line([x0, y0, x1, y1], fill=1, width=1)
    return np.array(im, dtype=bool)


def dilate(mask, r=1):
    from scipy.ndimage import binary_dilation
    if r <= 0: return mask
    k = np.ones((2*r+1, 2*r+1), dtype=bool)
    return binary_dilation(mask, structure=k)
