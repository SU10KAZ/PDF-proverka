"""Тайлы просмотрщика: наложение из масок и страницы A/B вектором.

Уровень ``L`` — уменьшение в ``2**L`` раз от полного разрешения сравнения.
При уменьшении слои отличий берутся МАКСИМУМОМ по блоку: мелкое отличие не
исчезает при отдалении (план: «надёжные наложения»).
"""
from __future__ import annotations

import math
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any

import cv2
import fitz
import numpy as np

from . import engine, pdfio

TILE = 512
DIFF_LAYERS = {"a_only", "b_only", "light_a", "light_b", "color", "style", "within_tol", "noise"}
DEFAULT_LAYERS = ("ink_any", "common", "light_a", "light_b", "style", "color", "a_only", "b_only", "zones")
_masks: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_docs: "OrderedDict[str, tuple[fitz.Document, threading.Lock]]" = OrderedDict()
_lock = threading.Lock()


def max_level(width: int, height: int) -> int:
    return max(0, int(math.ceil(math.log2(max(1, max(width, height)) / TILE))))


def _load_masks(result_dir: Path) -> dict[str, Any]:
    key = str(result_dir)
    with _lock:
        if key in _masks:
            _masks.move_to_end(key)
            return _masks[key]
    z = np.load(result_dir / "masks.npz")
    data = {"shape": tuple(int(v) for v in z["shape"]), "layers": {k: z[k] for k in z.files if k != "shape"}}
    with _lock:
        _masks[key] = data
        while len(_masks) > 6:
            _masks.popitem(last=False)
    return data


def _unpack(packed: np.ndarray, x0: int, y0: int, x1: int, y1: int) -> np.ndarray:
    b0, b1 = x0 // 8, (x1 + 7) // 8
    bits = np.unpackbits(packed[y0:y1, b0:b1], axis=1)
    off = x0 - b0 * 8
    return bits[:, off:off + (x1 - x0)]


def _reduce(arr: np.ndarray, f: int, mode: str) -> np.ndarray:
    if f == 1:
        return arr
    h, w = arr.shape
    hh, ww = (h + f - 1) // f, (w + f - 1) // f
    pad = np.zeros((hh * f, ww * f), arr.dtype)
    pad[:h, :w] = arr
    blocks = pad.reshape(hh, f, ww, f)
    if mode == "max":
        return blocks.max(axis=(1, 3))
    return (blocks.mean(axis=(1, 3)) >= 0.5).astype(np.uint8)


def _encode(rgb: np.ndarray) -> bytes:
    ok, buf = cv2.imencode(".png", cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR))
    if not ok:
        raise RuntimeError("PNG не закодирован")
    return bytes(buf)


def overlay_tile(result_dir: Path, level: int, tx: int, ty: int, layers: tuple[str, ...]) -> bytes | None:
    data = _load_masks(result_dir)
    h, w = data["shape"]
    f = 2 ** max(0, level)
    x0, y0 = tx * TILE * f, ty * TILE * f
    if x0 >= w or y0 >= h or tx < 0 or ty < 0:
        return None
    x1, y1 = min(w, x0 + TILE * f), min(h, y0 + TILE * f)
    region = {}
    for name in layers:
        packed = data["layers"].get(name)
        if packed is None:
            continue
        mode = "max" if name in DIFF_LAYERS or name in ("common", "ink_any") else "mean"
        region[name] = _reduce(_unpack(packed, x0, y0, x1, y1), f, mode)
    if not region:
        return None
    return _encode(engine.compose(region, layers))


def _doc(pdf: Path) -> tuple[fitz.Document, threading.Lock]:
    key = str(pdf)
    with _lock:
        if key in _docs:
            _docs.move_to_end(key)
            return _docs[key]
    doc = pdfio.open_memory(pdfio.read_bytes(pdf))
    entry = (doc, threading.Lock())
    with _lock:
        _docs[key] = entry
        while len(_docs) > 8:
            _docs.popitem(last=False)
    return entry


def page_tile(pdf: Path, page: int, canvas: dict[str, Any], matrix_b_to_a: list[list[float]] | None,
              level: int, tx: int, ty: int, *, gray_overlay: bool = False) -> bytes | None:
    """Тайл страницы (A, или B, перенесённой в систему A), рендер вектором."""
    s_full = canvas["dpi"] / 72.0
    f = 2 ** max(0, level)
    s = s_full / f
    ox, oy = canvas["origin_pt"]
    w_lvl = int(math.ceil(canvas["width_px"] / f))
    h_lvl = int(math.ceil(canvas["height_px"] / f))
    X0, Y0 = tx * TILE, ty * TILE
    if X0 >= w_lvl or Y0 >= h_lvl or tx < 0 or ty < 0:
        return None
    tw, th = min(TILE, w_lvl - X0), min(TILE, h_lvl - Y0)
    rect_a = fitz.Rect(X0 / s + ox, Y0 / s + oy, (X0 + tw) / s + ox, (Y0 + th) / s + oy)
    base = fitz.Matrix(1, 0, 0, 1, -ox, -oy) * fitz.Matrix(s, s) * fitz.Matrix(1, 0, 0, 1, -X0, -Y0)
    doc, lk = _doc(pdf)
    out = np.full((th, tw, 3), 255, np.uint8)
    with lk:
        pg = doc[page - 1]
        if matrix_b_to_a is None:
            clip = rect_a & pg.rect
            total = base
        else:
            m = np.array(matrix_b_to_a, dtype=np.float64)
            inv = cv2.invertAffineTransform(m)
            pts = [(inv[0][0] * x + inv[0][1] * y + inv[0][2], inv[1][0] * x + inv[1][1] * y + inv[1][2])
                   for x, y in ((rect_a.x0, rect_a.y0), (rect_a.x1, rect_a.y0), (rect_a.x0, rect_a.y1), (rect_a.x1, rect_a.y1))]
            clip = fitz.Rect(min(p[0] for p in pts), min(p[1] for p in pts), max(p[0] for p in pts),
                             max(p[1] for p in pts)) & pg.rect
            total = fitz.Matrix(m[0][0], m[1][0], m[0][1], m[1][1], m[0][2], m[1][2]) * base
        if clip.is_empty:
            return _encode(out)
        arr, px, py = pdfio.render(pg, total, clip=clip)
    sx0, sy0 = max(0, -px), max(0, -py)
    dx0, dy0 = max(0, px), max(0, py)
    cw = min(arr.shape[1] - sx0, tw - dx0)
    ch = min(arr.shape[0] - sy0, th - dy0)
    if cw > 0 and ch > 0:
        out[dy0:dy0 + ch, dx0:dx0 + cw] = arr[sy0:sy0 + ch, sx0:sx0 + cw]
    if gray_overlay:
        g = cv2.cvtColor(out, cv2.COLOR_RGB2GRAY)
        tinted = np.full_like(out, 255)
        tinted[g < 250] = engine.COLORS["ink_any"]
        tinted[g < 200] = engine.COLORS["common"]
        out = tinted
    return _encode(out)
