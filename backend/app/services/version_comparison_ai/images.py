"""Изображения пакета: вырезки A/B из исходных PDF и вырезка наложения (план §4.4).

Вырезка строится по рамке в pt видимой страницы — ``clip`` PyMuPDF принимает её
в той же системе, что и слова (проверено на ``/Rotate 90``, тест
``test_crop_follows_rotated_page``). Аннотации на вырезках видимы — как видит
инженер. Наложение берётся из ``masks.npz`` результата, без пересчёта.
"""
from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Any

import cv2
import fitz
import numpy as np
from PIL import Image, ImageDraw, ImageFont

from backend.app.services.version_comparison import engine, tiles

from . import geometry as geo

_FONT_PATHS = ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/usr/share/fonts/dejavu/DejaVuSans.ttf")
LABEL_COLOR = (220, 0, 120)


def font(size: int) -> ImageFont.ImageFont:
    for p in _FONT_PATHS:
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()


def render_crop(doc: fitz.Document, page: int, box: list[float], dpi: float) -> tuple[np.ndarray | None, list[float]]:
    """Вырезка страницы (1-based) по рамке pt видимой страницы. Возвращает (RGB, фактическая рамка)."""
    pg = doc[page - 1]
    r = geo.clip(box, pg.rect.width, pg.rect.height)
    if r[2] - r[0] < 0.5 or r[3] - r[1] < 0.5:
        return None, r
    s = dpi / 72.0
    pix = pg.get_pixmap(matrix=fitz.Matrix(s, s), clip=fitz.Rect(*r), annots=True, alpha=False,
                        colorspace=fitz.csRGB)
    arr = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)[:, :, :3].copy()
    return arr, r


def overlay_crop(result_dir: Path, canvas: dict[str, Any], box: list[float],
                 dpi: float) -> tuple[np.ndarray | None, list[float] | None]:
    """Вырезка наложения движка по рамке в системе A, приведённая к ``dpi``.

    Возвращает (RGB, фактическая рамка в pt системы A) — рамка обрезается холстом.
    """
    try:
        data = tiles._load_masks(result_dir)
    except OSError:
        return None, None
    h, w = data["shape"]
    px = geo.to_px(canvas, box)
    x0, y0 = max(0, int(math.floor(px[0]))), max(0, int(math.floor(px[1])))
    x1, y1 = min(w, int(math.ceil(px[2]))), min(h, int(math.ceil(px[3])))
    if x1 <= x0 or y1 <= y0:
        return None, None
    layers = {name: tiles._unpack(packed, x0, y0, x1, y1) for name, packed in data["layers"].items()}
    rgb = engine.compose(layers, tiles.DEFAULT_LAYERS)
    f = dpi / float(canvas.get("dpi") or 200)
    if abs(f - 1.0) > 0.01:
        rgb = cv2.resize(rgb, (max(1, int(rgb.shape[1] * f)), max(1, int(rgb.shape[0] * f))),
                         interpolation=cv2.INTER_AREA if f < 1 else cv2.INTER_NEAREST)
    s = float(canvas.get("dpi") or 200) / 72.0
    ox, oy = canvas.get("origin_pt") or [0.0, 0.0]
    return rgb, [x0 / s + ox, y0 / s + oy, x1 / s + ox, y1 / s + oy]


def blank(w: int, h: int) -> np.ndarray:
    return np.full((max(1, h), max(1, w), 3), 255, np.uint8)


def draw_boxes(img: np.ndarray, boxes: list[tuple[str, list[float]]], dashed: bool = False) -> np.ndarray:
    """Рамки записей с подписями ID. ``boxes`` — (подпись, рамка в px изображения)."""
    im = Image.fromarray(img)
    dr = ImageDraw.Draw(im)
    f = font(14)
    for label, r in boxes:
        x0, y0, x1, y1 = [int(round(v)) for v in r]
        dr.rectangle([x0 - 2, y0 - 2, x1 + 2, y1 + 2], outline=LABEL_COLOR, width=2)
        tw = dr.textlength(label, font=f)
        ty = y0 - 20 if y0 >= 20 else y1 + 4
        dr.rectangle([x0 - 2, ty, x0 + tw + 4, ty + 17], fill=(255, 255, 255), outline=LABEL_COLOR)
        dr.text((x0 + 1, ty + 1), label, fill=LABEL_COLOR, font=f)
    return np.array(im)


def caption(img: np.ndarray, text: str) -> np.ndarray:
    w = max(img.shape[1], 10)
    bar = Image.new("RGB", (w, 26), (236, 239, 243))
    ImageDraw.Draw(bar).text((6, 4), text, fill=(20, 20, 20), font=font(15))
    if img.shape[1] < w:
        # Вырезка уже полосы подписи (тонкая линия чертежа) — дополнить белым справа, иначе склейка падает.
        img = np.hstack([img, blank(w - img.shape[1], img.shape[0])])
    return np.vstack([np.array(bar), img])


def stack(parts: list[np.ndarray], gap: int = 8) -> np.ndarray:
    w = max(p.shape[1] for p in parts)
    rows = []
    for k, p in enumerate(parts):
        if p.shape[1] < w:
            p = np.hstack([p, blank(w - p.shape[1], p.shape[0])])
        rows.append(p)
        if k < len(parts) - 1:
            rows.append(np.full((gap, w, 3), 120, np.uint8))
    return np.vstack(rows)


def rotate(img: np.ndarray, deg: int) -> np.ndarray:
    if deg == 90:
        return cv2.rotate(img, cv2.ROTATE_90_CLOCKWISE)
    if deg == 270:
        return cv2.rotate(img, cv2.ROTATE_90_COUNTERCLOCKWISE)
    if deg == 180:
        return cv2.rotate(img, cv2.ROTATE_180)
    return img


def rotate_box(r: list[float], w: int, h: int, deg: int) -> list[float]:
    """Рамка в px изображения w×h после поворота изображения на ``deg`` по часовой."""
    pts = [(r[0], r[1]), (r[2], r[3])]
    if deg == 90:
        pts = [(h - y, x) for x, y in pts]
    elif deg == 270:
        pts = [(y, w - x) for x, y in pts]
    elif deg == 180:
        pts = [(w - x, h - y) for x, y in pts]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    return [min(xs), min(ys), max(xs), max(ys)]


def save_png(img: np.ndarray, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    ok, buf = cv2.imencode(".png", cv2.cvtColor(img, cv2.COLOR_RGB2BGR))
    if not ok:
        raise ValueError(f"не удалось записать {path.name}")
    data = bytes(buf)
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def check_png(path: Path, sha: str | None = None) -> str | None:
    """Пригодность изображения перед отправкой. None — в порядке, иначе причина."""
    if not path.exists():
        return "файл изображения отсутствует"
    data = path.read_bytes()
    if not data:
        return "файл изображения пустой"
    if sha and hashlib.sha256(data).hexdigest() != sha:
        return "SHA-256 изображения не совпадает с манифестом"
    arr = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
    if arr is None or arr.size == 0:
        return "изображение не декодируется"
    return None
