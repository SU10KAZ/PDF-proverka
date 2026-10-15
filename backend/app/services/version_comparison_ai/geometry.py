"""Координаты: один контракт для сборщика, вырезок и перехода в просмотрщик.

Контракт движка (``engine._rect_b_to_a``, ``result.transform.matrix``):

* все рамки — pt видимой страницы (после ``/Rotate``), начало вверху слева,
  X вправо, Y вниз (``pdfio.to_display``);
* система наложения (холст) — система листа **A**: pt холста = pt страницы A;
* ``transform.matrix`` — аффинная 2×3, переводит точку **страницы B в систему A**;
  обратная матрица переводит систему A на страницу B;
* пиксели холста: ``px = (pt − canvas.origin_pt) · canvas.dpi / 72``.
"""
from __future__ import annotations

from typing import Any, Iterable

Rect = list[float]


def apply(m: list[list[float]], x: float, y: float) -> tuple[float, float]:
    return m[0][0] * x + m[0][1] * y + m[0][2], m[1][0] * x + m[1][1] * y + m[1][2]


def invert(m: list[list[float]]) -> list[list[float]]:
    a, b, c = m[0]
    d, e, f = m[1]
    det = a * e - b * d
    if abs(det) < 1e-12:
        raise ValueError("матрица совмещения вырождена")
    ia, ib, id_, ie = e / det, -b / det, -d / det, a / det
    return [[ia, ib, -(ia * c + ib * f)], [id_, ie, -(id_ * c + ie * f)]]


def map_rect(m: list[list[float]], r: Rect) -> Rect:
    pts = [apply(m, x, y) for x, y in ((r[0], r[1]), (r[2], r[1]), (r[0], r[3]), (r[2], r[3]))]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    return [min(xs), min(ys), max(xs), max(ys)]


def b_page_to_a(transform: dict[str, Any], r: Rect) -> Rect:
    """Рамка страницы B → система A (холст). Прямая матрица движка."""
    return map_rect(transform["matrix"], r)


def a_to_b_page(transform: dict[str, Any], r: Rect) -> Rect:
    """Система A (холст) → страница B. Обратная матрица."""
    return map_rect(invert(transform["matrix"]), r)


def to_px(canvas: dict[str, Any], r: Rect) -> list[float]:
    s = float(canvas.get("dpi") or 200) / 72.0
    ox, oy = canvas.get("origin_pt") or [0.0, 0.0]
    return [(r[0] - ox) * s, (r[1] - oy) * s, (r[2] - ox) * s, (r[3] - oy) * s]


def union(rects: Iterable[Rect | None]) -> Rect | None:
    rs = [r for r in rects if r]
    if not rs:
        return None
    return [min(r[0] for r in rs), min(r[1] for r in rs), max(r[2] for r in rs), max(r[3] for r in rs)]


def expand(r: Rect, dx: float, dy: float | None = None) -> Rect:
    dy = dx if dy is None else dy
    return [r[0] - dx, r[1] - dy, r[2] + dx, r[3] + dy]


def contains(outer: Rect, inner: Rect) -> bool:
    return inner[0] >= outer[0] and inner[1] >= outer[1] and inner[2] <= outer[2] and inner[3] <= outer[3]


def intersects(a: Rect, b: Rect) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def clip(r: Rect, w: float, h: float) -> Rect:
    return [max(0.0, r[0]), max(0.0, r[1]), min(w, r[2]), min(h, r[3])]


def rounded(r: Rect | None, nd: int = 2) -> Rect | None:
    return None if r is None else [round(float(v), nd) for v in r]
