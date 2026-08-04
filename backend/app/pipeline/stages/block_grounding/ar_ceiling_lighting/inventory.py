"""Полный инвентарь векторного слоя листа.

Тексты — из ``page.get_texttrace()`` (сохраняет слой, цвет, seqno и
координаты ОТДЕЛЬНЫХ символов), слова — из ``get_text("words")`` и
pdfplumber (кросс-сверка). Графика — из ``page.get_drawings(extended=True)``
с развёрткой ``re``/``qu`` в рёбра, Безье — в полилинии (де Кастельжо),
детекцией окружностей (full_arc и парные полудуги).
"""
from __future__ import annotations

import collections
import math

from .coords import CanonicalPage, span_text

# Цветовые кластеры листа. Пороги мягкие: точное равенство RGB запрещено,
# принадлежность проверяется семейством (см. classify_color).
COLOR_FAMILIES = ("red", "green", "blue", "black", "white", "gray", "other")


def classify_color(color) -> str:
    if color is None:
        return "none"
    r, g, b = (float(v) for v in color)
    mx, mn = max(r, g, b), min(r, g, b)
    if mn > 0.93:
        return "white"
    if mx < 0.17:
        return "black"
    if mx - mn < 0.07:
        return "gray"
    if r > 0.75 and g < 0.4 and b < 0.4:
        return "red"
    if g > 0.42 and g >= r + 0.12 and g >= b + 0.12:
        return "green"
    if b > 0.38 and b >= r + 0.12 and b >= g + 0.12:
        return "blue"
    return "other"


def _flatten_bezier(p0, p1, p2, p3, *, max_dev: float = 0.35) -> list[tuple[float, float]]:
    """Кубический Безье → полилиния; глубина деления от стрелы прогиба."""
    chord = math.hypot(p3[0] - p0[0], p3[1] - p0[1])
    dev = max(
        abs((p1[0] - p0[0]) * (p3[1] - p0[1]) - (p1[1] - p0[1]) * (p3[0] - p0[0])),
        abs((p2[0] - p0[0]) * (p3[1] - p0[1]) - (p2[1] - p0[1]) * (p3[0] - p0[0])),
    ) / (chord + 1e-9)
    steps = max(2, min(12, int(math.sqrt(dev / max_dev) * 4) + 2))
    pts = []
    for k in range(steps + 1):
        t = k / steps
        mt = 1 - t
        x = mt**3 * p0[0] + 3 * mt**2 * t * p1[0] + 3 * mt * t**2 * p2[0] + t**3 * p3[0]
        y = mt**3 * p0[1] + 3 * mt**2 * t * p1[1] + 3 * mt * t**2 * p2[1] + t**3 * p3[1]
        pts.append((x, y))
    return pts


def _try_circle(items, rect) -> dict | None:
    """Окружность: замкнутый контур из 2–6 дуг с почти квадратным bbox
    и постоянным радиусом всех опорных точек."""
    curves = [it for it in items if it[0] == "c"]
    if not curves or len(curves) != len(items) or not (2 <= len(curves) <= 6):
        return None
    w, h = rect[2] - rect[0], rect[3] - rect[1]
    if w < 1.0 or h < 1.0 or abs(w - h) > 0.18 * max(w, h):
        return None
    cx, cy = (rect[0] + rect[2]) / 2, (rect[1] + rect[3]) / 2
    r = (w + h) / 4
    pts = []
    for it in curves:
        pts.extend(((it[1].x, it[1].y), (it[4].x, it[4].y)))
    for x, y in pts:
        if abs(math.hypot(x - cx, y - cy) - r) > 0.14 * r + 0.35:
            return None
    return {"center": (round(cx, 2), round(cy, 2)), "d": round(2 * r, 2)}


def collect_inventory(cp: CanonicalPage) -> dict:
    """Сырой инвентарь: texts / drawings / segments / circles / quads / words."""
    page = cp.page
    texts = []
    for span in page.get_texttrace():
        text = span_text(span)
        if not text.strip():
            continue
        b = span["bbox"]
        texts.append({
            "tid": len(texts),
            "text": text,
            "bbox": tuple(round(v, 2) for v in b),
            "center": (round((b[0] + b[2]) / 2, 2), round((b[1] + b[3]) / 2, 2)),
            "layer": span.get("layer") or "",
            "color": tuple(round(float(v), 4) for v in span.get("color") or ()),
            "color_family": classify_color(span.get("color")),
            "size": round(float(span.get("size") or 0.0), 2),
            "seqno": int(span.get("seqno") or 0),
            "opacity": round(float(span.get("opacity") if span.get("opacity") is not None else 1.0), 3),
            "chars": [{"c": chr(ch[0]), "bbox": tuple(round(v, 2) for v in ch[3])}
                      for ch in span["chars"]],
        })

    drawings = []
    segments = []
    circles = []
    quads = []
    half_arcs = []  # кандидаты парных полудуг: (did, center, d)
    for did, d in enumerate(page.get_drawings(extended=True)):
        if d.get("type") not in ("f", "s", "fs"):
            continue
        rect = d.get("rect")
        if rect is None:
            continue
        r = (round(rect.x0, 2), round(rect.y0, 2), round(rect.x1, 2), round(rect.y1, 2))
        item_kinds = collections.Counter(it[0] for it in d["items"])
        rec = {
            "did": did,
            "layer": d.get("layer") or "",
            "type": d["type"],
            "rect": r,
            "center": (round((r[0] + r[2]) / 2, 2), round((r[1] + r[3]) / 2, 2)),
            "color": tuple(round(float(v), 4) for v in d.get("color") or ()) if d.get("color") else None,
            "fill": tuple(round(float(v), 4) for v in d.get("fill") or ()) if d.get("fill") else None,
            "color_family": classify_color(d.get("color")),
            "fill_family": classify_color(d.get("fill")),
            "stroke_width": round(float(d.get("width") or 0.0), 3),
            "opacity": round(float(d.get("stroke_opacity") if d.get("stroke_opacity") is not None else 1.0), 3),
            "seqno": int(d.get("seqno") or 0),
            "items": dict(sorted(item_kinds.items())),
        }
        drawings.append(rec)

        circle = _try_circle(d["items"], r)
        if circle is not None:
            circles.append({"cid": len(circles), "did": did, "layer": rec["layer"],
                            "color_family": rec["color_family"], "fill_family": rec["fill_family"],
                            "repr": "full_arc" if len(d["items"]) >= 3 else "half_pair_single",
                            **circle})
            continue  # окружность не дублируем сегментами

        if item_kinds.get("c") == 2 and len(d["items"]) == 2:
            # возможная половина окружности из пары дуг другого drawing —
            # оставляем и как сегменты, и как кандидата полудуги
            half_arcs.append({"did": did, "rect": r, "layer": rec["layer"],
                              "color_family": rec["color_family"]})

        for item in d["items"]:
            kind = item[0]
            if kind == "l":
                _add_segment(segments, did, rec, (item[1].x, item[1].y), (item[2].x, item[2].y), "l")
            elif kind == "re":
                q = item[1]
                pts = [(q.x0, q.y0), (q.x1, q.y0), (q.x1, q.y1), (q.x0, q.y1)]
                for a, b2 in zip(pts, pts[1:] + pts[:1]):
                    _add_segment(segments, did, rec, a, b2, "re")
                quads.append(_quad_record(quads, did, rec, pts, "re"))
            elif kind == "qu":
                q = item[1]
                pts = [(q.ul.x, q.ul.y), (q.ur.x, q.ur.y), (q.lr.x, q.lr.y), (q.ll.x, q.ll.y)]
                for a, b2 in zip(pts, pts[1:] + pts[:1]):
                    _add_segment(segments, did, rec, a, b2, "qu")
                quads.append(_quad_record(quads, did, rec, pts, "qu"))
            elif kind == "c":
                pts = _flatten_bezier((item[1].x, item[1].y), (item[2].x, item[2].y),
                                      (item[3].x, item[3].y), (item[4].x, item[4].y))
                for a, b2 in zip(pts, pts[1:]):
                    _add_segment(segments, did, rec, a, b2, "c")

    words = [{"text": str(w[4]).strip(), "bbox": tuple(round(v, 2) for v in w[:4])}
             for w in page.get_text("words") if str(w[4]).strip()]

    return {
        "texts": texts,
        "drawings": drawings,
        "segments": segments,
        "circles": circles,
        "quads": quads,
        "half_arcs": half_arcs,
        "words": words,
    }


def _add_segment(segments, did, rec, p1, p2, src_kind) -> None:
    if abs(p1[0] - p2[0]) < 1e-6 and abs(p1[1] - p2[1]) < 1e-6:
        return
    segments.append({
        "sid": len(segments),
        "did": did,
        "layer": rec["layer"],
        "color_family": rec["color_family"],
        "width": rec["stroke_width"],
        "kind": src_kind,
        "p1": (round(p1[0], 2), round(p1[1], 2)),
        "p2": (round(p2[0], 2), round(p2[1], 2)),
    })


def _quad_record(quads, did, rec, pts, src_kind) -> dict:
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return {
        "qid": len(quads),
        "did": did,
        "layer": rec["layer"],
        "color_family": rec["color_family"],
        "kind": src_kind,
        "bbox": (round(min(xs), 2), round(min(ys), 2), round(max(xs), 2), round(max(ys), 2)),
        "w": round(max(xs) - min(xs), 2),
        "h": round(max(ys) - min(ys), 2),
    }


def pair_half_arcs(inventory: dict) -> None:
    """Парные полудуги (две дуги + две дуги в соседнем drawing) → окружность.

    CAD-паттерн: эллипс экспортируется двумя drawings по 2 дуги. Пары ищем
    по почти совпадающему bbox.
    """
    candidates = inventory["half_arcs"]
    used: set[int] = set()
    by_key: dict[tuple[int, int, int, int], list[dict]] = collections.defaultdict(list)
    for h in candidates:
        key = tuple(int(v // 1.5) for v in h["rect"])
        by_key[key].append(h)
    for key in sorted(by_key):
        group = [h for h in by_key[key] if h["did"] not in used]
        while len(group) >= 2:
            a, b = group[0], group[1]
            group = group[2:]
            used.update((a["did"], b["did"]))
            r = a["rect"]
            w, h2 = r[2] - r[0], r[3] - r[1]
            if abs(w - h2) > 0.2 * max(w, h2, 1.0):
                continue
            inventory["circles"].append({
                "cid": len(inventory["circles"]),
                "did": a["did"],
                "did2": b["did"],
                "layer": a["layer"],
                "color_family": a["color_family"],
                "fill_family": "none",
                "repr": "paired_halves",
                "center": (round((r[0] + r[2]) / 2, 2), round((r[1] + r[3]) / 2, 2)),
                "d": round((w + h2) / 2, 2),
            })
