# -*- coding: utf-8 -*-
"""Проба 2: детектор размера — засечки на размерной линии + число у середины.

Гипотеза: размерная цепочка = длинная тонкая линия (H или V), на ней засечки 45°
(короткие сегменты, середина ЛЕЖИТ на линии), между соседними засечками — число.
Проверяем согласование: подпись N мм / расстояние засечек в pt = масштаб ~const.
"""
import math, re
from collections import Counter
import fitz

FILES = [
    ("AR_uzel_019", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 019 архитектурный узел — архитектурный узел — 9HTK-Y74V-UJ6.pdf"),
    ("AR_plan_097", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 097 архитектурный план — план этажа — 9KPA-UAWT-9RF.pdf"),
    ("KJ_opalubka_017", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf"),
    ("KJ_balka_035", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 035 план армирования — армирование балки — FCTE-JGDT-L6X.pdf"),
]

NUM_RE = re.compile(r"^\d{2,5}$")

def all_segments(pg):
    """Все l-сегменты, включая внутри multi-item путей."""
    segs = []
    for di, d in enumerate(pg.get_drawings()):
        if d.get("fill") is not None and d.get("color") is None:
            continue  # чистые заливки — отдельно
        for ii, it in enumerate(d.get("items") or []):
            if it[0] != "l":
                continue
            a, b = it[1], it[2]
            L = math.hypot(b.x - a.x, b.y - a.y)
            if L < 0.3:
                continue
            segs.append({"p1": (a.x, a.y), "p2": (b.x, b.y), "len": L,
                         "w": float(d.get("width") or 0), "di": di})
    return segs

def pt_seg_dist(p, s):
    px, py = p; (x1, y1), (x2, y2) = s["p1"], s["p2"]
    dx, dy = x2-x1, y2-y1
    if dx == 0 and dy == 0:
        return math.hypot(px-x1, py-y1)
    t = max(0.0, min(1.0, ((px-x1)*dx+(py-y1)*dy)/(dx*dx+dy*dy)))
    return math.hypot(px-(x1+t*dx), py-(y1+t*dy))

for tag, path in FILES:
    doc = fitz.open(path); pg = doc[0]
    segs = all_segments(pg)
    words = pg.get_text("words")
    num_words = [w for w in words if NUM_RE.match(w[4])]

    # засечки: короткие диагональные ~45°
    ticks = []
    for s in segs:
        dx, dy = abs(s["p2"][0]-s["p1"][0]), abs(s["p2"][1]-s["p1"][1])
        if 1.0 <= s["len"] <= 8.0 and dx > 0.3 and dy > 0.3:
            ang = math.degrees(math.atan2(dy, dx))
            if 35 <= ang <= 55:
                mid = ((s["p1"][0]+s["p2"][0])/2, (s["p1"][1]+s["p2"][1])/2)
                ticks.append({"mid": mid, "len": s["len"], "ang": ang, "w": s["w"]})
    print(f"== {tag}: segs={len(segs)} ticks45={len(ticks)} num_words={len(num_words)}")
    tick_len_hist = Counter(round(t["len"], 0) for t in ticks)
    print(f"   tick len hist: {dict(sorted(tick_len_hist.items()))}")

    # длинные H/V линии
    hv = [s for s in segs if s["len"] >= 15 and
          (abs(s["p2"][0]-s["p1"][0]) < 0.5 or abs(s["p2"][1]-s["p1"][1]) < 0.5)]

    # размерная линия: H/V линия, на которой >=2 засечки (dist<=0.7)
    dim_lines = []
    for s in hv:
        on = [t for t in ticks if pt_seg_dist(t["mid"], s) <= 0.7]
        if len(on) >= 2:
            horiz = abs(s["p2"][1]-s["p1"][1]) < 0.5
            on_sorted = sorted(on, key=lambda t: t["mid"][0] if horiz else t["mid"][1])
            dim_lines.append({"s": s, "ticks": on_sorted, "horiz": horiz})
    print(f"   dim_lines(>=2 ticks): {len(dim_lines)}")

    # интервалы между соседними засечками + ближайшее число
    ratios = []
    matched = 0; unmatched = 0
    for dl in dim_lines[:400]:
        s = dl["s"]; horiz = dl["horiz"]
        for t1, t2 in zip(dl["ticks"], dl["ticks"][1:]):
            gap = (t2["mid"][0]-t1["mid"][0]) if horiz else (t2["mid"][1]-t1["mid"][1])
            if gap < 3:
                continue
            cx = (t1["mid"][0]+t2["mid"][0])/2; cy = (t1["mid"][1]+t2["mid"][1])/2
            best = None
            for w in num_words:
                wx, wy = (w[0]+w[2])/2, (w[1]+w[3])/2
                if horiz:
                    if abs(wx-cx) < gap/2+6 and -14 <= (cy-wy) <= 14:
                        d = abs(wx-cx)+abs(cy-wy)
                        if best is None or d < best[0]: best = (d, w)
                else:
                    if abs(wy-cy) < gap/2+6 and abs(wx-cx) <= 14:
                        d = abs(wy-cy)+abs(wx-cx)
                        if best is None or d < best[0]: best = (d, w)
            if best:
                matched += 1
                val = int(best[1][4])
                if val > 0 and gap > 1:
                    ratios.append(round(val/gap, 2))
            else:
                unmatched += 1
    print(f"   intervals: matched_num={matched} unmatched={unmatched}")
    rc = Counter(ratios)
    print(f"   scale mm/pt top6: {rc.most_common(6)}  (постоянство = размер согласован)")
    doc.close()
