# -*- coding: utf-8 -*-
"""Проба 1: инвентарь примитивов get_drawings на АР/КЖ вырезках.

Смотрим: типы items (l/re/qu/c), fill vs stroke, dashes, width, мелкие заливки.
"""
import sys, math, json
from collections import Counter
import fitz

FILES = [
    ("AR_uzel_019", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 019 архитектурный узел — архитектурный узел — 9HTK-Y74V-UJ6.pdf"),
    ("AR_fasad_uzel_033", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 033 архитектурный узел — фасадный узел — NWHW-Y3M3-FRP.pdf"),
    ("AR_plan_097", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 097 архитектурный план — план этажа — 9KPA-UAWT-9RF.pdf"),
    ("KJ_opalubka_017", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf"),
    ("KJ_balka_035", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 035 план армирования — армирование балки — FCTE-JGDT-L6X.pdf"),
    ("KJ_zakladnaya_001", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 001 закладные детали — узел закладной детали — 4HH4-XRCF-939.pdf"),
]

def seg_len(a, b):
    return math.hypot(b.x - a.x, b.y - a.y)

for tag, path in FILES:
    try:
        doc = fitz.open(path)
    except Exception as e:
        print(f"== {tag}: OPEN FAIL {e}")
        continue
    pg = doc[0]
    dr = pg.get_drawings()
    kinds = Counter()
    fill_only = stroke_only = both = 0
    dash_counter = Counter()
    width_counter = Counter()
    small_fills = []          # мелкие заливки (кандидаты: стрелки/точки/флажки)
    tick_candidates = []      # короткие сегменты под ~45°
    circles = []              # drawings из c-items c почти квадратным bbox
    for d in dr:
        items = d.get("items") or []
        for it in items:
            kinds[it[0]] += 1
        has_fill = d.get("fill") is not None
        has_stroke = d.get("color") is not None
        if has_fill and has_stroke: both += 1
        elif has_fill: fill_only += 1
        elif has_stroke: stroke_only += 1
        dsh = d.get("dashes")
        if dsh and dsh not in ("[] 0", "", None):
            dash_counter[str(dsh)] += 1
        w = d.get("width")
        if w is not None:
            width_counter[round(float(w), 2)] += 1
        r = d.get("rect")
        if has_fill and r is not None:
            area = float(r.width) * float(r.height)
            if area < 60:  # мелкая заливка
                small_fills.append({
                    "w": round(float(r.width), 2), "h": round(float(r.height), 2),
                    "kinds": "".join(it[0] for it in items),
                    "n_items": len(items),
                    "fill": tuple(round(v, 2) for v in (d.get("fill") or ())),
                })
        # кандидаты-засечки: одиночный l-item, короткий, ~45°
        if not has_fill and len(items) == 1 and items[0][0] == "l":
            a, b = items[0][1], items[0][2]
            L = seg_len(a, b)
            if 0.5 <= L <= 8.0:
                dx, dy = abs(b.x - a.x), abs(b.y - a.y)
                if dx > 0.1 and dy > 0.1:
                    ang = math.degrees(math.atan2(dy, dx))
                    if 30 <= ang <= 60:
                        tick_candidates.append({"len": round(L, 2), "ang": round(ang, 1),
                                                "w": round(float(d.get("width") or 0), 2),
                                                "dashes": str(d.get("dashes"))[:20]})
        # кандидаты-окружности: только c-items, bbox почти квадратный
        if items and all(it[0] == "c" for it in items) and r is not None:
            wdt, hgt = float(r.width), float(r.height)
            if 2 <= wdt <= 40 and hgt > 0 and 0.8 <= wdt / hgt <= 1.25:
                circles.append({"d": round((wdt + hgt) / 2, 2), "n_c": len(items),
                                "fill": d.get("fill") is not None,
                                "cx": round((r.x0+r.x1)/2,1), "cy": round((r.y0+r.y1)/2,1)})
    print(f"== {tag} | page {pg.rect.width:.0f}x{pg.rect.height:.0f} | drawings={len(dr)}")
    print(f"   kinds={dict(kinds)} | stroke_only={stroke_only} fill_only={fill_only} both={both}")
    print(f"   dashes(top6)={dash_counter.most_common(6)}")
    print(f"   widths(top8)={sorted(width_counter.items())[:8]} ... total_distinct={len(width_counter)}")
    sf_sizes = Counter((s["w"], s["h"], s["kinds"]) for s in small_fills)
    print(f"   small_fills={len(small_fills)} top8={sf_sizes.most_common(8)}")
    tick_lens = Counter(round(t["len"]) for t in tick_candidates)
    print(f"   tick45_candidates={len(tick_candidates)} len_hist={dict(sorted(tick_lens.items()))} sample={tick_candidates[:4]}")
    circ_d = Counter(round(c["d"]) for c in circles)
    print(f"   circle_candidates={len(circles)} d_hist={dict(sorted(circ_d.items()))} sample={circles[:5]}")
    doc.close()
