# -*- coding: utf-8 -*-
"""Зонд 1: инвентаризация get_drawings по представительным PDF КЖ/КМ."""
import collections, math, sys
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин"
PDFS = [
    ("КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf", "kj_formwork"),
    ("КЖ/КЖ — 059 план армирования — нижнее армирование плиты — 9FER-NNAD-CHY.pdf", "kj_reinf_slab"),
    ("КЖ/КЖ — 091 сечение армирования — сечение армирования — 4K9U-VDRL-WTJ.pdf", "kj_section"),
    ("КЖ/КЖ — 001 закладные детали — узел закладной детали — 4HH4-XRCF-939.pdf", "kj_embedded"),
    ("КЖ/КЖ — 027 маркировочная схема — маркировочная схема — 6VCK-LXXM-N33.pdf", "kj_marking"),
    ("КМ/КМ — 001 узел соединения — соединение стальных элементов — 7NC9-69EK-9HF.pdf", "km_connection"),
    ("КМ/КМ — 045 монтажная схема — схема балкона или каркаса — 6N7X-XKUA-9JC.pdf", "km_layout"),
    ("КМ/КМ — 037 стремянка — чертёж стремянки — 6WTD-GG69-3CE.pdf", "km_ladder"),
]

def seg_len(p1, p2):
    return math.hypot(p2[0]-p1[0], p2[1]-p1[1])

for rel, tag in PDFS:
    path = f"{BASE}/{rel}"
    try:
        doc = fitz.open(path)
    except Exception as e:
        print(f"== {tag}: OPEN FAIL {e}")
        continue
    pg = doc[0]
    words = pg.get_text("words")
    dr = pg.get_drawings()
    item_hist = collections.Counter()
    fill_only = stroke_only = both = 0
    dash_vals = collections.Counter()
    width_vals = collections.Counter()
    short_segs = 0          # 1-5 pt отрезки (кандидаты в засечки)
    diag45 = 0              # короткие ~45°
    closed_c_loops = []     # окружности из c-items
    filled_small = []       # мелкие заливки (стрелки/точки)
    for d in dr:
        kinds = tuple(i[0] for i in d["items"])
        item_hist[kinds if len(kinds) <= 4 else (f"{len(kinds)}items",)] += 1
        has_fill = d.get("fill") is not None
        has_stroke = d.get("color") is not None
        if has_fill and has_stroke: both += 1
        elif has_fill: fill_only += 1
        else: stroke_only += 1
        dv = d.get("dashes")
        if dv and dv not in ("[] 0", "", None): dash_vals[str(dv)[:24]] += 1
        w = d.get("width")
        if w is not None: width_vals[round(float(w), 2)] += 1
        r = d["rect"]
        if has_fill:
            area = (r.x1-r.x0)*(r.y1-r.y0)
            if area < 40:  # мелкие заливки
                filled_small.append((round(r.x1-r.x0,1), round(r.y1-r.y0,1), kinds))
        # окружность из c-items
        if kinds and all(k == "c" for k in kinds) and len(kinds) >= 2:
            wdt, hgt = r.x1-r.x0, r.y1-r.y0
            if 0.75 < (wdt/(hgt+1e-6)) < 1.33 and wdt > 2:
                closed_c_loops.append(round((wdt+hgt)/2, 1))
        for it in d["items"]:
            if it[0] == "l":
                p1, p2 = (it[1].x, it[1].y), (it[2].x, it[2].y)
                L = seg_len(p1, p2)
                if 1.0 <= L <= 5.0:
                    short_segs += 1
                    dx, dy = abs(p2[0]-p1[0]), abs(p2[1]-p1[1])
                    if dx > 0.3*L and dy > 0.3*L: diag45 += 1
    diam_hist = collections.Counter(closed_c_loops)
    print(f"== {tag} | page {pg.rect.width:.0f}x{pg.rect.height:.0f} | words {len(words)} | drawings {len(dr)}")
    print(f"   kinds top: {item_hist.most_common(8)}")
    print(f"   stroke_only {stroke_only} fill_only {fill_only} both {both}")
    print(f"   dashes: {dash_vals.most_common(6) or 'NONE'}")
    print(f"   widths: {sorted(width_vals.items())[:10]}")
    print(f"   short segs 1-5pt: {short_segs} (diag~45: {diag45})")
    print(f"   c-loop diameters: {diam_hist.most_common(8) or 'none'}")
    print(f"   small filled (<40pt2) n={len(filled_small)} sample={filled_small[:6]}")
    doc.close()
