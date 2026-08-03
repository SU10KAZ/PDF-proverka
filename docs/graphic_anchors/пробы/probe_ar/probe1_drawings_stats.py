#!/usr/bin/env python3
"""Проба 1: инвентаризация get_drawings по 6 представительным АР-PDF.
Что ищем: item-типы, dashes, width, fill-примитивы (стрелки/засечки/точки),
короткие сегменты под 45° (засечки размеров), окружности (осевые кружки)."""
import collections, math, sys
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/"
FILES = {
    "masonry_plan_7DU7": "АР — 145 кладочный план — кладочный план — 7DU7-346V-DN6.pdf",
    "facade_49XE": "АР — 061 фасад — фасадная развёртка — 49XE-EHU3-Y3V.pdf",
    "roof_detail_PDT9": "АР — 179 узел кровли — узел кровли — PDT9-6WQK-3PR.pdf",
    "opening_sketch_67AU": "АР — 157 эскиз заполнения проёма — эскиз двери или окна — 67AU-N79T-VUA.pdf",
    "wall_elev_69JM": "АР — 227 развёртка стены — развёртка стены — 69JM-X6EC-UTQ.pdf",
    "stair_6T9H": "АР — 216 лестница — план и разрез лестницы — 6T9H-T7HF-6AU.pdf",
}

def seg_len(p1, p2):
    return math.hypot(p2[0]-p1[0], p2[1]-p1[1])

for tag, fn in FILES.items():
    try:
        doc = fitz.open(BASE + fn)
    except Exception as e:
        print(f"== {tag}: OPEN FAIL {e}")
        continue
    pg = doc[0]
    drs = pg.get_drawings()
    item_types = collections.Counter()
    fill_only = stroke_only = both = 0
    dashed = collections.Counter()
    widths = collections.Counter()
    fill_areas = []          # мелкие заливки (кандидаты стрелки/точки)
    short_diag = []          # короткие диагональные сегменты (засечки)
    short_any = collections.Counter()
    curves = 0
    rects = 0
    n_segs = 0
    for d in drs:
        has_fill = d.get("fill") is not None
        has_stroke = d.get("color") is not None
        if has_fill and has_stroke: both += 1
        elif has_fill: fill_only += 1
        else: stroke_only += 1
        da = d.get("dashes")
        if da and da not in ("[] 0", "", None):
            dashed[str(da)[:40]] += 1
        w = d.get("width")
        if w is not None:
            widths[round(float(w), 2)] += 1
        r = d.get("rect")
        if has_fill:
            area = (r.x1-r.x0)*(r.y1-r.y0)
            if area < 30:
                fill_areas.append((round(area,2), round(r.x1-r.x0,2), round(r.y1-r.y0,2), len(d.get("items",[]))))
        for it in d.get("items", []):
            item_types[it[0]] += 1
            if it[0] == "l":
                n_segs += 1
                L = seg_len(it[1], it[2])
                if L < 8:
                    short_any[round(L)] += 1
                    dx = abs(it[2][0]-it[1][0]); dy = abs(it[2][1]-it[1][1])
                    if L > 0.5 and dx > 0.2*L and dy > 0.2*L:
                        short_diag.append(round(L,2))
            elif it[0] == "c":
                curves += 1
            elif it[0] in ("re","qu"):
                rects += 1
    print(f"== {tag} | page {pg.rect.width:.0f}x{pg.rect.height:.0f} | drawings={len(drs)} words={len(pg.get_text('words'))}")
    print(f"   items: {dict(item_types)} | stroke_only={stroke_only} fill_only={fill_only} both={both}")
    print(f"   dashes(top5): {dashed.most_common(5)}")
    print(f"   widths(top8): {widths.most_common(8)}")
    print(f"   fill<30pt2: n={len(fill_areas)} sample={sorted(fill_areas)[:8]}")
    sd = collections.Counter(round(x) for x in short_diag)
    print(f"   short diag segs(<8pt, len hist): {dict(sorted(sd.items()))} total={len(short_diag)}")
    print(f"   curves={curves} rect/quad_items={rects}")
    doc.close()
