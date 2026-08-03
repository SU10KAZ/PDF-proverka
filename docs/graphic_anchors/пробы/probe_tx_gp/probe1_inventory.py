# -*- coding: utf-8 -*-
"""Probe 1: инвентарь get_drawings по представительным PDF ТХ/ГП.
Считает: item-типы, fill vs stroke, dashes, width-гистограмма, мелкие fill-объекты (кандидаты стрелок),
окружности из 'c'-items (кандидаты флажков отметок/осевых кружков)."""
import collections, math, sys
import fitz

FILES = sys.argv[1:]

def rect_of_items(items):
    xs, ys = [], []
    for it in items:
        for p in it[1:]:
            if isinstance(p, fitz.Point):
                xs.append(p.x); ys.append(p.y)
            elif isinstance(p, fitz.Rect):
                xs += [p.x0, p.x1]; ys += [p.y0, p.y1]
            elif isinstance(p, fitz.Quad):
                for q in (p.ul, p.ur, p.ll, p.lr):
                    xs.append(q.x); ys.append(q.y)
    if not xs: return None
    return (min(xs), min(ys), max(xs), max(ys))

for f in FILES:
    doc = fitz.open(f)
    pg = doc[0]
    drs = pg.get_drawings()
    words = pg.get_text("words")
    kinds = collections.Counter()
    fill_only = stroke_only = both = 0
    dashes_vals = collections.Counter()
    width_vals = collections.Counter()
    small_fills = []   # маленькие заливки — кандидаты стрелок/точек
    circles = []       # path из 2-4 'c'-items, замкнутый, почти квадратный bbox
    for d in drs:
        ktup = "".join(it[0] for it in d.get("items") or [])
        kinds[ktup if len(ktup) <= 6 else ktup[:6]+"+"] += 1
        has_f = d.get("fill") is not None
        has_s = d.get("color") is not None
        if has_f and has_s: both += 1
        elif has_f: fill_only += 1
        else: stroke_only += 1
        dsh = d.get("dashes")
        if dsh and dsh not in ("", "[] 0"):
            dashes_vals[str(dsh)[:30]] += 1
        w = d.get("width")
        if w is not None:
            width_vals[round(float(w), 2)] += 1
        r = d.get("rect")
        if has_f and r is not None:
            area = (r.x1 - r.x0) * (r.y1 - r.y0)
            if 0 < area < 60:  # маленькие заливки
                small_fills.append((round(r.x1-r.x0,1), round(r.y1-r.y0,1), len(d.get("items") or [])))
        its = d.get("items") or []
        ck = [it[0] for it in its]
        if 2 <= len(ck) <= 4 and all(c == "c" for c in ck) and r is not None:
            wdt, hgt = r.x1-r.x0, r.y1-r.y0
            if wdt > 0.5 and hgt > 0.5 and 0.6 < wdt/max(hgt,0.01) < 1.6:
                circles.append((round(wdt,1), round(hgt,1)))
    print("="*100)
    print(f, f"| page {pg.rect.width:.0f}x{pg.rect.height:.0f} | drawings={len(drs)} words={len(words)} text_chars={len(pg.get_text())}")
    print(" kinds top:", kinds.most_common(10))
    print(f" fill_only={fill_only} stroke_only={stroke_only} both={both}")
    print(" dashes:", dashes_vals.most_common(8))
    print(" widths:", sorted(width_vals.items())[:12])
    sf = collections.Counter(small_fills)
    print(" small fills (w,h,items):", sf.most_common(10))
    cc = collections.Counter(circles)
    print(" circle candidates (w,h):", cc.most_common(10))
    doc.close()
