# -*- coding: utf-8 -*-
"""Probe 6: размерная цепочка = горизонтальная линия + засечки 45° на ней + размерные числа по пролётам."""
import math, re, sys, collections, statistics
import fitz

f = sys.argv[1]
doc = fitz.open(f); pg = doc[0]
words = pg.get_text("words")
dimw = [w for w in words if re.match(r"^\d{2,4}$", w[4])]

hlines = []; ticks = []
for d in pg.get_drawings():
    for it in d.get("items") or []:
        if it[0] != "l": continue
        p1, p2 = it[1], it[2]
        dx, dy = p2.x-p1.x, p2.y-p1.y
        L = math.hypot(dx, dy)
        if abs(dy) < 0.4 and L > 25:
            hlines.append((min(p1.x,p2.x), max(p1.x,p2.x), (p1.y+p2.y)/2))
        if 1.0 < L < 6.0 and dx and dy:
            ang = abs(math.degrees(math.atan2(dy, dx))) % 180
            if 30 < ang < 60 or 120 < ang < 150:
                ticks.append(((p1.x+p2.x)/2, (p1.y+p2.y)/2))

chains = 0; spans_ok = 0; spans_total = 0; samples = []
for (x0, x1, y) in hlines:
    on = sorted(set(round(tx,1) for tx, ty in ticks if abs(ty-y) < 1.2 and x0-2 <= tx <= x1+2))
    # схлопнуть близкие (двойные штрихи)
    merged = []
    for tx in on:
        if merged and tx - merged[-1] < 3: continue
        merged.append(tx)
    if len(merged) < 2: continue
    chains += 1
    for a, b in zip(merged, merged[1:]):
        spans_total += 1
        mid = (a+b)/2
        hit = None
        for w in dimw:
            cx, cy = (w[0]+w[2])/2, (w[1]+w[3])/2
            if abs(cx-mid) < max(6, (b-a)*0.25) and -14 < y-cy < 14:
                hit = w[4]; break
        if hit:
            spans_ok += 1
            if len(samples) < 10: samples.append((round(b-a,1), hit))
print(f"{f.split('—')[1].strip()}: hlines>25pt={len(hlines)}, ticks={len(ticks)}, цепочек(≥2 засечки)={chains}, пролётов={spans_total}, с числом={spans_ok}")
print("  примеры (span_pt, text):", samples)
# масштаб: mm/pt по совпавшим парам
ratios = []
for (x0,x1,y) in hlines:
    on = sorted(set(round(tx,1) for tx,ty in ticks if abs(ty-y)<1.2 and x0-2<=tx<=x1+2))
    merged=[]
    for tx in on:
        if merged and tx-merged[-1]<3: continue
        merged.append(tx)
    for a,b in zip(merged, merged[1:]):
        mid=(a+b)/2
        for w in dimw:
            cx,cy=(w[0]+w[2])/2,(w[1]+w[3])/2
            if abs(cx-mid)<max(6,(b-a)*0.25) and -14<y-cy<14 and b-a>3:
                ratios.append(float(w[4])/(b-a)); break
if ratios:
    print(f"  масштаб мм/pt: медиана={statistics.median(ratios):.2f} (n={len(ratios)}), p25={sorted(ratios)[len(ratios)//4]:.2f}, p75={sorted(ratios)[3*len(ratios)//4]:.2f}")
doc.close()
