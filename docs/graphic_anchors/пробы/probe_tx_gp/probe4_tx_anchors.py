# -*- coding: utf-8 -*-
"""Probe 4 (ТХ): (а) осевые кружки — окружность из c-items Ø15..25 + ровно одна марка внутри;
(б) размерные засечки/стрелки — тонкие fill-сливеры возле размерных чисел;
(в) флажки отметок на разрезе — fill ~5.6x10 возле [+-]N,NNN."""
import math, re, sys, collections
import fitz

f = sys.argv[1]
doc = fitz.open(f); pg = doc[0]
words = pg.get_text("words")

def tp(x, y):
    if not pg.rotation: return float(x), float(y)
    p = fitz.Point(float(x), float(y)) * pg.rotation_matrix
    return p.x, p.y

AX = re.compile(r"^(?:[А-ЯA-Z]|\d{1,2})(?:\.[А-ЯA-Z0-9]{1,3})?$")
DIM = re.compile(r"^\d{2,5}$")
ELEV = re.compile(r"^[+\-]\d{1,3}[,.]\d{3}$")

circles = []; slivers = []; flags = []
for d in pg.get_drawings():
    its = d.get("items") or []
    ck = [it[0] for it in its]
    r = d.get("rect")
    if r is None: continue
    x0, y0 = tp(r.x0, r.y0); x1, y1 = tp(r.x1, r.y1)
    x0, x1 = min(x0,x1), max(x0,x1); y0, y1 = min(y0,y1), max(y0,y1)
    w, h = x1-x0, y1-y0
    if 2 <= len(ck) <= 4 and all(c == "c" for c in ck) and 13 <= w <= 26 and 0.85 < w/max(h,0.01) < 1.18:
        circles.append((x0, y0, x1, y1, round(w,1)))
    if d.get("fill") is not None:
        if 1.5 < max(w,h) < 4.5 and min(w,h) < 0.5:
            slivers.append(((x0+x1)/2, (y0+y1)/2, round(w,1), round(h,1)))
        if 3 < w < 9 and 6 < h < 14:
            flags.append(((x0+x1)/2, (y0+y1)/2, round(w,1), round(h,1)))

print(f"{f.split('—')[1].strip()}: circles(13..26)={len(circles)} slivers={len(slivers)} flag-fills={len(flags)}; words={len(words)}")

# (а) марка внутри кружка
ok = 0; ex = []
for (x0, y0, x1, y1, dw) in circles:
    inside = [w for w in words if x0-1 < (w[0]+w[2])/2 < x1+1 and y0-1 < (w[1]+w[3])/2 < y1+1]
    if len(inside) == 1 and AX.match(inside[0][4]):
        ok += 1
        if len(ex) < 8: ex.append((inside[0][4], dw))
print(f"  осевые кружки с ровно одной маркой внутри: {ok}/{len(circles)}; примеры: {ex}")

# (б) засечки возле размерных чисел
dimw = [w for w in words if DIM.match(w[4])]
if dimw and slivers:
    import statistics
    ds = []
    for w in dimw:
        cx, cy = (w[0]+w[2])/2, (w[1]+w[3])/2
        best = min(math.hypot(sx-cx, sy-cy) for sx, sy, _, _ in slivers)
        ds.append(best)
    ds.sort()
    print(f"  размерных чисел={len(dimw)}; дистанция до ближайшего сливера: min={ds[0]:.1f} median={statistics.median(ds):.1f}; <15pt: {sum(1 for x in ds if x<15)}/{len(ds)}")

# (в) флажки у отметок
elevw = [w for w in words if ELEV.match(w[4])]
if elevw:
    got = 0; exf = []
    for w in elevw:
        cx, cy = (w[0]+w[2])/2, (w[1]+w[3])/2
        near = [fl for fl in flags if math.hypot(fl[0]-cx, fl[1]-cy) < 22]
        if near:
            got += 1
            if len(exf) < 5: exf.append((w[4], round(math.hypot(near[0][0]-cx, near[0][1]-cy),1), near[0][2], near[0][3]))
    print(f"  отметок={len(elevw)}; с fill-флажком в 22pt: {got}; примеры (текст, дист, w, h): {exf}")
doc.close()
