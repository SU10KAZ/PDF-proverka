# -*- coding: utf-8 -*-
"""Probe 2b: слова вокруг вытянутых fill-стрелок (w>=8 или h>=8, aspect>2.5) на планах ГП.
Плюс общая статистика типов слов."""
import math, re, sys, collections
import fitz

f = sys.argv[1]
doc = fitz.open(f); pg = doc[0]
words = pg.get_text("words")

pat = collections.Counter()
for w in words:
    t = w[4]
    if re.match(r"^\d{1,3}[,.]\d{2}$", t): pat["NN,NN (отметка без знака)"] += 1
    elif re.match(r"^[+\-]\d", t): pat["±N (знаковая)"] += 1
    elif re.match(r"^\d+$", t): pat["целое"] += 1
    elif re.match(r"^\d+[,.]\d+$", t): pat["число с дробью"] += 1
    else: pat["текст"] += 1
print("word classes:", pat.most_common())

def verts(d):
    vs = []
    for it in d.get("items") or []:
        if it[0] == "l": vs += [(it[1].x, it[1].y), (it[2].x, it[2].y)]
    return vs

longarrows = []
for d in pg.get_drawings():
    if d.get("fill") is None: continue
    r = d.get("rect")
    if r is None: continue
    w, h = r.x1-r.x0, r.y1-r.y0
    if max(w, h) < 8 or max(w, h) > 40: continue
    if max(w,h)/max(min(w,h),0.05) < 2.5: continue
    longarrows.append((r, verts(d)))
print(f"long-arrow fills (8..40pt, aspect>2.5): {len(longarrows)}")

samples = 0
for r, vs in longarrows:
    cx, cy = (r.x0+r.x1)/2, (r.y0+r.y1)/2
    near = []
    for w in words:
        wx, wy = (w[0]+w[2])/2, (w[1]+w[3])/2
        d0 = math.hypot(wx-cx, wy-cy)
        if d0 < 28: near.append((round(d0,1), w[4], round(wy-cy,1)))
    if near and samples < 12:
        near.sort()
        horiz = (r.x1-r.x0) > (r.y1-r.y0)
        print(f"  arrow@({cx:.0f},{cy:.0f}) {r.x1-r.x0:.1f}x{r.y1-r.y0:.1f} {'horiz' if horiz else 'vert'} near:", near[:5])
        samples += 1
doc.close()
