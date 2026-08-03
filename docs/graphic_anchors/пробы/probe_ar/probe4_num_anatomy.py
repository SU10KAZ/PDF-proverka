#!/usr/bin/env python3
"""Проба 4: что реально лежит вокруг 3-5-значного числа на кладочном плане.
Дамп сегментов в окне ±12pt вокруг bbox числа (ориентация, длина, ширина, смещение)."""
import math, re, collections
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/"
FN = "АР — 145 кладочный план — кладочный план — 7DU7-346V-DN6.pdf"
NUM = re.compile(r"^\d{3,5}$")

doc = fitz.open(BASE + FN); pg = doc[0]
segs = []
for d in pg.get_drawings():
    for it in d.get("items", []):
        if it[0] == "l":
            p1 = (it[1].x, it[1].y); p2 = (it[2].x, it[2].y)
            L = math.hypot(p2[0]-p1[0], p2[1]-p1[1])
            if L > 0.4: segs.append({"p1": p1, "p2": p2, "len": L, "w": d.get("width") or 0})
words = [w for w in pg.get_text("words") if NUM.match(w[4])]
print(f"3-5 digit numbers: {len(words)}; values top: {collections.Counter(w[4] for w in words).most_common(10)}")

def ori(s):
    dx = abs(s["p1"][0]-s["p2"][0]); dy = abs(s["p1"][1]-s["p2"][1])
    if dy <= 0.4: return "H"
    if dx <= 0.4: return "V"
    r = dx/max(dy,1e-9)
    return "D45" if 0.5 < r < 2 else "D"

for w in words[:8]:
    x0,y0,x1,y1,txt = w[0],w[1],w[2],w[3],w[4]
    print(f"-- '{txt}' bbox=({x0:.0f},{y0:.0f},{x1:.0f},{y1:.0f}) h={y1-y0:.1f}")
    near = []
    for s in segs:
        mx = (s["p1"][0]+s["p2"][0])/2; my = (s["p1"][1]+s["p2"][1])/2
        if x0-14 <= mx <= x1+14 and y0-14 <= my <= y1+14:
            near.append((ori(s), round(s["len"],1), round(s["w"],2), round(mx-(x0+x1)/2,1), round(my-(y0+y1)/2,1)))
    near.sort(key=lambda t: abs(t[3])+abs(t[4]))
    print(f"   near segs n={len(near)}: {near[:14]}")
doc.close()
