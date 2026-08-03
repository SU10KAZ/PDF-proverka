#!/usr/bin/env python3
"""Проба 2: анатомия размерной линии на кладочном плане и развёртке.
Гипотеза ГОСТ 21.501: размер = число над размерной линией; линия несёт засечки 45°
в точках выносных линий; выносные — перпендикулярны размерной.
Проверяем: для каждого числового слова ищем ближайшую длинную линию (гор/верт),
на ней — короткие 45°-сегменты (засечки), от засечек — перпендикулярные выносные."""
import collections, math
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/"
FILES = {
    "masonry_7DU7": "АР — 145 кладочный план — кладочный план — 7DU7-346V-DN6.pdf",
    "wall_elev_69JM": "АР — 227 развёртка стены — развёртка стены — 69JM-X6EC-UTQ.pdf",
}
import re
NUM = re.compile(r"^\d{2,5}$")

def segments(pg):
    out = []
    for d in pg.get_drawings():
        w = d.get("width") or 0.0
        for it in d.get("items", []):
            if it[0] == "l":
                p1 = (it[1].x, it[1].y); p2 = (it[2].x, it[2].y)
                L = math.hypot(p2[0]-p1[0], p2[1]-p1[1])
                if L > 0.3:
                    out.append({"p1": p1, "p2": p2, "len": L, "w": w})
    return out

def is_h(s, tol=0.5): return abs(s["p1"][1]-s["p2"][1]) <= tol
def is_v(s, tol=0.5): return abs(s["p1"][0]-s["p2"][0]) <= tol
def is_diag45(s):
    dx = abs(s["p1"][0]-s["p2"][0]); dy = abs(s["p1"][1]-s["p2"][1])
    return dx > 0.3 and dy > 0.3 and 0.5 < dx/max(dy,1e-6) < 2.0

for tag, fn in FILES.items():
    doc = fitz.open(BASE + fn); pg = doc[0]
    segs = segments(pg)
    hsegs = [s for s in segs if is_h(s) and s["len"] >= 6]
    vsegs = [s for s in segs if is_v(s) and s["len"] >= 6]
    ticks = [s for s in segs if is_diag45(s) and 1.0 <= s["len"] <= 6.0]
    words = pg.get_text("words")
    nums = [w for w in words if NUM.match(w[4])]
    print(f"== {tag}: segs={len(segs)} h(>=6)={len(hsegs)} v(>=6)={len(vsegs)} ticks45(1-6pt)={len(ticks)} num_words={len(nums)}")

    # длины засечек — гистограмма
    tl = collections.Counter(round(s["len"],1) for s in ticks)
    print(f"   tick len hist top: {tl.most_common(8)}")
    tw = collections.Counter(s["w"] for s in ticks)
    print(f"   tick width hist: {tw.most_common(5)}")

    # для каждого числа: ближайшая горизонтальная линия ПОД текстом (или верт. рядом)
    linked = 0; tick_confirmed = 0; details = []
    for w in nums[:400]:
        x0,y0,x1,y1,txt = w[0],w[1],w[2],w[3],w[4]
        cx, cy = (x0+x1)/2, (y0+y1)/2
        # горизонтальный кандидат: линия с y в [y1, y1+6], перекрывающая cx
        best = None
        for s in hsegs:
            sy = s["p1"][1]
            if y1-1 <= sy <= y1+7 and min(s["p1"][0],s["p2"][0])-2 <= cx <= max(s["p1"][0],s["p2"][0])+2:
                d = sy - y1
                if best is None or d < best[0]: best = (d, s, "h")
        # вертикальный кандидат: линия с x в [x1, x1+7] (число повернуто) или [x0-7,x0]
        for s in vsegs:
            sx = s["p1"][0]
            if (x1-1 <= sx <= x1+7 or x0-7 <= sx <= x0+1) and min(s["p1"][1],s["p2"][1])-2 <= cy <= max(s["p1"][1],s["p2"][1])+2:
                d = min(abs(sx-x1), abs(sx-x0))
                if best is None or d < best[0]: best = (d, s, "v")
        if not best: continue
        linked += 1
        d, line, ori = best
        # засечки на этой линии: центр засечки на линии +-1.2, в пределах отрезка
        tk = []
        for t in ticks:
            mx = (t["p1"][0]+t["p2"][0])/2; my = (t["p1"][1]+t["p2"][1])/2
            if ori == "h":
                if abs(my - line["p1"][1]) <= 1.5 and min(line["p1"][0],line["p2"][0])-1 <= mx <= max(line["p1"][0],line["p2"][0])+1:
                    tk.append(mx)
            else:
                if abs(mx - line["p1"][0]) <= 1.5 and min(line["p1"][1],line["p2"][1])-1 <= my <= max(line["p1"][1],line["p2"][1])+1:
                    tk.append(my)
        if len(tk) >= 2:
            tick_confirmed += 1
            if len(details) < 6:
                tk.sort()
                span = tk[-1]-tk[0]
                details.append((txt, ori, round(line["len"],1), len(tk), round(span,1)))
    print(f"   nums linked to line: {linked}/{min(len(nums),400)}; with >=2 ticks: {tick_confirmed}")
    print(f"   samples (num, ori, line_len, n_ticks, tick_span_pt): {details}")
    doc.close()
