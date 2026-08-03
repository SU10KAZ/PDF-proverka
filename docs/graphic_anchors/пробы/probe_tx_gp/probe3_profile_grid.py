# -*- coding: utf-8 -*-
"""Probe 3: сетка продольного профиля водоотвода — длинные вертикали (ординаты) и горизонтали
(строки подвала таблицы), их выравнивание со словами (отметки, расстояния, уклоны)."""
import math, re, sys, collections
import fitz

f = sys.argv[1]
doc = fitz.open(f); pg = doc[0]
W, H = pg.rect.width, pg.rect.height
words = pg.get_text("words")
print(f"page {W:.0f}x{H:.0f} rot={pg.rotation}, words={len(words)}")

def tp(x, y):
    if not pg.rotation: return float(x), float(y)
    p = fitz.Point(float(x), float(y)) * pg.rotation_matrix
    return p.x, p.y

vlines = []; hlines = []
for d in pg.get_drawings():
    wdt = d.get("width") or 0
    for it in d.get("items") or []:
        if it[0] != "l": continue
        a = tp(it[1].x, it[1].y); b = tp(it[2].x, it[2].y)
        p1 = fitz.Point(a); p2 = fitz.Point(b)
        dx, dy = abs(p1.x-p2.x), abs(p1.y-p2.y)
        if dx < 0.5 and dy > 25: vlines.append((p1.x, min(p1.y,p2.y), max(p1.y,p2.y), dy, wdt))
        if dy < 0.5 and dx > 60: hlines.append((p1.y, min(p1.x,p2.x), max(p1.x,p2.x), dx, wdt))
print(f"verticals(>25pt)={len(vlines)} horizontals(>60pt)={len(hlines)}")

# кластеризация горизонталей по Y (строки подвала)
ys = sorted(set(round(h[0],0) for h in hlines))
groups = []
for y in ys:
    if groups and y - groups[-1][-1] <= 3: groups[-1].append(y)
    else: groups.append([y])
print(f"distinct horizontal Y-rows: {len(groups)}; Y positions: {[round(g[0]) for g in groups][:40]}")

# слова-заголовки подвала
hdr = [w for w in words if re.search(r"отметк|расстоян|уклон|обознач|пикет|дно|лотк", w[4], re.I)]
print("header words:", [(w[4], round(w[1])) for w in hdr[:20]])

# вертикали: длинные ординаты профиля (проходящие через график и таблицу)
long_v = [v for v in vlines if v[3] > 100]
xs = sorted(v[0] for v in long_v)
xg = []
for x in xs:
    if xg and x - xg[-1][-1] <= 2: xg[-1].append(x)
    else: xg.append([x])
print(f"long verticals(>100pt): {len(long_v)}, X-clusters: {len(xg)}")
steps = [round(xg[i+1][0]-xg[i][0],1) for i in range(len(xg)-1)]
print("X-cluster steps:", steps[:30])

# числа расстояний между ординатами (строка «расстояние»): слова-числа между соседними ординатами
dist_words = [w for w in words if re.match(r"^\d+[.,]?\d*$", w[4])]
# найдём Y-строку «Расстояние»
ry = None
for w in hdr:
    if re.search(r"расстоян", w[4], re.I): ry = (w[1]+w[3])/2
if ry is not None:
    row = [w for w in dist_words if abs((w[1]+w[3])/2 - ry) < 10]
    row.sort(key=lambda w: w[0])
    print(f"row 'Расстояние' y={ry:.0f}: {[w[4] for w in row][:25]}")
    # проверка: центр числа ~ середина между соседними ординатами?
    if len(xg) >= 2 and row:
        mids = [ (xg[i][0]+xg[i+1][0])/2 for i in range(len(xg)-1) ]
        ok = 0
        for w in row:
            cx = (w[0]+w[2])/2
            if min(abs(cx-m) for m in mids) < 8: ok += 1
        print(f"  числа по центру пролёта (±8pt): {ok}/{len(row)}")
# отметки лотка: строка с NN,NN под своим ординатами
elev_words = [w for w in words if re.match(r"^\d{2,3}[.,]\d{2}$", w[4])]
byy = collections.Counter(round((w[1]+w[3])/2/5)*5 for w in elev_words)
print("elev rows (y-bucket:count):", byy.most_common(6))
if xg:
    # выравнивание отметок по ординатам
    aligned = 0
    for w in elev_words:
        cx = (w[0]+w[2])/2
        if min(abs(cx - g[0]) for g in xg) < 8: aligned += 1
    print(f"отметки, выровненные по ординате (±8pt): {aligned}/{len(elev_words)}")
doc.close()
