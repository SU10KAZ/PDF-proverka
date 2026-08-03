# -*- coding: utf-8 -*-
"""Зонд 2: адресная проверка якорей КЖ/КМ.
1) осевые кружки (c-петли 20-30pt) + токен внутри + штрихпунктирная цепочка
2) позиционные кружки армирования (c-петли 5-8pt) + цифра рядом/внутри
3) полочки выносок: горизонтальный сегмент прямо под текстом
4) засечки размеров: короткие 45-сегменты, середина которых лежит на длинной линии
5) болты КМ: окружности 14-17pt + текст М..
"""
import collections, math
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин"

def load(rel):
    doc = fitz.open(f"{BASE}/{rel}")
    return doc, doc[0]

def circles(dr, dmin, dmax):
    out = []
    for d in dr:
        kinds = tuple(i[0] for i in d["items"])
        if kinds and all(k == "c" for k in kinds) and len(kinds) >= 2:
            r = d["rect"]; w, h = r.x1-r.x0, r.y1-r.y0
            if dmin <= (w+h)/2 <= dmax and 0.75 < w/(h+1e-6) < 1.33:
                out.append(((r.x0+r.x1)/2, (r.y0+r.y1)/2, (w+h)/2, d))
    return out

def words_in(words, cx, cy, rad):
    hit = []
    for w in words:
        wx, wy = (w[0]+w[2])/2, (w[1]+w[3])/2
        if math.hypot(wx-cx, wy-cy) <= rad:
            hit.append(w[4])
    return hit

def segs(dr):
    out = []
    for d in dr:
        for it in d["items"]:
            if it[0] == "l":
                out.append(((it[1].x, it[1].y), (it[2].x, it[2].y), d.get("width") or 0))
    return out

def seg_len(s):
    return math.hypot(s[1][0]-s[0][0], s[1][1]-s[0][1])

def pt_seg_dist(px, py, s):
    (x1,y1),(x2,y2) = s[0], s[1]
    dx, dy = x2-x1, y2-y1
    L2 = dx*dx+dy*dy
    if L2 == 0: return math.hypot(px-x1, py-y1), 0.0
    t = max(0, min(1, ((px-x1)*dx+(py-y1)*dy)/L2))
    return math.hypot(px-(x1+t*dx), py-(y1+t*dy)), t

# ---------- 1+2. Кружки осей и позиций ----------
print("=== 1/2. КРУЖКИ (оси, позиции) ===")
for rel, tag, ranges in [
    ("КЖ/КЖ — 027 маркировочная схема — маркировочная схема — 6VCK-LXXM-N33.pdf", "kj_marking", [(20,30)]),
    ("КЖ/КЖ — 001 закладные детали — узел закладной детали — 4HH4-XRCF-939.pdf", "kj_embedded", [(20,30)]),
    ("КЖ/КЖ — 059 план армирования — нижнее армирование плиты — 9FER-NNAD-CHY.pdf", "kj_reinf", [(4.5,8.5),(20,30)]),
    ("КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf", "kj_formwork", [(2,4),(20,30)]),
]:
    doc, pg = load(rel)
    words = pg.get_text("words")
    dr = pg.get_drawings()
    for dmin, dmax in ranges:
        cs = circles(dr, dmin, dmax)
        print(f"-- {tag} d∈[{dmin},{dmax}]: {len(cs)} окружностей")
        for cx, cy, dia, d in cs[:8]:
            inside = words_in(words, cx, cy, dia*0.75)
            near = words_in(words, cx, cy, dia*1.6)
            print(f"   c=({cx:.0f},{cy:.0f}) Ø{dia:.1f} внутри={inside} рядом={near[:4]}")
    doc.close()

# ---------- 3. Полочки под текстом ----------
print("\n=== 3. ПОЛОЧКИ ПОД ТЕКСТОМ (горизонтальный сегмент под строкой) ===")
for rel, tag in [
    ("КЖ/КЖ — 059 план армирования — нижнее армирование плиты — 9FER-NNAD-CHY.pdf", "kj_reinf"),
    ("КЖ/КЖ — 001 закладные детали — узел закладной детали — 4HH4-XRCF-939.pdf", "kj_embedded"),
    ("КМ/КМ — 045 монтажная схема — схема балкона или каркаса — 6N7X-XKUA-9JC.pdf", "km_layout"),
]:
    doc, pg = load(rel)
    dr = pg.get_drawings()
    ss = segs(dr)
    horiz = [s for s in ss if abs(s[0][1]-s[1][1]) < 0.6 and seg_len(s) >= 8]
    # строки текста
    lines = {}
    for w in pg.get_text("words"):
        key = (w[5], w[6])
        lines.setdefault(key, []).append(w)
    n_shelf = 0; examples = []
    for key, ws in lines.items():
        x0 = min(w[0] for w in ws); x1 = max(w[2] for w in ws)
        y1 = max(w[3] for w in ws)
        text = " ".join(w[4] for w in sorted(ws, key=lambda w: w[0]))
        for s in horiz:
            sy = s[0][1]
            sx0, sx1 = min(s[0][0], s[1][0]), max(s[0][0], s[1][0])
            if 0 <= sy - y1 <= 4.0:  # сегмент в 0..4pt под базой строки
                ov = min(x1, sx1) - max(x0, sx0)
                if ov > 0.5*(x1-x0):
                    n_shelf += 1
                    if len(examples) < 8: examples.append((text[:48], round(sy-y1,1), round(sx1-sx0,1)))
                    break
    print(f"-- {tag}: строк {len(lines)}, с полочкой {n_shelf}")
    for e in examples: print(f"   '{e[0]}' зазор {e[1]}pt длина полочки {e[2]}pt")
    doc.close()

# ---------- 4. Засечки на размерных линиях ----------
print("\n=== 4. ЗАСЕЧКИ 45° НА ДЛИННЫХ ЛИНИЯХ ===")
for rel, tag in [
    ("КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf", "kj_formwork"),
    ("КЖ/КЖ — 091 сечение армирования — сечение армирования — 4K9U-VDRL-WTJ.pdf", "kj_section"),
    ("КМ/КМ — 037 стремянка — чертёж стремянки — 6WTD-GG69-3CE.pdf", "km_ladder"),
]:
    doc, pg = load(rel)
    dr = pg.get_drawings()
    ss = segs(dr)
    long_ax = [s for s in ss if seg_len(s) >= 25 and (abs(s[0][0]-s[1][0]) < 0.6 or abs(s[0][1]-s[1][1]) < 0.6)]
    ticks = []
    for s in ss:
        L = seg_len(s)
        if 1.2 <= L <= 6.0:
            dx, dy = abs(s[1][0]-s[0][0]), abs(s[1][1]-s[0][1])
            if dx > 0.5 and dy > 0.5 and 0.5 < dx/(dy+1e-6) < 2.0:
                ticks.append(s)
    tick_lens = collections.Counter(round(seg_len(t), 1) for t in ticks)
    on_line = 0; per_line = collections.Counter()
    for t in ticks[:4000]:
        mx, my = (t[0][0]+t[1][0])/2, (t[0][1]+t[1][1])/2
        for i, s in enumerate(long_ax):
            dist, tt = pt_seg_dist(mx, my, s)
            if dist < 0.7 and 0.02 < tt < 0.98:
                on_line += 1; per_line[i] += 1
                break
    multi = sum(1 for v in per_line.values() if v >= 2)
    print(f"-- {tag}: 45-ticks {len(ticks)}, длины(top5) {tick_lens.most_common(5)}, лежит на длинной H/V линии: {on_line}, линий с >=2 засечками: {multi}")
    doc.close()

# ---------- 5. Болты КМ ----------
print("\n=== 5. КМ УЗЕЛ: окружности-болты + текст ===")
doc, pg = load("КМ/КМ — 001 узел соединения — соединение стальных элементов — 7NC9-69EK-9HF.pdf")
words = pg.get_text("words")
dr = pg.get_drawings()
for dmin, dmax, name in [(13, 17.5, "болт?"), (20, 26, "шайба/отв?"), (40, 50, "марка узла?")]:
    cs = circles(dr, dmin, dmax)
    print(f"-- {name} d∈[{dmin},{dmax}]: {len(cs)}")
    for cx, cy, dia, d in cs[:6]:
        inside = words_in(words, cx, cy, dia*0.75)
        near = words_in(words, cx, cy, dia*3.0)
        print(f"   ({cx:.0f},{cy:.0f}) Ø{dia:.1f} внутри={inside} около={[t for t in near if t not in inside][:6]}")
# слова с 'М16'/'болт'
mt = [w for w in words if any(k in w[4] for k in ("М1","М2","болт","Болт","отв"))]
print(f"   токены крепежа: {[w[4] for w in mt][:14]}")
doc.close()
