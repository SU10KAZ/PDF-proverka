# -*- coding: utf-8 -*-
"""Зонд 3: (а) доля drawings вне cropbox; (б) масштабная константа число↔длина полочки;
(в) лидеры от позиционных кружков КМ; (г) штрихпунктирные цепочки (оси);
(д) кружки/позиции ВНУТРИ cropbox у плана армирования; (е) выносные линии на концах размерной."""
import collections, math, re
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин"

def load(rel):
    doc = fitz.open(f"{BASE}/{rel}")
    return doc, doc[0]

def segs_clipped(pg):
    out = []
    R = pg.rect
    for d in pg.get_drawings():
        r = d["rect"]
        if r.x1 < R.x0 or r.x0 > R.x1 or r.y1 < R.y0 or r.y0 > R.y1:
            continue
        for it in d["items"]:
            if it[0] == "l":
                out.append(((it[1].x, it[1].y), (it[2].x, it[2].y)))
    return out

def seg_len(s): return math.hypot(s[1][0]-s[0][0], s[1][1]-s[0][1])

# ---------- (а) доля вне cropbox ----------
print("=== (а) drawings вне cropbox ===")
for rel, tag in [
    ("КЖ/КЖ — 059 план армирования — нижнее армирование плиты — 9FER-NNAD-CHY.pdf", "kj_reinf"),
    ("КЖ/КЖ — 027 маркировочная схема — маркировочная схема — 6VCK-LXXM-N33.pdf", "kj_marking"),
]:
    doc, pg = load(rel)
    R = pg.rect; dr = pg.get_drawings()
    out = sum(1 for d in dr if d["rect"].x1 < R.x0 or d["rect"].x0 > R.x1 or d["rect"].y1 < R.y0 or d["rect"].y0 > R.y1)
    print(f"-- {tag}: cropbox {R}, drawings {len(dr)}, вне cropbox {out} ({100*out/len(dr):.0f}%)")
    doc.close()

# ---------- (б) масштабная константа ----------
print("\n=== (б) масштаб: число на полочке / длина линии ===")
NUM = re.compile(r"^\d{2,5}$")
for rel, tag in [
    ("КЖ/КЖ — 001 закладные детали — узел закладной детали — 4HH4-XRCF-939.pdf", "kj_embedded"),
    ("КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf", "kj_formwork"),
    ("КМ/КМ — 045 монтажная схема — схема балкона или каркаса — 6N7X-XKUA-9JC.pdf", "km_layout"),
    ("КМ/КМ — 037 стремянка — чертёж стремянки — 6WTD-GG69-3CE.pdf", "km_ladder"),
]:
    doc, pg = load(rel)
    ss = segs_clipped(pg)
    horiz = [s for s in ss if abs(s[0][1]-s[1][1]) < 0.6 and seg_len(s) >= 8]
    vert = [s for s in ss if abs(s[0][0]-s[1][0]) < 0.6 and seg_len(s) >= 8]
    ratios = []
    for w in pg.get_text("words"):
        t = w[4]
        if not NUM.match(t): continue
        val = int(t)
        if val < 30: continue
        x0, y0, x1, y1 = w[:4]
        # горизонтальная полочка под числом
        best = None
        for s in horiz:
            sy = s[0][1]; sx0, sx1 = min(s[0][0], s[1][0]), max(s[0][0], s[1][0])
            if 0 <= sy - y1 <= 4 and min(x1, sx1) - max(x0, sx0) > 0.6*(x1-x0):
                L = sx1 - sx0
                if best is None or abs(L/val - 0.06) < abs(best/val - 0.06):
                    best = L
        # вертикальный размер: число слева/справа от вертикальной линии (повёрнутый текст)
        if best is not None:
            ratios.append((val, best, best/val))
    ratios.sort(key=lambda r: r[2])
    rs = [r[2] for r in ratios]
    if rs:
        med = rs[len(rs)//2]
        good = sum(1 for r in rs if abs(r-med)/med < 0.05)
        print(f"-- {tag}: пар число↔полочка {len(rs)}, медиана масштаба {med:.4f} pt/мм, в ±5% от медианы {good} ({100*good/len(rs):.0f}%)")
        print(f"   примеры: {[(v, round(L,1), round(r,4)) for v, L, r in ratios[:3]]} ... {[(v, round(L,1), round(r,4)) for v, L, r in ratios[-3:]]}")
    else:
        print(f"-- {tag}: пар нет")
    doc.close()

# ---------- (в) лидеры позиционных кружков КМ ----------
print("\n=== (в) КМ узел: лидер от кружка позиции ===")
doc, pg = load("КМ/КМ — 001 узел соединения — соединение стальных элементов — 7NC9-69EK-9HF.pdf")
R = pg.rect
words = pg.get_text("words")
balloons = []
for d in pg.get_drawings():
    kinds = tuple(i[0] for i in d["items"])
    if kinds and all(k == "c" for k in kinds) and len(kinds) >= 2:
        r = d["rect"]; wd, hg = r.x1-r.x0, r.y1-r.y0
        if 20 <= (wd+hg)/2 <= 26 and 0.8 < wd/(hg+1e-6) < 1.25:
            cx, cy = (r.x0+r.x1)/2, (r.y0+r.y1)/2
            inside = [w[4] for w in words if abs((w[0]+w[2])/2-cx) < wd*0.4 and abs((w[1]+w[3])/2-cy) < hg*0.4]
            balloons.append((cx, cy, (wd+hg)/2, inside))
ss = segs_clipped(pg)
linked = 0
for cx, cy, dia, inside in balloons:
    rad = dia/2
    touch = []
    for s in ss:
        L = seg_len(s)
        if L < 6: continue
        for p in (s[0], s[1]):
            dd = math.hypot(p[0]-cx, p[1]-cy)
            if abs(dd - rad) <= 2.0:  # конец сегмента на ободе кружка
                other = s[1] if p is s[0] else s[0]
                touch.append((round(L,1), (round(other[0],0), round(other[1],0))))
    if touch: linked += 1
    if inside and touch and linked <= 5:
        print(f"   поз {inside} @({cx:.0f},{cy:.0f}): лидеров {len(touch)}, пример {touch[:2]}")
print(f"-- кружков-позиций {len(balloons)}, с лидером на ободе {linked}")
doc.close()

# ---------- (г) штрихпунктирные цепочки ----------
print("\n=== (г) штрихпунктир: цепочки коротких коллинеарных H/V сегментов ===")
for rel, tag in [
    ("КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf", "kj_formwork"),
    ("КЖ/КЖ — 027 маркировочная схема — маркировочная схема — 6VCK-LXXM-N33.pdf", "kj_marking"),
]:
    doc, pg = load(rel)
    ss = segs_clipped(pg)
    # горизонтальные короткие и длинные сегменты, сгруппированные по Y (0.5pt)
    rows = collections.defaultdict(list)
    for s in ss:
        if abs(s[0][1]-s[1][1]) < 0.4:
            rows[round(s[0][1]*2)/2].append(s)
    chains = 0; examples = []
    for y, group in rows.items():
        if len(group) < 5: continue
        xs = sorted((min(s[0][0], s[1][0]), max(s[0][0], s[1][0])) for s in group)
        lens = [b-a for a, b in xs]
        gaps = [xs[i+1][0]-xs[i][1] for i in range(len(xs)-1)]
        pos_gaps = [g for g in gaps if 0.5 < g < 12]
        # штрихпунктир: чередование длинный-короткий или равные штрихи с равными зазорами
        if len(pos_gaps) >= 4 and len(group) >= 6:
            span = xs[-1][1]-xs[0][0]
            if span > 100:
                chains += 1
                if len(examples) < 4:
                    examples.append((round(y,0), len(group), round(span,0), [round(l,1) for l in lens[:6]], [round(g,1) for g in pos_gaps[:5]]))
    print(f"-- {tag}: горизонтальных штрих-цепочек (>=6 штрихов, зазоры 0.5-12pt, длина >100pt): {chains}")
    for e in examples: print(f"   y={e[0]} штрихов={e[1]} длина={e[2]}pt длины_штрихов={e[3]} зазоры={e[4]}")
    doc.close()

# ---------- (д) план армирования: что внутри cropbox ----------
print("\n=== (д) kj_reinf: кружки и позиции внутри cropbox ===")
doc, pg = load("КЖ/КЖ — 059 план армирования — нижнее армирование плиты — 9FER-NNAD-CHY.pdf")
R = pg.rect
words = [w for w in pg.get_text("words") if R.x0 <= (w[0]+w[2])/2 <= R.x1 and R.y0 <= (w[1]+w[3])/2 <= R.y1]
print(f"   cropbox {R}; слов внутри {len(words)}")
incrop = []
for d in pg.get_drawings():
    r = d["rect"]
    if r.x1 < R.x0 or r.x0 > R.x1 or r.y1 < R.y0 or r.y0 > R.y1: continue
    kinds = tuple(i[0] for i in d["items"])
    if kinds and all(k == "c" for k in kinds) and len(kinds) >= 2:
        wd, hg = r.x1-r.x0, r.y1-r.y0
        if 3 <= (wd+hg)/2 <= 40 and 0.7 < wd/(hg+1e-6) < 1.4:
            cx, cy = (r.x0+r.x1)/2, (r.y0+r.y1)/2
            inside = [w[4] for w in words if abs((w[0]+w[2])/2-cx) < wd*0.6 and abs((w[1]+w[3])/2-cy) < hg*0.6]
            incrop.append((round(cx), round(cy), round((wd+hg)/2,1), inside))
print(f"   c-кружков внутри cropbox: {len(incrop)}: {incrop[:10]}")
sample = [w[4] for w in words if re.match(r"^\d{1,2}$", w[4])]
print(f"   односимвольные/двузначные числа (кандидаты позиций): {sample[:20]}")
# текст с Ø/шаг
sh = [w[4] for w in words if "Ø" in w[4] or "шаг" in w[4] or re.match(r"^[SТt]=", w[4])]
print(f"   токены Ø/шаг/t=: {sh[:15]}")
doc.close()

# ---------- (е) выносные линии на концах размерной ----------
print("\n=== (е) выносные линии перпендикулярно концам размерной линии ===")
doc, pg = load("КЖ/КЖ — 001 закладные детали — узел закладной детали — 4HH4-XRCF-939.pdf")
ss = segs_clipped(pg)
horiz = [s for s in ss if abs(s[0][1]-s[1][1]) < 0.6 and seg_len(s) >= 15]
vert = [s for s in ss if abs(s[0][0]-s[1][0]) < 0.6 and seg_len(s) >= 4]
NUM = re.compile(r"^\d{2,5}$")
checked = with_ext = 0
for w in pg.get_text("words"):
    if not NUM.match(w[4]): continue
    x0, y0, x1, y1 = w[:4]
    for s in horiz:
        sy = s[0][1]; sx0, sx1 = min(s[0][0], s[1][0]), max(s[0][0], s[1][0])
        if 0 <= sy - y1 <= 4 and min(x1, sx1) - max(x0, sx0) > 0.6*(x1-x0):
            checked += 1
            ends = 0
            for ex in (sx0, sx1):
                for v in vert:
                    vx = v[0][0]; vy0, vy1 = min(v[0][1], v[1][1]), max(v[0][1], v[1][1])
                    if abs(vx-ex) <= 1.0 and vy0 - 3 <= sy <= vy1 + 3:
                        ends += 1; break
            if ends == 2: with_ext += 1
            break
print(f"-- kj_embedded: размерных полочек {checked}, с двумя выносными на концах {with_ext}")
doc.close()
