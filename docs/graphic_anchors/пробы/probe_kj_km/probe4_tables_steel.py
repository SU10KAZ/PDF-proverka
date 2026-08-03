# -*- coding: utf-8 -*-
"""Зонд 4: (а) табличная сетка из длинных H/V линий (спецификация в блоке КМ-053);
(б) слова в ячейках; (в) подписи профилей на чертеже элемента; (г) перф km_connection."""
import collections, math, re, time
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин"

def load(rel):
    doc = fitz.open(f"{BASE}/{rel}")
    return doc, doc[0]

def clipped_segs(pg):
    out = []
    R = pg.rect
    for d in pg.get_drawings():
        r = d["rect"]
        if r.x1 < R.x0 or r.x0 > R.x1 or r.y1 < R.y0 or r.y0 > R.y1: continue
        for it in d["items"]:
            if it[0] == "l":
                out.append(((it[1].x, it[1].y), (it[2].x, it[2].y)))
            elif it[0] == "re":
                r2 = it[1]
                out.append(((r2.x0, r2.y0), (r2.x1, r2.y0)))
                out.append(((r2.x0, r2.y1), (r2.x1, r2.y1)))
                out.append(((r2.x0, r2.y0), (r2.x0, r2.y1)))
                out.append(((r2.x1, r2.y0), (r2.x1, r2.y1)))
    return out

def seg_len(s): return math.hypot(s[1][0]-s[0][0], s[1][1]-s[0][1])

print("=== (а/б) табличная сетка ===")
for rel, tag in [
    ("КМ/КМ — 053 чертёж элемента — опора или рама оборудования — 49HG-KVWP-WXQ.pdf", "km_member_053"),
    ("КЖ/КЖ — 027 маркировочная схема — маркировочная схема — 6VCK-LXXM-N33.pdf", "kj_marking_027"),
]:
    doc, pg = load(rel)
    ss = clipped_segs(pg)
    W, H = pg.rect.width, pg.rect.height
    # длинные линии
    horiz = {}
    vert = {}
    for s in ss:
        L = seg_len(s)
        if abs(s[0][1]-s[1][1]) < 0.5 and L > 40:
            y = round(s[0][1], 0)
            horiz.setdefault(y, 0)
            horiz[y] = max(horiz[y], L)
        if abs(s[0][0]-s[1][0]) < 0.5 and L > 40:
            x = round(s[0][0], 0)
            vert.setdefault(x, 0)
            vert[x] = max(vert[x], L)
    # кандидаты в таблицу: >=4 горизонталей одинаковой длины (±5%) с равномерными Y + >=3 вертикали
    lens = collections.Counter(round(v/10)*10 for v in horiz.values())
    print(f"-- {tag}: длинных H {len(horiz)}, V {len(vert)}; топ длин H: {lens.most_common(5)}")
    if lens:
        Lmode, cnt = lens.most_common(1)[0]
        ys = sorted(y for y, v in horiz.items() if abs(v - Lmode) <= max(10, 0.06*Lmode))
        if len(ys) >= 4:
            dys = [round(ys[i+1]-ys[i],1) for i in range(len(ys)-1)]
            print(f"   строки-кандидаты: {len(ys)} Y-линий, шаги {collections.Counter(dys).most_common(5)}")
            # вертикали в том же Y-диапазоне
            vx = sorted(x for x, v in vert.items() if v >= (ys[-1]-ys[0])*0.5)
            print(f"   вертикалей высотой >=50% диапазона: {len(vx)}: {vx[:12]}")
            # слова в первой строке-ячейках
            words = pg.get_text("words")
            if len(vx) >= 3 and len(ys) >= 2:
                row = [w for w in words if ys[0] <= (w[1]+w[3])/2 <= ys[1]]
                cells = collections.defaultdict(list)
                for w in row:
                    cx = (w[0]+w[2])/2
                    for i in range(len(vx)-1):
                        if vx[i] <= cx <= vx[i+1]:
                            cells[i].append(w[4]); break
                print(f"   1-я строка по ячейкам: {dict((k, ' '.join(v)[:24]) for k, v in sorted(cells.items()))}")
    doc.close()

print("\n=== (в) подписи профилей КМ ===")
doc, pg = load("КМ/КМ — 053 чертёж элемента — опора или рама оборудования — 49HG-KVWP-WXQ.pdf")
words = pg.get_text("words")
prof = [w[4] for w in words if re.search(r"(Швеллер|Уголок|Труба|Гн\.|L\d|\d+[xх]\d+[xх]\d)", w[4])]
print(f"-- km_member_053: слов {len(words)}, профильных токенов: {prof[:12]}")
txt = pg.get_text()[:600].replace(chr(10), " | ")
print(f"   начало текста: {txt[:400]}")
doc.close()

print("\n=== (г) перф km_connection (118K drawings) ===")
t0 = time.time()
doc, pg = load("КМ/КМ — 001 узел соединения — соединение стальных элементов — 7NC9-69EK-9HF.pdf")
dr = pg.get_drawings()
t1 = time.time()
n_items = sum(len(d["items"]) for d in dr)
t2 = time.time()
print(f"-- get_drawings: {t1-t0:.1f}s, {len(dr)} drawings, {n_items} items, обход items {t2-t1:.1f}s")
doc.close()
