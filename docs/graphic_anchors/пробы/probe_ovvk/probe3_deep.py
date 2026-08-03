#!/usr/bin/env python3
"""Проба 3: дератирование профиля; наклонные подписи аксонометрии;
магистраль лесенки; растр графика; штриховка vs засечки; стрелки у концов труб."""
import math, re
import fitz
from collections import Counter

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин"

def rot_segs(pg, min_len=0.0):
    m = pg.rotation_matrix
    out = []
    for d in pg.get_drawings():
        for it in d["items"]:
            if it[0] == "l":
                p1 = fitz.Point(it[1]) * m
                p2 = fitz.Point(it[2]) * m
                L = abs(p2 - p1)
                if L >= min_len:
                    out.append(((p1.x,p1.y),(p2.x,p2.y),L,d.get("width"),d.get("dashes")))
    return out

print("=== E2) профиль с дератированием (rotation=%s) ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 59 продольный профиль дождевой канализации — 7VQA-JP9G-K4D.pdf"); pg = doc[0]
print("rotation:", pg.rotation, "rect:", pg.rect)
S = rot_segs(pg, 3.0)
vert = [s for s in S if abs(s[0][0]-s[1][0]) < 0.6 and s[2] > 15]
horiz = [s for s in S if abs(s[0][1]-s[1][1]) < 0.6 and s[2] > 15]
words = pg.get_text("words")
elev = [w for w in words if re.fullmatch(r"\d{2,3}[.,]\d{2}", w[4])]
vx = sorted(set(round((s[0][0]+s[1][0])/2,1) for s in vert))
ex = [round((w[0]+w[2])/2,1) for w in elev]
hit = sum(1 for x in ex if any(abs(x-v)<8 for v in vx))
print(f"vert={len(vert)} horiz={len(horiz)} отметки={len(elev)} совпало X<8pt: {hit}/{len(ex)}")
# горизонтальные ряды «шапки» профиля (низ листа): Y длинных горизонталей
long_h = sorted(set(round(s[0][1],0) for s in horiz if s[2] > 200))
print("Y длинных горизонталей (>200pt):", long_h[:20])
# слова-заголовки строк таблицы профиля
rows = [w for w in words if re.search(r"(тметк|асстоя|клон|иаметр|снован|/ПК|ПК)", w[4])]
print("строчные заголовки:", [(w[4], round(w[1])) for w in rows][:14])
doc.close()

print("=== F2) углы подписей в аксонометрии К ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 086 аксонометрия канализации — вариант исходного комплекта — 4DHK-MVLH-NA4.pdf"); pg = doc[0]
td = pg.get_text("dict")
angles = Counter()
diag = []
for b in td["blocks"]:
    if b.get("type") != 0: continue
    for ln in b["lines"]:
        txt = "".join(sp["text"] for sp in ln["spans"]).strip()
        if not txt: continue
        d = ln["dir"]
        a = round(math.degrees(math.atan2(-d[1], d[0])) % 180)
        angles[a] += 1
        if 5 < a < 175 and a != 90 and re.search(r"(i=0[.,]0\d+|[Øø∅]\d{2,3}|L=\d+)", txt):
            diag.append((txt[:35], a, ln["bbox"]))
print("углы строк:", angles.most_common(8))
print(f"наклонных строк с i=/Ø/L=: {len(diag)}")
S = [s for s in rot_segs(pg, 8.0)]
def angl(s):
    return math.degrees(math.atan2(s[0][1]-s[1][1], s[1][0]-s[0][0])) % 180
match=0
for txt, a, bb in diag[:25]:
    cx, cy = (bb[0]+bb[2])/2, (bb[1]+bb[3])/2
    best=None
    for s in S:
        mx,my=(s[0][0]+s[1][0])/2,(s[0][1]+s[1][1])/2
        dd=math.hypot(mx-cx,my-cy)
        if best is None or dd<best[0]: best=(dd, angl(s))
    da=min(abs(a-best[1]),180-abs(a-best[1]))
    if da<12: match+=1
    print(f"  '{txt}' a={a} seg={round(best[1])} dist={round(best[0],1)} Δ={round(da,1)}")
print(f"наклонных с параллельным сегментом: {match}/{min(len(diag),25)}")
doc.close()

print("=== H) лесенка: магистраль/корень ===")
doc = fitz.open(f"{BASE}/ВК/01_13АВ-РД-ВК2.2-ПА_V1__6VW4-PCVA-TCN.pdf"); pg = doc[0]
words = pg.get_text("words")
sys_w = [w for w in words if re.fullmatch(r"(В\d+\.\d+|К\d+н?|Т\d+н?|Ст\.?\d+.*)", w[4])]
xs = sorted(round((w[0]+w[2])/2) for w in sys_w)
print(f"системных токенов: {len(sys_w)}, X-диапазон {xs[0] if xs else '-'}..{xs[-1] if xs else '-'}")
S = rot_segs(pg, 40.0)
horiz = [s for s in S if abs(s[0][1]-s[1][1]) < 1.0]
# горизонтали длиннее 300pt (магистрали, пересекают несколько стояков)
mains = [s for s in horiz if s[2] > 300]
ys = Counter(round(s[0][1]/10)*10 for s in mains)
print(f"горизонталей >300pt: {len(mains)}, топ Y-полос: {ys.most_common(6)}")
# «ввод»: слова насосная/ввод/гор.вода
root_w = [w[4] for w in words if re.search(r"(асосн|ввод|Ввод|агистрал)", w[4])]
print("слова-кандидаты корня:", root_w[:10])
doc.close()

print("=== I) график вентилятора: растр? ===")
doc = fitz.open(f"{BASE}/ОВ/OV — 36 аэродинамическая характеристика вентилятора — 6G36-HFKH-CQV.pdf"); pg = doc[0]
print("images:", [(x[0], x[2], x[3]) for x in pg.get_images()])
infos = pg.get_image_info()
print("image_info:", [(round(i['bbox'][0]),round(i['bbox'][1]),round(i['bbox'][2]),round(i['bbox'][3]), i['width'], i['height']) for i in infos][:5])
ws = pg.get_text("words")
print("числа рядом:", [w[4] for w in ws if re.fullmatch(r"\d+[.,]?\d*", w[4])][:20])
# 139 мелких заливных re — что это
sizes = Counter()
for d in pg.get_drawings():
    r=d["rect"]
    if d.get("fill") is not None and r.width*r.height < 30:
        sizes[(round(r.width,1), round(r.height,1))]+=1
print("мелкие fill-re размеры:", sizes.most_common(6))
doc.close()

print("=== J) засечки vs штриховка (узел клапана) ===")
doc = fitz.open(f"{BASE}/ОВ/OV — 050 монтажный узел — узел противопожарного клапана — 4TWR-7NJD-WMK.pdf"); pg = doc[0]
S = rot_segs(pg, 1.0)
d45 = [s for s in S if 1.2<=s[2]<=5.0 and (30<=math.degrees(math.atan2(abs(s[1][1]-s[0][1]), abs(s[1][0]-s[0][0])))<=60)]
# кластеризуем по «параллельной решётке»: сортировка по проекции на нормаль, шаг
d45.sort(key=lambda s: (s[0][0]+s[0][1]))
proj = sorted((s[0][0]+s[0][1])/math.sqrt(2) for s in d45)
gaps = Counter(round(proj[i+1]-proj[i],1) for i in range(len(proj)-1))
print(f"45°-коротких: {len(d45)}, топ шагов проекции: {gaps.most_common(6)}")
# изолированные (нет соседа с шагом <2.5pt) = кандидаты-засечки
iso = 0
for i,p in enumerate(proj):
    near = (i>0 and p-proj[i-1]<2.5) or (i+1<len(proj) and proj[i+1]-p<2.5)
    if not near: iso += 1
print(f"изолированных 45° (нет параллельного соседа ближе 2.5pt): {iso}")
doc.close()

print("=== K) стрелки на трубах (VK sewer): близость к концам сегментов ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 086 аксонометрия канализации — вариант исходного комплекта — 4DHK-MVLH-NA4.pdf"); pg = doc[0]
tris = []
for d in pg.get_drawings():
    if d.get("fill") is None: continue
    kinds=[i[0] for i in d["items"]]
    if kinds.count("l") in (2,3,4) and "c" not in kinds:
        r=d["rect"]
        if 5 < max(r.width,r.height) < 25:
            tris.append(((r.x0+r.x1)/2,(r.y0+r.y1)/2))
S = rot_segs(pg, 15.0)
near_end=0
for cx,cy in tris:
    dmin=1e9
    for s in S:
        for p in (s[0],s[1]):
            dmin=min(dmin, math.hypot(p[0]-cx,p[1]-cy))
    if dmin<10: near_end+=1
print(f"стрелок: {len(tris)}, у конца сегмента (<10pt): {near_end}")
doc.close()
