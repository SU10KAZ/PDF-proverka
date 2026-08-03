#!/usr/bin/env python3
"""Проба 2: целевые якоря.
A) глиф-кластеры в text=0 файлах; B) заливные стрелки (треугольники);
C) засечки размеров в узле; D) калибровка осей графика насоса;
E) сетка продольного профиля; F) направление текста подписи vs направление трубы (аксонометрия К);
G) окружности-позиции в гидравлике."""
import math, json
import fitz
from collections import Counter

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин"

def segs_of(pg, min_len=0.0):
    out = []
    for d in pg.get_drawings():
        if d.get("fill") is not None and d.get("color") is None:
            continue
        for it in d["items"]:
            if it[0] == "l":
                p1, p2 = (it[1].x, it[1].y), (it[2].x, it[2].y)
                L = math.hypot(p2[0]-p1[0], p2[1]-p1[1])
                if L >= min_len:
                    out.append((p1, p2, L, d.get("width"), tuple(d.get("color") or ())))
    return out

def ang(p1, p2):
    a = math.degrees(math.atan2(p2[1]-p1[1], p2[0]-p1[0])) % 180
    return a

print("=== A) глиф-кластеры (text=0) ===")
for tag, rel in [("OV_axon","ОВ/OV — 067 аксонометрия вентиляции — схема противодымной вентиляции — CLLU-QK33-EC9.pdf"),
                 ("VK_plan","ВК/ВК — 01 план внутренних сетей 1 этажа — MDLY-9UXJ-QRP.pdf")]:
    doc = fitz.open(f"{BASE}/{rel}"); pg = doc[0]
    glyphs = []
    for d in pg.get_drawings():
        if d.get("fill") is None: continue
        r = d["rect"]
        kinds = [i[0] for i in d["items"]]
        # глиф: мелкий, содержит кривые, чёрный/тёмный fill
        if 0.5 < r.width < 15 and 1.5 < r.height < 15 and ("c" in kinds):
            glyphs.append((r.x0, r.y0, r.x1, r.y1, r.height))
    hs = sorted(g[4] for g in glyphs)
    # кластеризация в строки: сортировка по (y//h, x), склейка по gap<0.8*h
    glyphs.sort(key=lambda g: (round(g[1]/4), g[0]))
    lines = []
    cur = []
    for g in glyphs:
        if cur and (g[0]-cur[-1][2] > 1.6*g[4] or abs(g[1]-cur[-1][1]) > 0.7*g[4]):
            lines.append(cur); cur=[]
        cur.append(g)
    if cur: lines.append(cur)
    word_like = [l for l in lines if len(l) >= 2]
    print(f"{tag}: glyphs={len(glyphs)} h_med={hs[len(hs)//2]:.2f} h_p10={hs[len(hs)//10]:.2f} h_p90={hs[9*len(hs)//10]:.2f} word_clusters(>=2)={len(word_like)}")
    doc.close()

print("=== B) заливные стрелки-треугольники ===")
for tag, rel in [("OV_axon","ОВ/OV — 067 аксонометрия вентиляции — схема противодымной вентиляции — CLLU-QK33-EC9.pdf"),
                 ("OV_hydronic","ОВ/OV — 053 гидравлическая схема — смесительный узел — 9EEF-CHWK-9DU.pdf"),
                 ("VK_sewer","ВК/ВК — 086 аксонометрия канализации — вариант исходного комплекта — 4DHK-MVLH-NA4.pdf"),
                 ("OV_detail","ОВ/OV — 050 монтажный узел — узел противопожарного клапана — 4TWR-7NJD-WMK.pdf")]:
    doc = fitz.open(f"{BASE}/{rel}"); pg = doc[0]
    tri = []
    for d in pg.get_drawings():
        if d.get("fill") is None: continue
        items = d["items"]
        kinds = [i[0] for i in items]
        if kinds.count("l") in (2,3,4) and "c" not in kinds and "re" not in kinds and "qu" not in kinds:
            r = d["rect"]
            if 0.5 < max(r.width, r.height) < 20 and min(r.width,r.height) > 0.2:
                tri.append((round(r.width,2), round(r.height,2)))
    c = Counter(tri)
    print(f"{tag}: fill-триугольники(l x2-4)={len(tri)} топ-размеры={c.most_common(5)}")
    doc.close()

print("=== C) засечки размеров в монтажном узле ===")
doc = fitz.open(f"{BASE}/ОВ/OV — 050 монтажный узел — узел противопожарного клапана — 4TWR-7NJD-WMK.pdf"); pg = doc[0]
S = segs_of(pg, 0.5)
# короткие сегменты 1.5-5pt под ~45 град
ticks = [s for s in S if 1.2 <= s[2] <= 5.0 and 30 <= ang(s[0],s[1]) <= 60 or 1.2 <= s[2] <= 5.0 and 120 <= ang(s[0],s[1]) <= 150]
tick_lens = sorted(round(s[2],2) for s in ticks)
print(f"узел: всего l>=0.5pt={len(S)}, 45°-засечки 1.2-5pt: {len(ticks)}, длины топ={Counter(tick_lens).most_common(6)}")
# числа-слова
import re
words = pg.get_text("words")
dims = [w for w in words if re.fullmatch(r"\d{2,4}", w[4])]
# для каждой размерной цифры: ближайшая засечка
near = 0
for w in dims:
    cx, cy = (w[0]+w[2])/2, (w[1]+w[3])/2
    dmin = min((math.hypot((s[0][0]+s[1][0])/2-cx, (s[0][1]+s[1][1])/2-cy) for s in ticks), default=1e9)
    if dmin < 30: near += 1
print(f"узел: число-слов(2-4 цифры)={len(dims)}, из них с засечкой ближе 30pt: {near}")
doc.close()

print("=== D) калибровка осей графика насоса ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 085 характеристика насоса — вариант исходного комплекта — 4KMQ-9K4P-97R.pdf"); pg = doc[0]
words = pg.get_text("words")
print("слова:", [(w[4], round(w[0]), round(w[1])) for w in words])
for d in pg.get_drawings():
    dsh = d.get("dashes")
    if dsh and dsh not in ("","[] 0"):
        r=d["rect"]; print("dashed:", str(dsh), "rect=", [round(v,1) for v in (r.x0,r.y0,r.x1,r.y1)], "kinds=", [i[0] for i in d["items"]][:6])
doc.close()

print("=== E) сетка продольного профиля ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 59 продольный профиль дождевой канализации — 7VQA-JP9G-K4D.pdf"); pg = doc[0]
S = segs_of(pg, 3.0)
vert = [s for s in S if abs(s[0][0]-s[1][0]) < 0.5 and s[2] > 15]
horiz = [s for s in S if abs(s[0][1]-s[1][1]) < 0.5 and s[2] > 15]
print(f"профиль: вертикалей(>15pt)={len(vert)} горизонталей={len(horiz)} page={pg.rect}")
words = pg.get_text("words")
import re
elev = [w for w in words if re.fullmatch(r"\d{2,3}[.,]\d{2}", w[4])]
pk = [w for w in words if re.match(r"ПК", w[4])]
nums = [w for w in words if re.fullmatch(r"\d+[.,]?\d*", w[4])]
print(f"слова всего={len(words)} отметки NN,NN={len(elev)} ПК={len(pk)}")
vx = sorted(round((s[0][0]+s[1][0])/2,1) for s in vert)
print("X вертикалей:", vx[:25])
ex = sorted(round((w[0]+w[2])/2,1) for w in elev)
print("X отметок:", ex[:25])
# сколько отметок имеют вертикаль в пределах 6pt по X
hit = sum(1 for x in ex if any(abs(x-v)<8 for v in vx))
print(f"отметок с вертикалью ближе 8pt по X: {hit}/{len(ex)}")
doc.close()

print("=== F) направление текста vs направление трубы (аксонометрия К) ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 086 аксонометрия канализации — вариант исходного комплекта — 4DHK-MVLH-NA4.pdf"); pg = doc[0]
td = pg.get_text("dict")
S = segs_of(pg, 8.0)
import re
param_lines = []
for b in td["blocks"]:
    if b.get("type") != 0: continue
    for ln in b["lines"]:
        txt = "".join(sp["text"] for sp in ln["spans"]).strip()
        if re.search(r"(i=0[.,]0\d+|[Øø∅]\s?\d{2,3}|Ду\s?\d+)", txt):
            d = ln["dir"]
            bb = ln["bbox"]
            param_lines.append((txt[:30], round(math.degrees(math.atan2(-d[1], d[0]))%180,1), bb))
print(f"строк с i=/Ø: {len(param_lines)}")
match = 0; tot = 0
for txt, tang, bb in param_lines[:40]:
    cx, cy = (bb[0]+bb[2])/2, (bb[1]+bb[3])/2
    # ближайший длинный сегмент
    best = None
    for s in S:
        mx, my = (s[0][0]+s[1][0])/2, (s[0][1]+s[1][1])/2
        dd = math.hypot(mx-cx, my-cy)
        if best is None or dd < best[0]:
            best = (dd, ang(s[0],s[1]), s[2])
    if best:
        tot += 1
        da = min(abs(tang-best[1]), 180-abs(tang-best[1]))
        if da < 12: match += 1
        if tot <= 12:
            print(f"  '{txt}' text_ang={tang} seg_ang={round(best[1],1)} dist={round(best[0],1)} Δ={round(da,1)}")
print(f"подписей с параллельным ближайшим сегментом (Δ<12°): {match}/{tot}")
doc.close()

print("=== G) окружности-позиции в гидравлике ===")
doc = fitz.open(f"{BASE}/ОВ/OV — 053 гидравлическая схема — смесительный узел — 9EEF-CHWK-9DU.pdf"); pg = doc[0]
circles = []
for d in pg.get_drawings():
    kinds = [i[0] for i in d["items"]]
    r = d["rect"]
    if all(k=="c" for k in kinds) and len(kinds)>=3 and 15 < r.width < 30 and abs(r.width-r.height)<3:
        circles.append(r)
words = pg.get_text("words")
import re
inside = 0
samples = []
for r in circles:
    ws = [w for w in words if r.x0-2 < (w[0]+w[2])/2 < r.x1+2 and r.y0-2 < (w[1]+w[3])/2 < r.y1+2]
    if ws:
        inside += 1
        samples.append([w[4] for w in ws])
print(f"окружностей Ø~22: {len(circles)}, со словом внутри: {inside}, примеры: {samples[:10]}")
doc.close()
