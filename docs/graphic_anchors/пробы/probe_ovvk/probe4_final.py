#!/usr/bin/env python3
"""Проба 4: координатные пространства профиля; стрелки point-segment;
наклонные Ø-подписи в других аксонометриях; выноски в узле воронки;
V-стрелки гидравлики; калибровка графика насоса числом."""
import math, re
import fitz
from collections import Counter

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин"

def psd(px, py, s):
    (x1,y1),(x2,y2) = s[0], s[1]
    dx, dy = x2-x1, y2-y1
    L2 = dx*dx+dy*dy
    if L2 == 0: return math.hypot(px-x1, py-y1)
    t = max(0, min(1, ((px-x1)*dx+(py-y1)*dy)/L2))
    return math.hypot(px-(x1+t*dx), py-(y1+t*dy))

print("=== E3) профиль: подбор координатного пространства ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 59 продольный профиль дождевой канализации — 7VQA-JP9G-K4D.pdf"); pg = doc[0]
words = pg.get_text("words")
elev = [w for w in words if re.fullmatch(r"\d{2,3}[.,]\d{2}", w[4])]
raw = []
for d in pg.get_drawings():
    for it in d["items"]:
        if it[0]=="l":
            raw.append(((it[1].x,it[1].y),(it[2].x,it[2].y)))
mats = {"raw": fitz.Matrix(1,0,0,1,0,0), "rot": pg.rotation_matrix, "derot": pg.derotation_matrix}
for name, m in mats.items():
    vx = set()
    for p1,p2 in raw:
        q1 = fitz.Point(p1)*m; q2 = fitz.Point(p2)*m
        if abs(q1.x-q2.x) < 0.6 and abs(q1.y-q2.y) > 15:
            vx.add(round((q1.x+q2.x)/2,1))
    hit = sum(1 for w in elev if any(abs((w[0]+w[2])/2 - v) < 8 for v in vx))
    print(f"  {name}: вертикалей={len(vx)} совпадений с X отметок: {hit}/{len(elev)}")
doc.close()

print("=== K2) стрелки: point-segment дистанция (VK sewer) ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 086 аксонометрия канализации — вариант исходного комплекта — 4DHK-MVLH-NA4.pdf"); pg = doc[0]
tris = []
for d in pg.get_drawings():
    if d.get("fill") is None: continue
    kinds=[i[0] for i in d["items"]]
    if kinds.count("l") in (2,3,4) and "c" not in kinds:
        r=d["rect"]
        if 5 < max(r.width,r.height) < 25:
            tris.append(((r.x0+r.x1)/2,(r.y0+r.y1)/2))
S = []
for d in pg.get_drawings():
    for it in d["items"]:
        if it[0]=="l":
            p1,p2=(it[1].x,it[1].y),(it[2].x,it[2].y)
            L=math.hypot(p2[0]-p1[0],p2[1]-p1[1])
            if L>=15: S.append((p1,p2,L))
on_seg = sum(1 for cx,cy in tris if min((psd(cx,cy,s) for s in S), default=1e9) < 4)
print(f"стрелок={len(tris)}, лежит НА сегменте (psd<4pt): {on_seg}")
doc.close()

print("=== F3) наклонные Ø/уклон в других файлах ===")
for tag, rel in [("VK_water_axon","ВК/ВК — 093 аксонометрия водоснабжения — вариант исходного комплекта — 4UCN-Y7UG-YYU.pdf"),
                 ("OV_vent_axon2","ОВ/OV — 044 аксонометрия вентиляции — схема общеобменной вентиляции — 76T6-4LC3-VNT.pdf"),
                 ("OV_heat_axon2","ОВ/OV — 057 аксонометрия отопления — схема отопления — 4GPP-76XP-K7F.pdf")]:
    try:
        doc = fitz.open(f"{BASE}/{rel}"); pg = doc[0]
    except Exception as e:
        print(tag, "ERR", e); continue
    td = pg.get_text("dict")
    S = []
    for d in pg.get_drawings():
        for it in d["items"]:
            if it[0]=="l":
                p1,p2=(it[1].x,it[1].y),(it[2].x,it[2].y)
                L=math.hypot(p2[0]-p1[0],p2[1]-p1[1])
                if L>=8: S.append((p1,p2,L))
    total=match=0; diag_total=diag_match=0
    for b in td["blocks"]:
        if b.get("type")!=0: continue
        for ln in b["lines"]:
            txt="".join(sp["text"] for sp in ln["spans"]).strip()
            if not re.search(r"(i=0[.,]0\d+|[Øø∅⌀]\s?\d{2,3}|Ду\s?\d+|\d{2,4}[xх]\d{2,4})", txt): continue
            dd=ln["dir"]; a=math.degrees(math.atan2(-dd[1],dd[0]))%180
            bb=ln["bbox"]; cx,cy=(bb[0]+bb[2])/2,(bb[1]+bb[3])/2
            best=None
            for s in S:
                mx,my=(s[0][0]+s[1][0])/2,(s[0][1]+s[1][1])/2
                dist=math.hypot(mx-cx,my-cy)
                if best is None or dist<best[0]:
                    sa=math.degrees(math.atan2(s[0][1]-s[1][1],s[1][0]-s[0][0]))%180
                    best=(dist,sa) if best is None or dist<best[0] else best
            if best is None: continue
            da=min(abs(a-best[1]),180-abs(a-best[1]))
            total+=1; match+= da<15
            if 8<a<172 and abs(a-90)>8:
                diag_total+=1; diag_match+= da<15
    print(f"{tag}: подписей-параметров={total} параллельных={match}; наклонных={diag_total} наклонных-параллельных={diag_match}")
    doc.close()

print("=== L) выноски в узле воронки (WYYH) ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 088 узел водосточной воронки — вариант исходного комплекта — WYYH-YHK9-D3L.pdf"); pg = doc[0]
words = pg.get_text("words")
# строки текста (обёртка): блоки из get_text dict
td = pg.get_text("dict")
tlines = []
for b in td["blocks"]:
    if b.get("type")!=0: continue
    for ln in b["lines"]:
        txt="".join(sp["text"] for sp in ln["spans"]).strip()
        if len(txt)>=3: tlines.append((txt, ln["bbox"]))
print(f"текстовых строк: {len(tlines)}; примеры: {[t[0][:40] for t in tlines[:8]]}")
# диагональные сегменты длины 8-80: конец у bbox строки (<8pt)
diag = []
for d in pg.get_drawings():
    if d.get("fill") is not None: continue
    for it in d["items"]:
        if it[0]=="l":
            p1,p2=(it[1].x,it[1].y),(it[2].x,it[2].y)
            dx,dy=abs(p2[0]-p1[0]),abs(p2[1]-p1[1])
            L=math.hypot(dx,dy)
            if 8<=L<=120 and dx>2 and dy>2:
                diag.append((p1,p2,L))
linked = 0
for txt, bb in tlines:
    ok=False
    for p1,p2,L in diag:
        for p in (p1,p2):
            if bb[0]-8<p[0]<bb[2]+8 and bb[1]-6<p[1]<bb[3]+6:
                ok=True; break
        if ok: break
    linked += ok
print(f"диагональных лидеров 8-120pt: {len(diag)}; строк с лидером у bbox: {linked}/{len(tlines)}")
doc.close()

print("=== M) V-стрелки (open arrows) в гидравлике ===")
doc = fitz.open(f"{BASE}/ОВ/OV — 053 гидравлическая схема — смесительный узел — 9EEF-CHWK-9DU.pdf"); pg = doc[0]
# пары коротких штрихов с общим концом под углом 20-60° друг к другу
shorts = []
for d in pg.get_drawings():
    for it in d["items"]:
        if it[0]=="l":
            p1,p2=(it[1].x,it[1].y),(it[2].x,it[2].y)
            L=math.hypot(p2[0]-p1[0],p2[1]-p1[1])
            if 2.5<=L<=12: shorts.append((p1,p2,L))
from collections import defaultdict
grid = defaultdict(list)
for i,(p1,p2,L) in enumerate(shorts):
    for p in (p1,p2):
        grid[(round(p[0]/2),round(p[1]/2))].append(i)
vcount=0
seen=set()
for key, idxs in grid.items():
    if len(idxs)>=2:
        s = tuple(sorted(set(idxs)))
        if len(s)>=2 and s not in seen:
            seen.add(s); vcount+=1
print(f"коротких штрихов 2.5-12pt: {len(shorts)}, узлов с ≥2 общими концами (V-кандидаты): {vcount}")
doc.close()

print("=== N) точная калибровка графика насоса ===")
doc = fitz.open(f"{BASE}/ВК/ВК — 085 характеристика насоса — вариант исходного комплекта — 4KMQ-9K4P-97R.pdf"); pg = doc[0]
words = pg.get_text("words")
ylab = [(float(w[4]), (w[1]+w[3])/2) for w in words if re.fullmatch(r"\d+(\.\d+)?", w[4]) and w[0]<40]
xlab = [(float(w[4]), (w[0]+w[2])/2) for w in words if re.fullmatch(r"\d+(\.\d+)?", w[4]) and w[1]>150]
import statistics
def linfit(pairs):
    n=len(pairs); sx=sum(p[1] for p in pairs); sy=sum(p[0] for p in pairs)
    sxx=sum(p[1]**2 for p in pairs); sxy=sum(p[0]*p[1] for p in pairs)
    b=(n*sxy-sx*sy)/(n*sxx-sx*sx); a=(sy-b*sx)/n
    resid=[abs(a+b*p[1]-p[0]) for p in pairs]
    return a,b,max(resid)
ay,by,ry = linfit(ylab); ax,bx,rx = linfit(xlab)
print(f"Y-ось: H = {ay:.3f} + {by:.4f}*y, max|resid|={ry:.3f} м ({len(ylab)} меток)")
print(f"X-ось: Q = {ax:.3f} + {bx:.4f}*x, max|resid|={rx:.3f} м3/ч ({len(xlab)} меток)")
# кривая: сплошные сегменты внутри поля графика
solid=[]
for d in pg.get_drawings():
    if d.get("dashes") and d["dashes"] not in ("","[] 0"): continue
    for it in d["items"]:
        if it[0]=="l":
            p1,p2=(it[1].x,it[1].y),(it[2].x,it[2].y)
            if 49<p1[0]<296 and 29<p1[1]<156 and math.hypot(p2[0]-p1[0],p2[1]-p1[1])>1:
                solid.append((p1,p2))
print(f"сплошных сегментов в поле графика: {len(solid)}")
if solid:
    pts=sorted({(round(p[0],1),round(p[1],1)) for s in solid for p in s})
    conv=[(round(ax+bx*x,2), round(ay+by*y,2)) for x,y in pts]
    print("точки кривой (Q,H) первые 12:", conv[:12])
doc.close()
