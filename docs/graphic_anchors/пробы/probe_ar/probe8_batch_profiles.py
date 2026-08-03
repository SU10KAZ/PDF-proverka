#!/usr/bin/env python3
"""Проба 8: батч-скан по одному PDF оставшихся профилей: ключевые regex-якоря,
сетка таблиц (длинные H+V), кружки, повторяемость марок (легенда vs экземпляры)."""
import collections, math, re, os
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/"
PICK = {}
for f in sorted(os.listdir(BASE)):
    if not f.endswith(".pdf"): continue
    m = re.match(r"АР — \d+ (.+?) — (.+?) — ", f)
    if not m: continue
    key = m.group(1)
    PICK.setdefault(key, f)

MARKS = {
    "finish": re.compile(r"^(?:СТ|ПЛ|ПТ|ПОТ|ПОЛ|ОТД)[-.]?\d+\w*$", re.I),
    "furniture": re.compile(r"^[МM][-–]?\d{1,3}$"),
    "opening": re.compile(r"^(?:Д|ДПМ|ДО|ОК|ЛЮК|ВК)[-№]?\d+\w*$", re.I),
    "elev": re.compile(r"^[+\-]\d{1,3}[,.]\d{3}$"),
    "num": re.compile(r"^\d{2,5}$"),
    "axis_tok": re.compile(r"^[А-ЯA-Z]$|^\d{1,2}$"),
    "area": re.compile(r"^\d+[,.]\d{1,2}$"),  # площади помещений
}

for key in ["отделка помещений","план потолка и освещения","план мебели и оборудования",
            "интерьерный план электрооборудования","план полов","маркировочный план",
            "фундамент под оборудование","ограждение","архитектурный разрез","план кровли",
            "узел кладки","архитектурный план"]:
    fn = PICK.get(key)
    if not fn: print(f"== {key}: PDF не найден"); continue
    doc = fitz.open(BASE+fn); pg = doc[0]
    W = pg.get_text("words")
    S = []
    circles = 0
    for d in pg.get_drawings():
        kinds=[it[0] for it in d.get("items",[])]
        if kinds and all(k=="c" for k in kinds) and 2<=len(kinds)<=6:
            r=d["rect"]; wd=r.x1-r.x0; ht=r.y1-r.y0
            if 5<=wd<=30 and 0.75<=wd/max(ht,1e-9)<=1.33: circles+=1
        for it in d.get("items",[]):
            if it[0]=="l":
                p1=(it[1].x,it[1].y); p2=(it[2].x,it[2].y)
                L=math.hypot(p2[0]-p1[0],p2[1]-p1[1])
                if L>0.4: S.append((p1,p2,L))
    longH = [s for s in S if abs(s[0][1]-s[1][1])<=0.6 and s[2]>=80]
    longV = [s for s in S if abs(s[0][0]-s[1][0])<=0.6 and s[2]>=40]
    hy = len(set(round(s[0][1]) for s in longH)); vx = len(set(round(s[0][0]) for s in longV))
    counts = {k: sum(1 for w in W if p.match(w[4])) for k,p in MARKS.items()}
    counts = {k:v for k,v in counts.items() if v}
    # повторяемость марок: марка, встречающаяся и в «таблице» (правый край/низ) и на «плане»
    marks = [w for w in W if MARKS["finish"].match(w[4]) or MARKS["furniture"].match(w[4]) or MARKS["opening"].match(w[4])]
    rep = collections.Counter(w[4].upper() for w in marks)
    multi = {k:v for k,v in rep.items() if v>=2}
    ticks = sum(1 for s in S if 1.0<=s[2]<=8 and abs(s[0][0]-s[1][0])>0.3 and abs(s[0][1]-s[1][1])>0.3
                and 0.4<abs(s[0][0]-s[1][0])/max(abs(s[0][1]-s[1][1]),1e-9)<2.5)
    print(f"== {key} [{fn.split('—')[-1].strip()[:9]}]: words={len(W)} segs={len(S)} grid(HxV)={hy}x{vx} circles={circles} ticks45={ticks}")
    print(f"   anchors={counts} repeated_marks(top6)={dict(sorted(multi.items(), key=lambda x:-x[1])[:6])}")
    doc.close()
