#!/usr/bin/env python3
"""Проба 6: (a) выноска состава пирога на узле кровли (полочка+leader);
(b) флажок отметки на фасаде (треугольник у +X,XXX);
(c) осевые кружки (окружность из 'c'-items + токен внутри);
(d) сетка ведомости дверей (строки Д-N и EI30 в одном Y-банде между H-линиями)."""
import collections, math, re
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/"

def lines_of(pg):
    out = []
    for b in pg.get_text("dict").get("blocks", []):
        for ln in b.get("lines", []):
            t = "".join(str(s.get("text") or "") for s in ln.get("spans", [])).strip()
            if t: out.append({"text": t, "bbox": ln["bbox"]})
    return out

def segs_of(pg):
    out = []
    for d in pg.get_drawings():
        for it in d.get("items", []):
            if it[0] == "l":
                p1=(it[1].x,it[1].y); p2=(it[2].x,it[2].y)
                L=math.hypot(p2[0]-p1[0],p2[1]-p1[1])
                if L>0.4: out.append({"p1":p1,"p2":p2,"len":L,"w":d.get("width") or 0})
    return out

# (a) КРОВЛЯ: пирог. Ищем текст-строки со словами материалов/толщин, полочку под ними
doc = fitz.open(BASE + "АР — 179 узел кровли — узел кровли — PDT9-6WQK-3PR.pdf"); pg = doc[0]
L = lines_of(pg); S = segs_of(pg)
mat = re.compile(r"(утеплит|мембран|стяжк|керамзит|гидроизол|пароизол|праймер|плита|слой|уклон|техноэласт|биполь|мм\b)", re.I)
cand = [l for l in L if mat.search(l["text"])]
print(f"(a) roof PDT9: text lines={len(L)}, material-ish={len(cand)}")
shelf_hits = 0; leader_hits = 0; samples=[]
for l in cand:
    x0,y0,x1,y1 = l["bbox"]
    # полочка: H-сегмент сразу под строкой (y1..y1+6), перекрывающий >=50% ширины текста
    shelves = [s for s in S if abs(s["p1"][1]-s["p2"][1])<=0.6 and y1-1<=s["p1"][1]<=y1+6
               and min(s["p2"][0],s["p1"][0]) <= x0+ (x1-x0)*0.5 and max(s["p1"][0],s["p2"][0]) >= x0+(x1-x0)*0.5]
    if shelves:
        shelf_hits += 1
        sh = shelves[0]
        # leader: диагональный сегмент, чей конец на конце полочки (±2pt)
        ex = [ (sh["p1"],sh["p2"]) ]
        e1, e2 = sh["p1"], sh["p2"]
        led = []
        for s in S:
            dx=abs(s["p1"][0]-s["p2"][0]); dy=abs(s["p1"][1]-s["p2"][1])
            if dx>1 and dy>1 and s["len"]>=5:
                for end in (e1,e2):
                    for sp in (s["p1"],s["p2"]):
                        if math.hypot(sp[0]-end[0],sp[1]-end[1])<2.0: led.append(round(s["len"],1))
        if led:
            leader_hits += 1
            if len(samples)<4: samples.append((l["text"][:38], round(sh["len"],1), led[:2]))
print(f"    with shelf under text: {shelf_hits}; shelf+diag leader: {leader_hits}; samples={samples}")
doc.close()

# (b) ФАСАД: отметки + флажок
doc = fitz.open(BASE + "АР — 061 фасад — фасадная развёртка — 49XE-EHU3-Y3V.pdf"); pg = doc[0]
S = segs_of(pg); W = pg.get_text("words")
ELEV = re.compile(r"^[+\-]\d{1,3}[,.]\d{3}$")
ews = [w for w in W if ELEV.match(w[4])]
print(f"(b) facade 49XE: elevation words={len(ews)}")
flag=0; shelf=0; details=[]
for w in ews[:200]:
    x0,y0,x1,y1,txt=w[0],w[1],w[2],w[3],w[4]
    # полочка под отметкой
    sh=[s for s in S if abs(s["p1"][1]-s["p2"][1])<=0.6 and y1-1<=s["p1"][1]<=y1+6 and min(s["p1"][0],s["p2"][0])<= (x0+x1)/2 <= max(s["p1"][0],s["p2"][0])+30]
    if sh: shelf+=1
    # треугольник-флажок: 2 диагональных сегмента 2-8pt, сходящиеся в точку на уровне полочки, возле края текста
    tri=[s for s in S if 1.5<=s["len"]<=9 and abs(s["p1"][0]-s["p2"][0])>0.5 and abs(s["p1"][1]-s["p2"][1])>0.5
         and (x0-14 <= (s["p1"][0]+s["p2"][0])/2 <= x1+14) and (y0-6 <= (s["p1"][1]+s["p2"][1])/2 <= y1+10)]
    if len(tri)>=2:
        flag+=1
        if len(details)<4: details.append((txt, len(tri), round(tri[0]["len"],1)))
print(f"    with shelf={shelf}/{len(ews)}; with >=2 diag(triangle marker)={flag}/{len(ews)}; samples={details}")
doc.close()

# (c) ОСЕВЫЕ КРУЖКИ: masonry 7DU7 + facade 49XE
for tag, fn in [("masonry 7DU7","АР — 145 кладочный план — кладочный план — 7DU7-346V-DN6.pdf"),
                ("facade 49XE","АР — 061 фасад — фасадная развёртка — 49XE-EHU3-Y3V.pdf"),
                ("stair 6T9H","АР — 216 лестница — план и разрез лестницы — 6T9H-T7HF-6AU.pdf")]:
    doc = fitz.open(BASE+fn); pg = doc[0]
    W = pg.get_text("words")
    short_tok = [w for w in W if re.fullmatch(r"[А-ЯA-Z]|[А-ЯA-Z]?\d{1,2}|\d[а-я]?", w[4])]
    circles=[]
    for d in pg.get_drawings():
        kinds=[it[0] for it in d.get("items",[])]
        if kinds and all(k=="c" for k in kinds) and 2<=len(kinds)<=6:
            r=d["rect"]; wd=r.x1-r.x0; ht=r.y1-r.y0
            if 6<=wd<=30 and 6<=ht<=30 and 0.75<=wd/max(ht,1e-9)<=1.33:
                circles.append(r)
    hits=0; diam=[]
    for r in circles:
        cx,cy=(r.x0+r.x1)/2,(r.y0+r.y1)/2
        for w in short_tok:
            wx,wy=(w[0]+w[2])/2,(w[1]+w[3])/2
            if abs(wx-cx)<(r.x1-r.x0)/2 and abs(wy-cy)<(r.y1-r.y0)/2:
                hits+=1; diam.append(round(r.x1-r.x0,1)); break
    print(f"(c) {tag}: circle candidates={len(circles)}, with short token inside={hits}, diam sample={sorted(set(diam))[:8]}")
    doc.close()

# (d) ВЕДОМОСТЬ ДВЕРЕЙ 67AU: Y-банды, Д-марка и EI в одной строке; линии сетки
doc = fitz.open(BASE + "АР — 157 эскиз заполнения проёма — эскиз двери или окна — 67AU-N79T-VUA.pdf"); pg = doc[0]
S = segs_of(pg); W = pg.get_text("words")
dm = [w for w in W if re.fullmatch(r"Д[-–]?\d+[а-яА-Я]*", w[4])]
ei = [w for w in W if re.fullmatch(r"(?:EI|EIS)\s*\d+|EI", w[4]) or re.fullmatch(r"EI\d+", w[4])]
eiw = [w for w in W if "EI" in w[4]]
longH = [s for s in S if abs(s["p1"][1]-s["p2"][1])<=0.6 and s["len"]>=100]
ys = sorted(set(round(s["p1"][1]) for s in longH))
print(f"(d) door schedule 67AU: Д-marks={len(dm)} EI-words={len(eiw)} longH(>=100pt)={len(ys)} rows_y={ys[:15]}")
pairs=0
for d_ in dm:
    dy=(d_[1]+d_[3])/2
    for e in eiw:
        ey=(e[1]+e[3])/2
        if abs(dy-ey)<6: pairs+=1; break
print(f"    Д-mark with EI in same Y-band(±6pt): {pairs}/{len(dm)}")
doc.close()
