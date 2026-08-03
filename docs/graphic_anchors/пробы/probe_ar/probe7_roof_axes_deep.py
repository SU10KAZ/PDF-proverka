#!/usr/bin/env python3
"""Проба 7: (a) как реально выглядит состав пирога на узлах кровли (3 файла);
(b) осевые марки на лестнице/фасаде — из чего сделан кружок."""
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
                Ln=math.hypot(p2[0]-p1[0],p2[1]-p1[1])
                if Ln>0.4: out.append({"p1":p1,"p2":p2,"len":Ln,"w":d.get("width") or 0})
    return out

print("(a) состав пирога — текст узлов кровли")
for fn in ["АР — 179 узел кровли — узел кровли — PDT9-6WQK-3PR.pdf",
           "АР — 180 узел кровли — узел кровли — 4EWV-QCNG-3XQ.pdf",
           "АР — 182 узел кровли — узел кровли — 79LK-YN6E-D6J.pdf"]:
    doc = fitz.open(BASE+fn); pg = doc[0]
    L = lines_of(pg)
    tag = fn.split("—")[-1].strip()[:9]
    # многострочные стопки: строки с X-перекрытием и малым Y-зазором
    mat = [l for l in L if re.search(r"(утеплит|мембран|стяжк|керамзит|гидро|паро|праймер|плит|слfollowers|Техноэласт|Биполь|уклон|геотекстил|бетон|ЭППС|пенополистирол|раствор)", l["text"], re.I)]
    print(f" {tag}: lines={len(L)} mat_lines={len(mat)}")
    for l in mat[:6]:
        print(f"    [{l['bbox'][0]:.0f},{l['bbox'][1]:.0f}] {l['text'][:70]}")
    doc.close()

print()
print("(b) осевые марки: лестница 6T9H и фасад 49XE — окрестность коротких токенов")
for fn, tag in [("АР — 216 лестница — план и разрез лестницы — 6T9H-T7HF-6AU.pdf","stair"),
                ("АР — 061 фасад — фасадная развёртка — 49XE-EHU3-Y3V.pdf","facade")]:
    doc = fitz.open(BASE+fn); pg = doc[0]
    W = pg.get_text("words"); S = segs_of(pg)
    toks = [w for w in W if re.fullmatch(r"[А-ЯA-Z]|\d{1,2}|[А-Я]с?", w[4])]
    print(f" {tag}: short tokens={len(toks)} -> {collections.Counter(w[4] for w in toks).most_common(10)}")
    shown=0
    for w in toks:
        if shown>=3: break
        x0,y0,x1,y1,txt=w[0],w[1],w[2],w[3],w[4]
        cx,cy=(x0+x1)/2,(y0+y1)/2
        near=[s for s in S if abs((s["p1"][0]+s["p2"][0])/2-cx)<14 and abs((s["p1"][1]+s["p2"][1])/2-cy)<14]
        if 6<len(near)<80:
            lens=collections.Counter(round(s["len"],1) for s in near)
            print(f"   '{txt}' at ({cx:.0f},{cy:.0f}): near_segs={len(near)} len_hist={lens.most_common(6)}")
            shown+=1
    # circle-like: many short segments forming ring? проверим равноудалённость от центра
    for w in toks[:40]:
        x0,y0,x1,y1,txt=w[0],w[1],w[2],w[3],w[4]
        cx,cy=(x0+x1)/2,(y0+y1)/2
        ring=[]
        for s in S:
            mx,my=(s["p1"][0]+s["p2"][0])/2,(s["p1"][1]+s["p2"][1])/2
            r=math.hypot(mx-cx,my-cy)
            if 4<r<16 and s["len"]<6: ring.append(round(r,1))
        if len(ring)>=8:
            rc=collections.Counter(ring).most_common(3)
            print(f"   RING? '{txt}': {len(ring)} short segs at radii {rc}")
            break
    doc.close()
