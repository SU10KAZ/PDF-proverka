# -*- coding: utf-8 -*-
"""Проба 6: (а) сетка таблицы overlap-кластеризацией (ОВ характеристика
вентилятора + КЖ опалубка); (б) АР узел: выноски с ослабленной диагональю."""
import math
from collections import Counter
import fitz

OV_TAB = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/ОВ/OV — 102 характеристика оборудования — характеристика вентилятора — TXVT-GM6X-D6V.pdf"
KJ_OPAL = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf"
AR_UZEL = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 019 архитектурный узел — архитектурный узел — 9HTK-Y74V-UJ6.pdf"

def load_segs(pg):
    rot = pg.rotation_matrix if pg.rotation else None
    pr = pg.rect
    segs = []
    for d in pg.get_drawings():
        r = d.get("rect")
        if r is not None:
            rr = fitz.Rect(r)
            if rot is not None: rr = rr * rot; rr.normalize()
            if not rr.intersects(pr): continue
        for it in d.get("items") or []:
            if it[0] == "re":
                # рамка -> 4 отрезка
                rq = it[1]
                pts = [(rq.x0, rq.y0), (rq.x1, rq.y0), (rq.x1, rq.y1), (rq.x0, rq.y1), (rq.x0, rq.y0)]
                for a, b in zip(pts, pts[1:]):
                    if rot is not None:
                        a = tuple(fitz.Point(a)*rot); b = tuple(fitz.Point(b)*rot)
                    segs.append({"p1": a, "p2": b})
                continue
            if it[0] != "l": continue
            a, b = it[1], it[2]
            if rot is not None:
                a = fitz.Point(a)*rot; b = fitz.Point(b)*rot
            segs.append({"p1": (a.x, a.y), "p2": (b.x, b.y)})
    return segs

def grid_probe(tag, path):
    doc = fitz.open(path); pg = doc[0]
    segs = load_segs(pg)
    words = pg.get_text("words")
    H = [s for s in segs if abs(s["p1"][1]-s["p2"][1]) < 0.6 and abs(s["p1"][0]-s["p2"][0]) > 30]
    V = [s for s in segs if abs(s["p1"][0]-s["p2"][0]) < 0.6 and abs(s["p1"][1]-s["p2"][1]) > 10]
    # дедуп горизонталей по (y, x0, x1)
    hset = {}
    for s in H:
        x0, x1 = sorted((s["p1"][0], s["p2"][0]))
        key = (round(s["p1"][1], 0), round(x0, 0), round(x1, 0))
        hset[key] = (s["p1"][1], x0, x1)
    hs = sorted(hset.values())
    # жадная сборка полос: группа горизонталей с взаимным X-overlap >= 60%
    used = [False]*len(hs)
    tables = []
    for i, (y0, a0, a1) in enumerate(hs):
        if used[i]: continue
        band = [(y0, a0, a1)]; used[i] = True
        for j in range(i+1, len(hs)):
            if used[j]: continue
            yb, b0, b1 = hs[j]
            ref = band[-1]
            ov = min(ref[2], b1) - max(ref[1], b0)
            if ov > 0.6*min(ref[2]-ref[1], b1-b0) and yb - ref[0] < 120:
                band.append(hs[j]); used[j] = True
        if len(band) >= 4:
            tables.append(band)
    tables.sort(key=len, reverse=True)
    print(f"== {tag}: H-линий(дедуп)={len(hs)} V-линий={len(V)} полос(>=4 гориз)={len(tables)}")
    for band in tables[:2]:
        ys = [b[0] for b in band]
        x0 = min(b[1] for b in band); x1 = max(b[2] for b in band)
        vin = sorted({round(s["p1"][0], 0) for s in V
                      if x0-3 <= s["p1"][0] <= x1+3
                      and min(s["p1"][1], s["p2"][1]) < max(ys) and max(s["p1"][1], s["p2"][1]) > min(ys)})
        # число слов в зоне
        win = [w for w in words if x0 <= (w[0]+w[2])/2 <= x1 and min(ys) <= (w[1]+w[3])/2 <= max(ys)]
        rh = Counter(round(b-a) for a, b in zip(sorted(ys), sorted(ys)[1:]))
        print(f"   таблица: строк={len(ys)} выс.строк={rh.most_common(4)} колонок-X={len(vin)} слов_в_зоне={len(win)} x∈[{x0:.0f},{x1:.0f}] y∈[{min(ys):.0f},{max(ys):.0f}]")
    doc.close()

grid_probe("OV_ventilator_102", OV_TAB)
grid_probe("KJ_opalubka_017", KJ_OPAL)

# (б) АР узел: полочка + ЛЮБОЙ примыкающий не-горизонтальный сегмент len>=3
doc = fitz.open(AR_UZEL); pg = doc[0]
segs = load_segs(pg)
tls = []
for b in pg.get_text("dict").get("blocks", []):
    for ln in b.get("lines", []):
        t = "".join(sp.get("text", "") for sp in ln.get("spans", [])).strip()
        if t: tls.append({"t": t, "bbox": ln.get("bbox")})
horiz = [s for s in segs if abs(s["p1"][1]-s["p2"][1]) < 0.6 and 4 <= abs(s["p1"][0]-s["p2"][0]) <= 150]
nonh = [s for s in segs if abs(s["p1"][1]-s["p2"][1]) > 1.5 and math.hypot(s["p1"][0]-s["p2"][0], s["p1"][1]-s["p2"][1]) >= 3]
hits = 0; samples = []
for h in horiz:
    sx0, sx1 = sorted((h["p1"][0], h["p2"][0])); sy = h["p1"][1]
    above = [tl for tl in tls if tl["bbox"][0] < sx1+2 and tl["bbox"][2] > sx0-2 and 0 <= sy-tl["bbox"][3] <= 4.5]
    if not above: continue
    att = None
    for dseg in nonh:
        for de in (dseg["p1"], dseg["p2"]):
            if min(abs(de[0]-sx0), abs(de[0]-sx1)) < 1.0 and abs(de[1]-sy) < 1.0:
                att = dseg; break
        if att: break
    if att:
        hits += 1
        if len(samples) < 8:
            samples.append({"text": above[0]["t"][:40],
                            "below": [tl["t"][:30] for tl in tls if tl["bbox"][0] < sx1 and tl["bbox"][2] > sx0 and 0 <= tl["bbox"][1]-sy <= 12][:1]})
print(f"== AR_uzel_019 (ослабленная диагональ): выносок={hits}")
for s in samples: print(f"   {s}")
doc.close()
