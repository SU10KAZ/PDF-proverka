# -*- coding: utf-8 -*-
"""Проба 5: (а) выноски раздельными сегментами (полочка-сегмент + примыкающая
диагональ + текст над полочкой), в т.ч. гроздья позиций КЖ; (б) геометрия у
отметок уровня АР-фасада; (в) сетка таблицы из длинных H/V линий; (г) кружки
позиций арматуры (сечение армирования); (д) crop-фильтр как обязательный шаг."""
import math, re
from collections import Counter, defaultdict
import fitz

AR_UZEL = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 019 архитектурный узел — архитектурный узел — 9HTK-Y74V-UJ6.pdf"
AR_FASAD = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 033 архитектурный узел — фасадный узел — NWHW-Y3M3-FRP.pdf"
KJ_SECH = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 091 сечение армирования — сечение армирования — 4K9U-VDRL-WTJ.pdf"
KJ_OPAL = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf"

def load(pg, clip=True):
    """Все l-сегменты с учётом rotation и crop-фильтра."""
    rot = pg.rotation_matrix if pg.rotation else None
    pr = pg.rect
    segs = []
    for di, d in enumerate(pg.get_drawings()):
        r = d.get("rect")
        if clip and r is not None:
            rr = fitz.Rect(r)
            if rot is not None:
                rr = rr * rot; rr.normalize()
            if not rr.intersects(pr):
                continue
        for it in d.get("items") or []:
            if it[0] != "l": continue
            a, b = it[1], it[2]
            if rot is not None:
                a = fitz.Point(a) * rot; b = fitz.Point(b) * rot
            L = math.hypot(b.x-a.x, b.y-a.y)
            if L < 0.3: continue
            segs.append({"p1": (a.x, a.y), "p2": (b.x, b.y), "len": L,
                         "w": float(d.get("width") or 0), "di": di,
                         "fill": d.get("fill") is not None})
    return segs

def text_lines(pg):
    out = []
    for b in pg.get_text("dict").get("blocks", []):
        for ln in b.get("lines", []):
            t = "".join(sp.get("text", "") for sp in ln.get("spans", [])).strip()
            if t: out.append({"t": t, "bbox": ln.get("bbox"), "dir": ln.get("dir", (1, 0))})
    return out

# ---------- (а) выноски раздельными сегментами ----------
print("### (а) выноски: полочка-сегмент + диагональ + текст (раздельные сегменты)")
for tag, path in (("AR_uzel_019", AR_UZEL), ("KJ_sechenie_091", KJ_SECH)):
    doc = fitz.open(path); pg = doc[0]
    segs = load(pg); tls = text_lines(pg)
    horiz = [s for s in segs if abs(s["p1"][1]-s["p2"][1]) < 0.6 and 4 <= abs(s["p1"][0]-s["p2"][0]) <= 120]
    diag = [s for s in segs if abs(s["p1"][0]-s["p2"][0]) > 1.5 and abs(s["p1"][1]-s["p2"][1]) > 1.5 and s["len"] >= 5]
    hits = []
    for h in horiz:
        sx0, sx1 = sorted((h["p1"][0], h["p2"][0])); sy = h["p1"][1]
        above = [tl for tl in tls
                 if tl["bbox"][0] < sx1+2 and tl["bbox"][2] > sx0-2
                 and 0 <= sy - tl["bbox"][3] <= 4.5
                 and abs(tl["dir"][0]) > 0.9]
        if not above: continue
        for dseg in diag:
            for de in (dseg["p1"], dseg["p2"]):
                if min(abs(de[0]-sx0), abs(de[0]-sx1)) < 1.2 and abs(de[1]-sy) < 1.2:
                    other = dseg["p2"] if de is dseg["p1"] else dseg["p1"]
                    hits.append({"text": above[0]["t"][:46], "tip": (round(other[0]), round(other[1])),
                                 "shelf": (round(sx0), round(sx1), round(sy, 1)),
                                 "below": len([tl for tl in tls if tl["bbox"][0] < sx1 and tl["bbox"][2] > sx0 and 0 <= tl["bbox"][1]-sy <= 12])})
                    break
            else:
                continue
            break
    print(f"  {tag}: полочек-кандидатов={len(horiz)} диагоналей={len(diag)} связанных выносок={len(hits)}")
    for hh in hits[:8]: print(f"    {hh}")
    doc.close()

# ---------- (б) отметки уровня: что за геометрия рядом ----------
print("\n### (б) АР фасад: геометрия у отметок ±N,NNN")
doc = fitz.open(AR_FASAD); pg = doc[0]
segs = load(pg); tls = text_lines(pg)
LEVEL_RE = re.compile(r"^[+-]?\d{1,2}[.,]\d{3}\*?$")
lv = [tl for tl in tls if LEVEL_RE.match(tl["t"])]
for tl in lv[:6]:
    bx = tl["bbox"]
    near = [s for s in segs
            if min(abs(s["p1"][0]-bx[0]), abs(s["p2"][0]-bx[0]), abs(s["p1"][0]-bx[2]), abs(s["p2"][0]-bx[2])) < 30
            and min(abs(s["p1"][1]-bx[3]), abs(s["p2"][1]-bx[3])) < 15]
    kinds = Counter()
    for s in near:
        dx, dy = abs(s["p1"][0]-s["p2"][0]), abs(s["p1"][1]-s["p2"][1])
        ang = round(math.degrees(math.atan2(dy, max(dx, 1e-6))))
        kinds[(round(s["len"]), ang)] += 1
    print(f"  '{tl['t']}' bbox={tuple(round(v) for v in bx)}: соседних сегментов={len(near)} (len,ang)={kinds.most_common(6)}")
doc.close()

# ---------- (в) сетка таблицы ----------
print("\n### (в) детектор сетки: длинные H/V линии → строки/колонки (КЖ опалубка, зона ведомости)")
doc = fitz.open(KJ_OPAL); pg = doc[0]
segs = load(pg)
W, H = pg.rect.width, pg.rect.height
longh = [s for s in segs if abs(s["p1"][1]-s["p2"][1]) < 0.5 and abs(s["p1"][0]-s["p2"][0]) > 60]
longv = [s for s in segs if abs(s["p1"][0]-s["p2"][0]) < 0.5 and abs(s["p1"][1]-s["p2"][1]) > 25]
# кластеризуем Y горизонталей с одинаковым X-диапазоном (полосы таблиц)
groups = defaultdict(list)
for s in longh:
    x0, x1 = sorted((s["p1"][0], s["p2"][0]))
    groups[(round(x0/8), round(x1/8))].append(round(s["p1"][1], 1))
tables = [(k, sorted(set(v))) for k, v in groups.items() if len(set(v)) >= 4]
tables.sort(key=lambda kv: -len(kv[1]))
for k, ys in tables[:3]:
    x0, x1 = k[0]*8, k[1]*8
    vs = [s for s in longv if x0-5 <= s["p1"][0] <= x1+5 and min(s["p1"][1], s["p2"][1]) < max(ys)+5 and max(s["p1"][1], s["p2"][1]) > min(ys)-5]
    xs = sorted({round(s["p1"][0], 1) for s in vs})
    row_h = [round(b-a, 1) for a, b in zip(ys, ys[1:])]
    print(f"  таблица x∈[{x0:.0f},{x1:.0f}]: строк-линий={len(ys)} высоты строк={row_h[:8]} колонок-линий={len(xs)} X={xs[:10]}")
doc.close()

# ---------- (г) кружки позиций в сечении армирования ----------
print("\n### (г) КЖ сечение 091: окружности и токены внутри (позиции)")
doc = fitz.open(KJ_SECH); pg = doc[0]
words = pg.get_text("words")
circ = []
for d in pg.get_drawings():
    r = d.get("rect")
    kinds = tuple(it[0] for it in (d.get("items") or []))
    if r is None or not kinds or not all(k == "c" for k in kinds): continue
    w, h = float(r.width), float(r.height)
    if len(kinds) == 2 and 1.5 <= w/max(h, .01) <= 2.6 and 5 <= w <= 40:
        circ.append(("half", fitz.Rect(r)))
    elif len(kinds) >= 3 and 0.85 <= w/max(h, .01) <= 1.18 and 5 <= w <= 40:
        circ.append(("full", fitz.Rect(r)))
# спарить полу-дуги
paired = []
halves = [r for t, r in circ if t == "half"]
used = set()
for i, u in enumerate(halves):
    if i in used: continue
    for j, v in enumerate(halves):
        if j <= i or j in used: continue
        if abs((u.x0+u.x1)/2-(v.x0+v.x1)/2) < 0.6 and min(abs(u.y1-v.y0), abs(v.y1-u.y0)) < 0.6:
            paired.append(fitz.Rect(min(u.x0, v.x0), min(u.y0, v.y0), max(u.x1, v.x1), max(u.y1, v.y1)))
            used.update((i, j)); break
full = [r for t, r in circ if t == "full"]
def toks(rect):
    return [w[4] for w in words if rect.x0-1 <= (w[0]+w[2])/2 <= rect.x1+1 and rect.y0-1 <= (w[1]+w[3])/2 <= rect.y1+1]
pos_p = [(round(r.width, 1), toks(r)) for r in paired if toks(r)]
pos_f = [(round(r.width, 1), toks(r)) for r in full if toks(r)]
print(f"  полу-дуг={len(halves)} спарено={len(paired)} полных={len(full)}")
print(f"  спаренные с токеном: {pos_p[:10]}")
print(f"  полные с токеном: {pos_f[:10]}")
doc.close()

# ---------- (д) опалубка: где осевые кружки? ----------
print("\n### (д) КЖ опалубка: все c-содержащие drawings (анатомия)")
doc = fitz.open(KJ_OPAL); pg = doc[0]
pr = pg.rect
shapes = Counter()
samples = []
for d in pg.get_drawings():
    kinds = tuple(it[0] for it in (d.get("items") or []))
    if "c" not in kinds: continue
    r = d.get("rect")
    inside = r is not None and fitz.Rect(r).intersects(pr)
    shapes[(("".join(kinds))[:12], round(float(r.width)), round(float(r.height)), inside)] += 1
for k, n in shapes.most_common(12):
    print(f"  kinds={k[0]} w={k[1]} h={k[2]} in_crop={k[3]} x{n}")
doc.close()
