# -*- coding: utf-8 -*-
"""Проба 4: (а) анатомия размерной полилинии «засечка+интервал» и сверка
числа с длиной интервала; (б) осевые кружки как полу-дуги ('c','c');
(в) рисунки за CropBox; (г) отметки уровня (флажок+число); (д) заливные
квадраты КЖ (сечения стержней?)."""
import math, re
from collections import Counter, defaultdict
import fitz

AR_FASAD = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 033 архитектурный узел — фасадный узел — NWHW-Y3M3-FRP.pdf"
AR_PLAN = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 097 архитектурный план — план этажа — 9KPA-UAWT-9RF.pdf"
KJ_OPAL = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf"
KJ_BALKA = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 035 план армирования — армирование балки — FCTE-JGDT-L6X.pdf"
KJ_ZAKL = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 001 закладные детали — узел закладной детали — 4HH4-XRCF-939.pdf"

NUM_RE = re.compile(r"^\d{2,5}$")
LEVEL_RE = re.compile(r"^[+-]?\d{1,2}[.,]\d{3}$")

def get_polylines(pg):
    out = []
    for di, d in enumerate(pg.get_drawings()):
        chain = []
        for it in d.get("items") or []:
            if it[0] != "l":
                if len(chain) >= 3: out.append(chain[:])
                chain = []; continue
            a, b = it[1], it[2]
            if chain and math.hypot(chain[-1][0]-a.x, chain[-1][1]-a.y) < 0.3:
                chain.append((b.x, b.y))
            else:
                if len(chain) >= 3: out.append(chain[:])
                chain = [(a.x, a.y), (b.x, b.y)]
        if len(chain) >= 3: out.append(chain[:])
    return out

# ---------- (а) размерная полилиния: интервал vs число ----------
print("### (а) размерные полилинии: число/интервал")
for tag, path in (("KJ_balka", KJ_BALKA), ("KJ_zakladnaya", KJ_ZAKL), ("KJ_opalubka", KJ_OPAL)):
    doc = fitz.open(path); pg = doc[0]
    raw = pg.get_text("dict")
    tls = []
    for b in raw.get("blocks", []):
        for ln in b.get("lines", []):
            t = "".join(sp.get("text", "") for sp in ln.get("spans", [])).strip()
            if t: tls.append({"t": t, "bbox": ln.get("bbox"), "dir": ln.get("dir", (1, 0))})
    ratios = []
    per_view = defaultdict(list)
    for pts in get_polylines(pg):
        if len(pts) != 3: continue
        def horiz(a, b): return abs(a[1]-b[1]) < 0.6 and abs(a[0]-b[0]) > 3
        def diag(a, b):
            dx, dy = abs(a[0]-b[0]), abs(a[1]-b[1])
            return dx > 0.5 and dy > 0.5 and 30 <= math.degrees(math.atan2(dy, dx)) <= 60
        # варианты: diag+horiz или horiz+diag
        if diag(pts[0], pts[1]) and horiz(pts[1], pts[2]):
            tick = (pts[0], pts[1]); seg = (pts[1], pts[2])
        elif horiz(pts[0], pts[1]) and diag(pts[1], pts[2]):
            seg = (pts[0], pts[1]); tick = (pts[1], pts[2])
        else:
            continue
        tick_len = math.hypot(tick[1][0]-tick[0][0], tick[1][1]-tick[0][1])
        seg_len = abs(seg[1][0]-seg[0][0])
        if seg_len < 5: continue
        sx0, sx1 = sorted((seg[0][0], seg[1][0])); sy = seg[0][1]
        cand = [tl for tl in tls if NUM_RE.match(tl["t"])
                and (tl["bbox"][0]+tl["bbox"][2])/2 > sx0-3
                and (tl["bbox"][0]+tl["bbox"][2])/2 < sx1+3
                and -13 <= sy-tl["bbox"][3] <= 5]
        if len(cand) == 1:
            val = int(cand[0]["t"])
            ratios.append((val, round(seg_len, 1), round(val/seg_len, 1), round(tick_len, 2)))
    doc.close()
    rc = Counter(r[2] for r in ratios)
    print(f"  {tag}: пар число↔интервал={len(ratios)}; масштабы mm/pt: {rc.most_common(5)}")
    print(f"    примеры (число, интервал_pt, мм/pt, засечка_pt): {ratios[:6]}")

# ---------- (б) осевые кружки как полу-дуги ----------
print("\n### (б) осевые кружки: полу-дуги ('c','c') и полные наборы")
for tag, path in (("AR_plan", AR_PLAN), ("KJ_opalubka", KJ_OPAL), ("KJ_zakladnaya", KJ_ZAKL)):
    doc = fitz.open(path); pg = doc[0]
    words = pg.get_text("words")
    halves = []; fulls = []
    for d in pg.get_drawings():
        r = d.get("rect")
        kinds = tuple(it[0] for it in (d.get("items") or []))
        if r is None or not kinds: continue
        if all(k == "c" for k in kinds):
            w, h = float(r.width), float(r.height)
            if len(kinds) == 2 and 1.6 <= w/max(h, 0.01) <= 2.5 and 6 <= w <= 40:
                halves.append(fitz.Rect(r))
            if len(kinds) >= 3 and 0.85 <= w/max(h, 0.01) <= 1.18 and 6 <= w <= 40:
                fulls.append(fitz.Rect(r))
    # спарить полу-дуги
    paired = []
    used = set()
    for i, u in enumerate(halves):
        if i in used: continue
        for j, v in enumerate(halves):
            if j <= i or j in used: continue
            if abs((u.x0+u.x1)/2-(v.x0+v.x1)/2) < 0.5 and min(abs(u.y1-v.y0), abs(v.y1-u.y0)) < 0.5:
                rect = fitz.Rect(min(u.x0, v.x0), min(u.y0, v.y0), max(u.x1, v.x1), max(u.y1, v.y1))
                paired.append(rect); used.update((i, j)); break
    def token_inside(rect):
        ws = [w for w in words if rect.x0 <= (w[0]+w[2])/2 <= rect.x1 and rect.y0 <= (w[1]+w[3])/2 <= rect.y1]
        return [w[4] for w in ws]
    ax_pair = [(round(r.width, 1), token_inside(r)) for r in paired]
    ax_full = [(round(r.width, 1), token_inside(r)) for r in fulls]
    print(f"  {tag}: полу-дуг={len(halves)} спарено={len(paired)} полных={len(fulls)}")
    print(f"    спаренные (d, слова внутри): {[a for a in ax_pair if a[1]][:10]}")
    print(f"    полные   (d, слова внутри): {[a for a in ax_full if a[1]][:10]}")
    doc.close()

# ---------- (в) рисунки за CropBox ----------
print("\n### (в) рисунки за CropBox")
for tag, path in (("KJ_zakladnaya", KJ_ZAKL), ("AR_plan", AR_PLAN), ("KJ_balka", KJ_BALKA)):
    doc = fitz.open(path); pg = doc[0]
    pr = pg.rect
    total = inside = 0
    for d in pg.get_drawings():
        r = d.get("rect")
        if r is None: continue
        total += 1
        if fitz.Rect(r).intersects(pr): inside += 1
    print(f"  {tag}: page.rect={pr} cropbox={pg.cropbox} mediabox={pg.mediabox} drawings={total} внутри={inside} за_кропом={total-inside}")
    doc.close()

# ---------- (г) отметки уровня ----------
print("\n### (г) отметки уровня: число ±N,NNN + треугольник-флажок рядом")
for tag, path in (("AR_fasad", AR_FASAD), ("KJ_balka", KJ_BALKA), ("KJ_opalubka", KJ_OPAL)):
    doc = fitz.open(path); pg = doc[0]
    words = pg.get_text("words")
    lv = [w for w in words if LEVEL_RE.match(w[4])]
    # треугольники: полилинии из 2-3 сегментов, замкнутый ход, размер 2-8 pt
    tris = []
    for pts in get_polylines(pg):
        if len(pts) not in (3, 4): continue
        xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
        w, h = max(xs)-min(xs), max(ys)-min(ys)
        if 1.5 <= w <= 12 and 1.5 <= h <= 12:
            tris.append(((min(xs)+max(xs))/2, (min(ys)+max(ys))/2, len(pts)))
    hits = 0; samples = []
    for w in lv:
        wx, wy = (w[0]+w[2])/2, (w[1]+w[3])/2
        near = [t for t in tris if abs(t[0]-wx) < 40 and abs(t[1]-wy) < 15]
        if near:
            hits += 1
            if len(samples) < 5: samples.append((w[4], round(near[0][0]), round(near[0][1])))
    print(f"  {tag}: отметок={len(lv)} с треугольником рядом={hits} примеры={samples}")
    doc.close()

# ---------- (д) заливные квадраты КЖ ----------
print("\n### (д) КЖ балка: заливные квадраты ~5.5pt — раскладка")
doc = fitz.open(KJ_BALKA); pg = doc[0]
sq = []
for d in pg.get_drawings():
    r = d.get("rect")
    kinds = tuple(it[0] for it in (d.get("items") or []))
    if r is None or kinds != ("re",) or d.get("fill") is None: continue
    w, h = float(r.width), float(r.height)
    if 4.5 <= w <= 6.5 and 4.5 <= h <= 6.5:
        sq.append(((r.x0+r.x1)/2, (r.y0+r.y1)/2))
rows = defaultdict(list)
for x, y in sq: rows[round(y/4)].append(x)
big_rows = sorted(((len(v), round(k*4), sorted(v)) for k, v in rows.items()), reverse=True)[:3]
for n, y, xs in big_rows:
    gaps = [round(b-a, 1) for a, b in zip(xs, xs[1:])]
    print(f"  ряд y≈{y}: n={n} шаги(top): {Counter(gaps).most_common(4)}")
words = pg.get_text("words")
sq_txt = [w[4] for w in words if re.search(r"шаг|Ø|%%C|ш\.", w[4])][:10]
print(f"  слова с 'шаг/Ø' на листе: {sq_txt}")
doc.close()
