# -*- coding: utf-8 -*-
"""Probe 2 (ГП рельеф/водоотвод): стрелки уклонов (мелкие fill-треугольники), опорные точки (кружки),
их близость к тексту уклонов i=/отметок. Проверка направления стрелки по вершинам."""
import math, re, sys, collections
import fitz

f = sys.argv[1]
doc = fitz.open(f); pg = doc[0]
words = pg.get_text("words")
W, H = pg.rect.width, pg.rect.height

SLOPE = re.compile(r"^(?:i\s*=?\s*)?0[,.]0\d{2,3}$|^i\s*=", re.I)
ELEV = re.compile(r"^[+\-]?\d{1,3}[,.]\d{2,3}$")

slope_words = [w for w in words if SLOPE.match(w[4])]
elev_words = [w for w in words if ELEV.match(w[4])]
print(f"words total={len(words)} slope-like={len(slope_words)} elev-like={len(elev_words)}")
print("slope examples:", [w[4] for w in slope_words[:12]])

# --- собрать fill-пути с вершинами ---
def verts(d):
    vs = []
    for it in d.get("items") or []:
        if it[0] == "l":
            vs += [(it[1].x, it[1].y), (it[2].x, it[2].y)]
        elif it[0] == "c":
            vs += [(it[1].x, it[1].y), (it[4].x, it[4].y)]
        elif it[0] in ("re", "qu"):
            r = it[1]
            if isinstance(r, fitz.Rect):
                vs += [(r.x0, r.y0), (r.x1, r.y1)]
    return vs

arrows = []   # маленькие вытянутые заливки (кандидаты стрелок)
dots = []     # маленькие круглые заливки/окружности (кандидаты опорных точек)
for d in pg.get_drawings():
    if d.get("fill") is None: continue
    r = d.get("rect")
    if r is None: continue
    w, h = r.x1-r.x0, r.y1-r.y0
    area = w*h
    if not (1.0 < area < 120): continue
    vs = verts(d)
    aspect = max(w, h)/max(min(w, h), 0.05)
    cx, cy = (r.x0+r.x1)/2, (r.y0+r.y1)/2
    kinds = "".join(it[0] for it in d.get("items") or [])
    if set(kinds) <= {"c"} and 0.7 < w/max(h,0.01) < 1.4 and area < 40:
        dots.append((cx, cy, round(w,1), round(h,1)))
    elif "l" in kinds and len(vs) >= 6:
        arrows.append((cx, cy, round(w,1), round(h,1), round(aspect,1), vs))

print(f"arrow-candidates={len(arrows)} dot-candidates={len(dots)}")

def nearest_word(cx, cy, wl):
    best = None
    for w in wl:
        wx, wy = (w[0]+w[2])/2, (w[1]+w[3])/2
        dd = math.hypot(wx-cx, wy-cy)
        if best is None or dd < best[0]: best = (dd, w[4])
    return best

# стрелки уклона: дистанция до ближайшего slope-текста
near_slope = [nearest_word(a[0], a[1], slope_words) for a in arrows[:4000]] if slope_words else []
ds = sorted(x[0] for x in near_slope if x)
if ds:
    import statistics
    print("arrow→slope-text dist: min=%.1f p25=%.1f median=%.1f p75=%.1f" % (
        ds[0], ds[len(ds)//4], statistics.median(ds), ds[3*len(ds)//4]))
    close = sum(1 for x in ds if x < 30)
    print(f"  arrows with slope-text ближе 30pt: {close}/{len(ds)}")

# опорные точки: дистанция до ближайшей отметки
near_elev = [nearest_word(d0[0], d0[1], elev_words) for d0 in dots[:4000]] if elev_words else []
de = sorted(x[0] for x in near_elev if x)
if de:
    import statistics
    print("dot→elev-text dist: min=%.1f median=%.1f p75=%.1f" % (de[0], statistics.median(de), de[3*len(de)//4]))
    close = sum(1 for x in de if x < 25)
    print(f"  dots with elev ближе 25pt: {close}/{len(de)}")

# для 5 стрелок, ближайших к slope-тексту: вершины => направление (наибольшая сторона)
if slope_words:
    scored = sorted(zip(arrows, near_slope), key=lambda t: t[1][0] if t[1] else 1e9)[:5]
    for (cx, cy, w, h, asp, vs), ns in scored:
        # направление = самый удалённый от центроида вершинный вектор
        mx = sum(v[0] for v in vs)/len(vs); my = sum(v[1] for v in vs)/len(vs)
        tip = max(vs, key=lambda v: math.hypot(v[0]-mx, v[1]-my))
        ang = math.degrees(math.atan2(tip[1]-my, tip[0]-mx))
        print(f"  arrow@({cx:.0f},{cy:.0f}) {w}x{h} asp={asp} tip_angle={ang:.0f}deg -> '{ns[1]}' d={ns[0]:.1f}")
doc.close()
