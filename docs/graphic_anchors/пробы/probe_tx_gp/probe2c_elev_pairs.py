# -*- coding: utf-8 -*-
"""Probe 2c: пары красная/чёрная отметка (NN,NN друг над другом + разделительная черта),
флажки '▐' и их привязка к отметкам."""
import math, re, sys
import fitz

f = sys.argv[1]
doc = fitz.open(f); pg = doc[0]
words = pg.get_text("words")
EL = re.compile(r"^\d{1,3}[.,]\d{2}$")
elev = [w for w in words if EL.match(w[4])]
flags = [w for w in words if w[4] in ("▐", "▌", "█", "◄", "►")]
print(f"elev words={len(elev)} flag glyphs={len(flags)} (unique glyphs: {sorted(set(w[4] for w in flags))[:5]})")

# горизонтальные сегменты (черта между отметками)
hsegs = []
for d in pg.get_drawings():
    for it in d.get("items") or []:
        if it[0] != "l": continue
        p1, p2 = it[1], it[2]
        if abs(p1.y - p2.y) < 0.5 and 6 < abs(p1.x - p2.x) < 45:
            hsegs.append((min(p1.x, p2.x), max(p1.x, p2.x), p1.y))
print(f"short horizontal segments 6..45pt: {len(hsegs)}")

pairs = 0; with_line = 0; samples = []
used = set()
for i, a in enumerate(elev):
    ax, ay = (a[0]+a[2])/2, (a[1]+a[3])/2
    for j, b in enumerate(elev):
        if j <= i: continue
        bx, by = (b[0]+b[2])/2, (b[1]+b[3])/2
        if abs(ax-bx) < 12 and 4 < by-ay < 16:
            pairs += 1
            # черта между ними?
            line = any(x0-3 <= ax <= x1+3 and ay < y < by for x0, x1, y in hsegs)
            if line: with_line += 1
            if len(samples) < 8: samples.append((a[4], b[4], round(by-ay,1), line))
            break
print(f"stacked elev pairs (|dx|<12, dy 4..16): {pairs}; из них с чертой между: {with_line}")
for s in samples: print("  ", s)

# флажок → ближайшая отметка
import statistics
if flags:
    ds = []
    for fl in flags:
        fx, fy = (fl[0]+fl[2])/2, (fl[1]+fl[3])/2
        best = min((math.hypot((w[0]+w[2])/2-fx, (w[1]+w[3])/2-fy), w[4]) for w in elev)
        ds.append(best[0])
    ds.sort()
    print("flag→elev dist: min=%.1f median=%.1f p90=%.1f; <20pt: %d/%d" % (
        ds[0], statistics.median(ds), ds[int(len(ds)*.9)], sum(1 for x in ds if x < 20), len(ds)))
doc.close()
