#!/usr/bin/env python3
"""ЭОМ силовой план: Гр.N/щиты vs цветные трассы — расстояния и цвета ближайших сегментов."""
import sys, json, re, math
from collections import Counter
import fitz

GR_RE = re.compile(r"^(Гр|гр)\.?\s*\d|^К\d+(\.\d+)+")
PANEL_RE = re.compile(r"^(ЩР|ЩО|ЩЭ|ЩК|ВРУ|ГРЩ|ЩАО|ЩС|ШР|ЯУО|ЩМ)")

def seg_dist(px, py, x1, y1, x2, y2):
    dx, dy = x2 - x1, y2 - y1
    L2 = dx*dx + dy*dy
    if L2 == 0:
        return math.hypot(px - x1, py - y1)
    t = max(0, min(1, ((px - x1)*dx + (py - y1)*dy) / L2))
    return math.hypot(px - (x1 + t*dx), py - (y1 + t*dy))

def main(path):
    doc = fitz.open(path)
    pg = doc[0]
    words = pg.get_text("words")
    segs = []  # (x1,y1,x2,y2,color,width,len)
    for d in pg.get_drawings():
        if d.get("fill") is not None and d.get("color") is None:
            continue
        c = d.get("color")
        w = d.get("width") or 0
        for it in d["items"]:
            if it[0] != "l":
                continue
            p1, p2 = it[1], it[2]
            ln = math.hypot(p2.x - p1.x, p2.y - p1.y)
            if ln < 1.0:
                continue
            segs.append((p1.x, p1.y, p2.x, p2.y, c, w, ln))
    def ckey(c):
        if c is None: return "none"
        return "(" + ",".join(f"{v:.2f}" for v in c) + ")"
    gr, panels = [], []
    for w in words:
        t = w[4]
        cx, cy = (w[0]+w[2])/2, (w[1]+w[3])/2
        if GR_RE.match(t):
            gr.append((t, cx, cy))
        elif PANEL_RE.match(t):
            panels.append((t, cx, cy))
    print(f"words={len(words)} segs={len(segs)} gr_labels={len(gr)} panel_labels={len(panels)}")
    # for each Гр label: nearest segment overall + nearest VIVID segment
    def vivid(c):
        if c is None: return False
        mx, mn = max(c), min(c)
        return mx - mn >= 0.2
    res = Counter(); vivid_res = Counter(); dists = []; vdists = []
    for t, cx, cy in gr[:250]:
        best = (1e9, None); bestv = (1e9, None)
        for s in segs:
            dd = seg_dist(cx, cy, s[0], s[1], s[2], s[3])
            if dd < best[0]:
                best = (dd, s)
            if vivid(s[4]) and dd < bestv[0]:
                bestv = (dd, s)
        if best[1]:
            res[ckey(best[1][4])] += 1; dists.append(best[0])
        if bestv[1]:
            vivid_res[ckey(bestv[1][4])] += 1; vdists.append(bestv[0])
    def stat(a):
        if not a: return None
        a = sorted(a)
        return {"min": round(a[0],1), "med": round(a[len(a)//2],1), "p90": round(a[int(len(a)*0.9)],1), "max": round(a[-1],1)}
    print("Гр→ближайший сегмент, цвета:", dict(res.most_common(8)))
    print("Гр→ближайший сегмент, дистанции:", stat(dists))
    print("Гр→ближайший VIVID сегмент, цвета:", dict(vivid_res.most_common(8)))
    print("Гр→ближайший VIVID, дистанции:", stat(vdists))
    print("панели (10):", [t for t,_,_ in panels[:10]])
    print("Гр примеры (15):", [t for t,_,_ in gr[:15]])

if __name__ == "__main__":
    main(sys.argv[1])
