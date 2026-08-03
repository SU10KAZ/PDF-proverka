#!/usr/bin/env python3
"""Проба: есть ли в табличных блоках линии сетки в get_drawings и раскладываются ли слова по ячейкам."""
import sys, statistics, collections
import fitz

def segs_from_drawings(page):
    """Все отрезки: 'l' как есть, 're'/'qu' развёрнуты в 4 стороны. + метаданные width/dashes/fill."""
    H, V, other = [], [], []
    meta = collections.Counter()
    widths = collections.Counter()
    dash_vals = collections.Counter()
    fills = 0
    for dr in page.get_drawings():
        w = dr.get("width")
        if dr.get("fill") is not None and dr.get("color") is None:
            fills += 1
        dash_vals[str(dr.get("dashes"))] += 1
        for it in dr["items"]:
            k = it[0]
            meta[k] += 1
            pts = []
            if k == "l":
                p1, p2 = it[1], it[2]
                pts = [(p1, p2)]
            elif k == "re":
                r = it[1]
                pts = [((r.x0, r.y0), (r.x1, r.y0)), ((r.x0, r.y1), (r.x1, r.y1)),
                       ((r.x0, r.y0), (r.x0, r.y1)), ((r.x1, r.y0), (r.x1, r.y1))]
            elif k == "qu":
                q = it[1]
                ps = [q.ul, q.ur, q.lr, q.ll]
                pts = [(ps[i], ps[(i + 1) % 4]) for i in range(4)]
            else:
                other.append(k)
                continue
            for p1, p2 in pts:
                x1, y1 = (p1.x, p1.y) if hasattr(p1, "x") else p1
                x2, y2 = (p2.x, p2.y) if hasattr(p2, "x") else p2
                dx, dy = abs(x2 - x1), abs(y2 - y1)
                if w is not None:
                    widths[round(w, 2)] += 1
                if dy < 0.6 and dx > 3:
                    H.append((min(x1, x2), max(x1, x2), (y1 + y2) / 2, w))
                elif dx < 0.6 and dy > 3:
                    V.append((min(y1, y2), max(y1, y2), (x1 + x2) / 2, w))
    return H, V, meta, widths, dash_vals, fills

def cluster(vals, tol):
    out = []
    for v in sorted(vals):
        if out and v - out[-1][-1] <= tol:
            out[-1].append(v)
        else:
            out.append([v])
    return [statistics.median(c) for c in out]

def probe(path):
    doc = fitz.open(path)
    pg = doc[0]
    words = pg.get_text("words")
    H, V, meta, widths, dashes, fills = segs_from_drawings(pg)
    print(f"\n===== {path.split('/')[-1][:80]}")
    print(f"page {pg.rect.width:.0f}x{pg.rect.height:.0f}pt, words={len(words)}, drawings={len(pg.get_drawings())}, fills(no-stroke)={fills}")
    print(f"item kinds: {dict(meta)}")
    print(f"top widths: {widths.most_common(6)}")
    print(f"dashes: {dashes.most_common(4)}")
    print(f"H-segs={len(H)}, V-segs={len(V)}")
    if not H or not V:
        print("  !! нет сетки"); return
    # длинные линии = кандидаты сетки (>15% габарита)
    Hl = [h for h in H if h[1] - h[0] > 0.15 * pg.rect.width]
    Vl = [v for v in V if v[1] - v[0] > 0.05 * pg.rect.height]
    ys = cluster([h[2] for h in Hl], 1.5)
    xs = cluster([v[2] for v in Vl], 1.5)
    print(f"длинные H={len(Hl)} → {len(ys)} Y-линий; длинные V={len(Vl)} → {len(xs)} X-линий")
    if len(ys) >= 3 and len(xs) >= 3:
        rh = [round(b - a, 1) for a, b in zip(ys, ys[1:])]
        cw = [round(b - a, 1) for a, b in zip(xs, xs[1:])]
        print(f"высоты строк ({len(rh)}): {rh[:18]}")
        print(f"ширины колонок ({len(cw)}): {cw[:18]}")
        # раскладка слов по ячейкам: центр слова между соседними линиями
        x0g, x1g, y0g, y1g = min(xs), max(xs), min(ys), max(ys)
        inside = 0
        for w in words:
            cx, cy = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
            if x0g <= cx <= x1g and y0g <= cy <= y1g:
                inside += 1
        print(f"слов в границах сетки: {inside}/{len(words)} = {inside/max(1,len(words)):.0%}")
        # проверка: слова не пересекают вертикальные линии (центр строго в одной ячейке)?
        crossing = 0
        for w in words:
            cy = (w[1] + w[3]) / 2
            if not (y0g <= cy <= y1g):
                continue
            for x in xs:
                if w[0] < x - 0.5 and w[2] > x + 0.5:
                    crossing += 1
                    break
        print(f"слов, пересекающих V-линию (влезли в 2 ячейки): {crossing}")

for p in sys.argv[1:]:
    try:
        probe(p)
    except Exception as e:
        print(p, "ERROR", e)
