#!/usr/bin/env python3
"""Проба 3: универсальный детектор ТАБЛИЧНЫХ ОБЛАСТЕЙ на странице.
Алгоритм-прототип table_structured: длинные H-линии → группировка по X-перекрытию и
Y-соседству → регион → V-линии региона → сетка xs/ys → ячейки → слова → шапка."""
import statistics, collections, bisect, sys
import fitz

def h_v_segments(page, min_len=60.0):
    H, V = [], []
    for dr in page.get_drawings():
        w = dr.get("width")
        for it in dr["items"]:
            k = it[0]
            if k == "l":
                pairs = [(it[1], it[2])]
            elif k == "re":
                rr = it[1]
                pairs = [((rr.x0, rr.y0), (rr.x1, rr.y0)), ((rr.x0, rr.y1), (rr.x1, rr.y1)),
                         ((rr.x0, rr.y0), (rr.x0, rr.y1)), ((rr.x1, rr.y0), (rr.x1, rr.y1))]
            elif k == "qu":
                q = it[1]
                ps = [q.ul, q.ur, q.lr, q.ll]
                pairs = [(ps[i], ps[(i + 1) % 4]) for i in range(4)]
            else:
                continue
            for p1, p2 in pairs:
                x1, y1 = (p1.x, p1.y) if hasattr(p1, "x") else p1
                x2, y2 = (p2.x, p2.y) if hasattr(p2, "x") else p2
                if abs(y2 - y1) < 0.6 and abs(x2 - x1) >= min_len:
                    H.append((min(x1, x2), max(x1, x2), (y1 + y2) / 2, w or 0))
                elif abs(x2 - x1) < 0.6 and abs(y2 - y1) >= 8:
                    V.append((min(y1, y2), max(y1, y2), (x1 + x2) / 2, w or 0))
    return H, V

def overlap(a0, a1, b0, b1):
    o = min(a1, b1) - max(a0, b0)
    return o / max(1e-6, min(a1 - a0, b1 - b0))

def cluster(vals, tol=1.5):
    out = []
    for v in sorted(vals):
        if out and v - out[-1][-1] <= tol:
            out[-1].append(v)
        else:
            out.append([v])
    return [statistics.median(c) for c in out]

def find_tables(page, min_rows=3, max_gap=70.0):
    H, V = h_v_segments(page)
    used = [False] * len(H)
    order = sorted(range(len(H)), key=lambda i: H[i][2])
    tables = []
    for i in order:
        if used[i]:
            continue
        grp = [i]; used[i] = True
        x0, x1, ylast = H[i][0], H[i][1], H[i][2]
        for j in order:
            if used[j]:
                continue
            if H[j][2] - ylast > max_gap:
                continue
            if overlap(x0, x1, H[j][0], H[j][1]) >= 0.75:
                grp.append(j); used[j] = True
                x0, x1 = min(x0, H[j][0]), max(x1, H[j][1])
                ylast = max(ylast, H[j][2])
        ys = cluster([H[j][2] for j in grp])
        if len(ys) < min_rows + 1:
            continue
        y0g, y1g = min(ys), max(ys)
        vin = [v for v in V if v[2] > x0 - 2 and v[2] < x1 + 2
               and overlap(y0g, y1g, v[0], v[1]) > 0 and (v[1] - v[0]) >= 0.5 * (y1g - y0g) / len(ys)]
        # V-линии, реально принадлежащие таблице: покрывают >=15% высоты региона
        vgood = [v for v in vin if (min(v[1], y1g) - max(v[0], y0g)) >= 0.15 * (y1g - y0g)]
        xs = cluster([v[2] for v in vgood])
        if len(xs) < 3:
            continue
        tables.append({"bbox": (min(xs), y0g, max(xs), y1g), "xs": xs, "ys": ys,
                       "n_h": len(grp), "n_v": len(vgood)})
    return tables

def parse_table(page, t, label=""):
    xs, ys = t["xs"], t["ys"]
    x0, y0, x1, y1 = t["bbox"]
    words = [w for w in page.get_text("words")
             if x0 - 1 <= (w[0] + w[2]) / 2 <= x1 + 1 and y0 - 1 <= (w[1] + w[3]) / 2 <= y1 + 1]
    cells = collections.defaultdict(list)
    for w in sorted(words, key=lambda w: (w[1], w[0])):
        cx, cy = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
        ci = bisect.bisect_right(xs, cx) - 1
        ri = bisect.bisect_right(ys, cy) - 1
        if 0 <= ci < len(xs) - 1 and 0 <= ri < len(ys) - 1:
            cells[(ri, ci)].append(w[4])
    nrows, ncols = len(ys) - 1, len(xs) - 1
    filled = len(cells)
    rh = [round(b - a, 1) for a, b in zip(ys, ys[1:])]
    print(f"  ТАБЛИЦА {label}: bbox=({x0:.0f},{y0:.0f},{x1:.0f},{y1:.0f}) {nrows}x{ncols} "
          f"(H-линий {t['n_h']}, V-линий {t['n_v']}), слов {len(words)}, ячеек заполнено {filled}")
    print(f"    высоты строк: {rh[:14]}")
    for r in range(min(3, nrows)):
        row = [" ".join(cells.get((r, c), []))[:16] for c in range(ncols)]
        print(f"    ряд {r}: {row}")
    body = [r for r in range(nrows) if any(cells.get((r, c)) for c in range(ncols))]
    if len(body) > 4:
        r = body[len(body) // 2]
        print(f"    середина р{r}: {[' '.join(cells.get((r, c), []))[:16] for c in range(ncols)]}")
    return cells, nrows, ncols

D = "/home/coder/projects/PDF-proverka/projects_v2/objects/214_Alia_ASTERUS/disciplines"
PAGES = [
    ("ОЗДС спецификация стр.21", f"{D}/EOM/documents/13АВ-РД-ОЗДС/versions/v001/02_work/document.pdf", 21),
    ("ОЗДС каб.журнал стр.19", f"{D}/EOM/documents/13АВ-РД-ОЗДС/versions/v001/02_work/document.pdf", 19),
    ("АР1.2-К6 спец.дверей стр.21", f"{D}/AR/documents/13АВ-РД-АР1.2-К6/versions/v001/02_work/document.pdf", 21),
    ("АР1.2-ПА ведомость отделки стр.11", f"{D}/AR/documents/13АВ-РД-АР1.2-ПА/versions/v001/02_work/document.pdf", 11),
    ("КЖ5.1-К1К2 спецификация стр.13", f"{D}/KJ/documents/13АВ-РД-КЖ5.1-К1К2/versions/v001/02_work/document.pdf", 13),
]
for name, pdf, pno in PAGES:
    try:
        doc = fitz.open(pdf)
        pg = doc[pno]
        tabs = find_tables(pg)
        # отсечь штамп (маленькие) — отчёт по 3 крупнейшим по площади
        tabs.sort(key=lambda t: -(t["bbox"][2] - t["bbox"][0]) * (t["bbox"][3] - t["bbox"][1]))
        print(f"\n===== {name}: найдено табличных регионов {len(tabs)} (стр {pg.rect.width:.0f}x{pg.rect.height:.0f})")
        for k, t in enumerate(tabs[:3]):
            parse_table(pg, t, f"#{k}")
    except Exception:
        import traceback; traceback.print_exc()
