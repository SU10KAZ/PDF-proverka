#!/usr/bin/env python3
"""Проба 2: grid-детекция ВНУТРИ bbox табличного блока (по данным инвентарей).
Проверяем: линии сетки в drawings, кластеризация в xs/ys, раскладка слов по ячейкам,
многоуровневая шапка (V-линии частичной высоты), толщины линий рамка/внутренние."""
import statistics, collections, json
import fitz

D = "/home/coder/projects/PDF-proverka/projects_v2/objects/214_Alia_ASTERUS/disciplines"
BLOCKS = [
    ("АР спец.дверей 99CR-HQYG-RNQ", f"{D}/AR/documents/13АВ-РД-АР1.2-К6/versions/v001/02_work/document.pdf",
     21, [0.6259977194982896, 0.3897724984900342, 0.7291904218928165, 0.4847996778739682]),
    ("АР сводная ведомость 736C-EWHA-C6T", f"{D}/AR/documents/13АВ-РД-АР4.1-К4/versions/v001/02_work/document.pdf",
     22, [0.6106043329532497, 0.5489721886336155, 0.9908779931584949, 0.7629987908101572]),
    ("АР ведомость отделки R76F-HEU6-WMH", f"{D}/AR/documents/13АВ-РД-АР1.2-ПА/versions/v001/02_work/document.pdf",
     11, [0.6771854438028736, 0.8483466362599772, 0.8235531086343494, 0.9857468643101482]),
    ("ЭОМ каб.журнал ОЗДС AACF-YPAE-6FH", f"{D}/EOM/documents/13АВ-РД-ОЗДС/versions/v001/02_work/document.pdf",
     19, [0.04754230459307011, 0.014245014245014245, 0.9895245769540693, 0.764102564102564]),
    ("ЭОМ спецификация ОЗДС 4TCV-V3PW-D6T", f"{D}/EOM/documents/13АВ-РД-ОЗДС/versions/v001/02_work/document.pdf",
     None, None),  # найдём по заголовку
]

def segs(page, clip):
    H, V = [], []
    widths_h = collections.Counter()
    for dr in page.get_drawings():
        r = dr["rect"]
        if clip and (r.x1 < clip[0] - 2 or r.x0 > clip[2] + 2 or r.y1 < clip[1] - 2 or r.y0 > clip[3] + 2):
            continue
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
                if abs(y2 - y1) < 0.6 and abs(x2 - x1) > 3:
                    H.append((min(x1, x2), max(x1, x2), (y1 + y2) / 2, w))
                    widths_h[round(w or 0, 2)] += 1
                elif abs(x2 - x1) < 0.6 and abs(y2 - y1) > 3:
                    V.append((min(y1, y2), max(y1, y2), (x1 + x2) / 2, w))
    return H, V, widths_h

def cluster(vals, tol=1.5):
    out = []
    for v in sorted(vals):
        if out and v - out[-1][-1] <= tol:
            out[-1].append(v)
        else:
            out.append([v])
    return [statistics.median(c) for c in out]

def analyze(name, pdf, page_no, bbox_norm):
    doc = fitz.open(pdf)
    if page_no is None:  # найти страницу спецификации ОЗДС
        for i in range(len(doc)):
            t = doc[i].get_text()
            if "Спецификация оборудования" in t and "Позиция" in t:
                page_no, bbox_norm = i, [0.03, 0.01, 0.99, 0.95]
                break
        else:
            print(name, ": страница не найдена"); return
    pg = doc[page_no]
    W, Hh = pg.rect.width, pg.rect.height
    # проверим 0-based/1-based по заголовку
    txt = pg.get_text()[:300].replace("\n", " ")
    clip = [bbox_norm[0] * W, bbox_norm[1] * Hh, bbox_norm[2] * W, bbox_norm[3] * Hh]
    bw, bh = clip[2] - clip[0], clip[3] - clip[1]
    H, V, widths = segs(pg, clip)
    Hl = [h for h in H if h[1] - h[0] >= 0.5 * bw]
    Vl = [v for v in V if v[1] - v[0] >= 0.10 * bh]
    ys = cluster([h[2] for h in Hl])
    xs = cluster([v[2] for v in Vl])
    words = [w for w in pg.get_text("words")
             if clip[0] - 1 <= (w[0] + w[2]) / 2 <= clip[2] + 1 and clip[1] - 1 <= (w[1] + w[3]) / 2 <= clip[3] + 1]
    print(f"\n===== {name}  (стр. {page_no}, блок {bw:.0f}x{bh:.0f}pt)")
    print(f"  контекст листа: {txt[:110]}")
    print(f"  H-сегм в блоке={len(H)} (длинных {len(Hl)} → {len(ys)} Y-линий), V-сегм={len(V)} (длинных {len(Vl)} → {len(xs)} X-линий), слов={len(words)}")
    print(f"  толщины H-линий: {widths.most_common(5)}")
    if len(ys) < 3 or len(xs) < 3:
        print("  !! сетка не собралась"); return
    rh = [round(b - a, 1) for a, b in zip(ys, ys[1:])]
    cw = [round(b - a, 1) for a, b in zip(xs, xs[1:])]
    print(f"  строк {len(rh)}, высоты: медиана {statistics.median(rh):.1f}, все: {rh[:20]}")
    print(f"  колонок {len(cw)}, ширины: {cw[:20]}")
    # V-линии полной высоты vs частичной (шапка с объединёнными ячейками / шапка над телом)
    table_top, table_bot = min(ys), max(ys)
    full = [v for v in Vl if v[0] <= table_top + 3 and v[1] >= table_bot - 3]
    partial = [v for v in Vl if v not in full]
    print(f"  V-линий полной высоты: {len(full)}, частичной: {len(partial)}")
    # раскладка слов по ячейкам
    import bisect
    cells = collections.defaultdict(list)
    orphan = 0
    for w in words:
        cx, cy = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
        ci = bisect.bisect_right(xs, cx) - 1
        ri = bisect.bisect_right(ys, cy) - 1
        if 0 <= ci < len(xs) - 1 and 0 <= ri < len(ys) - 1:
            cells[(ri, ci)].append(w[4])
        else:
            orphan += 1
    print(f"  слов в ячейках: {len(words)-orphan}/{len(words)} = {(len(words)-orphan)/max(1,len(words)):.0%}; заполненных ячеек {len(cells)} из {(len(xs)-1)*(len(ys)-1)}")
    # шапка = первые 1-2 ряда
    for r in range(min(2, len(ys) - 1)):
        hdr = [" ".join(cells.get((r, c), [])) for c in range(len(xs) - 1)]
        print(f"  ряд {r}: {[h[:14] for h in hdr]}")
    # пример 2 строк тела
    for r in range(2, min(5, len(ys) - 1)):
        row = [" ".join(cells.get((r, c), [])) for c in range(len(xs) - 1)]
        if any(row):
            print(f"  тело р{r}: {[x[:14] for x in row]}")

for b in BLOCKS:
    try:
        analyze(*b)
    except Exception as e:
        import traceback; traceback.print_exc()
