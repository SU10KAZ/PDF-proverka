#!/usr/bin/env python3
"""Проба 4: группировка H-линий по ОДИНАКОВОМУ X-спану (кластер (x0,x1) ±6pt),
а не по жадному overlap — гипотеза: это разлепляет таблицы от рамки листа и соседних таблиц."""
import statistics, collections, bisect
import fitz
exec(open('/tmp/claude-1001/-home-coder-projects-PDF-proverka/6f655732-ccf4-4ad5-bf7d-95fbe65d0668/scratchpad/probe_tables/probe_table_regions.py').read().split('D = "/home/coder')[0])

def find_tables_span(page, min_rows=3, max_gap=90.0, span_tol=6.0):
    H, V = h_v_segments(page)
    # кластеризуем по (x0, x1)
    groups = []
    for h in sorted(H, key=lambda h: (h[0], h[1], h[2])):
        placed = False
        for g in groups:
            if abs(h[0] - g["x0"]) <= span_tol and abs(h[1] - g["x1"]) <= span_tol:
                g["lines"].append(h); placed = True
                break
        if not placed:
            groups.append({"x0": h[0], "x1": h[1], "lines": [h]})
    tables = []
    for g in groups:
        ys_all = sorted(h[2] for h in g["lines"])
        # разрыв по Y > max_gap → отдельные таблицы одного спана
        runs, cur = [], [ys_all[0]]
        for y in ys_all[1:]:
            if y - cur[-1] <= max_gap:
                cur.append(y)
            else:
                runs.append(cur); cur = [y]
        runs.append(cur)
        for run in runs:
            ys = cluster(run)
            if len(ys) < min_rows + 1:
                continue
            y0g, y1g = min(ys), max(ys)
            x0, x1 = g["x0"], g["x1"]
            vin = [v for v in V if x0 - 3 <= v[2] <= x1 + 3
                   and (min(v[1], y1g) - max(v[0], y0g)) >= 0.15 * (y1g - y0g)]
            xs = cluster([v[2] for v in vin])
            if len(xs) < 3:
                continue
            tables.append({"bbox": (min(xs), y0g, max(xs), y1g), "xs": xs, "ys": ys,
                           "n_h": len(run), "n_v": len(vin)})
    return tables

D = "/home/coder/projects/PDF-proverka/projects_v2/objects/214_Alia_ASTERUS/disciplines"
CASES = [
    ("АР1.2-ПА стр.11 (эскизы+спец.дверей+экспликация)", f"{D}/AR/documents/13АВ-РД-АР1.2-ПА/versions/v001/02_work/document.pdf", 11),
    ("КЖ5.1-К1К2 стр.8 (спец.арматуры+вед.деталей)", f"{D}/KJ/documents/13АВ-РД-КЖ5.1-К1К2/versions/v001/02_work/document.pdf", 8),
    ("ОЗДС стр.19 (каб.журнал)", f"{D}/EOM/documents/13АВ-РД-ОЗДС/versions/v001/02_work/document.pdf", 19),
]
for name, pdf, pno in CASES:
    pg = fitz.open(pdf)[pno]
    tabs = find_tables_span(pg)
    tabs.sort(key=lambda t: -(t["bbox"][2] - t["bbox"][0]) * (t["bbox"][3] - t["bbox"][1]))
    print(f"\n===== {name}: регионов {len(tabs)}")
    for k, t in enumerate(tabs[:6]):
        x0, y0, x1, y1 = t["bbox"]
        w, h = x1 - x0, y1 - y0
        print(f"  #{k}: ({x0:.0f},{y0:.0f})-({x1:.0f},{y1:.0f}) {len(t['ys'])-1}стр x {len(t['xs'])-1}кол  [{w:.0f}x{h:.0f}pt]")
        parse_table(pg, t, f"#{k}")
