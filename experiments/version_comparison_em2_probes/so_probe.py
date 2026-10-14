"""Структура таблицы СО (11) стр. 53–54: шапка, колонки, строки, перенос позиции через страницу.

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, fitz
P=sys.argv[1]
d=fitz.open(f"{P}/133_23-ГК-ЭМ2 (11).pdf")
for pn in [53,54]:
    pg=d[pn-1]
    # vertical lines from drawings -> column boundaries
    xs=set()
    for dr in pg.get_drawings():
        for it in dr['items']:
            if it[0]=='l':
                p1,p2=it[1],it[2]
                if abs(p1.x-p2.x)<0.5 and abs(p1.y-p2.y)>200: xs.add(round(p1.x))
    print(f"p{pn} {pg.rect}: long vertical rules x =", sorted(xs))
    blocks=pg.get_text("dict")["blocks"]
    lines=[]
    for b in blocks:
        for l in b.get("lines",[]):
            t="".join(s["text"] for s in l["spans"]).strip()
            if t: lines.append((round(l["bbox"][1]),round(l["bbox"][0]),t))
    lines.sort()
    for y,x,t in lines[:45]: print(f"   y={y:4d} x={x:4d} {t[:70]}")
