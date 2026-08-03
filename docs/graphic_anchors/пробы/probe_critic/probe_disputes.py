# -*- coding: utf-8 -*-
"""Спот-чек двух спорных фактов между дизайнами:
1) dashes: "мёртв во всём корпусе" (primitives-layer Ф1) vs живые dashes на графике насоса (ov-vk факт 4)
2) осевые кружки: диапазон Ø 20-40 (primitives-layer) vs Ø11.9 на АР 7DU7 (ar-design P4)
"""
import fitz, collections

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин"
PUMP = BASE + "/ВК/ВК — 085 характеристика насоса — вариант исходного комплекта — 4KMQ-9K4P-97R.pdf"
MASN = BASE + "/АР/АР — 145 кладочный план — кладочный план — 7DU7-346V-DN6.pdf"

def dash_hist(path):
    doc = fitz.open(path); page = doc[0]
    h = collections.Counter()
    for d in page.get_drawings():
        h[str(d.get("dashes"))] += 1
    return h

print("=== 1) dashes на графике насоса 4KMQ ===")
print(dict(dash_hist(PUMP)))

print("\n=== 2) кружки на кладочном 7DU7 (drawings целиком из c-items, квадратный bbox) ===")
doc = fitz.open(MASN); page = doc[0]
words = page.get_text("words")
dias = collections.Counter()
with_token = 0
for d in page.get_drawings():
    kinds = [it[0] for it in d["items"]]
    if not kinds or any(k != "c" for k in kinds):
        continue
    r = d["rect"]; w, hgt = r.width, r.height
    if w < 4 or w > 60: continue
    if not (0.8 <= w / max(hgt, 0.01) <= 1.25): continue
    dias[round(w, 1)] += 1
    inside = [t for x0,y0,x1,y1,t,*_ in words
              if r.x0 <= (x0+x1)/2 <= r.x1 and r.y0 <= (y0+y1)/2 <= r.y1]
    if len(inside) == 1:
        with_token += 1
print("гистограмма диаметров all-c квадратных drawings:", dict(sorted(dias.items())))
print("из них ровно с 1 словом внутри:", with_token)
