#!/usr/bin/env python3
"""Проба 9: пары «подпись помещения ↔ площадь» (вертикальная пара на плане)
и колонки площадей (таблица-экспликация) на отделке."""
import collections, re
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/"
import os
fn = next(f for f in sorted(os.listdir(BASE)) if f.startswith("АР — ") and "отделка помещений" in f and f.endswith(".pdf"))
doc = fitz.open(BASE+fn); pg = doc[0]

lines = []
for b in pg.get_text("dict").get("blocks", []):
    for ln in b.get("lines", []):
        t = "".join(str(s.get("text") or "") for s in ln.get("spans", [])).strip()
        if t: lines.append({"t": t, "b": ln["bbox"]})

AREA = re.compile(r"^\d+[,.]\d{1,2}$")
ROOM = re.compile(r"(комнат|кухн|санузел|коридор|прихож|лоджи|балкон|ванн|гардероб|спальн|гостин|холл|кладов|ниша)", re.I)
areas = [l for l in lines if AREA.match(l["t"])]
rooms = [l for l in lines if ROOM.search(l["t"])]
print(f"{fn.split('—')[-1].strip()[:12]}: lines={len(lines)} area_lines={len(areas)} room_lines={len(rooms)}")

pair = 0; samples = []
for r in rooms:
    rx = (r["b"][0]+r["b"][2])/2; ry1 = r["b"][3]; h = r["b"][3]-r["b"][1]
    for a in areas:
        ax = (a["b"][0]+a["b"][2])/2
        if abs(ax-rx) < (r["b"][2]-r["b"][0])/2 + 6 and 0 <= a["b"][1]-ry1 < 2.2*h:
            pair += 1
            if len(samples) < 5: samples.append((r["t"][:22], a["t"]))
            break
print(f"room->area vertical pair: {pair}/{len(rooms)}; samples={samples}")

# колонки площадей (таблица): X-кластеры центров area-строк
xs = sorted((l["b"][0]+l["b"][2])/2 for l in areas)
clusters = []
for x in xs:
    if clusters and x-clusters[-1][-1] < 8: clusters[-1].append(x)
    else: clusters.append([x])
big = [c for c in clusters if len(c) >= 8]
print(f"area X-clusters>=8 (столбцы таблиц): {len(big)} sizes={[len(c) for c in big]}")
doc.close()
