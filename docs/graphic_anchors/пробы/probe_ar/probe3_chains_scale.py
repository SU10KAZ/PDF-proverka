#!/usr/bin/env python3
"""Проба 3: размерные ЦЕПОЧКИ на кладочном плане.
Кластеризуем коллинеарные горизонтальные сегменты (одна y ±0.7, перекрытие/зазор<=3),
находим засечки на кластере, числа над кластером; проверяем арифметику масштаба:
mm_число / pt_между_засечками = const (масштаб чертежа)."""
import collections, math, re
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/"
FN = "АР — 145 кладочный план — кладочный план — 7DU7-346V-DN6.pdf"
NUM = re.compile(r"^\d{2,5}$")

doc = fitz.open(BASE + FN); pg = doc[0]
segs = []
for d in pg.get_drawings():
    for it in d.get("items", []):
        if it[0] == "l":
            p1 = (it[1].x, it[1].y); p2 = (it[2].x, it[2].y)
            L = math.hypot(p2[0]-p1[0], p2[1]-p1[1])
            segs.append({"p1": p1, "p2": p2, "len": L, "w": d.get("width") or 0})

hs = [s for s in segs if abs(s["p1"][1]-s["p2"][1]) <= 0.5 and s["len"] >= 2]
ticks = [s for s in segs if 1.0 <= s["len"] <= 6.0 and abs(s["p1"][0]-s["p2"][0]) > 0.3 and abs(s["p1"][1]-s["p2"][1]) > 0.3]
words = pg.get_text("words")
nums = [w for w in words if NUM.match(w[4])]

# кластеризация коллинеарных горизонталей: bucket по round(y*2)/2, потом слить по X при зазоре<=4
by_y = collections.defaultdict(list)
for s in hs:
    by_y[round(s["p1"][1]*2)/2].append(s)
chains = []
for y, group in by_y.items():
    iv = sorted([(min(s["p1"][0],s["p2"][0]), max(s["p1"][0],s["p2"][0])) for s in group])
    cur = list(iv[0])
    merged = []
    for a,b in iv[1:]:
        if a <= cur[1] + 4: cur[1] = max(cur[1], b)
        else: merged.append(tuple(cur)); cur = [a,b]
    merged.append(tuple(cur))
    for a,b in merged:
        if b-a >= 25: chains.append({"y": y, "x0": a, "x1": b})

print(f"h-chain candidates(len>=25pt): {len(chains)}")
report = 0
scale_samples = []
for ch in chains:
    tk = sorted(mx for t in ticks
                if abs((t["p1"][1]+t["p2"][1])/2 - ch["y"]) <= 1.5
                and ch["x0"]-1.5 <= (mx:=(t["p1"][0]+t["p2"][0])/2) <= ch["x1"]+1.5)
    # дедуп засечек ближе 0.8pt
    dtk = []
    for x in tk:
        if not dtk or x - dtk[-1] > 0.8: dtk.append(x)
    if len(dtk) < 3: continue
    # числа над кластером: y1 текста в [y-8, y], центр внутри [x0,x1]
    ns = sorted((( (w[0]+w[2])/2, w[4]) for w in nums
                 if ch["y"]-9 <= w[3] <= ch["y"]+1 and ch["x0"]-2 <= (w[0]+w[2])/2 <= ch["x1"]+2))
    if not ns: continue
    # сопоставить числа с интервалами между соседними засечками (центр интервала ~ центр числа)
    pairs = []
    for cx, txt in ns:
        best = None
        for a,b in zip(dtk, dtk[1:]):
            mid = (a+b)/2
            if abs(mid-cx) < (b-a)/2 + 6:
                cand = (abs(mid-cx), b-a)
                if best is None or cand < best: best = cand
        if best: pairs.append((txt, round(best[1],2), round(int(txt)/best[1],2)))
    if pairs and report < 10:
        report += 1
        print(f" chain y={ch['y']:.1f} x=[{ch['x0']:.0f},{ch['x1']:.0f}] ticks={len(dtk)} nums={[t for _,t in ns]}")
        print(f"   num->gap_pt->mm_per_pt: {pairs}")
        scale_samples += [p[2] for p in pairs]
if scale_samples:
    scale_samples.sort()
    med = scale_samples[len(scale_samples)//2]
    close = sum(1 for s in scale_samples if abs(s-med)/med < 0.05)
    print(f"scale median={med} mm/pt; consistent(±5%)={close}/{len(scale_samples)}")
doc.close()
