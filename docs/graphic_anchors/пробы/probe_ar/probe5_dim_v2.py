#!/usr/bin/env python3
"""Проба 5: детектор размеров v2 (учёт разрыва линии текстом + повёрнутый текст).
Метрики: link-rate чисел, цепочки (>=3 засечки), масштаб мм/pt, сверка сумм."""
import collections, math, re
import fitz

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/"
FILES = {
    "masonry_7DU7": "АР — 145 кладочный план — кладочный план — 7DU7-346V-DN6.pdf",
    "wall_elev_69JM": "АР — 227 развёртка стены — развёртка стены — 69JM-X6EC-UTQ.pdf",
    "masonry_6QT6": "АР — 146 кладочный план — кладочный план — 6QT6-LNTR-DCC.pdf",
    "opening_plan": "АР — 054 план отверстий — маркировка отверстий — 44QM-MXHJ-R44.pdf",
}
import os
# найдём реальный файл плана отверстий
for f in os.listdir(BASE):
    if "план отверстий" in f and f.endswith(".pdf"):
        FILES["opening_plan"] = f; break

NUM = re.compile(r"^\d{2,5}$")

def load(pg):
    segs = []
    for d in pg.get_drawings():
        for it in d.get("items", []):
            if it[0] == "l":
                p1 = (it[1].x, it[1].y); p2 = (it[2].x, it[2].y)
                L = math.hypot(p2[0]-p1[0], p2[1]-p1[1])
                if L > 0.4: segs.append({"p1": p1, "p2": p2, "len": L, "w": d.get("width") or 0})
    return segs

def run(tag, fn):
    doc = fitz.open(BASE + fn); pg = doc[0]
    segs = load(pg)
    H = [s for s in segs if abs(s["p1"][1]-s["p2"][1]) <= 0.5 and s["len"] >= 2]
    V = [s for s in segs if abs(s["p1"][0]-s["p2"][0]) <= 0.5 and s["len"] >= 2]
    TK = [s for s in segs if 1.0 <= s["len"] <= 8.0 and abs(s["p1"][0]-s["p2"][0]) > 0.3 and abs(s["p1"][1]-s["p2"][1]) > 0.3
          and 0.4 < abs(s["p1"][0]-s["p2"][0])/max(abs(s["p1"][1]-s["p2"][1]),1e-9) < 2.5]
    words = pg.get_text("words")
    nums = [w for w in words if NUM.match(w[4])]
    linked = []
    for w in nums:
        x0,y0,x1,y1,txt = w[0],w[1],w[2],w[3],w[4]
        cx, cy = (x0+x1)/2, (y0+y1)/2
        rotated = (y1-y0) > (x1-x0) and len(txt) >= 2
        hit = None
        if not rotated:
            # a) разрыв: H-сегменты слева и справа на уровне cy +-2.5
            left = [s for s in H if abs(s["p1"][1]-cy) <= 2.5 and max(s["p1"][0],s["p2"][0]) <= x0+1 and x0-max(s["p1"][0],s["p2"][0]) < 12]
            right = [s for s in H if abs(s["p1"][1]-cy) <= 2.5 and min(s["p1"][0],s["p2"][0]) >= x1-1 and min(s["p1"][0],s["p2"][0])-x1 < 12]
            if left and right:
                hit = ("h_break", cy)
            else:
                below = [s for s in H if y1-0.5 <= s["p1"][1] <= y1+7 and min(s["p1"][0],s["p2"][0])-2 <= cx <= max(s["p1"][0],s["p2"][0])+2]
                if below: hit = ("h_below", below[0]["p1"][1])
        else:
            up = [s for s in V if abs(s["p1"][0]-cx) <= 8 and max(s["p1"][1],s["p2"][1]) <= y0+1 and y0-max(s["p1"][1],s["p2"][1]) < 12]
            dn = [s for s in V if abs(s["p1"][0]-cx) <= 8 and min(s["p1"][1],s["p2"][1]) >= y1-1 and min(s["p1"][1],s["p2"][1])-y1 < 12]
            if up and dn:
                sx = up[0]["p1"][0]
                hit = ("v_break", sx)
            else:
                side = [s for s in V if (x1-0.5 <= s["p1"][0] <= x1+7 or x0-7 <= s["p1"][0] <= x0+0.5) and min(s["p1"][1],s["p2"][1])-2 <= cy <= max(s["p1"][1],s["p2"][1])+2]
                if side: hit = ("v_side", side[0]["p1"][0])
        if hit: linked.append((w, hit))
    modes = collections.Counter(h[0] for _,h in linked)
    print(f"== {tag}: nums={len(nums)} linked={len(linked)} ({len(linked)/max(len(nums),1)*100:.0f}%) modes={dict(modes)}")

    # ЦЕПОЧКИ: группируем привязанные числа по (ось, координата линии round 1pt)
    groups = collections.defaultdict(list)
    for w, (mode, coord) in linked:
        axis = "h" if mode.startswith("h") else "v"
        groups[(axis, round(coord))].append(w)
    chains = 0; scale_all = []
    sum_checks = []
    for (axis, coord), ws in groups.items():
        if len(ws) < 2: continue
        # засечки на этой линии
        tks = []
        for t in TK:
            mx = (t["p1"][0]+t["p2"][0])/2; my = (t["p1"][1]+t["p2"][1])/2
            if axis == "h" and abs(my-coord) <= 2: tks.append(mx)
            if axis == "v" and abs(mx-coord) <= 2: tks.append(my)
        tks.sort()
        dtk = []
        for x in tks:
            if not dtk or x-dtk[-1] > 0.8: dtk.append(x)
        if len(dtk) < 3: continue
        chains += 1
        ws.sort(key=lambda w: (w[0] if axis=="h" else w[1]))
        vals = [int(w[4]) for w in ws]
        # число -> интервал между соседними засечками
        for w in ws:
            c = (w[0]+w[2])/2 if axis=="h" else (w[1]+w[3])/2
            best = None
            for a,b in zip(dtk, dtk[1:]):
                mid=(a+b)/2
                if abs(mid-c) < (b-a)/2 + 8:
                    cand=(abs(mid-c), b-a)
                    if best is None or cand<best: best=cand
            if best and best[1] > 1.5:
                scale_all.append(int(w[4])/best[1])
        # сумма цепочки vs полный охват засечек
        span = dtk[-1]-dtk[0]
        sum_checks.append((vals, round(span,1)))
    print(f"   chains(>=2 nums,>=3 ticks)={chains}")
    if scale_all:
        scale_all.sort(); med = scale_all[len(scale_all)//2]
        close = sum(1 for s in scale_all if abs(s-med)/med < 0.07)
        print(f"   scale med={med:.2f} mm/pt consistent±7%={close}/{len(scale_all)}")
        # сверка: сумма чисел цепочки ~ span*масштаб?
        ok=bad=0
        for vals, span in sum_checks:
            expect = span*med
            s = sum(vals)
            if abs(s-expect)/max(expect,1) < 0.12: ok+=1
            else: bad+=1
        print(f"   chain-sum vs tick-span*scale: ok={ok} mismatch={bad} (mismatch может быть неполной цепочкой)")
        for vals, span in sum_checks[:5]:
            print(f"     vals={vals} sum={sum(vals)} span_pt={span} span*scale={span*med:.0f}")
    doc.close()

for tag, fn in FILES.items():
    try: run(tag, fn)
    except Exception as e: print(f"== {tag} FAIL {e}")
