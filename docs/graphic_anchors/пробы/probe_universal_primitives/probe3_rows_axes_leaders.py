# -*- coding: utf-8 -*-
"""Проба 3: (а) размерные ЦЕПОЧКИ как коллинеарные ряды засечек (без требования
одной длинной линии); (б) повёрнутый текст размеров; (в) осевые кружки с токеном
внутри; (г) выноски: полилиния диагональ+полочка, текст над полочкой."""
import math, re
from collections import Counter, defaultdict
import fitz

FILES = [
    ("AR_uzel_019", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 019 архитектурный узел — архитектурный узел — 9HTK-Y74V-UJ6.pdf"),
    ("AR_plan_097", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/АР/АР — 097 архитектурный план — план этажа — 9KPA-UAWT-9RF.pdf"),
    ("KJ_opalubka_017", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 017 опалубочный план — опалубочный план — 9RUY-GPNG-QKF.pdf"),
    ("KJ_balka_035", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 035 план армирования — армирование балки — FCTE-JGDT-L6X.pdf"),
    ("KJ_zakladnaya_001", "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин/КЖ/КЖ — 001 закладные детали — узел закладной детали — 4HH4-XRCF-939.pdf"),
]

NUM_RE = re.compile(r"^\d{2,5}(?:\*\)?)?$")
AXIS_TOKEN_RE = re.compile(r"^([А-ЯA-Z]с?|\d{1,2})$")

def drawings_data(pg):
    rot = pg.rotation_matrix if pg.rotation else None
    def xf(p):
        if rot is None: return (p.x, p.y)
        q = fitz.Point(p.x, p.y) * rot
        return (q.x, q.y)
    segs, polylines = [], []
    for di, d in enumerate(pg.get_drawings()):
        items = d.get("items") or []
        pts_chain = []
        for it in items:
            if it[0] != "l":
                pts_chain.append(None); continue
            a, b = fitz.Point(*xf(it[1])), fitz.Point(*xf(it[2]))
            L = math.hypot(b.x-a.x, b.y-a.y)
            segs.append({"p1": (a.x, a.y), "p2": (b.x, b.y), "len": L,
                         "w": float(d.get("width") or 0), "di": di,
                         "stroke": d.get("color") is not None, "fill": d.get("fill") is not None})
            pts_chain.append(((a.x, a.y), (b.x, b.y)))
        # полилиния = подряд идущие l-items, конец=начало следующего
        chain = []
        for pc in pts_chain:
            if pc is None:
                if len(chain) >= 2: polylines.append({"pts": chain[:], "di": di})
                chain = []; continue
            if chain and math.hypot(chain[-1][0]-pc[0][0], chain[-1][1]-pc[0][1]) < 0.3:
                chain.append(pc[1])
            else:
                if len(chain) >= 3: polylines.append({"pts": chain[:], "di": di})
                chain = [pc[0], pc[1]]
        if len(chain) >= 3:
            polylines.append({"pts": chain[:], "di": di})
    return segs, polylines

for tag, path in FILES:
    doc = fitz.open(path); pg = doc[0]
    segs, polylines = drawings_data(pg)
    words = pg.get_text("words")
    print(f"\n===== {tag} =====")

    # --- (а) ряды засечек ---
    ticks = []
    for s in segs:
        dx, dy = abs(s["p2"][0]-s["p1"][0]), abs(s["p2"][1]-s["p1"][1])
        if 1.0 <= s["len"] <= 8.0 and dx > 0.3 and dy > 0.3:
            ang = math.degrees(math.atan2(dy, dx))
            if 35 <= ang <= 55:
                ticks.append(((s["p1"][0]+s["p2"][0])/2, (s["p1"][1]+s["p2"][1])/2, s["len"]))
    rows_h = defaultdict(list)
    for tx, ty, tl in ticks:
        rows_h[round(ty/1.2)].append((tx, ty, tl))
    h_chains = []
    for _, pts in rows_h.items():
        if len(pts) < 3: continue
        pts.sort()
        gaps = [b[0]-a[0] for a, b in zip(pts, pts[1:])]
        if not gaps: continue
        med = sorted(gaps)[len(gaps)//2]
        if med < 200:
            h_chains.append({"n": len(pts), "y": round(pts[0][1], 1),
                             "x0": round(pts[0][0], 1), "x1": round(pts[-1][0], 1),
                             "med_gap": round(med, 1)})
    rows_v = defaultdict(list)
    for tx, ty, tl in ticks:
        rows_v[round(tx/1.2)].append((ty, tx, tl))
    v_chains = []
    for _, pts in rows_v.items():
        if len(pts) < 3: continue
        pts.sort()
        gaps = [b[0]-a[0] for a, b in zip(pts, pts[1:])]
        med = sorted(gaps)[len(gaps)//2] if gaps else 0
        if 0 < med < 200:
            v_chains.append({"n": len(pts), "x": round(pts[0][1], 1)})
    print(f"(а) ticks={len(ticks)}; H-цепочек(>=3 засечек)={len(h_chains)}, V-цепочек={len(v_chains)}")
    for c in sorted(h_chains, key=lambda c: -c["n"])[:4]:
        print(f"    H-chain: n={c['n']} y={c['y']} span={c['x0']}..{c['x1']} med_gap={c['med_gap']}")

    # --- (б) повёрнутые слова-числа (dict со шрифтами/dir) ---
    raw = pg.get_text("dict")
    rot_nums = plain_nums = 0
    for b in raw.get("blocks", []):
        for ln in b.get("lines", []):
            d = ln.get("dir", (1, 0))
            t = "".join(sp.get("text", "") for sp in ln.get("spans", []))
            if NUM_RE.match(t.strip()):
                if abs(d[0]) < 0.9: rot_nums += 1
                else: plain_nums += 1
    print(f"(б) чисел-строк: гориз={plain_nums}, повёрнутых={rot_nums}")

    # --- (в) осевые кружки ---
    circles = []
    rotm = pg.rotation_matrix if pg.rotation else None
    for d in pg.get_drawings():
        items = d.get("items") or []
        r = d.get("rect")
        if not items or r is None: continue
        if rotm is not None:
            r = fitz.Rect(r) * rotm
            r.normalize()
        if all(it[0] == "c" for it in items) and len(items) >= 2:
            wdt, hgt = float(r.width), float(r.height)
            if 6 <= wdt <= 40 and 0.85 <= wdt/max(hgt, 0.01) <= 1.18 and d.get("fill") is None:
                circles.append({"cx": (r.x0+r.x1)/2, "cy": (r.y0+r.y1)/2, "d": (wdt+hgt)/2})
    ax_hits = []
    for c in circles:
        inside = [w for w in words
                  if c["cx"]-c["d"]/2 <= (w[0]+w[2])/2 <= c["cx"]+c["d"]/2
                  and c["cy"]-c["d"]/2 <= (w[1]+w[3])/2 <= c["cy"]+c["d"]/2]
        if len(inside) == 1 and AXIS_TOKEN_RE.match(inside[0][4]):
            ax_hits.append({"tok": inside[0][4], "d": round(c["d"], 1),
                            "cx": round(c["cx"], 1), "cy": round(c["cy"], 1)})
    diam_hist = Counter(round(h["d"]) for h in ax_hits)
    print(f"(в) окружностей(правильных)={len(circles)}; с единств. осевым токеном={len(ax_hits)} диаметры={dict(diam_hist)}")
    print(f"    примеры: {[(h['tok'], h['d']) for h in ax_hits[:12]]}")

    # --- (г) выноски: полилиния с диагональю и горизонтальным последним коленом,
    #     конец полочки у текста ---
    text_lines = []
    for b in raw.get("blocks", []):
        for ln in b.get("lines", []):
            t = "".join(sp.get("text", "") for sp in ln.get("spans", []))
            if t.strip():
                bb = ln.get("bbox")
                text_lines.append({"t": t.strip()[:40], "bbox": bb})
    leader_hits = 0; samples = []
    for pl in polylines:
        pts = pl["pts"]
        if not (2 <= len(pts)-1 <= 5): continue
        # последнее колено горизонтально?
        def is_horiz(a, b): return abs(a[1]-b[1]) < 0.6 and abs(a[0]-b[0]) > 3
        def is_diag(a, b): return abs(a[1]-b[1]) > 1.5 and abs(a[0]-b[0]) > 1.5
        cand = None
        if is_horiz(pts[-1], pts[-2]) and any(is_diag(a, b) for a, b in zip(pts, pts[1:])):
            cand = (pts[-1], pts[-2], pts[0])
        elif is_horiz(pts[0], pts[1]) and any(is_diag(a, b) for a, b in zip(pts, pts[1:])):
            cand = (pts[0], pts[1], pts[-1])
        if not cand: continue
        shelf_a, shelf_b, tip = cand
        sx0, sx1 = min(shelf_a[0], shelf_b[0]), max(shelf_a[0], shelf_b[0])
        sy = shelf_a[1]
        near = [tl for tl in text_lines
                if tl["bbox"][0] < sx1+4 and tl["bbox"][2] > sx0-4
                and -12 <= sy - tl["bbox"][3] <= 6]
        if near:
            leader_hits += 1
            if len(samples) < 6:
                samples.append({"text": near[0]["t"], "tip": (round(tip[0]), round(tip[1])),
                                "shelf_y": round(sy, 1), "n_pts": len(pts)})
    print(f"(г) полилиний={len(polylines)}; выносок с полочкой+текстом={leader_hits}")
    for s in samples: print(f"    {s}")
    doc.close()
