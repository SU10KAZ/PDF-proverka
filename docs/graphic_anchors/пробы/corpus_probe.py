#!/usr/bin/env python3
"""Полнокорпусный эмпирический зонд сигнатур graphic_primitives.

Прогоняет ВСЕ одностраничные PDF-вырезки галереи `experiments/блоки разных дисциплин/`
и измеряет для каждого блока сигнатуры, заложенные в дизайн
docs/graphic_anchors/ДИЗАЙН_обобщение_Вектографа.md §3:

- базовое: rotation, текст-слой, состав drawings (l/re/qu/c, stroke/fill), dashes,
  гистограмма width, доля drawings за CropBox, производительность;
- засечки размерных линий (45°, 1.0-7.0 pt) + валидация «серединой на линии»;
- привязка голых чисел к размеру (режимы break / near-line / tick-pair) + масштаб
  value/gap с долей инлайеров;
- полочки под текстом + диагональные лидеры (выноски);
- кружки (full-arc из c-items, спаренные полу-дуги, концентрические пары-болты)
  + «ровно один токен внутри»;
- отметки уровня (полочка + галочка);
- стрелки уклонов (fill-треугольник у текста 0,0N);
- сетки таблиц (полосы ≥4 длинных H одного спана, исключая рамку листа);
- глиф-слова на блоках без текст-слоя;
- rotation-самопроверка: на повёрнутых страницах привязка считается в raw- и
  derot-пространствах слов, выбирается лучшее (фиксируется какое).

Выход: JSONL по блоку (corpus_results.jsonl). Fail-soft: любой детектор в try/except,
жёсткий бюджет времени на блок. Запуск:
    python3 docs/graphic_anchors/пробы/corpus_probe.py [--workers 6] [--limit N]
"""
import argparse
import json
import math
import os
import re
import signal
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CORPUS = ROOT / "experiments" / "блоки разных дисциплин"
OUT = Path(__file__).resolve().parent / "corpus_results.jsonl"
DISCIPLINES = ["АР", "ВК", "ГП", "КЖ", "КМ", "ОВ", "СС", "ТХ", "ЭОМ"]

SEG_CAP = 300_000          # сегментов после флэттена (при превышении — детерм. прореживание)
LINES_CAP = 900            # текст-строк на полочный анализ
BLOCK_TIMEOUT = 100        # с на блок

RE_BARE_NUM = re.compile(r"^\d{2,5}\*?\)?$")
RE_ELEV = re.compile(r"^[+\-]?\d{1,2}[.,]\d{3}\*?$")
RE_SLOPE = re.compile(r"^0[.,]0\d{1,2}$")


def _angle(x0, y0, x1, y1):
    a = math.degrees(math.atan2(y1 - y0, x1 - x0)) % 180.0
    return a


def _seg_len(s):
    return math.hypot(s[2] - s[0], s[3] - s[1])


def _pt_seg_dist(px, py, s):
    x0, y0, x1, y1 = s[0], s[1], s[2], s[3]
    dx, dy = x1 - x0, y1 - y0
    L2 = dx * dx + dy * dy
    if L2 <= 1e-9:
        return math.hypot(px - x0, py - y0)
    t = max(0.0, min(1.0, ((px - x0) * dx + (py - y0) * dy) / L2))
    return math.hypot(px - (x0 + t * dx), py - (y0 + t * dy))


class Grid:
    """Простейший grid-hash по bbox сегментов."""

    def __init__(self, cell=24.0):
        self.cell = cell
        self.d = {}

    def add(self, idx, s):
        c = self.cell
        for gx in range(int(min(s[0], s[2]) // c), int(max(s[0], s[2]) // c) + 1):
            for gy in range(int(min(s[1], s[3]) // c), int(max(s[1], s[3]) // c) + 1):
                self.d.setdefault((gx, gy), []).append(idx)

    def near(self, x, y, r=1):
        c = self.cell
        gx, gy = int(x // c), int(y // c)
        out = []
        for dx in range(-r, r + 1):
            for dy in range(-r, r + 1):
                out.extend(self.d.get((gx + dx, gy + dy), ()))
        return out


def _overlap(a0, a1, b0, b1):
    return min(a1, b1) - max(a0, b0)


def probe_block(pdf_path: str) -> dict:
    import fitz  # noqa: локальный импорт для воркеров

    res = {"pdf": os.path.basename(pdf_path)}
    m = re.search(r"([0-9A-Z]{4}-[0-9A-Z]{4}-[0-9A-Z]{3})\.pdf$", pdf_path)
    res["block_id"] = m.group(1) if m else None
    res["discipline"] = Path(pdf_path).parent.name
    t0 = time.time()
    doc = fitz.open(pdf_path)
    pg = doc[0]
    res["page_count"] = doc.page_count
    res["rotation"] = pg.rotation
    rect = pg.rect
    res["page_w"], res["page_h"] = round(rect.width, 1), round(rect.height, 1)

    # ---- текст ----
    raw_text = pg.get_text()
    res["text_chars"] = len(raw_text.strip())
    td = pg.get_text("dict")
    lines = []  # (text, x0, y0, x1, y1, dirx, diry)
    for blk in td.get("blocks", []):
        for ln in blk.get("lines", []):
            txt = "".join(sp.get("text", "") for sp in ln.get("spans", [])).strip()
            if not txt:
                continue
            bb = ln["bbox"]
            d = ln.get("dir", (1, 0))
            lines.append((txt, bb[0], bb[1], bb[2], bb[3], round(d[0], 3), round(d[1], 3)))
    res["text_lines"] = len(lines)
    res["rotated_lines"] = sum(1 for l in lines if abs(l[5] - 1) > 0.05 or abs(l[6]) > 0.05)
    res["time_text"] = round(time.time() - t0, 2)

    # ---- drawings ----
    t1 = time.time()
    drawings = pg.get_drawings()
    res["n_drawings"] = len(drawings)
    res["time_drawings"] = round(time.time() - t1, 2)
    if len(drawings) > 80_000:  # защита от OOM на hatch-монстрах (ГП: >120К путей)
        stride = (len(drawings) + 79_999) // 80_000
        drawings = drawings[::stride]
        res["drawings_sampled_stride"] = stride

    item_c = Counter()
    width_c = Counter()
    dashes_nontrivial = 0
    off_crop = 0
    segs = []            # (x0,y0,x1,y1,width) — stroke: l + рёбра re/qu
    small_fills = []     # (kinds, x0,y0,x1,y1)
    circle_cands = []    # (cx, cy, dia, n_items, filled)
    half_cands = []      # (cx, cy, w, h, y0, y1)
    slack = 0.5
    for dr in drawings:
        r = dr["rect"]
        if (r.x1 < rect.x0 - slack or r.x0 > rect.x1 + slack
                or r.y1 < rect.y0 - slack or r.y0 > rect.y1 + slack):
            off_crop += 1
            continue
        items = dr["items"]
        kinds = "".join(it[0] for it in items)
        fill = dr.get("fill") is not None
        stroke_w = dr.get("width") or 0.0
        dsh = dr.get("dashes")
        if dsh and dsh not in ("[] 0", "", None):
            dashes_nontrivial += 1
        for it in items:
            item_c[it[0]] += 1
        w, h = r.width, r.height
        # кружки: all-c 2-6 items, почти квадрат
        n_c = kinds.count("c")
        if n_c == len(items) and 2 <= len(items) <= 6 and 3.5 <= w <= 60 and 3.5 <= h <= 60:
            asp = w / h if h else 99
            if 0.72 <= asp <= 1.38:
                circle_cands.append((r.x0 + w / 2, r.y0 + h / 2, (w + h) / 2, len(items), fill))
            elif 1.5 <= asp <= 2.6 and kinds == "cc":
                half_cands.append((r.x0 + w / 2, r.y0 + h / 2, w, h, r.y0, r.y1))
        if fill:
            if w * h < 60 and max(w, h) < 30:
                small_fills.append((kinds, r.x0, r.y0, r.x1, r.y1))
            continue
        width_c[round(stroke_w, 2)] += 1
        if len(segs) < SEG_CAP * 2:
            for it in items:
                if it[0] == "l":
                    p1, p2 = it[1], it[2]
                    segs.append((p1.x, p1.y, p2.x, p2.y, stroke_w))
                elif it[0] in ("re", "qu"):
                    rr = it[1]
                    if it[0] == "re":
                        pts = [(rr.x0, rr.y0), (rr.x1, rr.y0), (rr.x1, rr.y1), (rr.x0, rr.y1)]
                    else:
                        pts = [(q.x, q.y) for q in (rr.ul, rr.ur, rr.lr, rr.ll)]
                    for i in range(4):
                        a, b = pts[i], pts[(i + 1) % 4]
                        segs.append((a[0], a[1], b[0], b[1], stroke_w))
    res["items"] = dict(item_c)
    res["dashes_nontrivial"] = dashes_nontrivial
    res["off_crop_drawings"] = off_crop
    res["off_crop_share"] = round(off_crop / max(1, len(drawings)), 3)
    res["widths_top"] = width_c.most_common(6)
    res["segs_total"] = len(segs)
    res["capped"] = len(segs) > SEG_CAP
    if res["capped"]:
        step = len(segs) / SEG_CAP
        segs = [segs[int(i * step)] for i in range(SEG_CAP)]

    # классификация мелких заливок
    sf = Counter()
    arrow_fills = []
    for kinds, x0, y0, x1, y1 in small_fills:
        w, h = x1 - x0, y1 - y0
        nl, nc = kinds.count("l"), kinds.count("c")
        if nc >= 2 and nl == 0 and 1.4 <= max(w, h) <= 4.0 and 0.6 <= min(w, h) / max(w, h):
            sf["junction_dot"] += 1
        elif nl >= 2 and nc == 0 and 3.0 <= max(w, h) <= 26 and 1.5 <= min(w, h) <= 8:
            sf["arrow_triangle"] += 1
            arrow_fills.append(((x0 + x1) / 2, (y0 + y1) / 2))
        elif kinds == "re" and 4.0 <= w <= 7.5 and 4.0 <= h <= 7.5:
            sf["square_marker"] += 1
        elif min(w, h) <= 0.9 and 4 <= max(w, h) <= 30:
            sf["bar"] += 1
        else:
            sf["blob"] += 1
    res["small_fills"] = dict(sf)

    # ---- классы сегментов ----
    hsegs, vsegs, diags = [], [], []
    for s in segs:
        L = _seg_len(s)
        if L < 0.6:
            continue
        a = _angle(*s[:4])
        if a < 8 or a > 172:
            hsegs.append(s)
        elif 82 < a < 98:
            vsegs.append(s)
        elif 25 <= a <= 65 or 115 <= a <= 155:
            diags.append(s)
    res["hsegs"], res["vsegs"], res["diags"] = len(hsegs), len(vsegs), len(diags)

    long_hv = [s for s in hsegs + vsegs if _seg_len(s) >= 12]
    gridx = Grid(24.0)
    for i, s in enumerate(long_hv):
        gridx.add(i, s)

    # ---- засечки ----
    tick_lens = Counter()
    ticks = []
    for s in diags:
        L = _seg_len(s)
        if 1.0 <= L <= 7.0 and (s[4] <= 0.45 or s[4] == 0.0):
            mx, my = (s[0] + s[2]) / 2, (s[1] + s[3]) / 2
            on_line = False
            for j in gridx.near(mx, my):
                if _pt_seg_dist(mx, my, long_hv[j]) <= 1.6:
                    on_line = True
                    break
            if on_line:
                ticks.append((mx, my))
                tick_lens[round(L * 2) / 2] += 1
    res["ticks_raw45"] = sum(1 for s in diags if 1.0 <= _seg_len(s) <= 7.0)
    res["ticks_on_line"] = len(ticks)
    res["tick_len_top"] = tick_lens.most_common(4)

    # ---- слова-числа (2 пространства при rotation) ----
    def word_sets():
        base = [(t, x0, y0, x1, y1, dx, dy) for (t, x0, y0, x1, y1, dx, dy) in lines]
        yield "raw", base
        if pg.rotation:
            M = pg.derotation_matrix
            der = []
            for (t, x0, y0, x1, y1, dx, dy) in lines:
                p0 = fitz.Point(x0, y0) * M
                p1 = fitz.Point(x1, y1) * M
                der.append((t, min(p0.x, p1.x), min(p0.y, p1.y),
                            max(p0.x, p1.x), max(p0.y, p1.y), dx, dy))
            yield "derot", der

    tick_grid = Grid(30.0)
    for i, (mx, my) in enumerate(ticks):
        tick_grid.add(i, (mx, my, mx, my))
    thin_h = [s for s in hsegs if s[4] <= 0.45]
    thin_v = [s for s in vsegs if s[4] <= 0.45]
    gh = Grid(30.0)
    for i, s in enumerate(thin_h):
        gh.add(i, s)
    gv = Grid(30.0)
    for i, s in enumerate(thin_v):
        gv.add(i, s)

    def link_numbers(words):
        bare = [w for w in words if RE_BARE_NUM.match(w[0])]
        linked = Counter()
        ratios = []
        for (t, x0, y0, x1, y1, dx, dy) in bare:
            cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
            val = float(t.rstrip("*)"))
            got = None
            # (a) break: коллинеарные H-сегменты слева и справа на уровне середины
            left = right = None
            for j in gh.near(cx, cy):
                s = thin_h[j]
                if abs((s[1] + s[3]) / 2 - cy) > 3.2:
                    continue
                sx0, sx1 = min(s[0], s[2]), max(s[0], s[2])
                if -12 <= x0 - sx1 <= 12 and sx0 < x0:
                    left = s if left is None or sx1 > max(left[0], left[2]) else left
                if -12 <= sx0 - x1 <= 12 and sx1 > x1:
                    right = s if right is None or min(s[0], s[2]) < min(right[0], right[2]) else right
            if left is not None and right is not None:
                got = "break"
            # (b) near-line: тонкая линия сразу под/над с X-перекрытием
            if got is None:
                for j in gh.near(cx, y1 + 3):
                    s = thin_h[j]
                    sy = (s[1] + s[3]) / 2
                    if 0.2 <= sy - y1 <= 7.0 or 0.2 <= y0 - sy <= 7.0:
                        if _overlap(x0, x1, min(s[0], s[2]), max(s[0], s[2])) >= 0.3 * (x1 - x0):
                            got = "near"
                            break
            if got is None and (abs(dx - 1) > 0.05 or abs(dy) > 0.05):
                for j in gv.near(cx, cy):
                    s = thin_v[j]
                    sx = (s[0] + s[2]) / 2
                    if 0.2 <= abs(sx - cx) <= 7.0 + (x1 - x0):
                        if _overlap(y0, y1, min(s[1], s[3]), max(s[1], s[3])) >= 0.3 * (y1 - y0):
                            got = "near"
                            break
            # (c) tick-pair: пара засечек по обе стороны на одной оси
            if got is None and ticks:
                lt = rt = None
                for j in tick_grid.near(cx, cy) + tick_grid.near(cx - 25, cy) + tick_grid.near(cx + 25, cy):
                    tx, ty = ticks[j]
                    if abs(ty - cy) <= 14:
                        if tx < cx and (lt is None or tx > lt):
                            lt = tx
                        if tx > cx and (rt is None or tx < rt):
                            rt = tx
                if lt is not None and rt is not None and 3 <= rt - lt <= 400:
                    got = "tickpair"
                    if val >= 10:
                        ratios.append(val / (rt - lt))
            if got == "break" and val >= 10:
                gap = min(right[0], right[2]) - max(left[0], left[2])
                if gap > 2:
                    ratios.append(val / gap)
            if got:
                linked[got] += 1
        return len(bare), linked, ratios

    best = None
    for mode, words in word_sets():
        n_bare, linked, ratios = link_numbers(words)
        tot = sum(linked.values())
        if best is None or tot > best[2]:
            best = (mode, n_bare, tot, dict(linked), ratios, words)
    res["space_mode"], res["bare_nums"], res["nums_linked"], res["link_modes"], ratios, words_use = best
    res["link_rate"] = round(res["nums_linked"] / res["bare_nums"], 3) if res["bare_nums"] else None
    if len(ratios) >= 4:
        rs = sorted(ratios)
        med = rs[len(rs) // 2]
        inl = sum(1 for r in ratios if abs(r - med) <= 0.07 * med)
        res["scale_med"] = round(med, 3)
        res["scale_inlier_share"] = round(inl / len(ratios), 3)
        res["scale_n"] = len(ratios)

    # ---- полочки/лидеры ----
    shelf_n = leader_n = 0
    diag_long = [s for s in diags if _seg_len(s) >= 5]
    gd = Grid(30.0)
    for i, s in enumerate(diag_long):
        gd.add(i, s)
    for (t, x0, y0, x1, y1, dx, dy) in words_use[:LINES_CAP]:
        if len(t) < 2:
            continue
        found_shelf = None
        for j in gh.near((x0 + x1) / 2, y1 + 2):
            s = thin_h[j]
            sy = (s[1] + s[3]) / 2
            if -0.5 <= sy - y1 <= 4.5:
                if _overlap(x0, x1, min(s[0], s[2]), max(s[0], s[2])) >= 0.5 * min(x1 - x0, abs(s[2] - s[0])):
                    found_shelf = s
                    break
        if found_shelf is None:
            continue
        shelf_n += 1
        for endx, endy in ((min(found_shelf[0], found_shelf[2]), (found_shelf[1] + found_shelf[3]) / 2),
                           (max(found_shelf[0], found_shelf[2]), (found_shelf[1] + found_shelf[3]) / 2)):
            hit = False
            for j in gd.near(endx, endy):
                s = diag_long[j]
                for px, py in ((s[0], s[1]), (s[2], s[3])):
                    if math.hypot(px - endx, py - endy) <= 2.2:
                        hit = True
                        break
                if hit:
                    break
            if hit:
                leader_n += 1
                break
    res["shelf_texts"] = shelf_n
    res["leader_shelf_texts"] = leader_n

    # ---- отметки уровня ----
    elev = [w for w in words_use if RE_ELEV.match(w[0])]
    res["elev_nums"] = len(elev)
    lvl_shelf = lvl_flag = 0
    for (t, x0, y0, x1, y1, dx, dy) in elev[:300]:
        cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
        for j in gh.near(cx, y1 + 3):
            s = thin_h[j]
            sy = (s[1] + s[3]) / 2
            if -1 <= sy - y1 <= 6 and _overlap(x0, x1, min(s[0], s[2]), max(s[0], s[2])) > 0:
                lvl_shelf += 1
                break
        nd = 0
        for j in gd.near(cx, cy):
            s = diag_long[j]
            if 3.5 <= _seg_len(s) <= 9.5:
                mx, my = (s[0] + s[2]) / 2, (s[1] + s[3]) / 2
                if abs(mx - cx) <= 16 + (x1 - x0) / 2 and abs(my - cy) <= 14:
                    nd += 1
        if nd >= 2:
            lvl_flag += 1
    res["elev_with_shelf"] = lvl_shelf
    res["elev_with_flag"] = lvl_flag

    # ---- стрелки уклона ----
    slopes = [w for w in words_use if RE_SLOPE.match(w[0])]
    res["slope_nums"] = len(slopes)
    sa = 0
    for (t, x0, y0, x1, y1, dx, dy) in slopes[:200]:
        cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
        if any(math.hypot(ax - cx, ay - cy) <= 9 + (x1 - x0) / 2 for ax, ay in arrow_fills):
            sa += 1
    res["slope_with_arrow"] = sa

    # ---- кружки ----
    res["circles_full"] = len(circle_cands)
    # спаривание полу-дуг
    half_pairs = 0
    half_cands.sort(key=lambda h: (round(h[0], 0), h[4]))
    used = set()
    for i in range(len(half_cands)):
        if i in used:
            continue
        for j in range(i + 1, min(i + 6, len(half_cands))):
            if j in used:
                continue
            a, b = half_cands[i], half_cands[j]
            if abs(a[0] - b[0]) <= 0.6 and (abs(a[5] - b[4]) <= 0.6 or abs(b[5] - a[4]) <= 0.6):
                half_pairs += 1
                used.add(i)
                used.add(j)
                break
    res["circles_half_pairs"] = half_pairs
    dia_c = Counter(round(c[2]) for c in circle_cands)
    res["circle_dia_top"] = dia_c.most_common(6)
    # токен внутри
    with_tok = 0
    for (cx, cy, dia, ni, fl) in circle_cands[:800]:
        r = dia / 2
        inside = [w for w in words_use
                  if cx - r <= (w[1] + w[3]) / 2 <= cx + r and cy - r <= (w[2] + w[4]) / 2 <= cy + r]
        if len(inside) == 1 and len(inside[0][0]) <= 6:
            with_tok += 1
    res["circles_with_token"] = with_tok
    # концентрические пары (болты)
    conc = 0
    cc = sorted(circle_cands, key=lambda c: (round(c[0], 0), round(c[1], 0)))
    for i in range(len(cc) - 1):
        a, b = cc[i], cc[i + 1]
        if abs(a[0] - b[0]) <= 1.0 and abs(a[1] - b[1]) <= 1.0 and 0.5 <= abs(a[2] - b[2]) <= 3.5:
            conc += 1
    res["circles_concentric_pairs"] = conc

    # ---- сетки таблиц ----
    longh = [s for s in hsegs if _seg_len(s) >= 60]
    groups = {}
    for s in longh:
        key = (round(min(s[0], s[2]) / 6), round(max(s[0], s[2]) / 6))
        groups.setdefault(key, []).append(s)
    regions = []
    for key, gs in groups.items():
        if len(gs) < 4:
            continue
        span = (key[1] - key[0]) * 6
        if span >= 0.95 * rect.width:
            continue  # рамка листа
        ys = sorted((s[1] + s[3]) / 2 for s in gs)
        x0g, x1g = key[0] * 6, key[1] * 6
        vcross = sum(1 for s in vsegs if x0g - 3 <= (s[0] + s[2]) / 2 <= x1g + 3
                     and _overlap(min(s[1], s[3]), max(s[1], s[3]), ys[0], ys[-1]) > 0.3 * (ys[-1] - ys[0] + 1))
        win = sum(1 for w in words_use if x0g <= (w[1] + w[3]) / 2 <= x1g and ys[0] <= (w[2] + w[4]) / 2 <= ys[-1])
        regions.append({"rows": len(gs), "v": vcross, "words": win, "span": round(span)})
    regions.sort(key=lambda r: -(r["rows"] * max(1, r["v"])))
    res["table_regions"] = len(regions)
    res["table_best"] = regions[0] if regions else None

    # ---- глиф-слова (только на «пустом» тексте) ----
    if res["text_chars"] < 40:
        glyphs = [(x0, y0, x1, y1) for (kinds, x0, y0, x1, y1) in small_fills
                  if "c" in kinds and 2.5 <= (y1 - y0) <= 16 and (x1 - x0) <= 25]
        res["glyphs"] = len(glyphs)
        if glyphs:
            hs = sorted(g[3] - g[1] for g in glyphs)
            mh = hs[len(hs) // 2]
            glyphs.sort(key=lambda g: (round(g[1] / (mh * 1.2)), g[0]))
            boxes = 0
            px1 = py = None
            for (x0, y0, x1, y1) in glyphs:
                if px1 is None or x0 - px1 > 1.6 * mh or abs(y0 - py) > 0.7 * mh:
                    boxes += 1
                px1, py = x1, y0
            res["glyph_word_boxes"] = boxes

    res["time_total"] = round(time.time() - t0, 2)
    doc.close()
    return res


def _worker(pdf_path: str) -> dict:
    def _to(signum, frame):
        raise TimeoutError("block timeout")

    signal.signal(signal.SIGALRM, _to)
    signal.alarm(BLOCK_TIMEOUT)
    try:
        r = probe_block(pdf_path)
        r["ok"] = True
        return r
    except TimeoutError:
        return {"pdf": os.path.basename(pdf_path), "ok": False, "error": "timeout",
                "discipline": Path(pdf_path).parent.name}
    except Exception as e:  # noqa
        return {"pdf": os.path.basename(pdf_path), "ok": False, "error": f"{type(e).__name__}: {e}",
                "discipline": Path(pdf_path).parent.name}
    finally:
        signal.alarm(0)


def _run_chunk(files, workers, fh, progress):
    """Чанк через пул; при гибели воркера (OOM/kill -9) — пофайловый фолбэк."""
    from concurrent.futures.process import BrokenProcessPool

    try:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_worker, p): p for p in files}
            for fut in as_completed(futs):
                fh.write(json.dumps(fut.result(), ensure_ascii=False) + "\n")
                progress()
    except BrokenProcessPool:
        fh.flush()
        print("  пул погиб (OOM?) — чанк пофайлово…", flush=True)
        for p in files:
            try:
                with ProcessPoolExecutor(max_workers=1) as ex1:
                    r = ex1.submit(_worker, p).result()
            except BrokenProcessPool:
                r = {"pdf": os.path.basename(p), "ok": False, "error": "worker killed (OOM)",
                     "discipline": Path(p).parent.name}
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            progress()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--chunk", type=int, default=48)
    args = ap.parse_args()

    pdfs = []
    for d in DISCIPLINES:
        pdfs.extend(sorted(str(p) for p in (CORPUS / d).glob("*.pdf")))
    if args.limit:
        pdfs = pdfs[: args.limit]
    print(f"Блоков: {len(pdfs)}; воркеров: {args.workers}; выход: {OUT}", flush=True)

    state = {"done": 0}
    t0 = time.time()

    with open(OUT, "w", encoding="utf-8") as fh:
        def progress():
            state["done"] += 1
            if state["done"] % 50 == 0 or state["done"] == len(pdfs):
                fh.flush()
                print(f"{state['done']}/{len(pdfs)}  ({time.time()-t0:.0f}с)", flush=True)

        for i in range(0, len(pdfs), args.chunk):
            _run_chunk(pdfs[i:i + args.chunk], args.workers, fh, progress)

    errs = 0
    with open(OUT, encoding="utf-8") as fh:
        for line in fh:
            if '"ok": false' in line:
                errs += 1
    print(f"Готово за {time.time()-t0:.0f}с; ошибок/таймаутов: {errs}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
