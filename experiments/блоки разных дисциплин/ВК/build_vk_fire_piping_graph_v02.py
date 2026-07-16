#!/usr/bin/env python3
"""vk_fire_piping_graph_v0.2 — детерминированный граф схемы противопожарного водопровода (ВПВ/АПТ).

Пилот: лист 7 «Схема с системами пожаротушения (В2.2, В2.3, В21.5, В21.6)»,
комплект 13АВ-РД-ВК2-К6 (Противопожарный водопровод. АПТ. Корпус 6),
блок 4VEF-CC3P-P7K (кроп = левая половина листа: схемы В2.2/В2.3 + Узлы 1, 2).

Принципы честности:
  • каждая сущность/ребро трассируется к реальным токенам вектор-слоя (coords в evidence);
  • топология линий труб (page.get_drawings) в v0.2 НЕ разбирается — рёбра строятся только
    из геометрии подписей (X-колонки, Y→отметка регрессия) с указанием basis и confidence;
  • что не выводится детерминированно (стояк→связка по выноскам, шкаф→конкретный стояк
    внутри связки) — уходит в unresolved_groups / requires_review, а не выдумывается;
  • повторяющийся поэтажный отвод к ШПК описывается branch_template'ом с честным
    разделением evidence_floors (где подпись ⌀ есть) и assumed_floors (где предположено).

Запуск:  python3 build_vk_fire_piping_graph_v02.py
Выход:   03_13AV_RD_VK2_K6_sheet7_vk_graph.json (рядом со скриптом)
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import fitz  # PyMuPDF

HERE = Path(__file__).parent
PDF = HERE / "03_13АВ-РД-ВК2-К6_V1__4VEF-CC3P-P7K.pdf"
OUT = HERE / "03_13AV_RD_VK2_K6_sheet7_vk_graph.json"

# ── регексы доменной лексики ─────────────────────────────────────────────────
RISER_RE = re.compile(r"Ст\.\s*(В\d+\.\d+)-(\d+)\((\d+)\)\s*ø\s*(\d+)х([\d,.]+)")
SEG_RE = re.compile(r"^(В\d+\.\d+)\s*[⌀Ø∅]\s*(\d+)x\s*([\d,]+)?\s*$")
ELEV_RE = re.compile(r"^[+\-]?\d{1,2}[.,]\d{3}$")
SUPPORT_RE = re.compile(r"Неподвижная опора \"Энергия-Термо\" Ду(\d+)")
CAB_NAMED_RE = re.compile(r"^(ШПК-320-21 НЗ|ШП-К-О-Пульс-320-12НЗК)\s+(\d)\((\d)\)$")
DIGIT_RE = re.compile(r"^\d{1,4}$")


def r1(v):
    return round(float(v), 1)


def dedup_words(words):
    """CAD рисует часть текста дважды (double-draw). Схлопываем точные дубли, считаем occurrences."""
    seen = {}
    for w in words:
        key = (w[4], round(w[0]), round(w[1]))
        if key in seen:
            seen[key]["occurrences"] += 1
        else:
            seen[key] = {"x0": r1(w[0]), "y0": r1(w[1]), "x1": r1(w[2]), "y1": r1(w[3]),
                         "text": w[4], "occurrences": 1}
    return list(seen.values())


def build_lines(words):
    """Слова → строки-подписи: группировка по Y-полосе и X-стыковке (устойчивее, чем dict-lines CAD)."""
    ws = sorted(words, key=lambda w: (round(w["y0"] / 6), w["x0"]))
    lines = []
    for w in ws:
        placed = False
        for ln in lines:
            if abs(ln["y0"] - w["y0"]) < 6 and 0 <= w["x0"] - ln["x1"] < 14:
                ln["text"] += " " + w["text"]
                ln["x1"] = max(ln["x1"], w["x1"])
                ln["y1"] = max(ln["y1"], w["y1"])
                ln["words"].append(w)
                placed = True
                break
        if not placed:
            lines.append({"x0": w["x0"], "y0": w["y0"], "x1": w["x1"], "y1": w["y1"],
                          "text": w["text"], "words": [w]})
    return lines


def line_mid(ln):
    return (ln["x0"] + ln["x1"]) / 2.0, (ln["y0"] + ln["y1"]) / 2.0


def linreg(pairs):
    """Линейная регрессия y→v; возвращает (fn, quality=доля точек с |остаток|<1.0 м)."""
    if len(pairs) < 3:
        return None, 0.0
    n = len(pairs)
    sy = sum(p[0] for p in pairs); sv = sum(p[1] for p in pairs)
    syy = sum(p[0] * p[0] for p in pairs); syv = sum(p[0] * p[1] for p in pairs)
    den = n * syy - sy * sy
    if abs(den) < 1e-9:
        return None, 0.0
    k = (n * syv - sy * sv) / den
    b = (sv - k * sy) / n
    good = sum(1 for y, v in pairs if abs(k * y + b - v) < 1.0)
    return (lambda y: k * y + b), round(good / n, 3)


def main():
    doc = fitz.open(str(PDF))
    pg = doc[0]
    raw_words = pg.get_text("words")
    page_w, page_h = float(pg.rect.width), float(pg.rect.height)
    drawings = pg.get_drawings()
    dr_lines = sum(1 for d in drawings for it in d["items"] if it[0] == "l")
    dr_rects = sum(1 for d in drawings for it in d["items"] if it[0] == "re")

    words = dedup_words(raw_words)
    lines = build_lines(words)
    for i, ln in enumerate(lines):
        ln["id"] = f"LN-{i:03d}"
        ln["assigned"] = []

    def mark(ln, ent_id):
        ln["assigned"].append(ent_id)

    entities, edges, warnings, requires_review = [], [], [], []
    eid_seq = {}

    def add_entity(etype, **kw):
        n = eid_seq.get(etype, 0) + 1
        eid_seq[etype] = n
        ent = {"id": kw.pop("id", f"{etype.upper()}-{n:02d}"), "type": etype, **kw}
        entities.append(ent)
        return ent

    def add_edge(etype, frm, to, basis, confidence, **kw):
        edges.append({"id": f"E-{len(edges)+1:03d}", "type": etype, "from": frm, "to": to,
                      "basis": basis, "confidence": confidence, **kw})

    # ── зоны листа (по фактической раскладке кропа) ──────────────────────────
    def zone_of(x, y):
        if y < 30:
            return "titles"
        if y < 1300:
            if x < 655:
                return "S1"
            if x < 1100:
                return "S2"
            return "offcrop_right"
        if 1300 < y < 1460 and x >= 1000:
            return "detail_U2"
        if 1300 < y < 1460 and x >= 800:
            return "detail_U1"
        if x >= 790 and y > 1550:
            return "notes"
        return "bottom_feed"

    # особые случаи зоны примечаний (x 805..1130, y>1550)
    def zone_fix(ln):
        x, y = line_mid(ln)
        z = zone_of(x, y)
        if y > 1550 and x > 790:
            return "notes"
        return z

    # ── схемы (заголовки) ────────────────────────────────────────────────────
    schemes = {}
    for ln in lines:
        if ln["text"].startswith("Схема системы"):
            sid = "SCH-1" if "В2.2" in ln["text"] else "SCH-2"
            sc = add_entity("scheme", id=sid, title=ln["text"],
                            zone="1 зона (этажи 1–16)" if sid == "SCH-1" else "2 зона (этажи 16–28)",
                            scale="1:100",
                            bbox=[ln["x0"], ln["y0"], ln["x1"], ln["y1"]])
            schemes[sid] = sc
            mark(ln, sid)

    # ── системы ──────────────────────────────────────────────────────────────
    sys_meta = {
        "В2.2": {"kind": "внутренний противопожарный водопровод (ВПВ)", "zone": "1 зона, этажи 1–16",
                 "scheme": "SCH-1", "on_sheet": "full"},
        "В2.3": {"kind": "внутренний противопожарный водопровод (ВПВ)", "zone": "2 зона, этажи 16–28 (транзит через 1 зону)",
                 "scheme": "SCH-2", "on_sheet": "full"},
        "В21.5": {"kind": "АПТ (по составу листа); схема вне кропа", "zone": "1 зона",
                  "scheme": None, "on_sheet": "detail_only"},
        "В21.6": {"kind": "АПТ (по составу листа); схема вне кропа", "zone": "2 зона",
                  "scheme": None, "on_sheet": "detail_only"},
    }
    for code, meta in sys_meta.items():
        add_entity("system", id=f"SYS-{code}", code=code, **meta)

    # ── уровни (этаж + отметка) ──────────────────────────────────────────────
    # ПОСЛОВНО (не по строкам): подписи этажей склеиваются со стояками/шкафами в одну строку
    # («Ст. В2.3-3(6)ø89х3,5 Этаж 22 ШПК-320-21 НЗ») — строковый матч терял бы их
    def word_lines_index():
        idx = {}
        for ln in lines:
            for w in ln["words"]:
                idx[id(w)] = ln
        return idx

    w2line = word_lines_index()
    all_words_sorted = sorted(words, key=lambda w: (w["y0"], w["x0"]))

    floor_pts = []
    for ln in lines:
        ws = ln["words"]
        for i, w in enumerate(ws):
            if w["text"] != "Этаж":
                continue
            num = next((v for v in ws[i + 1:i + 3]
                        if re.fullmatch(r"\d{1,2}", v["text"]) and 0 <= v["x0"] - w["x1"] < 40), None)
            if not num:
                continue
            fl = int(num["text"])
            x = (w["x0"] + num["x1"]) / 2.0
            y = (w["y0"] + w["y1"]) / 2.0
            # отметка — слово-токен непосредственно НАД словом «Этаж» в той же X-колонке
            cand = [v for v in words if ELEV_RE.fullmatch(v["text"])
                    and abs(v["x0"] - w["x0"]) < 40 and 2 < w["y0"] - v["y0"] < 24]
            elev, elev_y = None, None
            if cand:
                c = min(cand, key=lambda v: abs(w["y0"] - v["y0"]))
                try:
                    elev = float(c["text"].replace(",", ".").replace("+", ""))
                    elev_y = (c["y0"] + c["y1"]) / 2.0
                    elev_x = (c["x0"] + c["x1"]) / 2.0
                    mark(w2line[id(c)], f"LVL-{fl:02d}")
                except ValueError:
                    elev_x = None
            else:
                elev_x = None
            floor_pts.append({"x": x, "y": y, "floor": fl, "elev": elev,
                              "elev_y": elev_y, "elev_x": elev_x,
                              "f_x0": w["x0"], "f_x1": num["x1"],
                              "e_x0": (c["x0"] if elev_x is not None else None),
                              "e_x1": (c["x1"] if elev_x is not None else None),
                              "line": ln})
            mark(ln, f"LVL-{fl:02d}")

    # сводная таблица уровней: этаж → отметка (сверяем непротиворечивость всех вхождений)
    lvl_map = {}
    for fp in floor_pts:
        if fp["elev"] is None:
            continue
        lvl_map.setdefault(fp["floor"], set()).add(fp["elev"])
    level_conflicts = {fl: sorted(v) for fl, v in lvl_map.items() if len(v) > 1}
    levels = {}
    for fl in sorted(lvl_map):
        elev = sorted(lvl_map[fl])[0]
        occ = [fp for fp in floor_pts if fp["floor"] == fl]
        levels[fl] = add_entity(
            "level", id=f"LVL-{fl:02d}", floor=fl, elevation_m=elev,
            occurrences=[{"x": r1(fp["x"]), "y": r1(fp["y"]), "zone": zone_fix(fp["line"])} for fp in occ])
    if level_conflicts:
        warnings.append(f"конфликт отметок этажей: {level_conflicts}")

    # рёбра последовательности уровней + честные аномалии шага
    fls = sorted(levels)
    for a, b in zip(fls, fls[1:]):
        if b - a == 1:
            step = round(levels[b]["elevation_m"] - levels[a]["elevation_m"], 3)
            add_edge("level_above", f"LVL-{a:02d}", f"LVL-{b:02d}",
                     basis="elevation_table", confidence=0.98, step_m=step)
    steps = [(b, round(levels[b]["elevation_m"] - levels[a]["elevation_m"], 3))
             for a, b in zip(fls, fls[1:]) if b - a == 1]
    typical = sorted(s for _, s in steps)[len(steps) // 2]
    for fl, s in steps:
        if abs(s - typical) > 0.05:
            requires_review.append({
                "kind": "нетиповой шаг отметок",
                "detail": f"этаж {fl-1}→{fl}: шаг {s} м при типовом {typical} м — сверить с АР",
                "severity": "info"})

    # ── связки стояков (bundles): колонки ШПК = геометрические оси ───────────
    # окна по фактическим медианам колонок ШПК: S1 ~306/428, S2 ~790/911
    bundles = {
        "BND-S1-A": {"scheme": "SCH-1", "x_win": (140, 370), "cab_col_x": 306},
        "BND-S1-B": {"scheme": "SCH-1", "x_win": (370, 655), "cab_col_x": 428},
        "BND-S2-A": {"scheme": "SCH-2", "x_win": (655, 850), "cab_col_x": 790},
        "BND-S2-B": {"scheme": "SCH-2", "x_win": (850, 1100), "cab_col_x": 911},
    }
    for bid, b in bundles.items():
        add_entity("riser_bundle", id=bid, scheme=b["scheme"],
                   cabinet_column_x=b["cab_col_x"],
                   note="ось связки = колонка подписей ШПК; точная геометрия линий труб не разбиралась")
        add_edge("bundle_in_scheme", bid, b["scheme"], basis="x_window", confidence=0.95)

    def bundle_for(x, y):
        if y >= 1300:
            return None
        for bid, b in bundles.items():
            if b["x_win"][0] <= x < b["x_win"][1]:
                return bid
        return None

    # ось связки ≈ колонка подписей ШПК минус ~45 pt (подписи правее оси) — ОЦЕНКА
    axis_est = {bid: b["cab_col_x"] - 45 for bid, b in bundles.items()}

    def nearest_axis(x):
        return min(axis_est, key=lambda bid: abs(axis_est[bid] - x))

    # per-bundle регрессия y→отметка (связки смещены по вертикали — общая шкала недопустима)
    bundle_reg = {}
    for bid, b in bundles.items():
        pairs = [(fp["y"], fp["elev"]) for fp in floor_pts
                 if fp["elev"] is not None and b["x_win"][0] <= fp["x"] < b["x_win"][1] and fp["y"] < 1300]
        fn, q = linreg(pairs)
        bundle_reg[bid] = {"fn": fn, "quality": q, "n_points": len(pairs)}

    def bind_floor(x, y, offset_map=None):
        """(x,y) → (bundle, floor, Δм, confidence) через per-bundle регрессию + таблицу уровней.

        offset_map: пер-связочная систематическая поправка (подписи класса сущностей стоят
        с постоянным сдвигом от линии уровня — напр. шкаф подписан выше уровня на полэтажа)."""
        bid = bundle_for(x, y)
        if not bid or not bundle_reg[bid]["fn"]:
            return None, None, None, 0.0
        elev = bundle_reg[bid]["fn"](y) - (offset_map or {}).get(bid, 0.0)
        fl = min(levels, key=lambda f: abs(levels[f]["elevation_m"] - elev))
        delta = round(abs(levels[fl]["elevation_m"] - elev), 2)
        conf = 0.93 if delta < 0.8 else (0.6 if delta < 1.6 else 0.3)
        return bid, fl, delta, conf

    # ── стояки (подписи «Ст. Вx.y-N(6)øDDхW») ────────────────────────────────
    risers = {}
    for ln in lines:
        for m in RISER_RE.finditer(ln["text"]):
            sys_code, num, korp, dia, wall = m.groups()
            rid = f"RSR-{sys_code}-{num}"
            x, y = ln["x0"], (ln["y0"] + ln["y1"]) / 2
            occ = {"x": r1(x), "y": r1(y), "zone": zone_fix(ln)}
            if rid in risers:
                risers[rid]["label_occurrences"].append(occ)
            else:
                risers[rid] = add_entity(
                    "riser", id=rid, system=sys_code, number=int(num), building=korp,
                    diameter_mm=int(dia), wall_mm=float(wall.replace(",", ".")),
                    label=f"Ст. {sys_code}-{num}({korp})ø{dia}х{wall}",
                    label_occurrences=[occ])
                add_edge("riser_of_system", rid, f"SYS-{sys_code}",
                         basis="label_parse", confidence=0.99)
            mark(ln, rid)

    # привязка стояков к связкам:
    #  В2.2: у основания S1 две ПАЧКИ подписей по 2 (x≈157 и x≈292) — по визуальному осмотру
    #  рендера выноски левой пачки ведут к связке A, правой — к связке B (сегмент SEG ⌀76
    #  riser_segment у базы B согласуется); вектор-трассировка выносок — кандидат v0.3;
    #  В2.3: подписи пачками МЕЖДУ связками, выноски веером — без трассировки линий не разрешимо.
    v23_bundle_unresolved = []
    v22 = sorted((r for r in risers.values() if r["system"] == "В2.2"),
                 key=lambda r: r["label_occurrences"][0]["x"])
    for i, r in enumerate(v22):
        bid = "BND-S1-A" if i < 2 else "BND-S1-B"
        add_edge("riser_on_bundle", r["id"], bid,
                 basis="label_pack_x_order_and_visual_leader_check", confidence=0.7,
                 note="две пачки подписей по 2 у оснований связок; принадлежность выносок проверена "
                      "визуально по рендеру, вектор-трассировка линий не выполнялась (v0.3)")
    for rid, r in risers.items():
        if r["system"] == "В2.3":
            in_s1 = [o for o in r["label_occurrences"] if o["zone"] == "S1"]
            if in_s1:
                add_edge("riser_transit_through", rid, "BND-S1-A",
                         basis="transit_label_in_zone1_leader", confidence=0.6,
                         note="подпись транзита в 1 зоне (x≈157, эт.10–11); связка по X-окну, выноска не трассировалась")
            v23_bundle_unresolved.append(rid)

    # ── пожарные шкафы ШПК (поэтажные) и именованные шкафы ───────────────────
    # двухпроходная привязка: подпись шкафа стоит с систематическим сдвигом от линии уровня
    # (обычно выше на ~полэтажа) — 1-й проход меряет медиану сдвига по связке, 2-й привязывает.
    cab_n = 0
    named_cabs = []
    pos_stray = []  # позиционные номера, прилипшие к строкам шкафов
    cab_pts = []  # (x, y, line)
    for ln in lines:
        t = ln["text"]
        x, y = line_mid(ln)
        mn = CAB_NAMED_RE.search(t)
        if mn:
            cab_n += 1
            model, num, korp = mn.groups()
            cid = f"CAB-N{num}"
            add_entity("fire_cabinet", id=cid, model=model, name=f"{num}({korp})",
                       placement="нижний уровень (зона подводок от В2.2)",
                       x=r1(x), y=r1(y), zone=zone_fix(ln))
            named_cabs.append(cid)
            mark(ln, cid)
            continue
        # пословно: слово «ШПК-320-21» + сосед «НЗ» (строка может содержать и другие сущности)
        ws = ln["words"]
        for i, w in enumerate(ws):
            if w["text"] != "ШПК-320-21" or w["y0"] >= 1300:
                continue
            nz = next((v for v in ws[i + 1:i + 2] if v["text"] == "НЗ"), None)
            if not nz:
                continue
            # прилипший позиционный номер после «НЗ» (верх связки)
            tail = next((v for v in ws[i + 2:i + 3]
                         if re.fullmatch(r"\d{1,2}", v["text"]) and int(v["text"]) < 20
                         and 0 <= v["x0"] - nz["x1"] < 20), None)
            if tail:
                pos_stray.append({"value": int(tail["text"]),
                                  "x": r1((tail["x0"] + tail["x1"]) / 2.0),
                                  "y": r1((tail["y0"] + tail["y1"]) / 2.0)})
            cab_pts.append(((w["x0"] + nz["x1"]) / 2.0, (w["y0"] + w["y1"]) / 2.0, ln))
    # проход 1: сырые предсказания отметки → медианный сдвиг подписи по каждой связке
    cab_offset = {}
    for bid, b in bundles.items():
        resid = []
        for (x, y, ln) in cab_pts:
            if bundle_for(x, y) != bid or not bundle_reg[bid]["fn"]:
                continue
            elev = bundle_reg[bid]["fn"](y)
            fl = min(levels, key=lambda f: abs(levels[f]["elevation_m"] - elev))
            resid.append(elev - levels[fl]["elevation_m"])
        if resid:
            cab_offset[bid] = sorted(resid)[len(resid) // 2]
    # проход 2: привязка с поправкой
    for (x, y, ln) in cab_pts:
        cab_n += 1
        cid = f"CAB-{cab_n:03d}"
        bid, fl, delta, conf = bind_floor(x, y, cab_offset)
        add_entity("fire_cabinet", id=cid, model="ШПК-320-21 НЗ",
                   x=r1(x), y=r1(y), zone=zone_of(x, y))
        if bid:
            add_edge("cabinet_on_bundle", cid, bid, basis="x_column", confidence=0.9)
        if fl is not None and delta < 1.6:
            add_edge("cabinet_at_level", cid, f"LVL-{fl:02d}",
                     basis="y_elevation_regression_offset_corrected", confidence=conf, delta_m=delta)
        else:
            requires_review.append({
                "kind": "шкаф вне сетки уровней", "severity": "check",
                "detail": f"{cid} ({r1(x)},{r1(y)}): подпись ШПК не легла на уровень даже с поправкой "
                          f"(Δ={delta} м) — кровля/техэтаж или сбой привязки"})
        mark(ln, cid)

    # ── неподвижные опоры ────────────────────────────────────────────────────
    sup_n = 0
    for ln in lines:
        m = SUPPORT_RE.search(ln["text"])
        if not m:
            continue
        x, y = line_mid(ln)
        sup_n += 1
        sid = f"SUP-{sup_n:02d}"
        du = int(m.group(1))
        # подписи опор идут на горизонтальных полках-выносках рядом с подписью своего уровня:
        # метрика = X-зазор от краёв строки опоры до токена уровня + |Δy| центров
        # (чистый |Δy| путает соседние этажи разных лесенок; связку сознательно не утверждаем)
        def fp_dist(fp):
            best = None
            for tx0, tx1, ty in ((fp["f_x0"], fp["f_x1"], fp["y"]),
                                 (fp.get("e_x0"), fp.get("e_x1"), fp.get("elev_y"))):
                if tx0 is None:
                    continue
                dx_gap = max(0.0, ln["x0"] - tx1, tx0 - ln["x1"])
                d = dx_gap + abs(ty - y)
                best = d if best is None else min(best, d)
            return best if best is not None else 999.0

        near_fp = min(floor_pts, key=fp_dist)
        dist = fp_dist(near_fp)
        add_entity("support", id=sid, model=f'Неподвижная опора "Энергия-Термо" Ду{du}',
                   du=du, x=r1(x), y=r1(y), zone=zone_fix(ln),
                   note="связка не определялась: длинная полка-выноска (нужна трассировка линий)")
        if dist < 32:
            add_edge("support_at_level", sid, f"LVL-{near_fp['floor']:02d}",
                     basis="adjacent_level_label_xy", confidence=0.7, dist_pt=round(dist, 1))
        elif dist < 70:
            add_edge("support_at_level", sid, f"LVL-{near_fp['floor']:02d}",
                     basis="adjacent_level_label_xy", confidence=0.5, dist_pt=round(dist, 1),
                     note="подпись опоры заметно смещена от подписи уровня — опора у верха связки "
                          "(граница зоны), уровень ориентировочный")
        # Ду опоры ↔ диаметр трубы: Ду65↔ø76х3,5 (В2.2), Ду80↔ø89х3,5 (В2.3) —
        # детерминированное доменное соответствие условного прохода наружному диаметру
        du_sys = {65: ("В2.2", 76), 80: ("В2.3", 89)}.get(du)
        if du_sys:
            add_edge("support_of_system", sid, f"SYS-{du_sys[0]}",
                     basis="du_to_pipe_diameter_match", confidence=0.7,
                     note=f"условный проход Ду{du} соответствует трубе ø{du_sys[1]}х3,5 — стояки {du_sys[0]}")
        mark(ln, sid)

    # ── сегменты труб (подписи «Вx.y ⌀DDx W») — ПОСЛОВНО ─────────────────────
    # строка может содержать несколько сущностей («Этаж 3 В2.2 ⌀57x 3,5») или
    # interleaved-дубль двух почти совпадающих подписей («В2.2 В2.2 ⌀57x ⌀57x 3,5 3,5»)
    seg_raw = []
    used_dia = set()
    for ln in lines:
        ws = sorted(ln["words"], key=lambda w: w["x0"])
        for w in ws:
            if not re.fullmatch(r"В\d+\.\d+", w["text"]):
                continue
            # ⌀ и стенка ищутся по всей Y-полосе (могли не склеиться в одну строку);
            # у заголовков схем ⌀-соседа нет → пропускаются
            row = [v for v in words if abs(v["y0"] - w["y0"]) < 6]
            dia_w = next((v for v in sorted(row, key=lambda v: v["x0"])
                          if id(v) not in used_dia
                          and re.fullmatch(r"[⌀Ø∅]\d{2,3}[xх]?", v["text"])
                          and 0 <= v["x0"] - w["x1"] < 60), None)
            if not dia_w:
                continue
            used_dia.add(id(dia_w))
            wall_w = next((v for v in sorted(row, key=lambda v: v["x0"])
                           if re.fullmatch(r"\d{1,2},\d|\d", v["text"])
                           and 0 <= v["x0"] - dia_w["x1"] < 50), None)
            # токен стенки, упирающийся в правую кромку страницы, ОБРЕЗАН кропом —
            # его значение нельзя выдавать за факт («3» = начало «3,5»)
            truncated = bool(wall_w) and wall_w["x1"] >= page_w - 1.0
            seg_raw.append({"system": w["text"],
                            "dia": int(re.sub(r"\D", "", dia_w["text"])),
                            "wall": (None if truncated else
                                     (wall_w["text"].replace(",", ".") if wall_w else None)),
                            "truncated": truncated,
                            "x": (w["x0"] + dia_w["x1"]) / 2.0,
                            "y": (w["y0"] + w["y1"]) / 2.0,
                            "line": ln})
            if wall_w is not None:
                mark(w2line[id(wall_w)], "SEG-wall")
    # дедуп interleaved-дублей: одинаковая подпись ближе 8 pt = двойная отрисовка CAD
    seg_dedup = []
    for s in sorted(seg_raw, key=lambda s: (s["system"], s["dia"], s["x"], s["y"])):
        twin = next((d for d in seg_dedup if d["system"] == s["system"] and d["dia"] == s["dia"]
                     and d["wall"] == s["wall"] and abs(d["x"] - s["x"]) < 8 and abs(d["y"] - s["y"]) < 8), None)
        if twin:
            twin["occurrences"] += 1
            mark(s["line"], twin["sid"])
            continue
        s["occurrences"] = 1
        seg_dedup.append(s)
    seg_n = 0
    for s in sorted(seg_dedup, key=lambda s: (s["y"], s["x"])):
        seg_n += 1
        sid = s["sid"] = f"SEG-{seg_n:02d}"
        sys_code, dia = s["system"], s["dia"]
        x, y = s["x"], s["y"]
        # роль: ⌀ подписи == ⌀ стояков системы → участок стояка; меньше → отвод/подводка
        riser_dias = {r["diameter_mm"] for r in risers.values() if r["system"] == sys_code}
        if riser_dias and dia in riser_dias:
            role = "riser_segment"
        elif riser_dias and dia < min(riser_dias):
            role = "branch_or_feed"
        else:
            role = "unclassified"
        z = zone_of(x, y)
        if s.get("truncated"):
            wall_disp = "…(стенка обрезана кромкой кропа)"
            requires_review.append({
                "kind": "токен обрезан кропом", "severity": "check",
                "detail": f"{sid} ({sys_code} ⌀{dia}): токен стенки упирается в правую кромку страницы — "
                          f"видимое «3» почти наверняка начало «3,5»; wall_mm не заполнен, "
                          f"значение брать с полного листа"})
        else:
            wall_disp = s["wall"].replace(".", ",") if s["wall"] else "?"
        add_entity("pipe_segment", id=sid, system=sys_code, diameter_mm=dia,
                   wall_mm=float(s["wall"]) if s["wall"] else None,
                   truncated_at_crop_edge=bool(s.get("truncated")),
                   role=role, x=r1(x), y=r1(y), zone=z,
                   label=f"{sys_code} ⌀{dia}x {wall_disp}",
                   occurrences=s["occurrences"])
        add_edge("segment_of_system", sid, f"SYS-{sys_code}", basis="label_parse", confidence=0.99)
        if z in ("S1", "S2"):
            # связка — по ближайшей оценочной оси В ПРЕДЕЛАХ своей схемы (подписи отводов
            # часто стоят по чужую сторону границы X-окон); этаж — по регрессии этой связки
            cand = [bid for bid, b in bundles.items()
                    if (b["scheme"] == "SCH-1") == (z == "S1")]
            bid = min(cand, key=lambda b: abs(axis_est[b] - x))
            axis_dist = round(abs(axis_est[bid] - x), 1)
            add_edge("segment_near_bundle", sid, bid, basis="nearest_axis_estimate",
                     confidence=0.6, axis_dist_pt=axis_dist)
            if bundle_reg[bid]["fn"]:
                elev = bundle_reg[bid]["fn"](y)
                fl = min(levels, key=lambda f: abs(levels[f]["elevation_m"] - elev))
                delta = round(abs(levels[fl]["elevation_m"] - elev), 2)
                if delta < 1.6:
                    add_edge("segment_at_level", sid, f"LVL-{fl:02d}",
                             basis="y_elevation_regression", confidence=0.7, delta_m=delta)
        mark(s["line"], sid)

    # ── ссылки-продолжения и прочие ссылки ───────────────────────────────────
    ref_n = 0
    ref_lines = {}
    for ln in lines:
        if ln["text"].startswith("Далее см. комплект"):
            ref_n += 1
            rid = f"REF-APT-{ref_n:02d}"
            x, y = line_mid(ln)
            ref_lines[rid] = ln
            add_entity("reference", id=rid, target_document="13АВ-РД-АПТ1-ПА",
                       kind="continuation", x=r1(x), y=r1(y), zone=zone_fix(ln))
            mark(ln, rid)
        elif "13АВ-РД-АПТ1-ПА" in ln["text"] and len(ln["text"]) < 20:
            # вторая строка той же ссылки
            mark(ln, "REF-APT-part")
    # «Узел креплений стояка из стали см. лист 8»
    for ln in lines:
        if ln["text"].startswith("Узел креплений"):
            add_entity("reference", id="REF-SHEET8", kind="detail_reference",
                       text="Узел креплений стояка из стали — см. лист 8",
                       x=r1(line_mid(ln)[0]), y=r1(line_mid(ln)[1]), zone=zone_fix(ln))
            add_edge("reference_unanchored", "REF-SHEET8", "SHEET",
                     basis="position_only", confidence=0.4,
                     note="выноска у оснований связок 1 зоны; конкретная точка крепления не трассировалась")
            mark(ln, "REF-SHEET8")
        elif ln["text"].startswith("см. лист"):
            mark(ln, "REF-SHEET8")

    # continuation-рёбра: связка → АПТ (ссылка у основания связки, y ниже этажа 1/16 связки)
    for rid, ln in ref_lines.items():
        x, y = line_mid(ln)
        bid = bundle_for(x, min(y, 1299))
        base_y = {"BND-S1-A": 1241, "BND-S1-B": 1308, "BND-S2-A": 1060, "BND-S2-B": 1141}
        if bid and y > base_y.get(bid, 1200) - 30 and y < base_y.get(bid, 1200) + 260:
            add_edge("bundle_continues_to", bid, rid, basis="reference_near_bundle_base",
                     confidence=0.75, note="стояки уходят вниз (насосная/источник в комплекте АПТ)")
            continue
        # нижняя зона подводок: ссылка рядом с подписью трубы ⌀57 → продолжение этой подводки
        near_seg = [e for e in entities if e["type"] == "pipe_segment"
                    and e["zone"] == "bottom_feed"
                    and abs(e["x"] - x) < 150 and abs(e["y"] - y) < 150]
        if near_seg:
            seg = min(near_seg, key=lambda e: abs(e["x"] - x) + abs(e["y"] - y))
            add_edge("segment_continues_to", seg["id"], rid, basis="label_adjacency",
                     confidence=0.6,
                     note="подпись трубы и ссылка на АПТ рядом (выноска одной подводки); линия не трассировалась")
        else:
            add_edge("reference_unanchored", rid, "SHEET",
                     basis="position_only", confidence=0.4,
                     note="точка привязки ссылки к конкретной трубе не трассировалась")

    # именованные шкафы внизу подключены подводками В2.2 ⌀57 (подписи рядом) → системная связь
    for cid in named_cabs:
        add_edge("cabinet_fed_by_system", cid, "SYS-В2.2",
                 basis="adjacent_feed_labels_B2.2_d57", confidence=0.7,
                 note="рядом подписи «В2.2 ⌀57x 3,5» и «Далее см. комплект АПТ1-ПА»; трасса не трассировалась")

    # транзит В2.3 ЧЕРЕЗ 1 зону (пунктир на чертеже — видел глазами, в векторе не разбирался)
    add_edge("transit_through_zone", "SYS-В2.3", "SCH-1",
             basis="dashed_line_visual_and_transit_label", confidence=0.6,
             note="стояки В2.3 проходят транзитом через 1 зону (подпись Ст. В2.3-2(6) в поле схемы 1 + "
                  "пунктир между схемами); подтверждено визуально, не вектор-трассировкой")

    # ── примечания ───────────────────────────────────────────────────────────
    notes = []
    for ln in lines:
        m = re.match(r"^(\d)\.(.+)", ln["text"])
        if m and zone_fix(ln) == "notes":
            notes.append({"n": int(m.group(1)), "text": m.group(1) + "." + m.group(2)})
            mark(ln, f"NOTE-{m.group(1)}")
        elif ln["text"] == "Примечания:":
            mark(ln, "NOTES-HDR")
    notes.sort(key=lambda n: n["n"])
    if notes:
        add_entity("notes_block", id="NOTES", items=[n["text"] for n in notes])

    # ── детали (Узел 1 / Узел 2) ─────────────────────────────────────────────
    detail_meta = {
        "DET-U1": {"title": "Узел 1", "x_win": (800, 1000), "system_hint": "В21.5"},
        "DET-U2": {"title": "Узел 2", "x_win": (1000, 1180), "system_hint": "В21.6"},
    }
    for did, dm in detail_meta.items():
        dim_tokens, pos_tokens, seg_ids = [], [], []
        for ln in lines:
            x, y = line_mid(ln)
            if not (1300 < y < 1560 and dm["x_win"][0] <= x < dm["x_win"][1]):
                continue
            t = ln["text"]
            if re.fullmatch(r"\d{1,4}( \d{1,4})*", t):
                # строка из одного или нескольких чисел (размеры могут слипаться: «300 155»)
                for w in ln["words"]:
                    v = int(w["text"])
                    (dim_tokens if v >= 100 else pos_tokens).append(
                        {"value": v, "x": w["x0"], "y": w["y0"]})
                mark(ln, did)
            elif t.startswith("Узел"):
                mark(ln, did)
        for e in entities:
            if e["type"] == "pipe_segment" and e["zone"] == f"detail_{did.split('-')[1]}":
                seg_ids.append(e["id"])
        add_entity("detail", id=did, title=dm["title"],
                   related_system=dm["system_hint"],
                   dimensions_mm=sorted((d["value"] for d in dim_tokens), reverse=True),
                   position_callouts=sorted(p["value"] for p in pos_tokens),
                   pipe_segments=seg_ids,
                   note="узел обвязки/крепления стояка; позиции — по спецификации вне кропа")
        add_edge("detail_of_system", did, f"SYS-{dm['system_hint']}",
                 basis="segment_labels_in_detail", confidence=0.85)

    # ── позиционные выноски у верха связок (1/3, 5/7) ────────────────────────
    # привязка выноски = ближайшая оценочная ось — ОЦЕНКА, не трассировка (поле bundle_estimate)
    pos_callouts = list(pos_stray)
    for ln in lines:
        t = ln["text"]
        x, y = line_mid(ln)
        if DIGIT_RE.fullmatch(t) and int(t) < 20 and y < 1300:
            # исключаем числа, уже приписанные уровням/др. сущностям
            if ln["assigned"]:
                continue
            pos_callouts.append({"value": int(t), "x": r1(x), "y": r1(y)})
            mark(ln, "POS-CALLOUTS")
    for p in pos_callouts:
        p["bundle_estimate"] = nearest_axis(p["x"])
    if pos_callouts:
        add_entity("position_callout_group", id="POS-CALLOUTS",
                   items=sorted(pos_callouts, key=lambda p: (p["x"], p["y"])),
                   axis_x_estimates={bid: axis_est[bid] for bid in sorted(axis_est)},
                   note="позиции у верха связок (арматура верха стояка?) — спецификация вне листа/кропа; "
                        "bundle_estimate = связка с ближайшей ОЦЕНОЧНОЙ осью из axis_x_estimates "
                        "(колонка подписей ШПК − 45 pt), не трассировка выноски и не близость к колонке подписей")

    # ── шаблон поэтажного отвода (branch template) ───────────────────────────
    cab_edges = [e for e in edges if e["type"] == "cabinet_at_level"]
    cab_by_bundle = {}
    for e in edges:
        if e["type"] == "cabinet_on_bundle":
            cab_by_bundle.setdefault(e["to"], []).append(e["from"])
    lvl_of_cab = {e["from"]: int(e["to"].split("-")[1]) for e in cab_edges}
    branch_ev = {}  # bundle → floors с подписью ⌀57-отвода
    for e in edges:
        if e["type"] == "segment_near_bundle":
            seg = next(x for x in entities if x["id"] == e["from"])
            if seg["role"] == "branch_or_feed":
                fl_e = [x for x in edges if x["type"] == "segment_at_level" and x["from"] == seg["id"]]
                if fl_e:
                    branch_ev.setdefault(e["to"], set()).add(int(fl_e[0]["to"].split("-")[1]))
    branch_templates = []
    for bid in bundles:
        cabs = cab_by_bundle.get(bid, [])
        floors_covered = sorted({lvl_of_cab[c] for c in cabs if c in lvl_of_cab})
        ev = sorted(branch_ev.get(bid, set()))
        rng = (min(floors_covered), max(floors_covered)) if floors_covered else (None, None)
        missing = sorted(set(range(rng[0], rng[1] + 1)) - set(floors_covered)) if floors_covered else []
        ev_in = [f for f in ev if f in floors_covered]
        ev_out = [f for f in ev if f not in floors_covered]
        branch_templates.append({
            "id": f"TPL-{bid}",
            "template": "поэтажный отвод: стояк → отвод ⌀57х3,5 → пожарный шкаф ШПК-320-21 НЗ",
            "bundle": bid,
            "cabinet_model": "ШПК-320-21 НЗ",
            "floors_with_cabinet": floors_covered,
            "floors_range": list(rng),
            "floors_missing_cabinet_label": missing,
            "branch_diameter_evidence_floors": ev_in,
            "feed_evidence_floors_outside_range": ev_out,
            "branch_diameter_assumed_floors": [f for f in floors_covered if f not in ev_in],
            "note": "⌀57х3,5 подписан выборочно (типовая деталь); на остальных этажах диаметр отвода — "
                    "допущение по шаблону, не факт; feed_evidence_* — подписи ⌀57 у связки вне этажей "
                    "со шкафами (подводка до первого шкафа)",
        })

    # ── unresolved groups ────────────────────────────────────────────────────
    unresolved = []
    if v23_bundle_unresolved:
        unresolved.append({
            "id": "UNR-V23-BUNDLES",
            "kind": "riser_to_bundle_mapping",
            "risers": sorted(v23_bundle_unresolved),
            "candidate_bundles": ["BND-S2-A", "BND-S2-B"],
            "reason": "подписи «Ст. В2.3-x(6)» стоят пачками между связками, выноски веером; "
                      "без трассировки линий выносок распределение 4 стояков по 2 связкам не выводимо",
        })
    unresolved.append({
        "id": "UNR-CAB-TO-RISER",
        "kind": "cabinet_to_exact_riser",
        "reason": "в каждой связке ≥2 стояка; какой из пары питает конкретный ШПК — не выводимо из подписей, "
                  "нужна трассировка линий (get_drawings) или план этажа (лист 2/4/5)",
    })
    offcrop = [ln for ln in lines if zone_fix(ln) == "offcrop_right" and not ln["assigned"]]
    for ln in offcrop:
        mark(ln, "UNR-OFFCROP")
    unresolved.append({
        "id": "UNR-OFFCROP",
        "kind": "cropped_out_content",
        "tokens": [{"text": ln["text"], "x": r1(ln["x0"]), "y": r1(ln["y0"])} for ln in offcrop],
        "reason": "схемы В21.5 (1 зона)/В21.6 (2 зона) — правее границы кропа блока; в кроп попали обрезки",
    })
    leftovers = [ln for ln in lines if not ln["assigned"]]
    unresolved.append({
        "id": "UNR-LEFTOVER-TOKENS",
        "kind": "unassigned_text",
        "tokens": [{"text": ln["text"], "x": r1(ln["x0"]), "y": r1(ln["y0"]), "zone": zone_fix(ln)}
                   for ln in leftovers],
        "reason": "строки вектор-слоя, не привязанные ни к одной сущности (полный список — для честного покрытия)",
    })

    # ── requires_review (доменные, честные) ──────────────────────────────────
    requires_review.extend([
        {"kind": "ревизия v0.1", "severity": "info",
         "detail": "в v0.1 (.graph.md) лесенки отметок были приняты за стояки R1–R5, а пропуски подписей "
                   "этажей 3/15 в шкалах — за «разрыв стояка»; в v0.2 стояки = 8 подписанных Ст.-сущностей, "
                   "флаги «разрыв стояка» и «немонотонность ⌀ [57,57,76]» сняты как ошибка интерпретации "
                   "(⌀57 — отводы к ШПК, ⌀76/⌀89 — стояки)"},
        {"kind": "транзит В2.3 в 1 зоне", "severity": "check",
         "detail": "в 1 зоне подписан только транзит Ст. В2.3-2(6); транзит В2.3-1/3/4 на схеме не подписан — "
                   "сверить с планами (листы 2, 4, 5)"},
        {"kind": "распределение В2.3 по связкам", "severity": "check",
         "detail": "какие 2 из 4 стояков В2.3 в какой связке 2-й зоны — см. UNR-V23-BUNDLES"},
        {"kind": "позиции 1/3/5/7 у верха связок", "severity": "check",
         "detail": "позиционные выноски на элементы верха стояков не расшифрованы на листе (спецификация вне кропа)"},
        {"kind": "нумерация нижних шкафов", "severity": "hypothesis",
         "detail": "шкафы 1(6)–4(6) внизу (ШПК-320-21 НЗ 2(6), ШП-К-О-Пульс-320-12НЗК 1/3/4(6)) возможно "
                   "соответствуют стоякам В2.2-1..4 — проверить по плану 1 этажа (лист 2); тип «Пульс» у 1/3/4 "
                   "отличается от поэтажных ШПК-320-21 — сверить со спецификацией"},
        {"kind": "источник/насосная", "severity": "check",
         "detail": "все стояки уходят вниз с «Далее см. комплект 13АВ-РД-АПТ1-ПА» — источник, насосы и вводы "
                   "в комплекте АПТ1-ПА (кросс-комплектная проверка)"},
        {"kind": "отметка кровли вне вектор-текста", "severity": "info",
         "detail": "Gemma OCR упоминала +90.480 — вероятная отметка кровли: согласуется с кровельными "
                   "ШПК CAB-006/CAB-010 (Δ≈+2.7 м над эт. 28 = ~+90.2); в вектор-тексте кропа такой "
                   "отметки нет, уровень «кровля» в граф не включён"},
        {"kind": "привязка В2.2 к связкам — визуальная", "severity": "check",
         "detail": "распределение В2.2-1/2 → связка A, В2.2-3/4 → связка B принято по порядку пачек "
                   "подписей и визуальному осмотру выносок рендера — подтвердить вектор-трассировкой "
                   "линий (v0.3) или планами"},
        {"kind": "опоры Ду80 наверху 1 зоны — транзитные В2.3", "severity": "info",
         "detail": "Ду65 соответствует трубе ø76х3,5 (В2.2), Ду80 — ø89х3,5 (В2.3); опоры Ду80 у "
                   "+49.350 в 1 зоне стоят на транзитных стояках В2.3, а не на В2.2 (В2.2 выше эт. 16 "
                   "не идёт) — согласуется с транзитом (см. рёбра support_of_system)"},
        {"kind": "подписи опор ≠ физические опоры", "severity": "info",
         "detail": "4 подписи Ду80 у отметки +49.350 (верх схемы 1 зоны и низ схемы 2 зоны) могут описывать "
                   "одни и те же физические опоры на границе зон — счётчик считает ПОДПИСИ, не изделия"},
        {"kind": "этажи 1–2 без поэтажных ШПК", "severity": "check",
         "detail": "поэтажные ШПК-320-21 НЗ на связках 1 зоны начинаются с этажа 3; внизу листа 4 именованных "
                   "шкафа (ШП-К-О-Пульс-320-12НЗК 1/3/4(6), ШПК-320-21 НЗ 2(6)) — вероятно покрытие этажей 1–2; "
                   "сверить с планом 1 этажа (лист 2)"},
    ])

    # ── счётчики покрытия ────────────────────────────────────────────────────
    assigned_lines = [ln for ln in lines if ln["assigned"]]
    coverage = {
        "raw_words": len(raw_words),
        "words_after_dedup": len(words),
        "dedup_removed_double_draw": len(raw_words) - len(words),
        "label_lines_total": len(lines),
        "label_lines_assigned": len(assigned_lines),
        "label_lines_unassigned": len(leftovers),
        "entities_total": len(entities),
        "edges_total": len(edges),
        "by_entity_type": {},
        "cabinets_floor_bound": sum(1 for e in edges if e["type"] == "cabinet_at_level"),
        "levels_detected": f"{len(levels)}/28 (этажи {min(levels)}–{max(levels)})",
        "vector_drawing_primitives_not_parsed": {"paths": len(drawings), "line_items": dr_lines,
                                                 "rect_items": dr_rects,
                                                 "note": "трассировка линий труб — кандидат на v0.3"},
    }
    for e in entities:
        coverage["by_entity_type"][e["type"]] = coverage["by_entity_type"].get(e["type"], 0) + 1

    # ── sheet_summary (обзорный слой, перенесён и исправлен из v0.1) ─────────
    n_cabs_floor = sum(1 for e in entities if e["type"] == "fire_cabinet" and e.get("model") == "ШПК-320-21 НЗ" and not e.get("name"))
    sheet_summary = {
        "sheet_purpose": "развёртки стояков внутреннего противопожарного водопровода корпуса 6: "
                         "В2.2 (1 зона, этажи 1–16) и В2.3 (2 зона, этажи 16–28, транзит через 1 зону); "
                         "справа за кропом — схемы АПТ В21.5/В21.6, от них в кроп попали Узлы 1, 2",
        "schemes": [{"id": s, "title": schemes[s]["title"]} for s in sorted(schemes)],
        "systems": sorted(sys_meta),
        "risers": sorted(risers),
        "risers_total": len(risers),
        "floors": {"range": [min(levels), max(levels)],
                   "elevations_m": [levels[min(levels)]["elevation_m"], levels[max(levels)]["elevation_m"]],
                   "typical_step_m": typical},
        "fire_cabinets": {"floor_mounted_ШПК-320-21_НЗ": n_cabs_floor,
                          "named_ground_level": sorted(f'{e["model"]} {e["name"]}' for e in entities
                                                       if e["type"] == "fire_cabinet" and e.get("name"))},
        "supports": {"count": sup_n, "models": sorted({e["model"] for e in entities if e["type"] == "support"})},
        "continuation": "вниз — комплект 13АВ-РД-АПТ1-ПА (7 ссылок); узлы крепления — лист 8",
        "notes_on_sheet": [n["text"] for n in notes],
    }

    # ── итоговый документ ────────────────────────────────────────────────────
    out = {
        "schema_version": "vk_fire_piping_graph_v0.2",
        "source_file": PDF.name,
        "drawing": {
            "complect": "13АВ-РД-ВК2-К6",
            "complect_title": "Противопожарный водопровод. АПТ. Корпус 6",
            "stage_version": "V1",
            "sheet_no": 7,
            "sheet_name": "Схема с системами пожаротушения (В2.2, В2.3, В21.5, В21.6)",
            "source_document_pdf": "13АВ-РД-ВК2-К6 V1.pdf",
            "source_document_page_index": 12,
            "block_id": "4VEF-CC3P-P7K",
            "metadata_origin": "sheet_no/sheet_name/complect_title/page_index — из штампа ПОЛНОГО документа "
                               "(13АВ-РД-ВК2-К6 V1_document.md, лист 7); штамп вне кропа блока и токенами "
                               "кроп-PDF не подтверждается",
            "crop_note": "кроп блока покрывает левую часть листа: схемы В2.2/В2.3, Узлы 1–2, примечания; "
                         "схемы В21.5/В21.6 — вне кропа (см. UNR-OFFCROP)",
            "scale": "1:100",
            "building": "корпус 6",
        },
        "extraction_diagnostics": {
            "method": "deterministic_vector_text_geometry_v0.2",
            "page_rect_pt": [round(page_w, 1), round(page_h, 1)],
            "elevation_regression_per_bundle": {
                bid: {"quality": bundle_reg[bid]["quality"], "n_points": bundle_reg[bid]["n_points"]}
                for bid in bundles},
            "cabinet_label_offset_correction_m": {bid: round(v, 3) for bid, v in cab_offset.items()},
            "level_elevation_conflicts": level_conflicts or None,
            "geometry_sources_used": ["get_text('words') + дедуп double-draw", "склейка строк по Y/X",
                                      "per-bundle линрегрессия y→отметка", "X-окна связок/зон"],
            "geometry_sources_NOT_used": ["page.get_drawings() трассировка труб/выносок (v0.3)",
                                          "OCR/LLM — не применялись, только вектор-текст"],
            "v01_revision_note": "интерпретация v0.1 (лесенки=стояки) пересмотрена, см. requires_review",
        },
        "sheet_summary": sheet_summary,
        "graph": {
            "entities": entities,
            "edges": edges,
            "branch_templates": branch_templates,
            "unresolved_groups": unresolved,
        },
        "coverage_counters": coverage,
        "requires_review": requires_review,
        "warnings": warnings,
        "status": "needs_review",
        "confidence": {
            "levels_and_elevations": 0.97,
            "riser_identity_and_attrs": 0.95,
            "cabinet_floor_binding": 0.9,
            "riser_to_bundle_B2.2": 0.7,
            "riser_to_bundle_B2.3": 0.0,
            "cabinet_to_exact_riser": 0.0,
            "pipe_topology_edges": 0.0,
            "overall": 0.75,
            "note": "0.0 = сознательно не извлекалось (нужна трассировка линий) — см. unresolved_groups; "
                    "для В2.3 кандидаты сужены до 2 связок (UNR-V23-BUNDLES), но распределение не извлечено; "
                    "0.7 у В2.2 — визуальная проверка выносок, не вектор-трассировка",
        },
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"OK → {OUT.name}")
    print(f"entities={len(entities)} edges={len(edges)} lines_unassigned={len(leftovers)}")
    for lv in leftovers[:25]:
        print("  UNASSIGNED:", lv["text"][:70], round(lv["x0"]), round(lv["y0"]))


if __name__ == "__main__":
    main()
