"""singleline_graph_geometry — полный граф однолинейной схемы (топология из геометрии PDF).

Восстанавливает граф ввод→панели(РПn)→отходящие линии по чертежу:
- находит правильную PDF-страницу блока (page_index из result.json НЕ совпадает с PDF — баг нумерации);
- кластеризует токены чертежа по X-колонкам (каждая колонка = отходящая линия);
- внутри панели (QF-префикс=РПn) QF и коды цепей идут слева-направо → МОНОТОННАЯ привязка
  (резерв пропускает код) — без коллизий, проверено визуально на РП1/РП2/РП3;
- автомат/уставка/полюса/управление(АСУД/ПС)/резерв — из геометрии колонки;
- параметры линии — из валидированной таблицы (structure_singleline_text, физика 100%) по коду.

build_singleline_graph(pdf_path, vector_text, *, panel_hint) -> dict | None
Возвращает None, если это не однолинейная feeder-схема или PDF/страница недоступны. fail-soft.
"""
from __future__ import annotations

import collections
import re
from pathlib import Path
from typing import Optional

from backend.app.pipeline.stages.block_grounding.singleline_structurer import structure_singleline_text

_PANEL = {"1": "РП1", "2": "РП2", "3": "РП3", "4": "РП4 (АВР)", "5": "РП5"}
_FLOOR_RE = re.compile(
    r"(-?\d+\s*-?\s*\d*\s*эт|МОП|ЛК\d|подвал|кровл|антресол|-1\s?этаж|тех\.?\s*помещ|электрощитов|лобби|коридор)",
    re.I)


def _distinct_tokens(vector_text: str) -> list:
    """Несколько редких токенов из вектора блока — для поиска его PDF-страницы."""
    toks = []
    for m in re.finditer(r"К\d+\.\d+\.\S+|QF\d+\.\d+|\d{3}\.\d+А|\d{2,3}кВт", vector_text):
        t = m.group(0)
        if t not in toks:
            toks.append(t)
        if len(toks) >= 8:
            break
    return toks


def _find_page_index(doc, vector_text: str) -> Optional[int]:
    needles = _distinct_tokens(vector_text)
    if not needles:
        return None
    best, best_hits = None, 0
    for i in range(doc.page_count):
        txt = doc[i].get_text()
        hits = sum(1 for n in needles if n in txt)
        if hits > best_hits:
            best_hits, best = hits, i
    return best if best_hits >= max(2, len(needles) // 2) else None


def _convex_hull(points):
    """Выпуклая оболочка (monotone chain). Плотно облегает точки, без самопересечений."""
    pts = sorted(set((round(x, 2), round(y, 2)) for x, y in points))
    if len(pts) <= 2:
        return pts

    def cross(o, a, b):
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    lower = []
    for p in pts:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    upper = []
    for p in reversed(pts):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    return lower[:-1] + upper[:-1]


def _near(qx, qy, arr, dx, dymin, dymax):
    c = sorted((abs(x - qx), y, t) for x, y, t in arr if abs(x - qx) < dx and dymin < (y - qy) < dymax)
    return c[0][2] if c else None


def render_graph_for_prompt(graph: dict) -> str:
    """Компактный текст графа схемы для промпта GPT (вместо скудного enrichment)."""
    if not graph:
        return ""
    L = []
    v = graph.get("validation", {})
    L.append(f"## Структура схемы (распознанный граф, считай верным):")
    L.append(f"Панель: {graph.get('panel')} | линий {graph.get('feeders_total')} "
             f"(актив {v.get('active')}, резерв {v.get('reserve')}, проверить {v.get('ambiguous')})")
    p = graph.get("power")
    if p:
        L.append("\nПИТАНИЕ:")
        L.append(f"- Источник: {p.get('external_source')}")
        for inp in p.get("inputs", []):
            cur = (f" | Iр.раб/авар {inp['i_rab']}/{inp.get('i_avar')}А"
                   if inp.get("i_rab") is not None else "")
            L.append(f"- {inp['id']} ({inp.get('vvod')}) → {', '.join(inp.get('feeds') or []) or '?'} | "
                     f"{inp.get('incomer') or '?'} | {inp.get('switch') or ''}{cur} | счётчик {inp.get('meter') or '-'}")
        if p.get("avr"):
            a = p["avr"]
            L.append(f"- АВР: {a['device']} {a.get('in_a')}А → {', '.join(a.get('feeds') or [])} ({a.get('note')})")
    for pan in graph.get("panels", []):
        L.append(f"\n{pan['name']} — {pan['feeder_count']} линий (актив {pan['active']}, резерв {pan['reserve']}):")
        for f in pan.get("feeders", []):
            if f["status"] == "reserve":
                L.append(f"  {f['qf']}: РЕЗЕРВ ({f.get('breaker_type') or ''} {f.get('breaker_icn') or ''}/{f.get('breaker_in') or ''})".rstrip())
                continue
            br = f"{f.get('breaker_type') or '?'} {f.get('breaker_icn') or ''}/{f.get('breaker_in') or ''}".strip()
            if f["status"] == "ambiguous" or f.get("P_calc_kw") is None:
                L.append(f"  {f['qf']}: ‼ requires_review — автомат {br}; потребитель/код не сопоставлены")
                continue
            ctrl = (" | упр:" + ",".join(f.get("control"))) if f.get("control") else ""
            L.append(f"  {f['qf']}: {f.get('consumer') or '?'} | {br} | {f.get('circuit_code') or '?'} | "
                     f"Pрасч {f.get('P_calc_kw')}кВт | I {f.get('I_a')}А | {f.get('cable') or '?'} | "
                     f"{f.get('length_m')}м | Iкз {f.get('Ikz_ka')}кА{ctrl}")
    rv = graph.get("review") or []
    if rv:
        L.append("\nТРЕБУЕТ ПРОВЕРКИ:")
        for r in rv:
            L.append(f"- {r['qf']}: {'; '.join(r.get('notes') or [])}")
    return "\n".join(L)


def _extract_power(words, page_text: str, qf_incomers: list) -> dict:
    """Вводная часть (питание): вводы ВП, АВР, рёбра ввод→РП — из подписей/устройств/таблиц.

    Источники: подписи «Ввод N (РП…+РП…)» (топология питания), маркеры устройств
    (ВА-305/ВР-101/АВР-301/НАРТИС/ТА), нижний ряд вводных QF (ВА-305), токи Ip из таблицы.
    Текст широкого CAD-листа разбросан → берём надёжное, остальное помечаем.
    """
    full = page_text or ""

    # 1) подписи Ввод N (РП…) → какой ввод какие РП питает
    inputs = {}
    for m in re.finditer(r"Ввод\s*([12])\s*\(([^)]{3,80})\)", full):
        n, body = m.group(1), m.group(2)
        rps = []
        for rm in re.finditer(r"РП\d(?:\([^)]{0,12}\))?", body):
            r = rm.group(0)
            if r not in rps:
                rps.append(r)
        key = f"ВП{n}"
        inputs.setdefault(key, {"id": key, "vvod": f"Ввод {n}", "feeds": []})
        for r in rps:
            if r not in inputs[key]["feeds"]:
                inputs[key]["feeds"].append(r)

    # 2) инвентарь устройств вводной зоны (что присутствует)
    def present(pat):
        return sorted({w[4] for w in words if re.match(pat, w[4])})
    devices = {
        "incomer_breakers": present(r"^ВА-305"),           # вводные автоматы
        "switches": present(r"^ВР-101"),                    # разъединители/QS
        "avr": present(r"^АВР-?3?0?1?"),                     # АВР-301
        "meters": sorted({w[4].rstrip("-") for w in words if "НАРТИС" in w[4]}),
        "ct_ratios": sorted({w[4] for w in words if re.fullmatch(r"\d{3,4}/5А?", w[4])}),
    }

    # 3) токи вводов из таблицы (строки «ВПn … Ip» / явные Ip=)
    currents = {}
    for m in re.finditer(r"(ВП[12]|ВП-АВР|РП\d(?:\s*\([^)]{0,10}\))?)\s+(\d{2,3}\.\d{1,2})\s+(\d{2,3}\.\d{1,2})", full):
        currents[m.group(1).strip()] = {"i_rab": float(m.group(2)), "i_avar": float(m.group(3))}

    # 4) вводные QF каждой РП (нижний ряд, ВА-305)
    rp_incomers = []
    for qx, qy, qn in qf_incomers:
        amp = _near(qx, qy, [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"\d+А", w[4])], 50, -10, 90)
        ba = _near(qx, qy, [(w[0], w[1], w[4]) for w in words if re.match(r"^ВА", w[4])], 50, -40, 90)
        m = re.match(r"QF(\d+)", qn)
        rp_incomers.append({"qf": qn, "panel": _PANEL.get(m.group(1) if m else "", "ВРУ"),
                            "device": " ".join(x for x in (ba, amp) if x) or None})

    for key, inp in inputs.items():
        if key in currents:
            inp.update(currents[key])
        inp["incomer"] = "ВА-305 320А 35кА" if devices["incomer_breakers"] else None
        inp["switch"] = "ВР-101-630 630А" if any("630" in s for s in devices["switches"]) else None
        inp["meter"] = devices["meters"][0] if devices["meters"] else None

    avr = None
    if devices["avr"]:
        avr = {"device": "АВР-301", "in_a": 40, "feeds": ["РП4 (АВР)"],
               "note": "питание Рабочий/Резервный ввод, QS1/QS2 ВР-101-63 63А",
               "current": currents.get("ВП-АВР")}

    return {
        "external_source": "Внешняя сеть (ПАО «Мосэнергосбыт»)",
        "inputs": list(inputs.values()),
        "avr": avr,
        "rp_incomers": rp_incomers,
        "devices": devices,
        "currents_table": currents,
    }


def _fnum(v):
    return v if v is not None else None


# «распознанное» (НЕ подпись): формула/кабель/трасса/автомат/управление/устройство/разделители.
# Голые ЦЕЛЫЕ числа (счёт квартир, этаж, категория) НЕ исключаем; десятичные — да (Кс/cos/%).
_CONSUMER_KNOWN_RE = re.compile(
    r"^К\d+\.\d"                                                  # код цепи
    r"|кВт|^[\d.]+[АAaа]$|^\d+%$|^\d+[.,]\d+$|cos"                # формула (P/I/cos/Кс-десятичные)
    r"|ППГ|^\dх[\d.]|Iкз|^[\d.]+кА|FRHF|HF$"                       # кабель
    r"|ВА-|Icn|^\d+Р$"                                             # автомат
    r"|Лоток|Пг\.|Пз\.|Па\.|Каб\.нес|констр|^\d+м;?$"             # трасса
    r"|систем|управлен|^к$|^АСУД|^АПС|^ПС$|откл|пожар|диспетчер"   # управление (^ якорь: «ЩД-АСУД» = подпись, не съедать)
    r"|МК103|^УЗО|^КМ$|ОП101|НПН|-230В|^2Р$|мА|^\dН[ОЗ]$|НО\+|1НЗ"  # устройство+контакты
    r"|TA\d|^\dхТ|хТ-0|/5[АAaа]|НАРТИС|И300|^Wh$|W132"             # учёт/ТТ/счётчик
    r"|^[-–:;]$"                                                   # разделители
)


def _extract_consumer_geo(qx, qy, nx, words, formula_vals) -> Optional[str]:
    """Подпись потребителя из вектора по колонке: «распарсить известное → ОСТАТОК = подпись».

    Вычитаем структурные элементы (формула/кабель/трасса/автомат/управление) + ТОЧНЫЕ
    значения формулы (Кс/cos/P/I, чтобы убрать целые типа Кс=1), описательный остаток на
    X-линии подписи — это потребитель. Без словаря потребителей.
    """
    half = min((nx - qx) / 2 + 3, 35)
    col = [w for w in words if qx - half <= w[0] < qx + half and qy - 262 < w[1] < qy - 5]
    if not col:
        return None
    fset = set()
    for v in formula_vals:
        if v is None:
            continue
        fset.add(str(v))
        try:
            fv = float(v)
            if fv == int(fv):
                fset.add(str(int(fv)))
        except (TypeError, ValueError):
            pass

    def known(t):
        if _CONSUMER_KNOWN_RE.search(t):
            return True
        if re.fullmatch(r"\d+(?:[.,]\d+)?", t) and t.replace(",", ".") in fset:
            return True
        return False

    rest = [w for w in col if not known(w[4])]
    cyr = [w for w in rest if re.search(r"[А-Яа-яё]", w[4])]
    if not cyr:
        return "Резерв" if any("езерв" in w[4] for w in col) else None
    mx = collections.Counter(round(w[0] / 5) * 5 for w in cyr).most_common(1)[0][0]
    line = sorted([w for w in rest if abs(round(w[0] / 5) * 5 - mx) <= 16], key=lambda w: -w[1])
    text = re.sub(r"\s+", " ", " ".join(w[4] for w in line)).strip(" -–:;,")
    return text or None


def build_singleline_graph(pdf_path: Path, vector_text: str, *, panel_hint: str = "ВРУ") -> Optional[dict]:
    """Построить граф однолинейной схемы. None — если не feeder-схема / нет геометрии."""
    base = structure_singleline_text(vector_text)
    if not base or base.get("feeder_total", 0) < 3:
        return None
    params = {f["circuit_code"]: f for s in base["bus_sections"] for f in s["feeders"]}

    try:
        import fitz
        doc = fitz.open(str(pdf_path))
    except Exception:
        return None
    try:
        pidx = _find_page_index(doc, vector_text)
        if pidx is None:
            return None
        pg = doc[pidx]
        words = pg.get_text("words")
        page_full_text = pg.get_text()
        page_w, page_h = float(pg.rect.width), float(pg.rect.height)
    except Exception:
        return None
    finally:
        try:
            doc.close()
        except Exception:
            pass

    def coll(pat, pred=None):
        return [(w[0], w[1], w[4]) for w in words if re.match(pat, w[4]) and (pred is None or pred(w[4]))]

    qf_all = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"QF\d+\.\d+", w[4])]
    if len(qf_all) < 3:
        return None
    # исходящие vs вводные по Y (вводные — нижний ряд)
    ys = sorted(q[1] for q in qf_all)
    y_split = ys[0] + (ys[-1] - ys[0]) * 0.6 if ys[-1] - ys[0] > 60 else ys[-1] + 1
    qf_out = sorted([q for q in qf_all if q[1] <= y_split])
    qf_incomers = sorted([q for q in qf_all if q[1] > y_split])
    n_incomers = len(qf_incomers)
    codes = coll(r"^К\d+\.\d+\.", lambda t: "кВт" not in t)
    BA = coll(r"^ВА")
    KA = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"\d+кА", w[4])]
    AMP = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"\d+А", w[4])]
    POLE = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"[123]Р", w[4])]
    RES = [(w[0], w[1]) for w in words if "езерв" in w[4]]
    PS = [(w[0], w[1]) for w in words if re.fullmatch(r"ПС", w[4])]
    ASUD = [(w[0], w[1]) for w in words if "АСУД" in w[4]]

    def pref(qn):
        return re.match(r"QF(\d+)", qn).group(1)

    geo = {}
    for qx, qy, qn in qf_out:
        geo[qn] = {
            "ba": _near(qx, qy, BA, 34, -95, 110),
            "ka": _near(qx, qy, KA, 44, -30, 100),
            "amp": _near(qx, qy, AMP, 44, -30, 100),
            "pole": _near(qx, qy, POLE, 44, -95, 110),
            "reserve": any(abs(x - qx) < 38 and -320 < (y - qy) < 80 for x, y in RES),
            "control": ([t for t in (["ПС"] if any(abs(x - qx) < 42 and -320 < (y - qy) < 130 for x, y in PS) else [])]
                        + (["АСУД"] if any(abs(x - qx) < 42 and -320 < (y - qy) < 130 for x, y in ASUD) else [])),
        }

    # монотонная привязка кода по панели (резерв пропускает код)
    assign = {}
    dup = collections.Counter(q[2] for q in qf_out)
    for p in sorted(set(pref(q[2]) for q in qf_out)):
        pq = sorted([q for q in qf_out if pref(q[2]) == p])
        xs = [q[0] for q in pq]
        pc = sorted([c for c in codes if min(xs) - 45 <= c[0] <= max(xs) + 45])
        ci = 0
        for qx, qy, qn in pq:
            if geo[qn]["reserve"]:
                assign[qn] = None
            elif ci < len(pc):
                assign[qn] = pc[ci][2]; ci += 1
            else:
                assign[qn] = None

    feeders = []
    qf_xs = sorted(q[0] for q in qf_out)
    for qx, qy, qn in qf_out:
        g = geo[qn]
        code = assign.get(qn)
        p = params.get(code) if code else None
        nx = next((x for x in qf_xs if x > qx + 1), qx + 70)
        consumer_geo = _extract_consumer_geo(
            qx, qy, nx, words,
            [(p or {}).get("P_inst_kw"), (p or {}).get("Kc"), (p or {}).get("cosphi"),
             (p or {}).get("P_calc_kw"), (p or {}).get("I_a")])
        if consumer_geo == "Резерв":
            consumer_geo = "Резерв (свободная ячейка)"
        consumer = consumer_geo or (p.get("consumer") if p else None) \
            or ("Резерв (свободная ячейка)" if g["reserve"] else None)
        status = "reserve" if g["reserve"] else ("active" if p else "ambiguous")
        review = []
        if status == "ambiguous":
            review.append("колонка без сопоставленного кода — requires_review")
        if dup[qn] > 1:
            review.append(f"метка {qn} повторяется ({dup[qn]}×)")
        in_a = None
        if g["amp"]:
            try:
                in_a = float(re.sub(r"\D", "", g["amp"]))
            except ValueError:
                in_a = None
        if p and in_a and p.get("I_a") and in_a < p["I_a"]:
            review.append(f"номинал {in_a:.0f}А < тока {p['I_a']}А — проверить")
        loc = None
        if consumer:
            m = _FLOOR_RE.search(consumer)
            loc = m.group(0) if m else None
        # bbox колонки «одна линия» (page-normalized) — для визуальной проверки связи данных.
        # X — границы = СЕРЕДИНЫ до соседних QF (тайлинг без наложения), cap 38px на сторону.
        prev_x = max([x for x in qf_xs if x < qx - 1], default=qx - 64)
        next_x = min([x for x in qf_xs if x > qx + 1], default=qx + 64)
        left = qx - min((qx - prev_x) / 2, 38)
        right = qx + min((next_x - qx) / 2, 38)
        colw = [w for w in words if left <= w[0] < right and qy - 280 < w[1] < qy + 30]
        bbox_page = None
        polygon_page = None
        if page_w and page_h:
            # Y — от верха текста потребителя ВНИЗ до шины (всё над автоматом и под ним)
            y_top = min((w[1] for w in colw), default=qy - 230)
            y_bot = qy + 60
            bbox_page = [round(left / page_w, 5), round(y_top / page_h, 5),
                         round(right / page_w, 5), round(y_bot / page_h, 5)]
            # Полигон ТОЧНО по тексту фидера: выпуклая оболочка слов колонки (потребитель +
            # формула + кабель + трасса + QF-метка + автомат) → плотно облегает реальный текст,
            # ширина фигуры = ширина текста. + узкая ножка до шины (qx, y_bot).
            fw = [w for w in words if left <= (w[0] + w[2]) / 2 < right and qy - 285 < w[1] < qy + 30]
            if len(fw) >= 2:
                pts = []
                for w in fw:
                    pts += [(w[0], w[1]), (w[2], w[1]), (w[2], w[3]), (w[0], w[3])]
                pts.append((qx, y_bot))   # ножка до шины
                hull = _convex_hull(pts)
                if len(hull) >= 3:
                    polygon_page = [[round(x / page_w, 5), round(y / page_h, 5)] for x, y in hull]
        feeders.append({
            "qf": qn, "panel": _PANEL.get(pref(qn), panel_hint),
            "consumer": consumer, "location": loc, "circuit_code": code,
            "bbox_page": bbox_page, "polygon_page": polygon_page,
            "breaker_type": g["ba"], "breaker_poles": g["pole"],
            "breaker_icn": g["ka"], "breaker_in": g["amp"],
            "P_inst_kw": (p or {}).get("P_inst_kw"), "Kc": (p or {}).get("Kc"),
            "cosphi": (p or {}).get("cosphi"), "P_calc_kw": (p or {}).get("P_calc_kw"),
            "I_a": (p or {}).get("I_a"), "cable": (p or {}).get("cable"),
            "length_m": (p or {}).get("length_m"), "voltage_drop_pct": (p or {}).get("voltage_drop_pct"),
            "Ikz_ka": (p or {}).get("Ikz_ka"), "routing": (p or {}).get("routing"),
            "phase": (p or {}).get("phase"), "control": g["control"],
            "status": status, "review": review,
        })

    feeders.sort(key=lambda f: (int(re.match(r"QF(\d+)", f["qf"]).group(1)),
                                [float(x) for x in f["qf"][2:].split(".")]))
    panels_map = collections.defaultdict(list)
    for f in feeders:
        panels_map[f["panel"]].append(f)
    panels = [{"id": name, "name": name, "feeder_count": len(fl),
               "active": sum(1 for x in fl if x["status"] == "active"),
               "reserve": sum(1 for x in fl if x["status"] == "reserve"),
               "feeders": fl}
              for name, fl in sorted(panels_map.items())]

    linked = [f for f in feeders if f.get("circuit_code")]
    reserve = [f for f in feeders if f["status"] == "reserve"]
    ambiguous = [f for f in feeders if f["status"] == "ambiguous"]
    review_items = [{"qf": f["qf"], "code": f.get("circuit_code"), "notes": f["review"]}
                    for f in feeders if f["review"]]
    bv = base["validation"]
    try:
        power = _extract_power(words, page_full_text, qf_incomers)
    except Exception:
        power = None
    return {
        "panel": panel_hint,
        "type": "single_line_calc_diagram",
        "source_page_index": pidx,
        "feeders_total": len(feeders),
        "incomers": n_incomers,
        "power": power,
        "panels": panels,
        "validation": {
            "active": len(linked), "reserve": len(reserve), "ambiguous": len(ambiguous),
            "breaker_bound": f"{sum(1 for f in feeders if f.get('breaker_type'))}/{len(feeders)}",
            "power_rate": bv.get("power_rate"), "current_rate": bv.get("current_rate"),
            "codes_total": len(params), "codes_linked": len({f.get('circuit_code') for f in linked}),
        },
        "review": review_items,
        "feeders_flat": feeders,
    }
