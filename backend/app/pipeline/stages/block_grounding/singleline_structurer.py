"""singleline_structurer — структуризатор вектор-текста однолинейной расчётной схемы.

Часть механизма «Вектограф» (vectograf): вектор-слой PDF → граф однолинейной схемы.
Вектограф = этот модуль (разбор текста-формул) + singleline_graph_geometry (топология
по координатам PDF) + рендер в Markdown. Полное описание: docs/vectograf.md.

Превращает «плоский» текст-слой PDF (pdfplumber) однолинейной схемы ВРУ/ГРЩ в граф,
повторяющий топологию: ввод → секции шин → массив отходящих линий (каждая с аппаратом,
кабелем, расчётными параметрами, трассой).

Метод (детерминированный, без LLM; разработан и провалидирован исследованием 2026-06-30):
1. СЕГМЕНТАЦИЯ: якорь линии = param-строка `код : Pуст кВт - Kc - cosφ - Pрасч кВт- I А`
   (сигнатура «кВт-…-кВт-…А» однозначно маркирует отходящую линию).
2. РАЗВОРОТ от якоря: ниже — кабель/Iкз + трасса; выше — потребитель.
3. ПРИВЯЗКА АППАРАТА по reading-order: тип ВА-… на 3–4 строки выше якоря, номинал «кА А»
   на 2–3 выше — «ближайшим вверх». QF принимаем ТОЛЬКО на дистанции ровно 2 (иначе null:
   QF дрейфует по X-колонке → ложная привязка). Никакого «жадного» захвата.
4. ИЕРАРХИЯ: маркеры `L1,L2,L3`+`PEN` делят линии на секции шин.
5. ВАЛИДАЦИЯ (физика): Pрасч≈Pуст·Kc; I≈Pрасч/(√3·0.38·cosφ) [3ф] | Pрасч/(0.22·cosφ) [1ф].

Чисто, офлайн, 0 токенов. Возвращает None, если это не feeder-схема (нет param-якорей).
"""
from __future__ import annotations

import math
import re
from typing import Optional

SQRT3 = math.sqrt(3)
U_LL = 0.38   # кВ, линейное
U_PH = 0.22   # кВ, фазное

# Якорь отходящей линии: <код> : <Pуст>кВт - <Kc> - <cosφ> - <Pрасч>кВт- <I>А
PARAM_RE = re.compile(
    r"^(?P<code>\S+?)\s*:\s*"
    r"(?P<pinst>[\d.]+)\s*кВт\s*-\s*"
    r"(?P<kc>[\d.]+)\s*-\s*"
    r"(?P<cos>[\d.]+)\s*-\s*"
    r"(?P<pcalc>[\d.]+)\s*кВт\s*-\s*"
    r"(?P<ia>[\d.]+)\s*А\s*$"
)
PHYS_RE = re.compile(
    r"^(?P<len>[\d.]+)\s*м\s*-\s*"
    r"(?P<du>[\d.]+)\s*%\s*-\s*"
    r"(?P<cable>.+?)\s*-\s*"
    r"Iкз\(1\)\s*=\s*(?P<ikz>[\d.]+)\s*кА"
)
BA_RE = re.compile(r"(ВА-?\d\d\S*|ВА\d\d\S*)(?:\s*(1Р|1P|2Р|2P|3Р|3P))?")
KA_RE = re.compile(r"(?P<ka>\d+)\s*кА\s+(?P<a>\d+)\s*А")  # '35кА 200А'
QF_RE = re.compile(r"^QF[\d.]+$")
ROUTE_RE = re.compile(r"^(Лоток|Пг\.|Каб\.|Пг |П\.|см\.)", re.IGNORECASE)
SECTION_RE = re.compile(r"^L1,?\s*L2,?\s*L3$")

NOISE_PREFIX = (
    "L1,L2,L3", "PEN", "QF", "QS", "Wh", "TA", "HL", "ВП", "ВА-", "ВА", "ВР-",
    "к системе", "АСУД", "(откл", "УЗО", "КМ ", "нет ", "Резерв", "ЯТП",
    "ЩМкв", "...", "Iкз", "Iу", "Ру=", "Кс", "Cos", "Рр", "Sр", "Ip",
)


def _is_noise(s: str) -> bool:
    s = s.strip()
    if not s:
        return True
    for p in NOISE_PREFIX:
        if s.startswith(p):
            return True
    if KA_RE.search(s):
        return True
    if ROUTE_RE.match(s):
        return True
    return False


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def structure_singleline_text(vector_text: str, *, panel: str = "схема") -> Optional[dict]:
    """Структурировать вектор-текст однолинейной схемы в граф. None, если не feeder-схема."""
    if not vector_text or not vector_text.strip():
        return None
    lines = vector_text.split("\n")
    n = len(lines)

    anchors = [i for i, l in enumerate(lines) if PARAM_RE.match(l.strip())]
    if len(anchors) < 2:
        return None  # не однолинейная feeder-схема
    anchor_set = set(anchors)
    section_marks = [i for i, l in enumerate(lines) if SECTION_RE.match(l.strip())]

    def section_of(line_idx: int) -> int:
        sec = 0
        for k, m in enumerate(section_marks, start=1):
            if m < line_idx:
                sec = k
        return sec

    feeders = []
    for ai, i in enumerate(anchors):
        gd = PARAM_RE.match(lines[i].strip()).groupdict()
        code = gd["code"]
        prev_anchor = anchors[ai - 1] if ai > 0 else -1

        # физическая строка (длина/ΔU/кабель/Iкз)
        cable = ikz = length_m = du = None
        phys_idx = i + 1
        for j in range(i + 1, min(n, i + 4)):
            pm = PHYS_RE.match(lines[j].strip())
            if pm:
                length_m, du = _f(pm.group("len")), _f(pm.group("du"))
                cable, ikz = pm.group("cable").strip(), _f(pm.group("ikz"))
                phys_idx = j
                break

        # трасса
        routing = None
        for j in range(phys_idx + 1, min(n, phys_idx + 3)):
            s = lines[j].strip()
            if j in anchor_set:
                break
            if ROUTE_RE.match(s):
                routing = s
                break

        # потребитель (первый «не-шумовой» текст вверх до пред. якоря)
        consumer = None
        for j in range(i - 1, max(prev_anchor, i - 8), -1):
            s = lines[j].strip()
            if not s or PHYS_RE.match(s) or ROUTE_RE.match(s):
                continue
            if BA_RE.search(s) or QF_RE.match(s) or _is_noise(s):
                continue
            consumer = s
            break

        up_limit = max(prev_anchor, i - 6)
        wide_limit = max(prev_anchor, i - 9)

        # тип аппарата (ближайший ВА- вверх)
        breaker_type = poles = bt_dist = None
        for lim in (up_limit, wide_limit):
            for j in range(i - 1, lim, -1):
                bm = BA_RE.search(lines[j].strip())
                if bm:
                    breaker_type, poles, bt_dist = bm.group(1), bm.group(2), i - j
                    break
            if breaker_type:
                break

        # номинал (ближайший 'кА А' вверх)
        icn_ka = in_a = kr_dist = None
        for lim in (up_limit, wide_limit):
            for j in range(i - 1, lim, -1):
                km = KA_RE.search(lines[j].strip())
                if km:
                    icn_ka, in_a, kr_dist = _f(km.group("ka")), _f(km.group("a")), i - j
                    break
            if in_a is not None:
                break

        # QF только на дистанции ровно 2 (иначе null)
        feeder_qf = qf_dist = None
        for j in range(i - 1, max(prev_anchor, i - 4), -1):
            s = lines[j].strip()
            if QF_RE.match(s):
                qf_dist = i - j
                if qf_dist == 2:
                    feeder_qf = s
                break

        if breaker_type and icn_ka is not None and bt_dist is not None and bt_dist <= 4:
            binding_conf = "high" if feeder_qf else "medium"
        elif breaker_type:
            binding_conf = "low"
        else:
            binding_conf = "none"

        pinst, kc, cosphi, pcalc, ia = (_f(gd["pinst"]), _f(gd["kc"]), _f(gd["cos"]),
                                        _f(gd["pcalc"]), _f(gd["ia"]))

        cm = re.search(r"\b([345])\s*[хx]\s*", cable or "")
        cores = int(cm.group(1)) if cm else None
        if cores == 3:
            is_1ph = True
        elif cores in (4, 5):
            is_1ph = False
        else:
            is_1ph = bool(poles and poles.startswith("1"))

        feeders.append({
            "line_index": ai,
            "feeder_qf": feeder_qf,
            "breaker_type": breaker_type,
            "breaker_poles": poles,
            "breaker_rating": {"icn_ka": icn_ka, "in_a": in_a},
            "consumer": consumer,
            "circuit_code": code,
            "P_inst_kw": pinst,
            "Kc": kc,
            "cosphi": cosphi,
            "P_calc_kw": pcalc,
            "I_a": ia,
            "length_m": length_m,
            "voltage_drop_pct": du,
            "cable": cable,
            "Ikz_ka": ikz,
            "routing": routing,
            "phase": "1ph" if is_1ph else "3ph",
            "binding_confidence": binding_conf,
            "_bus_section": section_of(i),
        })

    # иерархия: секции шин
    sections = []
    orphan = [d for d in feeders if d["_bus_section"] == 0]
    if orphan:
        sections.append({"bus_section_id": "СШ-0", "phases": "L1,L2,L3 + PEN",
                         "feeder_count": len(orphan), "feeders": orphan})
    for sec in range(1, len(section_marks) + 1):
        sf = [d for d in feeders if d["_bus_section"] == sec]
        sections.append({"bus_section_id": f"СШ-{sec}", "phases": "L1,L2,L3 + PEN",
                         "feeder_count": len(sf), "feeders": sf})

    # физпроверки
    power_ok = power_tot = cur_ok = cur_tot = 0
    for d in feeders:
        p, kc, pc = d["P_inst_kw"], d["Kc"], d["P_calc_kw"]
        if None not in (p, kc, pc):
            power_tot += 1
            if abs(p * kc - pc) <= max(0.01, 0.02 * max(pc, 0.01)):
                power_ok += 1
        pc, cos, ia = d["P_calc_kw"], d["cosphi"], d["I_a"]
        if None not in (pc, cos, ia) and cos > 0 and ia > 0:
            cur_tot += 1
            exp_i = pc / (U_PH * cos) if d["phase"] == "1ph" else pc / (SQRT3 * U_LL * cos)
            if abs(exp_i - ia) <= 0.12 * ia:
                cur_ok += 1

    return {
        "panel": panel,
        "type": "single_line_calc_diagram",
        "bus_sections": sections,
        "feeder_total": len(feeders),
        "section_markers_count": len(section_marks),
        "validation": {
            "feeders": len(feeders),
            "power_check": f"{power_ok}/{power_tot}",
            "power_rate": round(power_ok / max(power_tot, 1), 3),
            "current_check": f"{cur_ok}/{cur_tot}",
            "current_rate": round(cur_ok / max(cur_tot, 1), 3),
            "breaker_bound": f"{sum(1 for d in feeders if d['breaker_type'])}/{len(feeders)}",
            "qf_bound_strict": f"{sum(1 for d in feeders if d['feeder_qf'])}/{len(feeders)}",
        },
    }
