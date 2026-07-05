"""singleline_graph_geometry — полный граф однолинейной схемы (топология из геометрии PDF).

Восстанавливает граф ввод→панели(РПn)→отходящие линии по чертежу:
- находит правильную PDF-страницу блока (page_index из result.json НЕ совпадает с PDF — баг нумерации);
- кластеризует токены чертежа по X-колонкам (каждая колонка = отходящая линия);
- внутри панели привязка QF↔код — по ГЕОМЕТРИИ КОЛОНКИ (offset-corrected nearest column,
  `_bind_codes_columnwise`): код ставится в ТУ QF, в чьей x-колонке он реально лежит. QF без
  отходящего кода (QF3.1 на ВРУ-К1.2) остаётся непривязанным, а не «сдвигает» весь ряд;
  монотонная привязка — только fallback при ненадёжной геометрии, расхождения → GEOMETRY_CONFLICT;
- автомат/уставка/полюса/управление(АСУД/ПС)/резерв — из геометрии колонки;
- параметры линии — из валидированной таблицы (structure_singleline_text, физика 100%) по коду;
- метаданные листа (расчёты панелей, таблица ТТ, примечания, служебные элементы, дерево питания)
  извлекаются из текста страницы и собираются в полный граф.

build_singleline_graph(pdf_path, vector_text, *, panel_hint) -> dict | None
render_graph_for_prompt(graph)       — компактный текст для промпта Stage 02.
render_graph_etalon_markdown(graph)  — полный Markdown в формате эталона (8 разделов).
Возвращает None, если это не однолинейная feeder-схема или PDF/страница недоступны. fail-soft.
"""
from __future__ import annotations

import collections
import functools
import json
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


def _bbox_clip_enabled() -> bool:
    """Флаг VECTOGRAF_BBOX_CLIP_ENABLED (default ON). Лениво, fail-soft."""
    try:
        from backend.app.core import config as _cfg
        return bool(getattr(_cfg, "VECTOGRAF_BBOX_CLIP_ENABLED", True))
    except Exception:
        return True


def _polygon_text_only_enabled() -> bool:
    """Флаг VECTOGRAF_POLYGON_TEXT_ONLY_ENABLED (default ON). Лениво, fail-soft."""
    try:
        from backend.app.core import config as _cfg
        return bool(getattr(_cfg, "VECTOGRAF_POLYGON_TEXT_ONLY_ENABLED", True))
    except Exception:
        return True


def _filter_text_lines_to_region(text, region_words, *, thr=0.6):
    """Оставить в pdfplumber-тексте только строки, лежащие в области выделения блока — по
    принадлежности токенов строки множеству region_words (уже клипнутых по полигону/bbox слов).

    СОХРАНЯЕТ порядок строк pdfplumber (НЕ пересобирает: пересборка по Y рассыпает формулы фидеров
    `код : Pуст - Kc - ... - I` и ломает структуризатор — проверено, 52→0 фидеров). Строка остаётся,
    если ≥``thr`` её токенов встречаются среди region-слов; пустые/служебные строки сохраняются.
    Примечания/ТТ в «вырезе» контура уходят (их токены не в полигоне), фидеры/расчёты (внутри) — нет.

    fail-soft: нет текста/region_words, пул пуст или результат пуст → исходный текст.
    """
    if not text or not region_words:
        return text
    pool = collections.Counter(str(w[4]) for w in region_words
                               if len(w) >= 5 and str(w[4]).strip())
    if not pool:
        return text
    kept = []
    for line in text.split("\n"):
        toks = [t for t in line.split() if t.strip()]
        if not toks:
            kept.append(line)
            continue
        hit = sum(1 for t in toks if pool.get(t, 0) > 0)
        if hit / len(toks) >= thr:
            kept.append(line)
    result = "\n".join(kept)
    return result if result.strip() else text


def _clip_words_to_bbox(words, bbox_norm, page_w, page_h, *, margin=0.01):
    """Оставить только слова, чей ЦЕНТР попадает в область выделения блока (coords_norm,
    page-normalized [x0,y0,x1,y1]). Защищает геометрию Вектографа от чужого текста листа
    (соседняя схема / таблица / штамп), который ``get_text("words")`` отдаёт со ВСЕЙ страницы.

    fail-soft: bbox нет/битый, страница без размеров, или клип оставил <3 слов при многословном
    листе (bbox в чужой системе координат / слишком тесный) → возвращаем ВСЕ слова (как раньше).
    """
    if not bbox_norm or len(bbox_norm) < 4 or not page_w or not page_h:
        return words
    try:
        x0, y0, x1, y1 = (float(bbox_norm[0]), float(bbox_norm[1]),
                          float(bbox_norm[2]), float(bbox_norm[3]))
    except (TypeError, ValueError):
        return words
    if not (x1 > x0 and y1 > y0):
        return words
    x0 -= margin; y0 -= margin; x1 += margin; y1 += margin
    kept = []
    for w in words:
        try:
            cx = ((float(w[0]) + float(w[2])) / 2.0) / page_w
            cy = ((float(w[1]) + float(w[3])) / 2.0) / page_h
        except (TypeError, ValueError, ZeroDivisionError):
            return words
        if x0 <= cx <= x1 and y0 <= cy <= y1:
            kept.append(w)
    if len(kept) < 3 and len(words) >= 10:
        return words
    return kept


def _point_in_polygon(x, y, poly) -> bool:
    """Точка (x, y) внутри полигона (список (x, y)-вершин). Ray casting (even-odd)."""
    n = len(poly)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = poly[i]
        xj, yj = poly[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi):
            inside = not inside
        j = i
    return inside


def _clip_words_to_polygon(words, polygon_norm, page_w, page_h):
    """Оставить только слова, чей ЦЕНТР попадает в ПОЛИГОН выделения блока (polygon_points_norm,
    page-normalized список [x, y]-вершин). Точнее прямоугольника: у полигональных блоков (L/ступень)
    осевой bbox захватывает чужой контент в «вырезах» контура — полигон его отсекает
    (замер ВРУ-2.1: −550 слов таблицы ТТ вне контура при сохранении всех 70 QF схемы).

    fail-soft: полигон битый (<3 вершин), страница без размеров, или клип оставил <3 слов при
    многословном листе → возвращаем ВСЕ слова (как без клипа).
    """
    if not polygon_norm or len(polygon_norm) < 3 or not page_w or not page_h:
        return words
    try:
        poly = [(float(p[0]), float(p[1])) for p in polygon_norm if len(p) >= 2]
    except (TypeError, ValueError, IndexError):
        return words
    if len(poly) < 3:
        return words
    kept = []
    for w in words:
        try:
            cx = ((float(w[0]) + float(w[2])) / 2.0) / page_w
            cy = ((float(w[1]) + float(w[3])) / 2.0) / page_h
        except (TypeError, ValueError, ZeroDivisionError):
            return words
        if _point_in_polygon(cx, cy, poly):
            kept.append(w)
    if len(kept) < 3 and len(words) >= 10:
        return words
    return kept


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


def _near_xy(qx, qy, arr, dx, dymin, dymax):
    """Как _near, но возвращает (x, y, text) ближайшего токена (нужны координаты строки)."""
    c = sorted((abs(x - qx), y, x, t) for x, y, t in arr if abs(x - qx) < dx and dymin < (y - qy) < dymax)
    return (c[0][2], c[0][1], c[0][3]) if c else None


def _pole_for_breaker(qx, qy, ba_xy, poles, dx=44):
    """Полюса автомата = [123]Р на СТРОКЕ автомата (ближайший по Y к ВА-токену), а не ближайший
    по X — иначе захватывается «2Р» от устройства МК103/УЗО в колонке потребителя (выше QF)."""
    if not poles:
        return None
    if ba_xy:
        _, ba_y, _ = ba_xy
        cand = [(abs(y - ba_y), abs(x - qx), t) for x, y, t in poles
                if abs(x - qx) < dx and abs(y - ba_y) < 26]
        if cand:
            return sorted(cand)[0][2]
    return _near(qx, qy, poles, dx, -95, 110)


# ── Гибрид-энричмент: детерминированная проверка защиты по вектор-значениям ──────────
# Длительно допустимый ток медной жилы (ППГ/ВВГ, прокладка в трубе/лотке, ~30°C),
# ГОСТ 31996 / ПУЭ табл. 1.3.4 — консервативно. Используется для проверки
# «кабель не защищён от перегрузки» (ПУЭ 3.1.11): Iдоп жилы должен быть ≥ In автомата.
_CABLE_AMPACITY_CU = {
    1.5: 19, 2.5: 27, 4: 38, 6: 46, 10: 70, 16: 85, 25: 115, 35: 140,
    50: 175, 70: 215, 95: 260, 120: 300, 150: 355, 185: 405, 240: 480,
}


def _cable_section_mm2(cable) -> Optional[float]:
    """Сечение фазной жилы (мм²) из строки кабеля. Берём число ПОСЛЕ 'х' (не счётчик жил).

    «3х2.5»→2.5; «4х2.5»→2.5; «5х10»→10; «4х(1х120)+(1х70)»→120 (max = фаза).
    Только стандартные сечения (иначе None → проверку кабеля пропускаем, без ложных).
    """
    if not cable:
        return None
    s = str(cable).replace(",", ".")
    secs = []
    for x in re.findall(r"х\s*\(?\s*(?:\d+\s*х\s*)?(\d+(?:\.\d+)?)", s):
        try:
            fv = float(x)
        except ValueError:
            continue
        if fv in _CABLE_AMPACITY_CU:
            secs.append(fv)
    return max(secs) if secs else None


def _deterministic_protection_check(graph: dict) -> tuple[list, list]:
    """Пересчёт защиты по вектор-значениям (0 токенов, детерминированно).

    crit — жёсткие нарушения (недозащита кабеля от перегрузки; k_ч<3);
    warn — граница 5–10·In (мгновенное отключение C-хар. не гарантировано).
    Только active-линии с распознанным In. Даём GPT готовые факты, чтобы критику
    защиты не терять на вариативности прохода (см. 4NPQ: rich упускал Iкз-критал).
    """
    crit, warn = [], []
    for pan in graph.get("panels", []):
        for f in pan.get("feeders", []):
            if f.get("status") != "active":
                continue
            in_m = re.search(r"(\d+(?:\.\d+)?)", str(f.get("breaker_in") or ""))
            if not in_m:
                continue
            In = float(in_m.group(1))
            if In <= 0:
                continue
            qf = f.get("qf")
            cons = (f.get("consumer") or "")[:38]
            sec = _cable_section_mm2(f.get("cable"))
            if sec is not None:
                idop = _CABLE_AMPACITY_CU[sec]
                if idop < In:
                    crit.append(
                        f"- ‼ {qf} ({cons}): кабель {f.get('cable')} — Iдоп≈{idop}А < In автомата "
                        f"{In:.0f}А → жила не защищена от перегрузки (ПУЭ 3.1.11)")
            ikz = f.get("Ikz_ka")
            if ikz is not None:
                try:
                    kch = float(ikz) * 1000.0 / In
                except (TypeError, ValueError):
                    kch = None
                if kch is not None:
                    if kch < 3:
                        crit.append(
                            f"- ‼ {qf} ({cons}): Iкз(1)={ikz}кА = {kch:.1f}·In({In:.0f}А) < 3 → "
                            f"защита не проходит по чувствительности (ПУЭ 1.7.79 / 7.3.139)")
                    elif kch < 10:
                        warn.append(
                            f"- ⚠ {qf} ({cons}): Iкз(1)={ikz}кА = {kch:.1f}·In({In:.0f}А) — зона "
                            f"5–10·In, мгновенное отключение автомата C-хар. не гарантировано")
    return crit, warn


def _render_input_section(graph: dict) -> list:
    """«Ввод и защита» без хардкода «ВА-305 320А» (источник ложного критала).

    Ввод описываем реальной коммутацией (рубильник ВР-101 из service_elements) и
    вердиктом таблицы ТТ (Iр.раб vs Iн1тт + коммент ПУЭ) — это грунт, из-за которого
    rich не галлюцинировал занижение ввода. Плюс явная анти-FP инструкция.
    """
    L = []
    power = graph.get("power") or {}
    if not power.get("inputs"):
        return L
    tt = {}
    for r in graph.get("tt_check_table") or []:
        pn = r.get("panel")
        if pn:
            tt[str(pn).strip()] = r
    qs_switch = None
    for s in graph.get("service_elements") or []:
        el = str(s.get("element") or "")
        note = str(s.get("note") or "")
        if el.startswith("QS") or "рубильник" in note or "разъединит" in note:
            m = re.search(r"ВР-101-\d+\s*(?:\d+P\s*)?\d*А?", str(s.get("value") or ""))
            if m:
                qs_switch = m.group(0).strip()
                break
    L.append("\n## Ввод и защита (детерминированно из вектор-слоя, считай верным):")
    if power.get("external_source"):
        L.append(f"- Источник: {power['external_source']}")
    for inp in power.get("inputs", []):
        feeds = ", ".join(inp.get("feeds") or []) or "?"
        parts = [f"- {inp.get('id')} ({inp.get('vvod')}) → {feeds}"]
        if qs_switch:
            parts.append(f"коммутация ввода: {qs_switch}")
        elif inp.get("switch"):
            parts.append(f"коммутация ввода: {inp['switch']}")
        if inp.get("i_rab") is not None:
            parts.append(f"Iр.раб/авар {inp['i_rab']}/{inp.get('i_avar')}А")
        row = tt.get(inp.get("id"))
        if row:
            tv = f"ТТ: Iн1тт {row.get('In1tt')}, Iр.раб {row.get('Ir_rab')}"
            if row.get("comment"):
                tv += f" — {row['comment']}"
            L.append(" | ".join(parts) + f" | {tv}")
        else:
            L.append(" | ".join(parts))
    if power.get("avr"):
        a = power["avr"]
        L.append(f"- АВР: {a.get('device')} {a.get('in_a')}А → {', '.join(a.get('feeds') or [])} ({a.get('note')})")
    L.append("_Ввод коммутируется рубильниками ВР-101 и вводными автоматами; при Iр.раб ≤ Iн1тт "
             "ввод по номиналу НЕ занижен — не делать замечание о занижении вводного аппарата "
             "без прямого числового расхождения._")
    return L


def render_graph_for_prompt(graph: dict) -> str:
    """Гибрид-энричмент однолинейной схемы для промпта Stage 02 (заменяет прежний compact
    и схлопнутый rich — единый вариант). Структура: граф + анти-галлюцинационный ввод
    (ВР-101 + вердикт ТТ) + ДЕТЕРМИНИРОВАННАЯ проверка защиты (Iкз/сечение) + компактные
    отходящие линии + анти-boilerplate заметки. ~10–12К токенов."""
    if not graph:
        return ""
    L = []
    v = graph.get("validation", {})
    L.append(f"## Структура схемы (распознанный граф, считай верным):")
    L.append(f"Панель: {graph.get('panel')} | линий {graph.get('feeders_total')} "
             f"(актив {v.get('active')}, резерв {v.get('reserve')}, проверить {v.get('ambiguous')})")

    # Ввод + вердикт ТТ (без хардкода ВА-305 320А → нет ложного «вводной занижен»)
    L += _render_input_section(graph)

    # Детерминированная проверка защиты — готовые факты для findings
    crit, warn = _deterministic_protection_check(graph)
    L.append("\n## ⚠ Проверка защиты (детерминированный пересчёт по вектор-значениям — "
             "считай ФАКТОМ, обязательно отрази найденное в findings):")
    if crit:
        L += crit
    if warn:
        L += warn[:12]
        if len(warn) > 12:
            L.append(f"- … ещё {len(warn) - 12} линий в зоне 5–10·In (аналогично, проверить по листу)")
    if not crit and not warn:
        L.append("- по Iкз(1) и сечению кабеля нарушений не выявлено")

    # Взаиморезерв С1↔С2 с расхождениями (грунтованный источник, дёшево)
    flagged_pairs = [p for p in (graph.get("feeder_pairs") or []) if p.get("note")]
    if flagged_pairs:
        L.append("\n## Взаиморезерв С1↔С2 (расхождения номиналов — проверить селективность):")
        for p in flagged_pairs:
            L.append(f"- {p.get('base')}: {p.get('c1_qf')} ↔ {p.get('c2_qf')} — {p.get('note')}")

    # Отходящие линии (компактно, 1 строка/QF)
    for pan in graph.get("panels", []):
        L.append(f"\n{pan['name']} — {pan['feeder_count']} линий (актив {pan['active']}, резерв {pan['reserve']}):")
        for f in pan.get("feeders", []):
            if f["status"] == "reserve":
                L.append(f"  {f['qf']}: РЕЗЕРВ ({f.get('breaker_type') or ''} {f.get('breaker_icn') or ''}/{f.get('breaker_in') or ''})".rstrip())
                continue
            br = f"{f.get('breaker_type') or '?'} {f.get('breaker_icn') or ''}/{f.get('breaker_in') or ''}".strip()
            if f["status"] == "structural":
                L.append(f"  {f['qf']}: секционный/вводной автомат {br} (без отходящей линии)")
                continue
            if f["status"] == "no_code":
                L.append(f"  {f['qf']}: {f.get('consumer') or '?'} | {br} | без построчного кода "
                         f"в спецификации (не ошибка)")
                continue
            if f["status"] == "ambiguous" or f.get("P_calc_kw") is None:
                L.append(f"  {f['qf']}: ‼ requires_review — автомат {br}; потребитель/код не сопоставлены")
                continue
            ctrl = (" | упр:" + ",".join(f.get("control"))) if f.get("control") else ""
            dev = (" | доп:" + "; ".join(f.get("additional_devices"))) if f.get("additional_devices") else ""
            L.append(f"  {f['qf']}: {f.get('consumer') or '?'} | {br} | {f.get('circuit_code') or '?'} | "
                     f"Pрасч {f.get('P_calc_kw')}кВт | I {f.get('I_a')}А | {f.get('cable') or '?'} | "
                     f"{f.get('length_m')}м | Iкз {f.get('Ikz_ka')}кА{ctrl}{dev}")

    rv = graph.get("review") or []
    if rv:
        L.append("\nТРЕБУЕТ ПРОВЕРКИ:")
        for r in rv:
            L.append(f"- {r['qf']}: {'; '.join(r.get('notes') or [])}")

    # Анти-boilerplate: гасим известные ложные направления (см. верификацию 07-04)
    L.append("\n## Заметки проверяющему (чтобы не плодить ложные замечания):")
    L.append("- Кабели с индексом FRHF/HF уже огнестойкие/безгалогенные — НЕ поднимать отдельное "
             "замечание про ОКЛ/огнестойкость, если иных проблем с линией нет.")
    L.append("- Таблицы проверки ТТ, коэффициенты трансформации и раскладка учёта штатно приводятся "
             "на смежных листах тома — их неполнота на ДАННОМ листе не является дефектом проекта.")
    L.append("- Служебные пометки авторазметки (не привязано / геом.конфликты / unbound-коды) — "
             "ограничения извлечения, НЕ дефекты чертежа; замечаний по ним не делать.")
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
    # Y-окно вверх расширено до qy-345: подпись может тянуться 2-3 X-строки выше
    # формулы — «хвосты» вида «(настен.) (правый)», «(потол.) (левый)» лежат над «эт.»
    # (для QF3.10 хвост на y≈qy-330; прежний предел qy-262 их обрезал → §12 last-mile).
    col = [w for w in words if qx - half <= w[0] < qx + half and qy - 345 < w[1] < qy - 5]
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
    # Анти-мусор (A/B ЭМ-К1 07-04: LLM делала мета-замечания на артефакты разметки):
    #  - токены из чистой пунктуации («.», «)») выбрасываем (цифры в подписях легитимны:
    #    «Этажные щиты 43 кв.» — не трогаем);
    #  - итог с <4 буквами — не подпись, а обрывок («. при )», «1.97% Т.32-50м») → None,
    #    лучше честное отсутствие потребителя, чем мусор в промпте Stage 02.
    line = [w for w in line if not re.fullmatch(r"[.,;:()«»\-–+/]+", w[4])]
    text = re.sub(r"\s+", " ", " ".join(w[4] for w in line)).strip(" -–:;,")
    if sum(ch.isalpha() for ch in text) < 4:
        return None
    return text or None


# Доп-аппараты линии (после автомата, в вертикали QF): УЗО03/КМ/МК103 и их параметры.
_DEV_ANCHOR_RE = re.compile(r"^(УЗО|МК103|КМ$)")
_DEV_EXCLUDE_RE = re.compile(
    r"ППГ|Лоток|Пг\.|Пз\.|Каб|констр|^К\d+\.|кВт|%|см\.|систем|управл|пожар|откл|диспетч"
    r"|АСУД|^ПС$|^[А-Яа-яё]{4,}|^\d+м$|^\d+\.\d|FRHF|HF$|^[а-яё]$|^[:;.,)(+\-]+$")


def _extract_additional_devices(qx, qy, nx, words) -> list:
    """Доп-аппараты линии (УЗО03/КМ/МК103…), стоящие в ВЕРТИКАЛИ QF после автомата.

    Аппараты сидят в X-колонке QF между автоматом (чуть ниже метки) и подписью потребителя
    (кластерами по Y). Привязка по колонке (x±half от QF, half < полушага до соседа — чтобы не
    подхватить аппарат соседней линии) и по Y-полосе вокруг анкера устройства (УЗО/КМ/МК103).
    Возвращает список строк-аппаратов (напр. ["УЗО03 4Р 32А 100мА АС", "КМ 3Р 1НО+1НЗ 25А"]),
    сверху вниз. [] — если аппаратов нет (не выдумываем).
    """
    half = min((nx - qx) / 2, 31)
    zone = [(w[0], w[1], w[4]) for w in words
            if abs(w[0] - qx) < half and (qy - 120) < w[1] < (qy - 18)]
    anchors = sorted((y, x, t) for x, y, t in zone if _DEV_ANCHOR_RE.match(t.rstrip(",.")))
    if not anchors:
        return []
    out, seen = [], set()
    for ay, ax, at in anchors:
        near = sorted((y, x, t) for x, y, t in zone
                      if abs(y - ay) <= 22 and not _DEV_EXCLUDE_RE.search(t))
        s = re.sub(r"[,\s]+", " ", " ".join(t for _, _, t in near)).strip(" ,-")
        s = re.sub(r"(\d)\s+мА", r"\1мА", s)   # «30 мА» → «30мА»
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _median(xs):
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return 0.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _bind_codes_columnwise(pq, pc):
    """Привязка код→QF по ГЕОМЕТРИИ КОЛОНКИ (а не по порядку текста), alias-устойчивая.

    pq: отсортированный по X список панели [(qx, qy, qn)]; pc: коды панели [(cx, cy, code)].

    Коды смещены от своей QF на ~постоянную величину δ. Простая медиана «code−nearest_qf»
    работает, пока |δ| < полушага колонки. Но на листах-окончаниях (РП5.1) δ≈+38px при шаге
    ~62px — БОЛЬШЕ полушага: код ближе к СОСЕДНЕЙ QF, и медиана даёт алиас (δ−шаг), что сдвигает
    весь ряд на 1 (QF5.1.14→К1.2.1.12а вместо .13а). Поэтому перебираем алиасы δ ∈ {δ₀−шаг, δ₀,
    δ₀+шаг} в пределах одной колонки (|δ|<шаг) и среди равно-хороших по невязке выбираем тот, что
    даёт МИНИМУМ ведущих пустых колонок (физически резервы — в хвосте, а не в начале). Это:
    - сохраняет 9VCW (δ≈+20<полушага: QF3.1 верно без кода, QF3.10→К1.2.4-2);
    - чинит 7TLY РП5.1 (δ≈+38>полушага: QF5.1.1→К1.2.1.1а).

    Возвращает (assign{qn: code|None}, conflicts{qn: [note...]}).
    """
    names = [n for _, _, n in pq]
    centers = [x for x, _, _ in pq]
    assign = {n: None for n in names}
    conflicts = {}
    if not pc or not centers:
        return assign, conflicts

    def nearest(t):
        return min(range(len(centers)), key=lambda i: abs(centers[i] - t))

    spacing = (_median([centers[i + 1] - centers[i] for i in range(len(centers) - 1)])
               if len(centers) > 1 else 60.0) or 60.0
    code_xs = [cx for cx, _, _ in pc]
    draw = _median([cx - centers[nearest(cx)] for cx in code_xs])
    cands = [d for d in (draw - spacing, draw, draw + spacing) if abs(d) < spacing * 0.9] or [draw]

    def evaluate(d):
        fit, hit = 0, set()
        for cx in code_xs:
            j = nearest(cx - d)
            hit.add(j)
            if abs(centers[j] - (cx - d)) < 0.35 * spacing:
                fit += 1
        return fit, (min(hit) if hit else len(centers))   # (невязка-fit, ведущие пустые)

    scored = [(evaluate(d), d) for d in cands]
    max_fit = max(s[0][0] for s in scored)
    # среди δ с почти-лучшим fit: min ведущих пустых, затем min |δ|
    _, _, delta = min((s[0][1], abs(s[1]), s[1]) for s in scored if s[0][0] >= max_fit - 2)

    owner = {}
    for cx, _, code in pc:
        j = nearest(cx - delta)
        d1 = abs(centers[j] - (cx - delta))
        d2 = min((abs(centers[k] - (cx - delta)) for k in range(len(centers)) if k != j), default=1e9)
        owner.setdefault(names[j], []).append((d1, d2, code))
    for n, lst in owner.items():
        lst.sort()
        d1, d2, code = lst[0]
        assign[n] = code
        if d2 - d1 < 10:   # код почти равноудалён от двух колонок — спорно
            conflicts.setdefault(n, []).append(
                f"GEOMETRY_CONFLICT: код {code} у границы колонки (Δ={d2 - d1:.0f}px) — требует проверки")
        for _d1, _d2, code2 in lst[1:]:
            if code2 != code:   # 2 РАЗНЫХ кода в одну колонку — реальная коллизия
                conflicts.setdefault(n, []).append(
                    f"GEOMETRY_CONFLICT: в колонку {n} попадает 2 кода ({code}, {code2})")
    return assign, conflicts


# Метка QF: 2 сегмента (QF5.30 → панель «5») или 3 сегмента (QF5.1.17 → суб-панель «5.1»).
_QF_RE = re.compile(r"QF\d+(?:\.\d+){1,2}")


def _qf_panel_key(qn: str):
    """(panel_key, feeder_no). QF5.30 → ('5','30'); QF5.1.17 → ('5.1','17').

    Критично: QF5.1 — ПЕРВАЯ линия панели РП5 (key='5'), а НЕ панель РП5.1. Панель РП5.1
    появляется только для трёхсегментных QF5.1.N.
    """
    parts = qn[2:].split(".")
    if len(parts) >= 3:
        return ".".join(parts[:2]), parts[2]
    return parts[0], (parts[1] if len(parts) > 1 else "")


def _panel_name(panel_key: str, page_text: str) -> str:
    """Имя панели: из текста листа «РП<key> (ПЭСПЗ/ОДН/АВР)», иначе статическая карта / «РП<key>»."""
    m = re.search(r"РП" + re.escape(panel_key) + r"\s*\((ПЭСПЗ|ОДН|АВР)\)", page_text or "")
    if m:
        return f"РП{panel_key} ({m.group(1)})"
    return _PANEL.get(panel_key, f"РП{panel_key}")


def _split_codes_by_y_rows(qy_med: float, panel_codes):
    """Разделить коды панели на PRIMARY (ряд отходящих кодов, ближайший к ряду QF) и
    SECONDARY (верхние ряды вторичных цепей «ад»/«ан» — управление двигателями/нагревом).

    На листах-окончаниях основной код фидера и вторичная цепь стоят в почти одной X-колонке,
    но в РАЗНЫХ Y-рядах: primary ≈ на 70-135px выше QF, secondary ≈ на 500-780px выше. Только
    primary участвуют в привязке QF→circuit_code; secondary сохраняются отдельным слоем.

    panel_codes: [(x, y, code)]. Возвращает (primary, secondary) как списки тех же кортежей.
    """
    items = sorted(((qy_med - c[1]), c) for c in panel_codes if (qy_med - c[1]) > 5)
    if not items:
        return [], []
    base = items[0][0]                       # минимальное расстояние вверх = primary-ряд
    primary = [c for d, c in items if d <= base + 160]
    secondary = [c for d, c in items if d > base + 160]
    return primary, secondary


# ── Извлечение метаданных листа из текста страницы (расчёты/ТТ/примечания/служебное) ──

_CALC_RE = re.compile(
    r"(РП\d)(?:\s*\((АВР|ОДН)\))?\s*\(\s*([^)\n]{2,30}?)\s*\)\s*"
    r"Ру=\s*(----|[\d.,]+)\s*кВт\s*"
    r"Кс=\s*(#ДЕЛ/0!|[\d.,]+)\s*"
    r"Cos\s*f=\s*([\d.,]+)\s*"
    r"Рр=\s*(----|[\d.,]+)\s*кВт\s*"
    r"Sр=\s*(----|[\d.,]+)\s*кВА\s*"
    r"Ip=\s*(----|[\d.,]+)\s*А", re.I)
_KZ_RE = re.compile(
    r"(РП\d)(?:\s*\((АВР|ОДН)\))?\s*Iкз\(3\)=\s*([\d.]+)\s*кА\s*"
    r"Iу=\s*([\d.]+)\s*кА\s*Iкз\(1\)=\s*([\d.]+)\s*кА", re.I)
_VVOD_KZ_RE = re.compile(
    r"Iкз\(3\)=\s*(1[0-9]\.\d{3})\s*кА\s*Iу=\s*([\d.]+)\s*кА\s*Iкз\(1\)=\s*([\d.]+)\s*кА")
_TT_ANCHOR_RE = re.compile(
    r"^(ВП-АВР|ВП1|ВП2|РП5\.1 \(ПЭСПЗ\)|РП5 \(ПЭСПЗ\)|РП\d \(ОДН\))$")


def _calc_num(s):
    """'----'/'#ДЕЛ/0!'/'нет'/'-' оставляем как есть (не выдумывать), числа → float."""
    if s is None:
        return None
    s = s.strip()
    if s in ("----", "#ДЕЛ/0!", "нет"):
        return s
    if s in ("-", ""):
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return s


def _mode_key(raw: str) -> str:
    r = raw.lower()
    if "пожар" in r:
        return "пожар"
    if "авар" in r:
        return "авария"
    return "рабочий"


def _extract_panel_calcs(page_text: str) -> list:
    """Карточки панелей: токи КЗ + расчётные режимы (Ру/Кс/Cos f/Рр/Sр/Ip), как в ПД."""
    txt = page_text or ""
    panels = {}

    def slot(pid):
        return panels.setdefault(pid, {"id": pid, "ikz3": None, "iu": None, "ikz1": None, "modes": []})

    for m in _KZ_RE.finditer(txt):
        pid = m.group(1) + (f" ({m.group(2)})" if m.group(2) else "")
        s = slot(pid)
        s["ikz3"], s["iu"], s["ikz1"] = float(m.group(3)), float(m.group(4)), float(m.group(5))
    for m in _CALC_RE.finditer(txt):
        pid = m.group(1) + (f" ({m.group(2)})" if m.group(2) else "")
        slot(pid)["modes"].append({
            "mode": _mode_key(m.group(3)), "mode_raw": m.group(3).strip(),
            "Pu": _calc_num(m.group(4)), "Kc": _calc_num(m.group(5)), "cosphi": _calc_num(m.group(6)),
            "Pr": _calc_num(m.group(7)), "Sr": _calc_num(m.group(8)), "Ip": _calc_num(m.group(9))})
    # КЗ вводных панелей ВП (общая строка 13.717/19.399/9.940 кА)
    vk = _VVOD_KZ_RE.search(txt)
    if vk:
        for vp in ("ВП1", "ВП2"):
            s = slot(vp)
            if s["ikz3"] is None:
                s["ikz3"], s["iu"], s["ikz1"] = float(vk.group(1)), float(vk.group(2)), float(vk.group(3))
                s["type"] = "IP31"
    # порядок: ВП, затем РП по номеру
    def _ord(p):
        pid = p["id"]
        if pid.startswith("ВП"):
            return (0, pid)
        m = re.match(r"РП(\d)", pid)
        return (1, int(m.group(1)) if m else 9)
    return sorted(panels.values(), key=_ord)


_INPUT_CALC_RE = re.compile(
    r"(Ввод\s*[12]|Аварийный\s+режим)(.{0,170}?)"
    r"Ру=\s*(----|[\d.,]+)\s*кВт\s*Кс=\s*(#ДЕЛ/0!|[\d.,]+)\s*Cos\s*f=\s*([\d.,]+)\s*"
    r"Рр=\s*(----|[\d.,]+)\s*кВт\s*Sр=\s*(----|[\d.,]+)\s*кВА\s*Ip=\s*(----|[\d.,]+)\s*А",
    re.I | re.S)


def _extract_input_calcs(page_text: str) -> list:
    """Сводные расчётные режимы по ВВОДАМ (Ввод 1/Ввод 2/аварийный/пожар: Ру/Кс/Cos f/Рр/Sр/Ip).

    Аналог `_extract_panel_calcs`, но на уровне вводов, а не панелей РПn — вектограф фидеро-центричен
    и раньше эти сводные расчёты в граф не попадали. Заголовок «Ввод N (РП…+РП…)» может переноситься
    на несколько строк (re.S, ограниченный хвост ≤170 симв, чтобы не склеить чужой блок). Режим —
    из текста заголовка (`_mode_key`: рабочий/авария/пожар). Дедуп по (ввод, режим, Ру, Рр). Ничего
    не выдумывает: '----'/'#ДЕЛ/0!' сохраняются как есть.
    """
    txt = page_text or ""
    rows, seen = [], set()
    for m in _INPUT_CALC_RE.finditer(txt):
        head = re.sub(r"\s+", " ", (m.group(1) + m.group(2))).strip()
        mode = _mode_key(head)
        panels = re.findall(r"РП\d(?:\s*\((?:ОДН|АВР|ПЭСПЗ)\))?|ПЭСПЗ", head)
        key = (m.group(1).strip(), mode, m.group(3), m.group(6))
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "vvod": head[:90], "mode": mode, "panels": panels,
            "Pu": _calc_num(m.group(3)), "Kc": _calc_num(m.group(4)), "cosphi": _calc_num(m.group(5)),
            "Pr": _calc_num(m.group(6)), "Sr": _calc_num(m.group(7)), "Ip": _calc_num(m.group(8))})
    return rows


def _extract_tt_check(page_text: str) -> list:
    """Таблица проверки коэффициентов трансформации ТТ (если присутствует на листе)."""
    txt = page_text or ""
    head = "Проверка коэффициентов трансформации"
    if head not in txt:
        return []
    lines = [l.strip() for l in txt[txt.index(head):].split("\n")]
    idxs = [i for i, l in enumerate(lines) if _TT_ANCHOR_RE.match(l)]
    rows = []
    for k, i in enumerate(idxs):
        end = idxs[k + 1] if k + 1 < len(idxs) else len(lines)
        body = [l for l in lines[i + 1:end] if l]
        if len(body) < 10:
            continue
        f = body[:10]
        # хвост = комментарий-прозой; ОБОРВАТЬ на первой «схемной» строке, иначе для последней
        # строки таблицы (РП4 ОДН) в комментарий утечёт весь остаток листа (фидеры/режимы).
        _stop = re.compile(r"QF\d|ВА-?\d|К\d+\.\d|АВР\d|ВР-101|TA\d\.|УЗО|МК103|Розетка|=|режим|^РП\d")
        tail = []
        for l in body[10:]:
            if _stop.search(l):
                break
            tail.append(l)
        rows.append({
            "panel": lines[i], "Ir_rab": _calc_num(f[0]), "Ir_avar": _calc_num(f[1]),
            "In1tt": _calc_num(f[2]), "In1tt_avar": _calc_num(f[3]),
            "Isch_max": f[4], "cond1": f[5], "Ir_min": _calc_num(f[6]), "cond2": f[7],
            "max20": f[8], "max_avar": f[9], "comment": " ".join(tail).strip()})
    # авторазметка расхождения «РП4 (ОДН)» vs «РП3 (ОДН)» на схеме (не править молча)
    for r in rows:
        if r["panel"] == "РП4 (ОДН)":
            note = ("В ПД строка названа «РП4 (ОДН)», тогда как панель ОДН на схеме обозначена "
                    "«РП3 (ОДН)». Сохранено как в ПД; требует проверки.")
            r["comment"] = (r["comment"] + " " + note).strip() if r["comment"] else note
            r["review"] = True
    return rows


def _extract_notes(page_text: str) -> list:
    """Примечания листа 1..N отдельным списком (не привязывать к QF)."""
    txt = page_text or ""
    if "Примечание" not in txt:
        return []
    sub = txt[txt.index("Примечание") + len("Примечание"):]
    for stop in ("НАРТИС-И300", "Формат:", "Инв. № подл"):
        if stop in sub:
            sub = sub[:sub.index(stop)]
    sub = re.sub(r"\s+", " ", sub)
    notes, expect = [], 1
    for m in re.finditer(r"(?:^|\s)(\d{1,2})\.\s+(.+?)(?=\s\d{1,2}\.\s|\Z)", sub):
        n, body = int(m.group(1)), m.group(2).strip()
        if n == expect and body:
            notes.append({"n": n, "text": body})
            expect += 1
    return notes


def _extract_service_elements(page_text: str) -> list:
    """Реестр служебных/вторичных элементов (QS/Wh/TA/НАРТИС/ОП101/НПН2/К3/HL/АВР/УЗО/МК103/…)."""
    txt = page_text or ""
    el = []

    def add(name, value, note=""):
        el.append({"element": name, "value": value, "note": note})

    if "ОП101" in txt:
        add("ОП101", "ОП101-4Р-080-B-440 DeKraft", f"{txt.count('ОП101')} шт. (вводная часть ВП1/ВП2)")
    if "Мультиметр" in txt:
        add("Мультиметр", "МТ-72D-3PH-5А-600В-RS485-LED DeKraft", f"{txt.count('Мультиметр')} шт.")
    nartis = txt.count("НАРТИС-И300")
    if nartis:
        add("Wh (НАРТИС)", "НАРТИС-И300-W132-2-A5SR1-230-5-10A-TN-RS485-P1-HLMOQ1V3Z/1-D",
            f"{nartis} узлов учёта Wh")
    # ТТ по группам с попанельным коэффициентом
    ta_ratio = {}
    for m in re.finditer(r"TA(\d)\.\d+\.\.\.TA\1\.\d+\s+3хТ-0,66\s+(\d{3,4}/5А)", txt):
        ta_ratio.setdefault(m.group(1), m.group(2))
    for d in sorted(set(re.findall(r"TA(\d)\.\d", txt))):
        add(f"TA{d}.x", f"3хТ-0,66 {ta_ratio.get(d, '')}".strip(), "трансформаторы тока")
    if "НПН2" in txt:
        add("НПН2", "НПН2 6А", f"{txt.count('НПН2')} шт. (цепи сигнализации/контроля)")
    if "К3-1000В" in txt:
        add("К3", "К3-1000В 0.47мкФ", f"{txt.count('К3-1000В')} шт. (вводная часть)")
    for hl in ("HL1", "HL2"):
        if hl in txt:
            add(hl, "светосигнальный элемент", "индикация вводов")
    for qs in ("QS1", "QS2"):
        if qs in txt:
            add(qs, "ВП: ВР-101-1600 1000А; РП4: ВР-101-630 3P 315А", "разъединитель")
    if "АВР-303" in txt:
        m = re.search(r"АВР-303[^\n]*", txt)
        add("АВР-303", (m.group(0).strip() if m else "АВР-303") + " Iн=250А", "панель АВР (АВР1)")
    if "МК103" in txt:
        add("МК103", "МК103-20А -230В 2Р 2НО", f"{txt.count('МК103')} цепей (освещение/обогрев)")
    if "УЗО03" in txt:
        add("УЗО03", "УЗО03-2Р 30мА 20А", "розеточные линии электрощитовой")
    shul = sorted(set(re.findall(r"ШУ-Л\d", txt)))
    if shul:
        add("ШУ-Л", ", ".join(shul), "лифтовые/машинные помещения (верхняя зона)")
    if "ЩМкв" in txt:
        add("ЩМкв", "квартирные щиты", f"{txt.count('ЩМкв')} групп (стояки квартир)")
    if "ЯТП" in txt:
        add("ЯТП", "ЯТП", "конечный элемент верхней зоны схемы")
    if "РЕ-перемычка" in txt:
        add("РЕ-перемычка / коробка IP65", "РЕ-перемычка, коробка IP65", "зона графических выносок")
    refs = [r for r in ("см. схемы УЭРВ", "см. 13АВ-РД-ЭО-К1", "КЛ, см. том ГРЩ") if r in txt]
    if refs:
        add("Служебные ссылки", "; ".join(refs), "не переносить в потребители QF")
    return el


def _extract_hierarchy(page_text: str, power: dict, panels: list) -> dict:
    """Дерево питания ГРЩ→ВП→РП + рёбра. Источник: подписи «Ввод N (РП…)» + «КЛ, см. том ГРЩ»."""
    txt = page_text or ""
    flat = re.sub(r"\s+", " ", txt)
    # feeds по каждому вводу: окно после «Ввод N (» → все РПn(.m)(суффикс), плюс к РП5/РП5.1
    feeds = {"1": [], "2": []}
    for m in re.finditer(r"Ввод\s*([12])\s*\(", flat):
        n = m.group(1)
        win = flat[m.end():m.end() + 110]
        win = win[:win.find("))") + 1] if "))" in win else win
        for rm in re.finditer(r"РП\d(?:\.\d)?(?:\s*\((?:ПЭСПЗ|ОДН|АВР)\))?", win):
            r = re.sub(r"\s+", " ", rm.group(0)).strip()
            if r not in feeds[n]:
                feeds[n].append(r)
    has5 = "к РП5" in txt or "РП5 (ПЭСПЗ)" in txt
    has51 = "к РП5.1" in txt or "РП5.1 (ПЭСПЗ)" in txt
    src_label = "КЛ, см. том ГРЩ" if "КЛ, см. том ГРЩ" in txt else "КЛ"
    nodes, edges, lines = [], [], []
    for n in ("1", "2"):
        grsh = f"ГРЩ с.ш.{n}"
        vp = f"ВП{n}"
        f = list(feeds[n])
        if has5 and not any(re.match(r"^РП5(?!\.)", x) for x in f):
            f.append("к РП5")
        if has51 and not any(x.startswith("РП5.1") for x in f):
            f.append("к РП5.1")
        nodes += [grsh, vp]
        edges.append({"from": grsh, "to": vp, "type": "питание", "label": src_label})
        for r in f:
            edges.append({"from": vp, "to": r, "type": "распределение", "label": ""})
        lines.append(f"{grsh} -> {vp} -> " + " / ".join(f) if f else f"{grsh} -> {vp}")
    # РП4 (АВР) → свои QF4.*
    qf4 = sorted({q for q in re.findall(r"QF4\.\d+", txt)},
                 key=lambda s: [int(x) for x in s[2:].split(".")])
    if qf4:
        lines.append(f"РП4 (АВР) -> {qf4[0]}..{qf4[-1]}: отходящие линии АВР-зоны")
    return {"nodes": nodes, "edges": edges, "tree_lines": lines, "feeds": feeds}


def _extract_title_source(page_text: str, pdf_path: Path, page_index, panel_hint: str) -> dict:
    """Заголовок/метаданные листа из штампа (имя схемы, раздел, объект)."""
    txt = page_text or ""
    sheet_name = None
    m = re.search(r"Однолинейная расчетная схема\s*([^\n]+)(?:\s*\n\s*(\([^)\n]*\)))?", txt)
    if m:
        sheet_name = ("Однолинейная расчетная схема " + m.group(1).strip()
                      + (" " + m.group(2) if m.group(2) else "")).strip()
    pm = re.search(r"ВРУ-?К?[\d.]+|ГРЩ-?\S*|РП-?\S*", panel_hint or "")
    title = pm.group(0) if pm else (panel_hint or "")
    if not title:
        tm = re.search(r"схема\s+(ВРУ-?\S+|ГРЩ-?\S+)", txt)
        title = tm.group(1) if tm else (panel_hint or "схема")
    # Раздел: код «NNАВ-РД-…» из ШТАМПА, а не из ссылок «см. том …» в примечаниях.
    # Берём самый частый код, НЕ предварённый «см» (ссылки на смежные тома).
    section = None
    cand = collections.Counter()
    for sm in re.finditer(r"(\d{2}АВ-РД-[А-Яа-яA-Za-z0-9.\-]+)", txt):
        prefix = txt[max(0, sm.start() - 8):sm.start()].lower()
        if "см" in prefix or "том" in prefix:
            continue
        cand[sm.group(1).rstrip(".")] += 1
    if cand:
        section = cand.most_common(1)[0][0]
    obj = None
    om = re.search(r"(Внутреннее электроснабжение\s*\.?\s*Корпус\s*\d+)", txt)
    if om:
        obj = re.sub(r"\s+", " ", om.group(1)).strip()
    return {
        "title": title, "sheet_name": sheet_name, "section": section, "object": obj,
        "pdf_file": Path(pdf_path).name if pdf_path else None,
        "page_index": page_index,
    }


def _extract_bus_sections(words, qf_out, qf_incomers):
    """Секции шин: кластеры отходящих QF по X-разрывам + маркер шины (L1,L2,L3 / PEN) у кластера.

    На двухсекционных листах (ГРЩ) фидеры двух секций разделены большим X-разрывом, и у каждой
    группы своя подпись шины ЧУТЬ НИЖЕ ряда QF (QF y≈808..825, шина y≈844..862). Кластер
    признаётся секцией только если в его X-диапазоне есть свой маркер шины — иначе разрыв
    может быть просто компоновкой листа. Имя: токен «с.ш.N» рядом с маркером, иначе «шина N».

    Возвращает (sections:[{id,name,x_range,marker,feeder_qfs,incomer_qfs}], qf→section_id).
    Пустой список — если секция одна (не плодить сущность без информации).
    """
    if len(qf_out) < 4:
        return [], {}
    xs = sorted(q[0] for q in qf_out)
    step = _median([xs[i + 1] - xs[i] for i in range(len(xs) - 1)]) or 60.0
    qy_max = max(q[1] for q in qf_out)
    # маркеры шин: подпись L1,L2,L3 или PEN в полосе чуть ниже ряда отходящих QF
    marks = [(w[0], w[1]) for w in words
             if (re.fullmatch(r"L1,\s*L2,\s*L3", w[4]) or w[4] == "PEN")
             and qy_max - 5 < w[1] < qy_max + 150]
    if not marks:
        return [], {}
    # кластеры фидеров по X-разрывам
    clusters, cur = [], [sorted(qf_out)[0]]
    for q in sorted(qf_out)[1:]:
        if q[0] - cur[-1][0] > max(3 * step, 150):
            clusters.append(cur)
            cur = [q]
        else:
            cur.append(q)
    clusters.append(cur)
    # секция = кластер, в чьём X-диапазоне (с запасом) есть свой маркер шины
    sections, qmap = [], {}
    for cl in clusters:
        x0, x1 = cl[0][0] - 120, cl[-1][0] + 120
        m = [mk for mk in marks if x0 <= mk[0] <= x1]
        if not m:
            continue
        # имя: «с.ш.N» в окрестности маркера (±300px по X, ±60 по Y), иначе порядковое
        name = None
        for w in words:
            if "с.ш" in w[4] and any(abs(w[0] - mx) < 300 and abs(w[1] - my) < 60 for mx, my in m):
                name = w[4].strip(" ,;")
                break
        sections.append({"cluster": cl, "marker": [round(m[0][0]), round(m[0][1])], "name": name})
    if len(sections) < 2:      # одна секция = нет секционирования, слой не нужен
        return [], {}
    out = []
    for i, s in enumerate(sections, 1):
        sid = f"BS{i}"
        cl = s.pop("cluster")
        s["id"] = sid
        s["name"] = s["name"] or f"шина {i}"
        s["x_range"] = [round(cl[0][0]), round(cl[-1][0])]
        s["feeder_qfs"] = [q[2] for q in cl]
        for q in cl:
            qmap[q[2]] = sid
        # вводные аппараты секции (нижний ряд в X-диапазоне секции с запасом)
        s["incomer_qfs"] = [q[2] for q in qf_incomers
                            if cl[0][0] - 150 <= q[0] <= cl[-1][0] + 150]
        out.append(s)
    return out, qmap


def _extract_feeder_pairs(feeders):
    """Пары С1↔С2 (двухсекционные стояки/шины): коды с общей базой и суффиксами С1/С2
    (К1.1.1С1 ↔ К1.1.1С2) — одна нагрузка, питаемая с двух секций (взаиморезерв).

    Ребро позволяет ловить: несимметричное резервирование пары (разные номиналы
    автоматов/сечения кабеля у С1 и С2), пару без второй половины.
    """
    by_code = {}
    for f in feeders:
        c = f.get("circuit_code")
        if c:
            by_code.setdefault(c, f)   # при дубле привязки берём первого
    pairs = []
    for code, f1 in sorted(by_code.items()):
        m = re.match(r"(.+?)С1$", code)
        if not m:
            continue
        code2 = m.group(1) + "С2"
        f2 = by_code.get(code2)
        if not f2:
            pairs.append({"base": m.group(1), "c1": code, "c1_qf": f1.get("qf"),
                          "c2": None, "c2_qf": None,
                          "note": "пара не найдена: код С2 отсутствует/не привязан"})
            continue
        notes = []
        if f1.get("breaker_in") != f2.get("breaker_in"):
            notes.append(f"номиналы автоматов различаются: {f1.get('breaker_in')} vs {f2.get('breaker_in')}")
        if (f1.get("cable") or "").strip() != (f2.get("cable") or "").strip():
            notes.append("кабели пары различаются")
        pairs.append({"base": m.group(1), "c1": code, "c1_qf": f1.get("qf"),
                      "c2": code2, "c2_qf": f2.get("qf"),
                      "note": "; ".join(notes) or None})
    return pairs


_INPUT_DEV_TYPES = [
    ("switch", re.compile(r"^QS\d|^ВР-?\d|^ВН-?\d")),          # рубильник/выключатель нагрузки
    ("fuse", re.compile(r"^FU\d|^ППН-?\d")),                    # предохранитель
    ("spd", re.compile(r"^ОП10\d|^УЗИП|^FV\d")),                # УЗИП/разрядник
    ("relay_voltage", re.compile(r"^РН-?\d|^KV\d")),            # реле контроля напряжения
    ("indicator", re.compile(r"^HL\d")),                        # лампа индикации
]


def _extract_input_devices(words, qf_out, qf_incomers, bus_sections):
    """Устройства ВВОДНОЙ ЗОНЫ (ниже ряда отходящих QF): QS/ВР рубильники, FU/ППН
    предохранители, УЗИП (ОП10x/FV), РН/KV реле напряжения, HL индикация.

    Привязка: ближайший вводной аппарат (qf_incomers, X<150px) → incomer;
    иначе секция шин по x_range → bus_section; иначе unbound. Раньше эти
    устройства не моделировались вовсе (FU — 0 в графе, QS 18 за бортом).
    """
    if not qf_out:
        return []
    qy_out_max = max(q[1] for q in qf_out)
    devs = []
    for w in words:
        if w[1] <= qy_out_max + 40:      # только вводная зона (низ листа)
            continue
        for dtype, pat in _INPUT_DEV_TYPES:
            if pat.match(w[4]):
                devs.append({"device": w[4], "dtype": dtype, "x": round(w[0]), "y": round(w[1])})
                break
    for d in devs:
        d["incomer"] = None
        d["bus_section"] = None
        if qf_incomers:
            q = min(qf_incomers, key=lambda q: abs(q[0] - d["x"]))
            if abs(q[0] - d["x"]) < 150:
                d["incomer"] = q[2]
        if d["incomer"] is None:
            for s in bus_sections or []:
                x0, x1 = s.get("x_range") or (None, None)
                if x0 is not None and x0 - 150 <= d["x"] <= x1 + 150:
                    d["bus_section"] = s["id"]
                    break
    return devs


def _fallback_power_inputs(input_devices, bus_sections, metering):
    """Вводы из геометрии, когда паттерны подписи (ВП1/ВП2) не сработали (ГРЩ-номенклатура,
    power.inputs=[]). Каждый switch-кластер вводной зоны (QS/ВР, X-разрыв >300px) = ввод;
    ввод получает секцию по X, счётчик — из непривязанных/вводных точек учёта рядом (<250px)."""
    sw = sorted([d for d in input_devices if d["dtype"] == "switch"], key=lambda d: d["x"])
    if not sw:
        return []
    clusters, cur = [], [sw[0]]
    for d in sw[1:]:
        if d["x"] - cur[-1]["x"] > 300:
            clusters.append(cur)
            cur = [d]
        else:
            cur.append(d)
    clusters.append(cur)
    inputs = []
    for i, cl in enumerate(clusters, 1):
        cx = sum(d["x"] for d in cl) / len(cl)
        bs = next((s for s in bus_sections or []
                   if (s.get("x_range") or [9e9, -9e9])[0] - 200 <= cx <= (s.get("x_range") or [9e9, -9e9])[1] + 200), None)
        meter = None
        for p in metering or []:
            if p.get("kind") in ("incomer", "unbound") and p.get("meter"):
                meter = ", ".join(p["meter"])
                break
        inputs.append({
            "id": f"Ввод {i} (гео)", "vvod": None,
            "switch": ", ".join(sorted({d["device"] for d in cl})),
            "incomer": None, "meter": meter,
            "feeds": (bs or {}).get("panels") or ([] if bs is None else [bs["name"]]),
            "bus_section": (bs or {}).get("id"),
            "i_rab": None, "i_avar": None,
            "source": "geometry_fallback",
        })
    return inputs


_METER_DEV_RE = re.compile(r"^(TA|ТА)\d|^Wh$|^НАРТИС|^Меркур|^МТ-72")


def _extract_metering(words, qf_out, qf_incomers):
    """Цепь коммерческого/технического учёта: TA (трансформаторы тока), Wh (счётчик),
    модель счётчика (НАРТИС/Меркурий) и мультиметр МТ-72D → привязка к колонке фидера
    или ввода по X (тот же колоночный приём, что код↔QF).

    На ГРЩ учёт ПОФИДЕРНЫЙ: гребёнка «TA…»+«Wh» стоит над каждой отходящей линией —
    без этого слоя вся цепь учёта терялась (TA за бортом 60/61 токенов).

    Возвращает (points, qf→point) — точки учёта {kind: feeder|incomer, qf, ta[], wh, meter[]}.
    """
    devs = [(w[0], w[1], w[4]) for w in words if _METER_DEV_RE.match(w[4])]
    if not devs or not qf_out:
        return [], {}
    xs = sorted(q[0] for q in qf_out)
    step = _median([xs[i + 1] - xs[i] for i in range(len(xs) - 1)]) if len(xs) > 1 else 60.0
    half = 0.55 * (step or 60.0)
    points = {}
    for dx_, dy_, t in devs:
        # ближайшая колонка: отходящий QF (порог полшага) или вводной аппарат (порог 130px)
        best = None
        if qf_out:
            q = min(qf_out, key=lambda q: abs(q[0] - dx_))
            if abs(q[0] - dx_) < half:
                best = ("feeder", q[2], abs(q[0] - dx_))
        if qf_incomers:
            q = min(qf_incomers, key=lambda q: abs(q[0] - dx_))
            if abs(q[0] - dx_) < 130 and (best is None or abs(q[0] - dx_) < best[2]):
                best = ("incomer", q[2], abs(q[0] - dx_))
        key = (best[0], best[1]) if best else ("unbound", f"x{int(dx_ // 200)}")
        pt = points.setdefault(key, {"kind": key[0], "qf": key[1] if best else None,
                                     "ta": [], "wh": 0, "meter": []})
        if re.match(r"^(TA|ТА)\d", t):
            pt["ta"].append(t)
        elif t == "Wh":
            pt["wh"] += 1
        else:
            pt["meter"].append(t)
    out = []
    qmap = {}
    for pt in points.values():
        pt["ta"] = sorted(set(pt["ta"]))
        pt["meter"] = sorted(set(pt["meter"]))
        if not (pt["ta"] or pt["wh"] or pt["meter"]):
            continue
        out.append(pt)
        if pt["kind"] == "feeder" and pt["qf"]:
            qmap[pt["qf"]] = pt
    return out, qmap


def _fallback_bind_code(qn, qx, qy, consumer, words, params, qf_out, assigned):
    """Дожать привязку для колонки, оставшейся без кода. Код мог не попасть в primary-ряд
    (секционные «С2»-линии: К3.1.2С2 на dy≈-72) или лежать В ПОДПИСИ потребителя («Щит М4-1.1»
    на ГРЩ-листах). Берём код ТОЛЬКО из params, не занятый другой линией, и однозначно
    относящийся к ЭТОЙ колонке (в подписи ИЛИ единственный в тесной X-колонке с ближайшей QF=эта).
    Консервативно — чтобы не утащить код соседа. Возвращает код|None.
    """
    pkeys = [k for k in params if k not in assigned]
    if not pkeys:
        return None
    # 1) код в подписи потребителя. Длинные ключи раньше (М4-1.10 проверяется до М4-1.1 —
    #    иначе префикс украл бы совпадение).
    if consumer:
        for k in sorted(pkeys, key=len, reverse=True):
            if k in consumer:
                return k
    # 2) единственный params-токен в тесной X-колонке, чья ближайшая QF — именно эта.
    xs = sorted(q[0] for q in qf_out)
    spacing = (_median([xs[i + 1] - xs[i] for i in range(len(xs) - 1)]) if len(xs) > 1 else 60.0) or 60.0
    half = 0.42 * spacing
    pk = set(pkeys)
    found = set()
    for w in words:
        if w[4] in pk and abs(w[0] - qx) < half and -430 < (w[1] - qy) < 20:
            if min(qf_out, key=lambda q: abs(q[0] - w[0]))[2] == qn:
                found.add(w[4])
    return found.pop() if len(found) == 1 else None


def build_singleline_graph(pdf_path: Path, vector_text: str, *, panel_hint: str = "ВРУ",
                           bbox_norm: Optional[list] = None,
                           polygon_norm: Optional[list] = None) -> Optional[dict]:
    """Построить граф однолинейной схемы. None — если не feeder-схема / нет геометрии.

    Геометрия строится ТОЛЬКО по словам внутри области выделения блока (если включён
    VECTOGRAF_BBOX_CLIP_ENABLED), чтобы чужой текст листа не протекал в топологию:
    - ``polygon_norm`` (polygon_points_norm блока — список [x,y]-вершин) → point-in-polygon,
      точный контур (предпочтительно для полигональных блоков);
    - иначе ``bbox_norm`` (coords_norm [x0,y0,x1,y1]) → осевой прямоугольник.
    """
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
        # Геометрию (words с координатами) режем по области выделения блока (bbox_norm), чтобы
        # чужой текст листа — соседняя схема / таблица / штамп — НЕ протекал в топологию. Раньше
        # брали слова со ВСЕЙ страницы (координат в vector_text нет). Полигон точнее прямоугольника
        # (отсекает чужой контент в «вырезах» контура). fail-soft: без области или при подозрительном
        # клипе — весь лист (прежнее поведение).
        page_full_text = vector_text
        if _bbox_clip_enabled():
            _pw, _ph = float(pg.rect.width), float(pg.rect.height)
            if polygon_norm:
                words = _clip_words_to_polygon(words, polygon_norm, _pw, _ph)
            elif bbox_norm:
                words = _clip_words_to_bbox(words, bbox_norm, _pw, _ph)
            # Правило «вектограф = только текст внутри полигона»: ограничиваем и ТЕКСТ-разделы, не
            # только геометрию. Фильтруем строки vector_text по принадлежности области блока
            # (VECTOGRAF_POLYGON_TEXT_ONLY_ENABLED). Примечания/ТТ в «вырезе» контура уходят (у них
            # СВОИ text-блоки + MD → Stage 01, не теряются), фидеры и расчёты панелей (внутри контура)
            # остаются; фидеры ПЕРЕ-ПАРСИМ из отфильтрованного текста. fail-soft: если фильтр рушит
            # фидеры (<3) — оставляем исходный текст/base (прежнее поведение).
            if (polygon_norm or bbox_norm) and _polygon_text_only_enabled():
                region_text = _filter_text_lines_to_region(vector_text, words)
                _base2 = structure_singleline_text(region_text)
                if _base2 and _base2.get("feeder_total", 0) >= 3:
                    base = _base2
                    params = {f["circuit_code"]: f for s in base["bus_sections"] for f in s["feeders"]}
                    page_full_text = region_text
        # Текст-разделы (питание, панели-расчёты, связи, служебные элементы, ТТ, примечания, заголовок)
        # берём из текста блока (полигон-отфильтрованного, если правило включено). Фолбэк — весь лист.
        page_full_text = page_full_text or pg.get_text()
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

    qf_all = [(w[0], w[1], w[4]) for w in words if _QF_RE.fullmatch(w[4])]
    if len(qf_all) < 3:
        return None
    # Суб-панель из 3-сегментных QF (QF5.1.x → РП5.1) признаём только при ≥2 членах. ОДИНОЧНЫЙ
    # 3-сегментный QF (вложенный, напр. QF3.10.1 на ВРУ-К1.1) суб-панель НЕ образует — но и НЕ
    # выбрасываем его: пере-родителяем в ВЕРХНЮЮ панель (QF3.10.1 → РП3), чтобы у него была своя
    # область и свой код по колонке. Раньше выброс приводил к тому, что его код «утекал» соседу
    # (напр. К1.1.6-3 попадал в колонку QF3.9 → GEOMETRY_CONFLICT). Множество таких меток —
    # lone_subpanel_qf; их panel_key переопределяет pref() (ниже) на первый сегмент.
    _key_count = collections.Counter(_qf_panel_key(q[2])[0] for q in qf_all)
    lone_subpanel_qf = {q[2] for q in qf_all
                        if "." in _qf_panel_key(q[2])[0] and _key_count[_qf_panel_key(q[2])[0]] < 2}
    # исходящие vs вводные по Y (вводные — нижний ряд)
    ys = sorted(q[1] for q in qf_all)
    y_split = ys[0] + (ys[-1] - ys[0]) * 0.6 if ys[-1] - ys[0] > 60 else ys[-1] + 1
    qf_out = sorted([q for q in qf_all if q[1] <= y_split])
    qf_incomers = sorted([q for q in qf_all if q[1] > y_split])
    n_incomers = len(qf_incomers)
    # Дедуп повторных QF-меток среди отходящих: одна метка может встретиться дважды (повтор ниже
    # по листу / фрагмент). Дубль, попавший в X-колонку соседа, ВОРУЕТ его код (двухсекц. шина
    # ЭМ-К3: дубль QF2.1 крал К3.1.2С2 у QF2.2). Оставляем ВЕРХНЮЮ метку (min Y — ближе к ряду
    # кодов над QF). Активируется только при реальных дублях; на листах с уник. метками — no-op.
    if len({q[2] for q in qf_out}) != len(qf_out):
        _keep = {}
        for q in sorted(qf_out, key=lambda t: t[1]):
            _keep.setdefault(q[2], q)
        qf_out = sorted(_keep.values())
    # Код цепи: искать не по хардкод-префиксу, а по ключам, которые СТРУКТУРЕР уже распознал
    # (params) — тогда любая схема нумерации проекта подходит (К2.2.24, К5.3п, М-1.2, М4-4.3,
    # НП6.РП1-1, 1Мп-5.24, НС.А-4…). union с регэкспом `^К\d+\.\d` подстраховывает листы, где
    # структурер разобрал мало параметров, а на чертеже К-кодов больше (напр. ЭО-К3). Прежний
    # `^К\d+\.\d+\.` находил только трёхуровневые К-коды → 0% привязки на всех иных нумерациях.
    _pkeys = set(params)
    codes = [(w[0], w[1], w[4]) for w in words
             if w[4] in _pkeys or (re.match(r"^К\d+\.\d", w[4]) and "кВт" not in w[4])]
    BA = coll(r"^ВА")
    KA = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"\d+кА", w[4])]
    AMP = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"\d+А", w[4])]
    POLE = [(w[0], w[1], w[4]) for w in words if re.fullmatch(r"[123]Р", w[4])]
    RES = [(w[0], w[1]) for w in words if "езерв" in w[4]]
    PS = [(w[0], w[1]) for w in words if re.fullmatch(r"ПС", w[4])]
    ASUD = [(w[0], w[1]) for w in words if "АСУД" in w[4]]

    def pref(qn):
        # Одиночный 3-сегментный QF не образует суб-панель → относим к верхней панели (QF3.10.1 → «3»).
        if qn in lone_subpanel_qf:
            return qn[2:].split(".")[0]
        return _qf_panel_key(qn)[0]

    geo = {}
    for qx, qy, qn in qf_out:
        ba_xy = _near_xy(qx, qy, BA, 34, -95, 110)
        geo[qn] = {
            "ba": ba_xy[2] if ba_xy else None,
            "ka": _near(qx, qy, KA, 44, -30, 100),
            "amp": _near(qx, qy, AMP, 44, -30, 100),
            "pole": _pole_for_breaker(qx, qy, ba_xy, POLE),
            "reserve": any(abs(x - qx) < 38 and -320 < (y - qy) < 80 for x, y in RES),
            "control": ([t for t in (["ПС"] if any(abs(x - qx) < 42 and -320 < (y - qy) < 130 for x, y in PS) else [])]
                        + (["АСУД"] if any(abs(x - qx) < 42 and -320 < (y - qy) < 130 for x, y in ASUD) else [])),
        }

    # ── Привязка кода к QF: ПЕРВИЧНО по геометрии колонки, монотонная — только fallback ──
    # Текстовый слой PDF мешает подписи (строка «QF3.8 QF3.9 QF3.10…» + колонки потребителей),
    # поэтому порядок текста ненадёжен. Колоночная привязка (offset-corrected nearest column)
    # ставит код в ТУ QF, в чьей x-колонке он реально находится: QF без отходящего кода
    # (QF3.1 на ВРУ-К1.2) остаётся непривязанным, а не «сдвигает» весь ряд.
    assign = {}
    bind_method = {}
    bind_conflicts = {}
    primary_tokens = []          # все PRIMARY код-токены (occurrences, для честного покрытия)
    secondary_circuits = []      # вторичные цепи «ад»/«ан» отдельным слоем (не primary feeder code)
    panel_keys = sorted(set(pref(q[2]) for q in qf_out))
    panel_name_map = {pk: _panel_name(pk, page_full_text) for pk in panel_keys}
    dup = collections.Counter(q[2] for q in qf_out)
    for p in panel_keys:
        pq = sorted([q for q in qf_out if pref(q[2]) == p])
        xs = [q[0] for q in pq]
        qy_med = _median([q[1] for q in pq])
        pcodes = [c for c in codes if min(xs) - 50 <= c[0] <= max(xs) + 50]
        # Y-aware: только PRIMARY-ряд кодов участвует в привязке; SECONDARY (ад/ан) — отдельно
        prim, sec = _split_codes_by_y_rows(qy_med, pcodes)
        primary_tokens += [c[2] for c in prim]
        for cx, cy, code in sec:
            near = min(pq, key=lambda q: abs(q[0] - cx))
            secondary_circuits.append({"code": code, "panel": panel_name_map[p],
                                       "panel_key": p, "near_qf": near[2]})
        pc = sorted(prim)
        col_assign, conflicts = _bind_codes_columnwise(pq, pc)
        # монотонная привязка (резерв пропускает код) — для cross-check и fallback
        mono = {}
        ci = 0
        for qx, qy, qn in pq:
            if geo[qn]["reserve"]:
                mono[qn] = None
            elif ci < len(pc):
                mono[qn] = pc[ci][2]; ci += 1
            else:
                mono[qn] = None
        col_links = sum(1 for v in col_assign.values() if v)
        col_reliable = bool(pc) and col_links >= max(1, int(0.6 * len(pc)))
        for qx, qy, qn in pq:
            if geo[qn]["reserve"]:
                assign[qn] = None
                bind_method[qn] = "reserve"
                continue
            cv, mv = col_assign.get(qn), mono.get(qn)
            if col_reliable:
                assign[qn] = cv               # доверяем геометрии колонки
                bind_method[qn] = "column" if cv else "none"
            else:
                assign[qn] = mv               # геометрия колонки ненадёжна → fallback
                bind_method[qn] = "monotonic_fallback" if mv else "none"
            notes = list(conflicts.get(qn, []))
            # column↔monotonic расходятся, но привязка по колонке у границы → пометить, не сдвигать
            if col_reliable and cv and mv and cv != mv and notes:
                notes.append(f"монотонная привязка дала {mv}; принята колоночная {cv}")
            if notes:
                bind_conflicts[qn] = notes

    # Секции шин (ребро фидер→секция): fail-soft, пусто если секция одна
    try:
        bus_sections, _bs_map = _extract_bus_sections(words, qf_out, qf_incomers)
    except Exception:
        bus_sections, _bs_map = [], {}
    # Цепь учёта (ребро линия/ввод → TA → Wh): fail-soft
    try:
        metering, _mt_map = _extract_metering(words, qf_out, qf_incomers)
    except Exception:
        metering, _mt_map = [], {}

    feeders = []
    qf_xs = sorted(q[0] for q in qf_out)
    _qf_geom = {}   # qn → (qx, qy, next_x) для пост-обработки (control_edges)
    assigned_codes = {v for v in assign.values() if v}   # занятые коды — не переиспользовать в fallback
    for qx, qy, qn in qf_out:
        g = geo[qn]
        code = assign.get(qn)
        p = params.get(code) if code else None
        nx = next((x for x in qf_xs if x > qx + 1), qx + 70)
        _qf_geom.setdefault(qn, (qx, qy, nx))
        additional_devices = _extract_additional_devices(qx, qy, nx, words)
        consumer_geo = _extract_consumer_geo(
            qx, qy, nx, words,
            [(p or {}).get("P_inst_kw"), (p or {}).get("Kc"), (p or {}).get("cosphi"),
             (p or {}).get("P_calc_kw"), (p or {}).get("I_a")])
        if consumer_geo == "Резерв":
            consumer_geo = "Резерв (свободная ячейка)"
        consumer = consumer_geo or (p.get("consumer") if p else None) \
            or ("Резерв (свободная ячейка)" if g["reserve"] else None)
        # Дожать привязку недобитой колонки (секционные «С2» / код в подписи щита) — строго из params.
        if code is None and not g["reserve"]:
            fb = _fallback_bind_code(qn, qx, qy, consumer, words, params, qf_out, assigned_codes)
            if fb:
                code = fb
                p = params.get(fb)
                assigned_codes.add(fb)
                if not consumer:
                    consumer = (p.get("consumer") if p else None) or consumer
        # Статус линии. Бывший «ambiguous» разделён на три честных случая:
        #  - no_code:    реальная линия (есть потребитель/кабельные маркеры в колонке),
        #                но БЕЗ построчного кода в спецификации (ПС-системы, щиты, ТР через КМ);
        #  - structural: секционный/вводной автомат — в колонке только сам аппарат
        #                (ВА-… + номиналы), ни кода, ни кабеля, ни потребителя;
        #  - ambiguous:  остальное — настоящая неопределённость (requires_review).
        if g["reserve"]:
            status = "reserve"
        elif p:
            status = "active"
        else:
            _cons_letters = sum(ch.isalpha() for ch in (consumer or ""))
            # Слова текст-колонки часто НАЧИНАЮТСЯ левее символа QF (dx до −46: «Лоток-40м;»
            # у QF4.15) — полоса от qx−8 их теряла и реальная линия падала в ambiguous.
            _col_txt = " ".join(w[4] for w in words
                                if qx - 46 <= w[0] < min(nx, qx + 58) and qy - 330 < w[1] < qy + 40)
            _line_markers = re.search(r"Лоток|Пг\.|ППГнг|ВВГ|\dм;|\dм\b|Iкз|систем", _col_txt)
            if _cons_letters >= 4 or _line_markers:
                status = "no_code"
            elif not _cons_letters:
                status = "structural"
            else:
                status = "ambiguous"
        review = []
        if status == "ambiguous":
            review.append("колонка без сопоставленного кода — requires_review "
                          "(визуально отдельный аппарат, отходящий код не привязан)")
        elif status == "no_code":
            review.append("линия без построчного кода в спецификации (потребитель/кабель "
                          "извлечены; привязывать нечего — это не ошибка)")
        elif status == "structural":
            review.append("секционный/вводной автомат (только аппарат в колонке, без "
                          "отходящей линии)")
        review.extend(bind_conflicts.get(qn, []))
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
        # bbox/полигон колонки «одна линия» (page-normalized).
        # ВАЖНО: текст-колонка СМЕЩЕНА ВПРАВО от символа QF (~+12px) и шире межсимвольного
        # спейсинга. Если резать по серединам между символами — правый текст (кабель/трасса)
        # уходит соседнему фидеру. Поэтому полоса = «ОТ символа ДО следующего символа»
        # (символ+автомат слева, текст справа), cap по типовой ширине.
        next_x = min([x for x in qf_xs if x > qx + 1], default=qx + 64)
        spacing = next_x - qx
        left = qx - 8
        # Граница ТЕКСТА линии = координата СЛЕДУЮЩЕГО символа QF (весь текст линии лежит до
        # него; текст соседа начинается ещё правее, +12px). Раньше брал next_x-8 — это резало
        # правую трассу («Лоток»), когда она дотягивалась близко к соседнему символу → уходила соседу.
        tright = next_x if spacing < 90 else (qx + 58)
        right = tright
        colw = [w for w in words if left <= w[0] < right and qy - 280 < w[1] < qy + 30]
        bbox_page = None
        polygon_page = None
        polygons_page = None
        if page_w and page_h:
            y_top = min((w[1] for w in colw), default=qy - 230)
            y_bot = qy + 60
            bbox_page = [round(left / page_w, 5), round(y_top / page_h, 5),
                         round(right / page_w, 5), round(y_bot / page_h, 5)]
            # Область линии — по ЗНАЧЕНИЯМ из графа (а не геометрической полосой): берём слова PDF,
            # чей текст совпадает с известными данными линии (потребитель/код/автомат/кабель/трасса/
            # числа), в пределах колонки → обводим именно их (минимальная траектория по данным).
            # ОДНА фигура «I-контур»: текст-блок (сверху) + перешеек-провод + автомат-блок (снизу),
            # без выпуклого объединения (оно срезало бы углы текста — автомат уже и левее текста).
            def _nrm(t):
                return t.strip(" .,;:()[]«»\"'").replace("ё", "е").replace("Ё", "Е").lower()
            txt_targets, brk_targets = set(), set()
            for s in (consumer, code, (p or {}).get("cable"), (p or {}).get("routing")):
                if s:
                    for tok in re.split(r"\s+", str(s)):
                        n = _nrm(tok)
                        if len(n) >= 3:
                            txt_targets.add(n)
            for val, unit in (((p or {}).get("length_m"), "м"), ((p or {}).get("I_a"), "а"),
                              ((p or {}).get("Ikz_ka"), "ка"), ((p or {}).get("P_calc_kw"), "квт")):
                if isinstance(val, (int, float)):
                    txt_targets.add(_nrm(f"{val:g}{unit}"))
            for v in (g.get("ba"), g.get("ka"), g.get("amp"), g.get("pole")):
                if v:
                    brk_targets.add(_nrm(str(v)))
            brk_targets.add(_nrm(qn))

            def _hits(w, targets):
                nw = _nrm(w[4])
                return bool(nw) and any(nw == t or (len(t) >= 4 and t in nw) or (len(nw) >= 4 and nw in t)
                                        for t in targets)
            tw = [w for w in words if qx - 2 <= (w[0] + w[2]) / 2 < tright
                  and qy - 330 < w[1] < qy - 40 and _hits(w, txt_targets)]
            # Подпись автомата — ДВА столбца: левый (ВА-300/15кА/метка ≈ qx+12..+17) и правый
            # (поль/номинал «1Р», «10А» ≈ qx+35..+37). Жёсткое qx+34 срезало правый столбец у
            # фидеров, где символ qx сдвинут левее подписи (напр. с контактором МК103). Правую
            # границу расширяем до qx+46, но капим по соседнему символу (next_x-8), чтобы не
            # захватить подпись соседа (она начинается на next_x+12..+15).
            brk_right = min(qx + 46, next_x - 8)
            brw = [w for w in words if qx - 22 <= (w[0] + w[2]) / 2 <= brk_right
                   and -2 < (w[1] - qy) < 45 and _hits(w, brk_targets)]
            def _bbox(ws):
                return ((min(w[0] for w in ws), min(w[1] for w in ws),
                         max(w[2] for w in ws), max(w[3] for w in ws)) if ws else None)
            tb, bb = _bbox(tw), _bbox(brw)
            poly = None
            if tb and bb:
                tx0, ty0, tx1, ty1 = tb
                bx0, by0, bx1, by1 = bb
                ox0, ox1 = max(tx0, bx0), min(tx1, bx1)   # X-перекрытие = перешеек
                if ox0 >= ox1:                            # нет перекрытия → перешеек у символа
                    ox0, ox1 = qx - 4, qx + 4
                raw = [(tx0, ty0), (tx1, ty0), (tx1, ty1), (ox1, ty1), (ox1, by0),
                       (bx1, by0), (bx1, by1), (bx0, by1), (bx0, by0), (ox0, by0), (ox0, ty1), (tx0, ty1)]
                poly = [pt for i, pt in enumerate(raw) if pt != raw[i - 1]]   # без дублей
            elif tb or bb:
                x0, y0, x1, y1 = tb or bb
                poly = [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]
            if poly:
                polygon_page = [[round(x / page_w, 5), round(y / page_h, 5)] for x, y in poly]
                polygons_page = [polygon_page]            # одна фигура линии
        feeders.append({
            "qf": qn, "panel": panel_name_map.get(pref(qn), panel_hint),
            "consumer": consumer, "location": loc, "circuit_code": code,
            "bbox_page": bbox_page, "polygon_page": polygon_page, "polygons_page": polygons_page,
            "breaker_type": g["ba"], "breaker_poles": g["pole"],
            "breaker_icn": g["ka"], "breaker_in": g["amp"],
            "P_inst_kw": (p or {}).get("P_inst_kw"), "Kc": (p or {}).get("Kc"),
            "cosphi": (p or {}).get("cosphi"), "P_calc_kw": (p or {}).get("P_calc_kw"),
            "I_a": (p or {}).get("I_a"), "cable": (p or {}).get("cable"),
            "length_m": (p or {}).get("length_m"), "voltage_drop_pct": (p or {}).get("voltage_drop_pct"),
            "Ikz_ka": (p or {}).get("Ikz_ka"), "routing": (p or {}).get("routing"),
            "phase": (p or {}).get("phase"), "control": g["control"],
            "additional_devices": additional_devices,
            "binding_method": bind_method.get(qn), "status": status, "review": review,
            "bus_section": _bs_map.get(qn),
            "metering": ({"ta": _mt_map[qn]["ta"], "wh": _mt_map[qn]["wh"],
                          "meter": _mt_map[qn]["meter"]} if qn in _mt_map else None),
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
    # панель → секция шин (по большинству её фидеров); секции → имена панелей
    if bus_sections:
        for p in panels:
            cnt = collections.Counter(f.get("bus_section") for f in p["feeders"] if f.get("bus_section"))
            p["bus_section"] = cnt.most_common(1)[0][0] if cnt else None
        for s in bus_sections:
            s["panels"] = sorted({p["name"] for p in panels if p.get("bus_section") == s["id"]})

    linked = [f for f in feeders if f.get("circuit_code")]
    reserve = [f for f in feeders if f["status"] == "reserve"]
    ambiguous = [f for f in feeders if f["status"] == "ambiguous"]
    no_code = [f for f in feeders if f["status"] == "no_code"]
    structural = [f for f in feeders if f["status"] == "structural"]

    # ── Рёбра roadmap №3-9 (все fail-soft) ──
    try:
        feeder_pairs = _extract_feeder_pairs(feeders)              # №6: пары С1↔С2
    except Exception:
        feeder_pairs = []
    try:
        input_devices = _extract_input_devices(words, qf_out, qf_incomers, bus_sections)  # №4/5/8/9
    except Exception:
        input_devices = []
    # №7: сигнал (ПС/АСУД) → коммутационный аппарат (КМ/МК103) → линия; action из текста колонки
    control_edges = []
    try:
        for f in feeders:
            if not f.get("control"):
                continue
            qg = _qf_geom.get(f["qf"])
            action = None
            if qg:
                qx0, qy0, nx0 = qg
                col = " ".join(w[4] for w in words
                               if qx0 - 46 <= w[0] < nx0 and qy0 - 330 < w[1] < qy0 + 40)
                if re.search(r"откл", col, re.I):
                    action = "отключение при пожаре" if "ПС" in f["control"] else "отключение по сигналу"
            dev = next((d for d in (f.get("additional_devices") or [])
                        if re.search(r"КМ|МК103", d)), None)
            for sig in f["control"]:
                control_edges.append({"signal": sig, "device": dev, "qf": f["qf"],
                                      "circuit_code": f.get("circuit_code"),
                                      "consumer": f.get("consumer"), "action": action})
    except Exception:
        control_edges = []
    review_items = [{"qf": f["qf"], "code": f.get("circuit_code"), "notes": f["review"]}
                    for f in feeders if f["review"]]
    bv = base["validation"]
    try:
        power = _extract_power(words, page_full_text, qf_incomers)
    except Exception:
        power = None
    # №3: вводы из геометрии, когда подписи ВП1/ВП2 не сработали (ГРЩ-номенклатура)
    try:
        if input_devices and (power is None or not (power or {}).get("inputs")):
            _fb = _fallback_power_inputs(input_devices, bus_sections, metering)
            if _fb:
                power = power or {"external_source": None, "avr": None}
                power["inputs"] = _fb
                power["inputs_source"] = "geometry_fallback"
    except Exception:
        pass
    # ── Дополнительные слои графа (best-effort, fail-soft по каждому) ──
    def _safe(fn, *a, default=None):
        try:
            return fn(*a)
        except Exception:
            return default
    panel_calcs = _safe(_extract_panel_calcs, page_full_text, default=[])
    input_calcs = _safe(_extract_input_calcs, page_full_text, default=[])
    tt_check = _safe(_extract_tt_check, page_full_text, default=[])
    notes = _safe(_extract_notes, page_full_text, default=[])
    service_elements = _safe(_extract_service_elements, page_full_text, default=[])
    hierarchy = _safe(_extract_hierarchy, page_full_text, power, panels, default={}) or {}
    source = _safe(_extract_title_source, page_full_text, pdf_path, pidx, panel_hint, default={}) or {}

    geometry_conflicts = sum(
        1 for f in feeders
        for n in (f.get("review") or []) if "GEOMETRY_CONFLICT" in n)
    warnings = []
    if ambiguous:
        warnings.append(f"{len(ambiguous)} QF без сопоставленного кода (requires_review)")
    if geometry_conflicts:
        warnings.append(f"{geometry_conflicts} геометрических конфликтов привязки QF↔код")
    dups = [q for q, c in dup.items() if c > 1]
    if dups:
        warnings.append("повтор обозначений QF: " + ", ".join(sorted(dups)))
    for tr in tt_check:
        if tr.get("review"):
            warnings.append(f"ТТ: {tr['panel']} — расхождение обозначения, см. комментарий")
    if power_present_anomaly := any(
            (m.get("Pu") == "----" or m.get("Kc") == "#ДЕЛ/0!")
            for pc_ in panel_calcs for m in pc_.get("modes", [])):
        warnings.append("в расчётных режимах есть «Ру=----» / «Кс=#ДЕЛ/0!» — сохранено как в ПД")

    # ── Честные счётчики покрытия (occurrences vs unique; дубли и пропуски НЕ маскируем) ──
    param_list = [f["circuit_code"] for s in base["bus_sections"] for f in s["feeders"]]
    param_occ = collections.Counter(param_list)
    bound_codes = [f["circuit_code"] for f in linked]
    bound_counter = collections.Counter(bound_codes)
    duplicate_param_codes = sorted(c for c, n in param_occ.items() if n > 1)
    duplicate_bindings = sorted(c for c, n in bound_counter.items() if n > 1)
    unbound_param_codes = sorted(c for c in param_occ if c not in bound_counter)
    codes_linked = len(set(bound_codes))              # backward-compat (unique)
    # Пропущенная панель = КЛАСТЕР непривязанных PRIMARY-кодов с общим суб-префиксом (К<a>.<b>.<c>…),
    # т.е. на листе есть коды, но нет QF-колонок под них (как РП5.1 на 7TLY до фикса). По ОРФАНАМ,
    # а не по текстовым ссылкам — иначе начало-лист ложно «теряет» РП5/РП5.1 из таблицы ТТ.
    _bound_set = set(bound_codes)
    _orphan_primary = [c for c in primary_tokens if c not in _bound_set]
    _sub = collections.Counter()
    for c in _orphan_primary:
        if re.match(r"К\d+\.\d+\.\d+\.\d+", c):       # 4-числовой код = суб-панель РПx.y (К a.b.c.d)
            _sub[re.match(r"(К\d+\.\d+\.\d+)", c).group(1)] += 1
    missing_panel_warnings = [
        f"непривязанная суб-панель: коды {pre}.*а ×{n}, нет QF-колонок"
        for pre, n in _sub.items() if n >= 2]
    if missing_panel_warnings:
        warnings.append("пропущены панели: " + "; ".join(missing_panel_warnings))
    if duplicate_param_codes:
        warnings.append("дубль кода в ПД (не исправлено молча): " + ", ".join(duplicate_param_codes))
    if duplicate_bindings:
        warnings.append("один код на нескольких QF (дубль в вектор-слое ПД): "
                        + ", ".join(duplicate_bindings))
    codes_linked_occurrences = len(bound_codes)
    codes_total_occurrences = len(primary_tokens)
    coverage_gap = codes_linked_occurrences < codes_total_occurrences
    if coverage_gap:
        warnings.append(f"покрытие неполное: привязано {codes_linked_occurrences} из "
                        f"{codes_total_occurrences} primary-кодов")

    confidence = round(codes_linked / max(len(param_occ), 1), 3)
    status = "ok" if not (ambiguous or geometry_conflicts or coverage_gap
                          or missing_panel_warnings or duplicate_param_codes
                          or duplicate_bindings) else "needs_review"

    return {
        "panel": panel_hint,
        "type": "single_line_calc_diagram",
        "source_page_index": pidx,
        "source": source,
        "title": source.get("title") or panel_hint,
        "feeders_total": len(feeders),
        "incomers": n_incomers,
        "power": power,
        "hierarchy": hierarchy,
        "edges": hierarchy.get("edges", []),
        "bus_sections": bus_sections,
        "metering": metering,
        "feeder_pairs": feeder_pairs,
        "input_devices": input_devices,
        "control_edges": control_edges,
        "panels": panels,
        "panel_calculations": panel_calcs,
        "input_calculations": input_calcs,
        "tt_check_table": tt_check,
        "service_elements": service_elements,
        "secondary_circuits": secondary_circuits,
        "notes": notes,
        "validation": {
            "active": len(linked), "reserve": len(reserve), "ambiguous": len(ambiguous),
            "no_code": len(no_code), "structural": len(structural),
            "breaker_bound": f"{sum(1 for f in feeders if f.get('breaker_type'))}/{len(feeders)}",
            "power_rate": bv.get("power_rate"), "current_rate": bv.get("current_rate"),
            "codes_total": len(param_occ), "codes_linked": codes_linked,
            "geometry_conflicts": geometry_conflicts,
            # ── честные счётчики (occurrences vs unique; дубли/пропуски явно) ──
            "codes_total_occurrences": codes_total_occurrences,
            "codes_linked_occurrences": codes_linked_occurrences,
            "codes_total_unique": len(param_occ),
            "codes_linked_unique": codes_linked,
            "unbound_param_codes": unbound_param_codes,
            "duplicate_param_codes": duplicate_param_codes,
            "duplicate_bindings": duplicate_bindings,
            "secondary_codes_total": len(secondary_circuits),
            "panels_detected": [p["name"] for p in panels],
            "missing_panel_warnings": missing_panel_warnings,
        },
        "warnings": warnings,
        "confidence": confidence,
        "status": status,
        "review": review_items,
        "feeders_flat": feeders,
    }


# ── Rich-renderer: полный Markdown графа в формате эталона ──────────────────────────────

def _md_cell(v) -> str:
    if v is None:
        return "—"
    s = str(v).replace("|", "/").replace("\n", " ").strip()
    return s or "—"


def _fmt_breaker(f: dict) -> str:
    parts = []
    if f.get("breaker_type"):
        bt = f["breaker_type"]
        if f.get("breaker_poles"):
            bt += f" {f['breaker_poles']}"
        parts.append(bt)
    if f.get("breaker_icn"):
        parts.append(f"Icn={f['breaker_icn']}")
    if f.get("breaker_in"):
        parts.append(f"In={f['breaker_in']}")
    return "; ".join(parts) or "—"


def _fmt_load(f: dict) -> str:
    if f.get("P_calc_kw") is None:
        return "—"
    seg = [f"Ру={f.get('P_inst_kw')}кВт", f"Кс={f.get('Kc')}",
           f"Cos f={f.get('cosphi')}", f"Рр={f.get('P_calc_kw')}кВт", f"Ip={f.get('I_a')}А"]
    return "; ".join(seg)


def _fmt_cable(f: dict) -> str:
    if not any(f.get(k) for k in ("length_m", "cable", "Ikz_ka", "routing")):
        return "—"
    seg = []
    if f.get("length_m") is not None:
        seg.append(f"L={f['length_m']}м")
    if f.get("voltage_drop_pct") is not None:
        seg.append(f"ΔU={f['voltage_drop_pct']}%")
    if f.get("cable"):
        seg.append(str(f["cable"]))
    if f.get("Ikz_ka") is not None:
        seg.append(f"Iкз(1)={f['Ikz_ka']}кА")
    if f.get("routing"):
        seg.append(str(f["routing"]))
    return "; ".join(seg) or "—"


def _fmt_modes(p: dict) -> str:
    out = []
    for m in p.get("modes", []):
        out.append(f"{m['mode']}: Ру={m.get('Pu')}кВт; Кс={m.get('Kc')}; Cos f={m.get('cosphi')}; "
                   f"Рр={m.get('Pr')}кВт; Sр={m.get('Sr')}кВА; Ip={m.get('Ip')}А")
    return " // ".join(out) or "—"


def render_graph_etalon_markdown(graph: dict) -> str:
    """Полная текстовая разметка однолинейной схемы в формате эталона (8 разделов).

    В отличие от компактного `render_graph_for_prompt` (для промпта Stage 02), этот renderer
    выдаёт человекочитаемый Markdown, близкий к `claude_etalon_vru_k1_2_v2_qf3_fix.md`:
    дерево питания, карточки панелей, связи, отходящие линии, реестр служебных элементов,
    таблица ТТ, примечания, блок validation/requires_review.
    """
    if not graph:
        return ""
    L = []
    src = graph.get("source") or {}
    title = graph.get("title") or graph.get("panel") or "схема"
    v = graph.get("validation", {})

    L.append(f"# Эталонная текстовая разметка однолинейной схемы {title}")
    L.append("")
    if src.get("sheet_name"):
        L.append(f"**Лист:** {src['sheet_name']}")
    pi = graph.get("source_page_index")
    pdf_file = src.get("pdf_file") or "?"
    page_disp = f"стр. PDF {pi + 1}" if isinstance(pi, int) else "стр. ?"
    L.append(f"**Источник:** {pdf_file}, {page_disp} (source_page_index={pi})")
    L.append("**Тип схемы:** однолинейная расчётная (детерминированно из вектор-слоя PDF, без OCR/LLM)")
    meta = []
    if src.get("section"):
        meta.append(f"раздел {src['section']}")
    if src.get("object"):
        meta.append(src["object"])
    if meta:
        L.append("**Раздел/объект:** " + " | ".join(meta))
    L.append(
        f"**Сводка валидации:** линий {graph.get('feeders_total')}, привязано кодов "
        f"{v.get('codes_linked_occurrences')}/{v.get('codes_total_occurrences')} "
        f"(uniq {v.get('codes_linked_unique')}/{v.get('codes_total_unique')}), актив {v.get('active')}, "
        f"резерв {v.get('reserve')}, неоднозначных {v.get('ambiguous')}, без кода {v.get('no_code', 0)}, "
        f"структурных {v.get('structural', 0)}, геом.конфликтов "
        f"{v.get('geometry_conflicts')}, вторичных {v.get('secondary_codes_total')}, физика P/I "
        f"{v.get('power_rate')}/{v.get('current_rate')}, confidence {graph.get('confidence')}, "
        f"status `{graph.get('status')}`")
    for w in graph.get("warnings", []):
        L.append(f"  - ⚠ {w}")

    # 1. Дерево питания
    L.append("\n## 1. Текстовое дерево питания\n")
    hier = graph.get("hierarchy") or {}
    tree = hier.get("tree_lines") or []
    L.append("```text")
    L.extend(tree if tree else ["(дерево питания не извлечено)"])
    L.append("```")

    # 2. Панели и основные параметры
    L.append("\n## 2. Панели и основные параметры\n")
    L.append("| ID панели | Iкз(3), кА | Iу, кА | Iкз(1), кА | Расчётные режимы (Ру/Кс/Cos f/Рр/Sр/Ip) |")
    L.append("| --- | --- | --- | --- | --- |")
    for p in graph.get("panel_calculations", []):
        L.append(f"| {_md_cell(p.get('id'))} | {_md_cell(p.get('ikz3'))} | {_md_cell(p.get('iu'))} "
                 f"| {_md_cell(p.get('ikz1'))} | {_md_cell(_fmt_modes(p))} |")
    if not graph.get("panel_calculations"):
        L.append("| (расчёты панелей не извлечены) | — | — | — | — |")

    # 2.1 Расчёты по вводам (сводные режимы: рабочий/авария/пожар) — если извлечены
    ic = graph.get("input_calculations") or []
    if ic:
        L.append("\n### 2.1 Расчёты по вводам (сводные режимы)\n")
        L.append("| Ввод (питает) | Режим | Ру, кВт | Кс | Cos f | Рр, кВт | Sр, кВА | Ip, А |")
        L.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        for r in ic:
            L.append(f"| {_md_cell(r.get('vvod'))} | {_md_cell(r.get('mode'))} | {_md_cell(r.get('Pu'))} "
                     f"| {_md_cell(r.get('Kc'))} | {_md_cell(r.get('cosphi'))} | {_md_cell(r.get('Pr'))} "
                     f"| {_md_cell(r.get('Sr'))} | {_md_cell(r.get('Ip'))} |")

    # 3. Таблица связей
    L.append("\n## 3. Таблица связей (межпанельный граф)\n")
    L.append("| Откуда | Куда | Тип | Аппарат/кабель/примечание |")
    L.append("| --- | --- | --- | --- |")
    for e in graph.get("edges", []):
        L.append(f"| {_md_cell(e.get('from'))} | {_md_cell(e.get('to'))} | {_md_cell(e.get('type'))} "
                 f"| {_md_cell(e.get('label'))} |")
    if not graph.get("edges"):
        L.append("| — | — | — | — |")

    # 3.1 Секции шин (только если секций ≥2 — иначе слой пуст)
    bs = graph.get("bus_sections") or []
    if bs:
        L.append("\n### 3.1 Секции шин (фидер → секция, по геометрии)\n")
        L.append("| Секция | Панели | Отходящих фидеров | Вводные аппараты | X-диапазон |")
        L.append("| --- | --- | --- | --- | --- |")
        for s in bs:
            L.append(f"| {_md_cell(s.get('name'))} | {_md_cell(', '.join(s.get('panels') or []) or '—')} "
                     f"| {len(s.get('feeder_qfs') or [])} | "
                     f"{_md_cell(', '.join(s.get('incomer_qfs') or []) or '—')} "
                     f"| {s.get('x_range')} |")

    # 3.2 Пары С1↔С2 (взаиморезерв секций)
    fp = graph.get("feeder_pairs") or []
    if fp:
        L.append("\n### 3.2 Пары С1↔С2 (взаиморезерв с двух секций)\n")
        L.append("| База | С1 (QF) | С2 (QF) | Замечание |")
        L.append("| --- | --- | --- | --- |")
        for p in fp:
            L.append(f"| {_md_cell(p.get('base'))} | {_md_cell(p.get('c1'))} ({_md_cell(p.get('c1_qf'))}) "
                     f"| {_md_cell(p.get('c2'))} ({_md_cell(p.get('c2_qf'))}) | {_md_cell(p.get('note') or '—')} |")

    # 3.3 Управление: сигнал → аппарат → линия
    ce = graph.get("control_edges") or []
    if ce:
        L.append("\n### 3.3 Управление (сигнал → аппарат → линия)\n")
        L.append("| Сигнал | Аппарат | Линия (QF/код) | Потребитель | Действие |")
        L.append("| --- | --- | --- | --- | --- |")
        for e in ce:
            L.append(f"| {_md_cell(e.get('signal'))} | {_md_cell(e.get('device') or '—')} "
                     f"| {_md_cell(e.get('qf'))} / {_md_cell(e.get('circuit_code') or '—')} "
                     f"| {_md_cell(e.get('consumer') or '—')} | {_md_cell(e.get('action') or '—')} |")

    # 3.4 Устройства вводной зоны (QS/FU/УЗИП/РН/HL)
    idv = graph.get("input_devices") or []
    if idv:
        _dt = {"switch": "рубильник", "fuse": "предохранитель", "spd": "УЗИП",
               "relay_voltage": "реле напряжения", "indicator": "индикация"}
        L.append("\n### 3.4 Устройства вводной зоны\n")
        L.append("| Устройство | Тип | Привязка |")
        L.append("| --- | --- | --- |")
        for d in idv:
            b = d.get("incomer") or d.get("bus_section") or "не привязано"
            L.append(f"| {_md_cell(d.get('device'))} | {_md_cell(_dt.get(d.get('dtype'), d.get('dtype')))} "
                     f"| {_md_cell(b)} |")

    # 4. Отходящие линии (по панелям)
    L.append("\n## 4. Отходящие линии\n")
    for pan in graph.get("panels", []):
        L.append(f"\n### {pan['name']} — {pan['feeder_count']} линий "
                 f"(актив {pan['active']}, резерв {pan['reserve']})\n")
        L.append("| QF | Автомат (тип; Icn; In) | Потребитель | Код | "
                 "Ру/Кс/Cosφ/Рр/Ip | L/ΔU/кабель/Iкз(1)/трасса | Доп. аппараты | Управление | status | review |")
        L.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for f in pan.get("feeders", []):
            ctrl = ", ".join(f.get("control") or []) or "—"
            devs = "; ".join(f.get("additional_devices") or []) or "—"
            rev = "; ".join(f.get("review") or []) or "—"
            L.append(
                f"| {_md_cell(f.get('qf'))} | {_md_cell(_fmt_breaker(f))} | {_md_cell(f.get('consumer'))} "
                f"| {_md_cell(f.get('circuit_code'))} | {_md_cell(_fmt_load(f))} | {_md_cell(_fmt_cable(f))} "
                f"| {_md_cell(devs)} | {_md_cell(ctrl)} | {_md_cell(f.get('status'))} | {_md_cell(rev)} |")

    # 5. Реестр служебных элементов
    L.append("\n## 5. Реестр служебных элементов\n")
    L.append("| Элемент | Значение | Как учитывать |")
    L.append("| --- | --- | --- |")
    for s in graph.get("service_elements", []):
        L.append(f"| {_md_cell(s.get('element'))} | {_md_cell(s.get('value'))} | {_md_cell(s.get('note'))} |")
    if not graph.get("service_elements"):
        L.append("| (служебные элементы не извлечены) | — | — |")

    # 5.1 Вторичные цепи (ад/ан) — отдельный слой, НЕ primary feeder code
    sec = graph.get("secondary_circuits") or []
    if sec:
        L.append("\n## 5.1 Вторичные цепи (управление/дымоудаление)\n")
        L.append("_Коды верхних рядов («ад»/«ан») — вторичные цепи двигателей/нагрева, "
                 "не основные коды отходящих линий._\n")
        L.append("| Код | Панель | Ближайшая QF |")
        L.append("| --- | --- | --- |")
        for s in sec:
            L.append(f"| {_md_cell(s.get('code'))} | {_md_cell(s.get('panel'))} "
                     f"| {_md_cell(s.get('near_qf'))} |")

    # 6. Таблица проверки трансформаторов тока
    L.append("\n## 6. Таблица проверки трансформаторов тока\n")
    tt = graph.get("tt_check_table") or []
    if tt:
        L.append("| Панель | Iр.раб, А | Iр.авар, А | Iн1тт | Iн1тт.авар | Iсч.max | Iр.мин | "
                 "Доп. 20% | Макс. ток аварии | Комментарий |")
        L.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for r in tt:
            L.append(
                f"| {_md_cell(r.get('panel'))} | {_md_cell(r.get('Ir_rab'))} | {_md_cell(r.get('Ir_avar'))} "
                f"| {_md_cell(r.get('In1tt'))} | {_md_cell(r.get('In1tt_avar'))} | {_md_cell(r.get('Isch_max'))} "
                f"| {_md_cell(r.get('Ir_min'))} | {_md_cell(r.get('max20'))} | {_md_cell(r.get('max_avar'))} "
                f"| {_md_cell(r.get('comment'))} |")
    else:
        L.append("_Таблица проверки ТТ на листе не обнаружена._")

    # 6.1 Устройства учёта на схеме (TA/Wh/счётчик → колонка фидера/ввода)
    mt = graph.get("metering") or []
    if mt:
        L.append("\n### 6.1 Устройства учёта на схеме (привязка по колонкам)\n")
        L.append("| Точка | TA (транс. тока) | Wh | Счётчик/прибор |")
        L.append("| --- | --- | --- | --- |")
        n_unbound = 0
        for p in mt:
            if p.get("kind") not in ("feeder", "incomer"):
                n_unbound += 1     # техническое ограничение разметки — не строка таблицы
                continue
            where = {"feeder": "фидер", "incomer": "ввод"}[p["kind"]]
            tag = f"{where} {p.get('qf')}" if p.get("qf") else where
            L.append(f"| {_md_cell(tag)} | {_md_cell(', '.join(p.get('ta') or []) or '—')} "
                     f"| {p.get('wh') or '—'} | {_md_cell(', '.join(p.get('meter') or []) or '—')} |")
        if n_unbound:
            # A/B ЭМ-К1 07-04: строки «не привязано» в таблице провоцировали LLM на
            # мета-замечания «учёт не привязан» — а это ограничение АВТОРАЗМЕТКИ, не дефект
            # проекта. Выносим в явную техническую сноску, не как данные схемы.
            L.append(f"\n_⚙ Тех. пометка разметки: ещё {n_unbound} устройств(а) учёта не удалось "
                     f"автоматически привязать к колонке — это ограничение автоматической "
                     f"разметки, НЕ дефект проекта; замечаний по этому не делать._")

    # 7. Примечания
    L.append("\n## 7. Примечания\n")
    notes = graph.get("notes") or []
    if notes:
        for n in notes:
            L.append(f"{n['n']}. {n['text']}")
    else:
        L.append("_Примечания не извлечены._")

    # 8. Validation / requires_review
    L.append("\n## 8. Validation / requires_review\n")
    L.append(f"- panels_detected: {', '.join(v.get('panels_detected') or []) or '—'}")
    L.append(f"- codes (occurrences): linked {v.get('codes_linked_occurrences')}/"
             f"{v.get('codes_total_occurrences')}; (unique): {v.get('codes_linked_unique')}/"
             f"{v.get('codes_total_unique')}")
    if v.get("unbound_param_codes"):
        L.append(f"- unbound_param_codes: {', '.join(v['unbound_param_codes'])}")
    if v.get("duplicate_param_codes"):
        L.append(f"- duplicate_param_codes (дубль в ПД): {', '.join(v['duplicate_param_codes'])}")
    if v.get("duplicate_bindings"):
        L.append(f"- duplicate_bindings (один код на >1 QF): {', '.join(v['duplicate_bindings'])}")
    if v.get("missing_panel_warnings"):
        L.append(f"- missing_panel_warnings: {'; '.join(v['missing_panel_warnings'])}")
    L.append(f"- secondary_codes_total: {v.get('secondary_codes_total')}")
    L.append(f"- active: {v.get('active')}; reserve: {v.get('reserve')}; "
             f"ambiguous: {v.get('ambiguous')}")
    L.append(f"- breaker_bound: {v.get('breaker_bound')}; geometry_conflicts: "
             f"{v.get('geometry_conflicts')}")
    L.append(f"- power_rate: {v.get('power_rate')}; current_rate: {v.get('current_rate')}; "
             f"confidence: {graph.get('confidence')}; status: {graph.get('status')}")
    rev = graph.get("review") or []
    if rev:
        L.append("\n**Строки, требующие ручной проверки:**")
        for r in rev:
            L.append(f"- {r['qf']} ({r.get('code') or 'без кода'}): {'; '.join(r.get('notes') or [])}")
    else:
        L.append("\n_Строк, требующих ручной проверки, не выявлено._")

    return "\n".join(L)


# ── Промпт Stage 02 для схемного блока (компактный / rich) + резолвер по version_dir ──────

_STAGE02_TASK = ("## Задача:\nПосмотри на изображение блока и верни findings[]. "
                 "Только проблемы. Не описывай что видишь. Если всё корректно — пустой массив.")


def build_singleline_prompt(pdf_path: Path, vector_text: str, *, panel_hint: str = "ВРУ",
                            rich: bool = False, block_id: str = "", page=None,
                            bbox_norm: Optional[list] = None,
                            polygon_norm: Optional[list] = None) -> Optional[str]:
    """Собрать user_text Stage 02 для СХЕМНОГО блока. None — если блок не однолинейная схема.

    ЕДИНЫЙ вариант — гибрид `render_graph_for_prompt` (курируемый: ввод+ТТ без
    хардкода ВА-305, детерминированная проверка защиты, анти-boilerplate). Параметр
    `rich` сохранён для обратной совместимости сигнатуры, но обе ветки дают гибрид —
    третьего варианта нет (решение 07-04: не плодить версии, имя «compact»).

    ГЕЙТ КАЧЕСТВА (evaluate_vectograf_gate) применяется ВСЕГДА: не прошёл → None →
    вызывающий падает на штатный fallback (enrichment+page_text). Слабый граф хуже,
    чем честный «нет графа».
    """
    graph = build_singleline_graph(pdf_path, vector_text, panel_hint=panel_hint,
                                   bbox_norm=bbox_norm, polygon_norm=polygon_norm)
    if not graph:
        return None
    if not evaluate_vectograf_gate(graph)["use"]:
        return None
    body = render_graph_for_prompt(graph)
    return f"# Блок {block_id} | страница PDF {page}\n\n{body}\n\n{_STAGE02_TASK}"


@functools.lru_cache(maxsize=8)
def _result_blocks_vector_index(rp_str: str, _mtime: float) -> dict:
    """block_id → {"text": pdfplumber_text, "bbox_norm": coords_norm|None} из result.json
    (кэш по пути+mtime, чтобы не читать 2МБ на блок). bbox_norm нужен для клипа геометрии
    Вектографа по области выделения блока (см. ``_clip_words_to_bbox``)."""
    try:
        rj = json.loads(Path(rp_str).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    idx = {}
    for pg in rj.get("pages", []):
        for b in (pg.get("blocks") or []):
            bid = str(b.get("id") or b.get("block_id") or "")
            if bid:
                cn = b.get("coords_norm")
                pn = b.get("polygon_points_norm")
                idx[bid] = {"text": b.get("pdfplumber_text") or "",
                            "bbox_norm": list(cn) if isinstance(cn, (list, tuple)) and len(cn) >= 4 else None,
                            "polygon_norm": list(pn) if isinstance(pn, (list, tuple)) and len(pn) >= 3 else None}
    return idx


def evaluate_vectograf_gate(graph: Optional[dict]) -> dict:
    """Гейт качества Вектографа: годится ли граф как ЗАМЕНА Gemma-описания блока.

    Жёсткие критерии (подобраны по 15 боевым однолинейкам, все проходят; мусор — нет):
      - граф построен и линий ≥ 5 (меньше — не расчётная однолинейка);
      - физика P ≥ 0.8 (пересчёт P=√3·U·I·cosφ против подписанных значений);
      - честная привязка active/(active+ambiguous) ≥ 0.85;
      - геометрических конфликтов ≤ 15% линий.
    coverage/confidence НЕ блокируют (на честных листах гуляют из-за дублей токенов
    и кодов чужих панелей: ГРЩ 50%, К7 0.36) — уходят в warnings для shadow-аналитики.

    Возвращает {"use": bool, "reasons": [...], "warnings": [...], "metrics": {...}}.
    """
    if not graph:
        return {"use": False, "reasons": ["граф не построен (не feeder-однолинейка)"],
                "warnings": [], "metrics": {}}
    v = graph.get("validation") or {}
    ft = graph.get("feeders_total") or 0
    act = v.get("active") or 0
    amb = v.get("ambiguous") or 0
    bind = act / max(act + amb, 1)
    conf_rate = (v.get("geometry_conflicts") or 0) / max(ft, 1)
    power_rate = v.get("power_rate")
    cov = (v.get("codes_linked_occurrences") or 0) / max(v.get("codes_total_occurrences") or 1, 1)
    metrics = {"feeders_total": ft, "bind_rate": round(bind, 3),
               "power_rate": power_rate, "conflict_rate": round(conf_rate, 3),
               "coverage": round(cov, 3), "confidence": graph.get("confidence"),
               "status": graph.get("status")}
    reasons = []
    if ft < 5:
        reasons.append(f"мало линий ({ft} < 5)")
    if power_rate is not None and power_rate < 0.8:
        reasons.append(f"физика P {power_rate} < 0.8")
    if bind < 0.85:
        reasons.append(f"честная привязка {bind:.0%} < 85%")
    if conf_rate > 0.15:
        reasons.append(f"геом. конфликтов {conf_rate:.0%} > 15%")
    warnings = []
    if cov < 0.6:
        warnings.append(f"низкое покрытие кодов {cov:.0%} (дубли токенов/чужие панели?)")
    if graph.get("status") == "needs_review":
        warnings.append("status=needs_review")
    return {"use": not reasons, "reasons": reasons, "warnings": warnings, "metrics": metrics}


def vectograf_gate_for_block(version_dir, block_id: str, *, panel_hint: str = "ВРУ"):
    """Гейт по блоку из result.json: (decision, graph). fail-soft → (use=False, None).

    Используется shadow-режимом enrichment и (при включении) заменой Gemma-описания.
    """
    try:
        vd = Path(version_dir)
        rp = vd / "02_work" / "result.json"
        if not rp.exists():
            return evaluate_vectograf_gate(None), None
        idx = _result_blocks_vector_index(str(rp), rp.stat().st_mtime)
        entry = idx.get(str(block_id)) or {}
        vector_text = entry.get("text") or ""
        if not vector_text or len(vector_text) < 30:
            return evaluate_vectograf_gate(None), None
        pdf = vd / "02_work" / "document.pdf"
        if not pdf.exists() and (vd / "document.pdf").exists():
            pdf = vd / "document.pdf"
        if not pdf.exists():
            return evaluate_vectograf_gate(None), None
        graph = build_singleline_graph(pdf, vector_text, panel_hint=panel_hint,
                                       bbox_norm=entry.get("bbox_norm"),
                                       polygon_norm=entry.get("polygon_norm"))
        return evaluate_vectograf_gate(graph), graph
    except Exception:
        return {"use": False, "reasons": ["исключение при построении графа"],
                "warnings": [], "metrics": {}}, None


def resolve_singleline_prompt(version_dir, block_id: str, page, *, rich: bool) -> Optional[str]:
    """Промпт схемного блока по version_dir (читает 02_work/result.json + document.pdf). fail-soft.

    None — если нет данных или блок не однолинейная схема. Используется реальным Stage 02
    (call_gpt_for_block) и превью, чтобы флаг SINGLELINE_RICH_PROMPT давал одинаковый результат.
    """
    vd = Path(version_dir)
    rp = vd / "02_work" / "result.json"
    if not rp.exists():
        return None
    try:
        idx = _result_blocks_vector_index(str(rp), rp.stat().st_mtime)
    except OSError:
        return None
    entry = idx.get(str(block_id)) or {}
    vector_text = entry.get("text") or ""
    if not vector_text or len(vector_text) < 30:
        return None
    pdf = vd / "02_work" / "document.pdf"
    if not pdf.exists() and (vd / "document.pdf").exists():
        pdf = vd / "document.pdf"
    if not pdf.exists():
        return None
    return build_singleline_prompt(pdf, vector_text, rich=rich, block_id=str(block_id), page=page,
                                   bbox_norm=entry.get("bbox_norm"),
                                   polygon_norm=entry.get("polygon_norm"))
