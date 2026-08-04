"""Справочная область листа: условные обозначения, примечания, ведомость
помещений, карточки квартир, штамп.

Условные обозначения листа — источник эталонных векторных сигнатур для
классификации символов плана (Шаг 3 брифа). Семантика строки легенды
берётся из её ПОДПИСИ на самом листе (tier 4), а не из внешнего словаря.
Ничто из справочной области не попадает в инвентарь квартир.
"""
from __future__ import annotations

import collections
import re

from .spatial import seg_angle_deg

MARK_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
ELEV_RE = re.compile(r"^[+\-]\d+[.,]\d{2,3}$")
DECIMAL_RE = re.compile(r"^\d+[.,]\d$|^\d+[.,]\d\d$")
CARD_NUMBER_RE = re.compile(r"^\d{3,4}$")

# Семантика подписей легенды. Это словарь РУССКИХ формулировок условных
# обозначений (генерик для дисциплины), а не карта конкретного PDF: если
# подпись листа не матчится — строка легенды честно остаётся unresolved.
LEGEND_KIND_PATTERNS = (
    ("chandelier_output", re.compile(r"вывод\s+под\s+люстру", re.I)),
    ("light_output", re.compile(r"вывод\s+под\s+светильник", re.I)),
    ("group_label", re.compile(r"группа\s+светильников", re.I)),
    ("switch_1", re.compile(r"выключатель\s+одноклавишн", re.I)),
    ("switch_2", re.compile(r"выключатель\s+двух?клавишн", re.I)),
    ("switch_changeover", re.compile(r"переключатель", re.I)),
    ("master_switch", re.compile(r"мастер[\s-]*выключатель", re.I)),
    ("ceiling_type_tag", re.compile(r"маркировка\s+потолка", re.I)),
    ("ceiling_elevation_tag", re.compile(r"отметка\s+уровня\s+потолка", re.I)),
)

KIND_RU = {
    "chandelier_output": "вывод под люстру",
    "light_output": "вывод под светильник",
    "group_label": "группа светильников",
    "switch_1": "выключатель одноклавишный",
    "switch_2": "выключатель двухклавишный",
    "switch_changeover": "переключатель с нескольких мест",
    "master_switch": "мастер-выключатель",
    "ceiling_type_tag": "маркировка потолка",
    "ceiling_elevation_tag": "отметка уровня потолка",
}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _find_header(texts, pattern: str):
    rx = re.compile(pattern, re.I)
    hits = [t for t in texts if rx.search(_norm(t["text"]))]
    return min(hits, key=lambda t: t["bbox"][1]) if hits else None


def parse_reference(cp, inv: dict) -> dict:
    """Разбор всей справочной области. Возврат — словарь ref (см. ключи ниже)."""
    texts = inv["texts"]
    ref: dict = {"warnings": []}

    header = _find_header(texts, r"^условные\s+обозначени")
    notes_header = _find_header(texts, r"^примечани")
    ref["legend_header_bbox"] = header["bbox"] if header else None

    # --- строки легенды: подписи, начинающиеся с «-», в колонке заголовка ---
    labels = []
    if header is not None:
        col_x0 = header["bbox"][0] - 90
        col_x1 = header["bbox"][0] + 620
        y_top = header["bbox"][3]
        y_bot = notes_header["bbox"][1] if notes_header else y_top + 900
        seen_rows: set[tuple[str, int]] = set()
        for t in texts:
            b = t["bbox"]
            if not (col_x0 <= b[0] <= col_x1 and y_top < b[1] < y_bot):
                continue
            txt = _norm(t["text"])
            if not txt.startswith("-"):
                continue
            body = _norm(txt.lstrip("- "))
            if len(body) < 4:
                continue
            row_key = (body.lower(), int(b[1] // 5))
            if row_key in seen_rows:
                continue  # дубль подписи в параллельном слое
            seen_rows.add(row_key)
            labels.append({"text": body, "bbox": b, "layer": t["layer"]})
        labels.sort(key=lambda item: item["bbox"][1])
        # хвосты подписей без «-» (перенос строки) приклеиваем к предыдущей
        for t in sorted(texts, key=lambda x: x["bbox"][1]):
            b = t["bbox"]
            txt = _norm(t["text"])
            if txt.startswith("-") or not labels:
                continue
            for lab in labels:
                if 0 < b[1] - lab["bbox"][1] < 16 and abs(b[0] - lab["bbox"][0]) < 40 \
                        and col_x0 <= b[0] <= col_x1 and txt and not txt.startswith(("+",)):
                    canon = re.sub(r"[.,;\s]+", " ", txt.lower()).strip()
                    have = re.sub(r"[.,;\s]+", " ", lab["text"].lower()).strip()
                    if canon and canon not in have:
                        lab["text"] = _norm(lab["text"] + " " + txt)
                    break
    ref["legend_labels"] = labels

    # --- граница справочной колонки внутри CropBox ---
    col_candidates = [header["bbox"][0]] if header else []
    if notes_header:
        col_candidates.append(notes_header["bbox"][0])
    col_candidates.extend(lab["bbox"][0] - 70 for lab in labels)
    ref["ref_col_x0"] = round(min(col_candidates) - 8, 2) if col_candidates else cp.block_rect[2]

    # --- эталонные сигнатуры символов легенды ---
    ref["templates"] = _build_templates(inv, labels)
    zone_boxes = [t["symbol_zone"] for t in ref["templates"] if t.get("symbol_zone")]
    zone_boxes.extend(lab["bbox"] for lab in labels)
    if header:
        zone_boxes.append(header["bbox"])
    ref["legend_zone"] = _union_bbox(zone_boxes) if zone_boxes else None

    # --- ведомость помещений: марка → наименование ---
    ref["room_schedule"], ref["floors_label"] = _parse_room_schedule(texts, cp)

    # --- примечания листа (sheet_rules); правее начала ведомости не читаем ---
    schedule_x0 = min((row["bbox"][0] for row in ref["room_schedule"].values()),
                      default=cp.media_rect[2])
    ref["sheet_rules"] = _parse_notes(texts, notes_header, right_limit=schedule_x0 - 10)

    # --- карточки квартир ---
    ref["apartment_cards"] = _parse_apartment_cards(texts, cp, ref["ref_col_x0"])

    # --- штамп / метаданные листа ---
    ref["sheet_meta"] = _parse_sheet_meta(texts, cp)
    return ref


def _union_bbox(boxes):
    return (round(min(b[0] for b in boxes), 2), round(min(b[1] for b in boxes), 2),
            round(max(b[2] for b in boxes), 2), round(max(b[3] for b in boxes), 2))


def _build_templates(inv: dict, labels: list[dict]) -> list[dict]:
    """Эталонная векторная сигнатура для каждой строки легенды.

    Символы легенды крупнее шага строк и вертикально перекрываются,
    поэтому раскрой не «полосами», а назначением каждого элемента колонки
    ближайшему центру строки (по y-центру элемента).
    """
    templates = []
    if not labels:
        return templates
    x1 = min(lab["bbox"][0] for lab in labels) - 1
    x0 = x1 - 69
    y0 = min(lab["bbox"][1] for lab in labels) - 14
    y1 = max(lab["bbox"][3] for lab in labels) + 14
    column = (x0, y0, x1, y1)
    centers = [((lab["bbox"][1] + lab["bbox"][3]) / 2) for lab in labels]

    def row_of(bbox) -> int:
        cy = (bbox[1] + bbox[3]) / 2
        return min(range(len(centers)), key=lambda i: abs(centers[i] - cy))

    row_zones: dict[int, list] = {}
    for idx in range(len(labels)):
        row_zones[idx] = []
    for kind_name, objects in _column_objects(inv, column):
        for obj in objects:
            row_zones[row_of(obj["bbox"])].append((kind_name, obj))

    for idx, lab in enumerate(labels):
        sig = _signature_of_objects(row_zones[idx])
        kind = None
        for cand, rx in LEGEND_KIND_PATTERNS:
            if rx.search(lab["text"]):
                kind = cand
                break
        boxes = [obj["bbox"] for _, obj in row_zones[idx]] or [lab["bbox"]]
        templates.append({
            "kind": kind or "unresolved_legend_row",
            "label": lab["text"],
            "label_bbox": lab["bbox"],
            "symbol_zone": _union_bbox(boxes),
            "signature": sig,
        })
    return templates


def _column_objects(inv: dict, column):
    """Объекты колонки символов легенды по видам (circle/quad/line/text)."""
    x0, y0, x1, y1 = column

    def inside(bb):
        return bb[0] >= x0 - 0.5 and bb[2] <= x1 + 0.5 and bb[1] >= y0 - 0.5 and bb[3] <= y1 + 0.5

    circles = []
    for c in inv["circles"]:
        r = c["d"] / 2
        bb = (c["center"][0] - r, c["center"][1] - r, c["center"][0] + r, c["center"][1] + r)
        if inside(bb) and c["color_family"] not in ("white", "none"):
            circles.append({"bbox": bb, "ref": c})
    quads = [{"bbox": q["bbox"], "ref": q} for q in inv["quads"]
             if inside(q["bbox"]) and q["color_family"] not in ("white", "none")]
    lines = []
    for s in inv["segments"]:
        if s["kind"] != "l" or s["color_family"] in ("white", "none"):
            continue
        bb = (min(s["p1"][0], s["p2"][0]), min(s["p1"][1], s["p2"][1]),
              max(s["p1"][0], s["p2"][0]), max(s["p1"][1], s["p2"][1]))
        if inside(bb):
            lines.append({"bbox": bb, "ref": s})
    texts = [{"bbox": t["bbox"], "ref": t} for t in inv["texts"] if inside(t["bbox"])]
    return [("circle", circles), ("quad", quads), ("line", lines), ("text", texts)]


def _signature_of_objects(objects: list) -> dict:
    circles = sorted(round(obj["ref"]["d"], 1) for kind, obj in objects if kind == "circle")
    rects = sorted((round(min(obj["ref"]["w"], obj["ref"]["h"]), 1),
                    round(max(obj["ref"]["w"], obj["ref"]["h"]), 1))
                   for kind, obj in objects if kind == "quad")
    n_axis = n_diag = 0
    colors = collections.Counter()
    for kind, obj in objects:
        if kind == "line":
            ang = seg_angle_deg(obj["ref"]["p1"], obj["ref"]["p2"])
            if ang < 12 or ang > 168 or 78 < ang < 102:
                n_axis += 1
            else:
                n_diag += 1
        if kind in ("circle", "quad", "line"):
            fam = obj["ref"]["color_family"]
            if fam not in ("white", "none"):
                colors[fam] += 1
    inner_letters, inner_digits, inner_elev = [], [], []
    for kind, obj in objects:
        if kind != "text":
            continue
        val = _norm(obj["ref"]["text"])
        if ELEV_RE.match(val):
            inner_elev.append(val)
        elif val.isdigit():
            inner_digits.append(val)
        elif len(val) <= 2 and val.isalpha():
            inner_letters.append(val.upper().replace("M", "М"))  # гомоглиф M↔М
    return {
        "circles": circles,
        "rects": rects,
        "n_axis_lines": n_axis,
        "n_diag_lines": n_diag,
        "colors": dict(sorted(colors.items())),
        "inner_letters": sorted(set(inner_letters)),
        "inner_digits": sorted(inner_digits),
        "inner_elevations": sorted(inner_elev),
    }



def _parse_notes(texts, notes_header, *, right_limit: float = 10 ** 9) -> list[dict]:
    if notes_header is None:
        return []
    hb = notes_header["bbox"]
    rows = [t for t in texts
            if hb[3] < t["bbox"][1] < hb[3] + 260
            and hb[0] - 60 <= t["bbox"][0] <= hb[0] + 520
            and t["bbox"][2] <= right_limit]
    lines: dict[int, list] = collections.defaultdict(list)
    for t in rows:
        lines[int(t["bbox"][1] // 6)].append(t)
    assembled = []
    for key in sorted(lines):
        parts = sorted(lines[key], key=lambda t: t["bbox"][0])
        joined = []
        prev_x1 = None
        for p in parts:
            if prev_x1 is not None and p["bbox"][0] - prev_x1 > 25:
                break  # разрыв колонок: дальше уже таблицы/чужой текст
            joined.append(p["text"])
            prev_x1 = p["bbox"][2]
        assembled.append(_norm("".join(joined)))
    rules = []
    for line in assembled:
        m = re.match(r"^(\d+)\s*-\s*(.+)$", line)
        if m:
            rules.append({"no": int(m.group(1)), "text": _norm(m.group(2))})
        elif rules and line:
            rules[-1]["text"] = _norm(rules[-1]["text"] + " " + line)
    return rules


def _parse_room_schedule(texts, cp) -> tuple[dict, str]:
    """Ведомость «Наим. помещения»: строки марка → наименование."""
    schedule: dict[str, dict] = {}
    marks = [t for t in texts if MARK_RE.match(_norm(t["text"]))
             and t["bbox"][0] >= cp.block_rect[2] - 2]  # только за кропом (таблица)
    floors = ""
    for t in texts:
        m = re.match(r"^(\d+\s*[-–]\s*\d+\s+этаж)", _norm(t["text"]), re.I)
        if m and t["bbox"][0] >= cp.block_rect[2] - 2:
            floors = m.group(1)
            break
    for mk in marks:
        b = mk["bbox"]
        row = [t for t in texts
               if t["bbox"][0] > b[2] and abs((t["bbox"][1] + t["bbox"][3]) / 2 - (b[1] + b[3]) / 2) < 4.5]
        row.sort(key=lambda t: t["bbox"][0])
        name_parts = []
        prev_x1 = b[2]
        for t in row:
            if t["bbox"][0] - prev_x1 > 15:  # разрыв → следующая колонка/таблица
                break
            name_parts.append(_norm(t["text"]))
            prev_x1 = t["bbox"][2]
        if name_parts:
            schedule[_norm(mk["text"])] = {"name": " ".join(name_parts), "bbox": b}
    return schedule, floors


def _parse_apartment_cards(texts, cp, ref_col_x0: float) -> list[dict]:
    """Карточки квартир по периметру плана.

    Карточка = номер (3–4 цифры) + столбик из ≥2 десятичных площадей ниже
    + необязательные краткие токены типа. Размерные числа (целые) такой
    столбик не образуют — это и отделяет карточку от размеров.
    """
    cards = []
    numbers = [t for t in texts if CARD_NUMBER_RE.match(_norm(t["text"]))
               and t["bbox"][0] < ref_col_x0]
    for num in sorted(numbers, key=lambda t: (t["bbox"][1], t["bbox"][0])):
        b = num["bbox"]
        col = [t for t in texts
               if t is not num
               and abs((t["bbox"][0] + t["bbox"][2]) / 2 - (b[0] + b[2]) / 2) < 42
               and 0 < t["bbox"][1] - b[3] < 68]
        col.sort(key=lambda t: t["bbox"][1])
        areas = []
        type_tokens = []
        for t in col:
            val = _norm(t["text"])
            if re.match(r"^\d+[.,]\d{1,2}$", val):
                areas.append(val.replace(",", "."))
            elif re.match(r"^[-–]?[0-9A-ZА-Я][0-9A-ZА-Я\s.\-–]{0,6}$", val, re.I) and not val.isdigit():
                type_tokens.append(val)
        if len(areas) < 2:
            continue
        uniq_areas = list(dict.fromkeys(areas))
        card = {
            "apartment": _norm(num["text"]),
            "bbox": b,
            "areas_raw": areas,
            "type_raw": type_tokens,
            "type": _norm(re.sub(r"\s*[-–]\s*", "-", " ".join(type_tokens))) or None,
            "living_area": uniq_areas[0] if uniq_areas else None,
            "apartment_area": uniq_areas[1] if len(uniq_areas) > 1 else None,
            "total_area": uniq_areas[2] if len(uniq_areas) > 2 else (areas[-1] if areas else None),
            "requires_review": False,
        }
        try:
            seq = [float(card["living_area"]), float(card["apartment_area"] or card["living_area"]),
                   float(card["total_area"])]
            if not (seq[0] <= seq[1] + 1e-6 and seq[1] <= seq[2] + 5.0):
                card["requires_review"] = True  # общая может быть чуть меньше из-за лоджии с коэф.
        except (TypeError, ValueError):
            card["requires_review"] = True
        cards.append(card)
    # дедуп по номеру: берём карточку с максимумом полей
    best: dict[str, dict] = {}
    for card in cards:
        cur = best.get(card["apartment"])
        if cur is None or len(card["areas_raw"]) > len(cur["areas_raw"]):
            best[card["apartment"]] = card
    return [best[k] for k in sorted(best)]


def _parse_sheet_meta(texts, cp) -> dict:
    """Метаданные из штампа и заголовков за CropBox."""
    meta = {"doc_number": None, "sheet_name": None, "building": None,
            "zero_level": None, "sheet_no": None, "address": None}
    stamp = [t for t in texts if t["bbox"][0] >= cp.block_rect[2] - 60 and t["bbox"][1] > cp.media_rect[3] * 0.55]
    lines: dict[int, list] = collections.defaultdict(list)
    for t in stamp:
        lines[int((t["bbox"][1] + t["bbox"][3]) / 2 // 5)].append(t)
    rows = []
    for key in sorted(lines):
        parts = sorted(lines[key], key=lambda t: t["bbox"][0])
        rows.append(_norm(" ".join(p["text"] for p in parts)))
    joined = "\n".join(rows)
    compact = "\n".join(re.sub(r"\s+", "", row) for row in rows)
    m = re.search(r"(\d{2,3}[А-Я]{1,3}(?:-[А-Я0-9.]{1,8})+)", compact)
    if m:
        meta["doc_number"] = m.group(1)
    m = re.search(r"0[.,]000\s*=\s*([\d\s]+[.,]\d+)", joined)
    if m:
        meta["zero_level"] = _norm(m.group(1))
    m = re.search(r"Корпус\s*([\dА-Я.]+)", joined)
    if m:
        meta["building"] = m.group(1)
    m = re.search(r"(Многофункциональн[^\n]+|жило[йм][^\n]+комплекс[^\n]+)", joined, re.I)
    if m:
        meta["address"] = _norm(m.group(1))
    for t in texts:
        val = _norm(t["text"])
        if re.match(r"^план\s+потолк", val, re.I) or re.match(r"^план\s+.*освещени", val, re.I):
            meta["sheet_name"] = val
            break

    # значения под ячейками штампа «Стадия | Лист | Листов»
    def value_under(header_name, want=re.compile(r"^[\dА-ЯA-Z]{1,4}$")):
        heads = [t for t in stamp if _norm(t["text"]).lower() == header_name]
        for head in heads:
            hx = (head["bbox"][0] + head["bbox"][2]) / 2
            below = [t for t in stamp
                     if 0 < t["bbox"][1] - head["bbox"][3] < 30
                     and abs((t["bbox"][0] + t["bbox"][2]) / 2 - hx) < 26
                     and want.match(_norm(t["text"]))]
            if below:
                return _norm(min(below, key=lambda t: t["bbox"][1])["text"])
        return None

    meta["stage"] = value_under("стадия")
    meta["sheet_no"] = value_under("лист", re.compile(r"^\d{1,4}$"))
    meta["sheets_total"] = value_under("листов", re.compile(r"^\d{1,4}$"))
    return meta
