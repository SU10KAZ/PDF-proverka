"""Справочная область листа: условные обозначения, примечания, ведомость
помещений, карточки квартир, штамп.

Условные обозначения листа — источник эталонных векторных сигнатур для
классификации символов плана (Шаг 3 брифа). Семантика строки легенды
берётся из её ПОДПИСИ на самом листе (tier 4), а не из внешнего словаря.
Ничто из справочной области не попадает в инвентарь квартир.

Сборка строк легенды — ГЕОМЕТРИЧЕСКАЯ: строка собирается из нескольких
спанов по общей базовой линии (дефис может быть отдельным спаном, подпись
может переноситься на следующую строку, разделов «Условные обозначения»
может быть несколько). ``txt.startswith("-")`` одного спана гейтом не
является.
"""
from __future__ import annotations

import collections
import re

from .spatial import seg_angle_deg

MARK_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
ELEV_RE = re.compile(r"^[+\-]\d+[.,]\d{2,3}$")
DECIMAL_RE = re.compile(r"^\d+[.,]\d$|^\d+[.,]\d\d$")
CARD_NUMBER_RE = re.compile(r"^\d{3,4}$")
HEADER_RE = re.compile(r"^условные\s+обозначени[яй]\s*:?\s*$", re.I)
# токен «только дефис»: разделитель символа и подписи в строке легенды
DASH_TOKEN_RE = re.compile(r"^[\-–—]+\s*$")
# токены слева от дефиса, допустимые как ОБРАЗЕЦ символа легенды
SYMBOL_TOKEN_RE = re.compile(r"^[+\-]?\d[\d.,]*$|^[A-ZА-ЯЁ]{1,2}$|^.{1,3}$")

# Семантика подписей легенды. Это словарь РУССКИХ формулировок условных
# обозначений (генерик для дисциплины: оба CAD-диалекта подписей —
# «выключатель одноклавишный» и «подрозетник на 1 группу света»), а не
# карта конкретного PDF: если подпись листа не матчится — строка легенды
# честно остаётся unresolved. Порядок важен: первый совпавший побеждает.
LEGEND_KIND_PATTERNS = (
    ("ceiling_type_tag", re.compile(r"маркировка\s+потолка", re.I)),
    ("ceiling_elevation_tag", re.compile(r"отметка\s+уровня\s+потолка", re.I)),
    ("wall_light_output", re.compile(r"вывод\s+под\s+настенн", re.I)),
    ("chandelier_output", re.compile(r"вывод\s+под\s+люстру", re.I)),
    ("light_output", re.compile(r"вывод\s+под\s+светильник", re.I)),
    ("group_label", re.compile(r"группа\s+светильников", re.I)),
    ("master_switch", re.compile(r"мастер[\s\-]*выключател|подрозетник\s+под\s+мастер", re.I)),
    ("switch_changeover", re.compile(r"переключател", re.I)),
    ("switch_1", re.compile(r"выключатель\s+одноклавишн|подрозетник\s+на\s+1\s+групп", re.I)),
    ("switch_2", re.compile(r"выключатель\s+двух?клавишн|подрозетник\s+на\s+2\s+групп", re.I)),
    ("smoke_detector", re.compile(r"извещатель\s+пожарн", re.I)),
)

# профильные виды: по ним считается «профильное содержимое» раздела
PROFILE_KINDS = {
    "wall_light_output", "chandelier_output", "light_output", "group_label",
    "master_switch", "switch_changeover", "switch_1", "switch_2",
    "ceiling_type_tag", "ceiling_elevation_tag",
}
PROFILE_WORDS_RE = re.compile(
    r"светильник|люстр|выключател|переключател|мастер|потолк", re.I)
PROFILE_SYMBOL_LAYERS_RE = re.compile(
    r"09_Освещение|свет\s*нумерация|06_Потолок", re.I)

KIND_RU = {
    "chandelier_output": "вывод под люстру",
    "light_output": "вывод под светильник",
    "wall_light_output": "вывод под настенный светильник",
    "group_label": "группа светильников",
    "switch_1": "выключатель одноклавишный",
    "switch_2": "выключатель двухклавишный",
    "switch_changeover": "переключатель с нескольких мест",
    "master_switch": "мастер-выключатель",
    "smoke_detector": "пожарный извещатель",
    "ceiling_type_tag": "маркировка потолка",
    "ceiling_elevation_tag": "отметка уровня потолка",
}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _canon(text: str) -> str:
    return re.sub(r"[.,;:()\s]+", " ", text.lower()).strip()


def kind_of_label(label_text: str) -> str | None:
    for cand, rx in LEGEND_KIND_PATTERNS:
        if rx.search(label_text):
            return cand
    return None


def parse_reference(cp, inv: dict) -> dict:
    """Разбор всей справочной области. Возврат — словарь ref (см. ключи ниже)."""
    texts = inv["texts"]
    ref: dict = {"warnings": [], "conflicts": []}

    headers = [t for t in texts if HEADER_RE.match(_norm(t["text"]))]
    headers.sort(key=lambda t: (t["bbox"][1], t["bbox"][0]))
    notes_header = _find_first(texts, r"^примечани")
    section_stops = [t["bbox"][1] for t in texts
                     if re.match(r"^(примечани|спецификаци|ведомост)", _norm(t["text"]), re.I)]
    ref["legend_header_bbox"] = headers[0]["bbox"] if headers else None

    schedule_x0 = _schedule_left_edge(texts)

    sections = []
    for i, header in enumerate(headers):
        y_top = header["bbox"][3]
        next_ys = [h["bbox"][1] for h in headers[i + 1:]]
        next_ys += [y for y in section_stops if y > y_top + 4]
        y_bot = min(next_ys) if next_ys else y_top + 900
        rows = _assemble_rows(texts, header, y_top, y_bot, schedule_x0)
        sections.append({"header_bbox": header["bbox"], "rows": rows})

    labels, section_report = _merge_sections(sections, ref)
    ref["legend_labels"] = labels
    ref["legend_sections"] = section_report

    # --- граница справочной колонки внутри листа ---
    # Кандидаты — только объекты ПРАВОГО поля листа: легенда может лежать
    # и нижней полосой за кропом (тогда правую колонку она не задаёт,
    # низ отсекается самим CropBox).
    bx0, by0, bx1, by1 = cp.block_rect
    right_margin_x = bx0 + 0.55 * (bx1 - bx0)
    col_candidates = [h["bbox"][0] for h in headers if h["bbox"][0] >= right_margin_x]
    if notes_header is not None and notes_header["bbox"][0] >= right_margin_x:
        col_candidates.append(notes_header["bbox"][0])
    col_candidates.extend(lab["bbox"][0] - 70 for lab in labels
                          if lab["bbox"][0] >= right_margin_x)
    ref["ref_col_x0"] = round(min(col_candidates) - 8, 2) if col_candidates else cp.block_rect[2]

    def in_reference(bbox) -> bool:
        """Вне области плана: правая справочная колонка либо за CropBox."""
        if bbox[0] >= ref["ref_col_x0"]:
            return True
        return bbox[2] < bx0 or bbox[0] > bx1 or bbox[3] < by0 or bbox[1] > by1

    ref["in_reference"] = in_reference

    # --- эталонные сигнатуры символов легенды ---
    ref["templates"] = _build_templates(inv, labels)
    zone_boxes = [t["symbol_zone"] for t in ref["templates"] if t.get("symbol_zone")]
    zone_boxes.extend(lab["bbox"] for lab in labels)
    for h in headers:
        zone_boxes.append(h["bbox"])
    ref["legend_zones"] = _cluster_zones(zone_boxes)
    ref["legend_zone"] = _union_bbox(zone_boxes) if zone_boxes else None

    # --- ведомость помещений: марка → наименование ---
    ref["room_schedule"], ref["floors_label"] = _parse_room_schedule(texts, in_reference)

    # --- примечания листа (sheet_rules); правее начала ведомости не читаем ---
    sched_x = min((row["bbox"][0] for row in ref["room_schedule"].values()),
                  default=cp.media_rect[2])
    ref["sheet_rules"] = _parse_notes(texts, notes_header, right_limit=sched_x - 10)

    # --- карточки квартир ---
    ref["apartment_cards"] = _parse_apartment_cards(texts, cp, ref["ref_col_x0"])

    # --- штамп / метаданные листа ---
    ref["sheet_meta"] = _parse_sheet_meta(texts, cp, ref["ref_col_x0"])
    return ref


def _find_first(texts, pattern: str):
    rx = re.compile(pattern, re.I)
    hits = [t for t in texts if rx.search(_norm(t["text"]))]
    return min(hits, key=lambda t: t["bbox"][1]) if hits else None


def _schedule_left_edge(texts) -> float:
    """Левый край табличной ведомости (шапки «№ пом.» / «Наим…») —
    правый стоп для строк легенды, чтобы таблица не съедалась."""
    hits = [t for t in texts
            if re.match(r"^№\s*пом|^наим", _norm(t["text"]), re.I)]
    return min((t["bbox"][0] for t in hits), default=10 ** 9) - 12


def _assemble_rows(texts, header, y_top: float, y_bot: float, schedule_x0: float) -> list[dict]:
    """Геометрическая сборка строк легенды одной секции.

    Линия = спаны с общей базовой линией; строка легенды начинается с
    токена-дефиса, слева от которого только «символьные» токены-образцы;
    остальные линии приклеиваются к предыдущей строке как перенос.
    """
    hx0 = header["bbox"][0]
    col_x0, col_x1 = hx0 - 110, min(hx0 + 640, schedule_x0)
    spans = [t for t in texts
             if col_x0 <= t["bbox"][0] <= col_x1 and y_top < t["bbox"][1] < y_bot
             and _norm(t["text"])]
    if not spans:
        return []
    heights = sorted(t["bbox"][3] - t["bbox"][1] for t in spans)
    line_h = max(heights[len(heights) // 2], 3.0)

    # --- линии по базовой линии (bbox[3]) ---
    spans.sort(key=lambda t: (t["bbox"][3], t["bbox"][0]))
    raw_lines: list[list[dict]] = []
    for t in spans:
        if raw_lines and abs(t["bbox"][3] - raw_lines[-1][-1]["bbox"][3]) <= 0.45 * line_h:
            raw_lines[-1].append(t)
        else:
            raw_lines.append([t])
    # многоколоночная легенда (нижняя полоса листа): линия режется на
    # сегменты по большим горизонтальным разрывам — колонки независимы
    col_gap = max(22.0, 4.0 * line_h)
    lines: list[list[dict]] = []
    for raw in raw_lines:
        raw.sort(key=lambda t: t["bbox"][0])
        segment: list[dict] = []
        prev_x1 = None
        for t in raw:
            if segment and prev_x1 is not None and t["bbox"][0] - prev_x1 > col_gap:
                lines.append(segment)
                segment = []
            segment.append(t)
            prev_x1 = max(prev_x1 or t["bbox"][2], t["bbox"][2])
        if segment:
            lines.append(segment)

    rows: list[dict] = []
    dash_rows_seen = False
    for line in lines:
        tokens = [(t, _norm(t["text"])) for t in line]
        dash_idx = None
        inline_rest = None  # хвост токена «-текст», если дефис слит с подписью
        for i, (t, txt) in enumerate(tokens):
            m_inline = re.match(r"^([\-–—])\s+(\S.*)$", txt)
            if DASH_TOKEN_RE.match(txt) or m_inline:
                if all(SYMBOL_TOKEN_RE.match(pt) for _, pt in tokens[:i]):
                    dash_idx = i
                    if m_inline:
                        inline_rest = m_inline.group(2)
                break  # смотрим только первый дефис-кандидат
        if dash_idx is not None and (inline_rest or dash_idx + 1 < len(tokens)):
            label_tokens = [t for t, _ in tokens[dash_idx + 1:]]
            bbox_tokens = label_tokens or [tokens[dash_idx][0]]
            if inline_rest:
                bbox_tokens = [tokens[dash_idx][0]] + label_tokens
            body = _join_dedup([inline_rest] if inline_rest else [], label_tokens)
            if len(body) < 4:
                continue
            rows.append({
                "text": body,
                "bbox": _union_bbox([t["bbox"] for t in bbox_tokens]),
                "layer": bbox_tokens[0]["layer"],
                "assembled_from_spans": len(bbox_tokens),
            })
            dash_rows_seen = True
            continue
        # перенос: приклеиваем к последней строке СВОЕЙ колонки
        # (x-диапазоны пересекаются) на расстоянии до 2.6 высоты строки
        lx0, lx1 = line[0]["bbox"][0], max(t["bbox"][2] for t in line)
        host = None
        for row in reversed(rows):
            if 0 < line[0]["bbox"][1] - row["bbox"][1] <= 2.6 * line_h \
                    and lx0 <= row["bbox"][2] + 8 and lx1 >= row["bbox"][0] - 8:
                host = row
                break
        if host is not None:
            tail = _join_tokens_dedup([t for t, _ in tokens])
            if tail and _canon(tail) not in _canon(host["text"]):
                host["text"] = _norm(host["text"] + " " + tail)
                host["bbox"] = _union_bbox([host["bbox"]] +
                                           [t["bbox"] for t, _ in tokens])
    if not dash_rows_seen:
        # диалект легенды без дефисов: строкой считается каждая линия,
        # начинающаяся словом (не числом) — символ подтвердится колонкой
        for line in lines:
            body = _join_tokens([t for t, _ in [(t, _norm(t["text"])) for t in line]])
            body = _norm(body)
            if len(body) >= 4 and not body[0].isdigit() and not HEADER_RE.match(body):
                rows.append({
                    "text": body,
                    "bbox": _union_bbox([t["bbox"] for t in line]),
                    "layer": line[0]["layer"],
                    "assembled_from_spans": len(line),
                })
    return rows


def _join_tokens(tokens: list[dict]) -> str:
    parts = []
    prev_x1 = None
    for t in tokens:
        txt = t["text"]
        if prev_x1 is not None and t["bbox"][0] - prev_x1 > 0.35 * max(
                t["bbox"][3] - t["bbox"][1], 3.0) and parts and not parts[-1].endswith(" "):
            parts.append(" ")
        parts.append(txt)
        prev_x1 = t["bbox"][2]
    return _norm("".join(parts))


def _span_overlap_fraction(left: dict, right: dict) -> float:
    lb, rb = left["bbox"], right["bbox"]
    width = max(0.0, min(lb[2], rb[2]) - max(lb[0], rb[0]))
    height = max(0.0, min(lb[3], rb[3]) - max(lb[1], rb[1]))
    left_area = max(0.0, lb[2] - lb[0]) * max(0.0, lb[3] - lb[1])
    right_area = max(0.0, rb[2] - rb[0]) * max(0.0, rb[3] - rb[1])
    smaller_area = min(left_area, right_area)
    return width * height / smaller_area if smaller_area > 0 else 0.0


def _punctuation_follows_skipped_span(token: dict, skipped: list[dict]) -> bool:
    tb = token["bbox"]
    token_layer = token.get("layer") or ""
    token_h = max(tb[3] - tb[1], 1.0)
    token_cy = (tb[1] + tb[3]) / 2
    for span in skipped:
        sb = span["bbox"]
        span_h = max(sb[3] - sb[1], 1.0)
        gap = tb[0] - sb[2]
        if (token_layer == (span.get("layer") or "")
                and -0.25 * token_h <= gap <= 0.75 * max(token_h, span_h)
                and abs(token_cy - (sb[1] + sb[3]) / 2) <= 0.5 * max(token_h, span_h)):
            return True
    return False


def _join_tokens_dedup(tokens: list[dict]) -> str:
    """Геометрическая склейка с подавлением наложенных CAD-дублей.

    В отличие от _join_dedup сохраняет исходные промежутки между
    неповторяющимися спанами, включая отдельно вынесенную пунктуацию.
    """
    kept: list[dict] = []
    skipped: list[dict] = []
    body = ""
    for token in tokens:
        piece = _norm(token["text"])
        if not piece:
            continue
        canon_piece = _canon(piece)
        duplicate = canon_piece and any(
            canon_piece == _canon(_norm(prior["text"]))
            and _span_overlap_fraction(token, prior) >= 0.75
            for prior in kept
        )
        if duplicate:
            skipped.append(token)
            continue
        if (not canon_piece and body.rstrip().endswith(piece)
                and _punctuation_follows_skipped_span(token, skipped)):
            continue
        kept.append(token)
        body = _join_tokens(kept)
    return body


def _join_dedup(prefix_parts: list[str], tokens: list[dict]) -> str:
    """Склейка с подавлением дублей подписи из параллельных CAD-слоёв:
    токен, чей текст уже содержится в собранной строке, пропускается."""
    body = _norm(" ".join(prefix_parts))
    for t in tokens:
        piece = _norm(t["text"])
        if not piece:
            continue
        if _canon(piece) and _canon(piece) in _canon(body):
            continue
        body = _norm(body + " " + piece)
    return body


def _merge_sections(sections: list[dict], ref: dict) -> tuple[list[dict], list[dict]]:
    """Строки всех секций c дедупликацией; отчёт по секциям для metrics.

    Профильные шаблоны берутся из ВСЕХ секций (потолочные строки и
    электрические строки могут жить в разных разделах листа). Если один
    и тот же вид встречается в двух секциях с несовместимым текстом
    подписи — GEOMETRY_CONFLICT, молча не выбираем.
    """
    labels: list[dict] = []
    seen_rows: set[tuple[str, int]] = set()
    report = []
    kind_sources: dict[str, list[str]] = collections.defaultdict(list)
    for si, section in enumerate(sections):
        profile_rows = 0
        for row in section["rows"]:
            key = (_canon(row["text"]), int(row["bbox"][1] // 6))
            if key in seen_rows:
                continue  # дубль подписи в параллельном слое
            seen_rows.add(key)
            kind = kind_of_label(row["text"])
            row["kind"] = kind
            row["section_index"] = si
            labels.append(row)
            if kind in PROFILE_KINDS:
                profile_rows += 1
                kind_sources[kind].append(_canon(row["text"]))
        report.append({
            "header_bbox": section["header_bbox"],
            "rows_total": len(section["rows"]),
            "profile_rows": profile_rows,
        })
    for kind, texts_ in kind_sources.items():
        uniq = sorted(set(texts_))
        if len(uniq) > 1:
            ref["conflicts"].append({
                "type": "GEOMETRY_CONFLICT",
                "what": f"легенда: вид «{kind}» описан двумя разными подписями",
                "candidates": uniq,
                "detail": "разделы «Условные обозначения» дали несовместимые строки; "
                          "приоритет не выбирался",
            })
    labels.sort(key=lambda item: (item["bbox"][1], item["bbox"][0]))
    return labels, report


def _union_bbox(boxes):
    return (round(min(b[0] for b in boxes), 2), round(min(b[1] for b in boxes), 2),
            round(max(b[2] for b in boxes), 2), round(max(b[3] for b in boxes), 2))


def _cluster_zones(boxes, gap: float = 60.0) -> list[tuple]:
    """Несколько компактных зон легенды вместо одного гигантского union."""
    zones: list[list] = []
    for b in sorted(boxes, key=lambda b: (b[1], b[0])):
        placed = False
        for z in zones:
            if not (b[2] < z[0] - gap or b[0] > z[2] + gap
                    or b[3] < z[1] - gap or b[1] > z[3] + gap):
                z[0] = min(z[0], b[0]); z[1] = min(z[1], b[1])
                z[2] = max(z[2], b[2]); z[3] = max(z[3], b[3])
                placed = True
                break
        if not placed:
            zones.append(list(b))
    return [tuple(round(v, 2) for v in z) for z in zones]


def _build_templates(inv: dict, labels: list[dict]) -> list[dict]:
    """Эталонная векторная сигнатура для каждой строки легенды.

    Символы легенды крупнее шага строк и вертикально перекрываются,
    поэтому раскрой не «полосами», а назначением каждого элемента колонки
    ближайшему центру строки (по y-центру элемента). Колонка символов
    строится per-секция (у секций разные отступы).
    """
    templates = []
    if not labels:
        return templates
    by_section: dict[int, list[dict]] = collections.defaultdict(list)
    for lab in labels:
        by_section[lab.get("section_index", 0)].append(lab)

    for si in sorted(by_section):
        sec_labels = by_section[si]
        # широкая колонка для сбора кандидатов; точная полоса — per-row,
        # потому что подписи разных строк секции начинаются с разных x
        x1 = max(lab["bbox"][0] for lab in sec_labels) - 0.5
        x0 = min(lab["bbox"][0] for lab in sec_labels) - 72
        y0 = min(lab["bbox"][1] for lab in sec_labels) - 14
        y1 = max(lab["bbox"][3] for lab in sec_labels) + 14
        column = (x0, y0, x1, y1)
        centers = [((lab["bbox"][1] + lab["bbox"][3]) / 2) for lab in sec_labels]
        # допуск по y — доля медианного шага строк секции, не абсолют:
        # образцы соседних строк (красные выноски примеров) не должны
        # прилипать к чужому символу
        diffs = sorted(b - a for a, b in zip(sorted(centers), sorted(centers)[1:]) if b - a > 1.0)
        pitch = diffs[len(diffs) // 2] if diffs else 14.0
        dy_max = max(6.0, 0.5 * pitch)

        # цветовой фильтр полосы символа согласован с планом: устройства
        # рисуются red/green, потолочные марки — blue (+чёрная рамка);
        # чужие образцы (чёрные размеры соседних строк) в сигнатуру не входят
        def color_ok(kind: str | None, obj, kind_name: str) -> bool:
            if kind_name == "text":
                return True
            fam = obj["ref"].get("color_family")
            if kind in ("ceiling_type_tag", "ceiling_elevation_tag"):
                return fam in ("blue", "black")
            if kind in PROFILE_KINDS or kind == "smoke_detector":
                return fam in ("red", "green")
            return True

        row_zones: dict[int, list] = {idx: [] for idx in range(len(sec_labels))}
        for kind_name, objects in _column_objects(inv, column):
            for obj in objects:
                # кандидаты — только строки, в чью полосу символов (левее
                # начала подписи) объект попадает по x; из них ближайшая по y
                cy = (obj["bbox"][1] + obj["bbox"][3]) / 2
                best_idx = None
                best_dy = 1e9
                for idx, lab in enumerate(sec_labels):
                    lab_x0 = lab["bbox"][0]
                    if not (lab_x0 - 72 <= obj["bbox"][0] and obj["bbox"][2] <= lab_x0 - 0.5):
                        continue
                    if not color_ok(lab.get("kind"), obj, kind_name):
                        continue
                    dy = abs(centers[idx] - cy)
                    if dy < best_dy:
                        best_dy = dy
                        best_idx = idx
                if best_idx is not None and best_dy <= dy_max:
                    row_zones[best_idx].append((kind_name, obj))

        for idx, lab in enumerate(sec_labels):
            sig = _signature_of_objects(row_zones[idx])
            boxes = [obj["bbox"] for _, obj in row_zones[idx]] or [lab["bbox"]]
            templates.append({
                "kind": lab.get("kind") or "unresolved_legend_row",
                "label": lab["text"],
                "label_bbox": lab["bbox"],
                "symbol_zone": _union_bbox(boxes),
                "signature": sig,
                "source": "sheet_legend",
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
    layers = collections.Counter()
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
            layers[obj["ref"].get("layer") or ""] += 1
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
        "layers": dict(sorted(layers.items())),
        "inner_letters": sorted(set(inner_letters)),
        "inner_digits": sorted(inner_digits),
        "inner_elevations": sorted(inner_elev),
    }


def _parse_notes(texts, notes_header, *, right_limit: float = 10 ** 9) -> list[dict]:
    if notes_header is None:
        # диалект без заголовка «Примечания»: нумерованные правила листа
        rows = [t for t in texts
                if re.match(r"^\d{1,2}\s*[-.]\s+\S", _norm(t["text"]))
                and len(_norm(t["text"])) > 25 and t["bbox"][2] <= right_limit]
        rules = []
        for t in sorted(rows, key=lambda t: t["bbox"][1]):
            m = re.match(r"^(\d{1,2})\s*[-.]\s*(.+)$", _norm(t["text"]))
            if m and PROFILE_WORDS_RE.search(m.group(2)):
                rules.append({"no": int(m.group(1)), "text": _norm(m.group(2))})
        return rules
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


def _parse_room_schedule(texts, in_reference) -> tuple[dict, str]:
    """Ведомость «Наим. помещения»: строки марка → наименование.

    Таблица живёт в справочной области (правая колонка либо за CropBox —
    справа или снизу); критерий один — вне области плана."""
    schedule: dict[str, dict] = {}
    marks = [t for t in texts if MARK_RE.match(_norm(t["text"]))
             and in_reference(t["bbox"])]
    floors = ""
    for t in texts:
        m = re.match(r"^(\d+\s*[-–]\s*\d+\s+этаж)", _norm(t["text"]), re.I)
        if m and in_reference(t["bbox"]):
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


def _parse_sheet_meta(texts, cp, ref_col_x0: float) -> dict:
    """Метаданные из штампа и заголовков справочной области."""
    meta = {"doc_number": None, "sheet_name": None, "building": None,
            "zero_level": None, "sheet_no": None, "address": None}
    stamp_x0 = max(ref_col_x0, cp.media_rect[2] - 700)
    stamp = [t for t in texts if t["bbox"][0] >= stamp_x0 - 60
             and t["bbox"][1] > cp.media_rect[3] * 0.55]
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
