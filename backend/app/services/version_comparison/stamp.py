"""Основная надпись (ГОСТ Р 21.101) и служебные зоны листа по тексту и векторам.

Разбор якорный: ищется строка подписей «Изм. Кол.уч. Лист №док. Подп. Дата»,
справа — «Стадия / Лист / Листов». От них строятся зона штампа, обозначение,
номер и название листа и строки таблицы изменений. Привязки к конкретному
проекту нет. Если подписи не найдены — штамп считается нераспознанным, а при
наличии рамки ГОСТ берётся зона штампа по умолчанию с пометкой.

Все координаты — повёрнутая система страницы, pt.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

MM = 72.0 / 25.4

_LABEL_ROW = {"изм.", "изм", "кол.уч.", "кол.уч", "кол. уч.", "лист", "№док.", "№ док.", "№док",
              "подп.", "подпись", "дата"}
_RIGHT_LABELS = {"стадия", "листов"}
_SIGN_ROLES = ("разраб", "провер", "н.контр", "н. контр", "гип", "утв", "нач.отд", "составил")
_IZM_KIND = {"зам.", "зам", "нов.", "нов", "изм.", "аннул.", "аннул"}
_DESIGNATION = re.compile(r"^(?=[^-]*\w)(?=.*\d)[\w./]+(?:-[\w./]+)+$", re.U)
_DATE_FULL = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")
# Типовые отметки выпуска «В производство работ» (практика РФ). Расширяемо.
RELEASE_PATTERNS = (re.compile(r"в\s*производств", re.I), re.compile(r"к\s*производств", re.I))


def _low(text: str) -> str:
    return (text or "").strip().lower()


def _cx(w) -> float:
    return (w[0] + w[2]) / 2


def _cy(w) -> float:
    return (w[1] + w[3]) / 2


def _union(rects: Iterable[Iterable[float]]) -> list[float] | None:
    rects = [list(r) for r in rects]
    if not rects:
        return None
    return [min(r[0] for r in rects), min(r[1] for r in rects), max(r[2] for r in rects), max(r[3] for r in rects)]


def _lines(words: list[list[Any]], tol: float = 2.0) -> list[list[list[Any]]]:
    rows: list[list[list[Any]]] = []
    for w in sorted(words, key=lambda w: (_cy(w), w[0])):
        if rows and abs(_cy(rows[-1][0]) - _cy(w)) <= tol:
            rows[-1].append(w)
        else:
            rows.append([w])
    return [sorted(r, key=lambda w: w[0]) for r in rows]


def _label_row(corner: list[list[Any]]) -> list[list[Any]] | None:
    best = None
    for row in _lines(corner):
        labels = [w for w in row if _low(w[4]) in _LABEL_ROW]
        kinds = {_low(w[4]).rstrip(".").replace(" ", "") for w in labels}
        if len(kinds) >= 3 and (best is None or _cy(row[0]) > _cy(best[0])):
            best = labels
    return best


def detect_frame(page_w: float, page_h: float, segments: list[tuple[float, float, float, float]]) -> list[float] | None:
    """Рамка ГОСТ по длинным линиям у краёв: слева ≈20 мм, остальные ≈5 мм."""
    def near(values, lo, hi):
        hits = [v for v in values if lo <= v <= hi]
        return min(hits, key=lambda v: abs(v - (lo + hi) / 2)) if hits else None

    verticals = [x0 for x0, y0, x1, y1 in segments if abs(x0 - x1) < 0.8 and abs(y1 - y0) >= 0.4 * page_h]
    horizontals = [y0 for x0, y0, x1, y1 in segments if abs(y0 - y1) < 0.8 and abs(x1 - x0) >= 0.4 * page_w]
    left = near(verticals, 12 * MM, 28 * MM)
    right = near(verticals, page_w - 9 * MM, page_w - 2 * MM)
    top = near(horizontals, 2 * MM, 9 * MM)
    bottom = near(horizontals, page_h - 9 * MM, page_h - 2 * MM)
    found = [v is not None for v in (left, right, top, bottom)]
    if sum(found) < 3:
        return None
    return [left if left is not None else 20 * MM, top if top is not None else 5 * MM,
            right if right is not None else page_w - 5 * MM, bottom if bottom is not None else page_h - 5 * MM]


def parse(words: list[list[Any]], page_w: float, page_h: float, *, frame: list[float] | None = None) -> dict[str, Any]:
    """Штамп листа. Возвращает словарь с зоной, обозначением, номером, названием."""
    corner = [w for w in words if w[0] > page_w - 205 * MM and w[1] > page_h - 70 * MM]
    result: dict[str, Any] = {"recognized": False, "form": "none", "zone": None, "designation": "",
                              "sheet_no": "", "title": "", "izm_rows": [], "izm_max": None}
    labels = _label_row(corner)
    right_edge = frame[2] if frame else page_w
    bottom_edge = frame[3] if frame else page_h
    if not labels:
        if frame:
            result["zone"] = [right_edge - 185 * MM - 1 * MM, bottom_edge - 55 * MM - 1 * MM, page_w, page_h]
            result["form"] = "default"
        return result
    label_y = max(_cy(w) for w in labels)
    label_left = min(w[0] for w in labels)
    label_right = max(w[2] for w in labels)
    short = label_y > bottom_edge - 9 * MM  # форма 6: подписи — нижняя строка
    if short:
        top = bottom_edge - 15 * MM - 1.5 * MM
    else:
        top = max(bottom_edge - 55 * MM - 1.5 * MM, label_y - 24 * MM)
    zone = [label_left - 1.5 * MM, top, page_w, page_h]
    result.update(recognized=True, form="short" if short else "main", zone=zone)
    inside = [w for w in words if w[0] >= zone[0] - 0.5 and _cy(w) >= zone[1] and w[1] <= zone[3]]

    # Правая колонка: метка «Стадия» (основная форма) или «Лист» справа.
    stadia = [w for w in inside if _low(w[4]) in _RIGHT_LABELS]
    sheet_labels = [w for w in inside if _low(w[4]) == "лист" and w not in labels]
    if sheet_labels:
        lab = max(sheet_labels, key=lambda w: w[0])
        values = [w for w in inside
                  if re.fullmatch(r"\d{1,3}(\.\d{1,2})?[а-яa-z]?", _low(w[4]))
                  and abs(_cx(w) - _cx(lab)) < 7 * MM and 0 < _cy(w) - _cy(lab) < 9 * MM]
        if values:
            result["sheet_no"] = min(values, key=lambda w: _cy(w) - _cy(lab))[4]

    # Обозначение: самый длинный токен вида «133/23-ГК-ЭМ2»; склейка переносов «...-» + «ЭМ2».
    # Ищем правее таблицы изменений: в ней строки вида «1 зам. - 101/25» похожи на шифр.
    joined: list[str] = []
    for row in _lines([w for w in inside if w[0] >= label_right]):
        buf = ""
        for w in row:
            t = w[4].strip()
            buf = buf + t if buf and buf.endswith("-") else t
            if _DESIGNATION.match(buf) and len(buf) >= 6:
                joined.append(buf)
    if joined:
        result["designation"] = max(joined, key=len)

    # Название листа: нижняя строка средней колонки (основная форма).
    if not short:
        x_mid0 = label_right + 1 * MM
        x_mid1 = (min(w[0] for w in stadia) - 1 * MM) if stadia else page_w - 50 * MM
        # Ниже линии рамки — подпись формата («Формат А3», «420х297мм»), не название.
        title_words = [w for w in inside if x_mid0 <= _cx(w) <= x_mid1
                       and w[1] >= bottom_edge - 16 * MM and _cy(w) <= bottom_edge]
        result["title"] = " ".join(" ".join(x[4] for x in row) for row in _lines(title_words)).strip()

    # Строки таблицы изменений: левая часть, выше строки подписей.
    rows = []
    for row in _lines([w for w in inside if _cx(w) <= label_right + 1 * MM and _cy(w) < label_y - 1]):
        tokens = [_low(w[4]) for w in row]
        if tokens and re.fullmatch(r"\d{1,2}", tokens[0]) and any(t in _IZM_KIND for t in tokens[1:4]):
            rows.append({"izm": int(tokens[0]), "kind": tokens[1] if len(tokens) > 1 else "",
                         "text": " ".join(w[4] for w in row)})
    result["izm_rows"] = rows
    result["izm_max"] = max((r["izm"] for r in rows), default=None)
    return result


def release_blocks(blocks: list[list[Any]]) -> list[dict[str, Any]]:
    """Отметки выпуска («В производство работ»): блок с отметкой и соседние блоки.

    Отметка обычно разбита на несколько текстовых блоков (заголовок, должность,
    фамилия с датой), поэтому к блоку-заголовку присоединяются блоки, которые
    лежат ниже или выше не дальше 12 мм и перекрываются с ним по горизонтали.
    """
    items = [([float(v) for v in b[:4]], " ".join(str(b[4]).split())) for b in blocks]
    out = []
    for bbox, text in items:
        if not any(p.search(text) for p in RELEASE_PATTERNS):
            continue
        cluster = [bbox]
        texts = [text]
        changed = True
        while changed:
            changed = False
            box = _union(cluster)
            for other, other_text in items:
                if other in cluster:
                    continue
                h_overlap = min(box[2], other[2]) - max(box[0], other[0]) > -10 * MM
                v_gap = max(other[1] - box[3], box[1] - other[3])
                if h_overlap and v_gap <= 12 * MM and (other[2] - other[0]) < 120 * MM:
                    cluster.append(other)
                    texts.append(other_text)
                    changed = True
        joined = " ".join(texts)
        dates = _DATE_FULL.findall(joined)
        out.append({"bbox": _union(cluster), "text": joined, "date": dates[-1] if dates else ""})
    return out


def classify_kind(words: list[list[Any]], stamp: dict[str, Any], page_w: float, page_h: float) -> str:
    top = {_low(w[4]) for w in words if w[1] < page_h * 0.3}
    designation = stamp.get("designation") or ""
    if designation.upper().endswith(".СО") or ({"поз."} & top and any(t.startswith("наименование") for t in top)
                                               and any(t.startswith("кол") for t in top)):
        return "spec"
    first = [_low(w[4]) for w in sorted(words, key=lambda w: (w[1], w[0]))[:12]]
    if "разрешение" in first:
        return "permit"
    if stamp.get("form") == "main":
        return "drawing"
    if stamp.get("form") == "short":
        return "text"
    return "title"

