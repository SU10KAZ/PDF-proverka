"""Строки таблиц чертежа по линиям сетки — без универсального разбора таблиц.

Строка — полоса между ближайшими горизонтальными линиями, которые проходят через
столбец указателя (в CAD-таблицах разделители строк часто нарезаны по столбцам,
поэтому линии «во всю ширину» искать нельзя). Ширина строки — цепочка отрезков
этих линий, ячейки — вертикальные линии внутри полосы. Заголовок — ближайшая
полоса выше, где есть слова заголовка («Поз.», «Наименование», «Кол.» …).

Координаты — pt видимой страницы (после /Rotate), как у слов профиля.
"""
from __future__ import annotations

import re
from typing import Any

import fitz

ROW_MAX_PT = 420.0       # выше — не строка таблицы, а область листа
CHAIN_GAP_PT = 3.0       # разрыв отрезков одной линии
SAME_Y_PT = 0.8
HEADER_WORDS = re.compile(r"^(поз|обозначен|наименован|кол|марка|тип|примечан|площад|ед\.?|масса|№|номер|"
                          r"код|состав|схема|данные|материал)", re.IGNORECASE)
MAX_ROWS = 40


class Rulings:
    """Горизонтальные и вертикальные отрезки страницы в видимой системе координат."""

    def __init__(self, page: fitz.Page):
        m = page.rotation_matrix
        self.h: list[tuple[float, float, float]] = []   # (x0, x1, y)
        self.v: list[tuple[float, float, float]] = []   # (x, y0, y1)
        for d in page.get_drawings():
            for it in d.get("items") or []:
                if it[0] == "l":
                    p, q = it[1] * m, it[2] * m
                    self._add(p.x, p.y, q.x, q.y)
                elif it[0] == "re":
                    r = it[1] * m
                    r.normalize()
                    if r.height < 1.2 and r.width > 2:
                        self._add(r.x0, (r.y0 + r.y1) / 2, r.x1, (r.y0 + r.y1) / 2)
                    elif r.width < 1.2 and r.height > 2:
                        self._add((r.x0 + r.x1) / 2, r.y0, (r.x0 + r.x1) / 2, r.y1)
                    elif r.width > 2 and r.height > 2:
                        self._add(r.x0, r.y0, r.x1, r.y0)
                        self._add(r.x0, r.y1, r.x1, r.y1)
                        self._add(r.x0, r.y0, r.x0, r.y1)
                        self._add(r.x1, r.y0, r.x1, r.y1)

    def _add(self, x0: float, y0: float, x1: float, y1: float) -> None:
        if abs(y0 - y1) < 0.6 and abs(x1 - x0) > 1.5:
            self.h.append((min(x0, x1), max(x0, x1), (y0 + y1) / 2))
        elif abs(x0 - x1) < 0.6 and abs(y1 - y0) > 1.5:
            self.v.append(((x0 + x1) / 2, min(y0, y1), max(y0, y1)))

    def h_through(self, x: float) -> list[float]:
        return sorted({round(y, 1) for a, b, y in self.h if a - 0.5 <= x <= b + 0.5})

    def chain(self, y: float, x: float) -> tuple[float, float] | None:
        """Отрезок линии на высоте ``y``, содержащий ``x``, с учётом нарезки по столбцам."""
        segs = sorted((a, b) for a, b, yy in self.h if abs(yy - y) <= SAME_Y_PT)
        merged: list[list[float]] = []
        for a, b in segs:
            if merged and a <= merged[-1][1] + CHAIN_GAP_PT:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        for a, b in merged:
            if a - 0.5 <= x <= b + 0.5:
                return a, b
        return None

    def v_in(self, x0: float, x1: float, y0: float, y1: float) -> list[float]:
        mid = (y0 + y1) / 2
        return sorted({round(x, 1) for x, a, b in self.v if x0 + 1 < x < x1 - 1 and a <= mid <= b})


def _band(rul: Rulings, x: float, y: float) -> tuple[float, float] | None:
    ys = rul.h_through(x)
    top = max((v for v in ys if v < y - 0.5), default=None)
    bot = min((v for v in ys if v > y + 0.5), default=None)
    if top is None or bot is None or bot - top > ROW_MAX_PT:
        return None
    return top, bot


def _lines(words: list[list[Any]]) -> str:
    rows: list[list[Any]] = []
    for w in sorted(words, key=lambda w: ((w[1] + w[3]) / 2, w[0])):
        cy = (w[1] + w[3]) / 2
        if rows and abs(((rows[-1][0][1] + rows[-1][0][3]) / 2) - cy) <= 2.5:
            rows[-1].append(w)
        else:
            rows.append([w])
    return " / ".join(" ".join(str(w[4]) for w in sorted(r, key=lambda w: w[0])) for r in rows)


def row_at(rul: Rulings, words: list[list[Any]], box: list[float]) -> dict[str, Any] | None:
    """Строка таблицы, в которую попадает рамка ``box``; ``None`` — не таблица."""
    cx, cy = (box[0] + box[2]) / 2, (box[1] + box[3]) / 2
    band = _band(rul, cx, cy)
    if not band:
        return None
    top, bot = band
    ext = [rul.chain(top, cx), rul.chain(bot, cx)]
    ext = [e for e in ext if e]
    if not ext:
        return None
    x0, x1 = min(e[0] for e in ext), max(e[1] for e in ext)
    if x1 - x0 < 30:
        return None
    return _row(rul, words, x0, x1, top, bot)


def _row(rul: Rulings, words: list[list[Any]], x0: float, x1: float, top: float, bot: float) -> dict[str, Any]:
    xs = [x0] + rul.v_in(x0, x1, top, bot) + [x1]
    inside = [w for w in words if x0 <= (w[0] + w[2]) / 2 <= x1 and top <= (w[1] + w[3]) / 2 <= bot]
    cells = []
    for a, b in zip(xs, xs[1:]):
        cw = [w for w in inside if a <= (w[0] + w[2]) / 2 < b]
        if cw:
            cells.append({"x": [round(a, 1), round(b, 1)], "text": _lines(cw)})
    return {"box": [round(x0, 1), round(top, 1), round(x1, 1), round(bot, 1)], "cells": cells,
            "text": " ¦ ".join(c["text"] for c in cells), "graphic_cell": False}


def header_for(rul: Rulings, words: list[list[Any]], row: dict[str, Any]) -> dict[str, Any] | None:
    """Ближайшая полоса выше строки со словами заголовка (не дальше 20 полос)."""
    x0, top, x1, _ = row["box"]
    cx = (row["cells"][0]["x"][0] + row["cells"][0]["x"][1]) / 2 if row["cells"] else (x0 + x1) / 2
    y = top
    for _ in range(20):
        band = _band(rul, cx, y - 1.0)
        if not band:
            return None
        hdr = _row(rul, words, x0, x1, band[0], band[1])
        first = [str(w[4]) for w in words if x0 <= (w[0] + w[2]) / 2 <= x1 and band[0] <= (w[1] + w[3]) / 2 <= band[1]]
        kinds = {HEADER_WORDS.match(t).group(1).lower() for t in first if len(t) <= 12 and HEADER_WORDS.match(t)}
        if len(kinds) >= 2 and len(first) <= 30:
            return hdr
        y = band[0]
    return None


def rows_below(rul: Rulings, words: list[list[Any]], header: dict[str, Any], key_x: float,
               x0: float, x1: float) -> list[dict[str, Any]]:
    """Все строки таблицы под заголовком (по столбцу ключа) — для небольших таблиц целиком."""
    out = []
    y = header["box"][3]
    for _ in range(MAX_ROWS):
        band = _band(rul, key_x, y + 1.0)
        if not band:
            break
        # Конец таблицы: нижняя линия полосы уже, чем таблица, или в полосе нет разделителей столбцов.
        bottom = rul.chain(band[1], key_x)
        if not bottom or (bottom[1] - bottom[0]) < 0.6 * (x1 - x0):
            break
        heights = sorted(r["box"][3] - r["box"][1] for r in out)
        if heights and band[1] - band[0] > max(3 * heights[len(heights) // 2], 80):
            break                            # полоса намного выше строк — это уже не таблица
        if not rul.v_in(x0, x1, band[0], band[1]):
            if band[1] - band[0] <= 25:      # строка-раздел («Лючки металлические внутренние»)
                y = band[1]
                continue
            break
        row = _row(rul, words, x0, x1, band[0], band[1])
        if not row["cells"]:
            if band[1] - band[0] > 60:
                break
        else:
            out.append(row)
        y = band[1]
    return out


MARK = re.compile(r"^[A-Za-zА-Яа-яЁё]{1,4}[-.]?[0-9][0-9A-Za-zА-Яа-яЁё*/.\-]*$")


def row_key(row: dict[str, Any]) -> str:
    """Ключ строки для предварительного сопоставления: первая короткая ячейка-обозначение."""
    for c in row["cells"][:3]:
        t = c["text"].split(" / ")[0].strip()
        if t and len(t) <= 18 and (MARK.match(t) or re.match(r"^\d+(\.\d+)*\*?$", t)):
            return t
    return ""
