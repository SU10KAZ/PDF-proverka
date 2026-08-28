"""Разметка экспликации помещений, доказанная самим документом.

Модуль отвечает на два вопроса и больше ни на что:
  * сколько колонок объявил заголовок таблицы (и сколько раз он повторён);
  * раскладывается ли строка на полные помещения.

Оба ответа нужны и подготовке текста (чтобы разрезать сдвоенную строку до
сравнения), и производителю фактов (чтобы прочитать колонки). Общий модуль
избавляет их от кольцевого импорта и от двух расходящихся копий регулярок.
"""
from __future__ import annotations

import re
from typing import Any, Mapping

from .text_comparison import canonicalize_text

# Экспликация подписывает свои колонки сама, и у заголовка ровно столько ячеек,
# сколько у строк данных.  Отсутствовать может только четвёртая колонка и
# только с хвоста: у строки с пустой категорией остаётся три ячейки.
HEADER_COLUMNS = (
    re.compile(r"^номер(\s+помещения)?$"),
    re.compile(r"^наименование"),
    re.compile(r"^площад"),
    re.compile(r"^кат"),
)
# «28.1», «01.62г», «Б2.14» — номер помещения, но никогда голое целое: голое
# целое неотличимо от площади, а площадь стоит в третьей колонке.
ROOM_CODE_RE = re.compile(r"^[а-яa-z]?\d{1,3}(?:\.\d{1,3}){1,3}[а-яa-z]?$", re.I)
ROOM_AREA_RE = re.compile(r"^\d{1,6}(?:[.,]\d{1,3})?(?:\s*(?:м2|м²|m2))?$", re.I)
# Одна сторона пишет «288.62 м2», другая «185,03» про ту же колонку.  Единицу
# снимаем здесь и возвращаем один раз при нормализации значения, иначе две
# стороны неизменившейся площади разойдутся и выдумают изменение.
ROOM_AREA_UNIT_RE = re.compile(r"\s*(?:м2|м²|m2)\s*$", re.I)
ROOM_CATEGORY_RE = re.compile(r"^[абвгдabvgd]\s*[1-4]?$", re.I)
_BARE_NUMBER_RE = re.compile(r"^[+-]?\d+(?:[.,]\d+)?$")


def row_cells(fragment: Mapping[str, Any]) -> list[str]:
    return [
        canonicalize_text(str(value))
        for value in fragment.get("location_parts") or []
    ]


def header_units(fragment: Mapping[str, Any]) -> list[int] | None:
    """Ширины всех единиц «номер | наименование | площадь [| кат.]» заголовка.

    Лист «в две колонки» печатает один и тот же заголовок дважды в одной
    строке. Это ЧТЕНИЕ границы второй таблицы, а не догадка о ней: строка
    разбирается слева направо, и заголовок, оборвавшийся на середине единицы,
    отвергается целиком.
    """
    parts = row_cells(fragment)
    if len(parts) < 3:
        return None
    index = 0
    units: list[int] = []
    while index < len(parts):
        if not HEADER_COLUMNS[0].match(parts[index]):
            return None
        if index + 2 >= len(parts):
            return None
        if not HEADER_COLUMNS[1].match(parts[index + 1]):
            return None
        if not HEADER_COLUMNS[2].match(parts[index + 2]):
            return None
        width = 3
        if index + 3 < len(parts) and HEADER_COLUMNS[3].match(parts[index + 3]):
            width = 4
        units.append(width)
        index += width
    return units or None


def header_width(fragment: Mapping[str, Any]) -> int | None:
    """Самая широкая единица, доказанная этим заголовком."""
    units = header_units(fragment)
    return max(units) if units else None


def looks_like_header(fragment: Mapping[str, Any]) -> bool:
    parts = row_cells(fragment)
    return bool(parts) and HEADER_COLUMNS[0].match(parts[0]) is not None


def unit_is_valid(unit: list[str], width: int) -> bool:
    if not 3 <= len(unit) <= width:
        return False
    if not ROOM_CODE_RE.match(unit[0]):
        return False
    if not unit[1] or _BARE_NUMBER_RE.match(unit[1]):
        return False
    if not ROOM_AREA_RE.match(unit[2]):
        return False
    if len(unit) == 4 and not ROOM_CATEGORY_RE.match(unit[3]):
        return False
    return True


def row_units(parts: list[str], width: int) -> list[list[str]] | None:
    """Разложить строку на полные помещения или отказаться.

    Строка потребляется слева направо, на каждой позиции побеждает самая
    широкая единица, которая проходит проверку. Хвоста не остаётся: строка,
    закончившаяся ячейками, не образующими помещения, отвергается целиком —
    иначе бесхозное число прилипнет к предыдущему помещению как его площадь.

    Именно это делает читаемой строку «02.1 Рампа 185,03 B2 02.42 Коридор
    44,10»: второе помещение сохраняет собственную площадь, а не отдаёт её
    первому.
    """
    if not parts or width < 3:
        return None
    output: list[list[str]] = []
    index = 0
    while index < len(parts):
        taken: list[str] | None = None
        for candidate in ((4, 3) if width >= 4 else (3,)):
            unit = parts[index:index + candidate]
            if len(unit) == candidate and unit_is_valid(unit, width):
                taken = unit
                break
        if taken is None:
            return None
        output.append(taken)
        index += len(taken)
    return output or None


__all__ = [
    "HEADER_COLUMNS",
    "ROOM_AREA_RE",
    "ROOM_AREA_UNIT_RE",
    "ROOM_CATEGORY_RE",
    "ROOM_CODE_RE",
    "header_units",
    "header_width",
    "looks_like_header",
    "row_cells",
    "row_units",
    "unit_is_valid",
]
