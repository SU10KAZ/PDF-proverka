"""Детерминированный разбор электротехнических значений однолинейной схемы.

Модуль отвечает на один вопрос: «два прочитанных значения — это одно и то же
или разное?». Он ничего не извлекает из чертежа и ничего не утверждает об
изменении; он только приводит уже прочитанное к сравнимому виду и говорит,
доказано ли различие.

Зачем отдельный слой. Вектор-слой CAD-чертежа набран смешанной раскладкой:
марка кабеля «ППГнг(А)-НF» на одном листе написана кириллической «Н»
(U+041D), а на другом — латинской «H» (U+0048). Побайтовое сравнение объявит
это изменением марки кабеля, которого не было. Такое замечание хуже, чем
отсутствие замечания: оно тратит время инженера и подрывает доверие ко всем
остальным. Поэтому сравнение идёт по «скелету» строки, а показывается всегда
исходное написание.

Обратное правило столь же важно: складывать можно только графически
неразличимые буквы. «ВВГ» и «ППГ» — разные марки, и никакая нормализация не
имеет права их сблизить.

Второе сквозное правило — молчание вместо догадки. Если в строке два
кандидата на марку кабеля, марка не возвращается вовсе: «ППГнг(А)-НF» рядом с
«1ГРЩ-ВРУ4» не даёт права выбрать любой из них. Не прочитанное свойство — это
``None``, и сравнение его пропускает, а не объявляет изменившимся.
"""
from __future__ import annotations

import re
from typing import Any


#: Кириллические буквы, графически неотличимые от латинских в шрифтах CAD.
#: Список закрыт и содержит ТОЛЬКО пары-омоглифы: буквы, которые нельзя
#: различить глазом. Похожие, но различимые пары («И»/«N») сюда не входят.
_HOMOGLYPHS = {
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O",
    "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X", "І": "I", "Ј": "J",
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x",
    "і": "i", "ј": "j",
}

#: Разделитель «на» в записи жил и сечений: латинская x, кириллическая х,
#: знак умножения и звёздочка. После свёртки омоглифов кириллическая «х»
#: становится латинской «x», но исходные формы нужны при разборе сырой строки.
_MULTIPLIERS = "xXхХ×*"

_SPACE_RE = re.compile(r"\s+")


def fold_homoglyphs(value: Any) -> str:
    """Свести графически неразличимые буквы к латинским.

    Результат — «скелет» для сравнения, а НЕ текст для показа человеку.
    """
    return "".join(_HOMOGLYPHS.get(char, char) for char in str(value or ""))


def canonical_mark(value: Any) -> str:
    """Марка кабеля в сравнимом виде: без пробелов, регистра и омоглифов."""
    return _SPACE_RE.sub("", fold_homoglyphs(value).upper())


def marks_equal(left: Any, right: Any) -> bool:
    """Одна ли это марка. Разные раскладки одной марки — одна марка."""
    return canonical_mark(left) == canonical_mark(right)


def _number(value: Any) -> float | None:
    text = str(value or "").replace(",", ".").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _as_int(value: float | None) -> int | None:
    if value is None:
        return None
    return int(value) if float(value).is_integer() else None


_NUM = r"\d{1,4}(?:[.,]\d{1,2})?"
_MUL = rf"\s*[{_MULTIPLIERS}]\s*"

#: Порядок шаблонов — от самого доказательного к самому слабому, и первый
#: сработавший выигрывает: «3х(5х120)» обязано читаться как три параллельных
#: кабеля, а не как «3 жилы сечением 5».
#: «3х(5х120)» — параллельные, жилы, сечение.
_PARALLEL_BRACKET_RE = re.compile(
    rf"(?<![\d.,])(\d{{1,2}}){_MUL}\(\s*(\d{{1,3}}){_MUL}({_NUM})\s*\)"
)
#: «3×5×150» — те же три числа без скобок.
_PARALLEL_TRIPLE_RE = re.compile(
    rf"(?<![\d.,])(\d{{1,2}}){_MUL}(\d{{1,3}}){_MUL}({_NUM})(?![\d.,{_MULTIPLIERS}])"
)
#: «5х120» — жилы и сечение.
_CORES_SECTION_RE = re.compile(rf"(?<![\d.,])(\d{{1,3}}){_MUL}({_NUM})(?![\d.,])")
#: «3хППГнг(А)-HF» — число параллельных кабелей перед маркой.
_PARALLEL_MARK_RE = re.compile(
    rf"(?<![\d.,])(\d{{1,2}}){_MUL}(?=[A-Za-zА-Яа-яЁё])"
)
#: Кандидат в марку: буквенно-цифровой кусок, начинающийся с буквы.
_MARK_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё0-9()\-/]*")


def _cable_structure(folded: str) -> tuple[int, bool, int | None, float | None, tuple[int, int] | None]:
    """Параллельные кабели, жилы, сечение и занятый ими участок строки."""
    for pattern in (_PARALLEL_BRACKET_RE, _PARALLEL_TRIPLE_RE):
        match = pattern.search(folded)
        if match:
            return (
                int(match.group(1)),
                True,
                _as_int(_number(match.group(2))),
                _number(match.group(3)),
                match.span(),
            )
    match = _CORES_SECTION_RE.search(folded)
    if match:
        return (
            1,
            False,
            _as_int(_number(match.group(1))),
            _number(match.group(2)),
            match.span(),
        )
    match = _PARALLEL_MARK_RE.search(folded)
    if match:
        return int(match.group(1)), True, None, None, match.span()
    return 1, False, None, None, None


def _mark_span(remainder: str) -> tuple[int, int] | None:
    """Участок марки в остатке строки — только если кандидат ровно один.

    Два кандидата означают, что строка несёт не только марку («1ГРЩ-ВРУ4
    ППГнг(А)-НF»), и выбирать между ними нечем. Тогда марка не прочитана.
    """
    spans = [
        match.span()
        for match in _MARK_TOKEN_RE.finditer(remainder)
        if sum(char.isalpha() for char in match.group()) >= 2
    ]
    return spans[0] if len(spans) == 1 else None


def parse_cable(value: Any) -> dict[str, Any] | None:
    """Разложить запись кабеля на доказуемые части.

    Возвращает ``None``, когда в строке нет ничего, что можно назвать кабелем.
    Возвращаются только те части, которые действительно прочитаны: отсутствие
    сечения — это ``None``, а не догадка.

    Понимаются формы, встречающиеся на однолинейных схемах:
    ``5x120``, ``5х120``, ``3x(5x120)``, ``3×5×150``, ``2х(5х120)``,
    ``3хППГнг(А)-HF``, ``ППГнг(А)-НF 2х(5х120)``.
    """
    raw = str(value or "").strip()
    if not raw:
        return None
    # Свёртка омоглифов заменяет символ на символ, поэтому позиции в `folded` и
    # `raw` совпадают. Разобранный числовой участок затирается пробелами той же
    # длины — выравнивание сохраняется, и марку можно вернуть в ИСХОДНОМ
    # написании, разобрав при этом сложенную строку.
    folded = fold_homoglyphs(raw)
    parallel_count, parallel_proven, cores, section, span = _cable_structure(folded)
    remainder = (
        folded
        if span is None
        else folded[: span[0]] + " " * (span[1] - span[0]) + folded[span[1] :]
    )
    mark_span = _mark_span(remainder)
    mark = raw[mark_span[0] : mark_span[1]] if mark_span else None
    if mark is None and cores is None and section is None:
        return None
    return {
        "raw": raw,
        "parallel_count": parallel_count,
        "parallel_count_proven": parallel_proven,
        "cores": cores,
        "section_mm2": section,
        "mark": mark,
        "mark_canonical": canonical_mark(mark) if mark else None,
    }


#: Части кабеля, о которых можно говорить как об отдельных свойствах.
CABLE_FACETS = ("mark", "parallel_count", "cores", "section_mm2")

_CABLE_FACET_TITLES = {
    "mark": "Марка кабеля",
    "parallel_count": "Число параллельных кабелей",
    "cores": "Число жил",
    "section_mm2": "Сечение жилы, мм²",
}


def cable_facet_title(facet: str) -> str:
    return _CABLE_FACET_TITLES.get(facet, facet)


#: Различие доказано обеими сторонами и может быть утверждением.
PROVEN = "PROVEN"
#: Различие видно, но одна из сторон не объявила свойство явно. Такое идёт
#: человеку вопросом, а не публикуется как факт.
REVIEW = "REVIEW"


def compare_cables(left: Any, right: Any) -> list[dict[str, Any]]:
    """Различия двух записей кабеля — по одному на свойство.

    Свойство, не прочитанное хотя бы с одной стороны, не сравнивается вовсе:
    «сечение не распознано» не доказывает, что сечение изменилось.

    Число параллельных кабелей — отдельный случай. «ППГнг(А)-HF» без множителя
    обычно означает один кабель, но НЕ объявляет этого: единица здесь —
    умолчание разбора, а не прочитанное значение. Поэтому различие с явным
    «3хППГнг(А)-HF» возвращается со статусом ``REVIEW``: потерять такое
    изменение нельзя, а утверждать его как доказанное — нечестно.
    """
    left_parsed, right_parsed = parse_cable(left), parse_cable(right)
    if not left_parsed or not right_parsed:
        return []
    output: list[dict[str, Any]] = []
    for facet in CABLE_FACETS:
        before, after = left_parsed.get(facet), right_parsed.get(facet)
        if before is None or after is None:
            continue
        status = PROVEN
        if facet == "mark":
            if marks_equal(before, after):
                continue
        elif before == after:
            continue
        elif facet == "parallel_count" and not (
            left_parsed["parallel_count_proven"]
            and right_parsed["parallel_count_proven"]
        ):
            status = REVIEW
        output.append({
            "facet": facet,
            "title": cable_facet_title(facet),
            "before": before,
            "after": after,
            "status": status,
            "left_raw": left_parsed["raw"],
            "right_raw": right_parsed["raw"],
        })
    return output


def _as_sequence(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def cables_equivalent(left: Any, right: Any) -> bool:
    """Совпадают ли два списка кабелей с точностью до раскладки и порядка."""
    return sorted(canonical_mark(item) for item in _as_sequence(left)) == sorted(
        canonical_mark(item) for item in _as_sequence(right)
    )


def _numeric(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return _number(value)


def numeric_change(before: Any, after: Any) -> dict[str, Any] | None:
    """Доказанное числовое различие. ``None`` — различия нет либо это не число."""
    left_value, right_value = _numeric(before), _numeric(after)
    if left_value is None or right_value is None or left_value == right_value:
        return None
    return {
        "before": before,
        "after": after,
        "direction": "INCREASED" if right_value > left_value else "DECREASED",
        "delta": round(right_value - left_value, 3),
    }


def attributes_differ(before: Any, after: Any) -> bool:
    """Различаются ли значения по существу, а не по написанию."""
    if isinstance(before, str) and isinstance(after, str):
        return not marks_equal(before, after)
    if isinstance(before, (list, tuple)) and isinstance(after, (list, tuple)):
        return not cables_equivalent(before, after)
    return before != after


__all__ = [
    "CABLE_FACETS",
    "PROVEN",
    "REVIEW",
    "attributes_differ",
    "cable_facet_title",
    "cables_equivalent",
    "canonical_mark",
    "compare_cables",
    "fold_homoglyphs",
    "marks_equal",
    "numeric_change",
    "parse_cable",
]
