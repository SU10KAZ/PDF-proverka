"""Детерминированный связчик «строка таблицы нагрузок → объект схемы».

Зачем модуль нужен
------------------
Однолинейная схема несёт мощности и токи потребителей не в графе аппаратов, а
в подписях фидерных колонок: «Ру=157,5кВт / Рр=157,5 кВт / Iр=360 А» слева и
«335.0 кВт - 335.0 кВт - 676.8 А» справа. Граф щита (``dense_sectioned_board``)
читает у аппарата ровно три свойства — номинал, состояние и тип, — поэтому
изменение мощности холодильной машины со 157,5 до 335 кВт не попадало ни в одно
сравнение: значения были на листе, но ни с чем не связаны.

Модуль решает узкую задачу: собрать эти подписи в строки таблицы и связать
каждую строку с потребителем ПО СОВОКУПНОСТИ ДОКАЗАТЕЛЬСТВ. Это не
универсальный разбор текста: за пределами электрических однолинейных схем
модуль не применяется.

Почему связь не по числу
------------------------
Соседние колонки листа отличаются на единицы пунктов, а подписи повторяются:
``ДР1-ХМ1`` содержит подстроку ``ХМ1``; ``ВРУ1 ввод 1`` и ``ВРУ1 ввод 2`` — это
разные вводы одного щита; ``АУКРМ №1`` и ``АУКРМ №2`` различаются только
номером. Поэтому связь требует согласия нескольких независимых признаков:
нормализованного обозначения, номера ввода, секции, панели и геометрии
колонки. Совпадения одного числа недостаточно никогда.

Три исхода связывания (и только три)
------------------------------------
``BOUND``      — обозначение доказано и единственно; строка вправе давать факт.
``AMBIGUOUS``  — кандидатов несколько либо они противоречат друг другу; строка
                 уходит в проверку человеком и НЕ создаёт факт.
``UNBOUND``    — обозначения нет; строка остаётся диагностикой.

Отсутствие связи никогда не превращается в утверждение «значение убрали»:
модуль умеет говорить «связано» и «не связано», но не «не существует».
"""
from __future__ import annotations

import hashlib
import math
import re
from typing import Any, Iterable, Mapping, Optional, Sequence

CONTRACT_VERSION = "electrical-load-table.v1"
PRODUCER = "electrical-load-table-linker-v1"

#: Источник значений: вектор-слой PDF, приведённый к визуальным координатам.
SOURCE_VECTOR = "NATIVE_PDF_VECTOR"

BOUND = "BOUND"
AMBIGUOUS = "AMBIGUOUS"
UNBOUND = "UNBOUND"
BINDING_STATUSES = (BOUND, AMBIGUOUS, UNBOUND)


# --------------------------------------------------------------------------
# 1. Нормализация символов
# --------------------------------------------------------------------------
#: Латиница, набранная вместо кириллицы. В САПР-шрифтах (ISOCPEUR, GOST) обе
#: раскладки выглядят одинаково, поэтому «Ру» и «Py» — одно и то же свойство.
_LATIN_TO_CYR = str.maketrans(
    {
        "A": "А", "B": "В", "E": "Е", "K": "К", "M": "М", "H": "Н", "O": "О",
        "P": "Р", "C": "С", "T": "Т", "X": "Х", "Y": "У",
        "a": "а", "e": "е", "o": "о", "p": "р", "c": "с", "x": "х", "y": "у",
    }
)


def to_cyrillic(value: str) -> str:
    """Сводит омоглифы к кириллице, не трогая остальной текст."""
    return (value or "").translate(_LATIN_TO_CYR)


def _decimal(value: str) -> float:
    return float((value or "").replace(",", ".").replace(" ", "").strip())


# --------------------------------------------------------------------------
# 2. Свойства и единицы
# --------------------------------------------------------------------------
#: Префикс подписи → (facet, ожидаемая единица). Единица обязана совпасть с
#: префиксом: «Qр=200 А» не станет реактивной мощностью, а «Iр=200 кВАр» не
#: станет током. Иначе 200 А соседней строки превращается в 200 кВАр.
_PREFIX_FACETS = {
    "ру": ("installed_power_kw", "квт"),
    "рр": ("demand_active_power_kw", "квт"),
    "рп": ("demand_active_power_kw", "квт"),
    "iр": ("maximum_calculated_current_a", "а"),
    "iрасч": ("maximum_calculated_current_a", "а"),
    "qр": ("demand_reactive_power_kvar", "квар"),
    "sр": ("demand_apparent_power_kva", "ква"),
}

#: Единица → facet для БЕСПРЕФИКСНЫХ полос вида «335.0 кВт - 335.0 кВт - 676.8 А».
_UNIT_FACETS = {
    "квт": ("power_kw_positional", "кВт"),
    "квар": ("demand_reactive_power_kvar", "кВАр"),
    "ква": ("demand_apparent_power_kva", "кВА"),
    "а": ("maximum_calculated_current_a", "А"),
}

#: Отображаемые названия свойств. Пользователь не обязан знать facet_ref.
FACET_TITLES = {
    "installed_power_kw": ("Установленная мощность", "кВт"),
    "demand_active_power_kw": ("Расчётная активная мощность", "кВт"),
    "demand_reactive_power_kvar": ("Расчётная реактивная мощность", "кВАр"),
    "demand_apparent_power_kva": ("Расчётная полная мощность", "кВА"),
    "maximum_calculated_current_a": ("Расчётный ток", "А"),
}

_NUM = r"\d+(?:[.,]\d+)?"
_UNIT_ALT = "кВАр|кВт|кВА|Вт|А|В"

RE_PREFIXED = re.compile(
    r"(?P<pfx>[A-Za-zА-Яа-я]{1,6})\s*=\s*"
    r"(?P<val>" + _NUM + r"(?:\s*/\s*" + _NUM + r")?)\s*"
    r"(?P<unit>" + _UNIT_ALT + r")?",
    re.IGNORECASE,
)
RE_UNITED = re.compile(
    r"(?P<val>" + _NUM + r")\s*(?P<unit>" + _UNIT_ALT + r")(?![А-Яа-яA-Za-z])",
    re.IGNORECASE,
)

#: Слова, которые вправе остаться в строке значений, не делая её прозой.
_RESIDUAL_ALLOWED = {"cosf", "cos", "соsf", "соs", "ф", "вт", "квт", "ква", "квар", "а", "в"}

#: Подписи измерительных цепей. «ТА1 ТШП 1500/5А» стоит в той же колонке, что и
#: нагрузка, но 1500/5 — коэффициент трансформатора тока, а не ток потребителя.
RE_INSTRUMENT_NOISE = re.compile(
    r"(к\s+регулятору|ТШП|Меркурий|счетчик|счётчик|^\d?ТА\d|^ТТ\d|см\.\s|поз\.)",
    re.IGNORECASE,
)


def _normalize_prefix(value: str) -> str:
    normalized = to_cyrillic(value).lower()
    return re.sub(r"^[iі]", "i", normalized)


def parse_values(text: str) -> list[dict[str, Any]]:
    """Разбирает значения подписи.

    Сначала пробуются подписи с префиксом («Ру=157,5кВт») — они самодостаточны.
    Только если префиксов нет, читается позиционная полоса; её свойства
    определяются единицей, а не порядком, поэтому «кВт кВт А» даёт
    ``power_kw_positional`` дважды, а не «Ру» и «Рр» наугад.
    """
    source = to_cyrillic(text or "")
    prefixed: list[dict[str, Any]] = []
    contradicted = False
    for match in RE_PREFIXED.finditer(source):
        facet_unit = _PREFIX_FACETS.get(_normalize_prefix(match.group("pfx")))
        if not facet_unit:
            continue
        facet, expected_unit = facet_unit
        unit = to_cyrillic(match.group("unit") or "").lower()
        if unit and unit != expected_unit:
            # Единица противоречит префиксу — подпись не доказана. И читать её
            # дальше как беспрефиксную нельзя: «Qр=200 А» стало бы током,
            # хотя лист прямо назвал величину реактивной мощностью.
            contradicted = True
            continue
        try:
            numbers = [_decimal(part) for part in re.split(r"\s*/\s*", match.group("val"))]
        except ValueError:
            continue
        prefixed.append(
            {
                "facet_ref": facet,
                "values": numbers,
                "unit": expected_unit,
                "raw": match.group(0).strip(),
                "reading": "PREFIXED",
                "span": [match.start(), match.end()],
            }
        )
    if prefixed or contradicted:
        return prefixed

    positional: list[dict[str, Any]] = []
    for index, match in enumerate(RE_UNITED.finditer(source)):
        unit = to_cyrillic(match.group("unit")).lower()
        facet_unit = _UNIT_FACETS.get(unit)
        if not facet_unit:
            continue
        try:
            number = _decimal(match.group("val"))
        except ValueError:
            continue
        positional.append(
            {
                "facet_ref": facet_unit[0],
                "values": [number],
                "unit": unit,
                "raw": match.group(0).strip(),
                "reading": "POSITIONAL",
                "position": index,
                "span": [match.start(), match.end()],
            }
        )
    return positional


def resolve_positional_power(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Раскрывает беспрефиксную полосу мощностей в именованные свойства.

    Полоса «335.0 кВт - 335.0 кВт - 676.8 А» не подписана: какое число
    установленная мощность, а какое расчётная — из самой полосы не следует.
    Порядок угадывать нельзя. Но когда оба числа РАВНЫ, ответ не зависит от
    порядка: обе мощности равны этому числу при любом прочтении, и оба свойства
    доказаны. Когда числа различаются, свойства остаются неразрешёнными — это
    честнее, чем приписать расчётную мощность установленной.
    """
    resolved: list[dict[str, Any]] = []
    by_unit: dict[str, list[dict[str, Any]]] = {}
    for value in values:
        if value.get("facet_ref") == "power_kw_positional":
            by_unit.setdefault("квт", []).append(value)
        elif value.get("reading") == "POSITIONAL" and value.get("unit") == "квар":
            by_unit.setdefault("квар", []).append(value)
        else:
            resolved.append(value)

    for unit, group in by_unit.items():
        numbers = {tuple(item["values"]) for item in group}
        installed, demand = (
            ("installed_power_kw", "demand_active_power_kw")
            if unit == "квт"
            else ("installed_reactive_power_kvar", "demand_reactive_power_kvar")
        )
        if len(group) == 1:
            # Одно число без пары: расчётное значение — то, что подписывают.
            resolved.append({**group[0], "facet_ref": demand, "order_proof": "SINGLE_VALUE"})
            continue
        if len(numbers) == 1:
            for facet in (installed, demand):
                resolved.append(
                    {**group[0], "facet_ref": facet, "order_proof": "EQUAL_VALUES"}
                )
            continue
        for item in group:
            resolved.append(
                {
                    **item,
                    "facet_ref": None,
                    "unresolved_reason": "positional_power_order_unproven",
                }
            )
    return resolved


def value_residual(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Значения подписи и слова, которые они не поглотили."""
    source = to_cyrillic(text or "")
    values = parse_values(source)
    if not values:
        return [], []
    mask = bytearray(len(source))
    for match in list(RE_PREFIXED.finditer(source)) + list(RE_UNITED.finditer(source)):
        for index in range(match.start(), match.end()):
            mask[index] = 1
    residual = "".join(" " if mask[i] else ch for i, ch in enumerate(source))
    words = [
        word
        for word in re.findall(r"[A-Za-zА-Яа-я]{2,}", residual)
        if word.lower() not in _RESIDUAL_ALLOWED
    ]
    return values, words


def is_value_run(text: str) -> bool:
    """Подпись целиком состоит из значений.

    Проза, обозначения и подписи измерительных цепей отвергаются: «ХМ2» —
    обозначение, «ТА1 ТШП 1500/5А» — трансформатор тока, и ни то ни другое не
    вправе давать нагрузку потребителя.
    """
    if RE_INSTRUMENT_NOISE.search(to_cyrillic(text or "")):
        return False
    if designations(text):
        return False
    values, residual_words = value_residual(text)
    return bool(values) and not residual_words


# --------------------------------------------------------------------------
# 3. Обозначения потребителей
# --------------------------------------------------------------------------
#: Обозначение обязано стоять отдельным токеном: иначе ``ДР1-ХМ1`` отдаст
#: ``ХМ1``, и охладитель склеится с холодильной машиной.
RE_DESIGNATION = re.compile(
    r"(?:^|[\s(\[])("
    r"ДР\d+-ХМ\d+|ХМ\d+|"
    r"АУКРМ\s*(?:№\s*)?\d+|АУКРМ-\d+|"
    r"ВРУ-[А-Яа-я]{2,4}\d*|ВРУ\d*[а-я]?|"
    r"ШУ-[А-Яа-я]{2,4}|ЭБ-[А-Яа-я]{2,4}|ЩНО|ЯСН\s?ТП"
    r")(?=$|[\s,.:;)\]])"
)

RE_INPUT_NUMBER = re.compile(r"ввод\s*[-—]?\s*([12])", re.IGNORECASE)
RE_PANEL = re.compile(r"(РП\s*\d+)", re.IGNORECASE)
RE_FEEDER_TAG = re.compile(
    r"(?P<section>[12])ГРЩ[-\s]*"
    r"(?P<load>[А-Яа-яA-Za-z]{1,4}\d*(?:[.\-][А-Яа-яA-Za-z]{2,4}\d*)?)"
)
RE_PANEL_TAG = re.compile(r"ГРЩ(?P<section>\d)-(?P<panel>РП\d+)-(?P<feeder>\d+)")
RE_MODE = re.compile(
    r"(рабочий|пожарн\w*|авар\w*|ПП\s?режим|летн\w*|зимн\w*|послеаварийн\w*)",
    re.IGNORECASE,
)
RE_CABLE = re.compile(r"(ППГнг|ВВГнг|ПуГПнг|КПТнг|КППГнг|ППГ)", re.IGNORECASE)


def canonical_designation(value: str) -> str:
    """Приводит обозначение к сравнимому виду.

    ``АУКРМ №1``, ``АУКРМ-1`` и ``АУКРМ 1`` — один аппарат; ``ВРУа`` и
    ``ВРУ-А`` — один щит. Различия раскладки и разделителя не должны рождать
    два разных потребителя.
    """
    normalized = to_cyrillic(value or "").upper().replace("Ё", "Е")
    normalized = normalized.replace("№", "-")
    normalized = re.sub(r"\s+", "", normalized)
    normalized = re.sub(r"АУКРМ-?(\d+)", r"АУКРМ-\1", normalized)
    normalized = re.sub(r"^ВРУ([А-Я])$", r"ВРУ-\1", normalized)
    normalized = re.sub(r"^ВРУ-?(ИТП|АПТ|НСТ|ХЦ|ХВС)$", r"ВРУ-\1", normalized)
    normalized = re.sub(r"^ЯСНТП$", "ЯСН-ТП", normalized)
    # «ШУХЦ» и «ШУ-ХЦ» — одна и та же подпись, набранная с разделителем и без.
    normalized = re.sub(r"^(ШУ|ЩУ|ЭБ|ДР)-?([А-Я]{2,4}\d*)$", r"\1-\2", normalized)
    # Щ и Ш в этих подписях взаимозаменяемы («ЩНО» и «ШНО» — щит наружного
    # освещения). Та же свёртка действует в тождестве узлов графа щита, и
    # расхождение между ними породило бы два разных потребителя из одного.
    if normalized.startswith("Щ"):
        normalized = "Ш" + normalized[1:]
    return normalized


def designations(text: str) -> list[str]:
    """Все обозначения подписи в каноническом виде, в порядке появления."""
    padded = " " + to_cyrillic(text or "") + " "
    found: list[str] = []
    for match in RE_DESIGNATION.finditer(padded):
        canonical = canonical_designation(match.group(1))
        if canonical and canonical not in found:
            found.append(canonical)
    return found


def mode_label(text: str) -> Optional[str]:
    """Название режима подписи.

    Режим — часть тождества свойства, а не украшение: «рабочий» и «аварийный»
    режимы дают разные числа для одного щита, и подменять один другим нельзя.
    """
    match = RE_MODE.search(to_cyrillic(text or ""))
    if not match:
        return None
    return re.sub(r"\s+", " ", to_cyrillic(text).strip())


def feeder_tags(text: str) -> list[dict[str, Any]]:
    """Метки фидера: «1ГРЩ-ХМ1» и «ГРЩ1-РП1-12» несут секцию и панель."""
    source = to_cyrillic(text or "")
    tags: list[dict[str, Any]] = []
    for match in RE_FEEDER_TAG.finditer(source):
        tags.append(
            {
                "section": match.group("section"),
                "load": canonical_designation(match.group("load").replace(".", "-")),
                "panel": None,
                "raw": match.group(0),
            }
        )
    for match in RE_PANEL_TAG.finditer(source):
        panel = match.group("panel").replace(" ", "").upper()
        panel_index = re.sub(r"\D", "", panel)
        tags.append(
            {
                # Секцию задаёт номер панели, а не номер ГРЩ: «ГРЩ1-РП2-1» —
                # это вторая секция первого щита. Если брать цифру после «ГРЩ»,
                # обе секции правого листа сливаются в одну.
                "section": panel_index or None,
                "load": None,
                "panel": panel,
                "switchboard": match.group("section"),
                "feeder": match.group("feeder"),
                "raw": match.group(0),
            }
        )
    return tags


def section_ref(section: Optional[str], panel: Optional[str]) -> Optional[str]:
    """Единое обозначение секции для обеих редакций.

    Левый лист помечает фидер префиксом («1ГРЩ-ХМ1»), правый — панелью
    («ГРЩ1-РП1-12»). Обе стороны называют свои секции РП1 и РП2, поэтому
    сравнимая форма — именно она.
    """
    if panel:
        normalized = re.sub(r"\s+", "", str(panel)).upper()
        if re.fullmatch(r"РП\d+", normalized):
            return normalized
    if section:
        digits = re.sub(r"\D", "", str(section))
        if digits:
            return f"РП{digits}"
    return None


# --------------------------------------------------------------------------
# 4. Прогоны текста из вектор-слоя
# --------------------------------------------------------------------------
def _overlap(a0: float, a1: float, b0: float, b1: float) -> float:
    return max(0.0, min(a1, b1) - max(a0, b0))


def _run_axes(run: Mapping[str, Any]) -> tuple[float, float, float, float]:
    """(along0, along1, cross0, cross1) — вдоль чтения и поперёк."""
    if run["orientation"] == "H":
        return run["x0"], run["x1"], run["y0"], run["y1"]
    return run["y0"], run["y1"], run["x0"], run["x1"]


def _infer_reading(words: Sequence[Sequence[Any]]) -> tuple[str, int]:
    """Направление и знак чтения прогона.

    Порядок слов надёжнее пропорций: у строки из двух и более слов координата
    вдоль чтения монотонно меняется, а знак этого изменения и есть направление.
    Вертикальные подписи чертежей набраны снизу вверх, поэтому знак
    отрицательный, и угадывать его по содержимому не нужно. Для одиночного
    слова остаётся оценка по пропорции с поправкой на число знаков; знак тогда
    считается прямым, но и склеивать такой прогон не с чем.
    """
    if len(words) >= 2:
        delta_x = float(words[-1][0]) - float(words[0][0])
        delta_y = float(words[-1][1]) - float(words[0][1])
        if abs(delta_x) > abs(delta_y):
            return "H", 1 if delta_x >= 0 else -1
        if abs(delta_y) > abs(delta_x):
            return "V", 1 if delta_y >= 0 else -1
    first = words[0]
    width = float(first[2]) - float(first[0])
    height = float(first[3]) - float(first[1])
    glyphs = max(1, len(str(first[4] or "").strip()))
    # Горизонтальное слово из n знаков шире, чем высоко, примерно в n/2 раз.
    return ("H", 1) if width >= 0.35 * glyphs * max(height, 1e-6) else ("V", -1)


def build_visual_runs(visual_words: Iterable[Sequence[Any]]) -> list[dict[str, Any]]:
    """Собирает слова вектор-слоя в прогоны текста в порядке чтения.

    Слова приходят из ``VectorEvidence.visual_words`` уже в визуальных
    координатах (поворот страницы применён ровно один раз). Группировка идёт по
    исходной паре «блок + строка» PDF, порядок внутри строки — по номеру слова.
    Соседние куски одной строки склеиваются: иначе вертикальная полоса
    «0.9 кВт - 0.9 кВт - 1.4 А» распадается на три и хвост «1.4 А» теряется.
    """
    grouped: dict[tuple[Any, Any], list[Sequence[Any]]] = {}
    for word in visual_words or ():
        try:
            key = (word[5], word[6])
        except (IndexError, TypeError):
            continue
        text = str(word[4] or "")
        if not text.strip():
            continue
        grouped.setdefault(key, []).append(word)

    runs: list[dict[str, Any]] = []
    for (block, line), words in grouped.items():
        try:
            words.sort(key=lambda item: int(item[7]))
        except (IndexError, TypeError, ValueError):
            words.sort(key=lambda item: (float(item[1]), float(item[0])))
        orientation, reading_sign = _infer_reading(words)
        text = " ".join(str(word[4]).strip() for word in words if str(word[4]).strip())
        runs.append(
            {
                "block": block,
                "line": line,
                "orientation": orientation,
                "reading_sign": reading_sign,
                "text": re.sub(r"\s+", " ", text).strip(),
                "x0": min(float(word[0]) for word in words),
                "y0": min(float(word[1]) for word in words),
                "x1": max(float(word[2]) for word in words),
                "y1": max(float(word[3]) for word in words),
                "words": len(words),
            }
        )
    return _merge_adjacent_runs(runs)


def _merge_adjacent_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Склеивает куски одной подписи, разорванные разбивкой PDF на строки."""
    runs.sort(key=lambda run: (run["block"], _run_axes(run)[2], _run_axes(run)[0]))
    merged: list[dict[str, Any]] = []
    consumed = [False] * len(runs)
    for index, run in enumerate(runs):
        if consumed[index]:
            continue
        consumed[index] = True
        group = [run]
        a0, a1, c0, c1 = _run_axes(run)
        height = max(run["y1"] - run["y0"], run["x1"] - run["x0"], 1.0)
        changed = True
        while changed:
            changed = False
            for other_index, other in enumerate(runs):
                if consumed[other_index] or other["block"] != run["block"]:
                    continue
                if other["orientation"] != run["orientation"]:
                    continue
                b0, b1, d0, d1 = _run_axes(other)
                overlap = _overlap(c0, c1, d0, d1)
                shorter = min(c1 - c0, d1 - d0) or 1e-9
                if overlap / shorter < 0.7:
                    continue
                gap = max(a0 - b1, b0 - a1)
                if gap > 1.5 * _glyph_size(run, other):
                    # Далёкие куски — разные подписи одного ряда, не одна строка.
                    continue
                consumed[other_index] = True
                group.append(other)
                a0, a1 = min(a0, b0), max(a1, b1)
                c0, c1 = min(c0, d0), max(c1, d1)
                changed = True
        if len(group) == 1:
            merged.append(run)
            continue
        # Порядок кусков задаёт знак чтения, снятый с последовательности слов,
        # а не догадка по содержимому: иначе «1.3 кВт - 1.3 кВт - 2.1 А»
        # собирается задом наперёд и ток становится мощностью.
        signs = [item.get("reading_sign", 1) for item in group]
        sign = -1 if signs.count(-1) > signs.count(1) else 1
        group.sort(key=lambda item: _run_axes(item)[0], reverse=(sign < 0))
        text = " ".join(item["text"] for item in group if item["text"])
        merged.append(
            {
                "block": run["block"],
                "line": run["line"],
                "orientation": run["orientation"],
                "reading_sign": sign,
                "text": re.sub(r"\s+", " ", text).strip(),
                "x0": min(item["x0"] for item in group),
                "y0": min(item["y0"] for item in group),
                "x1": max(item["x1"] for item in group),
                "y1": max(item["y1"] for item in group),
                "words": sum(item["words"] for item in group),
                "merged_parts": len(group),
            }
        )
    return merged


def _glyph_size(*runs: Mapping[str, Any]) -> float:
    sizes = []
    for run in runs:
        if run["orientation"] == "H":
            sizes.append(run["y1"] - run["y0"])
        else:
            sizes.append(run["x1"] - run["x0"])
    return max([size for size in sizes if size > 0] or [8.0])


# --------------------------------------------------------------------------
# 5. Полосы (колонки таблицы)
# --------------------------------------------------------------------------
def build_stacks(
    runs: Sequence[Mapping[str, Any]],
    *,
    cross_gap: float,
    along_overlap: float = 0.35,
) -> list[dict[str, Any]]:
    """Группирует прогоны в полосы — колонки таблицы нагрузок.

    Полоса — это подписи одного фидера: они перекрываются вдоль чтения и стоят
    вплотную поперёк. Соседние фидеры разделены пустым промежутком в целую
    ширину колонки, поэтому склеиться не могут.
    """
    stacks: list[dict[str, Any]] = []
    for orientation in ("H", "V"):
        items = [run for run in runs if run["orientation"] == orientation]
        items.sort(key=lambda run: (_run_axes(run)[2], _run_axes(run)[0]))
        consumed = [False] * len(items)
        for index, item in enumerate(items):
            if consumed[index]:
                continue
            consumed[index] = True
            group = [item]
            a0, a1, c0, c1 = _run_axes(item)
            changed = True
            while changed:
                changed = False
                for other_index, other in enumerate(items):
                    if consumed[other_index]:
                        continue
                    b0, b1, d0, d1 = _run_axes(other)
                    overlap = _overlap(a0, a1, b0, b1)
                    shorter = min(a1 - a0, b1 - b0) or 1e-9
                    gap = max(c0 - d1, d0 - c1)
                    if overlap / shorter < along_overlap or gap > cross_gap:
                        continue
                    consumed[other_index] = True
                    group.append(other)
                    a0, a1 = min(a0, b0), max(a1, b1)
                    c0, c1 = min(c0, d0), max(c1, d1)
                    changed = True
            if len(group) < 2:
                continue
            group.sort(key=lambda run: _run_axes(run)[2])
            stacks.append(
                {
                    "orientation": orientation,
                    "along": [a0, a1],
                    "cross": [c0, c1],
                    "runs": group,
                }
            )
    return stacks


# --------------------------------------------------------------------------
# 6. Строка таблицы
# --------------------------------------------------------------------------
def _stable_id(prefix: str, *parts: Any) -> str:
    payload = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _row_from_stack(
    stack: Mapping[str, Any],
    *,
    side: str,
    page: Optional[int],
    table_id: str,
) -> Optional[dict[str, Any]]:
    labels: list[str] = []
    own_designations: list[str] = []
    values: list[dict[str, Any]] = []
    modes: list[str] = []
    cables: list[str] = []
    tags: list[dict[str, Any]] = []

    for run in stack["runs"]:
        text = run["text"]
        if not text:
            continue
        tags.extend(feeder_tags(text))
        if RE_CABLE.search(text):
            cables.append(text)
        if is_value_run(text):
            for value in parse_values(text):
                values.append(
                    {
                        **value,
                        "raw_run": text,
                        "bbox": [run["x0"], run["y0"], run["x1"], run["y1"]],
                    }
                )
            continue
        found = designations(text)
        if found:
            for item in found:
                if item not in own_designations:
                    own_designations.append(item)
        else:
            label_mode = mode_label(text)
            if label_mode:
                modes.append(label_mode)
                continue
        labels.append(text)

    if not values:
        return None

    values = resolve_positional_power(values)
    label_text = " | ".join(labels)
    joined = label_text + " " + " ".join(cables)
    input_match = RE_INPUT_NUMBER.search(to_cyrillic(joined))
    panel = next((tag["panel"] for tag in tags if tag.get("panel")), None)
    if not panel:
        panel_match = RE_PANEL.search(to_cyrillic(joined))
        panel = panel_match.group(1).replace(" ", "") if panel_match else None
    section = next((tag["section"] for tag in tags if tag.get("section")), None)
    tag_loads = [tag["load"] for tag in tags if tag.get("load")]

    row = {
        "row_id": _stable_id(
            "etrow", side, page, round(stack["cross"][0], 2), round(stack["along"][0], 2)
        ),
        "side": side,
        "page": page,
        "table_id": table_id,
        "orientation": stack["orientation"],
        "along_range": [round(stack["along"][0], 3), round(stack["along"][1], 3)],
        "cross_range": [round(stack["cross"][0], 3), round(stack["cross"][1], 3)],
        "bbox": [
            round(min(run["x0"] for run in stack["runs"]), 3),
            round(min(run["y0"] for run in stack["runs"]), 3),
            round(max(run["x1"] for run in stack["runs"]), 3),
            round(max(run["y1"] for run in stack["runs"]), 3),
        ],
        "consumer_label": label_text,
        "own_designations": own_designations,
        "row_designations": [],
        "feeder_designations": tag_loads,
        "input_number": int(input_match.group(1)) if input_match else None,
        "panel": panel,
        "section": section,
        "section_ref": section_ref(section, panel),
        # Подпись фидера («1ГРЩ-ХМ1», «ГРЩ1-РП1-12») означает нагрузку ОДНОЙ
        # линии; строка без неё — суммарную нагрузку потребителя. Складывать их
        # в одно свойство нельзя: у ВРУ4 фидер секции 1 несёт 118,2 кВт, а
        # потребитель целиком — 233,6/284,7 кВт.
        "row_kind": "FEEDER" if tags else "CONSUMER_TOTAL",
        "mode_label": modes[0] if modes else None,
        "mode_labels": modes,
        "cables": cables,
        "values": values,
        "source": SOURCE_VECTOR,
        "binding_status": UNBOUND,
        "binding_reasons": [],
        "consumer_designation": None,
    }
    return row


#: Слова, которые вправе стоять рядом с обозначением в подписи-ярлыке.
_LABEL_ALLOWED = {"ввод", "секция", "секц", "корпус", "щит", "шкаф"}


def is_designation_label(text: str) -> bool:
    """Подпись-ярлык: обозначение и почти ничего больше.

    Ссылки вроде «к регулятору АУКРМ №1» содержат то же обозначение, но
    относятся к цепи управления, а не к колонке под собой. Такая подпись
    геометрически накрывает соседний фидер и приписала бы ему чужую нагрузку,
    поэтому в ряд обозначений она не допускается.
    """
    source = to_cyrillic(text or "")
    if RE_INSTRUMENT_NOISE.search(source):
        return False
    stripped = RE_DESIGNATION.sub(" ", " " + source + " ")
    residual = [
        word
        for word in re.findall(r"[A-Za-zА-Яа-я]{2,}", stripped)
        if word.lower() not in _LABEL_ALLOWED
    ]
    return len(residual) <= 1


def attach_designation_rows(
    rows: list[dict[str, Any]],
    runs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Приписывает строке обозначение из отдельного ряда подписей.

    На правом листе колонка потребителя подписана «Холодильная машина (чиллер)»,
    а обозначение ``ХМ1`` вынесено в общий ряд над схемой. Ряд перпендикулярен
    колонкам, поэтому подпись принадлежит той колонке, чью поперечную полосу она
    перекрывает. Привязка принимается только при единственном кандидате либо при
    двукратном перевесе над вторым: подпись, накрывающая две колонки, ничего не
    доказывает.
    """
    inside = {id(run) for row in rows for run in row.get("_runs", ())}
    report: list[dict[str, Any]] = []
    for run in runs:
        if id(run) in inside:
            continue
        if not is_designation_label(run["text"]):
            continue
        found = designations(run["text"])
        if not found:
            continue
        along0, along1, _, _ = _run_axes(run)
        candidates: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            if row["orientation"] == run["orientation"]:
                continue
            overlap = _overlap(along0, along1, row["cross_range"][0], row["cross_range"][1])
            if overlap > 0:
                candidates.append((overlap, row))
        candidates.sort(key=lambda item: -item[0])
        designation = found[0]
        if not candidates:
            report.append({"designation": designation, "status": UNBOUND, "overlap": 0.0})
            continue
        best, runner_up = candidates[0][0], (candidates[1][0] if len(candidates) > 1 else 0.0)
        if len(candidates) > 1 and best < 2.0 * runner_up:
            report.append(
                {
                    "designation": designation,
                    "status": AMBIGUOUS,
                    "overlap": round(best, 3),
                    "runner_up": round(runner_up, 3),
                }
            )
            continue
        row = candidates[0][1]
        row["row_designations"].append(
            {
                "designation": designation,
                "text": run["text"],
                "bbox": [run["x0"], run["y0"], run["x1"], run["y1"]],
                "overlap": round(best, 3),
                "runner_up": round(runner_up, 3),
            }
        )
        report.append(
            {"designation": designation, "status": BOUND, "overlap": round(best, 3)}
        )
    return report


def resolve_binding(row: dict[str, Any]) -> dict[str, Any]:
    """Определяет исход связывания строки и записывает причины.

    Согласие независимых источников обозначения — подписи самой колонки, ряда
    подписей и метки фидера — усиливает связь; расхождение переводит строку в
    проверку человеком, а не выбирает победителя молча.
    """
    own = list(row.get("own_designations") or [])
    from_row = [item["designation"] for item in row.get("row_designations") or []]
    from_feeder = list(row.get("feeder_designations") or [])

    sources: dict[str, list[str]] = {}
    if own:
        sources["own_label"] = own
    if from_row:
        sources["designation_row"] = from_row
    if from_feeder:
        sources["feeder_tag"] = from_feeder

    candidates: list[str] = []
    for values in sources.values():
        for value in values:
            if value not in candidates:
                candidates.append(value)

    row["designation_sources"] = sources
    if not candidates:
        row["binding_status"] = UNBOUND
        row["binding_reasons"] = ["designation_not_proven"]
        row["consumer_designation"] = None
        return row
    if len(candidates) > 1:
        row["binding_status"] = AMBIGUOUS
        row["binding_reasons"] = ["designation_conflict"]
        row["consumer_designation"] = None
        row["conflicting_designations"] = candidates
        return row

    row["binding_status"] = BOUND
    row["consumer_designation"] = candidates[0]
    row["binding_reasons"] = sorted(sources)
    row["binding_signal_count"] = len(sources)
    return row


# --------------------------------------------------------------------------
# 7. Сборка таблицы листа
# --------------------------------------------------------------------------
def _median(values: Sequence[float]) -> float:
    ordered = sorted(value for value in values if value and value > 0)
    if not ordered:
        return 0.0
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (float(ordered[middle - 1]) + float(ordered[middle])) / 2.0


def build_load_table(evidence: Any, *, side: str) -> dict[str, Any]:
    """Строит таблицу нагрузок листа из вектор-слоя.

    Возвращает контракт ``electrical-load-table.v1``: строки, исход связывания
    каждой и диагностику. Модель не вызывается; результат зависит только от
    геометрии и текста листа.
    """
    words = list(getattr(evidence, "visual_words", None) or ())
    page = getattr(evidence, "page_index", None)
    if not words:
        return _empty_table(side, page, reason="vector_words_absent")

    runs = build_visual_runs(words)
    if not runs:
        return _empty_table(side, page, reason="vector_runs_absent")

    glyph = _median([_glyph_size(run) for run in runs]) or 8.0
    stacks = build_stacks(runs, cross_gap=0.9 * glyph)
    table_id = _stable_id("ettab", side, page, len(runs))

    rows: list[dict[str, Any]] = []
    for stack in stacks:
        row = _row_from_stack(stack, side=side, page=page, table_id=table_id)
        if row is None:
            continue
        row["_runs"] = stack["runs"]
        rows.append(row)

    designation_report = attach_designation_rows(rows, runs)
    for row in rows:
        resolve_binding(row)
        row.pop("_runs", None)

    contradictions = detect_row_contradictions(rows)
    counts = {status: 0 for status in BINDING_STATUSES}
    for row in rows:
        counts[row["binding_status"]] += 1

    return {
        "contract_version": CONTRACT_VERSION,
        "producer": PRODUCER,
        "side": side,
        "page_index": page,
        "table_id": table_id,
        "rows": rows,
        "contradictions": contradictions,
        "counts": {
            "rows": len(rows),
            "bound": counts[BOUND],
            "ambiguous": counts[AMBIGUOUS],
            "unbound": counts[UNBOUND],
        },
        "diagnostics": {
            "visual_words": len(words),
            "runs": len(runs),
            "stacks": len(stacks),
            "glyph_size": round(glyph, 3),
            "designation_row_bindings": designation_report,
            "uses_model": False,
            "uses_ocr": False,
        },
    }


def _empty_table(side: str, page: Optional[int], *, reason: str) -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "producer": PRODUCER,
        "side": side,
        "page_index": page,
        "table_id": None,
        "rows": [],
        "contradictions": [],
        "counts": {"rows": 0, "bound": 0, "ambiguous": 0, "unbound": 0},
        "diagnostics": {
            "visual_words": 0,
            "runs": 0,
            "stacks": 0,
            "reason": reason,
            "uses_model": False,
            "uses_ocr": False,
        },
    }


#: Линейное напряжение сети 0,4 кВ. Иного на однолинейной схеме ГРЩ не бывает,
#: но проверка всё равно допускает широкий разброс — она ищет грубую описку,
#: а не считает проект за проектировщика.
_LINE_VOLTAGE_KV = 0.38
#: Допуск сверки «мощность / cosφ / ток». 25% покрывают и округление, и разницу
#: в принятом cosφ; срабатывание означает уже не погрешность, а описку.
_ARITHMETIC_TOLERANCE = 0.25

RE_COS_PHI = re.compile(r"со?s\s*[fф]?\s*[= ]\s*(?P<value>0[.,]\d+)", re.IGNORECASE)


def check_row_arithmetic(row: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    """Сверяет расчётный ток строки с её же мощностью и cosφ.

    Строка «Рр=307,6кВт, cosf=0,87, Iрасч=205,8А» противоречит сама себе: при
    таком cosφ ток трёхфазной нагрузки был бы около 537 А. Это ошибка листа, а
    не изменение между редакциями, поэтому находка идёт в противоречия
    документа и никогда не превращается в стрелку «стало другим».
    """
    power = current = None
    for value in row.get("values") or ():
        if value.get("facet_ref") == "demand_active_power_kw" and len(value["values"]) == 1:
            power = value
        elif (
            value.get("facet_ref") == "maximum_calculated_current_a"
            and len(value["values"]) == 1
        ):
            current = value
    if not power or not current:
        return None
    raw = to_cyrillic(power.get("raw_run") or "")
    match = RE_COS_PHI.search(raw)
    if not match:
        return None
    try:
        cos_phi = _decimal(match.group("value"))
    except ValueError:
        return None
    if cos_phi <= 0:
        return None
    expected = power["values"][0] / (math.sqrt(3.0) * _LINE_VOLTAGE_KV * cos_phi)
    stated = current["values"][0]
    if stated <= 0:
        return None
    deviation = abs(expected - stated) / max(expected, stated)
    if deviation <= _ARITHMETIC_TOLERANCE:
        return None
    return {
        "kind": "ROW_ARITHMETIC_CONFLICT",
        "side": row["side"],
        "row_id": row["row_id"],
        "subject": row.get("consumer_designation") or row.get("consumer_label"),
        "summary": (
            f"В строке «{row.get('consumer_designation') or row.get('consumer_label')}» "
            f"указаны Рр={power['values'][0]:g} кВт и cosφ={cos_phi:g}, "
            f"но расчётный ток приведён как {stated:g} А — при этих данных он "
            f"составил бы около {expected:.0f} А."
        ),
        "evidence": {
            "raw": raw,
            "power_kw": power["values"][0],
            "cos_phi": cos_phi,
            "stated_current_a": stated,
            "expected_current_a": round(expected, 1),
            "deviation": round(deviation, 3),
            "bbox": row.get("bbox"),
            "reason": "stated current disagrees with power and power factor",
        },
    }


def detect_row_contradictions(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Внутренние противоречия листа, найденные при разборе таблицы.

    Расхождение подписи колонки с рядом подписей — ошибка самого листа
    (``ДР2-ХМ2`` в колонке, подписанной ``ДР1-ХМ2``), а не изменение между
    редакциями. Такие находки идут отдельным разделом и никогда не выдаются
    как «стало другим».
    """
    contradictions: list[dict[str, Any]] = []
    for row in rows:
        own = set(row.get("own_designations") or ())
        from_row = {item["designation"] for item in row.get("row_designations") or ()}
        if own and from_row and not (own & from_row):
            contradictions.append(
                {
                    "kind": "CONSUMER_LABEL_CONFLICT",
                    "side": row["side"],
                    "row_id": row["row_id"],
                    "subject": sorted(own)[0],
                    "summary": (
                        f"Колонка подписана «{sorted(own)[0]}», а в ряду обозначений "
                        f"над ней стоит «{sorted(from_row)[0]}»."
                    ),
                    "evidence": {
                        "column_label": sorted(own),
                        "row_label": sorted(from_row),
                        "bbox": row["bbox"],
                        "reason": "column label conflicts with designation row",
                    },
                }
            )
        arithmetic = check_row_arithmetic(row)
        if arithmetic:
            contradictions.append(arithmetic)
    return contradictions


__all__ = [
    "AMBIGUOUS",
    "BINDING_STATUSES",
    "BOUND",
    "CONTRACT_VERSION",
    "FACET_TITLES",
    "PRODUCER",
    "SOURCE_VECTOR",
    "UNBOUND",
    "attach_designation_rows",
    "build_load_table",
    "build_stacks",
    "build_visual_runs",
    "canonical_designation",
    "designations",
    "detect_row_contradictions",
    "feeder_tags",
    "is_value_run",
    "mode_label",
    "parse_values",
    "resolve_binding",
    "to_cyrillic",
    "value_residual",
]
