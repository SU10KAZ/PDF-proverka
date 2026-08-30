"""Внутренняя согласованность ОДНОГО листа: противоречия, а не изменения.

Зачем модуль нужен
------------------
Сравнение двух редакций отвечает на вопрос «что стало другим». Но часть ошибок
видна на одном листе, без всякой второй редакции: расчётный ток не сходится с
мощностью, сводная строка не сходится с суммой вводов, одно обозначение стоит у
двух разных линий, мощность записана в ваттах вместо киловатт. Такие находки
нельзя показывать как «было → стало»: второй стороны у них нет, и любая пара
значений здесь была бы выдумкой.

Поэтому модуль выдаёт отдельный вид находки — внутреннее противоречие листа. Он
ничего не исправляет и не выбирает, какая из двух подписей верна: он только
показывает инженеру оба числа и доказательства, по которым видно расхождение.

Чего модуль не делает
---------------------
* не сравнивает cosφ с «ожидаемым»: 0,95 и 0,88 — нормальные значения проекта,
  а не ошибка. Ловится только физически невозможное, то есть больше единицы;
* не считает любую сводную строку суммой вводов: сначала на самой таблице
  должно подтвердиться, что она так устроена;
* не объявляет ошибкой расхождение, которое объясняется коэффициентом
  одновременности: уменьшить расчётную нагрузку против суммы вводов — обычная
  практика, а не описка;
* не переименовывает повторяющееся обозначение и не выбирает «правильную»
  строку — это решение инженера;
* не считает значение неверной единицей только потому, что оно «слишком мало»:
  нужна арифметика по независимым числам той же подписи;
* не объявляет ошибкой необычную сигнальную связь — только помечает её как
  требующую проверки.

Модель не вызывается: результат зависит только от геометрии и текста листа.
"""
from __future__ import annotations

import hashlib
import math
import re
from typing import Any, Iterable, Mapping, Optional, Sequence

from backend.app.pipeline.stages.block_grounding.electrical_load_table import (
    BOUND,
    to_cyrillic,
)

CONTRACT_VERSION = "document-consistency.v1"
PRODUCER = "document-consistency-v1"

#: Находка доказана самим листом.
VERDICT_CONFIRMED = "CONFIRMED"
#: Находка правдоподобна, но доказательство не абсолютно: решает инженер.
VERDICT_REVIEW = "REVIEW"

KIND_IMPLIED_POWER_FACTOR = "IMPLIED_POWER_FACTOR_IMPOSSIBLE"
KIND_SUMMARY_INPUT_MISMATCH = "SUMMARY_INPUT_MISMATCH"
KIND_DUPLICATE_DESIGNATION = "DUPLICATE_DESIGNATION"
KIND_POWER_UNIT_MISMATCH = "POWER_UNIT_MISMATCH"
KIND_SIGNAL_LINK_OUTLIER = "SIGNAL_LINK_OUTLIER"

KINDS = frozenset(
    {
        KIND_IMPLIED_POWER_FACTOR,
        KIND_SUMMARY_INPUT_MISMATCH,
        KIND_DUPLICATE_DESIGNATION,
        KIND_POWER_UNIT_MISMATCH,
        KIND_SIGNAL_LINK_OUTLIER,
    }
)

_SQRT3 = math.sqrt(3.0)

#: Линейное напряжение сети 0,4 кВ. Значение не подставляется по умолчанию: оно
#: обязано быть напечатано на самом листе (см. ``proven_line_voltage_kv``).
LINE_VOLTAGE_KV = 0.38

#: Порог физически невозможного cosφ. Сама физика запрещает всё выше единицы,
#: но запас нужен шире: наибольший «нормальный» cosφ этой пары — 0,9985, а
#: расчёт тока по 400 В вместо 380 В завышает результат ровно на 5,26% и один
#: этот пересчёт даёт 1,051. Порог 1,15 переживает и его, и округление токов до
#: целых ампер, не стоя ни одной настоящей находки: реальные описки дают 1,4.
IMPOSSIBLE_POWER_FACTOR = 1.15

#: Абсолютный запас по току. Токи печатаются целыми и десятыми долями ампера,
#: и на малых значениях одно округление уже даёт проценты. Находка требует,
#: чтобы указанный ток был меньше физического минимума не меньше чем на 5 А.
IMPOSSIBLE_CURRENT_MARGIN_A = 5.0
#: Токи ниже этого значения не проверяются вовсе: там округление сравнимо с
#: самой величиной.
MIN_CHECKED_CURRENT_A = 10.0

#: Допуск равенства «сводка = сумма вводов». 1% покрывает округление слагаемых
#: до десятых (118,2 + 115,6 = 233,8 против напечатанных 233,6) и не прячет
#: расхождения в десятки процентов, ради которых проверка и делается.
SUMMARY_TOLERANCE = 0.01

#: Насколько должны сойтись отношения «сводка/сумма» по мощности и по току,
#: чтобы считать сводку равномерно масштабированной, а не ошибочной.
SCALING_TOLERANCE = 0.05

#: Сколько групп с точным равенством нужно, чтобы признать устройство таблицы
#: «сводка = сумма вводов» доказанным.
INVARIANT_MIN_GROUPS = 4
#: Из них — сколько должны иметь больше одного ввода. Группа с единственным
#: вводом проверяет только отсутствие коэффициента одновременности, а не саму
#: складываемость, поэтому одних таких групп мало.
INVARIANT_MIN_MULTI_INPUT_GROUPS = 2

#: Допуск сверки «мощность / cosφ / ток» для правила о единице измерения. Тот
#: же, что у сверки строки в ``electrical_load_table``: 25% покрывают и
#: округление, и разницу в принятом cosφ.
UNIT_ARITHMETIC_TOLERANCE = 0.25
#: Во сколько раз прочтение «в ваттах» должно расходиться с арифметикой, чтобы
#: считать единицу опиской, а не малой нагрузкой.
UNIT_MAGNITUDE_FACTOR = 100.0

#: Сколько колонок нужно в КАЖДОЙ секции, чтобы ряд сигнальных подписей вообще
#: считался рядом, а единственное отличие — исключением из правила.
SIGNAL_MIN_PER_SECTION = 4

#: Абсолютный пол допуска суммы: значения печатаются с точностью до 0,1 кВт,
#: поэтому сумма n слагаемых и сводки накапливает до 0,05·(n+1) кВт чистого
#: округления. Без этого пола группа из двух вводов по 0,9 и 1,3 кВт даёт
#: «расхождение» 6,8% на ровном месте.
ROUNDING_STEP_KW = 0.05

#: Признак недосчитанного ввода: сводка кратна сумме вводов целым числом, и
#: кратность одинакова по мощности и по току. Это не ошибка величины, а
#: пропущенная строка, и её называет отдельная находка о повторе обозначения.
INTEGER_MULTIPLE_TOLERANCE = 0.02

#: Вид противоречия, который выдаёт сама таблица нагрузок. Строка, уже
#: объявленная противоречивой, не вправе быть слагаемым другой находки.
ROW_ARITHMETIC_CONFLICT = "ROW_ARITHMETIC_CONFLICT"

#: Линейное напряжение, напечатанное на листе: «~380/220В», «380В», «0,4 кВ».
RE_LINE_VOLTAGE = re.compile(r"(?<!\d)380\s*(?:/\s*220)?\s*В|(?<!\d)0[.,]4\s*кВ", re.IGNORECASE)

#: Явный cosφ подписи. Ловится ЛЮБОЕ значение, включая «cosf=1» и «соs 0,65»:
#: если проектировщик его написал, подразумеваемый cosφ считать нечего —
#: работает сверка ``check_row_arithmetic`` по написанному числу.
RE_STATED_POWER_FACTOR = re.compile(
    r"соs\s*[фf]?\s*[=\s]\s*(?P<value>[01](?:[.,]\d+)?)", re.IGNORECASE
)

#: Пятижильный кабель — три фазы, нейтраль и защитный проводник. Запись бывает
#: «5х16», «2х(5х95)», «3хППГнг(А)-HF 5х150мм²». Предшествующая цифра
#: запрещена, иначе «35х…» прочтётся как пятижильный.
RE_THREE_PHASE_CABLE = re.compile(r"(?<!\d)5\s*х\s*\d")

#: Мощность, записанная в ваттах: «Рр=10Вт». Приставка «к» отсутствует.
#:
#: Только РАСЧЁТНАЯ мощность (Рр/Рп): с расчётным током тождеством
#: ``P = √3·U·I·cosφ`` связана именно она. У установленной мощности такой связи
#: нет, и «Ру=10Вт» проверить этой арифметикой нельзя. Регистр значим: поиск
#: «вт» без учёта регистра ловит «автостоянка» и «(втч ОЗДС)».
RE_POWER_IN_WATTS = re.compile(
    r"(?P<prefix>[Рp][рпpn])\s*=\s*(?P<value>\d+(?:[.,]\d+)?)\s*Вт(?![А-Яа-яA-Za-z])"
)

#: Резервный ввод: два ввода не работают одновременно, и складывать их нагрузки
#: нельзя — лист прямо об этом говорит. Требуется ТОЧНАЯ пара слов: подстрочный
#: поиск «резерв» исключил бы совершенно рабочие «Резервные баки ГВС».
RE_RESERVE_INPUT = re.compile(r"резервн\w*\s+ввод", re.IGNORECASE)

#: Подпись, объявляющая строку одним из вводов щита: «Рабочий ввод»,
#: «Резервный ввод». Два таких ввода одной секции — не повтор обозначения.
RE_INPUT_MODE = re.compile(r"(рабочий|резервн\w*)\s+ввод", re.IGNORECASE)

#: Сигнальная подпись колонки: короткая буквенная основа и номер («TS1», «TS2»).
RE_SIGNAL_TOKEN = re.compile(r"^(?P<stem>[A-Za-zА-Яа-я]{2,4})(?P<index>\d{1,2})$")

FACET_DEMAND_POWER = "demand_active_power_kw"
FACET_INSTALLED_POWER = "installed_power_kw"
FACET_CURRENT = "maximum_calculated_current_a"

ROW_KIND_TOTAL = "CONSUMER_TOTAL"


# --------------------------------------------------------------------------
# Вспомогательное
# --------------------------------------------------------------------------
def _stable_id(prefix: str, *parts: Any) -> str:
    payload = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _ru(value: float, digits: int = 1) -> str:
    """Число на языке чертежа: десятичная запятая, без хвостовых нулей.

    Хвостовые нули срезаются только у дробной части: у целого «380» они
    значащие, и обрезка превратила бы напряжение в «38».
    """
    text = f"{round(float(value), digits):.{digits}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return (text or "0").replace(".", ",")


def _row_text(row: Mapping[str, Any]) -> str:
    """Весь текст строки: подписи, кабели и сырые прогоны значений."""
    parts = [str(row.get("consumer_label") or "")]
    parts.extend(str(item) for item in row.get("cables") or ())
    parts.extend(
        str(value.get("raw_run") or "")
        for value in row.get("values") or ()
    )
    return to_cyrillic(" ".join(part for part in parts if part))


def _single_value(row: Mapping[str, Any], facet: str) -> Optional[dict[str, Any]]:
    """Единственное однозначное значение свойства строки.

    Два значения одного свойства («Рр=143,2/176,8 кВт» — рабочий и пожарный
    режимы) однозначного числа не дают, и выбирать из них нельзя.
    """
    found = [
        value
        for value in row.get("values") or ()
        if value.get("facet_ref") == facet and len(value.get("values") or ()) == 1
    ]
    if len(found) != 1:
        return None
    return found[0]


def _leading_value(row: Mapping[str, Any], facet: str) -> Optional[dict[str, Any]]:
    """Свойство строки целиком, включая многозначные подписи режимов."""
    found = [
        value
        for value in row.get("values") or ()
        if value.get("facet_ref") == facet and (value.get("values") or ())
    ]
    if len(found) != 1:
        return None
    return found[0]


def _mark(row: Mapping[str, Any]) -> str:
    """Марка линии так, как она напечатана на выноске."""
    label = str(row.get("consumer_label") or "").split("|")[0].strip()
    return label or str(row.get("consumer_designation") or "")


def _is_three_phase(text: str) -> bool:
    return bool(RE_THREE_PHASE_CABLE.search(text))


def _stated_power_factor(text: str) -> Optional[float]:
    match = RE_STATED_POWER_FACTOR.search(text)
    if not match:
        return None
    try:
        return float(match.group("value").replace(",", "."))
    except ValueError:
        return None


def proven_line_voltage_kv(texts: Iterable[str]) -> Optional[float]:
    """Линейное напряжение, ДОКАЗАННОЕ надписью на листе.

    Подставлять 0,4 кВ по умолчанию нельзя: тогда проверка cosφ считала бы
    напряжение за проектировщика, а на листе иного класса напряжения дала бы
    заведомо неверный результат. Нет надписи — нет и проверки.

    Надпись ищется и в отдельных словах, и в их склейке: вектор-слой разбивает
    «~380/220В» на «~380/» и «220В», и по отдельности ни одно из слов
    напряжения не доказывает.
    """
    joined = " ".join(str(text or "") for text in texts)
    if RE_LINE_VOLTAGE.search(to_cyrillic(joined)):
        return LINE_VOLTAGE_KV
    return None


def _finding(
    kind: str,
    *,
    side: str,
    subject: Optional[str],
    summary: str,
    evidence: Mapping[str, Any],
    verdict: str = VERDICT_CONFIRMED,
    row_id: Optional[str] = None,
    identity: Sequence[Any] = (),
) -> dict[str, Any]:
    return {
        "inconsistency_id": _stable_id("dinc", side, kind, *identity),
        "kind": kind,
        "verdict": verdict,
        "side": side,
        "row_id": row_id,
        "subject": subject,
        "summary": summary,
        "evidence": dict(evidence),
        "producer": PRODUCER,
    }


# --------------------------------------------------------------------------
# Правило 1. Физически невозможный подразумеваемый cosφ
# --------------------------------------------------------------------------
def implied_power_factor_conflicts(
    table: Mapping[str, Any],
    *,
    line_voltage_kv: Optional[float],
) -> list[dict[str, Any]]:
    """Мощность и ток строки несовместимы ни при каком коэффициенте мощности.

    Из ``P = √3·U·I·cosφ`` следует ``cosφ = P / (√3·U·I)``. Коэффициент мощности
    больше единицы физически невозможен: активная мощность не бывает больше
    полной. Значит одно из двух чисел подписи ошибочно, и лист противоречит сам
    себе.

    Считается ТОЛЬКО расчётная активная мощность (``Рр``). Установленная (``Ру``)
    для этого непригодна: расчётный ток соответствует расчётной мощности, а не
    установленной, и у строки «Ру=30 кВт, Рр=15 кВт, Iр=32,6 А» подстановка
    ``Ру`` дала бы cosφ 1,4 там, где лист совершенно исправен.

    Если cosφ на листе НАПИСАН, правило молчит: такую строку уже проверяет
    ``electrical_load_table.check_row_arithmetic`` по написанному числу, и вторая
    находка о том же была бы дублем.
    """
    if not line_voltage_kv:
        return []
    findings: list[dict[str, Any]] = []
    for row in table.get("rows") or ():
        power = _single_value(row, FACET_DEMAND_POWER)
        current = _single_value(row, FACET_CURRENT)
        if not power or not current:
            continue
        amperes = float(current["values"][0])
        if amperes <= 0:
            continue
        text = _row_text(row)
        if _stated_power_factor(text) is not None:
            continue
        if not _is_three_phase(text):
            # Трёхфазность не доказана: у однофазной линии знаменатель другой,
            # и «невозможный» cosφ оказался бы артефактом формулы.
            continue
        if power.get("reading") != "PREFIXED" and power.get("order_proof") != "EQUAL_VALUES":
            # Свойство доказано, только когда подпись сама его назвала («Рр=»)
            # либо оба числа беспрефиксной полосы равны — тогда ответ не
            # зависит от порядка колонок. Одиночное число полосы отнесено к
            # расчётной мощности по соглашению, и строить на нём утверждение о
            # физической невозможности нельзя.
            continue
        if amperes < MIN_CHECKED_CURRENT_A:
            continue
        kilowatts = float(power["values"][0])
        implied = kilowatts / (_SQRT3 * line_voltage_kv * amperes)
        if implied <= IMPOSSIBLE_POWER_FACTOR:
            continue
        # Наименьший ток, которым такая мощность вообще может течь: cosφ = 1.
        minimum_current = kilowatts / (_SQRT3 * line_voltage_kv)
        if minimum_current - amperes < IMPOSSIBLE_CURRENT_MARGIN_A:
            continue
        # Линия называется своей напечатанной подписью: одинаковый текст у двух
        # разных линий («1ГРЩ-ШУ.ХП» и «2ГРЩ-ШУ.ХП») слился бы в отчёте в одну
        # строку, и инженер не понял бы, к какой из них замечание.
        subject = (
            str(row.get("consumer_label") or "").strip()
            or row.get("consumer_designation")
        )
        findings.append(
            _finding(
                KIND_IMPLIED_POWER_FACTOR,
                side=str(row.get("side") or ""),
                subject=subject,
                row_id=row.get("row_id"),
                identity=(row.get("row_id"), kilowatts, amperes),
                summary=(
                    f"Расчётная мощность и ток линии «{subject}» физически"
                    f" несовместимы: при {_ru(kilowatts)} кВт и {_ru(amperes)} А"
                    f" коэффициент мощности получается {_ru(implied, 2)}, а больше"
                    f" единицы он быть не может. При напряжении"
                    f" {_ru(line_voltage_kv * 1000, 0)} В и пятижильном кабеле"
                    f" мощность {_ru(kilowatts)} кВт требует не меньше"
                    f" {_ru(minimum_current)} А даже при cosφ=1."
                    " Какое из двух чисел ошибочно, система не решает."
                ),
                evidence={
                    "raw": power.get("raw_run"),
                    "power_kw": kilowatts,
                    "current_a": amperes,
                    "line_voltage_kv": line_voltage_kv,
                    "implied_power_factor": round(implied, 3),
                    "minimum_current_a": round(minimum_current, 1),
                    "current_margin_a": round(minimum_current - amperes, 1),
                    "threshold": IMPOSSIBLE_POWER_FACTOR,
                    "power_reading": power.get("reading"),
                    "power_order_proof": power.get("order_proof"),
                    "three_phase_proof": "cable_five_core",
                    "bbox": row.get("bbox"),
                    "reason": "implied power factor exceeds unity",
                },
            )
        )
    return findings


# --------------------------------------------------------------------------
# Правило 2. Сводная строка против суммы вводов
# --------------------------------------------------------------------------
def _load_groups(table: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Группы «сводная строка + её вводы», собранные по обозначению.

    В группу попадают только связанные строки: несвязанная строка не вправе ни
    доказывать инвариант, ни расходиться с ним.
    """
    groups: dict[str, dict[str, Any]] = {}
    for row in table.get("rows") or ():
        if row.get("binding_status") != BOUND:
            continue
        designation = row.get("consumer_designation")
        if not designation:
            continue
        group = groups.setdefault(str(designation), {"total": [], "inputs": []})
        key = "total" if row.get("row_kind") == ROW_KIND_TOTAL else "inputs"
        group[key].append(row)
    return groups


def _group_arithmetic(group: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    """Сумма вводов против сводного значения — или ``None``, если считать нельзя.

    Группа отбрасывается целиком, если хотя бы у одного ввода мощность не
    прочиталась: сумма неполного набора — это не сумма, а меньшее число, и
    сравнивать её со сводкой значило бы выдать пробел распознавания за ошибку
    чертежа.
    """
    totals = group.get("total") or []
    inputs = group.get("inputs") or []
    if len(totals) != 1 or not inputs:
        return None
    total = totals[0]
    # «Не фидер» — это остаточный класс: в него попадают и подписи аппаратов, и
    # трансформаторы, и итоги режимов. Сводной строку делает положительный
    # признак: она несёт обе мощности, установленную и расчётную, — так устроена
    # именно сводная таблица потребителей.
    if not _leading_value(total, FACET_INSTALLED_POWER):
        return None
    if any(RE_RESERVE_INPUT.search(_row_text(row)) for row in inputs):
        # Взаимно резервируемые вводы не работают одновременно: их нагрузки
        # складывать нельзя, и сводка законно равна одному вводу.
        return None
    sections = [row.get("section_ref") for row in inputs]
    if len(inputs) != len(set(sections)) or any(section is None for section in sections):
        # Два ввода одной секции — это повтор обозначения, отдельная находка.
        # Приписывать такой группе арифметику значило бы считать по строкам,
        # принадлежность которых потребителю ещё под вопросом.
        return None

    input_powers = [_single_value(row, FACET_DEMAND_POWER) for row in inputs]
    total_power = _leading_value(total, FACET_DEMAND_POWER)
    if not total_power or any(value is None for value in input_powers):
        return None
    summary_power = float(total_power["values"][0])
    sum_power = sum(float(value["values"][0]) for value in input_powers)
    if summary_power <= 0 or sum_power <= 0:
        return None

    input_currents = [_single_value(row, FACET_CURRENT) for row in inputs]
    total_current = _leading_value(total, FACET_CURRENT)
    summary_current = sum_current = None
    if total_current and all(value is not None for value in input_currents):
        summary_current = float(total_current["values"][0])
        candidate = sum(float(value["values"][0]) for value in input_currents)
        if summary_current > 0 and candidate > 0:
            sum_current = candidate
        else:
            summary_current = None

    tolerance = max(
        SUMMARY_TOLERANCE,
        ROUNDING_STEP_KW * (len(inputs) + 1) / max(sum_power, summary_power),
    )
    installed = _leading_value(total, FACET_INSTALLED_POWER)
    installed_power = float(installed["values"][0]) if installed else None
    return {
        "total_row": total,
        "input_rows": list(inputs),
        "tolerance": tolerance,
        "installed_power_kw": installed_power,
        "summary_power_kw": summary_power,
        "input_powers_kw": [float(value["values"][0]) for value in input_powers],
        "sum_power_kw": sum_power,
        "power_deviation": abs(sum_power - summary_power) / max(sum_power, summary_power),
        "power_ratio": summary_power / sum_power,
        "summary_current_a": summary_current,
        "sum_current_a": sum_current,
        "current_ratio": (summary_current / sum_current) if sum_current else None,
    }


def summary_invariant(table: Mapping[str, Any]) -> dict[str, Any]:
    """Доказан ли на ЭТОЙ таблице закон «сводка = сумма вводов».

    Складывать нагрузки вводов вправе не всякая таблица: сводная колонка может
    показывать расчётную нагрузку с коэффициентом одновременности, режим,
    отличный от режима вводов, или вовсе другую величину. Поэтому сначала закон
    подтверждается на самой таблице — на группах, где равенство выполняется в
    пределах округления, — и только потом расхождения остальных групп получают
    право быть находкой.

    Групп с одним вводом мало: они доказывают лишь отсутствие коэффициента
    одновременности. Складываемость проверяют группы с двумя и более вводами,
    поэтому их требуется не меньше двух.
    """
    proven_groups: list[dict[str, Any]] = []
    multi_input = 0
    checked = 0
    for designation, group in sorted(_load_groups(table).items()):
        arithmetic = _group_arithmetic(group)
        if not arithmetic:
            continue
        checked += 1
        if arithmetic["power_deviation"] > arithmetic["tolerance"]:
            continue
        proven_groups.append(
            {
                "designation": designation,
                "inputs": len(arithmetic["input_rows"]),
                "sum_power_kw": round(arithmetic["sum_power_kw"], 3),
                "summary_power_kw": round(arithmetic["summary_power_kw"], 3),
                "deviation": round(arithmetic["power_deviation"], 5),
            }
        )
        if len(arithmetic["input_rows"]) > 1:
            multi_input += 1
    proven = (
        len(proven_groups) >= INVARIANT_MIN_GROUPS
        and multi_input >= INVARIANT_MIN_MULTI_INPUT_GROUPS
    )
    return {
        "proven": proven,
        "groups_checked": checked,
        "groups_equal": len(proven_groups),
        "groups_equal_multi_input": multi_input,
        "required_groups": INVARIANT_MIN_GROUPS,
        "required_multi_input_groups": INVARIANT_MIN_MULTI_INPUT_GROUPS,
        "tolerance": SUMMARY_TOLERANCE,
        "evidence": proven_groups,
    }


def _mismatch_is_explainable(
    arithmetic: Mapping[str, Any], *, conflicted_rows: frozenset[str]
) -> Optional[str]:
    """Почему расхождение НЕ является ошибкой величины, если это так.

    Проверок четыре, и каждая закрывает свой класс ложных находок.

    Испорченное слагаемое. Строка, уже объявленная противоречивой сверкой
    «мощность / cosφ / ток», не вправе быть слагаемым: расхождение группы тогда
    не новая находка, а следствие уже отчитанной. У ВРУа расчёт из указанного
    тока даёт 117,8 кВт вместо напечатанных 307,6, и с этим числом группа
    сходится — дефект один, и он уже назван.

    Недосчитанный ввод. Если сводка кратна сумме вводов целым числом больше
    единицы, и кратность одинакова по мощности и по току, — на листе не
    ошибочное число, а пропущенная строка. У ВРУ1 это ровно 2,01 и 2,00: второй
    ввод на листе есть, но подписан чужим обозначением, и об этом говорит
    отдельная находка о повторе обозначения. Утверждать здесь «сводка не
    совпадает» значило бы назвать верную сводку ошибочной.

    Другая колонка. Если сумма вводов совпадает не с расчётной мощностью
    сводки, а с её установленной, сравниваются разные величины: у ВРУ2
    14,9 + 20,4 = 35,3 — это в точности Ру сводки, а не её Рр.

    Коэффициент одновременности. Уменьшить расчётную нагрузку против суммы
    вводов — обычная практика. Признак: сводка меньше суммы И масштаб одинаков
    для мощности и для тока. Увеличить одновременность не умеет, поэтому
    превышение сводки над суммой этим не объясняется.
    """
    if any(
        str(row.get("row_id")) in conflicted_rows
        for row in [arithmetic["total_row"], *arithmetic["input_rows"]]
    ):
        return "row_already_reported_as_contradictory"

    power_ratio = arithmetic["power_ratio"]
    current_ratio = arithmetic.get("current_ratio")
    if current_ratio is not None:
        nearest = round(power_ratio)
        if nearest > 1 and abs(power_ratio - nearest) <= INTEGER_MULTIPLE_TOLERANCE:
            if abs(current_ratio - nearest) <= INTEGER_MULTIPLE_TOLERANCE:
                return "inputs_incomplete_summary_is_integer_multiple"

    installed = arithmetic.get("installed_power_kw")
    if (
        installed
        and abs(installed - arithmetic["summary_power_kw"]) > 1e-9
        and abs(arithmetic["sum_power_kw"] - installed) / max(arithmetic["sum_power_kw"], installed)
        <= arithmetic["tolerance"]
    ):
        return "sum_matches_installed_power_column"

    if power_ratio > 1.0 + arithmetic["tolerance"]:
        return None
    if current_ratio is None:
        # Ток не прочитан — проверить равномерность масштаба нечем. Молчим:
        # уменьшение против суммы вводов само по себе ошибкой не является.
        return "demand_factor_not_refutable_without_current"
    scale_gap = abs(power_ratio - current_ratio) / max(power_ratio, current_ratio)
    if scale_gap <= SCALING_TOLERANCE:
        return "uniform_demand_factor"
    return None


def _contradictory_row_ids(table: Mapping[str, Any]) -> frozenset[str]:
    """Строки, чью арифметику таблица уже объявила противоречивой."""
    return frozenset(
        str(item.get("row_id"))
        for item in table.get("contradictions") or ()
        if item.get("kind") == ROW_ARITHMETIC_CONFLICT and item.get("row_id")
    )


def summary_input_mismatches(table: Mapping[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Сводные строки, не сходящиеся с суммой своих вводов."""
    invariant = summary_invariant(table)
    conflicted = _contradictory_row_ids(table)
    suppressed: list[dict[str, Any]] = []
    invariant["suppressed"] = suppressed
    if not invariant["proven"]:
        return [], invariant

    findings: list[dict[str, Any]] = []
    for designation, group in sorted(_load_groups(table).items()):
        arithmetic = _group_arithmetic(group)
        if not arithmetic:
            continue
        if arithmetic["power_deviation"] <= arithmetic["tolerance"]:
            continue
        reason = _mismatch_is_explainable(arithmetic, conflicted_rows=conflicted)
        if reason:
            suppressed.append(
                {
                    "designation": designation,
                    "reason": reason,
                    "sum_power_kw": round(arithmetic["sum_power_kw"], 3),
                    "summary_power_kw": arithmetic["summary_power_kw"],
                    "power_ratio": round(arithmetic["power_ratio"], 4),
                    "current_ratio": (
                        round(arithmetic["current_ratio"], 4)
                        if arithmetic.get("current_ratio") is not None
                        else None
                    ),
                }
            )
            continue
        total = arithmetic["total_row"]
        powers = arithmetic["input_powers_kw"]
        summary_power = arithmetic["summary_power_kw"]
        if len(powers) == 1:
            head = (
                f"На листе приведён один ввод {_ru(powers[0])} кВт, а в сводной"
                f" строке указано {_ru(summary_power)} кВт."
            )
        else:
            head = (
                f"{' + '.join(_ru(value) for value in powers)} ="
                f" {_ru(arithmetic['sum_power_kw'])} кВт,"
                f" а в сводной строке указано {_ru(summary_power)} кВт."
            )
        tail = ""
        if arithmetic.get("current_ratio") is not None:
            if abs(arithmetic["current_ratio"] - 1.0) <= SUMMARY_TOLERANCE:
                tail = (
                    " Расчётные токи вводов при этом складываются точно"
                    f" ({_ru(arithmetic['sum_current_a'])} А), значит расходится"
                    " именно мощность."
                )
            else:
                tail = (
                    f" Сводный ток {_ru(arithmetic['summary_current_a'])} А против"
                    f" {_ru(arithmetic['sum_current_a'])} А по вводам."
                )
        findings.append(
            _finding(
                KIND_SUMMARY_INPUT_MISMATCH,
                side=str(total.get("side") or ""),
                subject=designation,
                row_id=total.get("row_id"),
                identity=(designation, summary_power, arithmetic["sum_power_kw"]),
                summary=(
                    f"Сводная расчётная мощность «{designation}» не совпадает с"
                    f" суммой вводов. {head}{tail}"
                ),
                evidence={
                    "summary_row_id": total.get("row_id"),
                    "input_row_ids": [row.get("row_id") for row in arithmetic["input_rows"]],
                    "input_powers_kw": powers,
                    "sum_power_kw": round(arithmetic["sum_power_kw"], 3),
                    "summary_power_kw": summary_power,
                    "difference_kw": round(arithmetic["sum_power_kw"] - summary_power, 3),
                    "deviation": round(arithmetic["power_deviation"], 4),
                    "summary_current_a": arithmetic.get("summary_current_a"),
                    "sum_current_a": arithmetic.get("sum_current_a"),
                    "bbox": total.get("bbox"),
                    "invariant": {
                        key: invariant[key]
                        for key in ("groups_equal", "groups_equal_multi_input", "evidence")
                    },
                    "reason": "summary value disagrees with the sum of its inputs",
                },
            )
        )
    return findings, invariant


# --------------------------------------------------------------------------
# Правило 3. Повтор обозначения внутри одной секции
# --------------------------------------------------------------------------
def _row_signature(row: Mapping[str, Any]) -> tuple:
    values = tuple(
        sorted(
            (str(value.get("facet_ref")), tuple(value.get("values") or ()))
            for value in row.get("values") or ()
        )
    )
    return (values, tuple(row.get("cables") or ()))


def duplicate_designations(table: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Одно обозначение у двух разных линий одной секции.

    ``2ГРЩ-ВРУ3`` не может обозначать сразу две линии второй секции: одна из
    подписей ошибочна. Какая именно — из листа не следует, и модуль этого не
    решает; он только показывает обе строки.

    Строки с одинаковым набором значений и одинаковым кабелем считаются одной и
    той же линией, прочитанной дважды, и находки не дают.
    """
    buckets: dict[tuple, list[Mapping[str, Any]]] = {}
    for row in table.get("rows") or ():
        if row.get("binding_status") != BOUND:
            continue
        section = row.get("section_ref")
        designation = row.get("consumer_designation")
        if not section or not designation:
            # Сводная таблица не имеет секции: её строки — итог по потребителю,
            # а не отдельные линии, и повтором обозначения не являются.
            continue
        if row.get("row_kind") == ROW_KIND_TOTAL:
            continue
        if not (row.get("own_designations") or row.get("feeder_designations")):
            # Обозначение получено только из ряда подписей, то есть привязано
            # геометрически. Повтор такой привязки — это повтор привязки, а не
            # повтор подписи на чертеже.
            continue
        buckets.setdefault((str(section), str(designation)), []).append(row)

    findings: list[dict[str, Any]] = []
    for (section, designation), rows in sorted(buckets.items()):
        if len(rows) < 2:
            continue
        if len({_row_signature(row) for row in rows}) < 2:
            continue
        if len({row.get("input_number") for row in rows}) > 1:
            # «Ввод 1» и «ввод 2» одного щита — не дубль обозначения, а два
            # ввода, которые лист сам различает номером.
            continue
        if any(RE_INPUT_MODE.search(_row_text(row)) for row in rows):
            continue
        described = []
        for row in rows:
            # Печатается то, что напечатано на листе: канонический вид
            # («ЩНО» → «ШНО», «ВРУа» → «ВРУ-А») инженер на чертеже не найдёт.
            label = str(row.get("consumer_label") or "").strip() or "подпись не прочитана"
            power = _single_value(row, FACET_DEMAND_POWER)
            current = _single_value(row, FACET_CURRENT)
            marks = []
            if power:
                marks.append(f"Рр={_ru(power['values'][0])} кВт")
            if current:
                marks.append(f"Iр={_ru(current['values'][0])} А")
            described.append(f"«{label}» ({'; '.join(marks) or 'значения не прочитаны'})")
        findings.append(
            _finding(
                KIND_DUPLICATE_DESIGNATION,
                side=str(rows[0].get("side") or ""),
                subject=designation,
                row_id=rows[0].get("row_id"),
                identity=(section, designation, *(row.get("row_id") for row in rows)),
                summary=(
                    f"Обозначение «{designation}» в секции {section} стоит у"
                    f" {len(rows)} разных линий: {' и '.join(described)}."
                    " Это разные линии, и одна из подписей ошибочна."
                    " Какая именно — решает инженер: система подписи не правит."
                ),
                evidence={
                    "section_ref": section,
                    "row_ids": [row.get("row_id") for row in rows],
                    "labels": [row.get("consumer_label") for row in rows],
                    "bbox": rows[0].get("bbox"),
                    "bboxes": [row.get("bbox") for row in rows],
                    "reason": "one designation on two different lines of one section",
                },
            )
        )
    return findings


# --------------------------------------------------------------------------
# Правило 4. Единица измерения мощности
# --------------------------------------------------------------------------
def _column_total_row(
    row: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> Optional[Mapping[str, Any]]:
    """Сводный блок потребителя НАД этой колонкой.

    Ряд сводных блоков перпендикулярен колонкам, поэтому блок принадлежит той
    колонке, чью поперечную полосу он перекрывает. Привязка принимается только
    при двукратном перевесе над вторым кандидатом — та же дисциплина, что и при
    привязке ряда обозначений: подпись, накрывающая две колонки, не доказывает
    ничего.
    """
    cross = row.get("cross_range") or []
    if len(cross) != 2:
        return None
    candidates: list[tuple[float, Mapping[str, Any]]] = []
    for other in rows:
        if other is row or other.get("row_id") == row.get("row_id"):
            continue
        if other.get("orientation") == row.get("orientation"):
            continue
        if other.get("row_kind") != ROW_KIND_TOTAL:
            continue
        along = other.get("along_range") or []
        if len(along) != 2:
            continue
        overlap = min(float(along[1]), float(cross[1])) - max(float(along[0]), float(cross[0]))
        if overlap > 0:
            candidates.append((overlap, other))
    if not candidates:
        return None
    candidates.sort(key=lambda item: -item[0])
    if len(candidates) > 1 and candidates[0][0] < 2.0 * candidates[1][0]:
        return None
    return candidates[0][1]


def power_unit_conflicts(
    table: Mapping[str, Any],
    *,
    line_voltage_kv: Optional[float],
) -> list[dict[str, Any]]:
    """Мощность записана в ваттах там, где тот же лист пишет её в киловаттах.

    Первично здесь НЕЗАВИСИМОЕ ДОКАЗАТЕЛЬСТВО, а не арифметика: над колонкой
    стоит сводный блок того же потребителя, и он печатает ту же величину с
    правильной единицей — «Рр=10Вт» на выноске против «Pp=10кВт» в блоке. Два
    напечатанных токена одного свойства одного объекта противоречат друг другу,
    и это видно без всякого расчёта.

    Арифметика по расчётному току и написанному cosφ той же подписи остаётся
    вторичной сверкой: она подтверждает, какое из двух прочтений верно. Одной
    её мало — «10 Вт выглядит слишком мало» не доказательство, и без
    независимого токена находка не выпускается.
    """
    if not line_voltage_kv:
        return []
    rows = list(table.get("rows") or ())
    findings: list[dict[str, Any]] = []
    for row in rows:
        text = _row_text(row)
        match = RE_POWER_IN_WATTS.search(text)
        if not match:
            continue
        current = _single_value(row, FACET_CURRENT)
        power_factor = _stated_power_factor(text)
        if not current or power_factor is None or power_factor <= 0:
            continue
        if not _is_three_phase(text):
            continue
        amperes = float(current["values"][0])
        if amperes <= 0:
            continue
        try:
            stated = float(match.group("value").replace(",", "."))
        except ValueError:
            continue
        if stated <= 0:
            continue
        expected_kw = _SQRT3 * line_voltage_kv * amperes * power_factor
        as_kilowatts = abs(stated - expected_kw) / max(stated, expected_kw)
        as_watts = expected_kw / (stated / 1000.0)
        if as_kilowatts > UNIT_ARITHMETIC_TOLERANCE:
            continue
        if as_watts < UNIT_MAGNITUDE_FACTOR:
            continue

        witness = _column_total_row(row, rows)
        if not witness:
            continue
        witness_power = _leading_value(witness, FACET_DEMAND_POWER)
        if not witness_power:
            continue
        witness_kw = float(witness_power["values"][0])
        if abs(witness_kw - stated) > 1e-9:
            # Независимый токен обязан нести ТО ЖЕ число. Иначе он говорит о
            # другой величине, и противоречия единиц не доказывает.
            continue

        # Объект называет сводный блок собственной колонки, а не обозначение,
        # снятое с марки кабеля: марку могли скопировать с соседней колонки, и
        # тогда находка отправила бы инженера не туда.
        subject = (
            witness.get("consumer_label")
            or witness.get("consumer_designation")
            or row.get("consumer_label")
        )
        findings.append(
            _finding(
                KIND_POWER_UNIT_MISMATCH,
                side=str(row.get("side") or ""),
                subject=subject,
                row_id=row.get("row_id"),
                identity=(row.get("row_id"), match.group(0)),
                summary=(
                    f"Мощность потребителя «{subject}» записана на выноске в"
                    f" ваттах: «{match.group(0)}». Тот же лист печатает эту"
                    f" величину в киловаттах — {_ru(witness_kw)} кВт в сводном"
                    f" блоке над этой же колонкой. Расчётный ток {_ru(amperes)} А"
                    f" и cosφ={_ru(power_factor, 2)} той же подписи отвечают"
                    f" примерно {_ru(expected_kw)} кВт, то есть верна запись в"
                    f" киловаттах, а в единице выноски пропущена приставка «к»."
                    f" Сама выноска подписана маркой «{_mark(row)}» —"
                    " искать её на листе следует по подписи потребителя."
                ),
                evidence={
                    "raw": match.group(0),
                    "stated_value": stated,
                    "stated_unit": "Вт",
                    "current_a": amperes,
                    "power_factor": power_factor,
                    "line_voltage_kv": line_voltage_kv,
                    "expected_kw": round(expected_kw, 3),
                    "deviation_as_kilowatts": round(as_kilowatts, 3),
                    "magnitude_as_watts": round(as_watts, 1),
                    "tolerance": UNIT_ARITHMETIC_TOLERANCE,
                    "witness_row_id": witness.get("row_id"),
                    "witness_label": witness.get("consumer_label"),
                    "witness_power_kw": witness_kw,
                    "bbox": row.get("bbox"),
                    "bboxes": [row.get("bbox"), witness.get("bbox")],
                    "reason": "unit contradicts the current and power factor of the same label",
                },
            )
        )
    return findings


# --------------------------------------------------------------------------
# Правило 5. Необычная связь сигнальной цепи
# --------------------------------------------------------------------------
def _outgoing_devices(graph: Mapping[str, Any]) -> list[dict[str, Any]]:
    devices = []
    for node in graph.get("nodes") or ():
        if node.get("type") != "OUTGOING_DEVICE":
            continue
        bbox = node.get("bbox") or []
        section = node.get("section")
        if len(bbox) != 4 or not section:
            continue
        devices.append(
            {
                "id": node.get("id"),
                "label": node.get("label"),
                "display_label": node.get("display_label"),
                "section": str(section),
                "x0": float(bbox[0]),
                "x1": float(bbox[2]),
            }
        )
    return devices


def _attach_to_device(
    token: Mapping[str, Any], devices: Sequence[Mapping[str, Any]]
) -> Optional[Mapping[str, Any]]:
    """Колонка, которой принадлежит подпись, — или ``None`` при неоднозначности.

    Требование двукратного перевеса над вторым кандидатом — та же дисциплина,
    что и при привязке ряда обозначений: подпись, накрывающая две колонки,
    не доказывает ничего.
    """
    overlaps = []
    for device in devices:
        overlap = min(token["x1"], device["x1"]) - max(token["x0"], device["x0"])
        if overlap > 0:
            overlaps.append((overlap, device))
    if not overlaps:
        return None
    overlaps.sort(key=lambda item: -item[0])
    if len(overlaps) > 1 and overlaps[0][0] < 2.0 * overlaps[1][0]:
        return None
    return overlaps[0][1]


def signal_link_outliers(
    evidence: Any,
    graph: Mapping[str, Any],
    *,
    side: str,
) -> list[dict[str, Any]]:
    """Одна колонка секции подписана не так, как все остальные её колонки.

    На правом листе над каждой отходящей линией стоит подпись сигнальной цепи.
    Все колонки первой секции подписаны одинаково, все колонки второй — тоже, и
    единственное отличие внутри секции выглядит опиской. «Выглядит» — не
    «является»: подпись могла относиться к соседней колонке, поэтому находка
    выдаётся как требующая проверки, а не как доказанное противоречие.

    Ряд принимается только целиком привязанным: если хоть одна подпись ряда
    накрывает две колонки сразу, ряд смещён относительно колонок, и исключение
    было бы артефактом привязки, а не ошибкой чертежа.
    """
    devices = _outgoing_devices(graph)
    if len(devices) < 2 * SIGNAL_MIN_PER_SECTION:
        return []
    words = list(getattr(evidence, "visual_words", None) or ())
    if not words:
        return []

    candidates: list[dict[str, Any]] = []
    for word in words:
        try:
            text = str(word[4] or "").strip()
            x0, y0, x1, y1 = (float(word[0]), float(word[1]), float(word[2]), float(word[3]))
        except (IndexError, TypeError, ValueError):
            continue
        match = RE_SIGNAL_TOKEN.match(to_cyrillic(text))
        if not match:
            continue
        candidates.append(
            {
                "text": text,
                "stem": match.group("stem").upper(),
                "index": match.group("index"),
                "label": text,
                "x0": x0,
                "x1": x1,
                "y": (y0 + y1) / 2.0,
                "height": max(y1 - y0, 1.0),
                "bbox": [x0, y0, x1, y1],
            }
        )
    if not candidates:
        return []
    heights = sorted(token["height"] for token in candidates)
    band_size = max(heights[len(heights) // 2], 1.0)

    # Ряд — это подписи на одной высоте с одинаковой буквенной основой. Высота
    # берётся общей для листа, а не своя у каждого слова: иначе один и тот же
    # ряд распался бы на несколько из-за разницы в начертании знаков.
    bands: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for token in candidates:
        bands.setdefault(
            (token["stem"], int(round(token["y"] / band_size))), []
        ).append(token)

    device_labels = {
        str(device.get("label") or "").upper() for device in devices
    } | {str(device.get("display_label") or "").upper() for device in devices}
    device_labels.discard("")

    findings: list[dict[str, Any]] = []
    for (stem, _band), tokens in sorted(bands.items()):
        if len(tokens) < 2 * SIGNAL_MIN_PER_SECTION:
            continue
        if any(token["text"].upper() in device_labels for token in tokens):
            # Это ряд обозначений самих аппаратов, а не сигнальных цепей.
            # Расхождение подписи аппарата с геометрией секции — отдельная,
            # уже существующая находка, и второй раз её выдавать нельзя.
            continue
        attached: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
        registered = True
        for token in tokens:
            device = _attach_to_device(token, devices)
            if device is None:
                registered = False
                break
            attached.append((token, device))
        if not registered or not attached:
            continue
        if len({device["id"] for _token, device in attached}) != len(attached):
            # Две подписи на одной колонке — ряд не является рядом колонок.
            continue

        by_section: dict[str, list[tuple[Mapping[str, Any], Mapping[str, Any]]]] = {}
        for token, device in attached:
            by_section.setdefault(device["section"], []).append((token, device))
        if len(by_section) < 2:
            continue
        if any(len(items) < SIGNAL_MIN_PER_SECTION for items in by_section.values()):
            continue
        # Хотя бы одна секция обязана быть подписана единогласно. Иначе ряд
        # подписей не выражает никакого правила, и «исключением» здесь считать
        # нечего.
        if not any(
            len({token["index"] for token, _device in items}) == 1
            for items in by_section.values()
        ):
            continue
        for section, items in sorted(by_section.items()):
            counts: dict[str, list[Mapping[str, Any]]] = {}
            labels: dict[str, str] = {}
            for token, device in items:
                counts.setdefault(token["index"], []).append(device)
                labels.setdefault(token["index"], token["label"])
            if len(counts) != 2:
                continue
            ordered = sorted(counts.items(), key=lambda item: -len(item[1]))
            (majority_index, majority), (minority_index, minority) = ordered
            if len(minority) != 1 or len(majority) < SIGNAL_MIN_PER_SECTION:
                continue
            # В тексте показывается подпись КАК НАПЕЧАТАНА. Сведение омоглифов
            # нужно только для группировки: инженер ищет на листе «TS1»,
            # а не «ТS1» со смешанными алфавитами.
            minority_label = labels[minority_index]
            majority_label = labels[majority_index]
            device = minority[0]
            name = device.get("display_label") or device.get("label") or device.get("id")
            findings.append(
                _finding(
                    KIND_SIGNAL_LINK_OUTLIER,
                    side=side,
                    subject=str(device.get("label") or name),
                    verdict=VERDICT_REVIEW,
                    identity=(stem, section, device.get("id")),
                    summary=(
                        f"Сигнальная цепь линии «{name}» подписана"
                        f" «{minority_label}», тогда как все остальные"
                        f" {len(majority)} линий той же секции подписаны"
                        f" «{majority_label}». Требуется проверка."
                    ),
                    evidence={
                        "stem": stem,
                        "section": section,
                        "minority_label": minority_label,
                        "majority_label": majority_label,
                        "majority_count": len(majority),
                        "minority_device": device.get("id"),
                        "bbox": next(
                            token["bbox"]
                            for token, attached_device in items
                            if attached_device["id"] == device["id"]
                        ),
                        "reason": "signal label differs from every other column of its section",
                    },
                )
            )
    return findings


# --------------------------------------------------------------------------
# Сборка
# --------------------------------------------------------------------------
def detect_document_consistency(
    *,
    load_table: Mapping[str, Any],
    evidence: Any = None,
    graph: Mapping[str, Any] | None = None,
    side: str | None = None,
) -> dict[str, Any]:
    """Все внутренние противоречия одного листа с диагностикой.

    Возвращает контракт ``document-consistency.v1``. Модель не вызывается.
    """
    resolved_side = str(side or load_table.get("side") or "")
    texts: list[str] = []
    if evidence is not None:
        texts = [str(word[4]) for word in getattr(evidence, "visual_words", None) or ()]
    if not texts:
        texts = [_row_text(row) for row in load_table.get("rows") or ()]
    line_voltage_kv = proven_line_voltage_kv(texts)

    power_factor = implied_power_factor_conflicts(
        load_table, line_voltage_kv=line_voltage_kv
    )
    summary, invariant = summary_input_mismatches(load_table)
    duplicates = duplicate_designations(load_table)
    units = power_unit_conflicts(load_table, line_voltage_kv=line_voltage_kv)
    signals: list[dict[str, Any]] = []
    if evidence is not None and graph:
        signals = signal_link_outliers(evidence, graph, side=resolved_side)

    items = power_factor + summary + duplicates + units + signals
    items.sort(key=lambda item: (item["kind"], item["inconsistency_id"]))
    return {
        "contract_version": CONTRACT_VERSION,
        "producer": PRODUCER,
        "side": resolved_side,
        "items": items,
        "counts": {
            "total": len(items),
            "confirmed": sum(1 for item in items if item["verdict"] == VERDICT_CONFIRMED),
            "review": sum(1 for item in items if item["verdict"] == VERDICT_REVIEW),
            KIND_IMPLIED_POWER_FACTOR: len(power_factor),
            KIND_SUMMARY_INPUT_MISMATCH: len(summary),
            KIND_DUPLICATE_DESIGNATION: len(duplicates),
            KIND_POWER_UNIT_MISMATCH: len(units),
            KIND_SIGNAL_LINK_OUTLIER: len(signals),
        },
        "diagnostics": {
            "line_voltage_kv": line_voltage_kv,
            "line_voltage_proven": line_voltage_kv is not None,
            "summary_invariant": invariant,
            "uses_model": False,
            "uses_ocr": False,
        },
    }


__all__ = [
    "CONTRACT_VERSION",
    "IMPOSSIBLE_POWER_FACTOR",
    "KINDS",
    "KIND_DUPLICATE_DESIGNATION",
    "KIND_IMPLIED_POWER_FACTOR",
    "KIND_POWER_UNIT_MISMATCH",
    "KIND_SIGNAL_LINK_OUTLIER",
    "KIND_SUMMARY_INPUT_MISMATCH",
    "LINE_VOLTAGE_KV",
    "PRODUCER",
    "SUMMARY_TOLERANCE",
    "VERDICT_CONFIRMED",
    "VERDICT_REVIEW",
    "detect_document_consistency",
    "duplicate_designations",
    "implied_power_factor_conflicts",
    "power_unit_conflicts",
    "proven_line_voltage_kv",
    "signal_link_outliers",
    "summary_input_mismatches",
    "summary_invariant",
]
