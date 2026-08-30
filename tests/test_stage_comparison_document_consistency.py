"""Внутренние противоречия одного листа — правила и их границы.

Каждая проверка здесь описана парой: случай, на котором правило ОБЯЗАНО
сработать, и соседний случай, на котором оно обязано молчать. Правило без
второй половины пары бесполезно: цель всей группы — ноль ложных находок, а не
максимум находок.
"""
from __future__ import annotations

import pytest

from backend.app.pipeline.stages.block_grounding import document_consistency as dc
from backend.app.pipeline.stages.block_grounding import electrical_load_table as elt


# --------------------------------------------------------------------------
# Сборка синтетических строк
# --------------------------------------------------------------------------
def value(facet, *numbers, raw="", reading="PREFIXED", order_proof=None):
    item = {
        "facet_ref": facet,
        "values": list(numbers),
        "raw_run": raw,
        "reading": reading,
    }
    if order_proof:
        item["order_proof"] = order_proof
    return item


def row(
    row_id,
    *,
    label,
    values,
    kind="FEEDER",
    section="РП1",
    side="LEFT",
    binding=elt.BOUND,
    designation="ШУ-ХП",
    cables=(),
    own=("ШУ-ХП",),
    feeders=(),
    input_number=None,
    cross=(0.0, 10.0),
    along=(0.0, 100.0),
    orientation="V",
):
    return {
        "row_id": row_id,
        "side": side,
        "row_kind": kind,
        "section_ref": section,
        "consumer_label": label,
        "consumer_designation": designation,
        "binding_status": binding,
        "own_designations": list(own),
        "feeder_designations": list(feeders),
        "row_designations": [],
        "input_number": input_number,
        "cables": list(cables),
        "values": list(values),
        "bbox": [0.0, 0.0, 10.0, 10.0],
        "cross_range": list(cross),
        "along_range": list(along),
        "orientation": orientation,
    }


def table(rows, *, side="LEFT", contradictions=()):
    return {
        "contract_version": elt.CONTRACT_VERSION,
        "side": side,
        "rows": list(rows),
        "contradictions": list(contradictions),
    }


SHEET_TEXT = ["~380/", "220В"]


# --------------------------------------------------------------------------
# Правило 1. Невозможный подразумеваемый cosφ
# --------------------------------------------------------------------------
def test_невозможный_косинус_найден():
    """30 кВт не могут течь при 32,6 А: минимум 45,6 А даже при cosφ=1."""
    raw = "Рр=30,0кВт, Iрасч=32,6А"
    rows = [
        row(
            "r1",
            label="1ГРЩ-ШУ.ХП ППГнг(А)-НF 5х16",
            values=[
                value("demand_active_power_kw", 30.0, raw=raw),
                value("maximum_calculated_current_a", 32.6, raw=raw),
            ],
        )
    ]
    found = dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38)
    assert len(found) == 1
    assert found[0]["kind"] == dc.KIND_IMPLIED_POWER_FACTOR
    assert found[0]["verdict"] == dc.VERDICT_CONFIRMED
    assert found[0]["evidence"]["implied_power_factor"] == pytest.approx(1.398, abs=0.002)
    assert found[0]["evidence"]["minimum_current_a"] == pytest.approx(45.6, abs=0.1)
    assert "45,6 А" in found[0]["summary"]
    # Предпосылки формулы стоят в самом тексте: инженер может их проверить.
    assert "380 В" in found[0]["summary"]
    assert "пятижильном кабеле" in found[0]["summary"]
    # Линия названа своей напечатанной подписью, а не канонической свёрткой:
    # у двух линий ШУ-ХП разных секций тексты обязаны различаться.
    assert found[0]["subject"] == "1ГРЩ-ШУ.ХП ППГнг(А)-НF 5х16"


def test_установленная_мощность_не_подставляется_вместо_расчётной():
    """Ру=30 при Рр=15 и Iр=32,6 — исправная строка, а не противоречие.

    Расчётный ток отвечает расчётной мощности. Подстановка установленной дала
    бы ровно тот же cosφ=1,4, что и у настоящей описки, — на строке, где лист
    не ошибается ничем.
    """
    raw = "Py=30 кВт Pp=15 кВт Ip=32,6 А"
    rows = [
        row(
            "r1",
            label="Насосная ХП ШУ-ХВС ППГнг(А)-НF 5х16",
            values=[
                value("installed_power_kw", 30.0, raw=raw),
                value("demand_active_power_kw", 15.0, raw=raw),
                value("maximum_calculated_current_a", 32.6, raw=raw),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38) == []


def test_обычный_косинус_не_является_ошибкой():
    """0,95 — нормальное значение проекта, и сравнивать его не с чем."""
    raw = "Рр=5кВт, Iрасч=8А"
    rows = [
        row(
            "r1",
            label="1ГРЩ-ШНО ППГнг(А)-НF 5х4",
            values=[
                value("demand_active_power_kw", 5.0, raw=raw),
                value("maximum_calculated_current_a", 8.0, raw=raw),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38) == []


def test_округление_не_создаёт_невозможного_косинуса():
    """60 кВт при 91,3 А дают cosφ 0,9985 — у порога, но исправно.

    Строка ближе всех к порогу во всём боевом корпусе. Если правило сработает
    здесь, оно сработает и на любом листе, где ток посчитан при 400 В.
    """
    raw = "Рр=60кВт, Iрасч=91,3А"
    rows = [
        row(
            "r1",
            label="2ГРЩ-ЭБ.ГВС ППГнг(А)-НF 5х35",
            values=[
                value("demand_active_power_kw", 60.0, raw=raw),
                value("maximum_calculated_current_a", 91.3, raw=raw),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38) == []


def test_написанный_косинус_отдаёт_строку_существующей_сверке():
    """Если cosφ напечатан, работает сверка по нему, а не по догадке."""
    raw = "Рр=307,6кВт, cosf=0,87, Iрасч=205,8А"
    rows = [
        row(
            "r1",
            label="1ГРЩ-ВРУа ППГнг(А)-НF 2х(5х95)",
            values=[
                value("demand_active_power_kw", 307.6, raw=raw),
                value("maximum_calculated_current_a", 205.8, raw=raw),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38) == []


def test_косинус_равный_единице_тоже_считается_написанным():
    """«cosf=1» — написанный cosφ, хотя и без ведущего нуля."""
    raw = "Рр=60кВт, cosf=1, Iрасч=86А"
    rows = [
        row(
            "r1",
            label="2ГРЩ-ЭБ.ГВС ППГнг(А)-НF 5х35",
            values=[
                value("demand_active_power_kw", 60.0, raw=raw),
                value("maximum_calculated_current_a", 86.0, raw=raw),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38) == []


def test_малый_ток_не_проверяется():
    """При токе в единицы ампер округление сравнимо с самой величиной."""
    raw = "Рр=6кВт, Iрасч=8А"
    rows = [
        row(
            "r1",
            label="1ГРЩ-ШНО ППГнг(А)-НF 5х4",
            values=[
                value("demand_active_power_kw", 6.0, raw=raw),
                value("maximum_calculated_current_a", 8.0, raw=raw),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38) == []


def test_беспрефиксное_одиночное_число_не_даёт_находки():
    """Одиночное число полосы отнесено к расчётной мощности по соглашению."""
    raw = "30.0 кВт 32.6 А"
    rows = [
        row(
            "r1",
            label="ГРЩ1-РП1-1 ППГнг(А)-HF 5х16",
            values=[
                value(
                    "demand_active_power_kw",
                    30.0,
                    raw=raw,
                    reading="POSITIONAL",
                    order_proof="SINGLE_VALUE",
                ),
                value("maximum_calculated_current_a", 32.6, raw=raw, reading="POSITIONAL"),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38) == []


def test_равные_числа_полосы_доказывают_свойство():
    """«30.0 кВт - 30.0 кВт - 32.6 А»: порядок колонок роли не играет."""
    raw = "30.0 кВт - 30.0 кВт - 32.6 А"
    rows = [
        row(
            "r1",
            label="ГРЩ1-РП1-1 ППГнг(А)-HF 5х16",
            values=[
                value(
                    "demand_active_power_kw",
                    30.0,
                    raw=raw,
                    reading="POSITIONAL",
                    order_proof="EQUAL_VALUES",
                ),
                value("maximum_calculated_current_a", 32.6, raw=raw, reading="POSITIONAL"),
            ],
        )
    ]
    assert len(dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38)) == 1


def test_реактивная_мощность_не_попадает_в_формулу():
    """У компенсатора «cosφ» тождественно равен единице — это не ошибка."""
    raw = "180.0 кВАр 180.0 кВАр 272.7 А"
    rows = [
        row(
            "r1",
            label="ГРЩ1-РП1-15 ППГнг(А)-HF 5х185мм² АУКРМ №1",
            designation="АУКРМ-1",
            values=[
                value(
                    "demand_reactive_power_kvar",
                    180.0,
                    raw=raw,
                    reading="POSITIONAL",
                    order_proof="EQUAL_VALUES",
                ),
                value("maximum_calculated_current_a", 272.7, raw=raw, reading="POSITIONAL"),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38) == []


def test_без_доказанного_напряжения_правило_молчит():
    """Напряжение не подставляется по умолчанию — оно должно быть на листе."""
    raw = "Рр=30,0кВт, Iрасч=32,6А"
    rows = [
        row(
            "r1",
            label="1ГРЩ-ШУ.ХП ППГнг(А)-НF 5х16",
            values=[
                value("demand_active_power_kw", 30.0, raw=raw),
                value("maximum_calculated_current_a", 32.6, raw=raw),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=None) == []
    assert dc.proven_line_voltage_kv(["Рр=30кВт", "Iрасч=32,6А"]) is None
    assert dc.proven_line_voltage_kv(SHEET_TEXT) == dc.LINE_VOLTAGE_KV


def test_однофазная_линия_не_проверяется_трёхфазной_формулой():
    """Без пятижильного кабеля знаменатель формулы не доказан."""
    raw = "Рр=30,0кВт, Iрасч=32,6А"
    rows = [
        row(
            "r1",
            label="1ГРЩ-ШУ.ХП ППГнг(А)-НF 3х2,5",
            values=[
                value("demand_active_power_kw", 30.0, raw=raw),
                value("maximum_calculated_current_a", 32.6, raw=raw),
            ],
        )
    ]
    assert dc.implied_power_factor_conflicts(table(rows), line_voltage_kv=0.38) == []


# --------------------------------------------------------------------------
# Правило 2. Сводная строка против суммы вводов
# --------------------------------------------------------------------------
def _consumer(designation, installed, demand, current, *, current_values=None):
    return row(
        f"total-{designation}",
        label=designation,
        kind="CONSUMER_TOTAL",
        section=None,
        designation=designation,
        own=(designation,),
        orientation="H",
        values=[
            value("installed_power_kw", installed, raw=f"Py={installed} кВт"),
            value("demand_active_power_kw", *(demand if isinstance(demand, tuple) else (demand,)),
                  raw=f"Рр={demand} кВт"),
            value("maximum_calculated_current_a",
                  *(current_values or (current,)), raw=f"Iр={current} А"),
        ],
    )


def _input(designation, section, power, current, *, label=None, cables=("ППГнг(А)-НF 5х16",)):
    raw = f"Рр={power}кВт, cosf=0,9, Iрасч={current}А"
    return row(
        f"in-{designation}-{section}",
        label=label or f"{section[-1]}ГРЩ-{designation} ППГнг(А)-НF 5х16",
        section=section,
        designation=designation,
        own=(),
        feeders=(designation,),
        cables=list(cables),
        values=[
            value("demand_active_power_kw", power, raw=raw),
            value("maximum_calculated_current_a", current, raw=raw),
        ],
    )


def _additive_table(extra_rows=(), contradictions=()):
    """Таблица, на которой закон «сводка = сумма вводов» доказан.

    Четыре группы с точным равенством, из них две с двумя вводами: ровно тот
    порог, ниже которого правило не имеет права ничего утверждать.
    """
    rows = [
        _consumer("ВРУ4", 400.0, 233.8, 374.6),
        _input("ВРУ4", "РП1", 118.2, 189.4),
        _input("ВРУ4", "РП2", 115.6, 185.2),
        _consumer("ШУ-ХЦ", 53.0, 27.4, 132.4),
        _input("ШУ-ХЦ", "РП1", 13.7, 66.2),
        _input("ШУ-ХЦ", "РП2", 13.7, 66.2),
        _consumer("ХМ1", 157.5, 157.5, 360.0),
        _input("ХМ1", "РП1", 157.5, 360.0),
        _consumer("ХМ2", 157.5, 157.5, 360.0),
        _input("ХМ2", "РП2", 157.5, 360.0),
    ]
    rows.extend(extra_rows)
    return table(rows, contradictions=contradictions)


def test_инвариант_таблицы_доказывается_до_всякой_находки():
    invariant = dc.summary_invariant(_additive_table())
    assert invariant["proven"] is True
    assert invariant["groups_equal"] == 4
    assert invariant["groups_equal_multi_input"] == 2


def test_недоказанный_инвариант_запрещает_находки():
    """Двух равенств мало: сводка может нести коэффициент одновременности."""
    rows = [
        _consumer("ВРУ4", 400.0, 233.8, 374.6),
        _input("ВРУ4", "РП1", 118.2, 189.4),
        _input("ВРУ4", "РП2", 115.6, 185.2),
        _consumer("ВРУ2", 60.0, 25.4, 40.6),
        _input("ВРУ2", "РП1", 14.9, 25.1),
        _input("ВРУ2", "РП2", 20.4, 32.6),
    ]
    findings, invariant = dc.summary_input_mismatches(table(rows))
    assert invariant["proven"] is False
    assert findings == []


def test_расхождение_сводки_с_суммой_найдено():
    """Сводка больше суммы вводов и не кратна ей — объяснения нет."""
    extra = [
        _consumer("ВРУ7", 300.0, 150.0, 240.0),
        _input("ВРУ7", "РП1", 40.0, 64.0),
        _input("ВРУ7", "РП2", 60.0, 96.0),
    ]
    findings, invariant = dc.summary_input_mismatches(_additive_table(extra))
    assert invariant["proven"] is True
    assert len(findings) == 1
    finding = findings[0]
    assert finding["kind"] == dc.KIND_SUMMARY_INPUT_MISMATCH
    assert finding["subject"] == "ВРУ7"
    assert finding["evidence"]["sum_power_kw"] == pytest.approx(100.0)
    assert finding["evidence"]["summary_power_kw"] == pytest.approx(150.0)
    assert "40 + 60 = 100 кВт" in finding["summary"]
    assert "150 кВт" in finding["summary"]


def test_расхождение_в_пределах_округления_не_находка():
    """118,2 + 115,6 = 233,8 против напечатанных 233,6 — это округление."""
    extra = [
        _consumer("ВРУ8", 400.0, 233.6, 374.6),
        _input("ВРУ8", "РП1", 118.2, 189.4),
        _input("ВРУ8", "РП2", 115.6, 185.2),
    ]
    findings, _ = dc.summary_input_mismatches(_additive_table(extra))
    assert [item["subject"] for item in findings] == []


def test_малые_числа_не_ловятся_относительным_допуском():
    """0,9 + 1,3 против 2,2: чистое округление даёт 6,8% относительной разницы."""
    extra = [
        _consumer("ВРУ-АПТ", 3.0, 2.3, 3.5),
        _input("ВРУ-АПТ", "РП1", 0.9, 1.4),
        _input("ВРУ-АПТ", "РП2", 1.3, 2.1),
    ]
    findings, _ = dc.summary_input_mismatches(_additive_table(extra))
    assert [item["subject"] for item in findings] == []


def test_коэффициент_одновременности_не_объявляется_ошибкой():
    """Сводка меньше суммы, и масштаб одинаков по мощности и по току."""
    extra = [
        _consumer("ВРУ9", 60.0, 13.6, 26.3),
        _input("ВРУ9", "РП1", 8.7, 17.1),
        _input("ВРУ9", "РП2", 9.3, 18.2),
    ]
    findings, invariant = dc.summary_input_mismatches(_additive_table(extra))
    assert [item["subject"] for item in findings] == []
    assert any(
        item["designation"] == "ВРУ9" and item["reason"] == "uniform_demand_factor"
        for item in invariant["suppressed"]
    )


def test_недосчитанный_ввод_не_объявляется_ошибкой_сводки():
    """Сводка ровно вдвое больше единственного ввода — не хватает строки.

    Это не ошибка величины: второй ввод на листе есть, но подписан чужим
    обозначением, и об этом говорит находка о повторе обозначения.
    """
    extra = [
        _consumer("ВРУ1", 1702.5, 365.7, 628.1),
        _input("ВРУ1", "РП1", 181.8, 314.4),
    ]
    findings, invariant = dc.summary_input_mismatches(_additive_table(extra))
    assert [item["subject"] for item in findings] == []
    assert any(
        item["designation"] == "ВРУ1"
        and item["reason"] == "inputs_incomplete_summary_is_integer_multiple"
        for item in invariant["suppressed"]
    )


def test_сумма_совпавшая_с_установленной_мощностью_не_находка():
    """14,9 + 20,4 = 35,3 — это Ру сводки, а не её Рр: разные колонки."""
    extra = [
        _consumer("ВРУ2", 35.3, 25.4, 40.6),
        _input("ВРУ2", "РП1", 14.9, 25.1),
        _input("ВРУ2", "РП2", 20.4, 32.6),
    ]
    findings, invariant = dc.summary_input_mismatches(_additive_table(extra))
    assert [item["subject"] for item in findings] == []
    assert any(
        item["designation"] == "ВРУ2"
        and item["reason"] == "sum_matches_installed_power_column"
        for item in invariant["suppressed"]
    )


def test_испорченное_слагаемое_не_даёт_второй_находки():
    """Строка, уже объявленная противоречивой, не слагаемое новой находки."""
    extra = [
        _consumer("ВРУ-А", 625.0, 232.8, 398.4),
        _input("ВРУ-А", "РП1", 307.6, 205.8),
        _input("ВРУ-А", "РП2", 111.4, 192.6),
    ]
    conflicts = [{"kind": "ROW_ARITHMETIC_CONFLICT", "row_id": "in-ВРУ-А-РП1"}]
    findings, invariant = dc.summary_input_mismatches(
        _additive_table(extra, contradictions=conflicts)
    )
    assert [item["subject"] for item in findings] == []
    assert any(
        item["reason"] == "row_already_reported_as_contradictory"
        for item in invariant["suppressed"]
    )


def test_резервный_ввод_исключает_группу():
    """Взаимно резервируемые вводы не работают одновременно."""
    reserve = _input("ШУ-АПТ", "РП2", 22.5, 52.7)
    reserve["consumer_label"] += " | Резервный ввод"
    extra = [
        _consumer("ШУ-АПТ", 45.0, 22.5, 52.7),
        _input("ШУ-АПТ", "РП1", 22.5, 52.7),
        reserve,
    ]
    findings, _ = dc.summary_input_mismatches(_additive_table(extra))
    assert [item["subject"] for item in findings] == []


def test_подпись_резервные_баки_не_считается_резервным_вводом():
    """«Резервные баки ГВС» — название потребителя, а не признак резерва."""
    assert dc.RE_RESERVE_INPUT.search("Резервные баки ГВС") is None
    assert dc.RE_RESERVE_INPUT.search("2ГРЩ-ШУ.ХП | Резервный ввод") is not None


def test_группа_без_прочитанной_мощности_ввода_не_сравнивается():
    """Сумма неполного набора — не сумма, а меньшее число."""
    blind = _input("ВРУ5", "РП2", 0.0, 20.0)
    blind["values"] = [value("maximum_calculated_current_a", 20.0, raw="Iрасч=20А")]
    extra = [
        _consumer("ВРУ5", 120.0, 60.0, 91.3),
        _input("ВРУ5", "РП1", 60.0, 91.3),
        blind,
    ]
    findings, _ = dc.summary_input_mismatches(_additive_table(extra))
    assert [item["subject"] for item in findings] == []


def test_подпись_аппарата_не_принимается_за_сводную_строку():
    """У сводной строки есть обе мощности; у подписи аппарата — только ток."""
    device = row(
        "device",
        label="1QF8",
        kind="CONSUMER_TOTAL",
        section=None,
        designation="ВРУ6",
        orientation="H",
        values=[value("maximum_calculated_current_a", 400.0, raw="400А")],
    )
    extra = [device, _input("ВРУ6", "РП1", 8.0, 12.8)]
    findings, _ = dc.summary_input_mismatches(_additive_table(extra))
    assert [item["subject"] for item in findings] == []


# --------------------------------------------------------------------------
# Правило 3. Повтор обозначения
# --------------------------------------------------------------------------
def test_повтор_обозначения_в_одной_секции_найден():
    rows = [
        _input("ВРУ3", "РП2", 72.7, 116.5, label="2ГРЩ-ВРУ3 ППГнг(А)-НF 2х(5х95)",
               cables=("ППГнг(А)-НF 2х(5х95)",)),
        _input("ВРУ3", "РП2", 183.9, 318.1, label="2ГРЩ-ВРУ3 ППГнг(А)-НF 3х(5х120)",
               cables=("ППГнг(А)-НF 3х(5х120)",)),
    ]
    rows[1]["row_id"] = "in-ВРУ3-РП2-b"
    found = dc.duplicate_designations(table(rows))
    assert len(found) == 1
    assert found[0]["kind"] == dc.KIND_DUPLICATE_DESIGNATION
    assert found[0]["subject"] == "ВРУ3"
    # В тексте стоит то, что напечатано на листе, а не каноническая свёртка.
    assert "2ГРЩ-ВРУ3 ППГнг(А)-НF 2х(5х95)" in found[0]["summary"]
    assert "3х(5х120)" in found[0]["summary"]
    # Находка НЕ утверждает, что лист не позволяет выбрать верную подпись:
    # лист это как раз позволяет. Утверждается только отказ системы решать.
    assert "из листа не следует" not in found[0]["summary"]
    assert "решает инженер" in found[0]["summary"]


def test_одно_обозначение_в_разных_секциях_не_дубль():
    rows = [
        _input("ВРУ3", "РП1", 70.5, 112.9),
        _input("ВРУ3", "РП2", 72.7, 116.5),
    ]
    assert dc.duplicate_designations(table(rows)) == []


def test_один_прогон_прочитанный_дважды_не_дубль():
    """Совпали и марка кабеля, и все значения — это одна линия."""
    first = _input("ВРУ3", "РП2", 72.7, 116.5)
    second = dict(first)
    second["row_id"] = "in-ВРУ3-РП2-copy"
    assert dc.duplicate_designations(table([first, second])) == []


def test_два_ввода_одного_щита_не_дубль():
    """Лист сам различает вводы номером — это не повтор обозначения."""
    first = _input("ШУ-АПТ", "РП1", 22.5, 52.7)
    first["input_number"] = 1
    second = _input("ШУ-АПТ", "РП1", 22.4, 52.6)
    second["row_id"] = "in-ШУ-АПТ-РП1-b"
    second["input_number"] = 2
    assert dc.duplicate_designations(table([first, second])) == []


def test_геометрически_привязанное_обозначение_не_участвует():
    """Обозначение из ряда подписей доказывает привязку, а не подпись линии."""
    rows = []
    for index, power in enumerate((335.0, 21.6)):
        item = _input("ХМ1", "РП1", power, 676.8)
        item["row_id"] = f"geo-{index}"
        item["own_designations"] = []
        item["feeder_designations"] = []
        item["row_designations"] = [{"designation": "ХМ1"}]
        rows.append(item)
    assert dc.duplicate_designations(table(rows)) == []


def test_строка_без_секции_не_участвует():
    """У сводной строки секции нет: она итог, а не отдельная линия."""
    rows = []
    for index in range(2):
        item = _input("ВРУ3", "РП2", 70.5 + index, 112.9 + index)
        item["row_id"] = f"nos-{index}"
        item["section_ref"] = None
        rows.append(item)
    assert dc.duplicate_designations(table(rows)) == []


# --------------------------------------------------------------------------
# Правило 4. Единица измерения мощности
# --------------------------------------------------------------------------
def _watt_table(*, witness_power=10.0, witness_offset=0.0):
    raw = "Рр=10Вт, cosf=0.9, Iрасч=20А"
    feeder = row(
        "feeder",
        label="2ГРЩ-ЭБ.ГВС ППГнг(А)-НF (5х4)",
        section="РП2",
        designation="ЭБ-ГВС",
        own=(),
        feeders=("ЭБ-ГВС",),
        cross=(2026.4, 2047.9),
        along=(695.8, 812.2),
        orientation="V",
        values=[value("maximum_calculated_current_a", 20.0, raw=raw)],
    )
    feeder["consumer_label"] = "2ГРЩ-ЭБ.ГВС ППГнг(А)-НF (5х4) | " + raw
    witness = row(
        "witness",
        label="ЯСН ТП",
        kind="CONSUMER_TOTAL",
        section=None,
        designation="ЯСН-ТП",
        own=("ЯСН-ТП",),
        orientation="H",
        cross=(578.6, 625.0),
        along=(2022.1 + witness_offset, 2056.1 + witness_offset),
        values=[
            value("installed_power_kw", witness_power, raw=f"Py={witness_power}кВт"),
            value("demand_active_power_kw", witness_power, raw=f"Pp={witness_power}кВт"),
            value("maximum_calculated_current_a", 20.0, raw="Ip=20 А"),
        ],
    )
    return table([feeder, witness])


def test_противоречие_единицы_найдено_по_независимому_токену():
    found = dc.power_unit_conflicts(_watt_table(), line_voltage_kv=0.38)
    assert len(found) == 1
    finding = found[0]
    assert finding["kind"] == dc.KIND_POWER_UNIT_MISMATCH
    # Объект назван по сводному блоку СВОЕЙ колонки, а не по марке кабеля,
    # скопированной с соседней: иначе инженер пойдёт не в ту колонку.
    assert finding["subject"] == "ЯСН ТП"
    assert finding["evidence"]["witness_power_kw"] == pytest.approx(10.0)
    assert finding["evidence"]["expected_kw"] == pytest.approx(11.847, abs=0.01)
    assert len(finding["evidence"]["bboxes"]) == 2


def test_без_независимого_токена_единица_не_угадывается():
    """Арифметики мало: «10 Вт выглядит мало» — не доказательство."""
    payload = _watt_table()
    payload["rows"] = [payload["rows"][0]]
    assert dc.power_unit_conflicts(payload, line_voltage_kv=0.38) == []


def test_независимый_токен_с_другим_числом_ничего_не_доказывает():
    assert dc.power_unit_conflicts(_watt_table(witness_power=60.0), line_voltage_kv=0.38) == []


def test_установленная_мощность_в_ваттах_не_проверяется():
    """У Ру нет арифметической связи с расчётным током."""
    payload = _watt_table()
    payload["rows"][0]["consumer_label"] = (
        "2ГРЩ-ЭБ.ГВС ППГнг(А)-НF (5х4) | Ру=10Вт, cosf=0.9, Iрасч=20А"
    )
    payload["rows"][0]["values"][0]["raw_run"] = "Ру=10Вт, cosf=0.9, Iрасч=20А"
    assert dc.power_unit_conflicts(payload, line_voltage_kv=0.38) == []


def test_слова_с_вт_не_принимаются_за_единицу():
    """«автостоянка» и «(втч ОЗДС)» не должны читаться как ватты."""
    for text in ("Подземная автостоянка", "освещение. (втч ОЗДС)", "блок-контакты"):
        assert dc.RE_POWER_IN_WATTS.search(text) is None
    assert dc.RE_POWER_IN_WATTS.search("Рр=10Вт, cosf=0.9") is not None


# --------------------------------------------------------------------------
# Правило 5. Необычная связь сигнальной цепи
# --------------------------------------------------------------------------
class _Evidence:
    def __init__(self, words):
        self.visual_words = words


def _signal_case(minority_index=1, columns=10):
    """Ряд подписей над колонками: половина в секции 1, половина в секции 2.

    Колонок в секции пять: четыре единогласных плюс одна выбивающаяся — это
    минимум, при котором большинство ещё достаточно велико, чтобы говорить о
    правиле, а меньшинство остаётся единственным.
    """
    nodes = []
    words = []
    for index in range(columns):
        section = "BUS1" if index < columns // 2 else "BUS2"
        x0 = 100.0 + index * 90.0
        nodes.append(
            {
                "type": "OUTGOING_DEVICE",
                "id": f"OUT:{index}",
                "label": f"{1 if section == 'BUS1' else 2}QF{index + 1}",
                "display_label": f"ГРЩ1-РП{1 if section == 'BUS1' else 2}-{index + 1}",
                "section": section,
                "bbox": [x0, 920.0, x0 + 20.0, 930.0],
            }
        )
        digit = 1 if section == "BUS1" else 2
        if index == columns - 2:
            digit = minority_index
        words.append([x0 + 2.0, 909.0, x0 + 16.0, 918.0, f"TS{digit}", 0, index, 0])
    return _Evidence(words), {"nodes": nodes}


def test_выбивающаяся_сигнальная_подпись_уходит_на_проверку():
    evidence, graph = _signal_case(minority_index=1)
    found = dc.signal_link_outliers(evidence, graph, side="RIGHT")
    assert len(found) == 1
    assert found[0]["kind"] == dc.KIND_SIGNAL_LINK_OUTLIER
    # Доказательство статистическое, а не абсолютное: утверждать ошибку нельзя.
    assert found[0]["verdict"] == dc.VERDICT_REVIEW
    assert found[0]["evidence"]["minority_label"] == "TS1"
    assert found[0]["evidence"]["majority_label"] == "TS2"
    assert "Требуется проверка" in found[0]["summary"]


def test_единогласный_ряд_подписей_не_даёт_находки():
    evidence, graph = _signal_case(minority_index=2)
    assert dc.signal_link_outliers(evidence, graph, side="RIGHT") == []


def test_ряд_обозначений_аппаратов_не_считается_сигнальным():
    """Расхождение подписи аппарата с секцией — уже существующая находка."""
    evidence, graph = _signal_case(minority_index=1)
    for index, word in enumerate(evidence.visual_words):
        word[4] = graph["nodes"][index]["label"].lstrip("12")
    for node in graph["nodes"]:
        node["label"] = node["label"].lstrip("12")
    assert dc.signal_link_outliers(evidence, graph, side="RIGHT") == []


def test_смещённый_ряд_подписей_отвергается_целиком():
    """Подпись, накрывающая две колонки поровну, не доказывает ничего.

    Ряд, съехавший относительно колонок, отвергается ЦЕЛИКОМ: иначе
    «исключение» оказалось бы артефактом привязки, а не ошибкой чертежа.
    """
    evidence, graph = _signal_case(minority_index=1)
    for index, word in enumerate(evidence.visual_words):
        base = 100.0 + index * 90.0
        word[0], word[2] = base + 10.0, base + 100.0
    assert dc.signal_link_outliers(evidence, graph, side="RIGHT") == []


def test_короткий_ряд_не_принимается():
    evidence, graph = _signal_case(minority_index=1, columns=6)
    assert dc.signal_link_outliers(evidence, graph, side="RIGHT") == []


# --------------------------------------------------------------------------
# Общее
# --------------------------------------------------------------------------
def test_константы_не_разъезжаются_с_таблицей_нагрузок():
    """Одно тождество — одно напряжение и один допуск на весь конвейер."""
    assert dc.LINE_VOLTAGE_KV == elt._LINE_VOLTAGE_KV
    assert dc.UNIT_ARITHMETIC_TOLERANCE == elt._ARITHMETIC_TOLERANCE


def test_целое_число_не_теряет_нулей():
    """«380» — не «38»: хвостовые нули срезаются только у дробной части."""
    assert dc._ru(380.0, 0) == "380"
    assert dc._ru(100.0) == "100"
    assert dc._ru(1.398, 2) == "1,4"
    assert dc._ru(72.7) == "72,7"


def test_сборка_не_обращается_к_модели():
    result = dc.detect_document_consistency(load_table=table([]), side="LEFT")
    assert result["diagnostics"]["uses_model"] is False
    assert result["diagnostics"]["uses_ocr"] is False
    assert result["counts"]["total"] == 0


def test_порядок_находок_детерминирован():
    payload = _watt_table()
    first = dc.detect_document_consistency(load_table=payload, side="LEFT")
    second = dc.detect_document_consistency(load_table=payload, side="LEFT")
    assert first == second
    assert [item["kind"] for item in first["items"]] == sorted(
        item["kind"] for item in first["items"]
    )
