"""Связчик «строка таблицы нагрузок → потребитель схемы».

Мощности и токи потребителей однолинейной схемы записаны в подписях фидерных
колонок, а не в графе аппаратов. Связать их с объектом можно только по
совокупности признаков: соседние колонки отличаются на единицы пунктов,
``ДР1-ХМ1`` содержит подстроку ``ХМ1``, а «ВРУ1 ввод 1» и «ВРУ1 ввод 2» — это
разные вводы одного щита.

Здесь проверяется прежде всего то, чего связчик делать НЕ должен: приписать
нагрузку соседа, спутать ампер с киловаром, принять подпись цепи управления за
подпись колонки и угадать порядок в неподписанной полосе.
"""
from __future__ import annotations

import pytest

from backend.app.pipeline.stages.block_grounding import electrical_load_table as elt
from backend.app.pipeline.stages.block_grounding import electrical_table_diff as etd


# --------------------------------------------------------------------------
# Строительные блоки: слово вектор-слоя — кортеж PyMuPDF
# (x0, y0, x1, y1, текст, блок, строка, номер слова).
# --------------------------------------------------------------------------
def _word(x0, y0, text, *, block=0, line=0, index=0, width=None, height=8.8):
    span = width if width is not None else 4.6 * len(text)
    return (x0, y0, x0 + span, y0 + height, text, block, line, index)


def _column(x, y_top, lines, *, block_start=0, pitch=11.0):
    """Колонка горизонтальных подписей, стоящих одна под другой."""
    words = []
    for offset, text in enumerate(lines):
        parts = text.split(" ")
        cursor = x
        for index, part in enumerate(parts):
            words.append(
                _word(
                    cursor,
                    y_top + offset * pitch,
                    part,
                    block=block_start + offset,
                    line=0,
                    index=index,
                )
            )
            cursor += 4.6 * len(part) + 3.0
    return words


class _Evidence:
    def __init__(self, words, page_index=0):
        self.visual_words = words
        self.page_index = page_index
        self.provenance = {}


# --------------------------------------------------------------------------
# 1. Разбор значений и единиц
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text, facet, value",
    [
        ("Ру=157,5кВт", "installed_power_kw", 157.5),
        ("Py=157,5кВт", "installed_power_kw", 157.5),  # латинская Y из САПР
        ("Рр=157,5 кВт", "demand_active_power_kw", 157.5),
        ("Iр=360 А", "maximum_calculated_current_a", 360.0),
        ("Ip=132 А", "maximum_calculated_current_a", 132.0),
        ("Qр=200 кВАр", "demand_reactive_power_kvar", 200.0),
    ],
)
def test_подпись_с_префиксом_даёт_своё_свойство(text, facet, value):
    values = elt.parse_values(text)
    assert [(item["facet_ref"], item["values"]) for item in values] == [(facet, [value])]


def test_запятая_и_точка_читаются_одинаково():
    assert elt.parse_values("Рр=157,5 кВт")[0]["values"] == [157.5]
    assert elt.parse_values("Рр=157.5 кВт")[0]["values"] == [157.5]


def test_ампер_не_становится_киловаром():
    """200 А соседней строки не вправе стать 200 кВАр компенсатора."""
    amperes = elt.parse_values("Iр=200 А")
    assert amperes[0]["facet_ref"] == "maximum_calculated_current_a"
    kvar = elt.parse_values("Qр=200 кВАр")
    assert kvar[0]["facet_ref"] == "demand_reactive_power_kvar"
    assert amperes[0]["facet_ref"] != kvar[0]["facet_ref"]


def test_единица_противоречащая_префиксу_отвергается():
    """«Qр=200 А» не доказано: префикс говорит о мощности, единица — о токе."""
    assert elt.parse_values("Qр=200 А") == []


def test_режим_остаётся_частью_подписи():
    assert elt.mode_label("Рабочий/пожарн.") == "Рабочий/пожарн."
    assert elt.mode_label("Авар. режим") == "Авар. режим"
    assert elt.mode_label("Холодильная машина") is None


# --------------------------------------------------------------------------
# 2. Обозначения потребителей
# --------------------------------------------------------------------------
def test_охладитель_не_отдаёт_обозначение_машины():
    """«ДР1-ХМ1» — охладитель, а не холодильная машина ХМ1."""
    assert elt.designations("ДР1-ХМ1 Охладитель") == ["ДР1-ХМ1"]
    assert "ХМ1" not in elt.designations("ДР1-ХМ1 Охладитель")


def test_машина_читается_отдельно_от_охладителя():
    assert elt.designations("Холодильная машина ХМ1") == ["ХМ1"]


@pytest.mark.parametrize(
    "written, canonical",
    [
        ("АУКРМ №1", "АУКРМ-1"),
        ("АУКРМ-1", "АУКРМ-1"),
        ("АУКРМ 1", "АУКРМ-1"),
        ("ВРУа", "ВРУ-А"),
        ("ВРУ-ИТП", "ВРУ-ИТП"),
        ("ЩНО", "ШНО"),
        ("ШУХЦ", "ШУ-ХЦ"),
    ],
)
def test_разное_написание_даёт_одно_обозначение(written, canonical):
    assert elt.canonical_designation(written) == canonical


def test_разные_компенсаторы_остаются_разными():
    assert elt.canonical_designation("АУКРМ №1") != elt.canonical_designation("АУКРМ №2")


def test_метка_фидера_несёт_секцию():
    assert elt.feeder_tags("1ГРЩ-ХМ1 ППГнг(А)-НF 2х(5х95)") == [
        {"section": "1", "load": "ХМ1", "panel": None, "raw": "1ГРЩ-ХМ1"}
    ]


def test_секция_правого_листа_берётся_у_панели():
    """«ГРЩ1-РП2-12» — вторая секция первого щита, а не первая."""
    tags = elt.feeder_tags("ГРЩ1-РП2-12 3хППГнг(А)-HF 5х150мм²")
    assert tags[0]["panel"] == "РП2"
    assert tags[0]["section"] == "2"
    assert elt.section_ref(tags[0]["section"], tags[0]["panel"]) == "РП2"


def test_обе_редакции_называют_секции_одинаково():
    assert elt.section_ref("1", None) == elt.section_ref(None, "РП1") == "РП1"


# --------------------------------------------------------------------------
# 3. Что не является строкой значений
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    [
        "ХМ2",                          # обозначение, а не значение
        "ВРУ4",
        "ТА1 (ф.С) ТШП 1500/5А 0.5S",   # трансформатор тока
        "к регулятору АУКРМ №1",        # цепь управления
        "Холодильная машина (чиллер)",
    ],
)
def test_проза_и_обозначения_не_дают_нагрузку(text):
    assert elt.is_value_run(text) is False


@pytest.mark.parametrize(
    "text",
    ["Ру=157,5кВт", "335.0 кВт - 335.0 кВт - 676.8 А", "Рр=157,5кВт, cosf=0,67, Iрасч=360А"],
)
def test_строка_значений_опознаётся(text):
    assert elt.is_value_run(text) is True


def test_подпись_цепи_управления_не_годится_для_ряда_обозначений():
    """«к регулятору АУКРМ №1» геометрически накрывает соседний фидер.

    Допустить её в ряд обозначений — значит приписать резервным бакам ГВС
    нагрузку компенсатора.
    """
    assert elt.is_designation_label("к регулятору АУКРМ №1") is False
    assert elt.is_designation_label("АУКРМ №1") is True


# --------------------------------------------------------------------------
# 4. Беспрефиксная полоса
# --------------------------------------------------------------------------
def test_равные_мощности_раскрываются_в_оба_свойства():
    """«335 кВт - 335 кВт - 676,8 А»: при любом прочтении обе мощности равны 335."""
    values = elt.resolve_positional_power(
        elt.parse_values("335.0 кВт - 335.0 кВт - 676.8 А")
    )
    facets = {item["facet_ref"]: item["values"] for item in values}
    assert facets["installed_power_kw"] == [335.0]
    assert facets["demand_active_power_kw"] == [335.0]
    assert facets["maximum_calculated_current_a"] == [676.8]


def test_разные_мощности_порядок_не_угадывают():
    """Какое число установленная мощность, а какое расчётная — полоса молчит."""
    values = elt.resolve_positional_power(
        elt.parse_values("300.0 кВт - 200.0 кВт - 400.0 А")
    )
    unresolved = [item for item in values if item["facet_ref"] is None]
    assert len(unresolved) == 2
    assert all(
        item["unresolved_reason"] == "positional_power_order_unproven"
        for item in unresolved
    )


def test_реактивная_полоса_остаётся_реактивной():
    values = elt.resolve_positional_power(
        elt.parse_values("180.0 кВАр 180.0 кВАр 272.7 А")
    )
    facets = {item["facet_ref"] for item in values}
    assert "demand_reactive_power_kvar" in facets
    assert "demand_active_power_kw" not in facets


# --------------------------------------------------------------------------
# 5. Сборка строк таблицы из вектор-слоя
# --------------------------------------------------------------------------
def _two_column_sheet():
    """Две соседние колонки: охладитель и холодильная машина."""
    words = []
    words += _column(100.0, 200.0, ["Охладитель", "ДР1-ХМ1", "Ру=21,6кВт", "Iр=41А"], block_start=0)
    words += _column(220.0, 200.0, ["Холодильная", "машина ХМ1", "Ру=157,5кВт", "Iр=360 А"], block_start=10)
    return _Evidence(words)


def test_строки_таблицы_собираются_и_связываются():
    table = elt.build_load_table(_two_column_sheet(), side="LEFT")
    bound = {
        row["consumer_designation"]: row
        for row in table["rows"]
        if row["binding_status"] == elt.BOUND
    }
    assert set(bound) == {"ДР1-ХМ1", "ХМ1"}


def test_машина_не_получает_мощность_охладителя():
    """Главная защита: 21,6 кВт соседа не приклеивается к ХМ1."""
    table = elt.build_load_table(_two_column_sheet(), side="LEFT")
    chiller = next(
        row for row in table["rows"] if row["consumer_designation"] == "ХМ1"
    )
    powers = [
        item["values"][0]
        for item in chiller["values"]
        if item["facet_ref"] == "installed_power_kw"
    ]
    assert powers == [157.5]
    assert 21.6 not in powers


def test_строка_без_доказанного_обозначения_не_даёт_факта():
    words = _column(100.0, 200.0, ["Ру=21,6кВт", "Iр=41А"], block_start=0)
    table = elt.build_load_table(_Evidence(words), side="LEFT")
    assert all(row["binding_status"] == elt.UNBOUND for row in table["rows"])
    assert all(row["consumer_designation"] is None for row in table["rows"])


def test_конфликт_обозначений_даёт_неоднозначность():
    """Подпись колонки против ряда обозначений — вопрос человеку."""
    row = elt.resolve_binding(
        {
            "own_designations": ["ДР2-ХМ2"],
            "row_designations": [{"designation": "ДР1-ХМ2"}],
            "feeder_designations": [],
        }
    )
    assert row["binding_status"] == elt.AMBIGUOUS
    assert row["consumer_designation"] is None
    assert row["binding_reasons"] == ["designation_conflict"]


def test_согласие_источников_усиливает_связь():
    row = elt.resolve_binding(
        {
            "own_designations": ["ХМ1"],
            "row_designations": [{"designation": "ХМ1"}],
            "feeder_designations": ["ХМ1"],
        }
    )
    assert row["binding_status"] == elt.BOUND
    assert row["binding_signal_count"] == 3


def test_противоречие_подписей_попадает_в_противоречия_документа():
    contradictions = elt.detect_row_contradictions(
        [
            {
                "side": "RIGHT",
                "row_id": "etrow_1",
                "bbox": [0, 0, 1, 1],
                "own_designations": ["ДР2-ХМ2"],
                "row_designations": [{"designation": "ДР1-ХМ2"}],
                "consumer_designation": "ДР2-ХМ2",
                "values": [],
            }
        ]
    )
    assert [item["kind"] for item in contradictions] == ["CONSUMER_LABEL_CONFLICT"]


def test_несходимость_мощности_и_тока_ловится():
    """«Рр=307,6 кВт, cosφ=0,87, Iрасч=205,8 А» противоречит само себе."""
    row = {
        "side": "LEFT",
        "row_id": "etrow_2",
        "bbox": [0, 0, 1, 1],
        "consumer_designation": "ВРУ-А",
        "values": [
            {
                "facet_ref": "demand_active_power_kw",
                "values": [307.6],
                "raw_run": "Рр=307,6кВт, cosf=0,87, Iрасч=205,8А",
            },
            {
                "facet_ref": "maximum_calculated_current_a",
                "values": [205.8],
                "raw_run": "Рр=307,6кВт, cosf=0,87, Iрасч=205,8А",
            },
        ],
    }
    conflict = elt.check_row_arithmetic(row)
    assert conflict is not None
    assert conflict["kind"] == "ROW_ARITHMETIC_CONFLICT"
    assert conflict["evidence"]["expected_current_a"] == pytest.approx(537.2, abs=1.0)


def test_сходящаяся_строка_противоречием_не_считается():
    row = {
        "side": "LEFT",
        "row_id": "etrow_3",
        "bbox": [0, 0, 1, 1],
        "consumer_designation": "ВРУ1",
        "values": [
            {
                "facet_ref": "demand_active_power_kw",
                "values": [181.8],
                "raw_run": "Рр=181,8кВт, cosf=0,88 Iрасч=314,4А",
            },
            {
                "facet_ref": "maximum_calculated_current_a",
                "values": [314.4],
                "raw_run": "Рр=181,8кВт, cosf=0,88 Iрасч=314,4А",
            },
        ],
    }
    assert elt.check_row_arithmetic(row) is None


# --------------------------------------------------------------------------
# 6. Сопоставление строк двух редакций
# --------------------------------------------------------------------------
def _row(designation, *, side, section=None, input_number=None, kind="FEEDER",
         mode=None, values=(), row_id=None):
    return {
        "row_id": row_id or f"etrow_{side}_{designation}_{section}_{input_number}",
        "side": side,
        "page": 0,
        "bbox": [0.0, 0.0, 1.0, 1.0],
        "binding_status": elt.BOUND,
        "consumer_designation": designation,
        "consumer_label": designation,
        "section_ref": section,
        "input_number": input_number,
        "row_kind": kind,
        "mode_label": mode,
        "designation_sources": {"own_label": [designation]},
        "values": [
            {"facet_ref": facet, "values": [value], "unit": "", "raw": str(value)}
            for facet, value in values
        ],
    }


def test_ввод_1_не_сравнивается_с_вводом_2():
    """Два ввода одного щита — разные объекты, а не одно значение."""
    left = [
        _row("ВРУ1", side="LEFT", section="РП1", values=[("demand_active_power_kw", 181.8)]),
        _row("ВРУ1", side="LEFT", section="РП2", values=[("demand_active_power_kw", 115.6)]),
    ]
    right = [
        _row("ВРУ1", side="RIGHT", section="РП1", input_number=1,
             values=[("demand_active_power_kw", 223.2)]),
        _row("ВРУ1", side="RIGHT", section="РП2", input_number=2,
             values=[("demand_active_power_kw", 190.6)]),
    ]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    pairs = {(item["section_ref"], item["before_value"], item["after_value"])
             for item in result["changes"]}
    assert pairs == {("РП1", 181.8, 223.2), ("РП2", 115.6, 190.6)}


def test_секции_не_перемешиваются():
    left = [_row("ХМ1", side="LEFT", section="РП1", values=[("demand_active_power_kw", 157.5)])]
    right = [_row("ХМ1", side="RIGHT", section="РП2", values=[("demand_active_power_kw", 335.0)])]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    # Совпадение только по обозначению остаётся возможным, но секция при этом
    # обязана быть видна в находке.
    assert all(item["match_method"] == etd.MATCH_DESIGNATION for item in result["changes"])


def test_охладитель_не_сливается_с_машиной():
    left = [
        _row("ДР1-ХМ1", side="LEFT", section="РП1", values=[("demand_active_power_kw", 21.6)]),
        _row("ХМ1", side="LEFT", section="РП1", values=[("demand_active_power_kw", 157.5)]),
    ]
    right = [
        _row("ДР1-ХМ1", side="RIGHT", section="РП1", values=[("demand_active_power_kw", 21.6)]),
        _row("ХМ1", side="RIGHT", section="РП1", values=[("demand_active_power_kw", 335.0)]),
    ]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    assert [(item["subject"], item["before_value"], item["after_value"])
            for item in result["changes"]] == [("ХМ1", 157.5, 335.0)]


def test_разные_режимы_не_сравниваются_напрямую():
    """«Рабочий» против «аварийного» — не стрелка X → Y."""
    left = [_row("ВРУ1", side="LEFT", section="РП1", mode="Рабочий/пожарн.",
                 values=[("demand_active_power_kw", 181.8)])]
    right = [_row("ВРУ1", side="RIGHT", section="РП1", mode="Авар. режим",
                  values=[("demand_active_power_kw", 223.2)])]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    assert result["changes"] == []
    assert [item["reason"] for item in result["blocked"]] == [etd.REASON_MODE_MISMATCH]


def test_две_подходящие_строки_уходят_человеку():
    """Один и тот же щит дважды в одной секции — выбрать пару нечем."""
    left = [
        _row("ВРУ3", side="LEFT", section="РП2", row_id="a",
             values=[("demand_active_power_kw", 72.7)]),
        _row("ВРУ3", side="LEFT", section="РП2", row_id="b",
             values=[("demand_active_power_kw", 183.9)]),
    ]
    right = [_row("ВРУ3", side="RIGHT", section="РП2",
                  values=[("demand_active_power_kw", 140.9)])]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    assert result["changes"] == []
    assert any(item["reason"] == etd.REASON_AMBIGUOUS_MATCH for item in result["blocked"])


def test_каждое_свойство_отдельным_изменением():
    """Мощность и ток — два решения инженера, а не одно «параметры»."""
    left = [_row("ХМ1", side="LEFT", section="РП1", values=[
        ("demand_active_power_kw", 157.5), ("maximum_calculated_current_a", 360.0)])]
    right = [_row("ХМ1", side="RIGHT", section="РП1", values=[
        ("demand_active_power_kw", 335.0), ("maximum_calculated_current_a", 676.8)])]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    assert len(result["changes"]) == 2
    assert {item["facet_ref"] for item in result["changes"]} == {
        "demand_active_power_kw",
        "maximum_calculated_current_a",
    }
    assert len({item["change_id"] for item in result["changes"]}) == 2


def test_совпавшее_значение_изменением_не_считается():
    left = [_row("ДР1-ХМ1", side="LEFT", section="РП1",
                 values=[("demand_active_power_kw", 21.6)])]
    right = [_row("ДР1-ХМ1", side="RIGHT", section="РП1",
                  values=[("demand_active_power_kw", 21.6)])]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    assert result["changes"] == []
    assert len(result["unchanged"]) == 1


def test_строка_без_пары_не_объявляется_удалённой():
    """Нет пары — значит не с чем сравнить, а не «значение убрали»."""
    left = [_row("ШУ-ХВС", side="LEFT", section="РП1",
                 values=[("demand_active_power_kw", 30.0)])]
    result = etd.compare_load_tables({"rows": left}, {"rows": []})
    assert result["changes"] == []
    assert [item["reason"] for item in result["unproven"]] == [etd.REASON_UNMATCHED]


def test_неоднозначная_строка_факта_не_создаёт():
    left = [
        dict(_row("ХМ1", side="LEFT", section="РП1",
                  values=[("demand_active_power_kw", 157.5)]),
             binding_status=elt.AMBIGUOUS)
    ]
    right = [_row("ХМ1", side="RIGHT", section="РП1",
                  values=[("demand_active_power_kw", 335.0)])]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    assert result["changes"] == []


def test_разная_форма_записи_не_сравнивается():
    """«233,6/284,7» против одного числа — разное число режимов, не изменение."""
    left = [{
        **_row("ВРУ4", side="LEFT", section="РП1"),
        "values": [{"facet_ref": "demand_active_power_kw",
                    "values": [233.6, 284.7], "unit": "", "raw": "233,6/284,7"}],
    }]
    right = [_row("ВРУ4", side="RIGHT", section="РП1",
                  values=[("demand_active_power_kw", 214.8)])]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    assert result["changes"] == []
    assert any(item["reason"] == etd.REASON_SHAPE_MISMATCH for item in result["blocked"])


def test_противоречие_ввода_и_секции_останавливает_сравнение():
    left = [_row("ВРУ1", side="LEFT", section="РП1",
                 values=[("demand_active_power_kw", 181.8)])]
    right = [_row("ВРУ1", side="RIGHT", section="РП1", input_number=2,
                  values=[("demand_active_power_kw", 223.2)])]
    result = etd.compare_load_tables({"rows": left}, {"rows": right})
    assert result["changes"] == []
    assert any(item["reason"] == etd.REASON_INPUT_CONFLICT for item in result["blocked"])
