"""test_vectograf_input_calcs — извлечение сводных расчётов по ВВОДАМ вектографом.

`_extract_input_calcs` ловит режимы уровня вводов (Ввод 1/Ввод 2/аварийный/пожар:
Ру/Кс/Cos f/Рр/Sр/Ip), которых раньше не было в графе (вектограф фидеро-центричен).
Самодостаточно: синтетический текст, без данных проекта.
"""
from __future__ import annotations

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    _extract_input_calcs,
)

# Заголовок «Ввод N (...)» может переноситься на несколько строк (как в pdfplumber).
TEXT = """\
Ввод 2 (РП2+РП3
(ОДН)+РП5(ПЭСПЗ)+РП4(АВР)) -
режим авария
Ру=1259.08кВт
Кс=0.19
Cos f= 0.74
Рр=237.39кВт
Sр=248.11кВА
Ip=375.94А
Ввод 1 (РП1+РП5(ПЭСПЗ)+РП4(АВР))
Ру=1259.08кВт
Кс=0.17
Cos f= 0.93
Рр=215.07кВт
Sр=232.02кВА
Ip=351.54А
Аварийный режим (один ввод)
Ру=2084.70кВт
Кс=0.15
Cos f= 0.91
Рр=316.19кВт
Sр=347.07кВА
Ip=525.87А
"""


def test_extracts_all_input_modes():
    rows = _extract_input_calcs(TEXT)
    assert len(rows) == 3
    modes = {r["mode"] for r in rows}
    assert modes == {"авария", "рабочий"}  # 2 аварийных заголовка + 1 рабочий → режимы


def test_multiline_header_and_values():
    rows = _extract_input_calcs(TEXT)
    avar = next(r for r in rows if "Ввод 2" in r["vvod"] and r["mode"] == "авария")
    assert avar["Pu"] == 1259.08
    assert avar["Kc"] == 0.19
    assert avar["Pr"] == 237.39
    assert avar["Ip"] == 375.94
    assert "РП2" in avar["panels"]


def test_dedup_identical_rows():
    # тот же блок дважды → одна строка (дедуп по ввод+режим+Ру+Рр)
    rows = _extract_input_calcs(TEXT + "\n" + TEXT)
    assert len(rows) == 3


def test_empty_and_non_matching_text():
    assert _extract_input_calcs("") == []
    assert _extract_input_calcs("просто текст без расчётов вводов") == []


def test_preserves_placeholder_values():
    txt = ("Ввод 1 (РП1) Ру=---- кВт Кс=#ДЕЛ/0! Cos f= 0.9 "
           "Рр=---- кВт Sр=---- кВА Ip=---- А")
    rows = _extract_input_calcs(txt)
    assert len(rows) == 1
    assert rows[0]["Pu"] == "----"
    assert rows[0]["Kc"] == "#ДЕЛ/0!"
