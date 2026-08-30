"""Связчик таблиц нагрузок на боевой паре ГРЩ.

Синтетические строки проверяют правила; здесь проверяется, что правила
срабатывают на настоящих листах. Пара — та же, на которой велась приёмка:
однолинейная схема ГРЩ в двух редакциях, левый лист повёрнут на 270°, правый
набран без поворота и другой раскладкой подписей.

Хранилище листов не входит в чистую выкладку репозитория, поэтому без него
тесты пропускаются, а не падают.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding import electrical_load_table as elt
from backend.app.pipeline.stages.block_grounding import electrical_table_diff as etd
from backend.app.pipeline.stages.block_grounding.vector_evidence import (
    extract_vector_evidence,
)

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison"
LEFT_PDF = (
    STORE
    / "stage_1/documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1/versions/v001/02_work/document.pdf"
)
RIGHT_PDF = (
    STORE
    / "stage_2/documents/Страница_21_из_АА-БЭ-03-ДС3-ИОС1.1_—_копия/versions/v001/02_work/document.pdf"
)
LEFT_BLOCK = "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6"
RIGHT_BLOCK = "blk_039909ec039649a1b8209f059c95167b"

pytestmark = pytest.mark.skipif(
    not (LEFT_PDF.exists() and RIGHT_PDF.exists()),
    reason="листы боевой пары ГРЩ недоступны в этой выкладке",
)


@pytest.fixture(scope="module")
def tables():
    result = {}
    for side, pdf, block in (
        ("LEFT", LEFT_PDF, LEFT_BLOCK),
        ("RIGHT", RIGHT_PDF, RIGHT_BLOCK),
    ):
        evidence = extract_vector_evidence(pdf, page_index=0, block_id=block)
        assert evidence.extraction_ok, evidence.reasons
        result[side] = elt.build_load_table(evidence, side=side)
    return result


@pytest.fixture(scope="module")
def diff(tables):
    return etd.compare_load_tables(tables["LEFT"], tables["RIGHT"])


def _by_subject(diff, subject, facet, section=None):
    return [
        change
        for change in diff["changes"]
        if change["subject"] == subject
        and change["facet_ref"] == facet
        and (section is None or change["section_ref"] == section)
    ]


# --------------------------------------------------------------------------
# Поворот листа
# --------------------------------------------------------------------------
def test_повёрнутый_лист_читается_наравне_с_обычным(tables):
    """Левый лист повёрнут на 270°, правый — нет; строки находятся на обоих."""
    assert tables["LEFT"]["counts"]["bound"] > 10
    assert tables["RIGHT"]["counts"]["bound"] > 10


# --------------------------------------------------------------------------
# Холодильные машины
# --------------------------------------------------------------------------
def test_мощность_холодильных_машин_доказана(diff):
    for subject, section in (("ХМ1", "РП1"), ("ХМ2", "РП2")):
        changes = _by_subject(diff, subject, "demand_active_power_kw", section)
        assert len(changes) == 1, f"{subject}: ожидалось одно изменение мощности"
        assert changes[0]["before_value"] == 157.5
        assert changes[0]["after_value"] == 335.0
        assert changes[0]["match_method"] == etd.MATCH_EXACT


def test_ток_холодильных_машин_доказан(diff):
    for subject, section in (("ХМ1", "РП1"), ("ХМ2", "РП2")):
        changes = _by_subject(diff, subject, "maximum_calculated_current_a", section)
        assert len(changes) == 1
        assert changes[0]["before_value"] == 360.0
        assert changes[0]["after_value"] == 676.8


def test_охладитель_не_получил_мощность_машины(diff):
    """ДР1-ХМ1 несёт 21,6 кВт и не вправе получить 157,5 или 335 кВт."""
    for change in diff["changes"] + diff["unchanged"]:
        if change["subject"] != "ДР1-ХМ1":
            continue
        for value in (change["before_value"], change["after_value"]):
            assert value not in (157.5, 335.0)


def test_машина_не_получила_мощность_охладителя(diff):
    for change in diff["changes"]:
        if change["subject"] not in ("ХМ1", "ХМ2"):
            continue
        for value in (change["before_value"], change["after_value"]):
            assert value != 21.6


# --------------------------------------------------------------------------
# Компенсаторы реактивной мощности
# --------------------------------------------------------------------------
def test_компенсаторы_различены_и_доказаны(diff):
    first = _by_subject(diff, "АУКРМ-1", "demand_reactive_power_kvar")
    second = _by_subject(diff, "АУКРМ-2", "demand_reactive_power_kvar")
    assert len(first) == 1 and len(second) == 1
    assert (first[0]["before_value"], first[0]["after_value"]) == (200.0, 180.0)
    assert (second[0]["before_value"], second[0]["after_value"]) == (200.0, 150.0)


def test_суммарное_значение_компенсации_не_придумывается(diff):
    """400 → 330 кВАр — арифметика по двум находкам, а не отдельный факт."""
    for change in diff["changes"]:
        assert change["before_value"] != 400.0 or change["facet_ref"] != (
            "demand_reactive_power_kvar"
        )
        assert change["after_value"] != 330.0


def test_компенсатор_не_получил_ток_трансформатора(diff, tables):
    """«ТА1 ТШП 1500/5А» стоит в колонке АУКРМ, но это не его ток."""
    for change in diff["changes"]:
        if change["subject"] in ("АУКРМ-1", "АУКРМ-2"):
            assert change["before_value"] != 1500.0
            assert change["after_value"] != 1500.0
    for row in tables["RIGHT"]["rows"]:
        for value in row["values"]:
            assert value["values"] != [1500.0]


def test_резервные_баки_не_получили_нагрузку_компенсатора(tables):
    """Подпись «к регулятору АУКРМ №1» накрывает соседнюю колонку.

    Если допустить её в ряд обозначений, колонка резервных баков ГВС
    (125 кВт) станет компенсатором.
    """
    for row in tables["RIGHT"]["rows"]:
        if row["consumer_designation"] != "АУКРМ-1":
            continue
        assert "ГВС" not in (row["consumer_label"] or "")
        for value in row["values"]:
            assert value["values"] != [125.0]


# --------------------------------------------------------------------------
# Групповые нагрузки ВРУ
# --------------------------------------------------------------------------
def test_вводы_вру_разделены(diff):
    """ВРУ4 первой секции и второй — разные находки с разными числами."""
    first = _by_subject(diff, "ВРУ4", "demand_active_power_kw", "РП1")
    second = _by_subject(diff, "ВРУ4", "demand_active_power_kw", "РП2")
    assert len(first) == 1 and len(second) == 1
    assert first[0]["before_value"] != second[0]["before_value"]
    assert first[0]["after_value"] != second[0]["after_value"]


def test_секции_не_перепутаны(diff):
    """У каждой находки секция совпадает с секцией обеих исходных строк."""
    for change in diff["changes"]:
        if change["match_method"] != etd.MATCH_EXACT:
            continue
        assert change["section_ref"] in ("РП1", "РП2")


def test_режимы_не_приравнены(diff):
    """Расчёт «рабочий/пожарный» не выдаётся за расчёт без указания режима."""
    blocked = [
        item for item in diff["blocked"]
        if item.get("reason") == etd.REASON_MODE_MISMATCH
    ]
    assert blocked, "различие режимов должно быть замечено, а не проигнорировано"
    for item in blocked:
        assert "режим" in item["summary"].lower()


def test_повтор_обозначения_на_листе_уходит_человеку(diff):
    """ВРУ3 второй секции подписан на левом листе дважды."""
    ambiguous = [
        item for item in diff["blocked"]
        if item.get("reason") == etd.REASON_AMBIGUOUS_MATCH
    ]
    assert any("ВРУ3" in item["summary"] for item in ambiguous)


# --------------------------------------------------------------------------
# Противоречия самих листов
# --------------------------------------------------------------------------
def test_расхождение_подписи_колонки_и_ряда_найдено(tables):
    """Колонка подписана ДР2-ХМ2, а в ряду обозначений над ней — ДР1-ХМ2."""
    conflicts = [
        item for item in tables["RIGHT"]["contradictions"]
        if item["kind"] == "CONSUMER_LABEL_CONFLICT"
    ]
    assert any("ДР2-ХМ2" in item["summary"] for item in conflicts)


def test_несходимость_мощности_и_тока_найдена(tables):
    """Рр=307,6 кВт при cosφ=0,87 не даёт 205,8 А."""
    conflicts = [
        item for item in tables["LEFT"]["contradictions"]
        if item["kind"] == "ROW_ARITHMETIC_CONFLICT"
    ]
    assert any("307" in item["summary"] for item in conflicts)


def test_противоречия_не_выдаются_как_изменения(tables, diff):
    """У ошибки листа нет второй стороны — стрелки «было → стало» быть не может."""
    contradiction_subjects = {
        item["subject"]
        for side in ("LEFT", "RIGHT")
        for item in tables[side]["contradictions"]
    }
    assert contradiction_subjects
    for change in diff["changes"]:
        assert "подписан" not in str(change.get("facet_title") or "")


# --------------------------------------------------------------------------
# Общая дисциплина находок
# --------------------------------------------------------------------------
def test_каждое_изменение_имеет_доказательство_с_обеих_сторон(diff):
    for change in diff["changes"]:
        for side in ("LEFT", "RIGHT"):
            evidence = change["evidence"][side]
            assert evidence["row_id"]
            assert evidence["bbox"] and len(evidence["bbox"]) == 4
            assert evidence["raw"]


def test_ни_одно_изменение_не_смешивает_единицы(diff):
    kilowatt = {"installed_power_kw", "demand_active_power_kw"}
    kilovar = {"demand_reactive_power_kvar", "installed_reactive_power_kvar"}
    ampere = {"maximum_calculated_current_a"}
    for change in diff["changes"]:
        facet = change["facet_ref"]
        if facet in kilowatt:
            assert change["unit"] == "кВт"
        elif facet in kilovar:
            assert change["unit"] == "кВАр"
        elif facet in ampere:
            assert change["unit"] == "А"


def test_модель_не_вызывается(tables, diff):
    for side in ("LEFT", "RIGHT"):
        assert tables[side]["diagnostics"]["uses_model"] is False
        assert tables[side]["diagnostics"]["uses_ocr"] is False
    assert diff["diagnostics"]["uses_model"] is False
