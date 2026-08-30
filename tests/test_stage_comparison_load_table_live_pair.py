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


# --------------------------------------------------------------------------
# Внутренние противоречия листов на той же боевой паре
# --------------------------------------------------------------------------
@pytest.fixture(scope="module")
def evidences():
    from backend.app.pipeline.stages.block_grounding.vector_evidence import (
        extract_vector_evidence as _extract,
    )

    return {
        side: _extract(pdf, page_index=0, block_id=block)
        for side, pdf, block in (
            ("LEFT", LEFT_PDF, LEFT_BLOCK),
            ("RIGHT", RIGHT_PDF, RIGHT_BLOCK),
        )
    }


@pytest.fixture(scope="module")
def consistency(tables, evidences):
    from backend.app.pipeline.stages.block_grounding import document_consistency as dc

    return {
        side: dc.detect_document_consistency(
            load_table=tables[side], evidence=evidences[side], side=side
        )
        for side in ("LEFT", "RIGHT")
    }


def test_напряжение_сети_доказано_надписью_листа(consistency):
    """0,4 кВ не подставляется: «~380/220В» и «380В» напечатаны на листах."""
    for side in ("LEFT", "RIGHT"):
        assert consistency[side]["diagnostics"]["line_voltage_proven"] is True


def test_невозможный_косинус_найден_на_боевом_листе(consistency):
    """«Рр=30,0кВт, Iрасч=32,6А» — 30 кВт требуют не меньше 45,6 А."""
    from backend.app.pipeline.stages.block_grounding import document_consistency as dc

    found = [
        item
        for item in consistency["LEFT"]["items"]
        if item["kind"] == dc.KIND_IMPLIED_POWER_FACTOR
    ]
    assert len(found) == 2, "обе линии ШУ-ХП первой и второй секции"
    for item in found:
        assert item["evidence"]["implied_power_factor"] == pytest.approx(1.398, abs=0.002)
        assert item["evidence"]["minimum_current_a"] == pytest.approx(45.6, abs=0.1)
    # Линии названы своими напечатанными подписями, иначе два замечания о двух
    # разных линиях слились бы в отчёте в одну неразличимую строку.
    subjects = sorted(str(item["subject"]) for item in found)
    assert subjects[0].startswith("1ГРЩ-ШУ.ХП")
    assert subjects[1].startswith("2ГРЩ-ШУ.ХП")


def test_исправные_строки_не_объявлены_невозможными(consistency):
    """На правом листе нет ни одной находки о невозможном коэффициенте."""
    from backend.app.pipeline.stages.block_grounding import document_consistency as dc

    assert not [
        item
        for item in consistency["RIGHT"]["items"]
        if item["kind"] == dc.KIND_IMPLIED_POWER_FACTOR
    ]


def test_повтор_обозначения_найден_на_боевом_листе(consistency):
    """«2ГРЩ-ВРУ3» и «2ГРЩ-ЭБ.ГВС» стоят у двух разных линий секции РП2."""
    from backend.app.pipeline.stages.block_grounding import document_consistency as dc

    subjects = sorted(
        item["subject"]
        for item in consistency["LEFT"]["items"]
        if item["kind"] == dc.KIND_DUPLICATE_DESIGNATION
    )
    assert subjects == ["ВРУ3", "ЭБ-ГВС"]


def test_повторов_нет_там_где_секции_разные(consistency):
    """На правом листе каждое обозначение встречается по разу в каждой секции."""
    from backend.app.pipeline.stages.block_grounding import document_consistency as dc

    assert not [
        item
        for item in consistency["RIGHT"]["items"]
        if item["kind"] == dc.KIND_DUPLICATE_DESIGNATION
    ]


def test_единица_мощности_найдена_по_независимому_токену(consistency):
    """«Рр=10Вт» против «Pp=10кВт» сводного блока над той же колонкой."""
    from backend.app.pipeline.stages.block_grounding import document_consistency as dc

    found = [
        item
        for item in consistency["LEFT"]["items"]
        if item["kind"] == dc.KIND_POWER_UNIT_MISMATCH
    ]
    assert len(found) == 1
    assert found[0]["evidence"]["witness_power_kw"] == pytest.approx(10.0)
    # Объект назван по своей колонке, а не по марке кабеля соседней.
    assert "ЯСН" in str(found[0]["subject"])


def test_сводка_против_суммы_вводов_молчит_на_этой_паре(consistency):
    """Закон таблицы доказан, но каждое расхождение имеет своё объяснение.

    Все четыре кандидата сняты названными причинами: испорченное слагаемое
    (ВРУа), недосчитанный ввод (ВРУ1), другая колонка (ВРУ2) и коэффициент
    одновременности (ВРУ-ИТП). Молчание здесь — результат проверки, а не
    отсутствие проверки, поэтому закреплены и доказанность закона, и причины.
    """
    from backend.app.pipeline.stages.block_grounding import document_consistency as dc

    invariant = consistency["LEFT"]["diagnostics"]["summary_invariant"]
    assert invariant["proven"] is True
    assert invariant["groups_equal"] >= dc.INVARIANT_MIN_GROUPS
    reasons = {item["designation"]: item["reason"] for item in invariant["suppressed"]}
    assert reasons == {
        "ВРУ-А": "row_already_reported_as_contradictory",
        "ВРУ-ИТП": "uniform_demand_factor",
        "ВРУ1": "inputs_incomplete_summary_is_integer_multiple",
        "ВРУ2": "sum_matches_installed_power_column",
    }
    assert not [
        item
        for item in consistency["LEFT"]["items"]
        if item["kind"] == dc.KIND_SUMMARY_INPUT_MISMATCH
    ]


def test_сигнальная_цепь_уходит_на_проверку_а_не_в_противоречия(evidences):
    """Единственная колонка секции 2, подписанная «TS1», — вопрос, не приговор."""
    import json

    from backend.app.pipeline.stages.block_grounding import document_consistency as dc
    from backend.app.pipeline.stages.block_grounding.dense_sectioned_board import (
        build_dense_sectioned_board_graph,
        detect_dense_sectioned_board,
    )

    evidence = evidences["RIGHT"]
    detection = detect_dense_sectioned_board(evidence)
    graph = build_dense_sectioned_board_graph(evidence, detection=detection)
    found = dc.signal_link_outliers(evidence, graph, side="RIGHT")
    assert len(found) == 1
    assert found[0]["verdict"] == dc.VERDICT_REVIEW
    assert found[0]["evidence"]["minority_label"] == "TS1"
    assert found[0]["evidence"]["majority_label"] == "TS2"
    assert found[0]["evidence"]["majority_count"] == 14
    assert json.dumps(found, ensure_ascii=False)


def test_противоречия_листа_не_становятся_изменениями(consistency, diff):
    """Ни одна новая находка не превращается в стрелку «было → стало»."""
    subjects = {
        str(item["subject"])
        for side in ("LEFT", "RIGHT")
        for item in consistency[side]["items"]
    }
    assert subjects
    for change in diff["changes"]:
        assert "физически несовместимы" not in str(change.get("summary") or "")
        assert "стоит у" not in str(change.get("summary") or "")


def test_новые_проверки_не_обращаются_к_модели(consistency):
    for side in ("LEFT", "RIGHT"):
        assert consistency[side]["diagnostics"]["uses_model"] is False
        assert consistency[side]["diagnostics"]["uses_ocr"] is False
