"""Предварительный отчёт: что система нашла, до решений инженера.

Отчёт отвечает на вопрос «что изменилось», а не «какие внутренние атомы
существуют». Поэтому проверяется прежде всего язык: внутренние коды
(MATERIAL_CHANGE, REVIEW_REQUIRED, UNKNOWN_DIMENSION) и служебные
идентификаторы узлов в текст попадать не вправе.

Отдельно проверяется главная граница: предварительный отчёт показывает ВСЁ, а
итоговый — только подтверждённое инженером. Смешать их нельзя ни в одну
сторону.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import preliminary_report as pr
from backend.app.services.stage_comparison.engineer_review import (
    build_engineer_decisions,
    build_final_report,
)
from backend.app.services.stage_comparison.unified_change_synthesizer.identity import (
    content_signature,
    stable_atomic_change_id,
)


def _change(
    change_id,
    *,
    dimension="PARAMETER",
    direction="INCREASED",
    facet_ref="rated_current_a",
    facet_title="Номинальный ток",
    unit="А",
    identity="ХМ1",
    before=400,
    after=800,
    outcome="MATERIAL_CHANGE",
    review_status="CONFIRMED",
    confidence="HIGH",
    producer="graphic-change-ledger-adapter-v1",
    extra_provenance=None,
    evidence=None,
    subject_kind="individual_node",
):
    subject = {"kind": subject_kind}
    if identity is not None:
        subject["identity"] = [identity]
    provenance = {
        "producer": producer,
        "structured": {
            "level": "NODE",
            "subject": subject,
            "relation": {
                "facet_ref": facet_ref,
                "facet_title": facet_title,
                "unit": unit,
                "left_value": before,
                "right_value": after,
            },
        },
    }
    provenance.update(extra_provenance or {})
    if evidence is not None:
        provenance["evidence"] = evidence
    identity_cell = {
        "identity_version": "unified-change-identity.v1",
        "scope_ref": "scope_1",
        "subject_ref": f"subject#{identity}",
        "dimension": dimension,
        "direction_class": direction,
        "facet_ref": facet_ref,
        "evidence_scope": f"graphic:{change_id}",
    }
    evidence_refs = [
        {
            "evidence_ref": change_id,
            "atom_id": f"graphic:{change_id}",
            "source": "GRAPHIC",
            "source_artifact": {
                "kind": "graphic_change_ledger",
                "schema_version": "graphic-change-ledger.v2",
                "artifact_ref": "sha256:0",
            },
        }
    ]
    return {
        "change_id": stable_atomic_change_id(identity_cell),
        "scope_ref": "scope_1",
        "subject_ref": f"subject#{identity}",
        "project_entity_ref": None,
        "facet_ref": facet_ref,
        "dimension": dimension,
        "direction": direction,
        "outcome": outcome,
        "source_mode": "GRAPHIC",
        "evidence_refs": evidence_refs,
        "relation_status": "SINGLE_SOURCE",
        "confidence": {"level": confidence, "basis": "SINGLE_SOURCE"},
        "before_value": before,
        "after_value": after,
        "review_status": review_status,
        "content_signature": content_signature(evidence_refs),
        "provenance": {
            "identity": identity_cell,
            "source_atoms": [
                {"atom_id": f"graphic:{change_id}", "source": "GRAPHIC", "provenance": provenance}
            ],
            "synthesis": "UNION_SINGLE_SOURCE",
        },
    }


def _synthesis(changes, review_items=()):
    """Полный конверт синтеза: его строго проверяет итоговый отчёт."""
    return {
        "synthesis_version": "unified-change-synthesis.v1",
        "kind": "stage_comparison_unified_changes",
        "direction": "LEFT_TO_RIGHT",
        "policy_version": "unified-change-policy-v1",
        "identity_version": "unified-change-identity.v1",
        "changes": list(changes),
        "review_items": list(review_items),
        "contested_groups": [],
        "presentation_groups": [],
        "diagnostics": {},
        "source_artifacts": [],
        "provenance": {
            "producer": "unified-change-synthesizer-v1",
            "input_contract": "unified-change-synthesis-input.v1",
            "uses_llm": False,
        },
        "validation": {
            "contract": "unified-change-synthesis.v1",
            "valid": True,
            "errors": [],
        },
    }


# --------------------------------------------------------------------------
# 1. Язык отчёта
# --------------------------------------------------------------------------
def test_фраза_вместо_внутренних_кодов():
    text = pr.describe_change(
        _change("c1", identity="QF1", before=2500, after=3200)
    )
    assert text == "QF1: номинальный ток увеличен с 2500 до 3200 А."


def test_внутренние_коды_в_отчёт_не_попадают():
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [
                _change("c1"),
                _change("c2", dimension="QUANTITY", identity=None,
                        subject_kind="repeated_node_group", facet_ref=None,
                        facet_title=None, unit=None, before=27, after=30),
            ]
        ),
    )
    blob = pr.render_markdown(report)
    for code in (
        "MATERIAL_CHANGE",
        "REVIEW_REQUIRED",
        "UNKNOWN_DIMENSION",
        "PARAMETER",
        "rated_current_a",
        "GRAPHIC",
    ):
        assert code not in blob, f"внутренний код {code} просочился в отчёт"


def test_служебное_обозначение_узла_заменяется_названием():
    """«SECTION-TIE#BUS1-BUS2» инженеру ничего не говорит."""
    text = pr.describe_change(
        _change("c1", identity="SECTION-TIE#BUS1-BUS2", before=1600, after=2000)
    )
    assert "SECTION-TIE" not in text
    assert text.startswith("Секционный аппарат между секциями 1 и 2:")


def test_обозначение_чертежа_сохраняется():
    """«ХМ1» и «ВРУ4» — язык самого документа, их подменять не надо."""
    assert pr.subject_name("ХМ1") == "ХМ1"
    assert pr.subject_name("ВРУ4") == "ВРУ4"


@pytest.mark.parametrize(
    "facet_ref, facet_title, expected",
    [
        ("rated_current_a", "Номинальный ток", "увеличен"),
        ("demand_active_power_kw", "Расчётная активная мощность", "увеличена"),
        ("cable_parallel_count", "Число параллельных кабелей", "увеличено"),
    ],
)
def test_род_свойства_согласован(facet_ref, facet_title, expected):
    text = pr.describe_change(
        _change("c1", facet_ref=facet_ref, facet_title=facet_title, unit=None)
    )
    assert expected in text


def test_уменьшение_названо_уменьшением():
    text = pr.describe_change(
        _change("c1", direction="DECREASED",
                facet_ref="demand_reactive_power_kvar",
                facet_title="Расчётная реактивная мощность", unit="кВАр",
                identity="АУКРМ-1", before=200.0, after=180.0)
    )
    assert text == "АУКРМ-1: расчётная реактивная мощность уменьшена с 200 до 180 кВАр."


@pytest.mark.parametrize(
    "value, expected",
    [(335.0, "335"), (157.5, "157,5"), (41, "41"), (676.8, "676,8"), ([233.6, 284.7], "233,6/284,7")],
)
def test_числа_печатаются_по_русски(value, expected):
    assert pr.format_number(value) == expected


def test_тип_аппарата_на_языке_чертежа():
    text = pr.describe_change(
        {
            **_change("c1", dimension="TYPE", direction="REPLACED",
                      identity="SECTION-TIE#BUS1-BUS2", facet_ref=None,
                      facet_title=None, unit=None, before=None, after=None),
        }
        | {"dimension": "TYPE"}
    )
    assert "SWITCH_DISCONNECTOR" not in text
    assert "CIRCUIT_BREAKER" not in text


def test_замена_типа_читается_словами():
    change = _change("c1", dimension="TYPE", direction="REPLACED",
                     identity="SECTION-TIE#BUS1-BUS2", facet_ref=None,
                     facet_title=None, unit=None, before=None, after=None)
    structured = change["provenance"]["source_atoms"][0]["provenance"]["structured"]
    structured["relation"] = {
        "left_effective_type": "SWITCH_DISCONNECTOR",
        "right_effective_type": "CIRCUIT_BREAKER",
    }
    text = pr.describe_change(change)
    assert text == (
        "Секционный аппарат между секциями 1 и 2: разъединитель заменён на "
        "автоматический выключатель."
    )


# --------------------------------------------------------------------------
# 2. Статусы
# --------------------------------------------------------------------------
def test_ровно_четыре_статуса():
    assert pr.STATUSES == (
        "Найдено автоматически",
        "Требуется проверка инженера",
        "Внутреннее противоречие документа",
        "Недостаточно доказательств",
    )


def test_каждая_строка_имеет_разрешённый_статус():
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis([_change("c1"), _change("c2", confidence="LOW")]),
        document_inconsistencies={
            "items": [
                {
                    "inconsistency_id": "dinc_1",
                    "side": "RIGHT",
                    "subject": "1QF1",
                    "summary": "Аппарат 1QF1 обозначен как относящийся к секции 1.",
                    "evidence": {"bbox": [0, 0, 1, 1]},
                }
            ]
        },
        electrical_table_changes={
            "blocked": [{"summary": "Режимы не совпадают.", "subject": "ВРУ1"}],
            "unproven": [],
        },
    )
    for section in report["sections"]:
        for item in section.get("items") or ():
            assert item["status"] in pr.STATUSES
        for group in section.get("groups") or ():
            for item in group["items"]:
                assert item["status"] in pr.STATUSES


def test_слабая_уверенность_уходит_в_проверку():
    report = pr.build_preliminary_report(
        pair_id="p1", synthesis=_synthesis([_change("c1", confidence="LOW")])
    )
    review = next(s for s in report["sections"] if s["section_id"] == pr.SECTION_REVIEW)
    assert len(review["items"]) == 1
    assert review["items"][0]["status"] == pr.STATUS_REVIEW


def test_противоречие_документа_не_выдаётся_как_изменение():
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis([]),
        document_inconsistencies={
            "items": [
                {
                    "inconsistency_id": "dinc_1",
                    "side": "RIGHT",
                    "subject": "1QF1",
                    "summary": "1QF1 стоит во второй секции.",
                    "evidence": {"bbox": [0, 0, 1, 1]},
                }
            ]
        },
    )
    section = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_INCONSISTENCIES
    )
    assert [item["status"] for item in section["items"]] == [pr.STATUS_INCONSISTENCY]
    assert section["items"][0]["change_ids"] == []


def test_недоказанное_показывается_а_не_умалчивается():
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis([]),
        electrical_table_changes={
            "blocked": [],
            "unproven": [
                {
                    "side": "LEFT",
                    "subject": "ШУ-ХВС",
                    "section_ref": "РП1",
                    "row_kind": "FEEDER",
                    "summary": "Строка «ШУ-ХВС» не имеет доказанной пары.",
                }
            ],
        },
    )
    section = next(s for s in report["sections"] if s["section_id"] == pr.SECTION_UNPROVEN)
    assert [item["status"] for item in section["items"]] == [pr.STATUS_UNPROVEN]


# --------------------------------------------------------------------------
# 3. Группировка
# --------------------------------------------------------------------------
def test_свойства_одного_объекта_под_одним_заголовком():
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [
                _change("c1", identity="ХМ1", facet_ref="rated_current_a",
                        facet_title="Номинальный ток", before=400, after=800),
                _change("c2", identity="ХМ1", facet_ref="demand_active_power_kw",
                        facet_title="Расчётная активная мощность", unit="кВт",
                        before=157.5, after=335.0),
            ]
        ),
    )
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    assert len(equipment["groups"]) == 1
    group = equipment["groups"][0]
    assert group["title"] == "ХМ1 — холодильная машина"
    assert len(group["items"]) == 2


def test_группа_нового_факта_не_создаёт():
    report = pr.build_preliminary_report(
        pair_id="p1", synthesis=_synthesis([_change("c1"), _change("c2")])
    )
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    for group in equipment["groups"]:
        assert group["creates_engineering_fact"] is False


def test_решение_остаётся_атомарным_при_схлопывании_повтора():
    """Одинаковая фраза из двух изменений — одна строка, но два решения."""
    first, duplicate = _change("c1"), _change("c2")
    assert first["change_id"] != duplicate["change_id"]
    report = pr.build_preliminary_report(
        pair_id="p1", synthesis=_synthesis([first, duplicate])
    )
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    items = equipment["groups"][0]["items"]
    assert len(items) == 1
    assert sorted(items[0]["change_ids"]) == sorted(
        [first["change_id"], duplicate["change_id"]]
    )


def test_изменения_уровня_схемы_отделены_от_оборудования():
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [
                _change("c1", identity="INPUT#BUS1", before=2500, after=3200),
                _change("c2", identity="ХМ1"),
            ]
        ),
    )
    scheme = next(s for s in report["sections"] if s["section_id"] == pr.SECTION_SCHEME)
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    assert len(scheme["items"]) == 1
    assert [group["subject"] for group in equipment["groups"]] == ["ХМ1"]


def test_секция_и_ввод_видны_в_строке():
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [
                _change(
                    "c1",
                    identity="ВРУ1",
                    producer="electrical-table-diff-v1",
                    extra_provenance={"section_ref": "РП2", "input_number": 2},
                )
            ]
        ),
    )
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    assert equipment["groups"][0]["items"][0]["detail"] == "секция РП2, ввод 2"


def test_оговорки_доходят_до_инженера():
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [
                _change(
                    "c1",
                    identity="АУКРМ-1",
                    producer="electrical-table-diff-v1",
                    extra_provenance={
                        "notes": [
                            "На левом листе величина приведена по потребителю целиком,"
                            " на правом — по фидеру."
                        ]
                    },
                )
            ]
        ),
    )
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    assert equipment["groups"][0]["items"][0]["notes"]
    assert "по фидеру" in pr.render_markdown(report)


# --------------------------------------------------------------------------
# 4. Предварительный против итогового
# --------------------------------------------------------------------------
def test_предварительный_не_итоговый():
    report = pr.build_preliminary_report(pair_id="p1", synthesis=_synthesis([_change("c1")]))
    assert report["constraints"]["is_final_report"] is False
    assert report["constraints"]["requires_engineer_review"] is True
    assert report["constraints"]["read_only"] is True
    assert report["kind"] != "stage_comparison_approved_changes_report"


def test_предварительный_показывает_неподтверждённое_итоговый_нет():
    """Главная граница: до подтверждения находка видна только в предварительном."""
    synthesis = _synthesis([_change("c1")])
    decisions = build_engineer_decisions(synthesis)
    final = build_final_report(synthesis, decisions, object_ref=None)
    assert final.get("approved_atomic_changes") == []

    report = pr.build_preliminary_report(pair_id="p1", synthesis=synthesis)
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    assert len(equipment["groups"]) == 1


def test_модель_не_вызывается():
    report = pr.build_preliminary_report(pair_id="p1", synthesis=_synthesis([_change("c1")]))
    assert report["constraints"]["uses_model"] is False


def test_пустой_синтез_не_ломает_отчёт():
    report = pr.build_preliminary_report(pair_id="p1", synthesis=None)
    assert report["summary"]["counts"]["changes"] == 0
    assert "Доказанных изменений между редакциями не найдено." in (
        report["summary"]["sentences"]
    )


# --------------------------------------------------------------------------
# 5. Ссылки на доказательства
# --------------------------------------------------------------------------
def test_строка_несёт_ссылку_на_доказательство():
    change = _change(
        "c1",
        identity="ХМ1",
        producer="electrical-table-diff-v1",
        evidence={
            "LEFT": {"page_index": 0, "bbox": [1.0, 2.0, 3.0, 4.0],
                     "raw": "Рр=157,5кВт", "row_id": "etrow_l"},
            "RIGHT": {"page_index": 0, "bbox": [5.0, 6.0, 7.0, 8.0],
                      "raw": "335.0 кВт", "row_id": "etrow_r"},
        },
    )
    report = pr.build_preliminary_report(pair_id="p1", synthesis=_synthesis([change]))
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    item = equipment["groups"][0]["items"][0]
    assert set(item["evidence"]) == {"LEFT", "RIGHT"}
    assert item["evidence"]["LEFT"]["bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert item["navigation"]["kind"] == "CHANGE"
    assert item["navigation"]["target_id"] == change["change_id"]


def test_ссылка_ведёт_на_конкретное_изменение():
    changes = [_change("c1"), _change("c2", identity="ВРУ4")]
    report = pr.build_preliminary_report(pair_id="p1", synthesis=_synthesis(changes))
    targets = {
        item["navigation"]["target_id"]
        for section in report["sections"]
        for group in section.get("groups") or ()
        for item in group["items"]
    }
    assert targets == {change["change_id"] for change in changes}


# --------------------------------------------------------------------------
# 6. Честность формулировок в разделе проверки
# --------------------------------------------------------------------------
def _review_evidence(review_id: str, after: str) -> dict:
    return {
        "review_evidence_id": review_id,
        "atom_id": f"tatom_{review_id}",
        "source": "TEXT",
        "before_value": None,
        "after_value": after,
        "outcome": "REVIEW_REQUIRED",
        "review_status": "REVIEW_REQUIRED",
    }


def test_нехватка_распознавания_не_выдаётся_за_появление():
    """Левый лист читается из вектор-слоя и вправе лишь подтверждать совпадение.

    Написать «на правом листе появилось» значило бы выдать непрочитанное за
    добавленное — ровно та ложная находка, от которой защищают урезанные права
    нативного текста.
    """
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis([], review_items=[_review_evidence("r1", "Формат А2х3")]),
    )
    review = next(s for s in report["sections"] if s["section_id"] == pr.SECTION_REVIEW)
    text = review["items"][0]["text"]
    assert "появилось" not in text
    assert "не сопоставлен" in text
    assert "Формат А2х3" in text


def test_инженерное_идёт_перед_текстом_штампа():
    """Шесть находок по оборудованию не должны тонуть в двух десятках подписей."""
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [_change("c1", confidence="LOW", identity="ВРУ1")],
            review_items=[
                _review_evidence(f"r{i}", f"ГИП Шараева 0{i}.26") for i in range(1, 6)
            ],
        ),
    )
    review = next(s for s in report["sections"] if s["section_id"] == pr.SECTION_REVIEW)
    statuses = [item.get("engineering", False) for item in review["items"]]
    assert statuses[0] is True
    assert statuses.count(True) == 1
    # Текст штампа не выброшен — он остаётся видимым, просто ниже.
    assert len(review["items"]) == 6


@pytest.mark.parametrize(
    "count, expected",
    [
        (1, "позицию"), (2, "позиции"), (4, "позиции"), (5, "позиций"),
        (11, "позиций"), (12, "позиций"), (14, "позиций"),
        (21, "позицию"), (24, "позиции"), (25, "позиций"),
    ],
)
def test_числительное_склоняется(count, expected):
    """«24 позиций» читается как машинный перевод — ровно то, что убирает отчёт."""
    assert pr.plural(count, "позицию", "позиции", "позиций") == expected


def test_сводка_согласована_по_числу():
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis([_change("c1")]),
        electrical_table_changes={
            "blocked": [],
            "unproven": [
                {"side": "LEFT", "subject": f"ВРУ{i}", "section_ref": None,
                 "row_kind": "FEEDER", "summary": f"строка {i}"}
                for i in range(1, 25)
            ],
        },
    )
    text = " ".join(report["summary"]["sentences"])
    assert "1 изменение" in text
    assert "24 позиции" in text
    assert "позиций" not in text


def test_разные_написания_одного_щита_дают_одну_группу():
    """Граф щита зовёт щит автостоянки «ВРУА», таблица — «ВРУ-А»."""
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [
                _change("c1", identity="ВРУА", facet_ref="rated_current_a",
                        facet_title="Номинальный ток", before=400, after=320),
                _change("c2", identity="ВРУ-А", facet_ref="demand_active_power_kw",
                        facet_title="Расчётная активная мощность", unit="кВт",
                        before=307.6, after=62.5),
            ]
        ),
    )
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    assert len(equipment["groups"]) == 1
    assert equipment["groups"][0]["title"].startswith("ВРУ-А")
    assert len(equipment["groups"][0]["items"]) == 2


def test_семейство_снятое_графом_возвращается_в_группу():
    """«1ГРЩ-ВРУ.ИТП» граф сводит к «ИТП», таблица держит «ВРУ-ИТП»."""
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [
                _change("c1", identity="ИТП", facet_ref="rated_current_a",
                        facet_title="Номинальный ток", before=50, after=63),
                _change("c2", identity="ВРУ-ИТП", facet_ref="demand_active_power_kw",
                        facet_title="Расчётная активная мощность", unit="кВт",
                        before=8.7, after=10.5),
            ]
        ),
    )
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    assert len(equipment["groups"]) == 1


def test_два_семейства_с_общим_хвостом_не_сливаются():
    """«ШУ-ХЦ» и «ВРУ-ХЦ» — разные щиты; свернуть «ХЦ» не с чем."""
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [
                _change("c1", identity="ХЦ"),
                _change("c2", identity="ШУ-ХЦ"),
                _change("c3", identity="ВРУ-ХЦ"),
            ]
        ),
    )
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    assert len(equipment["groups"]) == 3


def test_охладитель_не_поглощает_машину_при_группировке():
    """Свёртка семейств не смеет спрятать «ХМ1» внутрь группы «ДР1-ХМ1».

    «ДР1» — обозначение охладителя, а не название семейства щитов. Ровно эта
    подмена и есть та ложная связь, от которой защищает весь связчик.
    """
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis(
            [
                _change("c1", identity="ХМ1", facet_ref="demand_active_power_kw",
                        facet_title="Расчётная активная мощность", unit="кВт",
                        before=157.5, after=335.0),
                _change("c2", identity="ДР1-ХМ1", facet_ref="maximum_calculated_current_a",
                        facet_title="Расчётный ток", unit="А", before=41, after=43.6),
            ]
        ),
    )
    equipment = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_EQUIPMENT
    )
    titles = [group["title"] for group in equipment["groups"]]
    assert len(equipment["groups"]) == 2
    assert any(t.startswith("ХМ1") for t in titles)
    assert any(t.startswith("ДР1-ХМ1") for t in titles)


def test_находка_с_вердиктом_проверки_уходит_в_раздел_проверки():
    """Разделы отличаются не источником находки, а тем, что о ней утверждается.

    Необычная связь сигнальной цепи доказана статистикой ряда, а не самим
    чертежом. Назвать её «внутренним противоречием документа» значило бы выдать
    правдоподобие за факт, поэтому она идёт в раздел проверки — оставаясь при
    этом инженерной строкой, а не текстовым различием штампа.
    """
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis=_synthesis([]),
        document_inconsistencies={
            "items": [
                {
                    "inconsistency_id": "dinc_confirmed",
                    "verdict": "CONFIRMED",
                    "side": "LEFT",
                    "subject": "ШУ-ХП",
                    "summary": "Мощность и ток линии физически несовместимы.",
                    "evidence": {"bbox": [0, 0, 1, 1]},
                },
                {
                    "inconsistency_id": "dinc_review",
                    "verdict": "REVIEW",
                    "side": "RIGHT",
                    "subject": "2QF14",
                    "summary": "Сигнальная цепь подписана иначе. Требуется проверка.",
                    "evidence": {"bbox": [1, 1, 2, 2]},
                },
            ]
        },
    )
    inconsistencies = next(
        s for s in report["sections"] if s["section_id"] == pr.SECTION_INCONSISTENCIES
    )
    review = next(s for s in report["sections"] if s["section_id"] == pr.SECTION_REVIEW)
    assert [item["subject"] for item in inconsistencies["items"]] == ["ШУ-ХП"]
    assert [item["subject"] for item in review["items"]] == ["2QF14"]
    assert review["items"][0]["status"] == pr.STATUS_REVIEW
    assert review["items"][0]["engineering"] is True


def test_находки_чертежа_стоят_выше_текстовых_различий():
    """Инженерная строка не должна тонуть под различиями штампа."""
    report = pr.build_preliminary_report(
        pair_id="p1",
        synthesis={
            "changes": [],
            "review_items": [
                {
                    "evidence_id": "ev1",
                    "summary": "Различие в примечании штампа.",
                    "subject": None,
                }
            ],
        },
        document_inconsistencies={
            "items": [
                {
                    "inconsistency_id": "dinc_review",
                    "verdict": "REVIEW",
                    "side": "RIGHT",
                    "subject": "2QF14",
                    "summary": "Сигнальная цепь подписана иначе. Требуется проверка.",
                    "evidence": {"bbox": [1, 1, 2, 2]},
                }
            ]
        },
    )
    review = next(s for s in report["sections"] if s["section_id"] == pr.SECTION_REVIEW)
    assert review["items"][0]["subject"] == "2QF14"
