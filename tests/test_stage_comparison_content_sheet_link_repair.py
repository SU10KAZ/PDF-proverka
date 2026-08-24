from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import content_sheet_link_repair as content
from backend.app.services.stage_comparison import sheet_link_repair
from backend.app.services.stage_comparison.sheet_content_fingerprint import (
    build_sheet_content_fingerprint,
)


def record(page: int, text: str, title: str = "") -> dict:
    return {
        "pdf_page": page,
        "sheet_number": str(page),
        "title": title,
        "content_fingerprint": build_sheet_content_fingerprint(text, title=title),
    }


def link(link_id: str, left, right) -> dict:
    return {
        "id": link_id,
        "left_pages": left if isinstance(left, list) else [left],
        "right_pages": right if isinstance(right, list) else [right],
        "source": "manual",
        "confidence": "manual",
        "reason": [],
    }


def payload(links: list[dict]) -> dict:
    return {
        "version": 1,
        "pair_id": "pair-1",
        "links": links,
        "unlinked_left_pages": [],
        "updated_at": "before",
    }


LEFT = [
    record(1, "Узел стойки. Труба FHV-101 80x4. Анкер BSR-M12. Разрез крепления."),
    record(2, "Схема насосной. Насос PUMP-202. Шкаф SHU-202. Датчик PS-202."),
    record(3, "План фасада. Панель FAS-303. Кронштейн KR-303. Ось Z-303."),
]
RIGHT = [
    record(1, "Монтажный узел стойки. Труба FHV-101 80x4. Анкер BSR-M12. Разрез крепления."),
    record(2, "Принципиальная схема насосной. Насос PUMP-202. Шкаф SHU-202. Датчик PS-202."),
    record(3, "Фасадный план. Панель FAS-303. Кронштейн KR-303. Ось Z-303."),
]


def suggestions(left=LEFT, right=RIGHT) -> dict:
    return {"left_sheet_index": left, "right_sheet_index": right}


def plan(links: list[dict], left=LEFT, right=RIGHT, groups=None):
    return sheet_link_repair.plan_repairs(
        payload(links),
        suggestions(left, right),
        {item["id"] for item in links},
        source_groups=groups,
    )


def assessments(links: list[dict], left=LEFT, right=RIGHT, groups=None):
    return content.assess_content_candidates(
        payload(links), suggestions(left, right), {item["id"] for item in links}, groups,
    )


def repaired_pairs(result: dict) -> set[tuple[int, int]]:
    return {
        (item["left_pages"][0], item["right_pages"][0])
        for item in result["after_snapshot"]["links"]
    }


def test_unique_content_anchors_repair_a_wrong_link_without_titles():
    result = plan([link("bad", 1, 2)])

    assert result is not None
    assert repaired_pairs(result) == {(1, 1)}
    change = result["changes"][0]
    assert change["rule"] == content.CONTENT_UNIQUE_ANCHORS
    assert {"fhv-101", "bsr-m12"} <= set(change["unique_anchors"])
    assert change["confidence"] == "HIGH"
    assert change["current_score"] < change["best_score"]


def test_content_mutual_best_can_repair_with_one_strong_anchor_and_multiple_components():
    left = [record(1, "Узел стойки SYS-100. Анкер крепления. Разрез стойки."), record(2, "План кровли ROOF-2.")]
    right = [record(1, "Узел крепления SYS-100. Анкер стойки. Разрез стойки."), record(2, "План кровли ROOF-2.")]

    result = plan([link("bad", 1, 2)], left, right)

    assert result is not None
    assert result["changes"][0]["rule"] == content.CONTENT_MUTUAL_BEST
    assert result["changes"][0]["mutual_best"] is True


def test_high_score_with_low_margin_stays_review():
    left = [record(1, "Схема насоса PUMP-7. Шкаф CTRL-7. Датчик SENSOR-7.")]
    right = [
        record(1, "Схема насоса PUMP-7. Шкаф CTRL-7. Датчик SENSOR-7."),
        record(2, "Схема насоса PUMP-7. Шкаф CTRL-7. Датчик SENSOR-7. Резерв."),
        record(3, "План фасада WALL-9."),
    ]
    links = [link("bad", 1, 3)]

    assert plan(links, left, right) is None
    item = assessments(links, left, right)[0]
    assert item["best_score"] >= content.MIN_SCORE
    assert "LOW_MARGIN" in item["decision_reasons"]
    assert item["auto_repair"] is False


def test_high_score_that_is_not_mutual_best_stays_review():
    left = [
        record(1, "Узел стойки FHV-8. Анкер ANK-8."),
        record(2, "Узел стойки FHV-8. Анкер ANK-8. Разрез точный DETAIL-8."),
    ]
    right = [
        record(1, "Узел стойки FHV-8. Анкер ANK-8. Разрез точный DETAIL-8."),
        record(2, "План кровли ROOF-8."),
    ]
    links = [link("bad", 1, 2)]

    assert plan(links, left, right) is None
    assert "NON_MUTUAL_BEST" in assessments(links, left, right)[0]["decision_reasons"]


def test_purpose_conflict_blocks_content_auto_repair():
    left = [record(1, "План этажа MARK-51 PANEL-51 ROUTE-51."), record(2, "Узел стены WALL-2.")]
    right = [record(1, "Однолинейная схема MARK-51 PANEL-51 ROUTE-51."), record(2, "Узел стены WALL-2.")]
    links = [link("bad", 1, 2)]

    assert plan(links, left, right) is None
    assert "PURPOSE_CONFLICT" in assessments(links, left, right)[0]["decision_reasons"]


def test_content_swap_is_atomic():
    result = plan([link("a", 1, 2), link("b", 2, 1)])

    assert result is not None
    assert repaired_pairs(result) == {(1, 1), (2, 2)}
    assert {item["operation"] for item in result["changes"]} == {content.CONTENT_SWAP}
    assert len(result["before_links"]) == len(result["after_links"]) == 2


def test_content_three_cycle_is_atomic():
    result = plan([link("a", 1, 2), link("b", 2, 3), link("c", 3, 1)])

    assert result is not None
    assert repaired_pairs(result) == {(1, 1), (2, 2), (3, 3)}
    assert {item["operation"] for item in result["changes"]} == {content.CONTENT_3_CYCLE}


@pytest.mark.parametrize(
    "bad_link",
    [link("bad", [1, 2], 1), link("bad", 1, [1, 2])],
)
def test_many_to_many_is_never_content_auto_repaired(bad_link):
    assert plan([bad_link]) is None
    assert assessments([bad_link])[0]["decision_reasons"] == ["MANY_TO_MANY"]


def test_generic_title_ambiguity_without_content_does_not_repair():
    bare = [
        {"pdf_page": 1, "sheet_number": "1", "title": "Общие данные"},
        {"pdf_page": 2, "sheet_number": "2", "title": "Общие данные"},
    ]
    assert plan([link("bad", 1, 2)], bare, bare) is None


def test_missing_title_with_strong_content_still_repairs():
    left = [{**LEFT[0], "title": ""}, LEFT[1]]
    right = [{**RIGHT[0], "title": ""}, RIGHT[1]]
    assert repaired_pairs(plan([link("bad", 1, 2)], left, right)) == {(1, 1)}


def test_cross_sheet_evidence_confirms_but_does_not_replace_content_proof():
    groups = [{
        "group_id": "bad",
        "atomic_evidence": [{
            "evidence_id": "ev-1", "source_status": "FOUND_ON_OTHER_SHEET",
            "right_pages": [1],
        }],
    }]

    result = plan([link("bad", 1, 2)], groups=groups)

    assert result is not None
    confirmation = result["changes"][0]["cross_sheet_confirmation"]
    assert confirmation["confirmed"] is True
    assert confirmation["evidence_ids"] == ["ev-1"]


def test_cross_sheet_evidence_alone_never_auto_repairs():
    left = [record(1, "План этажа без специальных обозначений.")]
    right = [record(1, "Схема оборудования без специальных обозначений."), record(2, "Узел стены.")]
    groups = [{
        "group_id": "bad",
        "atomic_evidence": [{
            "evidence_id": "ev-1", "source_status": "FOUND_ON_OTHER_SHEET",
            "right_pages": [1],
        }],
    }]

    assert plan([link("bad", 1, 2)], left, right, groups) is None


def test_candidate_not_materially_better_than_current_is_not_applied():
    left = [record(1, "Узел стойки FHV-1 ANK-1 крепление разрез.")]
    right = [
        record(1, "Узел стойки FHV-1 ANK-1 крепление."),
        record(2, "Узел стойки FHV-1 ANK-1 крепление разрез дополнительный."),
    ]
    links = [link("bad", 1, 1)]

    assert plan(links, left, right) is None
    assert "NOT_MATERIALLY_BETTER" in assessments(links, left, right)[0]["decision_reasons"]


def test_medium_confidence_never_auto_repairs():
    left = [record(1, "Узел стойки COMMON-1 анкер крепление.")]
    right = [record(1, "Узел стойки COMMON-1 анкер."), record(2, "План стены OTHER-2.")]
    links = [link("bad", 1, 1)]
    item = assessments(links, left, right)[0]

    assert item["confidence"] in {"MEDIUM", "LOW"}
    assert item["auto_repair"] is False
    assert plan(links, left, right) is None


def test_package_common_anchor_is_downweighted_and_not_unique():
    left = [record(1, "Схема щита QF1 общий кабель."), record(2, "Схема щита QF1 общий кабель.")]
    right = [record(1, "Схема щита QF1 общий кабель."), record(2, "Схема щита QF1 общий кабель.")]

    assert plan([link("bad", 1, 2)], left, right) is None
    item = assessments([link("bad", 1, 2)], left, right)[0]
    assert "INSUFFICIENT_CONTENT_ANCHORS" in item["decision_reasons"] or "LOW_MARGIN" in item["decision_reasons"]


def test_correct_current_content_link_is_left_untouched():
    links = [link("correct", 1, 1)]

    assert plan(links) is None
    item = assessments(links)[0]
    assert item["right_sheet_after"] == 1
    assert "CURRENT_LINK_BEST" in item["decision_reasons"]


def test_fingerprint_is_bounded_and_drops_service_noise():
    fingerprint = build_sheet_content_fingerprint(
        "Организация ПРОЕКТ. ГИП Иванов. Телефон +7 999 123-45-67. "
        "Узел стойки FHV-101, анкер BSR-M12, труба 80x4."
    )

    flattened = " ".join(
        str(value) for key, values in fingerprint.items()
        if isinstance(values, list) for value in values
    )
    assert "иванов" not in flattened
    assert "телефон" not in flattened
    assert "fhv-101" in fingerprint["unique_designations"]
    assert len(fingerprint["rare_terms"]) <= 96
    assert "Организация ПРОЕКТ" not in str(fingerprint)


def test_drawing_grid_axes_are_structural_context_not_strong_designations():
    fingerprint = build_sheet_content_fingerprint(
        "Фасад в осях 1.А-2.В. Оси 1.А, 1.Б, 2.В. Панель FAS-303."
    )

    assert "1.а" not in fingerprint["unique_designations"]
    assert "1.а-2.в" not in fingerprint["unique_designations"]
    assert "1.а" in fingerprint["structural_tokens"]
    assert "fas-303" in fingerprint["unique_designations"]
