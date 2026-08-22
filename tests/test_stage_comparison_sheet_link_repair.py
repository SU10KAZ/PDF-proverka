from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import sheet_link_repair as repair


def index(*titles: str) -> list[dict]:
    return [
        {"pdf_page": page, "sheet_number": str(page), "title": title}
        for page, title in enumerate(titles, 1)
    ]


def link(link_id: str, left: int | list[int], right: int | list[int], *, source="manual") -> dict:
    return {
        "id": link_id,
        "left_pages": left if isinstance(left, list) else [left],
        "right_pages": right if isinstance(right, list) else [right],
        "source": source, "confidence": "manual", "reason": [],
    }


def plan(left_titles, right_titles, links, problem_ids):
    return repair.plan_repairs(
        {"version": 1, "pair_id": "p1", "links": links,
         "unlinked_left_pages": [], "updated_at": "before"},
        {"left_sheet_index": index(*left_titles), "right_sheet_index": index(*right_titles)},
        set(problem_ids),
    )


def pairs(result):
    return {
        (item["left_pages"][0], item["right_pages"][0])
        for item in result["after_snapshot"]["links"]
    }


def test_title_normalization_is_nfkc_case_and_punctuation_insensitive():
    assert repair.normalize_title("  ВРУ–А: СХЕМА. ") == "вру а схема"


def test_repairs_one_wrong_manual_link_by_unique_exact_title():
    result = plan(
        ["Молниезащита", "Однолинейная схема ВРУ-А"],
        ["Однолинейная схема ВРУ-А"], [link("bad", 1, 1)], ["bad"],
    )
    assert result is not None
    assert pairs(result) == {(2, 1)}
    assert result["changes"][0]["rule"] == "exact_unique_title"
    assert result["after_links"][0]["source"] == "auto_repair"


def test_released_wrong_left_page_becomes_explicitly_unlinked():
    result = plan(
        ["Молниезащита", "Однолинейная схема ВРУ-А"],
        ["Однолинейная схема ВРУ-А"], [link("bad", 1, 1)], ["bad"],
    )
    assert result["after_snapshot"]["unlinked_left_pages"] == [1]


def test_duplicate_left_title_is_not_high_confidence():
    assert plan(
        ["Однолинейная схема ВРУ-А", "Однолинейная схема ВРУ-А", "Молниезащита"],
        ["Однолинейная схема ВРУ-А"], [link("bad", 3, 1)], ["bad"],
    ) is None


def test_duplicate_right_title_is_not_high_confidence():
    assert plan(
        ["Однолинейная схема ВРУ-А", "Молниезащита"],
        ["Однолинейная схема ВРУ-А", "Однолинейная схема ВРУ-А"],
        [link("bad", 2, 1)], ["bad"],
    ) is None


@pytest.mark.parametrize("title", ["Архитектурные решения", "Общие данные", "Страница 12"])
def test_generic_or_placeholder_title_is_never_repaired(title):
    assert plan([title, "Молниезащита"], [title], [link("bad", 2, 1)], ["bad"]) is None


def test_accepts_fuzzy_mutual_best_above_threshold():
    result = plan(
        ["Молниезащита", "Узел крепления стойки фахверка ось 1"],
        ["Узел крепления стойки фахверка оси 1"], [link("bad", 1, 1)], ["bad"],
    )
    assert result is not None
    assert result["changes"][0]["rule"] == "fuzzy_mutual_unique_title"
    assert result["changes"][0]["similarity"] >= repair.FUZZY_THRESHOLD


def test_rejects_fuzzy_match_below_threshold():
    assert plan(
        ["Узел опирания балки перекрытия", "Молниезащита"],
        ["Узел крепления стойки фахверка"], [link("bad", 2, 1)], ["bad"],
    ) is None


def test_rejects_fuzzy_match_with_different_purpose_type():
    assert plan(
        ["План крепления стойки фахверка ось 1", "Молниезащита"],
        ["Узел крепления стойки фахверка оси 1"], [link("bad", 2, 1)], ["bad"],
    ) is None


def test_rejects_fuzzy_match_without_unique_margin():
    assert plan(
        ["Узел крепления стойки фахверка ось 1", "Узел крепления стойки фахверка ось 2", "Молниезащита"],
        ["Узел крепления стойки фахверка ось 3"], [link("bad", 3, 1)], ["bad"],
    ) is None


def test_rejects_fuzzy_match_that_is_not_mutual_best():
    assert plan(
        ["Узел крепления стойки фахверка ось 1", "Молниезащита"],
        ["Узел крепления стойки фахверка по оси 1", "Узел крепления стойки фахверка ось 1"],
        [link("bad", 2, 1), link("ok", 1, 2)], ["bad"],
    ) is None


def test_many_to_many_problem_link_is_never_touched():
    assert plan(
        ["Молниезащита", "Однолинейная схема ВРУ-А"],
        ["Однолинейная схема ВРУ-А"], [link("bad", [1, 2], 1)], ["bad"],
    ) is None


def test_candidate_owned_by_many_to_many_link_is_not_treated_as_free():
    assert plan(
        ["Молниезащита", "Однолинейная схема ВРУ-А", "План кровли"],
        ["Однолинейная схема ВРУ-А", "План кровли"],
        [link("bad", 1, 1), link("many", [2, 3], 2)], ["bad"],
    ) is None


def test_duplicate_page_membership_is_ambiguous():
    assert plan(
        ["Молниезащита", "Однолинейная схема ВРУ-А"],
        ["Однолинейная схема ВРУ-А", "Молниезащита"],
        [link("bad", 1, 1), link("duplicate", 1, 2)], ["bad"],
    ) is None


def test_repairs_two_way_swap_atomically():
    result = plan(
        ["Однолинейная схема ВРУ-А", "Молниезащита здания"],
        ["Однолинейная схема ВРУ-А", "Молниезащита здания"],
        [link("a", 1, 2), link("b", 2, 1)], ["a", "b"],
    )
    assert result is not None
    assert pairs(result) == {(1, 1), (2, 2)}
    assert len(result["before_links"]) == len(result["after_links"]) == 2


def test_repairs_three_cycle_atomically():
    result = plan(
        ["Однолинейная схема ВРУ-А", "Молниезащита здания", "Узел стойки фахверка"],
        ["Однолинейная схема ВРУ-А", "Молниезащита здания", "Узел стойки фахверка"],
        [link("a", 1, 2), link("b", 2, 3), link("c", 3, 1)], ["a", "b", "c"],
    )
    assert result is not None
    assert pairs(result) == {(1, 1), (2, 2), (3, 3)}
    assert len(result["changes"]) == 3


def test_does_not_partially_break_an_unproven_chain():
    assert plan(
        ["Однолинейная схема ВРУ-А", "Молниезащита здания"],
        ["Однолинейная схема ВРУ-А", "Молниезащита здания"],
        [link("bad", 2, 1), link("not-rejected", 1, 2)], ["bad"],
    ) is None


def test_unrelated_link_and_existing_unlinked_page_are_preserved():
    links = [link("bad", 1, 1), link("keep", 3, 2)]
    payload = {"version": 1, "pair_id": "p1", "links": links,
               "unlinked_left_pages": [4], "updated_at": "before"}
    result = repair.plan_repairs(
        payload,
        {"left_sheet_index": index("Молниезащита", "Однолинейная схема ВРУ-А", "Общие данные", "План кровли"),
         "right_sheet_index": index("Однолинейная схема ВРУ-А", "Общие данные")},
        {"bad"},
    )
    assert pairs(result) == {(2, 1), (3, 2)}
    assert result["after_snapshot"]["unlinked_left_pages"] == [1, 4]
