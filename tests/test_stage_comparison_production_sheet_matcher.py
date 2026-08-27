from __future__ import annotations

from backend.app.services.stage_comparison.sheet_matcher import (
    match_sheets,
    page_selection_suggestions,
)


def _sheet(page: int, **facts):
    return {"pdf_page": page, **facts}


def test_function_and_topology_outvote_changed_title():
    result = match_sheets(
        [_sheet(
            10,
            title="Щит распределительный ЩР-1",
            functional_content=["распределение питания секции 1"],
            main_entities=["ввод 1", "нагрузки секции 1"],
            topology=["ввод 1 -> секция 1", "секция 1 -> нагрузки"],
        )],
        [_sheet(
            24,
            title="Шкаф распределительный ШР-1",
            functional_content=["распределение питания секции 1"],
            main_entities=["ввод 1", "нагрузки секции 1"],
            topology=["ввод 1 -> секция 1", "секция 1 -> нагрузки"],
        )],
    )

    relation = result["relations"][0]
    assert relation["status"] == "HIGH"
    assert relation["left_pages"] == [10]
    assert relation["right_pages"] == [24]
    assert relation["provenance"]["title_is_primary"] is False


def test_two_pass_search_bounds_deep_comparisons():
    left = [_sheet(page, title=f"Лист {page}") for page in range(1, 5)]
    right = [_sheet(page, title=f"Другой лист {page}") for page in range(1, 21)]

    result = match_sheets(left, right, top_k=3)

    assert result["diagnostics"]["full_cartesian_pair_count"] == 80
    assert result["diagnostics"]["pass1_pair_count"] == 12
    assert result["diagnostics"]["deep_pair_count"] == 12


def test_one_to_many_is_first_class_split_relation():
    common = {
        "comparison_group_ref": "panel-1",
        "functional_content": ["распределение питания"],
        "main_entities": ["щит 1", "нагрузки 1"],
        "topology": ["ввод -> шины -> нагрузки"],
    }
    result = match_sheets(
        [_sheet(10, title="ЩР-1", **common)],
        [
            _sheet(24, title="ШР-1 часть 1", **common),
            _sheet(25, title="ШР-1 часть 2", **common),
        ],
    )

    relation = result["relations"][0]
    assert relation["relation_type"] == "SPLIT"
    assert relation["left_pages"] == [10]
    assert relation["right_pages"] == [24, 25]


def test_many_to_one_is_first_class_merged_relation():
    common = {
        "comparison_group_ref": "panel-1",
        "functional_content": ["распределение питания"],
        "main_entities": ["щит 1", "нагрузки 1"],
        "topology": ["ввод -> шины -> нагрузки"],
    }
    result = match_sheets(
        [
            _sheet(10, title="ЩР-1 часть 1", **common),
            _sheet(11, title="ЩР-1 часть 2", **common),
        ],
        [_sheet(24, title="ШР-1", **common)],
    )

    relation = result["relations"][0]
    assert relation["relation_type"] == "MERGED"
    assert relation["left_pages"] == [10, 11]
    assert relation["right_pages"] == [24]


def test_page_suggestion_never_replaces_user_selection():
    relations = match_sheets(
        [_sheet(
            52,
            functional_content=["распределение питания"],
            main_entities=["грщ"],
            topology=["ввод -> грщ"],
        )],
        [_sheet(
            24,
            functional_content=["распределение питания"],
            main_entities=["грщ"],
            topology=["ввод -> грщ"],
        )],
    )

    payload = page_selection_suggestions([52], [23], relations)

    assert payload["selected_scope"] == {"left_pages": [52], "right_pages": [23]}
    assert payload["selection_preserved"] is True
    assert payload["sheet_matcher_is_gate"] is False
    assert payload["suggestions"][0]["suggested_right_pages"] == [24]
    assert payload["suggestions"][0]["applied"] is False


def test_order_does_not_change_relation_ids():
    left = [
        _sheet(1, functional_content=["план кровли"], main_entities=["корпус 1"]),
        _sheet(2, functional_content=["план этажа"], main_entities=["корпус 1"]),
    ]
    right = [
        _sheet(8, functional_content=["план этажа"], main_entities=["корпус 1"]),
        _sheet(9, functional_content=["план кровли"], main_entities=["корпус 1"]),
    ]

    first = match_sheets(left, right, generated_at="fixed")
    second = match_sheets(reversed(left), reversed(right), generated_at="fixed")

    assert [item["relation_id"] for item in first["relations"]] == [
        item["relation_id"] for item in second["relations"]
    ]
