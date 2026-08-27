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


def test_deep_candidate_graph_proves_split_without_group_reference():
    result = match_sheets(
        [_sheet(
            10,
            title="Старая сводная схема",
            functional_content=["распределение питания"],
            main_entities=["QF1", "QF2"],
            topology=["шины -> QF1", "шины -> QF2"],
        )],
        [
            _sheet(
                24,
                title="Фидер А",
                functional_content=["распределение питания"],
                main_entities=["QF1"],
                topology=["шины -> QF1"],
            ),
            _sheet(
                25,
                title="Фидер Б",
                functional_content=["распределение питания"],
                main_entities=["QF2"],
                topology=["шины -> QF2"],
            ),
        ],
    )

    relation = result["relations"][0]
    assert relation["relation_type"] == "SPLIT"
    assert relation["left_pages"] == [10]
    assert relation["right_pages"] == [24, 25]
    assert {
        (edge["left_page"], edge["right_page"])
        for edge in relation["supported_edges"]
    } == {(10, 24), (10, 25)}
    aggregate = next(
        item for item in relation["evidence"] if item.get("kind") == "AGGREGATE_CONTENT"
    )
    assert aggregate["coverage"] == 1.0
    assert aggregate["title_used"] is False
    assert aggregate["page_proximity_used"] is False
    assert {item["page"] for item in aggregate["distinct_contributions"]} == {24, 25}


def test_deep_candidate_graph_proves_merge_without_group_reference():
    result = match_sheets(
        [
            _sheet(
                10,
                functional_content=["распределение питания"],
                main_entities=["QF1"],
                topology=["шины -> QF1"],
            ),
            _sheet(
                11,
                functional_content=["распределение питания"],
                main_entities=["QF2"],
                topology=["шины -> QF2"],
            ),
        ],
        [_sheet(
            24,
            functional_content=["распределение питания"],
            main_entities=["QF1", "QF2"],
            topology=["шины -> QF1", "шины -> QF2"],
        )],
    )

    relation = result["relations"][0]
    assert relation["relation_type"] == "MERGED"
    assert relation["left_pages"] == [10, 11]
    assert relation["right_pages"] == [24]
    assert {
        (edge["left_page"], edge["right_page"])
        for edge in relation["supported_edges"]
    } == {(10, 24), (11, 24)}


def test_explicit_group_drops_page_without_supported_deep_edge():
    common = {
        "comparison_group_ref": "panel-1",
        "functional_content": ["распределение питания"],
    }
    result = match_sheets(
        [_sheet(
            10,
            main_entities=["QF1", "QF2"],
            topology=["шины -> QF1", "шины -> QF2"],
            **common,
        )],
        [
            _sheet(
                24,
                main_entities=["QF1"],
                topology=["шины -> QF1"],
                **common,
            ),
            _sheet(
                25,
                main_entities=["QF2"],
                topology=["шины -> QF2"],
                **common,
            ),
            _sheet(
                26,
                comparison_group_ref="panel-1",
                functional_content=["план освещения"],
                main_entities=["светильник L1"],
                topology=["ЩО -> L1"],
            ),
        ],
    )

    split = next(item for item in result["relations"] if item["relation_type"] == "SPLIT")
    assert split["right_pages"] == [24, 25]
    assert 26 not in split["candidate_pages"]
    assert all(edge["right_page"] != 26 for edge in split["supported_edges"])
    assert result["unmatched_right_pages"] == [26]


def test_page_proximity_and_title_alone_produce_explicit_unknown_result():
    result = match_sheets(
        [_sheet(7, title="План первого этажа")],
        [_sheet(7, title="План первого этажа")],
    )

    unresolved_left = next(
        item for item in result["relations"] if item["left_pages"] == [7]
    )
    assert unresolved_left["status"] == "UNKNOWN"
    assert unresolved_left["relation_type"] == "UNCERTAIN"
    assert unresolved_left["right_pages"] == []
    assert unresolved_left["candidate_pages"] == [7]
    assert unresolved_left["automatic_scope"] is False
    assert result["unmatched_left_pages"] == [7]
    assert result["unmatched_right_pages"] == [7]
    deep = result["candidate_search"][0]["deep_candidates"][0]
    assert deep["status"] == "UNKNOWN"
    assert deep["score"] is None


def test_content_mismatch_produces_explicit_no_match_result():
    result = match_sheets(
        [_sheet(
            3,
            functional_content=["распределение питания"],
            main_entities=["QF1"],
        )],
        [_sheet(
            3,
            functional_content=["пожарная сигнализация"],
            main_entities=["ИП-1"],
        )],
    )

    unresolved_left = next(
        item for item in result["relations"] if item["left_pages"] == [3]
    )
    assert unresolved_left["status"] == "NO_MATCH"
    assert unresolved_left["relation_type"] == "UNCERTAIN"
    assert unresolved_left["right_pages"] == []
    assert unresolved_left["candidate_edges"][0]["right_page"] == 3
    assert unresolved_left["supported_edges"] == []


def test_duplicate_alternatives_do_not_infer_cardinality_from_titles_or_scores():
    common = {
        "functional_content": ["распределение питания"],
        "main_entities": ["QF1"],
        "topology": ["шины -> QF1"],
    }
    result = match_sheets(
        [_sheet(10, title="Схема", **common)],
        [
            _sheet(24, title="Схема часть 1", **common),
            _sheet(25, title="Схема часть 2", **common),
        ],
    )

    assert all(item["relation_type"] != "SPLIT" for item in result["relations"])
    matched = next(item for item in result["relations"] if item["left_pages"] == [10])
    assert matched["relation_type"] == "MATCHED"
    assert len(result["unmatched_right_pages"]) == 1
