from __future__ import annotations

import copy

from backend.app.services.stage_comparison import semantic_diff_v6a1 as semantic


def _word(text, x, y, block=0, line=0, word=0):
    return {"text": text, "bbox": [x, y, x + 8, y + 6], "block": block, "line": line, "word": word}


def _table(rows):
    words = []
    for row, values in enumerate(rows):
        for column, value in enumerate(values):
            if value:
                words.append(_word(value, column * 100 + 10, row * 30 + 10, line=row, word=column))
    return semantic.build_table_model(words, [0, 100, 200, 300], [0, 30, 60, 90])


def test_changed_value_inside_named_cell():
    left = _table([["Кабель", "Длина", "Марка"], ["К1", "150", "ВВГ"], ["К2", "80", "ВВГ"]])
    right = _table([["Кабель", "Длина", "Марка"], ["К1", "200", "ВВГ"], ["К2", "80", "ВВГ"]])
    result = semantic.compare_table_models(left, right)
    changed = next(item for item in result["changes"] if item["before"] == "150")
    assert changed["row_label"] == "К1"
    assert changed["column_label"] == "Длина"
    assert changed["after"] == "200"
    assert changed["evidence_level"] == "exact"


def test_inserted_table_row_does_not_shift_all_following_rows():
    left = _table([["Поз.", "Длина", "Марка"], ["К1", "150", "ВВГ"], ["К2", "80", "ВВГ"]])
    right = _table([["Поз.", "Длина", "Марка"], ["К0", "25", "ПВС"], ["К1", "150", "ВВГ"]])
    # Add a fourth physical row for K2 in the right model.
    right = semantic.build_table_model(
        [_word(v, c * 100 + 10, r * 30 + 10, line=r, word=c) for r, row in enumerate([
            ["Поз.", "Длина", "Марка"], ["К0", "25", "ПВС"], ["К1", "150", "ВВГ"], ["К2", "80", "ВВГ"]]) for c, v in enumerate(row)],
        [0, 100, 200, 300], [0, 30, 60, 90, 120])
    result = semantic.compare_table_models(left, right)
    assert any(row["label"] == "К0" for row in result["inserted_rows"])
    assert not any(change["row_label"] in {"К1", "К2"} for change in result["changes"])


def test_removed_table_row_is_explicit():
    left = _table([["Поз.", "Длина", "Марка"], ["К1", "150", "ВВГ"], ["К2", "80", "ВВГ"]])
    right = _table([["Поз.", "Длина", "Марка"], ["К1", "150", "ВВГ"], ["", "", ""]])
    result = semantic.compare_table_models(left, right)
    assert any(row["label"] == "К2" for row in result["removed_rows"])


def test_completely_different_tables_are_reported_as_replacement():
    left = _table([["Ведомость дверных проемов", "Высота", "Ширина"], ["Д1", "2100", "900"], ["Д2", "2100", "1000"]])
    right = _table([["Экспликация помещений", "Площадь", "Категория"], ["1.1", "23,4", "А"], ["1.2", "18,0", "Б"]])
    result = semantic.compare_table_models(left, right)
    assert result["table_replaced"] is True
    assert result["evidence_level"] == "strong"


def test_same_numbers_in_different_rows_use_labels():
    left = [
        {"value": "100", "normalized": semantic.normalize_number("100"), "bbox": [0, 0, 5, 5], "labels": ["К1"], "unit": "м", "context_key": "к1", "context_reliable": True},
        {"value": "100", "normalized": semantic.normalize_number("100"), "bbox": [0, 20, 5, 25], "labels": ["К2"], "unit": "м", "context_key": "к2", "context_reliable": True},
    ]
    right = [
        {**left[0], "value": "120", "normalized": semantic.normalize_number("120")},
        {**left[1]},
    ]
    result = semantic.match_numeric_contexts(left, right)
    assert [(item["label"], item["before"], item["after"]) for item in result] == [("К1", "100", "120")]


def test_repeated_labels_are_disambiguated_by_geometry():
    left = [
        {"value": "25", "normalized": semantic.normalize_number("25"), "bbox": [0, 0, 5, 5], "labels": ["QF"], "unit": "A", "context_key": "qf", "context_reliable": True},
        {"value": "40", "normalized": semantic.normalize_number("40"), "bbox": [0, 100, 5, 105], "labels": ["QF"], "unit": "A", "context_key": "qf", "context_reliable": True},
    ]
    right = [
        {**left[0], "value": "32", "normalized": semantic.normalize_number("32"), "bbox": [2, 1, 7, 6]},
        {**left[1]},
    ]
    result = semantic.match_numeric_contexts(left, right)
    assert len(result) == 1 and result[0]["before"] == "25" and result[0]["after"] == "32"


def test_28_and_2_8_remain_distinct_engineering_values():
    assert semantic.normalize_number("28")["canonical"] == "28"
    assert semantic.normalize_number("2.8")["canonical"] == "2.8"
    assert semantic.normalize_number("28")["numeric"] != semantic.normalize_number("2,8")["numeric"]


def test_entity_is_localized_by_label_text():
    blocks = [{"block_id": "plan", "entities": ["QF12", "QF99"]}]
    words = [_word("QF12", 10, 10)]
    result = semantic.localize_entities(blocks, words, [0, 0, 50, 50])
    assert next(item for item in result if item["entity"] == "QF12")["entity_location"] == "exact"


def test_entity_without_coordinates_or_text_anchor_is_uncertain():
    result = semantic.localize_entities([{"block_id": "plan", "entities": ["QF99"]}], [], [0, 0, 50, 50])
    assert result[0]["entity_location"] == "uncertain"
    assert result[0]["bbox"] is None


def test_small_group_does_not_localize_entity_elsewhere_in_big_block():
    blocks = [{"block_id": "large", "entities": ["QF12"]}]
    words = [_word("QF12", 900, 900)]
    result = semantic.localize_entities(blocks, words, [0, 0, 20, 20])
    assert result[0]["entity_location"] == "contextual"


def test_vector_only_has_honest_geometric_change():
    result = semantic._result_from_analysis(
        {"change_types": ["vector"]}, {}, {"applicable": False}, [], [], [],
        [{"kind": "vector", "change": "changed", "bbox": [1, 1, 2, 2]}])
    assert result["change_kind"] == "geometric_change"
    assert result["evidence_level"] == "insufficient"
    assert result["requires_human_review"] is True
    assert "инженерного смысла" in result["change_summary"]


def test_stamp_value_is_bound_to_column_label():
    left = [_word("№док.", 100, 30)]
    right = [_word("1388/26", 100, 12), _word("№док.", 100, 30)]
    result = semantic.stamp_field_changes(left, right, [90, 0, 140, 50])
    assert result[0]["field"] == "№док."
    assert result[0]["after"] == "1388/26"
    assert result[0]["evidence_level"] == "exact"


def test_no_evidence_is_insufficient():
    result = semantic._result_from_analysis({"change_types": ["text"]}, {}, {"applicable": False}, [], [], [], [])
    assert result["change_kind"] == "uncertain"
    assert result["evidence_level"] == "insufficient"


def test_deterministic_table_comparison_does_not_mutate_inputs():
    left = _table([["Поз.", "Длина", "Марка"], ["К1", "150", "ВВГ"], ["К2", "80", "ВВГ"]])
    right = _table([["Поз.", "Длина", "Марка"], ["К1", "200", "ВВГ"], ["К2", "80", "ВВГ"]])
    before = copy.deepcopy((left, right))
    assert semantic.compare_table_models(left, right) == semantic.compare_table_models(left, right)
    assert (left, right) == before
