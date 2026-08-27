from __future__ import annotations

from backend.app.services.stage_comparison.production_text_flow import (
    PREPARATION_KIND,
    PREPARATION_SCHEMA_VERSION,
    build_text_differences_from_preparation,
    normalize_comparison_groups,
)


def _fragment(fragment_id, page, text, bbox):
    return {
        "id": fragment_id,
        "pdf_page": page,
        "text": text,
        "canonical_text": text.casefold(),
        "source_block_id": "block-1",
        "source_kind": "table_row",
        "order": 1,
        "bboxes": [bbox],
    }


def _preparation(left, right, groups):
    return {
        "kind": PREPARATION_KIND,
        "schema_version": PREPARATION_SCHEMA_VERSION,
        "version": 1,
        "pair_id": "pair-1",
        "input_signature": "prepared-input",
        "comparison_groups": groups,
        "fragments": {"left": left, "right": right},
    }


def test_page_to_page_runs_without_sheet_matcher_or_parent_gate():
    group = {"left_pages": [52], "right_pages": [21], "relation_type": "USER_SELECTED"}
    stage3 = build_text_differences_from_preparation(_preparation(
        [_fragment("left-voltage", 52, "Напряжение 220 В", {"x": .1, "y": .2, "width": .3, "height": .04})],
        [_fragment("right-voltage", 21, "Напряжение 380 В", {"x": .2, "y": .3, "width": .3, "height": .04})],
        [group],
    ))

    assert stage3["sheet_groups"][0]["left_pages"] == [52]
    assert stage3["sheet_groups"][0]["right_pages"] == [21]
    assert stage3["summary"]["changed"] == 1
    assert stage3["constraints"]["sheet_matcher_is_gate"] is False
    assert stage3["constraints"]["parent_relation_required"] is False


def test_one_to_many_group_is_compared_as_one_scope():
    group = {"left_pages": [10], "right_pages": [24, 25], "relation_type": "SPLIT"}
    stage3 = build_text_differences_from_preparation(_preparation(
        [_fragment("left-1", 10, "QS1", {})],
        [
            _fragment("right-1", 24, "QF3", {}),
            _fragment("right-2", 25, "Дополнение", {}),
        ],
        [group],
    ))

    result = stage3["sheet_groups"][0]
    assert result["relation_type"] == "SPLIT"
    assert result["right_pages"] == [24, 25]
    assert {page for item in result["added"] for page in item["right_pages"]} == {24, 25}
    assert len(result["removed"]) == 1


def test_group_identity_is_order_independent():
    first = normalize_comparison_groups([{
        "left_pages": [11, 10], "right_pages": [25, 24], "relation_type": "UNCERTAIN",
    }])
    second = normalize_comparison_groups([{
        "left_pages": [10, 11], "right_pages": [24, 25], "relation_type": "UNCERTAIN",
    }])

    assert first == second
