from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import text_differences as td


def fragment(fragment_id: str, text: str, page: int = 1) -> dict:
    return {
        "id": fragment_id,
        "text": text,
        "canonical_text": text.lower(),
        "pdf_page": page,
        "bboxes": [{"x": 0.1, "y": 0.2, "width": 0.3, "height": 0.04}],
    }


def exclusions(*, left_excluded=(), right_excluded=()) -> dict:
    return {
        "valid": True,
        "source_signature": "stage2-signature",
        "contract_sha256": "contract-sha",
        "excluded_fragment_ids": {
            "left": list(left_excluded), "right": list(right_excluded),
        },
    }


def build(
    left: list[dict], right: list[dict], links: list[dict] | None = None,
    *, remaining_left: list[str] | None = None,
    remaining_right: list[str] | None = None,
    exclusion_payload: dict | None = None,
) -> dict:
    return td.build_text_differences(
        pair_id="pair",
        generated_at="2026-08-21T00:00:00+00:00",
        exclusions=exclusion_payload or exclusions(),
        comparison={
            "fragments": {"left": left, "right": right},
            "remaining": {
                "left": remaining_left if remaining_left is not None else [x["id"] for x in left],
                "right": remaining_right if remaining_right is not None else [x["id"] for x in right],
            },
        },
        links=links or [{"id": "link", "left_pages": [1], "right_pages": [1]}],
        labels={"left": {1: "Лист П-1"}, "right": {1: "Лист РД-1"}},
    )


def test_exact_changed_by_stable_key():
    result = td.compare_group(
        [fragment("l1", "101.35 БКТ 153,82 м²")],
        [fragment("r1", "101.35 БКТ 150,23 м²")],
    )
    assert len(result["changed"]) == 1
    assert result["changed"][0]["key"] == "101.35"
    assert result["changed"][0]["left_fragment_ids"] == ["l1"]
    assert result["changed"][0]["right_fragment_ids"] == ["r1"]


def test_numerical_value_change_is_concise_and_preserved():
    result = td.compare_group(
        [fragment("l1", "101.35 БКТ 153,82 м²")],
        [fragment("r1", "101.35 БКТ 150,23 м²")],
    )
    summary = result["changed"][0]["summary"]
    assert "153,82" in summary and "150,23" in summary and "→" in summary
    assert result["changed"][0]["before"] == "101.35 БКТ 153,82 м²"
    assert result["changed"][0]["after"] == "101.35 БКТ 150,23 м²"


def test_material_change_is_changed_not_removed_added():
    result = td.compare_group(
        [fragment("l1", "Перегородка из газобетонных блоков толщиной 100 мм")],
        [fragment("r1", "Перегородка из газобетонных блоков толщиной 200 мм")],
    )
    assert len(result["changed"]) == 1
    assert result["removed"] == []
    assert result["added"] == []


def test_removed_fragment():
    result = td.compare_group([fragment("l1", "101.36 ПУИ 4,15 м²")], [])
    assert [item["left_fragment_ids"] for item in result["removed"]] == [["l1"]]
    assert result["changed"] == [] and result["added"] == []


def test_added_fragment():
    result = td.compare_group([], [fragment("r1", "101.40 Электрощитовая 12,4 м²")])
    assert [item["right_fragment_ids"] for item in result["added"]] == [["r1"]]
    assert result["changed"] == [] and result["removed"] == []


def test_unrelated_removed_and_added_are_not_false_changed():
    result = td.compare_group(
        [fragment("l1", "Увеличение этажности корпуса 4 с 10 до 16 этажей")],
        [fragment("r1", "Увеличение высоты оконных проемов с 2,85 до 3,10 м")],
    )
    assert result["changed"] == []
    assert len(result["removed"]) == len(result["added"]) == 1


def test_table_rows_use_first_column_key():
    result = td.compare_group(
        [fragment("l1", "ЩР-1 | автомат 16 А | 3 шт.")],
        [fragment("r1", "ЩР-1 | автомат 20 А | 4 шт.")],
    )
    assert len(result["changed"]) == 1
    assert result["changed"][0]["key"] == "щр-1"


def test_already_matched_fragment_is_excluded_from_stage3_input():
    left = [fragment("same-left", "Совпавший текст"), fragment("remaining-left", "Удалено")]
    result = build(
        left, [], remaining_left=["remaining-left"],
        exclusion_payload=exclusions(left_excluded=["same-left"]),
    )
    ids = [item["left_fragment_ids"] for item in result["sheet_groups"][0]["removed"]]
    assert ids == [["remaining-left"]]


def test_found_elsewhere_fragment_is_excluded_from_stage3_input():
    right = [fragment("elsewhere-right", "Найден на другом листе"), fragment("remaining-right", "Добавлено")]
    result = build(
        [], right, remaining_right=["remaining-right"],
        exclusion_payload=exclusions(right_excluded=["elsewhere-right"]),
    )
    ids = [item["right_fragment_ids"] for item in result["sheet_groups"][0]["added"]]
    assert ids == [["remaining-right"]]


def test_remaining_may_not_overlap_excluded_contract():
    with pytest.raises(ValueError, match="remaining_contains_excluded_fragment"):
        build(
            [fragment("l1", "text")], [],
            exclusion_payload=exclusions(left_excluded=["l1"]),
        )


def test_strict_model_json_accepts_verbatim_provenance():
    left = [fragment("l1", "Толщина 100 мм")]
    right = [fragment("r1", "Толщина 200 мм")]
    payload = {
        "changed": [{
            "left_ids": ["l1"], "right_ids": ["r1"],
            "summary": "Толщина: 100 → 200 мм.",
            "before": "Толщина 100 мм", "after": "Толщина 200 мм",
        }],
        "removed": [], "added": [],
    }
    assert td.validate_model_response(
        payload, left_fragments=left, right_fragments=right
    ) == payload


def test_invalid_model_response_fails_closed():
    with pytest.raises(ValueError, match="invalid_model_response"):
        td.validate_model_response(
            {"changed": "markdown", "removed": [], "added": []},
            left_fragments=[], right_fragments=[],
        )


def test_model_cannot_hallucinate_fragment_ids():
    payload = {
        "changed": [],
        "removed": [{"left_ids": ["invented"], "summary": "x", "before": "x"}],
        "added": [],
    }
    with pytest.raises(ValueError, match="hallucinated_fragment_id"):
        td.validate_model_response(
            payload, left_fragments=[fragment("l1", "x")], right_fragments=[]
        )


def test_many_to_many_pages_form_one_group():
    left = [fragment("l5", "5.А.1 10 м²", 5), fragment("l6", "6.А.1 20 м²", 6)]
    right = [fragment("r10", "5.А.1 11 м²", 10), fragment("r11", "6.А.1 21 м²", 11)]
    links = [{"id": "many", "left_pages": [5, 6], "right_pages": [10, 11]}]
    result = build(left, right, links)
    assert len(result["sheet_groups"]) == 1
    assert result["sheet_groups"][0]["left_pages"] == [5, 6]
    assert result["sheet_groups"][0]["right_pages"] == [10, 11]
    assert len(result["sheet_groups"][0]["changed"]) == 2


def test_no_differences_produces_no_discrepancy_row():
    result = build(
        [fragment("l1", "У2 — плотность ≥130 кг/м³ — 150 мм")],
        [fragment("r1", r"У2 - плотность \geq 130 \text{ кг/м}^3 - 150 мм")],
    )
    assert result["sheet_groups"] == []
    assert result["summary"]["sheet_groups_with_differences"] == 0


def test_exact_pair_provenance_is_available_to_stage4_reviewer():
    result = td.compare_group(
        [fragment("l1", "Одинаковый смысл")],
        [fragment("r1", "Одинаковый смысл", 2)],
    )
    assert result["exact_equivalents"] == 1
    assert result["same"][0]["left_fragment_ids"] == ["l1"]
    assert result["same"][0]["right_fragment_ids"] == ["r1"]


def test_embedded_graphic_ocr_description_does_not_create_a_text_difference():
    result = build(
        [fragment(
            "l1",
            r"У1 [50] [thin line] - Утепление, плотностью \geq 130 \text{ кг/м}^3 - 50мм",
        )],
        [fragment("r1", "У1 ; 50; - Утепление, плотностью ≥130кг/м3 - 50мм")],
    )
    assert result["sheet_groups"] == []


def test_numeric_summary_does_not_truncate_values_before_units():
    result = td.compare_group(
        [fragment("l1", "У1 - плотность 130кг/м3 - 50мм")],
        [fragment("r1", "У1 - плотность 140кг/м3 - 50мм")],
    )
    summary = result["changed"][0]["summary"]
    assert "130 → 140" in summary
    assert "13 → 14" not in summary


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("13АВ-РД-АР2-К7", "13АВ-РД-АР2-K7"),
        ("раздел 13АВ-РД-КЖ", "раздел 1ЗАВ-РД-КЖ"),
        (
            "У3 - Заполнение деформационных швов из минеральной ваты",
            "УЗ - Заполнение деформационных швов из минеральной ваты",
        ),
    ],
)
def test_safe_ocr_confusables_are_formatting_only(left: str, right: str):
    assert td.canonicalize(left) == td.canonicalize(right)


def test_rerun_is_deterministic():
    left = [fragment("l1", "101.35 БКТ 153,82")]
    right = [fragment("r1", "101.35 БКТ 150,23")]
    assert td.compare_group(left, right) == td.compare_group(left, right)
    contract = exclusions()
    assert td.source_signature(contract) == td.source_signature(contract)


def test_one_row_per_sheet_group_not_per_difference():
    left = [fragment("l1", "101.35 БКТ 153,82"), fragment("l2", "101.36 ПУИ 4,15")]
    right = [fragment("r1", "101.35 БКТ 150,23"), fragment("r2", "101.40 Щитовая 12,4")]
    result = build(left, right)
    assert len(result["sheet_groups"]) == 1
    group = result["sheet_groups"][0]
    assert len(group["changed"]) + len(group["removed"]) + len(group["added"]) == 3
    assert result["constraints"]["one_row_per_sheet_group"] is True


def test_graphic_descriptions_are_not_analyzed_as_text():
    result = build(
        [fragment("graphic", "The image contains three handwritten signatures in purple ink.")],
        [],
    )
    assert result["sheet_groups"] == []
    assert result["constraints"]["graphics_analyzed"] is False
