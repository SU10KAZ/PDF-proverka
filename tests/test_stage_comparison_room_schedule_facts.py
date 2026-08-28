"""«Экспликация помещений» as structured facts — and what stays unresolved.

The architectural room schedule names its own columns, and unlike the
electrical load table its header has exactly as many cells as its data rows.
So the column meanings are read from the document.  Everything here is about
where that proof holds and where it must refuse to guess.
"""
from __future__ import annotations

from backend.app.services.stage_comparison.production_text_flow import (
    PREPARATION_KIND,
    PREPARATION_SCHEMA_VERSION,
    build_text_differences_from_preparation,
)
from backend.app.services.stage_comparison.text_fact_producer import produce_text_facts


def _row(fragment_id: str, side: str, parts: list[str], *, order: int) -> dict:
    return {
        "id": fragment_id,
        "stage": "stage_1" if side == "left" else "stage_2",
        "pdf_page": 10 if side == "left" else 24,
        "text": " ".join(parts),
        "canonical_text": " ".join(parts).casefold(),
        "source_block_id": f"{side}-block",
        "source_kind": "table_row",
        "source_group": f"{side}-block:table",
        "location_parts": list(parts),
        "order": order,
        "bboxes": [{"x": .1, "y": .2, "width": .3, "height": .04}],
    }


def _header(side: str, *, order: int = 1, width: int = 3) -> dict:
    parts = ["Номер помещения", "Наименование", "Площадь, м2"]
    if width == 4:
        parts.append("Кат. помещения")
    return _row(f"{side}-header", side, parts, order=order)


def _preparation(left: list[dict], right: list[dict]) -> dict:
    return {
        "kind": PREPARATION_KIND,
        "schema_version": PREPARATION_SCHEMA_VERSION,
        "version": 1,
        "pair_id": "pair-1",
        "input_signature": "prepared-input",
        "comparison_groups": [{
            "id": "group-1",
            "left_pages": [10],
            "right_pages": [24],
            "relation_type": "MATCHED",
            "relation_status": "HIGH",
        }],
        "fragments": {"left": left, "right": right},
    }


def _facts(left: list[dict], right: list[dict]) -> dict:
    preparation = _preparation(left, right)
    differences = build_text_differences_from_preparation(
        preparation, generated_at="fixed"
    )
    return produce_text_facts(differences, preparation, generated_at="fixed")


def _by_facet(result: dict) -> dict[str, dict]:
    return {fact["facet_ref"]: fact for fact in result["facts"]}


def test_a_proven_header_turns_a_changed_room_row_into_one_fact_per_column():
    result = _facts(
        [_header("left"), _row("l1", "left", ["28.1", "Холл", "15,71"], order=2)],
        [_header("right"), _row("r1", "right", ["28.1", "Холл", "16,20"], order=2)],
    )

    facts = _by_facet(result)
    assert set(facts) == {"room_area_m2"}
    area = facts["room_area_m2"]
    assert area["before_value"] == "15.71 м²"
    assert area["after_value"] == "16.20 м²"
    assert area["dimension"] == "PARAMETER"
    assert area["direction"] == "INCREASED"
    assert area["outcome"] == "MATERIAL_CHANGE"
    assert area["provenance"]["parser_rule"] == "recognized_room_schedule_table"
    assert result["diagnostics"]["recognized_room_schedule_tables"] == 2
    assert result["diagnostics"]["recognized_electrical_tables"] == 0
    assert result["diagnostics"]["uses_model"] is False


def test_a_renamed_room_is_a_type_fact_and_an_area_fact_at_once():
    result = _facts(
        [_header("left"), _row("l1", "left", ["48.1", "Холл", "6,36"], order=2)],
        [_header("right"), _row("r1", "right", ["48.1", "Гардеробная", "7,10"], order=2)],
    )

    facts = _by_facet(result)
    assert set(facts) == {"room_name", "room_area_m2"}
    assert facts["room_name"]["before_value"] == "холл"
    assert facts["room_name"]["after_value"] == "гардеробная"
    assert facts["room_name"]["dimension"] == "TYPE"


def test_the_category_column_may_be_missing_from_the_tail_of_a_row():
    result = _facts(
        [
            _header("left", width=4),
            _row("l1", "left", ["01.1", "Рампа", "288.62 м2", "В2"], order=2),
            _row("l2", "left", ["01.2", "Коридор", "17.85 м2"], order=3),
        ],
        [
            _header("right", width=4),
            _row("r1", "right", ["01.1", "Рампа", "290.00 м2", "В2"], order=2),
            _row("r2", "right", ["01.2", "Коридор", "18.40 м2"], order=3),
        ],
    )

    areas = sorted(
        fact["before_value"] for fact in result["facts"]
        if fact["facet_ref"] == "room_area_m2"
    )
    # The inline «м2» of one side and the bare number of the other end up
    # in the same normalized form, so an unchanged area stays unchanged.
    assert areas == ["17.85 м²", "288.62 м²"]
    # The unit is normalized away from the value, exactly as the header says.
    assert all(
        fact["after_value"].endswith(" м²") and "м2 м²" not in fact["after_value"]
        for fact in result["facts"] if fact["facet_ref"] == "room_area_m2"
    )


def test_a_changed_fire_category_is_a_fact_of_its_own():
    result = _facts(
        [_header("left", width=4), _row("l1", "left", ["01.1", "Рампа", "288.62", "В2"], order=2)],
        [_header("right", width=4), _row("r1", "right", ["01.1", "Рампа", "288.62", "В3"], order=2)],
    )

    facts = _by_facet(result)
    assert set(facts) == {"room_fire_category"}
    assert facts["room_fire_category"]["before_value"] == "в2"
    assert facts["room_fire_category"]["after_value"] == "в3"


# --- where the rule must refuse -------------------------------------------


def test_without_a_header_the_same_rows_stay_unresolved():
    result = _facts(
        [_row("l1", "left", ["28.1", "Холл", "15,71"], order=2)],
        [_row("r1", "right", ["28.1", "Холл", "16,20"], order=2)],
    )

    assert result["facts"] == []
    assert result["diagnostics"]["recognized_room_schedule_tables"] == 0


def test_two_rows_glued_into_one_are_refused_rather_than_misread():
    # «02.1 Рампа 2019,94 B2 02.42а Кладовая 6,18»: taking the last number as
    # the area of room 02.1 would be wrong by a factor of 327.
    glued = ["02.1", "Рампа", "2019,94", "В2", "02.42а", "Кладовая", "6,18"]
    result = _facts(
        [_header("left", width=4), _row("l1", "left", glued, order=2)],
        [_header("right", width=4), _row("r1", "right", [*glued[:2], "2100,00", *glued[3:]], order=2)],
    )

    assert result["facts"] == []


def test_a_two_up_header_does_not_prove_a_table():
    doubled = [
        "Номер помещения", "Наименование", "Площадь, м2", "Кат. помещения",
        "Номер помещения", "Наименование", "Площадь, м2", "Кат. помещения",
    ]
    result = _facts(
        [_row("l-header", "left", doubled, order=1),
         _row("l1", "left", ["28.1", "Холл", "15,71"], order=2)],
        [_row("r-header", "right", doubled, order=1),
         _row("r1", "right", ["28.1", "Холл", "16,20"], order=2)],
    )

    assert result["facts"] == []


def test_two_different_header_widths_in_one_table_prove_nothing():
    result = _facts(
        [
            _header("left", order=1, width=3),
            _header("left", order=3, width=4) | {"id": "left-header-2"},
            _row("l1", "left", ["28.1", "Холл", "15,71"], order=2),
        ],
        [_header("right"), _row("r1", "right", ["28.1", "Холл", "16,20"], order=2)],
    )

    assert result["facts"] == []


def test_a_bare_integer_is_not_a_room_number():
    # A room number and an area are lexically identical once the dot is gone,
    # so a cell without one is refused rather than read as a subject.
    result = _facts(
        [_header("left"), _row("l1", "left", ["263", "Техническое пространство", "260.85 м2"], order=2)],
        [_header("right"), _row("r1", "right", ["263", "Техническое пространство", "265.00 м2"], order=2)],
    )

    assert result["facts"] == []


def test_a_numeric_name_column_is_refused():
    result = _facts(
        [_header("left"), _row("l1", "left", ["28.1", "15,71", "16,20"], order=2)],
        [_header("right"), _row("r1", "right", ["28.1", "15,80", "16,20"], order=2)],
    )

    assert result["facts"] == []


def test_a_roof_build_up_row_is_not_a_room_and_is_left_alone():
    pie = ["Щебень фракции 5-20мм", "-перем."]
    result = _facts(
        [_header("left"), _row("l1", "left", pie, order=2)],
        [_header("right"), _row("r1", "right", ["Щебень фракции 5-20мм", "-150 мм"], order=2)],
    )

    assert result["facts"] == []


def test_the_header_row_itself_is_recorded_as_not_a_fact():
    result = _facts(
        [_header("left"), _row("l1", "left", ["28.1", "Холл", "15,71"], order=2)],
        [_header("right", width=4), _row("r1", "right", ["28.1", "Холл", "15,71"], order=2)],
    )

    reasons = {
        item["reason_code"]
        for item in result["not_applicable_source_evidence"]
    }
    assert "room_schedule_header_not_a_fact" in reasons
    assert not any(
        fact["subject_ref"].endswith("номер помещения") for fact in result["facts"]
    )


def test_different_rooms_that_share_a_name_never_become_one_change():
    # Stage 3 pairs «1.9 с/у 6.95» with «1.8 с/у 4.38» by text similarity; the
    # subject check must keep them two separate facts, not one 2.57 m² change.
    result = _facts(
        [_header("left"), _row("l1", "left", ["1.9", "С/у", "6,95"], order=2)],
        [_header("right"), _row("r1", "right", ["1.8", "С/у", "4,38"], order=2)],
    )

    directions = {fact["direction"] for fact in result["facts"]}
    assert directions <= {"ADDED", "REMOVED"}
    assert not any(fact["direction"] in {"INCREASED", "DECREASED"} for fact in result["facts"])
