"""Sheet identity from the drawing stamp: what it proves and what it refuses."""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison.sheet_identity import (
    EXTRACTOR_VERSION,
    SHEET_KINDS,
    covers_floors,
    identities_by_key,
    identity_from_dict,
    parse_stamp_title,
)


def _key(text: str) -> str | None:
    identity = parse_stamp_title(text)
    return identity.stamp_key if identity else None


def test_same_stamp_line_yields_the_same_key_across_both_documents():
    left = parse_stamp_title("Норм. контр. Корпуса 1, 2. План 3 этажа. М 1_200.", page=29)
    right = parse_stamp_title("Корпуса 1, 2. План 3 этажа. 12.25", page=8)

    assert left is not None and right is not None
    assert left.matches(right)
    assert left.buildings == ("1", "2")
    assert left.sheet_kind == "PLAN"
    assert left.floors == ("3",)
    assert left.page == 29 and right.page == 8


def test_different_floors_are_different_sheets():
    assert _key("Корпуса 1, 2. План 3 этажа") != _key("Корпуса 1, 2. План 4 этажа")


def test_different_buildings_are_different_sheets():
    assert _key("Корпус 1. План 1 этажа") != _key("Корпус 4. План 1 этажа")
    assert _key("Корпуса 1, 2. План кровли") != _key("Корпуса 3, 3.1. План кровли")


def test_roof_matches_roof_of_the_same_buildings():
    assert _key("Корпуса 3, 3.1. План кровли. М 1_200.") == _key(
        "Корпуса 3, 3.1. План кровли. 12.25"
    )


def test_building_list_normalizes_singular_plural_spacing_and_separators():
    variants = [
        "Корпуса 3, 3.1. План 2 этажа",
        "Корпуса  3 ,  3.1 . План 2 этажа",
        "Корпуса 3 и 3.1. План 2 этажа",
        "корпуса 3,3.1. план 2 этажа",
    ]
    assert len({_key(value) for value in variants}) == 1


def test_floor_range_expands_to_every_floor_it_covers():
    identity = parse_stamp_title("Корпус 4. План 3-15 этажей. М 1_200.")

    assert identity is not None
    assert identity.floor_range == (3, 15)
    assert identity.floors == tuple(str(value) for value in range(3, 16))
    assert identity.floor_set == frozenset(str(value) for value in range(3, 16))


@pytest.mark.parametrize("dash", ["-", "–", "—"])
def test_floor_range_accepts_every_dash_the_drawings_use(dash):
    assert _key(f"Корпус 4. План 3{dash}15 этажей") == _key("Корпус 4. План 3-15 этажей")


def test_floor_range_is_not_the_same_sheet_as_a_single_floor_inside_it():
    container = parse_stamp_title("Корпус 4. План 3-15 этажей")
    member = parse_stamp_title("Корпус 4. План 7 этажа")

    assert container is not None and member is not None
    assert not container.matches(member)
    # It is a grouping candidate for the 1->N / N->1 contract, not a pair.
    assert covers_floors(container, member)
    assert not covers_floors(member, container)


def test_floor_range_does_not_cover_another_building():
    container = parse_stamp_title("Корпус 4. План 3-15 этажей")
    other = parse_stamp_title("Корпуса 1, 2. План 7 этажа")

    assert not covers_floors(container, other)


def test_underground_levels_are_told_apart_by_their_ordinal():
    first = parse_stamp_title("План первого подземного этажа на отм. -6,000. М 1_200")
    second = parse_stamp_title("План второго подземного этажа на отм. -9,600. М 1_200")

    assert first is not None and second is not None
    assert first.sheet_kind == second.sheet_kind == "BASEMENT"
    assert first.basement_ordinal == 1 and second.basement_ordinal == 2
    assert not first.matches(second)


def test_elevation_survives_the_decimal_separator_the_two_sides_disagree_on():
    left = parse_stamp_title("План первого подземного этажа на отм. -6,000. М 1_200")
    right = parse_stamp_title("План первого подземного этажа на отм.-6.000. 12.25")

    assert left is not None and right is not None
    assert left.matches(right)
    assert left.elevation == right.elevation == "-6"


def test_technical_space_is_its_own_kind():
    identity = parse_stamp_title("План технического пространства на отм. -1,800. М1_200")

    assert identity is not None
    assert identity.sheet_kind == "TECHNICAL_SPACE"
    assert identity.sheet_kind in SHEET_KINDS
    assert not identity.matches(
        parse_stamp_title("План первого подземного этажа на отм. -6,000")
    )


def test_prose_mentioning_a_plan_is_not_a_stamp_title():
    # A real sheet note: «ПРИМЕЧАНИЕ: 1. Типовой этаж представлен планом 3 этажа.»
    assert parse_stamp_title("ПРИМЕЧАНИЕ: 1. Типовой этаж представлен планом 3 этажа.") is None
    assert parse_stamp_title("Часть 1. Архитектурные решения. Планы") is None
    assert parse_stamp_title("") is None
    assert parse_stamp_title("Изм. Кол.уч. Лист №док. Подп. Дата") is None


def test_absurd_floor_range_is_refused_rather_than_invented():
    assert parse_stamp_title("Корпус 4. План 15-3 этажей") is None
    assert parse_stamp_title("Корпус 4. План 1-9999 этажей") is None


def test_identity_round_trips_through_its_persisted_form():
    identity = parse_stamp_title("Корпус 4. План 3-15 этажей. М 1_200.", page=42)
    assert identity is not None

    restored = identity_from_dict(identity.to_dict())

    assert restored is not None
    assert restored.stamp_key == identity.stamp_key
    assert restored.floor_range == identity.floor_range
    assert restored.floors == identity.floors
    assert restored.page == 42
    assert identity.to_dict()["extractor"] == EXTRACTOR_VERSION


def test_unusable_persisted_identity_is_dropped_instead_of_guessed():
    assert identity_from_dict(None) is None
    assert identity_from_dict({}) is None
    assert identity_from_dict({"page": 1, "sheet_kind": "SOMETHING_ELSE"}) is None
    assert identity_from_dict({"page": "3", "sheet_kind": "PLAN"}) is None


def test_identities_group_by_key_so_ambiguity_is_visible():
    identities = [
        parse_stamp_title("Корпуса 1, 2. План 3 этажа", page=10),
        parse_stamp_title("Корпуса 1, 2. План 3 этажа", page=11),
        parse_stamp_title("Корпуса 1, 2. План 4 этажа", page=12),
    ]

    grouped = identities_by_key([item for item in identities if item])

    assert len(grouped) == 2
    duplicated = grouped[identities[0].stamp_key]
    assert [item.page for item in duplicated] == [10, 11]


def test_technical_spaces_at_different_elevations_are_different_sheets():
    # Both sheets are «План технического пространства» of the same buildings.
    # Neither has floors, an underground ordinal or an axis, so before the
    # elevation entered the key they collided into one exact match — and the
    # comparison then diffed two different levels as if they were one sheet.
    low = parse_stamp_title("Корпус 1. План технического пространства на отм. -1,800")
    high = parse_stamp_title("Корпус 1. План технического пространства на отм. -5,400")

    assert low is not None and high is not None
    assert low.sheet_kind == high.sheet_kind == "TECHNICAL_SPACE"
    assert low.elevation == "-1.8" and high.elevation == "-5.4"
    assert not low.matches(high)
    assert low.stamp_key != high.stamp_key


def test_technical_space_at_the_same_elevation_still_matches_across_sides():
    left = parse_stamp_title("Корпус 1. План технического пространства на отм. -1,800")
    right = parse_stamp_title("Корпус 1. План технического пространства на отм.-1.800")

    assert left is not None and right is not None
    assert left.matches(right)


def test_elevation_is_read_without_the_otm_prefix():
    # Real stamps write the level both ways; «-1.800» alone is still a level.
    identity = parse_stamp_title("Корпус 1. План технического пространства -1.800")

    assert identity is not None
    assert identity.elevation == "-1.8"
    assert identity.stamp_key.endswith("|E=-1.8")


@pytest.mark.parametrize("written", ["+0,000", "+0.000", "-0,000", "-0.000"])
def test_zero_level_is_one_level_however_its_sign_is_written(written):
    identity = parse_stamp_title(
        f"Корпус 1. План технического пространства на отм. {written}"
    )

    assert identity is not None
    assert identity.elevation == "0"


def test_a_scale_or_a_date_on_the_stamp_is_never_read_as_a_level():
    for text in (
        "Корпуса 1, 2. План 3 этажа. М 1_200. 12.25",
        "Корпуса 1, 2. План кровли. М1:200",
    ):
        identity = parse_stamp_title(text)
        assert identity is not None
        assert identity.elevation is None


def test_elevation_does_not_split_kinds_that_identify_themselves_otherwise():
    # A PLAN is told apart by its floors.  One side spelling the level and the
    # other not must not turn a proven pair into two different sheets.
    with_level = parse_stamp_title("Корпус 1. План 3 этажа на отм. +9,000")
    without_level = parse_stamp_title("Корпус 1. План 3 этажа")

    assert with_level is not None and without_level is not None
    assert with_level.elevation == "9" and without_level.elevation is None
    assert with_level.matches(without_level)


def test_unnamed_technical_space_level_is_not_declared_equal_to_a_named_one():
    named = parse_stamp_title("Корпус 1. План технического пространства на отм. -1,800")
    unnamed = parse_stamp_title("Корпус 1. План технического пространства")

    assert named is not None and unnamed is not None
    # Fail-closed: «the same sheet» is a claim, and nothing here proves it.
    assert not named.matches(unnamed)
