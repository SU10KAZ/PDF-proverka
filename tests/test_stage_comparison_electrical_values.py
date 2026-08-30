"""Разбор и сравнение электротехнических значений однолинейной схемы."""
from __future__ import annotations

import pytest

from backend.app.services.common import electrical_values as values


# ── Омоглифы ──────────────────────────────────────────────────────────────

def test_cyrillic_and_latin_spelling_is_one_mark():
    """«ППГнг(А)-НF» с кириллической Н и латинской H — одна марка.

    Оба листа выпущены из одного CAD, и раскладка буквы «Н» — свойство
    шрифта, а не проекта. Побайтовое сравнение объявило бы здесь изменение
    марки кабеля, которого не было.
    """
    cyrillic, latin = "ППГнг(А)-НF", "ППГнг(А)-HF"
    assert cyrillic != latin
    assert values.marks_equal(cyrillic, latin)
    assert values.compare_cables(cyrillic, latin) == []


def test_different_marks_are_not_folded_together():
    """Нормализация не имеет права сблизить «ВВГ» и «ППГ»."""
    assert not values.marks_equal("ВВГнг-LS", "ППГнг(А)-HF")


def test_visually_distinct_letters_are_not_folded():
    """«И» и «N» похожи, но различимы — их складывать нельзя."""
    assert not values.marks_equal("ИЗОЛ", "NЗОЛ")


# ── Разбор кабеля ─────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "text,parallel,cores,section",
    [
        ("5x120", 1, 5, 120.0),
        ("5х120", 1, 5, 120.0),
        ("5×120", 1, 5, 120.0),
        ("3x(5x120)", 3, 5, 120.0),
        ("2х(5х120)", 2, 5, 120.0),
        ("3×5×150", 3, 5, 150.0),
        ("4x2,5", 1, 4, 2.5),
    ],
)
def test_cable_forms_are_read_into_structure(text, parallel, cores, section):
    parsed = values.parse_cable(text)
    assert parsed["parallel_count"] == parallel
    assert parsed["cores"] == cores
    assert parsed["section_mm2"] == section


def test_leading_multiplier_before_a_mark_is_parallel_count():
    parsed = values.parse_cable("3хППГнг(А)-HF")
    assert parsed["parallel_count"] == 3
    assert parsed["parallel_count_proven"] is True
    assert parsed["mark"] == "ППГнг(А)-HF"


def test_mark_is_returned_in_original_spelling():
    """Сравнивается «скелет», а показывается то, что написано в документе."""
    parsed = values.parse_cable("ППГнг(А)-НF")
    assert parsed["mark"] == "ППГнг(А)-НF"
    assert parsed["mark_canonical"] == "ППГНГ(A)-HF"


def test_mark_and_dimensions_in_one_string():
    parsed = values.parse_cable("ППГнг(А)-НF 2х(5х120)")
    assert parsed["mark"] == "ППГнг(А)-НF"
    assert (parsed["parallel_count"], parsed["cores"], parsed["section_mm2"]) == (
        2, 5, 120.0,
    )


def test_two_mark_candidates_yield_no_mark():
    """Обозначение линии рядом с маркой — не повод выбрать одно из двух.

    В нативном тексте строка выглядит как «1ГРЩ-ВРУ4 ППГнг(А)-НF 2х(5х120)».
    Размеры прочитаны, а марка — нет, и выдумывать её нельзя.
    """
    parsed = values.parse_cable("1ГРЩ-ВРУ4 ППГнг(А)-НF 2х(5х120)")
    assert parsed["mark"] is None
    assert parsed["section_mm2"] == 120.0


def test_text_without_a_cable_is_not_a_cable():
    assert values.parse_cable("") is None
    assert values.parse_cable("   ") is None


# ── Сравнение кабелей ─────────────────────────────────────────────────────

def test_section_change_keeps_parallel_count():
    """«3x(5x120) → 3x5x150» — изменение сечения, а не строковый diff."""
    changes = values.compare_cables("3x(5x120)", "3х5х150")
    assert [item["facet"] for item in changes] == ["section_mm2"]
    assert (changes[0]["before"], changes[0]["after"]) == (120.0, 150.0)
    assert changes[0]["status"] == values.PROVEN


def test_proven_parallel_count_change():
    changes = values.compare_cables("2х(5х120)", "3х(5х120)")
    assert [item["facet"] for item in changes] == ["parallel_count"]
    assert changes[0]["status"] == values.PROVEN


def test_implicit_single_cable_goes_to_review_not_to_a_fact():
    """Единица без множителя — умолчание разбора, а не прочитанное значение."""
    changes = values.compare_cables("ППГнг(А)-НF", "3хППГнг(А)-HF")
    assert [item["facet"] for item in changes] == ["parallel_count"]
    assert changes[0]["status"] == values.REVIEW


def test_unreadable_side_is_not_a_change():
    """Непрочитанное сечение не доказывает, что сечение изменилось."""
    assert values.compare_cables("ППГнг(А)-НF", "ППГнг(А)-HF 5х120") == []


def test_mark_change_is_reported_with_original_spellings():
    changes = values.compare_cables("ВВГнг-LS 5х10", "ППГнг(А)-HF 5х10")
    assert [item["facet"] for item in changes] == ["mark"]
    assert changes[0]["before"] == "ВВГнг-LS"
    assert changes[0]["after"] == "ППГнг(А)-HF"


# ── Числа и списки ────────────────────────────────────────────────────────

def test_numeric_change_reports_direction():
    change = values.numeric_change(2500, 3200)
    assert change["direction"] == "INCREASED"
    assert change["delta"] == 700
    assert values.numeric_change(2500, 2500) is None
    assert values.numeric_change(None, 3200) is None


def test_cable_lists_differing_only_in_spelling_are_equivalent():
    assert values.cables_equivalent(["ППГнг(А)-НF"], ["ППГнг(А)-HF"])
    assert not values.cables_equivalent(["ППГнг(А)-НF"], ["ВВГнг-LS"])


def test_attributes_differ_ignores_spelling_but_not_meaning():
    assert not values.attributes_differ("ППГнг(А)-НF", "ППГнг(А)-HF")
    assert values.attributes_differ(2500, 3200)
    assert not values.attributes_differ(["ППГнг(А)-НF"], ["ППГнг(А)-HF"])
