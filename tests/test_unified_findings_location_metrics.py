"""reserc.md #59 — счётчики локации change'ей в summary unified_findings.

Раньше not_found-резолв локации был тихим: нельзя было увидеть долю изменений
без листа/страницы. Добавлены by_location_method / changes_without_page +
warning при высокой доле not_found.
"""
from __future__ import annotations

from backend.app.services.stage_comparison import unified_findings as uf


def test_empty_summary_has_location_fields():
    s = uf._empty_summary()
    assert s["by_location_method"] == {}
    assert s["changes_without_page"] == 0
    assert s["location_warnings"] == []


def test_no_changes_no_warnings():
    assert uf._compute_location_warnings({"total_changes": 0}) == []


def test_high_not_found_warns():
    s = {"total_changes": 10, "by_location_method": {"not_found": 6, "resolved": 4},
         "changes_without_page": 0}
    warns = uf._compute_location_warnings(s)
    assert any("not_found 6/10" in w for w in warns)


def test_high_changes_without_page_warns():
    s = {"total_changes": 8, "by_location_method": {"resolved": 8},
         "changes_without_page": 5}
    warns = uf._compute_location_warnings(s)
    assert any("без страницы: 5/8" in w for w in warns)


def test_low_not_found_no_warning():
    s = {"total_changes": 10, "by_location_method": {"not_found": 2, "resolved": 8},
         "changes_without_page": 1}
    assert uf._compute_location_warnings(s) == []
