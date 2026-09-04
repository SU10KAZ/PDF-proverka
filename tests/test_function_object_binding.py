"""Deterministic object binding recovery for MERGED.

Unit cases are synthetic Markdown; no project, page or file name of any corpus
appears in them.  The corpus fixture only records measured outcomes.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import object_binding as binding


def _page(body: str, *, stamp: str = "Sheet: 1 | Object: | Name: Схема") -> str:
    return f"## Page 1\n\n> **Stamp:** {stamp}\n\n{body}\n"


# --- what the extractor sees vs what the page says ---------------------------


def test_a_binding_outside_the_scanned_slice_is_invisible_today():
    rows = binding.page_bindings(_page("Питание подаётся на корпус 3."))

    assert rows[1]["page_body"] == ["корпус 3"]
    assert rows[1]["evidence_text"] == []


def test_a_binding_inside_the_scanned_slice_is_already_visible():
    rows = binding.page_bindings(
        _page("**Description:** Питание подаётся на корпус 3.")
    )

    assert rows[1]["page_body"] == ["корпус 3"]
    assert rows[1]["evidence_text"] == ["корпус 3"]


def test_the_project_address_is_never_a_binding_token():
    rows = binding.page_bindings(
        _page("текст", stamp="Object: ЖИЛОЙ КОМПЛЕКС ПО АДРЕСУ: УЛ. ПРИМЕРНАЯ, 1")
    )

    assert rows[1]["page_body"] == []
    assert rows[1]["stamp_object"].startswith("ЖИЛОЙ КОМПЛЕКС")


def test_sections_count_as_binding_and_floors_do_not():
    rows = binding.page_bindings(_page("Секция 2, отм. +7.950"))

    assert rows[1]["page_body"] == ["секция 2"]


# --- the refusal is measured, not assumed ------------------------------------


def test_a_constant_stamp_object_cannot_discriminate():
    bindings = {
        "X": {"left": {
            1: {"page_body": [], "evidence_text": [], "stamp_object": "ЖК АДРЕС"},
            2: {"page_body": [], "evidence_text": [], "stamp_object": "ЖК АДРЕС"},
        }}
    }

    verdict = binding.stamp_object_discrimination(bindings)

    assert verdict["sides_with_discriminating_values"] == 0
    assert verdict["usable_as_binding_evidence"] is False


def test_a_varying_stamp_object_would_discriminate():
    bindings = {
        "X": {"left": {
            1: {"page_body": [], "evidence_text": [], "stamp_object": "Корпус 1"},
            2: {"page_body": [], "evidence_text": [], "stamp_object": "Корпус 2"},
        }}
    }

    verdict = binding.stamp_object_discrimination(bindings)

    assert verdict["sides_with_discriminating_values"] == 1
    assert verdict["per_side"][0]["values_naming_an_object"] == 2


# --- a sheet is not a function -----------------------------------------------


def test_the_attribution_rule_is_declared():
    artifact = binding.build()

    assert artifact["safety"]["sheet_treated_as_function"] is False
    assert set(artifact["attribution"]["states"]) == set(binding.ATTRIBUTION_STATES)


# --- corpus outcome ----------------------------------------------------------


@pytest.fixture(scope="module")
def artifact() -> dict:
    return binding.build()


def test_the_survey_makes_no_model_calls(artifact: dict) -> None:
    assert artifact["model_calls"] == 0
    assert artifact["safety"]["production_modules_changed"] == 0
    assert artifact["safety"]["candidate_recall_loss"] == 0
    assert artifact["safety"]["non_discriminating_evidence_published"] is False


def test_replay_is_byte_identical() -> None:
    first = binding.build()
    second = binding.build()

    assert binding.stratified._json_bytes(first) == binding.stratified._json_bytes(
        second
    )


def test_the_stamp_object_is_parsed_and_unused(artifact: dict) -> None:
    stamp = artifact["stamp_object"]

    assert stamp["parsed_by_extractor"] is True
    assert stamp["used_for_serviced_object"] is False
    assert stamp["sides_with_discriminating_values"] == 0
    assert all(row["values_naming_an_object"] == 0 for row in stamp["per_side"])


def test_the_recovery_gap_is_real(artifact: dict) -> None:
    gap = artifact["recovery_gap"]

    assert gap["pages"] == 277
    assert gap["pages_with_binding_in_body"] == 54
    assert gap["pages_where_the_extractor_sees_it"] == 22
    assert gap["recoverable_pages"] == 32


def test_no_recovered_binding_can_be_attributed_to_a_function(
    artifact: dict,
) -> None:
    states = artifact["attribution"]["states"]

    assert states["ATTRIBUTABLE"] == 0
    assert states["PAGE_AMBIGUOUS"] == 24
    assert states["OBJECT_AMBIGUOUS"] == 2
    assert artifact["attribution"]["attributable_page_bindings"] == []


def test_recovery_certifies_no_merge_even_at_the_upper_bound(
    artifact: dict,
) -> None:
    impact = artifact["certificate_impact"]

    assert impact["upper_bound_ignoring_attributability"]["both_sides_recoverable"] == 0
    assert impact["upper_bound_ignoring_attributability"]["one_side_only"] == 18
    assert impact["upper_bound_ignoring_attributability"]["nothing_recoverable"] == 51
    assert impact["sound_recovery"]["not_soundly_bound"] == 69
    assert impact["partial_certificates_that_would_become_certified"] == 0


def test_the_single_refutation_is_not_sound(artifact: dict) -> None:
    impact = artifact["certificate_impact"]

    assert len(impact["candidate_refutations"]) == 1
    assert impact["refutations_are_sound"] is False


def test_recorded_verdict(artifact: dict) -> None:
    verdict = artifact["verdict"]

    assert verdict["binding_is_recoverable"] is True
    assert verdict["recovered_binding_is_attributable"] is False
    assert verdict["merge_tier_unlocked"] is False
    assert verdict["class"] == "E_DATA_LIMITED"
