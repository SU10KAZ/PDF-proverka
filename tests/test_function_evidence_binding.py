"""Deterministic function-local evidence attribution.

Unit cases are synthetic Markdown; no project, page or file name of any corpus
appears in them.  The corpus fixture only records measured outcomes.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import evidence_binding as binding


ELECTRICAL = "ELECTRICAL_DISTRIBUTION"
LIGHTING = "LIGHTING"
WATER = "WATER_SUPPLY"


def _units(body: str) -> list[dict]:
    return binding.segment_page("LEFT", 1, body)


def _bind(body: str, value: str, classes: tuple[str, ...]) -> dict:
    return binding.bind_value(value, _units(body), list(classes))


# --- segmentation ------------------------------------------------------------


def test_a_block_header_opens_a_block_and_a_stamp_is_its_own_unit():
    units = _units(
        "### BLOCK #1 [TEXT]: blk_a\n\n> **Stamp:** Sheet: 1 | Name: Схема\n\nТекст.\n"
    )

    kinds = [unit["unit_kind"] for unit in units]
    assert kinds == ["BLOCK", "STAMP", "PARAGRAPH"]
    assert units[0]["block_id"] == "blk_a"


def test_list_items_separated_by_blank_lines_stay_one_list():
    units = _units("Перечень:\n\n- первый\n\n- второй\n\n- третий\n")

    assert [unit["unit_kind"] for unit in units].count("LIST") == 1
    assert [unit["unit_kind"] for unit in units].count("LIST_ITEM") == 3


def test_a_list_lead_in_is_the_line_directly_above_it():
    units = _units("Устанавливается освещение:\n\n- первый\n")
    items = [unit for unit in units if unit["unit_kind"] == "LIST_ITEM"]

    assert items[0]["scope_kind"] == "LIST_LEAD_IN"


def test_a_paragraph_further_up_is_not_a_lead_in():
    units = _units("Устанавливается освещение:\n\nОбычный текст.\n\n- первый\n")
    items = [unit for unit in units if unit["unit_kind"] == "LIST_ITEM"]

    assert items[0]["scope_kind"] is None


def test_a_table_caption_is_the_line_directly_above_the_table():
    units = _units(
        "Спецификация оборудования освещения\n\n| Поз | Наименование |\n|---|---|\n| 1 | Светильник |\n"
    )
    rows = [unit for unit in units if unit["unit_kind"] == "TABLE_ROW"]

    assert rows and rows[0]["scope_kind"] == "TABLE_CAPTION"


def test_a_table_separator_row_is_not_a_row():
    units = _units("| Поз | Наименование |\n|---|---|\n| 1 | Светильник |\n")
    rows = [unit for unit in units if unit["unit_kind"] == "TABLE_ROW"]

    assert len(rows) == 2


# --- claims ------------------------------------------------------------------


def test_a_unit_claims_only_classes_that_exist_on_the_page():
    claims = binding.unit_claims("схема освещения", (ELECTRICAL,))

    assert claims == frozenset()


def test_a_unit_naming_nothing_claims_nothing():
    assert binding.unit_claims("прочий текст", (LIGHTING,)) == frozenset()


# --- Phase 6 negative controls ----------------------------------------------


def test_an_address_shared_by_a_whole_sheet_stays_sheet_shared():
    body = (
        "### BLOCK #1 [TEXT]: blk_a\n\n"
        "> **Stamp:** Sheet: 1 | Object: ЖИЛОЙ КОМПЛЕКС\n\n"
        "**Description:** Схема электроснабжения и схема освещения.\n\n"
        "Корпус 3.\n"
    )
    result = _bind(body, "Корпус 3", (ELECTRICAL, LIGHTING))

    assert result["binding_relation"] == "SHEET_SHARED"
    assert result["binding_status"] == "PARTIAL"
    assert result["owners"] == []


def test_a_label_claimed_by_two_fragments_is_ambiguous():
    body = (
        "**Description:** Схема электроснабжения корпуса 3.\n\n"
        "**Summary:** Схема освещения корпуса 3.\n"
    )
    result = _bind(body, "корпуса 3", (ELECTRICAL, LIGHTING))

    assert result["binding_status"] == "AMBIGUOUS"
    assert sorted(result["owners"]) == [ELECTRICAL, LIGHTING]


def test_a_unit_naming_one_class_owns_the_fact_it_contains():
    body = "**Description:** Схема освещения обслуживает корпус 3.\n"
    result = _bind(body, "корпус 3", (ELECTRICAL, LIGHTING))

    assert result["binding_status"] == "PROVEN"
    assert result["binding_relation"] == "DIRECT_CONTAINMENT"
    assert result["owners"] == [LIGHTING]


def test_a_table_row_inherits_a_caption_that_names_one_class():
    body = (
        "Спецификация оборудования освещения\n\n"
        "| Поз | Наименование |\n|---|---|\n| 1 | Прибор ЩО-1 |\n"
    )
    result = _bind(body, "ЩО-1", (ELECTRICAL, LIGHTING))

    assert result["binding_status"] == "PROVEN"
    assert result["binding_relation"] == "TABLE_ROW"
    assert result["owners"] == [LIGHTING]


def test_proximity_alone_never_binds():
    body = (
        "**Description:** Схема освещения.\n\n"
        "Оборудование ЩО-1 установлено в помещении.\n"
    )
    result = _bind(body, "ЩО-1", (ELECTRICAL, LIGHTING))

    assert result["binding_status"] != "PROVEN"
    assert result["binding_relation"] == "SHEET_SHARED"


def test_a_value_that_is_not_in_the_page_is_unknown():
    result = _bind("**Description:** Схема освещения.\n", "корпус 9", (LIGHTING,))

    assert result["binding_status"] == "UNKNOWN"
    assert result["binding_relation"] == "UNKNOWN"


def test_a_lone_function_on_a_page_does_not_own_an_unclaimed_fact():
    body = (
        "### BLOCK #1 [TEXT]: blk_a\n\n"
        "> **Stamp:** Sheet: 1 | Name: Схема освещения\n\n"
        "Корпус 3.\n"
    )
    result = _bind(body, "Корпус 3", (LIGHTING,))

    assert result["binding_status"] == "PARTIAL"
    assert result["owners"] == []


def test_a_stamp_fact_is_never_owned_by_a_fragment():
    body = "### BLOCK #1 [TEXT]: blk_a\n\n> **Stamp:** Name: Схема освещения корпуса 3\n"
    result = _bind(body, "корпуса 3", (LIGHTING,))

    assert result["binding_relation"] == "SHEET_SHARED"
    assert result["owners"] == []


def test_block_scope_alone_never_confers_ownership():
    body = (
        "### BLOCK #1 [TEXT]: blk_a\n\n"
        "**Description:** Схема освещения.\n\n"
        "Корпус 3.\n"
    )
    result = _bind(body, "Корпус 3", (LIGHTING,))

    assert result["deterministic_reason"] == "NO_BOUNDED_SCOPE_NAMES_A_FUNCTION"
    assert result["owners"] == []


# --- overlay -----------------------------------------------------------------


def _passport(function_id: str, fragment_id: str, **fields) -> dict:
    return {
        "function_id": function_id,
        "function_fragment_ids": [fragment_id],
        "side": "LEFT",
        "function_class": LIGHTING,
        "source_sheet": {"side": "LEFT", "physical_page": 1, "title": "Схема"},
        "provenance": {},
        **{field: None for field in binding.OVERLAY_FIELDS},
        **fields,
    }


def _binding_row(fragment_id: str, field: str, value: str, status: str) -> dict:
    return {
        "fragment_id": fragment_id,
        "field": field,
        "value_preview": value,
        "binding_status": status,
    }


def test_the_overlay_states_only_proven_values():
    passports = {"f1": _passport("f1", "frag1", corpus=["Корпус 3", "Корпус 4"])}
    rows = [
        _binding_row("frag1", "corpus", "Корпус 3", "PROVEN"),
        _binding_row("frag1", "corpus", "Корпус 4", "AMBIGUOUS"),
    ]

    overlay = binding.overlay_passports(passports, rows, mode="FRAGMENT_LOCAL")

    assert overlay["f1"]["corpus"] == ["Корпус 3"]


def test_a_field_with_no_proven_value_becomes_unknown_not_a_contradiction():
    passports = {"f1": _passport("f1", "frag1", corpus=["Корпус 3"])}
    rows = [_binding_row("frag1", "corpus", "Корпус 3", "SHEET_SHARED")]

    overlay = binding.overlay_passports(passports, rows, mode="FRAGMENT_LOCAL")

    assert overlay["f1"]["corpus"] is None


def test_a_sheet_shared_value_is_never_copied_into_a_sibling_fragment():
    passports = {
        "f1": _passport("f1", "frag1", corpus=["Корпус 3"]),
        "f2": _passport("f2", "frag2", corpus=["Корпус 3"]),
    }
    rows = [_binding_row("frag1", "corpus", "Корпус 3", "PROVEN")]

    overlay = binding.overlay_passports(passports, rows, mode="FRAGMENT_LOCAL")

    assert overlay["f1"]["corpus"] == ["Корпус 3"]
    assert overlay["f2"]["corpus"] is None


def test_the_strict_overlay_drops_a_title_that_is_not_bound():
    passports = {"f1": _passport("f1", "frag1")}

    overlay = binding.overlay_passports(passports, [], mode="FRAGMENT_LOCAL_STRICT")

    assert overlay["f1"]["source_sheet"]["title"] is None


def test_the_plain_overlay_keeps_the_title():
    passports = {"f1": _passport("f1", "frag1")}

    overlay = binding.overlay_passports(passports, [], mode="FRAGMENT_LOCAL")

    assert overlay["f1"]["source_sheet"]["title"] == "Схема"


def test_an_unknown_overlay_mode_is_refused():
    with pytest.raises(ValueError):
        binding.overlay_passports({}, [], mode="ANYTHING")


# --- corpus fixture ----------------------------------------------------------

CORPUS = (
    binding.stratified.COMPARISON_ROOT
    / "20260904_function_lineage_v2_9_evidence_binding"
    / "binding_metrics.json"
)
requires_corpus = pytest.mark.skipif(
    not CORPUS.is_file(), reason="binding artifact has not been produced"
)


@pytest.fixture(scope="module")
def metrics() -> dict:
    return binding.stratified._read_json(CORPUS)


@requires_corpus
def test_no_scope_fact_in_the_corpora_is_fragment_local(metrics):
    for field in ("serviced_object", "building", "corpus", "section"):
        assert metrics["by_field"][field].get("PROVEN", 0) == 0


@requires_corpus
def test_the_binding_layer_breaks_no_invariant(metrics):
    safety = metrics["corpus_safety"]
    assert safety["binding_invariants"]["violation_count"] == 0
    assert safety["binding_invariants"]["overlay_states_an_unproven_value"] == 0
    assert safety["sheet_equals_fragment_leakage"]["justified_by_absence_of_rivals"] == 0


@requires_corpus
def test_candidate_recall_and_scope_baselines_are_untouched(metrics):
    safety = metrics["corpus_safety"]
    assert safety["candidate_recall"]["unchanged"] is True
    assert safety["scope_safety"]["matches_frozen_baseline"] is True
    assert safety["candidate_generation_touched"] is False


@requires_corpus
def test_every_negative_control_holds(metrics):
    assert metrics["negative_controls"]["violation_count"] == 0


@requires_corpus
def test_the_callout_control_is_declared_unavailable_not_passed(metrics):
    control = metrics["negative_controls"]["controls"]["EXPLICIT_CALLOUT_TO_ONE_FRAGMENT"]
    assert control["structurally_available"] is False
    assert control["instances"] == 0


@requires_corpus
def test_the_run_calls_no_model_and_replays_byte_identically(metrics):
    assert metrics["model_calls"] == 0
    assert metrics["determinism"]["byte_identical"] is True


@requires_corpus
def test_no_tier_is_opened_and_the_verdict_says_so(metrics):
    verdict = metrics["verdict"]
    assert verdict["auto_merged_certified_after"] == 0
    assert verdict["auto_one_to_one_certified_after"] == 0
    assert verdict["production_relevant_tier_opened"] is False
