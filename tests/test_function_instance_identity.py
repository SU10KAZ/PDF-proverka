"""Deterministic function instance / series identity.

Every case is built from synthetic passports: no project, page or file name of
any corpus appears in this suite.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import instance_identity as identity


def _passport(
    function_id: str, *, title: str | None = None, page: int = 1,
    sheet: str | None = None, **fields,
) -> dict:
    return {
        "function_id": function_id,
        "side": "LEFT",
        "function_class": fields.pop("function_class", "CLASS_A"),
        "component_role": "ROLE_A",
        "document_role": "GRAPHIC_SHEET",
        "source_sheet": {
            "title": title,
            "physical_page": page,
            "graphic_sheet_number": sheet,
        },
        **fields,
    }


def _identity(**kwargs) -> dict:
    return identity.function_instance_identity(_passport(**kwargs))


# --- required cases ----------------------------------------------------------


def test_same_class_different_proven_mark_is_distinguishable():
    members = [
        _identity(function_id="f1", title="Схема щита ЩК1"),
        _identity(function_id="f2", title="Схема щита ЩК2"),
    ]

    verdict = identity.classify_cluster(members)

    assert verdict["classification"] == "UNIQUELY_IDENTIFIED"
    assert "mark" in verdict["distinguishing_facts"]
    assert members[0]["identity_facts"]["primary_mark"] == "ЩK1"
    assert members[1]["identity_facts"]["primary_mark"] == "ЩK2"


def test_same_class_different_zone_is_distinguishable():
    members = [
        _identity(function_id="f1", title="План систем", zone=["Корпус 1"]),
        _identity(function_id="f2", title="План систем", zone=["Корпус 2"]),
    ]

    verdict = identity.classify_cluster(members)

    assert verdict["classification"] == "UNIQUELY_IDENTIFIED"
    assert "zone" in verdict["distinguishing_facts"]


def test_same_class_without_distinguishing_evidence_is_indistinguishable():
    members = [
        _identity(function_id="f1", title="Внутренние системы"),
        _identity(function_id="f2", title="Внутренние системы"),
    ]

    verdict = identity.classify_cluster(members)

    assert verdict["classification"] == "UNKNOWN"
    assert verdict["distinguishing_facts"] == {}
    assert all(value["identity_status"] == "UNKNOWN" for value in members)


def test_a_physical_page_alone_is_not_identity():
    members = [
        _identity(function_id="f1", title="Внутренние системы", page=31, sheet="8"),
        _identity(function_id="f2", title="Внутренние системы", page=33, sheet="10"),
    ]

    verdict = identity.classify_cluster(members)

    assert verdict["classification"] == "UNKNOWN"
    for member in members:
        assert member["identity_status"] == "UNKNOWN"
        assert "physical_page" not in member["identity_facts"]
        assert member["provenance_only"]["physical_page"] in (31, 33)


def test_a_missing_fact_is_never_a_mismatch():
    members = [
        _identity(function_id="f1", title="Схема щита ЩК1", zone=["Корпус 1"]),
        _identity(function_id="f2", title="Схема щита ЩК2"),
    ]

    verdict = identity.classify_cluster(members)

    # zone is known for one member only, so it separates nothing and is
    # reported as missing rather than as a difference.
    assert "zone" not in verdict["distinguishing_facts"]
    assert "zone" in verdict["missing_distinguishing_facts"]
    assert verdict["classification"] == "UNIQUELY_IDENTIFIED"


def test_overlapping_values_do_not_separate_instances():
    members = [
        _identity(function_id="f1", title="План", zone=["Корпус 1", "Корпус 2"]),
        _identity(function_id="f2", title="План", zone=["Корпус 2"]),
    ]

    verdict = identity.classify_cluster(members)

    assert "zone" not in verdict["distinguishing_facts"]
    assert verdict["classification"] in {"INDISTINGUISHABLE", "UNKNOWN"}


def test_a_caption_naming_two_instances_proves_none_of_them():
    row = _identity(function_id="f1", title="Схема ЩК1 и ЩК2")

    assert row["identity_facts"]["title_marks"] == ["ЩK1", "ЩK2"]
    assert row["identity_facts"]["primary_mark"] is None
    assert row["identity_status"] != "PROVEN"


def test_sources_that_disagree_are_contradictory_not_distinguishing():
    members = [
        _identity(function_id="f1", title="Схема щита ЯК3", zone=["ЯК1"]),
        _identity(function_id="f2", title="Схема щита ЯК2"),
    ]

    verdict = identity.classify_cluster(members)

    assert verdict["classification"] == "CONTRADICTORY"
    assert verdict["contradictions"][0]["function_id"] == "f1"


def test_identity_evidence_provenance_is_exact():
    row = _identity(function_id="f1", title="Схема щита ЩК1", zone=["ЩК1"])

    assert row["identity_evidence_fields"] == ["source_sheet.title", "zone"]
    marks = {value["mark"]: value for value in row["identity_facts"]["marks"]}
    assert marks["ЩK1"]["source_fields"] == ["source_sheet.title", "zone"]


def test_ocr_digit_homoglyphs_are_folded():
    assert identity.normalize_mark("ЯК", "З") == "ЯK3"
    assert _identity(function_id="f1", title="Схема ЯКЗ")[
        "identity_facts"
    ]["primary_mark"] == "ЯK3"


def test_ordinary_words_with_numbers_are_not_designations():
    row = _identity(function_id="f1", title="Часть 1. Внутреннее электроснабжение")

    assert row["identity_facts"]["title_marks"] == []
    assert row["identity_status"] == "UNKNOWN"


def test_level_marks_are_extracted():
    row = _identity(function_id="f1", title="План на отм. +7.950")

    assert row["identity_facts"]["levels"] == ["+7.950"]
    assert "level" in row["present_fact_kinds"]


def test_identity_replay_is_deterministic():
    first = identity.survey()
    second = identity.survey()

    assert identity.stratified._json_bytes(first) == identity.stratified._json_bytes(
        second
    )


def test_survey_declares_its_rules():
    artifact = identity.survey()

    assert artifact["model_calls"] == 0
    assert artifact["rules"] == {
        "physical_page_is_identity": False,
        "graphic_sheet_number_is_identity": False,
        "missing_fact_is_mismatch": False,
        "project_or_page_specific_rules": False,
    }
    assert artifact["mark_source_fields"][0] == identity.PRIMARY_MARK_FIELD


def test_recorded_feasibility_of_the_certified_tier():
    """The measured reason this track cannot reach an acceptance run."""
    artifact = identity.survey()
    feasibility = artifact["certified_tier_feasibility"]
    resolution = artifact["contested_cluster_resolution"]

    assert feasibility["pure_one_to_one_tasks"] == 40
    assert feasibility["uncontended_pure_one_to_one_tasks"] == 1
    assert feasibility["both_sides_identity_proven_tasks"] == 0
    assert resolution["contested_clusters"] == 12
    assert resolution["resolved"] == 0
    assert resolution["usable_as_acceptance_evidence"] is False
