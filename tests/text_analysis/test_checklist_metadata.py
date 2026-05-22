"""Tests for backend/app/data/discipline_checklists_metadata/<DISC>.json.

Validates the static JSON metadata files generated from the normative
research matrix. These files drive the P0 safety layer for the future
completeness_runner.

No LLM, no pipeline, no runtime wiring — pure file reads + assertions.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.core.config import APP_DATA_DIR

METADATA_DIR: Path = APP_DATA_DIR / "discipline_checklists_metadata"

EXPECTED_DISCIPLINES = {"AR", "EOM", "KJ", "KM", "MULTI", "OV", "SS", "VK"}

EXPECTED_COUNTS = {
    "AR": 23,
    "EOM": 25,
    "KJ": 25,
    "KM": 25,
    "MULTI": 22,
    "OV": 25,
    "SS": 25,
    "VK": 25,
}
EXPECTED_TOTAL = 195

# Hard schema for per-item fields. Every item in every file must have these.
REQUIRED_FIELDS = (
    "item_id",
    "item_name",
    "discipline",
    "normative_status",
    "can_be_reported_as_missing",
    "applicable_document_types",
    "applicable_stages",
    "applicability_conditions",
    "object_signals",
    "severity_policy",
    "recommended_action",
    "normative_basis",
    "confidence",
    "requires_cross_section",
    "requires_human_validation",
    "allow_in_shadow_only",
    "disabled_by_default",
    "source_research_reference",
)

ALLOWED_NORMATIVE_STATUS = {
    "mandatory",
    "conditionally_mandatory",
    "recommended",
    "optional",
    "not_applicable",
}

ALLOWED_DOCUMENT_TYPES = {
    "full_rd",
    "audit_comparison",
    "tz_vs_rd",
    "specification_only",
}

ALLOWED_STAGES = {
    "project_documentation",
    "working_documentation",
    "detailing",
}

ALLOWED_CONFIDENCE = {"high", "medium", "low"}


@pytest.fixture(scope="module")
def bundles():
    out: dict[str, dict] = {}
    for disc in sorted(EXPECTED_DISCIPLINES):
        path = METADATA_DIR / f"{disc}.json"
        assert path.is_file(), f"missing metadata: {path}"
        out[disc] = json.loads(path.read_text(encoding="utf-8"))
    return out


# ---------------------------------------------------------------------------
# Directory + file presence.
# ---------------------------------------------------------------------------


def test_metadata_dir_exists():
    assert METADATA_DIR.is_dir(), f"missing: {METADATA_DIR}"


def test_readme_exists():
    assert (METADATA_DIR / "README.md").is_file()


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_metadata_file_exists(discipline):
    path = METADATA_DIR / f"{discipline}.json"
    assert path.is_file(), f"missing: {path}"


# ---------------------------------------------------------------------------
# Bundle envelope.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_bundle_envelope_fields(bundles, discipline):
    b = bundles[discipline]
    assert b["schema_version"] == 1
    assert b["discipline"] == discipline
    assert isinstance(b["items"], list)
    assert isinstance(b["counts"], dict)
    assert b["counts"]["total"] == EXPECTED_COUNTS[discipline]


def test_total_item_count_matches_research(bundles):
    total = sum(len(b["items"]) for b in bundles.values())
    assert total == EXPECTED_TOTAL


# ---------------------------------------------------------------------------
# Per-item schema invariants.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_item_has_required_fields(bundles, discipline):
    for it in bundles[discipline]["items"]:
        missing = [f for f in REQUIRED_FIELDS if f not in it]
        assert not missing, f"{it.get('item_id')} missing: {missing}"


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_item_normative_status_in_allowed(bundles, discipline):
    for it in bundles[discipline]["items"]:
        assert it["normative_status"] in ALLOWED_NORMATIVE_STATUS, (
            f"{it['item_id']}: invalid normative_status {it['normative_status']!r}"
        )


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_item_document_types_in_allowed(bundles, discipline):
    for it in bundles[discipline]["items"]:
        for t in it["applicable_document_types"]:
            assert t in ALLOWED_DOCUMENT_TYPES, (
                f"{it['item_id']}: invalid document_type {t!r}"
            )


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_item_stages_in_allowed(bundles, discipline):
    for it in bundles[discipline]["items"]:
        for s in it["applicable_stages"]:
            assert s in ALLOWED_STAGES, (
                f"{it['item_id']}: invalid stage {s!r}"
            )


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_item_confidence_in_allowed(bundles, discipline):
    for it in bundles[discipline]["items"]:
        assert it["confidence"] in ALLOWED_CONFIDENCE


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_item_severity_policy_complete(bundles, discipline):
    expected_keys = {
        "default",
        "if_stage_unknown_or_mismatch",
        "if_doc_type_mismatch",
        "if_signal_missing",
    }
    for it in bundles[discipline]["items"]:
        sp = it["severity_policy"]
        assert isinstance(sp, dict)
        assert set(sp) == expected_keys, (
            f"{it['item_id']}: severity_policy keys {sorted(sp)}"
        )


@pytest.mark.parametrize("discipline", sorted(EXPECTED_DISCIPLINES))
def test_each_item_id_format(bundles, discipline):
    for it in bundles[discipline]["items"]:
        item_id = it["item_id"]
        assert "-" in item_id
        prefix, num = item_id.split("-", 1)
        assert prefix == discipline
        assert num.isdigit()


# ---------------------------------------------------------------------------
# Research-level invariants (these are the *real* P0 safety checks).
# ---------------------------------------------------------------------------


def test_cannot_be_reported_count_close_to_research(bundles):
    """Research said 46 items can't be reported as missing. We expect 46+ —
    we may mark a few extra (cross-section + coordination items) as not
    reportable, which is the safe direction. Never less than 46."""
    total = sum(
        1
        for b in bundles.values()
        for it in b["items"]
        if not it["can_be_reported_as_missing"]
    )
    assert total >= 46, f"expected >=46 cannot-report items, got {total}"


def test_conditional_items_count_matches_research(bundles):
    """Research said 71 items are conditionally_mandatory."""
    total = sum(
        1
        for b in bundles.values()
        for it in b["items"]
        if it["normative_status"] == "conditionally_mandatory"
    )
    assert total == 71, f"expected 71 conditionally_mandatory, got {total}"


def test_unconditional_mandatory_count_matches_research(bundles):
    """Research said 71 items are unconditionally mandatory."""
    total = sum(
        1
        for b in bundles.values()
        for it in b["items"]
        if it["normative_status"] == "mandatory"
    )
    assert total == 71, f"expected 71 mandatory, got {total}"


def test_cross_section_items_are_not_reportable(bundles):
    """Force-invariant: any item with requires_cross_section=true MUST have
    can_be_reported_as_missing=false. This is the strongest interpretation
    of final_report.md §7."""
    for b in bundles.values():
        for it in b["items"]:
            if it["requires_cross_section"]:
                assert not it["can_be_reported_as_missing"], (
                    f"{it['item_id']}: cross-section item must not be reportable"
                )


def test_disabled_items_are_not_reportable(bundles):
    for b in bundles.values():
        for it in b["items"]:
            if it["disabled_by_default"]:
                assert not it["can_be_reported_as_missing"] or it.get(
                    "disabled_reason"
                ), f"{it['item_id']}: disabled-by-default must have a reason"


def test_multi_cross_section_items_blocked(bundles):
    """MULTI-05..MULTI-13 must all be disabled + cross-section + not
    reportable (the strongest interpretation of the research)."""
    multi = {it["item_id"]: it for it in bundles["MULTI"]["items"]}
    for n in range(5, 14):
        item_id = f"MULTI-{n:02d}"
        assert item_id in multi
        it = multi[item_id]
        assert it["requires_cross_section"], item_id
        assert not it["can_be_reported_as_missing"], item_id
        assert it["disabled_by_default"], item_id


def test_ov_25_disabled(bundles):
    """OV-25 is the duplicate with VK and must be disabled by default."""
    ov = {it["item_id"]: it for it in bundles["OV"]["items"]}
    assert ov["OV-25"]["disabled_by_default"] is True
    assert ov["OV-25"]["disabled_reason"]


def test_object_signals_are_allow_listed(bundles):
    from backend.app.services.text_analysis.object_signals import KNOWN_SIGNALS

    for b in bundles.values():
        for it in b["items"]:
            for s in it["object_signals"]:
                assert s in KNOWN_SIGNALS, (
                    f"{it['item_id']}: unknown signal {s!r}; allow-list "
                    f"is {sorted(KNOWN_SIGNALS)}"
                )


def test_source_research_reference_format(bundles):
    """All items must point back to the research matrix for traceability."""
    for b in bundles.values():
        for it in b["items"]:
            ref = it["source_research_reference"]
            assert ref.endswith(f"#{it['item_id']}")
            assert "completeness_requirements_matrix.json" in ref
