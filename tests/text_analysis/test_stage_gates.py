"""Tests for backend/app/services/text_analysis/stage_gates.py."""
from __future__ import annotations

import pytest

from backend.app.services.text_analysis.stage_gates import (
    ALLOWED_STAGES,
    DocumentStage,
    infer_stage_from_metadata,
    is_stage_applicable,
    normalize_stage,
    should_block_for_stage,
    should_downgrade_for_stage,
    should_force_shadow_only_for_stage,
)


def test_allowed_stages_match_enum():
    assert ALLOWED_STAGES == {s.value for s in DocumentStage}


# ---------------------------------------------------------------------------
# normalize_stage.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("project_documentation", DocumentStage.PROJECT_DOCUMENTATION),
        ("working_documentation", DocumentStage.WORKING_DOCUMENTATION),
        ("detailing", DocumentStage.DETAILING),
        ("mixed", DocumentStage.MIXED),
        ("unknown", DocumentStage.UNKNOWN),
        # Russian
        ("ПД", DocumentStage.PROJECT_DOCUMENTATION),
        ("РД", DocumentStage.WORKING_DOCUMENTATION),
        ("КМД", DocumentStage.DETAILING),
        # Latin short
        ("PD", DocumentStage.PROJECT_DOCUMENTATION),
        ("RD", DocumentStage.WORKING_DOCUMENTATION),
        ("KMD", DocumentStage.DETAILING),
        # Single-letter
        ("П", DocumentStage.PROJECT_DOCUMENTATION),
        ("Р", DocumentStage.WORKING_DOCUMENTATION),
        # Mixed-case whitespace
        ("  пд  ", DocumentStage.PROJECT_DOCUMENTATION),
    ],
)
def test_normalize_stage_aliases(raw, expected):
    assert normalize_stage(raw) is expected


@pytest.mark.parametrize("bad", [None, "", "garbage", 42, object(), []])
def test_normalize_stage_unknown_input_returns_unknown(bad):
    assert normalize_stage(bad) is DocumentStage.UNKNOWN


def test_normalize_stage_passes_through_enum():
    assert normalize_stage(DocumentStage.DETAILING) is DocumentStage.DETAILING


# ---------------------------------------------------------------------------
# infer_stage_from_metadata.
# ---------------------------------------------------------------------------


def test_infer_uses_canonical_field_first():
    item = {
        "applicable_stages": ["project_documentation"],
        "applicable_stages_raw": ["РД"],  # should be ignored
    }
    assert infer_stage_from_metadata(item) == {DocumentStage.PROJECT_DOCUMENTATION}


def test_infer_falls_back_to_russian_when_canonical_missing():
    item = {"applicable_stages_raw": ["ПД", "РД"]}
    assert infer_stage_from_metadata(item) == {
        DocumentStage.PROJECT_DOCUMENTATION,
        DocumentStage.WORKING_DOCUMENTATION,
    }


def test_infer_returns_empty_for_empty_input():
    assert infer_stage_from_metadata({}) == set()
    assert infer_stage_from_metadata({"applicable_stages": []}) == set()


def test_infer_ignores_unknown_tokens():
    item = {"applicable_stages": ["garbage", "ПД"]}
    assert infer_stage_from_metadata(item) == {DocumentStage.PROJECT_DOCUMENTATION}


# ---------------------------------------------------------------------------
# Gate rules.
# ---------------------------------------------------------------------------


PD_ONLY = {"applicable_stages": ["project_documentation"]}
RD_ONLY = {"applicable_stages": ["working_documentation"]}
PD_AND_RD = {
    "applicable_stages": ["project_documentation", "working_documentation"]
}


def test_is_stage_applicable_pd_only_in_pd_passes():
    assert is_stage_applicable(PD_ONLY, "project_documentation") is True


def test_is_stage_applicable_pd_only_in_rd_fails():
    assert is_stage_applicable(PD_ONLY, "working_documentation") is False


def test_is_stage_applicable_in_mixed_accepts_pd_or_rd_only():
    assert is_stage_applicable(PD_ONLY, "mixed") is True
    assert is_stage_applicable(RD_ONLY, "mixed") is True
    assert is_stage_applicable({"applicable_stages": ["detailing"]}, "mixed") is False


def test_is_stage_applicable_unknown_target_always_false():
    assert is_stage_applicable(PD_ONLY, "unknown") is False
    assert is_stage_applicable(PD_ONLY, None) is False
    assert is_stage_applicable(PD_AND_RD, "unknown") is False


def test_is_stage_applicable_empty_metadata_fails_closed():
    assert is_stage_applicable({}, "project_documentation") is False
    assert is_stage_applicable({"applicable_stages": []}, "project_documentation") is False


# ---------------------------------------------------------------------------
# Downgrade.
# ---------------------------------------------------------------------------


def test_pd_only_in_rd_gets_downgrade():
    assert should_downgrade_for_stage(PD_ONLY, "working_documentation") is True


def test_pd_only_in_unknown_gets_downgrade():
    assert should_downgrade_for_stage(PD_ONLY, "unknown") is True


def test_pd_only_in_mixed_gets_downgrade():
    assert should_downgrade_for_stage(PD_ONLY, "mixed") is True


def test_pd_only_in_pd_no_downgrade():
    assert should_downgrade_for_stage(PD_ONLY, "project_documentation") is False


def test_rd_only_no_downgrade_in_unknown():
    assert should_downgrade_for_stage(RD_ONLY, "unknown") is False


def test_pd_and_rd_no_downgrade():
    assert should_downgrade_for_stage(PD_AND_RD, "unknown") is False
    assert should_downgrade_for_stage(PD_AND_RD, "working_documentation") is False


# ---------------------------------------------------------------------------
# Hard block.
# ---------------------------------------------------------------------------


def test_block_when_stages_disjoint():
    assert should_block_for_stage(RD_ONLY, "project_documentation") is True
    assert should_block_for_stage(PD_ONLY, "detailing") is True


def test_no_block_when_stages_overlap():
    assert should_block_for_stage(PD_AND_RD, "project_documentation") is False
    assert should_block_for_stage(PD_AND_RD, "working_documentation") is False


def test_no_block_when_target_unknown_and_pd_only():
    # PD-only items at unknown stage are downgraded, not blocked.
    assert should_block_for_stage(PD_ONLY, "unknown") is False


def test_block_when_target_unknown_and_not_pd_only():
    assert should_block_for_stage(RD_ONLY, "unknown") is True
    assert should_block_for_stage(PD_AND_RD, "unknown") is True


def test_block_when_metadata_has_no_stages():
    assert should_block_for_stage({}, "project_documentation") is True


# ---------------------------------------------------------------------------
# Shadow-only.
# ---------------------------------------------------------------------------


def test_shadow_only_for_pd_only_unknown():
    assert should_force_shadow_only_for_stage(PD_ONLY, "unknown") is True


def test_shadow_only_not_for_known_stage():
    assert (
        should_force_shadow_only_for_stage(PD_ONLY, "project_documentation") is False
    )
    assert (
        should_force_shadow_only_for_stage(PD_ONLY, "working_documentation") is False
    )


def test_shadow_only_not_for_pd_and_rd():
    assert should_force_shadow_only_for_stage(PD_AND_RD, "unknown") is False
