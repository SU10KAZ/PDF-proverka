"""Tests for backend/app/services/text_analysis/cross_section_rules.py."""
from __future__ import annotations

import pytest

from backend.app.services.text_analysis.cross_section_rules import (
    block_reason_for_cross_section,
    can_report_cross_section_missing,
    has_cross_section_context,
    is_cross_section_item,
)


# ---------------------------------------------------------------------------
# has_cross_section_context.
# ---------------------------------------------------------------------------


def test_no_context_means_no_cross_section():
    assert has_cross_section_context(None) is False
    assert has_cross_section_context({}) is False


def test_md_count_above_threshold_satisfies():
    assert has_cross_section_context({"md_count": 2}) is True
    assert has_cross_section_context({"md_count": 5}) is True


def test_md_count_below_threshold_fails():
    assert has_cross_section_context({"md_count": 1}) is False
    assert has_cross_section_context({"md_count": 0}) is False


def test_md_count_bool_value_ignored():
    # True == 1 should NOT be misread as md_count=1.
    assert has_cross_section_context({"md_count": True}) is False


def test_single_md_with_multiple_sections_needs_explicit_marker():
    # Two sections in one MD do not unlock cross-section unless
    # the context flags it explicitly.
    ctx = {"md_count": 1, "available_sections": ["EOM", "OV"]}
    assert has_cross_section_context(ctx) is False
    ctx2 = {
        "md_count": 1,
        "available_sections": ["EOM", "OV"],
        "allow_single_md_cross_section": True,
    }
    assert has_cross_section_context(ctx2) is True


# ---------------------------------------------------------------------------
# is_cross_section_item.
# ---------------------------------------------------------------------------


def test_flag_true_means_cross_section():
    assert is_cross_section_item({"requires_cross_section": True}) is True


def test_flag_false_means_not_cross_section():
    assert is_cross_section_item({"requires_cross_section": False}) is False


def test_missing_flag_falls_back_on_multi_id():
    # MULTI-05..MULTI-13 are cross-section by definition.
    assert is_cross_section_item({"item_id": "MULTI-05"}) is True
    assert is_cross_section_item({"item_id": "MULTI-13"}) is True
    assert is_cross_section_item({"item_id": "MULTI-04"}) is False
    assert is_cross_section_item({"item_id": "MULTI-14"}) is False
    assert is_cross_section_item({"item_id": "AR-05"}) is False


def test_empty_metadata_not_cross_section():
    assert is_cross_section_item({}) is False
    assert is_cross_section_item(None) is False


# ---------------------------------------------------------------------------
# can_report_cross_section_missing.
# ---------------------------------------------------------------------------


def test_non_cross_section_item_always_can_report():
    item = {"item_id": "AR-02", "requires_cross_section": False}
    assert can_report_cross_section_missing(item, None) is True
    assert can_report_cross_section_missing(item, {"md_count": 1}) is True


def test_cross_section_blocked_in_single_md_pipeline():
    item = {"item_id": "MULTI-05", "requires_cross_section": True, "discipline": "MULTI"}
    assert can_report_cross_section_missing(item, {"md_count": 1}) is False


def test_cross_section_allowed_with_multi_md_and_marker():
    item = {
        "item_id": "MULTI-05",
        "requires_cross_section": True,
        "discipline": "MULTI",
    }
    ctx = {"md_count": 2, "cross_md_pipeline": True}
    assert can_report_cross_section_missing(item, ctx) is True


def test_multi_item_still_blocked_without_cross_md_marker():
    item = {
        "item_id": "MULTI-05",
        "requires_cross_section": True,
        "discipline": "MULTI",
    }
    ctx = {"md_count": 3}  # multiple MDs but no explicit pipeline marker
    assert can_report_cross_section_missing(item, ctx) is False


def test_non_multi_cross_section_unlocks_with_multi_md():
    """A coordination item from a normal discipline can theoretically be
    reported when the cross-section context is satisfied."""
    item = {
        "item_id": "EOM-17",
        "requires_cross_section": True,
        "discipline": "EOM",
    }
    ctx = {"md_count": 2}
    assert can_report_cross_section_missing(item, ctx) is True


# ---------------------------------------------------------------------------
# block_reason_for_cross_section.
# ---------------------------------------------------------------------------


def test_block_reason_none_when_not_cross_section():
    item = {"item_id": "AR-02", "requires_cross_section": False}
    assert block_reason_for_cross_section(item, None) is None


def test_block_reason_describes_single_md_block():
    item = {
        "item_id": "MULTI-05",
        "requires_cross_section": True,
        "discipline": "MULTI",
    }
    reason = block_reason_for_cross_section(item, {"md_count": 1})
    assert reason and "single-MD" in reason


def test_block_reason_describes_multi_without_marker():
    item = {
        "item_id": "MULTI-05",
        "requires_cross_section": True,
        "discipline": "MULTI",
    }
    reason = block_reason_for_cross_section(item, {"md_count": 3})
    assert reason and "cross_md_pipeline" in reason


def test_block_reason_none_when_pipeline_satisfied():
    item = {
        "item_id": "MULTI-05",
        "requires_cross_section": True,
        "discipline": "MULTI",
    }
    ctx = {"md_count": 3, "cross_md_pipeline": True}
    assert block_reason_for_cross_section(item, ctx) is None
