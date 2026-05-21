"""Tests for backend/app/services/text_analysis/checklist_gates.py.

End-to-end tests for the top-level reportability decisions. These run against
hand-crafted item-metadata fixtures AND against a representative sample
of real metadata loaded from `discipline_checklists_metadata/`.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.core.config import APP_DATA_DIR
from backend.app.services.text_analysis.checklist_gates import (
    can_report_missing,
    is_item_applicable,
    reportability_reason,
    requires_cross_section,
    requires_object_signal,
    requires_stage_gate,
    should_downgrade_severity,
    should_force_shadow_only,
)
from backend.app.services.text_analysis.object_signals import detect_object_signals

METADATA_DIR: Path = APP_DATA_DIR / "discipline_checklists_metadata"


# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def all_items():
    out: list[dict] = []
    for path in sorted(METADATA_DIR.glob("*.json")):
        b = json.loads(path.read_text(encoding="utf-8"))
        for it in b["items"]:
            out.append(it)
    assert out, "no items loaded from metadata"
    return out


def _find(all_items, item_id):
    for it in all_items:
        if it["item_id"] == item_id:
            return it
    raise AssertionError(f"item {item_id} not found in metadata")


# ---------------------------------------------------------------------------
# Simple unit predicates.
# ---------------------------------------------------------------------------


def test_requires_stage_gate_true_for_pd_only_item():
    item = {"applicable_stages": ["project_documentation"]}
    assert requires_stage_gate(item) is True


def test_requires_stage_gate_false_for_no_stage():
    assert requires_stage_gate({}) is False
    assert requires_stage_gate({"applicable_stages": []}) is False


def test_requires_object_signal_true():
    item = {"object_signals": ["high_rise"]}
    assert requires_object_signal(item) is True


def test_requires_object_signal_false():
    assert requires_object_signal({}) is False
    assert requires_object_signal({"object_signals": []}) is False


def test_requires_cross_section_proxy():
    assert requires_cross_section({"requires_cross_section": True}) is True
    assert requires_cross_section({"requires_cross_section": False}) is False


# ---------------------------------------------------------------------------
# can_report_missing: structural gates.
# ---------------------------------------------------------------------------


def _baseline(**overrides):
    item = {
        "item_id": "AR-02",
        "discipline": "AR",
        "normative_status": "mandatory",
        "can_be_reported_as_missing": True,
        "applicable_document_types": ["full_rd"],
        "applicable_stages": ["project_documentation", "working_documentation"],
        "applicable_stages_raw": ["ПД", "РД"],
        "object_signals": [],
        "requires_cross_section": False,
        "requires_human_validation": False,
        "allow_in_shadow_only": False,
        "disabled_by_default": False,
    }
    item.update(overrides)
    return item


def _ctx(**overrides):
    ctx = {
        "document_type": "full_rd",
        "stage": "working_documentation",
        "discipline": "AR",
        "md_count": 1,
        "detected_object_signals": {},
        "shadow_mode": False,
    }
    ctx.update(overrides)
    return ctx


def test_baseline_item_can_report():
    assert can_report_missing(_baseline(), _ctx()) is True


def test_disabled_item_cannot_report():
    assert can_report_missing(_baseline(disabled_by_default=True), _ctx()) is False


def test_cannot_report_flag_blocks():
    assert can_report_missing(_baseline(can_be_reported_as_missing=False), _ctx()) is False


def test_doc_type_mismatch_blocks():
    item = _baseline(applicable_document_types=["specification_only"])
    assert can_report_missing(item, _ctx(document_type="full_rd")) is False


def test_doc_type_match_passes():
    item = _baseline(applicable_document_types=["full_rd", "specification_only"])
    assert can_report_missing(item, _ctx(document_type="specification_only")) is True


def test_stage_mismatch_blocks():
    item = _baseline(
        applicable_stages=["project_documentation"],
        applicable_stages_raw=["ПД"],
    )
    # In RD stage, PD-only items are stage-blocked.
    assert can_report_missing(item, _ctx(stage="working_documentation")) is False


def test_stage_unknown_pd_only_shadow_only():
    item = _baseline(
        applicable_stages=["project_documentation"],
        applicable_stages_raw=["ПД"],
    )
    assert can_report_missing(item, _ctx(stage="unknown")) is False


def test_stage_unknown_with_non_pd_only_blocks():
    item = _baseline(
        applicable_stages=["working_documentation"],
        applicable_stages_raw=["РД"],
    )
    assert can_report_missing(item, _ctx(stage="unknown")) is False


def test_human_validation_blocks_outside_shadow():
    item = _baseline(requires_human_validation=True)
    assert can_report_missing(item, _ctx(shadow_mode=False)) is False


def test_human_validation_still_blocked_inside_shadow_via_report_gate():
    """`requires_human_validation=True` items are shadow-only — they do not
    surface even in shadow runs, because shadow is internal logging, not
    user-facing findings."""
    item = _baseline(requires_human_validation=True)
    assert can_report_missing(item, _ctx(shadow_mode=True)) is False


def test_cross_section_item_blocked_in_single_md():
    item = _baseline(
        item_id="MULTI-05",
        discipline="MULTI",
        normative_status="recommended",
        can_be_reported_as_missing=False,  # already enforced in matrix
        requires_cross_section=True,
        disabled_by_default=True,
    )
    assert can_report_missing(item, _ctx(md_count=1)) is False


def test_object_signal_missing_blocks():
    item = _baseline(
        normative_status="conditionally_mandatory",
        object_signals=["high_rise"],
    )
    # No high_rise in detected signals.
    assert can_report_missing(item, _ctx()) is False


def test_object_signal_present_passes():
    item = _baseline(
        normative_status="conditionally_mandatory",
        object_signals=["high_rise"],
    )
    ctx = _ctx(detected_object_signals={"high_rise": True})
    assert can_report_missing(item, ctx) is True


# ---------------------------------------------------------------------------
# Shadow / downgrade.
# ---------------------------------------------------------------------------


def test_downgrade_when_pd_only_in_rd():
    item = _baseline(
        applicable_stages=["project_documentation"],
        applicable_stages_raw=["ПД"],
    )
    assert should_downgrade_severity(item, _ctx(stage="working_documentation")) is True


def test_shadow_only_for_human_validation():
    item = _baseline(requires_human_validation=True)
    assert should_force_shadow_only(item, _ctx()) is True


def test_shadow_only_for_allow_in_shadow_only_outside_shadow():
    item = _baseline(
        normative_status="conditionally_mandatory",
        allow_in_shadow_only=True,
    )
    assert should_force_shadow_only(item, _ctx(shadow_mode=False)) is True


def test_no_shadow_when_shadow_mode_active():
    item = _baseline(
        normative_status="conditionally_mandatory",
        allow_in_shadow_only=True,
    )
    assert should_force_shadow_only(item, _ctx(shadow_mode=True)) is False


# ---------------------------------------------------------------------------
# reportability_reason.
# ---------------------------------------------------------------------------


def test_reason_none_for_ok_baseline():
    assert reportability_reason(_baseline(), _ctx()) is None


def test_reason_describes_cannot_report():
    item = _baseline(can_be_reported_as_missing=False)
    reason = reportability_reason(item, _ctx())
    assert reason and "can_be_reported_as_missing=false" in reason


def test_reason_describes_disabled():
    item = _baseline(
        disabled_by_default=True,
        disabled_reason="Дубль с VK",
    )
    reason = reportability_reason(item, _ctx())
    assert reason and "disabled_by_default" in reason and "Дубль" in reason


def test_reason_describes_missing_object_signals():
    item = _baseline(
        normative_status="conditionally_mandatory",
        object_signals=["high_rise"],
    )
    reason = reportability_reason(item, _ctx())
    assert reason and "missing_object_signals" in reason


def test_reason_describes_stage_mismatch():
    item = _baseline(
        applicable_stages=["project_documentation"],
        applicable_stages_raw=["ПД"],
    )
    reason = reportability_reason(item, _ctx(stage="working_documentation"))
    assert reason and "stage" in reason


# ---------------------------------------------------------------------------
# Sanity: run gates against the real metadata.
# ---------------------------------------------------------------------------


def test_real_multi_cross_section_items_never_report(all_items):
    """All MULTI-05..MULTI-13 should be blocked in default single-MD context."""
    for n in range(5, 14):
        item = _find(all_items, f"MULTI-{n:02d}")
        assert can_report_missing(item, _ctx(discipline="MULTI", md_count=1)) is False


def test_real_or_25_disabled(all_items):
    """OV-25 (dup with VK) must be disabled and unreportable."""
    item = _find(all_items, "OV-25")
    assert can_report_missing(item, _ctx(discipline="OV")) is False


def test_real_eom_22_blocked_without_lightning_signal(all_items):
    """EOM-22 «Молниезащита» is conditional → blocked without signal."""
    item = _find(all_items, "EOM-22")
    ctx = _ctx(discipline="EOM", stage="project_documentation")
    assert can_report_missing(item, ctx) is False


def test_real_eom_22_passes_with_signal(all_items):
    item = _find(all_items, "EOM-22")
    detected = detect_object_signals(
        "Молниезащита по СО-153-34.21.122-2003, категория II."
    )
    ctx = _ctx(
        discipline="EOM",
        stage="project_documentation",
        detected_object_signals=detected,
    )
    # EOM-22 has confidence=medium + is conditionally_mandatory → could be
    # human_validation = True (sets shadow). Skip if that's the case; the
    # test below validates non-shadow path is correct.
    if item.get("requires_human_validation"):
        pytest.skip("EOM-22 flagged for human validation in this metadata build")
    assert can_report_missing(item, ctx) is True


def test_real_pd_only_items_blocked_in_specification_only(all_items):
    """A PD-only item should not report in document_type=specification_only."""
    for item in all_items:
        if item["normative_status"] != "mandatory":
            continue
        if item["applicable_stages"] != ["project_documentation"]:
            continue
        if item["applicable_document_types"] == ["specification_only"]:
            continue
        ctx = _ctx(document_type="specification_only")
        assert can_report_missing(item, ctx) is False, item["item_id"]


def test_real_cannot_report_items_have_no_pathway(all_items):
    """46+ cannot_report items must all return False in any context."""
    n_blocked = 0
    for item in all_items:
        if item["can_be_reported_as_missing"]:
            continue
        ctx = _ctx(
            document_type=item["applicable_document_types"][0]
            if item["applicable_document_types"]
            else "full_rd",
            stage="project_documentation",
            detected_object_signals={s: True for s in item["object_signals"]},
            md_count=5,
            cross_md_pipeline=True,
            shadow_mode=True,
        )
        assert can_report_missing(item, ctx) is False, item["item_id"]
        n_blocked += 1
    assert n_blocked >= 46
