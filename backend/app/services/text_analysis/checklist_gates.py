"""Checklist gates (Phase 1 scaffolding, P0 safety layer).

Top-level decision module for "can this checklist item generate a missing
finding right now?". Composes the lower-level gate modules:

  - normative_status.py
  - object_signals.py
  - stage_gates.py
  - cross_section_rules.py

This module is NOT wired into any runtime. The future `completeness_runner`
will read each `backend/app/data/discipline_checklists_metadata/<DISC>.json`
item, build a Context, and call `can_report_missing(item, context)` to
decide whether to surface a finding.

Context (`Mapping`-like, all keys optional, all values defensive):

    {
        "document_type": str,              # full_rd / audit_comparison / ...
        "stage": str,                      # project_documentation / ...
        "discipline": str,
        "md_count": int,                   # for cross-section gate
        "available_sections": list[str],
        "detected_object_signals": dict[str, bool],  # from object_signals.detect_object_signals
        "cross_md_pipeline": bool,         # explicit flag for MULTI items
        "allow_single_md_cross_section": bool,
        "shadow_mode": bool,               # if True, return decisions that
                                           # include shadow-only items
    }

Public API (all functions are pure; no I/O, no LLM):

    is_item_applicable(item_metadata, context) -> bool
    requires_stage_gate(item_metadata) -> bool
    requires_object_signal(item_metadata) -> bool
    can_report_missing(item_metadata, context) -> bool
    requires_cross_section(item_metadata) -> bool
    should_downgrade_severity(item_metadata, context) -> bool
    should_force_shadow_only(item_metadata, context) -> bool
    reportability_reason(item_metadata, context) -> str | None
"""
from __future__ import annotations

from typing import Mapping, Optional

from backend.app.services.text_analysis.cross_section_rules import (
    block_reason_for_cross_section,
    can_report_cross_section_missing,
    is_cross_section_item,
)
from backend.app.services.text_analysis.normative_status import (
    is_status_unconditionally_required,
    normalize_normative_status,
)
from backend.app.services.text_analysis.object_signals import (
    has_required_signals,
    missing_required_signals,
)
from backend.app.services.text_analysis.stage_gates import (
    DocumentStage,
    infer_stage_from_metadata,
    is_stage_applicable,
    normalize_stage,
    should_block_for_stage,
    should_downgrade_for_stage,
    should_force_shadow_only_for_stage,
)


_SUPPORTED_DOCUMENT_TYPES: frozenset[str] = frozenset({
    "full_rd",
    "audit_comparison",
    "tz_vs_rd",
    "specification_only",
})


# --- internal helpers --------------------------------------------------------


def _bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    return default


def _ctx_signals(context: Mapping[str, object] | None) -> dict[str, bool]:
    if not context:
        return {}
    val = context.get("detected_object_signals")
    if isinstance(val, dict):
        return {k: bool(v) for k, v in val.items() if isinstance(k, str)}
    return {}


def _item_document_types(item_metadata: Mapping[str, object] | None) -> set[str]:
    if not item_metadata:
        return set()
    raw = item_metadata.get("applicable_document_types")
    if not isinstance(raw, (list, tuple)):
        return set()
    return {t for t in raw if isinstance(t, str) and t in _SUPPORTED_DOCUMENT_TYPES}


# --- public API --------------------------------------------------------------


def requires_stage_gate(item_metadata: Mapping[str, object] | None) -> bool:
    """True iff the item declares applicable_stages — i.e. stage matters."""
    return bool(infer_stage_from_metadata(item_metadata or {}))


def requires_object_signal(item_metadata: Mapping[str, object] | None) -> bool:
    """True iff the item declares one or more required object_signals."""
    if not item_metadata:
        return False
    raw = item_metadata.get("object_signals")
    if not isinstance(raw, (list, tuple)):
        return False
    return any(isinstance(s, str) and s for s in raw)


def requires_cross_section(item_metadata: Mapping[str, object] | None) -> bool:
    """Convenience re-export for the runner."""
    return is_cross_section_item(item_metadata or {})


def is_item_applicable(
    item_metadata: Mapping[str, object] | None,
    context: Mapping[str, object] | None,
) -> bool:
    """True iff the item is *potentially* applicable in this context.

    "Applicable" is weaker than "can report missing":
      - applicable means stage/document_type/discipline match
      - reportable additionally requires can_be_reported_as_missing, signals
        present, no cross-section block, and not disabled.

    Returns False when:
      - item is disabled_by_default
      - normative_status is `optional` / `not_applicable`
      - document_type is set in context and not in applicable_document_types
      - stage is explicitly blocked by stage gate
      - cross-section item without cross-section context
    """
    if not item_metadata:
        return False
    if _bool(item_metadata.get("disabled_by_default")):
        return False

    status = normalize_normative_status(item_metadata.get("normative_status"))
    if status is None:
        return False
    if status.value in {"optional", "not_applicable"}:
        return False

    # document_type gate.
    doc_type = (context or {}).get("document_type")
    declared_types = _item_document_types(item_metadata)
    if declared_types and isinstance(doc_type, str) and doc_type:
        if doc_type not in declared_types:
            return False

    # stage gate (hard block).
    stage = (context or {}).get("stage")
    if requires_stage_gate(item_metadata):
        if should_block_for_stage(item_metadata, stage):
            # Stage-mismatch hard block — not applicable in this context.
            # We still consider PD-only items in UNKNOWN stage as "applicable
            # but downgrade/shadow" (handled below), so re-check that case.
            if normalize_stage(stage) is DocumentStage.UNKNOWN:
                # PD-only items remain applicable but downgraded; everything
                # else blocked.
                if not should_force_shadow_only_for_stage(item_metadata, stage):
                    return False
            else:
                return False

    # cross-section block.
    if is_cross_section_item(item_metadata):
        # Cross-section items are *applicable for analysis* (context only),
        # but never for reporting in a single-MD pipeline. Applicability
        # here returns True so the runner can still inspect / log them in
        # shadow; report gate handled below.
        return True

    return True


def can_report_missing(
    item_metadata: Mapping[str, object] | None,
    context: Mapping[str, object] | None,
) -> bool:
    """The main gate. True iff runner is allowed to emit a missing finding."""
    if not item_metadata:
        return False
    if _bool(item_metadata.get("disabled_by_default")):
        return False

    # Hard flag from research: 46 items cannot be reported as missing.
    can_report = item_metadata.get("can_be_reported_as_missing")
    if not _bool(can_report, default=False):
        return False

    # Cross-section guard.
    if not can_report_cross_section_missing(item_metadata, context):
        return False

    # Human-validation gate: shadow-only items never surface.
    if _bool(item_metadata.get("requires_human_validation")):
        return False

    # Stage gate: hard block on stage-mismatch, shadow-only for PD-only in UNKNOWN.
    stage = (context or {}).get("stage")
    if requires_stage_gate(item_metadata):
        target = normalize_stage(stage)
        if target is DocumentStage.UNKNOWN:
            # PD-only items in unknown stage → shadow-only → block report.
            if should_force_shadow_only_for_stage(item_metadata, stage):
                return False
            # Non-PD-only items in unknown stage → hard block (cannot safely
            # decide applicability without a stage).
            if should_block_for_stage(item_metadata, stage):
                return False
        elif should_block_for_stage(item_metadata, stage):
            return False

    # document_type gate.
    doc_type = (context or {}).get("document_type")
    declared_types = _item_document_types(item_metadata)
    if declared_types and isinstance(doc_type, str) and doc_type:
        if doc_type not in declared_types:
            return False

    # Object signals.
    if requires_object_signal(item_metadata):
        if not has_required_signals(item_metadata, _ctx_signals(context)):
            return False

    # allow_in_shadow_only items never surface in non-shadow runs.
    if _bool(item_metadata.get("allow_in_shadow_only")) and not _bool(
        (context or {}).get("shadow_mode")
    ):
        # When `requires_human_validation=True` we already returned False
        # above, so this branch handles the rest of the shadow-only items.
        if not is_status_unconditionally_required(item_metadata.get("normative_status")):
            return False

    return True


def should_downgrade_severity(
    item_metadata: Mapping[str, object] | None,
    context: Mapping[str, object] | None,
) -> bool:
    """True iff severity should drop to «ПРОВЕРИТЬ_ПО_СМЕЖНЫМ» (or similar).

    Triggers:
      - stage downgrade rule (PD-only in unknown / mixed / RD)
      - cross-section item that is still "applicable" but cannot report:
        we surface as downgrade for shadow/log
      - object signal partially fired (some required, some missing — not
        currently distinguishable, so we treat any miss as block, not
        downgrade)
    """
    if not item_metadata:
        return False
    stage = (context or {}).get("stage")
    if should_downgrade_for_stage(item_metadata, stage):
        return True
    return False


def should_force_shadow_only(
    item_metadata: Mapping[str, object] | None,
    context: Mapping[str, object] | None,
) -> bool:
    """True iff the runner should only log the decision (shadow), not surface."""
    if not item_metadata:
        return False
    if _bool(item_metadata.get("requires_human_validation")):
        return True
    if _bool(item_metadata.get("allow_in_shadow_only")) and not _bool(
        (context or {}).get("shadow_mode")
    ):
        return True
    if should_force_shadow_only_for_stage(item_metadata, (context or {}).get("stage")):
        return True
    return False


def reportability_reason(
    item_metadata: Mapping[str, object] | None,
    context: Mapping[str, object] | None,
) -> Optional[str]:
    """Return None if reportable; otherwise a human-readable reason string.

    Used by the future runner for diagnostics and shadow-mode logs.
    Walks the gates in the same order as `can_report_missing` and stops at
    the first failing check.
    """
    if not item_metadata:
        return "no_item_metadata"
    if _bool(item_metadata.get("disabled_by_default")):
        reason = item_metadata.get("disabled_reason")
        if isinstance(reason, str) and reason:
            return f"disabled_by_default: {reason}"
        return "disabled_by_default"

    if not _bool(item_metadata.get("can_be_reported_as_missing"), default=False):
        return "can_be_reported_as_missing=false"

    if not can_report_cross_section_missing(item_metadata, context):
        return block_reason_for_cross_section(item_metadata, context)

    if _bool(item_metadata.get("requires_human_validation")):
        return "requires_human_validation=true"

    stage = (context or {}).get("stage")
    if requires_stage_gate(item_metadata):
        target = normalize_stage(stage)
        if target is DocumentStage.UNKNOWN and should_force_shadow_only_for_stage(
            item_metadata, stage
        ):
            return "stage_unknown_and_item_is_pd_only_shadow_only"
        if should_block_for_stage(item_metadata, stage) and target is not DocumentStage.UNKNOWN:
            return f"stage_mismatch: target={target.value}"

    doc_type = (context or {}).get("document_type")
    declared_types = _item_document_types(item_metadata)
    if declared_types and isinstance(doc_type, str) and doc_type:
        if doc_type not in declared_types:
            return f"document_type_mismatch: {doc_type} not in {sorted(declared_types)}"

    if requires_object_signal(item_metadata):
        missing = missing_required_signals(item_metadata, _ctx_signals(context))
        if missing:
            return f"missing_object_signals: {missing}"

    if _bool(item_metadata.get("allow_in_shadow_only")) and not _bool(
        (context or {}).get("shadow_mode")
    ):
        if not is_status_unconditionally_required(item_metadata.get("normative_status")):
            return "allow_in_shadow_only=true_outside_shadow_mode"

    return None


__all__ = [
    "is_item_applicable",
    "requires_stage_gate",
    "requires_object_signal",
    "can_report_missing",
    "requires_cross_section",
    "should_downgrade_severity",
    "should_force_shadow_only",
    "reportability_reason",
]
