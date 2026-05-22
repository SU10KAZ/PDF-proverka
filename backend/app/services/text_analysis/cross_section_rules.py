"""Cross-section rules (Phase 1 scaffolding, P0 safety layer).

Forbids the future `completeness_runner` from generating
«missing cross-discipline requirement» findings when the pipeline has no
cross-MD context.

This module is NOT wired into any runtime. The runner does not exist yet.

Rules (from `normative_checklist_research/final_report.md` §7 and §"Где
высокий риск FP"):

  - If `requires_cross_section=true` in item metadata, the item MUST NOT
    generate a missing finding in a single-MD pipeline.
  - MULTI cross-section items (MULTI-05..MULTI-13) must not surface even
    if the runner sees suggestive content — they require two distinct MDs.
  - Coordination artifacts (передача в смежник, закладные, отверстия) are
    process-artifacts often filed as separate letters, never the marker
    itself. They are also blocked here.

Context contract (`Mapping`-like, all keys optional, all values defensive):

    {
        "md_count": int,             # number of MD documents in pipeline
        "available_sections": list,  # discipline codes present
        "discipline": str,           # discipline of *this* item
        "document_type": str,        # for additional gating
    }
"""
from __future__ import annotations

from typing import Mapping, Optional


# Minimal contexts where cross-section comparisons are valid.
MIN_MD_COUNT_FOR_CROSS_SECTION: int = 2
MIN_SECTIONS_FOR_CROSS_SECTION: int = 2


def _ctx_int(context: Mapping[str, object] | None, key: str, default: int = 0) -> int:
    if not context:
        return default
    val = context.get(key)
    if isinstance(val, bool):  # avoid True/False being treated as 1/0
        return default
    if isinstance(val, int):
        return val
    return default


def _ctx_seq(context: Mapping[str, object] | None, key: str) -> list[str]:
    if not context:
        return []
    val = context.get(key)
    if isinstance(val, (list, tuple, set)):
        return [str(v) for v in val if isinstance(v, str) and v.strip()]
    return []


def has_cross_section_context(context: Mapping[str, object] | None) -> bool:
    """True iff pipeline has enough context to compare two sections.

    Requires either:
      - at least MIN_MD_COUNT_FOR_CROSS_SECTION distinct MDs in pipeline, OR
      - at least MIN_SECTIONS_FOR_CROSS_SECTION available_sections covered
        by a single MD (rare case: monolithic ПД том containing several
        sections side-by-side; still risky, so we accept only with extra
        evidence).
    """
    if context is None:
        return False
    md_count = _ctx_int(context, "md_count", default=0)
    sections = _ctx_seq(context, "available_sections")
    if md_count >= MIN_MD_COUNT_FOR_CROSS_SECTION:
        return True
    if len(set(sections)) >= MIN_SECTIONS_FOR_CROSS_SECTION:
        # Single MD with multiple sections — accept only if context flags it.
        # We never auto-trust this; the future cross-MD pipeline will set it.
        return bool(context.get("allow_single_md_cross_section", False))
    return False


def is_cross_section_item(item_metadata: Mapping[str, object] | None) -> bool:
    """True iff the item is a cross-section / coordination item.

    Reads `requires_cross_section` (canonical) first; falls back to
    looking at MULTI items 05..13 by id if the flag is missing.
    """
    if not item_metadata:
        return False
    flag = item_metadata.get("requires_cross_section")
    if isinstance(flag, bool):
        return flag
    # Defensive fallback: MULTI-05..MULTI-13 are cross-section by definition.
    item_id = item_metadata.get("item_id")
    if isinstance(item_id, str) and item_id.startswith("MULTI-"):
        try:
            num = int(item_id.split("-", 1)[1])
        except (ValueError, IndexError):
            return False
        return 5 <= num <= 13
    return False


def can_report_cross_section_missing(
    item_metadata: Mapping[str, object] | None,
    context: Mapping[str, object] | None,
) -> bool:
    """True iff a cross-section item is allowed to report missing.

    Hard rules:
      - Non-cross-section items: this function returns True (not our gate).
      - Cross-section items: require has_cross_section_context(context).
      - Cross-section items in MULTI discipline are *always* blocked
        when no cross-MD pipeline marker is present in context. This is
        the strongest interpretation of final_report.md §"MULTI items с
        requires_cross_section=true не должны попадать в final findings".
    """
    if not is_cross_section_item(item_metadata):
        return True
    if not has_cross_section_context(context):
        return False
    # Even with context, MULTI cross-section items need the explicit
    # cross-MD pipeline marker. The single-MD lens is not allowed to
    # surface them.
    discipline = (item_metadata or {}).get("discipline") if item_metadata else None
    if isinstance(discipline, str) and discipline.upper() == "MULTI":
        return bool((context or {}).get("cross_md_pipeline", False))
    return True


def block_reason_for_cross_section(
    item_metadata: Mapping[str, object] | None,
    context: Mapping[str, object] | None,
) -> Optional[str]:
    """Return a human-readable block reason or None if not blocked.

    Useful for runner diagnostics / shadow logs.
    """
    if not is_cross_section_item(item_metadata):
        return None
    if not has_cross_section_context(context):
        return (
            "cross_section_blocked: single-MD pipeline cannot verify "
            "cross-discipline consistency"
        )
    discipline = (item_metadata or {}).get("discipline") if item_metadata else None
    if isinstance(discipline, str) and discipline.upper() == "MULTI":
        if not (context or {}).get("cross_md_pipeline"):
            return (
                "cross_section_blocked: MULTI cross-section item requires "
                "explicit cross_md_pipeline=True in context"
            )
    return None


__all__ = [
    "MIN_MD_COUNT_FOR_CROSS_SECTION",
    "MIN_SECTIONS_FOR_CROSS_SECTION",
    "has_cross_section_context",
    "is_cross_section_item",
    "can_report_cross_section_missing",
    "block_reason_for_cross_section",
]
