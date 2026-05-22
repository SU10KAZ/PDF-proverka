"""Stage gates (Phase 1 scaffolding, P0 safety layer).

Determines whether a checklist item is applicable to the current document
stage. Rules per `normative_checklist_research/final_report.md` §6-§9:

  - `project_documentation` (ПД) — full project documentation per ПП РФ 87.
  - `working_documentation` (РД) — рабочая документация: particular mark
    (АР-К3, ЭМ-К3 etc.). Stage attributes from ПД (ПЗ, ТЭП, общие данные)
    may legitimately live in the ПД volume rather than the РД sheet.
  - `detailing` (КМД) — деталировочные чертежи металлоконструкций.
  - `mixed` — document mixes ПД-style and РД-style content (not yet a
    clean detect signal, but the gate must not error out).
  - `unknown` — stage could not be determined. Safe fallback: any
    ПД-only item is downgraded / shadow-only.

This module is NOT wired into any runtime.

The pivot rules:
  - never auto-classify a ПД-only item as missing in an РД sheet
  - never auto-classify a РД-only item as missing in `specification_only`
  - if stage unknown, ПД-only items are downgraded or routed to shadow
  - if `applicable_stages` is empty or has the "unknown" marker -> not
    applicable (shadow-only safe fallback)

Pure stdlib. Python 3.11+. No LLM, no network.
"""
from __future__ import annotations

from enum import Enum
from typing import Mapping, Optional


class DocumentStage(str, Enum):
    PROJECT_DOCUMENTATION = "project_documentation"
    WORKING_DOCUMENTATION = "working_documentation"
    DETAILING = "detailing"
    MIXED = "mixed"
    UNKNOWN = "unknown"


ALLOWED_STAGES: frozenset[str] = frozenset(s.value for s in DocumentStage)


# Tokens we accept from external inputs (project_info, matrix metadata).
_STAGE_ALIASES: dict[str, DocumentStage] = {
    # Canonical English
    "project_documentation": DocumentStage.PROJECT_DOCUMENTATION,
    "working_documentation": DocumentStage.WORKING_DOCUMENTATION,
    "detailing": DocumentStage.DETAILING,
    "mixed": DocumentStage.MIXED,
    "unknown": DocumentStage.UNKNOWN,
    # Russian acronyms used in the metadata's `applicable_stages_raw`
    "пд": DocumentStage.PROJECT_DOCUMENTATION,
    "рд": DocumentStage.WORKING_DOCUMENTATION,
    "кмд": DocumentStage.DETAILING,
    # Latin-ish abbreviations sometimes seen in project_info
    "pd": DocumentStage.PROJECT_DOCUMENTATION,
    "rd": DocumentStage.WORKING_DOCUMENTATION,
    "kmd": DocumentStage.DETAILING,
    # Single-letter штамп values
    "п": DocumentStage.PROJECT_DOCUMENTATION,
    "р": DocumentStage.WORKING_DOCUMENTATION,
    "p": DocumentStage.PROJECT_DOCUMENTATION,
    "r": DocumentStage.WORKING_DOCUMENTATION,
}


def normalize_stage(value: object) -> DocumentStage:
    """Map any input to a DocumentStage. Unknown / missing -> UNKNOWN.

    Never raises.
    """
    if isinstance(value, DocumentStage):
        return value
    if not isinstance(value, str):
        return DocumentStage.UNKNOWN
    key = value.strip().lower()
    if not key:
        return DocumentStage.UNKNOWN
    return _STAGE_ALIASES.get(key, DocumentStage.UNKNOWN)


def infer_stage_from_metadata(item_metadata: Mapping[str, object]) -> set[DocumentStage]:
    """Return the set of stages this item is applicable to.

    Reads `applicable_stages` (canonical) first; falls back to
    `applicable_stages_raw` (Russian) for legacy callers. An empty list /
    missing field returns an empty set — caller decides what that means.
    """
    if not item_metadata:
        return set()
    raw_canonical = item_metadata.get("applicable_stages")
    raw_russian = item_metadata.get("applicable_stages_raw")
    source = raw_canonical if isinstance(raw_canonical, (list, tuple)) and raw_canonical else raw_russian
    if not isinstance(source, (list, tuple)):
        return set()
    out: set[DocumentStage] = set()
    for token in source:
        stage = normalize_stage(token)
        if stage is not DocumentStage.UNKNOWN:
            out.add(stage)
    return out


def is_stage_applicable(
    item_metadata: Mapping[str, object],
    stage: object,
) -> bool:
    """True iff the item declares the given stage as applicable.

    Special cases:
      - `mixed` stage matches any item that lists *either* ПД or РД
      - `unknown` stage NEVER matches (caller must downgrade or shadow)
      - if item has no declared stages, the gate fails closed (False)
    """
    target = normalize_stage(stage)
    declared = infer_stage_from_metadata(item_metadata)
    if not declared:
        return False
    if target is DocumentStage.UNKNOWN:
        return False
    if target is DocumentStage.MIXED:
        # A mixed document may carry either ПД or РД attributes.
        return bool(
            declared & {
                DocumentStage.PROJECT_DOCUMENTATION,
                DocumentStage.WORKING_DOCUMENTATION,
            }
        )
    return target in declared


def should_downgrade_for_stage(
    item_metadata: Mapping[str, object],
    stage: object,
) -> bool:
    """True iff the item should keep being shown but with downgraded severity.

    Downgrade rules:
      - stage is UNKNOWN and item is ПД-only → downgrade
      - stage is MIXED and item is ПД-only → downgrade (we cannot be sure
        the ПЗ/ТЭП block is in this MD)
      - stage is WORKING_DOCUMENTATION and item is ПД-only → downgrade
    """
    target = normalize_stage(stage)
    declared = infer_stage_from_metadata(item_metadata)
    if not declared:
        return False

    is_pd_only = declared == {DocumentStage.PROJECT_DOCUMENTATION}

    if target is DocumentStage.UNKNOWN and is_pd_only:
        return True
    if target is DocumentStage.MIXED and is_pd_only:
        return True
    if target is DocumentStage.WORKING_DOCUMENTATION and is_pd_only:
        return True
    return False


def should_block_for_stage(
    item_metadata: Mapping[str, object],
    stage: object,
) -> bool:
    """True iff the item must NOT generate a missing finding for this stage.

    Hard block rules (no downgrade — actually drop):
      - stage and declared stages are completely disjoint
      - target stage is UNKNOWN AND item is not pd-only (we can't even
        downgrade meaningfully; default safe = block)
      - declared stages is empty (fail closed)
    """
    declared = infer_stage_from_metadata(item_metadata)
    if not declared:
        return True
    target = normalize_stage(stage)

    # If stage is known and overlaps, do not block here. Downgrade is a
    # separate question.
    if target is not DocumentStage.UNKNOWN:
        if target is DocumentStage.MIXED:
            return not bool(
                declared & {
                    DocumentStage.PROJECT_DOCUMENTATION,
                    DocumentStage.WORKING_DOCUMENTATION,
                }
            )
        return target not in declared

    # target is UNKNOWN. Safe defaults:
    #   - ПД-only items: do not hard-block (downgrade instead).
    #   - everything else: block.
    is_pd_only = declared == {DocumentStage.PROJECT_DOCUMENTATION}
    return not is_pd_only


def should_force_shadow_only_for_stage(
    item_metadata: Mapping[str, object],
    stage: object,
) -> bool:
    """True iff the item should be evaluated only in shadow mode for this stage.

    Shadow rule for stages: when stage is unknown AND the item is
    ПД-only, the runner should observe (shadow) but not surface.
    """
    if normalize_stage(stage) is not DocumentStage.UNKNOWN:
        return False
    declared = infer_stage_from_metadata(item_metadata)
    return declared == {DocumentStage.PROJECT_DOCUMENTATION}


__all__ = [
    "DocumentStage",
    "ALLOWED_STAGES",
    "normalize_stage",
    "infer_stage_from_metadata",
    "is_stage_applicable",
    "should_downgrade_for_stage",
    "should_block_for_stage",
    "should_force_shadow_only_for_stage",
]
