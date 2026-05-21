"""Normative-status type helpers (Phase 1 scaffolding, P0 safety layer).

Pure-stdlib utility module for the P0 checklist safety layer. Models the
normative-status enum and provides deterministic mappings to severity and
reportability behaviour.

This module is NOT wired into any runtime. It exists so the future
`completeness_runner` (not yet created) can apply consistent rules when
loading `backend/app/data/discipline_checklists_metadata/<DISC>.json`.

Status values come from `normative_checklist_research/final_report.md`:

    mandatory                 — applicable unconditionally; safe to report
                                 as missing (after document_type / stage
                                 gate).
    conditionally_mandatory   — applicable only if conditions are met
                                 (stage, object_signal, etc.). Reportable as
                                 missing only if condition fires.
    recommended               — best-practice; low severity; usually not a
                                 missing-finding candidate.
    optional                  — purely optional; never a missing-finding.
    not_applicable            — explicitly out-of-scope for the current
                                 lens; never a missing-finding.

Severity vocabulary matches the existing project convention
(КРИТИЧЕСКОЕ / ЭКСПЛУАТАЦИОННОЕ / ЭКОНОМИЧЕСКОЕ / РЕКОМЕНДАТЕЛЬНОЕ /
ПРОВЕРИТЬ_ПО_СМЕЖНЫМ — see CLAUDE.md "Категории").
"""
from __future__ import annotations

from enum import Enum
from typing import Optional


class NormativeStatus(str, Enum):
    """Canonical normative statuses for checklist items.

    Inherits from str so JSON-dumping just works.
    """

    MANDATORY = "mandatory"
    CONDITIONALLY_MANDATORY = "conditionally_mandatory"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"
    NOT_APPLICABLE = "not_applicable"


ALLOWED_STATUSES: frozenset[str] = frozenset(s.value for s in NormativeStatus)


# Default severity per status. The runner should use the item's
# `severity_policy.default` first; this is only the fallback when policy is
# missing.
_DEFAULT_SEVERITY: dict[NormativeStatus, str] = {
    NormativeStatus.MANDATORY: "ЭКСПЛУАТАЦИОННОЕ",
    NormativeStatus.CONDITIONALLY_MANDATORY: "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ",
    NormativeStatus.RECOMMENDED: "РЕКОМЕНДАТЕЛЬНОЕ",
    NormativeStatus.OPTIONAL: "РЕКОМЕНДАТЕЛЬНОЕ",
    NormativeStatus.NOT_APPLICABLE: "РЕКОМЕНДАТЕЛЬНОЕ",
}


# Default reportability per status (whether an item *of this status alone*
# can be reported as missing, before per-item gates). The actual decision
# always uses the per-item `can_be_reported_as_missing` flag — this is only
# a coarse fallback when metadata is absent.
_DEFAULT_REPORTABILITY: dict[NormativeStatus, bool] = {
    NormativeStatus.MANDATORY: True,
    NormativeStatus.CONDITIONALLY_MANDATORY: True,
    NormativeStatus.RECOMMENDED: False,
    NormativeStatus.OPTIONAL: False,
    NormativeStatus.NOT_APPLICABLE: False,
}


def normalize_normative_status(value: object) -> Optional[NormativeStatus]:
    """Return a `NormativeStatus` for any reasonable input or None.

    Accepts:
      - already a NormativeStatus -> returned as-is
      - str -> lowercased, stripped, matched against enum values
      - None / unrecognised -> None

    Never raises.
    """
    if isinstance(value, NormativeStatus):
        return value
    if not isinstance(value, str):
        return None
    key = value.strip().lower()
    if not key:
        return None
    if key in ALLOWED_STATUSES:
        return NormativeStatus(key)
    return None


def severity_for_status(
    status: object,
    fallback: str = "РЕКОМЕНДАТЕЛЬНОЕ",
) -> str:
    """Return default severity label for a status (or fallback)."""
    s = normalize_normative_status(status)
    if s is None:
        return fallback
    return _DEFAULT_SEVERITY[s]


def reportability_for_status(status: object) -> bool:
    """Return whether items of this status are *by default* reportable as
    missing. The actual decision must still check the item's
    `can_be_reported_as_missing` flag and the gates."""
    s = normalize_normative_status(status)
    if s is None:
        return False
    return _DEFAULT_REPORTABILITY[s]


def is_status_conditionally_required(status: object) -> bool:
    """True iff status == conditionally_mandatory.

    Helper so callers don't import the enum just to do an `==` comparison.
    """
    s = normalize_normative_status(status)
    return s is NormativeStatus.CONDITIONALLY_MANDATORY


def is_status_unconditionally_required(status: object) -> bool:
    """True iff status == mandatory."""
    s = normalize_normative_status(status)
    return s is NormativeStatus.MANDATORY
