"""
critic_v2/rule_filter.py
------------------------
Rule-based prefilter for findings quality.

Deterministic — no LLM. Returns reject_reason or None (= pass to scorer).

Safety bypass (revised):
  КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ with VALID or PARTIAL evidence bypass the
  soft action/impact/generic gates — but NOT the evidence gate itself,
  NOT the speculation gate, and NOT the cosmetic/OCR gates.

  КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ with WEAK or NONE evidence get capped at
  borderline in the scorer — they do NOT get an automatic pass.

Valid-evidence bypass for no_impact gate:
  A finding with VALID evidence AND a concrete measurable fact is granted
  a bypass specifically for Gate 8 (no_impact). The reasoning: if we have
  verified grounding for a concrete fact, lack of a detected impact_area
  label is a classification gap, not a signal that the finding is worthless.
  This prevents false rejects on structural (KJ/КЖ) findings where
  category names like cover_thickness or spec_mismatch were not yet mapped
  to an impact axis.
  This bypass does NOT apply to weak/none evidence or speculative findings.
"""
from __future__ import annotations

import re
from typing import Optional

from .models import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    NormalizedFinding,
)

# ─── Reject reason codes ─────────────────────────────────────────────────────

REJECT_NO_EVIDENCE = "no_evidence"
REJECT_NO_ACTION = "no_action"
REJECT_NO_IMPACT = "no_impact"
REJECT_GENERIC = "generic_wording"
REJECT_ASSUMPTION = "assumption_without_fact"
REJECT_OCR = "ocr_artifact"
REJECT_LOW_VALUE = "low_business_value"
REJECT_COSMETIC = "cosmetic_no_practical_impact"
REJECT_UNSUPPORTED = "unsupported_by_source"

# ─── Severity sets ────────────────────────────────────────────────────────────

_HIGH_SEVERITY = {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"}
_LOW_VALUE_CATEGORIES = {"documentation", "normative_refs", "normative"}

# ─── Pattern sets ────────────────────────────────────────────────────────────

_SPECULATION_PATTERNS = re.compile(
    r"\b(возможно|вероятно|предположительно|может быть|по-видимому"
    r"|possible|probably|potentially|может не соответствовать"
    r"|может оказаться|кажется|похоже|по всей видимости"
    r"|по предварительной оценке|предварительная оценка"
    r"|может свидетельствовать)\b",
    re.IGNORECASE,
)

_GENERIC_PATTERNS = re.compile(
    r"\b(необходимо проверить|требуется проверить|следует проверить"
    r"|рекомендуется проверить|нужно проверить"
    r"|следует уточнить|необходимо уточнить|требуется уточнить"
    r"|рекомендуется уточнить|рекомендуется рассмотреть"
    r"|требует дополнительной проверки|требует уточнения"
    r"|требует проверки|подлежит уточнению"
    r"|рекомендуется обратить внимание|рекомендуется пересмотреть"
    r"|следует рассмотреть|потенциально возможен)\b",
    re.IGNORECASE,
)

_OCR_ARTIFACT_PATTERNS = re.compile(
    r"(нечитаемый символ|нечитаемо|не распознаётся|ocr.{0,20}дефект"
    r"|распознавание|символ не распознан|artifacts?|garbled|unreadable"
    r"|текст не распознан|не поддаётся распознаванию)",
    re.IGNORECASE,
)

_COSMETIC_STAMP_PATTERNS = re.compile(
    r"(год разработки|год в штамп|в угловом штамп|наименование листа"
    r"|название листа|название раздела|в штампе указан год"
    r"|в штампе листа.*год|шифр.*штамп|штамп.*шифр"
    r"|расхождение названия листа|наименование.*штамп|штамп.*наименование)",
    re.IGNORECASE,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _has_concrete_fact(nf: NormalizedFinding) -> bool:
    text = f"{nf.title} {nf.description}"
    if re.search(r"\d", text):
        return True
    if re.search(
        r"\b(не соответствует|не предусмотрен|отсутствует|не обеспечивает"
        r"|противоречит|не совпадает|несоответствие|расхождение|ошибка)\b",
        text, re.IGNORECASE,
    ):
        return True
    return False


def _looks_like_ocr_artifact(nf: NormalizedFinding) -> bool:
    return bool(_OCR_ARTIFACT_PATTERNS.search(f"{nf.title} {nf.description}"))


def _is_cosmetic_stamp_or_name(nf: NormalizedFinding) -> bool:
    combined = f"{nf.title} {nf.description}"
    if not _COSMETIC_STAMP_PATTERNS.search(combined):
        return False
    return nf.category.lower() in ("documentation", "normative_refs", "unknown")


def _is_high_severity_with_credible_evidence(nf: NormalizedFinding) -> bool:
    """
    True only when severity is КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ AND evidence quality
    is PARTIAL or VALID. WEAK or NONE evidence does not grant the bypass.
    """
    return (
        nf.severity.upper() in _HIGH_SEVERITY
        and nf.evidence_quality in (EVIDENCE_PARTIAL, EVIDENCE_VALID)
    )


def _is_valid_evidence_concrete_finding(nf: NormalizedFinding) -> bool:
    """
    True when a finding has VALID evidence AND contains a concrete measurable
    fact. Used as a bypass specifically for Gate 8 (no_impact).

    Rationale: valid evidence + concrete fact means the finding IS grounded
    in reality. A missing impact_area label is a classification gap, not proof
    the finding has no value. We let the scorer decide rather than hard-reject.

    Does NOT bypass: evidence=none/weak, speculation, OCR artifacts, cosmetics.
    """
    if nf.evidence_quality != EVIDENCE_VALID:
        return False
    return _has_concrete_fact(nf)


# ─── Main gate ───────────────────────────────────────────────────────────────

def apply_rule_filter(
    nf: NormalizedFinding,
) -> tuple[Optional[str], Optional[str]]:
    """
    Apply rule-based prefilter.

    Returns:
        (reject_reason, explanation) → rejected
        (None, None)                 → pass to scorer

    Gate order (hard gates first, soft gates after):
      1. OCR artifact                   [hard, always]
      2. Cosmetic stamp/name            [hard, always]
      3. All phantom blocks             [hard, when block index present]
      4. Evidence none                  [hard, no safety bypass for none]
      5. Speculation without fact       [hard, applies even to critical]
      6. Evidence weak (no bypass)      [hard cap → only scorer enforces borderline max]
         → rule_filter passes it through; scorer handles the cap
      7. Action gate                    [soft, bypassed by high+credible evidence]
      8. Impact gate                    [soft, bypassed by high+credible evidence]
      9. Generic wording                [soft, bypassed by high+credible evidence]
     10. Low business value             [soft, not for high severity]
    """
    combined_text = f"{nf.title} {nf.description} {nf.action_required or ''}"
    high_credible = _is_high_severity_with_credible_evidence(nf)

    # ── Gate 1: OCR artifact ───────────────────────────────────────────────────
    if _looks_like_ocr_artifact(nf):
        return REJECT_OCR, "Finding describes an OCR artifact, not a project defect"

    # ── Gate 2: Cosmetic stamp / sheet name ───────────────────────────────────
    if _is_cosmetic_stamp_or_name(nf):
        return REJECT_COSMETIC, (
            "Cosmetic stamp/sheet-name mismatch — no impact on construction or acceptance"
        )

    # ── Gate 3: All phantom blocks (only when block index was provided) ───────
    if (
        nf.evidence_refs
        and nf.phantom_block_ids
        and not nf.verified_block_ids
        and not nf.evidence_quotes
    ):
        # Block index was provided (verified_block_ids set is empty but phantom has entries)
        # — all refs are phantom
        return REJECT_UNSUPPORTED, (
            f"All evidence block_ids are phantom: {nf.phantom_block_ids}. "
            "Finding cannot be verified."
        )

    # ── Gate 4: Evidence none ─────────────────────────────────────────────────
    if nf.evidence_quality == EVIDENCE_NONE:
        return REJECT_NO_EVIDENCE, (
            "No evidence refs, no quotes — finding cannot be grounded"
        )

    # ── Gate 5: Speculation without concrete fact ─────────────────────────────
    # Applies even to КРИТИЧЕСКОЕ — speculation without a proven fact is not a valid finding
    if _SPECULATION_PATTERNS.search(combined_text) and not _has_concrete_fact(nf):
        return REJECT_ASSUMPTION, (
            "Finding uses speculative language without a proven concrete fact"
        )

    # ── Gates 6-10: Soft gates (bypassed only for high+credible evidence) ─────
    valid_concrete = _is_valid_evidence_concrete_finding(nf)

    if not high_credible:

        # Gate 7: No action
        if not nf.action_required:
            return REJECT_NO_ACTION, (
                "No solution/action_required — finding has no actionable outcome"
            )

        # Gate 8: No impact
        # Bypass also when: valid evidence + concrete fact — classification gap,
        # not a signal of worthlessness (covers structural KJ/КЖ categories
        # like cover_thickness, spec_mismatch, reinforcement).
        if not nf.impact_area and not valid_concrete:
            return REJECT_NO_IMPACT, (
                "No impact area detected — no identifiable business axis"
            )

        # Gate 9: Generic wording without fact
        if _GENERIC_PATTERNS.search(combined_text) and not _has_concrete_fact(nf):
            return REJECT_GENERIC, (
                "Generic 'please check' wording without a concrete fact"
            )

        # Gate 10: Low business value for low-severity doc/normative
        # Bypass: VALID evidence + concrete fact → classification gap, not worthlessness.
        # Rationale: a grounded finding about an obsolete norm citation (e.g. cancelled
        # GOST with specific order numbers) carries real compliance/acceptance risk even
        # when impact_area resolves to "normative". The scorer already handles severity
        # weighting; a score≥6 concrete finding with VALID evidence must reach the scorer.
        # This bypass does NOT apply to weak/none evidence or non-concrete findings.
        sev = nf.severity.upper()
        if (
            sev not in _HIGH_SEVERITY
            and nf.category.lower() in _LOW_VALUE_CATEGORIES
            and (not nf.impact_area or nf.impact_area in {"documentation", "normative"})
            and not valid_concrete
        ):
            return REJECT_LOW_VALUE, (
                f"Low-severity '{nf.category}' finding with no business-axis impact"
            )

    return None, None
