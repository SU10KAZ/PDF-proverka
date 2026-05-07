"""
critic_v2/scorer.py
--------------------
Deterministic 0-10 usefulness scorer for findings.

No LLM. Score is based on structural signals only.

Score bands:
  0-4  → reject
  5-6  → borderline or low_priority
  7-10 → accept

Evidence quality hard caps (enforced in score_finding):
  none    → max 4  (reject territory — rule_filter should catch this first)
  weak    → max 5  (borderline at best)
  partial → max 6  (borderline)
  valid   → max 10 (full scoring)

Safety exception for evidence_quality caps:
  КРИТИЧЕСКОЕ  + partial evidence → max 7 (can reach accept territory)
  ЭКОНОМИЧЕСКОЕ + partial evidence → max 7 (can reach accept territory)
  КРИТИЧЕСКОЕ  + weak evidence    → max 5 (stays borderline)
"""
from __future__ import annotations

import re
from .models import EVIDENCE_NONE, EVIDENCE_PARTIAL, EVIDENCE_VALID, EVIDENCE_WEAK, NormalizedFinding

# Severity → score bonus
_SEVERITY_BONUS: dict[str, int] = {
    "КРИТИЧЕСКОЕ": 2,
    "ЭКОНОМИЧЕСКОЕ": 1,
    "ЭКСПЛУАТАЦИОННОЕ": 1,
    "РЕКОМЕНДАТЕЛЬНОЕ": 0,
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ": 0,
}

_HIGH_VALUE_AXES = {"safety", "cost_schedule", "acceptance", "legal"}
_MED_VALUE_AXES = {"construction", "reliability", "operations"}

# Evidence quality → score cap (before severity bonus)
_EVIDENCE_QUALITY_CAP: dict[str, int] = {
    EVIDENCE_NONE: 4,
    EVIDENCE_WEAK: 5,
    EVIDENCE_PARTIAL: 6,
    EVIDENCE_VALID: 10,
}


def _score_concrete_fact(nf: NormalizedFinding) -> int:
    """0-2: Does the finding contain a concrete measurable fact?"""
    text = f"{nf.title} {nf.description}"
    score = 0
    if re.search(r"\d[\d,.]*\s*(?:А|В|кВт|мм|м²|шт|%|кВ|мм²)", text, re.IGNORECASE):
        score += 2
    elif re.search(r"\d", text):
        score += 1
    if re.search(
        r"\b(не соответствует|не предусмотрен|отсутствует|не обеспечивает"
        r"|противоречит|не совпадает|расхождение|вместо|должн[оа])\b",
        text, re.IGNORECASE,
    ):
        score = min(score + 1, 2)
    return min(score, 2)


def _score_evidence(nf: NormalizedFinding) -> int:
    """0-2: Quality of evidence references, aware of evidence_quality."""
    if nf.evidence_quality == EVIDENCE_NONE:
        return 0
    if nf.evidence_quality == EVIDENCE_WEAK:
        return 1  # cap at 1 for weak evidence
    score = 0
    if nf.evidence_refs:
        score += 1
        if len(nf.evidence_refs) >= 2 or nf.verified_block_ids:
            score += 1
    if nf.evidence_quotes and score < 2:
        score += 1
    return min(score, 2)


def _score_impact(nf: NormalizedFinding) -> int:
    """0-2: Relevance of impact area."""
    if not nf.impact_area:
        return 0
    if nf.impact_area in _HIGH_VALUE_AXES:
        return 2
    if nf.impact_area in _MED_VALUE_AXES:
        return 1
    return 0


def _score_action(nf: NormalizedFinding) -> int:
    """0-2: Presence and quality of action_required."""
    if not nf.action_required:
        return 0
    action = nf.action_required.strip()
    if len(action) < 15:
        return 0
    if re.search(
        r"\b(проверить|уточнить|рассмотреть|пересмотреть)\b",
        action, re.IGNORECASE,
    ) and len(action) < 50:
        return 1
    return 2


def _score_not_garbage(nf: NormalizedFinding) -> int:
    """0-2: Absence of generic/speculation/OCR markers."""
    text = f"{nf.title} {nf.description}"
    deductions = 0
    if re.search(
        r"\b(вероятно|возможно|предположительно|может не соответствовать"
        r"|может оказаться|по предварительной оценке|предварительная оценка"
        r"|может свидетельствовать|probably|potentially|possibly)\b",
        text, re.IGNORECASE,
    ):
        deductions += 2
    elif re.search(
        r"\b(необходимо проверить|следует уточнить|рекомендуется проверить"
        r"|требуется проверить|требует уточнения)\b",
        text, re.IGNORECASE,
    ):
        deductions += 1
    if re.search(r"(нечитаемый|не распознаётся|ocr|unreadable)", text, re.IGNORECASE):
        deductions += 2
    if len(nf.description) < 30:
        deductions += 1
    return max(0, 2 - deductions)


def score_finding(nf: NormalizedFinding) -> int:
    """
    Return usefulness score 0-10.

    Applies evidence_quality hard cap before returning.
    Safety exception: КРИТИЧЕСКОЕ or ЭКОНОМИЧЕСКОЕ + partial → cap raised to 7.
    """
    raw_score = (
        _score_concrete_fact(nf)
        + _score_evidence(nf)
        + _score_impact(nf)
        + _score_action(nf)
        + _score_not_garbage(nf)
    )
    sev = nf.severity.upper()
    bonus = _SEVERITY_BONUS.get(sev, 0)
    total = min(raw_score + bonus, 10)

    # Apply evidence quality cap
    base_cap = _EVIDENCE_QUALITY_CAP.get(nf.evidence_quality, 10)

    # Safety exception: КРИТИЧЕСКОЕ or ЭКОНОМИЧЕСКОЕ with partial evidence may reach accept
    if nf.evidence_quality == EVIDENCE_PARTIAL and sev in {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"}:
        cap = 7
    else:
        cap = base_cap

    return min(total, cap)


def score_to_decision(score: int, reject_reason: str | None) -> str:
    """Map numeric score to a decision string."""
    if reject_reason:
        return "reject"
    if score <= 4:
        return "reject"
    if score <= 6:
        return "borderline"
    return "accept"
