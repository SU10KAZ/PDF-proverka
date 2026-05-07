"""
critic_v2/metrics.py
--------------------
Aggregate metrics computation from a list of QualityDecisions.
"""
from __future__ import annotations

from .models import CriticV2Metrics, QualityDecision


def compute_metrics(decisions: list[QualityDecision]) -> CriticV2Metrics:
    m = CriticV2Metrics(total_input=len(decisions))

    scores = [d.usefulness_score for d in decisions]
    m.average_usefulness_score = (
        round(sum(scores) / len(scores), 2) if scores else 0.0
    )

    rejection_reasons: dict[str, int] = {}

    for d in decisions:
        if d.decision == "accept":
            m.accepted += 1
        elif d.decision == "borderline":
            m.borderline += 1
        elif d.decision == "low_priority":
            m.low_priority += 1
        elif d.decision == "merge":
            m.merged += 1
        elif d.decision == "reject":
            # Distinguish rule-based from score-based rejects
            if d.reject_reason:
                m.rejected_by_rules += 1
                reason = d.reject_reason
            else:
                m.rejected_by_score += 1
                reason = "low_score"
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1

    m.rejection_reasons = rejection_reasons
    return m
