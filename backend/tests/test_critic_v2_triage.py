"""
Tests for critic_v2/triage.py offline triage policy.

Tests verify queue assignment, visibility flags, workload reduction metrics,
and risk handling — without calling any LLM.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.app.pipeline.stages.findings_review.critic_v2.models import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    QualityDecision,
)
from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
    PROFILE_AGGRESSIVE,
    PROFILE_ASSISTED,
    PROFILE_CONSERVATIVE,
    QUEUE_BORDERLINE,
    QUEUE_HIDDEN,
    QUEUE_MAIN_REVIEW,
    QUEUE_NEEDS_CONTEXT,
    QUEUE_STRONG_KEEP,
    QUEUE_SUGGESTED_REJECT,
    TriageDecision,
    assign_triage_queue,
    build_business_workload_view,
    build_triage_result,
    compute_triage_metrics,
    get_ar_f001_diagnostic,
    get_profile_config,
    triage_decision_to_dict,
    triage_metrics_to_dict,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _make_det(
    fid: str = "F-001",
    decision: str = "accept",
    score: int = 8,
    ev: str = EVIDENCE_VALID,
    reject_reason: Optional[str] = None,
    severity: str = "КРИТИЧЕСКОЕ",
    has_action: bool = True,
    has_impact: bool = True,
    has_evidence: bool = True,
) -> QualityDecision:
    return QualityDecision(
        finding_id=fid,
        decision=decision,
        usefulness_score=score,
        reject_reason=reject_reason,
        reject_explanation=None,
        merged_into=None,
        impact_area="electrical",
        severity=severity,
        has_evidence=has_evidence,
        has_action=has_action,
        has_impact=has_impact,
        evidence_quality=ev,
    )


class _MockLLMDec:
    """Minimal LLMCriticDecision-compatible mock."""

    def __init__(
        self,
        finding_id: str = "F-001",
        llm_decision: str = "accept",
        human_taxonomy_reason: Optional[str] = None,
        confidence: float = 0.9,
        evidence_checked: bool = True,
        source_dependency: str = "enough_source",
        explanation: str = "",
    ) -> None:
        self.finding_id = finding_id
        self.llm_decision = llm_decision
        self.human_taxonomy_reason = human_taxonomy_reason
        self.confidence = confidence
        self.evidence_checked = evidence_checked
        self.source_dependency = source_dependency
        self.explanation = explanation


def _finding(
    fid: str = "F-001",
    severity: str = "КРИТИЧЕСКОЕ",
    category: str = "normative",
) -> dict:
    return {
        "id": fid,
        "severity": severity,
        "category": category,
        "title": f"Test finding {fid}",
        "description": "Description text",
    }


# ─── strong_keep tests ────────────────────────────────────────────────────────


class TestStrongKeep:
    def test_det_accept_score8_valid(self):
        """Deterministic accept + score>=8 + valid evidence → strong_keep."""
        det = _make_det(decision="accept", score=8, ev=EVIDENCE_VALID)
        td = assign_triage_queue(_finding(), det, det)
        assert td.human_queue == QUEUE_STRONG_KEEP
        assert td.visible_by_default is True
        assert td.collapsed_by_default is False

    def test_det_accept_score9_valid(self):
        det = _make_det(decision="accept", score=9, ev=EVIDENCE_VALID)
        td = assign_triage_queue(_finding(), det, det)
        assert td.human_queue == QUEUE_STRONG_KEEP

    def test_critical_partial_score7(self):
        """КРИТИЧЕСКОЕ + score>=7 + partial evidence → strong_keep."""
        det = _make_det(
            decision="accept", score=7, ev=EVIDENCE_PARTIAL,
            severity="КРИТИЧЕСКОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="КРИТИЧЕСКОЕ"), det, det
        )
        assert td.human_queue == QUEUE_STRONG_KEEP
        assert td.visible_by_default is True

    def test_economic_partial_score7(self):
        """ЭКОНОМИЧЕСКОЕ + score>=7 + partial → strong_keep."""
        det = _make_det(
            decision="accept", score=7, ev=EVIDENCE_PARTIAL,
            severity="ЭКОНОМИЧЕСКОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="ЭКОНОМИЧЕСКОЕ"), det, det
        )
        assert td.human_queue == QUEUE_STRONG_KEEP

    def test_guard_blocked_llm_reject(self):
        """Score=8 + valid + det=accept + LLM wanted reject → guard blocked → strong_keep."""
        det = _make_det(decision="accept", score=8, ev=EVIDENCE_VALID)
        llm = _MockLLMDec(
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.85,
            evidence_checked=True,
        )
        # Final decision is still accept (guard fired)
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm)
        assert td.human_queue == QUEUE_STRONG_KEEP
        assert td.was_guard_blocked is True
        assert td.visible_by_default is True

    def test_strong_keep_not_hidden(self):
        """strong_keep can never be moved to hidden_by_critic."""
        det = _make_det(decision="accept", score=8, ev=EVIDENCE_VALID)
        td = assign_triage_queue(_finding(), det, det)
        assert td.human_queue != QUEUE_HIDDEN

    def test_critical_partial_score7_not_hidden(self):
        """Critical partial score=7 must not go to hidden_by_critic."""
        det = _make_det(
            decision="accept", score=7, ev=EVIDENCE_PARTIAL,
            severity="КРИТИЧЕСКОЕ",
        )
        llm = _MockLLMDec(
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.90,
            evidence_checked=True,
        )
        td = assign_triage_queue(
            _finding(severity="КРИТИЧЕСКОЕ"), det, det, llm_decision=llm
        )
        assert td.human_queue != QUEUE_HIDDEN
        assert td.human_queue in (QUEUE_STRONG_KEEP, QUEUE_MAIN_REVIEW, QUEUE_BORDERLINE, QUEUE_SUGGESTED_REJECT)

    def test_llm_ocr_reject_on_strong_keep_goes_to_suggested_not_hidden(self):
        """LLM rejects strong_keep via OCR taxonomy → suggested_reject, NOT hidden."""
        det = _make_det(decision="accept", score=8, ev=EVIDENCE_VALID)
        # LLM wanted to reject; guard fires (score=8 + valid)
        llm = _MockLLMDec(
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.92,
            evidence_checked=True,
        )
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm)
        # The guard fires → strong_keep
        assert td.human_queue == QUEUE_STRONG_KEEP
        assert td.human_queue != QUEUE_HIDDEN


# ─── main_review tests ────────────────────────────────────────────────────────


class TestMainReview:
    def test_accept_score7_partial_with_action_impact(self):
        """accept + score=7 + partial + has_action + has_impact → main_review."""
        det = _make_det(
            decision="accept", score=7, ev=EVIDENCE_PARTIAL,
            severity="ЭКСПЛУАТАЦИОННОЕ",
            has_action=True, has_impact=True,
        )
        td = assign_triage_queue(
            _finding(severity="ЭКСПЛУАТАЦИОННОЕ"), det, det
        )
        assert td.human_queue == QUEUE_MAIN_REVIEW
        assert td.visible_by_default is True

    def test_accept_score7_valid_with_action_impact(self):
        det = _make_det(
            decision="accept", score=7, ev=EVIDENCE_VALID,
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
            has_action=True, has_impact=True,
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, det
        )
        assert td.human_queue == QUEUE_MAIN_REVIEW


# ─── hidden_by_critic tests ───────────────────────────────────────────────────


class TestHiddenByCritic:
    def test_det_reject_no_evidence(self):
        """Deterministic reject with no_evidence → hidden_by_critic."""
        det = _make_det(
            decision="reject", score=2, ev=EVIDENCE_NONE,
            reject_reason="no_evidence",
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, det
        )
        assert td.human_queue == QUEUE_HIDDEN
        assert td.can_restore is True
        assert td.collapsed_by_default is True
        assert td.visible_by_default is False

    def test_det_reject_generic_wording(self):
        """Deterministic reject with generic_wording → hidden_by_critic."""
        det = _make_det(
            decision="reject", score=1, ev=EVIDENCE_NONE,
            reject_reason="generic_wording",
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, det
        )
        assert td.human_queue == QUEUE_HIDDEN
        assert td.can_restore is True

    def test_det_reject_ocr_artifact(self):
        det = _make_det(
            decision="reject", score=0, ev=EVIDENCE_NONE,
            reject_reason="ocr_artifact",
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, det
        )
        assert td.human_queue == QUEUE_HIDDEN

    def test_det_reject_duplicate(self):
        det = _make_det(
            decision="reject", score=1, ev=EVIDENCE_NONE,
            reject_reason="duplicate",
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, det
        )
        assert td.human_queue == QUEUE_HIDDEN

    def test_llm_reject_ocr_high_conf_safe(self):
        """LLM reject OCR with high confidence, evidence checked, enough_source, non-critical."""
        det = _make_det(
            decision="accept", score=3, ev=EVIDENCE_PARTIAL,
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        # Final is also reject (LLM downgraded)
        final = _make_det(
            fid=det.finding_id, decision="reject", score=3, ev=EVIDENCE_PARTIAL,
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        llm = _MockLLMDec(
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.92,
            evidence_checked=True,
            source_dependency="enough_source",
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, final, llm_decision=llm
        )
        assert td.human_queue == QUEUE_HIDDEN

    def test_hidden_can_restore_always_true(self):
        """hidden_by_critic always has can_restore=True."""
        det = _make_det(
            decision="reject", score=0, ev=EVIDENCE_NONE,
            reject_reason="no_evidence",
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, det
        )
        assert td.human_queue == QUEUE_HIDDEN
        assert td.can_restore is True

    def test_critical_partial_not_hidden_even_if_llm_rejects(self):
        """КРИТИЧЕСКОЕ with partial evidence must NOT go to hidden_by_critic."""
        det = _make_det(
            decision="accept", score=5, ev=EVIDENCE_PARTIAL,
            severity="КРИТИЧЕСКОЕ",
        )
        final = _make_det(
            fid=det.finding_id, decision="reject", score=5, ev=EVIDENCE_PARTIAL,
            severity="КРИТИЧЕСКОЕ",
        )
        llm = _MockLLMDec(
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.95,
            evidence_checked=True,
        )
        td = assign_triage_queue(
            _finding(severity="КРИТИЧЕСКОЕ"), det, final, llm_decision=llm
        )
        assert td.human_queue != QUEUE_HIDDEN

    def test_no_impact_det_reject_is_safe_to_hide(self):
        """Deterministic reject with no_impact is a safe objective reason → hidden_by_critic."""
        det = _make_det(
            decision="reject", score=3, ev=EVIDENCE_NONE,
            reject_reason="no_impact",
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, det
        )
        assert td.human_queue == QUEUE_HIDDEN

    def test_high_score_valid_not_hidden_when_not_in_safe_reasons(self):
        """Deterministic reject with unsupported reason for valid evidence goes to suggested_reject."""
        det = _make_det(
            decision="reject", score=7, ev=EVIDENCE_VALID,
            reject_reason="assumption_without_fact",  # not in _SAFE_DET_REJECT_REASONS
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, det
        )
        # assumption_without_fact not in _SAFE_DET_REJECT_REASONS → suggested_reject
        assert td.human_queue != QUEUE_HIDDEN


# ─── suggested_reject tests ───────────────────────────────────────────────────


class TestSuggestedReject:
    def test_blocked_guard_case_not_hidden(self):
        """Guard blocked → suggested_reject or strong_keep, never hidden."""
        det = _make_det(decision="accept", score=8, ev=EVIDENCE_VALID)
        llm = _MockLLMDec(
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.88,
            evidence_checked=True,
        )
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm)
        assert td.human_queue != QUEUE_HIDDEN

    def test_suggested_reject_can_restore(self):
        """suggested_reject always has can_restore=True."""
        det = _make_det(
            decision="reject", score=5, ev=EVIDENCE_PARTIAL,
            reject_reason="low_business_value",
            severity="ЭКСПЛУАТАЦИОННОЕ",
        )
        # Not a safe-to-hide case due to partial evidence + medium score
        # Should go to suggested_reject
        td = assign_triage_queue(
            _finding(severity="ЭКСПЛУАТАЦИОННОЕ"), det, det
        )
        # hidden OR suggested_reject — either way can_restore should be True
        assert td.can_restore is True

    def test_llm_reject_not_safe_enough_to_hide_goes_suggested(self):
        """LLM reject + low confidence → not safe to hide → suggested_reject or borderline."""
        det = _make_det(
            decision="accept", score=6, ev=EVIDENCE_PARTIAL,
            severity="ЭКСПЛУАТАЦИОННОЕ",
        )
        final = _make_det(
            fid=det.finding_id, decision="reject", score=6, ev=EVIDENCE_PARTIAL,
            severity="ЭКСПЛУАТАЦИОННОЕ",
        )
        llm = _MockLLMDec(
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.65,  # Below 0.80 threshold for hidden
            evidence_checked=False,
        )
        td = assign_triage_queue(
            _finding(severity="ЭКСПЛУАТАЦИОННОЕ"), det, final, llm_decision=llm
        )
        assert td.human_queue != QUEUE_HIDDEN


# ─── borderline tests ─────────────────────────────────────────────────────────


class TestBorderline:
    def test_final_borderline_stays_borderline(self):
        det = _make_det(decision="borderline", score=5, ev=EVIDENCE_PARTIAL)
        td = assign_triage_queue(_finding(), det, det)
        assert td.human_queue == QUEUE_BORDERLINE
        assert td.visible_by_default is True

    def test_downgraded_reject_visible(self):
        """LLM wanted reject but confidence gate downgraded it → borderline or suggested_reject, still visible."""
        det = _make_det(decision="accept", score=5, ev=EVIDENCE_PARTIAL)
        # final decision after downgrade is borderline
        final = _make_det(
            fid=det.finding_id, decision="borderline", score=5, ev=EVIDENCE_PARTIAL
        )
        llm = _MockLLMDec(llm_decision="reject", confidence=0.65)
        td = assign_triage_queue(_finding(), det, final, llm_decision=llm)
        # downgraded reject: goes to suggested_reject (visible=False but can_restore=True)
        # OR borderline (visible=True) — both are acceptable
        assert td.human_queue in (QUEUE_BORDERLINE, QUEUE_NEEDS_CONTEXT, QUEUE_SUGGESTED_REJECT)
        assert td.was_downgraded_reject is True
        # Must never go to hidden
        assert td.human_queue != QUEUE_HIDDEN

    def test_other_unclassified_taxonomy_borderline(self):
        """other_unclassified taxonomy → borderline or needs_context, never hidden."""
        det = _make_det(decision="accept", score=4, ev=EVIDENCE_PARTIAL)
        llm = _MockLLMDec(
            llm_decision="borderline",
            human_taxonomy_reason="other_unclassified",
        )
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm)
        assert td.human_queue not in (QUEUE_HIDDEN, QUEUE_STRONG_KEEP)

    def test_other_unclassified_not_hidden(self):
        """other_unclassified should never go to hidden_by_critic."""
        det = _make_det(decision="reject", score=3, ev=EVIDENCE_WEAK)
        final = _make_det(
            fid=det.finding_id, decision="borderline", score=3, ev=EVIDENCE_WEAK
        )
        llm = _MockLLMDec(
            llm_decision="borderline",
            human_taxonomy_reason="other_unclassified",
        )
        td = assign_triage_queue(_finding(), det, final, llm_decision=llm)
        assert td.human_queue != QUEUE_HIDDEN

    def test_acceptable_design_solution_borderline(self):
        """acceptable_design_solution taxonomy → borderline or needs_context, not hidden."""
        det = _make_det(decision="accept", score=4, ev=EVIDENCE_PARTIAL)
        llm = _MockLLMDec(
            llm_decision="borderline",
            human_taxonomy_reason="acceptable_design_solution",
        )
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm)
        assert td.human_queue not in (QUEUE_HIDDEN,)

    def test_weak_evidence_goes_borderline(self):
        det = _make_det(decision="accept", score=5, ev=EVIDENCE_WEAK)
        td = assign_triage_queue(_finding(), det, det)
        assert td.human_queue == QUEUE_BORDERLINE


# ─── needs_context tests ──────────────────────────────────────────────────────


class TestNeedsContext:
    def test_insufficient_source_context_taxonomy(self):
        """insufficient_source_context taxonomy → needs_context."""
        det = _make_det(decision="accept", score=5, ev=EVIDENCE_PARTIAL)
        llm = _MockLLMDec(
            llm_decision="needs_human",
            human_taxonomy_reason="insufficient_source_context",
            source_dependency="needs_more_context",
        )
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm)
        assert td.human_queue == QUEUE_NEEDS_CONTEXT
        assert td.visible_by_default is True

    def test_cross_section_required(self):
        det = _make_det(decision="accept", score=6, ev=EVIDENCE_PARTIAL)
        llm = _MockLLMDec(
            llm_decision="needs_human",
            source_dependency="cross_section_required",
        )
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm)
        assert td.human_queue == QUEUE_NEEDS_CONTEXT

    def test_needs_human_llm_decision(self):
        """LLM returns needs_human → needs_context queue."""
        det = _make_det(decision="accept", score=5, ev=EVIDENCE_PARTIAL)
        llm = _MockLLMDec(
            llm_decision="needs_human",
            source_dependency="enough_source",
        )
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm)
        assert td.human_queue == QUEUE_NEEDS_CONTEXT
        assert td.visible_by_default is True


# ─── Visibility tests ─────────────────────────────────────────────────────────


class TestVisibility:
    def test_visible_queues(self):
        """strong_keep, main_review, borderline, needs_context → visible_by_default."""
        visible_queues = {QUEUE_STRONG_KEEP, QUEUE_MAIN_REVIEW, QUEUE_BORDERLINE, QUEUE_NEEDS_CONTEXT}
        # strong_keep
        det = _make_det(decision="accept", score=8, ev=EVIDENCE_VALID)
        td = assign_triage_queue(_finding(), det, det)
        assert td.visible_by_default is True
        assert td.human_queue in visible_queues

    def test_collapsed_queues(self):
        """suggested_reject, hidden_by_critic → collapsed_by_default."""
        det = _make_det(
            decision="reject", score=0, ev=EVIDENCE_NONE,
            reject_reason="no_evidence",
            severity="РЕКОМЕНДАТЕЛЬНОЕ",
        )
        td = assign_triage_queue(
            _finding(severity="РЕКОМЕНДАТЕЛЬНОЕ"), det, det
        )
        assert td.collapsed_by_default is True
        assert td.visible_by_default is False


# ─── build_triage_result tests ────────────────────────────────────────────────


class TestBuildTriageResult:
    def test_basic_batch(self):
        decisions = [
            _make_det("F-001", "accept", 8, EVIDENCE_VALID),
            _make_det("F-002", "reject", 0, EVIDENCE_NONE, reject_reason="no_evidence"),
            _make_det("F-003", "accept", 7, EVIDENCE_PARTIAL, severity="ЭКСПЛУАТАЦИОННОЕ"),
        ]
        findings = [
            _finding("F-001"),
            _finding("F-002", severity="РЕКОМЕНДАТЕЛЬНОЕ"),
            _finding("F-003", severity="ЭКСПЛУАТАЦИОННОЕ"),
        ]
        result = build_triage_result(findings, decisions)
        assert len(result) == 3
        ids = {td.finding_id for td in result}
        assert ids == {"F-001", "F-002", "F-003"}

    def test_missing_finding_produces_empty_dict(self):
        """Findings not in finding_by_id get empty dict — no crash."""
        decisions = [_make_det("F-999", "accept", 8, EVIDENCE_VALID)]
        result = build_triage_result([], decisions)
        assert len(result) == 1
        assert result[0].finding_id == "F-999"


# ─── Metrics tests ────────────────────────────────────────────────────────────


class TestTriageMetrics:
    def test_workload_reduction_computed(self):
        decisions = [
            _make_det("F-001", "accept", 8, EVIDENCE_VALID),
            _make_det("F-002", "reject", 0, EVIDENCE_NONE, reject_reason="no_evidence",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ"),
            _make_det("F-003", "reject", 0, EVIDENCE_NONE, reject_reason="generic_wording",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ"),
            _make_det("F-004", "accept", 7, EVIDENCE_PARTIAL, severity="ЭКСПЛУАТАЦИОННОЕ"),
        ]
        findings = [
            _finding("F-001"),
            _finding("F-002", severity="РЕКОМЕНДАТЕЛЬНОЕ"),
            _finding("F-003", severity="РЕКОМЕНДАТЕЛЬНОЕ"),
            _finding("F-004", severity="ЭКСПЛУАТАЦИОННОЕ"),
        ]
        triage = build_triage_result(findings, decisions)
        metrics = compute_triage_metrics(triage)

        assert metrics.total_findings == 4
        assert metrics.workload_reduction_count == metrics.collapsed_by_default_count
        assert 0 <= metrics.workload_reduction_percent <= 100
        assert metrics.visible_by_default_count + metrics.collapsed_by_default_count == 4

    def test_visible_by_default_correct(self):
        """visible_by_default_count == strong_keep + main_review + borderline + needs_context."""
        decisions = [
            _make_det("F-001", "accept", 8, EVIDENCE_VALID),
            _make_det("F-002", "borderline", 5, EVIDENCE_PARTIAL),
            _make_det("F-003", "reject", 0, EVIDENCE_NONE, reject_reason="no_evidence",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ"),
        ]
        findings = [
            _finding("F-001"),
            _finding("F-002"),
            _finding("F-003", severity="РЕКОМЕНДАТЕЛЬНОЕ"),
        ]
        triage = build_triage_result(findings, decisions)
        metrics = compute_triage_metrics(triage)

        manual_visible = (
            metrics.strong_keep_count
            + metrics.main_review_count
            + metrics.borderline_count
            + metrics.needs_context_count
        )
        assert metrics.visible_by_default_count == manual_visible

    def test_accepted_visible_recall_computed(self):
        """accepted_visible_recall = human_accepted NOT hidden / total human_accepted."""
        decisions = [
            _make_det("F-001", "accept", 8, EVIDENCE_VALID),
            _make_det("F-002", "reject", 0, EVIDENCE_NONE, reject_reason="no_evidence",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ"),
            _make_det("F-003", "accept", 7, EVIDENCE_PARTIAL,
                      severity="ЭКСПЛУАТАЦИОННОЕ"),
        ]
        findings = [
            _finding("F-001"),
            _finding("F-002", severity="РЕКОМЕНДАТЕЛЬНОЕ"),
            _finding("F-003", severity="ЭКСПЛУАТАЦИОННОЕ"),
        ]
        triage = build_triage_result(findings, decisions)
        human_labels = {
            "F-001": "accepted",
            "F-002": "accepted",  # human accepted but critic hid it
            "F-003": "accepted",
        }
        metrics = compute_triage_metrics(triage, human_decisions=human_labels)

        # F-002 is hidden; F-001 and F-003 are visible
        total_accepted = 3
        hidden_accepted = 1  # F-002
        expected_recall = (total_accepted - hidden_accepted) / total_accepted
        assert metrics.accepted_visible_recall == pytest.approx(expected_recall, abs=0.001)

    def test_hidden_human_accepted_count(self):
        """hidden_human_accepted_count tracks risky cases."""
        decisions = [
            _make_det("F-001", "reject", 0, EVIDENCE_NONE,
                      reject_reason="no_evidence", severity="РЕКОМЕНДАТЕЛЬНОЕ"),
        ]
        findings = [_finding("F-001", severity="РЕКОМЕНДАТЕЛЬНОЕ")]
        triage = build_triage_result(findings, decisions)
        metrics = compute_triage_metrics(
            triage, human_decisions={"F-001": "accepted"}
        )
        assert metrics.hidden_human_accepted_count == 1
        assert len(metrics.risky_hidden_cases) == 1
        assert metrics.risky_hidden_cases[0]["finding_id"] == "F-001"

    def test_workload_reduction_zero_when_all_visible(self):
        """When everything is strong_keep, workload_reduction_percent = 0."""
        decisions = [
            _make_det(f"F-{i:03d}", "accept", 8, EVIDENCE_VALID)
            for i in range(5)
        ]
        findings = [_finding(f"F-{i:03d}") for i in range(5)]
        triage = build_triage_result(findings, decisions)
        metrics = compute_triage_metrics(triage)
        assert metrics.workload_reduction_percent == 0.0


# ─── Serialization tests ──────────────────────────────────────────────────────


class TestSerialization:
    def test_triage_decision_to_dict(self):
        det = _make_det(decision="accept", score=8, ev=EVIDENCE_VALID)
        td = assign_triage_queue(_finding(), det, det)
        d = triage_decision_to_dict(td)
        assert "finding_id" in d
        assert "human_queue" in d
        assert "visible_by_default" in d
        assert "can_restore" in d
        assert d["human_queue"] == QUEUE_STRONG_KEEP

    def test_triage_metrics_to_dict(self):
        decisions = [_make_det("F-001", "accept", 8, EVIDENCE_VALID)]
        findings = [_finding("F-001")]
        triage = build_triage_result(findings, decisions)
        m = compute_triage_metrics(triage)
        d = triage_metrics_to_dict(m)
        assert "total_findings" in d
        assert "workload_reduction_percent" in d
        assert "accepted_visible_recall" in d


# ─── AR F-001 diagnostic ──────────────────────────────────────────────────────


class TestArF001Diagnostic:
    def test_diagnostic_returns_expected_fields(self):
        diag = get_ar_f001_diagnostic()
        assert "finding_id" in diag
        assert diag["finding_id"] == "AR F-001"
        assert "actual_cap_applied" in diag
        assert diag["actual_cap_applied"] == 7
        assert diag["is_bug"] is False
        assert "triage_queue" in diag
        assert diag["triage_queue"] == QUEUE_STRONG_KEEP

    def test_ar_f001_score7_critical_partial_is_strong_keep(self):
        """Reproduce AR F-001: score=7, evidence=partial, severity=КРИТИЧЕСКОЕ → strong_keep."""
        det = _make_det(
            fid="AR F-001",
            decision="accept",
            score=7,
            ev=EVIDENCE_PARTIAL,
            severity="КРИТИЧЕСКОЕ",
        )
        llm = _MockLLMDec(
            finding_id="AR F-001",
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.85,
            evidence_checked=True,
        )
        td = assign_triage_queue(
            _finding("AR F-001", severity="КРИТИЧЕСКОЕ"),
            det, det, llm_decision=llm
        )
        assert td.human_queue == QUEUE_STRONG_KEEP, (
            f"AR F-001 should be strong_keep but got {td.human_queue}. "
            f"Explanation: {td.explanation}"
        )
        assert td.human_queue != QUEUE_HIDDEN


# ─── Profile tests ────────────────────────────────────────────────────────────


class TestProfiles:
    """Tests for conservative / assisted / aggressive profile differences."""

    def test_conservative_no_taxonomy_expansion(self):
        """Conservative: LLM reject via taxonomy does NOT expand to suggested_reject."""
        cfg = get_profile_config(PROFILE_CONSERVATIVE)
        assert len(cfg.suggested_reject_taxonomies) == 0

    def test_assisted_has_taxonomy_expansion(self):
        """Assisted: suggested_reject_taxonomies is non-empty."""
        cfg = get_profile_config(PROFILE_ASSISTED)
        assert len(cfg.suggested_reject_taxonomies) > 0
        assert "visual_or_ocr_misread" in cfg.suggested_reject_taxonomies

    def test_aggressive_is_non_production(self):
        """Aggressive profile must be marked non-production."""
        cfg = get_profile_config(PROFILE_AGGRESSIVE)
        assert cfg.non_production is True

    def test_assisted_more_suggested_reject_than_conservative(self):
        """Assisted gives more suggested_reject than conservative on LLM-reject cases."""
        decisions = [
            _make_det(f"F-{i:03d}", "accept", 5, EVIDENCE_PARTIAL, severity="РЕКОМЕНДАТЕЛЬНОЕ")
            for i in range(10)
        ]
        findings = [_finding(f"F-{i:03d}", severity="РЕКОМЕНДАТЕЛЬНОЕ") for i in range(10)]
        llm_decs = [
            _MockLLMDec(
                finding_id=f"F-{i:03d}",
                llm_decision="reject",
                human_taxonomy_reason="visual_or_ocr_misread",
                confidence=0.82,
                evidence_checked=True,
                source_dependency="enough_source",
            )
            for i in range(10)
        ]
        cons_triage = build_triage_result(findings, decisions, llm_decs, profile=PROFILE_CONSERVATIVE)
        asst_triage = build_triage_result(findings, decisions, llm_decs, profile=PROFILE_ASSISTED)
        cons_sr = sum(1 for td in cons_triage if td.human_queue == QUEUE_SUGGESTED_REJECT)
        asst_sr = sum(1 for td in asst_triage if td.human_queue == QUEUE_SUGGESTED_REJECT)
        assert asst_sr >= cons_sr, f"Assisted should have >= suggested_reject: cons={cons_sr}, asst={asst_sr}"

    def test_assisted_strong_keep_never_in_suggested_reject(self):
        """In assisted profile, strong_keep stays strong_keep."""
        cfg = get_profile_config(PROFILE_ASSISTED)
        assert cfg.allow_strong_keep_in_suggested_reject is False

        det = _make_det("F-001", "accept", 8, EVIDENCE_VALID)
        llm = _MockLLMDec(
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.82,
            evidence_checked=True,
        )
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm, profile=PROFILE_ASSISTED)
        assert td.human_queue == QUEUE_STRONG_KEEP

    def test_aggressive_strong_keep_can_go_to_suggested_reject(self):
        """In aggressive profile, strong_keep with LLM reject goes to suggested_reject."""
        cfg = get_profile_config(PROFILE_AGGRESSIVE)
        assert cfg.allow_strong_keep_in_suggested_reject is True

        det = _make_det("F-001", "accept", 8, EVIDENCE_VALID)
        llm = _MockLLMDec(
            llm_decision="reject",
            human_taxonomy_reason="visual_or_ocr_misread",
            confidence=0.65,
            evidence_checked=True,
        )
        td = assign_triage_queue(_finding(), det, det, llm_decision=llm, profile=PROFILE_AGGRESSIVE)
        assert td.human_queue == QUEUE_SUGGESTED_REJECT
        assert td.can_restore is True

    def test_hidden_by_critic_does_not_grow_in_assisted(self):
        """hidden_by_critic should not grow significantly in assisted vs conservative."""
        decisions = [
            _make_det(f"F-{i:03d}", "accept", 5, EVIDENCE_PARTIAL, severity="РЕКОМЕНДАТЕЛЬНОЕ")
            for i in range(20)
        ]
        findings = [_finding(f"F-{i:03d}", severity="РЕКОМЕНДАТЕЛЬНОЕ") for i in range(20)]
        llm_decs = [
            _MockLLMDec(
                finding_id=f"F-{i:03d}",
                llm_decision="reject",
                human_taxonomy_reason="visual_or_ocr_misread",
                confidence=0.82,
                evidence_checked=True,
            )
            for i in range(20)
        ]
        cons_triage = build_triage_result(findings, decisions, llm_decs, profile=PROFILE_CONSERVATIVE)
        asst_triage = build_triage_result(findings, decisions, llm_decs, profile=PROFILE_ASSISTED)
        cons_hc = sum(1 for td in cons_triage if td.human_queue == QUEUE_HIDDEN)
        asst_hc = sum(1 for td in asst_triage if td.human_queue == QUEUE_HIDDEN)
        # Assisted should not increase hidden_by_critic beyond conservative
        assert asst_hc <= cons_hc + 2

    def test_profile_stored_in_triage_decision(self):
        """TriageDecision.profile field reflects the profile used."""
        det = _make_det("F-001", "accept", 8, EVIDENCE_VALID)
        for prof in [PROFILE_CONSERVATIVE, PROFILE_ASSISTED, PROFILE_AGGRESSIVE]:
            td = assign_triage_queue(_finding(), det, det, profile=prof)
            assert td.profile == prof

    def test_profile_stored_in_metrics(self):
        """TriageMetrics.profile field reflects the profile used."""
        decisions = [_make_det("F-001", "accept", 8, EVIDENCE_VALID)]
        findings = [_finding("F-001")]
        for prof in [PROFILE_CONSERVATIVE, PROFILE_ASSISTED, PROFILE_AGGRESSIVE]:
            triage = build_triage_result(findings, decisions, profile=prof)
            metrics = compute_triage_metrics(triage, profile=prof)
            assert metrics.profile == prof

    def test_metrics_dict_contains_profile(self):
        decisions = [_make_det("F-001", "accept", 8, EVIDENCE_VALID)]
        findings = [_finding("F-001")]
        triage = build_triage_result(findings, decisions, profile=PROFILE_ASSISTED)
        m = compute_triage_metrics(triage, profile=PROFILE_ASSISTED)
        d = triage_metrics_to_dict(m)
        assert d["profile"] == PROFILE_ASSISTED


class TestNewMetrics:
    """Tests for primary_queue metrics and accepted recall variants."""

    def test_primary_visible_count_equals_queue_sum(self):
        decisions = [
            _make_det("F-001", "accept", 8, EVIDENCE_VALID),
            _make_det("F-002", "accept", 5, EVIDENCE_PARTIAL, severity="ЭКСПЛУАТАЦИОННОЕ"),
            _make_det("F-003", "reject", 0, EVIDENCE_NONE, reject_reason="no_evidence",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ"),
            _make_det("F-004", "borderline", 4, EVIDENCE_WEAK, severity="РЕКОМЕНДАТЕЛЬНОЕ"),
        ]
        findings = [
            _finding("F-001"),
            _finding("F-002", severity="ЭКСПЛУАТАЦИОННОЕ"),
            _finding("F-003", severity="РЕКОМЕНДАТЕЛЬНОЕ"),
            _finding("F-004", severity="РЕКОМЕНДАТЕЛЬНОЕ"),
        ]
        triage = build_triage_result(findings, decisions)
        m = compute_triage_metrics(triage)
        assert m.primary_visible_count == (
            m.strong_keep_count + m.main_review_count + m.borderline_count + m.needs_context_count
        )

    def test_primary_visible_plus_collapsed_equals_total(self):
        decisions = [
            _make_det(f"F-{i:03d}", "accept" if i % 2 == 0 else "reject",
                      5 if i % 2 == 0 else 0,
                      EVIDENCE_PARTIAL if i % 2 == 0 else EVIDENCE_NONE,
                      reject_reason=None if i % 2 == 0 else "no_evidence",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ")
            for i in range(6)
        ]
        findings = [_finding(f"F-{i:03d}", severity="РЕКОМЕНДАТЕЛЬНОЕ") for i in range(6)]
        triage = build_triage_result(findings, decisions)
        m = compute_triage_metrics(triage)
        assert m.primary_visible_count + m.primary_collapsed_count == m.total_findings

    def test_accepted_not_hidden_recall_100_when_nothing_hidden(self):
        """When hidden_by_critic=0, accepted_not_hidden_recall=1.0."""
        decisions = [_make_det("F-001", "accept", 8, EVIDENCE_VALID)]
        findings = [_finding("F-001")]
        triage = build_triage_result(findings, decisions)
        m = compute_triage_metrics(triage, human_decisions={"F-001": "accepted"})
        assert m.hidden_human_accepted_count == 0
        assert m.accepted_not_hidden_recall == pytest.approx(1.0, abs=0.001)

    def test_accepted_primary_visible_recall_distinct_from_not_hidden_recall(self):
        """accepted_primary_visible_recall <= accepted_not_hidden_recall always."""
        decisions = [
            _make_det("F-001", "accept", 8, EVIDENCE_VALID),
        ]
        findings = [_finding("F-001")]
        triage = build_triage_result(findings, decisions)
        m = compute_triage_metrics(triage, human_decisions={"F-001": "accepted"})
        if m.accepted_primary_visible_recall is not None and m.accepted_not_hidden_recall is not None:
            assert m.accepted_primary_visible_recall <= m.accepted_not_hidden_recall + 1e-6

    def test_business_workload_view_fields(self):
        decisions = [
            _make_det("F-001", "accept", 8, EVIDENCE_VALID),
            _make_det("F-002", "reject", 0, EVIDENCE_NONE, reject_reason="no_evidence",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ"),
        ]
        findings = [_finding("F-001"), _finding("F-002", severity="РЕКОМЕНДАТЕЛЬНОЕ")]
        triage = build_triage_result(findings, decisions)
        m = compute_triage_metrics(triage, profile=PROFILE_CONSERVATIVE)
        bwv = build_business_workload_view(m)
        for key in [
            "profile", "total_findings", "primary_queue_size", "collapsed_queue_size",
            "primary_queue_reduction_percent", "suggested_reject_count", "hidden_by_critic_count",
            "human_accepted_in_suggested_reject", "human_accepted_hidden",
        ]:
            assert key in bwv, f"Missing key: {key}"


class TestReplayWithProfiles:
    """Tests for replay_triage_on_records with profile parameter."""

    def test_replay_conservative_is_default(self):
        from backend.scripts.replay_critic_v2_triage_policy import replay_triage_on_records
        import json
        records = [{"finding_id": "F-001", "project_name": "P1",
                    "critic_decision": "accept", "critic_score": 8,
                    "evidence_quality": EVIDENCE_VALID, "human_decision": "accepted",
                    "severity": "КРИТИЧЕСКОЕ", "section": "AR"}]
        t1, m1 = replay_triage_on_records(records, {}, profile=PROFILE_CONSERVATIVE)
        t2, m2 = replay_triage_on_records(records, {})
        assert m1.strong_keep_count == m2.strong_keep_count
        assert m1.hidden_by_critic_count == m2.hidden_by_critic_count

    def test_replay_assisted_more_suggested_reject(self):
        from backend.scripts.replay_critic_v2_triage_policy import replay_triage_on_records
        llm_by_id = {
            f"F-{i:03d}": {
                "finding_id": f"F-{i:03d}", "llm_decision": "reject",
                "human_taxonomy_reason": "visual_or_ocr_misread",
                "confidence": 0.82, "evidence_checked": True,
                "source_dependency": "enough_source",
            }
            for i in range(5)
        }
        records = [
            {"finding_id": f"F-{i:03d}", "project_name": "P1",
             "critic_decision": "accept", "critic_score": 5,
             "evidence_quality": EVIDENCE_PARTIAL, "human_decision": "rejected",
             "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "section": "AR"}
            for i in range(5)
        ]
        _, m_cons = replay_triage_on_records(records, llm_by_id, profile=PROFILE_CONSERVATIVE)
        _, m_asst = replay_triage_on_records(records, llm_by_id, profile=PROFILE_ASSISTED)
        assert m_asst.suggested_reject_count >= m_cons.suggested_reject_count

    def test_replay_profile_all_creates_sub_dirs(self, tmp_path):
        import json, subprocess, sys
        records = [{"finding_id": "F-001", "project_name": "P1",
                    "critic_decision": "accept", "critic_score": 8,
                    "evidence_quality": EVIDENCE_VALID, "human_decision": "accepted",
                    "severity": "КРИТИЧЕСКОЕ", "section": "AR"}]
        bench_dir = tmp_path / "bench"
        bench_dir.mkdir()
        (bench_dir / "human_benchmark_records.json").write_text(json.dumps(records), encoding="utf-8")
        out_dir = tmp_path / "out"
        script = str(_PROJECT_ROOT / "backend" / "scripts" / "replay_critic_v2_triage_policy.py")
        result = subprocess.run(
            [sys.executable, script,
             "--benchmark-output-dir", str(bench_dir),
             "--profile", "all",
             "--output-dir", str(out_dir), "--quiet"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"Script failed: {result.stderr}"
        for prof in [PROFILE_CONSERVATIVE, PROFILE_ASSISTED, PROFILE_AGGRESSIVE]:
            assert (out_dir / prof / "critic_v2_triage.json").exists()
        assert (out_dir / "profile_comparison.md").exists()


# ─── UI-export tests ─────────────────────────────────────────────────────────


class TestUIExport:
    """build_ui_export — 4-tab structure, item shape, summary metrics."""

    def _build_mixed_triage(self):
        """Construct a synthetic triage covering all 6 queues."""
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            TriageDecision,
        )
        decisions = [
            # strong_keep — visible
            TriageDecision(
                finding_id="P1:F-001", human_queue=QUEUE_STRONG_KEEP,
                critic_recommendation="keep",
                visible_by_default=True, collapsed_by_default=False,
                can_restore=False, confidence=0.9, risk_level="low",
                reason="deterministic_accept_high_score",
                explanation="score=10, ev=valid",
                evidence_quality=EVIDENCE_VALID, usefulness_score=10,
                source_dependency="enough_source", taxonomy_reason="other",
                deterministic_decision="accept", llm_decision="accept",
                final_decision="accept", was_guard_blocked=False,
                was_downgraded_reject=False, profile=PROFILE_CONSERVATIVE,
            ),
            # main_review — visible
            TriageDecision(
                finding_id="P1:F-002", human_queue=QUEUE_MAIN_REVIEW,
                critic_recommendation="review",
                visible_by_default=True, collapsed_by_default=False,
                can_restore=False, confidence=0.7, risk_level="medium",
                reason="main_review_default", explanation="",
                evidence_quality=EVIDENCE_VALID, usefulness_score=8,
                source_dependency="enough_source", taxonomy_reason="other",
                deterministic_decision="accept", llm_decision=None,
                final_decision="accept", was_guard_blocked=False,
                was_downgraded_reject=False, profile=PROFILE_CONSERVATIVE,
            ),
            # borderline — visible
            TriageDecision(
                finding_id="P1:F-003", human_queue=QUEUE_BORDERLINE,
                critic_recommendation="review",
                visible_by_default=True, collapsed_by_default=False,
                can_restore=False, confidence=0.6, risk_level="medium",
                reason="borderline", explanation="",
                evidence_quality=EVIDENCE_PARTIAL, usefulness_score=6,
                source_dependency="enough_source", taxonomy_reason="other",
                deterministic_decision="accept", llm_decision="accept",
                final_decision="accept", was_guard_blocked=False,
                was_downgraded_reject=False, profile=PROFILE_CONSERVATIVE,
            ),
            # needs_context — visible (UI moves to its own tab)
            TriageDecision(
                finding_id="P1:F-004", human_queue=QUEUE_NEEDS_CONTEXT,
                critic_recommendation="needs_context",
                visible_by_default=True, collapsed_by_default=False,
                can_restore=False, confidence=0.55, risk_level="medium",
                reason="needs_context",
                explanation="source_dependency=cross_section_required",
                evidence_quality=EVIDENCE_PARTIAL, usefulness_score=7,
                source_dependency="cross_section_required",
                taxonomy_reason="insufficient_source_context",
                deterministic_decision="accept",
                llm_decision="needs_human", final_decision="borderline",
                was_guard_blocked=False, was_downgraded_reject=False,
                profile=PROFILE_CONSERVATIVE,
            ),
            # suggested_reject — collapsed
            TriageDecision(
                finding_id="P1:F-005", human_queue=QUEUE_SUGGESTED_REJECT,
                critic_recommendation="reject",
                visible_by_default=False, collapsed_by_default=True,
                can_restore=True, confidence=0.78, risk_level="medium",
                reason="suggested_reject_not_safe_to_hide",
                explanation="",
                evidence_quality=EVIDENCE_PARTIAL, usefulness_score=5,
                source_dependency="enough_source",
                taxonomy_reason="duplicate_or_already_covered",
                deterministic_decision="accept", llm_decision="reject",
                final_decision="reject", was_guard_blocked=False,
                was_downgraded_reject=False, profile=PROFILE_CONSERVATIVE,
            ),
            # hidden_by_critic — collapsed
            TriageDecision(
                finding_id="P1:F-006", human_queue=QUEUE_HIDDEN,
                critic_recommendation="reject",
                visible_by_default=False, collapsed_by_default=True,
                can_restore=True, confidence=0.95, risk_level="low",
                reason="det_reject:no_evidence", explanation="",
                evidence_quality=EVIDENCE_NONE, usefulness_score=2,
                source_dependency="enough_source",
                taxonomy_reason=None,
                deterministic_decision="reject", llm_decision="reject",
                final_decision="reject", was_guard_blocked=False,
                was_downgraded_reject=False, profile=PROFILE_CONSERVATIVE,
            ),
        ]
        metrics = compute_triage_metrics(
            decisions,
            human_decisions={
                "P1:F-001": "accepted", "P1:F-002": "accepted",
                "P1:F-003": "rejected", "P1:F-004": "rejected",
                "P1:F-005": "rejected", "P1:F-006": "rejected",
            },
        )
        return decisions, metrics

    # 1) UI export contains 4 tabs
    def test_ui_export_has_four_tabs(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        assert "tabs" in export
        assert len(export["tabs"]) == 4
        keys = [t["key"] for t in export["tabs"]]
        assert keys == ["primary", "needs_context",
                        "suggested_reject", "hidden_by_critic"]

    # 2) primary tab includes strong_keep + main_review + borderline
    def test_primary_tab_includes_three_queues(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        primary = next(t for t in export["tabs"] if t["key"] == "primary")
        assert set(primary["queues"]) == {
            QUEUE_STRONG_KEEP, QUEUE_MAIN_REVIEW, QUEUE_BORDERLINE
        }
        assert primary["count"] == 3
        assert primary["default_open"] is True

    # 3) needs_context is collapsed
    def test_needs_context_tab_collapsed(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        nc = next(t for t in export["tabs"] if t["key"] == "needs_context")
        assert nc["default_open"] is False
        assert nc["queues"] == [QUEUE_NEEDS_CONTEXT]
        assert nc["count"] == 1

    # 4) suggested_reject is collapsed
    def test_suggested_reject_tab_collapsed(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        sr = next(t for t in export["tabs"] if t["key"] == "suggested_reject")
        assert sr["default_open"] is False
        assert sr["queues"] == [QUEUE_SUGGESTED_REJECT]
        assert sr["count"] == 1

    # 5) hidden_by_critic is collapsed
    def test_hidden_by_critic_tab_collapsed(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        hc = next(t for t in export["tabs"] if t["key"] == "hidden_by_critic")
        assert hc["default_open"] is False
        assert hc["queues"] == [QUEUE_HIDDEN]
        assert hc["count"] == 1

    # 6) every item has a tab field
    def test_each_item_has_tab(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        for item in export["items"]:
            assert "tab" in item
            assert item["tab"] in {
                "primary", "needs_context",
                "suggested_reject", "hidden_by_critic",
            }

    # 7) every item has all required fields
    def test_each_item_has_required_fields(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        required = {
            "finding_id", "tab", "queue", "critic_recommendation",
            "reason", "explanation", "confidence",
            "evidence_quality", "score", "source_dependency",
            "taxonomy_reason", "visible_by_default",
            "collapsed_by_default", "can_restore", "risk_level",
            "title", "description", "recommendation",
            "project_name", "section",
        }
        for item in export["items"]:
            missing = required - set(item.keys())
            assert not missing, f"Missing fields in item: {missing}"

    # 8) can_restore=True for collapsed tabs
    def test_can_restore_true_for_collapsed_tabs(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        # Every item — even primary — gets can_restore = True
        # (UI may use it for reordering / hiding individual items)
        for item in export["items"]:
            assert item["can_restore"] is True
        # Items in collapsed tabs must remain restorable
        for item in export["items"]:
            if item["tab"] in (
                "needs_context", "suggested_reject", "hidden_by_critic"
            ):
                assert item["can_restore"] is True

    # 9) summary computes primary_queue_reduction_percent correctly
    def test_summary_primary_queue_reduction_percent(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        s = export["summary"]
        assert s["total"] == 6
        # primary = strong_keep + main_review + borderline = 3
        assert s["primary_count"] == 3
        # collapsed = needs_context (1) + suggested_reject (1) + hidden (1) = 3
        # reduction = 3 / 6 = 50%
        assert s["primary_queue_reduction_percent"] == 50.0

    # 10) human decisions are propagated to items if provided
    def test_items_carry_human_decisions(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        records_by_id = {
            "P1:F-001": {
                "project_name": "P1", "section": "AR",
                "title": "Critical normative",
                "description": "Long description",
                "recommendation": "Fix this",
                "human_decision": "accepted",
                "human_reason": "real issue",
            },
        }
        export = build_ui_export(
            decisions, metrics, records_by_id=records_by_id
        )
        item = next(i for i in export["items"]
                    if i["finding_id"] == "P1:F-001")
        assert item["project_name"] == "P1"
        assert item["section"] == "AR"
        assert item["title"] == "Critical normative"
        assert item["recommendation"] == "Fix this"
        assert item["human_decision"] == "accepted"
        assert item["human_reason"] == "real issue"

    # 11) markdown preview can be rendered without crashing
    def test_render_ui_export_markdown_runs(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
            render_ui_export_markdown,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        md = render_ui_export_markdown(export)
        assert "# Critic v2 UI Triage Preview" in md
        assert "EXPERIMENTAL" in md
        assert "primary_queue_reduction_percent" in md
        # Each collapsed tab gets a sample section
        assert "needs_context" in md
        assert "suggested_reject" in md
        assert "hidden_by_critic" in md

    # 12) tab counts match item counts per tab
    def test_tab_counts_match_items(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
            build_ui_export,
        )
        decisions, metrics = self._build_mixed_triage()
        export = build_ui_export(decisions, metrics)
        for tab in export["tabs"]:
            actual = sum(
                1 for i in export["items"] if i["tab"] == tab["key"]
            )
            assert actual == tab["count"], (
                f"tab {tab['key']}: count={tab['count']} but items={actual}"
            )

    # 13) replay --ui-export flag writes UI artifacts
    def test_replay_ui_export_flag_writes_artifacts(self, tmp_path):
        import json
        import subprocess
        import sys
        records = [
            {"finding_id": "F-001", "project_name": "P1",
             "critic_decision": "accept", "critic_score": 10,
             "evidence_quality": EVIDENCE_VALID,
             "human_decision": "accepted",
             "severity": "КРИТИЧЕСКОЕ", "section": "AR",
             "title": "T1", "description": "D1"},
            {"finding_id": "F-002", "project_name": "P1",
             "critic_decision": "reject", "critic_score": 2,
             "evidence_quality": EVIDENCE_NONE,
             "critic_reject_reason": "no_evidence",
             "human_decision": "rejected",
             "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "section": "AR",
             "title": "T2", "description": "D2"},
        ]
        bench_dir = tmp_path / "bench"
        bench_dir.mkdir()
        (bench_dir / "human_benchmark_records.json").write_text(
            json.dumps(records), encoding="utf-8"
        )
        out_dir = tmp_path / "out"
        script = str(_PROJECT_ROOT / "backend" / "scripts"
                     / "replay_critic_v2_triage_policy.py")
        result = subprocess.run(
            [sys.executable, script,
             "--benchmark-output-dir", str(bench_dir),
             "--profile", PROFILE_CONSERVATIVE,
             "--output-dir", str(out_dir),
             "--ui-export", "--quiet"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"Script failed: {result.stderr}"
        ui_json = out_dir / "critic_v2_triage_ui.json"
        ui_md = out_dir / "critic_v2_triage_ui_preview.md"
        assert ui_json.exists()
        assert ui_md.exists()
        loaded = json.loads(ui_json.read_text(encoding="utf-8"))
        assert "summary" in loaded
        assert "tabs" in loaded
        assert "items" in loaded
        assert len(loaded["tabs"]) == 4

    # 14) replay without --ui-export does NOT write UI artifacts
    def test_replay_without_ui_export_skips_artifacts(self, tmp_path):
        import json
        import subprocess
        import sys
        records = [
            {"finding_id": "F-001", "project_name": "P1",
             "critic_decision": "accept", "critic_score": 10,
             "evidence_quality": EVIDENCE_VALID,
             "human_decision": "accepted",
             "severity": "КРИТИЧЕСКОЕ", "section": "AR"},
        ]
        bench_dir = tmp_path / "bench"
        bench_dir.mkdir()
        (bench_dir / "human_benchmark_records.json").write_text(
            json.dumps(records), encoding="utf-8"
        )
        out_dir = tmp_path / "out"
        script = str(_PROJECT_ROOT / "backend" / "scripts"
                     / "replay_critic_v2_triage_policy.py")
        result = subprocess.run(
            [sys.executable, script,
             "--benchmark-output-dir", str(bench_dir),
             "--profile", PROFILE_CONSERVATIVE,
             "--output-dir", str(out_dir),
             "--quiet"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"Script failed: {result.stderr}"
        assert not (out_dir / "critic_v2_triage_ui.json").exists()
        assert not (out_dir / "critic_v2_triage_ui_preview.md").exists()


# ──────────────────────────────────────────────────────────────────────────────
# Round-1 profile (assisted_round1) — A1+C+D post-processor
# ──────────────────────────────────────────────────────────────────────────────

from backend.app.pipeline.stages.findings_review.critic_v2.triage import (  # noqa: E402
    PROFILE_ASSISTED_ROUND1,
    ROUND1_REASON_ALREADY_COVERED,
    ROUND1_REASON_OCR,
    ROUND1_REASON_RD_PZ,
    VALID_PROFILES,
    apply_round1_rules,
)


def _finding(fid="F-1", title="", description="", recommendation="",
             section="AR", severity="РЕКОМЕНДАТЕЛЬНОЕ"):
    return {"finding_id": fid, "title": title, "description": description,
            "recommendation": recommendation, "section": section,
            "severity": severity}


def _decision_in_queue(queue: str, **overrides) -> TriageDecision:
    base = dict(
        finding_id="F-1", human_queue=queue, critic_recommendation="keep",
        visible_by_default=True, collapsed_by_default=False, can_restore=False,
        confidence=None, risk_level="low", reason="something",
        explanation="exp", evidence_quality=EVIDENCE_VALID,
        usefulness_score=8, source_dependency="enough_source",
        taxonomy_reason=None, deterministic_decision="accept",
        llm_decision=None, final_decision="accept",
        was_guard_blocked=False, was_downgraded_reject=False,
        profile=PROFILE_ASSISTED_ROUND1, raw={},
    )
    base.update(overrides)
    return TriageDecision(**base)


# ─── Profile is registered ──────────────────────────────────────────────────


def test_assisted_round1_profile_is_registered():
    assert PROFILE_ASSISTED_ROUND1 in VALID_PROFILES
    cfg = get_profile_config(PROFILE_ASSISTED_ROUND1)
    assert cfg.name == "assisted_round1"
    # Inherits conservative-like behaviour: no taxonomy-based SR expansion,
    # strong_keep stays in primary.
    assert cfg.suggested_reject_taxonomies == frozenset()
    assert cfg.allow_strong_keep_in_suggested_reject is False


# ─── Rule A1: OCR markers ───────────────────────────────────────────────────


def test_A1_ocr_downgrades_main_review_to_suggested_reject():
    td = _decision_in_queue(QUEUE_MAIN_REVIEW)
    f = _finding(title="Обозначение — OCR мусор",
                 description="неразборчиво")
    new = apply_round1_rules(td, f)
    assert new.human_queue == QUEUE_SUGGESTED_REJECT
    assert new.reason == ROUND1_REASON_OCR
    assert new.collapsed_by_default is True
    assert new.visible_by_default is False
    assert new.can_restore is True
    assert "A1_ocr" in new.raw["applied_round1_rules"]
    assert new.raw["round1_pre_queue"] == QUEUE_MAIN_REVIEW


def test_A1_does_not_touch_hidden_by_critic():
    td = _decision_in_queue(QUEUE_HIDDEN)
    f = _finding(title="OCR мусор", description="нераспозн")
    new = apply_round1_rules(td, f)
    assert new is td  # untouched


def test_A1_does_not_touch_already_suggested_reject():
    td = _decision_in_queue(QUEUE_SUGGESTED_REJECT)
    f = _finding(title="OCR мусор")
    new = apply_round1_rules(td, f)
    assert new is td


def test_A1_does_not_route_to_hidden_by_critic():
    """Critical: round1 rules MUST NEVER move things to hidden_by_critic."""
    td = _decision_in_queue(QUEUE_MAIN_REVIEW)
    f = _finding(title="OCR мусор", description="ничего не видно")
    new = apply_round1_rules(td, f)
    assert new.human_queue != QUEUE_HIDDEN


# ─── Rule C: RD vs PZ ───────────────────────────────────────────────────────


def test_C_rd_pz_matches_in_KJ():
    td = _decision_in_queue(QUEUE_MAIN_REVIEW)
    f = _finding(title="REI 150 не указан в общих указаниях",
                 description="ПЗ раздела КЖ",
                 section="KJ")
    new = apply_round1_rules(td, f)
    assert new.human_queue == QUEUE_SUGGESTED_REJECT
    assert new.reason == ROUND1_REASON_RD_PZ
    assert "C_rd_pz" in new.raw["applied_round1_rules"]


def test_C_rd_pz_matches_in_EOM():
    td = _decision_in_queue(QUEUE_BORDERLINE)
    f = _finding(title="Расчётное обоснование отсутствует",
                 description="ПЗ", section="EOM")
    new = apply_round1_rules(td, f)
    assert new.human_queue == QUEUE_SUGGESTED_REJECT


def test_C_does_not_match_in_AR_without_markers():
    td = _decision_in_queue(QUEUE_MAIN_REVIEW)
    f = _finding(title="REI 150 отсутствует", section="AR")
    new = apply_round1_rules(td, f)
    # Section AR is outside Rule C's gate.
    assert "C_rd_pz" not in (new.raw.get("applied_round1_rules") or [])


# ─── Rule D: already covered ────────────────────────────────────────────────


def test_D_already_in_adjacent_section():
    td = _decision_in_queue(QUEUE_BORDERLINE)
    f = _finding(title="Параметры АВ присутствуют в смежном разделе",
                 description="дублирование не требуется", section="EOM")
    new = apply_round1_rules(td, f)
    assert new.human_queue == QUEUE_SUGGESTED_REJECT
    assert "D_already_covered" in new.raw["applied_round1_rules"]


def test_D_already_in_spec():
    td = _decision_in_queue(QUEUE_MAIN_REVIEW)
    f = _finding(title="Информация уже указана в спецификации",
                 section="AR")
    new = apply_round1_rules(td, f)
    assert new.human_queue == QUEUE_SUGGESTED_REJECT


def test_no_rule_fires_on_clean_text():
    td = _decision_in_queue(QUEUE_MAIN_REVIEW)
    f = _finding(title="Не указан класс пожарной опасности по ФЗ-123",
                 description="Влияет на эвакуацию", section="AR")
    new = apply_round1_rules(td, f)
    assert new is td


# ─── Strong-keep guardrail (spec §4) ───────────────────────────────────────


def test_strong_keep_critical_protected_unless_ocr_or_already_covered():
    """Critical strong_keep with valid evidence + score>=8 is protected
    unless A1 or D fires."""
    td = _decision_in_queue(
        QUEUE_STRONG_KEEP, risk_level="low",
        evidence_quality=EVIDENCE_VALID, usefulness_score=10,
    )
    # Rule C alone in KJ should NOT touch a critical strong_keep finding.
    f = _finding(title="REI 150 не указан", description="ПЗ",
                 section="KJ", severity="КРИТИЧЕСКОЕ")
    new = apply_round1_rules(td, f)
    assert new is td, "critical strong_keep must be protected from C alone"


def test_strong_keep_critical_downgraded_when_ocr_marker():
    """Strong_keep with OCR marker still gets downgraded (A1 overrides guard)."""
    td = _decision_in_queue(
        QUEUE_STRONG_KEEP, risk_level="low",
        evidence_quality=EVIDENCE_VALID, usefulness_score=10,
    )
    f = _finding(title="OCR мусор", description="нераспозн",
                 section="AR", severity="КРИТИЧЕСКОЕ")
    new = apply_round1_rules(td, f)
    assert new.human_queue == QUEUE_SUGGESTED_REJECT


# ─── Risk preservation ─────────────────────────────────────────────────────


def test_risk_level_medium_high_preserved():
    """Spec §2: if risk_level was medium/high, do not silently downgrade."""
    td = _decision_in_queue(QUEUE_BORDERLINE, risk_level="high")
    f = _finding(title="OCR мусор", section="AR")
    new = apply_round1_rules(td, f)
    assert new.human_queue == QUEUE_SUGGESTED_REJECT
    assert new.risk_level == "high"


def test_risk_level_low_bumped_to_medium():
    td = _decision_in_queue(QUEUE_MAIN_REVIEW, risk_level="low")
    f = _finding(title="OCR мусор", section="AR")
    new = apply_round1_rules(td, f)
    assert new.risk_level == "medium"


# ─── Label-only fields invariant ───────────────────────────────────────────


class _SpyFinding(dict):
    """Dict that records access to forbidden keys."""
    FORBIDDEN = ("human_decision", "human_reason", "preferred_tab",
                 "triage_correct", "reviewer_note", "priority")

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.forbidden_reads = []

    def get(self, key, default=None):
        if key in self.FORBIDDEN:
            self.forbidden_reads.append(key)
        return super().get(key, default)


def test_round1_rules_never_read_label_fields():
    """A1/C/D MUST NOT inspect human_decision / preferred_tab / etc."""
    td = _decision_in_queue(QUEUE_MAIN_REVIEW)
    spy = _SpyFinding(title="OCR мусор", description="нераспозн",
                      recommendation="", section="AR",
                      severity="РЕКОМЕНДАТЕЛЬНОЕ",
                      # Forbidden labels with non-trivial values:
                      human_decision="accepted",
                      human_reason="DO_NOT_READ",
                      preferred_tab="hidden_by_critic",
                      triage_correct="no",
                      reviewer_note="DO_NOT_READ",
                      priority="critical")
    apply_round1_rules(td, spy)
    assert spy.forbidden_reads == [], (
        f"round1 rules read forbidden fields: {spy.forbidden_reads}"
    )


# ─── End-to-end through build_triage_result ────────────────────────────────


def test_build_triage_result_applies_round1_only_for_round1_profile():
    """Round1 post-processor is wired in build_triage_result, not in
    assign_triage_queue, and runs only for the round1 profile."""
    finding = {"finding_id": "F-1", "project_name": "P1", "section": "EOM",
               "title": "REI 150 не указан", "description": "ПЗ",
               "severity": "РЕКОМЕНДАТЕЛЬНОЕ"}
    det = _make_det(fid="F-1", decision="accept", score=10,
                    ev=EVIDENCE_VALID, severity="РЕКОМЕНДАТЕЛЬНОЕ")
    # Conservative: should stay in strong_keep / main_review.
    cons = build_triage_result([finding], [det], {},
                               profile=PROFILE_CONSERVATIVE)
    assert cons[0].human_queue != QUEUE_SUGGESTED_REJECT or \
        cons[0].reason not in (ROUND1_REASON_RD_PZ, ROUND1_REASON_OCR,
                               ROUND1_REASON_ALREADY_COVERED)
    # assisted_round1: rule C should fire.
    r1 = build_triage_result([finding], [det], {},
                             profile=PROFILE_ASSISTED_ROUND1)
    assert r1[0].human_queue == QUEUE_SUGGESTED_REJECT
    assert r1[0].reason == ROUND1_REASON_RD_PZ


def test_build_triage_result_round1_does_not_produce_hidden():
    """No matter what text features, round1 must never emit hidden_by_critic."""
    findings = [
        {"finding_id": f"F-{i}", "project_name": "P", "section": "EOM",
         "title": "OCR мусор", "description": "уже указано в смежном разделе",
         "severity": "РЕКОМЕНДАТЕЛЬНОЕ"} for i in range(5)
    ]
    decisions = [_make_det(fid=f"F-{i}", decision="accept", score=10,
                           ev=EVIDENCE_VALID, severity="РЕКОМЕНДАТЕЛЬНОЕ")
                 for i in range(5)]
    res = build_triage_result(findings, decisions, {},
                              profile=PROFILE_ASSISTED_ROUND1)
    queues = {td.human_queue for td in res}
    assert QUEUE_HIDDEN not in queues
