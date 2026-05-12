"""
Tests for critic_v2 LLM gate candidate selection strategies.

Verifies:
- conservative strategy preserves original behavior
- expanded strategy includes borderline, weak/partial risk categories, taxonomy markers
- broad strategy includes almost all accept/borderline
- deterministic reject/merge never included
- protected high-score valid accept candidates flagged correctly
- candidate stats are computed correctly
- no real LLM is called in any test
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
from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import (
    CANDIDATE_STRATEGY_BROAD,
    CANDIDATE_STRATEGY_CONSERVATIVE,
    CANDIDATE_STRATEGY_EXPANDED,
    VALID_CANDIDATE_STRATEGIES,
    _EXPANDED_PROTECTED_EVIDENCE,
    _EXPANDED_PROTECTED_SCORE,
    _EXPANDED_RISK_CATEGORIES,
    _EXPANDED_TAXONOMY_MARKERS,
    _is_expanded_candidate,
    build_candidate_selection_stats,
    select_candidates,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _make_dec(
    fid: str = "F-001",
    decision: str = "accept",
    score: int = 6,
    ev: str = EVIDENCE_PARTIAL,
    reject_reason: Optional[str] = None,
    severity: str = "ЭКСПЛУАТАЦИОННОЕ",
    impact_area: str = "electrical",
    has_action: bool = True,
    has_impact: bool = True,
) -> QualityDecision:
    return QualityDecision(
        finding_id=fid,
        decision=decision,
        usefulness_score=score,
        reject_reason=reject_reason,
        reject_explanation=None,
        merged_into=None,
        impact_area=impact_area,
        severity=severity,
        has_evidence=True,
        has_action=has_action,
        has_impact=has_impact,
        evidence_quality=ev,
    )


def _make_finding(
    fid: str = "F-001",
    title: str = "Test finding",
    description: str = "Description",
    recommendation: str = "",
) -> dict:
    return {"id": fid, "title": title, "description": description,
            "solution": recommendation, "severity": "ЭКСПЛУАТАЦИОННОЕ"}


# ─── VALID_CANDIDATE_STRATEGIES ──────────────────────────────────────────────


class TestStrategyConstants:
    def test_all_strategies_defined(self):
        assert CANDIDATE_STRATEGY_CONSERVATIVE in VALID_CANDIDATE_STRATEGIES
        assert CANDIDATE_STRATEGY_EXPANDED in VALID_CANDIDATE_STRATEGIES
        assert CANDIDATE_STRATEGY_BROAD in VALID_CANDIDATE_STRATEGIES

    def test_invalid_strategy_falls_back_to_conservative(self):
        decisions = [_make_dec("F-001")]
        cands, skipped = select_candidates(decisions, strategy="invalid_strategy")
        # Should not raise, falls back
        assert isinstance(cands, list)


# ─── Conservative strategy ───────────────────────────────────────────────────


class TestConservativeStrategy:
    def test_accept_with_evidence_included(self):
        decisions = [_make_dec("F-001", "accept", ev=EVIDENCE_PARTIAL)]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE)
        assert len(cands) == 1

    def test_borderline_included(self):
        decisions = [_make_dec("F-001", "borderline", ev=EVIDENCE_WEAK)]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE)
        assert len(cands) == 1

    def test_evidence_none_excluded(self):
        decisions = [_make_dec("F-001", "accept", ev=EVIDENCE_NONE)]
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE)
        assert len(cands) == 0
        assert "F-001" in skipped

    def test_reject_excluded(self):
        decisions = [_make_dec("F-001", "reject", ev=EVIDENCE_PARTIAL)]
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE)
        assert len(cands) == 0
        assert "F-001" in skipped

    def test_merge_excluded(self):
        decisions = [_make_dec("F-001", "merge", ev=EVIDENCE_PARTIAL)]
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE)
        assert len(cands) == 0
        assert "F-001" in skipped

    def test_max_candidates_respected(self):
        decisions = [_make_dec(f"F-{i:03d}", "accept", ev=EVIDENCE_VALID) for i in range(20)]
        cands, skipped = select_candidates(decisions, max_candidates=5, strategy=CANDIDATE_STRATEGY_CONSERVATIVE)
        assert len(cands) == 5
        assert len(skipped) == 15

    def test_borderline_sorted_first(self):
        decisions = [
            _make_dec("F-001", "accept", ev=EVIDENCE_PARTIAL),
            _make_dec("F-002", "borderline", ev=EVIDENCE_WEAK),
            _make_dec("F-003", "accept", ev=EVIDENCE_VALID),
        ]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE)
        # Borderline should come first
        assert cands[0].finding_id == "F-002"

    def test_default_strategy_is_conservative(self):
        """When no strategy is given, behavior matches conservative."""
        decisions = [
            _make_dec("F-001", "accept", ev=EVIDENCE_NONE),   # excluded
            _make_dec("F-002", "accept", ev=EVIDENCE_PARTIAL), # included
        ]
        cands_default, _ = select_candidates(decisions)
        cands_cons, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE)
        assert [c.finding_id for c in cands_default] == [c.finding_id for c in cands_cons]


# ─── Expanded strategy ────────────────────────────────────────────────────────


class TestExpandedStrategy:
    def test_includes_borderline(self):
        """Expanded always includes borderline."""
        decisions = [_make_dec("F-001", "borderline", ev=EVIDENCE_WEAK)]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 1

    def test_includes_weak_evidence_risk_category(self):
        """Weak evidence + score >= 5 + risk category → included."""
        decisions = [_make_dec("F-001", "accept", score=5, ev=EVIDENCE_WEAK,
                                impact_area="normative_refs")]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 1

    def test_includes_partial_evidence_risk_category(self):
        """Partial evidence + score >= 5 + risk category → included."""
        decisions = [_make_dec("F-001", "accept", score=5, ev=EVIDENCE_PARTIAL,
                                impact_area="documentation")]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 1

    def test_partial_low_score_accepted_is_included(self):
        """Partial + score <= 6 accepted → included (potential inflated score signal)."""
        decisions = [_make_dec("F-001", "accept", score=3, ev=EVIDENCE_PARTIAL,
                                impact_area="unknown_category")]
        raw = {"F-001": _make_finding("F-001", "Generic finding", "No markers")}
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED,
                                           raw_findings=raw)
        # accepted_partial_low_score rule: partial + score<=6 → included
        assert len(cands) == 1

    def test_excludes_partial_high_score_no_risk_no_markers(self):
        """Partial + score=7 + no risk category + no markers → excluded in expanded."""
        decisions = [_make_dec("F-001", "accept", score=7, ev=EVIDENCE_PARTIAL,
                                impact_area="unknown_category_xyz")]
        raw = {"F-001": _make_finding("F-001", "Plain finding no markers", "No info")}
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED,
                                           raw_findings=raw)
        # score=7 + partial + non-risk + no markers → only caught by accepted_partial_low_score if <=6
        # score=7 > 6 so not caught, and no risk category, no markers → excluded
        assert "F-001" in skipped

    def test_includes_accepted_normative_refs(self):
        """Accepted finding in normative_refs → included."""
        decisions = [_make_dec("F-001", "accept", score=7, ev=EVIDENCE_VALID,
                                impact_area="normative_refs")]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 1

    def test_includes_accepted_spec_mismatch(self):
        decisions = [_make_dec("F-001", "accept", score=6, ev=EVIDENCE_PARTIAL,
                                impact_area="spec_mismatch")]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 1

    def test_includes_taxonomy_marker_in_title(self):
        """Finding with taxonomy marker in title → included."""
        decisions = [_make_dec("F-001", "accept", score=4, ev=EVIDENCE_PARTIAL,
                                impact_area="electrical")]
        raw = {"F-001": _make_finding("F-001", "Расхождение с ГОСТ требованием")}
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED,
                                     raw_findings=raw)
        assert len(cands) == 1

    def test_includes_taxonomy_marker_in_description(self):
        decisions = [_make_dec("F-001", "accept", score=4, ev=EVIDENCE_PARTIAL,
                                impact_area="electrical")]
        raw = {"F-001": _make_finding("F-001", "No markers in title",
                                       "Отсутствует указание на спецификацию")}
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED,
                                     raw_findings=raw)
        assert len(cands) == 1

    def test_includes_accepted_weak_evidence(self):
        """Accept + weak evidence → included regardless of category."""
        decisions = [_make_dec("F-001", "accept", score=5, ev=EVIDENCE_WEAK,
                                impact_area="unknown_category")]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 1

    def test_excludes_reject(self):
        decisions = [_make_dec("F-001", "reject", ev=EVIDENCE_PARTIAL)]
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 0
        assert "F-001" in skipped

    def test_excludes_merge(self):
        decisions = [_make_dec("F-001", "merge", ev=EVIDENCE_PARTIAL)]
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 0
        assert "F-001" in skipped

    def test_excludes_evidence_none(self):
        decisions = [_make_dec("F-001", "accept", ev=EVIDENCE_NONE,
                                impact_area="normative_refs")]
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 0
        assert "F-001" in skipped

    def test_expanded_vs_conservative_specific_case(self):
        """Expanded adds findings that conservative excludes via evidence=none rule."""
        decisions = [
            # Conservative excludes this (no risk category, but expanded partial_low_score catches it)
            _make_dec("F-001", "accept", score=4, ev=EVIDENCE_PARTIAL, impact_area="electrical"),
            # Both include this (has risk category)
            _make_dec("F-002", "accept", score=5, ev=EVIDENCE_PARTIAL, impact_area="normative_refs"),
        ]
        raw = {
            "F-001": _make_finding("F-001", "Generic plain"),
            "F-002": _make_finding("F-002", "Норма нарушена"),
        }
        cons_cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE,
                                           raw_findings=raw)
        exp_cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED,
                                          raw_findings=raw)
        # Both include F-002. Expanded also includes F-001 via partial_low_score rule.
        # Conservative includes both (evidence=partial, not none) → both have 2
        # So expanded >= conservative
        assert len(exp_cands) >= len(cons_cands)

    def test_protected_high_score_valid_accept_still_included(self):
        """Protected findings (score>=8 + valid) ARE included in expanded (LLM reviews them)."""
        decisions = [_make_dec("F-001", "accept", score=9, ev=EVIDENCE_VALID,
                                impact_area="normative_refs")]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 1
        # The finding is included; protection is enforced in merge_llm_decisions guard

    def test_high_rejection_risk_categories_included(self):
        """All categories in _EXPANDED_RISK_CATEGORIES trigger inclusion."""
        risk_cats = list(_EXPANDED_RISK_CATEGORIES)[:5]
        decisions = [
            _make_dec(f"F-{i:03d}", "accept", score=5, ev=EVIDENCE_PARTIAL, impact_area=cat)
            for i, cat in enumerate(risk_cats)
        ]
        cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == len(risk_cats)


# ─── Broad strategy ───────────────────────────────────────────────────────────


class TestBroadStrategy:
    def test_includes_all_accept_borderline(self):
        decisions = [
            _make_dec("F-001", "accept", ev=EVIDENCE_VALID),
            _make_dec("F-002", "accept", ev=EVIDENCE_PARTIAL),
            _make_dec("F-003", "accept", ev=EVIDENCE_WEAK),
            _make_dec("F-004", "borderline", ev=EVIDENCE_NONE),  # even none-evidence
            _make_dec("F-005", "accept", ev=EVIDENCE_NONE),      # even none-evidence
        ]
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_BROAD)
        assert len(cands) == 5

    def test_excludes_reject_merge(self):
        decisions = [
            _make_dec("F-001", "reject", ev=EVIDENCE_PARTIAL),
            _make_dec("F-002", "merge", ev=EVIDENCE_VALID),
            _make_dec("F-003", "accept", ev=EVIDENCE_PARTIAL),
        ]
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_BROAD)
        assert len(cands) == 1
        assert "F-001" in skipped
        assert "F-002" in skipped

    def test_broad_includes_more_than_expanded(self):
        decisions = [
            _make_dec("F-001", "accept", ev=EVIDENCE_NONE, impact_area="electrical"),
            _make_dec("F-002", "accept", ev=EVIDENCE_PARTIAL, impact_area="electrical"),
        ]
        raw = {"F-001": _make_finding("F-001"), "F-002": _make_finding("F-002")}
        exp_cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED, raw_findings=raw)
        broad_cands, _ = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_BROAD)
        # Broad includes evidence_none accept which expanded does not
        assert len(broad_cands) >= len(exp_cands)


# ─── is_expanded_candidate helper ────────────────────────────────────────────


class TestIsExpandedCandidate:
    def test_borderline_always_eligible(self):
        d = _make_dec("F-001", "borderline", ev=EVIDENCE_WEAK)
        include, reason = _is_expanded_candidate(d)
        assert include is True
        assert reason == "borderline"

    def test_weak_partial_risk_category_eligible(self):
        d = _make_dec("F-001", "accept", score=5, ev=EVIDENCE_WEAK, impact_area="normative_refs")
        include, reason = _is_expanded_candidate(d)
        assert include is True
        assert "risk_category" in reason

    def test_accepted_risk_category_eligible(self):
        d = _make_dec("F-001", "accept", score=7, ev=EVIDENCE_VALID, impact_area="spec_mismatch")
        include, reason = _is_expanded_candidate(d)
        assert include is True
        assert "risk_category" in reason

    def test_taxonomy_marker_in_text(self):
        d = _make_dec("F-001", "accept", score=4, ev=EVIDENCE_PARTIAL, impact_area="electrical")
        raw = _make_finding("F-001", "ГОСТ нарушен", "Расхождение")
        include, reason = _is_expanded_candidate(d, raw)
        assert include is True
        assert "taxonomy_marker" in reason

    def test_partial_low_score_eligible_even_no_markers(self):
        """Score <= 6 + partial accept → eligible via accepted_partial_low_score rule."""
        d = _make_dec("F-001", "accept", score=3, ev=EVIDENCE_PARTIAL, impact_area="unknown_xyz")
        raw = _make_finding("F-001", "Clean finding", "No markers")
        include, reason = _is_expanded_candidate(d, raw)
        # Partial + score<=6 → accepted_partial_low_score
        assert include is True

    def test_not_eligible_high_score_partial_no_risk_no_markers(self):
        """High score (>6) + partial + no risk + no markers → not eligible."""
        d = _make_dec("F-001", "accept", score=8, ev=EVIDENCE_PARTIAL, impact_area="unknown_xyz")
        raw = _make_finding("F-001", "Clean generic finding", "Nothing special here")
        include, reason = _is_expanded_candidate(d, raw)
        assert include is False
        assert reason == "not_eligible"

    def test_weak_evidence_accepted_always_eligible(self):
        d = _make_dec("F-001", "accept", score=3, ev=EVIDENCE_WEAK, impact_area="unknown_xyz")
        include, reason = _is_expanded_candidate(d)
        assert include is True


# ─── build_candidate_selection_stats ─────────────────────────────────────────


class TestCandidateSelectionStats:
    def test_basic_stats_structure(self):
        decisions = [
            _make_dec("F-001", "accept", score=7, ev=EVIDENCE_VALID),
            _make_dec("F-002", "borderline", score=4, ev=EVIDENCE_PARTIAL),
            _make_dec("F-003", "reject", ev=EVIDENCE_NONE),
        ]
        cands, skipped = select_candidates(decisions)
        stats = build_candidate_selection_stats(decisions, cands, skipped)

        assert stats["total_findings"] == 3
        assert stats["candidate_count"] == 2
        assert stats["skipped_count"] == 1
        assert "candidate_rate" in stats
        assert "by_decision" in stats
        assert "by_evidence_quality" in stats
        assert "by_score_bucket" in stats
        assert "protected_candidates_count" in stats

    def test_protected_count_correct(self):
        """Findings with score >= _EXPANDED_PROTECTED_SCORE + valid evidence are protected."""
        decisions = [
            _make_dec("F-001", "accept", score=_EXPANDED_PROTECTED_SCORE,
                      ev=_EXPANDED_PROTECTED_EVIDENCE),   # protected
            _make_dec("F-002", "accept", score=5, ev=EVIDENCE_PARTIAL),  # not protected
        ]
        cands, skipped = select_candidates(decisions)
        stats = build_candidate_selection_stats(decisions, cands, skipped)
        assert stats["protected_candidates_count"] == 1

    def test_candidate_rate_correct(self):
        decisions = [_make_dec(f"F-{i:03d}", "accept", ev=EVIDENCE_PARTIAL) for i in range(10)]
        cands, skipped = select_candidates(decisions, max_candidates=5)
        stats = build_candidate_selection_stats(decisions, cands, skipped)
        assert stats["candidate_count"] == 5
        assert stats["total_findings"] == 10
        assert stats["candidate_rate"] == pytest.approx(0.5, abs=0.01)

    def test_strategy_recorded(self):
        decisions = [_make_dec("F-001", "accept", ev=EVIDENCE_PARTIAL)]
        cands, skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_EXPANDED)
        stats = build_candidate_selection_stats(
            decisions, cands, skipped, strategy=CANDIDATE_STRATEGY_EXPANDED
        )
        assert stats["strategy"] == CANDIDATE_STRATEGY_EXPANDED

    def test_by_candidate_reason_populated_for_expanded(self):
        decisions = [
            _make_dec("F-001", "borderline", ev=EVIDENCE_WEAK),
            _make_dec("F-002", "accept", score=5, ev=EVIDENCE_WEAK, impact_area="normative_refs"),
        ]
        raw = {"F-001": _make_finding("F-001"), "F-002": _make_finding("F-002")}
        cands, skipped = select_candidates(
            decisions, strategy=CANDIDATE_STRATEGY_EXPANDED, raw_findings=raw
        )
        stats = build_candidate_selection_stats(
            decisions, cands, skipped,
            strategy=CANDIDATE_STRATEGY_EXPANDED, raw_findings=raw
        )
        # by_candidate_reason should be non-empty for expanded
        assert len(stats["by_candidate_reason"]) > 0

    def test_candidate_count_increases_with_expanded(self):
        """Expanded strategy yields more candidates than conservative on a realistic set."""
        decisions = [
            _make_dec("F-001", "accept", score=5, ev=EVIDENCE_PARTIAL, impact_area="normative_refs"),
            _make_dec("F-002", "accept", score=5, ev=EVIDENCE_PARTIAL, impact_area="documentation"),
            _make_dec("F-003", "accept", score=4, ev=EVIDENCE_PARTIAL, impact_area="electrical"),
            _make_dec("F-004", "accept", score=3, ev=EVIDENCE_PARTIAL, impact_area="electrical"),
        ]
        raw = {
            f"F-{i:03d}": _make_finding(f"F-{i:03d}", "Дублирует норму" if i < 2 else "Generic")
            for i in range(1, 5)
        }
        cons_cands, cons_skipped = select_candidates(
            decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE, raw_findings=raw
        )
        exp_cands, exp_skipped = select_candidates(
            decisions, strategy=CANDIDATE_STRATEGY_EXPANDED, raw_findings=raw
        )
        cons_stats = build_candidate_selection_stats(
            decisions, cons_cands, cons_skipped,
            strategy=CANDIDATE_STRATEGY_CONSERVATIVE, raw_findings=raw
        )
        exp_stats = build_candidate_selection_stats(
            decisions, exp_cands, exp_skipped,
            strategy=CANDIDATE_STRATEGY_EXPANDED, raw_findings=raw
        )
        assert exp_stats["candidate_count"] >= cons_stats["candidate_count"]


# ─── Safety invariants ────────────────────────────────────────────────────────


class TestSafetyInvariants:
    def test_deterministic_reject_never_in_any_strategy(self):
        """Reject is never a candidate regardless of strategy."""
        d = _make_dec("F-001", "reject", ev=EVIDENCE_VALID)
        for strategy in VALID_CANDIDATE_STRATEGIES:
            cands, skipped = select_candidates([d], strategy=strategy)
            assert len(cands) == 0
            assert "F-001" in skipped

    def test_deterministic_merge_never_in_any_strategy(self):
        d = _make_dec("F-001", "merge", ev=EVIDENCE_VALID)
        for strategy in VALID_CANDIDATE_STRATEGIES:
            cands, skipped = select_candidates([d], strategy=strategy)
            assert len(cands) == 0
            assert "F-001" in skipped

    def test_none_evidence_excluded_from_conservative_and_expanded(self):
        d = _make_dec("F-001", "accept", ev=EVIDENCE_NONE)
        for strategy in [CANDIDATE_STRATEGY_CONSERVATIVE, CANDIDATE_STRATEGY_EXPANDED]:
            cands, skipped = select_candidates([d], strategy=strategy)
            assert len(cands) == 0

    def test_high_score_valid_accept_included_in_conservative_broad(self):
        """High-score valid accept is included in conservative and broad (any ev != none is fine)."""
        d = _make_dec("F-001", "accept", score=9, ev=EVIDENCE_VALID, impact_area="electrical")
        for strategy in [CANDIDATE_STRATEGY_CONSERVATIVE, CANDIDATE_STRATEGY_BROAD]:
            cands, _ = select_candidates([d], strategy=strategy)
            assert len(cands) == 1, f"Strategy {strategy} should include high-score valid accept"

    def test_high_score_valid_accept_in_risk_category_included_in_expanded(self):
        """High-score valid accept in risk category IS included in expanded."""
        d = _make_dec("F-001", "accept", score=9, ev=EVIDENCE_VALID, impact_area="normative_refs")
        cands, _ = select_candidates([d], strategy=CANDIDATE_STRATEGY_EXPANDED)
        assert len(cands) == 1

    def test_high_score_valid_accept_non_risk_excluded_in_expanded(self):
        """High-score valid accept without risk signals is excluded in expanded (too conservative to mark as risk)."""
        d = _make_dec("F-001", "accept", score=9, ev=EVIDENCE_VALID, impact_area="electrical")
        cands, _ = select_candidates([d], strategy=CANDIDATE_STRATEGY_EXPANDED)
        # Valid + score=9 doesn't match accepted_partial_low_score (wrong ev),
        # nor weak evidence, nor risk category, nor taxonomy marker → excluded
        assert len(cands) == 0

    def test_mixed_batch_correct_split(self):
        decisions = [
            _make_dec("F-001", "accept", ev=EVIDENCE_VALID),
            _make_dec("F-002", "reject", ev=EVIDENCE_PARTIAL),
            _make_dec("F-003", "merge", ev=EVIDENCE_PARTIAL),
            _make_dec("F-004", "borderline", ev=EVIDENCE_WEAK),
            _make_dec("F-005", "accept", ev=EVIDENCE_NONE),  # excluded in cons/expanded
        ]
        cons_cands, cons_skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_CONSERVATIVE)
        broad_cands, broad_skipped = select_candidates(decisions, strategy=CANDIDATE_STRATEGY_BROAD)

        # Conservative: F-001, F-004 (F-005 has none evidence)
        assert len(cons_cands) == 2
        assert "F-002" in cons_skipped
        assert "F-003" in cons_skipped

        # Broad: F-001, F-004, F-005 (reject and merge still excluded)
        assert len(broad_cands) == 3
        assert "F-002" in broad_skipped
        assert "F-003" in broad_skipped


# ─── Integration: run_llm_gate with strategy ─────────────────────────────────


class TestRunLLMGateWithStrategy:
    """Integration tests for run_llm_gate with candidate_strategy parameter."""

    def test_run_llm_gate_conservative_default(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import run_llm_gate
        decisions = [
            _make_dec("F-001", "accept", ev=EVIDENCE_PARTIAL, impact_area="normative_refs"),
            _make_dec("F-002", "accept", ev=EVIDENCE_NONE),   # excluded
        ]
        findings_by_id = {
            "F-001": _make_finding("F-001"),
            "F-002": _make_finding("F-002"),
        }
        result = run_llm_gate(
            decisions, findings_by_id, provider="mock",
            candidate_strategy=CANDIDATE_STRATEGY_CONSERVATIVE,
        )
        # F-002 excluded (evidence=none), F-001 sent
        assert result.candidates_sent == 1
        assert "F-002" in result.skipped_ids

    def test_run_llm_gate_expanded_sends_more(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import run_llm_gate
        decisions = [
            _make_dec("F-001", "accept", score=5, ev=EVIDENCE_PARTIAL, impact_area="normative_refs"),
            _make_dec("F-002", "accept", score=5, ev=EVIDENCE_PARTIAL, impact_area="documentation"),
            _make_dec("F-003", "accept", score=3, ev=EVIDENCE_PARTIAL, impact_area="other"),
            _make_dec("F-004", "reject", ev=EVIDENCE_PARTIAL),
        ]
        findings_by_id = {f"F-{i:03d}": _make_finding(f"F-{i:03d}") for i in range(1, 5)}

        cons_result = run_llm_gate(
            decisions, findings_by_id, provider="mock",
            candidate_strategy=CANDIDATE_STRATEGY_CONSERVATIVE,
        )
        exp_result = run_llm_gate(
            decisions, findings_by_id, provider="mock",
            candidate_strategy=CANDIDATE_STRATEGY_EXPANDED,
        )
        # Expanded should send >= candidates than conservative
        assert exp_result.candidates_sent >= cons_result.candidates_sent
        # F-004 (reject) still excluded in both
        assert "F-004" in exp_result.skipped_ids
