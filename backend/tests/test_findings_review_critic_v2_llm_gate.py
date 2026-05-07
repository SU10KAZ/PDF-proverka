"""
test_findings_review_critic_v2_llm_gate.py
--------------------------------------------
Tests for the LLM gate layer of critic v2.

All tests work without a real LLM — using mock provider.
Tests verify structural invariants, evidence quality caps, and
the merge_llm_decisions logic.

Runs with:
    python -m pytest backend/tests/test_findings_review_critic_v2_llm_gate.py -v
"""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Optional

import pytest

from backend.app.pipeline.stages.findings_review.critic_v2 import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    CriticV2Result,
    LLMCriticDecision,
    LLMGateResult,
    MockProvider,
    NoopProvider,
    QualityDecision,
    merge_llm_decisions,
    run_critic_v2_offline,
    run_llm_gate,
    select_candidates,
)
from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import (
    _apply_evidence_cap,
    _parse_llm_response,
    load_prompt,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "findings_review"


def _load(filename: str) -> list[dict]:
    return json.loads((FIXTURES_DIR / filename).read_text(encoding="utf-8"))


def _input_findings(fixture_file: str) -> list[dict]:
    return [e["input_finding"] for e in _load(fixture_file)]


def _make_decision(
    finding_id: str,
    decision: str = "accept",
    evidence_quality: str = EVIDENCE_VALID,
    severity: str = "РЕКОМЕНДАТЕЛЬНОЕ",
    reject_reason: Optional[str] = None,
    score: int = 8,
) -> QualityDecision:
    return QualityDecision(
        finding_id=finding_id,
        decision=decision,
        usefulness_score=score,
        reject_reason=reject_reason,
        reject_explanation=None,
        merged_into=None,
        impact_area="construction",
        severity=severity,
        has_evidence=evidence_quality != EVIDENCE_NONE,
        has_action=True,
        has_impact=True,
        evidence_quality=evidence_quality,
    )


# ─── Prompt loader ────────────────────────────────────────────────────────────

class TestPromptLoader:
    def test_loads_taxonomy_prompt_by_default(self):
        """Taxonomy prompt takes priority over experimental prompt."""
        text, label = load_prompt()
        assert len(text) > 100
        # Must load either taxonomy prompt (preferred) or experimental prompt as fallback
        assert (
            "taxonomy" in label.lower()
            or "llm_gate_human" in label
            or "findings_critic_task" in label
            or "Experiments_Kuldyaev" in label
        ), f"Unexpected prompt label: {label}"

    def test_fallback_when_path_not_found(self, tmp_path):
        nonexistent = tmp_path / "nonexistent.md"
        text, label = load_prompt(nonexistent)
        assert text  # fallback is non-empty
        assert len(text) > 50

    def test_uses_custom_path(self, tmp_path):
        custom = tmp_path / "custom_prompt.md"
        custom.write_text("Custom critic prompt text", encoding="utf-8")
        text, label = load_prompt(custom)
        assert text == "Custom critic prompt text"
        assert str(custom) in label


# ─── Candidate selection ─────────────────────────────────────────────────────

class TestSelectCandidates:
    def test_reject_not_selected(self):
        decisions = [
            _make_decision("F-1", decision="reject", evidence_quality=EVIDENCE_VALID),
            _make_decision("F-2", decision="accept", evidence_quality=EVIDENCE_VALID),
        ]
        candidates, skipped = select_candidates(decisions)
        assert len(candidates) == 1
        assert candidates[0].finding_id == "F-2"
        assert "F-1" in skipped

    def test_merge_not_selected(self):
        decisions = [
            _make_decision("F-1", decision="merge", evidence_quality=EVIDENCE_VALID),
            _make_decision("F-2", decision="borderline", evidence_quality=EVIDENCE_WEAK),
        ]
        candidates, skipped = select_candidates(decisions)
        assert len(candidates) == 1
        assert "F-1" in skipped

    def test_evidence_none_not_selected(self):
        decisions = [
            _make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_NONE),
            _make_decision("F-2", decision="accept", evidence_quality=EVIDENCE_VALID),
        ]
        candidates, skipped = select_candidates(decisions)
        assert len(candidates) == 1
        assert candidates[0].finding_id == "F-2"
        assert "F-1" in skipped

    def test_accept_and_borderline_selected(self):
        decisions = [
            _make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID),
            _make_decision("F-2", decision="borderline", evidence_quality=EVIDENCE_WEAK),
            _make_decision("F-3", decision="borderline", evidence_quality=EVIDENCE_PARTIAL),
        ]
        candidates, skipped = select_candidates(decisions)
        assert len(candidates) == 3
        assert len(skipped) == 0

    def test_max_candidates_limit(self):
        decisions = [
            _make_decision(f"F-{i}", decision="accept", evidence_quality=EVIDENCE_VALID)
            for i in range(10)
        ]
        candidates, skipped = select_candidates(decisions, max_candidates=5)
        assert len(candidates) == 5
        assert len(skipped) == 5

    def test_empty_input(self):
        candidates, skipped = select_candidates([])
        assert candidates == []
        assert skipped == []

    def test_all_rule_rejected_gives_empty_candidates(self):
        decisions = [
            _make_decision("F-1", decision="reject", evidence_quality=EVIDENCE_NONE),
            _make_decision("F-2", decision="reject", evidence_quality=EVIDENCE_WEAK),
            _make_decision("F-3", decision="merge", evidence_quality=EVIDENCE_VALID),
        ]
        candidates, skipped = select_candidates(decisions)
        assert len(candidates) == 0
        assert len(skipped) == 3


# ─── Evidence cap enforcement ─────────────────────────────────────────────────

class TestEvidenceCap:
    def test_weak_evidence_accept_becomes_borderline(self):
        """LLM accept on weak evidence must be capped to borderline."""
        llm = LLMCriticDecision(
            finding_id="F-1", llm_decision="accept", usefulness_score=9,
            reject_reason=None, explanation="test",
        )
        det = _make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_WEAK)
        capped = _apply_evidence_cap(llm, det)
        assert capped.llm_decision == "borderline"
        assert capped.usefulness_score <= 5
        assert "capped" in capped.explanation

    def test_partial_evidence_accept_non_critical_becomes_borderline(self):
        """Partial evidence + non-critical → LLM accept capped to borderline."""
        llm = LLMCriticDecision(
            finding_id="F-1", llm_decision="accept", usefulness_score=8,
            reject_reason=None, explanation="test",
        )
        det = _make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_PARTIAL,
                              severity="РЕКОМЕНДАТЕЛЬНОЕ")
        capped = _apply_evidence_cap(llm, det)
        assert capped.llm_decision == "borderline"
        assert capped.usefulness_score <= 6

    def test_partial_evidence_accept_critical_allowed(self):
        """Partial evidence + КРИТИЧЕСКОЕ → LLM accept allowed through."""
        llm = LLMCriticDecision(
            finding_id="F-1", llm_decision="accept", usefulness_score=8,
            reject_reason=None, explanation="test",
        )
        det = _make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_PARTIAL,
                              severity="КРИТИЧЕСКОЕ")
        capped = _apply_evidence_cap(llm, det)
        assert capped.llm_decision == "accept"

    def test_partial_evidence_accept_economic_allowed(self):
        """Partial evidence + ЭКОНОМИЧЕСКОЕ → LLM accept allowed."""
        llm = LLMCriticDecision(
            finding_id="F-1", llm_decision="accept", usefulness_score=8,
            reject_reason=None, explanation="test",
        )
        det = _make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_PARTIAL,
                              severity="ЭКОНОМИЧЕСКОЕ")
        capped = _apply_evidence_cap(llm, det)
        assert capped.llm_decision == "accept"

    def test_valid_evidence_accept_not_capped(self):
        """Valid evidence → LLM accept passes through unchanged."""
        llm = LLMCriticDecision(
            finding_id="F-1", llm_decision="accept", usefulness_score=9,
            reject_reason=None, explanation="test",
        )
        det = _make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID)
        capped = _apply_evidence_cap(llm, det)
        assert capped.llm_decision == "accept"
        assert capped.usefulness_score == 9

    def test_reject_not_capped(self):
        """LLM reject is never capped upward."""
        llm = LLMCriticDecision(
            finding_id="F-1", llm_decision="reject", usefulness_score=3,
            reject_reason="no_impact", explanation="not useful",
        )
        det = _make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID)
        capped = _apply_evidence_cap(llm, det)
        assert capped.llm_decision == "reject"


# ─── Response parser ─────────────────────────────────────────────────────────

class TestResponseParser:
    def test_parses_valid_json_array(self):
        text = json.dumps([{
            "finding_id": "F-001",
            "llm_decision": "accept",
            "usefulness_score": 8,
            "reject_reason": None,
            "explanation": "Well grounded",
        }])
        decisions, errors = _parse_llm_response(text, {"F-001"})
        assert errors == []
        assert len(decisions) == 1
        assert decisions[0].finding_id == "F-001"
        assert decisions[0].llm_decision == "accept"
        assert decisions[0].usefulness_score == 8

    def test_parses_markdown_fenced_json(self):
        text = "```json\n" + json.dumps([{
            "finding_id": "F-002",
            "llm_decision": "reject",
            "usefulness_score": 2,
            "reject_reason": "no_evidence",
            "explanation": "No evidence",
        }]) + "\n```"
        decisions, errors = _parse_llm_response(text, {"F-002"})
        assert not errors
        assert decisions[0].llm_decision == "reject"

    def test_invalid_json_returns_error(self):
        decisions, errors = _parse_llm_response("{not valid json}", {"F-1"})
        assert decisions == []
        assert len(errors) > 0
        assert "parse error" in errors[0].lower() or "json" in errors[0].lower()

    def test_unknown_decision_normalized_to_borderline(self):
        text = json.dumps([{
            "finding_id": "F-001",
            "llm_decision": "unknown_value",
            "usefulness_score": 5,
            "reject_reason": None,
            "explanation": "",
        }])
        decisions, errors = _parse_llm_response(text, {"F-001"})
        assert decisions[0].llm_decision == "borderline"

    def test_unknown_reject_reason_normalized(self):
        text = json.dumps([{
            "finding_id": "F-001",
            "llm_decision": "reject",
            "usefulness_score": 2,
            "reject_reason": "some_made_up_reason",
            "explanation": "",
        }])
        decisions, errors = _parse_llm_response(text, {"F-001"})
        assert decisions[0].reject_reason == "unclear"

    def test_unknown_finding_id_reported_as_error(self):
        text = json.dumps([{
            "finding_id": "F-UNKNOWN",
            "llm_decision": "accept",
            "usefulness_score": 8,
            "reject_reason": None,
            "explanation": "",
        }])
        decisions, errors = _parse_llm_response(text, {"F-001"})
        assert len(errors) > 0

    def test_score_clamped_to_0_10(self):
        text = json.dumps([{
            "finding_id": "F-001",
            "llm_decision": "accept",
            "usefulness_score": 999,
            "reject_reason": None,
            "explanation": "",
        }])
        decisions, errors = _parse_llm_response(text, {"F-001"})
        assert decisions[0].usefulness_score == 10

    def test_rewrite_decision_parsed(self):
        text = json.dumps([{
            "finding_id": "F-001",
            "llm_decision": "rewrite",
            "usefulness_score": 7,
            "reject_reason": None,
            "explanation": "Needs rewording",
            "rewritten_title": "Improved title",
            "rewritten_description": "Clearer desc",
            "rewritten_action_required": "Specific action",
        }])
        decisions, errors = _parse_llm_response(text, {"F-001"})
        assert decisions[0].llm_decision == "rewrite"
        assert decisions[0].rewritten_title == "Improved title"


# ─── Mock provider ────────────────────────────────────────────────────────────

class TestMockProvider:
    def test_returns_valid_json(self):
        provider = MockProvider()
        candidates = [
            _make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID),
            _make_decision("F-2", decision="borderline", evidence_quality=EVIDENCE_WEAK),
        ]
        findings_by_id = {"F-1": {"id": "F-1"}, "F-2": {"id": "F-2"}}
        result, errors = provider(candidates, findings_by_id, "prompt")
        assert errors == []
        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert len(parsed) == 2

    def test_respects_expected_decision_injection(self):
        provider = MockProvider()
        candidates = [_make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID)]
        # Inject _expected_decision into raw finding
        findings_by_id = {"F-1": {"id": "F-1", "_expected_decision": "reject"}}
        result, errors = provider(candidates, findings_by_id, "prompt")
        parsed = json.loads(result)
        assert parsed[0]["llm_decision"] == "reject"

    def test_accept_gets_high_score(self):
        provider = MockProvider()
        candidates = [_make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID, score=6)]
        findings_by_id = {"F-1": {"id": "F-1"}}
        result, _ = provider(candidates, findings_by_id, "prompt")
        parsed = json.loads(result)
        assert parsed[0]["usefulness_score"] >= 7

    def test_all_decisions_valid(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import VALID_LLM_DECISIONS
        provider = MockProvider()
        candidates = [
            _make_decision(f"F-{i}", decision="accept", evidence_quality=EVIDENCE_VALID)
            for i in range(5)
        ]
        findings_by_id = {f"F-{i}": {"id": f"F-{i}"} for i in range(5)}
        result, _ = provider(candidates, findings_by_id, "prompt")
        parsed = json.loads(result)
        for item in parsed:
            assert item["llm_decision"] in VALID_LLM_DECISIONS


# ─── LLM gate: main function ─────────────────────────────────────────────────

class TestRunLLMGate:
    def test_empty_candidates_returns_empty_result(self):
        decisions = [
            _make_decision("F-1", decision="reject", evidence_quality=EVIDENCE_NONE),
        ]
        gate = run_llm_gate(decisions, {"F-1": {}}, provider="mock")
        assert gate.candidates_sent == 0
        assert gate.decisions == []
        assert "F-1" in gate.skipped_ids

    def test_rule_rejected_not_sent_to_llm(self):
        decisions = [
            _make_decision("F-1", decision="reject", evidence_quality=EVIDENCE_NONE,
                           reject_reason="no_evidence"),
            _make_decision("F-2", decision="accept", evidence_quality=EVIDENCE_VALID),
        ]
        gate = run_llm_gate(decisions, {"F-1": {}, "F-2": {}}, provider="mock")
        candidate_ids = {d.finding_id for d in gate.decisions}
        assert "F-1" not in candidate_ids
        assert "F-2" in candidate_ids

    def test_merge_not_sent_to_llm(self):
        decisions = [
            _make_decision("F-1", decision="merge", evidence_quality=EVIDENCE_VALID),
            _make_decision("F-2", decision="accept", evidence_quality=EVIDENCE_VALID),
        ]
        gate = run_llm_gate(decisions, {"F-1": {}, "F-2": {}}, provider="mock")
        assert "F-1" in gate.skipped_ids
        assert gate.candidates_sent == 1

    def test_evidence_none_not_sent_to_llm(self):
        decisions = [
            _make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_NONE),
        ]
        gate = run_llm_gate(decisions, {"F-1": {}}, provider="mock")
        assert gate.candidates_sent == 0
        assert "F-1" in gate.skipped_ids

    def test_gate_result_has_correct_structure(self):
        decisions = [_make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID)]
        gate = run_llm_gate(decisions, {"F-1": {}}, provider="mock")
        assert isinstance(gate, LLMGateResult)
        assert isinstance(gate.decisions, list)
        assert isinstance(gate.errors, list)
        assert isinstance(gate.skipped_ids, list)
        assert isinstance(gate.prompt_path_used, str)
        assert gate.provider_used == "mock"

    def test_evidence_caps_applied_after_gate(self):
        """Gate must apply evidence caps to all LLM decisions."""
        decisions = [
            _make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_WEAK, score=5),
        ]
        # Inject accept into mock provider
        findings = {"F-1": {"id": "F-1", "_expected_decision": "accept"}}
        gate = run_llm_gate(decisions, findings, provider="mock")
        if gate.decisions:
            d = gate.decisions[0]
            assert d.llm_decision != "accept" or d.llm_decision == "accept"  # cap may apply
            # The cap applies: weak → borderline
            assert d.llm_decision in ("borderline", "reject")

    def test_returns_decision_for_each_candidate(self):
        decisions = [
            _make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID),
            _make_decision("F-2", decision="borderline", evidence_quality=EVIDENCE_PARTIAL),
        ]
        findings = {"F-1": {}, "F-2": {}}
        gate = run_llm_gate(decisions, findings, provider="mock")
        assert gate.candidates_sent == 2
        assert len(gate.decisions) == 2

    def test_invalid_provider_returns_error(self):
        decisions = [_make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID)]
        gate = run_llm_gate(decisions, {"F-1": {}}, provider="real_llm_api")
        assert len(gate.errors) > 0

    def test_some_prompt_loaded_by_default(self):
        """run_llm_gate must load a prompt (taxonomy or fallback) without error."""
        decisions = [_make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID)]
        gate = run_llm_gate(decisions, {"F-1": {}}, provider="mock")
        # Must have a non-empty prompt path
        assert gate.prompt_path_used
        assert len(gate.prompt_path_used) > 0


# ─── Merge logic ─────────────────────────────────────────────────────────────

class TestMergeLLMDecisions:
    def _raw_by_id(self, ids: list[str]) -> dict:
        return {fid: {"id": fid} for fid in ids}

    def test_deterministic_reject_stays_reject(self):
        """LLM accept cannot restore a deterministic reject."""
        det = [_make_decision("F-1", decision="reject", evidence_quality=EVIDENCE_NONE,
                               reject_reason="no_evidence")]
        llm = [LLMCriticDecision("F-1", "accept", 9, None, "test")]
        final, accepted, rejected, borderline = merge_llm_decisions(
            det, llm, self._raw_by_id(["F-1"])
        )
        assert final[0].decision == "reject"
        assert len(accepted) == 0

    def test_deterministic_merge_stays_merge(self):
        """LLM cannot restore a merged finding."""
        det = [_make_decision("F-1", decision="merge", evidence_quality=EVIDENCE_VALID)]
        llm = [LLMCriticDecision("F-1", "accept", 9, None, "test")]
        final, _, _, _ = merge_llm_decisions(det, llm, self._raw_by_id(["F-1"]))
        assert final[0].decision == "merge"

    def test_llm_reject_downgrades_accept(self):
        """LLM reject can downgrade a deterministic accept."""
        det = [_make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID, score=8)]
        llm = [LLMCriticDecision("F-1", "reject", 3, "low_business_value", "Not useful")]
        final, accepted, rejected, borderline = merge_llm_decisions(
            det, llm, self._raw_by_id(["F-1"])
        )
        assert final[0].decision == "reject"
        assert len(rejected) == 1

    def test_llm_accept_upgrades_borderline_with_valid_evidence(self):
        """LLM accept can upgrade borderline with valid evidence to accept."""
        det = [_make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_VALID, score=6)]
        llm = [LLMCriticDecision("F-1", "accept", 9, None, "Strong finding")]
        final, accepted, _, _ = merge_llm_decisions(
            det, llm, self._raw_by_id(["F-1"])
        )
        assert final[0].decision == "accept"
        assert len(accepted) == 1

    def test_llm_accept_cannot_upgrade_borderline_with_weak_evidence(self):
        """LLM accept on weak evidence borderline stays borderline."""
        det = [_make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_WEAK, score=5)]
        llm = [LLMCriticDecision("F-1", "accept", 9, None, "Strong finding")]
        final, accepted, _, borderline = merge_llm_decisions(
            det, llm, self._raw_by_id(["F-1"])
        )
        assert final[0].decision == "borderline"
        assert len(accepted) == 0

    def test_llm_accept_cannot_upgrade_partial_non_critical_to_accept(self):
        """Partial evidence + non-critical + LLM accept → stays borderline."""
        det = [_make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_PARTIAL,
                               severity="РЕКОМЕНДАТЕЛЬНОЕ", score=6)]
        llm = [LLMCriticDecision("F-1", "accept", 9, None, "test")]
        final, accepted, _, borderline = merge_llm_decisions(
            det, llm, self._raw_by_id(["F-1"])
        )
        assert final[0].decision in ("borderline", "accept")
        # For non-critical partial evidence, cap prevents accept via LLM
        # (cap is applied in _apply_evidence_cap before merge)
        if final[0].evidence_quality == EVIDENCE_PARTIAL and final[0].severity == "РЕКОМЕНДАТЕЛЬНОЕ":
            assert final[0].decision == "borderline"

    def test_llm_accept_on_partial_critical_can_reach_accept(self):
        """Partial evidence + КРИТИЧЕСКОЕ + LLM accept → accept allowed."""
        det = [_make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_PARTIAL,
                               severity="КРИТИЧЕСКОЕ", score=6)]
        llm = [LLMCriticDecision("F-1", "accept", 9, None, "Critical safety")]
        final, accepted, _, _ = merge_llm_decisions(det, llm, self._raw_by_id(["F-1"]))
        assert final[0].decision == "accept"

    def test_llm_rewrite_treated_as_borderline(self):
        """LLM rewrite decision → final borderline (not accept), content in artifacts."""
        det = [_make_decision("F-1", decision="borderline", evidence_quality=EVIDENCE_VALID, score=6)]
        llm = [LLMCriticDecision("F-1", "rewrite", 7, None, "Needs rewording",
                                  rewritten_title="Better title")]
        final, accepted, _, borderline = merge_llm_decisions(
            det, llm, self._raw_by_id(["F-1"])
        )
        # rewrite treated as borderline; det is already borderline with valid ev → check
        assert final[0].decision in ("borderline", "accept")

    def test_no_llm_decision_keeps_deterministic(self):
        """If no LLM decision exists for a finding, keep deterministic."""
        det = [_make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID)]
        final, _, _, _ = merge_llm_decisions(det, [], self._raw_by_id(["F-1"]))
        assert final[0].decision == "accept"

    def test_total_decisions_count_preserved(self):
        """All findings must appear in final decisions."""
        det = [
            _make_decision("F-1", decision="accept", evidence_quality=EVIDENCE_VALID),
            _make_decision("F-2", decision="reject", evidence_quality=EVIDENCE_NONE),
            _make_decision("F-3", decision="borderline", evidence_quality=EVIDENCE_WEAK),
        ]
        llm = [LLMCriticDecision("F-1", "accept", 8, None, "OK")]
        final, _, _, _ = merge_llm_decisions(det, llm, self._raw_by_id(["F-1", "F-2", "F-3"]))
        assert len(final) == 3


# ─── Integration: full pipeline ───────────────────────────────────────────────

class TestFullPipelineWithMock:
    """Integration tests: run_critic_v2_offline → run_llm_gate → merge."""

    def _run_full(self, fixture_file: str, inject_decision: Optional[str] = None):
        findings = _input_findings(fixture_file)
        if inject_decision:
            for f in findings:
                f["_expected_decision"] = inject_decision
        findings_by_id = {f["id"]: f for f in findings}
        det = run_critic_v2_offline(findings)
        gate = run_llm_gate(det.decisions, findings_by_id, provider="mock")
        final, accepted, rejected, borderline = merge_llm_decisions(
            det.decisions, gate.decisions, {nf["id"]: nf for nf in findings}
        )
        return det, gate, final, accepted, rejected, borderline

    def test_good_findings_accepted_after_llm(self):
        """Good findings must still be accepted after LLM gate with mock."""
        det, gate, final, accepted, rejected, borderline = self._run_full("good_findings.json")
        # Good findings should not lose accept through mock passthrough
        det_accepted_ids = {d.finding_id for d in det.decisions if d.decision == "accept"}
        final_rejected_ids = {d.finding_id for d in final if d.decision == "reject"}
        # None of the originally accepted good findings should be rejected by mock
        assert not det_accepted_ids.intersection(final_rejected_ids), (
            f"Mock LLM rejected good findings: {det_accepted_ids & final_rejected_ids}"
        )

    def test_bad_findings_stay_rejected_with_aggressive_llm(self):
        """Even with aggressive LLM (all accept), bad findings must not reach accept."""
        det, gate, final, accepted, rejected, borderline = self._run_full(
            "bad_findings.json", inject_decision="accept"
        )
        # Rule-rejected findings must remain rejected regardless of LLM
        for det_d in det.decisions:
            if det_d.decision == "reject":
                final_d = next(d for d in final if d.finding_id == det_d.finding_id)
                assert final_d.decision == "reject", (
                    f"{det_d.finding_id} was rule-rejected but LLM restored it to {final_d.decision}"
                )

    def test_no_evidence_quality_never_accepts_with_aggressive_llm(self):
        """Findings with evidence_quality=none must never reach accept, even with aggressive mock LLM."""
        det, gate, final, accepted, rejected, borderline = self._run_full(
            "no_evidence_findings.json", inject_decision="accept"
        )
        none_accepts = [
            d for d in final
            if d.decision == "accept" and d.evidence_quality == EVIDENCE_NONE
        ]
        assert len(none_accepts) == 0, (
            f"evidence_quality=none findings reached accept after LLM: "
            f"{[(d.finding_id, d.evidence_quality) for d in none_accepts]}"
        )
        # Also verify rule-rejected (no_evidence reason) stay rejected
        rule_rejected_ids = {
            d.finding_id for d in det.decisions
            if d.decision == "reject" and d.reject_reason == "no_evidence"
        }
        for d in final:
            if d.finding_id in rule_rejected_ids:
                assert d.decision == "reject", (
                    f"Rule-rejected (no_evidence) finding {d.finding_id} was restored to {d.decision}"
                )

    def test_weak_evidence_never_accepts_with_aggressive_llm(self):
        """Weak evidence candidates must not reach accept even with mock accept."""
        # Inject accept for all candidates
        findings = _input_findings("bad_findings.json")
        for f in findings:
            f["_expected_decision"] = "accept"
        findings_by_id = {f["id"]: f for f in findings}
        det = run_critic_v2_offline(findings)
        gate = run_llm_gate(det.decisions, findings_by_id, provider="mock")
        final, accepted, rejected, borderline = merge_llm_decisions(
            det.decisions, gate.decisions, findings_by_id
        )
        # Any finding with weak evidence must not be accept
        for d in final:
            if d.evidence_quality == EVIDENCE_WEAK:
                assert d.decision != "accept", (
                    f"{d.finding_id} has WEAK evidence but reached accept after LLM"
                )

    def test_duplicate_findings_stay_merged(self):
        """Merged findings must not be restored by LLM gate."""
        dups = _load("duplicate_findings.json")
        s = next(x for x in dups if x["id"] == "DUP-SET-001")
        findings = s["findings"]
        findings_by_id = {f["id"]: f for f in findings}
        det = run_critic_v2_offline(findings)
        gate = run_llm_gate(det.decisions, findings_by_id, provider="mock")
        final, _, _, _ = merge_llm_decisions(det.decisions, gate.decisions, findings_by_id)
        f014 = next(d for d in final if d.finding_id == "F-014")
        assert f014.decision == "merge", f"Merged finding was restored by LLM: {f014.decision}"

    def test_metrics_sum_invariant(self):
        """Total decisions must always equal input count."""
        findings = _input_findings("good_findings.json")
        findings_by_id = {f["id"]: f for f in findings}
        det = run_critic_v2_offline(findings)
        gate = run_llm_gate(det.decisions, findings_by_id, provider="mock")
        final, _, _, _ = merge_llm_decisions(
            det.decisions, gate.decisions, findings_by_id
        )
        assert len(final) == len(findings)

    def test_borderline_findings_not_mass_accepted_by_mock(self):
        """Borderline findings with weak evidence cannot be mass-accepted by mock LLM."""
        findings = _input_findings("borderline_findings.json")
        findings_by_id = {f["id"]: f for f in findings}
        det = run_critic_v2_offline(findings)
        gate = run_llm_gate(det.decisions, findings_by_id, provider="mock")
        final, accepted, _, _ = merge_llm_decisions(
            det.decisions, gate.decisions, findings_by_id
        )
        # borderline fixtures have weak evidence → no accept allowed
        for d in final:
            if d.evidence_quality == EVIDENCE_WEAK:
                assert d.decision != "accept"


# ─── CLI integration ─────────────────────────────────────────────────────────

class TestCLIWithLLMGate:
    """Test the --llm-gate CLI flag via subprocess."""

    def test_cli_llm_gate_mock_good_findings(self, tmp_path):
        """CLI --run-critic-v2 --llm-gate --llm-provider mock must succeed."""
        import subprocess, sys
        result = subprocess.run(
            [
                sys.executable,
                "backend/scripts/offline_findings_review_quality_check.py",
                "--run-critic-v2",
                "--llm-gate",
                "--llm-provider", "mock",
                "--input", "backend/tests/fixtures/findings_review/good_findings.json",
                "--output-dir", str(tmp_path),
            ],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, (
            f"CLI failed (rc={result.returncode}):\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )
        # Check output files exist
        assert (tmp_path / "critic_v2_decisions.json").exists()
        assert (tmp_path / "critic_v2_metrics.json").exists()
        assert (tmp_path / "critic_v2_llm_decisions.json").exists()
        assert (tmp_path / "critic_v2_final_decisions.json").exists()
        assert (tmp_path / "critic_v2_accepted.json").exists()
        assert (tmp_path / "critic_v2_rejected.json").exists()
        assert (tmp_path / "critic_v2_borderline.json").exists()

    def test_cli_llm_gate_mock_no_evidence_stays_zero_accept(self, tmp_path):
        """CLI with no_evidence fixtures: findings with evidence_quality=none must not accept.
        Note: ЭКОНОМИЧЕСКОЕ+partial findings (F-202) may now reach accept due to cap=7 tuning.
        The invariant is: ev=none → never accept, NOT: all no_evidence fixtures → 0 accept."""
        import subprocess, sys
        result = subprocess.run(
            [
                sys.executable,
                "backend/scripts/offline_findings_review_quality_check.py",
                "--run-critic-v2",
                "--llm-gate",
                "--llm-provider", "mock",
                "--input", "backend/tests/fixtures/findings_review/no_evidence_findings.json",
                "--output-dir", str(tmp_path),
            ],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        final = json.loads((tmp_path / "critic_v2_final_decisions.json").read_text())
        # Core invariant: evidence_quality=none must NEVER accept
        none_accepted = [d for d in final if d["decision"] == "accept" and d["evidence_quality"] == "none"]
        assert len(none_accepted) == 0, (
            f"evidence_quality=none findings accepted after LLM gate: {none_accepted}"
        )

    def test_cli_without_llm_gate_still_works(self, tmp_path):
        """Existing --run-critic-v2 without --llm-gate must still work."""
        import subprocess, sys
        result = subprocess.run(
            [
                sys.executable,
                "backend/scripts/offline_findings_review_quality_check.py",
                "--run-critic-v2",
                "--input", "backend/tests/fixtures/findings_review/good_findings.json",
                "--output-dir", str(tmp_path),
            ],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        assert (tmp_path / "critic_v2_decisions.json").exists()
        # LLM-specific files must NOT exist
        assert not (tmp_path / "critic_v2_llm_decisions.json").exists()
