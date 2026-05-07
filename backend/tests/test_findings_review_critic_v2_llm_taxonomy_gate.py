"""
test_findings_review_critic_v2_llm_taxonomy_gate.py
-----------------------------------------------------
Tests for the taxonomy-aware LLM gate enhancements in critic_v2/llm_gate.py.

Verifies:
  - taxonomy fields parsed from LLM response
  - needs_human merges as borderline (never reject)
  - insufficient_source_context never becomes reject
  - confidence < 0.75 downgrades reject to borderline
  - visual_or_ocr_misread + high confidence → reject allowed
  - duplicate_or_already_covered + high confidence → reject allowed
  - acceptable_design_solution → borderline, not hard reject
  - deterministic reject invariant still holds
  - evidence quality caps still apply
  - MockProvider includes taxonomy fields
  - prompt loader prefers taxonomy prompt

Runs with:
    python -m pytest backend/tests/test_findings_review_critic_v2_llm_taxonomy_gate.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.findings_review.critic_v2 import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    LLM_REJECT_CONFIDENCE_THRESHOLD,
    VALID_TAXONOMY_REASONS,
    QualityDecision,
    run_critic_v2_offline,
)
from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import (
    LLMCriticDecision,
    MockProvider,
    NoopProvider,
    VALID_LLM_DECISIONS,
    _apply_confidence_and_taxonomy_gate,
    _apply_evidence_cap,
    _parse_llm_response,
    load_prompt,
    merge_llm_decisions,
    run_llm_gate,
    select_candidates,
)

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _det(
    fid: str,
    decision: str = "accept",
    ev: str = EVIDENCE_VALID,
    score: int = 8,
    severity: str = "КРИТИЧЕСКОЕ",
    reject_reason: str | None = None,
) -> QualityDecision:
    return QualityDecision(
        finding_id=fid,
        decision=decision,
        usefulness_score=score,
        reject_reason=reject_reason,
        reject_explanation=None,
        merged_into=None,
        impact_area="construction",
        severity=severity,
        has_evidence=True,
        has_action=True,
        has_impact=True,
        evidence_quality=ev,
    )


def _llm(
    fid: str,
    llm_decision: str = "accept",
    score: int = 8,
    taxonomy: str = "other",
    confidence: float = 1.0,
    reject_reason: str | None = None,
    source_dep: str = "enough_source",
) -> LLMCriticDecision:
    return LLMCriticDecision(
        finding_id=fid,
        llm_decision=llm_decision,
        usefulness_score=score,
        reject_reason=reject_reason,
        explanation=f"[test] {llm_decision}",
        human_taxonomy_reason=taxonomy,
        confidence=confidence,
        evidence_checked=True,
        source_dependency=source_dep,
    )


def _raw(fid: str, **kwargs) -> dict:
    d = {
        "id": fid,
        "problem": f"Problem {fid}",
        "description": f"Description {fid}",
        "solution": f"Fix {fid}",
        "evidence": [{"block_id": f"BLK-{fid}", "type": "image", "page": 1}],
        "related_block_ids": [f"BLK-{fid}"],
        "severity": "КРИТИЧЕСКОЕ",
        "category": "cable",
    }
    d.update(kwargs)
    return d


# ─── VALID_LLM_DECISIONS includes needs_human ─────────────────────────────────

class TestValidDecisions:
    def test_needs_human_in_valid_decisions(self):
        assert "needs_human" in VALID_LLM_DECISIONS

    def test_all_expected_decisions_present(self):
        for d in ("accept", "reject", "borderline", "needs_human", "rewrite"):
            assert d in VALID_LLM_DECISIONS


# ─── _apply_confidence_and_taxonomy_gate ─────────────────────────────────────

class TestConfidenceTaxonomyGate:
    def test_insufficient_source_context_never_reject(self):
        """insufficient_source_context → reject must become needs_human."""
        llm = _llm("F-001", llm_decision="reject", taxonomy="insufficient_source_context",
                   confidence=0.95, reject_reason="no_impact")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "needs_human"
        assert result.reject_reason is None

    def test_low_confidence_reject_becomes_borderline(self):
        """confidence=0.5 (< 0.75) with a standard reject taxonomy → reject downgraded."""
        # Use value_already_correct (llm_can_handle) to test confidence gate
        llm = _llm("F-001", llm_decision="reject", taxonomy="value_already_correct", confidence=0.5,
                   reject_reason="value_already_correct")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "borderline"
        assert result.reject_reason is None

    def test_other_taxonomy_reject_becomes_needs_human(self):
        """taxonomy=other has needs_human fitness → reject always becomes needs_human."""
        llm = _llm("F-001", llm_decision="reject", taxonomy="other", confidence=0.5,
                   reject_reason="low_business_value")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "needs_human"
        assert result.reject_reason is None

    def test_confidence_exactly_threshold_allows_reject(self):
        """confidence == 0.75 → reject is allowed (>= threshold)."""
        llm = _llm("F-001", llm_decision="reject", taxonomy="visual_or_ocr_misread",
                   confidence=LLM_REJECT_CONFIDENCE_THRESHOLD, reject_reason="visual_or_ocr_misread")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "reject"

    def test_visual_or_ocr_high_confidence_reject_allowed(self):
        """visual_or_ocr_misread with confidence=0.90 → reject stays."""
        llm = _llm("F-002", llm_decision="reject", taxonomy="visual_or_ocr_misread",
                   confidence=0.90, reject_reason="visual_or_ocr_misread")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "reject"

    def test_duplicate_high_confidence_reject_allowed(self):
        """duplicate_or_already_covered with confidence=0.80 → reject stays."""
        llm = _llm("F-003", llm_decision="reject", taxonomy="duplicate_or_already_covered",
                   confidence=0.80, reject_reason="duplicate_or_already_covered")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "reject"

    def test_visual_or_ocr_lower_threshold_65(self):
        """For OCR/dupe/norm errors, threshold is 0.65 (lower than default 0.75)."""
        llm = _llm("F-004", llm_decision="reject", taxonomy="visual_or_ocr_misread",
                   confidence=0.68, reject_reason="visual_or_ocr_misread")
        result = _apply_confidence_and_taxonomy_gate(llm)
        # 0.68 >= 0.65 → reject should stay for visual_or_ocr_misread
        assert result.llm_decision == "reject"

    def test_visual_or_ocr_below_65_threshold_becomes_borderline(self):
        """Even OCR errors with confidence < 0.65 → borderline."""
        llm = _llm("F-005", llm_decision="reject", taxonomy="visual_or_ocr_misread",
                   confidence=0.60, reject_reason="visual_or_ocr_misread")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "borderline"

    def test_accept_not_touched_by_confidence_gate(self):
        """Confidence gate only applies to reject; accept is not touched."""
        llm = _llm("F-006", llm_decision="accept", confidence=0.3)
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "accept"

    def test_borderline_not_touched_by_gate(self):
        llm = _llm("F-007", llm_decision="borderline", confidence=0.1)
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "borderline"

    def test_needs_human_passes_through(self):
        llm = _llm("F-008", llm_decision="needs_human", confidence=1.0)
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "needs_human"

    def test_acceptable_design_solution_low_confidence_stays_borderline(self):
        """acceptable_design_solution with low confidence reject → borderline."""
        llm = _llm("F-009", llm_decision="reject", taxonomy="acceptable_design_solution",
                   confidence=0.50, reject_reason="low_business_value")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "borderline"

    def test_explanation_updated_on_low_conf_downgrade(self):
        """Explanation must note the reason for confidence downgrade (standard reject taxonomy)."""
        llm = _llm("F-010", llm_decision="reject", taxonomy="value_already_correct",
                   confidence=0.4, reject_reason="value_already_correct")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "borderline"
        assert "low-confidence" in result.explanation or "confidence" in result.explanation.lower()

    def test_explanation_updated_on_taxonomy_safe_downgrade(self):
        """Explanation must note taxonomy-safe reason when reject downgraded due to fitness."""
        llm = _llm("F-011", llm_decision="reject", taxonomy="other",
                   confidence=0.95, reject_reason="low_business_value")
        result = _apply_confidence_and_taxonomy_gate(llm)
        assert result.llm_decision == "needs_human"
        assert "taxonomy-safe" in result.explanation


# ─── merge_llm_decisions: needs_human handling ───────────────────────────────

class TestNeedsHumanMerge:
    def test_needs_human_merges_as_borderline_not_reject(self):
        """LLM needs_human must produce borderline in final, NOT reject."""
        det = [_det("F-001", decision="accept", ev=EVIDENCE_VALID)]
        llm_decs = [_llm("F-001", llm_decision="needs_human")]
        raw = {"F-001": _raw("F-001")}
        final, accepted, rejected, borderline = merge_llm_decisions(det, llm_decs, raw)
        d = next(x for x in final if x.finding_id == "F-001")
        assert d.decision != "reject", "needs_human must not become reject"
        assert d.decision == "borderline"

    def test_needs_human_does_not_become_accept(self):
        """needs_human keeps as borderline even for valid evidence."""
        det = [_det("F-001", decision="accept", ev=EVIDENCE_VALID, score=9)]
        llm_decs = [_llm("F-001", llm_decision="needs_human")]
        raw = {"F-001": _raw("F-001")}
        final, _, _, _ = merge_llm_decisions(det, llm_decs, raw)
        d = next(x for x in final if x.finding_id == "F-001")
        assert d.decision == "borderline"

    def test_insufficient_source_context_via_merge(self):
        """Full path: reject with insufficient_source_context → needs_human → borderline."""
        det = [_det("F-001", decision="accept", ev=EVIDENCE_VALID)]
        llm_decs = [_llm("F-001", llm_decision="reject",
                         taxonomy="insufficient_source_context",
                         confidence=0.95, reject_reason="no_impact")]
        raw = {"F-001": _raw("F-001")}
        final, _, _, _ = merge_llm_decisions(det, llm_decs, raw)
        d = next(x for x in final if x.finding_id == "F-001")
        # Should be borderline (not reject, not accept)
        assert d.decision == "borderline", (
            f"insufficient_source_context reject must become borderline, got {d.decision}"
        )

    def test_low_confidence_reject_stays_borderline_via_merge(self):
        """LLM reject with confidence=0.5 → borderline in final."""
        det = [_det("F-001", decision="accept", ev=EVIDENCE_VALID)]
        llm_decs = [_llm("F-001", llm_decision="reject", taxonomy="other",
                         confidence=0.5, reject_reason="low_business_value")]
        raw = {"F-001": _raw("F-001")}
        final, _, _, _ = merge_llm_decisions(det, llm_decs, raw)
        d = next(x for x in final if x.finding_id == "F-001")
        assert d.decision == "borderline"

    def test_high_confidence_ocr_reject_stands(self):
        """visual_or_ocr_misread with confidence=0.90 → reject in final."""
        det = [_det("F-001", decision="accept", ev=EVIDENCE_VALID)]
        llm_decs = [_llm("F-001", llm_decision="reject",
                         taxonomy="visual_or_ocr_misread",
                         confidence=0.90, reject_reason="visual_or_ocr_misread")]
        raw = {"F-001": _raw("F-001")}
        final, _, rejected, _ = merge_llm_decisions(det, llm_decs, raw)
        d = next(x for x in final if x.finding_id == "F-001")
        assert d.decision == "reject"

    def test_deterministic_reject_invariant_holds(self):
        """LLM cannot restore a deterministically rejected finding."""
        det = [_det("F-001", decision="reject", ev=EVIDENCE_NONE, score=2,
                    reject_reason="no_evidence")]
        llm_decs = [_llm("F-001", llm_decision="accept", confidence=1.0)]
        raw = {"F-001": _raw("F-001")}
        final, _, _, _ = merge_llm_decisions(det, llm_decs, raw)
        d = next(x for x in final if x.finding_id == "F-001")
        assert d.decision == "reject"  # invariant: det reject stays


# ─── Taxonomy fields in parsed response ──────────────────────────────────────

class TestTaxonomyParsing:
    def _parse(self, items: list[dict]) -> tuple[list[LLMCriticDecision], list[str]]:
        candidate_ids = {item["finding_id"] for item in items}
        return _parse_llm_response(json.dumps(items, ensure_ascii=False), candidate_ids)

    def test_taxonomy_reason_parsed(self):
        items = [{
            "finding_id": "F-001",
            "llm_decision": "reject",
            "human_taxonomy_reason": "visual_or_ocr_misread",
            "confidence": 0.9,
            "usefulness_score": 3,
            "explanation": "OCR misread",
            "evidence_checked": True,
            "source_dependency": "enough_source",
            "rewrite": {"title": None, "description": None, "action_required": None},
        }]
        decisions, errors = self._parse(items)
        assert not errors
        assert len(decisions) == 1
        assert decisions[0].human_taxonomy_reason == "visual_or_ocr_misread"
        assert decisions[0].confidence == 0.9
        assert decisions[0].evidence_checked is True
        assert decisions[0].source_dependency == "enough_source"

    def test_needs_human_decision_parsed(self):
        items = [{
            "finding_id": "F-002",
            "llm_decision": "needs_human",
            "human_taxonomy_reason": "insufficient_source_context",
            "confidence": 0.8,
            "usefulness_score": 6,
            "explanation": "needs cross-section data",
            "evidence_checked": False,
            "source_dependency": "cross_section_required",
            "rewrite": None,
        }]
        decisions, _ = self._parse(items)
        assert decisions[0].llm_decision == "needs_human"
        assert decisions[0].human_taxonomy_reason == "insufficient_source_context"
        assert decisions[0].source_dependency == "cross_section_required"

    def test_invalid_taxonomy_normalized_to_other(self):
        items = [{
            "finding_id": "F-003",
            "llm_decision": "borderline",
            "human_taxonomy_reason": "totally_invented_reason",
            "confidence": 0.7,
            "usefulness_score": 6,
            "explanation": "test",
            "evidence_checked": False,
            "source_dependency": "enough_source",
            "rewrite": None,
        }]
        decisions, _ = self._parse(items)
        assert decisions[0].human_taxonomy_reason == "other"

    def test_rewrite_nested_dict_parsed(self):
        items = [{
            "finding_id": "F-004",
            "llm_decision": "rewrite",
            "human_taxonomy_reason": "other",
            "confidence": 0.8,
            "usefulness_score": 7,
            "explanation": "needs rewrite",
            "evidence_checked": True,
            "source_dependency": "enough_source",
            "rewrite": {
                "title": "Improved title",
                "description": "Better description",
                "action_required": "Fix this",
            },
        }]
        decisions, _ = self._parse(items)
        assert decisions[0].rewritten_title == "Improved title"
        assert decisions[0].rewritten_description == "Better description"
        assert decisions[0].rewritten_action_required == "Fix this"

    def test_confidence_clamped_to_01(self):
        items = [{
            "finding_id": "F-005",
            "llm_decision": "accept",
            "human_taxonomy_reason": "other",
            "confidence": 1.5,  # out of range
            "usefulness_score": 8,
            "explanation": "test",
            "evidence_checked": True,
            "source_dependency": "enough_source",
            "rewrite": None,
        }]
        decisions, _ = self._parse(items)
        assert decisions[0].confidence <= 1.0

    def test_missing_taxonomy_defaults_to_other(self):
        items = [{
            "finding_id": "F-006",
            "llm_decision": "accept",
            "usefulness_score": 8,
            "explanation": "test",
        }]
        decisions, _ = self._parse(items)
        assert decisions[0].human_taxonomy_reason == "other"

    def test_missing_confidence_defaults_to_1(self):
        items = [{
            "finding_id": "F-007",
            "llm_decision": "accept",
            "usefulness_score": 8,
            "explanation": "test",
        }]
        decisions, _ = self._parse(items)
        assert decisions[0].confidence == 1.0


# ─── MockProvider taxonomy support ───────────────────────────────────────────

class TestMockProviderTaxonomy:
    def test_mock_includes_taxonomy_fields(self):
        """MockProvider must return human_taxonomy_reason, confidence, evidence_checked."""
        candidates = [_det("F-001", decision="accept")]
        raw = {"F-001": _raw("F-001")}
        provider = MockProvider()
        resp_text, errors = provider(candidates, raw, "")
        assert not errors
        items = json.loads(resp_text)
        assert len(items) == 1
        item = items[0]
        assert "human_taxonomy_reason" in item
        assert "confidence" in item
        assert "evidence_checked" in item
        assert "source_dependency" in item
        assert item["human_taxonomy_reason"] in VALID_TAXONOMY_REASONS

    def test_mock_taxonomy_injection(self):
        """_taxonomy_reason in raw finding → MockProvider uses it."""
        candidates = [_det("F-001", decision="accept")]
        raw = {"F-001": _raw("F-001",
                              _expected_decision="reject",
                              _taxonomy_reason="visual_or_ocr_misread",
                              _confidence=0.92)}
        provider = MockProvider()
        resp_text, _ = provider(candidates, raw, "")
        items = json.loads(resp_text)
        assert items[0]["llm_decision"] == "reject"
        assert items[0]["human_taxonomy_reason"] == "visual_or_ocr_misread"
        assert abs(items[0]["confidence"] - 0.92) < 0.01

    def test_mock_confidence_injection(self):
        """_confidence injection controls confidence field."""
        candidates = [_det("F-001", decision="accept")]
        raw = {"F-001": _raw("F-001", _confidence=0.42)}
        provider = MockProvider()
        resp_text, _ = provider(candidates, raw, "")
        items = json.loads(resp_text)
        assert abs(items[0]["confidence"] - 0.42) < 0.01

    def test_mock_source_dependency_injection(self):
        candidates = [_det("F-001", decision="accept")]
        raw = {"F-001": _raw("F-001", _source_dependency="cross_section_required")}
        provider = MockProvider()
        resp_text, _ = provider(candidates, raw, "")
        items = json.loads(resp_text)
        assert items[0]["source_dependency"] == "cross_section_required"

    def test_mock_invalid_taxonomy_injection_defaults_other(self):
        candidates = [_det("F-001", decision="accept")]
        raw = {"F-001": _raw("F-001", _taxonomy_reason="invented_reason_xyz")}
        provider = MockProvider()
        resp_text, _ = provider(candidates, raw, "")
        items = json.loads(resp_text)
        assert items[0]["human_taxonomy_reason"] == "other"


# ─── load_prompt: taxonomy prompt priority ────────────────────────────────────

class TestPromptLoader:
    def test_taxonomy_prompt_loaded_by_default(self):
        """When no explicit path given, load_prompt should prefer the taxonomy prompt."""
        prompt_text, label = load_prompt(None)
        # Taxonomy prompt exists in prompts/ directory
        assert "taxonomy" in label.lower() or "visual_or_ocr" in prompt_text.lower(), (
            f"Expected taxonomy prompt to be loaded, got: {label}"
        )

    def test_taxonomy_prompt_contains_six_reasons(self):
        """Taxonomy prompt must document all 6 rejection categories."""
        prompt_text, _ = load_prompt(None)
        for reason in ("visual_or_ocr_misread", "duplicate_or_already_covered",
                       "wrong_norm_context", "acceptable_design_solution",
                       "not_functionally_significant", "insufficient_source_context"):
            assert reason in prompt_text, f"Prompt missing taxonomy reason: {reason}"

    def test_taxonomy_prompt_contains_needs_human_instruction(self):
        prompt_text, _ = load_prompt(None)
        assert "needs_human" in prompt_text

    def test_taxonomy_prompt_contains_confidence_instruction(self):
        prompt_text, _ = load_prompt(None)
        assert "confidence" in prompt_text.lower()


# ─── run_llm_gate end-to-end with taxonomy ───────────────────────────────────

class TestRunLLMGateTaxonomy:
    def test_insufficient_source_never_rejected(self):
        """Full pipeline: insufficient_source reject → borderline."""
        findings = [{
            "id": "F-001",
            "severity": "КРИТИЧЕСКОЕ",
            "category": "normative_refs",
            "problem": "Отсутствует расчёт по СП 385",
            "description": "Расчёт на прогрессирующее обрушение не приведён на чертежах",
            "solution": "Добавить расчёт",
            "evidence": [{"block_id": "BLK-A"}, {"block_id": "BLK-B"}],
            "related_block_ids": ["BLK-A", "BLK-B"],
            # Inject: LLM should reject with insufficient_source_context
            "_expected_decision": "reject",
            "_taxonomy_reason": "insufficient_source_context",
            "_confidence": 0.95,
        }]
        det_result = run_critic_v2_offline(findings)
        raw_by_id = {f["id"]: f for f in findings}
        gate = run_llm_gate(det_result.decisions, raw_by_id, provider="mock")
        final, _, rejected, borderline = merge_llm_decisions(
            det_result.decisions, gate.decisions, raw_by_id
        )
        # Must NOT be in rejected
        assert "F-001" not in [d.finding_id for d in final if d.decision == "reject"], (
            "insufficient_source_context must never produce reject"
        )

    def test_low_confidence_ocr_reject_downgraded(self):
        """visual_or_ocr_misread with confidence=0.5 → borderline, not reject."""
        findings = [{
            "id": "F-002",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "spec_mismatch",
            "problem": "Размер 300мм не соответствует",
            "description": "В спецификации указан размер 300мм, на чертеже 250мм",
            "solution": "Исправить размер",
            "evidence": [{"block_id": "BLK-A"}, {"block_id": "BLK-B"}],
            "related_block_ids": ["BLK-A", "BLK-B"],
            "_expected_decision": "reject",
            "_taxonomy_reason": "visual_or_ocr_misread",
            "_confidence": 0.50,  # Below threshold
        }]
        det_result = run_critic_v2_offline(findings)
        raw_by_id = {f["id"]: f for f in findings}
        gate = run_llm_gate(det_result.decisions, raw_by_id, provider="mock")
        final, _, _, _ = merge_llm_decisions(det_result.decisions, gate.decisions, raw_by_id)
        d = next((x for x in final if x.finding_id == "F-002"), None)
        if d:  # Only check if finding passed det filter
            assert d.decision != "reject", (
                f"Low confidence OCR reject must be borderline, got {d.decision}"
            )

    def test_high_confidence_ocr_reject_stands(self):
        """visual_or_ocr_misread with confidence=0.92 → reject stands."""
        findings = [{
            "id": "F-003",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "spec_mismatch",
            "problem": "Размер 300мм",
            "description": "ИИ прочитал 300, реальное значение 250мм по чертежу",
            "solution": "Исправить размер в спецификации",
            "evidence": [{"block_id": "BLK-A"}, {"block_id": "BLK-B"}],
            "related_block_ids": ["BLK-A", "BLK-B"],
            "_expected_decision": "reject",
            "_taxonomy_reason": "visual_or_ocr_misread",
            "_confidence": 0.92,
        }]
        det_result = run_critic_v2_offline(findings)
        raw_by_id = {f["id"]: f for f in findings}
        gate = run_llm_gate(det_result.decisions, raw_by_id, provider="mock")
        final, _, _, _ = merge_llm_decisions(det_result.decisions, gate.decisions, raw_by_id)
        d = next((x for x in final if x.finding_id == "F-003"), None)
        if d:
            assert d.decision == "reject", (
                f"High-confidence OCR reject should stay reject, got {d.decision}"
            )

    def test_deterministic_reject_never_restored(self):
        """LLM cannot restore det-rejected finding even with accept + high confidence."""
        # Create finding that will be det-rejected (no evidence)
        findings = [{
            "id": "F-004",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "documentation",
            "problem": "Test",
            "description": "No evidence finding",
            "solution": None,
            "evidence": [],
            "related_block_ids": [],
            "_expected_decision": "accept",
            "_confidence": 1.0,
        }]
        det_result = run_critic_v2_offline(findings)
        raw_by_id = {f["id"]: f for f in findings}
        # Verify F-004 was det-rejected
        det_d = next((d for d in det_result.decisions if d.finding_id == "F-004"), None)
        if det_d and det_d.decision == "reject":
            gate = run_llm_gate(det_result.decisions, raw_by_id, provider="mock")
            final, _, _, _ = merge_llm_decisions(det_result.decisions, gate.decisions, raw_by_id)
            d = next(x for x in final if x.finding_id == "F-004")
            assert d.decision == "reject", "Deterministic reject must stay reject"

    def test_weak_evidence_never_accept_with_taxonomy_gate(self):
        """Evidence quality caps must still apply: weak → max borderline."""
        findings = [{
            "id": "F-005",
            "severity": "КРИТИЧЕСКОЕ",
            "category": "cable",
            "problem": "Кабель без FRLS",
            "description": "Кабель ВВГнг-LS вместо FRLS на схеме",
            "solution": "Заменить",
            "risk": "Пожарная безопасность",
            "evidence": [{"block_id": "BLK-SINGLE"}],
            "related_block_ids": ["BLK-SINGLE"],
            "_expected_decision": "accept",
            "_confidence": 1.0,
        }]
        det_result = run_critic_v2_offline(findings)
        raw_by_id = {f["id"]: f for f in findings}
        gate = run_llm_gate(det_result.decisions, raw_by_id, provider="mock")
        final, _, _, _ = merge_llm_decisions(det_result.decisions, gate.decisions, raw_by_id)
        d = next((x for x in final if x.finding_id == "F-005"), None)
        if d:
            ev = d.evidence_quality
            assert not (d.decision == "accept" and ev == "weak"), (
                f"Weak evidence must not reach accept, got decision={d.decision} ev={ev}"
            )


# ─── select_candidates: risk-based ordering ──────────────────────────────────

class TestSelectCandidatesRisk:
    def test_borderline_prioritised_over_accept(self):
        """Borderline findings should appear before accepts of same risk level."""
        decisions = [
            _det("F-acc", decision="accept", ev=EVIDENCE_VALID, score=8),
            _det("F-brd", decision="borderline", ev=EVIDENCE_VALID, score=6),
        ]
        candidates, _ = select_candidates(decisions, max_candidates=10)
        ids = [d.finding_id for d in candidates]
        assert ids.index("F-brd") <= ids.index("F-acc")

    def test_weak_evidence_accept_higher_risk_than_valid(self):
        """Weak-evidence accept should rank higher risk than valid-evidence accept."""
        decisions = [
            _det("F-valid", decision="accept", ev=EVIDENCE_VALID, score=8),
            _det("F-weak", decision="accept", ev=EVIDENCE_WEAK, score=8),
        ]
        candidates, _ = select_candidates(decisions, max_candidates=10)
        ids = [d.finding_id for d in candidates]
        assert ids.index("F-weak") <= ids.index("F-valid")

    def test_reject_not_in_candidates(self):
        decisions = [
            _det("F-rej", decision="reject"),
            _det("F-acc", decision="accept"),
        ]
        candidates, skipped = select_candidates(decisions)
        assert "F-rej" not in [d.finding_id for d in candidates]
        assert "F-rej" in skipped

    def test_none_evidence_not_in_candidates(self):
        decisions = [
            _det("F-none", decision="accept", ev=EVIDENCE_NONE),
            _det("F-val", decision="accept", ev=EVIDENCE_VALID),
        ]
        candidates, skipped = select_candidates(decisions)
        assert "F-none" not in [d.finding_id for d in candidates]
        assert "F-none" in skipped


# ─── Full pipeline integration ────────────────────────────────────────────────

class TestFullPipelineTaxonomy:
    def test_good_findings_not_mass_rejected_by_ocr_gate(self):
        """Good findings with valid evidence should not be mass-rejected by low-confidence."""
        import json
        from pathlib import Path
        good_path = Path("backend/tests/fixtures/findings_review/good_findings.json")
        good_data = json.loads(good_path.read_text(encoding="utf-8"))
        findings = [e["input_finding"] for e in good_data]

        # Inject low-confidence reject for all
        for f in findings:
            f["_expected_decision"] = "reject"
            f["_taxonomy_reason"] = "other"
            f["_confidence"] = 0.4  # too low → should become borderline

        det_result = run_critic_v2_offline(findings)
        raw_by_id = {f["id"]: f for f in findings}
        gate = run_llm_gate(det_result.decisions, raw_by_id, provider="mock")
        final, _, rejected, _ = merge_llm_decisions(det_result.decisions, gate.decisions, raw_by_id)

        # No finding should be rejected due to low-confidence downgrade alone
        for d in final:
            if d.decision == "reject":
                # Only original det-rejects are allowed
                orig = next(x for x in det_result.decisions if x.finding_id == d.finding_id)
                assert orig.decision == "reject", (
                    f"{d.finding_id} rejected by LLM with low confidence (0.4) — safety violation"
                )

    def test_needs_human_count_in_gate_result(self):
        """run_llm_gate should return decisions with needs_human llm_decision."""
        findings = [{
            "id": "F-001",
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "category": "construction_sequence",
            "problem": "Последовательность работ не указана",
            "description": "Порядок возведения элементов не определён в РД",
            "solution": "Уточнить в ППР",
            "evidence": [{"block_id": "BLK-A"}, {"block_id": "BLK-B"}],
            "related_block_ids": ["BLK-A", "BLK-B"],
            "_expected_decision": "reject",
            "_taxonomy_reason": "insufficient_source_context",
            "_confidence": 0.88,
        }]
        det_result = run_critic_v2_offline(findings)
        raw_by_id = {f["id"]: f for f in findings}
        gate = run_llm_gate(det_result.decisions, raw_by_id, provider="mock")
        # After confidence/taxonomy gate, llm_decision should be needs_human
        if gate.decisions:
            d = gate.decisions[0]
            assert d.llm_decision == "needs_human", (
                f"insufficient_source_context should become needs_human, got {d.llm_decision}"
            )


# ─── v2 taxonomy: new llm_can_handle categories ──────────────────────────────

class TestV2TaxonomyNewCategories:
    """
    Tests for 4 new llm_can_handle taxonomy reasons added in v2:
      - value_already_correct
      - false_positive_due_to_missing_context
      - requirement_not_mandatory
      - already_resolved_by_project_note

    And 3 borderline/needs_human categories (never reject):
      - outside_audit_scope → borderline
      - human_marked_minor → borderline
      - design_stage_limitation → borderline
    """

    def _make_llm_decision(
        self,
        taxonomy: str,
        llm_decision: str = "reject",
        confidence: float = 0.85,
        evidence_checked: bool = True,
        source_dependency: str = "enough_source",
    ) -> LLMCriticDecision:
        return LLMCriticDecision(
            finding_id="F-001",
            llm_decision=llm_decision,
            usefulness_score=4,
            reject_reason=taxonomy if taxonomy in {
                "value_already_correct", "false_positive_due_to_missing_context",
                "requirement_not_mandatory", "already_resolved_by_project_note",
                "visual_or_ocr_misread", "not_functionally_significant",
            } else "unclear",
            explanation=f"test explanation [{taxonomy}]",
            human_taxonomy_reason=taxonomy,
            confidence=confidence,
            evidence_checked=evidence_checked,
            source_dependency=source_dependency,
        )

    # ── value_already_correct ──────────────────────────────────────────────────

    def test_value_already_correct_high_conf_reject_allowed(self):
        ld = self._make_llm_decision("value_already_correct", "reject", 0.85)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    def test_value_already_correct_low_conf_downgraded(self):
        ld = self._make_llm_decision("value_already_correct", "reject", 0.60)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"
        assert "[low-confidence" in result.explanation

    def test_value_already_correct_at_threshold_allowed(self):
        ld = self._make_llm_decision("value_already_correct", "reject", 0.75)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    def test_value_already_correct_just_below_threshold_downgraded(self):
        ld = self._make_llm_decision("value_already_correct", "reject", 0.74)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    # ── false_positive_due_to_missing_context ─────────────────────────────────

    def test_false_positive_missing_context_high_conf_reject_allowed(self):
        ld = self._make_llm_decision("false_positive_due_to_missing_context", "reject", 0.80)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    def test_false_positive_missing_context_low_conf_downgraded(self):
        ld = self._make_llm_decision("false_positive_due_to_missing_context", "reject", 0.50)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    # ── requirement_not_mandatory ─────────────────────────────────────────────

    def test_requirement_not_mandatory_high_conf_reject_allowed(self):
        ld = self._make_llm_decision("requirement_not_mandatory", "reject", 0.80)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    def test_requirement_not_mandatory_low_conf_downgraded(self):
        ld = self._make_llm_decision("requirement_not_mandatory", "reject", 0.70)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    # ── already_resolved_by_project_note ──────────────────────────────────────

    def test_already_resolved_high_conf_reject_allowed(self):
        ld = self._make_llm_decision("already_resolved_by_project_note", "reject", 0.80)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    def test_already_resolved_low_conf_downgraded(self):
        ld = self._make_llm_decision("already_resolved_by_project_note", "reject", 0.60)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    # ── borderline-only categories: never reject ──────────────────────────────

    def test_outside_audit_scope_reject_downgraded_to_borderline(self):
        """outside_audit_scope has borderline_llm fitness — reject never allowed."""
        ld = self._make_llm_decision("outside_audit_scope", "reject", 0.95)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"
        assert "[taxonomy-safe: outside_audit_scope]" in result.explanation

    def test_human_marked_minor_reject_downgraded_to_borderline(self):
        ld = self._make_llm_decision("human_marked_minor", "reject", 0.95)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"
        assert "[taxonomy-safe: human_marked_minor]" in result.explanation

    def test_design_stage_limitation_reject_downgraded_to_borderline(self):
        ld = self._make_llm_decision("design_stage_limitation", "reject", 0.95)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"
        assert "[taxonomy-safe: design_stage_limitation]" in result.explanation

    def test_other_taxonomy_reject_downgraded_to_needs_human(self):
        """'other' has needs_human fitness — reject becomes needs_human."""
        ld = self._make_llm_decision("other", "reject", 0.95)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "needs_human"

    def test_outside_audit_scope_accept_unchanged(self):
        """Accept decisions are never changed by the gate."""
        ld = self._make_llm_decision("outside_audit_scope", "accept", 0.90)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "accept"

    def test_outside_audit_scope_borderline_unchanged(self):
        ld = self._make_llm_decision("outside_audit_scope", "borderline", 0.90)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    # ── LLM_FITNESS_MAP completeness ─────────────────────────────────────────

    def test_all_valid_taxonomy_reasons_in_fitness_map(self):
        from backend.app.pipeline.stages.findings_review.critic_v2 import LLM_FITNESS_MAP
        for reason in VALID_TAXONOMY_REASONS:
            assert reason in LLM_FITNESS_MAP, f"Missing from LLM_FITNESS_MAP: {reason}"

    def test_fitness_map_values_valid(self):
        from backend.app.pipeline.stages.findings_review.critic_v2 import LLM_FITNESS_MAP
        valid_values = {"llm_can_handle", "borderline_llm", "needs_human"}
        for reason, fitness in LLM_FITNESS_MAP.items():
            assert fitness in valid_values, f"{reason} has invalid fitness: {fitness}"

    def test_new_llm_can_handle_categories_in_map(self):
        from backend.app.pipeline.stages.findings_review.critic_v2 import LLM_FITNESS_MAP
        new_can_handle = [
            "value_already_correct",
            "false_positive_due_to_missing_context",
            "requirement_not_mandatory",
            "already_resolved_by_project_note",
        ]
        for cat in new_can_handle:
            assert LLM_FITNESS_MAP.get(cat) == "llm_can_handle", (
                f"{cat} should be llm_can_handle, got {LLM_FITNESS_MAP.get(cat)}"
            )

    def test_borderline_only_categories_in_map(self):
        from backend.app.pipeline.stages.findings_review.critic_v2 import LLM_FITNESS_MAP
        borderline_only = ["outside_audit_scope", "human_marked_minor", "design_stage_limitation"]
        for cat in borderline_only:
            assert LLM_FITNESS_MAP.get(cat) == "borderline_llm", (
                f"{cat} should be borderline_llm, got {LLM_FITNESS_MAP.get(cat)}"
            )

    # ── MockProvider with new taxonomy reasons ───────────────────────────────

    def test_mock_provider_value_already_correct_reject(self):
        finding = {
            "id": "F-001",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "notation",
            "problem": "Обозначение не стандартное",
            "description": "В чертеже использована нестандартная нотация",
            "solution": "Заменить на ГОСТ-нотацию",
            "evidence": [],
            "_expected_decision": "reject",
            "_taxonomy_reason": "value_already_correct",
            "_confidence": 0.85,
        }
        det = run_critic_v2_offline([finding])
        raw = {finding["id"]: finding}
        provider = MockProvider()
        from backend.app.pipeline.stages.findings_review.critic_v2.models import QualityDecision
        from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import select_candidates
        candidates, _ = select_candidates(det.decisions)
        if not candidates:
            # If deterministic rejected, nothing to test
            return
        resp, errs = provider(candidates, raw, "test prompt")
        data = json.loads(resp)
        assert errs == []
        item = next((d for d in data if d["finding_id"] == "F-001"), None)
        assert item is not None
        assert item["llm_decision"] == "reject"
        assert item["human_taxonomy_reason"] == "value_already_correct"
        assert item["reject_reason"] == "value_already_correct"

    def test_mock_provider_outside_audit_scope_borderline(self):
        """outside_audit_scope injected → after gate becomes borderline, not reject."""
        finding = {
            "id": "F-002",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "documentation",
            "problem": "Вопрос к смежному разделу",
            "description": "Это замечание по разделу АР, а не КЖ",
            "solution": "Перенаправить в АР",
            "evidence": [{"block_id": "BLK-1"}],
            "_expected_decision": "reject",
            "_taxonomy_reason": "outside_audit_scope",
            "_confidence": 0.92,
        }
        det = run_critic_v2_offline([finding])
        raw = {finding["id"]: finding}
        gate = run_llm_gate(det.decisions, raw, provider="mock")
        if gate.decisions:
            d = gate.decisions[0]
            # After taxonomy gate: outside_audit_scope → borderline (never reject)
            assert d.llm_decision in ("borderline", "needs_human"), (
                f"outside_audit_scope should be borderline/needs_human, got {d.llm_decision}"
            )

    def test_new_taxonomy_reasons_in_valid_set(self):
        new_reasons = [
            "value_already_correct",
            "false_positive_due_to_missing_context",
            "requirement_not_mandatory",
            "already_resolved_by_project_note",
            "outside_audit_scope",
            "human_marked_minor",
            "design_stage_limitation",
        ]
        for reason in new_reasons:
            assert reason in VALID_TAXONOMY_REASONS, f"Missing from VALID_TAXONOMY_REASONS: {reason}"

    def test_new_reject_reasons_in_valid_reject_reasons(self):
        from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import VALID_LLM_REJECT_REASONS
        new_reject = [
            "value_already_correct",
            "false_positive_due_to_missing_context",
            "requirement_not_mandatory",
            "already_resolved_by_project_note",
        ]
        for reason in new_reject:
            assert reason in VALID_LLM_REJECT_REASONS, (
                f"Missing from VALID_LLM_REJECT_REASONS: {reason}"
            )

    # ── not_functionally_significant: confidence 0.80 ─────────────────────────

    def test_not_functionally_significant_requires_0_80(self):
        """not_functionally_significant has stricter confidence gate (0.80)."""
        ld = self._make_llm_decision("not_functionally_significant", "reject", 0.78)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    def test_not_functionally_significant_at_0_80_allowed(self):
        ld = self._make_llm_decision("not_functionally_significant", "reject", 0.80)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    # ── invariants: det reject not restored by new categories ────────────────

    def test_det_reject_not_restored_by_new_taxonomy(self):
        """Deterministic reject cannot be reversed by LLM, even with new taxonomy."""
        finding = {
            "id": "F-det-rej",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "notation",
            "problem": "Test",
            "description": "",
            "solution": "",
            "evidence": [],
            "_expected_decision": "accept",
            "_taxonomy_reason": "value_already_correct",
            "_confidence": 0.90,
        }
        det = run_critic_v2_offline([finding])
        raw = {finding["id"]: finding}
        det_map = {d.finding_id: d for d in det.decisions}
        if det_map.get("F-det-rej") and det_map["F-det-rej"].decision == "reject":
            gate = run_llm_gate(det.decisions, raw, provider="mock")
            final, _, _, _ = merge_llm_decisions(det.decisions, gate.decisions, raw)
            final_map = {d.finding_id: d for d in final}
            assert final_map["F-det-rej"].decision == "reject", (
                "Deterministic reject must stay reject even when LLM says accept"
            )


# ─── Policy v3: source_dependency + evidence_checked + contradictory label ───

class TestPolicyV3SourceDependencyAndEvidenceChecked:
    """
    Tests for new policy rules:
    R3. source_dependency: cross_section → needs_human; needs_more → borderline
    R4. evidence_checked=False → reject downgraded to borderline
    C1-C4. Contradictory accept+rejection-taxonomy label fix
    """

    def _ld(
        self,
        decision: str = "reject",
        taxonomy: str = "value_already_correct",
        confidence: float = 0.85,
        evidence_checked: bool = True,
        source_dependency: str = "enough_source",
    ) -> LLMCriticDecision:
        return LLMCriticDecision(
            finding_id="F-test",
            llm_decision=decision,
            usefulness_score=4,
            reject_reason=taxonomy if decision == "reject" else None,
            explanation="test",
            human_taxonomy_reason=taxonomy,
            confidence=confidence,
            evidence_checked=evidence_checked,
            source_dependency=source_dependency,
        )

    # ── R3: source_dependency checks ─────────────────────────────────────────

    def test_reject_cross_section_required_becomes_needs_human(self):
        ld = self._ld("reject", source_dependency="cross_section_required")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "needs_human"
        assert "cross-section-required" in result.explanation

    def test_reject_needs_more_context_becomes_borderline(self):
        ld = self._ld("reject", source_dependency="needs_more_context")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"
        assert "needs-more-context" in result.explanation

    def test_reject_enough_source_stays_reject(self):
        ld = self._ld("reject", source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    # ── R4: evidence_checked=False ────────────────────────────────────────────

    def test_reject_no_evidence_checked_becomes_borderline(self):
        ld = self._ld("reject", evidence_checked=False)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"
        assert "evidence-not-checked" in result.explanation

    def test_reject_evidence_checked_true_allowed(self):
        ld = self._ld("reject", evidence_checked=True)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    def test_evidence_checked_false_takes_priority_over_conf(self):
        """evidence_checked=False should downgrade even if confidence is high."""
        ld = self._ld("reject", confidence=0.98, evidence_checked=False)
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    # ── C1: Contradictory label fix (accept→reject) ────────────────────────

    def test_accept_value_already_correct_enough_source_becomes_borderline(self):
        """C1-semantic: accept + value_already_correct (semantic) → borderline for review.
        Not reject: LLM may misclassify valid findings as value_already_correct."""
        ld = self._ld("accept", "value_already_correct", confidence=0.85,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"
        assert "contradictory-label" in result.explanation

    def test_accept_already_resolved_enough_source_becomes_borderline(self):
        """Semantic rejection taxonomy → borderline (not reject for safety)."""
        ld = self._ld("accept", "already_resolved_by_project_note", confidence=0.80,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    def test_accept_requirement_not_mandatory_enough_source_becomes_borderline(self):
        ld = self._ld("accept", "requirement_not_mandatory", confidence=0.82,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    def test_accept_false_positive_missing_context_becomes_reject(self):
        """Technical taxonomy (false_positive_due_to_missing_context) → safe to reject."""
        ld = self._ld("accept", "false_positive_due_to_missing_context", confidence=0.80,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    def test_accept_visual_or_ocr_misread_becomes_reject(self):
        """Technical taxonomy: OCR misread → safe to reject (lower threshold 0.65)."""
        ld = self._ld("accept", "visual_or_ocr_misread", confidence=0.70,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    # ── C2: Contradictory label + source_dependency issues ────────────────────

    def test_accept_value_already_correct_needs_more_context_becomes_borderline(self):
        """C2b: accept+rejection-taxonomy but needs_more_context → borderline."""
        ld = self._ld("accept", "value_already_correct", confidence=0.85,
                      evidence_checked=True, source_dependency="needs_more_context")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"
        assert "contradictory-label" in result.explanation

    def test_accept_value_already_correct_cross_section_becomes_needs_human(self):
        ld = self._ld("accept", "value_already_correct", confidence=0.85,
                      evidence_checked=True, source_dependency="cross_section_required")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "needs_human"

    # ── C3: Contradictory label + evidence_checked=False ──────────────────────

    def test_accept_value_already_correct_no_evidence_checked_becomes_borderline(self):
        """C3: accept+rejection-taxonomy but evidence_checked=False → borderline."""
        ld = self._ld("accept", "value_already_correct", confidence=0.85,
                      evidence_checked=False, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"
        assert "contradictory-label" in result.explanation

    # ── C4: Contradictory label + low confidence ──────────────────────────────

    def test_accept_value_already_correct_low_conf_becomes_borderline(self):
        """C4: accept+rejection-taxonomy but conf<0.75 → borderline."""
        ld = self._ld("accept", "value_already_correct", confidence=0.70,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "borderline"

    def test_accept_value_already_correct_at_threshold_becomes_borderline(self):
        """value_already_correct is a semantic taxonomy → contradictory accept → borderline (safe)."""
        ld = self._ld("accept", "value_already_correct", confidence=0.75,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        # Semantic taxonomy → borderline for safety (not reject)
        assert result.llm_decision == "borderline"

    def test_accept_visual_ocr_at_threshold_becomes_reject(self):
        """Technical taxonomy at threshold 0.65 → reject allowed."""
        ld = self._ld("accept", "visual_or_ocr_misread", confidence=0.65,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "reject"

    # ── Non-rejection taxonomy: accept unchanged ──────────────────────────────

    def test_accept_other_taxonomy_not_converted(self):
        """'other' is needs_human fitness → accept should NOT be promoted to reject."""
        ld = self._ld("accept", "other", confidence=0.95,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "accept"

    def test_accept_acceptable_design_solution_not_converted(self):
        """borderline_llm fitness → accept stays accept."""
        ld = self._ld("accept", "acceptable_design_solution", confidence=0.95,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "accept"

    def test_accept_insufficient_source_context_not_converted(self):
        """needs_human fitness → accept stays accept (handled via needs_human only when reject)."""
        ld = self._ld("accept", "insufficient_source_context", confidence=0.95,
                      evidence_checked=True, source_dependency="enough_source")
        result = _apply_confidence_and_taxonomy_gate(ld)
        assert result.llm_decision == "accept"

    # ── Safety invariants ────────────────────────────────────────────────────

    def test_det_reject_not_affected_by_contradictory_fix(self):
        """Deterministic reject always immutable in merge."""
        finding = {
            "id": "F-det",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "notation",
            "problem": "No evidence finding",
            "description": "",
            "solution": "",
            "evidence": [],
        }
        det = run_critic_v2_offline([finding])
        raw = {finding["id"]: finding}
        det_map = {d.finding_id: d for d in det.decisions}
        if det_map.get("F-det") and det_map["F-det"].decision == "reject":
            # Even if LLM says accept with contradictory taxonomy, det reject stays
            gate = run_llm_gate(det.decisions, raw, provider="mock")
            final, _, _, _ = merge_llm_decisions(det.decisions, gate.decisions, raw)
            assert any(d.finding_id == "F-det" and d.decision == "reject" for d in final)

    def test_contradictory_fix_technical_taxonomy_reject_in_merge(self):
        """Technical taxonomy (visual_or_ocr_misread): accept → reject in gate and merge."""
        finding = {
            "id": "F-tech",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "notation",
            "problem": "OCR прочитал B-X22 как класс бетона",
            "description": "Неверное чтение марки хомута",
            "solution": "Исправить распознавание",
            "evidence": [{"block_id": "BLK-A", "type": "text"}],
            "related_block_ids": ["BLK-A"],
            "_expected_decision": "accept",
            "_taxonomy_reason": "visual_or_ocr_misread",
            "_confidence": 0.85,
            "_source_dependency": "enough_source",
        }
        det = run_critic_v2_offline([finding])
        raw = {finding["id"]: finding}
        det_decisions = det.decisions
        det_map = {d.finding_id: d for d in det_decisions}

        if det_map.get("F-tech") and det_map["F-tech"].decision != "reject":
            gate = run_llm_gate(det_decisions, raw, provider="mock")
            gated = {d.finding_id: d for d in gate.decisions}
            if "F-tech" in gated:
                # Technical taxonomy: should be reject
                assert gated["F-tech"].llm_decision == "reject", (
                    f"Expected gate to convert accept→reject via technical fix, "
                    f"got {gated['F-tech'].llm_decision}. explanation: {gated['F-tech'].explanation}"
                )

    def test_contradictory_fix_semantic_taxonomy_becomes_borderline_in_merge(self):
        """Semantic taxonomy (value_already_correct): accept → borderline (not reject)."""
        finding = {
            "id": "F-sem",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "notation",
            "problem": "В чертеже стандартная нотация",
            "description": "Обозначение условное стандартное",
            "solution": "Заменить нотацию",
            "evidence": [{"block_id": "BLK-A", "type": "text"}],
            "related_block_ids": ["BLK-A"],
            "_expected_decision": "accept",
            "_taxonomy_reason": "value_already_correct",
            "_confidence": 0.85,
            "_source_dependency": "enough_source",
        }
        det = run_critic_v2_offline([finding])
        raw = {finding["id"]: finding}
        det_decisions = det.decisions
        det_map = {d.finding_id: d for d in det_decisions}

        if det_map.get("F-sem") and det_map["F-sem"].decision != "reject":
            gate = run_llm_gate(det_decisions, raw, provider="mock")
            gated = {d.finding_id: d for d in gate.decisions}
            if "F-sem" in gated:
                # Semantic taxonomy: safe fix → borderline (not reject, to avoid false_reject)
                assert gated["F-sem"].llm_decision in ("borderline", "needs_human"), (
                    f"Expected borderline for semantic contradictory label, "
                    f"got {gated['F-sem'].llm_decision}"
                )
