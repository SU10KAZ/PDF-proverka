"""
critic_v2/llm_gate.py
----------------------
LLM critic gate — second layer after deterministic critic v2.

NOT connected to production pipeline.
Does NOT touch runner.py, manager.py, or production prompts.

Pipeline position:
    deterministic engine → [llm_gate] → final decisions

Gate responsibilities:
    1. Filter candidates: only accept/borderline, non-merged, evidence != none
    2. Call LLM provider (mock | claude_runner | openrouter)
    3. Parse LLM response into LLMCriticDecision list
    4. Enforce evidence quality hard caps on LLM output
    5. Enforce confidence gate: confidence < 0.75 → borderline
    6. Enforce taxonomy safety: insufficient_source_context → needs_human/borderline
    7. Merge LLM decisions with deterministic decisions

Evidence quality caps (enforced on LLM output, same as scorer):
    none    → LLM skips entirely (rule-rejected)
    weak    → max borderline (LLM cannot promote to accept)
    partial → max borderline, exception: КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ + valid LLM → accept
    valid   → accept possible

Confidence gate (applied BEFORE merge):
    confidence < 0.75 → reject downgraded to borderline
    insufficient_source_context taxonomy → reject forbidden, kept as needs_human

Human taxonomy reasons supported:
    visual_or_ocr_misread, duplicate_or_already_covered, wrong_norm_context,
    acceptable_design_solution, not_functionally_significant,
    insufficient_source_context, other

needs_human handling:
    needs_human → merged as borderline (never reject)

Invariants:
    - deterministic reject is never restored by LLM
    - deterministic merge is never restored by LLM
    - LLM rewrite stored in artifacts only, never touches source findings
    - LLM cannot raise a finding above the deterministic evidence cap
    - LLM reject requires confidence ≥ 0.75 (except for obvious OCR/dupe cases)
    - insufficient_source_context never becomes reject
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

from .models import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    QualityDecision,
)

# ─── Default prompt paths ────────────────────────────────────────────────────

_PROMPT_DEFAULT = Path(
    "Experiments_Kuldyaev/new_critic/findings_critic_task.ru.v1.md"
)

# Taxonomy-aware prompt (preferred when it exists)
_PROMPT_TAXONOMY = Path(__file__).parent / "prompts" / "llm_gate_human_taxonomy.ru.md"

# Minimum confidence for LLM reject to stand; below this → borderline
LLM_REJECT_CONFIDENCE_THRESHOLD = 0.75

# Human taxonomy reasons where reject is NEVER allowed
_TAXONOMY_NO_REJECT = {"insufficient_source_context"}

# Taxonomy reasons where reject is allowed at lower confidence (0.65)
# (clear OCR/dupe/norm errors: very obvious to verify)
_TAXONOMY_HIGH_CONFIDENCE_REJECT = {
    "visual_or_ocr_misread",
    "duplicate_or_already_covered",
    "wrong_norm_context",
}

# Taxonomy reasons that allow reject at standard threshold (0.75)
# (new v2 categories: value errors, context artifacts, non-mandatory requirements)
_TAXONOMY_STANDARD_REJECT = {
    "value_already_correct",
    "false_positive_due_to_missing_context",
    "requirement_not_mandatory",
    "already_resolved_by_project_note",
    "not_functionally_significant",   # 0.80 enforced in gate
}

# LLM fitness mapping: taxonomy reason → automation suitability
# llm_can_handle: LLM can safely reject
# borderline_llm: LLM should return borderline/needs_human, not reject
# needs_human: always needs_human, never reject
LLM_FITNESS_MAP: dict[str, str] = {
    # Original 6
    "visual_or_ocr_misread": "llm_can_handle",
    "duplicate_or_already_covered": "llm_can_handle",
    "wrong_norm_context": "llm_can_handle",
    "not_functionally_significant": "llm_can_handle",
    "acceptable_design_solution": "borderline_llm",
    "insufficient_source_context": "needs_human",
    # New v2 categories
    "value_already_correct": "llm_can_handle",
    "false_positive_due_to_missing_context": "llm_can_handle",
    "requirement_not_mandatory": "llm_can_handle",
    "already_resolved_by_project_note": "llm_can_handle",
    # Borderline/needs_human categories (not safe for auto-reject)
    "outside_audit_scope": "borderline_llm",
    "human_marked_minor": "borderline_llm",
    "design_stage_limitation": "borderline_llm",
    # Fallback
    "other": "needs_human",
}

# ── Rejection-oriented taxonomy reasons ──────────────────────────────────────
# These taxonomy labels IMPLY the finding is invalid (value is actually correct,
# norm doesn't apply, etc.). When LLM labels a finding with one of these BUT
# returns `accept`, the decision is contradictory. The gate fixes this by
# converting `accept → reject` if safety preconditions are met.
#
# Only TECHNICAL error categories are safe for auto-promotion to reject.
# Semantic categories (value_already_correct, requirement_not_mandatory, etc.)
# are NOT safe for auto-reject because LLM may misclassify valid findings.
# Those get downgraded to borderline instead when contradictory.
_REJECTION_ORIENTED_TAXONOMIES = {
    "visual_or_ocr_misread",
    "duplicate_or_already_covered",
    "wrong_norm_context",
    "value_already_correct",
    "false_positive_due_to_missing_context",
    "requirement_not_mandatory",
    "already_resolved_by_project_note",
    "wrong_element_or_location",
}

# Technical-error categories: safe for accept→reject promotion
# (OCR misread and page-nesting artifacts are objectively verifiable)
_TECHNICAL_REJECTION_TAXONOMIES = {
    "visual_or_ocr_misread",
    "false_positive_due_to_missing_context",
}

# Semantic categories: contradictory accept → borderline (not reject)
# The LLM may misclassify valid findings as these; safer to hold as borderline.
_SEMANTIC_REJECTION_TAXONOMIES = {
    "duplicate_or_already_covered",
    "wrong_norm_context",
    "value_already_correct",
    "requirement_not_mandatory",
    "already_resolved_by_project_note",
    "wrong_element_or_location",
}

# ── source_dependency policy ──────────────────────────────────────────────────
# When source_dependency is not enough_source, reject is not safe:
#   - needs_more_context  → borderline
#   - cross_section_required → needs_human (cross-section context unavailable offline)
_SOURCE_DEP_CROSS_SECTION = {"cross_section_required"}
_SOURCE_DEP_NEEDS_MORE = {"needs_more_context"}

_FALLBACK_PROMPT = """
# Offline critic v2 fallback prompt

You are a findings quality critic. For each finding in the input, decide:

- llm_decision: accept / reject / borderline / rewrite
- usefulness_score: 0-10
- reject_reason: one of: no_evidence, no_action, no_impact, generic_wording,
  assumption_without_fact, unsupported_by_source, duplicate, low_business_value, unclear
- explanation: one sentence
- rewritten_title: improved title or null
- rewritten_description: improved description or null
- rewritten_action_required: improved action or null

Criteria:
- Accept: concrete fact + evidence + impact + action_required
- Reject: generic wording / no evidence / no impact / speculation without fact
- Borderline: real defect but evidence or impact is weak
- Rewrite: finding is worth keeping but needs clearer wording

Output JSON array, one entry per finding:
[{"finding_id": "...", "llm_decision": "...", "usefulness_score": 0, ...}]
""".strip()

# ─── Valid LLM decision / reason codes ───────────────────────────────────────

VALID_LLM_DECISIONS = {"accept", "reject", "borderline", "needs_human", "rewrite"}

VALID_LLM_REJECT_REASONS = {
    "no_evidence", "no_action", "no_impact", "generic_wording",
    "assumption_without_fact", "unsupported_by_source", "duplicate",
    "low_business_value", "unclear",
    # taxonomy-aware reasons (v1)
    "visual_or_ocr_misread", "duplicate_or_already_covered",
    "wrong_norm_context", "not_functionally_significant",
    # taxonomy-aware reasons (v2)
    "value_already_correct", "false_positive_due_to_missing_context",
    "requirement_not_mandatory", "already_resolved_by_project_note",
}

VALID_TAXONOMY_REASONS = {
    # v1 original
    "visual_or_ocr_misread",
    "duplicate_or_already_covered",
    "wrong_norm_context",
    "acceptable_design_solution",
    "not_functionally_significant",
    "insufficient_source_context",
    # v2 new llm_can_handle categories
    "value_already_correct",
    "false_positive_due_to_missing_context",
    "requirement_not_mandatory",
    "already_resolved_by_project_note",
    # v2 borderline/needs_human categories (not safe for auto-reject)
    "outside_audit_scope",
    "human_marked_minor",
    "design_stage_limitation",
    # fallback
    "other",
}

VALID_SOURCE_DEPENDENCIES = {"enough_source", "needs_more_context", "cross_section_required"}

_HIGH_SEVERITY = {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"}

# ─── HIGH_SCORE_VALID_ACCEPT_GUARD threshold ─────────────────────────────────
# Deterministic accepts with score >= this AND evidence_quality=valid are
# protected from single-pass LLM hard-reject. LLM reject → downgraded to
# borderline (or needs_human when source context is insufficient).
HIGH_SCORE_VALID_ACCEPT_GUARD_THRESHOLD = 8


# ─── Data structures ─────────────────────────────────────────────────────────

@dataclass
class LLMCriticDecision:
    """One LLM critic decision for a single finding."""
    finding_id: str
    llm_decision: str               # accept / reject / borderline / needs_human / rewrite
    usefulness_score: int           # 0-10
    reject_reason: Optional[str]    # from VALID_LLM_REJECT_REASONS or None
    explanation: str
    rewritten_title: Optional[str] = None
    rewritten_description: Optional[str] = None
    rewritten_action_required: Optional[str] = None
    provider: str = "mock"
    raw_response: Optional[dict] = field(default=None, repr=False)
    # taxonomy-aware fields (v2)
    human_taxonomy_reason: Optional[str] = None   # from VALID_TAXONOMY_REASONS
    confidence: float = 1.0                       # LLM-reported confidence 0.0-1.0
    evidence_checked: bool = False                # LLM confirmed it checked evidence blocks
    source_dependency: str = "enough_source"      # enough_source|needs_more_context|cross_section_required


@dataclass
class LLMGateResult:
    """Result of the LLM gate pass."""
    candidates_sent: int
    decisions: list[LLMCriticDecision]
    prompt_path_used: str
    provider_used: str
    skipped_ids: list[str]          # rule-rejected / merged / evidence-none
    errors: list[str]


# ─── Prompt loader ────────────────────────────────────────────────────────────

def load_prompt(prompt_path: Optional[Path] = None) -> tuple[str, str]:
    """
    Load critic prompt text.

    Priority:
      1. Explicit --prompt-path (if provided and exists)
      2. Taxonomy-aware prompt (prompts/llm_gate_human_taxonomy.ru.md)
      3. Legacy experimental prompt
      4. Hardcoded fallback

    Returns:
        (prompt_text, path_used_label)
    """
    # 1. Explicit path provided
    if prompt_path is not None:
        if prompt_path.exists():
            return prompt_path.read_text(encoding="utf-8"), str(prompt_path)
        else:
            print(f"[llm_gate] WARNING: prompt_path not found: {prompt_path}, trying taxonomy prompt")

    # 2. Taxonomy-aware prompt (preferred)
    if _PROMPT_TAXONOMY.exists():
        return _PROMPT_TAXONOMY.read_text(encoding="utf-8"), str(_PROMPT_TAXONOMY)

    # 3. Legacy experimental path
    if _PROMPT_DEFAULT.exists():
        return _PROMPT_DEFAULT.read_text(encoding="utf-8"), str(_PROMPT_DEFAULT)

    # 4. Hardcoded fallback
    return _FALLBACK_PROMPT, "<fallback:hardcoded>"


# ─── Candidate selection ─────────────────────────────────────────────────────

# ── Candidate strategy names ──────────────────────────────────────────────────
CANDIDATE_STRATEGY_CONSERVATIVE = "conservative"
CANDIDATE_STRATEGY_EXPANDED = "expanded"
CANDIDATE_STRATEGY_BROAD = "broad"

VALID_CANDIDATE_STRATEGIES = {
    CANDIDATE_STRATEGY_CONSERVATIVE,
    CANDIDATE_STRATEGY_EXPANDED,
    CANDIDATE_STRATEGY_BROAD,
}

# Categories with higher false_accept risk based on human benchmark analysis
_HIGH_FALSE_ACCEPT_RISK_CATEGORIES = {
    "spec_mismatch", "normative_refs", "normative", "documentation",
    "cover_thickness", "rebar_class", "rebar_geometry", "concrete_class",
    "lap_length", "anchorage", "stirrups", "fire_rating",
    "ar_kj_coordination", "km_kj_coordination",
}

# ── Expanded strategy: risk categories that signal human-rejection probability ─
# These categories often correspond to human rejection reasons in the benchmark.
_EXPANDED_RISK_CATEGORIES = {
    # Original high-FA-risk categories
    "spec_mismatch", "normative_refs", "normative", "documentation",
    "cover_thickness", "rebar_class", "rebar_geometry", "concrete_class",
    "lap_length", "anchorage", "stirrups", "fire_rating",
    "ar_kj_coordination", "km_kj_coordination",
    # Additional high-rejection-probability categories
    "visual_or_ocr_misread", "duplicate_or_already_covered",
    "wrong_norm_context", "value_already_correct",
    "false_positive_due_to_missing_context", "requirement_not_mandatory",
    "already_resolved_by_project_note", "not_functionally_significant",
    "acceptable_design_solution", "low_business_value", "no_impact",
}

# ── Taxonomy marker words in text (title/description/recommendation) ──────────
# Presence of these indicates higher human-rejection probability.
_EXPANDED_TAXONOMY_MARKERS = {
    "гост", "сп ", "норма", "не указано", "отсутствует", "расхождение",
    "дублирует", "уже указано", "общие указания", "спецификация",
    "ведомость", "нулевое", "ocr", "опечатка", "не требуется",
    "не применим", "не обязательн", "уже учтен",
}

# ── Protected-from-hidden score/evidence floor ────────────────────────────────
# Candidates at or above this score+evidence level are sent to LLM with
# `protected=True`, meaning LLM can advise but cannot trigger hidden_by_critic.
_EXPANDED_PROTECTED_SCORE = 8
_EXPANDED_PROTECTED_EVIDENCE = EVIDENCE_VALID


def _false_accept_risk_score(d: QualityDecision) -> int:
    """
    Heuristic priority score: higher = more likely to be a false accept.
    Used to sort candidates so LLM reviews the highest-risk ones first.
    """
    score = 0
    if d.evidence_quality in (EVIDENCE_WEAK, EVIDENCE_PARTIAL):
        score += 3
    if d.decision == "accept":
        score += 2
    cat = (d.impact_area or "").lower()
    if cat in _HIGH_FALSE_ACCEPT_RISK_CATEGORIES:
        score += 1
    # Higher critic score + weak evidence = higher risk
    if d.usefulness_score >= 7 and d.evidence_quality == EVIDENCE_WEAK:
        score += 2
    return score


def _false_accept_risk_score_expanded(d: QualityDecision, raw_finding: Optional[dict] = None) -> int:
    """
    Extended risk score for expanded/broad strategies.
    Higher = more likely to be a false accept (LLM should review this first).
    """
    score = _false_accept_risk_score(d)

    # Additional signals for expanded strategy
    cat = (d.impact_area or "").lower()
    if cat in _EXPANDED_RISK_CATEGORIES:
        score += 2

    # Taxonomy marker words in the finding text
    if raw_finding is not None:
        text = " ".join([
            (raw_finding.get("title") or raw_finding.get("problem") or ""),
            (raw_finding.get("description") or ""),
            (raw_finding.get("solution") or raw_finding.get("recommendation") or ""),
        ]).lower()
        if any(marker in text for marker in _EXPANDED_TAXONOMY_MARKERS):
            score += 2

    # Borderline is highest priority regardless
    if d.decision == "borderline":
        score += 4

    # Weak evidence accepted findings are risky
    if d.decision == "accept" and d.evidence_quality == EVIDENCE_WEAK:
        score += 3

    return score


def _is_expanded_candidate(d: QualityDecision, raw_finding: Optional[dict] = None) -> tuple[bool, str]:
    """
    Determine whether a decision should be included under the expanded strategy.

    Returns (include, reason_label).
    """
    # Always include borderline
    if d.decision == "borderline":
        return True, "borderline"

    # Exclude none-evidence accepts (already filtered by conservative)
    if d.evidence_quality == EVIDENCE_NONE:
        return False, "evidence_none"

    cat = (d.impact_area or "").lower()

    # Weak/partial evidence with score >= 5 in risk category
    if (
        d.evidence_quality in (EVIDENCE_WEAK, EVIDENCE_PARTIAL)
        and d.usefulness_score >= 5
        and cat in _EXPANDED_RISK_CATEGORIES
    ):
        return True, "weak_partial_risk_category"

    # Accepted findings in high-risk categories
    if d.decision == "accept" and cat in _EXPANDED_RISK_CATEGORIES:
        return True, "accepted_risk_category"

    # Taxonomy marker words in finding text
    if raw_finding is not None:
        text = " ".join([
            (raw_finding.get("title") or raw_finding.get("problem") or ""),
            (raw_finding.get("description") or ""),
            (raw_finding.get("solution") or raw_finding.get("recommendation") or ""),
        ]).lower()
        if any(marker in text for marker in _EXPANDED_TAXONOMY_MARKERS):
            return True, "taxonomy_marker_text"

    # Weak evidence accepted (any category)
    if d.decision == "accept" and d.evidence_quality == EVIDENCE_WEAK:
        return True, "accepted_weak_evidence"

    # Partial evidence accepted with score <= 6 (potentially inflated)
    if d.decision == "accept" and d.evidence_quality == EVIDENCE_PARTIAL and d.usefulness_score <= 6:
        return True, "accepted_partial_low_score"

    return False, "not_eligible"


def select_candidates(
    decisions: list[QualityDecision],
    max_candidates: int = 50,
    strategy: str = CANDIDATE_STRATEGY_CONSERVATIVE,
    raw_findings: Optional[dict[str, dict]] = None,
) -> tuple[list[QualityDecision], list[str]]:
    """
    Select findings eligible for LLM review.

    Strategies
    ----------
    conservative (default):
        Original behaviour — accept/borderline, evidence != none.

    expanded:
        Adds higher-risk accepted findings based on category, evidence,
        taxonomy markers in text, and human-rejection-like patterns.
        Prioritises candidates most likely to be human-rejected.
        Suitable for production benchmarking.

    broad (non-production):
        Sends all accept/borderline regardless of evidence/category.
        Useful for measuring max coverage. Always mark as experimental.

    Returns
    -------
    (candidates, skipped_ids)
    """
    if strategy not in VALID_CANDIDATE_STRATEGIES:
        strategy = CANDIDATE_STRATEGY_CONSERVATIVE

    candidates: list[tuple[QualityDecision, str]] = []   # (decision, reason)
    skipped: list[str] = []

    for d in decisions:
        # Always skip deterministic rejects and merges
        if d.decision == "reject":
            skipped.append(d.finding_id)
            continue
        if d.decision == "merge":
            skipped.append(d.finding_id)
            continue

        raw = (raw_findings or {}).get(d.finding_id)

        if strategy == CANDIDATE_STRATEGY_CONSERVATIVE:
            # Original: only accept/borderline with evidence != none
            if d.evidence_quality == EVIDENCE_NONE:
                skipped.append(d.finding_id)
                continue
            candidates.append((d, "conservative_eligible"))

        elif strategy == CANDIDATE_STRATEGY_EXPANDED:
            # Always skip none-evidence
            if d.evidence_quality == EVIDENCE_NONE:
                skipped.append(d.finding_id)
                continue
            include, reason = _is_expanded_candidate(d, raw)
            if include:
                candidates.append((d, reason))
            else:
                skipped.append(d.finding_id)

        elif strategy == CANDIDATE_STRATEGY_BROAD:
            # All accept/borderline (even none-evidence, for coverage analysis)
            # Still skip deterministic rejects
            candidates.append((d, "broad_all"))

    # Sort by risk: highest risk first (best use of max_candidates budget)
    if strategy == CANDIDATE_STRATEGY_CONSERVATIVE:
        candidates.sort(
            key=lambda t: (0 if t[0].decision == "borderline" else 1, -_false_accept_risk_score(t[0])),
        )
    else:
        candidates.sort(
            key=lambda t: (
                0 if t[0].decision == "borderline" else 1,
                -_false_accept_risk_score_expanded(t[0], (raw_findings or {}).get(t[0].finding_id)),
            ),
        )

    # Apply max_candidates cap
    if len(candidates) > max_candidates:
        excess = candidates[max_candidates:]
        skipped.extend(t[0].finding_id for t in excess)
        candidates = candidates[:max_candidates]

    return [t[0] for t in candidates], skipped


def build_candidate_selection_stats(
    decisions: list[QualityDecision],
    candidates: list[QualityDecision],
    skipped_ids: list[str],
    strategy: str = CANDIDATE_STRATEGY_CONSERVATIVE,
    raw_findings: Optional[dict[str, dict]] = None,
) -> dict:
    """
    Build candidate selection statistics artifact.

    Returns a dict suitable for writing to critic_v2_llm_candidate_stats.json.
    """
    from collections import Counter, defaultdict

    total = len(decisions)
    candidate_count = len(candidates)
    candidate_rate = round(candidate_count / total, 4) if total else 0.0

    by_decision: Counter = Counter()
    by_evidence: Counter = Counter()
    by_category: Counter = Counter()
    by_score_bucket: Counter = Counter()
    protected_count = 0

    for d in candidates:
        by_decision[d.decision] += 1
        by_evidence[d.evidence_quality] += 1
        cat = (d.impact_area or "unknown").lower()
        by_category[cat] += 1
        bucket = f"{(d.usefulness_score // 2) * 2}-{(d.usefulness_score // 2) * 2 + 1}"
        by_score_bucket[bucket] += 1
        if (
            d.usefulness_score >= _EXPANDED_PROTECTED_SCORE
            and d.evidence_quality == _EXPANDED_PROTECTED_EVIDENCE
        ):
            protected_count += 1

    # Candidate reasons (only populated for expanded/broad)
    by_reason: Counter = Counter()
    if strategy != CANDIDATE_STRATEGY_CONSERVATIVE and raw_findings is not None:
        for d in candidates:
            raw = raw_findings.get(d.finding_id)
            _, reason = _is_expanded_candidate(d, raw)
            by_reason[reason] += 1

    return {
        "strategy": strategy,
        "total_findings": total,
        "candidate_count": candidate_count,
        "candidate_rate": candidate_rate,
        "skipped_count": len(skipped_ids),
        "by_decision": dict(by_decision.most_common()),
        "by_evidence_quality": dict(by_evidence.most_common()),
        "by_category": dict(by_category.most_common(20)),
        "by_score_bucket": dict(sorted(by_score_bucket.items())),
        "by_candidate_reason": dict(by_reason.most_common()) if by_reason else {},
        "protected_candidates_count": protected_count,
    }


# ─── Response parser ──────────────────────────────────────────────────────────

def _parse_llm_response(
    text: str,
    candidate_ids: set[str],
) -> tuple[list[LLMCriticDecision], list[str]]:
    """
    Parse LLM JSON output into LLMCriticDecision list.

    Returns:
        (decisions, errors)
    """
    decisions: list[LLMCriticDecision] = []
    errors: list[str] = []

    # Strip markdown code fences if present
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[^\n]*\n", "", text)
        text = re.sub(r"\n```$", "", text.strip())

    try:
        raw_list = json.loads(text)
    except json.JSONDecodeError as e:
        errors.append(f"JSON parse error: {e}")
        return [], errors

    if not isinstance(raw_list, list):
        errors.append(f"Expected JSON array, got {type(raw_list).__name__}")
        return [], errors

    for item in raw_list:
        if not isinstance(item, dict):
            errors.append(f"Expected dict in array, got {type(item).__name__}")
            continue

        fid = str(item.get("finding_id") or item.get("id") or "")
        if not fid:
            errors.append("LLM response item missing finding_id")
            continue
        if fid not in candidate_ids:
            errors.append(f"LLM returned decision for unknown finding: {fid}")
            continue

        raw_decision = str(item.get("llm_decision") or item.get("decision") or "borderline").lower()
        if raw_decision not in VALID_LLM_DECISIONS:
            raw_decision = "borderline"

        score = item.get("usefulness_score")
        try:
            score = max(0, min(10, int(score))) if score is not None else 5
        except (TypeError, ValueError):
            score = 5

        reason = item.get("reject_reason")
        if reason and reason not in VALID_LLM_REJECT_REASONS:
            reason = "unclear"

        # Taxonomy fields (new in v2)
        taxonomy_reason = item.get("human_taxonomy_reason")
        if taxonomy_reason not in VALID_TAXONOMY_REASONS:
            taxonomy_reason = "other"

        confidence_raw = item.get("confidence")
        try:
            confidence = float(confidence_raw) if confidence_raw is not None else 1.0
            confidence = max(0.0, min(1.0, confidence))
        except (TypeError, ValueError):
            confidence = 1.0

        evidence_checked = bool(item.get("evidence_checked", False))

        source_dep = item.get("source_dependency", "enough_source")
        if source_dep not in VALID_SOURCE_DEPENDENCIES:
            source_dep = "enough_source"

        # Extract rewrite fields (can be nested dict or flat)
        rewrite = item.get("rewrite") or {}
        if isinstance(rewrite, dict):
            rewritten_title = rewrite.get("title") or item.get("rewritten_title")
            rewritten_description = rewrite.get("description") or item.get("rewritten_description")
            rewritten_action = rewrite.get("action_required") or item.get("rewritten_action_required")
        else:
            rewritten_title = item.get("rewritten_title")
            rewritten_description = item.get("rewritten_description")
            rewritten_action = item.get("rewritten_action_required")

        decisions.append(LLMCriticDecision(
            finding_id=fid,
            llm_decision=raw_decision,
            usefulness_score=score,
            reject_reason=reason if raw_decision in ("reject", "rewrite") else None,
            explanation=str(item.get("explanation") or ""),
            rewritten_title=rewritten_title,
            rewritten_description=rewritten_description,
            rewritten_action_required=rewritten_action,
            raw_response=item,
            human_taxonomy_reason=taxonomy_reason,
            confidence=confidence,
            evidence_checked=evidence_checked,
            source_dependency=source_dep,
        ))

    return decisions, errors


# ─── Confidence & taxonomy safety gate ───────────────────────────────────────

def _apply_confidence_and_taxonomy_gate(
    llm_decision: LLMCriticDecision,
) -> LLMCriticDecision:
    """
    Enforce confidence threshold and taxonomy safety rules BEFORE merge.

    Policy (applied in order):

    FOR REJECT decisions:
      R1. insufficient_source_context → always needs_human
      R2. borderline_llm / needs_human fitness → downgrade to borderline/needs_human
          (outside_audit_scope, human_marked_minor, design_stage_limitation, other)
      R3. source_dependency checks:
          - cross_section_required → needs_human (can't verify without cross-section)
          - needs_more_context → borderline
      R4. evidence_checked=False → reject downgraded to borderline
          (LLM must have verified source blocks to reject)
      R5. confidence gate — threshold varies by category:
          - OCR/dupe/norm (0.65), standard reject (0.75), not_functionally_significant (0.80)

    FOR ACCEPT decisions with rejection-oriented taxonomy (CONTRADICTORY LABEL FIX):
      C1. If LLM says `accept` but taxonomy is rejection-oriented
          (value_already_correct, already_resolved_by_project_note, etc.) AND:
          - fitness == llm_can_handle
          - source_dependency == enough_source
          - evidence_checked == True
          - confidence >= threshold
          THEN convert to reject (the label implies the finding is invalid)
      C2. source_dependency != enough_source → borderline/needs_human
      C3. evidence_checked=False → borderline
      C4. confidence < threshold → borderline

    FOR ACCEPT/BORDERLINE (other cases):
      Not touched — handled in merge.
    """
    current = llm_decision.llm_decision
    taxonomy = llm_decision.human_taxonomy_reason or "other"
    conf = llm_decision.confidence
    fitness = LLM_FITNESS_MAP.get(taxonomy, "needs_human")
    source_dep = llm_decision.source_dependency or "enough_source"
    ev_checked = llm_decision.evidence_checked

    # ── REJECT decision path ──────────────────────────────────────────────────
    if current == "reject":

        # R1: insufficient_source_context → always needs_human
        if taxonomy in _TAXONOMY_NO_REJECT:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "needs_human",
                   "explanation": f"[taxonomy-safe: {taxonomy}] {llm_decision.explanation}",
                   "reject_reason": None,
                   }
            )

        # R2: borderline_llm / needs_human fitness → downgrade
        if fitness in ("borderline_llm", "needs_human"):
            new_decision = "needs_human" if fitness == "needs_human" else "borderline"
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": new_decision,
                   "explanation": f"[taxonomy-safe: {taxonomy}] {llm_decision.explanation}",
                   "reject_reason": None,
                   }
            )

        # R3: source_dependency checks — reject forbidden without verified source
        if source_dep in _SOURCE_DEP_CROSS_SECTION:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "needs_human",
                   "explanation": f"[cross-section-required] {llm_decision.explanation}",
                   "reject_reason": None,
                   }
            )
        if source_dep in _SOURCE_DEP_NEEDS_MORE:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "borderline",
                   "explanation": f"[needs-more-context] {llm_decision.explanation}",
                   "reject_reason": None,
                   }
            )

        # R4: evidence_checked=False → cannot reject without source verification
        if not ev_checked:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "borderline",
                   "explanation": f"[evidence-not-checked] {llm_decision.explanation}",
                   "reject_reason": None,
                   }
            )

        # R5: confidence gate — threshold varies by category
        if taxonomy in _TAXONOMY_HIGH_CONFIDENCE_REJECT:
            threshold = 0.65
        elif taxonomy == "not_functionally_significant":
            threshold = 0.80
        else:
            threshold = LLM_REJECT_CONFIDENCE_THRESHOLD

        if conf < threshold:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "borderline",
                   "explanation": (
                       f"[low-confidence: {conf:.2f}<{threshold}] {llm_decision.explanation}"
                   ),
                   "reject_reason": None,
                   }
            )

        return llm_decision

    # ── ACCEPT with rejection-oriented taxonomy: CONTRADICTORY LABEL FIX ─────
    # When LLM says 'accept' but taxonomy implies the finding is invalid,
    # the label and decision contradict each other.
    #
    # Policy split:
    # TECHNICAL categories (OCR/context-artifact): safe for accept→reject promotion
    # SEMANTIC categories (value correct, norm not applicable, etc.): accept→borderline only
    #   (LLM may misclassify valid findings as semantic reasons; safer to hold as borderline)
    if (
        current == "accept"
        and taxonomy in _REJECTION_ORIENTED_TAXONOMIES
        and fitness == "llm_can_handle"
    ):
        if taxonomy in _TAXONOMY_HIGH_CONFIDENCE_REJECT:
            threshold = 0.65
        elif taxonomy == "not_functionally_significant":
            threshold = 0.80
        else:
            threshold = LLM_REJECT_CONFIDENCE_THRESHOLD

        is_technical = taxonomy in _TECHNICAL_REJECTION_TAXONOMIES

        # C1 (technical only): safe to promote accept → reject
        if is_technical and source_dep == "enough_source" and ev_checked and conf >= threshold:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "reject",
                   "reject_reason": (
                       taxonomy if taxonomy in VALID_LLM_REJECT_REASONS else "unclear"
                   ),
                   "explanation": (
                       f"[contradictory-label-fix: accept→reject, "
                       f"taxonomy={taxonomy}, conf={conf:.2f}] {llm_decision.explanation}"
                   ),
                   }
            )

        # C1-semantic: semantic rejection taxonomy with contradictory accept → borderline
        # (not reject: LLM may have wrong taxonomy classification for valid findings)
        if taxonomy in _SEMANTIC_REJECTION_TAXONOMIES and source_dep == "enough_source" and ev_checked and conf >= threshold:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "borderline",
                   "explanation": (
                       f"[contradictory-label: accept+{taxonomy} → borderline for review, "
                       f"conf={conf:.2f}] {llm_decision.explanation}"
                   ),
                   "reject_reason": None,
                   }
            )

        # C2: cross-section source → needs_human
        if source_dep in _SOURCE_DEP_CROSS_SECTION:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "needs_human",
                   "explanation": (
                       f"[contradictory-label: {taxonomy} but cross-section-required] "
                       f"{llm_decision.explanation}"
                   ),
                   "reject_reason": None,
                   }
            )

        # C2b: needs_more_context → borderline
        if source_dep in _SOURCE_DEP_NEEDS_MORE:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "borderline",
                   "explanation": (
                       f"[contradictory-label: {taxonomy} but needs-more-context] "
                       f"{llm_decision.explanation}"
                   ),
                   "reject_reason": None,
                   }
            )

        # C3/C4: no evidence check or low confidence → borderline
        if not ev_checked or conf < threshold:
            return LLMCriticDecision(
                **{**llm_decision.__dict__,  # type: ignore[arg-type]
                   "llm_decision": "borderline",
                   "explanation": (
                       f"[contradictory-label: {taxonomy}, "
                       f"{'no-evidence-check' if not ev_checked else f'conf={conf:.2f}<{threshold}'}"
                       f"] {llm_decision.explanation}"
                   ),
                   "reject_reason": None,
                   }
            )

    # ── needs_human stays needs_human; borderline/accept(other) unchanged ────
    return llm_decision


# ─── Evidence quality cap enforcement ────────────────────────────────────────

def _apply_evidence_cap(
    llm_decision: LLMCriticDecision,
    det_decision: QualityDecision,
) -> LLMCriticDecision:
    """
    Enforce evidence quality hard caps on LLM output.

    Caps (matching scorer.py):
      none    → should never reach LLM (filtered in select_candidates)
      weak    → max borderline (LLM cannot promote to accept)
      partial → max borderline, exception: КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ → accept allowed
      valid   → no cap, accept is allowed

    Also caps usefulness_score to match the decision.
    """
    eq = det_decision.evidence_quality
    sev = (det_decision.severity or "").upper()
    current = llm_decision.llm_decision

    # weak: max borderline regardless of severity
    if eq == EVIDENCE_WEAK and current == "accept":
        return LLMCriticDecision(
            finding_id=llm_decision.finding_id,
            llm_decision="borderline",
            usefulness_score=min(llm_decision.usefulness_score, 5),
            reject_reason=None,
            explanation=f"[capped: weak evidence] {llm_decision.explanation}",
            rewritten_title=llm_decision.rewritten_title,
            rewritten_description=llm_decision.rewritten_description,
            rewritten_action_required=llm_decision.rewritten_action_required,
            provider=llm_decision.provider,
            raw_response=llm_decision.raw_response,
        )

    # partial: max borderline except КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ
    if eq == EVIDENCE_PARTIAL and current == "accept" and sev not in _HIGH_SEVERITY:
        return LLMCriticDecision(
            finding_id=llm_decision.finding_id,
            llm_decision="borderline",
            usefulness_score=min(llm_decision.usefulness_score, 6),
            reject_reason=None,
            explanation=f"[capped: partial evidence, non-critical] {llm_decision.explanation}",
            rewritten_title=llm_decision.rewritten_title,
            rewritten_description=llm_decision.rewritten_description,
            rewritten_action_required=llm_decision.rewritten_action_required,
            provider=llm_decision.provider,
            raw_response=llm_decision.raw_response,
        )

    return llm_decision


# ─── HIGH_SCORE_VALID_ACCEPT_GUARD ───────────────────────────────────────────

def _apply_high_score_valid_accept_guard(
    llm: LLMCriticDecision,
    det: QualityDecision,
    effective_llm_decision: str,
) -> tuple[str, bool]:
    """
    Safety guardrail: prevent a single LLM pass from hard-rejecting a finding
    that the deterministic engine accepted with high score and valid evidence.

    Conditions for guard activation:
      - deterministic decision = accept
      - deterministic usefulness_score >= HIGH_SCORE_VALID_ACCEPT_GUARD_THRESHOLD (8)
      - evidence_quality = valid
      - effective LLM decision after confidence/taxonomy gate = reject

    When activated:
      - Downgrade reject to borderline (source_dep=enough_source)
        or to needs_human (source_dep=needs_more_context or cross_section_required)
      - Return (downgraded_decision, was_blocked=True)

    When NOT activated:
      - Return (effective_llm_decision, was_blocked=False)

    This guard applies to ALL taxonomy reasons including visual_or_ocr_misread,
    false_positive_due_to_missing_context, etc. Even if the LLM is correct, a
    second-opinion pass or human review is safer than an automatic hard-reject
    for high-score valid evidence findings.
    """
    if (
        effective_llm_decision == "reject"
        and det.decision == "accept"
        and det.usefulness_score >= HIGH_SCORE_VALID_ACCEPT_GUARD_THRESHOLD
        and det.evidence_quality == EVIDENCE_VALID
    ):
        source_dep = llm.source_dependency or "enough_source"
        if source_dep in _SOURCE_DEP_CROSS_SECTION or source_dep in _SOURCE_DEP_NEEDS_MORE:
            downgraded = "needs_human"
        else:
            downgraded = "borderline"
        return downgraded, True

    return effective_llm_decision, False


# ─── Guard case collector (module-level, thread-unsafe but script-safe) ──────
# Stores the blocked_guard_cases from the most recent merge_llm_decisions call.
# Scripts can call get_last_blocked_guard_cases() after merge to retrieve stats
# without breaking the existing 4-tuple API of merge_llm_decisions.
_last_blocked_guard_cases: list[dict] = []


def get_last_blocked_guard_cases() -> list[dict]:
    """Return blocked high-score valid accept guard cases from the last merge call."""
    return list(_last_blocked_guard_cases)


# ─── Merge LLM + deterministic decisions ─────────────────────────────────────

def merge_llm_decisions(
    det_decisions: list[QualityDecision],
    llm_results: list[LLMCriticDecision],
    raw_by_id: dict[str, dict],
) -> tuple[list[QualityDecision], list[dict], list[dict], list[dict]]:
    """
    Merge LLM decisions into deterministic decisions.

    Invariants:
    - deterministic reject stays reject (LLM cannot restore)
    - deterministic merge stays merge (LLM cannot restore)
    - LLM reject can downgrade deterministic accept/borderline to reject
      (EXCEPT when HIGH_SCORE_VALID_ACCEPT_GUARD blocks it)
    - LLM accept can upgrade deterministic borderline → accept only if evidence valid
    - LLM rewrite treated as borderline (content in artifacts only)
    - evidence caps re-enforced after merge
    - HIGH_SCORE_VALID_ACCEPT_GUARD: det=accept + score>=8 + evidence=valid
      → LLM reject downgraded to borderline/needs_human, never hard-reject

    Returns:
        (final_decisions, accepted, rejected, borderline_list)
    """
    llm_by_id = {d.finding_id: d for d in llm_results}
    final: list[QualityDecision] = []
    blocked_guard_cases: list[dict] = []

    for det in det_decisions:
        llm = llm_by_id.get(det.finding_id)

        # ── Immutable decisions ──
        if det.decision == "reject":
            final.append(det)
            continue
        if det.decision == "merge":
            final.append(det)
            continue

        # ── No LLM decision for this finding ──
        if llm is None:
            final.append(det)
            continue

        # ── Apply confidence + taxonomy safety gate ──
        llm = _apply_confidence_and_taxonomy_gate(llm)

        # ── Apply evidence cap to LLM decision ──
        llm = _apply_evidence_cap(llm, det)

        # ── Compute final decision ──
        # rewrite → treat as borderline (content stored in artifacts)
        # needs_human → force borderline (keep for human review, NOT accept even if det=accept)
        # If explanation contains "[low-confidence" or "[taxonomy-safe" the original intent
        # was reject but safety gate softened it — treat as borderline (not accept).
        effective_llm = llm.llm_decision
        needs_human_flag = (effective_llm == "needs_human")
        was_downgraded_reject = (
            effective_llm == "borderline"
            and (
                "[low-confidence" in llm.explanation
                or "[taxonomy-safe" in llm.explanation
                or "[needs-more-context]" in llm.explanation
                or "[cross-section-required]" in llm.explanation
                or "[evidence-not-checked]" in llm.explanation
            )
        )
        if effective_llm == "rewrite":
            effective_llm = "borderline"
        if effective_llm == "needs_human":
            effective_llm = "borderline"

        # ── HIGH_SCORE_VALID_ACCEPT_GUARD ──
        # After all taxonomy/confidence gates, if LLM still says reject but deterministic
        # engine accepted with score>=8 and valid evidence, block the hard-reject.
        was_blocked_by_guard = False
        effective_llm, was_blocked_by_guard = _apply_high_score_valid_accept_guard(
            llm, det, effective_llm
        )
        if was_blocked_by_guard:
            blocked_guard_cases.append({
                "finding_id": det.finding_id,
                "det_score": det.usefulness_score,
                "det_decision": det.decision,
                "evidence_quality": det.evidence_quality,
                "original_llm_decision": "reject",
                "original_taxonomy_reason": llm.human_taxonomy_reason,
                "original_reject_reason": llm.reject_reason,
                "original_explanation": llm.explanation,
                "downgraded_to": effective_llm,
                "source_dependency": llm.source_dependency,
            })
            # Patch explanation to surface the guard activation
            llm = LLMCriticDecision(
                **{**llm.__dict__,  # type: ignore[arg-type]
                   "llm_decision": effective_llm,
                   "explanation": (
                       f"[high-score-valid-accept-guard: reject→{effective_llm}, "
                       f"det_score={det.usefulness_score}, ev=valid, "
                       f"orig_taxonomy={llm.human_taxonomy_reason}] "
                       f"{llm.explanation}"
                   ),
                   "reject_reason": None,
                   }
            )
            # needs_human → borderline in effective_llm for the branch below
            if effective_llm == "needs_human":
                needs_human_flag = True
                effective_llm = "borderline"
            # Guard blocked a reject → treat as downgraded reject so the borderline
            # branch keeps it at borderline and does NOT re-promote to accept.
            was_downgraded_reject = True

        if effective_llm == "reject":
            # LLM downgrades accept/borderline to reject
            final_decision = "reject"
            final_reason = llm.reject_reason or "unclear"
            final_expl = llm.explanation
            final_score = min(det.usefulness_score, llm.usefulness_score)
        elif effective_llm == "accept":
            # LLM accepts — must still respect evidence cap
            if det.evidence_quality == EVIDENCE_VALID:
                final_decision = "accept"
            elif det.evidence_quality == EVIDENCE_PARTIAL and (
                (det.severity or "").upper() in _HIGH_SEVERITY
            ):
                final_decision = "accept"
            else:
                final_decision = "borderline"
            final_reason = det.reject_reason
            final_expl = llm.explanation
            final_score = max(det.usefulness_score, llm.usefulness_score)
        elif effective_llm == "borderline":
            # needs_human always stays borderline — it signals need for human review
            # Downgraded reject (low-confidence) also stays borderline — LLM had doubts
            # Pure LLM borderline: keep accept if det=accept+valid, otherwise borderline
            if needs_human_flag or was_downgraded_reject:
                final_decision = "borderline"
            elif det.decision == "accept" and det.evidence_quality == EVIDENCE_VALID:
                final_decision = "accept"
            else:
                final_decision = "borderline"
            final_reason = det.reject_reason
            final_expl = llm.explanation
            final_score = det.usefulness_score
        else:
            # Unknown → keep deterministic
            final.append(det)
            continue

        final.append(QualityDecision(
            finding_id=det.finding_id,
            decision=final_decision,
            usefulness_score=max(0, min(10, final_score)),
            reject_reason=final_reason if final_decision == "reject" else det.reject_reason,
            reject_explanation=final_expl,
            merged_into=det.merged_into,
            impact_area=det.impact_area,
            severity=det.severity,
            has_evidence=det.has_evidence,
            has_action=det.has_action,
            has_impact=det.has_impact,
            evidence_quality=det.evidence_quality,
        ))

    # ── Re-bucket ──
    accepted = [raw_by_id[d.finding_id] for d in final if d.decision == "accept" and d.finding_id in raw_by_id]
    rejected = [raw_by_id[d.finding_id] for d in final if d.decision == "reject" and d.finding_id in raw_by_id]
    borderline = [
        raw_by_id[d.finding_id] for d in final
        if d.decision in ("borderline", "low_priority") and d.finding_id in raw_by_id
    ]
    # blocked_guard_cases is exposed via the _last_blocked_guard_cases module-level cache
    # for callers that want the guard stats without breaking the 4-tuple API.
    _last_blocked_guard_cases.clear()
    _last_blocked_guard_cases.extend(blocked_guard_cases)
    return final, accepted, rejected, borderline


# ─── Context-enriched payload builder ───────────────────────────────────────

def _build_enriched_payload_text(
    candidates: "list[QualityDecision]",
    findings_by_id: dict,
    prompt: str,
    context_packages: "Optional[dict[str, Any]]" = None,
) -> str:
    """
    Build the full LLM input text with optional context enrichment.

    If context_packages is provided (dict: finding_id → FindingContextPackage),
    each finding entry in the payload includes a context_text section showing:
    - common notes / general instructions
    - neighboring blocks
    - cross-references
    - table rows
    - spec context
    This allows the LLM to check whether the issue is already covered elsewhere
    in the document.
    """
    payload_findings = []
    for d in candidates:
        raw = findings_by_id.get(d.finding_id, {})
        entry: dict = {
            "finding_id": d.finding_id,
            "title": raw.get("title") or raw.get("problem") or "",
            "description": raw.get("description") or "",
            "solution": raw.get("solution") or raw.get("action_required") or "",
            "severity": raw.get("severity") or d.severity or "",
            "category": raw.get("category") or "",
            "evidence_quality": d.evidence_quality,
            "critic_score": d.usefulness_score,
            "norm": raw.get("norm") or "",
            "norm_quote": raw.get("norm_quote") or "",
            "evidence_refs": raw.get("evidence_refs") or raw.get("related_block_ids") or [],
        }
        if context_packages:
            pkg = context_packages.get(d.finding_id)
            if pkg is not None:
                ctx_text = pkg.to_llm_text(max_chars=2000)
                if ctx_text:
                    entry["context_from_document"] = ctx_text
        payload_findings.append(entry)

    return (
        f"{prompt}\n\n"
        f"## Входные данные\n\n"
        f"```json\n{json.dumps(payload_findings, ensure_ascii=False, indent=2)}\n```\n"
    )


# ─── Providers ───────────────────────────────────────────────────────────────

class MockProvider:
    """
    Mock LLM provider for offline testing.

    Returns deterministic decisions based on the input finding content:
    - If the finding dict has an _expected_decision key, use it as llm_decision
    - If the finding dict has _taxonomy_reason, use it as human_taxonomy_reason
    - If the finding dict has _confidence, use it as confidence
    - Otherwise echo the deterministic decision back unchanged
    - Simulates realistic JSON structure including taxonomy fields
    """

    def __call__(
        self,
        candidates: list[QualityDecision],
        findings_by_id: dict[str, dict],
        prompt: str,
        context_packages: Optional[dict] = None,
    ) -> tuple[str, list[str]]:
        """
        Returns:
            (json_response_text, errors)
        """
        items = []
        for d in candidates:
            raw = findings_by_id.get(d.finding_id, {})

            # Allow test injection
            expected = raw.get("_expected_decision")
            if expected in VALID_LLM_DECISIONS:
                llm_dec = expected
            elif d.decision == "accept":
                llm_dec = "accept"
            elif d.decision == "borderline":
                llm_dec = "borderline"
            else:
                llm_dec = "borderline"

            taxonomy_reason = raw.get("_taxonomy_reason", "other")
            if taxonomy_reason not in VALID_TAXONOMY_REASONS:
                taxonomy_reason = "other"

            confidence = float(raw.get("_confidence", 1.0))

            source_dep = raw.get("_source_dependency", "enough_source")
            if source_dep not in VALID_SOURCE_DEPENDENCIES:
                source_dep = "enough_source"

            score = d.usefulness_score
            if llm_dec == "accept":
                score = max(score, 7)
            elif llm_dec in ("reject",):
                score = min(score, 4)

            reject_reason = None
            if llm_dec in ("reject", "rewrite"):
                # Use taxonomy reason directly if it's also a valid reject reason
                if taxonomy_reason in VALID_LLM_REJECT_REASONS:
                    reject_reason = taxonomy_reason
                else:
                    reject_reason = "low_business_value"

            items.append({
                "finding_id": d.finding_id,
                "llm_decision": llm_dec,
                "usefulness_score": score,
                "reject_reason": reject_reason,
                "explanation": (
                    f"[mock] {d.decision} finding with {d.evidence_quality} evidence"
                    f" | taxonomy={taxonomy_reason} conf={confidence:.2f}"
                ),
                "human_taxonomy_reason": taxonomy_reason,
                "confidence": confidence,
                "evidence_checked": True,
                "source_dependency": source_dep,
                "rewrite": {
                    "title": None,
                    "description": None,
                    "action_required": None,
                },
            })
        return json.dumps(items, ensure_ascii=False), []


class NoopProvider:
    """Pass-through provider: returns deterministic decisions as LLM decisions unchanged."""

    def __call__(
        self,
        candidates: list[QualityDecision],
        findings_by_id: dict[str, dict],
        prompt: str,
        context_packages: Optional[dict] = None,
    ) -> tuple[str, list[str]]:
        items = []
        for d in candidates:
            llm_dec = d.decision if d.decision in VALID_LLM_DECISIONS else "borderline"
            items.append({
                "finding_id": d.finding_id,
                "llm_decision": llm_dec,
                "usefulness_score": d.usefulness_score,
                "reject_reason": None,
                "explanation": "[noop] passthrough",
                "human_taxonomy_reason": "other",
                "confidence": 1.0,
                "evidence_checked": False,
                "source_dependency": "enough_source",
                "rewrite": {"title": None, "description": None, "action_required": None},
            })
        return json.dumps(items, ensure_ascii=False), []


class ClaudeRunnerProvider:
    """
    Real LLM provider via claude -p subprocess.

    Calls the Claude CLI synchronously (blocking), suitable for offline scripts.
    Production pipeline uses the async version; this is script-layer only.

    Invariants:
    - Never touches production artifacts
    - Never writes any files
    - Always returns (json_text, errors)
    """

    def __init__(
        self,
        model: str = "claude-sonnet-4-6",
        timeout: int = 180,
        temperature: float = 0.0,
    ):
        self.model = model
        self.timeout = timeout
        self.temperature = temperature
        self._cli: Optional[str] = None

    def _resolve_cli(self) -> Optional[str]:
        if self._cli:
            return self._cli
        import shutil, sys
        # Try PATH
        found = shutil.which("claude")
        if found and Path(found).exists():
            self._cli = found
            return found
        # VSCode extension path scan
        vscode_dirs = [
            Path.home() / ".vscode-server" / "extensions",
            Path.home() / ".vscode" / "extensions",
        ]
        for vdir in vscode_dirs:
            if not vdir.exists():
                continue
            for ext_dir in sorted(vdir.glob("anthropic.claude-code-*"), reverse=True):
                candidate = ext_dir / "resources" / "native-binary" / "claude"
                if candidate.exists():
                    self._cli = str(candidate)
                    return self._cli
        return None

    def __call__(
        self,
        candidates: list[QualityDecision],
        findings_by_id: dict[str, dict],
        prompt: str,
        context_packages: Optional[dict] = None,
    ) -> tuple[str, list[str]]:
        import subprocess
        import os

        cli = self._resolve_cli()
        if not cli:
            return "[]", [
                "claude CLI not found. Install Claude Code or set PATH. "
                "Use --llm-provider mock for offline testing."
            ]

        task_text = _build_enriched_payload_text(
            candidates, findings_by_id, prompt, context_packages,
        )

        env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE_")}
        cmd = [
            cli, "-p",
            "--model", self.model,
            "--allowedTools", "none",
            "--output-format", "text",
        ]
        if self.temperature == 0.0:
            pass  # default in claude

        try:
            proc = subprocess.run(
                cmd,
                input=task_text,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                env=env,
                cwd="/tmp",
            )
        except subprocess.TimeoutExpired:
            return "[]", [f"claude CLI timed out after {self.timeout}s"]
        except OSError as e:
            return "[]", [f"claude CLI exec error: {e}"]

        if proc.returncode not in (0, 1):  # CLI may return 1 on partial success
            stderr = (proc.stderr or "").strip()[:300]
            return "[]", [f"claude CLI exited with code {proc.returncode}: {stderr}"]

        # stdout contains the response text; look for JSON array
        output = proc.stdout.strip()
        if not output:
            stderr = (proc.stderr or "").strip()[:200]
            return "[]", [f"Empty claude output. stderr: {stderr}"]

        # Try to extract JSON array from output (may have preamble text)
        start = output.find("[")
        end = output.rfind("]")
        if start != -1 and end > start:
            output = output[start:end + 1]

        return output, []


class OpenRouterProvider:
    """
    Real LLM provider via OpenRouter REST API (synchronous requests).

    Uses OPENROUTER_API_KEY from environment / config.
    Suitable for offline benchmark scripts (no event loop required).
    """

    def __init__(
        self,
        model: str = "openai/gpt-4o-mini",
        timeout: int = 120,
        temperature: float = 0.0,
    ):
        self.model = model
        self.timeout = timeout
        self.temperature = temperature

    def __call__(
        self,
        candidates: list[QualityDecision],
        findings_by_id: dict[str, dict],
        prompt: str,
        context_packages: Optional[dict] = None,
    ) -> tuple[str, list[str]]:
        import os
        try:
            import requests as _requests
        except ImportError:
            return "[]", ["'requests' package not available. pip install requests"]

        # Resolve API key
        api_key = os.environ.get("OPENROUTER_API_KEY", "")
        if not api_key:
            try:
                from backend.app.core.config import OPENROUTER_API_KEY as _key
                api_key = _key
            except ImportError:
                pass
        if not api_key:
            return "[]", [
                "OPENROUTER_API_KEY not set. Export it or use --llm-provider mock."
            ]

        user_content = _build_enriched_payload_text(
            candidates, findings_by_id, prompt, context_packages,
        )

        body = {
            "model": self.model,
            "temperature": self.temperature,
            "messages": [{"role": "user", "content": user_content}],
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:8081",
            "X-Title": "critic_v2_llm_gate",
        }

        # ─── Paid API guard ─────────────────────────────────────────
        # critic_v2 OpenRouterProvider — это offline benchmark / experimental
        # path. Прямой requests.post в OpenRouter в обход llm_runner.
        # Не оставляем известный обходной путь. context_packages при
        # ручном запуске может передать project_id/manual_run_id; без них
        # guard вернёт missing_manual_run_id (fail-closed).
        try:
            from backend.app.services.llm.paid_api_guard import (
                PaidApiBlockedError as _PaidApiBlockedError,
                PaidApiContext as _PaidApiContext,
                assert_paid_api_allowed as _assert_paid_api_allowed,
            )
            _ctx_meta = (context_packages or {}).get("_paid_api_ctx", {}) if isinstance(context_packages, dict) else {}
            _assert_paid_api_allowed(_PaidApiContext(
                source="critic_v2.openrouter_provider",
                model=self.model,
                project_id=_ctx_meta.get("project_id", "") or "",
                version_id=_ctx_meta.get("version_id", "") or "",
                stage="findings_review",
                manual_run_id=_ctx_meta.get("manual_run_id", "") or "",
                job_id=_ctx_meta.get("job_id", "") or "",
            ))
        except _PaidApiBlockedError as _e:
            return "[]", [f"paid_api_blocked: {_e.reason}"]

        try:
            resp = _requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=body,
                timeout=self.timeout,
            )
        except _requests.Timeout:
            return "[]", [f"OpenRouter request timed out after {self.timeout}s"]
        except _requests.ConnectionError as e:
            return "[]", [f"OpenRouter connection error: {e}"]

        if resp.status_code != 200:
            try:
                err_body = resp.json()
                err_msg = err_body.get("error", {}).get("message", resp.text[:200])
            except Exception:
                err_msg = resp.text[:200]
            return "[]", [f"OpenRouter HTTP {resp.status_code}: {err_msg}"]

        try:
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, ValueError) as e:
            return "[]", [f"OpenRouter response parse error: {e}. Body: {resp.text[:300]}"]

        # Extract JSON array from content
        content = content.strip()
        start = content.find("[")
        end = content.rfind("]")
        if start != -1 and end > start:
            content = content[start:end + 1]

        return content, []


def _make_provider(
    provider_name: str,
    model: Optional[str] = None,
    timeout: int = 180,
    temperature: float = 0.0,
) -> Callable:
    """Resolve provider name to callable."""
    if provider_name == "mock":
        return MockProvider()
    if provider_name == "noop":
        return NoopProvider()
    if provider_name == "claude_runner":
        resolved_model = model or "claude-sonnet-4-6"
        return ClaudeRunnerProvider(model=resolved_model, timeout=timeout, temperature=temperature)
    if provider_name == "openrouter":
        resolved_model = model or "openai/gpt-4o-mini"
        return OpenRouterProvider(model=resolved_model, timeout=timeout, temperature=temperature)
    raise ValueError(f"Unknown LLM provider: {provider_name!r}")


# ─── Main gate function ───────────────────────────────────────────────────────

def run_llm_gate(
    det_decisions: list[QualityDecision],
    findings_by_id: dict[str, dict],
    provider: str = "mock",
    prompt_path: Optional[Path] = None,
    max_candidates: int = 50,
    model: Optional[str] = None,
    timeout: int = 180,
    temperature: float = 0.0,
    context_packages: Optional[dict] = None,
    candidate_strategy: str = CANDIDATE_STRATEGY_CONSERVATIVE,
) -> LLMGateResult:
    """
    Run LLM gate on deterministic decisions.

    Args:
        det_decisions: decisions from run_critic_v2_offline
        findings_by_id: map finding_id → raw finding dict
        provider: "mock" | "noop" | "claude_runner" | "openrouter"
        prompt_path: optional custom prompt path
        max_candidates: max findings to send to LLM
        model: optional model override (for claude_runner / openrouter)
        timeout: request timeout in seconds (for real providers)
        temperature: sampling temperature (0.0 = deterministic)
        context_packages: optional dict mapping finding_id → FindingContextPackage
                         (from ContextCollector.collect_all). When provided, each
                         finding's LLM payload is enriched with neighbor blocks,
                         general notes, cross-references, and table context.
        candidate_strategy: "conservative" | "expanded" | "broad"
                           Controls which findings are sent to LLM.

    Returns:
        LLMGateResult with decisions and metadata
    """
    ctx_enriched = context_packages is not None and len(context_packages) > 0
    prompt_text, prompt_label = load_prompt(prompt_path)
    print(f"[llm_gate] Prompt: {prompt_label}")
    print(
        f"[llm_gate] Provider: {provider}" + (f" / model: {model}" if model else "") +
        (f" [strategy={candidate_strategy}]") +
        (" [+context]" if ctx_enriched else "")
    )

    candidates, skipped = select_candidates(
        det_decisions, max_candidates,
        strategy=candidate_strategy,
        raw_findings=findings_by_id,
    )
    print(f"[llm_gate] Candidates: {len(candidates)} ({candidate_strategy}), Skipped: {len(skipped)}")

    if not candidates:
        return LLMGateResult(
            candidates_sent=0,
            decisions=[],
            prompt_path_used=prompt_label,
            provider_used=provider,
            skipped_ids=skipped,
            errors=[],
        )

    # Call provider
    try:
        provider_fn = _make_provider(provider, model=model, timeout=timeout, temperature=temperature)
    except (NotImplementedError, ValueError) as e:
        return LLMGateResult(
            candidates_sent=len(candidates),
            decisions=[],
            prompt_path_used=prompt_label,
            provider_used=provider,
            skipped_ids=skipped,
            errors=[str(e)],
        )

    candidate_ids = {d.finding_id for d in candidates}
    raw_response, call_errors = provider_fn(
        candidates, findings_by_id, prompt_text, context_packages,
    )

    # Parse response
    llm_decisions, parse_errors = _parse_llm_response(raw_response, candidate_ids)

    # Apply confidence/taxonomy safety gate, then evidence caps
    det_by_id = {d.finding_id: d for d in det_decisions}
    gated_decisions = []
    for ld in llm_decisions:
        if ld.finding_id not in det_by_id:
            continue
        ld.provider = provider
        ld = _apply_confidence_and_taxonomy_gate(ld)
        ld = _apply_evidence_cap(ld, det_by_id[ld.finding_id])
        gated_decisions.append(ld)
    capped_decisions = gated_decisions

    all_errors = call_errors + parse_errors
    if all_errors:
        for e in all_errors:
            print(f"[llm_gate] ERROR: {e}")

    return LLMGateResult(
        candidates_sent=len(candidates),
        decisions=capped_decisions,
        prompt_path_used=prompt_label,
        provider_used=provider,
        skipped_ids=skipped,
        errors=all_errors,
    )
