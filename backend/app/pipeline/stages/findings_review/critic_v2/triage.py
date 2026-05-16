"""
critic_v2/triage.py
--------------------
Offline triage policy for critic_v2 with three configurable profiles.

NOT connected to production pipeline.
Does NOT modify production artifacts.
Does NOT call any LLM.

Triage queues (human_queue field):
  strong_keep      — cannot be hidden automatically; human MUST see it
  main_review      — accepted, good evidence, show by default
  borderline       — uncertain/disputed, show but mark as borderline
  needs_context    — requires cross-section or additional context
  suggested_reject — critic recommends reject, human makes the call (collapsed)
  hidden_by_critic — safest rejects only (collapsed, can_restore=True)

Profiles:
  conservative — current safe behaviour, minimal suggested_reject expansion
  assisted     — expands suggested_reject via LLM taxonomy; primary workload driver
  aggressive   — non-production experimental; larger suggested_reject + hidden pool

Definitions:
  primary_visible  = strong_keep | main_review | borderline | needs_context
  collapsed        = suggested_reject | hidden_by_critic

KPIs:
  primary_queue_reduction_percent  = collapsed / total_findings * 100
  accepted_primary_visible_recall  = human_accepted in primary_visible / total_human_accepted
  accepted_not_hidden_recall       = human_accepted NOT in hidden_by_critic / total_human_accepted
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from .models import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    QualityDecision,
)

# ─── Profile names ────────────────────────────────────────────────────────────

PROFILE_CONSERVATIVE = "conservative"
PROFILE_ASSISTED = "assisted"
PROFILE_AGGRESSIVE = "aggressive"
# Experimental profile derived from round 1 manual feedback (9 projects, 499 items).
# Inherits conservative behaviour, then applies post-processing rules A1+C+D
# which downgrade specific patterns into suggested_reject only — never into
# hidden_by_critic. See backend/scripts/simulate_critic_v2_tuning_rules_round1.py
# for the simulation evidence and risk metrics.
PROFILE_ASSISTED_ROUND1 = "assisted_round1"

VALID_PROFILES = {
    PROFILE_CONSERVATIVE, PROFILE_ASSISTED,
    PROFILE_AGGRESSIVE, PROFILE_ASSISTED_ROUND1,
}

# ─── Human queue labels ───────────────────────────────────────────────────────

QUEUE_STRONG_KEEP = "strong_keep"
QUEUE_MAIN_REVIEW = "main_review"
QUEUE_BORDERLINE = "borderline"
QUEUE_NEEDS_CONTEXT = "needs_context"
QUEUE_SUGGESTED_REJECT = "suggested_reject"
QUEUE_HIDDEN = "hidden_by_critic"

# ─── Critic recommendation labels ────────────────────────────────────────────

REC_KEEP = "keep"
REC_REVIEW = "review"
REC_REJECT = "reject"
REC_NEEDS_CONTEXT = "needs_context"

# ─── Risk levels ─────────────────────────────────────────────────────────────

RISK_LOW = "low"
RISK_MEDIUM = "medium"
RISK_HIGH = "high"

# ─── Deterministic reject reasons safe for hidden_by_critic ──────────────────
_SAFE_DET_REJECT_REASONS = {
    "no_evidence",
    "generic_wording",
    "ocr_artifact",
    "duplicate",
    "unsupported_by_source",
    "no_action",
    "no_impact",
    "low_business_value",
    "cosmetic_no_practical_impact",
}

# ─── LLM taxonomy reasons safe for hidden_by_critic (conservative) ───────────
_SAFE_LLM_TAXONOMY_FOR_HIDDEN = {
    "visual_or_ocr_misread",
    "duplicate_or_already_covered",
    "already_resolved_by_project_note",
    "false_positive_due_to_missing_context",
    "wrong_norm_context",
    "requirement_not_mandatory",
    "value_already_correct",
}

# ─── LLM taxonomy reasons eligible for suggested_reject (assisted/aggressive) ─
# These are safe enough for "suggested reject" (collapsed + can_restore),
# but NOT safe for hard hidden_by_critic without additional preconditions.
_ASSISTED_SUGGESTED_REJECT_TAXONOMIES = {
    "visual_or_ocr_misread",
    "duplicate_or_already_covered",
    "already_resolved_by_project_note",
    "false_positive_due_to_missing_context",
    "wrong_norm_context",
    "requirement_not_mandatory",
    "value_already_correct",
    "wrong_element_or_location",
}

# Additional taxonomies unlocked in aggressive mode for suggested_reject
_AGGRESSIVE_EXTRA_SUGGESTED_REJECT_TAXONOMIES = {
    "not_functionally_significant",
    "acceptable_design_solution",
    "outside_audit_scope",
    "human_marked_minor",
    "design_stage_limitation",
}

# LLM confidence thresholds by profile
_CONF_THRESHOLD_HIDDEN = 0.80          # conservative: high bar for hiding
_CONF_THRESHOLD_ASSISTED_SR = 0.70     # assisted: standard bar for suggested_reject
_CONF_THRESHOLD_AGGRESSIVE_SR = 0.60   # aggressive: lower bar

# ─── Taxonomy reasons mapping to borderline / needs_context ──────────────────
_BORDERLINE_TAXONOMY = {
    "acceptable_design_solution",
    "not_functionally_significant",
    "other_unclassified",
    "other",
    "outside_audit_scope",
    "human_marked_minor",
    "design_stage_limitation",
}

_NEEDS_CONTEXT_TAXONOMY = {
    "insufficient_source_context",
}

# High-severity categories that raise the floor for strong_keep
_HIGH_SEVERITY = {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"}


# ─── Data structures ──────────────────────────────────────────────────────────


@dataclass
class TriageDecision:
    """Triage outcome for one finding."""
    finding_id: str
    human_queue: str        # strong_keep | main_review | borderline | needs_context | suggested_reject | hidden_by_critic
    critic_recommendation: str  # keep | review | reject | needs_context
    visible_by_default: bool
    collapsed_by_default: bool
    can_restore: bool
    confidence: Optional[float]
    risk_level: str         # low | medium | high
    reason: str
    explanation: str
    evidence_quality: str
    usefulness_score: int
    source_dependency: str
    taxonomy_reason: Optional[str]
    deterministic_decision: str
    llm_decision: Optional[str]
    final_decision: str
    was_guard_blocked: bool
    was_downgraded_reject: bool
    profile: str = PROFILE_CONSERVATIVE
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class TriageMetrics:
    """Aggregate triage metrics for a run."""
    total_findings: int = 0
    strong_keep_count: int = 0
    main_review_count: int = 0
    borderline_count: int = 0
    needs_context_count: int = 0
    suggested_reject_count: int = 0
    hidden_by_critic_count: int = 0

    # Legacy alias names (kept for backward compat)
    visible_by_default_count: int = 0
    collapsed_by_default_count: int = 0
    workload_reduction_count: int = 0
    workload_reduction_percent: float = 0.0

    # New primary-queue metrics
    primary_visible_count: int = 0      # = strong_keep + main_review + borderline + needs_context
    primary_collapsed_count: int = 0    # = suggested_reject + hidden_by_critic
    primary_queue_reduction_percent: float = 0.0

    # Human-label metrics (populated when human decisions are provided)
    hidden_human_accepted_count: int = 0
    hidden_human_rejected_count: int = 0
    suggested_reject_human_accepted_count: int = 0
    suggested_reject_human_rejected_count: int = 0

    accepted_visible_recall: Optional[float] = None          # legacy: NOT hidden
    accepted_primary_visible_recall: Optional[float] = None  # in primary_visible queue
    accepted_not_hidden_recall: Optional[float] = None       # NOT in hidden_by_critic

    hidden_precision_against_human: Optional[float] = None
    suggested_reject_precision_against_human: Optional[float] = None
    risky_hidden_cases: list[dict] = field(default_factory=list)
    profile: str = PROFILE_CONSERVATIVE


# ─── Profile config ───────────────────────────────────────────────────────────


@dataclass
class TriageProfileConfig:
    """Configuration that drives queue assignment differences between profiles."""
    name: str
    # Minimum confidence for LLM taxonomy-based suggested_reject
    suggested_reject_conf_threshold: float
    # Taxonomy reasons that trigger suggested_reject in this profile
    suggested_reject_taxonomies: frozenset
    # Whether strong_keep findings can appear in suggested_reject (not in hidden)
    allow_strong_keep_in_suggested_reject: bool
    # Minimum confidence for hidden_by_critic via LLM taxonomy
    hidden_conf_threshold: float
    # Whether det-reject findings with low score/ev go to hidden (vs suggested_reject)
    aggressive_det_hide: bool
    # Label for reports
    label: str
    # Non-production warning
    non_production: bool = False


_PROFILE_CONFIGS: dict[str, TriageProfileConfig] = {
    PROFILE_CONSERVATIVE: TriageProfileConfig(
        name=PROFILE_CONSERVATIVE,
        label="Conservative (safe, minimal suggested_reject)",
        suggested_reject_conf_threshold=_CONF_THRESHOLD_HIDDEN,  # same high bar
        suggested_reject_taxonomies=frozenset(),  # no taxonomy-based expansion
        allow_strong_keep_in_suggested_reject=False,
        hidden_conf_threshold=_CONF_THRESHOLD_HIDDEN,
        aggressive_det_hide=False,
    ),
    PROFILE_ASSISTED: TriageProfileConfig(
        name=PROFILE_ASSISTED,
        label="Assisted (expands suggested_reject, primary workload driver)",
        suggested_reject_conf_threshold=_CONF_THRESHOLD_ASSISTED_SR,
        suggested_reject_taxonomies=frozenset(_ASSISTED_SUGGESTED_REJECT_TAXONOMIES),
        allow_strong_keep_in_suggested_reject=False,  # strong_keep never goes to SR
        hidden_conf_threshold=_CONF_THRESHOLD_HIDDEN,
        aggressive_det_hide=False,
    ),
    PROFILE_AGGRESSIVE: TriageProfileConfig(
        name=PROFILE_AGGRESSIVE,
        label="Aggressive (non-production experimental)",
        suggested_reject_conf_threshold=_CONF_THRESHOLD_AGGRESSIVE_SR,
        suggested_reject_taxonomies=frozenset(
            _ASSISTED_SUGGESTED_REJECT_TAXONOMIES | _AGGRESSIVE_EXTRA_SUGGESTED_REJECT_TAXONOMIES
        ),
        allow_strong_keep_in_suggested_reject=True,  # even strong_keep can be SR in aggressive
        hidden_conf_threshold=_CONF_THRESHOLD_HIDDEN,
        aggressive_det_hide=True,
        non_production=True,
    ),
    # assisted_round1: inherits the conservative base profile, then a
    # post-processor applies A1+C+D rules from the round-1 manual review.
    # Rationale: simulation showed precision ~30-50% with risk <5% accepted.
    # All downgrades land in suggested_reject only; hidden_by_critic is never
    # touched by these rules. See simulate_critic_v2_tuning_rules_round1.py.
    PROFILE_ASSISTED_ROUND1: TriageProfileConfig(
        name=PROFILE_ASSISTED_ROUND1,
        label="Assisted round1 (conservative + A1+C+D post-rules)",
        suggested_reject_conf_threshold=_CONF_THRESHOLD_HIDDEN,
        suggested_reject_taxonomies=frozenset(),
        allow_strong_keep_in_suggested_reject=False,
        hidden_conf_threshold=_CONF_THRESHOLD_HIDDEN,
        aggressive_det_hide=False,
        non_production=True,  # experimental: do not use in production reports
    ),
}


# ──────────────────────────────────────────────────────────────────────────────
# Round-1 post-processing rules (A1 / C / D)
#
# Pre-review features only. The rules MUST NOT read human_decision /
# preferred_tab / reviewer_note / human_reason / triage_correct / priority.
# That invariant is enforced by tests via a SpyDict.
# ──────────────────────────────────────────────────────────────────────────────

ROUND1_REASON_OCR = "round1_ocr_artifact_suggested_reject"
ROUND1_REASON_RD_PZ = "round1_rd_vs_pz_suggested_reject"
ROUND1_REASON_ALREADY_COVERED = "round1_already_covered_suggested_reject"

# OCR markers — case-insensitive substrings.
_ROUND1_OCR_MARKERS = (
    "ocr", "распозна", "нераспозн", "мусор",
    "битый", "неразборчив", "неверное определени",
    "артефакт распозн",
)
# Broken short tokens like "А-А-А" or "ЦТ/ТЦ" — but only when at least one
# textual OCR/обозначение marker is present nearby (per spec).
import re as _re  # local alias to avoid shadowing module-level imports below
_ROUND1_BROKEN_TOKEN_RE = _re.compile(
    r"\b[а-яa-z]{1,3}(?:[-/][а-яa-z]{1,3}){2,}\b",
    _re.IGNORECASE,
)
_ROUND1_OCR_CONTEXT_HINT = ("обозначен", "распозна", "ocr",
                            "знак", "символ", "марк")

# RD vs PZ markers (calculation parameters living in ПЗ, not in РД drawings).
_ROUND1_RD_PZ_MARKERS = (
    "пз раздела", "пояснительная записк", "расчёт", "расчет",
    "расчётный параметр", "расчетный параметр",
    "огнестойк", "rei ", " rei", "сп 468", "сп 385",
    "экспертиз", "нормативная база",
    "расчётное обоснован", "расчетное обоснован",
)
_ROUND1_RD_PZ_SECTIONS = {"KJ", "EOM"}

# Already covered / adjacent section / spec / table.
_ROUND1_ALREADY_COVERED_MARKERS = (
    "смежн", "дублирован",
    "присутствует в раздел", "присутствует на сторон",
    "указано в спецификац", "указано в ведомост",
    "есть схема", "имеется схема",
    "уже указан", "уже учт",
    "определяется по таблиц",
    "в марке кабел",
    "в общих указани",
)


def _round1_text_blob(finding: dict) -> str:
    """
    Build the text blob the round1 rules inspect.

    Only pre-review fields are read: title, description, recommendation,
    sub_problem, explanation. reviewer_note / human_reason are NEVER touched.
    """
    if not finding:
        return ""
    parts = []
    for key in ("title", "description", "recommendation",
                "sub_problem", "explanation"):
        v = finding.get(key)
        if v:
            parts.append(str(v))
    return " ".join(parts).lower()


def _round1_match_ocr(blob: str) -> bool:
    """A1 — OCR artifact rule."""
    if any(m in blob for m in _ROUND1_OCR_MARKERS):
        return True
    if _ROUND1_BROKEN_TOKEN_RE.search(blob):
        # only count broken tokens when supporting context appears
        return any(h in blob for h in _ROUND1_OCR_CONTEXT_HINT)
    return False


def _round1_match_rd_pz(section: Optional[str], blob: str) -> bool:
    """C — RD vs PZ rule. Section gated to KJ/EOM."""
    if section not in _ROUND1_RD_PZ_SECTIONS:
        return False
    return any(m in blob for m in _ROUND1_RD_PZ_MARKERS)


def _round1_match_already_covered(blob: str) -> bool:
    """D — already covered / adjacent section / spec rule."""
    return any(m in blob for m in _ROUND1_ALREADY_COVERED_MARKERS)


def _round1_eligible(decision: TriageDecision) -> bool:
    """
    Guardrails: round1 rules may only fire when it's still possible (and safe)
    to downgrade the item to suggested_reject.

    Skips:
      - already in hidden_by_critic (we never upgrade to hidden)
      - already in suggested_reject (no point)
      - already in needs_context (handled by spec separately, F is not enabled)
      - strong_keep with critical evidence (per spec, do not touch if
        evidence is valid + score >= 8 unless there's an explicit OCR/duplicate
        marker — that check is performed in apply_round1_rules below).
    """
    if decision.human_queue in (
        QUEUE_HIDDEN, QUEUE_SUGGESTED_REJECT, QUEUE_NEEDS_CONTEXT
    ):
        return False
    return True


def apply_round1_rules(decision: TriageDecision,
                       finding: dict) -> TriageDecision:
    """
    Post-process a TriageDecision using A1+C+D round1 rules.

    Returns the original decision when no rule fires or the decision is not
    eligible. Otherwise returns a *new* TriageDecision moved to
    suggested_reject, with the matching reason and `applied_round1_rules` in
    `raw`.

    This function is pure: it does not mutate the input.
    """
    if not _round1_eligible(decision):
        return decision

    blob = _round1_text_blob(finding)
    if not blob:
        return decision

    section = finding.get("section") or finding.get("discipline")
    matched: list[str] = []
    primary_reason: Optional[str] = None

    if _round1_match_ocr(blob):
        matched.append("A1_ocr")
        primary_reason = ROUND1_REASON_OCR
    if _round1_match_rd_pz(section, blob):
        matched.append("C_rd_pz")
        if primary_reason is None:
            primary_reason = ROUND1_REASON_RD_PZ
    if _round1_match_already_covered(blob):
        matched.append("D_already_covered")
        if primary_reason is None:
            primary_reason = ROUND1_REASON_ALREADY_COVERED

    if not matched:
        return decision

    # Strong-keep extra guard (spec §4): don't override safety-critical findings
    # unless there's an explicit OCR or duplicate marker.
    if decision.human_queue == QUEUE_STRONG_KEEP:
        severity = (finding.get("severity")
                    or finding.get("category") or "").upper()
        evidence = decision.evidence_quality
        score = decision.usefulness_score or 0
        if (
            severity in _HIGH_SEVERITY
            and evidence == EVIDENCE_VALID
            and score >= 8
            and "A1_ocr" not in matched
            and "D_already_covered" not in matched
        ):
            return decision

    # Preserve risk_level if it was already medium/high (spec §2).
    new_risk = decision.risk_level
    if new_risk not in (RISK_MEDIUM, RISK_HIGH):
        new_risk = RISK_MEDIUM

    new_raw = dict(decision.raw) if decision.raw else {}
    new_raw["applied_round1_rules"] = matched
    new_raw["round1_rule_reason"] = primary_reason
    new_raw["round1_pre_queue"] = decision.human_queue
    new_raw["round1_pre_reason"] = decision.reason

    return TriageDecision(
        finding_id=decision.finding_id,
        human_queue=QUEUE_SUGGESTED_REJECT,
        critic_recommendation=REC_REJECT,
        visible_by_default=False,
        collapsed_by_default=True,
        can_restore=True,
        confidence=decision.confidence,
        risk_level=new_risk,
        reason=primary_reason or decision.reason,
        explanation=(
            f"round1_rules={'+'.join(matched)} (was {decision.human_queue}); "
            f"{decision.explanation}"
        ),
        evidence_quality=decision.evidence_quality,
        usefulness_score=decision.usefulness_score,
        source_dependency=decision.source_dependency,
        taxonomy_reason=decision.taxonomy_reason,
        deterministic_decision=decision.deterministic_decision,
        llm_decision=decision.llm_decision,
        final_decision=decision.final_decision,
        was_guard_blocked=decision.was_guard_blocked,
        was_downgraded_reject=decision.was_downgraded_reject,
        profile=decision.profile,
        raw=new_raw,
    )


def get_profile_config(profile: str) -> TriageProfileConfig:
    """Return profile config for the given profile name."""
    if profile not in _PROFILE_CONFIGS:
        raise ValueError(f"Unknown triage profile: {profile!r}. Valid: {sorted(VALID_PROFILES)}")
    return _PROFILE_CONFIGS[profile]


# ─── Core assignment logic ────────────────────────────────────────────────────


def assign_triage_queue(
    finding: dict,
    deterministic_decision: QualityDecision,
    final_decision: QualityDecision,
    llm_decision: Optional[Any] = None,
    profile: str = PROFILE_CONSERVATIVE,
) -> TriageDecision:
    """
    Assign a finding to a human triage queue.

    Parameters
    ----------
    finding:
        Original finding dict from 03_findings.json.
    deterministic_decision:
        Decision from the deterministic critic_v2 engine.
    final_decision:
        Merged final decision (may differ from deterministic if LLM ran).
    llm_decision:
        Optional LLMCriticDecision (None when LLM did not run).
    profile:
        Triage profile: "conservative" | "assisted" | "aggressive".

    Returns
    -------
    TriageDecision with human_queue, visibility flags, risk level, and metrics.
    """
    cfg = get_profile_config(profile)

    # assisted_round1 inherits the conservative routing (its profile config
    # uses an empty suggested_reject_taxonomies set and the strong-keep guard).
    # Round1 rules are applied as a post-processor in build_triage_result.

    fid = deterministic_decision.finding_id
    det_dec = deterministic_decision.decision
    final_dec = final_decision.decision
    ev = final_decision.evidence_quality
    score = final_decision.usefulness_score
    severity = (finding.get("severity") or finding.get("category") or "").upper()
    has_action = final_decision.has_action
    has_impact = final_decision.has_impact

    # LLM fields
    llm_dec_str: Optional[str] = None
    llm_taxonomy: Optional[str] = None
    llm_conf: Optional[float] = None
    llm_ev_checked: bool = False
    source_dep: str = "enough_source"
    was_guard_blocked = False
    was_downgraded_reject = False

    if llm_decision is not None:
        llm_dec_str = getattr(llm_decision, "llm_decision", None)
        llm_taxonomy = getattr(llm_decision, "human_taxonomy_reason", None)
        llm_conf = getattr(llm_decision, "confidence", None)
        llm_ev_checked = bool(getattr(llm_decision, "evidence_checked", False))
        source_dep = getattr(llm_decision, "source_dependency", "enough_source") or "enough_source"
        was_downgraded_reject = (
            llm_dec_str == "reject"
            and final_dec in ("borderline", "needs_human", "accept")
        )

    # Detect guard-blocked cases
    if (
        det_dec == "accept"
        and deterministic_decision.evidence_quality == EVIDENCE_VALID
        and deterministic_decision.usefulness_score >= 8
        and llm_dec_str == "reject"
        and final_dec in ("accept", "borderline", "needs_human")
    ):
        was_guard_blocked = True

    # ── Queue assignment ──────────────────────────────────────────────────────

    # 1. needs_context — always takes priority regardless of profile
    if (
        source_dep in ("needs_more_context", "cross_section_required")
        or llm_taxonomy in _NEEDS_CONTEXT_TAXONOMY
        or llm_dec_str == "needs_human"
    ):
        return _make_needs_context(
            fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
            was_guard_blocked, was_downgraded_reject, finding,
            deterministic_decision, final_decision, llm_decision, profile,
        )

    # 2. strong_keep — always visible; never goes to hidden_by_critic
    #    In assisted/aggressive: strong_keep CAN go to suggested_reject only
    #    when profile allows AND LLM explicitly suggested it. But by default stays strong_keep.
    is_sk = _is_strong_keep(det_dec, ev, score, severity, was_guard_blocked, deterministic_decision)
    if is_sk and not cfg.allow_strong_keep_in_suggested_reject:
        return _make_strong_keep(
            fid, ev, score, llm_taxonomy, llm_dec_str, llm_conf,
            was_guard_blocked, was_downgraded_reject, finding,
            deterministic_decision, final_decision, llm_decision, profile,
        )

    # In aggressive: strong_keep with LLM-reject can go to suggested_reject
    if is_sk and cfg.allow_strong_keep_in_suggested_reject and llm_dec_str == "reject":
        return _make_suggested_reject(
            fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
            was_guard_blocked, was_downgraded_reject, finding,
            deterministic_decision, final_decision, llm_decision, profile,
            reason="aggressive_strong_keep_llm_reject",
        )
    elif is_sk:
        return _make_strong_keep(
            fid, ev, score, llm_taxonomy, llm_dec_str, llm_conf,
            was_guard_blocked, was_downgraded_reject, finding,
            deterministic_decision, final_decision, llm_decision, profile,
        )

    # 3. Assisted/aggressive: taxonomy-based suggested_reject expansion
    if _is_taxonomy_suggested_reject(
        llm_dec_str, llm_taxonomy, llm_conf, llm_ev_checked, source_dep,
        det_dec, ev, score, severity, was_guard_blocked, deterministic_decision, cfg,
    ):
        return _make_suggested_reject(
            fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
            was_guard_blocked, was_downgraded_reject, finding,
            deterministic_decision, final_decision, llm_decision, profile,
            reason="assisted_taxonomy_suggested_reject",
        )

    # 4. hidden_by_critic — safest rejects only (all profiles)
    if _is_safe_to_hide(
        det_dec, final_dec, ev, score, severity, llm_dec_str, llm_taxonomy,
        llm_conf, llm_ev_checked, source_dep, was_guard_blocked,
        deterministic_decision, llm_decision, cfg,
    ):
        return _make_hidden(
            fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
            was_guard_blocked, was_downgraded_reject, finding,
            deterministic_decision, final_decision, llm_decision, profile,
        )

    # 5. suggested_reject — LLM recommends reject but not safe to hide
    if (
        final_dec == "reject"
        or (llm_dec_str == "reject" and not is_sk)
    ):
        return _make_suggested_reject(
            fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
            was_guard_blocked, was_downgraded_reject, finding,
            deterministic_decision, final_decision, llm_decision, profile,
        )

    # 6. borderline
    if (
        final_dec == "borderline"
        or final_dec == "needs_human"
        or was_downgraded_reject
        or llm_taxonomy in _BORDERLINE_TAXONOMY
        or ev == EVIDENCE_WEAK
    ):
        return _make_borderline(
            fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
            was_guard_blocked, was_downgraded_reject, finding,
            deterministic_decision, final_decision, llm_decision, profile,
        )

    # 7. main_review
    if (
        final_dec == "accept"
        and score >= 7
        and ev in (EVIDENCE_PARTIAL, EVIDENCE_VALID)
        and has_action
        and has_impact
    ):
        return _make_main_review(
            fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
            was_guard_blocked, was_downgraded_reject, finding,
            deterministic_decision, final_decision, llm_decision, profile,
        )

    # 8. Default: borderline
    return _make_borderline(
        fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
        was_guard_blocked, was_downgraded_reject, finding,
        deterministic_decision, final_decision, llm_decision, profile,
    )


# ─── Queue predicate helpers ──────────────────────────────────────────────────


def _is_strong_keep(
    det_dec: str,
    ev: str,
    score: int,
    severity: str,
    was_guard_blocked: bool,
    det: QualityDecision,
) -> bool:
    if was_guard_blocked:
        return True
    if det_dec == "accept" and ev == EVIDENCE_VALID and score >= 8:
        return True
    if (
        severity in _HIGH_SEVERITY
        and score >= 7
        and ev in (EVIDENCE_PARTIAL, EVIDENCE_VALID)
        and det_dec == "accept"
    ):
        return True
    return False


def _is_taxonomy_suggested_reject(
    llm_dec_str: Optional[str],
    llm_taxonomy: Optional[str],
    llm_conf: Optional[float],
    llm_ev_checked: bool,
    source_dep: str,
    det_dec: str,
    ev: str,
    score: int,
    severity: str,
    was_guard_blocked: bool,
    det: QualityDecision,
    cfg: TriageProfileConfig,
) -> bool:
    """
    True when profile-specific taxonomy expansion should route to suggested_reject.

    Conditions:
    - LLM decision is reject
    - LLM taxonomy is in the profile's suggested_reject_taxonomies
    - Confidence >= profile threshold
    - source_dependency is enough_source
    - NOT a strong_keep finding (they stay strong_keep or go to suggested_reject
      only in aggressive mode, handled separately above)
    - Guard-blocked findings are handled as strong_keep above; skip here
    """
    if not cfg.suggested_reject_taxonomies:
        return False
    if was_guard_blocked:
        return False
    if llm_dec_str != "reject":
        return False
    if llm_taxonomy not in cfg.suggested_reject_taxonomies:
        return False
    if llm_conf is None or llm_conf < cfg.suggested_reject_conf_threshold:
        return False
    if source_dep not in ("enough_source",):
        return False
    return True


def _is_safe_to_hide(
    det_dec: str,
    final_dec: str,
    ev: str,
    score: int,
    severity: str,
    llm_dec_str: Optional[str],
    llm_taxonomy: Optional[str],
    llm_conf: Optional[float],
    llm_ev_checked: bool,
    source_dep: str,
    was_guard_blocked: bool,
    det: QualityDecision,
    llm_decision: Optional[Any],
    cfg: TriageProfileConfig,
) -> bool:
    # Never hide strong_keep
    if _is_strong_keep(det_dec, ev, score, severity, was_guard_blocked, det):
        return False

    # Never hide critical/economic with partial/valid evidence
    if severity in _HIGH_SEVERITY and ev in (EVIDENCE_PARTIAL, EVIDENCE_VALID):
        return False

    if was_guard_blocked:
        return False

    # Case A: deterministic reject with safe objective reason
    if det_dec == "reject" and det.reject_reason in _SAFE_DET_REJECT_REASONS:
        # In aggressive mode also hides low-confidence deterministic accepts
        return True

    # Case B: LLM reject with safety preconditions
    if (
        llm_dec_str == "reject"
        and final_dec == "reject"
        and llm_taxonomy in _SAFE_LLM_TAXONOMY_FOR_HIDDEN
        and llm_conf is not None
        and llm_conf >= cfg.hidden_conf_threshold
        and llm_ev_checked
        and source_dep == "enough_source"
        and ev != EVIDENCE_NONE
    ):
        return True

    return False


# ─── Queue constructors ───────────────────────────────────────────────────────


def _common_raw(
    det: QualityDecision,
    final: QualityDecision,
    llm_decision: Optional[Any],
) -> dict:
    raw: dict[str, Any] = {
        "det_decision": det.decision,
        "det_score": det.usefulness_score,
        "det_reject_reason": det.reject_reason,
        "final_decision": final.decision,
        "final_score": final.usefulness_score,
    }
    if llm_decision is not None:
        raw["llm_decision"] = getattr(llm_decision, "llm_decision", None)
        raw["llm_taxonomy"] = getattr(llm_decision, "human_taxonomy_reason", None)
        raw["llm_confidence"] = getattr(llm_decision, "confidence", None)
        raw["llm_evidence_checked"] = getattr(llm_decision, "evidence_checked", False)
        raw["llm_source_dependency"] = getattr(llm_decision, "source_dependency", None)
        raw["llm_explanation"] = getattr(llm_decision, "explanation", None)
    return raw


def _make_strong_keep(
    fid, ev, score, llm_taxonomy, llm_dec_str, llm_conf,
    was_guard_blocked, was_downgraded_reject, finding, det, final, llm_dec,
    profile: str = PROFILE_CONSERVATIVE,
) -> TriageDecision:
    reason = "deterministic_accept_high_score" if not was_guard_blocked else "guard_blocked_llm_reject"
    exp_parts = [f"score={score}, ev={ev}"]
    if was_guard_blocked:
        exp_parts.append("HIGH_SCORE_VALID_ACCEPT_GUARD blocked LLM reject")
    if llm_dec_str and llm_dec_str != "accept":
        exp_parts.append(f"LLM said {llm_dec_str} but overridden")
    return TriageDecision(
        finding_id=fid,
        human_queue=QUEUE_STRONG_KEEP,
        critic_recommendation=REC_KEEP,
        visible_by_default=True,
        collapsed_by_default=False,
        can_restore=False,
        confidence=llm_conf,
        risk_level=RISK_LOW,
        reason=reason,
        explanation="; ".join(exp_parts),
        evidence_quality=ev,
        usefulness_score=score,
        source_dependency=getattr(llm_dec, "source_dependency", "enough_source") if llm_dec else "enough_source",
        taxonomy_reason=llm_taxonomy,
        deterministic_decision=det.decision,
        llm_decision=llm_dec_str,
        final_decision=final.decision,
        was_guard_blocked=was_guard_blocked,
        was_downgraded_reject=was_downgraded_reject,
        profile=profile,
        raw=_common_raw(det, final, llm_dec),
    )


def _make_main_review(
    fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
    was_guard_blocked, was_downgraded_reject, finding, det, final, llm_dec,
    profile: str = PROFILE_CONSERVATIVE,
) -> TriageDecision:
    return TriageDecision(
        finding_id=fid,
        human_queue=QUEUE_MAIN_REVIEW,
        critic_recommendation=REC_KEEP,
        visible_by_default=True,
        collapsed_by_default=False,
        can_restore=False,
        confidence=llm_conf,
        risk_level=RISK_LOW,
        reason="accepted_good_score_evidence",
        explanation=f"final=accept, score={score}, ev={ev}, action={final.has_action}, impact={final.has_impact}",
        evidence_quality=ev,
        usefulness_score=score,
        source_dependency=source_dep,
        taxonomy_reason=llm_taxonomy,
        deterministic_decision=det.decision,
        llm_decision=llm_dec_str,
        final_decision=final.decision,
        was_guard_blocked=was_guard_blocked,
        was_downgraded_reject=was_downgraded_reject,
        profile=profile,
        raw=_common_raw(det, final, llm_dec),
    )


def _make_borderline(
    fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
    was_guard_blocked, was_downgraded_reject, finding, det, final, llm_dec,
    profile: str = PROFILE_CONSERVATIVE,
) -> TriageDecision:
    reasons = []
    if final.decision in ("borderline", "needs_human"):
        reasons.append(f"final={final.decision}")
    if was_downgraded_reject:
        reasons.append("llm_reject_downgraded_by_confidence_gate")
    if llm_taxonomy in _BORDERLINE_TAXONOMY:
        reasons.append(f"taxonomy={llm_taxonomy}")
    if ev == EVIDENCE_WEAK:
        reasons.append("weak_evidence")
    if not reasons:
        reasons.append("uncertain")
    return TriageDecision(
        finding_id=fid,
        human_queue=QUEUE_BORDERLINE,
        critic_recommendation=REC_REVIEW,
        visible_by_default=True,
        collapsed_by_default=False,
        can_restore=False,
        confidence=llm_conf,
        risk_level=RISK_MEDIUM,
        reason="borderline",
        explanation="; ".join(reasons),
        evidence_quality=ev,
        usefulness_score=score,
        source_dependency=source_dep,
        taxonomy_reason=llm_taxonomy,
        deterministic_decision=det.decision,
        llm_decision=llm_dec_str,
        final_decision=final.decision,
        was_guard_blocked=was_guard_blocked,
        was_downgraded_reject=was_downgraded_reject,
        profile=profile,
        raw=_common_raw(det, final, llm_dec),
    )


def _make_needs_context(
    fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
    was_guard_blocked, was_downgraded_reject, finding, det, final, llm_dec,
    profile: str = PROFILE_CONSERVATIVE,
) -> TriageDecision:
    return TriageDecision(
        finding_id=fid,
        human_queue=QUEUE_NEEDS_CONTEXT,
        critic_recommendation=REC_NEEDS_CONTEXT,
        visible_by_default=True,
        collapsed_by_default=False,
        can_restore=False,
        confidence=llm_conf,
        risk_level=RISK_MEDIUM,
        reason="needs_context",
        explanation=f"source_dependency={source_dep}, taxonomy={llm_taxonomy}, llm={llm_dec_str}",
        evidence_quality=ev,
        usefulness_score=score,
        source_dependency=source_dep,
        taxonomy_reason=llm_taxonomy,
        deterministic_decision=det.decision,
        llm_decision=llm_dec_str,
        final_decision=final.decision,
        was_guard_blocked=was_guard_blocked,
        was_downgraded_reject=was_downgraded_reject,
        profile=profile,
        raw=_common_raw(det, final, llm_dec),
    )


def _make_suggested_reject(
    fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
    was_guard_blocked, was_downgraded_reject, finding, det, final, llm_dec,
    profile: str = PROFILE_CONSERVATIVE,
    reason: str = "suggested_reject_not_safe_to_hide",
) -> TriageDecision:
    risk = RISK_MEDIUM
    if ev in (EVIDENCE_PARTIAL, EVIDENCE_VALID) and score >= 6:
        risk = RISK_HIGH
    return TriageDecision(
        finding_id=fid,
        human_queue=QUEUE_SUGGESTED_REJECT,
        critic_recommendation=REC_REJECT,
        visible_by_default=False,
        collapsed_by_default=True,
        can_restore=True,
        confidence=llm_conf,
        risk_level=risk,
        reason=reason,
        explanation=(
            f"final={final.decision}, ev={ev}, score={score}, llm={llm_dec_str}, "
            f"taxonomy={llm_taxonomy}, guard_blocked={was_guard_blocked}, profile={profile}"
        ),
        evidence_quality=ev,
        usefulness_score=score,
        source_dependency=source_dep,
        taxonomy_reason=llm_taxonomy,
        deterministic_decision=det.decision,
        llm_decision=llm_dec_str,
        final_decision=final.decision,
        was_guard_blocked=was_guard_blocked,
        was_downgraded_reject=was_downgraded_reject,
        profile=profile,
        raw=_common_raw(det, final, llm_dec),
    )


def _make_hidden(
    fid, ev, score, source_dep, llm_taxonomy, llm_dec_str, llm_conf,
    was_guard_blocked, was_downgraded_reject, finding, det, final, llm_dec,
    profile: str = PROFILE_CONSERVATIVE,
) -> TriageDecision:
    if det.decision == "reject" and det.reject_reason in _SAFE_DET_REJECT_REASONS:
        reason = f"det_reject:{det.reject_reason}"
    elif llm_dec_str == "reject" and llm_taxonomy:
        reason = f"llm_reject:{llm_taxonomy}"
    else:
        reason = "safe_reject"
    return TriageDecision(
        finding_id=fid,
        human_queue=QUEUE_HIDDEN,
        critic_recommendation=REC_REJECT,
        visible_by_default=False,
        collapsed_by_default=True,
        can_restore=True,
        confidence=llm_conf,
        risk_level=RISK_LOW,
        reason=reason,
        explanation=(
            f"det={det.decision}({det.reject_reason}), "
            f"llm={llm_dec_str}({llm_taxonomy}), ev={ev}, score={score}"
        ),
        evidence_quality=ev,
        usefulness_score=score,
        source_dependency=source_dep,
        taxonomy_reason=llm_taxonomy,
        deterministic_decision=det.decision,
        llm_decision=llm_dec_str,
        final_decision=final.decision,
        was_guard_blocked=was_guard_blocked,
        was_downgraded_reject=was_downgraded_reject,
        profile=profile,
        raw=_common_raw(det, final, llm_dec),
    )


# ─── Batch builder ────────────────────────────────────────────────────────────


def build_triage_result(
    findings: list[dict],
    decisions: list[QualityDecision],
    llm_decisions: Optional[list[Any]] = None,
    profile: str = PROFILE_CONSERVATIVE,
) -> list[TriageDecision]:
    """
    Assign triage queues for a batch of findings.

    Parameters
    ----------
    findings:
        Original finding dicts from 03_findings.json.
    decisions:
        QualityDecision list from the deterministic engine (or merged final decisions).
    llm_decisions:
        Optional list of LLMCriticDecision.
    profile:
        Triage profile: "conservative" | "assisted" | "aggressive".
    """
    finding_by_id: dict[str, dict] = {}
    for f in findings:
        fid = f.get("id") or f.get("finding_id") or ""
        if fid:
            finding_by_id[fid] = f

    llm_by_id: dict[str, Any] = {}
    if llm_decisions:
        for ld in llm_decisions:
            lid = getattr(ld, "finding_id", None)
            if lid:
                llm_by_id[lid] = ld

    result: list[TriageDecision] = []
    for det_dec in decisions:
        fid = det_dec.finding_id
        finding = finding_by_id.get(fid, {})
        llm_dec = llm_by_id.get(fid)
        final_dec = det_dec

        td = assign_triage_queue(
            finding=finding,
            deterministic_decision=det_dec,
            final_decision=final_dec,
            llm_decision=llm_dec,
            profile=profile,
        )
        # Apply round-1 post-processor for the experimental profile.
        # Pure function: returns either the original `td` or a fresh
        # TriageDecision moved to suggested_reject with the matching reason.
        if profile == PROFILE_ASSISTED_ROUND1:
            td = apply_round1_rules(td, finding)
        result.append(td)

    return result


# ─── Metrics computation ──────────────────────────────────────────────────────


def compute_triage_metrics(
    triage_decisions: list[TriageDecision],
    human_decisions: Optional[dict[str, str]] = None,
    profile: str = PROFILE_CONSERVATIVE,
) -> TriageMetrics:
    """
    Compute triage metrics from a list of TriageDecision.

    Parameters
    ----------
    triage_decisions:
        Output of build_triage_result / assign_triage_queue calls.
    human_decisions:
        Optional dict mapping finding_id → "accepted"|"rejected".
    profile:
        Profile name for reporting.
    """
    m = TriageMetrics(profile=profile)
    m.total_findings = len(triage_decisions)

    for td in triage_decisions:
        if td.human_queue == QUEUE_STRONG_KEEP:
            m.strong_keep_count += 1
        elif td.human_queue == QUEUE_MAIN_REVIEW:
            m.main_review_count += 1
        elif td.human_queue == QUEUE_BORDERLINE:
            m.borderline_count += 1
        elif td.human_queue == QUEUE_NEEDS_CONTEXT:
            m.needs_context_count += 1
        elif td.human_queue == QUEUE_SUGGESTED_REJECT:
            m.suggested_reject_count += 1
        elif td.human_queue == QUEUE_HIDDEN:
            m.hidden_by_critic_count += 1

        if td.visible_by_default:
            m.visible_by_default_count += 1
        if td.collapsed_by_default:
            m.collapsed_by_default_count += 1

    # Primary queue metrics
    m.primary_visible_count = (
        m.strong_keep_count + m.main_review_count
        + m.borderline_count + m.needs_context_count
    )
    m.primary_collapsed_count = m.suggested_reject_count + m.hidden_by_critic_count

    # Legacy aliases
    m.workload_reduction_count = m.collapsed_by_default_count
    m.visible_by_default_count = m.primary_visible_count  # keep in sync

    if m.total_findings > 0:
        m.workload_reduction_percent = round(
            m.workload_reduction_count / m.total_findings * 100, 1
        )
        m.primary_queue_reduction_percent = round(
            m.primary_collapsed_count / m.total_findings * 100, 1
        )

    # Human-label metrics
    if human_decisions:
        primary_visible_ids = {
            td.finding_id for td in triage_decisions
            if td.human_queue in (QUEUE_STRONG_KEEP, QUEUE_MAIN_REVIEW,
                                   QUEUE_BORDERLINE, QUEUE_NEEDS_CONTEXT)
        }
        hidden_ids = {td.finding_id for td in triage_decisions if td.human_queue == QUEUE_HIDDEN}
        suggested_ids = {td.finding_id for td in triage_decisions if td.human_queue == QUEUE_SUGGESTED_REJECT}

        total_human_accepted = sum(1 for v in human_decisions.values() if v == "accepted")
        hidden_human_accepted = sum(
            1 for fid, dec in human_decisions.items()
            if dec == "accepted" and fid in hidden_ids
        )
        hidden_human_rejected = sum(
            1 for fid, dec in human_decisions.items()
            if dec == "rejected" and fid in hidden_ids
        )
        suggested_human_accepted = sum(
            1 for fid, dec in human_decisions.items()
            if dec == "accepted" and fid in suggested_ids
        )
        suggested_human_rejected = sum(
            1 for fid, dec in human_decisions.items()
            if dec == "rejected" and fid in suggested_ids
        )
        primary_visible_accepted = sum(
            1 for fid, dec in human_decisions.items()
            if dec == "accepted" and fid in primary_visible_ids
        )

        m.hidden_human_accepted_count = hidden_human_accepted
        m.hidden_human_rejected_count = hidden_human_rejected
        m.suggested_reject_human_accepted_count = suggested_human_accepted
        m.suggested_reject_human_rejected_count = suggested_human_rejected

        if total_human_accepted > 0:
            # Legacy: NOT in hidden_by_critic
            m.accepted_visible_recall = round(
                (total_human_accepted - hidden_human_accepted) / total_human_accepted, 4
            )
            # New: in primary_visible queue (strong_keep | main_review | borderline | needs_context)
            m.accepted_primary_visible_recall = round(
                primary_visible_accepted / total_human_accepted, 4
            )
            # New: NOT in hidden_by_critic (same as legacy accepted_visible_recall)
            m.accepted_not_hidden_recall = m.accepted_visible_recall

        total_hidden_with_human = hidden_human_accepted + hidden_human_rejected
        if total_hidden_with_human > 0:
            m.hidden_precision_against_human = round(
                hidden_human_rejected / total_hidden_with_human, 4
            )

        total_suggested_with_human = suggested_human_accepted + suggested_human_rejected
        if total_suggested_with_human > 0:
            m.suggested_reject_precision_against_human = round(
                suggested_human_rejected / total_suggested_with_human, 4
            )

        m.risky_hidden_cases = [
            {
                "finding_id": td.finding_id,
                "human_decision": human_decisions.get(td.finding_id),
                "ev": td.evidence_quality,
                "score": td.usefulness_score,
                "reason": td.reason,
                "taxonomy": td.taxonomy_reason,
                "det_decision": td.deterministic_decision,
                "llm_decision": td.llm_decision,
                "profile": td.profile,
            }
            for td in triage_decisions
            if td.human_queue == QUEUE_HIDDEN
            and human_decisions.get(td.finding_id) == "accepted"
        ]

    return m


# ─── Artifact serialization ───────────────────────────────────────────────────


def triage_decision_to_dict(td: TriageDecision) -> dict:
    return {
        "finding_id": td.finding_id,
        "human_queue": td.human_queue,
        "critic_recommendation": td.critic_recommendation,
        "visible_by_default": td.visible_by_default,
        "collapsed_by_default": td.collapsed_by_default,
        "can_restore": td.can_restore,
        "confidence": td.confidence,
        "risk_level": td.risk_level,
        "reason": td.reason,
        "explanation": td.explanation,
        "evidence_quality": td.evidence_quality,
        "usefulness_score": td.usefulness_score,
        "source_dependency": td.source_dependency,
        "taxonomy_reason": td.taxonomy_reason,
        "deterministic_decision": td.deterministic_decision,
        "llm_decision": td.llm_decision,
        "final_decision": td.final_decision,
        "was_guard_blocked": td.was_guard_blocked,
        "was_downgraded_reject": td.was_downgraded_reject,
        "profile": td.profile,
        "raw": td.raw,
    }


def triage_metrics_to_dict(m: TriageMetrics) -> dict:
    return {
        "total_findings": m.total_findings,
        "profile": m.profile,
        "strong_keep_count": m.strong_keep_count,
        "main_review_count": m.main_review_count,
        "borderline_count": m.borderline_count,
        "needs_context_count": m.needs_context_count,
        "suggested_reject_count": m.suggested_reject_count,
        "hidden_by_critic_count": m.hidden_by_critic_count,
        "visible_by_default_count": m.visible_by_default_count,
        "collapsed_by_default_count": m.collapsed_by_default_count,
        "workload_reduction_count": m.workload_reduction_count,
        "workload_reduction_percent": m.workload_reduction_percent,
        "primary_visible_count": m.primary_visible_count,
        "primary_collapsed_count": m.primary_collapsed_count,
        "primary_queue_reduction_percent": m.primary_queue_reduction_percent,
        "hidden_human_accepted_count": m.hidden_human_accepted_count,
        "hidden_human_rejected_count": m.hidden_human_rejected_count,
        "suggested_reject_human_accepted_count": m.suggested_reject_human_accepted_count,
        "suggested_reject_human_rejected_count": m.suggested_reject_human_rejected_count,
        "accepted_visible_recall": m.accepted_visible_recall,
        "accepted_primary_visible_recall": m.accepted_primary_visible_recall,
        "accepted_not_hidden_recall": m.accepted_not_hidden_recall,
        "hidden_precision_against_human": m.hidden_precision_against_human,
        "suggested_reject_precision_against_human": m.suggested_reject_precision_against_human,
        "risky_hidden_cases": m.risky_hidden_cases,
    }


def build_triage_artifacts(
    triage_decisions: list[TriageDecision],
    metrics: TriageMetrics,
) -> dict[str, Any]:
    all_dicts = [triage_decision_to_dict(td) for td in triage_decisions]
    hidden = [d for d in all_dicts if d["human_queue"] == QUEUE_HIDDEN]
    suggested = [d for d in all_dicts if d["human_queue"] == QUEUE_SUGGESTED_REJECT]
    visible = [d for d in all_dicts if d["visible_by_default"]]

    return {
        "critic_v2_triage": {"decisions": all_dicts, "count": len(all_dicts), "profile": metrics.profile},
        "critic_v2_triage_metrics": triage_metrics_to_dict(metrics),
        "critic_v2_hidden_by_critic": {"decisions": hidden, "count": len(hidden)},
        "critic_v2_suggested_reject": {"decisions": suggested, "count": len(suggested)},
        "critic_v2_visible_by_default": {"decisions": visible, "count": len(visible)},
        "critic_v2_risky_hidden_cases": metrics.risky_hidden_cases,
    }


# ─── Business workload view ───────────────────────────────────────────────────


def build_business_workload_view(metrics: TriageMetrics) -> dict:
    """
    Build a human-readable summary of workload reduction for the given metrics.
    Used in markdown reports.
    """
    m = metrics
    total = m.total_findings or 1
    return {
        "profile": m.profile,
        "total_findings": m.total_findings,
        "primary_queue_size": m.primary_visible_count,
        "collapsed_queue_size": m.primary_collapsed_count,
        "primary_queue_reduction_percent": m.primary_queue_reduction_percent,
        "suggested_reject_count": m.suggested_reject_count,
        "hidden_by_critic_count": m.hidden_by_critic_count,
        "human_accepted_in_suggested_reject": m.suggested_reject_human_accepted_count,
        "human_accepted_hidden": m.hidden_human_accepted_count,
        "accepted_primary_visible_recall": m.accepted_primary_visible_recall,
        "accepted_not_hidden_recall": m.accepted_not_hidden_recall,
        "hidden_precision": m.hidden_precision_against_human,
        "suggested_reject_precision": m.suggested_reject_precision_against_human,
    }


# ─── UI-ready triage export (offline, no production impact) ───────────────────

UI_TAB_PRIMARY = "primary"
UI_TAB_NEEDS_CONTEXT = "needs_context"
UI_TAB_SUGGESTED_REJECT = "suggested_reject"
UI_TAB_HIDDEN = "hidden_by_critic"

UI_TAB_LAYOUT: list[dict[str, Any]] = [
    {
        "key": UI_TAB_PRIMARY,
        "title": "Основная проверка",
        "default_open": True,
        "queues": [QUEUE_STRONG_KEEP, QUEUE_MAIN_REVIEW, QUEUE_BORDERLINE],
    },
    {
        "key": UI_TAB_NEEDS_CONTEXT,
        "title": "Требует смежников / контекста",
        "default_open": False,
        "queues": [QUEUE_NEEDS_CONTEXT],
    },
    {
        "key": UI_TAB_SUGGESTED_REJECT,
        "title": "Критик рекомендует отклонить",
        "default_open": False,
        "queues": [QUEUE_SUGGESTED_REJECT],
    },
    {
        "key": UI_TAB_HIDDEN,
        "title": "Скрыто критиком",
        "default_open": False,
        "queues": [QUEUE_HIDDEN],
    },
]

_QUEUE_TO_TAB: dict[str, str] = {
    q: tab["key"] for tab in UI_TAB_LAYOUT for q in tab["queues"]
}


def _ui_tab_for_queue(queue: str) -> str:
    return _QUEUE_TO_TAB.get(queue, UI_TAB_PRIMARY)


def build_ui_export(
    triage_decisions: list[TriageDecision],
    metrics: TriageMetrics,
    records_by_id: Optional[dict[str, dict]] = None,
    human_decisions: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """
    Build UI-ready triage export grouped into 4 tabs.

    The export is an OFFLINE artifact only. It does NOT modify
    03_findings_review.json or any production pipeline state.

    Tab layout:
      primary           = strong_keep + main_review + borderline (default open)
      needs_context     = needs_context (collapsed, can_restore)
      suggested_reject  = suggested_reject (collapsed, can_restore)
      hidden_by_critic  = hidden_by_critic (collapsed, can_restore)

    Parameters
    ----------
    triage_decisions:
        Output of build_triage_result.
    metrics:
        Output of compute_triage_metrics for the same decisions.
    records_by_id:
        Optional dict finding_id → benchmark record (project_name, section, title,
        description, recommendation, human_decision, human_reason, ...).
        Used to enrich items with project/section/text fields.
    human_decisions:
        Optional dict finding_id → "accepted"|"rejected" (benchmark mode).
    """
    records_by_id = records_by_id or {}
    human_decisions = human_decisions or {}

    # ── Per-tab counts ───────────────────────────────────────────────────────
    tab_counts: dict[str, int] = {tab["key"]: 0 for tab in UI_TAB_LAYOUT}
    queue_counts: dict[str, int] = {}
    for td in triage_decisions:
        queue_counts[td.human_queue] = queue_counts.get(td.human_queue, 0) + 1
        tab_counts[_ui_tab_for_queue(td.human_queue)] += 1

    # ── Tabs descriptor ──────────────────────────────────────────────────────
    tabs: list[dict[str, Any]] = []
    for tab in UI_TAB_LAYOUT:
        tabs.append({
            "key": tab["key"],
            "title": tab["title"],
            "default_open": tab["default_open"],
            "queues": list(tab["queues"]),
            "count": tab_counts[tab["key"]],
        })

    # ── Items ────────────────────────────────────────────────────────────────
    items: list[dict[str, Any]] = []
    for td in triage_decisions:
        rec = records_by_id.get(td.finding_id, {})
        h_dec = human_decisions.get(td.finding_id) or rec.get("human_decision")
        h_reason = rec.get("human_reason")

        item: dict[str, Any] = {
            "finding_id": td.finding_id,
            "project_name": rec.get("project_name") or rec.get("project") or "",
            "section": rec.get("section", ""),
            "title": rec.get("title", ""),
            "description": rec.get("description", ""),
            "recommendation": rec.get("recommendation") or rec.get("action") or "",
            "tab": _ui_tab_for_queue(td.human_queue),
            "queue": td.human_queue,
            "critic_recommendation": td.critic_recommendation,
            "reason": td.reason,
            "explanation": td.explanation,
            "confidence": td.confidence,
            "evidence_quality": td.evidence_quality,
            "score": td.usefulness_score,
            "source_dependency": td.source_dependency,
            "taxonomy_reason": td.taxonomy_reason,
            "visible_by_default": td.visible_by_default,
            "collapsed_by_default": td.collapsed_by_default,
            "can_restore": True,
            "risk_level": td.risk_level,
        }
        if h_dec in ("accepted", "rejected"):
            item["human_decision"] = h_dec
        if h_reason:
            item["human_reason"] = h_reason
        items.append(item)

    # ── Summary ──────────────────────────────────────────────────────────────
    primary_count = tab_counts[UI_TAB_PRIMARY]
    nc_count = tab_counts[UI_TAB_NEEDS_CONTEXT]
    sr_count = tab_counts[UI_TAB_SUGGESTED_REJECT]
    hc_count = tab_counts[UI_TAB_HIDDEN]

    total = metrics.total_findings or len(triage_decisions)
    collapsed = nc_count + sr_count + hc_count
    primary_queue_reduction_percent = round(
        (collapsed / total) * 100, 1
    ) if total else 0.0

    summary: dict[str, Any] = {
        "total": total,
        "primary_count": primary_count,
        "needs_context_count": nc_count,
        "suggested_reject_count": sr_count,
        "hidden_by_critic_count": hc_count,
        "primary_queue_reduction_percent": primary_queue_reduction_percent,
        "accepted_not_hidden_recall": metrics.accepted_not_hidden_recall,
        "accepted_primary_visible_recall": metrics.accepted_primary_visible_recall,
        "profile": metrics.profile,
        "experimental": True,
    }

    return {
        "summary": summary,
        "tabs": tabs,
        "items": items,
    }


def render_ui_export_markdown(
    ui_export: dict[str, Any],
    *,
    examples_per_collapsed_tab: int = 10,
) -> str:
    """Render a human-readable preview of the UI export."""
    summary = ui_export.get("summary", {})
    tabs = ui_export.get("tabs", [])
    items = ui_export.get("items", [])

    lines: list[str] = []
    lines.append("# Critic v2 UI Triage Preview")
    lines.append("")
    lines.append("> **EXPERIMENTAL — critic_v2 offline.** "
                 "Production pipeline is NOT modified. "
                 "03_findings_review.json format is unchanged. "
                 "This artifact is an offline UI hint only.")
    lines.append("")

    # ── Summary ──────────────────────────────────────────────────────────────
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- profile: `{summary.get('profile', '?')}`")
    lines.append(f"- total findings: **{summary.get('total', 0)}**")
    lines.append(f"- primary visible: **{summary.get('primary_count', 0)}**")
    lines.append(f"- needs_context: **{summary.get('needs_context_count', 0)}**")
    lines.append(f"- suggested_reject: **{summary.get('suggested_reject_count', 0)}**")
    lines.append(f"- hidden_by_critic: **{summary.get('hidden_by_critic_count', 0)}**")
    lines.append(
        f"- primary_queue_reduction_percent: "
        f"**{summary.get('primary_queue_reduction_percent', 0):.1f}%**"
    )
    nh = summary.get("accepted_not_hidden_recall")
    if nh is not None:
        lines.append(f"- accepted_not_hidden_recall: **{nh*100:.1f}%**")
    pv = summary.get("accepted_primary_visible_recall")
    if pv is not None:
        lines.append(f"- accepted_primary_visible_recall: **{pv*100:.1f}%**")
    lines.append("")

    # ── Tab logic ────────────────────────────────────────────────────────────
    lines.append("## Tab Logic")
    lines.append("")
    lines.append(
        "Primary stays open by default. The other three tabs are collapsed but "
        "every item is `can_restore = true`, so nothing is physically removed."
    )
    lines.append("")
    lines.append("| Tab | Default open | Queues | Count |")
    lines.append("|---|---|---|---|")
    for tab in tabs:
        queues = ", ".join(tab.get("queues", []))
        do = "✅" if tab.get("default_open") else "—"
        lines.append(
            f"| `{tab['key']}` — {tab['title']} | {do} | {queues} | "
            f"{tab.get('count', 0)} |"
        )
    lines.append("")

    # ── Examples per collapsed tab ───────────────────────────────────────────
    items_by_tab: dict[str, list[dict]] = {}
    for it in items:
        items_by_tab.setdefault(it.get("tab", ""), []).append(it)

    for tab in tabs:
        if tab.get("default_open"):
            continue
        key = tab["key"]
        sample = items_by_tab.get(key, [])[:examples_per_collapsed_tab]
        lines.append(f"## Examples — `{key}` ({tab.get('count', 0)})")
        lines.append("")
        if not sample:
            lines.append("_no items_")
            lines.append("")
            continue
        for it in sample:
            fid = it.get("finding_id", "?")
            title_lines = (it.get("title") or "").strip().splitlines()
            title = title_lines[0][:160] if title_lines else ""
            sec = it.get("section", "")
            score = it.get("score")
            ev = it.get("evidence_quality")
            tax = it.get("taxonomy_reason")
            h = it.get("human_decision", "—")
            lines.append(
                f"- `{fid}` [{sec}] score={score} ev={ev} taxonomy={tax} "
                f"human={h}"
            )
            if title:
                lines.append(f"    {title}")
        lines.append("")

    lines.append("---")
    lines.append("_Generated by triage.build_ui_export. "
                 "Production pipeline NOT modified._")
    return "\n".join(lines)


# ─── AR F-001 diagnostic ──────────────────────────────────────────────────────

_AR_F001_SCORE_CAP_DIAGNOSTIC = {
    "finding_id": "AR F-001",
    "reported_issue": (
        "Earlier report stated score=7, evidence=partial, score_cap=6, but finding passed. "
        "This appeared contradictory."
    ),
    "actual_cap_applied": 7,
    "explanation": (
        "The scorer applies a higher cap for КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ findings with partial evidence. "
        "The cap is raised from 6 to 7 for these high-severity categories. "
        "Therefore score=7 + partial + КРИТИЧЕСКОЕ is correctly within the cap. "
        "The earlier report used the generic partial cap (6) but the severity-aware cap (7) applied. "
        "No scorer logic change needed."
    ),
    "is_bug": False,
    "guard_status": (
        "HIGH_SCORE_VALID_ACCEPT_GUARD requires score>=8 + valid evidence. "
        "AR F-001 has score=7 + partial evidence → guard threshold NOT met. "
        "The finding was accepted deterministically and NOT protected by the guard. "
        "LLM classified it as visual_or_ocr_misread (false_reject). "
        "Triage policy: score=7 + partial + КРИТИЧЕСКОЕ → strong_keep (critical partial rule). "
        "This prevents LLM from hard-rejecting it via triage."
    ),
    "triage_queue": QUEUE_STRONG_KEEP,
}


def get_ar_f001_diagnostic() -> dict:
    return dict(_AR_F001_SCORE_CAP_DIAGNOSTIC)
