"""
critic_v2/engine.py
--------------------
Main entry point for critic v2 offline engine.

NOT connected to production pipeline.
Does NOT call any LLM.

Usage:
    from backend.app.pipeline.stages.findings_review.critic_v2.engine import (
        run_critic_v2_offline,
    )
    result = run_critic_v2_offline(findings_list)
    result = run_critic_v2_offline(findings_list, blocks_index={"BLK-001", "BLK-002"})
"""
from __future__ import annotations

from typing import Optional

from .deduplicator import deduplicate
from .metrics import compute_metrics
from .models import EVIDENCE_NONE, CriticV2Metrics, CriticV2Result, QualityDecision
from .normalize import normalize_findings
from .rule_filter import apply_rule_filter
from .scorer import score_finding, score_to_decision


def run_critic_v2_offline(
    findings: list[dict],
    blocks_index: Optional[set[str]] = None,
) -> CriticV2Result:
    """
    Run deterministic critic v2 on a list of finding dicts.

    Pipeline:
        normalize → rule_filter → score → deduplication → classify

    Args:
        findings: list of finding dicts (from 03_findings.json or fixtures)
        blocks_index: optional set of known block_ids from 02_blocks_analysis.json.
                      When provided, enables phantom-block detection and upgrades
                      evidence_quality for verified blocks.

    Returns CriticV2Result with decisions, bucketed findings, and metrics.
    """
    if not findings:
        return CriticV2Result(
            decisions=[],
            accepted_findings=[],
            rejected_findings=[],
            merged_findings=[],
            borderline_findings=[],
            metrics=CriticV2Metrics(),
        )

    # ── Step 1: Normalize (with optional block index) ─────────────────────────
    normalized = normalize_findings(findings, blocks_index=blocks_index)

    # ── Step 2: Rule filter + scoring ────────────────────────────────────────
    scores: dict[str, int] = {}
    rule_results: dict[str, tuple] = {}

    for nf in normalized:
        reject_reason, reject_expl = apply_rule_filter(nf)
        rule_results[nf.finding_id] = (reject_reason, reject_expl)
        scores[nf.finding_id] = score_finding(nf)

    # ── Step 3: Deduplication (only non-rejected findings) ────────────────────
    non_rejected = [
        nf for nf in normalized
        if rule_results[nf.finding_id][0] is None
    ]
    merge_map: dict[str, str | None] = deduplicate(non_rejected, scores)

    for nf in normalized:
        if nf.finding_id not in merge_map:
            merge_map[nf.finding_id] = None

    # ── Step 4: Build decisions ───────────────────────────────────────────────
    decisions: list[QualityDecision] = []
    nf_by_id = {nf.finding_id: nf for nf in normalized}

    for nf in normalized:
        reject_reason, reject_expl = rule_results[nf.finding_id]
        score = scores[nf.finding_id]
        merged_into = merge_map.get(nf.finding_id)

        if merged_into is not None:
            decision = "merge"
        elif reject_reason:
            decision = "reject"
        else:
            decision = score_to_decision(score, None)

        decisions.append(QualityDecision(
            finding_id=nf.finding_id,
            decision=decision,
            usefulness_score=score,
            reject_reason=reject_reason,
            reject_explanation=reject_expl,
            merged_into=merged_into,
            impact_area=nf.impact_area,
            severity=nf.severity,
            has_evidence=bool(nf.evidence_refs or nf.evidence_quotes),
            has_action=bool(nf.action_required),
            has_impact=bool(nf.impact_area),
            evidence_quality=nf.evidence_quality,
        ))

    # ── Step 5: Bucket raw findings ───────────────────────────────────────────
    raw_by_id = {nf.finding_id: nf.raw for nf in normalized}

    accepted_findings = [raw_by_id[d.finding_id] for d in decisions if d.decision == "accept"]
    rejected_findings = [raw_by_id[d.finding_id] for d in decisions if d.decision == "reject"]
    merged_findings = [raw_by_id[d.finding_id] for d in decisions if d.decision == "merge"]
    borderline_findings = [
        raw_by_id[d.finding_id]
        for d in decisions if d.decision in ("borderline", "low_priority")
    ]

    # ── Step 6: Metrics ───────────────────────────────────────────────────────
    metrics = compute_metrics(decisions)

    return CriticV2Result(
        decisions=decisions,
        accepted_findings=accepted_findings,
        rejected_findings=rejected_findings,
        merged_findings=merged_findings,
        borderline_findings=borderline_findings,
        metrics=metrics,
    )
