"""
critic_v2/models.py
-------------------
Dataclasses for critic v2 offline engine.

NOT connected to production pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


# Evidence quality levels (ordered weakest → strongest)
EVIDENCE_NONE = "none"       # no refs, no quotes, no block_ids
EVIDENCE_WEAK = "weak"       # refs present but unverifiable (no block index) or single-block
                              # absence claim, or semantic mismatch by name heuristic
EVIDENCE_PARTIAL = "partial" # some refs verifiable, some phantom; or single existence ref
EVIDENCE_VALID = "valid"     # ≥1 verified ref (block exists in index) or ≥1 quote, or
                              # multiple independent refs pointing to concrete content


@dataclass
class NormalizedFinding:
    """Normalized view of a finding — backward-compatible with all project variants."""
    finding_id: str
    title: str
    description: str
    severity: str
    category: str
    evidence_refs: list[str]        # block_ids from evidence[] + related_block_ids
    evidence_quotes: list[str]      # text snippets from evidence_text_refs or description
    impact_area: Optional[str]      # category / risk keywords mapped to impact axis
    action_required: Optional[str]  # solution / action field
    confidence: Optional[float]     # from quality.confidence if present
    raw: dict[str, Any]             # original finding dict
    evidence_quality: str = EVIDENCE_NONE  # none / weak / partial / valid
    phantom_block_ids: list[str] = field(default_factory=list)  # block_ids not found in index
    verified_block_ids: list[str] = field(default_factory=list) # block_ids confirmed in index


@dataclass
class QualityDecision:
    """Decision made by critic v2 for one finding."""
    finding_id: str
    decision: str                    # accept / reject / borderline / merge / low_priority
    usefulness_score: int            # 0-10
    reject_reason: Optional[str]     # no_evidence / no_action / no_impact / generic_wording /
                                     # assumption_without_fact / ocr_artifact /
                                     # unsupported_by_source / low_business_value / duplicate /
                                     # cosmetic_no_practical_impact
    reject_explanation: Optional[str]
    merged_into: Optional[str]       # finding_id of primary when decision == merge
    impact_area: Optional[str]
    severity: Optional[str]
    has_evidence: bool
    has_action: bool
    has_impact: bool
    evidence_quality: str = EVIDENCE_NONE


@dataclass
class CriticV2Metrics:
    """Aggregate metrics for a critic v2 run."""
    total_input: int = 0
    rejected_by_rules: int = 0
    rejected_by_score: int = 0
    accepted: int = 0
    borderline: int = 0
    low_priority: int = 0
    merged: int = 0
    rejection_reasons: dict[str, int] = field(default_factory=dict)
    average_usefulness_score: float = 0.0


@dataclass
class CriticV2Result:
    """Full result of run_critic_v2_offline."""
    decisions: list[QualityDecision]
    accepted_findings: list[dict]
    rejected_findings: list[dict]
    merged_findings: list[dict]
    borderline_findings: list[dict]
    metrics: CriticV2Metrics
