"""
critic_v2 — offline findings quality engine.

NOT connected to production pipeline. Does NOT call any LLM in deterministic mode.

Public API:
    from .engine import run_critic_v2_offline
    from .llm_gate import run_llm_gate, merge_llm_decisions, select_candidates
    from .models import CriticV2Result, QualityDecision, CriticV2Metrics, NormalizedFinding
    from .models import EVIDENCE_NONE, EVIDENCE_WEAK, EVIDENCE_PARTIAL, EVIDENCE_VALID
    from .llm_gate import LLMCriticDecision, LLMGateResult, MockProvider
"""
from .engine import run_critic_v2_offline
from .llm_gate import (
    LLMCriticDecision,
    LLMGateResult,
    LLM_REJECT_CONFIDENCE_THRESHOLD,
    LLM_FITNESS_MAP,
    MockProvider,
    NoopProvider,
    ClaudeRunnerProvider,
    OpenRouterProvider,
    VALID_TAXONOMY_REASONS,
    _REJECTION_ORIENTED_TAXONOMIES,
    merge_llm_decisions,
    run_llm_gate,
    select_candidates,
)
from .models import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    CriticV2Metrics,
    CriticV2Result,
    NormalizedFinding,
    QualityDecision,
)

__all__ = [
    # engine
    "run_critic_v2_offline",
    # llm_gate
    "run_llm_gate",
    "merge_llm_decisions",
    "select_candidates",
    "LLMCriticDecision",
    "LLMGateResult",
    "MockProvider",
    "NoopProvider",
    "ClaudeRunnerProvider",
    "OpenRouterProvider",
    "LLM_REJECT_CONFIDENCE_THRESHOLD",
    "LLM_FITNESS_MAP",
    "VALID_TAXONOMY_REASONS",
    # models
    "CriticV2Result",
    "QualityDecision",
    "CriticV2Metrics",
    "NormalizedFinding",
    "EVIDENCE_NONE",
    "EVIDENCE_WEAK",
    "EVIDENCE_PARTIAL",
    "EVIDENCE_VALID",
]
