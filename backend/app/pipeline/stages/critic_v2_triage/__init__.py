"""
critic_v2_triage — post-processing stage поверх готовых findings.

Read-only по отношению к production artifacts. НЕ заменяет legacy critic,
НЕ меняет 03_findings.json/03_findings_review.json/expert_review.json.
По умолчанию НЕ подключён к manager pipeline (CRITIC_V2_ENABLED=False).
Все artifacts пишутся в <project>/_output/<CRITIC_V2_OUTPUT_SUBDIR>/.

Public API:
    from .runner import run_critic_v2_triage, CriticV2TriageStageResult
"""
from .runner import (
    CriticV2TriageStageResult,
    run_critic_v2_triage,
)

__all__ = [
    "CriticV2TriageStageResult",
    "run_critic_v2_triage",
]
