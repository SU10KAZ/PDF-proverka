"""Closed status vocabulary for the question-closure experiment."""
from __future__ import annotations

CONTRACT_SCHEMA_VERSION = "stage-comparison-question-closure-contracts.v1"
ANALYSIS_SCHEMA_VERSION = "stage-comparison-question-closure-analysis.v1"
TASKS_SCHEMA_VERSION = "stage-comparison-question-closure-ai-tasks.v1"
RUN_SCHEMA_VERSION = "stage-comparison-question-closure-run.v1"
GATE_SCHEMA_VERSION = "stage-comparison-question-closure-gate.v1"
AUDIT_SCHEMA_VERSION = "stage-comparison-question-closure-manual-audit.v1"

OPEN = "OPEN"
CLOSED_DETERMINISTIC = "CLOSED_DETERMINISTIC"
CLOSED_AI_STABLE = "CLOSED_AI_STABLE"
BLOCKED_MISSING_EVIDENCE = "BLOCKED_MISSING_EVIDENCE"
BLOCKED_AMBIGUOUS_EVIDENCE = "BLOCKED_AMBIGUOUS_EVIDENCE"
BLOCKED_POLICY = "BLOCKED_POLICY"
PARTIALLY_RESOLVED = "PARTIALLY_RESOLVED"

CLOSED_STATUSES = frozenset({CLOSED_DETERMINISTIC, CLOSED_AI_STABLE})
STATUSES = (
    OPEN,
    CLOSED_DETERMINISTIC,
    CLOSED_AI_STABLE,
    BLOCKED_MISSING_EVIDENCE,
    BLOCKED_AMBIGUOUS_EVIDENCE,
    BLOCKED_POLICY,
    PARTIALLY_RESOLVED,
)

SAFE_TO_CLOSE = "SAFE_TO_CLOSE"
UNSAFE_TO_CLOSE = "UNSAFE_TO_CLOSE"
PENDING = "PENDING"

__all__ = [name for name in globals() if name.isupper()]
