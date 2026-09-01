"""Experimental HRO question-closure layer above AI Analyst v3 artifacts."""

from .closure import (
    analyze_question_closure,
    apply_closure_gate,
    build_pending_manual_audit,
    evaluate_closure_gate,
    materialize_closure_run,
)
from .selector import QuestionClosureSelector
from .production import (
    build_production_contracts,
    failure_artifact,
    run_production_question_closure,
)

__all__ = [
    "QuestionClosureSelector",
    "analyze_question_closure",
    "apply_closure_gate",
    "build_pending_manual_audit",
    "evaluate_closure_gate",
    "materialize_closure_run",
    "build_production_contracts",
    "failure_artifact",
    "run_production_question_closure",
]
