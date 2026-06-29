"""Evidence Verifier — document-grounded finding validation."""
from .engine import EvidenceVerifier, EVResult
from .golden_set import build_golden_set, load_golden_set
from .kb_routing import should_run_evidence_verifier
from .parse import EVDecision

__all__ = [
    "EvidenceVerifier",
    "EVResult",
    "EVDecision",
    "build_golden_set",
    "load_golden_set",
    "should_run_evidence_verifier",
]
