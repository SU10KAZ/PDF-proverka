"""Tests for Evidence Verifier parse and routing."""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.app.pipeline.stages.findings_review.evidence_verifier.classifier import (
    classify_evidence_case,
)
from backend.app.pipeline.stages.findings_review.evidence_verifier.kb_routing import (
    should_run_evidence_verifier,
)
from backend.app.pipeline.stages.findings_review.evidence_verifier.parse import (
    coerce_confidence,
    parse_verification_response,
)


def test_coerce_confidence_clamps():
    assert coerce_confidence(2) == 1.0
    assert coerce_confidence(-1) == 0.0
    assert coerce_confidence("bad") == 0.5


def test_parse_downgrades_weak_reject():
    raw = """[{"finding_id":"F-1","llm_decision":"reject","confidence":0.5,"verification_path":"graphic"}]"""
    out = parse_verification_response(raw, expected_ids={"F-1"}, verification_path="graphic")
    assert len(out) == 1
    assert out[0].llm_decision == "borderline"


def test_kb_routing_borderline_always_runs():
    run, reason = should_run_evidence_verifier({"id": "F-1"}, kb_decision={"llm_decision": "borderline"})
    assert run is True
    assert reason == "kb_borderline"


def test_classify_graphic_confirmed():
    finding = {
        "evidence": [{"type": "image", "block_id": "ABC-123"}],
        "grounding_level": "grounded_strong",
    }
    assert classify_evidence_case(finding, "accepted") == "graphic_confirmed"
