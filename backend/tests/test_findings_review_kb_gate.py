"""Tests for the KB-augmented findings validator gate."""
from __future__ import annotations

import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.app.pipeline.stages.findings_review.critic_v2 import kb_gate
from backend.app.pipeline.stages.findings_review.critic_v2.kb_gate import KBGate, _parse_response


class DummyRetriever:
    def find_similar(self, finding: dict, top_k: int = 5):
        return []


def test_parse_response_downgrades_low_confidence_reject_and_filters_examples():
    raw = """
    Here is the JSON:
    [
      {
        "finding_id": "F-1",
        "llm_decision": "reject",
        "human_taxonomy_reason": "visual_or_ocr_misread",
        "explanation": "OCR ошибся",
        "confidence": 0.7,
        "kb_examples_used": ["DEC-1", "DEC-X"],
        "evidence_checked": true
      }
    ]
    """

    decisions = _parse_response(raw, {"F-1": ["DEC-1", "DEC-2"]}, expected_ids={"F-1"})

    assert len(decisions) == 1
    assert decisions[0].finding_id == "F-1"
    assert decisions[0].llm_decision == "borderline"
    assert decisions[0].human_taxonomy_reason == "visual_or_ocr_misread"
    assert decisions[0].confidence == 0.7
    assert decisions[0].kb_examples_used == ["DEC-1"]


def test_parse_response_ignores_unexpected_ids_and_clamps_confidence():
    raw = json.dumps([
        {"finding_id": "F-1", "llm_decision": "accept", "confidence": 2},
        {"finding_id": "F-X", "llm_decision": "reject", "confidence": 0.99},
    ])

    decisions = _parse_response(raw, {"F-1": []}, expected_ids={"F-1"})

    assert len(decisions) == 1
    assert decisions[0].finding_id == "F-1"
    assert decisions[0].confidence == 1.0


def test_validate_fills_missing_llm_decisions_as_needs_human(monkeypatch):
    def fake_call(prompt: str, model: str, timeout: int = 180) -> str:
        return json.dumps([
            {
                "finding_id": "F-1",
                "llm_decision": "accept",
                "human_taxonomy_reason": None,
                "explanation": "Валидно",
                "confidence": 0.9,
                "kb_examples_used": [],
                "evidence_checked": True,
            }
        ])

    monkeypatch.setattr(kb_gate, "_call_claude_cli", fake_call)

    gate = KBGate(DummyRetriever(), model="sonnet", batch_size=2, top_k=0)
    result = gate.validate([
        {"id": "F-1", "problem": "Есть замечание"},
        {"id": "F-2", "problem": "Claude не вернул решение"},
    ])

    by_id = {d.finding_id: d for d in result.decisions}
    assert result.total_input == 2
    assert len(result.decisions) == 2
    assert result.accepted == 1
    assert result.needs_human == 1
    assert result.errors == 1
    assert by_id["F-2"].llm_decision == "needs_human"
    assert "ручная проверка" in by_id["F-2"].explanation
