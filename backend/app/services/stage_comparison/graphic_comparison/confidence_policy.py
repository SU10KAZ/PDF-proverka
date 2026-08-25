"""Versioned numeric-to-ledger confidence mapping for Mode 2."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LedgerConfidencePolicy:
    policy_id: str
    high_minimum: float
    medium_minimum: float

    def map(self, raw_confidence: Any) -> str:
        if (
            not isinstance(raw_confidence, (int, float))
            or isinstance(raw_confidence, bool)
            or not math.isfinite(float(raw_confidence))
            or not 0 <= raw_confidence <= 1
        ):
            raise ValueError("raw confidence must be a finite number in [0,1]")
        if raw_confidence >= self.high_minimum:
            return "HIGH"
        if raw_confidence >= self.medium_minimum:
            return "MEDIUM"
        return "LOW"

    def public_dict(self) -> dict:
        return {
            "policy_id": self.policy_id,
            "input": "numeric_0_to_1",
            "output": ["HIGH", "MEDIUM", "LOW"],
            "thresholds": {
                "high_minimum_inclusive": self.high_minimum,
                "medium_minimum_inclusive": self.medium_minimum,
            },
        }


MODE2_CONFIDENCE_POLICY_V1 = LedgerConfidencePolicy(
    policy_id="system-graph-ledger-confidence-v1",
    high_minimum=0.85,
    medium_minimum=0.60,
)

_POLICIES = {MODE2_CONFIDENCE_POLICY_V1.policy_id: MODE2_CONFIDENCE_POLICY_V1}


def confidence_policy_by_id(policy_id: Any) -> LedgerConfidencePolicy | None:
    return _POLICIES.get(policy_id) if isinstance(policy_id, str) else None


__all__ = [
    "LedgerConfidencePolicy",
    "MODE2_CONFIDENCE_POLICY_V1",
    "confidence_policy_by_id",
]
