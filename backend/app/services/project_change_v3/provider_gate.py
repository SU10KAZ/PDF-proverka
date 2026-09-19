"""Provider readiness without inference.

Inspects environment / flags only. During production promotion deploy this
intentionally reports unavailable so zero model calls occur.
"""
from __future__ import annotations

import os
from typing import Any


def check_provider_readiness() -> dict[str, Any]:
    """Return readiness without contacting any model provider."""
    allow = os.environ.get("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0").strip()
    # Deploy posture: never claim ready unless explicit opt-in AND provider
    # flags are present. Even then we still do not call models here.
    provider_flag = os.environ.get("PROJECT_COMPARISON_V3_PROVIDER_READY", "0").strip()
    available = allow == "1" and provider_flag == "1"
    reason = (
        "provider_ready_flag_set"
        if available
        else "deploy_gate_unavailable_no_inference"
    )
    return {
        "available": False if os.environ.get("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1").strip() != "0" else available,
        "reason": reason if os.environ.get("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1").strip() == "0" else "deploy_force_unavailable",
        "allow_inference": allow == "1",
        "checked_without_inference": True,
        "model_calls": 0,
    }
