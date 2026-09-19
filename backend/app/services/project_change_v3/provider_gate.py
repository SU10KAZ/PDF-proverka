"""Provider readiness checks without performing inference."""
from __future__ import annotations

import os
from typing import Any


def check_provider_readiness() -> dict[str, Any]:
    """Inspect env / gateway config only — never call a model.

    FORCE_UNAVAILABLE=1 reports unavailable (safe zero-quota deploy posture).
    Fake/test provider injection bypasses this gate inside the engine.
    """
    force = os.environ.get("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "0").strip()
    allow = os.environ.get("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0").strip() == "1"
    provider_flag = os.environ.get("PROJECT_COMPARISON_V3_PROVIDER_READY", "0").strip() == "1"

    gateway_ok = False
    gateway_detail = "not_checked"
    try:
        from backend.app.services.stage_comparison.ai import gateway

        report = gateway.validate_runtime()
        if isinstance(report, dict):
            gateway_ok = bool(
                report.get("ok")
                or report.get("codex_ok")
                or report.get("ready")
                or report.get("codex", {}).get("ok")
            )
            gateway_detail = str(
                report.get("status") or report.get("detail") or "checked"
            )
        else:
            gateway_detail = "checked_non_dict"
    except Exception as exc:  # pragma: no cover
        gateway_detail = f"validate_runtime_error:{type(exc).__name__}"

    if force == "1":
        return {
            "available": False,
            "reason": "deploy_force_unavailable",
            "allow_inference": allow,
            "provider_flag": provider_flag,
            "gateway_ok": gateway_ok,
            "gateway_detail": gateway_detail,
            "checked_without_inference": True,
            "model_calls": 0,
        }

    available = allow and (provider_flag or gateway_ok)
    return {
        "available": available,
        "reason": (
            "provider_ready"
            if available
            else ("inference_kill_switch" if not allow else "provider_not_ready")
        ),
        "allow_inference": allow,
        "provider_flag": provider_flag,
        "gateway_ok": gateway_ok,
        "gateway_detail": gateway_detail,
        "checked_without_inference": True,
        "model_calls": 0,
    }
