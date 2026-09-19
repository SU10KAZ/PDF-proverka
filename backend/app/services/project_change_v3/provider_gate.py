"""Provider readiness checks without performing inference.

Live V3 inference is admitted ONLY when all of these hold (see
``docs/V3_PROVIDER_ENABLEMENT.md``):

1. ``PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE`` is not ``1``;
2. ``PROJECT_COMPARISON_V3_ALLOW_INFERENCE`` is exactly ``1``;
3. ``PROJECT_COMPARISON_V3_PROVIDER_READY`` is ``1`` OR the offline gateway
   runtime check (``gateway.validate_runtime(require_vision=True, deep=False)``:
   codex CLI present, structured output, sandbox, image input, isolation
   features off) reports ``ok``.

The engine itself is selected by ``PROJECT_COMPARISON_ENGINE`` (default
``v3``).  No other switch exists.  The runtime check only asks the CLI for its
own ``--version``/``--help``/``features list`` — zero provider requests.
"""
from __future__ import annotations

import os
from typing import Any


def _gateway_runtime() -> tuple[bool, str]:
    try:
        from backend.app.services.stage_comparison.ai import gateway

        report = gateway.validate_runtime(require_vision=True, deep=False)
    except Exception as exc:  # noqa: BLE001 — readiness is reported, not raised
        return False, f"validate_runtime_error:{type(exc).__name__}"
    if not isinstance(report, dict):
        return False, "checked_non_dict"
    ok = bool(report.get("ok"))
    problems = report.get("problems") or []
    return ok, "ok" if ok else "; ".join(str(p) for p in problems)[:500] or "not_ok"


def check_provider_readiness() -> dict[str, Any]:
    """Inspect env / offline CLI capabilities only — never call a model."""
    force = os.environ.get("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "0").strip() == "1"
    allow = os.environ.get("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0").strip() == "1"
    provider_flag = os.environ.get("PROJECT_COMPARISON_V3_PROVIDER_READY", "0").strip() == "1"
    base = {
        "allow_inference": allow,
        "force_unavailable": force,
        "provider_flag": provider_flag,
        "checked_without_inference": True,
        "model_calls": 0,
    }
    if force:
        return {**base, "available": False, "reason": "deploy_force_unavailable",
                "gateway_ok": False, "gateway_detail": "not_checked"}
    if not allow:
        return {**base, "available": False, "reason": "inference_kill_switch",
                "gateway_ok": False, "gateway_detail": "not_checked"}
    gateway_ok, gateway_detail = (True, "skipped_provider_flag") if provider_flag else _gateway_runtime()
    available = provider_flag or gateway_ok
    return {
        **base,
        "available": available,
        "reason": "provider_ready" if available else "provider_not_ready",
        "gateway_ok": gateway_ok,
        "gateway_detail": gateway_detail,
    }
