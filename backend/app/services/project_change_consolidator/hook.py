"""Production hook: optionally queue a shadow consolidation after a completed V3 run.

OFF by default (``PROJECTCHANGE_CONSOLIDATOR_SHADOW`` must be exactly ``1``).
Even when ON it never calls a model unless an explicit provider configuration
is given (``PROJECTCHANGE_CONSOLIDATOR_PROVIDER=claude_code_cli:<model>:<effort>``)
and the V3 inference gate is open.  There is no default provider and no
fallback.  The hook runs after the pair lock is released, in the background;
whatever happens in the shadow never changes the V3 state it was given, and
nothing reads the shadow result automatically.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any

log = logging.getLogger(__name__)

FLAG = "PROJECTCHANGE_CONSOLIDATOR_SHADOW"
PROVIDER_ENV = "PROJECTCHANGE_CONSOLIDATOR_PROVIDER"
MAX_CALLS_ENV = "PROJECTCHANGE_CONSOLIDATOR_MAX_CALLS"
DEFAULT_MAX_CALLS = 12


def enabled() -> bool:
    return os.environ.get(FLAG, "0").strip() == "1"


def provider_from_spec(spec: str) -> Any:
    parts = [p.strip() for p in (spec or "").split(":")]
    if len(parts) != 3 or parts[0] != "claude_code_cli" or not parts[1] or not parts[2]:
        raise ValueError(f"unsupported provider configuration {spec!r}")
    from backend.app.services.project_change_v3.provider import ClaudeOpusProvider

    return ClaudeOpusProvider(model=parts[1], reasoning=parts[2])


def _max_calls() -> int:
    try:
        return max(0, int(os.environ.get(MAX_CALLS_ENV, str(DEFAULT_MAX_CALLS))))
    except ValueError:
        return DEFAULT_MAX_CALLS


def after_v3_run(session_id: str, pair_id: str, state: dict[str, Any], *, start=True) -> dict[str, Any]:
    """Decide whether to queue a shadow run; never raises and never changes ``state``."""
    try:
        if not enabled():
            return {"status": "SKIPPED_FLAG_OFF"}
        if not isinstance(state, dict) or state.get("reason_code") != "v3_completed" or state.get(
                "status") not in ("COMPLETED", "REVIEW") or not state.get("run_id"):
            return {"status": "SKIPPED_SOURCE_NOT_COMPLETED"}
        spec = os.environ.get(PROVIDER_ENV, "").strip()
        if not spec:
            return {"status": "SKIPPED_NO_PROVIDER_CONFIG"}
        try:
            provider = provider_from_spec(spec)
        except ValueError as exc:
            return {"status": "SKIPPED_BAD_PROVIDER_CONFIG", "detail": str(exc)}
        from backend.app.services.project_change_v3 import provider_gate

        gate = provider_gate.check_provider_readiness()
        if not gate.get("available"):
            return {"status": "SKIPPED_INFERENCE_GATE", "detail": gate.get("reason")}
        run_id = str(state["run_id"])
        if not start:
            return {"status": "QUEUED", "run_id": run_id}
        threading.Thread(target=_run, args=(session_id, pair_id, run_id, provider), daemon=True,
                         name=f"consolidator-shadow-{pair_id}").start()
        return {"status": "QUEUED", "run_id": run_id}
    except Exception as exc:  # noqa: BLE001 — the shadow must never break the V3 run
        log.exception("consolidator shadow hook failed")
        return {"status": "HOOK_ERROR", "detail": f"{type(exc).__name__}: {exc}"}


def _run(session_id: str, pair_id: str, run_id: str, provider: Any) -> None:
    try:
        from backend.app.services.project_change_v3 import run_storage
        from backend.app.services.stage_comparison import paths

        from .shadow import run_shadow
        from .source_view import load_completed_run
        from .storage import SHADOW_DIR_NAME

        bundle = load_completed_run(session_id, pair_id, run_id)
        production = paths.production_dir(session_id, pair_id)
        manifest = run_shadow(bundle, provider, production / SHADOW_DIR_NAME, max_calls=_max_calls(),
                              watch_roots=[run_storage.run_dir(session_id, pair_id, run_id),
                                           production / "current_run.json"],
                              flags={FLAG: os.environ.get(FLAG, "")})
        log.info("consolidator shadow %s for %s/%s: %s", manifest.get("consolidator_run_id"), pair_id, run_id,
                 manifest.get("state"))
    except Exception:  # noqa: BLE001
        log.exception("consolidator shadow run failed for %s/%s", pair_id, run_id)
