"""V3 production comparison entry — never falls back to legacy."""
from __future__ import annotations

import os
from typing import Any

from .contracts import ENGINE_NAME, ENGINE_VERSION
from .provenance import build_provenance
from .provider_gate import check_provider_readiness

UNAVAILABLE_RU = (
    "Сравнение проектов движком V3 недоступно: модель gpt-6-astra "
    "не готова или квота исчерпана. Результат не сгенерирован. "
    "Legacy (subject-first) не запускался."
)
KILL_SWITCH_RU = (
    "Движок сравнения проектов V3 выбран, но живой inference "
    "запрещён (PROJECT_COMPARISON_V3_ALLOW_INFERENCE!=1). "
    "Legacy не запускался."
)


def _write_v3_state(
    session_id: str,
    pair_id: str,
    *,
    status: str,
    message: str,
    reason_code: str,
    provenance: dict[str, Any],
) -> dict[str, Any]:
    """Persist a closed V3 state via production_store when available."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    state: dict[str, Any] = {
        "kind": "stage_comparison_production_state",
        "schema_version": 1,
        "version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "status": status,
        "progress": 100,
        "message": message,
        "reason_code": reason_code,
        "engine": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "provenance": provenance,
        "technical_provenance": provenance,
        "started_at": now,
        "last_activity_at": now,
        "failed_at": now if status == "FAILED" else None,
        "current_stage": None,
        "current_substage": None,
        "model_calls": 0,
        "legacy_invoked": False,
    }
    try:
        from backend.app.services.stage_comparison import production_store

        production_store.save_artifact(session_id, pair_id, "state", state)
        # Also stash a thin result artifact with provenance for audits.
        production_store.save_artifact(
            session_id,
            pair_id,
            "project_change_v3_result",
            {
                "status": status,
                "message": message,
                "reason_code": reason_code,
                "provenance": provenance,
                "projectchanges": [],
                "model_calls": 0,
            },
        )
    except Exception:
        # Store may be unavailable in unit contexts; still return state.
        pass
    return state


def run_v3_production_comparison(
    session_id: str,
    pair_id: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Entry for PROJECT_COMPARISON_ENGINE=v3.

    Caller handles routing. This function never invokes legacy comparison.
    """
    _ = kwargs  # reserved for future V3 inputs
    engine_env = os.environ.get("PROJECT_COMPARISON_ENGINE", "v3").strip().lower()
    provenance = build_provenance(
        source_prep_version=str(kwargs.get("source_prep_version") or "unspecified")
    )
    if engine_env not in {"v3", "projectchange_v3", "project_change_v3"}:
        # Defensive: orchestrator should not call us for other engines.
        return _write_v3_state(
            session_id,
            pair_id,
            status="FAILED",
            message=UNAVAILABLE_RU,
            reason_code="engine_mismatch",
            provenance=provenance,
        )

    gate = check_provider_readiness()
    allow = os.environ.get("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0").strip() == "1"

    if not gate.get("available"):
        return _write_v3_state(
            session_id,
            pair_id,
            status="FAILED",
            message=UNAVAILABLE_RU,
            reason_code="v3_provider_unavailable",
            provenance={**provenance, "provider_gate": gate},
        )

    if not allow:
        # Structure reserved for a future V3 run path, but refuse inference.
        return _write_v3_state(
            session_id,
            pair_id,
            status="FAILED",
            message=KILL_SWITCH_RU,
            reason_code="v3_inference_kill_switch",
            provenance={
                **provenance,
                "provider_gate": gate,
                "future_run_structured": True,
            },
        )

    # Even if both flags are set, this promotion task still refuses live
    # inference so deployment cannot accidentally bill model calls.
    return _write_v3_state(
        session_id,
        pair_id,
        status="FAILED",
        message=(
            "Путь V3 подготовлен, но полный inference в этом релизе "
            "ещё не включён. Model calls: 0. Legacy не запускался."
        ),
        reason_code="v3_inference_not_implemented_in_promotion",
        provenance={**provenance, "provider_gate": gate},
    )
