# -*- coding: utf-8 -*-
"""Pipeline V2 — controlled enforce STATE write-layer (deactivate / rollback).

Безопасная деактивация active controlled_enforce_state (ручной rollback оператора).
Деактивация — НЕ удаление: запись помечается ``active=false`` + audit-метаданные
(``deactivated_at`` / ``deactivated_by`` / ``deactivation_comment``) + запись в
``history``. Физически ничего не удаляется (обратимо).

Жёсткие инварианты:

* пишется ТОЛЬКО ``controlled_enforce_state.json`` (атомарно, temp + os.replace);
* НЕ трогает protected reports / findings / block links / delta / grounded;
* НЕ запускает модели / jobs / сеть / subprocess;
* требует точного ``confirmation == "DEACTIVATE_CONTROLLED_STATE"`` — иначе отказ
  без записи;
* неизвестный/уже неактивный ``run_id`` → отказ без записи.

Связано: [[stage_comparison_pipeline_v2_controlled_enforce_executor]].
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_executor import (
    STATE_KIND,
    STATE_FILENAME,
)

DEACTIVATE_CONFIRMATION = "DEACTIVATE_CONTROLLED_STATE"
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"


class ControlledEnforceStateError(ValueError):
    """Ошибка валидации/деактивации controlled state (без записи)."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── read ────────────────────────────────────────────────────────────────────


def read_controlled_enforce_state(pipeline_v2_dir: "str | Path") -> Optional[dict]:
    """Прочитать controlled_enforce_state.json (read-only, fail-soft → None)."""
    p = Path(pipeline_v2_dir) / STATE_FILENAME
    try:
        if not p.is_file():
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:  # noqa: BLE001 — fail-soft
        return None


def _active_exclusions(state: Any) -> list[dict]:
    if not isinstance(state, dict):
        return []
    return [e for e in (state.get("applied_exclusions") or [])
            if isinstance(e, dict) and e.get("active") is True]


def state_summary(state: Any) -> dict:
    """Сводка active=True исключений (для ответа endpoint)."""
    active = _active_exclusions(state)
    transitions = {e.get("transition_id") for e in active if e.get("transition_id")}
    block_pairs = 0
    for e in active:
        lbs = e.get("left_block_ids") or []
        rbs = e.get("right_block_ids") or []
        block_pairs += min(len(lbs), len(rbs))
    return {
        "active_exclusions": len(active),
        "active_transitions": len(transitions),
        "active_block_pairs": block_pairs,
    }


# ─── validate ────────────────────────────────────────────────────────────────


def validate_deactivate_payload(payload: Any) -> dict:
    """Проверить payload деактивации. Бросает ControlledEnforceStateError при ошибке.

    Возвращает нормализованный ``{run_id, comment, updated_by}``.
    """
    if not isinstance(payload, dict):
        raise ControlledEnforceStateError("payload must be a JSON object")
    confirmation = payload.get("confirmation")
    if confirmation != DEACTIVATE_CONFIRMATION:
        raise ControlledEnforceStateError(
            f"confirmation must equal {DEACTIVATE_CONFIRMATION!r}")
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ControlledEnforceStateError("run_id is required (non-empty string)")
    comment = payload.get("comment")
    comment = comment.strip() if isinstance(comment, str) else ""
    updated_by = payload.get("updated_by")
    updated_by = updated_by.strip() if isinstance(updated_by, str) else "operator"
    return {"run_id": run_id.strip(), "comment": comment,
            "updated_by": updated_by or "operator"}


# ─── deactivate (pure transform) ─────────────────────────────────────────────


def deactivate_controlled_enforce_run(
        state: dict, run_id: str, *,
        comment: str = "", updated_by: str = "operator",
        now_iso: Optional[str] = None) -> tuple[dict, dict]:
    """Деактивировать все active записи с данным run_id (чистая трансформация).

    Возвращает ``(new_state, result)``. Запись НЕ удаляется: ``active=false`` +
    audit-метаданные + запись в ``history``. Бросает ControlledEnforceStateError,
    если нет active-записей с этим run_id.
    """
    if not isinstance(state, dict) or state.get("kind") != STATE_KIND:
        raise ControlledEnforceStateError("not a valid controlled enforce state")
    now = now_iso or _now_iso()
    new_state = json.loads(json.dumps(state))  # deep copy
    exclusions = new_state.get("applied_exclusions") or []
    deactivated = 0
    deactivated_pairs: list[dict] = []
    for ex in exclusions:
        if not isinstance(ex, dict):
            continue
        if ex.get("run_id") == run_id and ex.get("active") is True:
            ex["active"] = False
            ex["deactivated_at"] = now
            ex["deactivated_by"] = updated_by
            ex["deactivation_comment"] = comment
            deactivated += 1
            deactivated_pairs.append({
                "transition_id": ex.get("transition_id"),
                "left_block_ids": list(ex.get("left_block_ids") or []),
                "right_block_ids": list(ex.get("right_block_ids") or []),
            })
    if deactivated == 0:
        raise ControlledEnforceStateError(
            f"no active exclusion found for run_id {run_id!r} "
            f"(already inactive or unknown run_id)")

    # history запись (НЕ удаляем существующую историю)
    history = new_state.get("history")
    if not isinstance(history, list):
        history = []
    history.append({
        "action": "deactivate",
        "run_id": run_id,
        "at": now,
        "by": updated_by,
        "comment": comment,
        "deactivated_count": deactivated,
    })
    new_state["history"] = history
    new_state["updated_at"] = now
    # top-level status: inactive, если не осталось active-исключений
    new_state["status"] = (STATUS_ACTIVE if _active_exclusions(new_state)
                           else STATUS_INACTIVE)

    result = {
        "deactivated": True,
        "run_id": run_id,
        "deactivated_count": deactivated,
        "deactivated_pairs": deactivated_pairs,
        "state_status": new_state["status"],
        "summary": state_summary(new_state),
    }
    return new_state, result


# ─── atomic write ────────────────────────────────────────────────────────────


def write_controlled_enforce_state_atomic(
        pipeline_v2_dir: "str | Path", state: dict) -> str:
    """Атомарно записать controlled_enforce_state.json. ЕДИНСТВЕННАЯ мутация."""
    p = Path(pipeline_v2_dir) / STATE_FILENAME
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, p)
    return str(p)


# ─── orchestration (read → validate → deactivate → write) ────────────────────


def run_deactivate_controlled_enforce_state(
        pipeline_v2_dir: "str | Path", payload: Any, *,
        now_iso: Optional[str] = None,
        write: bool = True) -> dict:
    """Прогнать deactivate: read → validate → deactivate → (опц.) write.

    Возвращает ответ endpoint'а. ``write=False`` — dry (для тестов), ничего не
    пишет. Любая ошибка валидации/поиска run_id → ``status=error`` БЕЗ записи.
    """
    valid = validate_deactivate_payload(payload)  # бросит при неверном confirmation/run_id
    state = read_controlled_enforce_state(pipeline_v2_dir)
    if state is None:
        return {"status": "not_found", "available": False, "deactivated": False,
                "message": "controlled enforce state not found for this pair",
                "warnings": []}
    if state.get("kind") != STATE_KIND:
        return {"status": "error", "available": False, "deactivated": False,
                "message": "controlled enforce state is not valid", "warnings": []}

    new_state, result = deactivate_controlled_enforce_run(
        state, valid["run_id"], comment=valid["comment"],
        updated_by=valid["updated_by"], now_iso=now_iso)

    path = None
    if write:
        path = write_controlled_enforce_state_atomic(pipeline_v2_dir, new_state)

    return {
        "status": "ok",
        "available": True,
        "kind": STATE_KIND,
        "written": bool(write),
        "state_path": path,
        **result,
        "warnings": [],
    }


__all__ = [
    "DEACTIVATE_CONFIRMATION", "STATUS_ACTIVE", "STATUS_INACTIVE",
    "ControlledEnforceStateError",
    "read_controlled_enforce_state",
    "state_summary",
    "validate_deactivate_payload",
    "deactivate_controlled_enforce_run",
    "write_controlled_enforce_state_atomic",
    "run_deactivate_controlled_enforce_state",
]
