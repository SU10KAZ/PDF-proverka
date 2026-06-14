# -*- coding: utf-8 -*-
"""Pipeline V2 — formal ENRICHMENT-SELECTION observe plan под controlled state.

Формальный observe-отчёт: какой enrichment-selection список ушёл бы дальше,
какие пары исключены active ``controlled_enforce_state.json`` (scope
``exclude_from_enrichment``), какие остались, и что НЕ попадёт в будущий
Qwen/enrichment — **без запуска Qwen и без пересчёта pipeline**.

Использует тот же реальный candidate-selection (``select_vision_candidates_v2``
по ``visual_equivalence_gate_report.json``), что и production enrichment, и тот
же чистый хук ``filter_candidates_by_controlled_enforce_state``. select —
детерминированная выборка по visual gate, Qwen в ней НЕ вызывается.

Жёсткие инварианты:

* НЕ вызывает модели (Qwen/Gemma/Opus/Claude), не создаёт jobs, не рендерит
  новые crops, не ходит в сеть/subprocess;
* НЕ меняет ``controlled_enforce_state.json`` и НЕ трогает protected reports;
* пишет ТОЛЬКО собственный отчёт и только при ``write=True``.

Связано: [[stage_comparison_pipeline_v2_controlled_enforce_executor]].
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_executor import (
    STATE_KIND,
    STATE_FILENAME,
    filter_candidates_by_controlled_enforce_state,
)

OBSERVE_VERSION = 1
OBSERVE_KIND = "stage_comparison_pipeline_v2_enrichment_selection_observe"
OBSERVE_FILENAME = "controlled_enforce_enrichment_selection_observe_report.json"
VISUAL_GATE_FILENAME = "visual_equivalence_gate_report.json"

_REMAINING_SAMPLE_LIMIT = 12


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_load(path: Path) -> Optional[Any]:
    try:
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — fail-soft
        return None


def _active_exclusions(state: Any) -> list[dict]:
    out: list[dict] = []
    if not isinstance(state, dict) or state.get("kind") != STATE_KIND:
        return out
    for ex in state.get("applied_exclusions") or []:
        if isinstance(ex, dict) and ex.get("active") is True:
            out.append(ex)
    return out


def _cand_key(c: Any) -> Optional[tuple]:
    if not isinstance(c, dict):
        return None
    return (c.get("left_block_id"), c.get("right_block_id"))


def _excluded_entries(state: Any, default_keys: set, removed_keys: set) -> list[dict]:
    """Авторитетный список исключённых пар из active state (+ provenance из пула)."""
    out: list[dict] = []
    run_id = state.get("run_id") if isinstance(state, dict) else None
    for ex in _active_exclusions(state):
        tid = ex.get("transition_id")
        lL = ex.get("left_entity_label")
        rL = ex.get("right_entity_label")
        scope = dict(ex.get("scope") or {})
        ex_run = ex.get("run_id") or run_id
        lbs = ex.get("left_block_ids") or []
        rbs = ex.get("right_block_ids") or []
        for lb, rb in zip(lbs, rbs):
            if not (lb and rb):
                continue
            out.append({
                "left_block_id": lb, "right_block_id": rb,
                "left_entity_label": lL, "right_entity_label": rL,
                "transition_id": tid,
                "controlled_enforce_run_id": ex_run,
                "reason": "controlled_enforce_state_active",
                "scope": scope,
                "in_default_selection": (lb, rb) in default_keys,
                "removed_from_selection": (lb, rb) in removed_keys,
            })
    return out


def _resolve_select_fn(select_fn: Optional[Callable]):
    if select_fn is not None:
        return select_fn, None
    try:
        from backend.app.services.stage_comparison.pipeline_v2_graphic_vision_enrichment import (  # noqa: E501
            select_vision_candidates_v2)
        return select_vision_candidates_v2, None
    except Exception as exc:  # noqa: BLE001
        return None, f"select fn unavailable: {type(exc).__name__}: {exc}"


def build_enrichment_selection_observe(
        pipeline_v2_dir: "str | Path", *,
        session_id: str, pair_id: Optional[str] = None,
        select_fn: Optional[Callable] = None) -> dict:
    """Построить enrichment-selection observe report (НИЧЕГО не пишет). read-only."""
    d = Path(pipeline_v2_dir)
    state = _safe_load(d / STATE_FILENAME)
    active = _active_exclusions(state)
    warnings: list[str] = []

    select, sel_err = _resolve_select_fn(select_fn)
    gate = _safe_load(d / VISUAL_GATE_FILENAME)

    default_total: Optional[int] = None
    state_on_total: Optional[int] = None
    default_keys: set = set()
    removed_keys: set = set()
    remaining_sample: list[dict] = []
    selection_source = "unavailable"

    if sel_err:
        warnings.append(sel_err)
    elif gate is None:
        warnings.append("visual_equivalence_gate_report.json unavailable")
    else:
        try:
            sel_def, _stats_def, _w1 = select(gate, options={"max_items": 100000})
            sel_on, stats_on, _w2 = select(gate, options={
                "max_items": 100000,
                "use_controlled_enforce_state": True,
                "controlled_enforce_state": state})
            default_total = len(sel_def)
            state_on_total = len(sel_on)
            default_keys = {k for k in (_cand_key(c) for c in sel_def) if k}
            on_keys = {k for k in (_cand_key(c) for c in sel_on) if k}
            removed_keys = default_keys - on_keys
            selection_source = "real_candidate_pool"
            for c in sel_on[:_REMAINING_SAMPLE_LIMIT]:
                if isinstance(c, dict):
                    remaining_sample.append({
                        "left_block_id": c.get("left_block_id"),
                        "right_block_id": c.get("right_block_id"),
                        "candidate_kind": c.get("candidate_kind"),
                        "left_entity_label": c.get("left_entity_label"),
                        "right_entity_label": c.get("right_entity_label"),
                    })
        except Exception as exc:  # noqa: BLE001
            warnings.append(
                f"real-pool selection skipped: {type(exc).__name__}: {exc}")

    excluded = _excluded_entries(state, default_keys, removed_keys)
    tids = sorted({e["transition_id"] for e in excluded if e.get("transition_id")})

    # Если реальный пул недоступен — отражаем хотя бы авторитетное число
    # исключений из state (selection counts остаются null + warning).
    return {
        "version": OBSERVE_VERSION,
        "kind": OBSERVE_KIND,
        "status": "ok",
        "session_id": session_id,
        "pair_id": pair_id,
        "created_at": _now_iso(),
        "state_available": bool(active),
        "controlled_enforce_run_id": state.get("run_id") if isinstance(state, dict) else None,
        "selection_source": selection_source,
        "summary": {
            "default_candidates_total": default_total,
            "state_on_candidates_total": state_on_total,
            "excluded_by_state": len(excluded),
            "excluded_logical_transitions": len(tids),
            "qwen_calls": 0,
            "runtime_modified": False,
            "protected_reports_modified": False,
        },
        "excluded_by_state": excluded,
        "remaining_candidates_sample": remaining_sample,
        "invariants": {
            "qwen_not_called": True,
            "runtime_not_modified_by_selection": True,
            "state_not_modified": True,
            "protected_reports_unchanged": True,
        },
        "warnings": warnings,
    }


def write_enrichment_selection_observe(
        pipeline_v2_dir: "str | Path", report: dict) -> str:
    """Атомарно записать отчёт. ЕДИНСТВЕННАЯ возможная мутация модуля."""
    d = Path(pipeline_v2_dir)
    path = d / OBSERVE_FILENAME
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return str(path)


def run_enrichment_selection_observe(
        pipeline_v2_dir: "str | Path", *,
        session_id: str, pair_id: Optional[str] = None,
        write: bool = False,
        select_fn: Optional[Callable] = None) -> tuple[dict, Optional[str]]:
    """Построить (и при ``write=True`` записать) enrichment-selection observe report."""
    report = build_enrichment_selection_observe(
        pipeline_v2_dir, session_id=session_id, pair_id=pair_id, select_fn=select_fn)
    path = None
    if write:
        path = write_enrichment_selection_observe(pipeline_v2_dir, report)
    return report, path


__all__ = [
    "OBSERVE_KIND", "OBSERVE_FILENAME", "OBSERVE_VERSION",
    "build_enrichment_selection_observe",
    "write_enrichment_selection_observe",
    "run_enrichment_selection_observe",
]
