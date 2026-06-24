# -*- coding: utf-8 -*-
"""Pipeline V2 — controlled enforce OBSERVE-mode selection report (read-only).

Строит сравнение enrichment-selection «default OFF» против «controlled enforce
state ON», НЕ запуская Qwen/Opus/jobs и НЕ пересчитывая pipeline. Назначение —
дать оператору видимость того, какие block-pairs активный
``controlled_enforce_state.json`` исключил бы из БУДУЩЕГО enrichment-selection.

Жёсткие инварианты:

* НЕ вызывает модели (Qwen/Gemma/Opus/Claude), не создаёт jobs, не рендерит
  новые crops, не ходит в сеть/subprocess;
* НЕ меняет ``controlled_enforce_state.json`` и НЕ трогает protected reports
  (entity_diff / grounded_evidence / delta_explanation / block_link_preview и
  пр.) — пишет ТОЛЬКО собственный observe-отчёт (и то лишь через ``write=True``);
* selection-hook применяется через ту же чистую функцию
  ``filter_candidates_by_controlled_enforce_state``, что и в production
  selection (``select_vision_candidates_v2``), поэтому observe честно отражает
  реальное поведение хука.

Связано: [[stage_comparison_pipeline_v2_controlled_enforce_executor]].
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Optional

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_executor import (
    STATE_KIND,
    STATE_FILENAME,
    filter_candidates_by_controlled_enforce_state,
)

OBSERVE_VERSION = 1
OBSERVE_KIND = "stage_comparison_pipeline_v2_controlled_enforce_selection_observe"
OBSERVE_FILENAME = "controlled_enforce_selection_observe_report.json"
VISUAL_GATE_FILENAME = "visual_equivalence_gate_report.json"


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


def _state_excluded_pairs(state: Any) -> list[dict]:
    """Список {left_block_id, right_block_id, transition_id, reason} для active exclusions."""
    pairs: list[dict] = []
    for ex in _active_exclusions(state):
        tid = ex.get("transition_id")
        lbs = ex.get("left_block_ids") or []
        rbs = ex.get("right_block_ids") or []
        for lb, rb in zip(lbs, rbs):
            if lb and rb:
                pairs.append({
                    "left_block_id": lb, "right_block_id": rb,
                    "transition_id": tid,
                    "reason": "controlled_enforce_state_active",
                })
    return pairs


def _real_pool_selection(d: Path, state: Any,
                         select_fn: Optional[Callable]) -> dict:
    """Best-effort selection на РЕАЛЬНОМ candidate-pool (visual gate report).

    Observe-only: select_vision_candidates_v2 — чистая выборка по visual gate,
    Qwen НЕ вызывается. Возвращает счётчики default/on + сколько state-хук убрал
    из текущего пула. Любой сбой → note, без падения.
    """
    out = {"default_selected": None, "state_on_selected": None,
           "excluded_from_current_pool": 0, "available": False, "note": None}
    if select_fn is None:
        try:
            from backend.app.services.stage_comparison.pipeline_v2_graphic_vision_enrichment import (  # noqa: E501
                select_vision_candidates_v2 as select_fn)
        except Exception as exc:  # noqa: BLE001
            out["note"] = f"select fn unavailable: {type(exc).__name__}: {exc}"
            return out
    gate = _safe_load(d / VISUAL_GATE_FILENAME)
    if gate is None:
        out["note"] = "visual_equivalence_gate_report.json unavailable"
        return out
    try:
        sel_def, _stats_def, _ = select_fn(gate, options={"max_items": 100000})
        sel_on, stats_on, _ = select_fn(gate, options={
            "max_items": 100000,
            "use_controlled_enforce_state": True,
            "controlled_enforce_state": state})
        out["default_selected"] = len(sel_def)
        out["state_on_selected"] = len(sel_on)
        out["excluded_from_current_pool"] = int(
            (stats_on or {}).get("controlled_enforce_excluded", 0) or 0)
        out["available"] = True
    except Exception as exc:  # noqa: BLE001
        out["note"] = f"real-pool selection skipped: {type(exc).__name__}: {exc}"
    return out


def build_controlled_enforce_selection_observe(
        pipeline_v2_dir: "str | Path", *,
        session_id: str, pair_id: Optional[str] = None,
        select_fn: Optional[Callable] = None) -> dict:
    """Построить observe-report (НИЧЕГО не пишет). read-only.

    summary.default_selected / state_on_selected — из РЕАЛЬНОГО candidate-pool,
    если доступен; иначе из verification-pool. summary.excluded_by_state —
    авторитетно из active state (что state исключает из будущего selection).
    verification-блок детерминированно доказывает, что hook убирает РОВНО
    active block-pairs (когда они в пуле).
    """
    d = Path(pipeline_v2_dir)
    state = _safe_load(d / STATE_FILENAME)
    excluded_pairs = _state_excluded_pairs(state)
    active_transitions = sorted(
        {p["transition_id"] for p in excluded_pairs if p.get("transition_id")})

    # ── real-pool selection (observe-only, без Qwen) ──
    real = _real_pool_selection(d, state, select_fn)

    # ── детерминированная верификация hook'а на гарантированном пуле ──
    state_cands = [{"left_block_id": p["left_block_id"],
                    "right_block_id": p["right_block_id"]} for p in excluded_pairs]
    controls = [{"left_block_id": f"OBSERVE-CTRL-L{i}",
                 "right_block_id": f"OBSERVE-CTRL-R{i}"} for i in range(1, 4)]
    verify_pool = state_cands + controls
    v_off, off_removed = filter_candidates_by_controlled_enforce_state(
        list(verify_pool), state, enabled=False)
    v_on, on_removed = filter_candidates_by_controlled_enforce_state(
        list(verify_pool), state, enabled=True)

    # summary selection counts: реальный пул приоритетнее, иначе verification
    if real["available"]:
        default_selected = real["default_selected"]
        state_on_selected = real["state_on_selected"]
        selection_source = "real_candidate_pool"
    else:
        default_selected = len(v_off)
        state_on_selected = len(v_on)
        selection_source = "verification_pool"

    excluded_by_state_count = len(excluded_pairs)

    return {
        "version": OBSERVE_VERSION,
        "kind": OBSERVE_KIND,
        "status": "ok",
        "session_id": session_id,
        "pair_id": pair_id,
        "state_available": bool(_active_exclusions(state)),
        "summary": {
            "default_selected": default_selected,
            "state_on_selected": state_on_selected,
            "excluded_by_state": excluded_by_state_count,
            "excluded_logical_transitions": len(active_transitions),
            "qwen_calls": 0,
            "would_modify_runtime": False,
            "selection_source": selection_source,
        },
        "excluded_by_state": excluded_pairs,
        "active_transitions": active_transitions,
        "real_pool_selection": real,
        "verification_pool": {
            "pool_size": len(verify_pool),
            "default_off_unchanged": v_off == verify_pool,
            "state_on_kept": len(v_on),
            "state_on_removed_count": len(on_removed),
            "state_on_removed_keys": sorted(on_removed),
        },
        "invariants": {
            "qwen_not_called": True,
            "runtime_not_modified_by_selection": True,
            "protected_reports_unchanged": True,
        },
    }


def write_controlled_enforce_selection_observe(
        pipeline_v2_dir: "str | Path", report: dict) -> str:
    """Атомарно записать observe-отчёт. ЕДИНСТВЕННАЯ возможная мутация модуля."""
    d = Path(pipeline_v2_dir)
    path = d / OBSERVE_FILENAME
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return str(path)


def run_controlled_enforce_selection_observe(
        pipeline_v2_dir: "str | Path", *,
        session_id: str, pair_id: Optional[str] = None,
        write: bool = False,
        select_fn: Optional[Callable] = None) -> tuple[dict, Optional[str]]:
    """Построить (и при ``write=True`` записать) observe-отчёт.

    ``write=False`` (default) → ничего не пишет. Возвращает ``(report, path|None)``.
    """
    report = build_controlled_enforce_selection_observe(
        pipeline_v2_dir, session_id=session_id, pair_id=pair_id, select_fn=select_fn)
    path = None
    if write:
        path = write_controlled_enforce_selection_observe(pipeline_v2_dir, report)
    return report, path


__all__ = [
    "OBSERVE_KIND", "OBSERVE_FILENAME", "OBSERVE_VERSION",
    "build_controlled_enforce_selection_observe",
    "write_controlled_enforce_selection_observe",
    "run_controlled_enforce_selection_observe",
]
