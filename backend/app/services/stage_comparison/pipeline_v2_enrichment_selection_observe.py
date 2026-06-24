# -*- coding: utf-8 -*-
"""Pipeline V2 — enrichment-selection observe, ВЫРОВНЕННЫЙ под РЕАЛЬНЫЙ production path.

Главный источник истины — РЕАЛЬНЫЙ production selection
(``build_graphic_vision_enrichment_plan``, entity_aware + graphic reports +
enrichment mode), а не упрощённая gate-only выборка. Это важно: реальная выборка
исключает часть пар как ``mismatch_likely`` ДО controlled-state хука, поэтому
controlled state может быть в реальном пути РЕДУНДАНТЕН (пара уже исключена), хотя
gate-only показывает «−2».

Отчёт разделяет два уровня:

* ``real_path`` — основной (для UI/summary): default/state_on/excluded_by_state +
  ``mismatch_excluded_before_state`` + ``controlled_state_effective``;
* ``gate_only_diagnostic`` — вспомогательная диагностика (gate-only выборка без
  graphic reports), помечена как diagnostic;
* ``redundant_state_matches`` — controlled-state пары, уже исключённые раньше
  (``already_excluded_by``, напр. ``mismatch_likely``) → ``redundant_safety_net``.

Observe-only инварианты: НЕ вызывает модели (Qwen=0), не создаёт jobs, не рендерит
кропы (``render_crops=False``), не пишет prompts (``write_prompts=False``), не
ходит в сеть; НЕ меняет state и protected reports; пишет ТОЛЬКО свой отчёт и только
при ``write=True``.

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
)

OBSERVE_VERSION = 2
OBSERVE_KIND = "stage_comparison_pipeline_v2_enrichment_selection_observe"
OBSERVE_FILENAME = "controlled_enforce_enrichment_selection_observe_report.json"
VISUAL_GATE_FILENAME = "visual_equivalence_gate_report.json"
_REMAINING_SAMPLE_LIMIT = 12

# Опции реального enrichment-selection пути (observe-safe: ничего не рендерим/пишем)
_REAL_BASE_OPTIONS = {
    "candidate_selection": "entity_aware", "selection_mode": "enrichment",
    "enabled": False, "write_prompts": False, "render_crops": False,
    "max_items": 100000,
}


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


def _state_pairs(state: Any) -> list[dict]:
    """[{left_block_id,right_block_id,transition_id,left/right_entity_label,run_id}] active."""
    pairs: list[dict] = []
    run_id = state.get("run_id") if isinstance(state, dict) else None
    for ex in _active_exclusions(state):
        tid = ex.get("transition_id")
        lL, rL = ex.get("left_entity_label"), ex.get("right_entity_label")
        ex_run = ex.get("run_id") or run_id
        for lb, rb in zip(ex.get("left_block_ids") or [], ex.get("right_block_ids") or []):
            if lb and rb:
                pairs.append({"left_block_id": lb, "right_block_id": rb,
                              "transition_id": tid, "left_entity_label": lL,
                              "right_entity_label": rL, "run_id": ex_run})
    return pairs


def _keys(items: Any) -> list[tuple]:
    out = []
    for it in items or []:
        if isinstance(it, dict):
            out.append((it.get("left_block_id"), it.get("right_block_id")))
    return out


def _resolve_fns(plan_fn: Optional[Callable], select_fn: Optional[Callable]):
    err = None
    if plan_fn is None or select_fn is None:
        try:
            from backend.app.services.stage_comparison.pipeline_v2_graphic_vision_enrichment import (  # noqa: E501
                build_graphic_vision_enrichment_plan, select_vision_candidates_v2)
            plan_fn = plan_fn or build_graphic_vision_enrichment_plan
            select_fn = select_fn or select_vision_candidates_v2
        except Exception as exc:  # noqa: BLE001
            err = f"graphic vision fns unavailable: {type(exc).__name__}: {exc}"
    return plan_fn, select_fn, err


def build_enrichment_selection_observe(
        pipeline_v2_dir: "str | Path", *,
        session_id: str, pair_id: Optional[str] = None,
        plan_fn: Optional[Callable] = None,
        select_fn: Optional[Callable] = None) -> dict:
    """Построить выровненный под реальный production path observe-report (read-only)."""
    d = Path(pipeline_v2_dir)
    state = _safe_load(d / STATE_FILENAME)
    state_pairs = _state_pairs(state)
    state_keyset = {(p["left_block_id"], p["right_block_id"]) for p in state_pairs}
    warnings: list[str] = []

    plan_fn, select_fn, fn_err = _resolve_fns(plan_fn, select_fn)
    gate = _safe_load(d / VISUAL_GATE_FILENAME)
    left_model = _safe_load(d / "left_normalized_document_model.json")
    right_model = _safe_load(d / "right_normalized_document_model.json")
    lg = _safe_load(d / "left_graphic_descriptor_report.json")
    rg = _safe_load(d / "right_graphic_descriptor_report.json")
    matched = _safe_load(d / "graphic_descriptor_matched_report.json")
    overrides = _safe_load(d / "entity_mapping_overrides.json")

    real_path: dict[str, Any] = {
        "available": False, "default_candidates_total": None,
        "state_on_candidates_total": None, "excluded_by_state": None,
        "mismatch_excluded_before_state": None, "controlled_state_effective": None,
        "removed_pairs": [],
    }
    gate_only: dict[str, Any] = {
        "available": False, "default_candidates_total": None,
        "state_on_candidates_total": None, "excluded_by_state": None,
        "controlled_state_would_exclude_if_candidate_reached_hook": None,
        "removed_pairs": [],
    }
    classified_kind: dict[tuple, str] = {}
    remaining_sample: list[dict] = []

    if fn_err:
        warnings.append(fn_err)
    elif gate is None:
        warnings.append("visual_equivalence_gate_report.json unavailable")
    else:
        # ── REAL production path (build_graphic_vision_enrichment_plan) ──
        try:
            def _plan(use_state):
                o = dict(_REAL_BASE_OPTIONS)
                o["use_controlled_enforce_state"] = bool(use_state)
                if use_state:
                    o["controlled_enforce_state"] = state
                return plan_fn(left_model, right_model, gate,
                               left_graphic_report=lg, right_graphic_report=rg,
                               graphic_matched_report=matched,
                               overrides_report=overrides, options=o)
            p_off = _plan(False)
            p_on = _plan(True)
            off_keys = _keys(p_off.get("items"))
            on_keys = _keys(p_on.get("items"))
            removed = sorted(set(off_keys) - set(on_keys))
            stats_off = p_off.get("stats") or {}
            real_path.update({
                "available": True,
                "default_candidates_total": len(off_keys),
                "state_on_candidates_total": len(on_keys),
                "excluded_by_state": len(removed),
                "mismatch_excluded_before_state": int(stats_off.get("mismatch_excluded", 0) or 0),
                "controlled_state_effective": len(removed) > 0,
                "removed_pairs": [f"{a}__{b}" for a, b in removed],
            })
            for it in (p_on.get("items") or [])[:_REMAINING_SAMPLE_LIMIT]:
                if isinstance(it, dict):
                    remaining_sample.append({
                        "left_block_id": it.get("left_block_id"),
                        "right_block_id": it.get("right_block_id"),
                        "candidate_kind": it.get("candidate_kind")})
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"real path skipped: {type(exc).__name__}: {exc}")

        # ── GATE-only diagnostic (select_vision_candidates_v2, без graphic reports) ──
        try:
            g_off, _s1, _w1 = select_fn(gate, options={"max_items": 100000})
            g_on, _s2, _w2 = select_fn(gate, options={
                "max_items": 100000, "use_controlled_enforce_state": True,
                "controlled_enforce_state": state})
            go, gn = _keys(g_off), _keys(g_on)
            gremoved = sorted(set(go) - set(gn))
            gate_only.update({
                "available": True,
                "default_candidates_total": len(go),
                "state_on_candidates_total": len(gn),
                "excluded_by_state": len(gremoved),
                "controlled_state_would_exclude_if_candidate_reached_hook": len(gremoved) > 0,
                "removed_pairs": [f"{a}__{b}" for a, b in gremoved],
            })
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"gate-only diagnostic skipped: {type(exc).__name__}: {exc}")

        # ── permissive classification (exclude_mismatch_likely=False) для already_excluded_by ──
        try:
            cl, _s3, _w3 = select_fn(gate, left_graphic_report=lg, right_graphic_report=rg,
                                     graphic_matched_report=matched, overrides_report=overrides,
                                     options={"candidate_selection": "entity_aware",
                                              "selection_mode": "enrichment", "max_items": 100000,
                                              "exclude_mismatch_likely": False,
                                              "include_exclude_from_vision": True})
            for c in cl or []:
                if isinstance(c, dict):
                    classified_kind[(c.get("left_block_id"), c.get("right_block_id"))] = \
                        c.get("candidate_kind")
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"classification skipped: {type(exc).__name__}: {exc}")

    # ── redundant_state_matches: state-пары, не убранные хуком в real path ──
    # (real excluded_by_state считает фактические removed; всё, что хук не убрал в
    #  реальном пути, но помечено в state — redundant_safety_net с already_excluded_by).
    redundant: list[dict] = []
    effective_pairs: list[dict] = []
    real_removed_set = set()
    if real_path["available"]:
        real_removed_set = {tuple(k.split("__", 1)) for k in real_path["removed_pairs"]}
    for p in state_pairs:
        key = (p["left_block_id"], p["right_block_id"])
        entry = {"left_block_id": p["left_block_id"], "right_block_id": p["right_block_id"],
                 "transition_id": p["transition_id"], "state_active": True}
        if key in real_removed_set:
            entry["controlled_state_effect"] = "effective_in_real_path"
            effective_pairs.append(entry)
        else:
            kind = classified_kind.get(key)
            entry["already_excluded_by"] = kind or "upstream_filter_or_not_candidate"
            entry["controlled_state_effect"] = "redundant_safety_net"
            redundant.append(entry)

    summary = {
        "real_default_candidates_total": real_path["default_candidates_total"],
        "real_state_on_candidates_total": real_path["state_on_candidates_total"],
        "real_excluded_by_state": real_path["excluded_by_state"],
        "gate_only_excluded_by_state": gate_only["excluded_by_state"],
        "mismatch_excluded_before_state": real_path["mismatch_excluded_before_state"],
        "redundant_state_pairs": len(redundant),
        "effective_state_pairs": len(effective_pairs),
        "qwen_calls": 0,
        "runtime_modified": False,
        "protected_reports_modified": False,
    }

    return {
        "version": OBSERVE_VERSION,
        "kind": OBSERVE_KIND,
        "status": "ok",
        "session_id": session_id,
        "pair_id": pair_id,
        "created_at": _now_iso(),
        "state_available": bool(state_pairs),
        "controlled_enforce_run_id": state.get("run_id") if isinstance(state, dict) else None,
        "summary": summary,
        "real_path": real_path,
        "gate_only_diagnostic": gate_only,
        "redundant_state_matches": redundant,
        "effective_state_matches": effective_pairs,
        "remaining_candidates_sample": remaining_sample,
        "invariants": {
            "qwen_not_called": True,
            "runtime_not_modified_by_selection": True,
            "state_not_modified": True,
            "protected_reports_unchanged": True,
        },
        "warnings": warnings,
    }


def write_enrichment_selection_observe(pipeline_v2_dir: "str | Path", report: dict) -> str:
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
        plan_fn: Optional[Callable] = None,
        select_fn: Optional[Callable] = None) -> tuple[dict, Optional[str]]:
    """Построить (и при ``write=True`` записать) выровненный observe-report."""
    report = build_enrichment_selection_observe(
        pipeline_v2_dir, session_id=session_id, pair_id=pair_id,
        plan_fn=plan_fn, select_fn=select_fn)
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
