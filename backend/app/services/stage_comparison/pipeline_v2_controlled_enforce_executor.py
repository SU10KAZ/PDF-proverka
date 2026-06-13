# -*- coding: utf-8 -*-
"""Pipeline V2 — controlled enforce EXECUTOR v0 (code-only / diagnostics-only).

Executor умеет:

* читать config + reports (skip_readiness / preflight / dry_run / overrides /
  exclusion_preview);
* валидировать config + runtime guards;
* строить **execution plan** (что было бы применено);
* готовить **future controlled_enforce_state** preview (active=false, не пишется);
* снимать **protected-hash sentinel**;
* готовить **rollback plan**.

В v0 (эта задача) executor НИЧЕГО не применяет:

* ``apply=False`` по умолчанию → runtime не меняется, active state не пишется,
  selection по умолчанию не меняется;
* ``apply=True`` в v0 **не реализован** → ``run_controlled_enforce_executor``
  поднимает ``ControlledEnforceNotImplemented`` (real skip — отдельная задача).

Read-only, offline: не вызывает модели/джобы/сеть/subprocess. Единственный
backend-импорт — config-валидатор + KIND/статус-константы preflight/dry-run.

Связано: [[stage_comparison_pipeline_v2_first_controlled_skip_protocol]],
[[stage_comparison_pipeline_v2_controlled_enforce_executor]].
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_config import (
    validate_controlled_enforce_config,
    PROTECTED_REPORTS,
    ROOT_GUARD_OK,
)

# KIND'ы preflight / dry-run (для проверки статусов входов)
_PREFLIGHT_KIND = "stage_comparison_pipeline_v2_controlled_enforce_preflight"
_PREFLIGHT_STATUS_OK = "preflight_ok"
_DRY_RUN_KIND = "stage_comparison_pipeline_v2_controlled_enforce_dry_run"
_DRY_RUN_STATUS_OK = "ok"
_SKIP_READINESS_KIND = "skip_readiness_report_v1"

STATE_VERSION = 1
STATE_KIND = "stage_comparison_pipeline_v2_controlled_enforce_state"
STATE_FILENAME = "controlled_enforce_state.json"

PLAN_VERSION = 1
PLAN_KIND = "stage_comparison_pipeline_v2_controlled_enforce_execution_plan"
PLAN_FILENAME = "controlled_enforce_execution_plan.json"

# Файлы-входы (read-only)
SKIP_READINESS_FILENAME = "skip_readiness_report.json"
PREFLIGHT_FILENAME = "controlled_enforce_preflight_report.json"
DRY_RUN_FILENAME = "controlled_enforce_dry_run_report.json"
EXCLUSION_PREVIEW_FILENAME = "exclusion_preview_v2_report.json"
EXCLUSION_REVIEW_OVERRIDES_FILENAME = "exclusion_review_overrides.json"

PLAN_STATUS_READY = "ready_but_not_applied"
PLAN_STATUS_BLOCKED_CONFIG = "blocked_by_config"
PLAN_STATUS_BLOCKED = "blocked"


class ControlledEnforceNotImplemented(RuntimeError):
    """apply=True в v0 не реализован — real skip это отдельная задача."""


# ─── helpers ─────────────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_load_json(path: Path) -> Optional[dict]:
    try:
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — fail-soft
        return None


def _sha256_file(path: Path) -> Optional[str]:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception:  # noqa: BLE001
        return None


def snapshot_protected_hashes(pipeline_v2_dir: "str | Path",
                              protected_reports=PROTECTED_REPORTS) -> dict[str, str]:
    """Снять sha256 baseline защищённых отчётов (read-only)."""
    d = Path(pipeline_v2_dir)
    out: dict[str, str] = {}
    for name in protected_reports:
        sha = _sha256_file(d / name)
        if sha:
            out[name] = sha
    return out


# ─── runtime guards ──────────────────────────────────────────────────────────


def validate_controlled_enforce_runtime_guards(
        *,
        config: dict,
        preflight_report: Optional[dict],
        dry_run_report: Optional[dict],
        skip_readiness_report: Optional[dict] = None,
        root_guard_status: Optional[str] = None,
        queue_active: bool = False,
        protected_hashes: Optional[dict] = None) -> dict[str, Any]:
    """Проверить config + runtime guards. Возвращает {apply_allowed, blocked_reasons, ...}.

    В v0 проверяются ТОЛЬКО offline-флаги (queue_active передаётся снаружи; live
    очередь модуль не трогает).
    """
    blocked: list[str] = []

    cfg_v = validate_controlled_enforce_config(config, root_guard_status=root_guard_status)
    if not cfg_v.get("enforce_allowed"):
        for r in cfg_v.get("deny_reasons", []):
            blocked.append(f"config:{r}")

    # preflight
    if not isinstance(preflight_report, dict) or preflight_report.get("kind") != _PREFLIGHT_KIND:
        blocked.append("preflight_missing")
    elif preflight_report.get("status") != _PREFLIGHT_STATUS_OK:
        blocked.append("preflight_not_ok")
    elif preflight_report.get("fatal_blocks"):
        blocked.append("preflight_fatal_blocks")

    # dry-run
    if not isinstance(dry_run_report, dict) or dry_run_report.get("kind") != _DRY_RUN_KIND:
        blocked.append("dry_run_missing")
    elif dry_run_report.get("status") != _DRY_RUN_STATUS_OK:
        blocked.append("dry_run_not_ok")
    else:
        elig = (dry_run_report.get("summary") or {}).get("eligible_items", 0)
        if not elig:
            blocked.append("no_eligible_items")

    # skip_readiness (опционально, но если передан — ready_to_skip>0)
    if isinstance(skip_readiness_report, dict):
        rts = (skip_readiness_report.get("summary") or {}).get("ready_to_skip", 0)
        if not rts:
            blocked.append("ready_to_skip_zero")

    # root guard
    if root_guard_status is not None and root_guard_status != ROOT_GUARD_OK:
        blocked.append(f"root_guard_{root_guard_status}")

    # queue (offline flag — передаётся снаружи; live не трогаем)
    if queue_active:
        blocked.append("queue_active")

    # protected hashes sentinel
    if not protected_hashes:
        blocked.append("protected_hashes_unavailable")

    return {
        "apply_allowed": not blocked,
        "blocked_reasons": sorted(set(blocked)),
        "config_validation": cfg_v,
    }


# ─── execution plan ──────────────────────────────────────────────────────────


def build_controlled_enforce_execution_plan(
        *,
        session_id: str,
        pair_id: Optional[str],
        config: dict,
        dry_run_report: Optional[dict],
        guards: dict[str, Any],
        protected_hashes_before: Optional[dict] = None,
        apply_requested: bool = False) -> dict[str, Any]:
    """Построить execution plan (что было бы применено). НИЧЕГО не пишет."""
    cfg = config if isinstance(config, dict) else {}
    dr = dry_run_report if isinstance(dry_run_report, dict) else {}
    dr_summary = dr.get("summary") or {}
    transitions = list(dr.get("logical_transitions") or [])

    eligible_items = int(dr_summary.get("eligible_items", 0) or 0)
    n_transitions = len(transitions) or int(dr_summary.get("logical_transitions", 0) or 0)
    block_pairs = int(dr_summary.get("would_skip_block_pairs", 0) or 0)

    blocked = list(guards.get("blocked_reasons") or [])
    if not blocked:
        status = PLAN_STATUS_READY
    elif cfg.get("enabled") is not True or any(
            b.startswith("config:") for b in blocked):
        status = PLAN_STATUS_BLOCKED_CONFIG
    else:
        status = PLAN_STATUS_BLOCKED

    return {
        "version": PLAN_VERSION,
        "kind": PLAN_KIND,
        "status": status,
        "session_id": session_id,
        "pair_id": pair_id,
        "created_at": _now_iso(),
        "config": {
            "enabled": bool(cfg.get("enabled")),
            "mode": cfg.get("mode"),
        },
        "summary": {
            "eligible_items": eligible_items,
            "logical_transitions": n_transitions,
            "block_pairs": block_pairs,
            "would_create_state_entries": n_transitions,
            "would_modify_runtime": False,
            "would_modify_protected_reports": False,
            "apply_requested": bool(apply_requested),
            "applied": False,
        },
        "logical_transitions": transitions,
        "would_write": [STATE_FILENAME],
        "protected_hashes_before": dict(protected_hashes_before or {}),
        "blocked_reasons": blocked,
        "warnings": [],
    }


# ─── future state preview ────────────────────────────────────────────────────


def build_controlled_enforce_state_preview(
        *,
        session_id: str,
        pair_id: Optional[str],
        dry_run_report: Optional[dict],
        config: dict,
        run_id: str = "PREVIEW",
        rollback_id: str = "PREVIEW") -> dict[str, Any]:
    """Построить PREVIEW будущего controlled_enforce_state.

    Инвариант: ``active=False`` (active state не создаётся без apply=True).
    ``status="preview"`` — это не активный артефакт.
    """
    dr = dry_run_report if isinstance(dry_run_report, dict) else {}
    transitions = list(dr.get("logical_transitions") or [])
    items = [w for w in (dr.get("would_skip_items") or []) if isinstance(w, dict)]
    scope = dict((config or {}).get("allowed_scope") or {})

    # operator decision id — из would_skip_items / overrides (best-effort)
    op_decision_id = None
    for w in items:
        if w.get("operator_decision_id"):
            op_decision_id = w["operator_decision_id"]
            break

    applied_exclusions = []
    for t in transitions:
        t_items = list(t.get("items") or [])
        left_blocks, right_blocks = [], []
        for w in items:
            if w.get("item_id") in t_items:
                if w.get("left_block_id"):
                    left_blocks.append(w["left_block_id"])
                if w.get("right_block_id"):
                    right_blocks.append(w["right_block_id"])
        applied_exclusions.append({
            "run_id": run_id,
            "transition_id": t.get("transition_id"),
            "item_ids": t_items,
            "left_entity_label": t.get("left_entity_label"),
            "right_entity_label": t.get("right_entity_label"),
            "left_block_ids": left_blocks,
            "right_block_ids": right_blocks,
            "operator_decision_id": op_decision_id,
            "scope": scope,
            # HARD INVARIANT: preview не активен (active state требует apply=True)
            "active": False,
            "created_at": _now_iso(),
            "created_by": "controlled_enforce_v0",
            "rollback_id": rollback_id,
        })

    return {
        "version": STATE_VERSION,
        "kind": STATE_KIND,
        "status": "preview",           # НЕ "active": preview не пишется как state
        "session_id": session_id,
        "pair_id": pair_id,
        "updated_at": _now_iso(),
        "applied_exclusions": applied_exclusions,
        "history": [],
    }


# ─── rollback plan ───────────────────────────────────────────────────────────


def build_controlled_enforce_rollback_plan(
        *,
        run_id: str = "PREVIEW",
        rollback_id: str = "PREVIEW") -> dict[str, Any]:
    """Построить rollback plan (diagnostics-only)."""
    return {
        "rollback_id": rollback_id,
        "would_remove_run_id": run_id,
        "would_restore_state_from_backup": True,
        "protected_reports_expected_unchanged": True,
        "manual_steps": [
            "снять controlled_enforce_state.json active=false для run_id (или "
            "восстановить из backup пары)",
            "сверить protected_reports sha256 до/после == без изменений",
            "повторно прогнать selection (enrichment) без use_controlled_enforce_state, "
            "чтобы убедиться, что исключение снято",
        ],
    }


# ─── executor (apply=False) ──────────────────────────────────────────────────


def run_controlled_enforce_executor(
        pipeline_v2_dir: "str | Path",
        *,
        config: dict,
        session_id: str,
        pair_id: Optional[str] = None,
        root_guard_status: Optional[str] = None,
        queue_active: bool = False,
        apply: bool = False) -> dict[str, Any]:
    """Прогнать executor v0. ``apply=False`` (default) → НИЧЕГО не пишет.

    Возвращает ``{plan, state_preview, rollback_plan, guards, protected_hashes_before}``.
    ``apply=True`` в v0 НЕ реализован → ``ControlledEnforceNotImplemented``.
    """
    if apply:
        raise ControlledEnforceNotImplemented(
            "controlled enforce executor v0 does NOT implement real apply; "
            "real skip is a separate task with backup + sentinel + audit-trail")

    d = Path(pipeline_v2_dir)
    skip_readiness = _safe_load_json(d / SKIP_READINESS_FILENAME)
    preflight = _safe_load_json(d / PREFLIGHT_FILENAME)
    dry_run = _safe_load_json(d / DRY_RUN_FILENAME)
    protected_before = snapshot_protected_hashes(d)

    guards = validate_controlled_enforce_runtime_guards(
        config=config, preflight_report=preflight, dry_run_report=dry_run,
        skip_readiness_report=skip_readiness, root_guard_status=root_guard_status,
        queue_active=queue_active, protected_hashes=protected_before)

    plan = build_controlled_enforce_execution_plan(
        session_id=session_id, pair_id=pair_id, config=config,
        dry_run_report=dry_run, guards=guards,
        protected_hashes_before=protected_before, apply_requested=False)
    state_preview = build_controlled_enforce_state_preview(
        session_id=session_id, pair_id=pair_id, dry_run_report=dry_run, config=config)
    rollback_plan = build_controlled_enforce_rollback_plan()

    return {
        "apply": False,
        "applied": False,
        "runtime_changed": False,
        "guards": guards,
        "plan": plan,
        "state_preview": state_preview,
        "rollback_plan": rollback_plan,
        "protected_hashes_before": protected_before,
    }


# ─── selection hook helper (default OFF) ─────────────────────────────────────


def _active_excluded_keys(state: Any) -> set[tuple]:
    """Множество (left_block_id, right_block_id) исключённых из enrichment.

    Учитываются ТОЛЬКО записи с ``active=True`` и
    ``scope.exclude_from_enrichment=True``. В preview (active=False) — пусто.
    """
    keys: set[tuple] = set()
    if not isinstance(state, dict):
        return keys
    if state.get("kind") != STATE_KIND:
        return keys
    for ex in state.get("applied_exclusions") or []:
        if not isinstance(ex, dict):
            continue
        if ex.get("active") is not True:
            continue
        scope = ex.get("scope") or {}
        if not bool(scope.get("exclude_from_enrichment")):
            continue
        lbs = ex.get("left_block_ids") or []
        rbs = ex.get("right_block_ids") or []
        # пары собираем по совпадающим индексам, плюс декартово как fallback
        for i, lb in enumerate(lbs):
            rb = rbs[i] if i < len(rbs) else None
            if lb and rb:
                keys.add((lb, rb))
        # дополнительно одиночные block_id (на случай разной длины списков)
        for lb in lbs:
            for rb in rbs:
                if lb and rb:
                    keys.add((lb, rb))
    return keys


def filter_candidates_by_controlled_enforce_state(
        candidates: list[dict],
        state: Any,
        *,
        enabled: bool = False) -> tuple[list[dict], list[str]]:
    """Отфильтровать enrichment-кандидатов по active controlled_enforce_state.

    Default ``enabled=False`` → возвращает кандидатов БЕЗ изменений (старое
    поведение). При ``enabled=True`` исключает block-pairs, активные в state со
    scope ``exclude_from_enrichment=true``. Чистая функция, ничего не пишет.

    Возвращает ``(filtered_candidates, excluded_keys_str)``.
    """
    if not enabled or not isinstance(candidates, list):
        return (candidates if isinstance(candidates, list) else []), []
    excluded = _active_excluded_keys(state)
    if not excluded:
        return candidates, []
    kept: list[dict] = []
    removed: list[str] = []
    for c in candidates:
        if not isinstance(c, dict):
            kept.append(c)
            continue
        key = (c.get("left_block_id"), c.get("right_block_id"))
        if key in excluded:
            removed.append(f"{key[0]}__{key[1]}")
        else:
            kept.append(c)
    return kept, removed


__all__ = [
    "ControlledEnforceNotImplemented",
    "STATE_KIND", "STATE_FILENAME", "PLAN_KIND", "PLAN_FILENAME",
    "PLAN_STATUS_READY", "PLAN_STATUS_BLOCKED_CONFIG", "PLAN_STATUS_BLOCKED",
    "snapshot_protected_hashes",
    "validate_controlled_enforce_runtime_guards",
    "build_controlled_enforce_execution_plan",
    "build_controlled_enforce_state_preview",
    "build_controlled_enforce_rollback_plan",
    "run_controlled_enforce_executor",
    "filter_candidates_by_controlled_enforce_state",
]
