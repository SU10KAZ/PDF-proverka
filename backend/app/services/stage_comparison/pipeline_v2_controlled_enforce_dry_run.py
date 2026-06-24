# -*- coding: utf-8 -*-
"""Pipeline V2 — controlled enforce DRY-RUN / impact report (mark-only).

Слой НАД ``controlled_enforce_preflight``. Показывает, что *БЫЛО БЫ* пропущено,
если бы real enforce был включён, — но **ничего не применяет и не пишет в
pipeline inputs**:

* какие eligible items были бы пропущены (``would_skip_items``);
* какой scope skip у каждого (MVP: только ``exclude_from_enrichment``);
* какие block-pairs / entity-pairs затронуты;
* **logical transitions** — несколько block-pair записей одного логического
  перехода (напр. ВРУ-3→ВРУ-2 на 2 block-pairs = 1 logical transition);
* какие downstream артефакты НЕ должны меняться (``must_remain_unchanged``);
* что блокирует реальное применение (``blocked_from_real_apply_reasons``).

Жёсткие инварианты (никогда не нарушаются):

```
would_apply           = false
enforce_enabled       = false
runtime_write_allowed = false   (на каждом item)
enforce_allowed       = false   (на каждом item)
protected_artifacts.will_modify = []
```

Источник eligible — ИСКЛЮЧИТЕЛЬНО ``controlled_enforce_preflight.eligible_items``.
Если preflight отсутствует/blocked/без eligible — dry-run не строит would_skip.

Read-only, offline: не импортирует/не зовёт модели/джобы/LLM; единственный
backend-импорт — kind/status-константы preflight-слоя + ``paths`` через
from-dir helper. Запись ограничена собственным
``controlled_enforce_dry_run_report.json`` (диагностический, не вход pipeline).
"""
from __future__ import annotations

import json
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce import (
    PREFLIGHT_KIND,
    PREFLIGHT_FILENAME,
    SKIP_READINESS_FILENAME,
    EXCLUSION_PREVIEW_FILENAME,
    EXCLUSION_REVIEW_OVERRIDES_FILENAME,
    STATUS_BLOCKED,
    STATUS_PREFLIGHT_OK,
    STATUS_NO_ELIGIBLE,
)

# ─── artifact kind / filename ────────────────────────────────────────────────

DRY_RUN_VERSION = 1
DRY_RUN_KIND = "stage_comparison_pipeline_v2_controlled_enforce_dry_run"
DRY_RUN_FILENAME = "controlled_enforce_dry_run_report.json"

# статусы dry-run (зеркалят preflight gate)
STATUS_OK = "ok"

DRY_RUN_ACTION_ENRICHMENT = "would_exclude_from_enrichment"

# Downstream артефакты, которые real enforce НЕ должен трогать (mark-only гарантия).
_MUST_REMAIN_UNCHANGED = (
    "entity_diff_report.json",
    "grounded_evidence_report.json",
    "delta_explanation_report.json",
    "block_link_preview_report.json",
    "link_validation_report.json",
    "exclusion_preview_v2_report.json",
    "entity_alignment_preview_report.json",
    "visual_equivalence_gate_report.json",
    "graphic_vision_enrichment_report.json",
    "graphic_vision_grounding_report.json",
)

_BLOCKED_FROM_REAL_APPLY = ["dry_run_only", "enforce_config_disabled"]

_REQUIRED_SCOPE = {
    "exclude_from_enrichment": True,
    "exclude_from_grounded_evidence": False,
    "exclude_from_delta_explanation": False,
    "exclude_from_findings": False,
}


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


def _norm_label(value: Any) -> str:
    """Канонизировать метку сущности для группировки (NFKC, ё→е, lower, ws)."""
    s = unicodedata.normalize("NFKC", str(value or "")).replace("ё", "е").replace("Ё", "Е")
    return " ".join(s.lower().split())


def _index_by_item_id(report: Optional[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    items = (report or {}).get("items") if isinstance(report, dict) else None
    for it in (items or []):
        if isinstance(it, dict) and it.get("item_id"):
            out[it["item_id"]] = it
    return out


def _scope_from(sr_item: Optional[dict]) -> dict:
    sc = (sr_item or {}).get("skip_scope") if isinstance(sr_item, dict) else None
    if isinstance(sc, dict):
        # форсим только известные ключи, дефолт — MVP scope
        return {k: bool(sc.get(k, _REQUIRED_SCOPE[k])) for k in _REQUIRED_SCOPE}
    return dict(_REQUIRED_SCOPE)


def _link_validation_decision(xp_item: Optional[dict]) -> Optional[str]:
    lv = (xp_item or {}).get("link_validation") if isinstance(xp_item, dict) else None
    if isinstance(lv, dict):
        d = lv.get("decision")
        return str(d) if d else None
    return None


# ─── core builder ────────────────────────────────────────────────────────────


def build_controlled_enforce_dry_run(
        *,
        session_id: str,
        pair_id: Optional[str],
        preflight_report: Optional[dict],
        skip_readiness_report: Optional[dict] = None,
        overrides_report: Optional[dict] = None,
        exclusion_preview_report: Optional[dict] = None) -> dict:
    """Построить controlled_enforce_dry_run report (mark-only, ничего не применяет).

    eligible берётся ТОЛЬКО из ``preflight_report.eligible_items`` и только при
    ``preflight.status == preflight_ok``.
    """
    warnings: list[str] = []

    def _envelope(status: str, *, reasons: Optional[list[str]] = None) -> dict:
        return {
            "version": DRY_RUN_VERSION,
            "kind": DRY_RUN_KIND,
            "status": status,
            "session_id": session_id,
            "pair_id": pair_id,
            "created_at": _now_iso(),
            "summary": {
                "eligible_items": 0,
                "logical_transitions": 0,
                "would_skip_block_pairs": 0,
                "would_exclude_from_enrichment": 0,
                "would_modify_runtime": False,
                "would_modify_findings": False,
                "would_modify_block_links": False,
                "would_modify_delta_explanation": False,
                "would_apply": False,
                "enforce_enabled": False,
            },
            "logical_transitions": [],
            "would_skip_items": [],
            "protected_artifacts": {
                "will_modify": [],
                "must_remain_unchanged": list(_MUST_REMAIN_UNCHANGED),
            },
            "blocked_reasons": reasons or [],
            "warnings": warnings,
            # HARD INVARIANTS
            "would_apply": False,
            "enforce_enabled": False,
        }

    # ─── gate на preflight ───────────────────────────────────────────────────
    if not isinstance(preflight_report, dict) or \
            preflight_report.get("kind") != PREFLIGHT_KIND:
        return _envelope(STATUS_BLOCKED, reasons=["preflight_missing"])

    pf_status = preflight_report.get("status")
    if pf_status == STATUS_BLOCKED:
        fatal = list(preflight_report.get("fatal_blocks") or [])
        return _envelope(STATUS_BLOCKED,
                         reasons=fatal or ["preflight_blocked"])
    if pf_status == STATUS_NO_ELIGIBLE:
        return _envelope(STATUS_NO_ELIGIBLE, reasons=["no_eligible_items"])
    if pf_status != STATUS_PREFLIGHT_OK:
        return _envelope(STATUS_BLOCKED,
                         reasons=[f"preflight_status:{pf_status}"])

    # ─── eligible items из preflight ────────────────────────────────────────
    eligible = [e for e in (preflight_report.get("eligible_items") or [])
                if isinstance(e, dict) and e.get("item_id")]
    if not eligible:
        return _envelope(STATUS_NO_ELIGIBLE, reasons=["no_eligible_items"])

    sr_idx = _index_by_item_id(skip_readiness_report)
    xp_idx = _index_by_item_id(exclusion_preview_report)

    would_skip_items: list[dict] = []
    for e in eligible:
        iid = e["item_id"]
        sr_item = sr_idx.get(iid) or {}
        xp_item = xp_idx.get(iid) or {}
        scope = _scope_from(sr_item)
        would_skip_items.append({
            "item_id": iid,
            "left_block_id": sr_item.get("left_block_id") or xp_item.get("left_block_id"),
            "right_block_id": sr_item.get("right_block_id") or xp_item.get("right_block_id"),
            "left_entity_label": sr_item.get("left_entity_label") or xp_item.get("left_entity_label"),
            "right_entity_label": sr_item.get("right_entity_label") or xp_item.get("right_entity_label"),
            "operator_decision": e.get("operator_decision") or sr_item.get("operator_decision"),
            "source_readiness": e.get("source_readiness") or "ready_to_skip",
            "dry_run_action": DRY_RUN_ACTION_ENRICHMENT,
            "skip_scope": scope,
            # HARD INVARIANTS — ничего не применяется, ничего не пишется
            "would_apply": False,
            "enforce_allowed": False,
            "runtime_write_allowed": False,
            "source_signals": list(xp_item.get("source_signals") or []),
            "blocked_from_real_apply_reasons": list(_BLOCKED_FROM_REAL_APPLY),
        })

    # ─── logical transition grouping ────────────────────────────────────────
    groups: dict[tuple, dict] = {}
    order: list[tuple] = []
    for w in would_skip_items:
        ln = _norm_label(w.get("left_entity_label"))
        rn = _norm_label(w.get("right_entity_label"))
        key = (ln, rn)
        if key not in groups:
            order.append(key)
            groups[key] = {
                "left_entity_label": w.get("left_entity_label"),
                "right_entity_label": w.get("right_entity_label"),
                "items": [],
                "operator_decisions": set(),
                "link_validation_decisions": set(),
                "scope": w.get("skip_scope"),
            }
        g = groups[key]
        g["items"].append(w["item_id"])
        if w.get("operator_decision"):
            g["operator_decisions"].add(w["operator_decision"])
        lv = _link_validation_decision(xp_idx.get(w["item_id"]))
        if lv:
            g["link_validation_decisions"].add(lv)

    logical_transitions: list[dict] = []
    for key in order:
        g = groups[key]
        ll = g["left_entity_label"] or "?"
        rl = g["right_entity_label"] or "?"
        # confidence: max по exclusion_preview items группы
        confs = [xp_idx.get(i, {}).get("confidence") for i in g["items"]]
        confs = [c for c in confs if isinstance(c, (int, float))]
        ops = sorted(g["operator_decisions"])
        logical_transitions.append({
            "transition_id": f"{ll}→{rl}",
            "left_entity_label": g["left_entity_label"],
            "right_entity_label": g["right_entity_label"],
            "item_count": len(g["items"]),
            "items": list(g["items"]),
            "operator_decision": ops[0] if len(ops) == 1 else (ops or None),
            "link_validation_decisions": sorted(g["link_validation_decisions"]),
            "confidence": max(confs) if confs else None,
            "recommended_scope": g["scope"] or dict(_REQUIRED_SCOPE),
        })

    would_enrichment = sum(1 for w in would_skip_items
                           if w["skip_scope"].get("exclude_from_enrichment"))

    report = {
        "version": DRY_RUN_VERSION,
        "kind": DRY_RUN_KIND,
        "status": STATUS_OK,
        "session_id": session_id,
        "pair_id": pair_id,
        "created_at": _now_iso(),
        "summary": {
            "eligible_items": len(would_skip_items),
            "logical_transitions": len(logical_transitions),
            "would_skip_block_pairs": len(would_skip_items),
            "would_exclude_from_enrichment": would_enrichment,
            "would_modify_runtime": False,
            "would_modify_findings": False,
            "would_modify_block_links": False,
            "would_modify_delta_explanation": False,
            "would_apply": False,
            "enforce_enabled": False,
        },
        "logical_transitions": logical_transitions,
        "would_skip_items": would_skip_items,
        "protected_artifacts": {
            "will_modify": [],
            "must_remain_unchanged": list(_MUST_REMAIN_UNCHANGED),
        },
        "runtime_root": dict(preflight_report.get("runtime_root") or {}),
        "warnings": warnings,
        # HARD INVARIANTS
        "would_apply": False,
        "enforce_enabled": False,
    }
    return report


def build_controlled_enforce_dry_run_from_dir(
        pipeline_v2_dir: "str | Path", *,
        session_id: str, pair_id: Optional[str] = None) -> dict:
    """Прочитать артефакты пары из каталога и построить dry-run (read-only)."""
    d = Path(pipeline_v2_dir)
    pf = _safe_load_json(d / PREFLIGHT_FILENAME)
    sr = _safe_load_json(d / SKIP_READINESS_FILENAME)
    ov = _safe_load_json(d / EXCLUSION_REVIEW_OVERRIDES_FILENAME)
    xp = _safe_load_json(d / EXCLUSION_PREVIEW_FILENAME)
    return build_controlled_enforce_dry_run(
        session_id=session_id, pair_id=pair_id,
        preflight_report=pf, skip_readiness_report=sr,
        overrides_report=ov, exclusion_preview_report=xp)


def write_controlled_enforce_dry_run_report(out_path: "str | Path",
                                            report: dict) -> Path:
    """Атомарно записать dry-run report (диагностический артефакт, не вход)."""
    import os
    import tempfile
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=out_path.name + ".", suffix=".tmp",
                              dir=str(out_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, out_path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out_path


__all__ = [
    "DRY_RUN_KIND", "DRY_RUN_FILENAME", "DRY_RUN_VERSION",
    "STATUS_OK", "STATUS_BLOCKED", "STATUS_NO_ELIGIBLE",
    "build_controlled_enforce_dry_run",
    "build_controlled_enforce_dry_run_from_dir",
    "write_controlled_enforce_dry_run_report",
]
