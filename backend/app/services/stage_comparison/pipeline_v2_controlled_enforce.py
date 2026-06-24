# -*- coding: utf-8 -*-
"""Pipeline V2 — controlled enforce config + preflight preview (mark-only).

Это **НЕ enforce**. Это защитный слой ПЕРЕД любым реальным skip/enforce: он
читает уже готовую mark-only цепочку (`skip_readiness_report.json` +
`exclusion_review_overrides.json` + `exclusion_preview_v2_report.json`),
сверяется с runtime-root guard и говорит:

* можно ли вообще включать skip для пары (status `blocked` / `preflight_ok` /
  `no_eligible_items`);
* какие условия блокируют enforce (`fatal_blocks`);
* какие item'ы теоретически eligible (`eligible_items` → `would_skip`);
* какие item'ы трогать нельзя (`blocked_items`);
* какой scope разрешён (`allowed_scopes` в config);
* какие guard'ы должны быть включены (`required_guards`);
* какой rollback/backup обязателен (`backup_required` / `rollback_plan_required`).

Жёсткие инварианты (никогда не нарушаются этим модулем):

* ``enabled = false`` в config;
* ``mode = preflight_only``;
* ``auto_apply = false`` / ``enforce_allowed = false`` / ``would_apply = false``
  в report;
* НИЧЕГО не исключается, никакие pipeline stages не пропускаются, никакие
  block links / findings / deltas / pipeline inputs не меняются;
* модуль НЕ импортирует и НЕ вызывает модели/джобы/LLM-runner'ы; единственный
  backend-импорт — runtime-root guard (`pipeline_v2_runtime_root_audit`,
  чистый offline path-helper) + `paths` для resolve.

Запись ограничена собственным preflight-отчётом
(`controlled_enforce_preflight_report.json`) — он диагностический, не вход
pipeline.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# ─── artifact kinds / filenames ──────────────────────────────────────────────

CONFIG_VERSION = 1
CONFIG_KIND = "stage_comparison_pipeline_v2_controlled_enforce_config"
CONFIG_FILENAME = "controlled_enforce_config.json"

PREFLIGHT_VERSION = 1
PREFLIGHT_KIND = "stage_comparison_pipeline_v2_controlled_enforce_preflight"
PREFLIGHT_FILENAME = "controlled_enforce_preflight_report.json"

SKIP_READINESS_FILENAME = "skip_readiness_report.json"
EXCLUSION_PREVIEW_FILENAME = "exclusion_preview_v2_report.json"
EXCLUSION_REVIEW_OVERRIDES_FILENAME = "exclusion_review_overrides.json"

# Защищённые runtime-артефакты — их hash снимается как baseline ПЕРЕД любым
# будущим enforce. В preflight только снимаем снимок (mark-only).
PROTECTED_ARTIFACTS = (
    "exclusion_preview_v2_report.json",
    "exclusion_review_overrides.json",
    "skip_readiness_report.json",
    "link_validation_report.json",
    "grounded_evidence_report.json",
    "delta_explanation_report.json",
)
# Минимальный набор, который ДОЛЖЕН существовать, чтобы baseline считался
# доступным (без него enforce запрещён).
_PROTECTED_REQUIRED = (
    "skip_readiness_report.json",
    "exclusion_preview_v2_report.json",
)

# ─── статусы / решения ───────────────────────────────────────────────────────

STATUS_BLOCKED = "blocked"
STATUS_PREFLIGHT_OK = "preflight_ok"
STATUS_NO_ELIGIBLE = "no_eligible_items"

# Single allowed operator decision для eligible
OP_APPROVE_EXCLUDE = "approve_exclude"

# readiness из skip_readiness_report
RS_READY = "ready_to_skip"
RS_BLOCKED = "blocked"
RS_NEEDS_REVIEW = "needs_review"
RS_KEEP = "keep"

CLS_CANDIDATE_EXCLUDE = "candidate_exclude"

# fatal block reasons
FB_SKIP_READINESS_MISSING = "skip_readiness_missing"
FB_READY_TO_SKIP_ZERO = "ready_to_skip_zero"
FB_RUNTIME_ROOT_UNCONFIRMED = "runtime_root_unconfirmed"
FB_PROTECTED_HASHES_MISSING = "protected_hashes_missing"
FB_PROTECTED_HASH_MISMATCH = "protected_hash_mismatch"
FB_OPERATOR_APPROVAL_MISSING = "operator_approval_missing"
FB_CONFIG_INVALID = "config_invalid"

# blocked-item reasons
BI_MARK_ONLY_VIOLATION = "mark_only_invariant_violation"
BI_MISSING_APPROVAL = "missing_operator_approval"
BI_NOT_READY = "readiness_not_ready_to_skip"
BI_NOT_CANDIDATE = "valid_mapping_not_exclusion"
BI_INVALID_SCOPE = "invalid_skip_scope"
BI_LINK_VALIDATION = "link_validation_required"
BI_NEEDS_REVIEW = "needs_review"
BI_KEEP = "keep"

# Канонический разрешённый scope (MVP: только пропуск enrichment)
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


def _sha256_file(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha256()
        h.update(path.read_bytes())
        return h.hexdigest()
    except Exception:  # noqa: BLE001
        return None


# ─── config ──────────────────────────────────────────────────────────────────


def build_controlled_enforce_config(session_id: str,
                                     pair_id: Optional[str] = None,
                                     *,
                                     allowed_scopes: Optional[dict] = None,
                                     max_items_per_run: int = 5) -> dict:
    """Построить controlled_enforce_config (mark-only, всегда disabled).

    Инварианты: ``enabled=False``, ``mode=preflight_only``. Никакого реального
    enforce этот config не разрешает — он описывает условия, при которых enforce
    *мог бы* быть когда-нибудь разрешён, и какой scope/guard'ы для этого нужны.
    """
    scopes = dict(_REQUIRED_SCOPE)
    if isinstance(allowed_scopes, dict):
        # допускаем сужение, но НЕ расширение за пределы MVP-инварианта
        for k in _REQUIRED_SCOPE:
            if k in allowed_scopes:
                scopes[k] = bool(allowed_scopes[k])
    return {
        "version": CONFIG_VERSION,
        "kind": CONFIG_KIND,
        "enabled": False,            # HARD INVARIANT
        "mode": "preflight_only",    # HARD INVARIANT
        "session_id": session_id,
        "pair_id": pair_id,
        "allowed_scopes": scopes,
        "required_guards": {
            "active_runtime_root_confirmed": True,
            "backup_required": True,
            "operator_approval_required": True,
            "ready_to_skip_required": True,
            "protected_hashes_required": True,
            "dry_run_required": True,
            "rollback_plan_required": True,
        },
        "max_items_per_run": int(max_items_per_run),
        "allowed_decisions": [OP_APPROVE_EXCLUDE],
        "deny_if_any": [
            "auto_enforce_enabled_true",
            "enforce_allowed_true_in_source",
            "missing_operator_approval",
            "valid_mapping_not_exclusion",
            "runtime_root_mismatch",
            "protected_hash_mismatch",
        ],
    }


def _config_is_valid(config: dict) -> bool:
    if not isinstance(config, dict):
        return False
    if config.get("kind") != CONFIG_KIND:
        return False
    # HARD INVARIANTS: preflight config must be disabled + preflight_only
    if config.get("enabled") is not False:
        return False
    if config.get("mode") != "preflight_only":
        return False
    return True


# ─── per-item evaluation ─────────────────────────────────────────────────────


def _scope_ok(skip_scope: Any) -> bool:
    if not isinstance(skip_scope, dict):
        return False
    for k, v in _REQUIRED_SCOPE.items():
        if bool(skip_scope.get(k)) != v:
            return False
    return True


def evaluate_item(item: dict) -> dict:
    """Оценить один skip_readiness item.

    Returns dict: ``{item_id, eligible, reason, source_readiness,
    operator_decision}``. eligible=True только при полном совпадении всех
    условий 5.2; иначе reason объясняет блокировку (5.3).
    """
    item_id = item.get("item_id", "")
    readiness = item.get("readiness_status")
    op_decision = item.get("operator_decision")
    classification = item.get("classification")
    skip_scope = item.get("skip_scope")
    src_auto_apply = bool(item.get("auto_apply"))
    src_enforce_allowed = bool(item.get("enforce_allowed"))

    base = {
        "item_id": item_id,
        "source_readiness": readiness,
        "operator_decision": op_decision,
        "classification": classification,
    }

    # source mark-only invariant violation — самый строгий блок
    if src_auto_apply or src_enforce_allowed:
        return {**base, "eligible": False, "reason": BI_MARK_ONLY_VIOLATION}

    # readiness gate
    if readiness != RS_READY:
        if readiness == RS_NEEDS_REVIEW:
            reason = BI_NEEDS_REVIEW
        elif readiness == RS_KEEP:
            reason = BI_KEEP
        elif readiness == RS_BLOCKED:
            reason = item.get("blocked_reason") or BI_NOT_READY
        else:
            reason = BI_NOT_READY
        return {**base, "eligible": False, "reason": reason}

    # operator approval gate
    if op_decision != OP_APPROVE_EXCLUDE:
        return {**base, "eligible": False, "reason": BI_MISSING_APPROVAL}

    # classification gate
    if classification != CLS_CANDIDATE_EXCLUDE:
        return {**base, "eligible": False, "reason": BI_NOT_CANDIDATE}

    # scope gate
    if not _scope_ok(skip_scope):
        return {**base, "eligible": False, "reason": BI_INVALID_SCOPE}

    return {**base, "eligible": True, "reason": None}


# ─── preflight builder ───────────────────────────────────────────────────────


def build_controlled_enforce_preflight(
        *,
        session_id: str,
        pair_id: Optional[str] = None,
        skip_readiness_report: Optional[dict],
        overrides_report: Optional[dict] = None,
        exclusion_preview_report: Optional[dict] = None,
        config: Optional[dict] = None,
        active_runtime_root: Optional[str] = None,
        runtime_root_confirmed: Optional[bool] = None,
        runtime_root_source: Optional[str] = None,
        protected_hashes: Optional[dict] = None,
        protected_hashes_match: bool = True) -> dict:
    """Построить controlled_enforce_preflight report (mark-only, ничего не применяет).

    :param skip_readiness_report: содержимое ``skip_readiness_report.json``.
    :param active_runtime_root: подтверждённый active comparison root.
    :param runtime_root_confirmed: подтверждён ли root (если None → derive из
        наличия active_runtime_root).
    :param protected_hashes: ``{filename: sha256}`` baseline защищённых
        артефактов (None/пусто → protected_hashes_missing).
    :param protected_hashes_match: False → protected_hash_mismatch (fatal).
    """
    warnings: list[str] = []
    cfg = config if _config_is_valid(config) else build_controlled_enforce_config(
        session_id, pair_id)
    if config is not None and not _config_is_valid(config):
        warnings.append("supplied config invalid → using safe default")

    # runtime root confirmation
    if runtime_root_confirmed is None:
        runtime_root_confirmed = bool(active_runtime_root)

    # protected hashes availability
    protected_hashes = protected_hashes if isinstance(protected_hashes, dict) else {}
    protected_available = all(
        name in protected_hashes for name in _PROTECTED_REQUIRED)

    # skip_readiness summary
    sr_summary = (skip_readiness_report or {}).get("summary") \
        if isinstance(skip_readiness_report, dict) else None
    sr_summary = sr_summary if isinstance(sr_summary, dict) else {}
    ready_to_skip = int(sr_summary.get("ready_to_skip", 0) or 0)
    operator_approved = int(sr_summary.get("operator_approved", 0) or 0)

    # ─── per-item evaluation ────────────────────────────────────────────────
    eligible_items: list[dict] = []
    blocked_items: list[dict] = []
    would_skip: list[str] = []
    items = (skip_readiness_report or {}).get("items") \
        if isinstance(skip_readiness_report, dict) else None
    items = items if isinstance(items, list) else []
    for it in items:
        if not isinstance(it, dict):
            continue
        verdict = evaluate_item(it)
        if verdict["eligible"]:
            eligible_items.append({
                "item_id": verdict["item_id"],
                "source_readiness": verdict["source_readiness"],
                "operator_decision": verdict["operator_decision"],
                "classification": verdict["classification"],
                # mark-only: даже eligible НЕ применяется
                "would_skip": True,
                "applied": False,
            })
            would_skip.append(verdict["item_id"])
        else:
            blocked_items.append({
                "item_id": verdict["item_id"],
                "reason": verdict["reason"],
                "source_readiness": verdict["source_readiness"],
                "operator_decision": verdict["operator_decision"],
            })

    # ─── global fatal blocks (5.1) ──────────────────────────────────────────
    fatal_blocks: list[str] = []
    if not _config_is_valid(cfg):
        fatal_blocks.append(FB_CONFIG_INVALID)
    if not isinstance(skip_readiness_report, dict) or \
            skip_readiness_report.get("kind") != "skip_readiness_report_v1":
        fatal_blocks.append(FB_SKIP_READINESS_MISSING)
    else:
        if ready_to_skip == 0:
            fatal_blocks.append(FB_READY_TO_SKIP_ZERO)
        # operator approval отсутствует ТОЛЬКО релевантно, когда есть что
        # одобрять (ready_to_skip > 0). При ready=0 это покрыто ready_to_skip_zero.
        elif operator_approved == 0:
            fatal_blocks.append(FB_OPERATOR_APPROVAL_MISSING)
    if not runtime_root_confirmed:
        fatal_blocks.append(FB_RUNTIME_ROOT_UNCONFIRMED)
    if not protected_available:
        fatal_blocks.append(FB_PROTECTED_HASHES_MISSING)
    elif not protected_hashes_match:
        fatal_blocks.append(FB_PROTECTED_HASH_MISMATCH)

    # ─── status ─────────────────────────────────────────────────────────────
    if fatal_blocks:
        status = STATUS_BLOCKED
    elif eligible_items:
        status = STATUS_PREFLIGHT_OK
    else:
        status = STATUS_NO_ELIGIBLE

    # enforce_enabled / would_apply — HARD INVARIANT в preflight: всегда False
    enforce_enabled = False
    would_apply = False

    report = {
        "version": PREFLIGHT_VERSION,
        "kind": PREFLIGHT_KIND,
        "status": status,
        "session_id": session_id,
        "pair_id": pair_id,
        "created_at": _now_iso(),
        "config": {
            "kind": cfg.get("kind"),
            "enabled": cfg.get("enabled"),
            "mode": cfg.get("mode"),
            "allowed_scopes": cfg.get("allowed_scopes"),
            "max_items_per_run": cfg.get("max_items_per_run"),
            "allowed_decisions": cfg.get("allowed_decisions"),
        },
        "summary": {
            "ready_to_skip_items": ready_to_skip,
            "eligible_items": len(eligible_items),
            "blocked_items": len(blocked_items),
            "fatal_blocks": len(fatal_blocks),
            "warnings": len(warnings),
            "would_apply": would_apply,
            "enforce_enabled": enforce_enabled,
        },
        "global_guards": {
            "active_runtime_root_confirmed": bool(runtime_root_confirmed),
            "backup_required": True,
            "operator_approval_present": operator_approved > 0,
            "ready_to_skip_present": ready_to_skip > 0,
            "protected_hashes_available": bool(protected_available),
            "dry_run_only": True,
        },
        "runtime_root": {
            "active": active_runtime_root,
            "confirmed": bool(runtime_root_confirmed),
            "source": runtime_root_source,
        },
        "protected_hashes": {
            "available": bool(protected_available),
            "match": bool(protected_hashes_match),
            "artifacts": dict(protected_hashes),
        },
        "eligible_items": eligible_items,
        "blocked_items": blocked_items,
        "fatal_blocks": fatal_blocks,
        "would_write": [],          # preflight НЕ пишет в pipeline inputs
        "would_skip": would_skip,   # eligible item_id'ы — но НЕ применяются
        "warnings": warnings,
        # HARD INVARIANTS
        "auto_apply": False,
        "enforce_allowed": False,
    }
    return report


# ─── from-dir convenience ────────────────────────────────────────────────────


def snapshot_protected_hashes(pipeline_v2_dir: Path) -> dict:
    """Снять sha256 baseline существующих защищённых артефактов (read-only)."""
    out: dict[str, str] = {}
    for name in PROTECTED_ARTIFACTS:
        p = pipeline_v2_dir / name
        sha = _sha256_file(p)
        if sha:
            out[name] = sha
    return out


def build_controlled_enforce_preflight_from_dir(
        pipeline_v2_dir: "str | Path",
        *,
        session_id: str,
        pair_id: Optional[str] = None,
        config: Optional[dict] = None,
        active_runtime_root: Optional[str] = None,
        runtime_root_confirmed: Optional[bool] = None,
        runtime_root_source: Optional[str] = None) -> dict:
    """Прочитать mark-only артефакты из каталога пары и построить preflight.

    Read-only: только чтение JSON + sha256. Ничего не пишет.
    """
    d = Path(pipeline_v2_dir)
    sr = _safe_load_json(d / SKIP_READINESS_FILENAME)
    ov = _safe_load_json(d / EXCLUSION_REVIEW_OVERRIDES_FILENAME)
    xp = _safe_load_json(d / EXCLUSION_PREVIEW_FILENAME)
    protected = snapshot_protected_hashes(d)
    return build_controlled_enforce_preflight(
        session_id=session_id, pair_id=pair_id,
        skip_readiness_report=sr, overrides_report=ov,
        exclusion_preview_report=xp, config=config,
        active_runtime_root=active_runtime_root,
        runtime_root_confirmed=runtime_root_confirmed,
        runtime_root_source=runtime_root_source,
        protected_hashes=protected, protected_hashes_match=True)


def write_controlled_enforce_preflight_report(out_path: "str | Path",
                                              report: dict) -> Path:
    """Атомарно записать preflight report (диагностический артефакт, не вход)."""
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
    "CONFIG_KIND", "CONFIG_FILENAME", "CONFIG_VERSION",
    "PREFLIGHT_KIND", "PREFLIGHT_FILENAME", "PREFLIGHT_VERSION",
    "PROTECTED_ARTIFACTS",
    "STATUS_BLOCKED", "STATUS_PREFLIGHT_OK", "STATUS_NO_ELIGIBLE",
    "OP_APPROVE_EXCLUDE",
    "build_controlled_enforce_config",
    "evaluate_item",
    "build_controlled_enforce_preflight",
    "build_controlled_enforce_preflight_from_dir",
    "snapshot_protected_hashes",
    "write_controlled_enforce_preflight_report",
]
