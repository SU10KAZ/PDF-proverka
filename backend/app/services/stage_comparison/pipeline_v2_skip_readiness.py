"""
Pipeline V2 Skip Readiness — mark-only слой, определяющий что *теоретически*
может быть пропущено в будущем обогащении.

Читает уже вычисленные артефакты ``exclusion_preview_v2_report.json`` и
``exclusion_review_overrides.json``, объединяет их сигналы и выдаёт
``skip_readiness_report.json`` — план того, что *могло бы* быть пропущено
при наличии явного оператора-подтверждения.

HARD INVARIANTS (проверяются на каждом item и в итоговом report):
  - ``auto_apply = False``         — никакого автоматического применения
  - ``enforce_allowed = False``    — запрет принудительного пропуска
  - ``requires_explicit_operator_approval = True``  — нужно явное ОК оператора
  - Модели (LLM / Qwen / Opus / Gemma) НЕ вызываются
  - Входные артефакты НЕ изменяются
  - Ни один блок физически не пропускается (Stage 1 — только наблюдение)
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ─── константы классификаций exclusion_preview ───────────────────────────────

CLS_EXCLUDE = "candidate_exclude"
CLS_REVIEW = "review_only"
CLS_KEEP = "keep"
CLS_LINK_VALIDATION = "link_validation_required"

# ─── статусы skip_readiness ──────────────────────────────────────────────────

STATUS_READY = "ready_to_skip"
STATUS_BLOCKED = "blocked"
STATUS_NEEDS_REVIEW = "needs_review"
STATUS_KEEP = "keep"

# ─── причины BLOCKED ─────────────────────────────────────────────────────────

BLOCKED_MISSING_APPROVAL = "missing_operator_approval"
BLOCKED_VALID_MAPPING = "valid_mapping_not_exclusion"
BLOCKED_MARK_ONLY_SAFETY = "mark_only_safety_guard"

# ─── причины NEEDS_REVIEW ────────────────────────────────────────────────────

REVIEW_ABSENT_LV = "absent_link_validation"
REVIEW_MANUAL = "manual_review_required"
REVIEW_LV_REQUIRED = "link_validation_required"
REVIEW_ONLY_CLS = "review_only_classification"

# ─── причины KEEP ────────────────────────────────────────────────────────────

KEEP_PREVIEW_CLS = "preview_classification_keep"
KEEP_OPERATOR = "operator_marked_keep"
KEEP_REJECTED = "operator_rejected_exclusion"

# ─── MVP: только пропуск enrichment ─────────────────────────────────────────

SKIP_SCOPE_MVP: Dict[str, bool] = {
    "exclude_from_enrichment": True,
    "exclude_from_grounded_evidence": False,
    "exclude_from_delta_explanation": False,
    "exclude_from_findings": False,
}

# ─── artifact schema ─────────────────────────────────────────────────────────

REPORT_VERSION = "1"
REPORT_KIND = "skip_readiness_report_v1"
ARTIFACT_FILENAME = "skip_readiness_report.json"

# ─── operator decisions ──────────────────────────────────────────────────────

_OP_APPROVE = "approve_exclude"
_OP_REJECT = "reject_exclude"
_OP_KEEP = "keep"
_OP_NEEDS_REVIEW = "needs_review"
_OP_RUN_LV = "run_link_validation"


# ─── helpers ─────────────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, data: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp",
                               dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path


def _safe_load_json(path: Path) -> Optional[dict]:
    """Загрузить JSON-файл; вернуть None если файл отсутствует или повреждён."""
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None
    except Exception:  # noqa: BLE001
        return None


def _lv_key_for_item(item: dict) -> str:
    """Ключ для поиска item в link_validation_report по block_id'ам."""
    lb = item.get("left_block_id") or ""
    rb = item.get("right_block_id") or ""
    return f"lv_{lb}__{rb}"


def _build_lv_index(lv_report: Optional[dict]) -> Dict[str, dict]:
    """Построить индекс link_validation items по ключу `lv_LEFT__RIGHT`."""
    if not isinstance(lv_report, dict):
        return {}
    index: Dict[str, dict] = {}
    for it in lv_report.get("items") or []:
        if isinstance(it, dict):
            iid = it.get("item_id", "")
            # item_id вида "lv_LEFT__RIGHT"
            index[iid] = it
            # Также индексируем по block_id'ам (для надёжного lookup)
            lb = it.get("left_block_id") or ""
            rb = it.get("right_block_id") or ""
            if lb or rb:
                index[f"lv_{lb}__{rb}"] = it
    return index


def _find_operator_decision(overrides_data: Optional[dict],
                            item: dict) -> Optional[dict]:
    """
    Найти operator decision для item в overrides.

    Ищет по: item_id, exclusion_item_id, left/right block_id.
    Возвращает словарь decision или None.
    """
    if not isinstance(overrides_data, dict):
        return None
    decisions = overrides_data.get("decisions") or []
    if not decisions:
        return None

    item_id = item.get("item_id", "")
    lb = item.get("left_block_id") or ""
    rb = item.get("right_block_id") or ""
    ll = (item.get("left_entity_label") or "").strip()
    rl = (item.get("right_entity_label") or "").strip()

    for d in decisions:
        if not isinstance(d, dict):
            continue
        eid = d.get("exclusion_item_id") or d.get("item_id") or ""
        if item_id and eid == item_id:
            return d
        dlb = d.get("left_block_id") or ""
        drb = d.get("right_block_id") or ""
        if lb and rb and dlb == lb and drb == rb:
            return d
        dll = (d.get("left_entity_label") or "").strip()
        drl = (d.get("right_entity_label") or "").strip()
        if ll and rl and dll == ll and drl == rl:
            return d
    return None


def _classify_item(item: dict, overrides_data: Optional[dict],
                   lv_index: Dict[str, dict]) -> dict:
    """
    Определить readiness_status для одного exclusion_preview item.

    Returns dict с полями readiness_status, blocked_reason (если BLOCKED),
    operator_decision, skip_scope + hard-invariants.
    """
    classification = item.get("classification", "")

    # Достать operator decision
    op_dec = _find_operator_decision(overrides_data, item)
    op_decision: Optional[str] = op_dec.get("operator_decision") if op_dec else None
    op_comment: Optional[str] = op_dec.get("comment") if op_dec else None
    op_updated_at: Optional[str] = op_dec.get("updated_at") if op_dec else None

    # Embedded link_validation из exclusion_preview item
    lv_embedded = item.get("link_validation") or {}
    lv_embedded_decision = (lv_embedded.get("decision")
                            if isinstance(lv_embedded, dict) else None)

    # LV из внешнего отчёта (по block_ids)
    lv_entry = lv_index.get(_lv_key_for_item(item)) or {}
    lv_report_decision = lv_entry.get("decision") if lv_entry else None

    # Итоговое решение link_validation (приоритет: embedded > report)
    effective_lv_decision = lv_embedded_decision or lv_report_decision

    def _result(status: str, reason: Optional[str] = None) -> dict:
        r: dict = {
            "item_id": item.get("item_id", ""),
            "target_type": item.get("target_type", ""),
            "left_block_id": item.get("left_block_id"),
            "right_block_id": item.get("right_block_id"),
            "left_entity_label": item.get("left_entity_label"),
            "right_entity_label": item.get("right_entity_label"),
            "classification": classification,
            "confidence": item.get("confidence"),
            "severity": item.get("severity"),
            "recommended_action": item.get("recommended_action"),
            "readiness_status": status,
            "skip_scope": dict(SKIP_SCOPE_MVP),
            # HARD INVARIANTS
            "auto_apply": False,
            "enforce_allowed": False,
            "requires_explicit_operator_approval": True,
            "operator_decision": op_decision,
            "operator_comment": op_comment,
            "operator_updated_at": op_updated_at,
        }
        if reason is not None:
            r["blocked_reason"] = reason
        return r

    # ── Шаг 1: оператор явно обозначил keep / reject ──────────────────────────
    if op_decision == _OP_KEEP:
        return _result(STATUS_KEEP, KEEP_OPERATOR)
    if op_decision == _OP_REJECT:
        return _result(STATUS_KEEP, KEEP_REJECTED)

    # ── Шаг 2: preview=keep (нет смысла рассматривать как candidate) ──────────
    if classification == CLS_KEEP:
        return _result(STATUS_KEEP, KEEP_PREVIEW_CLS)

    # ── Шаг 3: оператор запросил ручную проверку / link_validation ────────────
    if op_decision == _OP_NEEDS_REVIEW:
        return _result(STATUS_NEEDS_REVIEW, REVIEW_MANUAL)
    if op_decision == _OP_RUN_LV:
        return _result(STATUS_NEEDS_REVIEW, REVIEW_LV_REQUIRED)

    # ── Шаг 4: review_only — нельзя скипнуть без дополнительной проверки ──────
    if classification == CLS_REVIEW:
        return _result(STATUS_NEEDS_REVIEW, REVIEW_ONLY_CLS)

    # ── Шаг 5: link_validation_required ──────────────────────────────────────
    if classification == CLS_LINK_VALIDATION:
        if op_decision == _OP_APPROVE:
            # Оператор вручную одобрил — но блокируем если LV показал valid_mapping
            if effective_lv_decision == "valid_mapping":
                return _result(STATUS_BLOCKED, BLOCKED_VALID_MAPPING)
            return _result(STATUS_READY)
        # Нет оператора или неизвестное решение → нужна link_validation
        return _result(STATUS_NEEDS_REVIEW, REVIEW_ABSENT_LV)

    # ── Шаг 6: candidate_exclude ──────────────────────────────────────────────
    if classification == CLS_EXCLUDE:
        if op_decision == _OP_APPROVE:
            # Блокируем если link_validation показал valid_mapping
            if effective_lv_decision == "valid_mapping":
                return _result(STATUS_BLOCKED, BLOCKED_VALID_MAPPING)
            return _result(STATUS_READY)
        # Нет явного одобрения — blocked (безопасный default)
        return _result(STATUS_BLOCKED, BLOCKED_MISSING_APPROVAL)

    # ── Fallback: неизвестная классификация ───────────────────────────────────
    return _result(STATUS_BLOCKED, BLOCKED_MISSING_APPROVAL)


# ─── публичный API ───────────────────────────────────────────────────────────


def build_skip_readiness_report(
    *,
    session_id: Optional[str] = None,
    pair_id: Optional[str] = None,
    exclusion_preview_report: Optional[dict] = None,
    overrides_report: Optional[dict] = None,
    link_validation_report: Optional[dict] = None,
    options: Optional[dict] = None,
) -> dict:
    """
    Собрать skip_readiness_report из готовых mark-only артефактов.

    Параметры
    ---------
    exclusion_preview_report:
        Содержимое ``exclusion_preview_v2_report.json`` (обязательный вход;
        если None — report получает status="missing_input").
    overrides_report:
        Содержимое ``exclusion_review_overrides.json`` (опционально; fail-soft).
    link_validation_report:
        Содержимое ``link_validation_report.json`` (опционально; fail-soft;
        используется как дополнительный источник lv-решений).
    options:
        Зарезервировано для будущих флагов тюнинга.

    Returns
    -------
    dict — skip_readiness_report со схемой::

        {version, kind, status, session_id, pair_id, created_at,
         summary{...}, items[], warnings[]}

    HARD INVARIANTS гарантируются на уровне report и каждого item.
    """
    options = options or {}
    warnings: List[str] = []

    # ── Проверка входов ───────────────────────────────────────────────────────
    if not isinstance(exclusion_preview_report, dict):
        return {
            "version": REPORT_VERSION,
            "kind": REPORT_KIND,
            "status": "missing_input",
            "session_id": session_id,
            "pair_id": pair_id,
            "created_at": _now_iso(),
            "summary": {
                "items_total": 0,
                "ready_to_skip": 0,
                "blocked": 0,
                "needs_review": 0,
                "keep": 0,
                "operator_approved": 0,
                "operator_rejected": 0,
                "missing_operator_decision": 0,
                "auto_enforce_enabled": False,
            },
            "items": [],
            "warnings": ["exclusion_preview_report not available — cannot compute readiness"],
            # HARD INVARIANTS (report-level)
            "auto_enforce_enabled": False,
            "enforce_allowed": False,
        }

    preview_items = exclusion_preview_report.get("items") or []
    if not isinstance(preview_items, list):
        preview_items = []
        warnings.append("exclusion_preview_report.items is not a list")

    if not isinstance(overrides_report, dict):
        if overrides_report is not None:
            warnings.append("overrides_report invalid type — treated as empty")
        overrides_report = {}

    # ── Построить индексы ─────────────────────────────────────────────────────
    lv_index = _build_lv_index(link_validation_report)

    # ── Классифицировать каждый item ──────────────────────────────────────────
    result_items: List[dict] = []
    counts = {
        STATUS_READY: 0,
        STATUS_BLOCKED: 0,
        STATUS_NEEDS_REVIEW: 0,
        STATUS_KEEP: 0,
    }
    operator_approved = 0
    operator_rejected = 0
    missing_decision = 0

    for item in preview_items:
        if not isinstance(item, dict):
            warnings.append("skipped non-dict item in exclusion_preview_report.items")
            continue
        try:
            classified = _classify_item(item, overrides_report, lv_index)
        except Exception as exc:  # noqa: BLE001 — per-item fail-soft
            warnings.append(f"classify_item failed for {item.get('item_id', '?')}: {exc}")
            classified = {
                "item_id": item.get("item_id", ""),
                "readiness_status": STATUS_BLOCKED,
                "blocked_reason": BLOCKED_MARK_ONLY_SAFETY,
                "classification": item.get("classification", ""),
                "confidence": item.get("confidence"),
                "severity": item.get("severity"),
                "skip_scope": dict(SKIP_SCOPE_MVP),
                "auto_apply": False,
                "enforce_allowed": False,
                "requires_explicit_operator_approval": True,
                "operator_decision": None,
            }

        # Enforce hard invariants
        classified["auto_apply"] = False
        classified["enforce_allowed"] = False
        classified["requires_explicit_operator_approval"] = True

        status = classified.get("readiness_status", STATUS_BLOCKED)
        if status in counts:
            counts[status] += 1
        else:
            counts[STATUS_BLOCKED] += 1

        op_dec = classified.get("operator_decision")
        if op_dec == _OP_APPROVE:
            operator_approved += 1
        elif op_dec in (_OP_REJECT, _OP_KEEP):
            operator_rejected += 1
        elif op_dec is None and item.get("classification") in (CLS_EXCLUDE, CLS_LINK_VALIDATION):
            missing_decision += 1

        result_items.append(classified)

    # ── Итоговый report ───────────────────────────────────────────────────────
    overall_status = "ok"
    if warnings:
        overall_status = "completed_with_warnings"

    report = {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "status": overall_status,
        "session_id": session_id,
        "pair_id": pair_id,
        "created_at": _now_iso(),
        "summary": {
            "items_total": len(result_items),
            "ready_to_skip": counts[STATUS_READY],
            "blocked": counts[STATUS_BLOCKED],
            "needs_review": counts[STATUS_NEEDS_REVIEW],
            "keep": counts[STATUS_KEEP],
            "operator_approved": operator_approved,
            "operator_rejected": operator_rejected,
            "missing_operator_decision": missing_decision,
            # HARD INVARIANT в summary
            "auto_enforce_enabled": False,
        },
        "items": result_items,
        "warnings": warnings,
        # HARD INVARIANTS (report-level, проверяются downstream)
        "auto_enforce_enabled": False,
        "enforce_allowed": False,
    }
    return report


def build_skip_readiness_report_from_dir(
    pipeline_v2_dir: Path,
    *,
    session_id: Optional[str] = None,
    pair_id: Optional[str] = None,
    options: Optional[dict] = None,
) -> dict:
    """
    Читает артефакты из директории pipeline_v2 и строит skip_readiness_report.

    Удобен для автономного (smoke / diagnostics) запуска без dry-run оркестратора.
    Fail-soft: если обязательный ``exclusion_preview_v2_report.json`` отсутствует,
    возвращает report со ``status="missing_input"``.
    """
    xp_path = pipeline_v2_dir / "exclusion_preview_v2_report.json"
    ov_path = pipeline_v2_dir / "exclusion_review_overrides.json"
    lv_path = pipeline_v2_dir / "link_validation_report.json"

    xp_report = _safe_load_json(xp_path)
    ov_report = _safe_load_json(ov_path)
    lv_report = _safe_load_json(lv_path)

    return build_skip_readiness_report(
        session_id=session_id,
        pair_id=pair_id,
        exclusion_preview_report=xp_report,
        overrides_report=ov_report,
        link_validation_report=lv_report,
        options=options,
    )


def write_skip_readiness_report(out_path: "str | Path", report: dict) -> Path:
    """Атомарно записать skip_readiness_report.json (tmp + os.replace)."""
    return _atomic_write_json(Path(out_path), report)


__all__ = [
    "REPORT_VERSION",
    "REPORT_KIND",
    "ARTIFACT_FILENAME",
    "SKIP_SCOPE_MVP",
    "STATUS_READY",
    "STATUS_BLOCKED",
    "STATUS_NEEDS_REVIEW",
    "STATUS_KEEP",
    "BLOCKED_MISSING_APPROVAL",
    "BLOCKED_VALID_MAPPING",
    "BLOCKED_MARK_ONLY_SAFETY",
    "REVIEW_ABSENT_LV",
    "REVIEW_MANUAL",
    "REVIEW_LV_REQUIRED",
    "REVIEW_ONLY_CLS",
    "KEEP_PREVIEW_CLS",
    "KEEP_OPERATOR",
    "KEEP_REJECTED",
    "build_skip_readiness_report",
    "build_skip_readiness_report_from_dir",
    "write_skip_readiness_report",
]
