# -*- coding: utf-8 -*-
"""Pipeline V2 — read-only детализация link validation report для UI.

Превращает ``link_validation_report.json`` в компактный ответ для портала:
summary + отфильтрованный/пагинированный список validation-items. Чистая
трансформация уже прочитанного report-dict.

Гарантии:

* НИЧЕГО не запускает и не пишет; вход — уже прочитанный report-dict;
* raw prompt / raw image / огромные тексты НЕ отдаются (их и нет в report —
  они только в diagnostics); evidence-списки усечены до безопасной длины;
* фильтры ``decision`` / ``agreement`` и пагинация применяются детерминированно;
* инвариант сохраняется в выдаче: ``use_as_grounded_fact`` /
  ``use_for_delta_explanation`` всегда False.
"""
from __future__ import annotations

from typing import Any, Optional

from backend.app.services.stage_comparison.pipeline_v2_link_validation import (
    REPORT_KIND,
)

DETAIL_KIND = REPORT_KIND

_MAX_LIMIT = 500
_STR_CAP = 240
_LABEL_CAP = 80
_MAX_LIST = 12

_DECISIONS = {"valid_mapping", "manual_review", "reject_mapping"}
_DECISION_FILTERS = _DECISIONS | {"all"}
_AGREEMENT_FILTERS = {"agrees", "conflicts", "all"}

_SUMMARY_KEYS = (
    "candidates_total", "attempted", "succeeded", "failed",
    "valid_mapping", "manual_review", "reject_mapping",
    "agrees_with_manual_mapping", "conflicts_with_manual_mapping",
    "orientation_failed",
)

# полезное выше: конфликты первыми (то, что требует внимания инженера)
_STATUS_ORDER = {"done": 0, "failed": 1, "skipped_no_runner": 2}


def _trunc(value: Any, cap: int) -> Optional[str]:
    if value is None:
        return None
    s = value if isinstance(value, str) else str(value)
    s = s.strip()
    if not s:
        return None
    return s if len(s) <= cap else s[: cap - 1] + "…"


def _str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for v in value[:_MAX_LIST]:
        s = _trunc(v, _STR_CAP)
        if s:
            out.append(s)
    return out


def _norm(value: Optional[str], allowed: set, default: str) -> str:
    v = (value or "").strip()
    return v if v in allowed else default


def _validation_safe(v: Any) -> Optional[dict]:
    """Только whitelisted поля validation (никакого raw prompt/image)."""
    if not isinstance(v, dict):
        return None
    return {
        "old_new_orientation_ok": bool(v.get("old_new_orientation_ok")),
        "entity_relation": _trunc(v.get("entity_relation"), _LABEL_CAP),
        "decision": _trunc(v.get("decision"), _LABEL_CAP),
        "confidence": v.get("confidence") if isinstance(
            v.get("confidence"), (int, float)) else None,
        "old_entity_label": _trunc(v.get("old_entity_label"), _LABEL_CAP),
        "new_entity_label": _trunc(v.get("new_entity_label"), _LABEL_CAP),
        "supporting_visual_evidence": _str_list(v.get("supporting_visual_evidence")),
        "conflicting_visual_evidence": _str_list(v.get("conflicting_visual_evidence")),
        "key_devices_old": _str_list(v.get("key_devices_old")),
        "key_devices_new": _str_list(v.get("key_devices_new")),
        "notable_changes": _str_list(v.get("notable_changes")),
        "risks": _str_list(v.get("risks")),
        # инвариант: link-validation никогда не grounded-факт
        "do_not_use_as_fact": True,
    }


def _agreement_safe(a: Any) -> Optional[dict]:
    if not isinstance(a, dict):
        return None
    return {
        "agrees_with_manual_mapping": bool(a.get("agrees_with_manual_mapping")),
        "conflicts_with_manual_mapping": bool(a.get("conflicts_with_manual_mapping")),
        "reason": _trunc(a.get("reason"), _STR_CAP),
    }


def _item_card(it: dict) -> dict:
    return {
        "item_id": _trunc(it.get("item_id"), _LABEL_CAP * 2),
        "mapping_id": _trunc(it.get("mapping_id"), _LABEL_CAP),
        "left_block_id": _trunc(it.get("left_block_id"), _LABEL_CAP),
        "right_block_id": _trunc(it.get("right_block_id"), _LABEL_CAP),
        "left_page_number": it.get("left_page_number"),
        "right_page_number": it.get("right_page_number"),
        "left_entity_label": _trunc(it.get("left_entity_label"), _LABEL_CAP),
        "right_entity_label": _trunc(it.get("right_entity_label"), _LABEL_CAP),
        "manual_decision": _trunc(it.get("manual_decision"), _LABEL_CAP),
        "candidate_kind": _trunc(it.get("candidate_kind"), _LABEL_CAP),
        "candidate_rank": it.get("candidate_rank"),
        "status": str(it.get("status") or ""),
        "validation": _validation_safe(it.get("validation")),
        "agreement": _agreement_safe(it.get("agreement")),
        "recommended_action": _trunc(it.get("recommended_action"), _LABEL_CAP),
        # инварианты mark-only слоя — всегда False в выдаче
        "use_as_grounded_fact": False,
        "use_for_delta_explanation": False,
    }


def _report_version(report: Any) -> int:
    v = report.get("version") if isinstance(report, dict) else None
    return v if isinstance(v, int) else 1


def build_link_validation_detail(
        report: Any, *, session_id: str, pair_id: Optional[str],
        decision: str = "all", agreement: str = "all",
        limit: int = 100, offset: int = 0,
        source: Optional[str] = None,
        extra_warnings: Optional[list[str]] = None) -> dict:
    """Собрать detail-ответ из link validation report-dict (read-only)."""
    decision = _norm(decision, _DECISION_FILTERS, "all")
    agreement = _norm(agreement, _AGREEMENT_FILTERS, "all")
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = 100
    try:
        offset = int(offset)
    except (TypeError, ValueError):
        offset = 0
    limit = max(1, min(_MAX_LIMIT, limit))
    offset = max(0, offset)

    rsummary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    items_in = report.get("items") if isinstance(report.get("items"), list) else []

    cards: list[dict] = []
    for it in items_in:
        if not isinstance(it, dict):
            continue
        card = _item_card(it)
        v = card.get("validation") or {}
        a = card.get("agreement") or {}
        if decision != "all" and v.get("decision") != decision:
            continue
        if agreement == "agrees" and not a.get("agrees_with_manual_mapping"):
            continue
        if agreement == "conflicts" and not a.get("conflicts_with_manual_mapping"):
            continue
        cards.append(card)

    # конфликты выше, затем done/failed/skipped, затем по rank
    cards.sort(key=lambda c: (
        0 if (c.get("agreement") or {}).get("conflicts_with_manual_mapping") else 1,
        _STATUS_ORDER.get(c.get("status"), 9),
        c.get("candidate_rank") if isinstance(c.get("candidate_rank"), int) else 999,
        c.get("item_id") or ""))
    total = len(cards)
    page = cards[offset:offset + limit]

    warnings = [w for w in (report.get("warnings") or []) if isinstance(w, str)][:20]
    warnings += [w for w in (extra_warnings or []) if isinstance(w, str)][:20]

    return {
        "version": _report_version(report),
        "kind": DETAIL_KIND,
        "status": "ok",
        "available": True,
        "session_id": session_id,
        "pair_id": pair_id,
        "source": source,
        "report_status": str(report.get("status") or ""),
        "created_at": _trunc(report.get("created_at"), _LABEL_CAP),
        "summary": {k: rsummary.get(k) for k in _SUMMARY_KEYS if k in rsummary},
        "filters": {"decision": decision, "agreement": agreement},
        "items": page,
        "pagination": {"limit": limit, "offset": offset,
                       "returned": len(page), "total": total},
        "warnings": warnings,
    }


__all__ = ["DETAIL_KIND", "build_link_validation_detail"]
