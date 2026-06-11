# -*- coding: utf-8 -*-
"""Pipeline V2 — read-only детализация entity alignment preview для UI.

Превращает ``entity_alignment_preview_report.json`` в компактный ответ для
портала: summary-счётчики + отфильтрованный/пагинированный список пар +
unpaired-сущности. Чистая трансформация уже прочитанного report-dict.

Гарантии:

* НИЧЕГО не запускает и не пишет; вход — уже прочитанный report-dict;
* raw full text / raw Qwen response НЕ отдаются (их и нет в report);
* длинные строки (имена листов, reasons) обрезаются до безопасной длины;
* пагинация и фильтр classification применяются детерминированно.
"""
from __future__ import annotations

from typing import Any, Optional

from backend.app.services.stage_comparison.pipeline_v2_entity_alignment_preview import (
    ALIGN_LINK_VALIDATION,
    ALIGN_MISMATCH,
    ALIGN_RENAME,
    ALIGN_SAME,
    ALIGN_SCOPE,
    REPORT_KIND,
)

DETAIL_KIND = REPORT_KIND

_MAX_LIMIT = 500
_SHEET_CAP = 200
_REASON_CAP = 160
_LABEL_CAP = 80
_MAX_REASONS = 12
_MAX_RISK_FLAGS = 12
_MAX_BLOCK_IDS = 100

_CLASSIFICATIONS = {
    ALIGN_SAME, ALIGN_RENAME, ALIGN_SCOPE, ALIGN_MISMATCH, ALIGN_LINK_VALIDATION,
}
_CLASS_FILTERS = _CLASSIFICATIONS | {"all"}

# полезное выше: same → rename → scope → mismatch → link_validation
_CLASS_ORDER = {
    ALIGN_SAME: 0, ALIGN_RENAME: 1, ALIGN_SCOPE: 2,
    ALIGN_MISMATCH: 3, ALIGN_LINK_VALIDATION: 4,
}

_SUMMARY_KEYS = (
    "graphic_pairs_total", "same_entity_likely", "possible_rename",
    "scope_reorganized", "mismatch_likely", "link_validation_candidate",
    "needs_manual_mapping", "unpaired_left", "unpaired_right",
)

# Явный whitelist полей evidence (контракт entity_alignment_preview): только
# скаляры/булевы признаки + короткий visual_status. Любые НЕизвестные ключи
# (в т.ч. потенциальный raw-текст) отбрасываются — гарантия «no raw leak».
_EVIDENCE_KEYS = (
    "sheet_title_similarity", "entity_id_match", "entity_family_match",
    "numbered_entity_conflict", "discipline_match", "graphic_type_match",
    "visual_status", "grounded_entities_overlap", "equipment_overlap_informative",
)


def _trunc(value: Any, cap: int) -> Optional[str]:
    if value is None:
        return None
    s = value if isinstance(value, str) else str(value)
    s = s.strip()
    if not s:
        return None
    return s if len(s) <= cap else s[: cap - 1] + "…"


def _norm_class(value: Optional[str]) -> str:
    v = (value or "").strip()
    return v if v in _CLASS_FILTERS else "all"


def _safe_str_list(value: Any, cap: int, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for v in value[:limit]:
        s = _trunc(v, cap)
        if s:
            out.append(s)
    return out


def _evidence_safe(ev: Any) -> dict:
    """Только whitelisted скалярные/булевые поля evidence (никакого raw-текста).

    Неизвестные ключи (потенциальный raw Qwen / большие anchor-тексты) НЕ
    пробрасываются вовсе — даже усечёнными.
    """
    if not isinstance(ev, dict):
        return {}
    out: dict[str, Any] = {}
    for k in _EVIDENCE_KEYS:
        if k not in ev:
            continue
        v = ev[k]
        if isinstance(v, (bool, int, float)) or v is None:
            out[k] = v
        elif isinstance(v, str):
            out[k] = _trunc(v, _LABEL_CAP)
    return out


def _pair_card(p: dict) -> dict:
    return {
        "pair_key": _trunc(p.get("pair_key"), _LABEL_CAP * 2),
        "left_block_id": _trunc(p.get("left_block_id"), _LABEL_CAP),
        "right_block_id": _trunc(p.get("right_block_id"), _LABEL_CAP),
        "left_page_number": p.get("left_page_number"),
        "right_page_number": p.get("right_page_number"),
        "left_sheet_name": _trunc(p.get("left_sheet_name"), _SHEET_CAP),
        "right_sheet_name": _trunc(p.get("right_sheet_name"), _SHEET_CAP),
        "left_entity_label": _trunc(p.get("left_entity_label"), _LABEL_CAP),
        "right_entity_label": _trunc(p.get("right_entity_label"), _LABEL_CAP),
        "entity_family": _trunc(p.get("entity_family"), _LABEL_CAP),
        "classification": str(p.get("classification") or ""),
        "confidence": p.get("confidence"),
        "recommended_action": _trunc(p.get("recommended_action"), _LABEL_CAP),
        "reasons": _safe_str_list(p.get("reasons"), _REASON_CAP, _MAX_REASONS),
        "risk_flags": _safe_str_list(p.get("risk_flags"), _LABEL_CAP, _MAX_RISK_FLAGS),
        "evidence": _evidence_safe(p.get("evidence")),
    }


def _unpaired_card(e: dict) -> dict:
    block_ids = e.get("block_ids") if isinstance(e.get("block_ids"), list) else []
    return {
        "entity_label": _trunc(e.get("entity_label"), _LABEL_CAP),
        "family": _trunc(e.get("family"), _LABEL_CAP),
        "graphic_type": _trunc(e.get("graphic_type"), _LABEL_CAP),
        "sheet_name": _trunc(e.get("sheet_name"), _SHEET_CAP),
        "block_ids": [_trunc(b, _LABEL_CAP) for b in block_ids[:_MAX_BLOCK_IDS]],
    }


def _unpaired_side(value: Any) -> list[dict]:
    if not isinstance(value, list):
        return []
    return [_unpaired_card(e) for e in value if isinstance(e, dict)]


def _report_version(report: Any) -> int:
    v = report.get("version") if isinstance(report, dict) else None
    return v if isinstance(v, int) else 1


def build_entity_alignment_detail(
        report: Any, *, session_id: str, pair_id: Optional[str],
        classification: str = "all", limit: int = 100, offset: int = 0,
        source: Optional[str] = None,
        extra_warnings: Optional[list[str]] = None) -> dict:
    """Собрать detail-ответ из entity alignment report-dict (read-only)."""
    classification = _norm_class(classification)
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
    pairs_in = report.get("pairs") if isinstance(report.get("pairs"), list) else []
    unpaired_in = report.get("unpaired_entities") if isinstance(
        report.get("unpaired_entities"), dict) else {}

    cards: list[dict] = []
    for p in pairs_in:
        if not isinstance(p, dict):
            continue
        if classification != "all" and str(p.get("classification")) != classification:
            continue
        cards.append(_pair_card(p))
    cards.sort(key=lambda c: (_CLASS_ORDER.get(c["classification"], 9),
                              str(c.get("left_page_number") or ""),
                              c.get("pair_key") or ""))
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
        "summary": {k: rsummary.get(k) for k in _SUMMARY_KEYS if k in rsummary},
        "filters": {"classification": classification},
        "pairs": page,
        "unpaired_entities": {
            "left": _unpaired_side(unpaired_in.get("left")),
            "right": _unpaired_side(unpaired_in.get("right")),
        },
        "pagination": {"limit": limit, "offset": offset,
                       "returned": len(page), "total": total},
        "warnings": warnings,
    }


__all__ = ["DETAIL_KIND", "build_entity_alignment_detail"]
