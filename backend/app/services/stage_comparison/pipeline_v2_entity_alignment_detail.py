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
from backend.app.services.stage_comparison.pipeline_v2_entity_mapping_overrides import (
    _mapping_identity,
    manual_status_for_decision,
    summarize_overrides,
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


def _manual_card(ov: Optional[dict]) -> dict:
    """Компактное представление ручного override'а для карточки."""
    if not isinstance(ov, dict):
        return {"status": "none"}
    decision = ov.get("manual_decision")
    return {
        "status": manual_status_for_decision(decision),
        "decision": decision,
        "mapping_id": ov.get("mapping_id"),
        "comment": _trunc(ov.get("comment"), _SHEET_CAP),
        "updated_at": ov.get("updated_at"),
    }


def _build_override_lookup(overrides: Any) -> dict:
    """Индексы для привязки override'ов к карточкам: by_id / by block_id."""
    by_id: dict = {}
    by_left_block: dict = {}
    by_right_block: dict = {}
    mappings = (overrides.get("mappings") if isinstance(overrides, dict) else None) or []
    for m in mappings:
        if not isinstance(m, dict):
            continue
        mid = m.get("mapping_id")
        if mid:
            by_id[mid] = m
        lb, rb = m.get("left_block_id"), m.get("right_block_id")
        if lb:
            by_left_block.setdefault(lb, m)
        if rb:
            by_right_block.setdefault(rb, m)
    return {"by_id": by_id, "by_left_block": by_left_block,
            "by_right_block": by_right_block}


def _manual_for_pair(p: dict, lookup: dict) -> dict:
    """Найти override для пары (по идентичности block-ids; иначе по меткам)."""
    if not lookup:
        return {"status": "none"}
    mid = _mapping_identity({
        "left_block_id": p.get("left_block_id"),
        "right_block_id": p.get("right_block_id"),
        "left_entity_label": p.get("left_entity_label"),
        "right_entity_label": p.get("right_entity_label"),
        "source_classification": p.get("classification"),
    })
    ov = lookup["by_id"].get(mid)
    if ov is None and p.get("left_block_id"):
        cand = lookup["by_left_block"].get(p.get("left_block_id"))
        if cand and cand.get("right_block_id") == p.get("right_block_id"):
            ov = cand
    return _manual_card(ov)


def _manual_for_unpaired(e: dict, side: str, lookup: dict) -> dict:
    """Найти override для односторонней сущности (по её block_id на нужной стороне)."""
    if not lookup:
        return {"status": "none"}
    idx = lookup["by_left_block"] if side == "left" else lookup["by_right_block"]
    for bid in (e.get("block_ids") or []):
        ov = idx.get(bid)
        if ov is not None:
            return _manual_card(ov)
    return {"status": "none"}


def _unpaired_side_with_overrides(value: Any, side: str, lookup: dict) -> list[dict]:
    if not isinstance(value, list):
        return []
    out: list[dict] = []
    for e in value:
        if not isinstance(e, dict):
            continue
        card = _unpaired_card(e)
        card["manual_mapping"] = _manual_for_unpaired(e, side, lookup)
        out.append(card)
    return out


def build_entity_alignment_detail(
        report: Any, *, session_id: str, pair_id: Optional[str],
        classification: str = "all", limit: int = 100, offset: int = 0,
        source: Optional[str] = None, overrides: Any = None,
        extra_warnings: Optional[list[str]] = None) -> dict:
    """Собрать detail-ответ из entity alignment report-dict (read-only).

    ``overrides`` — прочитанный ``entity_mapping_overrides.json`` (или None);
    если задан, к каждой карточке/сущности добавляется ``manual_mapping``, а в
    summary — агрегат ``manual_mapping``. Отсутствие overrides не меняет вывод.
    """
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

    lookup = _build_override_lookup(overrides)

    cards: list[dict] = []
    for p in pairs_in:
        if not isinstance(p, dict):
            continue
        if classification != "all" and str(p.get("classification")) != classification:
            continue
        card = _pair_card(p)
        card["manual_mapping"] = _manual_for_pair(p, lookup)
        cards.append(card)
    cards.sort(key=lambda c: (_CLASS_ORDER.get(c["classification"], 9),
                              str(c.get("left_page_number") or ""),
                              c.get("pair_key") or ""))
    total = len(cards)
    page = cards[offset:offset + limit]

    warnings = [w for w in (report.get("warnings") or []) if isinstance(w, str)][:20]
    warnings += [w for w in (extra_warnings or []) if isinstance(w, str)][:20]

    summary = {k: rsummary.get(k) for k in _SUMMARY_KEYS if k in rsummary}
    if overrides is not None:
        summary["manual_mapping"] = summarize_overrides(overrides)

    return {
        "version": _report_version(report),
        "kind": DETAIL_KIND,
        "status": "ok",
        "available": True,
        "session_id": session_id,
        "pair_id": pair_id,
        "source": source,
        "report_status": str(report.get("status") or ""),
        "summary": summary,
        "filters": {"classification": classification},
        "pairs": page,
        "unpaired_entities": {
            "left": _unpaired_side_with_overrides(unpaired_in.get("left"), "left", lookup),
            "right": _unpaired_side_with_overrides(unpaired_in.get("right"), "right", lookup),
        },
        "pagination": {"limit": limit, "offset": offset,
                       "returned": len(page), "total": total},
        "warnings": warnings,
    }


__all__ = ["DETAIL_KIND", "build_entity_alignment_detail"]
