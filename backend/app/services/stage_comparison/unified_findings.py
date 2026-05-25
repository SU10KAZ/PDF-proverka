"""Unified findings + unified-diff-flat: агрегация comparison_result.json по сессии.

Зачем отдельный файл, а не findings.json:
    1. Старый findings.json формируется графическим diff'ом + difflib + text_llm.
    2. Unified pipeline ещё не вытеснил старый — мы держим оба варианта
       параллельно. Слияние в один файл породило бы конфликты типа/категории.
    3. UI читает unified_findings.json через отдельный endpoint
       `/unified-diff-flat`, а старая «Расхождения» UI продолжает работать.

Что хранится:
    comparison/sessions/<sid>/unified_findings.json:
        {
          "version": 1,
          "updated_at": "...",
          "summary": {...},
          "items": [{...flat finding...}]
        }

Каждый finding содержит:
    - pair_id, pair_label, left/right pdf names
    - source_layer: text|image_enrichment|scheme_analysis|table|stamp|mixed
    - type, category, severity, confidence
    - title, summary, old_value, new_value, construction_impact, cost_impact
    - evidence_left/right
    - sheet/page/left_page/right_page/alignment_slot (через text_location)
    - status: "new" (для UI; mutations пока не поддерживаются)
"""
from __future__ import annotations

import json
import logging
import threading
import uuid
from datetime import datetime
from typing import Any, Optional

from . import enriched_comparison as enriched_mod
from . import paths as paths_mod
from . import store as store_mod
from . import text_location as text_location_mod

logger = logging.getLogger(__name__)

VERSION = 1
_lock = threading.RLock()


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _pair_label(pair: dict) -> str:
    left = (pair or {}).get("left") or {}
    right = (pair or {}).get("right") or {}
    return f"{left.get('filename') or '—'} ↔ {right.get('filename') or '—'}"


def _sheet_label(left_page: Optional[int], right_page: Optional[int]) -> str:
    if left_page is None and right_page is None:
        return "Не определён"
    if left_page is not None and right_page is not None and left_page == right_page:
        return f"Лист {left_page}"
    parts = []
    if left_page is not None:
        parts.append(f"A={left_page}")
    if right_page is not None:
        parts.append(f"B={right_page}")
    return "Лист " + " / ".join(parts)


_SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}


def _severity_sort_key(item: dict) -> int:
    return _SEVERITY_RANK.get(str(item.get("severity") or "").lower(), 99)


# ─── IO ──────────────────────────────────────────────────────────────────


def _read_unified(session_id: str) -> dict:
    p = paths_mod.unified_findings_path(session_id)
    if not p.exists():
        return {"version": VERSION, "updated_at": None, "summary": {}, "items": []}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("not a dict")
        return data
    except (OSError, json.JSONDecodeError, ValueError):
        return {"version": VERSION, "updated_at": None, "summary": {}, "items": []}


def _write_unified(session_id: str, payload: dict) -> dict:
    p = paths_mod.unified_findings_path(session_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload.setdefault("version", VERSION)
    payload["updated_at"] = _utc_now()
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return payload


# ─── Core: build flat items ──────────────────────────────────────────────


def _empty_summary() -> dict:
    return {
        "total_pairs": 0,
        "done_pairs": 0,
        "not_run_pairs": 0,
        "error_pairs": 0,
        "skipped_pairs": 0,
        "total_changes": 0,
        "by_source": {
            "text": 0, "image_enrichment": 0, "scheme_analysis": 0,
            "table": 0, "stamp": 0, "mixed": 0,
        },
        "by_severity": {"high": 0, "medium": 0, "low": 0, "unknown": 0},
        "requires_human_review": 0,
    }


def build_unified_flat(session_id: str, pair_id: Optional[str] = None) -> dict:
    """Собрать плоский список unified findings и сводку по сессии.

    Это read-only — не пишет файлы. Используется `unified-diff-flat`
    endpoint. Для persistance — `rebuild_unified_findings`.

    `pair_id`: если задан, summary и items считаются только по этой паре.
    Это нужно UI, чтобы вкладка «Расхождения» по умолчанию была привязана
    к активной PDF-паре и не подмешивала stale findings других пар.
    `pair_modes` тоже фильтруется до одной пары (если найдена).
    """
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError(f"session not found: {session_id}")

    pairs = session.get("pairs") or []
    summary = _empty_summary()
    items: list[dict] = []
    # Per-pair analysis_mode map (UI рисует badge даже если у пары 0 changes).
    pair_modes: list[dict] = []
    filter_pair_id = (pair_id or "").strip() or None

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        if pair.get("status") == "disabled":
            continue
        pid = str(pair.get("id") or "")
        if not pid:
            continue
        # Фильтр по pair_id применяется до summary/items/pair_modes, иначе
        # счётчики total_pairs/not_run/done пересчитаются по всей сессии и
        # UI получит summary, не соответствующий items.
        if filter_pair_id is not None and pid != filter_pair_id:
            continue
        summary["total_pairs"] += 1

        _pm = str(pair.get("analysis_mode") or "block_links")
        if _pm not in ("block_links", "concept_no_block_links"):
            _pm = "block_links"
        pair_modes.append({"pair_id": pid, "analysis_mode": _pm,
                           "pair_label": _pair_label(pair)})

        result = enriched_mod.get_comparison_result(session_id, pid)
        if result is None:
            summary["not_run_pairs"] += 1
            continue

        status = str(result.get("status") or "")
        if status == "done":
            summary["done_pairs"] += 1
        elif status in ("error", "invalid_json", "timeout", "too_large", "provider_not_available"):
            summary["error_pairs"] += 1
            continue
        elif status in ("disabled", "not_ready"):
            summary["skipped_pairs"] += 1
            continue
        else:
            summary["not_run_pairs"] += 1
            continue

        # Получаем alignment_items для location-резолвера.
        try:
            al = store_mod.get_alignment(session_id, pid)
            alignment_items = (al.get("alignment") or {}).get("items") or []
        except Exception:  # noqa: BLE001
            alignment_items = []

        pair_label = _pair_label(pair)
        left_pdf_name = str((pair.get("left") or {}).get("filename") or "")
        right_pdf_name = str((pair.get("right") or {}).get("filename") or "")
        # analysis_mode хранится в pair.json как passthrough. Default — block_links.
        pair_analysis_mode = str(pair.get("analysis_mode") or "block_links")
        if pair_analysis_mode not in ("block_links", "concept_no_block_links"):
            pair_analysis_mode = "block_links"

        for ch in (result.get("changes") or []):
            if not isinstance(ch, dict):
                continue
            try:
                loc = text_location_mod.resolve_text_change_location(
                    pair, ch, alignment_items=alignment_items,
                )
            except Exception:  # noqa: BLE001
                loc = {"left_page": None, "right_page": None,
                       "alignment_slot": None, "confidence": 0.0,
                       "method": "not_found"}

            source_layer = str(ch.get("source") or "text").lower()
            if source_layer not in summary["by_source"]:
                source_layer = "text"
            summary["by_source"][source_layer] += 1

            sev = str(ch.get("severity") or "").lower()
            sev_bucket = sev if sev in ("low", "medium", "high") else "unknown"
            summary["by_severity"][sev_bucket] = summary["by_severity"].get(sev_bucket, 0) + 1
            if bool(ch.get("requires_human_review")):
                summary["requires_human_review"] += 1
            summary["total_changes"] += 1

            left_page = loc.get("left_page")
            right_page = loc.get("right_page")
            page_for_sort = left_page if left_page is not None else right_page
            finding_id = str(ch.get("id") or "").strip() or f"uf_{uuid.uuid4().hex[:10]}"
            items.append({
                "id": finding_id,
                "pair_id": pid,
                "pair_label": pair_label,
                "analysis_mode": pair_analysis_mode,
                "left_pdf_name": left_pdf_name,
                "right_pdf_name": right_pdf_name,
                "sheet": _sheet_label(left_page, right_page),
                "page": page_for_sort,
                "left_page": left_page,
                "right_page": right_page,
                "alignment_slot": loc.get("alignment_slot"),
                "location_method": loc.get("method") or "not_found",
                "location_confidence": float(loc.get("confidence") or 0.0),
                "source_layer": source_layer,
                "type": str(ch.get("type") or "changed"),
                "category": str(ch.get("category") or "general"),
                "severity": sev or "unknown",
                "title": str(ch.get("title") or "").strip(),
                "summary": str(ch.get("summary") or "").strip(),
                "old_value": str(ch.get("old_value") or ""),
                "new_value": str(ch.get("new_value") or ""),
                "construction_impact": str(ch.get("construction_impact") or "").strip(),
                "cost_impact": str(ch.get("cost_impact") or "unknown"),
                "requires_human_review": bool(ch.get("requires_human_review") or False),
                "confidence": float(ch.get("confidence") or 0.0),
                "evidence_left": ch.get("evidence_left") or {},
                "evidence_right": ch.get("evidence_right") or {},
                "status": "new",
            })

    # Сортируем: PDF-пара → страница → severity.
    items.sort(key=lambda it: (
        str(it.get("pair_label") or ""),
        (it.get("page") if it.get("page") is not None else 9999),
        (it.get("alignment_slot") if it.get("alignment_slot") is not None else 9999),
        _severity_sort_key(it),
    ))

    return {
        "session_id": session_id,
        "summary": summary,
        "items": items,
        "pair_modes": pair_modes,
    }


def rebuild_unified_findings(session_id: str) -> dict:
    """Собрать flat и сохранить в unified_findings.json (атомарно)."""
    with _lock:
        flat = build_unified_flat(session_id)
        payload = {
            "version": VERSION,
            "session_id": session_id,
            "updated_at": _utc_now(),
            "summary": flat.get("summary") or _empty_summary(),
            "items": flat.get("items") or [],
            "pair_modes": flat.get("pair_modes") or [],
        }
        return _write_unified(session_id, payload)


def get_unified_findings(session_id: str) -> dict:
    """Прочитать сохранённый unified_findings.json (или вернуть пустую структуру)."""
    return _read_unified(session_id)


__all__ = [
    "VERSION",
    "build_unified_flat",
    "rebuild_unified_findings",
    "get_unified_findings",
]
