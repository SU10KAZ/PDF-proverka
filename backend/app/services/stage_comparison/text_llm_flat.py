"""Session-level плоский список текстовых смысловых изменений.

Эндпоинт `GET .../text-llm-diff-flat` использует этот модуль чтобы собрать
все `text_llm_diff.json` сессии в один список items[] с привязкой к листу/
странице/slot-у (через `text_location`), плюс сводку (по парам и severity).

Это **read-only агрегатор** — LLM не вызывается, новых файлов не пишется.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from . import store as store_mod
from . import text_llm as text_llm_mod
from . import text_location as text_location_mod

logger = logging.getLogger(__name__)


def _pair_label(pair: dict) -> str:
    """Удобное человекочитаемое имя пары для UI: `A.pdf ↔ B.pdf`."""
    left = (pair or {}).get("left") or {}
    right = (pair or {}).get("right") or {}
    l_name = str(left.get("filename") or "—")
    r_name = str(right.get("filename") or "—")
    return f"{l_name} ↔ {r_name}"


def _sheet_label(left_page: Optional[int], right_page: Optional[int]) -> str:
    """Сформировать видимую метку «Лист N» / «—» для таблицы.

    Если у обеих сторон одна страница — `Лист N`. Если расходятся —
    `Лист A=N / B=M`. Если ни одной — «Не определён».
    """
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
    sev = str(item.get("severity") or "").lower()
    return _SEVERITY_RANK.get(sev, 99)


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def build_flat(session_id: str) -> dict:
    """Собрать flat-список изменений + сводку по всей сессии.

    Возвращает словарь, который роутер отдаёт `as is`:

        {
            "session_id": "...",
            "summary": {
                "total_pairs": N,
                "done_pairs": int,
                "not_run_pairs": int,
                "error_pairs": int,
                "skipped_pairs": int,
                "total_changes": int,
                "by_severity": {"high": x, "medium": y, "low": z, "unknown": w},
                "requires_human_review": int,
            },
            "items": [
                {pair_id, pair_label, left_pdf_name, right_pdf_name,
                 sheet, page, left_page, right_page, alignment_slot,
                 type, category, severity, title, summary,
                 old_value, new_value, construction_impact, cost_impact,
                 requires_human_review, confidence,
                 evidence_left, evidence_right, status: "done"}
            ]
        }

    Если у пары `text_llm_diff.json` отсутствует — пара попадает в
    `not_run_pairs`. status=`error`/`too_large`/`provider_not_available` →
    `error_pairs`. status=`disabled`/`missing_md` → `skipped_pairs`.
    """
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError(f"session not found: {session_id}")

    pairs = session.get("pairs") or []
    summary = {
        "total_pairs": 0,
        "done_pairs": 0,
        "not_run_pairs": 0,
        "error_pairs": 0,
        "skipped_pairs": 0,
        "total_changes": 0,
        "by_severity": {"high": 0, "medium": 0, "low": 0, "unknown": 0},
        "requires_human_review": 0,
    }
    items: list[dict] = []

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        if pair.get("status") == "disabled":
            continue
        summary["total_pairs"] += 1
        pid = str(pair.get("id") or "")
        if not pid:
            continue

        payload = text_llm_mod.get_text_llm_diff(session_id, pid)
        if payload is None:
            summary["not_run_pairs"] += 1
            continue
        status = str(payload.get("status") or "")
        if status == "done":
            summary["done_pairs"] += 1
        elif status in ("error", "too_large", "provider_not_available"):
            summary["error_pairs"] += 1
            continue
        elif status in ("disabled", "missing_md", "blocked"):
            summary["skipped_pairs"] += 1
            continue
        else:
            summary["not_run_pairs"] += 1
            continue

        # Alignment — нужно для location resolver
        try:
            al = store_mod.get_alignment(session_id, pid)
            alignment_items = (al.get("alignment") or {}).get("items") or []
        except Exception:  # noqa: BLE001
            alignment_items = []

        label = _pair_label(pair)
        left_pdf = str((pair.get("left") or {}).get("filename") or "")
        right_pdf = str((pair.get("right") or {}).get("filename") or "")

        for ch in (payload.get("changes") or []):
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

            sev = str(ch.get("severity") or "").lower()
            sev_bucket = sev if sev in ("low", "medium", "high") else "unknown"
            summary["by_severity"][sev_bucket] = summary["by_severity"].get(sev_bucket, 0) + 1
            if bool(ch.get("requires_human_review")):
                summary["requires_human_review"] += 1
            summary["total_changes"] += 1

            ev_left = ch.get("evidence_left") if isinstance(ch.get("evidence_left"), dict) else {}
            ev_right = ch.get("evidence_right") if isinstance(ch.get("evidence_right"), dict) else {}
            left_page = loc.get("left_page")
            right_page = loc.get("right_page")
            page_for_sort = left_page if left_page is not None else right_page
            items.append({
                "pair_id": pid,
                "pair_label": label,
                "left_pdf_name": left_pdf,
                "right_pdf_name": right_pdf,
                "sheet": _sheet_label(left_page, right_page),
                "page": page_for_sort,
                "left_page": left_page,
                "right_page": right_page,
                "alignment_slot": loc.get("alignment_slot"),
                "location_method": loc.get("method") or "not_found",
                "location_confidence": float(loc.get("confidence") or 0.0),
                "id": str(ch.get("id") or ""),
                "type": str(ch.get("type") or "changed"),
                "category": str(ch.get("category") or "other"),
                "severity": sev or "unknown",
                "title": str(ch.get("title") or "").strip(),
                "summary": str(ch.get("summary") or "").strip(),
                "old_value": str(ch.get("old_value") or ""),
                "new_value": str(ch.get("new_value") or ""),
                "construction_impact": str(ch.get("construction_impact") or "").strip(),
                "cost_impact": str(ch.get("cost_impact") or "unknown"),
                "requires_human_review": bool(ch.get("requires_human_review") or False),
                "confidence": float(ch.get("confidence") or 0.0),
                "evidence_left": ev_left,
                "evidence_right": ev_right,
                "status": "done",
            })

    # Сортировка: сначала по PDF-паре, потом по странице, потом по severity.
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
    }


__all__ = ["build_flat"]
