"""V2-режим вкладки «Расхождения» — pair-scoped список изменений + ручная
верификация инженера.

Зачем отдельный слой поверх `unified_findings.build_unified_flat`:

    1. **Scope = ТОЛЬКО текущая PDF-пара.** V2 никогда не подмешивает
       изменения других пар сессии. Источник данных — существующий
       `comparison_result.json` конкретной пары (через build_unified_flat
       с `pair_id`), без запуска Qwen/Opus/unified-analysis.

    2. **Ручные статусы хранятся отдельно.** `comparison_result.json` —
       production-артефакт, его мутировать нельзя. Решения инженера
       (подтверждено/отклонено/комментарий/…) лежат в
       `pairs/<pid>/v2_review_status.json` и накладываются на лету.

    3. **Стабильный id.** Чтобы статус «прилипал» к изменению между
       перестроениями списка, id детерминирован:
       `v2_<sha1(pair_id :: raw_id|content)>`. Если у изменения есть
       стабильный `chg_…` id — он берётся за основу; иначе хэшируется
       контент (title + old/new + evidence). build_unified_flat для
       безымянных изменений генерирует случайный `uf_…` id — его мы НЕ
       используем как основу (он не стабилен).

    4. **Quality label не выдумывается.** В production `comparison_result`
       нет поля `quality_label`, поэтому метка ВЫВОДИТСЯ детерминированно
       из реальных полей: `requires_human_review`, `evidence_verified`
       (если есть в fallback-changes) и `confidence`. Ничего не
       синтезируется «из воздуха».
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import datetime
from typing import Any, Optional

from . import paths as paths_mod
from . import unified_findings as unified_findings_mod

logger = logging.getLogger(__name__)

VERSION = 1
_lock = threading.RLock()

# Допустимые статусы ручной верификации (review_status).
VALID_REVIEW_STATUSES = {
    "not_reviewed",
    "confirmed",
    "rejected",
    "needs_clarification",
    "cost_impact",
    "no_cost_impact",
    "send_to_designer",
    "send_to_estimate",
}

# Метки качества, которые мы умеем выводить детерминированно.
QUALITY_LABELS = {
    "good",
    "needs_human_review",
    "questionable",
}


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── Stable id ───────────────────────────────────────────────────────────


def _evidence_quote(value: Any) -> str:
    """Извлекает текстовую цитату из evidence_left/right (dict|str|None)."""
    if isinstance(value, dict):
        return str(value.get("quote") or "")
    if value is None:
        return ""
    return str(value)


def make_v2_id(pair_id: str, item: dict) -> str:
    """Детерминированный стабильный id изменения в рамках пары.

    Приоритет — стабильный `chg_…` id из comparison_result. Если его нет
    (build_unified_flat подставил случайный `uf_…`), хэшируем контент.
    """
    raw_id = str(item.get("id") or "").strip()
    if raw_id and not raw_id.startswith("uf_"):
        base = raw_id
    else:
        base = "".join([
            str(item.get("title") or ""),
            str(item.get("old_value") or ""),
            str(item.get("new_value") or ""),
            _evidence_quote(item.get("evidence_left")),
            _evidence_quote(item.get("evidence_right")),
        ])
    digest = hashlib.sha1(f"{pair_id}::{base}".encode("utf-8")).hexdigest()[:16]
    return f"v2_{digest}"


# ─── Quality label (выводится, не выдумывается) ──────────────────────────


def derive_quality_label(item: dict) -> str:
    """Вывести метку качества из реальных полей изменения.

    - requires_human_review=True       → needs_human_review
    - evidence_verified is False        → questionable (есть в fallback-changes)
    - 0 < confidence < 0.5              → questionable
    - иначе                             → good
    """
    if bool(item.get("requires_human_review")):
        return "needs_human_review"
    ev = item.get("evidence_verified")
    if ev is False:
        return "questionable"
    try:
        conf = float(item.get("confidence") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    if 0.0 < conf < 0.5:
        return "questionable"
    return "good"


# ─── Persisted manual statuses ───────────────────────────────────────────


def _empty_status_file() -> dict:
    return {"version": VERSION, "updated_at": None, "items": {}}


def _read_status_file(session_id: str, pair_id: str) -> dict:
    p = paths_mod.v2_review_status_path(session_id, pair_id)
    if not p.exists():
        return _empty_status_file()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("not a dict")
        if not isinstance(data.get("items"), dict):
            data["items"] = {}
        data.setdefault("version", VERSION)
        return data
    except (OSError, json.JSONDecodeError, ValueError):
        return _empty_status_file()


def _write_status_file(session_id: str, pair_id: str, payload: dict) -> dict:
    p = paths_mod.v2_review_status_path(session_id, pair_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload.setdefault("version", VERSION)
    payload["updated_at"] = _utc_now()
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return payload


# ─── Build pair-scoped V2 changes ────────────────────────────────────────


def _flat_item_to_v2(pair_id: str, it: dict, status_map: dict) -> dict:
    v2_id = make_v2_id(pair_id, it)
    quality = derive_quality_label(it)
    st = status_map.get(v2_id) if isinstance(status_map.get(v2_id), dict) else {}
    review_status = str((st or {}).get("review_status") or "not_reviewed")
    if review_status not in VALID_REVIEW_STATUSES:
        review_status = "not_reviewed"
    return {
        "id": v2_id,
        "pair_id": pair_id,
        "raw_id": str(it.get("id") or ""),
        "sheet": it.get("sheet") or "",
        "page": it.get("page"),
        # location-поля сохраняются для кнопки «Перейти к месту».
        "left_page": it.get("left_page"),
        "right_page": it.get("right_page"),
        "alignment_slot": it.get("alignment_slot"),
        "source_layer": str(it.get("source_layer") or "text"),
        "type": str(it.get("type") or "changed"),
        "category": str(it.get("category") or "general"),
        "severity": str(it.get("severity") or "unknown"),
        "title": str(it.get("title") or ""),
        "summary": str(it.get("summary") or ""),
        "old_value": str(it.get("old_value") or ""),
        "new_value": str(it.get("new_value") or ""),
        "construction_impact": str(it.get("construction_impact") or ""),
        "cost_impact": str(it.get("cost_impact") or "unknown"),
        "evidence_left": _evidence_quote(it.get("evidence_left")),
        "evidence_right": _evidence_quote(it.get("evidence_right")),
        "quality_label": quality,
        "requires_human_review": bool(it.get("requires_human_review") or False),
        "confidence": float(it.get("confidence") or 0.0),
        "review_status": review_status,
        "review_comment": str((st or {}).get("review_comment") or ""),
        "reviewed_by": str((st or {}).get("reviewed_by") or ""),
        "reviewed_at": str((st or {}).get("reviewed_at") or ""),
    }


def build_pair_v2_changes(session_id: str, pair_id: str) -> dict:
    """Собрать V2-список изменений ТОЛЬКО для одной PDF-пары.

    Read-only по отношению к comparison_result. Накладывает сохранённые
    ручные статусы. Бросает KeyError, если сессия не найдена.
    """
    flat = unified_findings_mod.build_unified_flat(session_id, pair_id=pair_id)
    raw_items = flat.get("items") or []
    status_map = (_read_status_file(session_id, pair_id) or {}).get("items") or {}

    items: list[dict] = []
    seen_ids: set[str] = set()
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        v2 = _flat_item_to_v2(pair_id, it, status_map)
        # Коллизия id (одинаковый контент) — добавим суффикс по порядку,
        # чтобы строки не схлопывались в таблице.
        if v2["id"] in seen_ids:
            v2["id"] = f"{v2['id']}_{len(items)}"
        seen_ids.add(v2["id"])
        items.append(v2)

    return {
        "session_id": session_id,
        "pair_id": pair_id,
        "summary": compute_summary(items),
        "items": items,
    }


def compute_summary(items: list[dict]) -> dict:
    summary = {
        "total": 0,
        "high": 0, "medium": 0, "low": 0,
        "good": 0, "needs_human_review": 0, "questionable": 0,
        "confirmed": 0, "rejected": 0, "not_reviewed": 0,
    }
    for it in items:
        summary["total"] += 1
        sev = str(it.get("severity") or "").lower()
        if sev in ("high", "medium", "low"):
            summary[sev] += 1
        ql = str(it.get("quality_label") or "")
        if ql in ("good", "needs_human_review", "questionable"):
            summary[ql] += 1
        rs = str(it.get("review_status") or "not_reviewed")
        if rs == "confirmed":
            summary["confirmed"] += 1
        elif rs == "rejected":
            summary["rejected"] += 1
        elif rs == "not_reviewed":
            summary["not_reviewed"] += 1
    return summary


# ─── Mutations (manual review statuses) ──────────────────────────────────


def _apply_patch_to_entry(entry: dict, patch: dict) -> dict:
    """Применить частичный patch к одной записи статуса. Идемпотентно."""
    out = dict(entry or {})
    touched = False
    if "review_status" in patch and patch["review_status"] is not None:
        rs = str(patch["review_status"])
        if rs not in VALID_REVIEW_STATUSES:
            raise ValueError(f"invalid review_status: {rs}")
        out["review_status"] = rs
        touched = True
    if "review_comment" in patch and patch["review_comment"] is not None:
        out["review_comment"] = str(patch["review_comment"])
        touched = True
    if "reviewed_by" in patch and patch["reviewed_by"] is not None:
        out["reviewed_by"] = str(patch["reviewed_by"])
        touched = True
    if touched:
        out["reviewed_at"] = _utc_now()
    return out


def patch_change(session_id: str, pair_id: str, change_id: str, patch: dict) -> dict:
    """Обновить статус одного изменения. Возвращает обновлённую запись.

    Бросает KeyError, если change_id не принадлежит текущей паре —
    защищает от записи статусов на «фантомные» id.
    """
    with _lock:
        built = build_pair_v2_changes(session_id, pair_id)
        valid_ids = {it["id"] for it in built["items"]}
        if change_id not in valid_ids:
            raise KeyError(change_id)
        data = _read_status_file(session_id, pair_id)
        entry = _apply_patch_to_entry(data["items"].get(change_id) or {}, patch)
        data["items"][change_id] = entry
        _write_status_file(session_id, pair_id, data)
        return entry


def bulk_patch(session_id: str, pair_id: str, ids: list[str], patch: dict) -> dict:
    """Пакетное обновление статусов. Применяется ТОЛЬКО к id текущей пары.

    Возвращает {"updated": [ids], "skipped": [ids]}.
    """
    with _lock:
        built = build_pair_v2_changes(session_id, pair_id)
        valid_ids = {it["id"] for it in built["items"]}
        data = _read_status_file(session_id, pair_id)
        updated: list[str] = []
        skipped: list[str] = []
        for cid in (ids or []):
            cid = str(cid)
            if cid not in valid_ids:
                skipped.append(cid)
                continue
            data["items"][cid] = _apply_patch_to_entry(data["items"].get(cid) or {}, patch)
            updated.append(cid)
        if updated:
            _write_status_file(session_id, pair_id, data)
        return {"updated": updated, "skipped": skipped}


__all__ = [
    "VERSION",
    "VALID_REVIEW_STATUSES",
    "QUALITY_LABELS",
    "make_v2_id",
    "derive_quality_label",
    "build_pair_v2_changes",
    "compute_summary",
    "patch_change",
    "bulk_patch",
]
