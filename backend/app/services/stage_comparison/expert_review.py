"""Экспертная оценка расхождений в сессии «Сравнение стадий».

Хранение — `comparison/sessions/<sid>/expert_review.json`. Ключ — стабильный
raw `id` расхождения из `unified_findings.json` (chg_… либо uf_…). Решение
по группе в UI агрегируется из решений по её `source_finding_ids` — это
автоматически переживает регруппировку (group_id меняется, raw id — нет).

Schema:
{
  "version": 1,
  "updated_at": "<iso>",
  "decisions": {
    "<raw_id>": {
      "decision": "accepted" | "rejected",
      "rejection_reason": "" | "...",
      "reviewer": "",
      "timestamp": "<iso>"
    }
  }
}
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from . import paths as paths_mod

VERSION = 1
_lock = threading.RLock()


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _empty_payload() -> dict:
    return {"version": VERSION, "updated_at": None, "decisions": {}}


def load(session_id: str) -> dict:
    """Прочитать решения; вернуть пустую структуру если файла нет."""
    path = paths_mod.expert_review_path(session_id)
    if not path.exists():
        return _empty_payload()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _empty_payload()
    if not isinstance(data, dict):
        return _empty_payload()
    data.setdefault("version", VERSION)
    data.setdefault("decisions", {})
    if not isinstance(data["decisions"], dict):
        data["decisions"] = {}
    return data


def _summary(decisions: dict) -> dict:
    accepted = 0
    rejected = 0
    for entry in decisions.values():
        if not isinstance(entry, dict):
            continue
        d = (entry.get("decision") or "").lower()
        if d == "accepted":
            accepted += 1
        elif d == "rejected":
            rejected += 1
    return {"accepted": accepted, "rejected": rejected, "total": accepted + rejected}


def get_with_summary(session_id: str) -> dict:
    data = load(session_id)
    return {
        "session_id": session_id,
        "version": data.get("version") or VERSION,
        "updated_at": data.get("updated_at"),
        "decisions": data.get("decisions") or {},
        "summary": _summary(data.get("decisions") or {}),
    }


def apply_batch(
    session_id: str,
    decisions: Iterable[dict],
    removed_ids: Optional[Iterable[str]] = None,
    reviewer: str = "",
) -> dict:
    """Записать пачку решений; вернуть итоговый payload + summary.

    Каждый элемент `decisions` должен содержать:
      - `item_id` (raw id)
      - `decision` ("accepted" | "rejected")
      - `rejection_reason` (optional)

    `removed_ids` — id, для которых решение нужно очистить.
    """
    with _lock:
        data = load(session_id)
        store = data.get("decisions") or {}
        now = _utc_now()
        applied = 0
        for raw in decisions or ():
            if not isinstance(raw, dict):
                continue
            item_id = str(raw.get("item_id") or "").strip()
            decision = str(raw.get("decision") or "").strip().lower()
            if not item_id or decision not in ("accepted", "rejected"):
                continue
            store[item_id] = {
                "decision": decision,
                "rejection_reason": str(raw.get("rejection_reason") or "")[:1000],
                "reviewer": reviewer or "",
                "timestamp": now,
            }
            applied += 1
        for rid in removed_ids or ():
            rid_s = str(rid or "").strip()
            if rid_s and rid_s in store:
                store.pop(rid_s, None)
        data["decisions"] = store
        data["updated_at"] = now
        data["version"] = VERSION
        _atomic_write_json(paths_mod.expert_review_path(session_id), data)
        return {
            "session_id": session_id,
            "applied": applied,
            "removed": len(list(removed_ids or [])),
            "decisions": store,
            "summary": _summary(store),
            "updated_at": now,
        }
