# -*- coding: utf-8 -*-
"""Pipeline V2 — ручные решения оператора по Exclusion Preview v2 (mark-only).

Инженер/оператор фиксирует своё решение по каждому item из
``exclusion_preview_v2_report.json`` в ОТДЕЛЬНЫЙ обратимый артефакт
``exclusion_review_overrides.json``. Это **opt-in layer** — он НЕ исключает
блоки, НЕ применяет skip/enforce, НЕ меняет block links, НЕ создаёт
замечаний, НЕ запускает Qwen/Opus/Claude/jobs.

Решение сохраняется для последующего контролируемого enforce/skip, который
здесь НЕ реализуется.

Гарантии:
* atomic write (tmp + ``os.replace``);
* append-only ``history`` (с cap);
* fail-soft read (битый файл → пустой ok-результат + warning);
* строгая валидация session_id/pair_id (path traversal → ошибка);
* запись ТОЛЬКО в целевой artifact целевой пары;
* никаких вызовов моделей, сети, jobs.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from backend.app.services.stage_comparison.paths import (
    _safe_id,
    pair_dir,
    sessions_root_path,
)

ARTIFACT_VERSION = 1
ARTIFACT_KIND = "stage_comparison_pipeline_v2_exclusion_review_overrides"
ARTIFACT_FILENAME = "exclusion_review_overrides.json"
PIPELINE_V2_DIRNAME = "pipeline_v2"

# допустимые решения оператора
DECISION_APPROVE_EXCLUDE = "approve_exclude"
DECISION_REJECT_EXCLUDE = "reject_exclude"
DECISION_NEEDS_REVIEW = "needs_review"
DECISION_KEEP = "keep"
DECISION_RUN_LINK_VALIDATION = "run_link_validation"
_VALID_DECISIONS = frozenset({
    DECISION_APPROVE_EXCLUDE,
    DECISION_REJECT_EXCLUDE,
    DECISION_NEEDS_REVIEW,
    DECISION_KEEP,
    DECISION_RUN_LINK_VALIDATION,
})

_HISTORY_CAP = 500
_COMMENT_CAP = 2000
_LABEL_CAP = 200
_BY_CAP = 120


class ExclusionReviewValidationError(ValueError):
    """Невалидный payload решения оператора."""


# ─── path resolve ────────────────────────────────────────────────────────────


def _check_ids(session_id: str, pair_id: str) -> None:
    if not session_id or _safe_id(session_id) != session_id:
        raise ExclusionReviewValidationError(f"invalid session_id: {session_id!r}")
    if not pair_id or _safe_id(pair_id) != pair_id:
        raise ExclusionReviewValidationError(f"invalid pair_id: {pair_id!r}")


def overrides_path(session_id: str, pair_id: str, *, create: bool = False) -> Path:
    """Путь к ``exclusion_review_overrides.json`` пары.

    ``create=True`` материализует каталоги для записи. ``create=False``
    резолвит путь БЕЗ ``mkdir`` (read-only потребители не материализуют дерево).
    """
    _check_ids(session_id, pair_id)
    if create:
        pv2 = pair_dir(session_id, pair_id) / PIPELINE_V2_DIRNAME
        pv2.mkdir(parents=True, exist_ok=True)
        return pv2 / ARTIFACT_FILENAME
    return (sessions_root_path() / _safe_id(session_id) / "pairs"
            / _safe_id(pair_id) / PIPELINE_V2_DIRNAME / ARTIFACT_FILENAME)


# ─── helpers ─────────────────────────────────────────────────────────────────


def _now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _clean(value: Any, cap: int) -> Optional[str]:
    if value is None:
        return None
    s = value if isinstance(value, str) else str(value)
    s = s.strip()
    if not s:
        return None
    return s[:cap]


def _decision_id(exclusion_item_id: Optional[str],
                 left_block_id: Optional[str],
                 right_block_id: Optional[str],
                 left_entity_label: Optional[str],
                 right_entity_label: Optional[str]) -> str:
    """Стабильный ключ идентичности решения (для идемпотентного upsert).

    Приоритет — exclusion_item_id (точнее всего), затем block ids, затем labels.
    """
    if _clean(exclusion_item_id, _LABEL_CAP):
        basis = f"xpid::{exclusion_item_id}"
    else:
        lb = _clean(left_block_id, _LABEL_CAP) or ""
        rb = _clean(right_block_id, _LABEL_CAP) or ""
        if lb or rb:
            basis = f"blk::{lb}__{rb}"
        else:
            ll = _clean(left_entity_label, _LABEL_CAP) or ""
            rl = _clean(right_entity_label, _LABEL_CAP) or ""
            basis = f"lab::{ll}__{rl}"
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:12]
    return f"xrd_{digest}"


def empty_overrides(session_id: str, pair_id: str) -> dict:
    return {
        "version": ARTIFACT_VERSION,
        "kind": ARTIFACT_KIND,
        "status": "ok",
        "session_id": session_id,
        "pair_id": pair_id,
        "updated_at": None,
        "decisions": [],
        "history": [],
    }


def validate_exclusion_review_payload(decision: Any) -> dict:
    """Проверить и нормализовать payload одного решения. Бросает на ошибке."""
    if not isinstance(decision, dict):
        raise ExclusionReviewValidationError("decision must be an object")
    op_decision = decision.get("operator_decision")
    if op_decision not in _VALID_DECISIONS:
        raise ExclusionReviewValidationError(
            f"invalid operator_decision: {op_decision!r} "
            f"(allowed: {sorted(_VALID_DECISIONS)})")
    # нужна хоть какая-то идентификация
    iid = _clean(decision.get("exclusion_item_id"), _LABEL_CAP)
    lb = _clean(decision.get("left_block_id"), _LABEL_CAP)
    rb = _clean(decision.get("right_block_id"), _LABEL_CAP)
    ll = _clean(decision.get("left_entity_label"), _LABEL_CAP)
    rl = _clean(decision.get("right_entity_label"), _LABEL_CAP)
    if not (iid or lb or rb or ll or rl):
        raise ExclusionReviewValidationError(
            "decision must identify the item "
            "(exclusion_item_id, left/right block_id or entity_label)")
    return {
        "exclusion_item_id": iid,
        "left_block_id": lb,
        "right_block_id": rb,
        "left_entity_label": ll,
        "right_entity_label": rl,
        "preview_classification": _clean(decision.get("preview_classification"), 80),
        "preview_severity": _clean(decision.get("preview_severity"), 40),
        "operator_decision": op_decision,
        "comment": _clean(decision.get("comment"), _COMMENT_CAP),
    }


# ─── read (fail-soft) ────────────────────────────────────────────────────────


def read_exclusion_review_overrides(session_id: str, pair_id: str) -> dict:
    """Прочитать overrides (fail-soft). Нет файла → пустой ok-результат."""
    _check_ids(session_id, pair_id)
    path = overrides_path(session_id, pair_id, create=False)
    if not path.is_file():
        return empty_overrides(session_id, pair_id)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        out = empty_overrides(session_id, pair_id)
        out["status"] = "error"
        out["warnings"] = [f"{ARTIFACT_FILENAME}: {type(exc).__name__}: {exc}"]
        return out
    if not isinstance(data, dict):
        out = empty_overrides(session_id, pair_id)
        out["status"] = "error"
        out["warnings"] = [f"{ARTIFACT_FILENAME}: expected JSON object"]
        return out
    data.setdefault("version", ARTIFACT_VERSION)
    data.setdefault("kind", ARTIFACT_KIND)
    data.setdefault("status", "ok")
    data["session_id"] = session_id
    data["pair_id"] = pair_id
    for key in ("decisions", "history"):
        if not isinstance(data.get(key), list):
            data[key] = []
    return data


def list_exclusion_review_decisions(session_id: str, pair_id: str) -> list[dict]:
    """Список всех решений оператора."""
    data = read_exclusion_review_overrides(session_id, pair_id)
    return [d for d in data.get("decisions", []) if isinstance(d, dict)]


def index_decisions_by_item(data: Any) -> dict:
    """Индекс decision_id → decision (для wiring в preview endpoint)."""
    out: dict[str, dict] = {}
    if not isinstance(data, dict):
        return out
    for d in data.get("decisions", []) or []:
        if isinstance(d, dict) and d.get("decision_id"):
            out[d["decision_id"]] = d
    # также индексируем по exclusion_item_id (для быстрого lookup)
    for d in data.get("decisions", []) or []:
        if isinstance(d, dict) and d.get("exclusion_item_id"):
            out.setdefault(d["exclusion_item_id"], d)
    return out


def find_decision_for_item(overrides_data: Any, item: dict) -> Optional[dict]:
    """Найти решение оператора для конкретного exclusion item."""
    if not isinstance(overrides_data, dict):
        return None
    decisions = overrides_data.get("decisions") or []
    iid = item.get("item_id") if isinstance(item, dict) else None
    lb = item.get("left_block_id") if isinstance(item, dict) else None
    rb = item.get("right_block_id") if isinstance(item, dict) else None
    ll = item.get("left_entity_label") if isinstance(item, dict) else None
    rl = item.get("right_entity_label") if isinstance(item, dict) else None
    # 1. по exclusion_item_id
    if iid:
        for d in decisions:
            if isinstance(d, dict) and d.get("exclusion_item_id") == iid:
                return d
    # 2. по block ids
    if lb or rb:
        for d in decisions:
            if isinstance(d, dict) and d.get("left_block_id") == lb and d.get("right_block_id") == rb:
                return d
    # 3. по labels
    if ll or rl:
        for d in decisions:
            if isinstance(d, dict) and d.get("left_entity_label") == ll and d.get("right_entity_label") == rl:
                return d
    return None


def summarize_decisions(data: Any) -> dict:
    """Сводка по решениям оператора (для summary endpoint)."""
    decisions = (data.get("decisions") if isinstance(data, dict) else None) or []
    counts: dict[str, int] = {
        "total": 0,
        DECISION_APPROVE_EXCLUDE: 0,
        DECISION_REJECT_EXCLUDE: 0,
        DECISION_NEEDS_REVIEW: 0,
        DECISION_KEEP: 0,
        DECISION_RUN_LINK_VALIDATION: 0,
    }
    for d in decisions:
        if not isinstance(d, dict):
            continue
        op = d.get("operator_decision")
        if op in _VALID_DECISIONS:
            counts[op] = counts.get(op, 0) + 1
            counts["total"] += 1
    return counts


# ─── write (atomic) ──────────────────────────────────────────────────────────


def _atomic_write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp",
                               dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass
    return path


def write_exclusion_review_overrides(session_id: str, pair_id: str,
                                     data: dict) -> Path:
    """Атомарно записать весь artifact (после нормализации формы)."""
    _check_ids(session_id, pair_id)
    data.setdefault("version", ARTIFACT_VERSION)
    data.setdefault("kind", ARTIFACT_KIND)
    data["status"] = "ok"
    data["session_id"] = session_id
    data["pair_id"] = pair_id
    for key in ("decisions", "history"):
        if not isinstance(data.get(key), list):
            data[key] = []
    path = overrides_path(session_id, pair_id, create=True)
    return _atomic_write(path, data)


def upsert_exclusion_review_decision(session_id: str, pair_id: str,
                                     decision: Any, *,
                                     created_by: Optional[str] = None,
                                     now: Optional[str] = None) -> dict:
    """Создать/обновить одно решение (идемпотентно по идентичности item).

    Возвращает ``{"decision": <entry>, "created": bool, "summary": {...}}``.
    Бросает :class:`ExclusionReviewValidationError` на невалидный payload.
    """
    normalized = validate_exclusion_review_payload(decision)
    ts = now or _now()
    by = _clean(created_by, _BY_CAP)
    data = read_exclusion_review_overrides(session_id, pair_id)
    if data.get("status") == "error":
        raise ExclusionReviewValidationError(
            "existing overrides file is unreadable; refusing to overwrite")
    decision_id = _decision_id(
        normalized.get("exclusion_item_id"),
        normalized.get("left_block_id"),
        normalized.get("right_block_id"),
        normalized.get("left_entity_label"),
        normalized.get("right_entity_label"),
    )
    decisions = data["decisions"]
    existing_idx = next(
        (i for i, d in enumerate(decisions)
         if isinstance(d, dict) and d.get("decision_id") == decision_id),
        None,
    )
    created = existing_idx is None
    if created:
        entry = {
            "decision_id": decision_id,
            **normalized,
            "created_by": by,
            "created_at": ts,
            "updated_at": ts,
        }
        decisions.append(entry)
        action = "created"
    else:
        entry = decisions[existing_idx]
        entry.update(normalized)
        entry.setdefault("created_by", by)
        entry.setdefault("created_at", ts)
        entry["updated_at"] = ts
        action = "updated"
    hist = data["history"]
    hist.append({
        "decision_id": decision_id,
        "action": action,
        "operator_decision": normalized["operator_decision"],
        "at": ts,
        "by": by,
    })
    if len(hist) > _HISTORY_CAP:
        data["history"] = hist[-_HISTORY_CAP:]
    data["updated_at"] = ts
    write_exclusion_review_overrides(session_id, pair_id, data)
    return {"decision": entry, "created": created,
            "summary": summarize_decisions(data)}


def delete_exclusion_review_decision(session_id: str, pair_id: str,
                                     decision_id: str, *,
                                     created_by: Optional[str] = None,
                                     now: Optional[str] = None) -> dict:
    """Удалить решение по decision_id. Возвращает ``{"deleted": bool, ...}``."""
    decision_id = _clean(decision_id, _LABEL_CAP) or ""
    if not decision_id:
        raise ExclusionReviewValidationError("decision_id required")
    ts = now or _now()
    by = _clean(created_by, _BY_CAP)
    data = read_exclusion_review_overrides(session_id, pair_id)
    if data.get("status") == "error":
        raise ExclusionReviewValidationError(
            "existing overrides file is unreadable; refusing to overwrite")
    decisions = data["decisions"]
    idx = next(
        (i for i, d in enumerate(decisions)
         if isinstance(d, dict) and d.get("decision_id") == decision_id),
        None,
    )
    if idx is None:
        return {"deleted": False, "summary": summarize_decisions(data)}
    removed = decisions.pop(idx)
    data["history"].append({
        "decision_id": decision_id,
        "action": "deleted",
        "operator_decision": removed.get("operator_decision"),
        "at": ts,
        "by": by,
    })
    if len(data["history"]) > _HISTORY_CAP:
        data["history"] = data["history"][-_HISTORY_CAP:]
    data["updated_at"] = ts
    write_exclusion_review_overrides(session_id, pair_id, data)
    return {"deleted": True, "summary": summarize_decisions(data)}


def operator_review_for_item(overrides_data: Any, item: dict) -> dict:
    """Собрать operator_review-блок для одного exclusion item.

    Возвращает ``{status, decision_id, operator_decision, comment, updated_at}``
    или ``{status: "none"}`` если решения нет.
    """
    d = find_decision_for_item(overrides_data, item)
    if d is None:
        return {"status": "none"}
    return {
        "status": "reviewed",
        "decision_id": d.get("decision_id"),
        "operator_decision": d.get("operator_decision"),
        "comment": d.get("comment"),
        "updated_at": d.get("updated_at"),
    }


__all__ = [
    "ARTIFACT_VERSION", "ARTIFACT_KIND", "ARTIFACT_FILENAME",
    "DECISION_APPROVE_EXCLUDE", "DECISION_REJECT_EXCLUDE",
    "DECISION_NEEDS_REVIEW", "DECISION_KEEP", "DECISION_RUN_LINK_VALIDATION",
    "ExclusionReviewValidationError",
    "overrides_path", "empty_overrides", "validate_exclusion_review_payload",
    "read_exclusion_review_overrides", "list_exclusion_review_decisions",
    "write_exclusion_review_overrides", "upsert_exclusion_review_decision",
    "delete_exclusion_review_decision",
    "index_decisions_by_item", "find_decision_for_item",
    "summarize_decisions", "operator_review_for_item",
    "_decision_id",
]
