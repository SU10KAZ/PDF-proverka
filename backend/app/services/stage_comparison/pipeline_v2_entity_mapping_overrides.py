# -*- coding: utf-8 -*-
"""Pipeline V2 — ручные override'ы выравнивания сущностей (mark-only).

Инженер подтверждает/отклоняет/перепривязывает пары сущностей OLD↔NEW поверх
``entity_alignment_preview_report.json``. Это **отдельный обратимый artifact**
``entity_mapping_overrides.json`` — он НЕ меняет ни preview-отчёт, ни связи
блоков (``block_link_preview``), ни сравнение, ни findings. Никаких моделей,
сети, jobs.

```
entity_alignment_preview  →  manual entity mapping overrides (этот модуль)
                          →  (будущий) vision selection / Exclusion Preview v2
```

Гарантии:

* atomic write (tmp + ``os.replace``);
* append-only ``history`` (с cap), backup НЕ перетирается молча;
* fail-soft read (битый файл → пустой ok-результат + warning);
* строгая валидация id/session/pair (path traversal обрезается ``_safe_id``,
  значение, которое пришлось бы переписать, отклоняется);
* запись ТОЛЬКО в целевой artifact целевой пары.
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
ARTIFACT_KIND = "stage_comparison_pipeline_v2_entity_mapping_overrides"
ARTIFACT_FILENAME = "entity_mapping_overrides.json"
PIPELINE_V2_DIRNAME = "pipeline_v2"

# допустимые решения
DECISION_SAME = "confirmed_same_entity"
DECISION_RENAME = "confirmed_rename"
DECISION_REORG = "confirmed_reorganized"
DECISION_REJECTED = "rejected_mapping"
DECISION_NO_MATCH = "no_match"
_VALID_DECISIONS = frozenset({
    DECISION_SAME, DECISION_RENAME, DECISION_REORG,
    DECISION_REJECTED, DECISION_NO_MATCH,
})
_CONFIRMED_DECISIONS = frozenset({DECISION_SAME, DECISION_RENAME, DECISION_REORG})

_HISTORY_CAP = 500
_COMMENT_CAP = 2000
_LABEL_CAP = 200
_BY_CAP = 120


class EntityMappingValidationError(ValueError):
    """Невалидный payload override'а (некорректное решение/идентификация)."""


# ─── path resolve (write-aware, без traversal) ───────────────────────────────


def _check_ids(session_id: str, pair_id: str) -> None:
    if not session_id or _safe_id(session_id) != session_id:
        raise EntityMappingValidationError(f"invalid session_id: {session_id!r}")
    if not pair_id or _safe_id(pair_id) != pair_id:
        raise EntityMappingValidationError(f"invalid pair_id: {pair_id!r}")


def overrides_path(session_id: str, pair_id: str, *, create: bool = False) -> Path:
    """Путь к ``entity_mapping_overrides.json`` пары.

    ``create=True`` материализует каталоги (``pair_dir`` + ``pipeline_v2``) для
    записи. ``create=False`` резолвит путь БЕЗ ``mkdir`` (read-only потребители
    не должны материализовать дерево пары при простом GET).
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


def _clean_str(value: Any, cap: int) -> Optional[str]:
    if value is None:
        return None
    s = value if isinstance(value, str) else str(value)
    s = s.strip()
    if not s:
        return None
    return s[:cap]


def _mapping_identity(mapping: dict) -> str:
    """Стабильный ключ идентичности пары (для идемпотентного upsert).

    Приоритет — block ids (точнее), иначе — метки + источник. Один и тот же
    pair при повторном сохранении обновляется на месте, не дублируется.
    """
    lb = _clean_str(mapping.get("left_block_id"), _LABEL_CAP) or ""
    rb = _clean_str(mapping.get("right_block_id"), _LABEL_CAP) or ""
    if lb or rb:
        basis = f"blk|{lb}|{rb}"
    else:
        ll = _clean_str(mapping.get("left_entity_label"), _LABEL_CAP) or ""
        rl = _clean_str(mapping.get("right_entity_label"), _LABEL_CAP) or ""
        sc = _clean_str(mapping.get("source_classification"), _LABEL_CAP) or ""
        basis = f"lab|{ll}|{rl}|{sc}"
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:12]
    return f"m_{digest}"


def empty_overrides(session_id: str, pair_id: str) -> dict:
    return {
        "version": ARTIFACT_VERSION, "kind": ARTIFACT_KIND, "status": "ok",
        "session_id": session_id, "pair_id": pair_id, "updated_at": None,
        "mappings": [], "rejected": [], "no_match": [], "history": [],
    }


def validate_entity_mapping_payload(mapping: Any) -> dict:
    """Проверить и нормализовать payload одного override'а. Бросает на ошибке."""
    if not isinstance(mapping, dict):
        raise EntityMappingValidationError("mapping must be an object")
    decision = mapping.get("manual_decision")
    if decision not in _VALID_DECISIONS:
        raise EntityMappingValidationError(
            f"invalid manual_decision: {decision!r} "
            f"(allowed: {sorted(_VALID_DECISIONS)})")
    out: dict[str, Any] = {
        "left_entity_label": _clean_str(mapping.get("left_entity_label"), _LABEL_CAP),
        "right_entity_label": _clean_str(mapping.get("right_entity_label"), _LABEL_CAP),
        "left_block_id": _clean_str(mapping.get("left_block_id"), _LABEL_CAP),
        "right_block_id": _clean_str(mapping.get("right_block_id"), _LABEL_CAP),
        "left_page_number": _coerce_page(mapping.get("left_page_number")),
        "right_page_number": _coerce_page(mapping.get("right_page_number")),
        "source_classification": _clean_str(mapping.get("source_classification"), _LABEL_CAP),
        "manual_decision": decision,
        "comment": _clean_str(mapping.get("comment"), _COMMENT_CAP),
        "pair_key": _clean_str(mapping.get("pair_key"), _LABEL_CAP),
    }
    # должна быть хоть какая-то идентификация (иначе override «ни о чём»)
    has_left = bool(out["left_entity_label"] or out["left_block_id"])
    has_right = bool(out["right_entity_label"] or out["right_block_id"])
    if not (has_left or has_right):
        raise EntityMappingValidationError(
            "mapping must identify at least one side "
            "(left/right entity_label or block_id)")
    # confirmed_* связывает ДВЕ сущности — нужны обе стороны
    if decision in _CONFIRMED_DECISIONS and not (has_left and has_right):
        raise EntityMappingValidationError(
            f"{decision} requires both left and right identification")
    return out


def _coerce_page(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ─── read (fail-soft) ────────────────────────────────────────────────────────


def read_entity_mapping_overrides(session_id: str, pair_id: str) -> dict:
    """Прочитать overrides (fail-soft). Нет файла → пустой ok-результат."""
    _check_ids(session_id, pair_id)
    path = overrides_path(session_id, pair_id, create=False)
    if not path.is_file():
        return empty_overrides(session_id, pair_id)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 — read обязан быть fail-soft
        out = empty_overrides(session_id, pair_id)
        out["status"] = "error"
        out["warnings"] = [f"{ARTIFACT_FILENAME}: {type(exc).__name__}: {exc}"]
        return out
    if not isinstance(data, dict):
        out = empty_overrides(session_id, pair_id)
        out["status"] = "error"
        out["warnings"] = [f"{ARTIFACT_FILENAME}: expected JSON object"]
        return out
    # нормализация формы (старые/частичные файлы не должны ронять чтение)
    data.setdefault("version", ARTIFACT_VERSION)
    data.setdefault("kind", ARTIFACT_KIND)
    data.setdefault("status", "ok")
    data["session_id"] = session_id
    data["pair_id"] = pair_id
    for key in ("mappings", "rejected", "no_match", "history"):
        if not isinstance(data.get(key), list):
            data[key] = []
    return data


def list_entity_mapping_overrides(session_id: str, pair_id: str) -> list[dict]:
    """Список canonical mappings (все решения)."""
    data = read_entity_mapping_overrides(session_id, pair_id)
    return [m for m in data.get("mappings", []) if isinstance(m, dict)]


def overrides_by_identity(data: Any) -> dict:
    """Индекс mapping_id → override (для интеграции в preview)."""
    out: dict[str, dict] = {}
    if not isinstance(data, dict):
        return out
    for m in data.get("mappings", []) or []:
        if isinstance(m, dict) and m.get("mapping_id"):
            out[m["mapping_id"]] = m
    return out


def index_overrides_for_lookup(data: Any) -> dict:
    """Индексы поиска override'а по разным ключам (для wiring в selection).

    Возвращает ``{by_block_pair, by_pair_key, by_label_pair, by_id}``:
    ключи — (left_block_id, right_block_id) / pair_key / (left_label, right_label)
    / mapping_id. Пустые/None-стороны допускаются (односторонние no_match).
    """
    by_block_pair: dict = {}
    by_pair_key: dict = {}
    by_label_pair: dict = {}
    by_id: dict = {}
    mappings = (data.get("mappings") if isinstance(data, dict) else None) or []
    for m in mappings:
        if not isinstance(m, dict):
            continue
        mid = m.get("mapping_id")
        if mid:
            by_id.setdefault(mid, m)
        lb, rb = m.get("left_block_id"), m.get("right_block_id")
        if lb or rb:
            by_block_pair.setdefault((lb, rb), m)
        pk = m.get("pair_key")
        if pk:
            by_pair_key.setdefault(pk, m)
        ll, rl = m.get("left_entity_label"), m.get("right_entity_label")
        if ll or rl:
            by_label_pair.setdefault((ll, rl), m)
    return {"by_block_pair": by_block_pair, "by_pair_key": by_pair_key,
            "by_label_pair": by_label_pair, "by_id": by_id}


def find_override_for_pair(index: Any, *, left_block_id: Optional[str] = None,
                           right_block_id: Optional[str] = None,
                           pair_key: Optional[str] = None,
                           left_label: Optional[str] = None,
                           right_label: Optional[str] = None,
                           mapping_id: Optional[str] = None) -> Optional[dict]:
    """Найти override по (block-ids → pair_key → labels → mapping_id), в этом
    порядке приоритета. ``index`` — результат :func:`index_overrides_for_lookup`.
    """
    if not isinstance(index, dict):
        return None
    if mapping_id:
        hit = index.get("by_id", {}).get(mapping_id)
        if hit:
            return hit
    if left_block_id is not None or right_block_id is not None:
        hit = index.get("by_block_pair", {}).get((left_block_id, right_block_id))
        if hit:
            return hit
    if pair_key:
        hit = index.get("by_pair_key", {}).get(pair_key)
        if hit:
            return hit
    if left_label is not None or right_label is not None:
        hit = index.get("by_label_pair", {}).get((left_label, right_label))
        if hit:
            return hit
    return None


def summarize_overrides(data: Any) -> dict:
    """Сводка по решениям (для summary preview-эндпоинта)."""
    mappings = (data.get("mappings") if isinstance(data, dict) else None) or []
    confirmed = rejected = no_match = 0
    for m in mappings:
        if not isinstance(m, dict):
            continue
        d = m.get("manual_decision")
        if d in _CONFIRMED_DECISIONS:
            confirmed += 1
        elif d == DECISION_REJECTED:
            rejected += 1
        elif d == DECISION_NO_MATCH:
            no_match += 1
    return {"total": confirmed + rejected + no_match, "confirmed": confirmed,
            "rejected": rejected, "no_match": no_match}


def manual_status_for_decision(decision: Optional[str]) -> str:
    if decision in _CONFIRMED_DECISIONS:
        return "mapped"
    if decision == DECISION_REJECTED:
        return "rejected"
    if decision == DECISION_NO_MATCH:
        return "no_match"
    return "none"


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


def _rebuild_views(data: dict) -> None:
    """Пересобрать derived-списки rejected/no_match из canonical mappings."""
    mappings = data.get("mappings", [])
    data["rejected"] = [m for m in mappings if isinstance(m, dict)
                        and m.get("manual_decision") == DECISION_REJECTED]
    data["no_match"] = [m for m in mappings if isinstance(m, dict)
                        and m.get("manual_decision") == DECISION_NO_MATCH]


def write_entity_mapping_overrides(session_id: str, pair_id: str,
                                   data: dict) -> Path:
    """Атомарно записать весь artifact (после нормализации формы)."""
    _check_ids(session_id, pair_id)
    data.setdefault("version", ARTIFACT_VERSION)
    data.setdefault("kind", ARTIFACT_KIND)
    data["status"] = "ok"
    data["session_id"] = session_id
    data["pair_id"] = pair_id
    for key in ("mappings", "rejected", "no_match", "history"):
        if not isinstance(data.get(key), list):
            data[key] = []
    _rebuild_views(data)
    path = overrides_path(session_id, pair_id, create=True)
    return _atomic_write(path, data)


def upsert_entity_mapping(session_id: str, pair_id: str, mapping: Any, *,
                          created_by: Optional[str] = None,
                          now: Optional[str] = None) -> dict:
    """Создать/обновить один override (идемпотентно по идентичности пары).

    Возвращает ``{"override": <entry>, "created": bool, "summary": {...}}``.
    Бросает :class:`EntityMappingValidationError` на невалидный payload.
    """
    normalized = validate_entity_mapping_payload(mapping)
    ts = now or _now()
    by = _clean_str(created_by, _BY_CAP)
    data = read_entity_mapping_overrides(session_id, pair_id)
    if data.get("status") == "error":
        # битый файл не затираем «в слепую» — это потеря данных; явная ошибка
        raise EntityMappingValidationError(
            "existing overrides file is unreadable; refusing to overwrite")
    mapping_id = _mapping_identity(normalized)
    mappings = data["mappings"]
    existing_idx = next((i for i, m in enumerate(mappings)
                         if isinstance(m, dict) and m.get("mapping_id") == mapping_id),
                        None)
    created = existing_idx is None
    if created:
        entry = {"mapping_id": mapping_id, **normalized,
                 "confidence": "manual_confirmed",
                 "created_at": ts, "created_by": by, "updated_at": ts}
        mappings.append(entry)
        action = "created"
    else:
        entry = mappings[existing_idx]
        entry.update(normalized)
        entry["confidence"] = "manual_confirmed"
        entry.setdefault("created_at", ts)
        entry.setdefault("created_by", by)
        entry["updated_at"] = ts
        action = "updated"
    hist = data["history"]
    hist.append({"mapping_id": mapping_id, "action": action,
                 "manual_decision": normalized["manual_decision"],
                 "at": ts, "by": by})
    if len(hist) > _HISTORY_CAP:
        data["history"] = hist[-_HISTORY_CAP:]
    data["updated_at"] = ts
    write_entity_mapping_overrides(session_id, pair_id, data)
    return {"override": entry, "created": created,
            "summary": summarize_overrides(data)}


def delete_entity_mapping(session_id: str, pair_id: str, mapping_id: str, *,
                          created_by: Optional[str] = None,
                          now: Optional[str] = None) -> dict:
    """Удалить override по mapping_id. Возвращает ``{"deleted": bool, ...}``."""
    mapping_id = _clean_str(mapping_id, _LABEL_CAP) or ""
    if not mapping_id:
        raise EntityMappingValidationError("mapping_id required")
    ts = now or _now()
    by = _clean_str(created_by, _BY_CAP)
    data = read_entity_mapping_overrides(session_id, pair_id)
    if data.get("status") == "error":
        raise EntityMappingValidationError(
            "existing overrides file is unreadable; refusing to overwrite")
    mappings = data["mappings"]
    idx = next((i for i, m in enumerate(mappings)
                if isinstance(m, dict) and m.get("mapping_id") == mapping_id), None)
    if idx is None:
        return {"deleted": False, "summary": summarize_overrides(data)}
    removed = mappings.pop(idx)
    data["history"].append({"mapping_id": mapping_id, "action": "deleted",
                            "manual_decision": removed.get("manual_decision"),
                            "at": ts, "by": by})
    if len(data["history"]) > _HISTORY_CAP:
        data["history"] = data["history"][-_HISTORY_CAP:]
    data["updated_at"] = ts
    write_entity_mapping_overrides(session_id, pair_id, data)
    return {"deleted": True, "summary": summarize_overrides(data)}


__all__ = [
    "ARTIFACT_VERSION", "ARTIFACT_KIND", "ARTIFACT_FILENAME",
    "DECISION_SAME", "DECISION_RENAME", "DECISION_REORG",
    "DECISION_REJECTED", "DECISION_NO_MATCH",
    "EntityMappingValidationError",
    "overrides_path", "empty_overrides", "validate_entity_mapping_payload",
    "read_entity_mapping_overrides", "list_entity_mapping_overrides",
    "write_entity_mapping_overrides", "upsert_entity_mapping",
    "delete_entity_mapping", "overrides_by_identity", "summarize_overrides",
    "index_overrides_for_lookup", "find_override_for_pair",
    "manual_status_for_decision", "_mapping_identity",
]
