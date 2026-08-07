"""Реестр действующих норм, отсутствующих в нормативной базе.

``missing_norms_vault.json`` намеренно хранит только JSON-массив строк:

    ["ГОСТ ...", "СП ..."]

Проекты, замечания, даты и статусы в этом файле не сохраняются. Перед
добавлением и при чтении каждая запись повторно проверяется по единому
индексу статусов: известные базе, отменённые и заменённые документы из
реестра удаляются.
"""
from __future__ import annotations

import json
import re
import threading
from pathlib import Path
from typing import Any, Literal

from backend.app.core.config import MISSING_NORMS_VAULT_FILE
from norms.external_provider import resolve_norm_status

_STORE_PATH = MISSING_NORMS_VAULT_FILE
_REVIEW_RULES_PATH = Path(__file__).resolve().parents[2] / "data" / "missing_norms_review_rules.json"
_lock = threading.Lock()

NormStatus = Literal["pending", "added", "dismissed"]
_REJECTED_STATUSES = {
    "cancelled",
    "canceled",
    "replaced",
    "invalid",
    "dismissed",
}


def _normalize_doc(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("**", "").replace("*", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text.rstrip(".,;: ")


def _doc_key(doc: str) -> str:
    return re.sub(r"\s+", "", doc).replace("_", ".").casefold()


def _deduplicate(docs: list[str]) -> list[str]:
    unique: dict[str, str] = {}
    for raw in docs:
        doc = _normalize_doc(raw)
        if doc:
            unique.setdefault(_doc_key(doc), doc)
    return sorted(unique.values(), key=str.casefold)


def _load_review_rules() -> tuple[dict[str, str], set[str]]:
    """Загрузить подтверждённые человеком исправления и исключения."""
    try:
        data = json.loads(_REVIEW_RULES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}, set()

    normalizations = {
        _doc_key(_normalize_doc(source)): _normalize_doc(target)
        for source, target in (data.get("normalizations") or {}).items()
        if _normalize_doc(source) and _normalize_doc(target)
    }
    excluded = {
        _doc_key(doc)
        for doc in (data.get("excluded") or [])
        if _normalize_doc(doc)
    }
    return normalizations, excluded


def _apply_review_rule(
    doc: str,
    rules: tuple[dict[str, str], set[str]],
) -> str | None:
    """Исправить известную ошибку либо отклонить проверенную плохую запись."""
    normalizations, excluded = rules
    key = _doc_key(doc)
    if key in excluded:
        return None
    return normalizations.get(key, doc)


def _load() -> list[str]:
    """Прочитать новый формат и однократно поддержать старую схему."""
    if not _STORE_PATH.exists():
        return []
    try:
        data = json.loads(_STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(data, list):
        return _deduplicate([item for item in data if isinstance(item, str)])

    # Миграция legacy {"version": 1, "norms": {doc: metadata}}.
    # dismissed уже были проверены человеком как отменённые/заменённые/
    # ошибочные и не должны возвращаться в активный реестр.
    if isinstance(data, dict) and isinstance(data.get("norms"), dict):
        docs = []
        for key, entry in data["norms"].items():
            if isinstance(entry, dict) and entry.get("status") == "dismissed":
                continue
            docs.append(entry.get("doc_number", key) if isinstance(entry, dict) else key)
        return _deduplicate(docs)

    return []


def _save(docs: list[str]) -> None:
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STORE_PATH.write_text(
        json.dumps(_deduplicate(docs), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _item_status(item: dict[str, Any]) -> str:
    value = (
        item.get("doc_status")
        or item.get("status")
        or item.get("effective_status")
        or ""
    )
    return str(value).strip().lower()


def _is_missing_current_norm(doc: str, item: dict[str, Any] | None = None) -> bool:
    """True только для поддержанной нормы, которой действительно нет в базе."""
    if item and _item_status(item) in _REJECTED_STATUSES:
        return False

    try:
        resolved = resolve_norm_status(doc)
    except Exception:
        # При недоступном индексе безопаснее не загрязнять постоянный реестр.
        return False

    if resolved.get("found"):
        return False
    if str(resolved.get("status", "")).lower() in _REJECTED_STATUSES:
        return False
    return (
        resolved.get("resolution_reason") == "not_in_index"
        and bool(resolved.get("supported_family"))
    )


def _reconcile(docs: list[str]) -> list[str]:
    """Удалить всё, что уже есть в базе либо известно как неактуальное."""
    rules = _load_review_rules()
    reviewed = [
        canonical
        for doc in docs
        if (canonical := _apply_review_rule(doc, rules))
        and _is_missing_current_norm(canonical)
    ]
    return _deduplicate(reviewed)


def reconcile_missing_norms() -> int:
    """Привести файл к актуальному простому формату.

    Возвращает количество оставшихся отсутствующих действующих норм.
    """
    with _lock:
        docs = _reconcile(_load())
        _save(docs)
        return len(docs)


def accumulate_from_queue(project_id: str, queue_path: Path) -> int:
    """Добавить из очереди только нормы, отсутствующие в базе и не отменённые."""
    del project_id  # В новом формате источник нормы намеренно не хранится.

    if not queue_path.exists():
        return 0
    try:
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
    except Exception:
        return 0

    if isinstance(queue, dict):
        items = queue.get("items") or queue.get("queue") or []
    elif isinstance(queue, list):
        items = queue
    else:
        return 0

    with _lock:
        docs = _reconcile(_load())
        known_keys = {_doc_key(doc) for doc in docs}
        review_rules = _load_review_rules()
        added = 0

        for raw_item in items:
            item = raw_item if isinstance(raw_item, dict) else {"norm": raw_item}
            if item.get("action", "add_document_to_vault") == "review_family_support":
                continue
            doc = _normalize_doc(
                item.get("norm")
                or item.get("doc_number")
                or item.get("norm_key")
                or ""
            )
            doc = _apply_review_rule(doc, review_rules) if doc else None
            if not doc or not _is_missing_current_norm(doc, item):
                continue
            key = _doc_key(doc)
            if key in known_keys:
                continue
            docs.append(doc)
            known_keys.add(key)
            added += 1

        _save(docs)
        return added


def get_missing_norms(status: NormStatus | None = None) -> list[dict[str, str]]:
    """Вернуть API-представление активного списка.

    Исторические статусы больше не хранятся: в реестре существуют только
    pending-записи. Фильтры added/dismissed поэтому всегда пусты.
    """
    if status not in (None, "", "pending"):
        return []
    with _lock:
        docs = _reconcile(_load())
        _save(docs)
    return [
        {
            "doc_number": doc,
            "family": doc.split(maxsplit=1)[0] if doc else "",
            "status": "pending",
        }
        for doc in docs
    ]


def _remove(doc_number: str) -> bool:
    key = _doc_key(_normalize_doc(doc_number))
    with _lock:
        docs = _load()
        kept = [doc for doc in docs if _doc_key(doc) != key]
        if len(kept) == len(docs):
            return False
        _save(kept)
        return True


def mark_added(doc_number: str) -> bool:
    """После добавления нормы в базу удалить её из missing-реестра."""
    return _remove(doc_number)


def mark_dismissed(doc_number: str) -> bool:
    """Удалить подтверждённо отменённую/ошибочную норму из активного списка."""
    return _remove(doc_number)


def mark_pending(doc_number: str) -> bool:
    """Legacy-операция недоступна: история удалённых записей не хранится."""
    del doc_number
    return False


def get_stats() -> dict[str, int]:
    with _lock:
        docs = _reconcile(_load())
        _save(docs)
    total = len(docs)
    return {"pending": total, "added": 0, "dismissed": 0, "total": total}


def backfill_from_all_projects(projects_dir: Path) -> int:
    """Пересобрать реестр по проектным missing_norms_queue.json."""
    total = 0
    for queue_file in projects_dir.rglob("missing_norms_queue.json"):
        project_id = queue_file.parent.parent.name
        total += accumulate_from_queue(project_id, queue_file)
    return total
