"""
Сервис накопительного списка норм, отсутствующих в vault.

После каждой проверки missing_norms_queue.json читается и нормы
добавляются в глобальный файл missing_norms_vault.json.
Пользователь может отметить норму как добавленную в vault.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Literal

from webapp.config import BASE_DIR

_STORE_PATH = BASE_DIR / "webapp" / "data" / "missing_norms_vault.json"
_lock = threading.Lock()

NormStatus = Literal["pending", "added", "dismissed"]


def _load() -> dict:
    if _STORE_PATH.exists():
        try:
            return json.loads(_STORE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"version": 1, "norms": {}}


def _save(data: dict) -> None:
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STORE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def accumulate_from_queue(project_id: str, queue_path: Path) -> int:
    """Добавить нормы из missing_norms_queue.json в глобальный список.

    Returns: количество новых норм (не было раньше).
    """
    if not queue_path.exists():
        return 0
    try:
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
    except Exception:
        return 0

    # Поддерживаем оба формата: items[] и queue[]
    items = queue.get("items") or queue.get("queue") or []
    if not items:
        return 0

    now = datetime.now().isoformat(timespec="seconds")
    added = 0

    with _lock:
        data = _load()
        norms = data.setdefault("norms", {})

        for item in items:
            # Поддерживаем оба формата имён полей
            doc = (item.get("norm") or item.get("doc_number") or item.get("norm_key") or "").strip().rstrip(".")
            if not doc:
                continue
            family = item.get("detected_family") or item.get("family") or ""
            findings = item.get("affected_findings") or []
            action = item.get("action", "add_document_to_vault")

            if action == "review_family_support":
                # ФЗ и другие несупортированные — пропускаем
                continue

            occurrence = {
                "project_id": project_id,
                "findings": findings,
                "seen_at": now,
            }

            if doc not in norms:
                norms[doc] = {
                    "doc_number": doc,
                    "family": family,
                    "status": "pending",
                    "added_to_vault_at": None,
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "occurrences": [occurrence],
                }
                added += 1
            else:
                entry = norms[doc]
                entry["last_seen_at"] = now
                # Добавить occurrence если проект ещё не записан
                existing_projects = {o["project_id"] for o in entry.get("occurrences", [])}
                if project_id not in existing_projects:
                    entry.setdefault("occurrences", []).append(occurrence)
                # Если было dismissed — сбрасываем обратно в pending
                if entry.get("status") == "dismissed":
                    entry["status"] = "pending"
                    added += 1

        _save(data)

    return added


def get_missing_norms(status: NormStatus | None = None) -> list[dict]:
    """Вернуть список норм с опциональной фильтрацией по статусу."""
    with _lock:
        data = _load()
    norms = list(data.get("norms", {}).values())
    if status:
        norms = [n for n in norms if n.get("status") == status]
    # Сортировка: pending первыми, затем по дате последнего появления
    order = {"pending": 0, "added": 1, "dismissed": 2}
    norms.sort(key=lambda n: (order.get(n.get("status", "pending"), 9), n.get("last_seen_at", "")), reverse=False)
    # Добавить агрегированную статистику
    for n in norms:
        occs = n.get("occurrences", [])
        n["project_count"] = len({o["project_id"] for o in occs})
        all_findings = []
        for o in occs:
            all_findings.extend(o.get("findings", []))
        n["finding_count"] = len(set(all_findings))
    return norms


def mark_added(doc_number: str) -> bool:
    """Отметить норму как добавленную в vault."""
    with _lock:
        data = _load()
        entry = data.get("norms", {}).get(doc_number)
        if not entry:
            return False
        entry["status"] = "added"
        entry["added_to_vault_at"] = datetime.now().isoformat(timespec="seconds")
        _save(data)
    return True


def mark_dismissed(doc_number: str) -> bool:
    """Снять норму из списка (не нужна)."""
    with _lock:
        data = _load()
        entry = data.get("norms", {}).get(doc_number)
        if not entry:
            return False
        entry["status"] = "dismissed"
        _save(data)
    return True


def mark_pending(doc_number: str) -> bool:
    """Вернуть норму в список ожидающих."""
    with _lock:
        data = _load()
        entry = data.get("norms", {}).get(doc_number)
        if not entry:
            return False
        entry["status"] = "pending"
        entry["added_to_vault_at"] = None
        _save(data)
    return True


def get_stats() -> dict:
    with _lock:
        data = _load()
    norms = data.get("norms", {}).values()
    return {
        "pending": sum(1 for n in norms if n.get("status") == "pending"),
        "added": sum(1 for n in norms if n.get("status") == "added"),
        "dismissed": sum(1 for n in norms if n.get("status") == "dismissed"),
        "total": len(list(norms)),
    }


def backfill_from_all_projects(projects_dir: Path) -> int:
    """Пройти по всем проектам и накопить existing missing_norms_queue.json."""
    total = 0
    for queue_file in projects_dir.rglob("missing_norms_queue.json"):
        project_id = queue_file.parent.parent.name
        n = accumulate_from_queue(project_id, queue_file)
        total += n
    return total
