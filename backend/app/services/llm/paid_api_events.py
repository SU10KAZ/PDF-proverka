"""Append-only журналы платных и заблокированных вызовов.

Файлы:
  - paid_cost_events.jsonl    — каждый успешный paid_cost_tracker.add()
  - paid_api_blocked_events.jsonl — каждый блок от paid_api_guard

Журналы НЕ truncate'ятся (даже при clear_project_usage / reset_display).
Это forensic-источник истины: что отправили в платный API и что заблокировали.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.app.core.config import (
    PAID_COST_EVENTS_FILE,
    PAID_API_BLOCKED_EVENTS_FILE,
)

logger = logging.getLogger(__name__)

_paid_lock = threading.Lock()
_blocked_lock = threading.Lock()


def _append_event(file_path: Path, event: dict[str, Any], lock: threading.Lock) -> None:
    """Атомарный append одной строки JSON в .jsonl-файл."""
    try:
        line = json.dumps(event, ensure_ascii=False)
    except (TypeError, ValueError):
        # На случай не-сериализуемого значения — отбрасываем такие поля
        safe = {k: (v if _is_json_serializable(v) else repr(v)) for k, v in event.items()}
        line = json.dumps(safe, ensure_ascii=False)

    with lock:
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError as e:
            logger.warning("Failed to append event to %s: %s", file_path, e)


def _is_json_serializable(v: Any) -> bool:
    try:
        json.dumps(v)
        return True
    except (TypeError, ValueError):
        return False


def record_paid_event(
    *,
    cost_usd: float,
    model: str = "",
    project_id: str = "",
    version_id: str = "",
    stage: str = "",
    source: str = "",
    manual_run_id: str = "",
    job_id: str = "",
    input_tokens: int = 0,
    output_tokens: int = 0,
    response_id: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """Записать факт реального платного вызова."""
    mrid = manual_run_id or ""
    mrid_present = bool(mrid.strip())
    mrid_hash = (
        hashlib.sha256(mrid.encode("utf-8")).hexdigest()[:12]
        if mrid_present
        else ""
    )
    event = {
        "ts": datetime.now().isoformat(),
        "event": "paid_api_cost",
        "cost_usd": round(float(cost_usd or 0.0), 8),
        "model": model or "",
        "project_id": project_id or "",
        "version_id": version_id or "",
        "stage": stage or "",
        "source": source or "",
        "manual_run_id": mrid,
        "manual_run_id_present": mrid_present,
        "manual_run_id_hash": mrid_hash,
        "job_id": job_id or "",
        "input_tokens": int(input_tokens or 0),
        "output_tokens": int(output_tokens or 0),
        "response_id": response_id or "",
        "pid": os.getpid(),
        "parent_pid": os.getppid(),
    }
    if extra:
        for k, v in extra.items():
            if k not in event:
                event[k] = v
    _append_event(PAID_COST_EVENTS_FILE, event, _paid_lock)


def record_blocked_event(
    *,
    reason: str,
    model: str = "",
    project_id: str = "",
    version_id: str = "",
    stage: str = "",
    source: str = "",
    manual_run_id: str = "",
    job_id: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """Записать факт того, что платный вызов был заблокирован guard'ом."""
    event = {
        "ts": datetime.now().isoformat(),
        "event": "paid_api_blocked",
        "reason": reason or "unknown",
        "model": model or "",
        "project_id": project_id or "",
        "version_id": version_id or "",
        "stage": stage or "",
        "source": source or "",
        "manual_run_id": manual_run_id or "",
        "job_id": job_id or "",
        "pid": os.getpid(),
    }
    if extra:
        for k, v in extra.items():
            if k not in event:
                event[k] = v
    _append_event(PAID_API_BLOCKED_EVENTS_FILE, event, _blocked_lock)


def _read_tail(file_path: Path, limit: int) -> list[dict]:
    """Прочитать последние N строк .jsonl-файла (forensic для UI)."""
    if not file_path.exists() or limit <= 0:
        return []
    out: list[dict] = []
    try:
        # Простой подход: читаем целиком и берём хвост. Файлы не должны вырасти
        # очень сильно (десятки/сотни записей в день).
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for line in lines[-limit:]:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    except OSError:
        pass
    return out


def read_paid_events_tail(limit: int = 100) -> list[dict]:
    return _read_tail(PAID_COST_EVENTS_FILE, limit)


def read_blocked_events_tail(limit: int = 100) -> list[dict]:
    return _read_tail(PAID_API_BLOCKED_EVENTS_FILE, limit)


def count_blocked_today() -> int:
    """Сколько blocked-событий за сегодня (для status endpoint)."""
    today = datetime.now().date().isoformat()
    if not PAID_API_BLOCKED_EVENTS_FILE.exists():
        return 0
    n = 0
    try:
        with open(PAID_API_BLOCKED_EVENTS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                # быстрый pre-filter без полного json.loads
                if today in line[:60]:
                    try:
                        ev = json.loads(line)
                        if ev.get("ts", "").startswith(today):
                            n += 1
                    except json.JSONDecodeError:
                        continue
    except OSError:
        pass
    return n
