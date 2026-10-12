"""Очередь анализа нескольких пар: выбранные пары сравниваются по одной.

Инженер отмечает пары на шаге «Загрузка документации», один раз выбирает режим
и модель, и сервер по очереди выполняет для каждой тот же прогон, что кнопка
«Запустить сравнение» строки (``run_production_comparison``, ``DOCUMENT``).
Очередь живёт в фоновом потоке процесса backend, поэтому вкладку можно
закрыть. Состояние пишется в ``<session>/production_batch.json``; после
перезапуска backend незавершённые пункты помечаются «прервано», сами прогоны
повторно не запускаются.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from . import paths as paths_mod
from . import production_store
from . import store

logger = logging.getLogger(__name__)

KIND = "stage_comparison_production_batch"
ACTIVE = "RUNNING"
FINAL_ITEM = {"COMPLETED", "REVIEW", "FAILED", "CANCELLED", "STOPPED", "INTERRUPTED", "SKIPPED"}
MAX_ITEMS = 100

_lock = threading.RLock()
_threads: dict[str, threading.Thread] = {}
_stop: dict[str, threading.Event] = {}


class BatchConflictError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _path(session_id: str) -> Path:
    return paths_mod.session_dir(session_id) / "production_batch.json"


def _write(session_id: str, batch: dict[str, Any]) -> None:
    path = _path(session_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(batch, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def _read(session_id: str) -> dict[str, Any] | None:
    try:
        batch = json.loads(_path(session_id).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return batch if isinstance(batch, dict) and batch.get("kind") == KIND else None


def _alive(session_id: str) -> bool:
    thread = _threads.get(session_id)
    return bool(thread and thread.is_alive())


def status(session_id: str) -> dict[str, Any] | None:
    """Состояние очереди сессии; очередь, потерянная при перезапуске, — «прервано»."""
    with _lock:
        batch = _read(session_id)
        if batch and batch.get("status") == ACTIVE and not _alive(session_id):
            for item in batch.get("items") or []:
                if item.get("status") not in FINAL_ITEM:
                    item.update(status="INTERRUPTED", finished_at=_now(),
                                message="Очередь прервана перезапуском сервера; пара не анализировалась.")
            batch.update(status="INTERRUPTED", finished_at=_now())
            _write(session_id, batch)
        return batch


def start(session_id: str, pairs: list[dict[str, str]], *, ai_mode: str | None,
          model_profile: str | None, requested_by: str | None = None,
          runner: Callable[..., dict[str, Any]] | None = None) -> dict[str, Any]:
    """Поставить пары в очередь и сразу начать первую."""
    if store.get_session(session_id) is None:
        raise KeyError("session_not_found")
    items, seen = [], set()
    for pair in pairs:
        left, right = str(pair.get("left_pdf") or ""), str(pair.get("right_pdf") or "")
        if not left or not right or (left, right) in seen:
            continue
        seen.add((left, right))
        items.append({"left_pdf": left, "right_pdf": right, "label": str(pair.get("label") or "")[:200],
                      "pair_id": None, "status": "QUEUED", "message": "", "run_id": None,
                      "started_at": None, "finished_at": None})
    if not items:
        raise ValueError("Не выбрано ни одной пары документов.")
    if len(items) > MAX_ITEMS:
        raise ValueError(f"За один раз можно поставить не больше {MAX_ITEMS} пар.")
    with _lock:
        current = status(session_id)
        if current and current.get("status") == ACTIVE:
            raise BatchConflictError("Очередь анализа этой сессии уже выполняется.")
        batch = {"kind": KIND, "schema_version": 1, "batch_id": uuid.uuid4().hex, "status": ACTIVE,
                 "ai_mode": ai_mode, "model_profile": model_profile, "requested_by": requested_by,
                 "created_at": _now(), "finished_at": None, "stop_requested": False, "items": items}
        _write(session_id, batch)
        stop = threading.Event()
        _stop[session_id] = stop
        thread = threading.Thread(target=_work, args=(session_id, batch["batch_id"], stop, runner),
                                  name=f"production-batch-{session_id}", daemon=True)
        _threads[session_id] = thread
        thread.start()
        return batch


def stop(session_id: str, *, cancel_current: bool = False, requested_by: str | None = None) -> dict[str, Any] | None:
    """Не начинать следующие пары; ``cancel_current`` — остановить и идущую."""
    with _lock:
        batch = _read(session_id)
        if not batch or batch.get("status") != ACTIVE:
            return status(session_id)
        event = _stop.get(session_id)
        if event:
            event.set()
        batch["stop_requested"] = True
        _write(session_id, batch)
        running = next((i for i in batch["items"] if i.get("status") == "RUNNING"), None)
    if cancel_current and running and running.get("pair_id"):
        from . import production_orchestrator
        try:
            production_orchestrator.cancel_production_comparison(
                session_id, running["pair_id"], requested_by=requested_by)
        except Exception:  # noqa: BLE001 — остановка очереди важнее отмены одной пары
            logger.exception("production batch: cancel of the current pair failed")
    return status(session_id)


def _update(session_id: str, batch_id: str, index: int | None = None, **fields: Any) -> dict[str, Any] | None:
    with _lock:
        batch = _read(session_id)
        if not batch or batch.get("batch_id") != batch_id:
            return None
        if index is None:
            batch.update(fields)
        else:
            batch["items"][index].update(fields)
        _write(session_id, batch)
        return batch


def _work(session_id: str, batch_id: str, stop_event: threading.Event,
          runner: Callable[..., dict[str, Any]] | None) -> None:
    if runner is None:
        from .production_orchestrator import run_production_comparison as runner
    try:
        batch = _read(session_id) or {}
        for index, item in enumerate(batch.get("items") or []):
            if stop_event.is_set():
                _update(session_id, batch_id, index, status="STOPPED", finished_at=_now(),
                        message="Очередь остановлена до начала анализа этой пары.")
                continue
            _update(session_id, batch_id, index, status="RUNNING", started_at=_now(), message="Идёт анализ…")
            try:
                view = store.create_pair(session_id, item["left_pdf"], item["right_pdf"])
                pair_id = str((view.get("pair") or view).get("id") or "")
                if not pair_id:
                    raise ValueError("пара не создана")
                _update(session_id, batch_id, index, pair_id=pair_id)
                state = runner(session_id, pair_id, input_mode="DOCUMENT",
                               ai_mode=batch.get("ai_mode"), model_profile=batch.get("model_profile")) or {}
                result = str(state.get("status") or "FAILED").upper()
                _update(session_id, batch_id, index, status=result if result in FINAL_ITEM else "COMPLETED",
                        run_id=state.get("run_id") or state.get("generation_run_id"), finished_at=_now(),
                        message=str(state.get("message") or "")[:500])
            except production_store.ProductionConflictError:
                _update(session_id, batch_id, index, status="SKIPPED", finished_at=_now(),
                        message="По этой паре уже идёт анализ, запущенный отдельно.")
            except Exception as exc:  # noqa: BLE001 — одна пара не должна ронять очередь
                logger.exception("production batch: pair %s failed", item.get("label"))
                _update(session_id, batch_id, index, status="FAILED", finished_at=_now(),
                        message=f"Анализ не выполнен ({type(exc).__name__}).")
        _update(session_id, batch_id, status="STOPPED" if stop_event.is_set() else "DONE", finished_at=_now())
    finally:
        with _lock:
            if _threads.get(session_id) is threading.current_thread():
                _threads.pop(session_id, None)
                _stop.pop(session_id, None)
