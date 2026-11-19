"""Очереди анализа нескольких пар: выбранные пары сравниваются по одной.

Инженер отмечает пары на шаге «Загрузка документации», один раз выбирает режим
и модель, и сервер по очереди выполняет для каждой тот же прогон, что кнопка
«Запустить сравнение» строки (``run_production_comparison``, ``DOCUMENT``).

У сессии две независимые очереди — «дорожки» по семейству модели: Codex
(Astra, Sol) и Claude (Opus, Sonnet). Подписки Codex и Claude не делят
мощности, поэтому дорожки идут параллельно; внутри дорожки пары — строго по
одной. Дорожку определяет модель очереди.

Очередь живёт в фоновом потоке процесса backend, поэтому вкладку можно
закрыть. Состояние пишется в ``<session>/production_batch_<дорожка>.json``
(прежний единый ``production_batch.json`` читается как история своей
дорожки); после перезапуска backend незавершённые пункты помечаются
«прервано», сами прогоны повторно не запускаются.
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
FINAL_ITEM = {"PAUSED", "COMPLETED", "REVIEW", "FAILED", "CANCELLED", "STOPPED", "INTERRUPTED", "SKIPPED"}
MAX_ITEMS = 100
#: Дорожки очереди = семейства провайдеров ProjectChange V3 (``ModelProfile.selection``).
LANES = ("codex", "claude")
LANE_LABELS = {"codex": "Codex", "claude": "Claude"}

_lock = threading.RLock()
_threads: dict[tuple[str, str], threading.Thread] = {}
_stop: dict[tuple[str, str], threading.Event] = {}


class BatchConflictError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def lane_for(model_profile: str | None) -> str:
    """Дорожка модели: разрешённый профиль установки или её модель по умолчанию."""
    from backend.app.services.project_change_v3.contracts import resolve_profile
    return resolve_profile(model_profile).selection


def _recorded_lane(batch: dict[str, Any]) -> str:
    """Дорожка уже записанной очереди (в т.ч. прежнего единого файла)."""
    lane = batch.get("lane")
    if lane in LANES:
        return lane
    from backend.app.services.project_change_v3.contracts import DEFAULT_PROFILE_KEY, MODEL_PROFILES
    profile = MODEL_PROFILES.get(str(batch.get("model_profile") or "")) or MODEL_PROFILES[DEFAULT_PROFILE_KEY]
    return profile.selection


def _check_lane(lane: str) -> str:
    if lane not in LANES:
        raise ValueError(f"unknown queue lane {lane!r}")
    return lane


def _legacy_path(session_id: str) -> Path:
    return paths_mod.session_dir(session_id) / "production_batch.json"


def _path(session_id: str, lane: str) -> Path:
    return paths_mod.session_dir(session_id) / f"production_batch_{_check_lane(lane)}.json"


def _write(session_id: str, batch: dict[str, Any]) -> None:
    lane = _recorded_lane(batch)
    batch["lane"] = lane
    path = _path(session_id, lane)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(batch, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def _load(path: Path) -> dict[str, Any] | None:
    try:
        batch = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return batch if isinstance(batch, dict) and batch.get("kind") == KIND else None


def _read(session_id: str, lane: str) -> dict[str, Any] | None:
    batch = _load(_path(session_id, lane))
    if batch is None:
        legacy = _load(_legacy_path(session_id))
        if legacy is not None and _recorded_lane(legacy) == lane:
            batch = legacy
    if batch is not None:
        batch["lane"] = lane
    return batch


def _alive(session_id: str, lane: str) -> bool:
    thread = _threads.get((session_id, lane))
    return bool(thread and thread.is_alive())


def _refresh_pair_statuses(session_id: str, batch: dict[str, Any]) -> None:
    """Present newer individual runs without rewriting the queue's history."""
    for item in batch.get("items") or []:
        if item.get("status") not in FINAL_ITEM or not item.get("pair_id"):
            continue
        state = production_store.load_artifact(session_id, item["pair_id"], "state") or {}
        try:
            newer = datetime.fromisoformat(state["started_at"]) > datetime.fromisoformat(
                item.get("finished_at") or item["started_at"])
        except (KeyError, TypeError, ValueError):
            newer = False
        same_run = bool(state.get("run_id") and state["run_id"] == item.get("run_id"))
        if not newer and not same_run:
            continue
        result = str(state.get("status") or "").upper()
        if state.get("reason_code") == "v3_cancelled":
            result = "CANCELLED"
        if result not in FINAL_ITEM | {"RUNNING"}:
            continue
        item.update(status=result, run_id=state.get("run_id"),
                    message=str(state.get("message") or "")[:500])
        item["completed_at"] = state.get("completed_at") or state.get("finished_at")
        if newer:
            item["started_at"] = state.get("started_at")
            item["finished_at"] = item["completed_at"]


def status(session_id: str, lane: str | None = None, *, repair: bool = True) -> dict[str, Any] | None:
    """Состояние очереди дорожки; очередь, потерянная при перезапуске, — «прервано».

    Без ``lane`` — сводная «главная» очередь для прежних клиентов: идущая, а если
    идущих нет — последняя по времени создания. ``repair=False`` — только чтение.
    """
    if lane is None:
        batches = statuses(session_id, repair=repair)
        running = [b for b in batches if b.get("status") == ACTIVE]
        pool = running or batches
        return max(pool, key=lambda b: str(b.get("created_at") or "")) if pool else None
    _check_lane(lane)
    with _lock:
        batch = _read(session_id, lane)
        if repair and batch and batch.get("status") == ACTIVE and not _alive(session_id, lane):
            for item in batch.get("items") or []:
                if item.get("status") not in FINAL_ITEM:
                    item.update(status="INTERRUPTED", finished_at=_now(),
                                message="Очередь прервана перезапуском сервера; пара не анализировалась.")
            batch.update(status="INTERRUPTED", finished_at=_now())
            _write(session_id, batch)
        if batch:
            _refresh_pair_statuses(session_id, batch)
            batch["lane_label"] = LANE_LABELS[lane]
        return batch


def statuses(session_id: str, *, repair: bool = True) -> list[dict[str, Any]]:
    """Очереди всех дорожек сессии (только существующие), в порядке ``LANES``."""
    return [batch for lane in LANES if (batch := status(session_id, lane, repair=repair))]


def _active_keys(session_id: str, *, except_lane: str) -> dict[tuple[str, str], str]:
    """Пары, стоящие или идущие в других активных дорожках: ключ → дорожка."""
    taken: dict[tuple[str, str], str] = {}
    for lane in LANES:
        if lane == except_lane:
            continue
        batch = status(session_id, lane)
        if not batch or batch.get("status") != ACTIVE:
            continue
        for item in batch.get("items") or []:
            if item.get("status") in ("QUEUED", "RUNNING"):
                taken[(str(item.get("left_pdf") or ""), str(item.get("right_pdf") or ""))] = lane
    return taken


def start(session_id: str, pairs: list[dict[str, str]], *, ai_mode: str | None,
          model_profile: str | None, requested_by: str | None = None,
          comparison_mode: str = "standard",
          parallelism: int = 5, runner: Callable[..., dict[str, Any]] | None = None) -> dict[str, Any]:
    """Поставить пары в очередь и сразу начать первую."""
    if type(parallelism) is not int or parallelism not in (5, 10):
        raise ValueError("parallelism must be 5 or 10")
    if comparison_mode not in ("standard", "subjects"):
        raise ValueError("unknown comparison mode")
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
    lane = lane_for(model_profile)
    with _lock:
        current = status(session_id, lane)
        if current and current.get("status") == ACTIVE:
            raise BatchConflictError(f"Очередь {LANE_LABELS[lane]} этой сессии уже выполняется.")
        taken = _active_keys(session_id, except_lane=lane)
        busy = [item["label"] or item["left_pdf"] for item in items
                if (item["left_pdf"], item["right_pdf"]) in taken]
        if busy:
            raise BatchConflictError("Пары уже стоят в другой очереди анализа: " + "; ".join(busy[:5])
                                     + (f" и ещё {len(busy) - 5}" if len(busy) > 5 else ""))
        batch = {"kind": KIND, "schema_version": 2, "batch_id": uuid.uuid4().hex, "status": ACTIVE, "lane": lane,
                 "ai_mode": ai_mode, "model_profile": model_profile, "parallelism": parallelism, "requested_by": requested_by,
                 "comparison_mode": comparison_mode,
                 "created_at": _now(), "finished_at": None, "stop_requested": False, "items": items}
        _write(session_id, batch)
        stop_event = threading.Event()
        _stop[(session_id, lane)] = stop_event
        thread = threading.Thread(target=_work, args=(session_id, lane, batch["batch_id"], stop_event, runner),
                                  name=f"production-batch-{session_id}-{lane}", daemon=True)
        _threads[(session_id, lane)] = thread
        thread.start()
        return {**batch, "lane_label": LANE_LABELS[lane]}


def stop(session_id: str, lane: str | None = None, *, cancel_current: bool = False,
         requested_by: str | None = None) -> dict[str, Any] | None:
    """Не начинать следующие пары; ``cancel_current`` — остановить и идущую.

    Без ``lane`` останавливаются все идущие очереди сессии.
    """
    for one in ([_check_lane(lane)] if lane else list(LANES)):
        _stop_lane(session_id, one, cancel_current=cancel_current, requested_by=requested_by)
    return status(session_id, lane)


def _stop_lane(session_id: str, lane: str, *, cancel_current: bool, requested_by: str | None) -> None:
    with _lock:
        batch = _read(session_id, lane)
        if not batch or batch.get("status") != ACTIVE:
            return
        event = _stop.get((session_id, lane))
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


def _update(session_id: str, lane: str, batch_id: str, index: int | None = None,
            **fields: Any) -> dict[str, Any] | None:
    with _lock:
        batch = _read(session_id, lane)
        if not batch or batch.get("batch_id") != batch_id:
            return None
        if index is None:
            batch.update(fields)
        else:
            batch["items"][index].update(fields)
        _write(session_id, batch)
        return batch


def _work(session_id: str, lane: str, batch_id: str, stop_event: threading.Event,
          runner: Callable[..., dict[str, Any]] | None) -> None:
    if runner is None:
        from .production_orchestrator import run_production_comparison as runner

    def update(index: int | None = None, **fields: Any) -> None:
        _update(session_id, lane, batch_id, index, **fields)

    try:
        batch = _read(session_id, lane) or {}
        for index, item in enumerate(batch.get("items") or []):
            if stop_event.is_set():
                update(index, status="STOPPED", finished_at=_now(),
                       message="Очередь остановлена до начала анализа этой пары.")
                continue
            update(index, status="RUNNING", started_at=_now(), message="Идёт анализ…")
            try:
                view = store.create_pair(session_id, item["left_pdf"], item["right_pdf"])
                pair_id = str((view.get("pair") or view).get("id") or "")
                if not pair_id:
                    raise ValueError("пара не создана")
                update(index, pair_id=pair_id)
                state = runner(session_id, pair_id, input_mode="DOCUMENT",
                               ai_mode=batch.get("ai_mode"), model_profile=batch.get("model_profile"),
                               comparison_mode=batch.get("comparison_mode", "standard"),
                               parallelism=batch.get("parallelism", 5)) or {}
                result = str(state.get("status") or "FAILED").upper()
                if state.get("reason_code") == "v3_cancelled":
                    result = "CANCELLED"
                update(index, status=result if result in FINAL_ITEM else "COMPLETED",
                       run_id=state.get("run_id") or state.get("generation_run_id"), finished_at=_now(),
                       message=str(state.get("message") or "")[:500])
            except production_store.ProductionConflictError:
                update(index, status="SKIPPED", finished_at=_now(),
                       message="По этой паре уже идёт анализ, запущенный отдельно.")
            except Exception as exc:  # noqa: BLE001 — одна пара не должна ронять очередь
                logger.exception("production batch: pair %s failed", item.get("label"))
                update(index, status="FAILED", finished_at=_now(),
                       message=f"Анализ не выполнен ({type(exc).__name__}).")
        update(status="STOPPED" if stop_event.is_set() else "DONE", finished_at=_now())
    finally:
        with _lock:
            key = (session_id, lane)
            if _threads.get(key) is threading.current_thread():
                _threads.pop(key, None)
                _stop.pop(key, None)
