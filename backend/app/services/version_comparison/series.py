"""«Сравнить все соседние»: серия прогонов (0)→(1), (1)→(2), … (план §9, §15 п. 2).

Пары идут по очереди, каждая — обычным прогоном ``jobs.create_run`` со своим
окном задач и кэшем пар листов: повторная серия без изменений собирается из
кэша. После рестарта незавершённая серия — ``interrupted``, «Досчитать»
продолжает с первой недосчитанной пары, готовые прогоны не повторяются.

Сводка перехода детерминированная: счётчики и фиксированные формулировки,
свободного текста нет.
"""
from __future__ import annotations

import json
import secrets
import threading
import time
from collections import Counter
from datetime import datetime
from typing import Any

from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save

from . import jobs, paths, store, work_queue

_lock = threading.Lock()
_cancel: dict[str, threading.Event] = {}
_threads: dict[str, threading.Thread] = {}
POLL_S = 0.5
ACTIVE = ("queued", "running", "cancelling", "pausing")
RESUMABLE = ("interrupted", "cancelled", "failed", "paused")
_pause: dict[str, threading.Event] = {}


def _update(object_id: str, series_id: str, fn) -> dict[str, Any]:
    return load_modify_save(paths.series_path(object_id, series_id), fn)


def summary(run: dict[str, Any] | None) -> dict[str, Any]:
    """Сводка перехода по прогону: счётчики и детерминированный вывод."""
    if not run:
        return {}
    c = Counter(it.get("result_status") or it["status"] for it in run["items"])
    compared = sum(1 for it in run["items"] if it["kind"] == "compare")
    diff, review = c.get("different", 0), c.get("needs_review", 0) + c.get("unmatched", 0)
    added, removed = c.get("added", 0), c.get("removed", 0)
    zones, same = c.get("same_zones_changed", 0), c.get("same", 0)
    failed = sum(1 for it in run["items"] if it["status"] == "failed")
    style_only = sum(1 for it in run["items"] if it.get("result_status") == "different"
                     and it.get("regions") and it.get("style_regions") == it.get("regions"))
    verdict: list[str] = []
    if run.get("state") != "done":
        verdict.append("переход досчитан не полностью")
    if not diff and not review and not added and not removed and not failed:
        verdict.append("основное содержание совпадает" + (", изменены только служебные зоны" if zones else ""))
    if diff:
        verdict.append(f"есть отличия на {diff} л." + (f" (из них только оформление линий — {style_only})" if style_only else ""))
    if added or removed:
        verdict.append(f"изменён состав (+{added} / −{removed} л.)")
    if review:
        verdict.append(f"нужна проверка: {review}")
    if failed:
        verdict.append(f"ошибка расчёта: {failed}")
    return {"compared": compared, "same": same, "zones_only": zones, "different": diff, "style_only": style_only,
            "needs_review": review, "added": added, "removed": removed, "failed": failed, "verdict": verdict}


def create(object_id: str, project_id: str, *, series_id: str | None = None) -> dict[str, Any]:
    if series_id is not None:
        paths.safe_id(series_id)
        if paths.series_path(object_id, series_id).exists():
            existing = get(object_id, series_id)
            if existing["project_id"] != project_id:
                raise store.CatalogError("идентификатор серии уже занят")
            return existing
    catalog = store.load_catalog(object_id)
    proj = store.project(catalog, project_id)
    order = list(proj["versions"])
    if len(order) < 2:
        raise store.CatalogError("для серии нужно не меньше двух версий")
    if jobs.matrix(object_id, project_id)["state"] != "ready":
        raise store.CatalogError("сопоставление листов ещё не готово")
    for s in list_series(object_id, project_id):
        if s["state"] in ACTIVE:
            raise store.CatalogError("серия по этому проекту уже идёт")

    def label(vid: str) -> str:
        v = catalog["versions"][vid]
        return v.get("user_label") or v.get("file_name") or vid
    series_id = series_id or (datetime.now().strftime("ser%Y%m%d%H%M%S") + secrets.token_hex(2))
    data = {
        "schema": "version_comparison.series/v1", "series_id": series_id, "object_id": object_id,
        "project_id": project_id, "state": "queued", "owner": jobs.BOOT_ID, "owner_process": jobs._identity(),
        "created_at": jobs._now(),
        "pairs": [{"a": a, "b": b, "a_label": label(a), "b_label": label(b), "run_id": None, "state": "queued"}
                  for a, b in zip(order, order[1:])],
    }
    path = paths.series_path(object_id, series_id)
    if work_queue.enabled():
        data["auto_resume"] = True
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, data)
    _start(object_id, series_id)
    return get(object_id, series_id)


def _start(object_id: str, series_id: str) -> None:
    ev = threading.Event()
    t = threading.Thread(target=_job, args=(object_id, series_id, ev), name=f"vc-ser-{series_id[-6:]}", daemon=True)
    with _lock:
        if series_id in _cancel:
            return
        _cancel[series_id] = ev
        _pause[series_id] = threading.Event()
        _threads[series_id] = t
    t.start()


def _job(object_id: str, series_id: str, cancel: threading.Event) -> None:
    with _lock:
        pause = _pause.setdefault(series_id, threading.Event())

    def set_state(state: str):
        def fn(s):
            s["state"] = state
            s["owner"] = jobs.BOOT_ID
            s["owner_process"] = jobs._identity()
            if state in ("done", "cancelled", "failed"):
                s["finished_at"] = jobs._now()
            elif state == "paused":
                s["paused_at"] = jobs._now()
            else:
                s.setdefault("started_at", jobs._now())
                s.pop("paused_at", None)
            return s
        _update(object_id, series_id, fn)

    def set_pair(i: int, **fields):
        def fn(s):
            s["pairs"][i].update(fields)
            return s
        _update(object_id, series_id, fn)

    try:
        set_state("running")
        data = json.loads(paths.series_path(object_id, series_id).read_text(encoding="utf-8"))
        for i, pair in enumerate(data["pairs"]):
            if cancel.is_set() or pause.is_set():
                break
            run = None
            if pair.get("run_id"):
                try:
                    run = jobs.get_run(object_id, pair["run_id"])
                except store.CatalogError:
                    run = None
            if run and run["state"] == "done":
                set_pair(i, state="done")
                continue
            try:
                if run and run["state"] in jobs.RESUMABLE_RUN:
                    run = jobs.resume_run(object_id, run["run_id"])
                elif not run or run["state"] not in ACTIVE:
                    run = jobs.create_run(object_id, data["project_id"], pair["a"], pair["b"], "all")
            except store.CatalogError as exc:
                set_pair(i, state="failed", error=str(exc))
                continue
            set_pair(i, run_id=run["run_id"], state="running")
            while True:
                disk_state = json.loads(paths.series_path(object_id, series_id).read_text()).get("state")
                if disk_state == "cancelling":
                    cancel.set()
                elif disk_state == "pausing":
                    pause.set()
                run = jobs.get_run(object_id, run["run_id"])
                if run["state"] not in jobs.ACTIVE_RUN:
                    break
                if cancel.is_set() and run["state"] != "cancelling":
                    jobs.cancel_run(object_id, run["run_id"])
                elif pause.is_set() and not cancel.is_set() and run["state"] in ("queued", "running"):
                    jobs.pause_run(object_id, run["run_id"])
                time.sleep(POLL_S)
            set_pair(i, state=run["state"] if run["state"] in ("done", "cancelled", "failed", "paused") else "interrupted")
            if run["state"] == "interrupted":
                set_state("interrupted")
                return
        if cancel.is_set():
            set_state("cancelled")
        elif pause.is_set() and any(p.get("state") != "done" for p in _read(object_id, series_id)["pairs"]):
            set_state("paused")
        else:
            set_state("done")
    except Exception as exc:  # pragma: no cover - защитный путь
        _update(object_id, series_id, lambda s: s | {"state": "failed", "error": f"{type(exc).__name__}: {exc}"})
    finally:
        with _lock:
            _cancel.pop(series_id, None)
            _pause.pop(series_id, None)
            _threads.pop(series_id, None)


def get(object_id: str, series_id: str, repair: bool = True) -> dict[str, Any]:
    """Серия с состоянием переходов. ``repair=False`` (viewer) — «ремонт» статуса только в ответе."""
    path = paths.series_path(object_id, series_id)
    if not path.exists():
        raise store.CatalogError("серия не найдена")
    data = json.loads(path.read_text(encoding="utf-8"))
    with _lock:
        alive = series_id in _cancel
    remote_alive = False
    if data.get("owner") != jobs.BOOT_ID:
        from .recovery import owner_alive
        remote_alive = owner_alive(data.get("owner_process"))
    if data.get("state") in ACTIVE and not remote_alive and (data.get("owner") != jobs.BOOT_ID or not alive):
        def mark(s):
            return s | {"state": {"cancelling": "cancelled", "pausing": "paused"}.get(s.get("state"), "interrupted")} \
                if s.get("state") in ACTIVE else s
        data = _update(object_id, series_id, mark) if jobs.may_write(repair) else mark(data)
    for pair in data["pairs"]:
        run = None
        if pair.get("run_id"):
            try:
                run = jobs.get_run(object_id, pair["run_id"], repair)
            except store.CatalogError:
                run = None
        pair["run_state"] = run["state"] if run else None
        pair["progress"] = run.get("progress") if run else None
        pair["summary"] = summary(run)
    data["progress"] = {"total": len(data["pairs"]),
                        "done": sum(1 for p in data["pairs"] if p["run_state"] == "done")}
    return data


def cancel(object_id: str, series_id: str) -> dict[str, Any]:
    with _lock:
        ev = _cancel.get(series_id)
    if ev:
        ev.set()
    _update(object_id, series_id, lambda s: s | {"state": "cancelling"} if s.get("state") in ACTIVE else s)
    return get(object_id, series_id)


def pause(object_id: str, series_id: str) -> dict[str, Any]:
    """Пауза серии: идущие листы текущего перехода досчитываются, следующие листы и переходы не начинаются."""
    with _lock:
        ev = _pause.get(series_id)
    if ev:
        ev.set()
    _update(object_id, series_id, lambda s: s | {"state": "pausing"} if s.get("state") in ("queued", "running") else s)
    return get(object_id, series_id)


def _read(object_id: str, series_id: str) -> dict[str, Any]:
    return json.loads(paths.series_path(object_id, series_id).read_text(encoding="utf-8"))


def _busy_text(data: dict) -> str:
    if data.get("state") in ACTIVE:
        return "серия уже выполняется — продолжать не нужно"
    return "продолжить можно только прерванную, приостановленную или отменённую серию"


def resume(object_id: str, series_id: str) -> dict[str, Any]:
    resumable = RESUMABLE
    data = get(object_id, series_id)
    if data["state"] not in resumable:
        raise store.CatalogError(_busy_text(data))
    fields = {"state": "queued", "owner": jobs.BOOT_ID, "owner_process": jobs._identity()}
    if work_queue.enabled():
        fields["auto_resume"] = True
    claimed = []

    def claim(s):
        # Проверка и захват под одной блокировкой файла: серию ведёт ровно один процесс.
        if s.get("state") not in resumable:
            return s
        claimed.append(True)
        return s | fields
    current = _update(object_id, series_id, claim)
    if not claimed:
        raise store.CatalogError(_busy_text(current))
    _start(object_id, series_id)
    return get(object_id, series_id)


def list_series(object_id: str, project_id: str | None = None, repair: bool = True) -> list[dict[str, Any]]:
    root = paths.series_path(object_id, "x").parent.parent
    out = []
    if not root.exists():
        return out
    for d in root.iterdir():
        if not (d / "series.json").exists():
            continue
        try:
            data = get(object_id, d.name, repair)
        except Exception:
            continue
        if project_id and data.get("project_id") != project_id:
            continue
        out.append(data)
    # Batch series IDs are hashes; their lexical order says nothing about age.
    out.sort(key=lambda s: (s.get("created_at") or "", s["series_id"]), reverse=True)
    return out
