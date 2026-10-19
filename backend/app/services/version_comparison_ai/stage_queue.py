"""Очередь запуска этапов со страницы «Проекты и этапы»: выбранные этапы выполняются строго по одному.

Порядок — проект за проектом (как выбраны), внутри проекта — по столбцам: ИИ-проверка сомнительных пар (Луна) →
сравнение соседних → ИИ-описание изменений (обычные листы → полные спецификации → сводка MD) → PDF-отчёт.
Каждый столбец — свой этап: ИИ-проверка пар других этапов не запускает. Весь процесс по проекту целиком —
только ``pipeline=True`` (POST ai-sheet-match без ``luna_only``; кнопки в интерфейсе с 19.10.2026 нет). Следующий этап начинается, когда предыдущий закончился. Состояние этапов читается из обзора
(``version_comparison.overview``) — того же, что видит пользователь.

Файл очереди — ``objects/<объект>/stage_queue.json``; шаг очереди — фоновый поток роутера ИИ (``dispatch_loop``).
Пауза очереди ставит на паузу текущий этап (начатое доводится) и не берёт следующие; «Продолжить» —
продолжает текущий и идёт дальше. Этап, поставленный на паузу кнопкой в строке, тоже останавливает очередь.
"""
from __future__ import annotations

import json
import fcntl
import logging
import threading
import time
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Any

from backend.app.services.common.atomic_json import load_modify_save
from backend.app.services.version_comparison import jobs as vc_jobs
from backend.app.services.version_comparison import overview, paths, series as vc_series, store

from . import settings, sheet_match, whole_specifications
from .md_report import runner as md_report
from .sheet_changes import series_ai, series_summary

SCHEMA = "version_comparison_ai.stage_queue/v1"
STAGES = ("luna", "series", "ai", "specifications", "summary", "report")
PIPELINE = STAGES[:5]
STAGE_LABELS = {"luna": "ИИ-проверка сомнительных пар", "series": "Сравнение соседних",
                "ai": "ИИ-описание изменений: обычные листы", "specifications": "ИИ-описание изменений: спецификации",
                "summary": "Сводка изменений (MD)", "report": "PDF-отчёт"}
# Столбец «ИИ-описание изменений» — три шага подряд; ошибка любого останавливает очередь, сводка — только после всех.
AI_CHAIN = ("ai", "specifications", "summary")
AI_STAGES = ("luna", "ai", "specifications", "report")
TICK_S = 3
GRACE_S = 6           # после запуска этапа обзор может ещё показывать прежнее состояние
BUSY = ("running", "queued", "pausing")
FINISHED = {"luna": ("done", "attention", "skip"), "series": ("done",), "ai": ("done", "partial"),
            "specifications": ("done",), "summary": ("done",), "report": ("done",)}
_lock = threading.RLock()
log = logging.getLogger(__name__)


class QueueError(ValueError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _path(object_id: str):
    return paths.object_dir(object_id) / "stage_queue.json"


def _empty(object_id: str) -> dict[str, Any]:
    return {"schema": SCHEMA, "object_id": object_id, "state": "idle", "items": []}


def _load(object_id: str) -> dict[str, Any]:
    p = _path(object_id)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else _empty(object_id)


def _modify(object_id: str, fn) -> dict[str, Any]:
    return load_modify_save(_path(object_id), fn, default=_empty(object_id))


def status(object_id: str) -> dict[str, Any]:
    q = _load(object_id)
    items = q["items"]
    q["counts"] = {s: sum(1 for it in items if it["state"] == s)
                   for s in ("pending", "running", "paused", "done", "failed", "skipped")}
    q["current"] = next((it for it in items if it["state"] in ("running", "paused")), None)
    q["progress"] = {"done": sum(it["state"] in ("done", "skipped") for it in items), "total": len(items)}
    return q


# ------------------------------------------------------------------ постановка

def enqueue(object_id: str, items: list[dict], *, consent: bool, user: str | None = None, pipeline: bool = False) -> dict[str, Any]:
    """Добавить этапы в конец очереди. Порядок: проекты — как переданы, этапы проекта — по порядку столбцов."""
    catalog = store.load_catalog(object_id)
    known = {p["project_id"]: p for p in catalog["projects"]}
    order: list[str] = []
    wanted: dict[str, set[str]] = {}
    restart: set[str] = set()
    for it in items:
        pid, stage = it.get("project_id"), it.get("stage")
        if pid not in known:
            raise QueueError("проект не найден")
        if stage not in STAGES:
            raise QueueError(f"неизвестный этап: {stage}")
        if pid not in wanted:
            order.append(pid)
            wanted[pid] = set()
        wanted[pid].add(stage)
        if pipeline:
            wanted[pid].update(PIPELINE)
        elif stage == "ai":
            wanted[pid].update(AI_CHAIN)
        if it.get("restart"):
            restart.add(pid)
    if not order:
        raise QueueError("ничего не выбрано")
    if any(s in AI_STAGES for st in wanted.values() for s in st):
        if not settings.enabled():
            raise QueueError("ИИ в «Сравнении версий» выключен — Луну, ИИ-описание и отчёт запустить нельзя")
        if not consent:
            raise QueueError("нужно подтвердить передачу материалов исполнителю")

    def add(q):
        busy = {(it["project_id"], it["stage"]) for it in q["items"] if it["state"] in ("pending", "running", "paused")}
        for pid in order:
            for stage in STAGES:
                if stage in wanted[pid] and (pid, stage) not in busy:
                    proj = known[pid]
                    q["items"].append({"project_id": pid, "code": proj.get("code") or proj.get("title") or pid,
                                       "stage": stage, "label": STAGE_LABELS[stage], "state": "pending",
                                       "pipeline": pipeline or (stage != "luna" and "ai" in wanted[pid]),
                                       "queued_at": _now(), "user": user, "consent": bool(consent)})
                    if stage == "ai" and pid in restart:
                        q["items"][-1]["restart"] = True
        if q.get("state") in (None, "idle", "done"):
            q["state"] = "running"
        return q
    with _lock:
        _modify(object_id, add)
    dispatch(object_id)
    return status(object_id)


def pause(object_id: str) -> dict[str, Any]:
    """Пауза очереди: текущий этап — на паузу (начатое доводится), следующие не начинаются."""
    with _lock:
        q = _load(object_id)
        idx = next((i for i, it in enumerate(q["items"]) if it["state"] == "running"), None)
        if idx is not None:
            try:
                _pause_stage(object_id, q["items"][idx])
            except Exception as exc:  # noqa: BLE001 — этап уже закончился или не ставится на паузу
                log.info("очередь этапов: пауза %s: %s", q["items"][idx]["code"], exc)
            _set(object_id, idx, state="paused")
        _modify(object_id, lambda d: d | {"state": "paused", "paused_at": _now()})
    return status(object_id)


def resume(object_id: str) -> dict[str, Any]:
    """Продолжить: текущий этап — с того же места, затем следующие."""
    def go(q):
        for it in q["items"]:
            if it["state"] in ("paused", "failed"):
                it["state"], it["resume"] = "running", True
                it.pop("retried", None)
                break
        q["state"] = "running"
        q.pop("paused_at", None)
        return q
    with _lock:
        _modify(object_id, go)
    dispatch(object_id)
    return status(object_id)


def _process(items: list[dict]) -> bool:
    """В очереди «весь процесс» (кнопка над матрицами): его этап ИИ-проверки пар помечен ``pipeline``."""
    return any(it.get("pipeline") and it["stage"] == "luna" for it in items)


def luna_status(object_id: str) -> dict:
    result = sheet_match.status(object_id)
    q = status(object_id)
    if not _process(q["items"]):
        return result
    current = q.get("current") or next((it for it in q["items"] if it["state"] == "failed"), None)
    return result | {"pipeline": q, "state": q["state"], "phase": (current or {}).get("stage") or "done",
                     "scope": list(dict.fromkeys(it["project_id"] for it in q["items"] if it.get("pipeline"))),
                     "reason": (current or {}).get("reason")}


def start_luna(object_id: str, *, consent: bool, user: str | None = None,
               project_ids: list[str] | None = None, luna_only: bool = False) -> dict:
    """Весь процесс по проектам; ``luna_only`` — только ИИ-проверка сомнительных пар (кнопка в строке проекта)."""
    if not consent:
        raise QueueError("нужно подтвердить передачу материалов исполнителю")
    catalog = store.load_catalog(object_id)
    projects = [p["project_id"] for p in catalog["projects"] if len(p["versions"]) >= 2
                and (not project_ids or p["project_id"] in project_ids)]
    if project_ids and set(project_ids) - set(projects):
        raise QueueError("проект не найден или содержит меньше двух версий")
    q = status(object_id)
    if luna_only:
        cur = q.get("current")
        if cur and cur["stage"] == "luna" and cur["state"] == "paused" and cur["project_id"] in projects:
            resume(object_id)                     # проверка стоит на паузе внутри очереди — продолжить её там
        else:
            sheet_match.start(object_id, consent=True, user=user, project_ids=projects)
        return luna_status(object_id)
    pending_scope = {it["project_id"] for it in q["items"] if it["state"] not in ("done", "skipped")}
    if q["state"] in ("paused", "failed") and pending_scope <= set(projects):
        resume(object_id)
    else:
        if q["state"] in ("idle", "done"):
            clear(object_id)
        enqueue(object_id, [{"project_id": pid, "stage": "luna"} for pid in projects],
                consent=consent, user=user, pipeline=True)
    return luna_status(object_id)


def pause_luna(object_id: str) -> dict:
    q = status(object_id)
    cur = q.get("current") or {}
    if _process(q["items"]) or (cur.get("stage") == "luna" and cur.get("state") == "running"):
        pause(object_id)
    else:
        sheet_match.pause(object_id)
    return luna_status(object_id)


def cancel_luna(object_id: str) -> dict:
    q = _load(object_id)
    if not _process(q["items"]):
        return sheet_match.cancel(object_id)
    pause(object_id)
    # Graceful stop preserves completed answers. No following stage is dispatched.
    _modify(object_id, lambda d: d | {"state": "paused", "reason": "Процесс остановлен пользователем"})
    return luna_status(object_id)


def clear(object_id: str) -> dict[str, Any]:
    """Убрать из очереди ещё не начатые этапы и историю; идущий этап продолжает работать сам по себе."""
    def drop(q):
        q["items"] = [it for it in q["items"] if it["state"] in ("running", "paused")]
        if not q["items"]:
            q["state"] = "idle"
        return q
    with _lock:
        _modify(object_id, drop)
    return status(object_id)


def remove(object_id: str, project_id: str, stage: str) -> dict[str, Any]:
    with _lock:
        _modify(object_id, lambda q: q | {"items": [it for it in q["items"] if not (
            it["project_id"] == project_id and it["stage"] == stage and it["state"] == "pending")]})
    return status(object_id)


# ------------------------------------------------------------------ выполнение

def _stages(object_id: str, project_id: str) -> dict[str, dict]:
    sheet_match.status(object_id)  # recover a stopped match worker even without an open browser
    for p in overview.build(object_id)["projects"]:
        if p["project_id"] == project_id:
            st = p["stages"]
            sid = st["series"].get("series_id")
            st["specifications"] = whole_specifications.status(object_id, sid) if sid else {"state": "todo"}
            st["summary"] = {"state": "done" if sid and (paths.series_path(object_id, sid).parent / "summary.md").exists() else "todo"}
            return st
    raise QueueError("проект не найден")


def _matrix_ready(object_id: str, project_id: str) -> str:
    """ready / building / failed — с запуском разбора и сопоставления, если их ещё нет."""
    m = vc_jobs.matrix(object_id, project_id)
    return "ready" if m["state"] == "ready" else ("failed" if m["state"] in ("failed", "empty") else "building")


def _start_stage(object_id: str, it: dict) -> str | None:
    """Запустить или продолжить этап. Строка — причина пропуска; None — запущен."""
    pid, stage = it["project_id"], it["stage"]
    st = _stages(object_id, pid)
    s = st[stage]
    if s.get("state") == "na":
        return "одна версия — сравнивать не с чем"
    if s.get("state") == "pausing":
        return "wait"                                   # ещё доводит начатое — продолжить после паузы
    if s.get("state") in ("running", "queued"):
        return None  # attach to the already running stage; never launch it a second time
    if stage in ("luna", "series"):
        mx = _matrix_ready(object_id, pid)
        if mx == "failed":
            return "разбор или сопоставление листов с ошибкой"
        if mx == "building":
            return "wait"
    if stage == "luna":
        if s["state"] in ("paused", "interrupted"):
            sheet_match.start(object_id, consent=True, user=it.get("user"), project_ids=s.get("scope") or [pid])
        else:
            sheet_match.start(object_id, consent=True, user=it.get("user"), project_ids=[pid])
    elif stage == "series":
        if s["state"] == "done" and s.get("series_id") and it.get("pipeline"):
            existing = vc_series.get(object_id, s["series_id"])
            if all(not any(item.get("pair_stale") for item in vc_jobs.get_run(object_id, p["run_id"])["items"])
                   for p in existing["pairs"] if p.get("run_id")):
                return None
        if s["state"] in ("paused", "interrupted", "failed") and s.get("series_id"):
            vc_series.resume(object_id, s["series_id"])
        else:
            vc_series.create(object_id, pid)
    elif stage == "ai":
        ser = st["series"]
        if ser.get("state") != "done" or not ser.get("series_id"):
            return "сравнение соседних не досчитано"
        if it.get("pipeline") and s["state"] in ("done", "partial") and not it.get("restart"):
            # Готово, но есть листы вне заданий (новые/переклассифицированные) — задания заново, готовые
            # листы берутся из кэша; иначе неполные листы — заново только они; всё полно — этап готов.
            if series_ai.missing(object_id, ser["series_id"]):
                series_ai.start(object_id, ser["series_id"], consent=True, user=it.get("user"))
            elif series_ai.incomplete(object_id, ser["series_id"]):
                series_ai.retry_incomplete(object_id, ser["series_id"], user=it.get("user"))
            return None
        if s["state"] in ("paused", "interrupted"):
            series_ai.resume(object_id, ser["series_id"], user=it.get("user"))
        elif not any(pair["sheets"] for pair in series_ai._pairs(object_id, ser["series_id"])):
            return "обычных листов с изменениями содержания нет; спецификации — на следующем этапе"
        else:
            series_ai.start(object_id, ser["series_id"], consent=True, user=it.get("user"))
    elif stage == "specifications":
        sid = st["series"].get("series_id")
        if not sid or st["series"].get("state") != "done":
            raise QueueError("сравнение соседних не завершено")
        whole_specifications.start(object_id, sid, consent=it.get("consent", False), user=it.get("user"))
    elif stage == "summary":
        sid = st["series"].get("series_id")
        if not sid or not whole_specifications.ready(object_id, sid):
            raise QueueError("полные спецификации ещё не готовы")
        if any(p["sheets"] for p in series_ai._pairs(object_id, sid)) and (
                series_ai.status(object_id, sid)["state"] not in series_ai.DONE_STATES
                or series_ai.incomplete(object_id, sid) or series_ai.missing(object_id, sid)):
            raise QueueError("описания обычных листов ещё не завершены полностью")
        result = series_summary.build(object_id, sid)
        target = paths.series_path(object_id, sid).parent / "summary.md"
        tmp = target.with_suffix(".md.tmp")
        tmp.write_text(result["text"], encoding="utf-8")
        tmp.replace(target)
    elif stage == "report":
        rep = s.get("md_report") or {}
        if s["state"] in ("paused", "interrupted", "failed") and rep.get("report_id"):
            md_report.resume(object_id, rep["report_id"])
        else:
            sid = st["series"].get("series_id")
            if not s.get("summary_md") or not sid:
                return "нет сводки изменений — сначала ИИ-описание"
            md = series_summary.build(object_id, sid)
            md_report.create(object_id, md["text"].encode("utf-8"), md["filename"], project_id=pid, consent=True,
                             user=it.get("user"), origin={"kind": "series", "series_id": sid}, reuse=True)
    return None


def _pause_stage(object_id: str, it: dict) -> None:
    st = _stages(object_id, it["project_id"])
    s = st[it["stage"]]
    if s.get("state") not in ("running", "queued"):
        return
    if it["stage"] == "luna":
        sheet_match.pause(object_id)
    elif it["stage"] == "series":
        vc_series.pause(object_id, s["series_id"])
    elif it["stage"] == "ai":
        series_ai.pause(object_id, st["series"]["series_id"])
    elif it["stage"] == "report":
        md_report.pause(object_id, s["md_report"]["report_id"])
    elif it["stage"] == "specifications":
        whole_specifications.pause(object_id, st["series"]["series_id"])


def _set(object_id: str, index: int, **fields) -> None:
    def fn(q):
        q["items"][index].update(fields)
        return q
    _modify(object_id, fn)


@contextmanager
def _dispatch_owner(object_id: str):
    path = paths.object_dir(object_id) / ".stage_queue.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def dispatch(object_id: str) -> None:
    """Шаг очереди: следить за текущим этапом; закончился — взять следующий."""
    with _lock, _dispatch_owner(object_id) as owned:
        if not owned:
            return
        q = _load(object_id)
        if q.get("state") != "running":
            return
        idx = next((i for i, it in enumerate(q["items"]) if it["state"] in ("running", "paused")), None)
        if idx is not None:
            it = q["items"][idx]
            if it["state"] == "paused":
                return
            if not it.get("started_at") or it.get("resume") or it.get("waiting"):
                _launch(object_id, idx, it)
                return
            if time.time() - datetime.fromisoformat(it["started_at"]).timestamp() < GRACE_S:
                return
            stages = _stages(object_id, it["project_id"])
            s = stages[it["stage"]]
            state = s.get("state")
            if state in BUSY:
                return
            if state in FINISHED[it["stage"]]:
                # «Частично» из-за пометок движка при полном разборе — не блок; блок — ошибки и неразобранные записи.
                sid = stages["series"].get("series_id")
                n = 0
                if it.get("pipeline") and it["stage"] == "ai":
                    n = sum(len(v) for v in series_ai.incomplete(object_id, sid).values()) \
                        if state == "partial" else 0
                    n += series_ai.missing(object_id, sid)
                if n and not it.get("retried"):
                    _set(object_id, idx, retried=True, resume=True)       # один доразбор / дозапуск листов
                    return
                if n:
                    _set(object_id, idx, state="failed", finished_at=_now(),
                         reason=f"обычные листы описаны не полностью после доразбора: {n} "
                                "(ошибка ИИ, записи без решения или лист вне заданий) — «Продолжить» повторит их")
                    _modify(object_id, lambda d: d | {"state": "failed"})
                    return
                _set(object_id, idx, state="done", finished_at=_now(), result=state, reason=None)
                if it.get("pipeline") and it["stage"] == "specifications" and series_ai.missing(object_id, sid):
                    # Полное сравнение исключило версии — их листы СО описывает листовой ИИ: этап описания
                    # проекта ещё раз (готовые листы из кэша), затем сводка.
                    pid = it["project_id"]

                    def again(q):
                        for other in q["items"]:
                            if other["project_id"] == pid and other["stage"] == "ai" and other.get("pipeline"):
                                for k in ("started_at", "finished_at", "reason", "result", "resume", "retried",
                                          "waiting", "note"):
                                    other.pop(k, None)
                                other["state"] = "pending"
                        return q
                    _modify(object_id, again)
            elif state == "paused":
                # Этап поставлен на паузу кнопкой в строке (или лимитом подписки) — очередь тоже ждёт.
                _set(object_id, idx, state="paused")
                _modify(object_id, lambda d: d | {"state": "paused", "paused_at": _now()})
                return
            elif state == "interrupted" and not it.get("retried"):
                _set(object_id, idx, retried=True, resume=True)       # прерван перезапуском — досчитать один раз
                return
            else:
                _set(object_id, idx, state="failed", finished_at=_now(), reason=s.get("reason") or f"этап: {state}")
                if it.get("pipeline"):
                    _modify(object_id, lambda d: d | {"state": "failed"})
                    return
            q = _load(object_id)
        nxt = next((i for i, it in enumerate(q["items"]) if it["state"] == "pending"), None)
        if nxt is None:
            _modify(object_id, lambda d: d | {"state": "done" if d["items"] else "idle", "finished_at": _now()})
            return
        _launch(object_id, nxt, q["items"][nxt])


def _launch(object_id: str, idx: int, it: dict) -> None:
    try:
        reason = _start_stage(object_id, it)
    except Exception as exc:  # noqa: BLE001 — один этап не держит очередь
        log.warning("очередь этапов: %s/%s не запущен: %s", it.get("code"), it.get("stage"), exc)
        if "уже идёт" in str(exc) or "уже выполняется" in str(exc) or "уже в очереди" in str(exc):
            _set(object_id, idx, state="running", waiting=True, note=str(exc))     # занят другим — подождать
            return
        _set(object_id, idx, state="failed", finished_at=_now(), reason=str(exc))
        if it.get("pipeline"):
            _modify(object_id, lambda d: d | {"state": "failed"})
        return
    if reason == "wait":
        _set(object_id, idx, state="running", waiting=True, note="разбор и сопоставление листов")
        return
    if reason:
        if it.get("pipeline") and not reason.startswith(("обычных листов", "одна версия")):
            _set(object_id, idx, state="failed", finished_at=_now(), reason=reason)
            _modify(object_id, lambda d: d | {"state": "failed"})
            return
        _set(object_id, idx, state="skipped", finished_at=_now(), reason=reason)
        return
    # «Заново» действует только на первый запуск; доразбор и «Продолжить» идут с того же места.
    _set(object_id, idx, state="running", started_at=_now(), resume=False, waiting=False, note=None, restart=False)


def _objects() -> list[str]:
    root = paths.root() / "objects"
    return [p.parent.name for p in root.glob("*/stage_queue.json")] if root.exists() else []


def dispatch_loop(stop: threading.Event) -> None:
    """Фоновый шаг очередей всех объектов раз в ``TICK_S`` секунд (поток службы, см. роутер ИИ)."""
    while not stop.wait(TICK_S):
        for oid in _objects():
            try:
                dispatch(oid)
            except Exception:  # noqa: BLE001 — сбой шага не останавливает поток
                log.exception("очередь этапов объекта %s", oid)
