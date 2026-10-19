"""«Описать изменения ИИ» по всем соседним переходам проекта: (1)→(2), (2)→(3), … до последнего.

На каждый посчитанный переход серии — обычное задание способа «Изменения в листе» (``jobs.create_job``,
method ``sheet_changes``). В модель уходят только листы, где движок нашёл изменения содержания
(``jobs.SC_SKIP_STATUSES``). Листы всех переходов делят один предел одновременных обращений
(``settings.sc_parallel()``): освободилось место — берётся следующий лист любого перехода.

Запись — ``series/<id>/ai_sheet_changes.json`` рядом с серией: какие задания созданы по каким переходам.
Ход, время и токены собираются из заданий при каждом чтении (ничего не дублируется).

Очередь проектов (``ai_series_queue.json`` в корне раздела): описание идёт по одной серии за раз на всю
установку. Запуск, пока другая серия идёт, ставит проект в конец очереди; закончилась текущая — сама
начинается следующая (``dispatch``, шаг фонового потока). Пауза текущей (лимит подписки, вход в Codex)
держит очередь до «Досчитать» или «Отменить»; прерванная перезапуском сервера — досчитывается сама.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.app.services.common.atomic_json import atomic_write_json
from backend.app.services.version_comparison import jobs as vc_jobs
from backend.app.services.version_comparison import paths, series as vc_series, store

from .. import executor as ex
from .. import jobs, settings
from . import single, view

SCHEMA = "version_comparison_ai.series_sheet_changes/v1"
QUEUE_SCHEMA = "version_comparison_ai.series_queue/v1"
DONE_STATES = ("done", "partial")
RESUMABLE = ("interrupted", "cancelled", "paused_quota", "paused_auth", "paused_user", "failed")
QUEUE_TICK_S = 5
_qlock = threading.RLock()
log = logging.getLogger(__name__)


def file_path(object_id: str, series_id: str) -> Path:
    return paths.series_path(object_id, series_id).parent / "ai_sheet_changes.json"


def _pairs(object_id: str, series_id: str) -> list[dict[str, Any]]:
    """Переходы серии и листы каждого, которые уйдут в модель (без сборки пакетов — быстро)."""
    ser = vc_series.get(object_id, series_id)
    out = []
    for i, pr in enumerate(ser["pairs"]):
        row = {"index": i, "a_label": pr["a_label"], "b_label": pr["b_label"], "run_id": pr.get("run_id"),
               "sheets": 0, "skipped": 0, "added": 0, "removed": 0, "reason": None}
        if not pr.get("run_id") or pr.get("run_state") != "done":
            row["reason"] = "сравнение перехода не досчитано"
        else:
            items, skipped = jobs._sc_items(vc_jobs.get_run(object_id, pr["run_id"]), None)
            row.update(sheets=len(items), skipped=len(skipped),
                       added=sum(1 for it in items if it.get("kind") == "only_b"),
                       removed=sum(1 for it in items if it.get("kind") == "only_a"))
            if not items:
                row["reason"] = "изменений содержания нет (по данным сравнения)"
        out.append(row)
    return out


def estimate(object_id: str, series_id: str) -> dict[str, Any]:
    """Объём без вызова модели: переходы, листы и предел обращений (3 на лист)."""
    pairs = _pairs(object_id, series_id)
    sheets = sum(p["sheets"] for p in pairs)
    qs = queue_status()
    return {"pairs": pairs, "sheets": sheets, "calls_primary": sheets, "calls_max": 3 * sheets,
            "queue": {"busy": bool(qs["current"] or qs["waiting"]), "current": (qs["current"] or {}).get("code"),
                      "waiting": len(qs["waiting"])},
            "parallel": settings.sc_parallel(), "enabled": settings.enabled(),
            "executor": ex.get("sheet_changes").describe_runtime(), "settings": settings.sc_snapshot(),
            "transfer": "Тексты строк таблиц, надписи и слова мест изменений, вырезки этих мест и обзор листа "
                        "каждой пары соседних версий будут переданы исполнителю. Секреты в пакеты не входят."}


def _load(object_id: str, series_id: str) -> dict[str, Any] | None:
    p = file_path(object_id, series_id)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def start(object_id: str, series_id: str, *, consent: bool, user: str | None = None) -> dict[str, Any]:
    """Запуск описания серии; если идёт другая серия — в конец очереди проектов."""
    if not settings.enabled():
        raise jobs.JobError("ИИ-описания выключены")
    if not consent:
        raise jobs.JobError("нужно подтвердить передачу документов исполнителю")
    with _qlock:
        q = _qload()
        _check_free(q, object_id, series_id)
        if not any(p["sheets"] for p in _pairs(object_id, series_id)):
            raise jobs.JobError("в переходах нет листов с изменениями содержания")
        if q["current"] and _same(q["current"], object_id, series_id):
            q["current"]["released"] = True          # новый запуск вместо приостановленного
        _submit(q, _entry(object_id, series_id, "start", user))
    return status(object_id, series_id)


def _launch(object_id: str, series_id: str, user: str | None) -> None:
    """Задание «Изменения в листе» на каждый переход с изменениями содержания."""
    pairs = _pairs(object_id, series_id)
    if not any(p["sheets"] for p in pairs):
        raise jobs.JobError("в переходах нет листов с изменениями содержания")
    rec = {"schema": SCHEMA, "series_id": series_id, "object_id": object_id,
           "project_id": vc_series.get(object_id, series_id)["project_id"],
           "created_at": jobs._now(), "user": user, "parallel": settings.sc_parallel(), "pairs": []}
    for p in pairs:
        row = {k: p[k] for k in ("index", "a_label", "b_label", "run_id", "sheets", "skipped", "added", "removed",
                                 "reason")}
        row["job_id"] = None
        if p["sheets"]:
            try:
                row["job_id"] = jobs.create_job(object_id, p["run_id"], consent=True, user=user,
                                                method="sheet_changes", trigger="series")["job_id"]
            except jobs.JobError as exc:
                row["reason"] = str(exc)
        rec["pairs"].append(row)
    atomic_write_json(file_path(object_id, series_id), rec)


def _sheet_counts(object_id: str, run_id: str, job: dict) -> dict[str, Any]:
    """Изменения и вопросы по парам листов; листы без пары — отдельным списком с кратким описанием."""
    c: dict[str, Any] = {"changes": 0, "unresolved": 0, "singles": []}
    for s in job["sheets"]:
        res = view.load_result(object_id, run_id, s["spid"])
        mine = res if res and res.get("job_id") == job["job_id"] else None
        if s.get("item_kind") in single.KINDS:
            kind = single.KINDS[s["item_kind"]]
            c["singles"].append({"spid": s["spid"], "title": s.get("title"), "kind": kind, "label": single.LABELS[kind],
                                 "engine_status": s.get("engine_status"), "page": s.get("b_page") or s.get("a_page"),
                                 "side": "B" if kind == "added" else "A", "state": s["ai_status"],
                                 "description": (mine or {}).get("single")})
            continue
        if mine:
            c["changes"] += len(mine.get("changes") or [])
            c["unresolved"] += len(mine.get("unresolved") or [])
    return c


def _ts(s: str | None) -> float | None:
    try:
        return datetime.fromisoformat(s).timestamp() if s else None
    except ValueError:
        return None


def _zero_total() -> dict[str, int]:
    return {"sheets": 0, "sheets_done": 0, "sheets_failed": 0, "changes": 0, "unresolved": 0, "singles": 0, "calls": 0,
            "input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0, "reasoning_output_tokens": 0}


def _progress_status(object_id: str, series_id: str) -> dict[str, Any]:
    """Ход по переходам и итог: листы, изменения, вопросы, обращения, токены, время."""
    rec = _load(object_id, series_id)
    if not rec:
        return {"state": "none", "pairs": [], "total": _zero_total()}
    tot = _zero_total()
    states, ends = [], []
    out_pairs = []
    for p in rec["pairs"]:
        row = dict(p)
        if p.get("job_id"):
            try:
                j = jobs.get_job(object_id, p["run_id"], p["job_id"])
            except jobs.JobError:
                j = None
            if j:
                prog, u = j.get("progress") or {}, j.get("usage") or {}
                failed = sum(1 for s in j["sheets"] if s["ai_status"] in ("failed", "not_sent"))
                row.update(state=j["state"], reason=j.get("reason") or p.get("reason"),
                           sheets_total=prog.get("sheets_total", len(j["sheets"])),
                           sheets_done=prog.get("sheets_done", 0), sheets_failed=failed,
                           running=sum(1 for s in j["sheets"] if s["ai_status"] == "running"),
                           calls=u.get("calls", 0), input_tokens=u.get("input_tokens", 0),
                           cached_input_tokens=u.get("cached_input_tokens", 0),
                           output_tokens=u.get("output_tokens", 0),
                           reasoning_output_tokens=u.get("reasoning_output_tokens", 0),
                           started_at=j.get("started_at"), finished_at=j.get("finished_at"))
                row.update(_sheet_counts(object_id, p["run_id"], j))
                states.append(j["state"])
                if j.get("finished_at"):
                    ends.append(_ts(j["finished_at"]))
                row["sha_a"], row["sha_b"] = j["a"].get("sha256"), j["b"].get("sha256")
                for k in ("calls", "input_tokens", "cached_input_tokens", "output_tokens",
                          "reasoning_output_tokens", "changes", "unresolved"):
                    tot[k] += row.get(k) or 0
                tot["sheets"] += row["sheets_total"]
                tot["sheets_done"] += row["sheets_done"]
                tot["sheets_failed"] += failed
                tot["singles"] += len(row.get("singles") or [])
        else:
            row["state"] = "skipped"
        out_pairs.append(row)
    if "pausing" in states:
        state = "pausing"
    elif any(s in jobs.ACTIVE for s in states):
        state = "running"
    elif any(s in jobs.PAUSED for s in states):
        state = "paused"
    elif any(s in ("interrupted", "cancelled", "failed") for s in states):
        state = "interrupted"
    elif states and all(s == "done" for s in states):
        state = "done"
    else:
        state = "partial" if states else "none"
    t0 = _ts(rec["created_at"])
    t1 = max([e for e in ends if e] or [0]) if state not in ("running",) else None
    wall = round(((t1 or time.time()) - t0), 1) if t0 else None
    return {"schema": SCHEMA, "series_id": series_id, "state": state, "created_at": rec["created_at"],
            "user": rec.get("user"), "parallel": rec.get("parallel"), "wall_s": wall, "pairs": out_pairs,
            "total": tot, "enabled": settings.enabled()}


def status(object_id: str, series_id: str) -> dict[str, Any]:
    """Ход серии и её место в очереди проектов: ``queued`` — ждёт, ``queue.ahead`` — кто перед ней."""
    out = _progress_status(object_id, series_id)
    q = _qload()
    cur = q["current"]
    for i, e in enumerate(q["waiting"]):
        if _same(e, object_id, series_id):
            ph = _holds(cur)
            ahead = ([cur | {"phase": ph}] if ph else []) + q["waiting"][:i]
            return out | {"state": "queued", "queue": {
                "position": i + 1, "action": e["action"], "queued_at": e["queued_at"],
                "ahead": [{"code": x["code"], "object_id": x["object_id"], "phase": x.get("phase") or "queued"}
                          for x in ahead]}}
    if cur and _same(cur, object_id, series_id):
        out["queue"] = {"current": True, "waiting": len(q["waiting"])}
    note = q["notes"].get(_key(object_id, series_id))
    if note:
        out["queue_error"] = note["reason"]
        if out["state"] == "none":
            out["state"] = "failed"
    return out


def cancel(object_id: str, series_id: str) -> dict[str, Any]:
    """Ждёт в очереди — убрать из очереди; идёт или на паузе — отменить задания и отпустить очередь."""
    with _qlock:
        q = _qload()
        rest = [e for e in q["waiting"] if not _same(e, object_id, series_id)]
        if len(rest) != len(q["waiting"]):
            q["waiting"] = rest
            _qsave(q)
            return status(object_id, series_id)
        for p in (_load(object_id, series_id) or {}).get("pairs", []):
            if p.get("job_id"):
                try:
                    if jobs.get_job(object_id, p["run_id"], p["job_id"])["state"] in jobs.ACTIVE + jobs.PAUSED:
                        jobs.cancel_job(object_id, p["run_id"], p["job_id"])
                except jobs.JobError:
                    pass
        if q["current"] and _same(q["current"], object_id, series_id):
            q["current"]["released"] = True
            _qsave(q)
    dispatch()
    return status(object_id, series_id)


def pause(object_id: str, series_id: str) -> dict[str, Any]:
    """Пауза: начатые листы доводятся, новые не начинаются; очередь отпускается — следующий проект идёт.
    «Досчитать» продолжает с того же места (встаёт в очередь, если она занята)."""
    with _qlock:
        q = _qload()
        if any(_same(e, object_id, series_id) for e in q["waiting"]):
            raise jobs.JobError("проект ещё ждёт в очереди — его можно убрать из очереди")
        n = 0
        for p in (_load(object_id, series_id) or {}).get("pairs", []):
            if p.get("job_id"):
                try:
                    if jobs.get_job(object_id, p["run_id"], p["job_id"])["state"] in ("queued", "running"):
                        jobs.pause_job(object_id, p["run_id"], p["job_id"])
                        n += 1
                except jobs.JobError:
                    pass
        if not n:
            raise jobs.JobError("ИИ-описание сейчас не идёт — ставить на паузу нечего")
        if q["current"] and _same(q["current"], object_id, series_id):
            q["current"]["released"] = True
            _qsave(q)
    return status(object_id, series_id)


INCOMPLETE = ("failed", "not_sent", "cancelled", "queued", "running")


def incomplete(object_id: str, series_id: str) -> dict[str, set[str]]:
    """Листы, описанные не полностью: ошибка/не отправлен или модель оставила записи без решения.

    «Частично» из-за пометок движка (дубль листа, нет текстового слоя) при полном разборе записей —
    не неполнота: модель рассмотрела всё. Ключ — job_id, значение — spid листов."""
    out: dict[str, set[str]] = {}
    if not series_id:
        return out
    for p in (_load(object_id, series_id) or {}).get("pairs", []):
        if not p.get("job_id"):
            continue
        try:
            j = jobs.get_job(object_id, p["run_id"], p["job_id"])
        except jobs.JobError:
            continue
        bad = {s["spid"] for s in j["sheets"] if s["ai_status"] in INCOMPLETE
               or (s["ai_status"] == "partial" and (s.get("coverage") or {}).get("not_reviewed"))}
        if bad:
            out[p["job_id"]] = bad
    return out


def missing(object_id: str, series_id: str) -> int:
    """Листы, которые сейчас уходят в модель, но в готовых заданиях серии их нет (например, чертёж
    раньше ошибочно считался спецификацией). Ноль — задания покрывают всё."""
    rec = _load(object_id, series_id) if series_id else None
    if not rec:
        return 0
    jobs_by_run = {p["run_id"]: p.get("job_id") for p in rec["pairs"] if p.get("run_id")}
    n = 0
    for pr in vc_series.get(object_id, series_id)["pairs"]:
        if not pr.get("run_id") or pr.get("run_state") != "done":
            continue
        items, _ = jobs._sc_items(vc_jobs.get_run(object_id, pr["run_id"]), None)
        have: set[str] = set()
        if jobs_by_run.get(pr["run_id"]):
            try:
                have = {s["spid"] for s in jobs.get_job(object_id, pr["run_id"], jobs_by_run[pr["run_id"]])["sheets"]}
            except jobs.JobError:
                pass
        n += sum(1 for it in items if it["spid"] not in have)
    return n


def retry_incomplete(object_id: str, series_id: str, user: str | None = None) -> dict[str, Any]:
    """Заново описать (мимо кэша) только неполные листы готовых заданий серии; через очередь проектов."""
    with _qlock:
        q = _qload()
        _check_free(q, object_id, series_id)
        if not incomplete(object_id, series_id):
            raise jobs.JobError("неполных листов нет")
        _submit(q, _entry(object_id, series_id, "retry", user))
    return status(object_id, series_id)


def _retry_jobs(object_id: str, series_id: str) -> int:
    bad = incomplete(object_id, series_id)
    n = 0
    for p in (_load(object_id, series_id) or {}).get("pairs", []):
        if p.get("job_id") in bad:
            jobs.resume_job(object_id, p["run_id"], p["job_id"], retry_failed=True, retry_spids=bad[p["job_id"]])
            n += 1
    return n


def resume(object_id: str, series_id: str, user: str | None = None) -> dict[str, Any]:
    """Досчитать прерванные, отменённые и приостановленные задания переходов; готовые листы не повторяются.
    Идёт другая серия — досчёт встаёт в очередь проектов."""
    with _qlock:
        q = _qload()
        _check_free(q, object_id, series_id)
        if not any(st in RESUMABLE for st in _job_states(object_id, series_id)):
            raise jobs.JobError("досчитывать нечего")
        cur = q["current"]
        if cur and _same(cur, object_id, series_id):
            _resume_jobs(object_id, series_id)
            cur["released"] = False
            _qsave(q)
        else:
            _submit(q, _entry(object_id, series_id, "resume", user))
    return status(object_id, series_id)


# ------------------------------------------------------------------ очередь проектов

def queue_path() -> Path:
    return paths.root() / "ai_series_queue.json"


def _key(object_id: str, series_id: str) -> str:
    return f"{object_id}/{series_id}"


def _same(e: dict[str, Any], object_id: str, series_id: str) -> bool:
    return e["object_id"] == object_id and e["series_id"] == series_id


def _qload() -> dict[str, Any]:
    path = queue_path()
    q = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    return {"current": q.get("current"), "waiting": list(q.get("waiting") or []), "notes": dict(q.get("notes") or {})}


def _qsave(q: dict[str, Any]) -> None:
    atomic_write_json(queue_path(), {"schema": QUEUE_SCHEMA} | q)


def _entry(object_id: str, series_id: str, action: str, user: str | None) -> dict[str, Any]:
    pid = vc_series.get(object_id, series_id)["project_id"]
    try:
        proj = store.project(store.load_catalog(object_id), pid)
        code = proj.get("code") or proj.get("title") or pid
    except (store.CatalogError, OSError, ValueError, KeyError):
        code = pid
    return {"object_id": object_id, "series_id": series_id, "project_id": pid, "code": code,
            "action": action, "user": user, "queued_at": jobs._now()}


def _job_states(object_id: str, series_id: str) -> list[str]:
    out = []
    for p in (_load(object_id, series_id) or {}).get("pairs", []):
        if p.get("job_id"):
            try:
                out.append(jobs.get_job(object_id, p["run_id"], p["job_id"])["state"])
            except jobs.JobError:
                pass
    return out


def _phase(object_id: str, series_id: str) -> str:
    """running — задания идут; paused — пауза (лимит подписки, вход); interrupted — прервано перезапуском
    сервера; finished — остальное (готово, отменено, ошибка)."""
    states = _job_states(object_id, series_id)
    if any(st in jobs.ACTIVE for st in states):
        return "running"
    if any(st in jobs.PAUSED for st in states):
        return "paused"
    return "interrupted" if "interrupted" in states else "finished"


def _holds(cur: dict[str, Any] | None) -> str | None:
    """Фаза текущей серии, пока она занимает очередь; отпущенная («Отменить») держит только пока идёт."""
    if not cur:
        return None
    ph = _phase(cur["object_id"], cur["series_id"])
    return ph if ph == "running" or (ph in ("paused", "interrupted") and not cur.get("released")) else None


def _check_free(q: dict[str, Any], object_id: str, series_id: str) -> None:
    if any(_same(e, object_id, series_id) for e in q["waiting"]):
        raise jobs.JobError("проект уже в очереди ИИ-описания")
    if _phase(object_id, series_id) == "running":
        raise jobs.JobError("ИИ-описание изменений уже идёт")


def _resume_jobs(object_id: str, series_id: str, only: tuple[str, ...] = RESUMABLE) -> int:
    n = 0
    for p in (_load(object_id, series_id) or {}).get("pairs", []):
        if p.get("job_id") and jobs.get_job(object_id, p["run_id"], p["job_id"])["state"] in only:
            jobs.resume_job(object_id, p["run_id"], p["job_id"])
            n += 1
    return n


def _run_entry(e: dict[str, Any]) -> None:
    if e["action"] == "resume":
        if not _resume_jobs(e["object_id"], e["series_id"]):
            raise jobs.JobError("досчитывать нечего")
    elif e["action"] == "retry":
        if not _retry_jobs(e["object_id"], e["series_id"]):
            raise jobs.JobError("неполных листов нет")
    else:
        _launch(e["object_id"], e["series_id"], e.get("user"))


def _submit(q: dict[str, Any], e: dict[str, Any]) -> None:
    """Очередь свободна — сразу в работу, иначе — в конец очереди."""
    if _holds(q["current"]) or q["waiting"]:
        q["waiting"].append(e)
    else:
        _run_entry(e)
        q["current"] = e
    q["notes"].pop(_key(e["object_id"], e["series_id"]), None)
    _qsave(q)


def dispatch() -> None:
    """Шаг очереди: текущая идёт или на паузе — ждать; прервана перезапуском — досчитать; закончилась —
    запустить следующую. Не запустилась — заметка у серии (видна в её статусе), берётся следующая."""
    with _qlock:
        q = _qload()
        if not q["current"] and not q["waiting"]:
            return
        ph = _holds(q["current"])
        if ph == "interrupted":
            _resume_jobs(q["current"]["object_id"], q["current"]["series_id"], only=("interrupted",))
            return
        if ph:
            return
        while q["waiting"]:
            e = q["waiting"].pop(0)
            try:
                _run_entry(e)
            except Exception as exc:  # noqa: BLE001 — сбой одного проекта не держит очередь
                log.warning("очередь ИИ-описаний: %s не запущен: %s", _key(e["object_id"], e["series_id"]), exc)
                q["notes"][_key(e["object_id"], e["series_id"])] = {"reason": str(exc), "at": jobs._now()}
                continue
            q["current"] = e
            q["notes"].pop(_key(e["object_id"], e["series_id"]), None)
            _qsave(q)
            return
        q["current"] = None
        _qsave(q)


def queue_status() -> dict[str, Any]:
    """Очередь проектов на всю установку: что идёт сейчас и кто ждёт по порядку."""
    q = _qload()
    ph = _holds(q["current"])
    return {"current": q["current"] | {"phase": ph} if ph else None, "waiting": q["waiting"]}


def dispatch_loop(stop: threading.Event) -> None:
    """Фоновый шаг очереди раз в ``QUEUE_TICK_S`` секунд (поток службы, см. роутер)."""
    while not stop.wait(QUEUE_TICK_S):
        try:
            if settings.enabled() and queue_path().exists():
                dispatch()
        except Exception:  # noqa: BLE001
            log.exception("очередь ИИ-описаний: шаг не выполнен, повтор на следующем")
