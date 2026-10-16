"""«Описать изменения ИИ» по всем соседним переходам проекта: (1)→(2), (2)→(3), … до последнего.

На каждый посчитанный переход серии — обычное задание способа «Изменения в листе» (``jobs.create_job``,
method ``sheet_changes``). В модель уходят только листы, где движок нашёл изменения содержания
(``jobs.SC_SKIP_STATUSES``). Листы всех переходов делят один предел одновременных обращений
(``settings.sc_parallel()``): освободилось место — берётся следующий лист любого перехода.

Запись — ``series/<id>/ai_sheet_changes.json`` рядом с серией: какие задания созданы по каким переходам.
Ход, время и токены собираются из заданий при каждом чтении (ничего не дублируется).
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.app.services.common.atomic_json import atomic_write_json
from backend.app.services.version_comparison import jobs as vc_jobs
from backend.app.services.version_comparison import paths, series as vc_series

from .. import executor as ex
from .. import jobs, settings
from . import view

SCHEMA = "version_comparison_ai.series_sheet_changes/v1"
DONE_STATES = ("done", "partial")


def file_path(object_id: str, series_id: str) -> Path:
    return paths.series_path(object_id, series_id).parent / "ai_sheet_changes.json"


def _pairs(object_id: str, series_id: str) -> list[dict[str, Any]]:
    """Переходы серии и листы каждого, которые уйдут в модель (без сборки пакетов — быстро)."""
    ser = vc_series.get(object_id, series_id)
    out = []
    for i, pr in enumerate(ser["pairs"]):
        row = {"index": i, "a_label": pr["a_label"], "b_label": pr["b_label"], "run_id": pr.get("run_id"),
               "sheets": 0, "skipped": 0, "reason": None}
        if not pr.get("run_id") or pr.get("run_state") != "done":
            row["reason"] = "сравнение перехода не досчитано"
        else:
            items, skipped = jobs._sc_items(vc_jobs.get_run(object_id, pr["run_id"]), None)
            row.update(sheets=len(items), skipped=len(skipped))
            if not items:
                row["reason"] = "изменений содержания нет (по данным сравнения)"
        out.append(row)
    return out


def estimate(object_id: str, series_id: str) -> dict[str, Any]:
    """Объём без вызова модели: переходы, листы и предел обращений (3 на лист)."""
    pairs = _pairs(object_id, series_id)
    sheets = sum(p["sheets"] for p in pairs)
    return {"pairs": pairs, "sheets": sheets, "calls_primary": sheets, "calls_max": 3 * sheets,
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
    if not settings.enabled():
        raise jobs.JobError("ИИ-описания выключены")
    if not consent:
        raise jobs.JobError("нужно подтвердить передачу документов исполнителю")
    cur = status(object_id, series_id)
    if cur.get("state") in jobs.ACTIVE:
        raise jobs.JobError("ИИ-описание переходов уже идёт")
    pairs = _pairs(object_id, series_id)
    if not any(p["sheets"] for p in pairs):
        raise jobs.JobError("в переходах нет листов с изменениями содержания")
    rec = {"schema": SCHEMA, "series_id": series_id, "object_id": object_id,
           "project_id": vc_series.get(object_id, series_id)["project_id"],
           "created_at": jobs._now(), "user": user, "parallel": settings.sc_parallel(), "pairs": []}
    for p in pairs:
        row = {k: p[k] for k in ("index", "a_label", "b_label", "run_id", "sheets", "skipped", "reason")}
        row["job_id"] = None
        if p["sheets"]:
            try:
                row["job_id"] = jobs.create_job(object_id, p["run_id"], consent=True, user=user,
                                                method="sheet_changes", trigger="series")["job_id"]
            except jobs.JobError as exc:
                row["reason"] = str(exc)
        rec["pairs"].append(row)
    atomic_write_json(file_path(object_id, series_id), rec)
    return status(object_id, series_id)


def _sheet_counts(object_id: str, run_id: str, job: dict) -> dict[str, int]:
    c = {"changes": 0, "unresolved": 0}
    for s in job["sheets"]:
        res = view.load_result(object_id, run_id, s["spid"])
        if res and res.get("job_id") == job["job_id"]:
            c["changes"] += len(res.get("changes") or [])
            c["unresolved"] += len(res.get("unresolved") or [])
    return c


def _ts(s: str | None) -> float | None:
    try:
        return datetime.fromisoformat(s).timestamp() if s else None
    except ValueError:
        return None


def status(object_id: str, series_id: str) -> dict[str, Any]:
    """Ход по переходам и итог: листы, изменения, вопросы, обращения, токены, время."""
    rec = _load(object_id, series_id)
    if not rec:
        return {"state": "none", "pairs": []}
    tot = {"sheets": 0, "sheets_done": 0, "sheets_failed": 0, "changes": 0, "unresolved": 0, "calls": 0,
           "input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0, "reasoning_output_tokens": 0}
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
                for k in ("calls", "input_tokens", "cached_input_tokens", "output_tokens",
                          "reasoning_output_tokens", "changes", "unresolved"):
                    tot[k] += row.get(k) or 0
                tot["sheets"] += row["sheets_total"]
                tot["sheets_done"] += row["sheets_done"]
                tot["sheets_failed"] += failed
        else:
            row["state"] = "skipped"
        out_pairs.append(row)
    if any(s in jobs.ACTIVE for s in states):
        state = "running"
    elif any(s in ("paused_quota", "paused_auth") for s in states):
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


def cancel(object_id: str, series_id: str) -> dict[str, Any]:
    for p in (_load(object_id, series_id) or {}).get("pairs", []):
        if p.get("job_id"):
            try:
                if jobs.get_job(object_id, p["run_id"], p["job_id"])["state"] in jobs.ACTIVE:
                    jobs.cancel_job(object_id, p["run_id"], p["job_id"])
            except jobs.JobError:
                pass
    return status(object_id, series_id)


def resume(object_id: str, series_id: str) -> dict[str, Any]:
    """Досчитать прерванные, отменённые и приостановленные задания переходов; готовые листы не повторяются."""
    n = 0
    for p in (_load(object_id, series_id) or {}).get("pairs", []):
        if not p.get("job_id"):
            continue
        st = jobs.get_job(object_id, p["run_id"], p["job_id"])["state"]
        if st in ("interrupted", "cancelled", "paused_quota", "paused_auth", "failed"):
            jobs.resume_job(object_id, p["run_id"], p["job_id"])
            n += 1
    if not n:
        raise jobs.JobError("досчитывать нечего")
    return status(object_id, series_id)

