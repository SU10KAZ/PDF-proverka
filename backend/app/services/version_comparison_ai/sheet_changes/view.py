"""Окно «Изменения в листе»: состояние и сохранённый результат пары листов — без обращений к модели.

Открытие окна и перелистывание читают только файлы: индекс результатов прогона,
``sheet_changes.json`` листа и ``job.json`` задания. Устаревание — по результату движка
и версии способа; ссылки на доказательства не переназначаются.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.app.services.version_comparison import jobs as vc_jobs

from .. import geometry as geo
from .. import jobs
from .. import packet as old_packet
from .. import settings
from . import report
from . import run as sc_run
from . import schema as sc
from . import validate as val

ENGINE_SAME = ("same", "same_zones_changed")


def _index(object_id: str, run_id: str) -> dict:
    f = sc_run.index_path(object_id, run_id)
    try:
        return json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}
    except ValueError:
        return {}


def _active_job(object_id: str, run_id: str, spid: str) -> dict | None:
    for j in jobs.list_jobs(object_id, run_id):
        if j.get("method") == "sheet_changes" and j.get("state") in jobs.ACTIVE:
            full = jobs.get_job(object_id, run_id, j["job_id"])
            s = next((x for x in full["sheets"] if x["spid"] == spid), None)
            if s and s["ai_status"] in ("queued", "running"):
                return full | {"_sheet": s}
    return None


def _old_result(object_id: str, run_id: str, spid: str) -> str | None:
    for j in jobs.list_jobs(object_id, run_id):
        if (j.get("method") or "records") == "records" and \
                (jobs.sheet_dir(object_id, run_id, j["job_id"], spid) / "result.json").exists():
            return j["job_id"]
    return None


def load_result(object_id: str, run_id: str, spid: str, job_id: str | None = None) -> dict | None:
    entries = _index(object_id, run_id).get(spid) or []
    if job_id:
        entries = [e for e in entries if e["job_id"] == job_id]
    for e in reversed(entries):
        f = jobs.sheet_dir(object_id, run_id, e["job_id"], spid) / "sheet_changes.json"
        if f.exists():
            return json.loads(f.read_text(encoding="utf-8"))
    return None


def stale_reason(object_id: str, run_id: str, res: dict) -> str:
    try:
        _data, rdir = vc_jobs.sheet_result(object_id, run_id, res["spid"])
    except Exception:  # noqa: BLE001
        return "лист больше не найден в прогоне"
    cur = old_packet._sha_file(rdir / "result.json") if rdir else None
    if cur != (res.get("engine") or {}).get("result_sha256"):
        return "результат сравнения этого листа изменился после описания"
    if (res.get("method") or {}) != settings.sc_versions():
        return "описание сделано прежней версией способа"
    return ""


def _change_view(c: dict) -> dict:
    table = None
    if c.get("members"):
        cols = []
        for m in c["members"]:
            for f in m["fields"]:
                if f["name"] not in cols:
                    cols.append(f["name"])
        table = {"columns": cols, "rows": [{"object": m["object"], "place": m.get("place") or "",
                                            "cells": {f["name"]: {"before": f["before"], "after": f["after"],
                                                                  "unit": f.get("unit") or ""} for f in m["fields"]}}
                                           for m in c["members"]]}
    return {"id": c["id"], "object": c["object"], "place": c["place"], "kind": c["kind"],
            "kind_label": sc.KIND_LABELS.get(c["kind"], c["kind"]), "summary": c["summary"], "fields": c["fields"],
            "table": table, "limitation": c.get("limitation") or "", "unclear": c.get("unclear"),
            "confirmation": c.get("confirmation"), "evidence_count": len(c.get("evidence") or []),
            "candidate_ids": c["candidate_ids"], "image_ids": c["image_ids"]}


def panel(object_id: str, run_id: str, spid: str) -> dict[str, Any]:
    run = vc_jobs.get_run(object_id, run_id)
    item = next((it for it in run["items"] if it["spid"] == spid), None)
    if item is None:
        raise jobs.JobError("лист прогона не найден")
    head = {"spid": spid, "title": item.get("title"), "a": {"label": run["a"].get("label"), "page": item.get("a_page")},
            "b": {"label": run["b"].get("label"), "page": item.get("b_page")},
            "enabled": settings.enabled(), "history": _index(object_id, run_id).get(spid) or []}
    active = _active_job(object_id, run_id, spid)
    res = load_result(object_id, run_id, spid)
    if active:
        prog = active.get("progress") or {}
        head.update(state="running" if active["_sheet"]["ai_status"] == "running" else "queued",
                    job={"job_id": active["job_id"], "state": active["state"], "started_at": active.get("started_at"),
                         "current": prog.get("current"), "sheets_total": prog.get("sheets_total"),
                         "sheets_done": prog.get("sheets_done")})
        head["state_label"] = val.STATE_LABELS[head["state"]]
        if not res:
            return head
    if not res:
        if item.get("result_status") in ENGINE_SAME:
            state = "engine_same"
        else:
            state = "not_processed"
        return head | {"state": state, "state_label": val.STATE_LABELS[state],
                       "old_job": _old_result(object_id, run_id, spid)}
    stale = stale_reason(object_id, run_id, res)
    out = head | {
        "state": head.get("state") or ("stale" if stale else res["state"]),
        "result_state": res["state"], "stale": stale, "job_id": res["job_id"], "created_at": res["created_at"],
        "changes": [_change_view(c) for c in res["changes"]],
        "unresolved": [{"id": u["id"], "object": u["object"], "place": u.get("place") or "", "question": u["question"],
                        "evidence_count": len(u.get("evidence") or [])} for u in res["unresolved"]],
        "declared": res.get("declared") or [], "disclaimer": res["disclaimer"],
        "excluded": _excluded_summary(res),
        "diagnostics": _diagnostics(res),
    }
    out["state_label"] = val.STATE_LABELS.get(out["state"], out["state"])
    return out


def _excluded_summary(res: dict) -> list[dict]:
    counts: dict[str, int] = {}
    for e in res.get("excluded") or []:
        counts[e["reason"]] = counts.get(e["reason"], 0) + len(e["candidate_ids"])
    out = [{"label": sc.EXCLUDE_LABELS.get(k, k), "count": v, "by": "модель"} for k, v in counts.items()]
    from .collect import REASONS
    for k, v in (res["diagnostics"].get("excluded_by_program") or {}).items():
        out.append({"label": REASONS.get(k, k), "count": v, "by": "программа (указатели движка)"})
    return out


def _diagnostics(res: dict) -> dict:
    d = res["diagnostics"]
    calls = d.get("calls") or []
    tot = {k: sum(int((c.get("usage") or {}).get(k) or 0) for c in calls)
           for k in ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens")}
    return {"executor": res.get("executor"), "launches": d.get("launches"), "cached": d.get("cached"),
            "calls": [{k: c.get(k) for k in ("launch", "purpose", "seconds", "ok", "error_kind", "images",
                                             "prompt_chars")} | {"usage": c.get("usage")} for c in calls],
            "tokens": tot, "prepare_s": d.get("prepare_s"), "validate_s": d.get("validate_s"),
            "total_s": d.get("total_s"), "budget": d.get("budget"), "coverage": res.get("coverage"),
            "pointer_ledger_counts": d.get("pointer_ledger_counts"), "issues": d.get("issues") or [],
            "context_requests": d.get("context_requests") or [], "context": d.get("context") or [],
            "packet_id": res.get("packet_id")}


def focus(object_id: str, run_id: str, spid: str, item_id: str) -> dict[str, Any]:
    """Рамки доказательств пункта для просмотрщика (pt → пиксели холста системы A)."""
    res = load_result(object_id, run_id, spid)
    if not res:
        return {"available": False, "reason": "описания листа нет"}
    stale = stale_reason(object_id, run_id, res)
    if stale.startswith("результат сравнения"):
        return {"available": False, "reason": "Описание устарело: " + stale}
    it = next((x for x in res["changes"] + res["unresolved"] if x["id"] == item_id), None)
    if not it:
        return {"available": False, "reason": "пункт не найден"}
    canvas = res.get("canvas") or {}
    boxes = []
    for e in it.get("evidence") or []:
        b = val.box_a(res, e)
        boxes.append({"id": f"{e['id']} {e['side']}", "side": e["side"], "what": e["what"],
                      "box_a": geo.rounded(b), "px": geo.rounded(geo.to_px(canvas, b), 1)})
    if not boxes:
        return {"available": False, "reason": "у пункта нет рамок доказательств"}
    union = geo.union([b["box_a"] for b in boxes])
    return {"available": True, "spid": spid, "label": it.get("object") or item_id, "boxes": boxes,
            "union_px": geo.rounded(geo.to_px(canvas, union), 1)}


def md(object_id: str, run_id: str, spid: str) -> str:
    res = load_result(object_id, run_id, spid)
    if not res:
        raise jobs.JobError("описания листа нет")
    return report.sheet_md(res)


def packet_file(object_id: str, run_id: str, spid: str, name: str) -> Path:
    res = load_result(object_id, run_id, spid)
    if not res:
        raise jobs.JobError("описания листа нет")
    pdir = jobs.sheet_dir(object_id, run_id, res["job_id"], spid) / "packet"
    allowed = {"prompt.md", "packet.json", "manifest.json", "prompt.context.md"}
    if name in allowed:
        return pdir / name
    if name.startswith("images/") and name.endswith(".png") and "/" not in name[7:]:
        from backend.app.services.version_comparison import paths
        return pdir / "images" / paths.safe_id(name[7:])
    raise jobs.JobError("неизвестный файл пакета")
