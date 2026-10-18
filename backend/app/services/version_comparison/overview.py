"""Обзор проектов объекта: этапы подготовки и проверки каждого проекта одной таблицей.

Страница «Проекты» раздела «Сравнение версий». Только чтение файлов хранилища:
штатные ``jobs.matrix`` / ``series.get`` / ``jobs.get_run`` / ``get_job`` при чтении
дописывают состояние (прерванные задания) или ставят расчёт в очередь, поэтому здесь
JSON читается напрямую. Разобранные файлы кэшируются по (mtime, размер).

Этапы проекта:
    upload   — загрузка (пакет «Загрузить несколько проектов»);
    profile  — разбор версий (профили страниц);
    matrix   — сопоставление листов (матрица lineage);
    luna     — проверка сомнительных пар Луной (ответы в lineage ``ai``);
    series   — «Сравнить все соседние» (последняя серия проекта);
    ai       — «Описать изменения ИИ» по серии (файл ai_sheet_changes.json и задания пар);
    report   — «Сводка изменений (MD)» и «Отчёт PDF».
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from . import jobs, paths, recovery, series, store

ACTIVE = ("queued", "running", "cancelling", "pausing")
AI_ACTIVE = ("queued", "running", "cancelling")
AI_PAUSED = ("paused_quota", "paused_auth", "paused_user")
AI_STOPPED = ("interrupted", "cancelled", "failed")
BATCH_LABELS = {"uploading": "загрузка файлов", "queued": "ожидает разбора", "detecting": "разбор PDF",
                "review": "требуется проверка", "ready": "ожидает обработки", "preparing": "подготовка версий",
                "comparing": "сравнение версий", "done": "загружен", "failed": "ошибка", "skipped": "исключён"}
BATCH_TERMINAL = ("done", "failed", "skipped")

_cache: dict[str, tuple[int, int, Any]] = {}
_cache_lock = threading.Lock()


def _read(path: Path, reduce=None) -> Any:
    """JSON-файл (или результат ``reduce`` над ним) с кэшем по mtime/размеру; None — файла нет."""
    try:
        st = path.stat()
    except OSError:
        return None
    key = str(path)
    with _cache_lock:
        hit = _cache.get(key)
    if hit and hit[0] == st.st_mtime_ns and hit[1] == st.st_size:
        return hit[2]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    value = reduce(data) if reduce else data
    with _cache_lock:
        _cache[key] = (st.st_mtime_ns, st.st_size, value)
    return value


# ------------------------------------------------------------------ этапы

def _profile_stage(object_id: str, versions: list[dict]) -> dict[str, Any]:
    states = [jobs.profile_status(object_id, v["sha256"]).get("state") for v in versions]
    total, ready = len(states), states.count("ready")
    failed = states.count("failed")
    failed_versions = [v["version_id"] for v, s in zip(versions, states) if s == "failed"]
    running = sum(1 for s in states if s in ("queued", "running"))
    if failed:
        state = "failed"
    elif ready == total:
        state = "done"
    elif running:
        state = "running"
    else:
        state = "todo"
    return {"state": state, "ready": ready, "total": total, "failed": failed, "failed_versions": failed_versions}


def _lineage_reduce(data: dict) -> dict[str, Any]:
    res = data.get("result") or {}
    rows = res.get("rows") or []
    unmatched = groups = 0
    for row in rows:
        for cell in (row.get("cells") or {}).values():
            if cell.get("state") == "unmatched":
                unmatched += 1
            elif cell.get("confidence") == "group":
                groups += 1
    ai = data.get("ai") or {}
    decisions: dict[str, int] = {}
    for note in (ai.get("notes") or {}).values():
        d = note.get("decision") or "?"
        decisions[d] = decisions.get(d, 0) + 1
    return {"signature": data.get("signature"), "has_result": bool(res), "built_at": data.get("built_at"),
            "rows": len(rows), "unmatched": unmatched, "groups": groups,
            "unassigned": sum(len(v) for v in (res.get("unassigned") or {}).values()),
            "manual": len(data.get("edits") or []), "edits": data.get("edits") or [],
            "ai_edits": list(ai.get("edits") or []), "ai_at": ai.get("at"), "decisions": decisions,
            "has_ai": bool(data.get("ai"))}


def _matrix_stage(object_id: str, project_id: str, versions: list[dict], profile: dict) -> dict[str, Any]:
    lin = _read(paths.lineage_path(object_id, project_id), _lineage_reduce) or {}
    out = {k: lin.get(k) for k in ("rows", "unmatched", "groups", "unassigned", "manual", "built_at")}
    if len(versions) < 2:
        return out | {"state": "na"}
    if profile["state"] != "done" or not lin.get("has_result"):
        return out | {"state": "todo"}
    sig = jobs._signature(versions, lin["ai_edits"] + lin["edits"])  # noqa: SLF001 — та же проверка, что в matrix()
    if lin.get("signature") != sig:
        return out | {"state": "stale"}
    return out | {"state": "attention" if lin["unmatched"] else "done"}


def _luna_stage(lin: dict, luna_job: dict | None, project_id: str) -> dict[str, Any]:
    dec = lin.get("decisions") or {}
    hints = sum(v for k, v in dec.items() if k.startswith("hint_"))
    against = dec.get("medium_other", 0) + dec.get("medium_none", 0)
    out = {"accepted": len(lin.get("ai_edits") or []), "hints": hints, "against": against,
           "questions": sum(dec.values()), "at": lin.get("ai_at")}
    job = luna_job or {}
    job_state = job.get("state") or "none"
    scope = job.get("scope")
    mine = not scope or project_id in scope
    # Задание Луны одно на объект: идёт по другим шифрам — запуск по этому проекту подождёт.
    if job_state in ("running", "cancelling", "pausing"):
        if mine:
            return out | {"state": "pausing" if job_state == "pausing" else "running", "scope": scope}
        out["busy_other"] = True
    elif mine and job_state in ("paused", "interrupted", "cancelled", "paused_quota", "paused_auth") \
            and job.get("created_at", "") > (lin.get("ai_at") or ""):
        return out | {"state": "paused" if job_state == "paused" else "interrupted", "scope": scope,
                      "reason": job.get("reason")}
    if lin.get("has_ai"):
        return out | {"state": "attention" if against or hints else "done"}
    if job_state == "done":
        listed = {p.get("project_id"): p for p in job.get("projects") or []}
        if project_id in listed and not listed[project_id].get("questions"):
            return out | {"state": "skip"}
    return out | {"state": "todo"}


def _run_reduce(run: dict) -> dict[str, Any]:
    return {"state": run.get("state"), "progress": run.get("progress"), "summary": series.summary(run),
            "owner_process": run.get("owner_process")}


def _series_stage(object_id: str, project: dict, series_list: list[dict]) -> dict[str, Any]:
    versions = project["versions"]
    if len(versions) < 2:
        return {"state": "na"}
    mine = sorted((s for s in series_list if s.get("project_id") == project["project_id"]),
                  key=lambda s: (s.get("created_at") or "", s.get("series_id") or ""), reverse=True)
    if not mine:
        return {"state": "todo", "total": len(versions) - 1, "done": 0}
    s = mine[0]
    pairs = s.get("pairs") or []
    matches = (len(pairs) == len(versions) - 1
               and all(p.get("a") == versions[i] and p.get("b") == versions[i + 1] for i, p in enumerate(pairs)))
    done = changed = different = review = added = removed = failed = sheets_done = sheets_total = 0
    for p in pairs:
        run = _read(paths.run_dir(object_id, p["run_id"]) / "run.json", _run_reduce) if p.get("run_id") else None
        if not run:
            continue
        prog = run.get("progress") or {}
        sheets_done += prog.get("done") or 0
        sheets_total += prog.get("total") or 0
        if run.get("state") == "done":
            done += 1
        sm = run.get("summary") or {}
        different += sm.get("different", 0)
        review += sm.get("needs_review", 0)
        added += sm.get("added", 0)
        removed += sm.get("removed", 0)
        failed += sm.get("failed", 0)
        if sm.get("different") or sm.get("needs_review") or sm.get("added") or sm.get("removed"):
            changed += 1
    raw = s.get("state") or "queued"
    if raw in ACTIVE and not recovery.owner_alive(s.get("owner_process")):
        raw = "paused" if raw == "pausing" else "interrupted"
    state = {"done": "done", "queued": "running", "running": "running", "cancelling": "running", "pausing": "pausing",
             "paused": "paused", "interrupted": "interrupted", "cancelled": "interrupted",
             "failed": "failed"}.get(raw, raw)
    if state == "done" and not matches:
        state = "stale"
    return {"state": state, "raw_state": s.get("state"), "series_id": s.get("series_id"), "matches_order": matches,
            "total": len(pairs), "done": done, "changed": changed, "different": different, "needs_review": review,
            "added": added, "removed": removed, "failed": failed, "sheets_done": sheets_done,
            "sheets_total": sheets_total, "finished_at": s.get("finished_at"), "created_at": s.get("created_at")}


def _ai_job_reduce(job: dict) -> dict[str, Any]:
    return {"state": job.get("state"), "progress": job.get("progress") or {}, "usage": job.get("usage") or {},
            "finished_at": job.get("finished_at")}


def _ai_stage(object_id: str, ser: dict, queue: dict) -> dict[str, Any]:
    sid = ser.get("series_id")
    if not sid or ser.get("state") == "na":
        return {"state": "na"}
    waiting = [w for w in queue.get("waiting") or [] if w.get("object_id") == object_id]
    for i, w in enumerate(waiting):
        if w.get("series_id") == sid:
            return {"state": "queued", "position": i + 1 + (1 if queue.get("current") else 0)}
    record = _read(paths.object_dir(object_id) / "series" / paths.safe_id(sid) / "ai_sheet_changes.json")
    if not record:
        return {"state": "todo" if ser.get("done") else "blocked"}
    states: list[str] = []
    sheets = sheets_done = failed = calls = 0
    finished = None
    for p in record.get("pairs") or []:
        if not p.get("job_id") or not p.get("run_id"):
            continue
        job = _read(paths.run_dir(object_id, p["run_id"]) / "ai" / paths.safe_id(p["job_id"]) / "job.json",
                    _ai_job_reduce)
        if not job:
            continue
        states.append(job.get("state") or "")
        prog = job["progress"]
        sheets += prog.get("sheets_total") or 0
        sheets_done += prog.get("sheets_done") or 0
        failed += prog.get("chunks_failed") or 0
        calls += job["usage"].get("calls") or 0
        finished = max(finished or "", job.get("finished_at") or "") or finished
    if not states:
        state = "todo"
    elif "pausing" in states:
        state = "pausing"
    elif any(s in AI_ACTIVE for s in states):
        state = "running"
    elif any(s in AI_PAUSED for s in states):
        state = "paused"
    elif any(s in AI_STOPPED for s in states):
        state = "interrupted"
    elif all(s == "done" for s in states):
        state = "done"
    else:
        state = "partial"
    cur = queue.get("current") or {}
    if cur.get("object_id") == object_id and cur.get("series_id") == sid and state == "todo":
        state = "running"
    return {"state": state, "sheets": sheets, "sheets_done": sheets_done, "failed": failed, "calls": calls,
            "pairs": len(states), "finished_at": finished, "summary_md": bool(states), "series_id": sid,
            "user_paused": "paused_user" in states}


def _report_reduce(job: dict) -> dict[str, Any]:
    return {k: job.get(k) for k in ("report_id", "project_id", "state", "stage", "created_at", "finished_at")} | {
        "series_id": (job.get("origin") or {}).get("series_id")}


def _reports(object_id: str) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for path in (paths.object_dir(object_id) / "md_reports").glob("*/job.json"):
        rep = _read(path, _report_reduce)
        if not rep or not rep.get("project_id"):
            continue
        rep = rep | {"pdf": rep.get("state") == "done" and (path.parent / "report.pdf").exists()}
        prev = latest.get(rep["project_id"])
        if not prev or (rep.get("created_at") or "") > (prev.get("created_at") or ""):
            latest[rep["project_id"]] = rep
    return latest


def _batch_items(object_id: str) -> tuple[dict[str, dict], list[dict]]:
    """Позиции пакетов: по project_id — незавершённые; отдельно — ещё не опубликованные в каталоге."""
    by_project: dict[str, dict] = {}
    pending: list[dict] = []
    for path in sorted((paths.object_dir(object_id) / "batches").glob("*/batch.json")):
        batch = _read(path)
        if not batch or batch.get("deleted"):
            continue
        for it in batch.get("items") or []:
            state = it.get("state") or ""
            info = {"batch_id": batch.get("batch_id"), "item_id": it.get("item_id"), "title": it.get("title"),
                    "code": it.get("code"), "state": state, "label": BATCH_LABELS.get(state, state),
                    "error": it.get("error"), "waiting_reason": it.get("waiting_reason"),
                    "files": len(it.get("files") or []), "compare_progress": it.get("compare_progress"),
                    "paused": bool(batch.get("paused"))}
            if it.get("published") and it.get("project_id"):
                if state not in BATCH_TERMINAL or state == "failed":
                    by_project[it["project_id"]] = info
            elif state != "skipped":
                pending.append(info)
    return by_project, pending


# ------------------------------------------------------------------ шаг

def _next_step(st: dict[str, dict], versions: int) -> dict[str, str]:
    up, pr, mx, ln, se, ai, rp = (st[k] for k in ("upload", "profile", "matrix", "luna", "series", "ai", "report"))
    run = lambda label: {"code": "wait", "label": label, "tone": "run"}  # noqa: E731
    act = lambda code, label: {"code": code, "label": label, "tone": "act"}  # noqa: E731
    if up.get("state") == "failed":
        return {"code": "upload_failed", "label": "Ошибка загрузки — открыть пакет", "tone": "bad"}
    if up.get("state") == "running":
        return run("Идёт загрузка: " + (up.get("label") or ""))
    if pr["state"] == "failed":
        return {"code": "profile_failed", "label": "Разбор версии с ошибкой — повторить", "tone": "bad"}
    if versions < 2:
        return {"code": "one_version", "label": "Одна версия — сравнивать не с чем", "tone": "muted"}
    if pr["state"] == "running":
        return run("Идёт разбор версий")
    if pr["state"] == "todo" or mx["state"] in ("todo", "stale"):
        return act("prepare", "Подготовить: разбор и сопоставление листов")
    for key, stage, name in (("luna", ln, "Луна"), ("series", se, "Сравнение соседних"),
                             ("ai", ai, "ИИ-описание"), ("report", rp, "Отчёт PDF")):
        if stage.get("state") == "pausing":
            return run(f"{name}: ставится на паузу")
        if stage.get("state") in ("running", "queued"):
            return run(f"{name}: идёт" if stage["state"] == "running" else f"{name}: в очереди, №{stage.get('position')}")
        if stage.get("state") == "paused":
            return act(key + "_resume", f"{name} на паузе — продолжить")
        if stage.get("state") in ("interrupted", "failed") and key != "report":
            return act(key + "_resume", f"{name} прервано — досчитать")
    if se["state"] in ("todo", "stale"):
        return act("series", "Сравнить все соседние")
    if mx.get("unmatched") and ln["state"] == "todo":
        return act("luna", f"Несопоставленных листов: {mx['unmatched']} — Луна или вручную")
    if ai["state"] == "todo":
        if not se.get("changed"):
            return {"code": "done", "label": "Изменений содержания нет", "tone": "ok"}
        return act("ai", "Описать изменения ИИ")
    return {"code": "done", "label": "Готово", "tone": "ok"}


def build(object_id: str) -> dict[str, Any]:
    catalog = store.load_catalog(object_id)
    odir = paths.object_dir(object_id)
    series_list = [s for s in (_read(p) for p in sorted((odir / "series").glob("*/series.json"))) if s]
    queue = _read(paths.root() / "ai_series_queue.json") or {}
    luna_job = _read(odir / "sheet_match" / "job.json")
    reports = _reports(object_id)
    batch_by_project, batch_pending = _batch_items(object_id)
    projects = []
    for proj in catalog["projects"]:
        pid = proj["project_id"]
        versions = store.project_versions(catalog, pid)
        lin = _read(paths.lineage_path(object_id, pid), _lineage_reduce) or {}
        up = batch_by_project.get(pid)
        stages: dict[str, dict] = {"upload": {"state": "done", "versions": len(versions)}
                                   if not up else up | {"state": "failed" if up["state"] == "failed" else "running"}}
        stages["profile"] = _profile_stage(object_id, versions)
        stages["matrix"] = _matrix_stage(object_id, pid, versions, stages["profile"])
        stages["luna"] = _luna_stage(lin, luna_job, pid) if len(versions) > 1 else {"state": "na"}
        stages["series"] = _series_stage(object_id, proj, series_list)
        stages["ai"] = _ai_stage(object_id, stages["series"], queue)
        rep = reports.get(pid)
        rstate = (rep or {}).get("state")
        stages["report"] = {"state": ("done" if rep and rep.get("pdf") else
                                      "pausing" if rstate == "pausing" else
                                      "running" if rstate in ("queued", "running", "cancelling") else
                                      "paused" if rstate in ("paused", "paused_quota", "paused_auth") else
                                      "interrupted" if rstate in ("interrupted", "cancelled") else
                                      "failed" if rstate in ("failed", "incomplete") else
                                      "todo" if stages["ai"].get("summary_md") else "blocked"),
                            "series_id": stages["series"].get("series_id"),
                            "summary_md": bool(stages["ai"].get("summary_md")), "md_report": rep}
        if stages["series"].get("series_id"):
            from backend.app.services.version_comparison_ai.whole_specifications import summary_gate
            gate = summary_gate(object_id, stages["series"]["series_id"])
            if gate is not None:
                stages["report"]["summary_md"] = gate["ready"]
        last = versions[-1] if versions else {}
        projects.append({
            "project_id": pid, "title": proj.get("title"), "code": proj.get("code"),
            "versions": len(versions), "pages": last.get("page_count"),
            "last_label": last.get("user_label") or last.get("file_name"),
            "first_label": (versions[0].get("user_label") or versions[0].get("file_name")) if versions else None,
            "uploaded_at": max((v.get("uploaded_at") or "" for v in versions), default=None),
            "updated_at": proj.get("updated_at"), "stages": stages,
            "next": _next_step(stages, len(versions)),
        })
    luna = {k: (luna_job or {}).get(k) for k in ("state", "phase", "finished_at", "totals", "scope")} \
        if luna_job else {"state": "none"}
    return {"object_id": object_id, "projects": projects, "pending_uploads": batch_pending, "luna": luna,
            "ai_queue": {"current": (queue.get("current") or {}).get("code"),
                         "waiting": [w.get("code") for w in queue.get("waiting") or []]}}
