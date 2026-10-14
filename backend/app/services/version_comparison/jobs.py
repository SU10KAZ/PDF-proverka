"""Фоновые задачи сравнения версий: профили, строки матрицы, прогоны пар.

CPU-тяжёлая работа подаётся в ОБЩИЙ пул процессов бэкенда
(``common/cpu_pool``) окном не больше ``VERSION_COMPARISON_WORKERS``; если
пула нет, считаем в потоке. Состояние прогона — в ``runs/<id>/run.json``;
после рестарта незавершённый прогон помечается «прерван» и может быть досчитан
(готовые листы не пересчитываются: результаты кэшируются по содержимому).
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import traceback
from concurrent.futures import BrokenExecutor, Future, ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.app.services.common import cpu_pool
from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save

from . import engine, lineage, paths, profile, settings, store
from .settings import ALGO_VERSION
from .status import STATUS_LABELS

BOOT_ID = secrets.token_hex(6)
_lock = threading.Lock()
_profile_state: dict[tuple[str, str], dict[str, Any]] = {}
_lineage_state: dict[tuple[str, str], dict[str, Any]] = {}
_cancel_events: dict[str, threading.Event] = {}
_threads = ThreadPoolExecutor(max_workers=4, thread_name_prefix="vc-job")        # профили, строки матрицы
_run_threads = ThreadPoolExecutor(max_workers=2, thread_name_prefix="vc-run")    # прогоны пар
_key_locks: dict[str, threading.Lock] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _executor():
    return cpu_pool.get_executor()


def _call(fn: Callable, *args) -> Any:
    """Выполнить CPU-функцию в общем пуле (или в потоке, если пула нет)."""
    ex = _executor()
    if ex is None:
        return fn(*args)
    try:
        return ex.submit(fn, *args).result()
    except (BrokenExecutor, OSError) as exc:
        cpu_pool.disable_pool(f"{type(exc).__name__}: {exc}")
        return fn(*args)


def _pool_profile(root: str, object_id: str, sha: str) -> int:
    os.environ["VERSION_COMPARISON_ROOT"] = root
    return profile.build(object_id, sha)["page_count"]


# ------------------------------------------------------------------- профили

def profile_status(object_id: str, sha: str) -> dict[str, Any]:
    if (paths.profile_dir(object_id, sha) / "pages.json").exists():
        return {"state": "ready"}
    with _lock:
        st = _profile_state.get((object_id, sha))
    if st:
        return dict(st)
    failed = paths.profile_dir(object_id, sha) / "failed.json"
    if failed.exists():
        return json.loads(failed.read_text(encoding="utf-8"))
    return {"state": "missing"}


def ensure_profiles(object_id: str, shas: list[str]) -> None:
    for sha in shas:
        if profile_status(object_id, sha)["state"] in ("ready", "queued", "running"):
            continue
        with _lock:
            _profile_state[(object_id, sha)] = {"state": "queued"}
        _threads.submit(_profile_job, object_id, sha)


def _profile_job(object_id: str, sha: str) -> None:
    with _lock:
        _profile_state[(object_id, sha)] = {"state": "running"}
    try:
        _call(_pool_profile, str(paths.root()), object_id, sha)
        with _lock:
            _profile_state.pop((object_id, sha), None)
    except Exception as exc:
        info = {"state": "failed", "error": f"{type(exc).__name__}: {exc}"}
        atomic_write_json(paths.profile_dir(object_id, sha) / "failed.json", info)
        with _lock:
            _profile_state[(object_id, sha)] = info


def retry_profile(object_id: str, sha: str) -> None:
    (paths.profile_dir(object_id, sha) / "failed.json").unlink(missing_ok=True)
    with _lock:
        _profile_state.pop((object_id, sha), None)
    ensure_profiles(object_id, [sha])


# ------------------------------------------------------------- строки матрицы

def _lineage_file(object_id: str, project_id: str) -> Path:
    return paths.lineage_path(object_id, project_id)


def _load_lineage(object_id: str, project_id: str) -> dict[str, Any]:
    path = _lineage_file(object_id, project_id)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"edits": []}


def _signature(versions: list[dict], edits: list[dict]) -> str:
    raw = lineage.signature(versions) + json.dumps(edits, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()


def _lineage_job(object_id: str, project_id: str, versions: list[dict], edits: list[dict], sig: str) -> None:
    key = (object_id, project_id)
    try:
        result = lineage.build(object_id, versions, edits)

        def save(data):
            if _signature(versions, data.get("edits", [])) == sig:
                data["signature"] = sig
                data["result"] = result
                data["built_at"] = _now()
            return data
        load_modify_save(_lineage_file(object_id, project_id), save, default={"edits": []})
        with _lock:
            _lineage_state.pop(key, None)
    except Exception as exc:
        with _lock:
            _lineage_state[key] = {"state": "failed", "error": f"{type(exc).__name__}: {exc}", "signature": sig}


def _version_header(object_id: str, v: dict[str, Any], index: int) -> dict[str, Any]:
    prof = profile.load(object_id, v["sha256"])
    summary = (prof or {}).get("summary") or {}
    return {
        "version_id": v["version_id"], "index": index, "user_label": v.get("user_label"),
        "file_name": v.get("file_name"), "page_count": v.get("page_count") or (prof or {}).get("page_count"),
        "sha256": v["sha256"], "uploaded_at": v.get("uploaded_at"),
        "detected": {"izm_max": summary.get("izm_max", (v.get("detected") or {}).get("izm_max")),
                     "release_dates": summary.get("release_dates") or (v.get("detected") or {}).get("release_dates") or [],
                     "designation": summary.get("base_designation") or (v.get("detected") or {}).get("designation"),
                     "kinds": summary.get("kinds")},
        "profile": profile_status(object_id, v["sha256"]),
    }


def matrix(object_id: str, project_id: str) -> dict[str, Any]:
    catalog = store.load_catalog(object_id)
    proj = store.project(catalog, project_id)
    versions = store.project_versions(catalog, project_id)
    ensure_profiles(object_id, [v["sha256"] for v in versions])
    headers = [_version_header(object_id, v, i) for i, v in enumerate(versions)]
    base = {"project": {k: proj.get(k) for k in ("project_id", "title", "code")}, "versions": headers}
    if not versions:
        return base | {"state": "empty", "rows": []}
    pending = [h for h in headers if h["profile"]["state"] != "ready"]
    if pending:
        failed = [h for h in pending if h["profile"]["state"] == "failed"]
        return base | {"state": "failed" if failed else "profiling", "rows": [],
                       "progress": {"ready": len(headers) - len(pending), "total": len(headers)}}
    data = _load_lineage(object_id, project_id)
    edits = data.get("edits", [])
    sig = _signature(versions, edits)
    if data.get("signature") == sig and data.get("result"):
        res = data["result"]
        return base | {"state": "ready", "rows": res["rows"], "unassigned": res.get("unassigned", {}),
                       "duplicates": res.get("duplicates", {}), "edits": edits, "built_at": data.get("built_at")}
    key = (object_id, project_id)
    with _lock:
        st = _lineage_state.get(key)
        if st and st.get("signature") == sig and st["state"] == "failed":
            return base | {"state": "failed", "error": st["error"], "rows": []}
        if not st or st.get("signature") != sig:
            _lineage_state[key] = {"state": "building", "signature": sig}
            _threads.submit(_lineage_job, object_id, project_id, versions, edits, sig)
    return base | {"state": "building", "rows": []}


def add_link_edit(object_id: str, project_id: str, edit: dict[str, Any]) -> dict[str, Any]:
    op = edit.get("op")
    if op not in ("move", "detach", "absent"):
        raise store.CatalogError("неизвестная правка связи")
    edit = {k: edit[k] for k in ("op", "page", "to_row_anchor", "row_anchor", "version") if k in edit}
    edit["at"] = _now()

    def add(data):
        data.setdefault("edits", []).append(edit)
        return data
    load_modify_save(_lineage_file(object_id, project_id), add, default={"edits": []})
    return matrix(object_id, project_id)


def undo_link_edit(object_id: str, project_id: str) -> dict[str, Any]:
    def pop(data):
        if data.get("edits"):
            data["edits"].pop()
        return data
    load_modify_save(_lineage_file(object_id, project_id), pop, default={"edits": []})
    return matrix(object_id, project_id)


# ------------------------------------------------------------------- прогоны

def _run_path(object_id: str, run_id: str) -> Path:
    return paths.run_dir(object_id, run_id) / "run.json"


def _pair_confidence(row: dict[str, Any], order: list[str], a_vid: str, b_vid: str) -> str:
    ia, ib = order.index(a_vid), order.index(b_vid)
    lo, hi = min(ia, ib), max(ia, ib)
    cells = [row["cells"][order[i]] for i in range(lo, hi + 1)]
    if any(c.get("manual") for c in cells if c["state"] == "matched"):
        return "manual"
    if any(c["state"] != "matched" for c in cells[1:-1]):
        return "bridged"
    if any(c.get("confidence") == "medium" for c in cells[1:]):
        return "medium"
    return "high"


def _profile_page(object_id: str, sha: str, page: int, cache: dict) -> dict[str, Any]:
    if sha not in cache:
        cache[sha] = profile.load(object_id, sha)
    return cache[sha]["pages"][page - 1]


def create_run(object_id: str, project_id: str, a_vid: str, b_vid: str, scope: str,
               sheet_pairs: list[dict[str, int]] | None = None) -> dict[str, Any]:
    if a_vid == b_vid:
        raise store.CatalogError("выберите две разные версии")
    catalog = store.load_catalog(object_id)
    proj = store.project(catalog, project_id)
    if a_vid not in proj["versions"] or b_vid not in proj["versions"]:
        raise store.CatalogError("версии не принадлежат проекту")
    va, vb = catalog["versions"][a_vid], catalog["versions"][b_vid]
    mx = matrix(object_id, project_id)
    if mx["state"] != "ready":
        raise store.CatalogError("сопоставление листов ещё не готово")
    order = [h["version_id"] for h in mx["versions"]]
    items: list[dict[str, Any]] = []
    if scope == "manual":
        if not sheet_pairs:
            raise store.CatalogError("не выбраны пары листов")
        for i, sp in enumerate(sheet_pairs):
            items.append({"spid": f"m{i + 1}", "kind": "compare", "row_id": None,
                          "a_page": int(sp["a_page"]), "b_page": int(sp["b_page"]),
                          "confidence": "manual", "reasons": [], "status": "queued"})
    elif scope == "all":
        for row in mx["rows"]:
            ca, cb = row["cells"].get(a_vid), row["cells"].get(b_vid)
            base = {"row_id": row["row_id"], "title": row.get("display_title")}
            if ca["state"] == "matched" and cb["state"] == "matched":
                conf = _pair_confidence(row, order, a_vid, b_vid)
                reasons = []
                if conf in ("medium", "bridged"):
                    reasons.append("MATCH_LOW_CONFIDENCE")
                if ca.get("dups") or cb.get("dups"):
                    reasons.append("DUPLICATE_SHEET")
                items.append(base | {"spid": row["row_id"], "kind": "compare", "a_page": ca["page"],
                                     "b_page": cb["page"], "confidence": conf, "reasons": reasons,
                                     "status": "queued"})
            elif ca["state"] == "matched":
                st = "removed" if cb["state"] == "absent" else "unmatched"
                items.append(base | {"spid": row["row_id"], "kind": "only_a", "a_page": ca["page"], "b_page": None,
                                     "status": "done", "result_status": st, "candidates": cb.get("candidates", [])})
            elif cb["state"] == "matched":
                st = "added" if ca["state"] == "absent" else "unmatched"
                items.append(base | {"spid": row["row_id"], "kind": "only_b", "a_page": None, "b_page": cb["page"],
                                     "status": "done", "result_status": st, "candidates": ca.get("candidates", [])})
    else:
        raise store.CatalogError("неизвестный охват прогона")
    run_id = datetime.now().strftime("run%Y%m%d%H%M%S") + secrets.token_hex(2)
    total = sum(1 for it in items if it["kind"] == "compare")
    run = {
        "schema": "version_comparison.run/v1", "run_id": run_id, "object_id": object_id, "project_id": project_id,
        "a": {"version_id": a_vid, "label": va.get("user_label"), "file_name": va.get("file_name"), "sha256": va["sha256"]},
        "b": {"version_id": b_vid, "label": vb.get("user_label"), "file_name": vb.get("file_name"), "sha256": vb["sha256"]},
        "scope": scope, "params": {"dpi": settings.dpi(), "tol_pt": settings.tolerance_pt(), "algo": ALGO_VERSION},
        "state": "queued", "owner": BOOT_ID, "created_at": _now(), "items": items,
        "progress": {"total": total, "done": 0, "failed": 0},
    }
    atomic_write_json(_run_path(object_id, run_id), run)
    _start(object_id, run_id)
    return run


def _start(object_id: str, run_id: str) -> None:
    ev = threading.Event()
    with _lock:
        _cancel_events[run_id] = ev
    _run_threads.submit(_run_job, object_id, run_id, ev)


def _cache_key(rec_a: dict, rec_b: dict, params: dict, reasons: list[str]) -> str | None:
    if not rec_a.get("content_hash") or not rec_b.get("content_hash"):
        return None
    raw = json.dumps([rec_a["content_hash"], rec_b["content_hash"], rec_a.get("annots_hash"), rec_b.get("annots_hash"),
                      params, sorted(reasons), ALGO_VERSION], sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _task_for(object_id: str, run: dict, item: dict, cache: dict) -> tuple[dict, Path]:
    sha_a, sha_b = run["a"]["sha256"], run["b"]["sha256"]
    rec_a = _profile_page(object_id, sha_a, item["a_page"], cache)
    rec_b = _profile_page(object_id, sha_b, item["b_page"], cache)
    key = _cache_key(rec_a, rec_b, run["params"], item.get("reasons", []))
    out_dir = (paths.pair_cache_dir(object_id) / key) if key else paths.sheet_dir(object_id, run["run_id"], item["spid"])
    task = {
        "sheet_pair_id": item["spid"], "row_id": item.get("row_id"), "out_dir": str(out_dir),
        "params": {"dpi": run["params"]["dpi"], "tol_pt": run["params"]["tol_pt"]},
        "reasons": item.get("reasons", []), "match": {"confidence": item.get("confidence")},
        "a": {"version_id": run["a"]["version_id"], "sha": sha_a, "page": item["a_page"],
              "pdf": str(paths.source_pdf(object_id, sha_a)), "profile_dir": str(paths.profile_dir(object_id, sha_a)),
              "profile": rec_a},
        "b": {"version_id": run["b"]["version_id"], "sha": sha_b, "page": item["b_page"],
              "pdf": str(paths.source_pdf(object_id, sha_b)), "profile_dir": str(paths.profile_dir(object_id, sha_b)),
              "profile": rec_b},
    }
    return task, out_dir


def _update(object_id: str, run_id: str, fn) -> dict[str, Any]:
    return load_modify_save(_run_path(object_id, run_id), fn, default={})


def _finish_item(object_id: str, run_id: str, spid: str, **fields) -> None:
    def apply(run):
        for it in run["items"]:
            if it["spid"] == spid:
                it.update(fields)
        done = sum(1 for it in run["items"] if it["kind"] == "compare" and it["status"] in ("done", "failed"))
        run["progress"] = {"total": run["progress"]["total"], "done": done,
                           "failed": sum(1 for it in run["items"] if it["status"] == "failed")}
        return run
    _update(object_id, run_id, apply)


def _compute_cached(task: dict, out_dir: Path) -> dict[str, Any]:
    result = out_dir / "result.json"

    def cached() -> dict[str, Any]:
        data = json.loads(result.read_text(encoding="utf-8"))
        regions = data.get("regions", [])
        return {"status": data["status"], "regions": len(regions),
                "style_regions": sum(1 for rg in regions if rg.get("class") == "style"), "cached": True}

    if result.exists():
        return cached()
    with _lock:
        lk = _key_locks.setdefault(str(out_dir), threading.Lock())
    with lk:
        if result.exists():
            return cached()
        summary = _call(engine.compute_sheet_pair, task)
        summary["cached"] = False
        return summary


def _run_job(object_id: str, run_id: str, cancel: threading.Event) -> None:
    def start(run):
        run["state"] = "running"
        run["owner"] = BOOT_ID
        run.setdefault("started_at", _now())
        return run
    run = _update(object_id, run_id, start)
    pending = [it for it in run["items"] if it["kind"] == "compare" and it["status"] not in ("done",)]
    cache: dict = {}
    window = settings.workers()
    futures: dict[Future, dict] = {}
    pool = ThreadPoolExecutor(max_workers=window, thread_name_prefix=f"vc-{run_id[-6:]}")
    try:
        queue = list(pending)
        while queue or futures:
            while queue and len(futures) < window and not cancel.is_set():
                item = queue.pop(0)
                try:
                    task, out_dir = _task_for(object_id, run, item, cache)
                except Exception as exc:
                    _finish_item(object_id, run_id, item["spid"], status="failed", error=f"подготовка: {exc}")
                    continue
                _finish_item(object_id, run_id, item["spid"], status="running", result_dir=str(out_dir))
                futures[pool.submit(_compute_cached, task, out_dir)] = item
            if cancel.is_set():
                queue.clear()
            if not futures:
                break
            done, _ = wait(list(futures), return_when=FIRST_COMPLETED)
            for fut in done:
                item = futures.pop(fut)
                try:
                    summary = fut.result()
                    _finish_item(object_id, run_id, item["spid"], status="done", result_status=summary["status"],
                                 regions=summary.get("regions", 0), style_regions=summary.get("style_regions", 0),
                                 cached=summary.get("cached", False))
                except Exception as exc:
                    _finish_item(object_id, run_id, item["spid"], status="failed", result_status="needs_review",
                                 error=f"{type(exc).__name__}: {exc}", trace=traceback.format_exc()[-2000:])
    finally:
        pool.shutdown(wait=True)

    def finish(run):
        if cancel.is_set():
            for it in run["items"]:
                if it["status"] in ("queued", "running"):
                    it["status"] = "cancelled"
            run["state"] = "cancelled"
        else:
            run["state"] = "done"
        run["finished_at"] = _now()
        return run
    _update(object_id, run_id, finish)
    with _lock:
        _cancel_events.pop(run_id, None)


def get_run(object_id: str, run_id: str) -> dict[str, Any]:
    path = _run_path(object_id, run_id)
    if not path.exists():
        raise store.CatalogError("прогон не найден")
    run = json.loads(path.read_text(encoding="utf-8"))
    with _lock:
        alive = run_id in _cancel_events
    if run.get("state") in ("queued", "running", "cancelling") and (run.get("owner") != BOOT_ID or not alive):
        def mark(r):
            if r.get("state") in ("queued", "running", "cancelling"):
                r["state"] = "interrupted"
                for it in r["items"]:
                    if it["status"] in ("queued", "running"):
                        it["status"] = "queued"
            return r
        run = _update(object_id, run_id, mark)
    return run


def cancel_run(object_id: str, run_id: str) -> dict[str, Any]:
    with _lock:
        ev = _cancel_events.get(run_id)
    if ev:
        ev.set()
        _update(object_id, run_id, lambda r: r | {"state": "cancelling"})
    return get_run(object_id, run_id)


def resume_run(object_id: str, run_id: str) -> dict[str, Any]:
    run = get_run(object_id, run_id)
    if run["state"] not in ("interrupted", "cancelled"):
        raise store.CatalogError("досчитать можно только прерванный или отменённый прогон")

    def reset(r):
        for it in r["items"]:
            if it["status"] in ("cancelled", "queued", "running"):
                it["status"] = "queued"
        r["state"] = "queued"
        r["owner"] = BOOT_ID
        return r
    _update(object_id, run_id, reset)
    _start(object_id, run_id)
    return get_run(object_id, run_id)


def list_runs(object_id: str, project_id: str | None = None) -> list[dict[str, Any]]:
    root = paths.runs_dir(object_id)
    out = []
    if not root.exists():
        return out
    for d in sorted(root.iterdir(), reverse=True):
        f = d / "run.json"
        if not f.exists():
            continue
        try:
            run = get_run(object_id, d.name)
        except Exception:
            continue
        if project_id and run.get("project_id") != project_id:
            continue
        counts: dict[str, int] = {}
        for it in run["items"]:
            key = it.get("result_status") or it["status"]
            counts[key] = counts.get(key, 0) + 1
        out.append({k: run.get(k) for k in ("run_id", "project_id", "a", "b", "scope", "state", "created_at",
                                             "finished_at", "progress")} | {"counts": counts})
    return out


def sheet_result(object_id: str, run_id: str, spid: str) -> tuple[dict[str, Any], Path | None]:
    run = get_run(object_id, run_id)
    item = next((it for it in run["items"] if it["spid"] == spid), None)
    if item is None:
        raise store.CatalogError("лист прогона не найден")
    if item["kind"] != "compare" or not item.get("result_dir"):
        return {"item": item, "result": None, "status_label": STATUS_LABELS.get(item.get("result_status", ""), "")}, None
    rdir = Path(item["result_dir"])
    result_file = rdir / "result.json"
    if not result_file.exists():
        return {"item": item, "result": None}, None
    result = json.loads(result_file.read_text(encoding="utf-8"))
    # Результат кэшируется по содержимому — подписи версий берём из прогона.
    result["a"].update(version_id=run["a"]["version_id"], label=run["a"]["label"], page=item["a_page"])
    result["b"].update(version_id=run["b"]["version_id"], label=run["b"]["label"], page=item["b_page"])
    result["sheet_pair_id"] = spid
    result["status_label"] = STATUS_LABELS.get(result["status"], result["status"])
    return {"item": item, "result": result}, rdir
