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

import numpy as np

from backend.app.services.common import cpu_pool
from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save

from . import engine, lineage, parts, paths, permit, profile, settings, spec, status as st, store, work_queue
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


_slot_sem: threading.BoundedSemaphore | None = None


def _slots() -> threading.BoundedSemaphore:
    global _slot_sem
    with _lock:
        if _slot_sem is None:
            _slot_sem = threading.BoundedSemaphore(settings.workers())
        return _slot_sem


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _executor():
    return cpu_pool.get_executor()


def _call(fn: Callable, *args, cancel_event=None, key="") -> Any:
    """Выполнить CPU-функцию в общем пуле (или в потоке, если пула нет)."""
    if work_queue.enabled():
        operations = {engine.compute_sheet_pair: "sheet", _pool_profile: "profile", spec.build: "spec"}
        operation = operations[fn]
        return work_queue.execute(operation, list(args), key=key, cancel_event=cancel_event)
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
    if work_queue.enabled():
        work_queue.retry_failed("profile", [str(paths.root()), object_id, sha])
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
                       "duplicates": res.get("duplicates", {}), "edits": edits, "built_at": data.get("built_at"),
                       "parts": _matrix_parts(object_id, versions)}
    key = (object_id, project_id)
    with _lock:
        st = _lineage_state.get(key)
        if st and st.get("signature") == sig and st["state"] == "failed":
            return base | {"state": "failed", "error": st["error"], "rows": []}
        if not st or st.get("signature") != sig:
            _lineage_state[key] = {"state": "building", "signature": sig}
            _threads.submit(_lineage_job, object_id, project_id, versions, edits, sig)
    return base | {"state": "building", "rows": []}


def _matrix_parts(object_id: str, vers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Связи «часть листа» для соседних версий (по штампам; дёшево — без рендера)."""
    out = []
    for va, vb in zip(vers, vers[1:]):
        try:
            pa = profile.load(object_id, va["sha256"])["pages"]
            pb = profile.load(object_id, vb["sha256"])["pages"]
        except Exception:
            continue
        for pr in parts.find(pa, pb):
            out.append(pr | {"a_version": va["version_id"], "b_version": vb["version_id"]})
    return out


def add_link_edit(object_id: str, project_id: str, edit: dict[str, Any]) -> dict[str, Any]:
    return add_link_edits(object_id, project_id, [edit])


def add_link_edits(object_id: str, project_id: str, edits: list[dict[str, Any]]) -> dict[str, Any]:
    """Сохранить все ручные правки атомарно, затем сопоставить листы один раз."""
    store.project(store.load_catalog(object_id), project_id)
    prepared = []
    for edit in edits:
        op = edit.get("op")
        if op not in ("move", "detach", "absent"):
            raise store.CatalogError("неизвестная правка связи")
        required = {"move": ("page", "to_row_anchor"), "detach": ("page",),
                    "absent": ("row_anchor",)}[op]
        for key in required:
            value = edit.get(key)
            if (not isinstance(value, list) or len(value) != 2 or not isinstance(value[0], str)
                    or not value[0] or type(value[1]) is not int or value[1] < 1):
                raise store.CatalogError(f"некорректная страница: {key}")
        if op == "absent" and not edit.get("version"):
            raise store.CatalogError("не указана версия")
        prepared.append({k: edit[k] for k in ("op", "page", "to_row_anchor", "row_anchor", "version")
                         if k in edit} | {"at": _now()})

    def add(data):
        data.setdefault("edits", []).extend(prepared)
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


def link_fingerprint(sha_a: str, a_page: int, sha_b: str, b_page: int) -> str:
    """Отпечаток пары листов без её состояния: подтверждение той же пары вручную его не меняет."""
    return hashlib.sha1(f"{sha_a}:{int(a_page)}|{sha_b}:{int(b_page)}".encode()).hexdigest()[:16]


def current_pairs(object_id: str, project_id: str, a_vid: str, b_vid: str) -> dict[int, int | None] | None:
    """Текущая связь страниц A → B по сохранённому сопоставлению (для любой пары версий, не только
    соседних): {страница A: страница B или None}. None — сопоставление ещё не готово/перестраивается."""
    try:
        catalog = store.load_catalog(object_id)
        versions = store.project_versions(catalog, project_id)
    except Exception:  # noqa: BLE001
        return None
    data = _load_lineage(object_id, project_id)
    if data.get("signature") != _signature(versions, data.get("edits", [])) or not data.get("result"):
        return None
    out: dict[int, int | None] = {}
    for row in data["result"]["rows"]:
        ca, cb = row["cells"].get(a_vid) or {}, row["cells"].get(b_vid) or {}
        if ca.get("state") == "matched":
            out[ca["page"]] = cb.get("page") if cb.get("state") == "matched" else None
    return out


def mark_stale_items(object_id: str, run: dict[str, Any]) -> dict[str, Any]:
    """Пометить элементы прогона, чья пара листов больше не совпадает с текущим сопоставлением
    (settings «stale»). Неизменные пары не трогаются; результаты не удаляются."""
    if "stale" not in settings.match_rules() or run.get("scope") != "all":
        return run
    cur = current_pairs(object_id, run["project_id"], run["a"]["version_id"], run["b"]["version_id"])
    if cur is None:
        return run
    for it in run["items"]:
        if it.get("kind") != "compare":
            continue
        now = cur.get(it["a_page"], "нет")
        if now != it["b_page"]:
            it["pair_stale"] = (f"связь листов изменилась после расчёта: сейчас стр. {it['a_page']} A → "
                                + (f"стр. {now} B" if isinstance(now, int) else "без пары"))
    return run


def direct_links(object_id: str, a_vid: str, sha_a: str, b_vid: str, sha_b: str) -> dict[int, tuple[int, bool]]:
    """Прямое сопоставление страниц двух любых версий (без промежуточных): {страница A: (страница B, решающее
    доказательство)}. Те же правила, что у соседних версий; цепочка через промежуточные версии сюда не входит.
    Совпадение с цепочкой — два независимых подтверждения; пара без цепочки принимается только при решающем
    доказательстве с обеих сторон (lineage.direct_pairs)."""
    rules = settings.match_rules()
    feats = lineage.load_features(object_id, [{"version_id": a_vid, "sha256": sha_a},
                                              {"version_id": b_vid, "sha256": sha_b}], rules)
    return lineage.direct_pairs(feats[a_vid], feats[b_vid], rules)


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
    rules = settings.match_rules()
    adjacent = abs(order.index(a_vid) - order.index(b_vid)) == 1
    # Несоседняя пара: страницы A и B сверяются напрямую; цепочка через промежуточные версии — только подсказка.
    direct = (direct_links(object_id, a_vid, va["sha256"], b_vid, vb["sha256"])
              if scope == "all" and not adjacent and "direct" in rules else None)
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
                if direct is not None and (direct.get(ca["page"]) or (None,))[0] == cb["page"]:
                    conf = "direct"     # пара по цепочке подтверждена прямой сверкой A ↔ B
                elif "chain" in rules and not adjacent and conf != "manual":
                    # Пара через версию собрана только по цепочке соседних связей (без транзитивности) и прямой
                    # сверкой не подтверждена: не сравнивается, пока инженер не подтвердит её сам. Строка, где
                    # инженер уже связал лист вручную, сравнивается: ручное решение главнее.
                    items.append(base | {"spid": row["row_id"], "kind": "chain", "a_page": ca["page"], "b_page": cb["page"],
                                         "confidence": conf, "reasons": ["CHAIN_ONLY"], "status": "done",
                                         "result_status": "unmatched",
                                         "direct_b_page": ((direct or {}).get(ca["page"]) or (None,))[0]})
                    continue
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
    if direct:
        items = _with_direct(items, direct)
    if scope == "all":
        b_rows = {row["cells"][b_vid]["page"]: row["row_id"] for row in mx["rows"]
                  if (row["cells"].get(b_vid) or {}).get("state") == "matched"}
        items = _with_parts(object_id, va["sha256"], vb["sha256"], items, b_rows)
    if "stale" in rules:
        for it in items:
            if it["kind"] == "compare":
                it["link_fingerprint"] = link_fingerprint(va["sha256"], it["a_page"], vb["sha256"], it["b_page"])
    run_id = datetime.now().strftime("run%Y%m%d%H%M%S") + secrets.token_hex(2)
    total = sum(1 for it in items if it["kind"] == "compare")
    run = {
        "schema": "version_comparison.run/v1", "run_id": run_id, "object_id": object_id, "project_id": project_id,
        "a": {"version_id": a_vid, "label": va.get("user_label"), "file_name": va.get("file_name"), "sha256": va["sha256"]},
        "b": {"version_id": b_vid, "label": vb.get("user_label"), "file_name": vb.get("file_name"), "sha256": vb["sha256"]},
        "scope": scope, "params": {"dpi": settings.dpi(), "tol_pt": settings.tolerance_pt(), "algo": ALGO_VERSION},
        "state": "queued", "owner": BOOT_ID, "owner_process": _identity(), "created_at": _now(), "items": items,
        "progress": {"total": total, "done": 0, "failed": 0},
    }
    if work_queue.enabled():
        run.update(auto_resume=True, execution_generation=0)
    if rules:
        run.update(match_rules=settings.match_rules_tag(rules),
                   matching_signature=_load_lineage(object_id, project_id).get("signature"))
    atomic_write_json(_run_path(object_id, run_id), run)
    _start(object_id, run_id)
    return run


def _with_direct(items: list[dict[str, Any]], direct: dict[int, int]) -> list[dict[str, Any]]:
    """Пара, найденная прямой сверкой несоседних версий там, где цепочки нет (страница A и страница B
    в матрице без пары): сравнивается автоматически вместо «только в A» / «только в B»."""
    only_a = {it["a_page"]: it for it in items if it["kind"] == "only_a"}
    only_b = {it["b_page"]: it for it in items if it["kind"] == "only_b"}
    drop: set[int] = set()
    for a_page, (b_page, strong) in direct.items():
        ia, ib = only_a.get(a_page), only_b.get(b_page)
        if ia is None or ib is None or not strong:   # без цепочки — только с решающим доказательством
            continue
        ia.update(kind="compare", b_page=b_page, confidence="direct", reasons=[], status="queued")
        for k in ("result_status", "candidates"):
            ia.pop(k, None)
        drop.add(id(ib))
    return [it for it in items if id(it) not in drop]


def _with_parts(object_id: str, sha_a: str, sha_b: str, items: list[dict[str, Any]],
                b_rows: dict[int, str] | None = None) -> list[dict[str, Any]]:
    """Разделение листа 1 → N: пары «часть листа» вместо «удалён / добавлен» (план §5 шаг 2)."""
    found = parts.find(profile.load(object_id, sha_a)["pages"], profile.load(object_id, sha_b)["pages"])
    if not found:
        return items
    a_parts = {pr["a_page"] for pr in found}
    b_parts = {pr["b_page"] for pr in found}
    by_pair = {(it["a_page"], it["b_page"]): it for it in items if it["kind"] == "compare"}
    out = []
    for it in items:
        if it["kind"] == "only_a" and it["a_page"] in a_parts:
            continue
        if it["kind"] == "only_b" and it["b_page"] in b_parts:
            continue
        if it["kind"] == "compare" and it["b_page"] in b_parts and (it["a_page"], it["b_page"]) not in \
                {(pr["a_page"], pr["b_page"]) for pr in found}:
            continue    # страница B — часть другого листа A, а не продолжение этой строки
        out.append(it)
    for pr in found:
        key = (pr["a_page"], pr["b_page"])
        label = f"{pr['b_title']} — {'с типового листа' if pr['kind'] == 'floors' else 'часть листа'} «{pr['a_title']}»"
        if key in by_pair:
            by_pair[key].update(part=pr["kind"], confidence="part", title=label)
        else:
            # Строка матрицы — строка страницы B: в ней и показывается результат части.
            out.append({"spid": f"p{pr['a_page']}-{pr['b_page']}", "kind": "compare",
                        "row_id": (b_rows or {}).get(pr["b_page"]), "title": label,
                        "a_page": pr["a_page"], "b_page": pr["b_page"], "confidence": "part", "part": pr["kind"],
                        "reasons": [], "status": "queued"})
    return out


def _start(object_id: str, run_id: str) -> None:
    ev = threading.Event()
    with _lock:
        if run_id in _cancel_events:
            return
        _cancel_events[run_id] = ev
    _run_threads.submit(_guarded_run_job, object_id, run_id, ev)


def _guarded_run_job(object_id: str, run_id: str, ev: threading.Event) -> None:
    try:
        _run_job(object_id, run_id, ev)
    except Exception as exc:
        _update(object_id, run_id, lambda r: r | {"state": "failed", "error": str(exc), "finished_at": _now()})
    finally:
        with _lock:
            _cancel_events.pop(run_id, None)


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
    key = _cache_key(rec_a, rec_b, run["params"] | ({"part": item["part"]} if item.get("part") else {}),
                     item.get("reasons", []))
    out_dir = (paths.pair_cache_dir(object_id) / key) if key else paths.sheet_dir(object_id, run["run_id"], item["spid"])
    task = {
        "sheet_pair_id": item["spid"], "row_id": item.get("row_id"), "out_dir": str(out_dir),
        "params": {"dpi": run["params"]["dpi"], "tol_pt": run["params"]["tol_pt"]},
        "reasons": item.get("reasons", []), "match": {"confidence": item.get("confidence")},
        "part": item.get("part"),
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


def _compute_cached(task: dict, out_dir: Path, cancel_event=None, execution_key="") -> dict[str, Any]:
    result = out_dir / "result.json"

    def cached() -> dict[str, Any]:
        data = json.loads(result.read_text(encoding="utf-8"))
        regions = data.get("regions", [])
        shifts = sum(1 for rg in regions if rg.get("class") in engine.SHIFT_CLASSES)
        return {"status": data["status"], "regions": len(regions) - shifts,
                "style_regions": sum(1 for rg in regions if rg.get("class") == "style"), "shift_regions": shifts,
                "cached": True}

    if result.exists():
        try:
            return cached()
        except (ValueError, KeyError):
            if not work_queue.enabled():
                raise
    with _lock:
        lk = _key_locks.setdefault(str(out_dir), threading.Lock())
    with lk:
        if result.exists():
            try:
                return cached()
            except (ValueError, KeyError):
                if not work_queue.enabled():
                    raise
        # Общий предел одновременных расчётов на процесс: несколько прогонов
        # (или серия рядом с ручным прогоном) не умножают нагрузку на ядра.
        with _slots():
            if cancel_event is not None and cancel_event.is_set():
                raise work_queue.WorkCancelled("Расчёт отменён")
            summary = _call(engine.compute_sheet_pair, task, cancel_event=cancel_event, key=execution_key)
        summary["cached"] = False
        return summary


def _run_job(object_id: str, run_id: str, cancel: threading.Event) -> None:
    def start(run):
        if run.get("state") == "cancelling":
            cancel.set()
        else:
            run["state"] = "running"
        run["owner"] = BOOT_ID
        run["owner_process"] = _identity()
        run.setdefault("started_at", _now())
        return run
    run = _update(object_id, run_id, start)
    pending = [it for it in run["items"] if it["kind"] == "compare" and it["status"] not in ("done",)]
    cache: dict = {}
    window = settings.workers()
    futures: dict[Future, dict] = {}
    pool = ThreadPoolExecutor(max_workers=window, thread_name_prefix=f"vc-{run_id[-6:]}")
    unavailable = False
    try:
        queue = list(pending)
        while queue or futures:
            if json.loads(_run_path(object_id, run_id).read_text()).get("state") == "cancelling":
                cancel.set()
            while queue and len(futures) < window and not cancel.is_set():
                item = queue.pop(0)
                try:
                    task, out_dir = _task_for(object_id, run, item, cache)
                except Exception as exc:
                    _finish_item(object_id, run_id, item["spid"], status="failed", error=f"подготовка: {exc}")
                    continue
                _finish_item(object_id, run_id, item["spid"], status="running", result_dir=str(out_dir))
                execution_key = f"{run_id}:{item['spid']}:{run.get('execution_generation', 0)}"
                futures[pool.submit(_compute_cached, task, out_dir, cancel, execution_key)] = item
            if cancel.is_set():
                queue.clear()
            if not futures:
                break
            done, _ = wait(list(futures), timeout=0.5, return_when=FIRST_COMPLETED)
            for fut in done:
                item = futures.pop(fut)
                try:
                    summary = fut.result()
                    _finish_item(object_id, run_id, item["spid"], status="done", result_status=summary["status"],
                                 regions=summary.get("regions", 0), style_regions=summary.get("style_regions", 0),
                                 shift_regions=summary.get("shift_regions", 0), cached=summary.get("cached", False))
                except work_queue.WorkCancelled:
                    _finish_item(object_id, run_id, item["spid"], status="cancelled")
                except work_queue.WorkUnavailable as exc:
                    unavailable = True
                    queue.clear()
                    _finish_item(object_id, run_id, item["spid"], status="queued", error=str(exc))
                except Exception as exc:
                    _finish_item(object_id, run_id, item["spid"], status="failed", result_status="needs_review",
                                 error=f"{type(exc).__name__}: {exc}", trace=traceback.format_exc()[-2000:])
    finally:
        pool.shutdown(wait=True)

    if not cancel.is_set() and not unavailable:
        try:
            _spec_phase(object_id, run_id)
        except work_queue.WorkUnavailable:
            unavailable = True
        except work_queue.WorkCancelled:
            cancel.set()
        except Exception as exc:  # pragma: no cover - защитный путь: СО не мешает остальному результату
            _update(object_id, run_id, lambda r: r | {"spec_error": f"{type(exc).__name__}: {exc}",
                                                      "spec_trace": traceback.format_exc()[-2000:]})
        try:
            _permit_phase(object_id, run_id)
        except Exception as exc:  # pragma: no cover - защитный путь
            _update(object_id, run_id, lambda r: r | {"permit_error": f"{type(exc).__name__}: {exc}"})
        try:
            _parts_phase(object_id, run_id)
        except Exception as exc:  # pragma: no cover - защитный путь
            _update(object_id, run_id, lambda r: r | {"parts_error": f"{type(exc).__name__}: {exc}"})

    def finish(run):
        if unavailable and not cancel.is_set():
            run["state"] = "interrupted"
            run["error"] = "Служба сравнения недоступна; очередь сохранена"
        elif cancel.is_set():
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


# ------------------------------------------------- спецификации по позициям

def _spec_path(object_id: str, run_id: str) -> Path:
    return paths.run_dir(object_id, run_id) / "spec.json"


def _spec_of_version(object_id: str, sha: str, skip: list[int], *, cancel_event=None, key="") -> dict[str, Any] | None:
    prof = profile.load(object_id, sha)
    out = paths.profile_dir(object_id, sha)
    _call(spec.build, str(paths.source_pdf(object_id, sha)), prof["pages"], str(out), skip,
          cancel_event=cancel_event, key=key)
    return spec.load(str(out))


def _positions_status(result: dict[str, Any], n_changes: int, n_ambiguous: int) -> tuple[str, list[str], dict]:
    """Статус листа СО по позициям: наложение перепагинированных страниц — справочно."""
    channels = json.loads(json.dumps(result["channels"]))
    for name in ("geometry", "color", "text"):
        ch = channels.get(name) or {}
        channels[name] = st.checked(0, basis="СО сравнивается по позициям; наложение страниц — справочно",
                                    overlay_diffs=ch.get("diffs"))
    channels["spec_positions"] = (st.unchecked("граница позиции неоднозначна", ambiguous=n_ambiguous)
                                  if n_ambiguous else st.checked(n_changes, basis="позиции через страницы"))
    if n_changes:
        # Отличия позиций — основное содержание листа СО.
        channels["text"] = st.checked(n_changes, basis="изменения позиций СО")
    reasons = [r for r in result.get("reasons", []) if not r.startswith("UNCHECKED_CHANNEL")]
    status, reasons = st.decide(channels, reasons)
    return status, reasons, channels


def _spec_phase(object_id: str, run_id: str) -> None:
    run = get_run(object_id, run_id)
    if run.get("scope") == "manual":
        return
    try:
        dups = matrix(object_id, run["project_id"]).get("duplicates") or {}
    except Exception:
        dups = {}
    skip_a = sorted(int(p) for p in (dups.get(run["a"]["version_id"]) or {}))
    skip_b = sorted(int(p) for p in (dups.get(run["b"]["version_id"]) or {}))
    with _lock:
        cancel_event = _cancel_events.get(run_id)
    key = f"{run_id}:{run.get('execution_generation', 0)}"
    sa = _spec_of_version(object_id, run["a"]["sha256"], skip_a, cancel_event=cancel_event, key=key)
    sb = _spec_of_version(object_id, run["b"]["sha256"], skip_b, cancel_event=cancel_event, key=key)
    if not sa or not sb or not (sa["pages"] and sb["pages"]):
        return
    cmp_ = spec.compare(sa, sb)
    data = {"schema": "version_comparison.run_spec/v1", "spec_version": spec.SPEC_VERSION,
            "a_pages": sa["pages"], "b_pages": sb["pages"], "skip": {"a": skip_a, "b": skip_b}} | cmp_
    atomic_write_json(_spec_path(object_id, run_id), data)
    parsed_a = {p["page"]: p["parsed"] for p in sa["pages"]}
    parsed_b = {p["page"]: p["parsed"] for p in sb["pages"]}
    by_id = {c["id"]: c for c in cmp_["changes"]}

    def apply(r):
        for it in r["items"]:
            if it["kind"] != "compare" or it["status"] != "done" or not it.get("result_dir"):
                continue
            in_a, in_b = it["a_page"] in parsed_a, it["b_page"] in parsed_b
            if not (in_a or in_b):
                continue
            rfile = Path(it["result_dir"]) / "result.json"
            if not rfile.exists():
                continue
            result = json.loads(rfile.read_text(encoding="utf-8"))
            it.setdefault("overlay_status", it.get("result_status"))
            if parsed_a.get(it["a_page"]) and parsed_b.get(it["b_page"]):
                ids = sorted(set(spec.page_changes(cmp_, "a", it["a_page"])) | set(spec.page_changes(cmp_, "b", it["b_page"])))
                main = [i for i in ids if by_id[i]["kind"] in ("changed", "added", "removed")]
                amb = [i for i in ids if by_id[i]["kind"] == "ambiguous"]
                status, reasons, _ch = _positions_status(result, len(main), len(amb))
                it["spec"] = {"basis": "positions", "changes": len(main), "ambiguous": len(amb), "ids": ids}
            else:
                reasons = list(dict.fromkeys(result.get("reasons", []) + ["SPEC_NOT_PARSED"]))
                status = "needs_review"
                it["spec"] = {"basis": "overlay", "error": "таблица СО не разобрана — остаётся постраничное наложение"}
            it["result_status"] = status
            it["spec_reasons"] = reasons
        r["spec"] = {"available": True, "changes": sum(1 for c in cmp_["changes"] if c["kind"] in ("changed", "added", "removed")),
                     "moved_between_pages": cmp_["moved_between_pages"], "renumbered": sum(1 for c in cmp_["changes"] if c["kind"] == "renumbered")}
        return r
    _update(object_id, run_id, apply)


# ------------------------------------------ разделение листа: покрытие

def _parts_phase(object_id: str, run_id: str) -> None:
    """Покрытие листа A его частями в B (по маске 36 dpi без служебных зон)."""
    run = get_run(object_id, run_id)
    groups: dict[int, list[dict]] = {}
    for it in run["items"]:
        if it.get("part") and it["kind"] == "compare":
            groups.setdefault(it["a_page"], []).append(it)
    if not groups:
        return
    rec_pages = profile.load(object_id, run["a"]["sha256"])["pages"]
    summary = []
    for a_page, its in sorted(groups.items()):
        kind = its[0]["part"]
        entry = {"a_page": a_page, "kind": kind, "b_pages": sorted(it["b_page"] for it in its)}
        if kind == "split":
            mask = profile.page_mask36(object_id, run["a"]["sha256"], a_page)
            areas = []
            for it in its:
                rf = Path(it.get("result_dir") or "") / "result.json"
                if rf.exists():
                    res = json.loads(rf.read_text(encoding="utf-8"))
                    part = res.get("part") or {}
                    # Область части осмысленна только при надёжном совмещении с листом A.
                    if part.get("area_a_pt") and (res.get("transform") or {}).get("reliable"):
                        areas.append(part["area_a_pt"])
                    elif part.get("area_a_pt"):
                        entry["unreliable"] = entry.get("unreliable", 0) + 1
            if entry.get("unreliable"):
                entry["coverage"] = None
                entry["note"] = "совмещение части с листом A ненадёжно — покрытие не считается"
            elif mask is not None and areas:
                k = 36 / 72.0
                ink = mask > 0
                for z in rec_pages[a_page - 1].get("zones", []):
                    b = z["bbox"]
                    ink[max(0, int(b[1] * k)):int(b[3] * k) + 1, max(0, int(b[0] * k)):int(b[2] * k) + 1] = False
                cov = np.zeros_like(ink)
                for r in areas:
                    cov[max(0, int(r[1] * k)):int(r[3] * k) + 1, max(0, int(r[0] * k)):int(r[2] * k) + 1] = True
                total = int(ink.sum())
                entry["coverage"] = round(float((ink & cov).sum()) / total, 4) if total else None
        summary.append(entry)
    cov_by_a = {e["a_page"]: e.get("coverage") for e in summary}

    def apply(r):
        for it in r["items"]:
            if it.get("part") and it["a_page"] in cov_by_a:
                it["part_coverage"] = cov_by_a[it["a_page"]]
        r["parts"] = summary
        return r
    _update(object_id, run_id, apply)


# --------------------------------------- разрешение: заявлено ↔ найдено

def _permits_of_version(object_id: str, sha: str) -> list[dict[str, Any]]:
    path = paths.profile_dir(object_id, sha) / "permits_v2.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    prof = profile.load(object_id, sha)
    data = permit.read_version(pdfio_bytes(object_id, sha), prof["pages"])
    atomic_write_json(path, data)
    return data


def pdfio_bytes(object_id: str, sha: str) -> bytes:
    return paths.source_pdf(object_id, sha).read_bytes()


def _izm_of_version(object_id: str, sha: str) -> int:
    rows = [r["izm"] for p in _permits_of_version(object_id, sha) for r in p["rows"]]
    stamps = [int(p["stamp"].get("izm_max") or 0) for p in profile.load(object_id, sha)["pages"]
              if (p.get("stamp") or {}).get("izm_max")]
    return max(rows + stamps + [0])


def _permit_phase(object_id: str, run_id: str) -> None:
    run = get_run(object_id, run_id)
    if run.get("scope") == "manual":
        return
    permits_b = _permits_of_version(object_id, run["b"]["sha256"])
    if not permits_b:
        return
    izm_a = _izm_of_version(object_id, run["a"]["sha256"])
    pages_b = profile.load(object_id, run["b"]["sha256"])["pages"]
    found = [{"b_page": it.get("b_page"), "a_page": it.get("a_page"), "status": it.get("result_status"),
              "title": it.get("title")} for it in run["items"] if it.get("b_page")]
    rec = permit.reconcile(permits_b, izm_a, pages_b, found)
    data = {"schema": "version_comparison.run_permit/v1", "permits": permits_b} | rec
    atomic_write_json(paths.run_dir(object_id, run_id) / "permit.json", data)
    summary = {"available": True, "izm_new": rec["izm_new"], "declared_and_found": len(rec["declared_and_found"]),
               "declared_no_diff": len(rec["declared_no_diff"]), "diff_not_declared": len(rec["diff_not_declared"]),
               "needs_review": len(rec["needs_review"])}
    _update(object_id, run_id, lambda r: r | {"permit": summary})


def run_permit(object_id: str, run_id: str) -> dict[str, Any]:
    path = paths.run_dir(object_id, run_id) / "permit.json"
    if not path.exists():
        return {"available": False}
    return {"available": True} | json.loads(path.read_text(encoding="utf-8"))


def run_spec(object_id: str, run_id: str) -> dict[str, Any]:
    path = _spec_path(object_id, run_id)
    if not path.exists():
        return {"available": False}
    return {"available": True} | json.loads(path.read_text(encoding="utf-8"))


def get_run(object_id: str, run_id: str) -> dict[str, Any]:
    path = _run_path(object_id, run_id)
    if not path.exists():
        raise store.CatalogError("прогон не найден")
    run = json.loads(path.read_text(encoding="utf-8"))
    with _lock:
        alive = run_id in _cancel_events
    # Readers can use a different executor while sharing this data directory.
    # Process identity, not the reader's executor, determines ownership.
    if run.get("owner") != BOOT_ID:
        from .recovery import owner_alive
        if owner_alive(run.get("owner_process")):
            return run
    if run.get("state") in ("queued", "running", "cancelling") and (run.get("owner") != BOOT_ID or not alive):
        def mark(r):
            if r.get("state") in ("queued", "running", "cancelling"):
                cancelling = r["state"] == "cancelling"
                r["state"] = "cancelled" if cancelling else "interrupted"
                for it in r["items"]:
                    if it["status"] in ("queued", "running"):
                        it["status"] = "cancelled" if cancelling else "queued"
            return r
        run = _update(object_id, run_id, mark)
        if run["state"] == "cancelled" and work_queue.enabled():
            work_queue.cancel_run(run_id)
    if work_queue.enabled():
        info = work_queue.heartbeat()
        run["compute_worker"] = {k: info.get(k) for k in ("online", "active", "workers", "waiting_reason")}
    return mark_stale_items(object_id, run)


def cancel_run(object_id: str, run_id: str) -> dict[str, Any]:
    with _lock:
        ev = _cancel_events.get(run_id)
    if ev:
        ev.set()
    _update(object_id, run_id, lambda r: r | {"state": "cancelling"}
            if r.get("state") in ("queued", "running") else r)
    if work_queue.enabled():
        work_queue.cancel_run(run_id)
    return get_run(object_id, run_id)


def _identity() -> dict:
    """Процесс-владелец (pid, время старта, загрузка ОС) — всегда, не только в режиме исполнителя.

    По нему другой процесс с теми же данными (портал 8081 или сервер серий 8095) видит, что
    владелец жив, и не перехватывает прогон или серию (16.10.2026: портал продолжал серию
    без отметки, сервер серий счёл владельца мёртвым и повёл её параллельно — два прогона).
    """
    from .recovery import identity
    return identity()


def _busy_text(data: dict, what: str) -> str:
    if data.get("state") in ("queued", "running", "cancelling"):
        return f"{what} уже выполняется — продолжать не нужно"
    return f"досчитать можно только прерванный или отменённый {what}"


def resume_run(object_id: str, run_id: str) -> dict[str, Any]:
    run = get_run(object_id, run_id)
    if run["state"] not in ("interrupted", "cancelled"):
        raise store.CatalogError(_busy_text(run, "прогон"))
    claimed = []

    def reset(r):
        # Проверка и захват под одной файловой блокировкой: два процесса (портал и сервер
        # серий) не могут одновременно продолжить один прогон.
        if r.get("state") not in ("interrupted", "cancelled"):
            return r
        claimed.append(True)
        if r["state"] == "cancelled":
            r["execution_generation"] = r.get("execution_generation", 0) + 1
        for it in r["items"]:
            if it["status"] in ("cancelled", "queued", "running"):
                it["status"] = "queued"
        r["state"] = "queued"
        r["owner"] = BOOT_ID
        r["owner_process"] = _identity()
        if work_queue.enabled():
            r["auto_resume"] = True
        r.pop("finished_at", None)
        r.pop("error", None)
        return r
    current = _update(object_id, run_id, reset)
    if not claimed:
        raise store.CatalogError(_busy_text(current, "прогон"))
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
    if item.get("spec"):
        # Лист СО: статус по позициям прогона (наложение страниц — справочно).
        result["overlay_status"] = result["status"]
        sp = run_spec(object_id, run_id)
        by_id = {c["id"]: c for c in sp.get("changes", [])}
        result["spec_page"] = item["spec"] | {"items": [by_id[i] for i in item["spec"].get("ids", []) if i in by_id]}
        if item["spec"].get("basis") == "positions":
            status, reasons, channels = _positions_status(result, item["spec"]["changes"], item["spec"]["ambiguous"])
            result["status"], result["reasons"], result["channels"] = status, reasons, channels
        else:
            result["status"], result["reasons"] = "needs_review", item.get("spec_reasons") or result["reasons"]
    result["status_label"] = STATUS_LABELS.get(result["status"], result["status"])
    return {"item": item, "result": result}, rdir


# ------------------------------------------------------------------ отчёты

_report_state: dict[str, dict[str, Any]] = {}


def _reports_dir(object_id: str, run_id: str) -> Path:
    return paths.run_dir(object_id, run_id) / "reports"


def _report_job(object_id: str, run_id: str) -> None:
    from . import report
    try:
        run = get_run(object_id, run_id)
        data = report.collect(object_id, run)
        out = _reports_dir(object_id, run_id)
        out.mkdir(parents=True, exist_ok=True)
        (out / "report.xlsx").write_bytes(report.build_xlsx(data))
        (out / "report_html.zip").write_bytes(report.build_html_zip(object_id, data))
        state = {"state": "ready", "built_at": _now(), "files": ["report.xlsx", "report_html.zip"]}
    except Exception as exc:  # pragma: no cover - защитный путь
        state = {"state": "failed", "error": f"{type(exc).__name__}: {exc}", "trace": traceback.format_exc()[-2000:]}
    with _lock:
        _report_state[run_id] = state


def build_reports(object_id: str, run_id: str) -> dict[str, Any]:
    run = get_run(object_id, run_id)
    if run["state"] != "done":
        raise store.CatalogError("отчёт строится по завершённому прогону")
    with _lock:
        if (_report_state.get(run_id) or {}).get("state") == "building":
            return _report_state[run_id]
        _report_state[run_id] = {"state": "building"}
    _threads.submit(_report_job, object_id, run_id)
    return {"state": "building"}


def reports_status(object_id: str, run_id: str) -> dict[str, Any]:
    with _lock:
        st_ = _report_state.get(run_id)
    if st_:
        return st_
    out = _reports_dir(object_id, run_id)
    files = [f for f in ("report.xlsx", "report_html.zip") if (out / f).exists()]
    return {"state": "ready", "files": files} if len(files) == 2 else {"state": "none"}


def report_file(object_id: str, run_id: str, name: str) -> Path:
    if name not in ("report.xlsx", "report_html.zip"):
        raise store.CatalogError("неизвестный файл отчёта")
    path = _reports_dir(object_id, run_id) / name
    if not path.exists():
        raise store.CatalogError("отчёт ещё не построен")
    return path


def series_report(object_id: str, series_id: str) -> bytes:
    from . import report, series as series_mod
    ser = series_mod.get(object_id, series_id)
    runs = [get_run(object_id, p["run_id"]) for p in ser["pairs"] if p.get("run_id")]
    return report.build_series_xlsx(ser, runs)
