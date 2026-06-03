"""Job-оркестрация для live Large Sheet Enrichment (tile→Qwen).

Live tile→Qwen потенциально долгий (десятки tiles × секунды), поэтому direct
endpoint его не выполняет — только job в фоне (asyncio.create_task), с
per-tile progress и cancel. Конвенции зеркалят
``jobs.py`` / ``md_enrichment_jobs.py``:

  * job персистится в ``comparison/sessions/<sid>/jobs/<job_id>.json``;
  * без ``confirm=True`` создаётся job со ``status='rejected_no_confirm'`` и в
    фон НЕ уходит;
  * background-таск трекается в ``_active_tasks`` (cancel/stale-detection);
  * каждый item = (pair_id, side, page); внутри item — per-tile progress.

Live Qwen вызывается ТОЛЬКО здесь и ТОЛЬКО через ``_build_describe_fn`` —
инъекцию провайдера. Тесты подменяют ``_build_describe_fn`` фейком, поэтому
сеть в тестах не дёргается.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional

from . import paths as paths_mod
from . import large_sheet_enrichment as ls
from . import store as store_mod

logger = logging.getLogger(__name__)

_JOB_PREFIX = "lsj_"
_active_tasks: dict[str, dict[str, "asyncio.Task[Any]"]] = {}


# ─── helpers ────────────────────────────────────────────────────────────────

def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_job_id() -> str:
    return f"{_JOB_PREFIX}{uuid.uuid4().hex[:16]}"


def _read_job(session_id: str, job_id: str) -> Optional[dict]:
    try:
        p = paths_mod.job_json_path(session_id, job_id)
    except ValueError:
        return None
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _write_job(session_id: str, job: dict) -> None:
    p = paths_mod.job_json_path(session_id, job["id"])
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)


def _is_task_alive(session_id: str, job_id: str) -> bool:
    bucket = _active_tasks.get(session_id) or {}
    task = bucket.get(job_id)
    return bool(task and not task.done())


def _maybe_mark_interrupted(job: dict) -> dict:
    """Если job на диске running, но живой таски нет (рестарт uvicorn) —
    помечаем interrupted, а не вечный running."""
    if not isinstance(job, dict) or job.get("status") != "running":
        return job
    sid, jid = job.get("session_id"), job.get("id")
    if not sid or not jid or _is_task_alive(sid, jid):
        return job
    job["status"] = "interrupted"
    job["updated_at"] = _utc_now()
    for it in job.get("items") or []:
        if it.get("status") in ("queued", "running"):
            it["status"] = "interrupted"
    try:
        _write_job(sid, job)
    except OSError:
        pass
    return job


def _normalize_items(scope: str, items: Optional[list[dict]], pair_id: Optional[str],
                     side: Optional[str], page: Optional[int]) -> list[dict]:
    out: list[dict] = []
    if scope == "page":
        if not (pair_id and side and page):
            raise ValueError("scope=page requires pair_id, side, page")
        out.append({"pair_id": pair_id, "side": side, "page": int(page)})
    elif scope == "selected":
        for it in items or []:
            try:
                out.append({
                    "pair_id": str(it["pair_id"]),
                    "side": str(it["side"]),
                    "page": int(it["page"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
        if not out:
            raise ValueError("scope=selected requires non-empty items[]")
    else:
        raise ValueError(f"unknown scope: {scope}")
    # валидация значений
    for it in out:
        if it["side"] not in ("left", "right"):
            raise ValueError("side must be 'left' or 'right'")
        if it["page"] < 1:
            raise ValueError("page must be >= 1")
    return out


# ─── create / get / cancel ──────────────────────────────────────────────────

def create_job(
    session_id: str, *, scope: str = "page",
    items: Optional[list[dict]] = None,
    pair_id: Optional[str] = None, side: Optional[str] = None,
    page: Optional[int] = None,
    force: bool = False, confirm: bool = False,
) -> dict:
    """Создать job. Без ``confirm=True`` → ``status='rejected_no_confirm'``
    (в фон не уходит)."""
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")

    norm_items = _normalize_items(scope, items, pair_id, side, page)

    job_id = _new_job_id()
    job = {
        "id": job_id, "session_id": session_id, "kind": "large_sheet_enrichment",
        "scope": scope, "force": bool(force),
        "created_at": _utc_now(), "updated_at": _utc_now(),
        "status": "queued",
        "items": [
            {**it, "status": "queued", "tiles_total": 0, "tiles_done": 0,
             "tiles_failed": 0, "tiles_cache": 0, "error": None}
            for it in norm_items
        ],
        "progress": {
            "total": len(norm_items), "done": 0, "failed": 0, "skipped": 0,
            "current": None,
        },
    }
    if not confirm:
        job["status"] = "rejected_no_confirm"
        for it in job["items"]:
            it["status"] = "rejected_no_confirm"
    _write_job(session_id, job)
    return job


def get_job(session_id: str, job_id: str) -> Optional[dict]:
    job = _read_job(session_id, job_id)
    if job is None:
        return None
    return _maybe_mark_interrupted(job)


def cancel_job(session_id: str, job_id: str) -> Optional[dict]:
    job = _read_job(session_id, job_id)
    if job is None:
        return None
    if job.get("status") in ("done", "failed", "cancelled", "rejected_no_confirm"):
        return job
    job["status"] = "cancelled"
    job["updated_at"] = _utc_now()
    for it in job.get("items") or []:
        if it.get("status") in ("queued", "running"):
            it["status"] = "cancelled"
    _write_job(session_id, job)
    # отменить живую таску, если есть
    bucket = _active_tasks.get(session_id) or {}
    task = bucket.get(job_id)
    if task and not task.done():
        task.cancel()
    return job


def list_jobs(session_id: str) -> list[dict]:
    out: list[dict] = []
    root = paths_mod.jobs_root(session_id)
    for p in sorted(root.glob(f"{_JOB_PREFIX}*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        out.append(_maybe_mark_interrupted(data))
    return out


# ─── provider injection (тесты подменяют это) ───────────────────────────────

def _build_describe_fn(cfg, model: str) -> Callable[..., Awaitable[Any]]:
    """Вернуть async describe_fn(image_path, prompt, model=...) поверх реального
    локального Qwen-провайдера. Это ЕДИНСТВЕННАЯ точка, где живой Qwen может
    быть вызван; тесты её monkeypatch'ат фейком."""
    from . import graphic_llm_local as g

    async def _describe(image_path, prompt, *, model: Optional[str] = None):
        return await g.describe_image_local(image_path, prompt, model=model or "", cfg=cfg)

    return _describe


# ─── run ────────────────────────────────────────────────────────────────────

async def run_job(session_id: str, job_id: str) -> dict:
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") in ("done", "failed", "cancelled", "rejected_no_confirm"):
        return job
    if job.get("status") == "running" and _is_task_alive(session_id, job_id):
        return job

    # provider + model
    from . import graphic_llm_local as g
    cfg = g.load_local_graphic_llm_config()
    model = getattr(cfg, "model", "") or ""
    describe_fn = _build_describe_fn(cfg, model)

    job["status"] = "running"
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    force = bool(job.get("force"))

    def _is_cancelled() -> bool:
        latest = _read_job(session_id, job_id)
        return bool(latest and latest.get("status") == "cancelled")

    try:
        for idx, item in enumerate(job["items"]):
            if _is_cancelled():
                break
            item["status"] = "running"
            job["progress"]["current"] = {
                "pair_id": item["pair_id"], "side": item["side"],
                "page": item["page"], "tile_id": None,
            }
            job["updated_at"] = _utc_now()
            _write_job(session_id, job)

            def _on_tile(ev, _item=item):
                _item["tiles_total"] = ev.get("total", _item.get("tiles_total", 0))
                if ev.get("status") in ("done", "partial"):
                    _item["tiles_done"] = _item.get("tiles_done", 0) + 1
                elif ev.get("status") == "cache":
                    _item["tiles_done"] = _item.get("tiles_done", 0) + 1
                    _item["tiles_cache"] = _item.get("tiles_cache", 0) + 1
                elif ev.get("status") == "error":
                    _item["tiles_failed"] = _item.get("tiles_failed", 0) + 1
                cur = job["progress"].get("current") or {}
                cur["tile_id"] = ev.get("tile_id")
                job["progress"]["current"] = cur
                job["updated_at"] = _utc_now()
                _write_job(session_id, job)

            try:
                result = await ls.run_large_sheet_enrichment_live(
                    session_id, item["pair_id"], item["side"], item["page"],
                    describe_fn=describe_fn, model=model, force=force,
                    cache_enabled=True, on_tile_progress=_on_tile,
                    is_cancelled=_is_cancelled,
                )
                item["status"] = "done"
                item["page_enriched_json_path"] = result.get("page_enriched_json_path")
                item["page_enriched_md_path"] = result.get("page_enriched_md_path")
                item["diagnostics_path"] = result.get("diagnostics_path")
                item["circuits_detected"] = result.get("circuits_detected")
                item["tiles_total"] = result.get("tiles_total", item.get("tiles_total", 0))
                item["tiles_failed"] = result.get("tiles_failed", item.get("tiles_failed", 0))
                job["progress"]["done"] += 1
                if result.get("tiles_failed"):
                    job["progress"]["failed"] += 0  # page всё равно done (fail-soft по tile)
            except asyncio.CancelledError:
                item["status"] = "cancelled"
                raise
            except Exception as exc:  # noqa: BLE001 — одна страница не валит job
                item["status"] = "error"
                item["error"] = f"{type(exc).__name__}:{exc}"
                job["progress"]["failed"] += 1
                logger.exception("large-sheet job item failed")
            finally:
                job["progress"]["current"] = None
                job["updated_at"] = _utc_now()
                _write_job(session_id, job)

        # финальный статус
        latest = _read_job(session_id, job_id)
        if latest and latest.get("status") == "cancelled":
            job["status"] = "cancelled"
        else:
            any_error = any(it.get("status") == "error" for it in job["items"])
            job["status"] = "failed" if (any_error and job["progress"]["done"] == 0) else "done"
    except asyncio.CancelledError:
        job["status"] = "cancelled"
        for it in job["items"]:
            if it.get("status") in ("queued", "running"):
                it["status"] = "cancelled"
    except Exception as exc:  # noqa: BLE001
        job["status"] = "failed"
        job["error"] = f"{type(exc).__name__}:{exc}"
        logger.exception("large-sheet job failed")
    finally:
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)
    return job


def start_job_in_background(session_id: str, job_id: str) -> str:
    loop = asyncio.get_event_loop()
    task = loop.create_task(run_job(session_id, job_id))
    bucket = _active_tasks.setdefault(session_id, {})
    bucket[job_id] = task

    def _cleanup(_t):
        try:
            bucket.pop(job_id, None)
        except KeyError:
            pass

    task.add_done_callback(_cleanup)
    return job_id


__all__ = [
    "create_job", "get_job", "cancel_job", "list_jobs",
    "run_job", "start_job_in_background",
]
