"""Пакетные jobs для семантического LLM-анализа текстовых расхождений.

Похоже на jobs.py (graphic_llm_batch), но проще:
  • один тип job: "text_llm_batch";
  • scope: "pair" (один pair_id), "session" (все pairs сессии), "selected" (pair_ids[]);
  • запуск только с confirm=true;
  • каждый item — это pair_id; работа = run_text_comparison(session_id, pair_id);
  • после job вызываем findings_mod.rebuild_findings (опционально).

Хранение: comparison/sessions/<sid>/jobs/<job_id>.json (общий с графическими jobs).

Поскольку run_text_comparison делает blocking subprocess.run внутри ClaudeCode
provider, мы выносим его в asyncio.to_thread (или run_in_executor), чтобы не
блокировать event loop FastAPI.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import uuid
from datetime import datetime
from typing import Any, Optional

from . import paths as paths_mod
from . import store as store_mod
from . import findings as findings_mod
from . import text_llm as text_llm_mod

logger = logging.getLogger(__name__)

_lock = threading.RLock()

# In-process registry of active text-llm tasks per session
_active_tasks: dict[str, dict[str, "asyncio.Task[Any]"]] = {}


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_job_id() -> str:
    return f"txtjob_{uuid.uuid4().hex[:12]}"


def _read_job(session_id: str, job_id: str) -> dict | None:
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
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _collect_pair_ids(session: dict, *, scope: str, pair_id: str | None, pair_ids: list[str] | None) -> list[str]:
    all_pairs = [p for p in (session.get("pairs") or []) if p.get("status") != "disabled" and p.get("id")]
    valid_ids = {p["id"] for p in all_pairs}
    if scope == "pair":
        if not pair_id or pair_id not in valid_ids:
            return []
        return [pair_id]
    if scope == "selected":
        return [pid for pid in (pair_ids or []) if pid in valid_ids]
    if scope == "session":
        return [p["id"] for p in all_pairs]
    return []


# ─── Public API ──────────────────────────────────────────────────────────


def create_text_llm_job(
    session_id: str,
    *,
    scope: str,
    pair_id: str | None = None,
    pair_ids: list[str] | None = None,
    confirm: bool,
) -> dict:
    """Создать job по тексту. Без confirm=true сразу rejected."""
    with _lock:
        session = store_mod.get_session(session_id)
        if session is None:
            raise KeyError("session_not_found")
        ids = _collect_pair_ids(session, scope=scope, pair_id=pair_id, pair_ids=pair_ids)
        job_id = _new_job_id()
        now = _utc_now()
        items = [{"pair_id": pid, "status": "queued", "error": None} for pid in ids]
        job = {
            "id": job_id,
            "session_id": session_id,
            "type": "text_llm_batch",
            "scope": scope,
            "pair_id": pair_id,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "items": items,
            "progress": {
                "total": len(items),
                "done": 0,
                "failed": 0,
                "skipped": 0,
            },
            "confirm": bool(confirm),
        }
        if not confirm:
            job["status"] = "rejected_no_confirm"
            job["updated_at"] = _utc_now()
        _write_job(session_id, job)
        return job


def get_job(session_id: str, job_id: str) -> dict | None:
    return _read_job(session_id, job_id)


def cancel_job(session_id: str, job_id: str) -> dict | None:
    with _lock:
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
    return job


def list_text_llm_jobs(session_id: str) -> list[dict]:
    try:
        root = paths_mod.jobs_root(session_id)
    except (OSError, ValueError):
        return []
    out: list[dict] = []
    for p in sorted(root.glob("*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            if d.get("type") == "text_llm_batch":
                out.append(d)
        except (OSError, json.JSONDecodeError):
            continue
    out.sort(key=lambda j: j.get("created_at") or "", reverse=True)
    return out


# ─── Runner ──────────────────────────────────────────────────────────────


async def run_text_llm_job(session_id: str, job_id: str, *, auto_rebuild_findings: bool = True) -> dict:
    """Прогнать job: для каждого pair вызвать run_text_comparison.

    run_text_comparison делает блокирующий subprocess Claude — выносим в
    asyncio.to_thread.
    """
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") not in ("queued", "running"):
        return job
    job["status"] = "running"
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    items = list(job.get("items") or [])
    for idx, item in enumerate(items):
        # Re-read для проверки cancel
        latest = _read_job(session_id, job_id)
        if latest and latest.get("status") == "cancelled":
            return latest
        if item.get("status") != "queued":
            continue
        item["status"] = "running"
        item["updated_at"] = _utc_now()
        job["items"][idx] = item
        _write_job(session_id, job)
        pid = item.get("pair_id")
        try:
            result = await asyncio.to_thread(
                text_llm_mod.run_text_comparison, session_id, pid, force=True,
            )
            status = result.get("status")
            if status == "done":
                item["status"] = "done"
                job["progress"]["done"] += 1
            elif status in ("missing_md", "disabled", "provider_not_available", "too_large"):
                item["status"] = "skipped"
                item["error"] = status
                job["progress"]["skipped"] += 1
            else:
                item["status"] = "failed"
                item["error"] = result.get("error") or status or "unknown"
                job["progress"]["failed"] += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("text_llm_job: item %s failed", pid)
            item["status"] = "failed"
            item["error"] = str(exc)[:300]
            job["progress"]["failed"] += 1
        job["items"][idx] = item
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)

    # Финальный статус
    latest = _read_job(session_id, job_id)
    if latest and latest.get("status") == "cancelled":
        return latest
    job["status"] = "done"
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    if auto_rebuild_findings:
        try:
            findings_mod.rebuild_findings(session_id)
        except Exception:
            logger.exception("text_llm_job: rebuild_findings failed for session=%s", session_id)

    return job


def start_job_in_background(session_id: str, job_id: str, *, auto_rebuild_findings: bool = True) -> str:
    """Запустить run_text_llm_job в фоне; вернуть job_id."""
    loop = asyncio.get_event_loop()
    task = loop.create_task(run_text_llm_job(session_id, job_id, auto_rebuild_findings=auto_rebuild_findings))
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
    "create_text_llm_job",
    "get_job",
    "cancel_job",
    "list_text_llm_jobs",
    "run_text_llm_job",
    "start_job_in_background",
]
