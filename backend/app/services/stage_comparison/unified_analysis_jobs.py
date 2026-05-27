"""Batch jobs для unified stage comparison analysis.

Один тип job: `"unified_stage_comparison"`. Делает то же, что `unified_analysis.run_pair`,
но по списку pair_ids в фоне.

scope:
    pair      — одна PDF-пара (pair_id обязателен)
    session   — все PDF-пары сессии
    selected  — pair_ids[]

Без confirm=true создаётся rejected_no_confirm job (для истории).
Хранение: общий `comparison/sessions/<sid>/jobs/<job_id>.json`.

Каждый item:
{
    "pair_id": "...",
    "status": "queued|enriching|comparing|done|failed|skipped|cancelled",
    "enrichment_status": "...",
    "comparison_status": "...",
    "changes_count": 0,
    "error": null,
    "duration_sec": 0.0
}
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from datetime import datetime
from typing import Any, Optional

from . import paths as paths_mod
from . import store as store_mod
from . import unified_analysis as ua_mod
from . import unified_findings as uf_mod

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_active_tasks: dict[str, dict[str, "asyncio.Task[Any]"]] = {}


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_job_id() -> str:
    return f"uajob_{uuid.uuid4().hex[:12]}"


def _read_job(session_id: str, job_id: str) -> Optional[dict]:
    try:
        p = paths_mod.job_json_path(session_id, job_id)
    except ValueError:
        return None
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_job(session_id: str, job: dict) -> None:
    p = paths_mod.job_json_path(session_id, job["id"])
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _collect_pair_ids(
    session: dict,
    *,
    scope: str,
    pair_id: Optional[str],
    pair_ids: Optional[list[str]],
) -> list[str]:
    all_pairs = [p for p in (session.get("pairs") or [])
                 if p.get("status") != "disabled" and p.get("id")]
    valid = {p["id"] for p in all_pairs}
    if scope == "pair":
        if not pair_id or pair_id not in valid:
            return []
        return [pair_id]
    if scope == "selected":
        return [pid for pid in (pair_ids or []) if pid in valid]
    if scope == "session":
        return [p["id"] for p in all_pairs]
    return []


def create_unified_job(
    session_id: str,
    *,
    scope: str,
    pair_id: Optional[str] = None,
    pair_ids: Optional[list[str]] = None,
    force_enrichment: bool = False,
    force_compare: bool = False,
    confirm: bool = False,
) -> dict:
    """Создать unified job. Без confirm=true сразу rejected."""
    if scope not in ("pair", "session", "selected"):
        raise ValueError("scope must be pair|session|selected")
    with _lock:
        session = store_mod.get_session(session_id)
        if session is None:
            raise KeyError("session_not_found")
        ids = _collect_pair_ids(session, scope=scope, pair_id=pair_id, pair_ids=pair_ids)
        job_id = _new_job_id()
        now = _utc_now()
        items = [
            {
                "pair_id": pid,
                "status": "queued",
                "enrichment_status": "not_run",
                "comparison_status": "not_run",
                "changes_count": 0,
                "error": None,
                "duration_sec": 0.0,
            }
            for pid in ids
        ]
        job = {
            "id": job_id,
            "session_id": session_id,
            "type": "unified_stage_comparison",
            "scope": scope,
            "pair_id": pair_id,
            "force_enrichment": bool(force_enrichment),
            "force_compare": bool(force_compare),
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


def get_job(session_id: str, job_id: str) -> Optional[dict]:
    return _read_job(session_id, job_id)


def cancel_job(session_id: str, job_id: str) -> Optional[dict]:
    with _lock:
        job = _read_job(session_id, job_id)
        if job is None:
            return None
        if job.get("status") in ("done", "failed", "cancelled", "rejected_no_confirm"):
            return job
        job["status"] = "cancelled"
        job["updated_at"] = _utc_now()
        for it in job.get("items") or []:
            if it.get("status") in ("queued", "running", "enriching", "comparing"):
                it["status"] = "cancelled"
        _write_job(session_id, job)
    return job


def list_unified_jobs(session_id: str) -> list[dict]:
    try:
        root = paths_mod.jobs_root(session_id)
    except (OSError, ValueError):
        return []
    out: list[dict] = []
    for p in sorted(root.glob("*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            if d.get("type") == "unified_stage_comparison":
                out.append(d)
        except (OSError, json.JSONDecodeError):
            continue
    out.sort(key=lambda j: j.get("created_at") or "", reverse=True)
    return out


async def run_unified_job(session_id: str, job_id: str) -> dict:
    """Прогнать unified job: для каждой пары вызвать unified_analysis.run_pair.

    После обработки всех пар — обновить unified_findings.json.
    """
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") not in ("queued", "running"):
        return job
    job["status"] = "running"
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    force_e = bool(job.get("force_enrichment"))
    force_c = bool(job.get("force_compare"))
    items = list(job.get("items") or [])
    for idx, item in enumerate(items):
        latest = _read_job(session_id, job_id)
        if latest and latest.get("status") == "cancelled":
            return latest
        if item.get("status") not in ("queued",):
            continue
        pid = item.get("pair_id")
        if not pid:
            item["status"] = "failed"
            item["error"] = "missing_pair_id"
            job["progress"]["failed"] += 1
            job["items"][idx] = item
            _write_job(session_id, job)
            continue

        def _on_progress(res: ua_mod.PairRunResult, _idx=idx):
            current = job["items"][_idx]
            current["status"] = res.status
            current["enrichment_status"] = res.enrichment_status
            current["comparison_status"] = res.comparison_status
            current["changes_count"] = res.changes_count
            current["duration_sec"] = round(res.duration_sec, 3)
            current["error"] = res.error
            job["items"][_idx] = current
            job["updated_at"] = _utc_now()
            try:
                _write_job(session_id, job)
            except OSError:
                pass

        try:
            res = await ua_mod.run_pair(
                session_id, pid,
                force_enrichment=force_e,
                force_compare=force_c,
                progress_cb=_on_progress,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("unified_job: item %s failed", pid)
            item["status"] = "failed"
            item["error"] = str(exc)[:300]
            job["progress"]["failed"] += 1
            job["items"][idx] = item
            _write_job(session_id, job)
            continue

        item["status"] = res.status
        item["enrichment_status"] = res.enrichment_status
        item["comparison_status"] = res.comparison_status
        item["changes_count"] = res.changes_count
        item["duration_sec"] = round(res.duration_sec, 3)
        item["error"] = res.error
        if res.status == "done":
            job["progress"]["done"] += 1
        elif res.status == "failed":
            job["progress"]["failed"] += 1
        elif res.status in ("skipped", "cancelled"):
            job["progress"]["skipped"] += 1
        job["items"][idx] = item
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)

    latest = _read_job(session_id, job_id)
    if latest and latest.get("status") == "cancelled":
        return latest

    job["status"] = "done"
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    # Обновляем unified findings (read-only агрегатор).
    try:
        uf_mod.rebuild_unified_findings(session_id)
    except Exception:  # noqa: BLE001
        logger.exception("unified_job: rebuild_unified_findings failed session=%s", session_id)

    return job


def start_job_in_background(session_id: str, job_id: str) -> str:
    loop = asyncio.get_event_loop()
    task = loop.create_task(run_unified_job(session_id, job_id))
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
    "create_unified_job",
    "get_job",
    "cancel_job",
    "list_unified_jobs",
    "run_unified_job",
    "start_job_in_background",
]
