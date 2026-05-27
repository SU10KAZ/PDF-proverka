"""Batch jobs для MD image enrichment.

Поведение похоже на text_llm_jobs.py:
  * один тип job: "md_enrichment_batch";
  * scope: "pair" | "session" | "selected";
  * без confirm=true сразу rejected;
  * каждый item — это (pair_id, side); работа = enrich_side(...).

Хранение в общем `comparison/sessions/<sid>/jobs/<job_id>.json`.
"""
from __future__ import annotations

import asyncio
import functools
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import md_image_enrichment as md_mod
from . import paths as paths_mod
from . import store as store_mod
from . import graphic_llm_local as graphic_local_mod

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_active_tasks: dict[str, dict[str, "asyncio.Task[Any]"]] = {}


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_job_id() -> str:
    return f"mdenrich_{uuid.uuid4().hex[:12]}"


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
    session: dict, *, scope: str, pair_id: Optional[str], pair_ids: Optional[list[str]]
) -> list[str]:
    all_pairs = [p for p in (session.get("pairs") or []) if p.get("status") != "disabled" and p.get("id")]
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


def _pair_summary_is_done(session_id: str, pair_id: str, side: str) -> bool:
    """Pair side считается готовым, если есть enriched MD и нет ошибок."""
    try:
        from . import md_image_enrichment as _mi
        from . import paths as _paths
        summary = _mi.read_summary_only(session_id, pair_id, side)
    except Exception:  # noqa: BLE001
        return False
    if (summary.get("status") or "not_run") != "done":
        return False
    if int(summary.get("errors") or 0) > 0:
        return False
    if int(summary.get("pending") or 0) > 0:
        return False
    enriched = summary.get("enriched_md_path")
    if not enriched:
        return False
    try:
        return Path(enriched).exists()
    except Exception:  # noqa: BLE001
        return False


def _pair_label(session_id: str, pair_id: Optional[str]) -> Optional[str]:
    if not pair_id:
        return None
    session = store_mod.get_session(session_id)
    if session is None:
        return None
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        return None
    left = (pair.get("left") or {}).get("filename") or "—"
    right = (pair.get("right") or {}).get("filename") or "—"
    return f"{left} ↔ {right}"


def _resolve_pair_paths(session_id: str, pair_id: str, side: str) -> tuple[Optional[str], Optional[str]]:
    """Найти (md_path, result_json_path) стороны пары в session.json."""
    session = store_mod.get_session(session_id)
    if session is None:
        return None, None
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        return None, None
    side_obj = pair.get(side) or {}
    return side_obj.get("md_path"), side_obj.get("result_json_path")


def _make_render_callback(session_id: str, pair_id: str, side: str):
    """functools.partial для store.render_block_crop, чтобы передать в enrich_side."""
    def _render(block_id: str) -> Optional[Path]:
        try:
            return store_mod.render_block_crop(session_id, pair_id, side, block_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug("md_enrichment_jobs: render_block_crop failed: %s", exc)
            return None
    return _render


def create_md_enrichment_job(
    session_id: str,
    *,
    scope: str,
    pair_id: Optional[str] = None,
    pair_ids: Optional[list[str]] = None,
    side: str = "both",
    force: bool = False,
    confirm: bool,
    skip_done: bool = True,
) -> dict:
    """Создать MD enrichment job.

    `side` — "left" / "right" / "both": какую сторону каждой пары обработать.
    `skip_done` — пропускать стороны, у которых enriched MD уже готов без
    ошибок. Игнорируется при force=True.
    """
    if side not in ("left", "right", "both"):
        raise ValueError("side must be 'left' | 'right' | 'both'")
    with _lock:
        session = store_mod.get_session(session_id)
        if session is None:
            raise KeyError("session_not_found")
        ids = _collect_pair_ids(session, scope=scope, pair_id=pair_id, pair_ids=pair_ids)
        sides = ("left", "right") if side == "both" else (side,)
        items: list[dict] = []
        skipped_items: list[dict] = []
        do_skip = bool(skip_done) and not bool(force)
        for pid in ids:
            for s in sides:
                if do_skip and _pair_summary_is_done(session_id, pid, s):
                    skipped_items.append(
                        {"pair_id": pid, "side": s, "status": "skipped",
                         "error": None, "summary_status": "done",
                         "skipped_reason": "already_done"}
                    )
                    continue
                items.append({"pair_id": pid, "side": s, "status": "queued", "error": None})
        all_items = items + skipped_items
        job_id = _new_job_id()
        now = _utc_now()
        job = {
            "id": job_id,
            "session_id": session_id,
            "type": "md_enrichment_batch",
            "scope": scope,
            "pair_id": pair_id,
            "side": side,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "items": all_items,
            "force": bool(force),
            "confirm": bool(confirm),
            "skip_done": bool(skip_done),
            "current": {
                "pair_id": None,
                "pair_label": None,
                "side": None,
                "block_index": 0,
                "total_blocks": 0,
                "page": None,
                "status_message": "",
            },
            "progress": {
                "total": len(all_items),
                "done": len(skipped_items),
                "failed": 0,
                "skipped": len(skipped_items),
            },
        }
        if not confirm:
            job["status"] = "rejected_no_confirm"
            job["updated_at"] = _utc_now()
        elif not items:
            # ничего не нужно делать — всё уже готово
            job["status"] = "done"
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
            if it.get("status") in ("queued", "running"):
                it["status"] = "cancelled"
        _write_job(session_id, job)
        return job


def list_md_enrichment_jobs(session_id: str) -> list[dict]:
    try:
        root = paths_mod.jobs_root(session_id)
    except (OSError, ValueError):
        return []
    out: list[dict] = []
    for p in sorted(root.glob("*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            if d.get("type") == "md_enrichment_batch":
                out.append(d)
        except (OSError, json.JSONDecodeError):
            continue
    out.sort(key=lambda j: j.get("created_at") or "", reverse=True)
    return out


async def run_md_enrichment_job(session_id: str, job_id: str) -> dict:
    """Прогнать job: для каждой (pair_id, side) пары вызвать enrich_side."""
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") not in ("queued", "running"):
        return job
    job["status"] = "running"
    if not job.get("started_at"):
        job["started_at"] = _utc_now()
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    items = list(job.get("items") or [])
    cfg = graphic_local_mod.load_local_graphic_llm_config()
    force = bool(job.get("force"))

    for idx, item in enumerate(items):
        latest = _read_job(session_id, job_id)
        if latest and latest.get("status") == "cancelled":
            return latest
        if item.get("status") != "queued":
            continue
        item["status"] = "running"
        item["updated_at"] = _utc_now()
        job["items"][idx] = item
        # job-level current
        pair_label = _pair_label(session_id, item.get("pair_id"))
        job["current"] = {
            "pair_id": item.get("pair_id"),
            "pair_label": pair_label,
            "side": item.get("side"),
            "block_index": 0,
            "total_blocks": 0,
            "page": None,
            "status_message": "running",
        }
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)

        pid = item.get("pair_id")
        side = item.get("side")
        md_path, rjp = _resolve_pair_paths(session_id, pid, side)

        def _on_block(progress_payload: dict, _idx=idx, _pid=pid, _side=side, _label=pair_label) -> None:
            """Callback из enrich_side: обновить item.current + перезаписать job.json."""
            try:
                with _lock:
                    latest_job = _read_job(session_id, job_id)
                    if latest_job is None:
                        return
                    if latest_job.get("status") == "cancelled":
                        return
                    items_l = latest_job.get("items") or []
                    if _idx >= len(items_l):
                        return
                    block_index = int(progress_payload.get("block_index") or 0)
                    total = int(progress_payload.get("total") or 0)
                    page = progress_payload.get("page")
                    last_status = progress_payload.get("status")
                    items_l[_idx]["current"] = {
                        "block_index": block_index,
                        "total": total,
                        "block_id": progress_payload.get("block_id"),
                        "page": page,
                        "last_status": last_status,
                    }
                    items_l[_idx]["updated_at"] = _utc_now()
                    latest_job["current"] = {
                        "pair_id": _pid,
                        "pair_label": _label,
                        "side": _side,
                        "block_index": block_index,
                        "total_blocks": total,
                        "page": page,
                        "status_message": last_status or "running",
                    }
                    latest_job["updated_at"] = _utc_now()
                    _write_job(session_id, latest_job)
            except Exception:  # noqa: BLE001
                logger.debug("md_enrichment_job on_block update failed", exc_info=True)

        try:
            summary = await md_mod.enrich_side(
                session_id, pid, side,
                md_path=md_path,
                result_json_path=rjp,
                render_crop=_make_render_callback(session_id, pid, side),
                run_model=True,
                force=force,
                cfg=cfg,
                on_block_progress=_on_block,
            )
            item["image_blocks"] = summary.image_blocks
            item["described"] = summary.described
            item["from_cache"] = summary.from_cache
            item["errors"] = summary.errors
            item["pending"] = summary.pending
            item["summary_status"] = summary.status
            if summary.status in ("done", "partial"):
                item["status"] = "done"
                job["progress"]["done"] += 1
            else:
                item["status"] = "failed"
                item["error"] = ";".join(summary.warnings) or summary.status
                job["progress"]["failed"] += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("md_enrichment_job: item %s/%s failed", pid, side)
            item["status"] = "failed"
            item["error"] = str(exc)[:300]
            job["progress"]["failed"] += 1

        job["items"][idx] = item
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)

    latest = _read_job(session_id, job_id)
    if latest and latest.get("status") == "cancelled":
        return latest
    job["status"] = "done"
    job["current"] = {
        "pair_id": None,
        "pair_label": None,
        "side": None,
        "block_index": 0,
        "total_blocks": 0,
        "page": None,
        "status_message": "completed",
    }
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)
    return job


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        s = value.rstrip("Z")
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def aggregate_job_progress(session_id: str, job: dict) -> dict:
    """Подсчитать session-level прогресс по job для UI.

    Возвращает структуру, которую UI использует напрямую: счётчики по парам,
    счётчики по image-блокам, информацию о текущей паре, elapsed_sec, и
    per-pair статусы (`pair_statuses`).
    """
    items = list(job.get("items") or [])

    # ── per-pair агрегация ──────────────────────────────────────────
    by_pair: dict[str, dict[str, dict]] = {}  # pair_id → {side → item}
    for it in items:
        pid = it.get("pair_id")
        side = it.get("side")
        if not pid or side not in ("left", "right"):
            continue
        by_pair.setdefault(pid, {})[side] = it

    pair_statuses: dict[str, dict] = {}
    total_pairs = 0
    done_pairs = 0
    partial_pairs = 0
    error_pairs = 0
    skipped_pairs = 0
    not_run_pairs = 0
    running_pair_id: Optional[str] = None

    total_image_blocks = 0
    done_image_blocks = 0
    failed_image_blocks = 0
    cache_hits = 0

    for pid, sides in by_pair.items():
        total_pairs += 1
        side_statuses: dict[str, dict] = {}
        for side in ("left", "right"):
            it = sides.get(side)
            if it is None:
                side_statuses[side] = {"status": "not_in_job"}
                continue
            st = (it.get("summary_status") or "").lower()
            it_status = (it.get("status") or "queued").lower()
            ib = int(it.get("image_blocks") or 0)
            described = int(it.get("described") or 0)
            from_cache = int(it.get("from_cache") or 0)
            errors_n = int(it.get("errors") or 0)
            pending = int(it.get("pending") or 0)
            total_image_blocks += ib
            done_image_blocks += described
            failed_image_blocks += errors_n
            cache_hits += from_cache
            # производный статус стороны
            if it_status == "cancelled":
                side_status = "cancelled"
            elif it_status == "running":
                side_status = "running"
            elif it_status == "queued":
                side_status = "queued"
            elif it_status == "skipped":
                side_status = "skipped"
            elif it_status == "failed":
                side_status = "error"
            elif st == "done":
                side_status = "done"
            elif st == "partial":
                side_status = "partial"
            elif st == "error":
                side_status = "error"
            elif st == "not_run":
                side_status = "not_run"
            else:
                side_status = it_status or "not_run"
            side_statuses[side] = {
                "status": side_status,
                "image_blocks": ib,
                "described": described,
                "from_cache": from_cache,
                "errors": errors_n,
                "pending": pending,
                "error": it.get("error"),
                "current": it.get("current"),
            }

        side_status_vals = [s.get("status") for s in side_statuses.values()]
        # производный статус пары
        if any(s == "running" for s in side_status_vals):
            pair_status = "running"
            running_pair_id = pid
        elif any(s == "error" for s in side_status_vals):
            pair_status = "error"
        elif any(s == "partial" for s in side_status_vals):
            pair_status = "partial"
        elif any(s == "queued" for s in side_status_vals):
            pair_status = "queued"
        elif all(s in ("done", "skipped", "not_in_job") for s in side_status_vals) and any(
            s in ("done", "skipped") for s in side_status_vals
        ):
            pair_status = "done"
        elif all(s == "skipped" for s in side_status_vals):
            pair_status = "skipped"
        elif all(s in ("cancelled", "not_in_job") for s in side_status_vals):
            pair_status = "cancelled"
        else:
            pair_status = "not_run"

        ready = (
            side_statuses.get("left", {}).get("status") in ("done", "skipped")
            and side_statuses.get("right", {}).get("status") in ("done", "skipped")
            and int(side_statuses.get("left", {}).get("errors") or 0) == 0
            and int(side_statuses.get("right", {}).get("errors") or 0) == 0
        )

        if pair_status == "done":
            done_pairs += 1
        elif pair_status == "partial":
            partial_pairs += 1
        elif pair_status == "error":
            error_pairs += 1
        elif pair_status == "skipped":
            skipped_pairs += 1
        elif pair_status == "running":
            pass  # учитываем отдельно через running_pair_id
        else:
            not_run_pairs += 1

        pair_statuses[pid] = {
            "pair_id": pid,
            "pair_label": _pair_label(session_id, pid),
            "status": pair_status,
            "left": side_statuses.get("left"),
            "right": side_statuses.get("right"),
            "ready_for_unified_analysis": ready,
        }

    # ── elapsed_sec ─────────────────────────────────────────────────
    started = _parse_iso(job.get("started_at"))
    last_ts = _parse_iso(job.get("updated_at"))
    if (job.get("status") or "") in ("queued", "running") and started:
        now_dt = datetime.now(timezone.utc)
        elapsed = (now_dt - started).total_seconds()
    elif started and last_ts:
        elapsed = (last_ts - started).total_seconds()
    else:
        elapsed = 0.0

    cur = job.get("current") or {}
    return {
        "job_id": job.get("id"),
        "status": job.get("status"),
        "scope": job.get("scope"),
        "force": bool(job.get("force")),
        "skip_done": bool(job.get("skip_done", True)),
        "started_at": job.get("started_at"),
        "updated_at": job.get("updated_at"),
        "created_at": job.get("created_at"),
        "elapsed_sec": max(0.0, round(elapsed, 1)),
        "total_pairs": total_pairs,
        "done_pairs": done_pairs,
        "partial_pairs": partial_pairs,
        "error_pairs": error_pairs,
        "skipped_pairs": skipped_pairs,
        "not_run_pairs": not_run_pairs,
        "running_pair_id": running_pair_id,
        "total_image_blocks": total_image_blocks,
        "done_image_blocks": done_image_blocks,
        "failed_image_blocks": failed_image_blocks,
        "cache_hits": cache_hits,
        "current_pair_id": cur.get("pair_id"),
        "current_pair_label": cur.get("pair_label"),
        "current_side": cur.get("side"),
        "current_block_index": int(cur.get("block_index") or 0),
        "current_total_blocks": int(cur.get("total_blocks") or 0),
        "current_page": cur.get("page"),
        "current_status_message": cur.get("status_message") or "",
        "pair_statuses": pair_statuses,
    }


def get_job_with_progress(session_id: str, job_id: str) -> Optional[dict]:
    """get_job + aggregate_job_progress в одну структуру для UI."""
    job = _read_job(session_id, job_id)
    if job is None:
        return None
    job = dict(job)
    job["aggregate"] = aggregate_job_progress(session_id, job)
    return job


def find_active_session_job(session_id: str) -> Optional[dict]:
    """Найти самую свежую `md_enrichment_batch` job сессии.

    Возвращает job в состоянии queued/running, если такая есть. Иначе —
    самую свежую завершённую (done/failed/cancelled), чтобы UI мог
    показать «последний прогон». Если jobs пусто — None.
    """
    jobs = list_md_enrichment_jobs(session_id)
    if not jobs:
        return None
    for j in jobs:
        if j.get("status") in ("queued", "running"):
            return get_job_with_progress(session_id, j["id"])
    return get_job_with_progress(session_id, jobs[0]["id"])


def start_job_in_background(session_id: str, job_id: str) -> str:
    loop = asyncio.get_event_loop()
    task = loop.create_task(run_md_enrichment_job(session_id, job_id))
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
    "create_md_enrichment_job",
    "get_job",
    "get_job_with_progress",
    "find_active_session_job",
    "aggregate_job_progress",
    "cancel_job",
    "list_md_enrichment_jobs",
    "run_md_enrichment_job",
    "start_job_in_background",
]
