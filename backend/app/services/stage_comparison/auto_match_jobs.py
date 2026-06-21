"""Пакетное авто-сопоставление листов по штампам (batch auto-match).

Проходит по ВСЕМ парам сессии, для каждой:
  1. строит suggestions через `store.suggest_alignment_by_stamp` (тот же
     алгоритм, что ручной `suggest-by-stamp`);
  2. безопасные пары применяет в `page_alignment`
     (`store.apply_safe_stamp_alignment_for_pair`);
  3. рискованные/низкоуверенные оставляет на ручную проверку;
  4. не перезаписывает ручное выравнивание (если overwrite_existing=False).

Конвенции job'а зеркалят `large_sheet_enrichment_jobs.py`:
  * job персистится в ``comparison/sessions/<sid>/jobs/<job_id>.json``;
  * background-таск трекается в ``_active_tasks`` (cancel/stale-detection);
  * per-pair work выносится в ``asyncio.to_thread`` (suggest может звать claude
    subprocess) — event loop не блокируется.

Fail-soft: ошибка по одной паре пишется в failed_pairs, job идёт дальше.
Сеть/Qwen/crop_url НЕ задействованы (это offline-матчинг по именам листов;
опц. LLM-доматчинг — отдельный subprocess под env kill-switch).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from . import paths as paths_mod
from . import store as store_mod

logger = logging.getLogger(__name__)

_JOB_PREFIX = "amj_"
_active_tasks: dict[str, dict[str, "asyncio.Task[Any]"]] = {}
_MAX_EVENTS = 25


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


def _doc_name(side: dict | None) -> str:
    side = side or {}
    for key in ("pdf_path", "md_path", "result_json_path"):
        v = side.get(key)
        if v:
            return Path(str(v)).name
    return side.get("name") or "?"


def _pair_label(pair: dict) -> dict:
    left = pair.get("left") or {}
    right = pair.get("right") or {}
    return {
        "old_document": _doc_name(left),
        "new_document": _doc_name(right),
        "old_stage": left.get("stage") or left.get("stage_label") or "",
        "new_stage": right.get("stage") or right.get("stage_label") or "",
    }


# ─── create / get / cancel ──────────────────────────────────────────────────

def create_job(
    session_id: str, *,
    use_llm: bool = True,
    overwrite_existing: bool = False,
    auto_apply: bool = True,
) -> dict:
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")

    pairs = [p for p in (session.get("pairs") or []) if p.get("id")]
    job_id = _new_job_id()
    job = {
        "id": job_id, "session_id": session_id, "kind": "page_alignment_auto_match",
        "use_llm": bool(use_llm), "overwrite_existing": bool(overwrite_existing),
        "auto_apply": bool(auto_apply),
        "created_at": _utc_now(), "updated_at": _utc_now(),
        "status": "queued",
        "items": [
            {"pair_id": p["id"], **_pair_label(p), "status": "queued",
             "applied": 0, "review": 0, "skipped_reason": None,
             "split_prevented": 0, "true_left_only": 0, "true_right_only": 0,
             "positional_alignment": 0,
             "review_reasons": [],
             "confidence": 0.0, "errors": []}
            for p in pairs
        ],
        "total_pairs": len(pairs),
        "processed_pairs": 0,
        "current_pair": None,
        "summary": {
            "total_pairs": len(pairs), "processed_pairs": 0,
            # Раздельные счётчики (Task 9): применено vs оставлено на ревью vs
            # истинно односторонние vs предотвращённые разрывы пар vs позиционно.
            "applied_matched_pairs": 0, "review_matched_pairs": 0,
            "split_prevented": 0, "true_left_only": 0, "true_right_only": 0,
            "positional_alignment": 0,
            "needs_review_pairs": 0,
            "skipped_existing_alignment": 0, "failed_pairs": 0,
            # #65: агрегаты LLM-доматчинга листов по штампу.
            "llm_pairs_added": 0, "llm_failures": 0, "llm_status_distribution": {},
            # Backward-compat алиасы (старый UI/артефакты читали эти ключи).
            "applied_pairs": 0, "review_pairs": 0,
        },
        "last_events": [],
    }
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
    if job.get("status") in ("done", "failed", "cancelled", "interrupted"):
        return job
    job["status"] = "cancelled"
    job["updated_at"] = _utc_now()
    for it in job.get("items") or []:
        if it.get("status") in ("queued", "running"):
            it["status"] = "cancelled"
    _write_job(session_id, job)
    bucket = _active_tasks.get(session_id) or {}
    task = bucket.get(job_id)
    if task and not task.done():
        task.cancel()
    return job


def latest_job(session_id: str) -> Optional[dict]:
    root = paths_mod.jobs_root(session_id)
    best = None
    best_ts = ""
    for p in root.glob(f"{_JOB_PREFIX}*.json"):
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        ts = str(data.get("created_at") or "")
        if best is None or ts > best_ts:
            best, best_ts = data, ts
    return _maybe_mark_interrupted(best) if best else None


def read_last_run(session_id: str) -> Optional[dict]:
    """Прочитать project-local артефакт последнего прогона (для reload UI)."""
    try:
        p = paths_mod.auto_match_last_run_path(session_id)
    except (ValueError, OSError):
        return None
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _write_artifact(session_id: str, job: dict) -> None:
    try:
        p = paths_mod.auto_match_last_run_path(session_id)
        artifact = {
            "started_at": job.get("created_at"),
            "finished_at": job.get("finished_at"),
            "job_id": job.get("id"),
            "use_llm": job.get("use_llm"),
            "overwrite_existing": job.get("overwrite_existing"),
            "status": job.get("status"),
            "total_pairs": job.get("total_pairs"),
            "processed_pairs": job.get("processed_pairs"),
            "summary": job.get("summary"),
            "pairs": [
                {"pair_id": it["pair_id"], "old_document": it.get("old_document"),
                 "new_document": it.get("new_document"), "status": it.get("status"),
                 "applied": it.get("applied", 0), "review": it.get("review", 0),
                 "split_prevented": it.get("split_prevented", 0),
                 "true_left_only": it.get("true_left_only", 0),
                 "true_right_only": it.get("true_right_only", 0),
                 "positional_alignment": it.get("positional_alignment", 0),
                 "review_reasons": it.get("review_reasons", []),
                 "skipped_reason": it.get("skipped_reason"),
                 "errors": it.get("errors", [])}
                for it in (job.get("items") or [])
            ],
        }
        tmp = p.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(artifact, f, ensure_ascii=False, indent=2)
        os.replace(tmp, p)
    except OSError:
        logger.warning("auto_match: failed to write last_run artifact", exc_info=True)


# ─── run ────────────────────────────────────────────────────────────────────

def _preflight_llm(use_llm: bool) -> tuple[bool, dict]:
    """#66: один preflight LLM-провайдера на весь auto-match job.

    Раньше доступность ClaudeCodeProvider проверялась внутри suggest на КАЖДУЮ
    пару. Если провайдер недоступен (или LLM выключен флагом) — выключаем use_llm
    на весь прогон, не дёргая check десятки раз. Возвращает (effective_use_llm, diag).
    fail-soft: любая ошибка → use_llm=False с пометкой.
    """
    if not use_llm:
        return False, {"status": "not_requested"}
    try:
        from . import stamp_llm_match as _slm
        from .text_llm_provider import ClaudeCodeProvider
        if not _slm.stamp_llm_enabled():
            return False, {"status": "disabled_by_flag"}
        ok, reason = ClaudeCodeProvider().check_availability()
        if not ok:
            return False, {"status": "provider_unavailable", "reason": reason}
        return True, {"status": "ok"}
    except Exception as exc:  # noqa: BLE001 — fail-soft, не валим job
        return False, {"status": "preflight_exception", "error": str(exc)}


async def run_job(session_id: str, job_id: str) -> dict:
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") in ("done", "failed", "cancelled"):
        return job
    if job.get("status") == "running" and _is_task_alive(session_id, job_id):
        return job

    job["status"] = "running"
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    use_llm = bool(job.get("use_llm"))
    # #66: один preflight провайдера на весь прогон (не per-pair).
    use_llm, job["llm_preflight"] = _preflight_llm(use_llm)
    job["use_llm_effective"] = use_llm
    _write_job(session_id, job)
    overwrite = bool(job.get("overwrite_existing"))
    auto_apply = bool(job.get("auto_apply", True))

    def _is_cancelled() -> bool:
        latest = _read_job(session_id, job_id)
        return bool(latest and latest.get("status") == "cancelled")

    def _record_event(ev: dict) -> None:
        evs = job.setdefault("last_events", [])
        evs.append(ev)
        if len(evs) > _MAX_EVENTS:
            del evs[:-_MAX_EVENTS]

    try:
        for item in job["items"]:
            if _is_cancelled():
                break
            item["status"] = "running"
            job["current_pair"] = {
                "pair_id": item["pair_id"], "old_document": item.get("old_document"),
                "new_document": item.get("new_document"),
                "old_stage": item.get("old_stage"), "new_stage": item.get("new_stage"),
            }
            job["updated_at"] = _utc_now()
            _write_job(session_id, job)

            try:
                # suggest + (опц.) save вынесены в thread: внутри может быть
                # claude subprocess (LLM-доматчинг) — не блокируем event loop.
                summary = await asyncio.to_thread(
                    store_mod.apply_safe_stamp_alignment_for_pair,
                    session_id, item["pair_id"],
                    use_llm=use_llm,
                    overwrite_existing=overwrite,
                ) if auto_apply else await asyncio.to_thread(
                    store_mod.suggest_alignment_by_stamp,
                    session_id, item["pair_id"], use_llm=use_llm,
                )
                if auto_apply:
                    item["status"] = summary.get("status", "done")
                    item["applied"] = summary.get("applied", 0)
                    item["review"] = summary.get("review", 0)
                    item["split_prevented"] = summary.get("split_prevented", 0)
                    item["true_left_only"] = summary.get("true_left_only", 0)
                    item["true_right_only"] = summary.get("true_right_only", 0)
                    item["positional_alignment"] = summary.get("positional_alignment", 0)
                    # Компактные причины review-пар (для UI «на ручную проверку»).
                    item["review_reasons"] = [
                        {"left_page": r.get("left_page"), "right_page": r.get("right_page"),
                         "reason": r.get("reason"), "match_type": r.get("match_type"),
                         "score": r.get("score")}
                        for r in (summary.get("review_items") or [])
                    ]
                    item["skipped_reason"] = summary.get("skipped_reason")
                    item["confidence"] = summary.get("confidence", 0.0)
                    s = job["summary"]
                    s["applied_matched_pairs"] += item["applied"]
                    s["review_matched_pairs"] += item["review"]
                    s["split_prevented"] += item["split_prevented"]
                    s["true_left_only"] += item["true_left_only"]
                    s["true_right_only"] += item["true_right_only"]
                    s["positional_alignment"] += item["positional_alignment"]
                    # backward-compat алиасы
                    s["applied_pairs"] = s["applied_matched_pairs"]
                    s["review_pairs"] = s["review_matched_pairs"]
                    # #65: свод LLM-диагностики доматчинга.
                    llm = summary.get("llm") or {}
                    item["llm"] = llm
                    if llm:
                        s["llm_pairs_added"] += int(llm.get("pairs_added") or 0)
                        st = str(llm.get("status") or "unknown")
                        s["llm_status_distribution"][st] = (
                            s["llm_status_distribution"].get(st, 0) + 1
                        )
                        if llm.get("error") or st in ("setup_exception", "provider_not_available"):
                            s["llm_failures"] += 1
                    if item["status"] == "skipped_existing_alignment":
                        s["skipped_existing_alignment"] += 1
                    elif item["status"] == "needs_review":
                        s["needs_review_pairs"] += 1
                    elif item["status"] == "error":
                        s["failed_pairs"] += 1
                        item["errors"] = summary.get("errors") or ["apply_failed"]
                else:
                    item["status"] = "done"
                    item["applied"] = 0
                    item["review"] = summary.get("matched_count", 0)
                    item["confidence"] = summary.get("confidence", 0.0)
            except asyncio.CancelledError:
                item["status"] = "cancelled"
                raise
            except Exception as exc:  # noqa: BLE001 — одна пара не валит job
                item["status"] = "error"
                item["errors"] = [f"{type(exc).__name__}:{exc}"]
                job["summary"]["failed_pairs"] += 1
                logger.exception("auto_match: pair %s failed", item.get("pair_id"))
            finally:
                job["processed_pairs"] += 1
                job["summary"]["processed_pairs"] = job["processed_pairs"]
                job["current_pair"] = None
                _record_event({
                    "pair": item["pair_id"], "status": item["status"],
                    "applied": item.get("applied", 0), "review": item.get("review", 0),
                    "confidence": item.get("confidence", 0.0),
                })
                job["updated_at"] = _utc_now()
                _write_job(session_id, job)

        latest = _read_job(session_id, job_id)
        if latest and latest.get("status") == "cancelled":
            job["status"] = "cancelled"
        else:
            job["status"] = "finished"
    except asyncio.CancelledError:
        job["status"] = "cancelled"
        for it in job["items"]:
            if it.get("status") in ("queued", "running"):
                it["status"] = "cancelled"
    except Exception as exc:  # noqa: BLE001
        job["status"] = "failed"
        job["error"] = f"{type(exc).__name__}:{exc}"
        logger.exception("auto_match job failed")
    finally:
        job["finished_at"] = _utc_now()
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)
        _write_artifact(session_id, job)
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
    "create_job", "get_job", "cancel_job", "latest_job", "read_last_run",
    "run_job", "start_job_in_background",
]
