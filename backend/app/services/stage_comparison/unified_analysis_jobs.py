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
from datetime import datetime, timezone
from typing import Any, Optional

from . import enriched_comparison as ec_mod
from . import evidence_first_fallback as ef_mod
from . import md_image_enrichment as md_mod
from . import paths as paths_mod
from . import store as store_mod
from . import unified_analysis as ua_mod
from . import unified_findings as uf_mod
from . import unified_grouping as ug_mod

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_active_tasks: dict[str, dict[str, "asyncio.Task[Any]"]] = {}


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_job_id() -> str:
    return f"uajob_{uuid.uuid4().hex[:12]}"


def _read_job_raw(session_id: str, job_id: str) -> Optional[dict]:
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


def _read_job(session_id: str, job_id: str) -> Optional[dict]:
    data = _read_job_raw(session_id, job_id)
    if data is None:
        return None
    return _maybe_mark_interrupted(session_id, data)


def _write_job(session_id: str, job: dict) -> None:
    p = paths_mod.job_json_path(session_id, job["id"])
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


_STALE_QUEUED_GRACE_SECONDS = 60
_NON_TERMINAL_ITEM_STATES = ("queued", "running", "enriching", "comparing")


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.rstrip("Z")).replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def _is_task_alive(session_id: str, job_id: str) -> bool:
    bucket = _active_tasks.get(session_id) or {}
    task = bucket.get(job_id)
    return bool(task and not task.done())


def _maybe_mark_interrupted(session_id: str, job: dict) -> dict:
    """Stale-job detection для unified-jobs.

    Если на диске job со status=running/queued, но `asyncio.Task` в текущем
    процессе не живёт (uvicorn перезапустился или task крэшнулся) — помечаем
    job как `failed_interrupted`, чтобы UI не висел на «Обработка…».
    Незавершённые items тоже становятся `failed_interrupted` — это нужно,
    чтобы при resume через `skip_ineligible=true, force_compare=false`
    готовые пары попали в skip_done, а незавершённые перезапустились.

    `queued` имеет grace-period 60s на гонку между create_job и
    start_job_in_background.
    """
    if not isinstance(job, dict):
        return job
    status = job.get("status")
    if status not in ("running", "queued"):
        return job
    job_id = job.get("id")
    if not job_id:
        return job
    if _is_task_alive(session_id, job_id):
        return job
    if status == "queued":
        updated = _parse_iso(job.get("updated_at") or job.get("created_at"))
        if updated is None:
            return job
        age = (datetime.now(timezone.utc) - updated).total_seconds()
        if age < _STALE_QUEUED_GRACE_SECONDS:
            return job
    job["status"] = "failed_interrupted"
    job["updated_at"] = _utc_now()
    job.setdefault(
        "error",
        "Backend перезапустился во время выполнения, воркер был потерян.",
    )
    for it in job.get("items") or []:
        if it.get("status") in _NON_TERMINAL_ITEM_STATES:
            it["status"] = "failed_interrupted"
    try:
        _write_job(session_id, job)
    except OSError:
        pass
    return job


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


def _classify_pair_for_batch(
    session_id: str,
    pair_id: str,
    *,
    force_compare: bool,
    force_fallback: bool = False,
) -> dict:
    """Pre-flight per-pair: подходит ли пара для Opus batch.

    Возвращает {"action": "run|skip_not_ready|skip_too_large|skip_done|skip_error",
                "reason": str, "too_large": bool, "enriched_total_chars": int,
                "comparison_status": str, "format_version_ok": bool, "ready": bool}.

    Не вызывает Qwen и не запускает enrichment.
    """
    enriched_status = ec_mod.enriched_md_status(session_id, pair_id)
    ready = bool(enriched_status.get("ready"))
    enriched_total_chars = int(enriched_status.get("total_chars") or 0)
    fmt_version = enriched_status.get("enriched_md_format_version")
    left_fmt = (enriched_status.get("left") or {}).get("format_version")
    right_fmt = (enriched_status.get("right") or {}).get("format_version")
    target_fmt = md_mod.ENRICHED_MD_FORMAT_VERSION
    fmt_ok = (left_fmt == target_fmt and right_fmt == target_fmt)

    cfg = ec_mod.load_config()
    too_large = bool(
        cfg.max_chars and cfg.max_chars > 0 and enriched_total_chars > cfg.max_chars
    )
    # evidence_first_s2_fallback: при включённом флаге too_large НЕ блокирует —
    # пара запускается через fallback-стратегию в run_enriched_comparison.
    # При выключенном флаге поведение прежнее (skip_too_large).
    # force_fallback — явный per-pair override из UI: too_large прогоняется
    # через fallback даже если глобальный флаг выключен.
    fallback_enabled = bool(ef_mod.load_fallback_config().enabled)
    fallback_active = fallback_enabled or bool(force_fallback)
    too_large_blocks = too_large and not fallback_active

    existing = ec_mod.get_comparison_result(session_id, pair_id)
    comparison_status = str((existing or {}).get("status") or "not_run")

    info = {
        "ready": ready,
        "format_version_ok": fmt_ok,
        "enriched_total_chars": enriched_total_chars,
        "enriched_limit_chars": int(cfg.max_chars or 0),
        "too_large": too_large,
        "fallback_enabled": fallback_enabled,
        "fallback_forced": bool(force_fallback),
        "comparison_status": comparison_status,
        "outdated_format": bool(enriched_status.get("outdated_format")),
    }
    if too_large and fallback_active:
        info["analysis_strategy"] = ef_mod.STRATEGY

    if not ready:
        info["action"] = "skip_not_ready"
        info["reason"] = "enriched_md_missing"
        return info
    if too_large_blocks:
        info["action"] = "skip_too_large"
        info["reason"] = "enriched_total_chars_exceeds_limit"
        return info
    # comparison уже done и force_compare=False — пропускаем (resume-like).
    if comparison_status == "done" and not force_compare:
        info["action"] = "skip_done"
        info["reason"] = "comparison_already_done"
        return info
    info["action"] = "run"
    info["reason"] = ""
    return info


def preflight_session_for_batch(
    session_id: str,
    *,
    scope: str,
    pair_id: Optional[str] = None,
    pair_ids: Optional[list[str]] = None,
    force_compare: bool = False,
) -> dict:
    """Dry-run сводка перед созданием Opus batch job.

    Возвращает {total, will_run, skip_not_ready, skip_too_large, skip_done,
                items:[{pair_id, action, reason, ...}]}.

    Не пишет job на диск, ничего не запускает.
    """
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    ids = _collect_pair_ids(session, scope=scope, pair_id=pair_id, pair_ids=pair_ids)
    items: list[dict] = []
    counts = {
        "run": 0,
        "skip_not_ready": 0,
        "skip_too_large": 0,
        "skip_done": 0,
    }
    will_run_fallback = 0
    for pid in ids:
        try:
            info = _classify_pair_for_batch(
                session_id, pid, force_compare=bool(force_compare),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("preflight_session_for_batch: %s failed: %s", pid, exc)
            info = {"action": "skip_not_ready", "reason": f"preflight_exception:{exc}"}
        action = info.get("action") or "skip_not_ready"
        counts[action] = counts.get(action, 0) + 1
        if action == "run" and info.get("analysis_strategy") == ef_mod.STRATEGY:
            will_run_fallback += 1
        items.append({"pair_id": pid, **info})
    return {
        "session_id": session_id,
        "scope": scope,
        "force_compare": bool(force_compare),
        "total_pairs": len(ids),
        "will_run": counts["run"],
        "will_run_fallback": will_run_fallback,
        "skip_not_ready": counts["skip_not_ready"],
        "skip_too_large": counts["skip_too_large"],
        "skip_done": counts["skip_done"],
        "items": items,
    }


def create_unified_job(
    session_id: str,
    *,
    scope: str,
    pair_id: Optional[str] = None,
    pair_ids: Optional[list[str]] = None,
    force_enrichment: bool = False,
    force_compare: bool = False,
    force_fallback: bool = False,
    confirm: bool = False,
    skip_ineligible: bool = False,
) -> dict:
    """Создать unified job. Без confirm=true сразу rejected.

    `skip_ineligible` — pre-flight фильтр пар перед записью job:
        * not_ready (enriched MD отсутствует) → пропускаем;
        * too_large (превышает enriched_max_chars) → пропускаем;
        * comparison.status=='done' и not force_compare → пропускаем как done.
    Пары, прошедшие фильтр, попадают в `items` со status='queued';
    отфильтрованные — со status='skipped' и filled-in reason. Это
    предотвращает запуск Qwen / Opus по неготовым / слишком большим парам
    в session-level batch.

    `force_fallback` — явный per-pair override: too_large прогоняется через
    evidence_first_s2_fallback даже при выключенном глобальном флаге. Не
    меняет алгоритм fallback, только gate включения. Применяется UI-кнопкой
    «запустить fallback» на большой паре (обычно scope=selected с одной парой).
    """
    if scope not in ("pair", "session", "selected"):
        raise ValueError("scope must be pair|session|selected")
    with _lock:
        session = store_mod.get_session(session_id)
        if session is None:
            raise KeyError("session_not_found")
        ids = _collect_pair_ids(session, scope=scope, pair_id=pair_id, pair_ids=pair_ids)
        job_id = _new_job_id()
        now = _utc_now()

        items: list[dict] = []
        skipped_count = 0
        for pid in ids:
            base = {
                "pair_id": pid,
                "status": "queued",
                "enrichment_status": "not_run",
                "comparison_status": "not_run",
                "changes_count": 0,
                "error": None,
                "duration_sec": 0.0,
            }
            if skip_ineligible:
                try:
                    info = _classify_pair_for_batch(
                        session_id, pid, force_compare=bool(force_compare),
                        force_fallback=bool(force_fallback),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("create_unified_job: classify %s failed: %s", pid, exc)
                    info = {"action": "skip_not_ready",
                            "reason": f"preflight_exception:{exc}"}
                action = info.get("action") or "run"
                base["preflight_action"] = action
                base["preflight_reason"] = info.get("reason") or ""
                if info.get("analysis_strategy"):
                    base["analysis_strategy"] = info["analysis_strategy"]
                if action != "run":
                    base["status"] = "skipped"
                    base["enrichment_status"] = "skipped"
                    base["comparison_status"] = info.get("comparison_status") or "not_run"
                    base["error"] = info.get("reason") or action
                    skipped_count += 1
            items.append(base)

        job = {
            "id": job_id,
            "session_id": session_id,
            "type": "unified_stage_comparison",
            "scope": scope,
            "pair_id": pair_id,
            "force_enrichment": bool(force_enrichment),
            "force_compare": bool(force_compare),
            "force_fallback": bool(force_fallback),
            "skip_ineligible": bool(skip_ineligible),
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "items": items,
            "progress": {
                "total": len(items),
                "done": 0,
                "failed": 0,
                "skipped": skipped_count,
            },
            "confirm": bool(confirm),
        }
        if not confirm:
            job["status"] = "rejected_no_confirm"
            job["updated_at"] = _utc_now()
        elif skip_ineligible and skipped_count == len(items) and items:
            # Все пары отфильтрованы (все done / too_large / not_ready) →
            # job моментально завершён, нечего запускать.
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
        if job.get("status") in (
            "done", "failed", "cancelled", "rejected_no_confirm",
            "failed_interrupted",
        ):
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
    force_fb = bool(job.get("force_fallback"))
    items = list(job.get("items") or [])
    for idx, item in enumerate(items):
        latest = _read_job(session_id, job_id)
        if latest and latest.get("status") == "cancelled":
            return latest
        if item.get("status") == "skipped":
            # Pre-flight уже пометил пару как not_ready / too_large /
            # done. Не запускаем модели.
            continue
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
                force_fallback=force_fb,
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

        # Каждую успешную пару — обновляем агрегаты, чтобы вкладка
        # «Расхождения» видела свежие данные ещё до завершения job.
        # rebuild дешёвый: пробегает по pair-папкам и пишет JSON.
        if res.status == "done":
            try:
                uf_mod.rebuild_unified_findings(session_id)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "unified_job: per-pair rebuild_unified_findings failed sid=%s pid=%s",
                    session_id, pid,
                )
            try:
                ug_mod.build_unified_grouped(
                    session_id, force=True, persist=True,
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "unified_job: per-pair build_unified_grouped failed sid=%s pid=%s",
                    session_id, pid,
                )

    latest = _read_job(session_id, job_id)
    if latest and latest.get("status") == "cancelled":
        return latest

    job["status"] = "done"
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    # Финальный rebuild — гарантия что результат отражает все обработанные
    # пары (даже если последняя пара была skipped и per-pair rebuild не
    # сработал).
    try:
        uf_mod.rebuild_unified_findings(session_id)
    except Exception:  # noqa: BLE001
        logger.exception("unified_job: rebuild_unified_findings failed session=%s", session_id)
    try:
        ug_mod.build_unified_grouped(
            session_id, force=True, persist=True,
        )
    except Exception:  # noqa: BLE001
        logger.exception("unified_job: build_unified_grouped failed session=%s", session_id)

    return job


def aggregate_job_progress(job: dict) -> dict:
    """Live-агрегаты прогресса unified-job (для UI на этапе 1).

    Не запускает моделей. Считает по items: done/failed/skipped/queued/running,
    текущую пару (первый non-terminal item), changes по завершённым,
    total/elapsed/avg duration.
    """
    items = list((job or {}).get("items") or [])
    total = len(items)
    counts = {
        "done": 0,
        "failed": 0,
        "skipped": 0,
        "queued": 0,
        "running": 0,
        "cancelled": 0,
        "too_large": 0,
        "not_ready": 0,
    }
    total_changes = 0
    duration_sum = 0.0
    duration_n = 0
    current_pair_id: Optional[str] = None
    current_status: Optional[str] = None
    current_idx_human: int = 0
    finished_with_progress: int = 0
    for idx, it in enumerate(items):
        s = (it.get("status") or "").lower()
        if s in counts:
            counts[s] += 1
        # enriching/comparing — это активные подсостояния running; объединяем.
        elif s in ("enriching", "comparing"):
            counts["running"] += 1
        if s == "skipped":
            reason = (it.get("preflight_action") or it.get("error") or "")
            if "too_large" in reason:
                counts["too_large"] += 1
            elif "not_ready" in reason or "missing" in reason:
                counts["not_ready"] += 1
        if s in ("done", "failed", "skipped", "cancelled"):
            finished_with_progress += 1
            if it.get("duration_sec"):
                duration_sum += float(it.get("duration_sec") or 0.0)
                duration_n += 1
            total_changes += int(it.get("changes_count") or 0)
        elif s in ("running", "enriching", "comparing", "queued"):
            if current_pair_id is None and s != "queued":
                current_pair_id = it.get("pair_id")
                current_status = s
                current_idx_human = idx + 1
            elif current_pair_id is None and s == "queued":
                # первый queued — это «следующая на очереди»
                current_pair_id = it.get("pair_id")
                current_status = s
                current_idx_human = idx + 1

    avg = (duration_sum / duration_n) if duration_n else 0.0
    remaining_pairs = total - finished_with_progress
    eta_sec = int(avg * remaining_pairs) if avg and remaining_pairs > 0 else 0

    return {
        "total_pairs": total,
        "done": counts["done"],
        "failed": counts["failed"],
        "skipped": counts["skipped"],
        "queued": counts["queued"],
        "running": counts["running"],
        "cancelled": counts["cancelled"],
        "skipped_too_large": counts["too_large"],
        "skipped_not_ready": counts["not_ready"],
        "total_changes": total_changes,
        "avg_duration_sec": round(avg, 3),
        "duration_sum_sec": round(duration_sum, 3),
        "current_pair_id": current_pair_id,
        "current_pair_status": current_status,
        "current_pair_index": current_idx_human,
        "eta_sec": eta_sec,
    }


def get_job_with_progress(session_id: str, job_id: str) -> Optional[dict]:
    job = _read_job(session_id, job_id)
    if job is None:
        return None
    job = dict(job)
    job["aggregate"] = aggregate_job_progress(job)
    return job


def find_active_session_job(session_id: str) -> Optional[dict]:
    """Найти самую релевантную unified-job сессии для UI resume.

    Возвращает running/queued, иначе самую свежую завершённую. None если
    ничего нет. Приоритет session-scope над pair-scope (среди active И среди
    completed) — как у md_enrichment_jobs.find_active_session_job.
    """
    jobs = list_unified_jobs(session_id)
    if not jobs:
        return None

    def _is_session_scoped(j: dict) -> bool:
        scope = (j.get("scope") or "").lower()
        return scope in ("session", "selected", "")

    active = [j for j in jobs if j.get("status") in ("queued", "running")]
    if active:
        sess_active = [j for j in active if _is_session_scoped(j)]
        choice = sess_active[0] if sess_active else active[0]
        return get_job_with_progress(session_id, choice["id"])

    sess_jobs = [j for j in jobs if _is_session_scoped(j)]
    choice = sess_jobs[0] if sess_jobs else jobs[0]
    return get_job_with_progress(session_id, choice["id"])


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
    "get_job_with_progress",
    "find_active_session_job",
    "aggregate_job_progress",
    "preflight_session_for_batch",
    "cancel_job",
    "list_unified_jobs",
    "run_unified_job",
    "start_job_in_background",
]
