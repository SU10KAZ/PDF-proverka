# -*- coding: utf-8 -*-
"""Recompute-only job/service-слой для visual_block_equivalence (Stage 3A).

Готовит job-обвязку для будущего запуска
:func:`visual_block_equivalence.run_pair_visual_block_equivalence` по одной
паре / списку пар / всей сессии. На Stage 3A слой:

  * НЕ зарегистрирован ни в каком router (FastAPI не импортируется);
  * НЕ подключён к ``md_enrichment_jobs`` / observe-hook / ``pipeline_queue``;
  * НЕ делает реального skip Qwen/MD/Opus (это mark-only прекчек Stage 2);
  * по умолчанию НИЧЕГО не пишет на диск (``persist_to_disk=False``) — registry
    in-memory; персист в ``comparison/sessions/<sid>/jobs/`` опционален и
    включается явным флагом (для будущих endpoint'ов).

Конвенции используют общую job-модель (fail-soft per-pair,
async-обёртка + background scheduler), но registry здесь **in-memory**
(``_JOBS``), а тяжёлый per-pair runner **инъектируется** (``runner_fn``) —
тесты прогоняют job без store/PDF/cv2/Qwen/Opus.

Поток статусов job: ``queued → running → completed|failed|cancelled``.
Поток статусов пары: ``pending → running → completed|failed|cancelled``.
"""
from __future__ import annotations

import copy
import logging
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from .visual_block_equivalence import (
    VisualBlockEquivalenceConfig,
    run_pair_visual_block_equivalence,
)

logger = logging.getLogger(__name__)

_JOB_PREFIX = "vbej_"
JOB_TYPE = "visual_block_equivalence"

# In-memory registry — источник истины (по требованию Stage 3A).
_JOBS: dict[str, dict] = {}
_LOCK = threading.RLock()
# Трекинг background asyncio-тасков (для будущих endpoint'ов + cancel).
_active_tasks: dict[str, Any] = {}

# Статусы
JOB_QUEUED = "queued"
JOB_RUNNING = "running"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"
JOB_CANCELLED = "cancelled"
_JOB_FINISHED = {JOB_COMPLETED, JOB_FAILED, JOB_CANCELLED}

PAIR_PENDING = "pending"
PAIR_RUNNING = "running"
PAIR_COMPLETED = "completed"
PAIR_FAILED = "failed"
PAIR_CANCELLED = "cancelled"

# Ключи summary, агрегируемые по всем парам (совпадают с per-pair summary Stage 2).
_SUMMARY_KEYS = (
    "links_total",
    "links_compared",
    "identical_visual",
    "minor_render_noise",
    "changed_visual",
    "uncertain",
    "render_failed",
    "skipped",
    "potential_qwen_saved",
    "potential_opus_blocks_removed",
)


# ═══════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


@dataclass
class VisualBlockEquivalenceJobsConfig:
    """Конфиг job-слоя. По умолчанию OFF / неавтоматический запуск.

    ``enabled`` — информационный флаг для будущих endpoint'ов (Stage 3B);
    сами функции job-слоя работают при прямом вызове независимо от него
    (его никто не проверяет на этом этапе, к активному API он не подключён).

    ``persist_to_disk`` — писать ли job JSON в
    ``comparison/sessions/<sid>/jobs/<job_id>.json``. По умолчанию ВЫКЛ →
    registry полностью in-memory, никаких файлов не создаётся.
    """

    enabled: bool = False
    persist_to_disk: bool = False

    @classmethod
    def from_env(cls) -> "VisualBlockEquivalenceJobsConfig":
        return cls(
            enabled=_env_flag("STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_JOBS_ENABLED", False),
            persist_to_disk=_env_flag(
                "STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_JOBS_PERSIST", False),
        )


# Тип per-pair runner: (session_id, pair_id, *, cfg, write_artifact, write_debug,
# generated_at) -> report dict (как у run_pair_visual_block_equivalence).
RunnerFn = Callable[..., dict]


# ═══════════════════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════════════════


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_job_id() -> str:
    return f"{_JOB_PREFIX}{uuid.uuid4().hex[:16]}"


def _empty_summary() -> dict:
    return {k: 0 for k in _SUMMARY_KEYS}


def _resolve_pair_ids(session_id: str, scope: str,
                      pair_ids: Optional[list[str]]) -> list[str]:
    """Развернуть scope в конкретный список pair_id.

    * ``pair``     — ровно один pair_id;
    * ``selected`` — переданный список pair_ids (дедуп, порядок сохраняется);
    * ``session``  — все пары сессии (через ``store.get_session``).
    """
    scope = (scope or "selected").lower()
    ids = [str(p) for p in (pair_ids or []) if p]
    if scope == "pair":
        if len(ids) != 1:
            raise ValueError("scope=pair requires exactly one pair_id")
        return ids
    if scope == "selected":
        if not ids:
            raise ValueError("scope=selected requires non-empty pair_ids")
        return list(dict.fromkeys(ids))
    if scope == "session":
        return _session_pair_ids(session_id)
    raise ValueError(f"unknown scope: {scope!r}")


def _session_pair_ids(session_id: str) -> list[str]:
    """Список pair_id всех пар сессии (lazy import store, fail-soft)."""
    try:
        from . import store as store_mod
        session = store_mod.get_session(session_id)
    except Exception as exc:  # noqa: BLE001
        raise KeyError("session_not_found") from exc
    if session is None:
        raise KeyError("session_not_found")
    return [str(p["id"]) for p in (session.get("pairs") or []) if p.get("id")]


def _maybe_persist(job: dict, jobs_cfg: VisualBlockEquivalenceJobsConfig) -> None:
    """Опциональный персист job на диск (default OFF → нет файлов).

    Путь через ``paths.job_json_path`` (учитывает COMPARISON_ROOT)."""
    if not jobs_cfg.persist_to_disk:
        return
    try:
        import json
        from . import paths as paths_mod
        p = paths_mod.job_json_path(job["session_id"], job["job_id"])
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(job, f, ensure_ascii=False, indent=2)
        os.replace(tmp, p)
    except Exception as exc:  # noqa: BLE001 — персист не критичен
        logger.warning("visual_block_equivalence_jobs: persist failed %s: %s",
                       job.get("job_id"), exc)


def _aggregate_summary(job: dict) -> None:
    agg = _empty_summary()
    for it in job.get("pairs") or []:
        psum = it.get("summary") or {}
        for k in _SUMMARY_KEYS:
            v = psum.get(k)
            if isinstance(v, (int, float)):
                agg[k] += int(v)
    job["summary"] = agg


# ═══════════════════════════════════════════════════════════════════════════
# create / get / list / cancel / cleanup
# ═══════════════════════════════════════════════════════════════════════════


def create_visual_block_equivalence_job(
    session_id: str,
    *,
    scope: str = "selected",
    pair_ids: Optional[list[str]] = None,
    cfg: Optional[VisualBlockEquivalenceConfig] = None,
    jobs_cfg: Optional[VisualBlockEquivalenceJobsConfig] = None,
    write_artifact: bool = True,
    write_debug: bool = False,
) -> dict:
    """Создать job (status=queued), зарегистрировать в in-memory registry.

    НЕ запускает выполнение — это делает :func:`run_visual_block_equivalence_job`
    (или :func:`start_visual_block_equivalence_job`).
    """
    jobs_cfg = jobs_cfg or VisualBlockEquivalenceJobsConfig.from_env()
    cfg = cfg or VisualBlockEquivalenceConfig.from_env()
    resolved = _resolve_pair_ids(session_id, scope, pair_ids)

    job_id = _new_job_id()
    now = _utc_now()
    job = {
        "job_id": job_id,
        "type": JOB_TYPE,
        "session_id": session_id,
        "scope": (scope or "selected").lower(),
        "status": JOB_QUEUED,
        "created_at": now,
        "started_at": None,
        "finished_at": None,
        "total_pairs": len(resolved),
        "processed_pairs": 0,
        "failed_pairs": 0,
        "cancel_requested": False,
        "enabled": jobs_cfg.enabled,
        "enforced": False,            # Stage 2/3A: реального skip нет
        "persist_to_disk": jobs_cfg.persist_to_disk,
        "write_artifact": bool(write_artifact),
        "write_debug": bool(write_debug),
        "pairs": [
            {"pair_id": pid, "status": PAIR_PENDING, "summary": {},
             "artifact_path": None, "error": None}
            for pid in resolved
        ],
        "summary": _empty_summary(),
        "warnings": [],
        "updated_at": now,
    }
    # Конфиг хранится отдельно от persisted-job (не сериализуется в JSON).
    with _LOCK:
        _JOBS[job_id] = job
        _JOB_CFG[job_id] = (cfg, jobs_cfg)
    _maybe_persist(job, jobs_cfg)
    return copy.deepcopy(job)


# cfg per job (не часть persisted JSON-модели)
_JOB_CFG: dict[str, tuple[VisualBlockEquivalenceConfig, VisualBlockEquivalenceJobsConfig]] = {}


def get_visual_block_equivalence_job(job_id: str) -> Optional[dict]:
    """Снимок job (deepcopy) или None."""
    with _LOCK:
        job = _JOBS.get(job_id)
        return copy.deepcopy(job) if job is not None else None


def list_visual_block_equivalence_jobs(session_id: Optional[str] = None) -> list[dict]:
    """Список job'ов (deepcopy), опционально отфильтрованных по сессии.

    Сортировка по ``created_at`` по убыванию (свежие первыми)."""
    with _LOCK:
        jobs = [copy.deepcopy(j) for j in _JOBS.values()
                if session_id is None or j.get("session_id") == session_id]
    jobs.sort(key=lambda j: str(j.get("created_at") or ""), reverse=True)
    return jobs


def cancel_visual_block_equivalence_job(job_id: str) -> Optional[dict]:
    """Запросить отмену job.

    * выставляет ``cancel_requested=True``;
    * если job ещё не стартовал (queued) — сразу переводит в cancelled и
      помечает пары cancelled;
    * если выполняется — running-цикл увидит флаг и остановит дальнейшие пары;
    * отменяет background asyncio-таск, если он есть.
    """
    with _LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return None
        if job.get("status") in _JOB_FINISHED:
            return copy.deepcopy(job)
        job["cancel_requested"] = True
        job["updated_at"] = _utc_now()
        if job.get("status") == JOB_QUEUED:
            job["status"] = JOB_CANCELLED
            job["finished_at"] = _utc_now()
            for it in job.get("pairs") or []:
                if it.get("status") in (PAIR_PENDING, PAIR_RUNNING):
                    it["status"] = PAIR_CANCELLED
        jobs_cfg = _JOB_CFG.get(job_id, (None, None))[1] or VisualBlockEquivalenceJobsConfig()
        snapshot = copy.deepcopy(job)
        task = _active_tasks.get(job_id)
    _maybe_persist(snapshot, jobs_cfg)
    try:
        if task is not None and not task.done():
            task.cancel()
    except Exception:  # noqa: BLE001
        pass
    return snapshot


def cleanup_finished_jobs(*, session_id: Optional[str] = None,
                          keep_last: int = 50) -> int:
    """Удалить из in-memory registry завершённые job'ы сверх ``keep_last``
    (на сессию, если задан session_id, иначе глобально). Возвращает число
    удалённых. Live-данные не трогаются (это in-memory hygiene)."""
    removed = 0
    with _LOCK:
        finished = [
            (jid, j) for jid, j in _JOBS.items()
            if j.get("status") in _JOB_FINISHED
            and (session_id is None or j.get("session_id") == session_id)
        ]
        finished.sort(key=lambda kv: str(kv[1].get("finished_at")
                                         or kv[1].get("created_at") or ""))
        excess = max(0, len(finished) - max(0, keep_last))
        for jid, _job in finished[:excess]:
            _JOBS.pop(jid, None)
            _JOB_CFG.pop(jid, None)
            removed += 1
    return removed


# ═══════════════════════════════════════════════════════════════════════════
# run (sync) — тестируется напрямую без сервера
# ═══════════════════════════════════════════════════════════════════════════


def _start_running(job_id: str) -> Optional[dict]:
    """Перевести job в running (если можно). Возвращает рабочий dict из
    registry (НЕ копию) или None, если запускать нельзя."""
    with _LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return None
        if job.get("status") in _JOB_FINISHED:
            return None
        if job.get("cancel_requested"):
            # отмена до старта
            job["status"] = JOB_CANCELLED
            job["finished_at"] = _utc_now()
            for it in job.get("pairs") or []:
                if it.get("status") in (PAIR_PENDING, PAIR_RUNNING):
                    it["status"] = PAIR_CANCELLED
            job["updated_at"] = _utc_now()
            return None
        job["status"] = JOB_RUNNING
        job["started_at"] = job.get("started_at") or _utc_now()
        job["updated_at"] = _utc_now()
        return job


def _jobs_cfg_for(job_id: str) -> VisualBlockEquivalenceJobsConfig:
    return _JOB_CFG.get(job_id, (None, None))[1] or VisualBlockEquivalenceJobsConfig()


def _cfg_for(job_id: str) -> VisualBlockEquivalenceConfig:
    return _JOB_CFG.get(job_id, (None, None))[0] or VisualBlockEquivalenceConfig()


def _is_cancel_requested(job_id: str) -> bool:
    with _LOCK:
        job = _JOBS.get(job_id)
        return bool(job and job.get("cancel_requested"))


def _apply_pair_result(job: dict, item: dict, *, report: Optional[dict],
                       error: Optional[str], session_id: str) -> None:
    """Записать результат одной пары в job (под предположением, что вызывающий
    держит _LOCK на время мутации общего dict)."""
    if error is not None:
        item["status"] = PAIR_FAILED
        item["error"] = error
        job["failed_pairs"] = int(job.get("failed_pairs", 0)) + 1
    else:
        item["status"] = PAIR_COMPLETED
        item["summary"] = (report or {}).get("summary") or {}
        if job.get("write_artifact"):
            item["artifact_path"] = _artifact_path_str(session_id, item["pair_id"])
    job["processed_pairs"] = int(job.get("processed_pairs", 0)) + 1
    job["updated_at"] = _utc_now()


def _artifact_path_str(session_id: str, pair_id: str) -> Optional[str]:
    try:
        from . import paths as paths_mod
        return str(paths_mod.visual_block_equivalence_report_path(session_id, pair_id))
    except Exception:  # noqa: BLE001
        return None


def _finalize(job: dict) -> None:
    """Финализировать статус + агрегировать summary (под _LOCK у вызывающего)."""
    _aggregate_summary(job)
    if job.get("cancel_requested"):
        job["status"] = JOB_CANCELLED
    elif job.get("status") not in _JOB_FINISHED:
        job["status"] = JOB_COMPLETED
    job["finished_at"] = job.get("finished_at") or _utc_now()
    job["updated_at"] = _utc_now()


def _call_runner(runner_fn: RunnerFn, session_id: str, pair_id: str, *,
                 cfg: VisualBlockEquivalenceConfig, job: dict) -> dict:
    return runner_fn(
        session_id, pair_id,
        cfg=cfg,
        write_artifact=bool(job.get("write_artifact")),
        write_debug=bool(job.get("write_debug")),
    )


def run_visual_block_equivalence_job(
    job_id: str,
    *,
    runner_fn: Optional[RunnerFn] = None,
) -> Optional[dict]:
    """Синхронно выполнить job: пройти по парам, вызвать runner для каждой,
    fail-soft, проверять отмену между парами, агрегировать summary.

    ``runner_fn`` по умолчанию ``run_pair_visual_block_equivalence``; тесты
    инъектируют mock, чтобы не трогать store/PDF/cv2/Qwen/Opus.

    Возвращает снимок завершённого job (deepcopy) или None, если job не найден.
    """
    runner_fn = runner_fn or run_pair_visual_block_equivalence
    job = _start_running(job_id)
    if job is None:
        return get_visual_block_equivalence_job(job_id)

    session_id = job["session_id"]
    cfg = _cfg_for(job_id)
    jobs_cfg = _jobs_cfg_for(job_id)
    _maybe_persist(copy.deepcopy(job), jobs_cfg)

    try:
        for item in job["pairs"]:
            if _is_cancel_requested(job_id):
                with _LOCK:
                    if item.get("status") in (PAIR_PENDING, PAIR_RUNNING):
                        item["status"] = PAIR_CANCELLED
                continue  # помечаем остаток cancelled, не вызываем runner
            with _LOCK:
                item["status"] = PAIR_RUNNING
                job["updated_at"] = _utc_now()
            report = None
            error = None
            try:
                report = _call_runner(runner_fn, session_id, item["pair_id"],
                                      cfg=cfg, job=job)
            except Exception as exc:  # noqa: BLE001 — одна пара не валит job
                error = f"{type(exc).__name__}: {exc}"
                logger.warning("visual_block_equivalence_jobs: pair %s failed: %s",
                               item.get("pair_id"), exc)
            with _LOCK:
                _apply_pair_result(job, item, report=report, error=error,
                                  session_id=session_id)
            _maybe_persist(copy.deepcopy(job), jobs_cfg)
    except Exception as exc:  # noqa: BLE001 — неожиданный сбой верхнего уровня
        with _LOCK:
            job["status"] = JOB_FAILED
            job["error"] = f"{type(exc).__name__}: {exc}"
        logger.exception("visual_block_equivalence_jobs: job %s failed", job_id)
    finally:
        with _LOCK:
            _finalize(job)
            snapshot = copy.deepcopy(job)
        _maybe_persist(snapshot, jobs_cfg)
    return snapshot


def start_visual_block_equivalence_job(
    session_id: str,
    *,
    scope: str = "selected",
    pair_ids: Optional[list[str]] = None,
    cfg: Optional[VisualBlockEquivalenceConfig] = None,
    jobs_cfg: Optional[VisualBlockEquivalenceJobsConfig] = None,
    write_artifact: bool = True,
    write_debug: bool = False,
    runner_fn: Optional[RunnerFn] = None,
    run_in_background: bool = False,
) -> dict:
    """Создать и запустить job.

    ``run_in_background=False`` (default) — выполнить синхронно и вернуть
    завершённый job (удобно для тестов/CLI, не нужен event loop).

    ``run_in_background=True`` — запланировать asyncio-таск (для будущих
    endpoint'ов Stage 3B; требует работающий event loop) и вернуть job в
    статусе queued/running.
    """
    job = create_visual_block_equivalence_job(
        session_id, scope=scope, pair_ids=pair_ids, cfg=cfg, jobs_cfg=jobs_cfg,
        write_artifact=write_artifact, write_debug=write_debug)
    job_id = job["job_id"]
    if run_in_background:
        start_job_in_background(job_id, runner_fn=runner_fn)
        return get_visual_block_equivalence_job(job_id) or job
    return run_visual_block_equivalence_job(job_id, runner_fn=runner_fn) or job


# ═══════════════════════════════════════════════════════════════════════════
# async / background (подготовлено для Stage 3B endpoint'ов; не используется в 3A)
# ═══════════════════════════════════════════════════════════════════════════


async def run_visual_block_equivalence_job_async(
    job_id: str,
    *,
    runner_fn: Optional[RunnerFn] = None,
) -> Optional[dict]:
    """Асинхронная обёртка: каждый per-pair runner уводится в
    ``asyncio.to_thread`` (рендер/cv2 — CPU-bound). Логика идентична
    синхронной версии. Готово для подключения из endpoint'а на Stage 3B."""
    import asyncio

    runner_fn = runner_fn or run_pair_visual_block_equivalence
    job = _start_running(job_id)
    if job is None:
        return get_visual_block_equivalence_job(job_id)

    session_id = job["session_id"]
    cfg = _cfg_for(job_id)
    jobs_cfg = _jobs_cfg_for(job_id)
    _maybe_persist(copy.deepcopy(job), jobs_cfg)

    try:
        for item in job["pairs"]:
            if _is_cancel_requested(job_id):
                with _LOCK:
                    if item.get("status") in (PAIR_PENDING, PAIR_RUNNING):
                        item["status"] = PAIR_CANCELLED
                continue
            with _LOCK:
                item["status"] = PAIR_RUNNING
                job["updated_at"] = _utc_now()
            report = None
            error = None
            try:
                report = await asyncio.to_thread(
                    _call_runner, runner_fn, session_id, item["pair_id"],
                    cfg=cfg, job=job)
            except asyncio.CancelledError:
                with _LOCK:
                    item["status"] = PAIR_CANCELLED
                    job["cancel_requested"] = True
                raise
            except Exception as exc:  # noqa: BLE001
                error = f"{type(exc).__name__}: {exc}"
                logger.warning("visual_block_equivalence_jobs(async): pair %s failed: %s",
                               item.get("pair_id"), exc)
            with _LOCK:
                _apply_pair_result(job, item, report=report, error=error,
                                  session_id=session_id)
            _maybe_persist(copy.deepcopy(job), jobs_cfg)
    except asyncio.CancelledError:
        with _LOCK:
            job["status"] = JOB_CANCELLED
            for it in job["pairs"]:
                if it.get("status") in (PAIR_PENDING, PAIR_RUNNING):
                    it["status"] = PAIR_CANCELLED
    except Exception as exc:  # noqa: BLE001
        with _LOCK:
            job["status"] = JOB_FAILED
            job["error"] = f"{type(exc).__name__}: {exc}"
        logger.exception("visual_block_equivalence_jobs(async): job %s failed", job_id)
    finally:
        with _LOCK:
            _finalize(job)
            snapshot = copy.deepcopy(job)
        _maybe_persist(snapshot, jobs_cfg)
    return snapshot


def start_job_in_background(job_id: str, *,
                            runner_fn: Optional[RunnerFn] = None) -> str:
    """Запланировать background asyncio-таск (Stage 3B). Требует event loop."""
    import asyncio

    loop = asyncio.get_event_loop()
    task = loop.create_task(run_visual_block_equivalence_job_async(job_id, runner_fn=runner_fn))
    with _LOCK:
        _active_tasks[job_id] = task

    def _cleanup(_t: Any) -> None:
        with _LOCK:
            _active_tasks.pop(job_id, None)

    task.add_done_callback(_cleanup)
    return job_id


# ═══════════════════════════════════════════════════════════════════════════
# test-only registry reset (НЕ для production; чистит in-memory state)
# ═══════════════════════════════════════════════════════════════════════════


def _reset_registry_for_tests() -> None:
    with _LOCK:
        _JOBS.clear()
        _JOB_CFG.clear()
        _active_tasks.clear()


__all__ = [
    "VisualBlockEquivalenceJobsConfig",
    "JOB_TYPE",
    "create_visual_block_equivalence_job",
    "start_visual_block_equivalence_job",
    "run_visual_block_equivalence_job",
    "run_visual_block_equivalence_job_async",
    "start_job_in_background",
    "get_visual_block_equivalence_job",
    "list_visual_block_equivalence_jobs",
    "cancel_visual_block_equivalence_job",
    "cleanup_finished_jobs",
    # status constants
    "JOB_QUEUED", "JOB_RUNNING", "JOB_COMPLETED", "JOB_FAILED", "JOB_CANCELLED",
    "PAIR_PENDING", "PAIR_RUNNING", "PAIR_COMPLETED", "PAIR_FAILED", "PAIR_CANCELLED",
]
