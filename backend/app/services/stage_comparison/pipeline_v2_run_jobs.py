# -*- coding: utf-8 -*-
"""Pipeline V2 — controlled operator-triggered run jobs (UI «Запустить V2»).

State-changing слой ПОВЕРХ существующего runner'а
:func:`pipeline_v2_dry_run.run_pipeline_v2_dry_run`. НЕ дублирует pipeline-логику:
этот модуль только оркестрирует запуск (job lifecycle, lock, backup, manifest,
safety gates), а сам анализ делает уже существующий dry-run runner.

Инфраструктура job'ов зеркалит :mod:`md_enrichment_jobs`:

* job персистится в ``comparison/sessions/<sid>/jobs/<job_id>.json``;
* фоновая asyncio.Task трекается в ``_active_tasks[sid][job_id]``;
* CPU/IO-bound runner уводится в ``asyncio.to_thread`` (не блокирует event loop);
* умерший после рестарта job помечается ``failed_interrupted`` (вечного lock'а нет).

Безопасность:

* **offline по умолчанию** — runner вызывается с ``llm_runner=None,
  vision_runner=None``, поэтому Qwen/Gemma/Opus/Claude НЕ задействуются
  (delta_explanation / graphic_vision → ``skipped_no_runner``). Это
  фиксируется в manifest'е ``models_touched`` (все false);
* пишет ТОЛЬКО артефакты pipeline_v2 указанной пары + job-статус + manifest +
  (при rerun) backup; не трогает другие пары, controlled state, findings,
  diagnostics;
* read-only ``ui-payload`` сервис остаётся read-only — здесь отдельный слой.
"""
from __future__ import annotations

import asyncio
import json
import logging
import shutil
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import paths as paths_mod
from . import store as store_mod
from . import pipeline_v2_payload_service as payload_mod
from . import pipeline_v2_dry_run as dry_run_mod

logger = logging.getLogger(__name__)

JOB_TYPE = "pipeline_v2_run"
RUN_MANIFEST_PREFIX = "ui_run_manifest_"
BACKUP_LABEL = "ui_run"

# Инжектируемая ссылка на runner — тесты её monkeypatch'ят, чтобы НЕ гонять
# реальный pipeline/модели.
run_pipeline_v2_dry_run = dry_run_mod.run_pipeline_v2_dry_run

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"
STATUS_FAILED_INTERRUPTED = "failed_interrupted"
_TERMINAL = (STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED,
             STATUS_FAILED_INTERRUPTED)
_NON_TERMINAL = (STATUS_QUEUED, STATUS_RUNNING)

_STALE_QUEUED_GRACE_SECONDS = 60

_lock = threading.RLock()
_active_tasks: dict[str, dict[str, "asyncio.Task[Any]"]] = {}


# ─── exceptions (router маппит в HTTP-коды) ──────────────────────────────

class PipelineV2RunError(Exception):
    """База: ошибка запуска controlled Pipeline V2 run."""


class PipelineV2RunConfirmError(PipelineV2RunError):
    """Не пройдены confirm-гейты (→ 422)."""


class PipelineV2RunNotFound(PipelineV2RunError):
    """Сессия/пара не найдены (→ 404)."""


class PipelineV2RunConflict(PipelineV2RunError):
    """Артефакты уже есть без rerun, либо уже идёт run на эту пару (→ 409)."""


# ─── time / id helpers ───────────────────────────────────────────────────

def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _parse_iso(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc)
    except ValueError:
        return None


def _new_job_id() -> str:
    return f"pv2run_{uuid.uuid4().hex[:12]}"


def _duration_sec(started: Any, finished: Any) -> Optional[int]:
    s = _parse_iso(started)
    f = _parse_iso(finished)
    if s is None or f is None:
        return None
    return max(0, int((f - s).total_seconds()))


# ─── job persistence ─────────────────────────────────────────────────────

def _read_job(session_id: str, job_id: str) -> Optional[dict]:
    try:
        p = paths_mod.job_json_path(session_id, job_id)
    except ValueError:
        return None
    if not p.is_file():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _write_job(session_id: str, job: dict) -> None:
    p = paths_mod.job_json_path(session_id, job["id"])
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _is_task_alive(session_id: str, job_id: str) -> bool:
    bucket = _active_tasks.get(session_id) or {}
    task = bucket.get(job_id)
    return bool(task and not task.done())


def _maybe_mark_interrupted(session_id: str, job: dict) -> dict:
    """Если job висит running/queued, но живой asyncio.Task нет (рестарт
    uvicorn / краш) — пометить ``failed_interrupted``. Это снимает «вечный
    lock» на пару."""
    status = job.get("status")
    if status not in _NON_TERMINAL:
        return job
    if _is_task_alive(session_id, job.get("id")):
        return job
    if status == STATUS_QUEUED:
        created = _parse_iso(job.get("created_at"))
        if created is not None:
            age = (datetime.now(timezone.utc) - created).total_seconds()
            if age < _STALE_QUEUED_GRACE_SECONDS:
                return job
    job["status"] = STATUS_FAILED_INTERRUPTED
    job["updated_at"] = _utc_now()
    if not job.get("finished_at"):
        job["finished_at"] = _utc_now()
    job.setdefault("error", "Backend перезапущен или поток прерван; "
                            "Pipeline V2 run не завершён.")
    try:
        _write_job(session_id, job)
    except OSError:
        pass
    return job


# ─── pair → packages ─────────────────────────────────────────────────────

def _side_package(side: Any) -> dict:
    """Из pair.json left/right собрать пакет для run_pipeline_v2_dry_run.

    pair.json хранит markdown в ключе ``md_path``; runner ждёт
    ``document_md_path`` — мапим явно.
    """
    s = side if isinstance(side, dict) else {}
    return {
        "pdf_path": s.get("pdf_path"),
        "result_json_path": s.get("result_json_path"),
        "document_md_path": s.get("document_md_path") or s.get("md_path"),
        "ocr_html_path": s.get("ocr_html_path"),
    }


def resolve_pair_packages(session_id: str, pair_id: str) -> tuple[dict, dict]:
    """Вернуть (left_package, right_package) для пары.

    Raises:
        PipelineV2RunNotFound: сессия или пара не найдены.
        PipelineV2RunError: у пары нет result_json (нечего анализировать).
    """
    if store_mod.get_session(session_id) is None:
        raise PipelineV2RunNotFound("session_not_found")
    pair = store_mod._find_pair_meta(session_id, pair_id)
    if not isinstance(pair, dict) or not pair.get("id"):
        raise PipelineV2RunNotFound("pair_not_found")
    left = _side_package(pair.get("left"))
    right = _side_package(pair.get("right"))
    if not left.get("result_json_path") or not right.get("result_json_path"):
        raise PipelineV2RunError("pair_missing_result_json")
    return left, right


# ─── artifacts / backup ──────────────────────────────────────────────────

def pair_has_artifacts(session_id: str, pair_id: str) -> bool:
    """Есть ли у пары готовые pipeline_v2 артефакты (summary / ui_payload)."""
    art = payload_mod.pipeline_v2_artifacts_dir(session_id, pair_id)
    return ((art / payload_mod.SUMMARY_FILENAME).is_file()
            or (art / payload_mod.UI_PAYLOAD_FILENAME).is_file())


def backup_existing_pipeline_v2(session_id: str, pair_id: str) -> Optional[str]:
    """Скопировать текущий pipeline_v2/ в
    ``pipeline_v2_backup_before_ui_run_<TS>`` (как в существующих runtime-writes).
    Возвращает путь к backup'у или None, если копировать нечего."""
    art = payload_mod.pipeline_v2_artifacts_dir(session_id, pair_id)
    if not art.is_dir() or not any(art.iterdir()):
        return None
    pair_root = paths_mod.pair_dir(session_id, pair_id)
    backup = pair_root / f"pipeline_v2_backup_before_{BACKUP_LABEL}_{_utc_stamp()}"
    shutil.copytree(art, backup)
    return str(backup)


# ─── validation + create ─────────────────────────────────────────────────

def validate_run_request(session_id: str, pair_id: str, payload: dict) -> dict:
    """Проверить confirm-гейты. Raises PipelineV2RunConfirmError при провале."""
    if not isinstance(payload, dict):
        raise PipelineV2RunConfirmError("payload_must_be_object")
    if payload.get("confirm") is not True:
        raise PipelineV2RunConfirmError("confirm_required")
    if payload.get("confirm_session_id") != session_id:
        raise PipelineV2RunConfirmError("confirm_session_id_mismatch")
    if payload.get("confirm_pair_id") != pair_id:
        raise PipelineV2RunConfirmError("confirm_pair_id_mismatch")
    mode = str(payload.get("mode") or "dry_run")
    if mode != "dry_run":
        raise PipelineV2RunConfirmError("unsupported_mode")
    return {
        "mode": mode,
        "rerun_existing": bool(payload.get("rerun_existing")),
        "create_backup": payload.get("create_backup", True) is not False,
        "operator_note": str(payload.get("operator_note") or "")[:500],
    }


def find_active_pair_job(session_id: str, pair_id: str) -> Optional[dict]:
    """Найти НЕ-терминальный job по этой паре (lock против двойного запуска).
    Мёртвые (после рестарта) джобы предварительно помечаются
    failed_interrupted, поэтому вечного lock'а нет."""
    try:
        jobs_root = paths_mod.jobs_root(session_id)
    except OSError:
        return None
    for jp in sorted(jobs_root.glob("pv2run_*.json")):
        try:
            data = json.loads(jp.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        if data.get("type") != JOB_TYPE or data.get("pair_id") != pair_id:
            continue
        if data.get("status") not in _NON_TERMINAL:
            continue
        data = _maybe_mark_interrupted(session_id, data)
        if data.get("status") in _NON_TERMINAL:
            return data
    return None


def create_run_job(session_id: str, pair_id: str, payload: dict) -> dict:
    """Создать (но НЕ запустить) controlled Pipeline V2 run job.

    Порядок гейтов: confirm → session/pair exist + packages → artifacts-exist
    (409 без rerun) → concurrency lock (409). Возвращает job (status=queued).
    """
    opts = validate_run_request(session_id, pair_id, payload)
    # session/pair существуют + есть что анализировать
    left, right = resolve_pair_packages(session_id, pair_id)
    with _lock:
        has_art = pair_has_artifacts(session_id, pair_id)
        if has_art and not opts["rerun_existing"]:
            raise PipelineV2RunConflict("artifacts_exist")
        if find_active_pair_job(session_id, pair_id) is not None:
            raise PipelineV2RunConflict("run_already_active")
        job_id = _new_job_id()
        art_dir = payload_mod.pipeline_v2_artifacts_dir(session_id, pair_id)
        job = {
            "id": job_id,
            "type": JOB_TYPE,
            "session_id": session_id,
            "pair_id": pair_id,
            "mode": opts["mode"],
            "status": STATUS_QUEUED,
            "trigger": "ui_pipeline_v2_run_button",
            "rerun_existing": opts["rerun_existing"],
            "create_backup": opts["create_backup"],
            "operator_note": opts["operator_note"],
            "had_artifacts": has_art,
            "created_backup": False,
            "backup_path": None,
            "output_dir": str(art_dir),
            "created_at": _utc_now(),
            "updated_at": _utc_now(),
            "started_at": None,
            "finished_at": None,
            "duration_sec": None,
            "error": None,
            "logs_tail": "",
            "artifacts_summary": None,
            "manifest_path": None,
            "models_touched": {"qwen": False, "gemma": False,
                               "opus": False, "claude": False},
        }
        _write_job(session_id, job)
    return job


# ─── run (background) ────────────────────────────────────────────────────

def _artifacts_summary(art_dir: Path, summary: Any) -> dict:
    files = []
    try:
        files = sorted(p.name for p in art_dir.glob("*.json"))
    except OSError:
        pass
    out: dict[str, Any] = {"artifact_files": files, "artifact_count": len(files)}
    if isinstance(summary, dict):
        out["runner_status"] = summary.get("status")
        warns = summary.get("warnings")
        if isinstance(warns, list):
            out["warning_count"] = len(warns)
    return out


def _write_manifest(art_dir: Path, job: dict, summary: Any) -> Optional[str]:
    from backend.app.core import config as _cfg  # локальный импорт: путь корня
    manifest = {
        "job_id": job["id"],
        "trigger": job.get("trigger"),
        "session_id": job["session_id"],
        "pair_id": job["pair_id"],
        "mode": job.get("mode"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "status": job.get("status"),
        "backend_base_dir": str(getattr(_cfg, "ROOT_DIR", "")),
        "comparison_root": str(paths_mod.comparison_root_path()),
        "pair_dir": str(paths_mod.pair_dir(job["session_id"], job["pair_id"])),
        "output_dir": str(art_dir),
        "created_backup": job.get("created_backup"),
        "backup_path": job.get("backup_path"),
        "runner": "run_pipeline_v2_dry_run",
        "models_touched": job.get("models_touched"),
        "error": job.get("error"),
        "operator_note": job.get("operator_note"),
        "runner_status": summary.get("status") if isinstance(summary, dict) else None,
    }
    try:
        path = art_dir / f"{RUN_MANIFEST_PREFIX}{job['id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                        encoding="utf-8")
        return str(path)
    except OSError as exc:
        logger.warning("pv2_run: manifest write failed: %s", exc)
        return None


def _run_sync(session_id: str, pair_id: str, job: dict) -> dict:
    """Синхронная часть прогона (уводится в asyncio.to_thread)."""
    art_dir = payload_mod.pipeline_v2_artifacts_dir(session_id, pair_id)
    if job.get("rerun_existing") and job.get("create_backup"):
        try:
            bp = backup_existing_pipeline_v2(session_id, pair_id)
            if bp:
                job["created_backup"] = True
                job["backup_path"] = bp
        except OSError as exc:
            raise PipelineV2RunError(f"backup_failed:{exc}") from exc
    left, right = resolve_pair_packages(session_id, pair_id)
    art_dir.mkdir(parents=True, exist_ok=True)
    # offline: llm_runner/vision_runner=None → модели НЕ вызываются
    summary = run_pipeline_v2_dry_run(left, right, art_dir,
                                      llm_runner=None, vision_runner=None)
    job["artifacts_summary"] = _artifacts_summary(art_dir, summary)
    runner_status = summary.get("status") if isinstance(summary, dict) else None
    if runner_status == "failed":
        job["error"] = (summary.get("error") if isinstance(summary, dict)
                        else None) or "runner_failed"
    return summary


async def run_pipeline_v2_run_job(session_id: str, job_id: str) -> dict:
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") not in _NON_TERMINAL:
        return job
    if job.get("status") == STATUS_RUNNING and _is_task_alive(session_id, job_id):
        return job
    job["status"] = STATUS_RUNNING
    job["started_at"] = job.get("started_at") or _utc_now()
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    summary: Any = None
    try:
        summary = await asyncio.to_thread(_run_sync, session_id, pair_id_of(job), job)
        runner_status = summary.get("status") if isinstance(summary, dict) else None
        job["status"] = STATUS_FAILED if runner_status == "failed" else STATUS_COMPLETED
    except Exception as exc:  # noqa: BLE001 — fail-soft, статус сохраняем
        logger.exception("pv2_run job %s failed", job_id)
        job["status"] = STATUS_FAILED
        job["error"] = str(exc)[:500]
    job["finished_at"] = _utc_now()
    job["duration_sec"] = _duration_sec(job.get("started_at"), job["finished_at"])
    job["updated_at"] = _utc_now()
    art_dir = payload_mod.pipeline_v2_artifacts_dir(session_id, pair_id_of(job))
    job["manifest_path"] = _write_manifest(art_dir, job, summary)
    _write_job(session_id, job)
    return job


def pair_id_of(job: dict) -> str:
    return str(job.get("pair_id") or "")


# ─── read / cancel / background start ────────────────────────────────────

def get_job(session_id: str, job_id: str) -> Optional[dict]:
    job = _read_job(session_id, job_id)
    if job is None:
        return None
    return _maybe_mark_interrupted(session_id, job)


def cancel_job(session_id: str, job_id: str) -> Optional[dict]:
    with _lock:
        job = _read_job(session_id, job_id)
        if job is None:
            return None
        if job.get("status") in _TERMINAL:
            return job
        job["status"] = STATUS_CANCELLED
        job["finished_at"] = job.get("finished_at") or _utc_now()
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)
        return job


def start_job_in_background(session_id: str, job_id: str) -> str:
    loop = asyncio.get_event_loop()
    task = loop.create_task(run_pipeline_v2_run_job(session_id, job_id))
    bucket = _active_tasks.setdefault(session_id, {})
    bucket[job_id] = task

    def _cleanup(_t: Any) -> None:
        try:
            bucket.pop(job_id, None)
        except KeyError:
            pass

    task.add_done_callback(_cleanup)
    return job_id


def status_url(session_id: str, pair_id: str, job_id: str) -> str:
    return (f"/api/stage-comparison/pipeline-v2/{session_id}"
            f"/pairs/{pair_id}/run-status/{job_id}")


__all__ = [
    "JOB_TYPE",
    "PipelineV2RunError",
    "PipelineV2RunConfirmError",
    "PipelineV2RunNotFound",
    "PipelineV2RunConflict",
    "validate_run_request",
    "resolve_pair_packages",
    "pair_has_artifacts",
    "backup_existing_pipeline_v2",
    "find_active_pair_job",
    "create_run_job",
    "run_pipeline_v2_run_job",
    "get_job",
    "cancel_job",
    "start_job_in_background",
    "status_url",
]
