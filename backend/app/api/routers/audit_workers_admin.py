"""Административный API «Аудит-воркеры»: `/api/workers/*`.

Контур оператора: обычная портальная cookie-сессия (PortalAuthMiddleware).
Токен воркера сюда доступа НЕ даёт — контуры разделены намеренно (§20.2).

Все опасные действия (одобрение, отзыв, ротация токена, выдача задания)
пишутся в сквозной журнал действий с kind="worker".
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse

from backend.app.core import action_log
from backend.app.models.distributed_workers import (
    ApproveRequest,
    CreateTestJobRequest,
    JobState,
)
from backend.app.services.distributed_workers import (
    database,
    event_service,
    job_service,
    progress_service,
    registration_service,
    repositories,
    worker_registry,
)
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersConfigError,
    get_settings,
)

router = APIRouter(prefix="/api/workers", tags=["audit-workers-admin"])
# Отдельный роутер: единственный эндпоинт, который обязан отвечать и при
# ВЫКЛЮЧЕННОЙ подсистеме — фронту нужно честно показать «функция отключена».
# Остальные маршруты при выключенном флаге не регистрируются вовсе (404).
status_router = APIRouter(prefix="/api/workers", tags=["audit-workers-admin"])


def _actor(request: Request) -> str:
    user = getattr(request.state, "portal_user", None)
    return f"operator:{user}" if user else "operator:anonymous"


def _settings_or_404():
    settings = get_settings()
    if not settings.enabled:
        raise HTTPException(
            status_code=404,
            detail="Подсистема распределённых воркеров отключена "
                   "(DISTRIBUTED_WORKERS_ENABLED=false).",
        )
    return settings


def _audit(request: Request, action: str, **extra: Any) -> None:
    try:
        action_log.log_event(
            "worker", event=action, actor=_actor(request).split(":", 1)[-1], **extra
        )
    except Exception:  # noqa: BLE001 — журнал не должен ронять действие
        pass


@status_router.get("/status")
async def subsystem_status() -> dict[str, Any]:
    """Состояние подсистемы. Единственный эндпоинт, работающий при выключенном флаге.

    Нужен фронту, чтобы честно показать «функция отключена», а не пустой экран.
    """
    settings = get_settings()
    if not settings.enabled:
        return {
            "enabled": False,
            "reason": "DISTRIBUTED_WORKERS_ENABLED=false",
            "message": "Распределённые audit-worker отключены.",
        }
    config_error: Optional[str] = None
    try:
        settings.require_bootstrap_secret()
    except DistributedWorkersConfigError as exc:
        config_error = str(exc)
    # Операторский контур мог не подняться из-за выключенной портальной
    # авторизации — экран должен сказать об этом прямо, а не показывать
    # пустые списки и загадочные 404.
    from backend.app.core import portal_auth as _portal_auth

    admin_available = (
        _portal_auth.get_settings().enabled or settings.allow_insecure_admin
    )
    if not admin_available and not config_error:
        config_error = (
            "Операторский API не поднят: PORTAL_AUTH_ENABLED=false, а своей "
            "аутентификации у него нет. Включите портальную защиту либо "
            "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN=true для локального пилота."
        )
    return {
        "enabled": True,
        "protocol_version": settings.protocol_version,
        "manifest_version": settings.manifest_version,
        "data_dir": str(settings.data_dir),
        "heartbeat_stale_sec": settings.heartbeat_stale_sec,
        "heartbeat_offline_sec": settings.heartbeat_offline_sec,
        "upload_chunk_bytes": settings.upload_chunk_bytes,
        "test_job_max_sec": settings.test_job_max_sec,
        "admin_api_available": admin_available,
        "config_error": config_error,
    }


# ─── Воркеры ─────────────────────────────────────────────────────────────────
@router.get("")
async def list_workers() -> dict[str, Any]:
    settings = _settings_or_404()
    rows = await database.run_db(
        worker_registry.refresh_connectivity, settings=settings
    )
    now = time.time()
    workers = [worker_registry.to_view(r, now=now) for r in rows]
    online = sum(1 for w in workers if w["connection_status"] == "online")
    free_slots = sum(w["calculated_free_slots"] for w in workers if w["connection_status"] == "online")
    return {
        "workers": workers,
        "summary": {
            "total": len(workers),
            "online": online,
            "free_slots": free_slots,
            "active_jobs": sum(len(w["active_jobs"]) for w in workers),
            # Заявки, ждущие решения оператора: их нельзя «не заметить» —
            # до одобрения воркер вообще не получает токен.
            "pending": sum(
                1 for w in workers if w["registration_status"] == "pending"
            ),
        },
        "server_time": now,
    }


@router.get("/{worker_id}")
async def get_worker(worker_id: str) -> dict[str, Any]:
    settings = _settings_or_404()
    row = await database.run_db(repositories.get_worker, worker_id, settings=settings)
    if row is None:
        raise HTTPException(status_code=404, detail="Воркер не найден.")
    jobs = await database.run_db(
        repositories.list_jobs, worker_id=worker_id, settings=settings
    )
    return {
        "worker": worker_registry.to_view(row),
        "jobs": [job_service.to_view(j, settings=settings) for j in jobs],
    }


@router.post("/{worker_id}/approve")
async def approve_worker(
    worker_id: str, payload: ApproveRequest, request: Request
) -> dict[str, Any]:
    settings = _settings_or_404()
    try:
        row = await database.run_db(
            registration_service.approve_worker,
            worker_id=worker_id,
            display_name=payload.display_name,
            configured_max_slots=payload.configured_max_slots,
            settings=settings,
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit(request, "worker_approved", worker_id=worker_id)
    return {"worker": worker_registry.to_view(row)}


@router.post("/{worker_id}/reject")
async def reject_worker(worker_id: str, request: Request) -> dict[str, Any]:
    """Отклонить заявку. Claim-secret обесценивается, токен не выдаётся."""
    settings = _settings_or_404()
    try:
        row = await database.run_db(
            registration_service.reject_worker, worker_id=worker_id, settings=settings
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit(request, "worker_rejected", worker_id=worker_id)
    return {"worker": worker_registry.to_view(row)}


@router.post("/{worker_id}/revoke")
async def revoke_worker(worker_id: str, request: Request) -> dict[str, Any]:
    settings = _settings_or_404()
    try:
        row = await database.run_db(
            registration_service.revoke_worker, worker_id=worker_id, settings=settings
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit(request, "worker_revoked", worker_id=worker_id)
    return {"worker": worker_registry.to_view(row)}


@router.post("/{worker_id}/rotate-token")
async def rotate_token(worker_id: str, request: Request) -> dict[str, Any]:
    settings = _settings_or_404()
    try:
        row, token = await database.run_db(
            registration_service.rotate_token, worker_id=worker_id, settings=settings
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit(request, "worker_token_rotated", worker_id=worker_id)
    return {
        "worker": worker_registry.to_view(row),
        "worker_token": token,
        "note": "Старый токен отозван немедленно. Пропишите новый на воркере "
                "и перезапустите его.",
    }


# ─── Задания ─────────────────────────────────────────────────────────────────
@router.get("/jobs/list")
async def list_jobs(
    worker_id: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = _settings_or_404()
    rows = await database.run_db(
        repositories.list_jobs, worker_id=worker_id, limit=limit, settings=settings
    )
    now = time.time()
    out = []
    for row in rows:
        view = job_service.to_view(row, settings=settings)
        view["progress"] = progress_service.build_view(
            row, view.get("progress_snapshot"), now=now
        )
        out.append(view)
    return {"jobs": out, "server_time": now}


@router.post("/jobs")
async def create_test_job(payload: CreateTestJobRequest, request: Request) -> dict[str, Any]:
    """Ручная выдача БЕЗОПАСНОГО тестового задания конкретному воркеру.

    Единственный доступный тип — test_pipeline_v1. Ни команды, ни argv, ни
    путей в задании нет: воркер строит фиксированный argv сам (§4 задания).
    """
    settings = _settings_or_404()
    try:
        job = await database.run_db(
            job_service.create_test_job,
            worker_id=payload.worker_id,
            project_id=payload.project_id,
            version_id=payload.version_id,
            params=payload.params,
            actor=_actor(request),
            settings=settings,
        )
    except repositories.ActiveJobExists as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except job_service.JobError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    _audit(
        request,
        "test_job_created",
        worker_id=payload.worker_id,
        job_id=job["job_id"],
        project=payload.project_id,
    )
    view = job_service.to_view(job, settings=settings)
    # execution_token наружу оператору не отдаём — он предназначен только воркеру.
    view.pop("_execution_token_plain", None)
    view.pop("_manifest", None)
    return {"job": view}


@router.get("/jobs/{job_id}")
async def get_job(job_id: str) -> dict[str, Any]:
    settings = _settings_or_404()
    row = await database.run_db(repositories.get_job, job_id, settings=settings)
    if row is None:
        raise HTTPException(status_code=404, detail="Задание не найдено.")
    view = job_service.to_view(row, settings=settings)
    view["progress"] = progress_service.build_view(row, view.get("progress_snapshot"))
    transitions = await database.run_db(
        repositories.list_transitions, job_id, settings=settings
    )
    return {"job": view, "transitions": transitions}


@router.get("/jobs/{job_id}/events")
async def get_job_events(
    job_id: str,
    after_seq: int = Query(default=0, ge=0),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    settings = _settings_or_404()
    events = await database.run_db(
        repositories.list_events, job_id, after_seq=after_seq, limit=limit, settings=settings
    )
    return {"events": events}


@router.get("/jobs/{job_id}/logs")
async def get_job_logs(
    job_id: str,
    attempt: Optional[str] = Query(default=None),
    after_seq: int = Query(default=0, ge=0),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict[str, Any]:
    settings = _settings_or_404()
    row = await database.run_db(repositories.get_job, job_id, settings=settings)
    if row is None:
        raise HTTPException(status_code=404, detail="Задание не найдено.")
    try:
        lines = await database.run_db(
            event_service.read_log_lines,
            job_id,
            attempt or row["attempt_id"],
            after_seq=after_seq,
            limit=limit,
            settings=settings,
        )
    except event_service.UnsafeIdentifier as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"lines": lines, "attempt_id": attempt or row["attempt_id"]}


@router.get("/jobs/{job_id}/result")
async def download_result(job_id: str):
    """Скачать провалидированный пакет результата."""
    settings = _settings_or_404()
    row = await database.run_db(repositories.get_job, job_id, settings=settings)
    if row is None:
        raise HTTPException(status_code=404, detail="Задание не найдено.")
    if row["state"] != JobState.COMPLETED.value:
        raise HTTPException(
            status_code=409,
            detail=f"Результат ещё не принят и не проверен (состояние: {row['state']}).",
        )
    archive = job_service.validated_result_path(row, settings=settings)
    if archive is None or not archive.is_file():
        raise HTTPException(status_code=404, detail="Файл результата не найден.")
    return FileResponse(
        path=str(archive), media_type="application/octet-stream", filename=archive.name
    )
