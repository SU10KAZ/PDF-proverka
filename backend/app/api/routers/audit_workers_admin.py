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
    CancelAttemptRequest,
    CreateAttemptRequest,
    CreateTestJobRequest,
    JobState,
    MarkAttemptLostRequest,
    RequestDeletionRequest,
)
from backend.app.services.distributed_workers import (
    attempt_service,
    database,
    event_service,
    identifiers,
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
    """Кто действует. Берётся ИЗ АУТЕНТИФИКАЦИИ, а не из тела запроса (§6)."""
    user = getattr(request.state, "portal_user", None)
    return f"operator:{user}" if user else "operator:anonymous"


def _audit_meta(request: Request) -> dict[str, Any]:
    client = request.client.host if request.client else None
    return {
        "actor_display_name": getattr(request.state, "portal_user", None) or "anonymous",
        "request_id": request.headers.get("X-Request-Id"),
        "source_ip": request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or client,
        "user_agent": (request.headers.get("User-Agent") or "")[:300],
    }


# Заголовок «это осознанный вызов из нашего интерфейса». Вместе с
# SameSite=lax у портальной cookie (см. core/portal_auth) он и есть та самая
# CSRF-защита: простой межсайтовый POST не может выставить произвольный
# заголовок, а запрос с ним становится preflight'ным и отбивается CORS.
INTENT_HEADER = "X-Requested-With"
INTENT_VALUE = "audit-workers"


def _require_intent_header(request: Request) -> None:
    """Только CSRF-рубеж, без ключа идемпотентности.

    Стоит на действиях, повтор которых не создаёт второго эффекта (одобрить,
    отклонить, отозвать, создать задание — второе активное задание на пару
    «проект+версия» отбивает уникальный индекс). Раньше эти четыре ручки
    вообще не имели гейта: отозвать все воркеры можно было без него, а
    ротировать токен — нет.
    """
    if (request.headers.get(INTENT_HEADER) or "").strip() != INTENT_VALUE:
        raise HTTPException(
            status_code=403,
            detail=f"Требуется заголовок {INTENT_HEADER}: {INTENT_VALUE}",
        )


def _require_operator_intent(request: Request) -> str:
    """Опасное действие: проверить намерение и обязательный ключ идемпотентности."""
    _require_intent_header(request)
    key = (request.headers.get("Idempotency-Key") or "").strip()
    if not key:
        raise HTTPException(
            status_code=400,
            detail="Требуется заголовок Idempotency-Key: повтор действия должен "
                   "быть безопасным (I-09).",
        )
    return key[:128]


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
    """Сквозной журнал действий портала (logs/actions/*.jsonl)."""
    try:
        action_log.log_event(
            "worker", event=action, actor=_actor(request).split(":", 1)[-1], **extra
        )
    except Exception:  # noqa: BLE001 — журнал не должен ронять действие
        pass


async def _record_admin_action(
    request: Request,
    *,
    action_type: str,
    settings: Any,
    worker_id: Optional[str] = None,
    job_id: Optional[str] = None,
    attempt_id: Optional[str] = None,
    reason: str = "",
    idempotency_key: Optional[str] = None,
    previous_state: Optional[dict[str, Any]] = None,
    requested_state: Optional[dict[str, Any]] = None,
    result: Optional[dict[str, Any]] = None,
    result_status: str = "ok",
) -> None:
    """Неизменяемый журнал операторских действий (таблица worker_admin_actions).

    Отличается от `_audit` тем, что живёт в БД подсистемы и доступен на экране:
    сквозной журнал портала — про запросы, этот — про решения оператора (I-15).
    """
    meta = _audit_meta(request)
    await database.run_db(
        repositories.record_admin_action,
        actor_id=_actor(request),
        actor_display_name=str(meta["actor_display_name"]),
        action_type=action_type,
        worker_id=worker_id,
        job_id=job_id,
        attempt_id=attempt_id,
        previous_state=previous_state,
        requested_state=requested_state,
        reason=reason,
        idempotency_key=idempotency_key,
        request_id=meta.get("request_id"),
        source_ip=meta.get("source_ip"),
        user_agent=meta.get("user_agent"),
        result_status=result_status,
        result=result,
        settings=settings,
    )


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


# ─── Журнал операторских действий ────────────────────────────────────────────
@router.get("/admin-actions")
async def admin_actions(
    job_id: Optional[str] = Query(default=None),
    worker_id: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    """Журнал только для чтения. Эндпоинта удаления записей нет намеренно (I-15)."""
    settings = _settings_or_404()
    items = await database.run_db(
        repositories.list_admin_actions,
        job_id=job_id,
        worker_id=worker_id,
        limit=limit,
        settings=settings,
    )
    return {"actions": items, "server_time": time.time()}


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
    _require_intent_header(request)
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
    await _record_admin_action(
        request, action_type="approve_worker", worker_id=worker_id, settings=settings
    )
    return {"worker": worker_registry.to_view(row)}


@router.post("/{worker_id}/reject")
async def reject_worker(worker_id: str, request: Request) -> dict[str, Any]:
    """Отклонить заявку. Claim-secret обесценивается, токен не выдаётся."""
    settings = _settings_or_404()
    _require_intent_header(request)
    try:
        row = await database.run_db(
            registration_service.reject_worker, worker_id=worker_id, settings=settings
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit(request, "worker_rejected", worker_id=worker_id)
    await _record_admin_action(
        request, action_type="reject_worker", worker_id=worker_id, settings=settings
    )
    return {"worker": worker_registry.to_view(row)}


@router.post("/{worker_id}/revoke")
async def revoke_worker(worker_id: str, request: Request) -> dict[str, Any]:
    settings = _settings_or_404()
    _require_intent_header(request)
    try:
        row = await database.run_db(
            registration_service.revoke_worker, worker_id=worker_id, settings=settings
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit(request, "worker_revoked", worker_id=worker_id)
    await _record_admin_action(
        request, action_type="revoke_worker", worker_id=worker_id, settings=settings
    )
    return {"worker": worker_registry.to_view(row)}


@router.post("/{worker_id}/rotate-token")
async def rotate_token(worker_id: str, request: Request) -> dict[str, Any]:
    """Выдать новый токен. Опасно: токен показывается один раз и открытым текстом.

    Поэтому здесь стоит тот же гейт намерения, что и на остальных меняющих
    состояние ручках. Что он даёт честно: межсайтовый запрос (форма с чужого
    сайта, «простой» POST) заголовки не поставит. Чего он НЕ даёт: защиты от
    XSS в самой странице — same-origin скрипт выставит любые заголовки. От
    XSS защищает только то, что страница нигде не собирает HTML из данных.
    """
    settings = _settings_or_404()
    _require_operator_intent(request)
    try:
        row, token = await database.run_db(
            registration_service.rotate_token, worker_id=worker_id, settings=settings
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit(request, "worker_token_rotated", worker_id=worker_id)
    await _record_admin_action(
        request, action_type="rotate_worker_token", worker_id=worker_id,
        settings=settings,
        result={"note": "старый токен отозван атомарно, новый показан один раз"},
    )
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
    _require_intent_header(request)
    try:
        job = await database.run_db(
            job_service.create_test_job,
            worker_id=payload.worker_id,
            project_id=payload.project_id,
            version_id=payload.version_id,
            params=payload.params,
            actor=_actor(request),
            display_name=payload.project_display_name,
            settings=settings,
        )
    except repositories.ActiveJobExists as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except identifiers.UnsafeIdentifier as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except job_service.JobError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    _audit(
        request,
        "test_job_created",
        worker_id=payload.worker_id,
        job_id=job["job_id"],
        project=payload.project_id,
    )
    await _record_admin_action(
        request, action_type="create_job", worker_id=payload.worker_id,
        job_id=job["job_id"], attempt_id=job["attempt_id"],
        requested_state={"project_external_id": payload.project_id},
        settings=settings,
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


# ─── Попытки ─────────────────────────────────────────────────────────────────
@router.get("/jobs/{job_id}/attempts")
async def list_attempts(job_id: str) -> dict[str, Any]:
    """История попыток задания: что было, кем и чем закончилось."""
    settings = _settings_or_404()
    logical = await database.run_db(
        repositories.get_logical_job, job_id, settings=settings
    )
    if logical is None:
        raise HTTPException(status_code=404, detail="Задание не найдено.")
    attempts = await database.run_db(
        attempt_service.attempts_view, job_id=job_id, settings=settings
    )
    return {
        "job": {
            "job_id": logical["job_id"],
            "project_external_id": logical["project_external_id"],
            "project_display_name": logical.get("project_display_name"),
            "project_version_id": logical.get("project_version_id"),
            "overall_state": logical.get("overall_state"),
            "current_attempt_id": logical.get("current_attempt_id"),
            "created_by": logical.get("created_by"),
            "created_at": logical.get("created_at"),
        },
        "attempts": attempts,
        "server_time": time.time(),
    }


@router.post("/jobs/{job_id}/attempts/{attempt_id}/cancel")
async def cancel_attempt(
    job_id: str, attempt_id: str, payload: CancelAttemptRequest, request: Request
) -> dict[str, Any]:
    """Запросить отмену попытки. Не обещает мгновенной остановки (§5.1)."""
    settings = _settings_or_404()
    key = _require_operator_intent(request)
    try:
        result = await database.run_db(
            attempt_service.request_cancel,
            job_id=job_id,
            attempt_id=attempt_id,
            reason=payload.reason,
            confirmation=payload.confirmation,
            grace_period_sec=payload.grace_period_sec,
            actor=_actor(request),
            idempotency_key=key,
            audit=_audit_meta(request),
            settings=settings,
        )
    except attempt_service.ConfirmationRequired as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except attempt_service.OperatorError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _audit(request, "attempt_cancel_requested", job_id=job_id, attempt_id=attempt_id)
    return result


@router.post("/jobs/{job_id}/attempts/{attempt_id}/mark-lost")
async def mark_attempt_lost(
    job_id: str, attempt_id: str, payload: MarkAttemptLostRequest, request: Request
) -> dict[str, Any]:
    """Признать попытку потерянной. НЕ утверждает, что процесс остановлен (I-06)."""
    settings = _settings_or_404()
    key = _require_operator_intent(request)
    try:
        result = await database.run_db(
            attempt_service.mark_lost,
            job_id=job_id,
            attempt_id=attempt_id,
            reason=payload.mandatory_reason,
            typed_confirmation=payload.typed_confirmation,
            observed_worker_state=payload.observed_worker_state,
            operator_note=payload.optional_operator_note,
            actor=_actor(request),
            idempotency_key=key,
            audit=_audit_meta(request),
            settings=settings,
        )
    except attempt_service.ConfirmationRequired as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except attempt_service.OperatorError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _audit(request, "attempt_marked_lost", job_id=job_id, attempt_id=attempt_id)
    return result


@router.post("/jobs/{job_id}/attempts")
async def create_attempt(
    job_id: str, payload: CreateAttemptRequest, request: Request
) -> dict[str, Any]:
    """Создать новую попытку. Поверх работающей — нельзя (I-05)."""
    settings = _settings_or_404()
    key = _require_operator_intent(request)
    try:
        result = await database.run_db(
            attempt_service.create_attempt,
            job_id=job_id,
            worker_id=payload.worker_id,
            reason=payload.reason,
            source_attempt_id=payload.source_attempt_id,
            confirmation=payload.confirmation,
            actor=_actor(request),
            idempotency_key=key,
            audit=_audit_meta(request),
            settings=settings,
        )
    except attempt_service.ConfirmationRequired as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except repositories.ActiveAttemptExists as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except attempt_service.OperatorError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _audit(request, "attempt_created", job_id=job_id, worker_id=payload.worker_id)
    return result


@router.post("/jobs/{job_id}/attempts/{attempt_id}/request-deletion")
async def request_attempt_deletion(
    job_id: str, attempt_id: str, payload: RequestDeletionRequest, request: Request
) -> dict[str, Any]:
    """Попросить воркер удалить локальные данные попытки. Центральная копия остаётся."""
    settings = _settings_or_404()
    key = _require_operator_intent(request)
    try:
        result = await database.run_db(
            attempt_service.request_data_deletion,
            job_id=job_id,
            attempt_id=attempt_id,
            reason=payload.reason,
            confirmation=payload.confirmation,
            actor=_actor(request),
            idempotency_key=key,
            audit=_audit_meta(request),
            settings=settings,
        )
    except attempt_service.ConfirmationRequired as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except attempt_service.OperatorError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _audit(request, "worker_data_deletion_requested", job_id=job_id, attempt_id=attempt_id)
    return result


@router.get("/jobs/{job_id}/attempts/{attempt_id}/result")
async def download_attempt_result(job_id: str, attempt_id: str):
    """Скачать пакет КОНКРЕТНОЙ попытки — в том числе устаревшей.

    Файл открывается по UUID из БД; человекочитаемое имя уходит только в
    заголовок (I-11). Устаревший результат подписан явно и никогда не
    выдаётся за актуальный.
    """
    settings = _settings_or_404()
    attempt = await database.run_db(
        repositories.get_attempt, attempt_id, settings=settings
    )
    if attempt is None or attempt["job_id"] != job_id:
        raise HTTPException(status_code=404, detail="Попытка не найдена.")
    archive = job_service.validated_result_path(attempt, settings=settings)
    prefix = ""
    if archive is None:
        archive = job_service.superseded_result_path(attempt, settings=settings)
        prefix = "УСТАРЕВШАЯ-ПОПЫТКА_"
    if archive is None or not archive.is_file():
        raise HTTPException(status_code=404, detail="Файл результата не найден.")
    filename = identifiers.safe_download_filename(
        f"{prefix}{attempt.get('project_display_name') or ''}"
        f"_попытка{attempt.get('attempt_no')}",
        fallback=f"attempt_{attempt_id}",
        suffix="".join(archive.suffixes[-2:]),
    )
    return FileResponse(
        path=str(archive), media_type="application/octet-stream", filename=filename
    )
