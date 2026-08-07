"""API для audit-worker: `/api/v1/worker/*`.

Контур машинной аутентификации (bearer-токен воркера). Портальная cookie здесь
не работает и не должна: см. §20.2 техпроекта, тест `test_auth_contours_isolated`.

Все обращения к SQLite идут через `database.run_db` (asyncio.to_thread) —
синхронный sqlite3 в event loop уже был причиной смерти бэкенда по вотчдогу.

Этап 0: единственный тип задания — `test_pipeline_v1`. Канала «выполни
произвольную команду» нет: список команд — закрытый enum WorkerCommandType.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse

from backend.app.models.distributed_workers import (
    AcceptRequest,
    ClaimRequest,
    ClaimResponse,
    CommandAckRequest,
    ConnectivityState,
    EventBatchRequest,
    EventBatchResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    JobAssignment,
    JobState,
    JobType,
    JobsNextRequest,
    PackageRef,
    ReconcileJobVerdict,
    ReconcileRequest,
    ReconcileResponse,
    RegisterRequest,
    RegisterResponse,
    RegistrationStatus,
    RegistrationUpdateRequest,
    RejectRequest,
    TestJobParams,
    UploadCompleteRequest,
    UploadCompleteResponse,
    UploadCreateRequest,
    UploadSessionInfo,
    WorkerCommandOut,
)
from backend.app.services.distributed_workers import (
    auth,
    database,
    event_service,
    job_service,
    package_service,
    registration_service,
    repositories,
    upload_service,
    worker_registry,
)
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersConfigError,
    get_settings,
)

router = APIRouter(prefix="/api/v1/worker", tags=["audit-worker-agent"])

HEARTBEAT_INTERVAL_SEC = 30


def _bearer(authorization: Optional[str]) -> Optional[str]:
    """Достать значение строго из схемы Bearer.

    Раньше бралось всё после первого пробела при ЛЮБОЙ схеме — то есть
    `Authorization: Foo <секрет>` тоже проходил.
    """
    if not authorization:
        return None
    scheme, _, value = authorization.partition(" ")
    if scheme.strip().lower() != "bearer":
        return None
    return value.strip() or None


def _settings():
    settings = get_settings()
    if not settings.enabled:
        raise HTTPException(status_code=404, detail="Подсистема воркеров отключена.")
    return settings


async def _idempotent(
    *,
    principal: auth.WorkerPrincipal,
    request: Request,
    endpoint: str,
    body: Any,
    compute,
):
    """Обёртка идемпотентности по заголовку `Idempotency-Key`.

    Повтор с тем же ключом и тем же телом → сохранённый ответ.
    Повтор с тем же ключом, но ДРУГИМ телом → 409: это почти наверняка ошибка
    клиента, и молча выдать чужой ответ было бы хуже, чем отказать.
    Без заголовка обработчик выполняется как обычно (сами операции и так
    спроектированы безопасными к повтору).
    """
    key = (request.headers.get("Idempotency-Key") or "").strip()
    if not key:
        return await compute()

    settings = principal.settings
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, default=str)
    request_sha256 = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    scoped = f"{principal.worker_id}:{endpoint}:{key}"

    prior = await database.run_db(
        repositories.get_idempotent_response, scoped, settings=settings
    )
    if prior is not None:
        if prior["request_sha256"] != request_sha256:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "idempotency_key_reuse",
                    "message": "Тот же Idempotency-Key использован с другим телом запроса",
                    "endpoint": endpoint,
                },
            )
        return JSONResponse(
            status_code=int(prior["status_code"]),
            content=json.loads(prior["response_json"]),
        )

    result = await compute()
    payload = jsonable_encoder(result)
    await database.run_db(
        repositories.save_idempotent_response,
        key=scoped,
        worker_id=principal.worker_id,
        endpoint=endpoint,
        request_sha256=request_sha256,
        response_json=json.dumps(payload, ensure_ascii=False),
        status_code=200,
        settings=settings,
    )
    return result


async def _load_job_for_worker(
    job_id: str, principal: auth.WorkerPrincipal, execution_token: Optional[str]
) -> dict[str, Any]:
    job = await database.run_db(
        repositories.get_job, job_id, settings=principal.settings
    )
    if job is None:
        raise HTTPException(status_code=404, detail="Задание не найдено.")
    if job.get("assigned_worker_id") != principal.worker_id:
        raise HTTPException(status_code=403, detail="Задание закреплено за другим воркером.")
    auth.require_execution_token(job, execution_token)
    return job


# ─── Регистрация ─────────────────────────────────────────────────────────────
@router.post("/register", response_model=RegisterResponse, status_code=201)
async def register(
    payload: RegisterRequest,
    authorization: Optional[str] = Header(default=None),
) -> RegisterResponse:
    """Заявка на регистрацию. Требует bootstrap-секрет; одобряет оператор вручную."""
    settings = _settings()
    auth.verify_bootstrap_secret(_bearer(authorization))
    if payload.protocol_version != settings.protocol_version:
        raise HTTPException(
            status_code=426,
            detail=(
                f"Несовместимая версия протокола: воркер {payload.protocol_version}, "
                f"центр {settings.protocol_version}."
            ),
        )
    try:
        worker, claim_secret, created = await database.run_db(
            registration_service.register_worker,
            instance_id=payload.instance_id,
            display_name_hint=payload.display_name_hint,
            worker_version=payload.worker_version,
            protocol_version=payload.protocol_version,
            pipeline_revision=payload.pipeline_revision,
            capabilities=payload.capabilities.model_dump(),
            configured_max_slots_hint=payload.configured_max_slots_hint,
            settings=settings,
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if created:
        message = ("Заявка принята. Ожидает одобрения оператора, после чего "
                   "заберите токен через POST /claim.")
    else:
        message = (
            "Регистрация уже существует. Повторная заявка claim-secret НЕ "
            "перевыпускает (иначе чужой instance_id перехватывал бы выдачу "
            "токена). Секрет утерян — оператор отклоняет заявку или делает "
            "ротацию токена."
        )
    return RegisterResponse(
        worker_id=worker["worker_id"],
        registration_status=RegistrationStatus(worker["registration_status"]),
        claim_secret=claim_secret,
        heartbeat_interval_sec=HEARTBEAT_INTERVAL_SEC,
        poll_timeout_sec=settings.long_poll_sec,
        chunk_size_bytes=settings.upload_chunk_bytes,
        protocol_version=settings.protocol_version,
        message=message,
    )


@router.post("/claim", response_model=ClaimResponse)
async def claim(payload: ClaimRequest) -> ClaimResponse:
    """Обменять одноразовый claim-secret на постоянный токен.

    Доступно ТОЛЬКО после одобрения оператором и ровно один раз. Повтор → 409:
    токен хранится на центре хэшем и извлечь его обратно нельзя, при утере
    оператор делает ротацию.

    Эндпоинт намеренно не требует bearer-токена (его ещё нет) — правом
    служит сам claim-secret.
    """
    settings = _settings()
    try:
        worker, token = await database.run_db(
            registration_service.claim_token,
            worker_id=payload.worker_id,
            claim_secret=payload.claim_secret,
            settings=settings,
        )
    except registration_service.ClaimRejected as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await database.run_db(
        repositories.update_worker_fields,
        worker["worker_id"],
        {"instance_id": payload.instance_id},
        settings=settings,
    )
    return ClaimResponse(
        worker_id=worker["worker_id"],
        registration_status=RegistrationStatus(worker["registration_status"]),
        worker_token=token,
        heartbeat_interval_sec=HEARTBEAT_INTERVAL_SEC,
        poll_timeout_sec=settings.long_poll_sec,
        chunk_size_bytes=settings.upload_chunk_bytes,
        protocol_version=settings.protocol_version,
    )


@router.put("/registration")
async def update_registration(
    payload: RegistrationUpdateRequest,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> dict[str, Any]:
    worker = await database.run_db(
        registration_service.update_registration,
        worker_id=principal.worker_id,
        instance_id=payload.instance_id,
        worker_version=payload.worker_version,
        protocol_version=payload.protocol_version,
        pipeline_revision=payload.pipeline_revision,
        capabilities=payload.capabilities.model_dump(),
        settings=principal.settings,
    )
    return {"worker_id": worker.get("worker_id"), "updated": True}


# ─── Heartbeat и ресурсы ─────────────────────────────────────────────────────
@router.post("/heartbeat", response_model=HeartbeatResponse)
async def heartbeat(
    payload: HeartbeatRequest,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> HeartbeatResponse:
    """Живость + ресурсы + активные задания.

    Идемпотентен: повтор меняет только last_seen_at. Ни одно поле ответа не
    меняет состояние заданий — молчание воркера тоже не меняет (I-01, I-02).
    """
    settings = principal.settings
    await database.run_db(
        worker_registry.record_heartbeat,
        worker_id=principal.worker_id,
        instance_id=payload.instance_id,
        worker_state=payload.worker_state.value,
        configured_max_slots=payload.configured_max_slots,
        calculated_free_slots=payload.calculated_free_slots,
        active_jobs=[j.model_dump() for j in payload.active_jobs],
        resource_snapshot=payload.resource_snapshot.model_dump()
        if payload.resource_snapshot
        else None,
        warnings=payload.warnings,
        settings=settings,
    )
    cursors = await database.run_db(
        repositories.cursors_for_worker, principal.worker_id, settings=settings
    )
    commands = await database.run_db(
        repositories.pending_commands, principal.worker_id, settings=settings
    )
    has_work = await database.run_db(
        repositories.has_assigned_job, principal.worker_id, settings=settings
    )
    # ВАЖНО: выборка именно по наличию retention_until, а не «нетерминальные».
    # Срок хранения проставляется ВМЕСТЕ с переходом в терминальное состояние,
    # поэтому прежний фильтр гарантированно давал пустой список.
    jobs = await database.run_db(
        repositories.jobs_with_retention, principal.worker_id, settings=settings
    )
    retention_updates = [
        {
            "job_id": j["job_id"],
            "attempt_id": j["attempt_id"],
            "retention_until": j.get("retention_until"),
        }
        for j in jobs
    ]
    return HeartbeatResponse(
        server_time=time.time(),
        connection_status=ConnectivityState.ONLINE,
        has_pending_commands=bool(commands),
        has_available_work=bool(has_work),
        next_heartbeat_in_sec=HEARTBEAT_INTERVAL_SEC,
        acked_cursors=[
            {
                "job_id": c["job_id"],
                "attempt_id": c["attempt_id"],
                "last_seen_seq": c["last_seen_seq"],
            }
            for c in cursors
        ],
        retention_updates=retention_updates,
    )


@router.post("/resources")
async def post_resources(
    snapshot: dict[str, Any],
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> dict[str, Any]:
    """Внеочередной снимок ресурсов (обычно едет внутри heartbeat)."""
    await database.run_db(
        repositories.record_resource_snapshot,
        principal.worker_id,
        snapshot,
        settings=principal.settings,
    )
    await database.run_db(
        repositories.update_worker_fields,
        principal.worker_id,
        {"resource_snapshot": json.dumps(snapshot, ensure_ascii=False)},
        settings=principal.settings,
    )
    return {"accepted": True, "server_time": time.time()}


# ─── Выдача задания ──────────────────────────────────────────────────────────
@router.post("/jobs/next")
async def jobs_next(
    payload: JobsNextRequest,
    request: Request,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
):
    """Long-poll за следующим заданием. 204 = заданий нет.

    Задание уже закреплено оператором за этим воркером (ADR-004: автовыбора
    нет), поэтому «взять» = атомарно перевести assigned → source_uploading.
    """
    settings = principal.settings
    if payload.free_slots <= 0:
        raise HTTPException(status_code=409, detail="Нет свободных слотов у воркера.")

    key = (request.headers.get("Idempotency-Key") or "").strip()
    if key:
        prior = await database.run_db(
            repositories.get_idempotent_response,
            f"{principal.worker_id}:jobs_next:{key}",
            settings=settings,
        )
        if prior is not None:
            # Повтор того же запроса не должен выдать ВТОРОЕ задание.
            # Токен попытки в кэше НЕ хранится (иначе он лежал бы в БД
            # открытым текстом, обесценивая хранение только хэша) — на повторе
            # выпускаем свежий и переписываем хэш: задание то же самое.
            cached = json.loads(prior["response_json"])
            replay_token = auth.generate_execution_token()
            await database.run_db(
                repositories.update_job_fields,
                cached["job_id"],
                {"execution_token_sha256": auth.hash_token(replay_token)},
                settings=settings,
            )
            cached["execution_token"] = replay_token
            return JSONResponse(status_code=int(prior["status_code"]), content=cached)

    deadline = time.monotonic() + min(payload.wait_sec, settings.long_poll_sec)
    job: Optional[dict[str, Any]] = None
    while True:
        job = await database.run_db(
            repositories.claim_next_job_for_worker,
            principal.worker_id,
            settings=settings,
        )
        if job is not None or time.monotonic() >= deadline:
            break
        # Пауза вне транзакции: длинное ожидание не держит БД (§19.6).
        await asyncio.sleep(1.0)

    if job is None:
        return Response(status_code=204)

    token = auth.generate_execution_token()
    await database.run_db(
        repositories.update_job_fields,
        job["job_id"],
        {"execution_token_sha256": auth.hash_token(token)},
        settings=settings,
    )

    archive = job_service.source_package_path(job, settings=settings)
    if archive is None:
        raise HTTPException(status_code=500, detail="Исходный пакет задания не найден.")
    manifest_path = archive.parent / package_service.MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload_obj = json.loads(job.get("payload") or "{}")

    assignment = JobAssignment(
        job_id=job["job_id"],
        attempt_id=job["attempt_id"],
        attempt_no=int(job.get("attempt_no", 1)),
        execution_token=token,
        assigned_at=float(job.get("assigned_at") or time.time()),
        assign_ttl_sec=job_service.ASSIGN_TTL_SEC,
        job_type=JobType(job["job_type"]),
        project_id=job["project_id"],
        version_id=job.get("version_id"),
        params=TestJobParams(**(payload_obj.get("params") or {})),
        package=PackageRef(
            package_id=manifest["package_id"],
            package_type="source",
            url=f"/api/v1/worker/jobs/{job['job_id']}/source",
            size_bytes=int(manifest["archive"]["compressed_bytes"]),
            sha256=manifest["archive"]["sha256"],
            compression=manifest["compression"],
            manifest_version=int(manifest["manifest_version"]),
        ),
        fingerprints={
            "protocol_version": settings.protocol_version,
            "package_manifest_version": settings.manifest_version,
        },
        event_start_seq=1,
        heartbeat_interval_sec=HEARTBEAT_INTERVAL_SEC,
    )
    if key:
        # Из кэша вырезаем секрет попытки: в БД он не должен оседать открытым.
        cacheable = jsonable_encoder(assignment)
        cacheable.pop("execution_token", None)
        await database.run_db(
            repositories.save_idempotent_response,
            key=f"{principal.worker_id}:jobs_next:{key}",
            worker_id=principal.worker_id,
            endpoint="jobs_next",
            request_sha256=hashlib.sha256(
                json.dumps(payload.model_dump(), sort_keys=True, default=str).encode()
            ).hexdigest(),
            response_json=json.dumps(cacheable, ensure_ascii=False),
            status_code=200,
            settings=settings,
        )
    return assignment


@router.get("/jobs/{job_id}/source")
async def download_source(
    job_id: str,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
    x_execution_token: Optional[str] = Header(default=None),
):
    """Отдать исходный пакет. FileResponse поддерживает Range → докачка."""
    job = await _load_job_for_worker(job_id, principal, x_execution_token)
    archive = job_service.source_package_path(job, settings=principal.settings)
    if archive is None or not archive.is_file():
        raise HTTPException(status_code=404, detail="Исходный пакет не найден.")
    return FileResponse(
        path=str(archive),
        media_type="application/octet-stream",
        filename=archive.name,
        headers={"ETag": f'"{job.get("source_package_hash") or ""}"'},
    )


@router.post("/jobs/{job_id}/accept")
async def accept_job(
    job_id: str,
    payload: AcceptRequest,
    request: Request,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
    x_execution_token: Optional[str] = Header(default=None),
):
    return await _idempotent(
        principal=principal, request=request, endpoint=f"accept:{job_id}",
        body=payload.model_dump(),
        compute=lambda: _accept_job_impl(job_id, payload, principal, x_execution_token),
    )


async def _accept_job_impl(
    job_id: str,
    payload: AcceptRequest,
    principal: auth.WorkerPrincipal,
    x_execution_token: Optional[str],
) -> dict[str, Any]:
    job = await _load_job_for_worker(job_id, principal, x_execution_token)
    if payload.attempt_id != job["attempt_id"]:
        raise HTTPException(
            status_code=409,
            detail={"error": "attempt_superseded", "current_attempt": job["attempt_id"]},
        )
    if job["state"] == JobState.ACCEPTED_BY_WORKER.value:
        return {"state": job["state"], "event_start_seq": 1, "replayed": True,
                "server_time": time.time()}
    if job["state"] == JobState.SOURCE_UPLOADING.value:
        job = await database.run_db(
            job_service.transition,
            job_id=job_id,
            to_state=JobState.SOURCE_READY,
            actor="worker",
            reason="sha256 исходного пакета сошёлся",
            settings=principal.settings,
        )
    try:
        job = await database.run_db(
            job_service.transition,
            job_id=job_id,
            to_state=JobState.ACCEPTED_BY_WORKER,
            actor="worker",
            reason="манифест проверен, дерево распаковано",
            settings=principal.settings,
        )
    except job_service.IllegalTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"state": job["state"], "event_start_seq": 1, "server_time": time.time()}


@router.post("/jobs/{job_id}/reject")
async def reject_job(
    job_id: str,
    payload: RejectRequest,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
    x_execution_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    job = await _load_job_for_worker(job_id, principal, x_execution_token)
    try:
        job = await database.run_db(
            job_service.transition,
            job_id=job_id,
            to_state=JobState.FAILED,
            actor="worker",
            reason=f"воркер отказался: {payload.reason}",
            fields={
                "error": json.dumps(
                    {"code": "rejected_by_worker", "message": payload.reason},
                    ensure_ascii=False,
                )
            },
            settings=principal.settings,
        )
    except job_service.IllegalTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"state": job["state"]}


# ─── События ─────────────────────────────────────────────────────────────────
@router.post("/events", response_model=EventBatchResponse)
async def post_events(
    payload: EventBatchRequest,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
    x_execution_token: Optional[str] = Header(default=None),
) -> EventBatchResponse:
    """Приём непрерывного пакета событий. Разрыв → 409 с ожидаемым seq."""
    job = await _load_job_for_worker(payload.job_id, principal, x_execution_token)
    if payload.attempt_id != job["attempt_id"]:
        raise HTTPException(
            status_code=409,
            detail={"error": "attempt_superseded", "current_attempt": job["attempt_id"]},
        )
    events = [
        {
            "seq": e.seq,
            "event_id": e.event_id,
            "event_type": e.event_type.value,
            "occurred_at": e.occurred_at,
            "schema_version": e.schema_version,
            "payload": e.payload,
        }
        for e in payload.events
    ]
    try:
        result = await database.run_db(
            event_service.ingest_batch,
            job=job,
            worker_id=principal.worker_id,
            first_seq=payload.first_seq,
            events=events,
            settings=principal.settings,
        )
    except event_service.SequenceGap as gap:
        return JSONResponse(
            status_code=409,
            content={
                "error": "sequence_gap",
                "message": "Пропущены события",
                "detail": {
                    "expected_seq": gap.expected_seq,
                    "received_first_seq": gap.received_first_seq,
                },
            },
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return EventBatchResponse(**result)


# ─── Загрузка результата ─────────────────────────────────────────────────────
@router.post("/uploads", response_model=UploadSessionInfo)
async def create_upload(
    payload: UploadCreateRequest,
    request: Request,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
    x_execution_token: Optional[str] = Header(default=None),
):
    return await _idempotent(
        principal=principal, request=request, endpoint="uploads",
        body=payload.model_dump(),
        compute=lambda: _create_upload_impl(payload, principal, x_execution_token),
    )


async def _create_upload_impl(
    payload: UploadCreateRequest,
    principal: auth.WorkerPrincipal,
    x_execution_token: Optional[str],
) -> UploadSessionInfo:
    job = await _load_job_for_worker(payload.job_id, principal, x_execution_token)
    if payload.attempt_id != job["attempt_id"]:
        raise HTTPException(
            status_code=409,
            detail={"error": "attempt_superseded", "current_attempt": job["attempt_id"]},
        )
    try:
        session, replayed = await database.run_db(
            upload_service.open_or_create_session,
            job=job,
            package_type=payload.package_type,
            expected_size=payload.expected_size,
            expected_hash=payload.expected_hash,
            settings=principal.settings,
        )
    except upload_service.UploadError as exc:
        status = 507 if "места" in str(exc) else 413
        raise HTTPException(status_code=status, detail=str(exc)) from exc

    if job["state"] == JobState.COMPLETED_LOCALLY.value:
        await database.run_db(
            job_service.transition,
            job_id=job["job_id"],
            to_state=JobState.RESULT_UPLOADING,
            actor="worker",
            reason="создана upload-сессия",
            settings=principal.settings,
        )
    info = await database.run_db(
        upload_service.session_info, session, settings=principal.settings
    )
    return UploadSessionInfo(**info, replayed=replayed)


@router.get("/uploads/{upload_id}", response_model=UploadSessionInfo)
async def get_upload(
    upload_id: str,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> UploadSessionInfo:
    session = await database.run_db(
        repositories.get_upload_session, upload_id, settings=principal.settings
    )
    if session is None:
        raise HTTPException(status_code=404, detail="Сессия загрузки не найдена.")
    # Соседние ручки эту проверку делают, а эта её не делала: чужой воркер
    # читал состояние чужой выгрузки, зная только upload_id.
    job = await database.run_db(
        repositories.get_job, session["job_id"], settings=principal.settings
    )
    if job is None or job.get("assigned_worker_id") != principal.worker_id:
        raise HTTPException(status_code=403, detail="Сессия принадлежит другому воркеру.")
    info = await database.run_db(
        upload_service.session_info, session, settings=principal.settings
    )
    return UploadSessionInfo(**info)


@router.put("/uploads/{upload_id}/chunks/{idx}")
async def put_chunk(
    upload_id: str,
    idx: int,
    request: Request,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
    x_chunk_sha256: Optional[str] = Header(default=None),
    x_execution_token: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    settings = principal.settings
    session = await database.run_db(
        repositories.get_upload_session, upload_id, settings=settings
    )
    if session is None:
        raise HTTPException(status_code=404, detail="Сессия загрузки не найдена.")
    # Через _load_job_for_worker — так же, как все остальные ручки по заданию:
    # здесь не хватало проверки актуальности попытки (I-05).
    job = await _load_job_for_worker(session["job_id"], principal, x_execution_token)

    body = await request.body()
    if len(body) > settings.upload_chunk_bytes + 1024:
        raise HTTPException(status_code=413, detail="Чанк превышает объявленный размер.")
    try:
        outcome = await database.run_db(
            upload_service.store_chunk,
            session=session,
            idx=idx,
            data=body,
            declared_sha256=x_chunk_sha256,
            settings=settings,
        )
    except upload_service.ChunkConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except upload_service.UploadError as exc:
        raise HTTPException(status_code=410, detail=str(exc)) from exc
    received = await database.run_db(
        repositories.received_chunks, upload_id, settings=settings
    )
    return {"outcome": outcome, "replayed": outcome == "replayed",
            "received_chunks": received}


@router.post("/uploads/{upload_id}/complete", response_model=UploadCompleteResponse)
async def complete_upload(
    upload_id: str,
    payload: UploadCompleteRequest,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
    x_execution_token: Optional[str] = Header(default=None),
) -> UploadCompleteResponse:
    settings = principal.settings
    session = await database.run_db(
        repositories.get_upload_session, upload_id, settings=settings
    )
    if session is None:
        raise HTTPException(status_code=404, detail="Сессия загрузки не найдена.")
    job = await _load_job_for_worker(session["job_id"], principal, x_execution_token)

    # Повторный complete: результат уже принят — возвращаем то же самое.
    if session.get("status") == "verified":
        fresh = await database.run_db(
            repositories.get_job, job["job_id"], settings=settings
        )
        return UploadCompleteResponse(
            state=JobState(fresh["state"]),
            validation={"replayed": True, "upload_id": upload_id},
            server_time=time.time(),
            retention_until=fresh.get("retention_until"),
        )

    # Занимаем сессию под сборку: два одновременных complete раньше писали в
    # один tmp-файл и портили архив друг другу.
    previous_status = await database.run_db(
        repositories.claim_upload_for_assembly, upload_id, settings=settings
    )
    if previous_status is None:
        raise HTTPException(
            status_code=409,
            detail="Сборка этой загрузки уже идёт. Повторите запрос позже.",
        )

    try:
        archive = await database.run_db(
            upload_service.assemble,
            session=session,
            declared_hash=payload.sha256,
            settings=settings,
        )
    except upload_service.UploadError as exc:
        await database.run_db(
            repositories.update_upload_session,
            upload_id,
            {"status": "failed", "error": str(exc)},
            settings=settings,
        )
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Попытка уже провалена или отменена: результат принимаем на ХРАНЕНИЕ,
    # но не публикуем. Иначе готовый архив некуда деть и он висит на воркере.
    if job["state"] in (JobState.FAILED.value, JobState.CANCELLED.value):
        stored = await database.run_db(
            job_service.store_unpublished_result,
            job=job,
            archive=archive,
            settings=settings,
        )
        await database.run_db(
            repositories.update_upload_session,
            upload_id,
            {"status": "verified", "finalized_at": time.time()},
            settings=settings,
        )
        await database.run_db(upload_service.cleanup_chunks, upload_id, settings=settings)
        return UploadCompleteResponse(
            state=JobState(stored["state"]),
            validation={
                "published": False,
                "stored_only": True,
                "reason": "попытка уже завершена как неуспешная — результат не публикуется",
            },
            server_time=time.time(),
            retention_until=stored.get("retention_until"),
        )

    try:
        job = await database.run_db(
            job_service.catch_up_to_result_received,
            job_id=job["job_id"],
            settings=settings,
        )
    except job_service.JobError as exc:
        # Несогласованность состояний — это 409 с внятным текстом, а не 500:
        # воркеру нужно понять, повторять ли, и не потерять готовый архив.
        await database.run_db(
            repositories.update_upload_session,
            upload_id,
            {"status": "failed", "error": str(exc)},
            settings=settings,
        )
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    try:
        updated, report = await database.run_db(
            job_service.finalize_result,
            job=job,
            archive=archive,
            expected_hash=payload.sha256,
            expected_size=payload.total_size,
            settings=settings,
        )
    except job_service.JobError as exc:
        await database.run_db(
            repositories.update_upload_session,
            upload_id,
            {"status": "failed", "error": str(exc)},
            settings=settings,
        )
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await database.run_db(
        repositories.update_upload_session,
        upload_id,
        {"status": "verified" if report.ok else "failed", "finalized_at": time.time()},
        settings=settings,
    )
    await database.run_db(upload_service.cleanup_chunks, upload_id, settings=settings)

    return UploadCompleteResponse(
        state=JobState(updated["state"]),
        validation=report.as_dict(),
        server_time=time.time(),
        retention_until=updated.get("retention_until"),
    )


# ─── Команды ─────────────────────────────────────────────────────────────────
@router.get("/commands")
async def get_commands(
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> dict[str, Any]:
    items = await database.run_db(
        repositories.pending_commands,
        principal.worker_id,
        mark_delivered=True,
        settings=principal.settings,
    )
    commands = [
        WorkerCommandOut(
            command_id=i["command_id"],
            command_type=i["command_type"],
            payload=json.loads(i.get("payload") or "{}"),
            created_at=i["created_at"],
            idempotency_key=i["idempotency_key"],
        ).model_dump()
        for i in items
    ]
    return {"commands": commands}


@router.post("/commands/{command_id}/ack")
async def ack_command(
    command_id: str,
    payload: CommandAckRequest,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> dict[str, Any]:
    # Порядок важен: раньше запись шла ПЕРВОЙ, и чужой воркер, получив 403,
    # успевал погасить команду — настоящий адресат не видел её никогда.
    existing = await database.run_db(
        repositories.get_command, command_id, settings=principal.settings
    )
    if not existing:
        raise HTTPException(status_code=404, detail="Команда не найдена.")
    if existing.get("worker_id") != principal.worker_id:
        raise HTTPException(status_code=403, detail="Команда адресована другому воркеру.")
    item, replayed = await database.run_db(
        repositories.ack_command, command_id, payload.result, settings=principal.settings
    )
    return {"result": payload.result, "replayed": replayed}


# ─── Обновление воркера (контракт; на этапе 0 обновлений нет) ────────────────
@router.get("/update/manifest", status_code=204)
async def update_manifest(
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> Response:
    """Контракт централизованного обновления (ADR-012).

    На этапе 0 обновление НЕ реализуется — эндпоинт всегда отвечает 204
    «обновлений нет». Он существует, чтобы воркер пилота не пришлось потом
    переписывать; притворяться работающим механизмом он не должен.
    """
    return Response(status_code=204)


# ─── Reconciliation ──────────────────────────────────────────────────────────
@router.post("/reconcile", response_model=ReconcileResponse)
async def reconcile(
    payload: ReconcileRequest,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> ReconcileResponse:
    """Сверка после рестарта любой из сторон.

    Воркер не принимает решений о судьбе задания сам — он спрашивает и
    исполняет; `action` — закрытый enum.
    """
    settings = principal.settings
    verdicts: list[ReconcileJobVerdict] = []
    unknown: list[str] = []
    superseded: list[str] = []

    terminal_states = (
        JobState.COMPLETED.value,
        JobState.CANCELLED.value,
        JobState.FAILED.value,
        JobState.SUPERSEDED_RESULT_RECEIVED.value,
    )

    for known in payload.known_jobs:
        job = await database.run_db(
            repositories.get_job, known.job_id, settings=settings
        )
        if job is None or job.get("assigned_worker_id") != principal.worker_id:
            unknown.append(known.job_id)
            continue
        attempt_valid = job["attempt_id"] == known.attempt_id
        cursor = await database.run_db(
            repositories.get_cursor, job["job_id"], job["attempt_id"], settings=settings
        )
        is_terminal = job["state"] in terminal_states
        if not attempt_valid:
            action = "stop_superseded"
            superseded.append(known.job_id)
        elif is_terminal:
            action = "await_operator"
        elif known.result_ready or job["state"] in (
            JobState.COMPLETED_LOCALLY.value,
            JobState.RESULT_UPLOADING.value,
        ):
            action = "upload_result"
        elif known.local_state == "running" and not known.processes_alive:
            # Процесс не пережил рестарт. Сказать «continue» нечестно: продолжать
            # нечего. Центр НЕ объявляет провал сам (ребра running → failed для
            # роли center нет вовсе, I-01/I-02) — решение за оператором, а о
            # смерти процесса отчитывается сам воркер своим событием job_failed.
            action = "await_operator"
        else:
            action = "continue"

        hint = None
        if action == "upload_result" and known.result_hash:
            session = await database.run_db(
                repositories.find_open_upload,
                job["job_id"],
                job["attempt_id"],
                package_service.normalize_hash(known.result_hash),
                settings=settings,
            )
            if session is not None:
                hint = await database.run_db(
                    upload_service.session_info, session, settings=settings
                )

        # Результат принят — сообщаем retention_until даже если воркер был
        # офлайн в момент приёма и пропустил retention_update в heartbeat.
        # Без этого пакет остаётся retention_unconfirmed навсегда (I-08).
        result_accepted = job["state"] == JobState.COMPLETED.value and bool(
            job.get("validated_at")
        )

        verdicts.append(
            ReconcileJobVerdict(
                job_id=job["job_id"],
                attempt_id=job["attempt_id"],
                center_state=JobState(job["state"]),
                attempt_valid=attempt_valid,
                expected_next_seq=cursor + 1,
                action=action,
                upload_hint=hint,
                execution_token_valid=attempt_valid and not is_terminal,
                result_accepted=result_accepted,
                retention_until=job.get("retention_until") if result_accepted else None,
            )
        )

    # Задания, закреплённые за этим воркером, о которых он НЕ знает, а работа
    # ещё не начиналась. Так выглядит потерянный ответ на /jobs/next: задание
    # уже не `assigned`, поэтому опрос его не вернёт, а воркер не знает ни
    # job_id, ни токена — раньше это навсегда блокировало пару (проект,
    # версия) через ux_jobs_active_project. Возвращаем в очередь.
    known_ids = {k.job_id for k in payload.known_jobs}
    reoffered = await database.run_db(
        job_service.reoffer_unknown_jobs,
        worker_id=principal.worker_id,
        known_job_ids=known_ids,
        settings=settings,
    )

    commands = await database.run_db(
        repositories.pending_commands, principal.worker_id, settings=settings
    )
    return ReconcileResponse(
        server_time=time.time(),
        jobs=verdicts,
        unknown_jobs=unknown,
        superseded_jobs=superseded,
        reoffered_jobs=reoffered,
        pending_commands=len(commands),
    )
