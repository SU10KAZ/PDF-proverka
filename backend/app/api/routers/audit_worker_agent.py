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
    COMMAND_PAYLOAD_MODELS,
    TERMINAL_JOB_STATES,
    AcceptRequest,
    ClaimRequest,
    ClaimResponse,
    CommandAckRequest,
    CommandsNextRequest,
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
    WorkerCommandType,
)
from backend.app.services.distributed_workers import (
    attempt_service,
    auth,
    database,
    event_service,
    job_service,
    package_service,
    registration_service,
    repositories,
    slots,
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


async def _resolve_attempt(
    job_id: str,
    principal: auth.WorkerPrincipal,
    execution_token: Optional[str],
) -> tuple[dict[str, Any], bool]:
    """Найти ПОПЫТКУ, от имени которой пришёл воркер.

    Раньше опознавали задание, а токен сверяли с текущей попыткой — из-за
    этого вернувшийся старый воркер получал 409 и деть готовый результат было
    некуда. Теперь попытка ищется ПО ХЭШУ ТОКЕНА: у каждой свой, и старый
    воркер попадает в контур собственной попытки.

    Второй элемент кортежа — «попытка активна». Неактивной (отменённой,
    признанной потерянной, вытесненной) разрешено сдать события и архив в
    СВОЮ историю, но менять актуальное состояние задания она не может (I-07).
    """
    settings = principal.settings
    attempt: Optional[dict[str, Any]] = None
    if execution_token:
        attempt = await database.run_db(
            repositories.find_attempt_by_token_hash,
            job_id,
            auth.hash_token(execution_token.strip()),
            settings=settings,
        )
    if attempt is None:
        job = await database.run_db(repositories.get_job, job_id, settings=settings)
        if job is None:
            raise HTTPException(status_code=404, detail="Задание не найдено.")
        if job.get("assigned_worker_id") != principal.worker_id:
            raise HTTPException(
                status_code=403, detail="Задание закреплено за другим воркером."
            )
        auth.require_execution_token(job, execution_token)
        attempt = job
    if attempt.get("assigned_worker_id") != principal.worker_id:
        raise HTTPException(
            status_code=403, detail="Попытка закреплена за другим воркером."
        )
    return attempt, (attempt.get("attempt_disposition") or "active") == "active"


async def _load_job_for_worker(
    job_id: str, principal: auth.WorkerPrincipal, execution_token: Optional[str]
) -> dict[str, Any]:
    """Активная попытка задания. Неактивная → 409 «попытка отозвана»."""
    attempt, active = await _resolve_attempt(job_id, principal, execution_token)
    if not active:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "attempt_superseded",
                "message": "Попытка отозвана оператором",
                "attempt_disposition": attempt.get("attempt_disposition"),
                "current_attempt": attempt.get("current_attempt_id"),
            },
        )
    return attempt


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
        executor=payload.executor.model_dump() if payload.executor else None,
        disk=payload.disk.model_dump() if payload.disk else None,
        max_verified_slots=payload.max_verified_slots,
        active_local_jobs=payload.active_local_jobs,
        running_processes=payload.running_processes,
        locally_reserved_slots=payload.locally_reserved_slots,
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
    # Критическая нехватка диска блокирует ВЫДАЧУ новых заданий. Текущие
    # продолжают работать, ничего не удаляется (§12.5).
    #
    # Здесь же гасится «работа есть» при исчерпанных слотах: иначе агент ходил
    # бы за заданием, которое центр отдать не может, и получал 409 по кругу.
    if has_work:
        fresh = await database.run_db(
            repositories.get_worker, principal.worker_id, settings=settings
        )
        allowed, _why = worker_registry.can_receive_jobs(fresh or principal.row)
        if not allowed:
            has_work = False
        else:
            usage = await database.run_db(
                repositories.worker_slot_snapshot, principal.worker_id, settings=settings
            )
            limit = slots.effective_limit(
                fresh or principal.row, protocol_version=settings.protocol_version
            )
            if usage.reserved >= limit.value:
                has_work = False
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
    """Внеочередной снимок ресурсов (обычно едет внутри heartbeat).

    Снимок обязательно проходит тот же санитайзер, что и в heartbeat. Без него
    воркер мог записать в колонку любой мусор (`{"executor": "PWNED"}`), а
    операторский экран падал на нём 500-й — и не для одного воркера, а для
    всего списка, потому что `to_view` вызывается в цикле.
    """
    clean = worker_registry.sanitize_resource_snapshot(snapshot)
    await database.run_db(
        repositories.record_resource_snapshot,
        principal.worker_id,
        clean,
        settings=principal.settings,
    )
    await database.run_db(
        repositories.update_worker_fields,
        principal.worker_id,
        {"resource_snapshot": json.dumps(clean, ensure_ascii=False)},
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
    # Тот же гейт, что и при создании задания. Проверяется здесь ещё раз,
    # потому что воркер опрашивает центр сам и мог не посмотреть на ответ
    # heartbeat: критический диск не должен получать новую работу (§12.5).
    fresh = await database.run_db(
        repositories.get_worker, principal.worker_id, settings=settings
    )
    ok, why = worker_registry.can_receive_jobs(fresh or principal.row)
    if not ok and "диск" in why.lower():
        raise HTTPException(status_code=409, detail=why)

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
            #
            # Переписывать хэш можно ТОЛЬКО у той самой попытки, что лежит в
            # кэше, и только пока она жива и принадлежит этому воркеру. Раньше
            # запись шла по job_id, то есть в «текущую попытку задания»: воркер,
            # чью попытку признали потерянной, повтором старого ключа
            # перевыпускал токен НОВОЙ попытки — её законный исполнитель
            # получал 409 на всех ручках и не мог сдать готовый результат.
            cached = json.loads(prior["response_json"])
            cached_attempt = str(cached.get("attempt_id") or "")
            row = (
                await database.run_db(
                    repositories.get_attempt, cached_attempt, settings=settings
                )
                if cached_attempt
                else None
            )
            if (
                row is None
                or row.get("attempt_disposition") != "active"
                or row.get("assigned_worker_id") != principal.worker_id
                or row.get("execution_state") in {s.value for s in TERMINAL_JOB_STATES}
            ):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "idempotency_key_stale",
                        "message": (
                            "Задание этого ключа больше не принадлежит воркеру. "
                            "Повторите запрос с новым Idempotency-Key."
                        ),
                    },
                )
            replay_token = auth.generate_execution_token()
            await database.run_db(
                repositories.update_attempt_fields,
                cached_attempt,
                {"execution_token_sha256": auth.hash_token(replay_token)},
                settings=settings,
            )
            cached["execution_token"] = replay_token
            return JSONResponse(status_code=int(prior["status_code"]), content=cached)

    # Сколько центр готов держать на ЭТОМ воркере одновременно. Заявленный
    # воркером `free_slots` — не лимит, а «сколько возьму ещё»: он уходит
    # отдельной подсказкой и может лимит только понизить (S-15).
    limit = slots.effective_limit(
        fresh or principal.row,
        protocol_version=settings.protocol_version,
        executor_status=payload.executor_status or None,
    )

    deadline = time.monotonic() + min(payload.wait_sec, settings.long_poll_sec)
    job: Optional[dict[str, Any]] = None
    slot_block: Optional[repositories.SlotLimitReached] = None
    while True:
        try:
            job = await database.run_db(
                repositories.claim_next_job_for_worker,
                principal.worker_id,
                limit_override=limit.value,
                worker_free_hint=payload.free_slots,
                settings=settings,
            )
            slot_block = None
        except repositories.SlotLimitReached as exc:
            # Задание есть, но слотов нет. Это НЕ ошибка задания: оно остаётся
            # `assigned` и уйдёт в работу после освобождения слота (§18).
            job, slot_block = None, exc
        if job is not None or time.monotonic() >= deadline:
            break
        # Пауза вне транзакции: длинное ожидание не держит БД (§19.6).
        await asyncio.sleep(1.0)

    if job is None:
        if slot_block is not None:
            return JSONResponse(
                status_code=409,
                content={
                    "error": "no_free_slots",
                    "message": str(slot_block),
                    "effective_limit": limit.value,
                    "limit_binding": limit.binding,
                    "occupied": getattr(slot_block.usage, "occupied", None),
                    "unproven_remote": getattr(slot_block.usage, "unproven", None),
                },
            )
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
    # Переходы адресуются ПОПЫТКОЙ, а не заданием. Авторизация здесь идёт по
    # токену попытки, а запись по job_id уходила в «текущую попытку»: если
    # между проверкой и записью оператор успевал признать попытку потерянной и
    # создать новую, старый воркер менял состояние ЧУЖОЙ активной попытки.
    if job["state"] == JobState.SOURCE_UPLOADING.value:
        job = await database.run_db(
            job_service.transition,
            attempt_id=job["attempt_id"],
            to_state=JobState.SOURCE_READY,
            actor="worker",
            reason="sha256 исходного пакета сошёлся",
            settings=principal.settings,
        )
    try:
        job = await database.run_db(
            job_service.transition,
            attempt_id=job["attempt_id"],
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
            attempt_id=job["attempt_id"],
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
    """Приём непрерывного пакета событий. Разрыв → 409 с ожидаемым seq.

    События принимаются и от ОТОЗВАННОЙ попытки: они уходят в её собственную
    историю и не трогают актуальную (I-07). Отказать здесь означало бы
    потерять диагностику именно там, где она нужнее всего.
    """
    job, _active = await _resolve_attempt(payload.job_id, principal, x_execution_token)
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
    # Отозванная попытка тоже вправе передать готовый архив — он ляжет в
    # superseded_results и не станет результатом задания (§5.5).
    job, _active = await _resolve_attempt(payload.job_id, principal, x_execution_token)
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
            attempt_id=job["attempt_id"],
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
    # читал состояние чужой выгрузки, зная только upload_id. Привязка идёт к
    # ПОПЫТКЕ сессии, а не к текущей попытке задания: у отозванной попытки своя
    # выгрузка, и её владелец — прежний воркер.
    attempt = await database.run_db(
        repositories.get_attempt, session["attempt_id"], settings=principal.settings
    )
    if attempt is None or attempt.get("assigned_worker_id") != principal.worker_id:
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
    # Через _resolve_attempt — так же, как все остальные ручки по заданию:
    # здесь не хватало ни привязки к принципалу, ни проверки попытки.
    job, _active = await _resolve_attempt(
        session["job_id"], principal, x_execution_token
    )
    if session["attempt_id"] != job["attempt_id"]:
        raise HTTPException(
            status_code=409,
            detail={"error": "attempt_superseded", "current_attempt": job["attempt_id"]},
        )

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
    job, active = await _resolve_attempt(
        session["job_id"], principal, x_execution_token
    )
    if session["attempt_id"] != job["attempt_id"]:
        raise HTTPException(
            status_code=409,
            detail={"error": "attempt_superseded", "current_attempt": job["attempt_id"]},
        )

    # Повторный complete: результат уже принят — возвращаем то же самое.
    if session.get("status") == "verified":
        fresh = await database.run_db(
            repositories.get_attempt, job["attempt_id"], settings=settings
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

    # Попытка не публикуется (провалена, отменена или отозвана оператором):
    # результат принимаем на ХРАНЕНИЕ. Иначе готовый архив некуда деть, и он
    # висит на воркере как retention_unconfirmed навсегда.
    if not active or job["state"] in (JobState.FAILED.value, JobState.CANCELLED.value):
        stored = await database.run_db(
            job_service.store_unpublished_result,
            job=job,
            archive=archive,
            settings=settings,
            reason=(
                "результат отозванной попытки"
                if not active
                else "результат получен после провала/отмены попытки"
            ),
            expected_hash=payload.sha256,
            expected_size=payload.total_size,
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
                "storage_class": "superseded",
                "reason": (
                    "Результат устаревшей попытки — автоматически не используется"
                ),
            },
            server_time=time.time(),
            retention_until=stored.get("retention_until"),
        )

    try:
        job = await database.run_db(
            job_service.catch_up_to_result_received,
            attempt_id=job["attempt_id"],
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
    except job_service.AttemptNoLongerActive:
        # Оператор отозвал попытку, пока центр собирал её архив. Публиковать
        # такой результат нельзя, но и терять готовую работу тоже: кладём на
        # хранение — ровно как для попытки, отозванной до начала приёмки.
        stored = await database.run_db(
            job_service.store_unpublished_result,
            job=job,
            archive=archive,
            settings=settings,
            reason="попытка отозвана оператором во время приёмки результата",
            expected_hash=payload.sha256,
            expected_size=payload.total_size,
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
                "storage_class": "superseded",
                "reason": "Попытка отозвана во время приёмки — результат сохранён",
            },
            server_time=time.time(),
            retention_until=stored.get("retention_until"),
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
def _serialize_commands(
    items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    """Отдать только те команды, чья нагрузка проходит строгую схему.

    Валидация повторяется здесь, хотя центр проверял её при постановке: если
    в таблицу когда-нибудь попадёт строка с чужим типом или лишним полем,
    воркер её не увидит вовсе. Каждый рубеж держит оборону сам (I-10).

    Второй элемент — команды, которые исполнять нельзя. Их НЕ бросаем молча:
    молча отброшенная команда висела бы в очереди вечно, а `has_pending_commands`
    навсегда остался бы true. Вызывающий гасит их машинным ответом.
    """
    out: list[dict[str, Any]] = []
    rejected: list[tuple[str, str]] = []
    for item in items:
        ctype = item.get("command_type")
        model = COMMAND_PAYLOAD_MODELS.get(str(ctype))
        raw = json.loads(item.get("payload") or "{}")
        if model is None:
            rejected.append((item["command_id"], "unsupported_command_type"))
            continue
        try:
            payload = model(**raw).model_dump()
        except Exception:  # noqa: BLE001 — негодная команда не выдаётся
            rejected.append((item["command_id"], "invalid_command_payload"))
            continue
        out.append(
            WorkerCommandOut(
                command_id=item["command_id"],
                command_type=ctype,
                payload=payload,
                created_at=item["created_at"],
                idempotency_key=item["idempotency_key"],
                job_id=item.get("job_id"),
                attempt_id=item.get("attempt_id"),
                expires_at=item.get("expires_at"),
            ).model_dump()
        )
    return out, rejected


async def _deliverable_commands(
    principal: auth.WorkerPrincipal,
) -> list[dict[str, Any]]:
    items = await database.run_db(
        repositories.pending_commands,
        principal.worker_id,
        mark_delivered=True,
        settings=principal.settings,
    )
    commands, rejected = _serialize_commands(items)
    for command_id, code in rejected:
        await database.run_db(
            repositories.ack_command,
            command_id,
            {"status": "error", "detail": {"outcome": code, "actor": "center"}},
            settings=principal.settings,
        )
    return commands


@router.get("/commands")
async def get_commands(
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> dict[str, Any]:
    """Исторический синоним POST /commands/next (этап 0). Оставлен для совместимости."""
    return {"commands": await _deliverable_commands(principal)}


@router.post("/commands/next")
async def commands_next(
    payload: CommandsNextRequest,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> dict[str, Any]:
    """Long-poll за командами. Воркер получает ТОЛЬКО свои и только живые."""
    settings = principal.settings
    deadline = time.monotonic() + min(payload.wait_sec, settings.long_poll_sec)
    while True:
        commands = await _deliverable_commands(principal)
        if commands or time.monotonic() >= deadline:
            return {"commands": commands, "server_time": time.time()}
        await asyncio.sleep(1.0)


@router.post("/commands/{command_id}/ack")
async def ack_command(
    command_id: str,
    payload: CommandAckRequest,
    principal: auth.WorkerPrincipal = Depends(auth.require_worker),
) -> dict[str, Any]:
    # Порядок важен: раньше запись шла ПЕРВОЙ, и чужой воркер, получив 403,
    # успевал погасить команду — настоящий адресат не видел её никогда.
    settings = principal.settings
    existing = await database.run_db(
        repositories.get_command, command_id, settings=settings
    )
    if not existing:
        raise HTTPException(status_code=404, detail="Команда не найдена.")
    if existing.get("worker_id") != principal.worker_id:
        raise HTTPException(status_code=403, detail="Команда адресована другому воркеру.")
    try:
        item, replayed = await database.run_db(
            repositories.ack_command, command_id, payload.result, settings=settings
        )
    except repositories.CommandAckConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    # Последствия подтверждения применяет ЦЕНТР, а не воркер: только он вправе
    # менять состояние попытки. Отмена становится `cancelled` лишь тогда, когда
    # воркер доказал, что исполнять больше нечего (§10, критерий готовности 6).
    #
    # Эффект применяется и на ПОВТОРНОМ подтверждении тоже. Раньше стоял гард
    # `if not replayed`, и падение центра между записью ACK и применением
    # эффекта оставляло попытку в `cancel_requested` навсегда: воркер отмену
    # подтвердил, повтор возвращал replayed=True, эффект не наступал никогда.
    # Обе функции идемпотентны по состоянию, повтор для них безопасен.
    new_state: Optional[str] = None
    ctype = existing.get("command_type")
    if ctype == WorkerCommandType.CANCEL_ATTEMPT.value:
        new_state = await database.run_db(
            attempt_service.apply_cancel_ack,
            command=existing,
            result=payload.result,
            settings=settings,
        )
    elif ctype == WorkerCommandType.DELETE_ATTEMPT_DATA.value:
        await database.run_db(
            attempt_service.apply_deletion_ack,
            command=existing,
            result=payload.result,
            settings=settings,
        )
    return {"result": payload.result, "replayed": replayed, "attempt_state": new_state}


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
        # Ищем ИМЕННО ту попытку, о которой говорит воркер, а не текущую
        # попытку задания: у отозванной попытки своя судьба, свои события и
        # свой результат — и их нельзя ни потерять, ни выдать за актуальные.
        attempt = await database.run_db(
            repositories.get_attempt, known.attempt_id, settings=settings
        )
        if attempt is not None and (
            attempt["job_id"] != known.job_id
            or attempt.get("assigned_worker_id") != principal.worker_id
        ):
            attempt = None
        if attempt is None:
            # Попытки центр не знает. Различаем два случая: задание известно —
            # значит попытка вытеснена (остановить, но НЕ выбрасывать данные);
            # задания нет вовсе — только тогда «забудь».
            job_row = await database.run_db(
                repositories.get_job, known.job_id, settings=settings
            )
            if job_row is None or job_row.get("assigned_worker_id") != principal.worker_id:
                unknown.append(known.job_id)
                continue
            superseded.append(known.job_id)
            cursor = await database.run_db(
                repositories.get_cursor, known.job_id, known.attempt_id, settings=settings
            )
            verdicts.append(
                ReconcileJobVerdict(
                    job_id=known.job_id,
                    attempt_id=known.attempt_id,
                    center_state=None,
                    attempt_valid=False,
                    expected_next_seq=cursor + 1,
                    action="stop_superseded",
                    execution_token_valid=False,
                    attempt_disposition="unknown",
                    current_attempt_id=job_row.get("attempt_id"),
                    event_ingestion_allowed=False,
                    restart_forbidden=True,
                )
            )
            continue
        job = attempt
        disposition = job.get("attempt_disposition") or "active"
        attempt_valid = disposition == "active"
        cursor = await database.run_db(
            repositories.get_cursor, job["job_id"], job["attempt_id"], settings=settings
        )
        is_terminal = job["state"] in terminal_states
        has_result = known.result_ready or job["state"] in (
            JobState.COMPLETED_LOCALLY.value,
            JobState.RESULT_UPLOADING.value,
        )
        if not attempt_valid:
            # Отозванная попытка: процессы остановить, но если результат уже
            # готов — сначала сдать его в контур СВОЕЙ попытки (§5.5).
            action = "upload_result" if (has_result and not is_terminal) else "stop_superseded"
            superseded.append(known.job_id)
        elif is_terminal:
            action = "await_operator"
        elif has_result:
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
        result_accepted = (
            job["state"] == JobState.COMPLETED.value and bool(job.get("validated_at"))
        ) or (
            # Результат отозванной попытки тоже «принят»: он лежит в центре, и
            # воркеру пора заводить срок хранения, иначе пакет вечный.
            job["state"] == JobState.SUPERSEDED_RESULT_RECEIVED.value
            and bool(job.get("result_acknowledged_at"))
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
                attempt_disposition=disposition,
                current_attempt_id=job.get("current_attempt_id"),
                assignment_generation=int(job.get("assignment_generation") or 1),
                # События принимаются всегда: молчание отозванной попытки
                # означало бы потерю диагностики. Они уходят в её историю.
                event_ingestion_allowed=True,
                # Повторный запуск процесса запрещён при любом вердикте:
                # решение о повторе принимает только оператор (§14).
                restart_forbidden=True,
                deletion_status=job.get("retention_state"),
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
