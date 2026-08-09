"""Административный API «Аудит-воркеры»: `/api/workers/*`.

Контур оператора: обычная портальная cookie-сессия (PortalAuthMiddleware).
Токен воркера сюда доступа НЕ даёт — контуры разделены намеренно (§20.2).

Права проверяются на КАЖДОМ маршруте зависимостью из
`services/distributed_workers/authorization.py`: три уровня — `view`,
`operate`, `admin`. UI границей безопасности не является (R-01): скрытая
кнопка не защищает ничего, а прямой HTTP-запрос обязан получить 401/403
независимо от того, что нарисовано на экране.

Все опасные действия (одобрение, отзыв, ротация токена, выдача задания)
пишутся в сквозной журнал действий с kind="worker", а решения оператора —
ещё и в таблицу worker_admin_actions с actor'ом ИЗ СЕССИИ.
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse

from backend.app.core import action_log
from backend.app.models.distributed_workers import (
    ApproveRequest,
    CancelAttemptRequest,
    CreateAttemptRequest,
    CreateTestJobRequest,
    JobState,
    MarkAttemptLostRequest,
    RemoteAuditLaunchRequest,
    RequestDeletionRequest,
    SubscriptionAccountUpdate,
    WorkerProviderGroupUpdate,
)
from backend.app.services.distributed_workers import (
    attempt_service,
    authorization,
    database,
    event_service,
    identifiers,
    job_service,
    progress_service,
    provider_accounts,
    provider_view,
    registration_service,
    repositories,
    slots,
    worker_registry,
)
from backend.app.services.distributed_workers.authorization import (
    Actor,
    require_admin,
    require_operator,
    require_view,
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
    """Кто действует. Берётся ИЗ АУТЕНТИФИКАЦИИ, а не из тела запроса (§6).

    Источник — `authorization.resolve_actor`, то есть подписанная портальная
    cookie. `request.state.portal_user` тут больше не читается напрямую:
    единственная точка определения субъекта должна быть одна, иначе роутер и
    журнал однажды разойдутся в том, кто именно нажал кнопку.
    """
    return authorization.actor_of(request).audit_id()


def _source_ip(request: Request, client: Optional[str]) -> Optional[str]:
    """Адрес источника для неизменяемого журнала решений.

    Раньше в поле уезжал первый элемент `X-Forwarded-For`, то есть значение,
    полностью подконтрольное тому, чьи действия журнал и фиксирует: оператор
    с правом `operate` мог подписать своё действие чужим адресом. Списка
    доверенных прокси в приложении нет и завести его тут нечем.

    Поэтому в поле идёт РЕАЛЬНЫЙ адрес соединения, а заявленный клиентом —
    рядом и с явной пометкой «заявлено». Портал ходит через cloudflared, где
    peer почти всегда 127.0.0.1, так что выбросить заголовок совсем нельзя:
    он остаётся единственной подсказкой о настоящем источнике — но подсказкой,
    а не свидетельством.
    """
    claimed = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
    if not claimed or claimed == client:
        return client
    return f"{client or '?'} (заявлено X-Forwarded-For: {claimed[:64]})"


def _audit_meta(request: Request, *, permission: Optional[str] = None) -> dict[str, Any]:
    client = request.client.host if request.client else None
    actor = authorization.actor_of(request)
    return {
        "actor_display_name": actor.display_name or "anonymous",
        "actor_role": actor.role,
        "permission": permission,
        "request_id": request.headers.get("X-Request-Id"),
        "source_ip": _source_ip(request, client),
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
    permission: str,
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
    actor = authorization.actor_of(request)
    await database.run_db(
        repositories.record_admin_action,
        actor_id=_actor(request),
        actor_display_name=str(meta["actor_display_name"]),
        actor_role=actor.role,
        permission=permission,
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
    # Диагностика ролей: САМ факт настройки, без единого имени субъекта.
    # Список пользователей на фронте — это утечка, а не удобство (§7 задания).
    role_config = authorization.load_role_config()
    roles_error = role_config.diagnostics()
    if roles_error and not config_error:
        config_error = roles_error
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
        "portal_auth_enabled": _portal_auth.get_settings().enabled,
        "roles_configured": role_config.configured and role_config.ok,
        "roles_error": roles_error,
        "max_verified_slots": slots.MAX_VERIFIED_SLOTS,
        "config_error": config_error,
    }


@status_router.get("/me")
async def whoami(
    actor: Actor = Depends(authorization.current_actor),
) -> dict[str, Any]:
    """Кто я и что мне позволено в подсистеме.

    Единственный эндпоинт, отвечающий БЕЗ прав: экран обязан уметь честно
    сказать «недостаточно прав», а для этого ответ должен приходить и тому, у
    кого прав нет. Ответ содержит только собственные разрешения вызывающего —
    ни чужих субъектов, ни списков, ни токенов (R-01, §12).
    """
    settings = get_settings()
    view = actor.as_view()
    view["subsystem_enabled"] = settings.enabled
    return view


# ─── Журнал операторских действий ────────────────────────────────────────────
@router.get("/admin-actions")
async def admin_actions(
    job_id: Optional[str] = Query(default=None),
    worker_id: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    actor: Actor = Depends(require_admin),
) -> dict[str, Any]:
    """Полный журнал операторских решений — только администратору (§9).

    Оператору и наблюдателю по-прежнему видны действия ПО КОНКРЕТНОЙ попытке
    (они приходят внутри истории попыток), но сводный журнал по всем воркерам
    и всем субъектам — административные сведения.

    Эндпоинта удаления записей нет намеренно (I-15).
    """
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
async def _worker_view(row: dict[str, Any], settings: Any, *, now: Optional[float] = None):
    """Карточка воркера вместе с занятостью слотов, посчитанной ЦЕНТРОМ."""
    usage = await database.run_db(
        repositories.worker_slot_snapshot, row["worker_id"], settings=settings
    )
    return worker_registry.to_view(row, now=now, usage=usage)


@router.get("")
async def list_workers(actor: Actor = Depends(require_view)) -> dict[str, Any]:
    settings = _settings_or_404()
    rows = await database.run_db(
        worker_registry.refresh_connectivity, settings=settings
    )
    now = time.time()
    workers = [await _worker_view(r, settings, now=now) for r in rows]
    online = sum(1 for w in workers if w["connection_status"] == "online")
    # Свободные слоты в сводке — РАССЧИТАННЫЕ ЦЕНТРОМ, а не заявленные
    # воркером: сумма чужих обещаний не то число, по которому назначают работу.
    free_slots = sum(
        (w.get("slots") or {}).get("center_free_slots", 0)
        for w in workers
        if w["connection_status"] == "online"
    )
    return {
        "workers": workers,
        "summary": {
            "total": len(workers),
            "online": online,
            "free_slots": free_slots,
            "active_jobs": sum(len(w["active_jobs"]) for w in workers),
            "slot_mismatch": sum(
                1 for w in workers if (w.get("slots") or {}).get("slot_count_mismatch")
            ),
            # Заявки, ждущие решения оператора: их нельзя «не заметить» —
            # до одобрения воркер вообще не получает токен.
            "pending": sum(
                1 for w in workers if w["registration_status"] == "pending"
            ),
        },
        "max_verified_slots": slots.MAX_VERIFIED_SLOTS,
        "permissions": sorted(actor.permissions),
        "server_time": now,
    }


@router.get("/audit/targets")
async def audit_targets(actor: Actor = Depends(require_view)) -> dict[str, Any]:
    """Куда можно отправить РЕАЛЬНЫЙ аудит и почему нельзя во всё остальное.

    Несовместимый воркер не просто «не показывается» — он показывается с точной
    причиной. Молчаливое «недоступен» оператор не может ни понять, ни исправить.
    """
    from backend.app.pipeline.execution import registry as execution_registry
    from backend.app.services.distributed_workers import audit_job_service

    settings = _settings_or_404()
    enabled, reason = execution_registry.remote_execution_available()
    targets = await database.run_db(
        audit_job_service.list_compatible_workers, settings=settings
    )
    return {
        "remote_execution_enabled": enabled,
        "disabled_reason": None if enabled else reason,
        "profile": audit_job_service.REMOTE_AUDIT_PILOT_V1,
        "center_pipeline_revision": audit_job_service.center_pipeline_revision() or None,
        "audit_slot_limit": audit_job_service.REAL_AUDIT_MAX_SLOTS,
        # Правда, а не пожелание: нормативный этап на воркере не выполняется.
        "norm_stage_location": "center",
        "workers": targets,
        "permissions": sorted(actor.permissions),
        "server_time": time.time(),
    }


@router.post("/audit/launch")
async def audit_launch(
    payload: RemoteAuditLaunchRequest,
    request: Request,
    actor: Actor = Depends(require_operator),
) -> dict[str, Any]:
    """Отправить проект на выбранный audit-worker. Ручной режим, без автовыбора."""
    from backend.app.pipeline.manager import pipeline_manager
    from backend.app.services.distributed_workers import audit_job_service

    settings = _settings_or_404()
    _require_operator_intent(request)
    worker = await database.run_db(
        repositories.get_worker, payload.worker_id, settings=settings
    )
    if worker is None:
        raise HTTPException(status_code=404, detail="Воркер не найден")
    attempts = await database.run_db(
        repositories.attempts_for_worker_nonterminal, payload.worker_id,
        settings=settings,
    )
    report = audit_job_service.compatibility_report(
        worker, settings=settings, active_attempts=attempts
    )
    if not report["compatible"]:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "worker_incompatible",
                "reasons": report["reasons"],
            },
        )
    try:
        job = await pipeline_manager.start_remote_audit(
            payload.project_id,
            worker_id=payload.worker_id,
            version_id=payload.version_id,
            action=payload.action,
            # Атрибуция запуска на чужой VPS — не косметика: она попадает в
            # `logical_jobs.created_by` и в журнал переходов.
            actor=actor.subject or actor.display_name or "operator",
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _audit(request, "remote_audit_launched", worker_id=payload.worker_id)
    return {
        "status": "queued",
        "job": job.model_dump(),
        "worker": {
            "worker_id": report["worker_id"],
            "display_name": report["display_name"],
            "provider_mode": report["provider_mode"],
            "real_llm_enabled": report["real_llm_enabled"],
            "audit_slot_label": report["audit_slot_label"],
        },
        "profile": audit_job_service.REMOTE_AUDIT_PILOT_V1,
        "norm_stage_location": "center",
        "notice": (
            "Пакет проекта соберётся при старте элемента очереди. Нормативный "
            "этап и финальная сборка выполняются на центре. "
            + (
                "Внимание: на воркере включены НАСТОЯЩИЕ Claude/Codex."
                if report["real_llm_enabled"]
                else "На воркере настоящие Claude/Codex отключены — работают "
                     "поддельные провайдеры тестового режима."
            )
        ),
    }


# ─── Провайдеры и учётные записи подписок (этап 11) ──────────────────────────
# ВАЖНО про порядок регистрации: эти маршруты объявлены ДО `/{worker_id}`.
# FastAPI сопоставляет пути в порядке объявления, и `/api/workers/providers`
# ушло бы в `get_worker(worker_id="providers")`, вернув 404 «воркер не найден»
# вместо списка провайдеров.
@router.get("/providers/overview")
async def providers_overview(actor: Actor = Depends(require_view)) -> dict[str, Any]:
    """Провайдеры по воркерам + учётные записи подписок.

    Только чтение. Ни одного обращения к провайдеру отсюда не происходит:
    показываются последние снимки, присланные воркерами в heartbeat.
    """
    settings = _settings_or_404()
    now = time.time()
    states = await database.run_db(
        provider_accounts.list_worker_provider_states, settings=settings
    )
    accounts = await database.run_db(
        provider_view.accounts_overview, settings=settings, now=now
    )
    by_worker: dict[str, list[dict[str, Any]]] = {}
    for state in states:
        by_worker.setdefault(state["worker_id"], []).append(state)
    return {
        "worker_providers": by_worker,
        "accounts": accounts,
        "providers": list(provider_accounts.PROVIDERS),
        "account_kinds": list(provider_accounts.ACCOUNT_KINDS),
        "policy_states": list(provider_accounts.POLICY_STATES),
        "recurrences": list(provider_accounts._RECURRENCE_ALLOWED),
        "low_threshold_pct": (
            settings.quota_low_threshold_pct
            if settings.quota_low_threshold_pct > 0 else None
        ),
        "quota_stale_sec": settings.quota_stale_sec,
        # Явно и проверяемо: автоматической выдачи заданий нет.
        "auto_dispatch_enabled": False,
        "permissions": sorted(actor.permissions),
        "server_time": now,
    }


@router.get("/providers/accounts/{account_id}/history")
async def provider_account_history(
    account_id: str,
    limit: int = Query(default=200, ge=1, le=2000),
    actor: Actor = Depends(require_view),
) -> dict[str, Any]:
    """Ограниченная история снимков квоты учётной записи (§24)."""
    settings = _settings_or_404()
    account = await database.run_db(
        provider_accounts.get_account, account_id, settings=settings
    )
    if account is None:
        raise HTTPException(status_code=404, detail="Учётная запись не найдена.")
    rows = await database.run_db(
        provider_accounts.account_history, account_id, limit=limit, settings=settings
    )
    return {
        "account_id": account_id,
        "provider": account["provider"],
        "account_group_id": account["account_group_id"],
        "history": rows,
        "retention_days": settings.quota_history_retention_days,
        "server_time": time.time(),
    }


@router.put("/providers/accounts/{account_id}")
async def update_provider_account(
    account_id: str,
    payload: SubscriptionAccountUpdate,
    request: Request,
    actor: Actor = Depends(require_operator),
) -> dict[str, Any]:
    """Ручные поля учётной записи. Право `operate`; viewer получает 403.

    Что здесь НЕЛЬЗЯ изменить в принципе: токен, пароль, cookie. Их нет ни в
    модели запроса, ни в таблице — записать некуда.
    """
    settings = _settings_or_404()
    _require_intent_header(request)
    account = await database.run_db(
        provider_accounts.get_account, account_id, settings=settings
    )
    if account is None:
        raise HTTPException(status_code=404, detail="Учётная запись не найдена.")
    fields = payload.model_dump(exclude_unset=True)
    try:
        if fields.pop("clear_manual_reset", False):
            # Стирание — отдельная операция: в upsert `None` означает «не
            # трогать», иначе форма без поля молча удаляла бы ручную дату.
            await database.run_db(
                provider_accounts.clear_manual_reset, account_id, settings=settings
            )
        updated = await database.run_db(
            provider_accounts.upsert_account,
            provider=account["provider"],
            account_group_id=account["account_group_id"],
            settings=settings,
            **fields,
        )
    except provider_accounts.ProviderAccountError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    changed = sorted(fields)
    await _record_admin_action(
        request,
        action_type="provider_account_update",
        permission=authorization.PERM_OPERATE,
        settings=settings,
        reason=f"поля: {', '.join(changed) or '—'}",
        requested_state={"account_id": account_id, "changed_fields": changed},
    )
    _audit(request, "provider_account_update", account_id=account_id, fields=changed)
    return {"account": updated, "server_time": time.time()}


@router.put("/{worker_id}/providers/{provider}/account-group")
async def bind_worker_provider_group(
    worker_id: str,
    provider: str,
    payload: WorkerProviderGroupUpdate,
    request: Request,
    actor: Actor = Depends(require_operator),
) -> dict[str, Any]:
    """Привязать провайдера воркера к общей учётной записи (§15).

    Делает это ОПЕРАТОР вручную. Автоматически связать два VPS «по одному и
    тому же аккаунту» нельзя: для этого пришлось бы сверять секретные данные,
    что прямо запрещено (§8).
    """
    settings = _settings_or_404()
    _require_intent_header(request)
    row = await database.run_db(repositories.get_worker, worker_id, settings=settings)
    if row is None:
        raise HTTPException(status_code=404, detail="Воркер не найден.")
    try:
        result = await database.run_db(
            provider_accounts.set_worker_provider_group,
            worker_id=worker_id,
            provider=provider,
            account_group_id=payload.account_group_id,
            settings=settings,
        )
    except provider_accounts.ProviderAccountError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await _record_admin_action(
        request,
        action_type="provider_account_group_bind",
        permission=authorization.PERM_OPERATE,
        settings=settings,
        worker_id=worker_id,
        reason=f"{provider} → {result['account_group_id'] or 'без группы'}",
        requested_state=result,
    )
    _audit(
        request, "provider_account_group_bind",
        worker_id=worker_id, provider=provider,
        account_group_id=result["account_group_id"],
    )
    return {"binding": result, "server_time": time.time()}


@router.get("/providers/ranking-preview")
async def providers_ranking_preview(
    provider: str = Query(...),
    actor: Actor = Depends(require_view),
) -> dict[str, Any]:
    """Предпросмотр порядка воркеров под будущее задание (§26).

    Ничего не назначает и назначить не может: автоматическая выдача заданий на
    этом этапе выключена, а функция не имеет доступа к очереди.
    """
    settings = _settings_or_404()
    rows = await database.run_db(repositories.list_workers, settings=settings)
    now = time.time()
    views = [await _worker_view(r, settings, now=now) for r in rows]
    try:
        return await database.run_db(
            provider_view.rank_workers_for_future_job,
            provider=provider,
            settings=settings,
            workers=views,
            now=now,
        )
    except provider_accounts.ProviderAccountError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{worker_id}")
async def get_worker(
    worker_id: str, actor: Actor = Depends(require_view)
) -> dict[str, Any]:
    settings = _settings_or_404()
    row = await database.run_db(repositories.get_worker, worker_id, settings=settings)
    if row is None:
        raise HTTPException(status_code=404, detail="Воркер не найден.")
    jobs = await database.run_db(
        repositories.list_jobs, worker_id=worker_id, settings=settings
    )
    return {
        "worker": await _worker_view(row, settings),
        "jobs": [job_service.to_view(j, settings=settings) for j in jobs],
    }


@router.post("/{worker_id}/approve")
async def approve_worker(
    worker_id: str,
    payload: ApproveRequest,
    request: Request,
    actor: Actor = Depends(require_admin),
) -> dict[str, Any]:
    settings = _settings_or_404()
    _require_intent_header(request)
    limit = slots.normalize_max_slots(
        payload.configured_max_slots, source="настройка оператора"
    )
    try:
        row = await database.run_db(
            registration_service.approve_worker,
            worker_id=worker_id,
            display_name=payload.display_name,
            configured_max_slots=limit.value,
            settings=settings,
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit(request, "worker_approved", worker_id=worker_id)
    await _record_admin_action(
        request, action_type="approve_worker", permission=authorization.PERM_ADMIN,
        worker_id=worker_id, settings=settings,
        requested_state={"configured_max_slots": payload.configured_max_slots},
        result={"configured_max_slots": limit.value, "notice": limit.notice},
    )
    return {
        "worker": await _worker_view(row, settings),
        "configured_max_slots": limit.value,
        # Молчаливое зажатие «5 → 2» оставило бы оператора в уверенности, что у
        # него пять слотов. Предупреждение возвращается прямо в ответе.
        "slot_limit_notice": limit.notice,
    }


@router.post("/{worker_id}/reject")
async def reject_worker(
    worker_id: str, request: Request, actor: Actor = Depends(require_admin)
) -> dict[str, Any]:
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
        request, action_type="reject_worker", permission=authorization.PERM_ADMIN,
        worker_id=worker_id, settings=settings,
    )
    return {"worker": await _worker_view(row, settings)}


@router.post("/{worker_id}/revoke")
async def revoke_worker(
    worker_id: str, request: Request, actor: Actor = Depends(require_admin)
) -> dict[str, Any]:
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
        request, action_type="revoke_worker", permission=authorization.PERM_ADMIN,
        worker_id=worker_id, settings=settings,
    )
    return {"worker": await _worker_view(row, settings)}


@router.post("/{worker_id}/rotate-token")
async def rotate_token(
    worker_id: str, request: Request, actor: Actor = Depends(require_admin)
) -> dict[str, Any]:
    """Выдать новый токен. Опасно: токен показывается один раз и открытым текстом.

    Поэтому здесь стоит тот же гейт намерения, что и на остальных меняющих
    состояние ручках. Что он даёт честно: межсайтовый запрос (форма с чужого
    сайта, «простой» POST) заголовки не поставит. Чего он НЕ даёт: защиты от
    XSS в самой странице — same-origin скрипт выставит любые заголовки. От
    XSS защищает только то, что страница нигде не собирает HTML из данных.
    """
    settings = _settings_or_404()
    idempotency_key = _require_operator_intent(request)
    # Ключ здесь обязан РАБОТАТЬ, а не просто требоваться. Ротация гасит все
    # прежние токены разом, поэтому «ответ потерялся, клиент повторил запрос»
    # (таймаут прокси, вотчдог, двойной клик) убило бы токен, который админ
    # уже прописал на VPS: воркер ушёл бы в 401. Повтор по тому же ключу
    # останавливаем ДО rotate_token. Показать токен второй раз нельзя — он
    # нигде не хранится в открытом виде, и это правильно.
    already = await database.run_db(
        repositories.find_admin_action_by_key,
        action_type="rotate_worker_token",
        idempotency_key=idempotency_key,
        settings=settings,
    )
    if already:
        raise HTTPException(
            status_code=409,
            detail="Этот Idempotency-Key уже использован для ротации токена "
                   "(действие " + str(already.get("action_id") or "?") + "). "
                   "Токен показывается один раз и повторно не выдаётся. Если "
                   "он потерян — повторите ротацию с НОВЫМ ключом.",
        )
    try:
        row, token = await database.run_db(
            registration_service.rotate_token, worker_id=worker_id, settings=settings
        )
    except registration_service.RegistrationConflict as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit(request, "worker_token_rotated", worker_id=worker_id)
    await _record_admin_action(
        request, action_type="rotate_worker_token", permission=authorization.PERM_ADMIN,
        worker_id=worker_id, settings=settings, idempotency_key=idempotency_key,
        result={"note": "старый токен отозван атомарно, новый показан один раз"},
    )
    return {
        "worker": await _worker_view(row, settings),
        "worker_token": token,
        "note": "Старый токен отозван немедленно. Пропишите новый на воркере "
                "и перезапустите его.",
    }


# ─── Задания ─────────────────────────────────────────────────────────────────
@router.get("/jobs/list")
async def list_jobs(
    worker_id: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    actor: Actor = Depends(require_view),
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
async def create_test_job(
    payload: CreateTestJobRequest,
    request: Request,
    actor: Actor = Depends(require_operator),
) -> dict[str, Any]:
    """Ручная выдача БЕЗОПАСНОГО тестового задания конкретному воркеру.

    Единственный доступный тип — test_pipeline_v1. Ни команды, ни argv, ни
    путей в задании нет: воркер строит фиксированный argv сам (§4 задания).

    Создание разрешено и при занятых слотах: задание остаётся `assigned` и
    ждёт освобождения. Ответ честно говорит, пойдёт оно в работу сразу или
    встанет в очередь (§31 задания) — «кнопка неактивна» скрыла бы от
    оператора то, что система и так умеет.
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
        request, action_type="create_job", permission=authorization.PERM_OPERATE,
        worker_id=payload.worker_id,
        job_id=job["job_id"], attempt_id=job["attempt_id"],
        requested_state={"project_external_id": payload.project_id},
        settings=settings,
    )
    view = job_service.to_view(job, settings=settings)
    # execution_token наружу оператору не отдаём — он предназначен только воркеру.
    view.pop("_execution_token_plain", None)
    view.pop("_manifest", None)

    worker_row = await database.run_db(
        repositories.get_worker, payload.worker_id, settings=settings
    )
    usage = await database.run_db(
        repositories.worker_slot_snapshot, payload.worker_id, settings=settings
    )
    limit = slots.effective_limit(
        worker_row or {}, protocol_version=settings.protocol_version
    )
    waiting = usage.reserved >= limit.value
    return {
        "job": view,
        "slots": slots.build_slot_view(worker_row or {}, usage, limit),
        "will_wait_for_slot": waiting,
        "queue_note": (
            f"Свободных слотов нет ({usage.reserved} из {limit.value}) — задание "
            "встанет в очередь и уйдёт в работу после освобождения слота."
            if waiting
            else None
        ),
    }


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, actor: Actor = Depends(require_view)) -> dict[str, Any]:
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
    actor: Actor = Depends(require_view),
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
    actor: Actor = Depends(require_view),
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
async def download_result(job_id: str, actor: Actor = Depends(require_view)):
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
async def list_attempts(
    job_id: str, actor: Actor = Depends(require_view)
) -> dict[str, Any]:
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
    job_id: str,
    attempt_id: str,
    payload: CancelAttemptRequest,
    request: Request,
    actor: Actor = Depends(require_operator),
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
            audit=_audit_meta(request, permission=authorization.PERM_OPERATE),
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
    job_id: str,
    attempt_id: str,
    payload: MarkAttemptLostRequest,
    request: Request,
    actor: Actor = Depends(require_operator),
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
            audit=_audit_meta(request, permission=authorization.PERM_OPERATE),
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
    job_id: str,
    payload: CreateAttemptRequest,
    request: Request,
    actor: Actor = Depends(require_operator),
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
            audit=_audit_meta(request, permission=authorization.PERM_OPERATE),
            accept_capacity_risk=payload.accept_capacity_risk,
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
    job_id: str,
    attempt_id: str,
    payload: RequestDeletionRequest,
    request: Request,
    actor: Actor = Depends(require_operator),
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
            audit=_audit_meta(request, permission=authorization.PERM_OPERATE),
            settings=settings,
        )
    except attempt_service.ConfirmationRequired as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except attempt_service.OperatorError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _audit(request, "worker_data_deletion_requested", job_id=job_id, attempt_id=attempt_id)
    return result


@router.get("/jobs/{job_id}/attempts/{attempt_id}/result")
async def download_attempt_result(
    job_id: str, attempt_id: str, actor: Actor = Depends(require_view)
):
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
