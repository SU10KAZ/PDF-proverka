"""Аутентификация воркеров (bearer-токен) и владение попыткой (execution token).

Два независимых контура (§20.2 техпроекта), смешивать их нельзя:
  * `/api/v1/worker/*` — bearer-токен машины. Портальная cookie здесь не
    работает и не должна;
  * `/api/workers/*`  — портальная cookie оператора. Токен воркера туда не
    даёт доступа.

Почему sha256, а не bcrypt/pbkdf2: токен — 256 бит энтропии, а не пароль.
Перебор невозможен по построению, а pbkdf2 на каждом heartbeat — лишняя
нагрузка. Для паролей операторов остаётся pbkdf2_sha256 в core/portal_auth.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import Header, HTTPException, Request

from backend.app.models.distributed_workers import RegistrationStatus
from backend.app.services.distributed_workers import database, repositories
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersConfigError,
    DistributedWorkersSettings,
    get_settings,
)

TOKEN_PREFIX = "wtk_"
EXEC_TOKEN_PREFIX = "etk_"
CLAIM_PREFIX = "clm_"


def generate_token() -> str:
    return TOKEN_PREFIX + secrets.token_urlsafe(32)


def generate_execution_token() -> str:
    return EXEC_TOKEN_PREFIX + secrets.token_urlsafe(24)


def generate_claim_secret() -> str:
    """Одноразовый секрет получения токена. Хранится на центре только хэшем."""
    return CLAIM_PREFIX + secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def constant_time_equals(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def verify_bootstrap_secret(provided: Optional[str]) -> None:
    """Проверить одноразовый секрет регистрации.

    Отсутствие секрета в конфигурации — ошибка конфигурации (503), а не
    «пропустить всех»: эндпоинт регистрации публичный.
    """
    settings = get_settings()
    try:
        expected = settings.require_bootstrap_secret()
    except DistributedWorkersConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not provided or not constant_time_equals(provided.strip(), expected):
        raise HTTPException(
            status_code=401,
            detail="Неверный или отсутствующий секрет регистрации воркера.",
        )


@dataclass(frozen=True)
class WorkerPrincipal:
    """Аутентифицированный воркер + снимок его записи."""

    worker_id: str
    instance_id: Optional[str]
    row: dict[str, Any]
    settings: DistributedWorkersSettings


def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


async def require_worker(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    x_worker_id: Optional[str] = Header(default=None),
    x_instance_id: Optional[str] = Header(default=None),
    x_protocol_version: Optional[str] = Header(default=None),
) -> WorkerPrincipal:
    """FastAPI-зависимость: аутентификация воркера по bearer-токену."""
    settings = get_settings()
    if not settings.enabled:
        raise HTTPException(status_code=404, detail="Подсистема воркеров отключена.")

    token = _extract_bearer(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Требуется Authorization: Bearer <token>.")

    if x_protocol_version:
        try:
            client_proto = int(x_protocol_version)
        except ValueError:
            raise HTTPException(status_code=400, detail="X-Protocol-Version: ожидается целое.")
        if client_proto != settings.protocol_version:
            raise HTTPException(
                status_code=426,
                detail=(
                    f"Несовместимая версия протокола: воркер {client_proto}, "
                    f"центр {settings.protocol_version}."
                ),
            )

    worker = await database.run_db(
        repositories.find_worker_by_token_hash, hash_token(token), settings=settings
    )
    if worker is None:
        raise HTTPException(status_code=401, detail="Токен воркера не распознан или отозван.")

    if x_worker_id and x_worker_id != worker["worker_id"]:
        raise HTTPException(status_code=401, detail="X-Worker-Id не соответствует токену.")

    status = worker.get("registration_status")
    if status == RegistrationStatus.REVOKED.value:
        raise HTTPException(status_code=403, detail="Доступ воркера отозван.")
    if status != RegistrationStatus.APPROVED.value:
        raise HTTPException(
            status_code=403,
            detail="Регистрация воркера ещё не одобрена оператором.",
        )

    request.state.distributed_worker_id = worker["worker_id"]
    return WorkerPrincipal(
        worker_id=worker["worker_id"],
        instance_id=x_instance_id or worker.get("instance_id"),
        row=worker,
        settings=settings,
    )


def require_execution_token(job_row: dict[str, Any], provided: Optional[str]) -> None:
    """Проверить право действовать от имени ТЕКУЩЕЙ попытки задания.

    Несовпадение → 409: попытка отозвана, воркер обязан остановиться (I-05).
    """
    expected_hash = job_row.get("execution_token_sha256")
    if not expected_hash:
        raise HTTPException(status_code=409, detail="Попытка не выпущена.")
    if not provided or not constant_time_equals(hash_token(provided.strip()), expected_hash):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "attempt_superseded",
                "message": "Попытка отозвана или токен неверен",
                "current_attempt": job_row.get("attempt_id"),
            },
        )
