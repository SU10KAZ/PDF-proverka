"""Вход, выход, статус сессии, заявка на доступ и смена собственного пароля.

Пароли и хеши не логируются и не возвращаются. Ошибки валидации тел запросов
этого роутера не эхо-возвращают введённые значения (FastAPI по умолчанию
кладёт `input` в ответ 422 — здесь тела разбираются вручную).
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, ValidationError

from backend.app.core import portal_auth
from backend.app.core.access import (
    MODE_ACCOUNTS,
    MODE_DISABLED,
    MODE_EMERGENCY,
    AccountContext,
    portal_mode,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    username: str
    password: str


class RegisterRequest(BaseModel):
    # Любое лишнее поле (role, status, sections, staff_id, …) — отказ.
    model_config = ConfigDict(extra="forbid")
    login: str
    # Совместимость со старой формой; новой достаточно логина и пароля.
    display_name: str = ""
    password: str
    contact: str = ""
    note: str = ""


class PasswordChangeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    current_password: str
    new_password: str


def _err(status: int, code: str, message: str, **extra: Any) -> JSONResponse:
    headers = {"Retry-After": str(extra.pop("retry_after"))} if "retry_after" in extra else None
    return JSONResponse({"detail": {"code": code, "message": message, **extra}},
                        status_code=status, headers=headers)


async def _body(request: Request, model):
    """Разбор JSON-тела без эха значений в ответе об ошибке."""
    try:
        raw = await request.json()
    except Exception:  # noqa: BLE001
        return None, _err(422, "invalid_body", "Ожидается JSON.")
    try:
        return model.model_validate(raw), None
    except ValidationError as exc:
        fields = sorted({".".join(str(p) for p in e.get("loc", ())) for e in exc.errors()})
        return None, _err(422, "invalid_body", "Неверные поля запроса.", fields=fields)


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else ""


# Аварийный режим: ограничение попыток в памяти (БД аккаунтов может быть
# недоступна — ради этого режим и существует).
_EMERGENCY_FAILS: dict[str, list[float]] = {}


def _emergency_locked(key: str) -> bool:
    now = time.time()
    hits = [t for t in _EMERGENCY_FAILS.get(key, []) if now - t < 15 * 60]
    _EMERGENCY_FAILS[key] = hits
    return len(hits) >= 5


_STATUS_MESSAGES = {
    "account_pending": "Заявка ожидает одобрения администратором.",
    "account_rejected": "Заявка отклонена администратором.",
    "account_blocked": "Доступ заблокирован администратором.",
}


@router.post("/login")
async def login(request: Request, response: Response):
    payload, error = await _body(request, LoginRequest)
    if error is not None:
        return error
    settings = portal_auth.get_settings()
    mode = portal_mode()
    if mode == MODE_DISABLED:
        return {"authenticated": True, "username": payload.username, "auth_enabled": False}
    username = (payload.username or "").strip()

    if mode == MODE_ACCOUNTS:
        from backend.app.services.accounts import db, store

        def _admin_count() -> int:
            return store.active_admin_count() if db.exists() else 0

        if await asyncio.to_thread(_admin_count) < 1:
            return _err(503, "no_active_admin",
                        "Нет активного администратора. Обратитесь к инженеру.")
        result = await asyncio.to_thread(store.authenticate, username, payload.password or "",
                                         ip=_client_ip(request))
        if not result.ok:
            if result.code == "locked":
                return _err(429, "locked", "Слишком много попыток. Попробуйте позже.",
                            retry_after=result.retry_after)
            if result.code in _STATUS_MESSAGES:
                return _err(403, result.code, _STATUS_MESSAGES[result.code])
            return _err(401, "invalid_credentials", "Неверный логин или пароль")
        ctx = result.context
        token = portal_auth.issue_token_v2(ctx, settings)
        portal_auth.set_session_cookie(response, token, request, settings)
        return {"authenticated": True, "username": ctx.login, "auth_enabled": True,
                "must_change_password": ctx.must_change_password}

    # env_users / emergency_owner: строка логинов из окружения.
    key = f"{_client_ip(request)}|{username}"
    if mode == MODE_EMERGENCY and _emergency_locked(key):
        return _err(429, "locked", "Слишком много попыток. Попробуйте позже.", retry_after=900)
    ok = await asyncio.to_thread(portal_auth.verify_credentials, username,
                                 payload.password or "", settings)
    if not ok:
        if mode == MODE_EMERGENCY:
            _EMERGENCY_FAILS.setdefault(key, []).append(time.time())
        return JSONResponse({"authenticated": False, "detail": "Неверный логин или пароль"},
                            status_code=401)
    token = portal_auth.issue_token(username, settings)
    portal_auth.set_session_cookie(response, token, request, settings)
    return {"authenticated": True, "username": username, "auth_enabled": True,
            "must_change_password": False}


@router.post("/logout")
async def logout(request: Request, response: Response):
    """Удалить cookie у этого браузера (idempotent)."""
    portal_auth.clear_session_cookie(response, request, portal_auth.get_settings())
    return {"ok": True}


@router.get("/me")
async def me(request: Request):
    """PUBLIC. Аноним → authenticated:false; действующая сессия → профиль и
    права; отозванная/истёкшая cookie → authenticated:false + reason, cookie
    удаляется."""
    settings = portal_auth.get_settings()
    mode = portal_mode()
    if mode == MODE_DISABLED:
        return {"authenticated": True, "username": None, "auth_enabled": False, "mode": mode}
    resolved = await portal_auth.resolve_request_account(request)
    base = {"auth_enabled": True, "mode": mode,
            "registration_enabled": portal_auth.registration_enabled()}
    if resolved is None:
        return {"authenticated": False, "username": None, **base}
    ctx = resolved.context
    if ctx is None:
        out = JSONResponse({"authenticated": False, "username": None,
                            "reason": resolved.reason or "session_invalid", **base})
        portal_auth.clear_session_cookie(out, request, settings)
        return out
    return {"authenticated": True, "username": ctx.login, **base, "account": ctx.as_public()}


@router.post("/register")
async def register(request: Request):
    if not portal_auth.registration_enabled():
        return _err(403, "registration_closed", "Регистрация закрыта.")
    payload, error = await _body(request, RegisterRequest)
    if error is not None:
        return error
    from backend.app.services.accounts import store

    try:
        await asyncio.to_thread(
            store.create_pending, login=payload.login,
            display_name=payload.display_name.strip() or payload.login.strip(),
            password=payload.password, contact=payload.contact, note=payload.note,
            ip=_client_ip(request),
        )
    except store.AccountError as exc:
        return _err(exc.status, exc.code, exc.message)
    return {"ok": True, "message": "Заявка отправлена. Доступ появится после одобрения администратором."}


def _account_ctx(request: Request) -> Optional[AccountContext]:
    ctx = getattr(request.state, "portal_account", None)
    return ctx if isinstance(ctx, AccountContext) and ctx.mode == MODE_ACCOUNTS else None


@router.post("/password")
async def change_password(request: Request, response: Response):
    """Смена собственного пароля: нужен текущий; все прежние сеансы завершаются,
    этому браузеру выдаётся новая cookie."""
    ctx = _account_ctx(request)
    if ctx is None:
        return _err(404, "not_supported", "Смена пароля доступна в режиме аккаунтов.")
    payload, error = await _body(request, PasswordChangeRequest)
    if error is not None:
        return error
    from backend.app.services.accounts import store

    try:
        fresh = await asyncio.to_thread(store.change_password, ctx.account_id,
                                        payload.current_password, payload.new_password)
    except store.AccountError as exc:
        return _err(exc.status, exc.code, exc.message)
    from backend.app.ws.manager import ws_manager

    await ws_manager.revoke_account(ctx.account_id)
    settings = portal_auth.get_settings()
    portal_auth.set_session_cookie(response, portal_auth.issue_token_v2(fresh, settings), request, settings)
    return {"ok": True}


@router.post("/sessions/revoke")
async def revoke_own_sessions(request: Request, response: Response):
    """«Выйти на всех устройствах»."""
    ctx = _account_ctx(request)
    if ctx is None:
        return _err(404, "not_supported", "Доступно в режиме аккаунтов.")
    from backend.app.services.accounts import store

    await asyncio.to_thread(store.revoke_sessions, ctx.account_id, ctx.account_id)
    from backend.app.ws.manager import ws_manager

    await ws_manager.revoke_account(ctx.account_id)
    portal_auth.clear_session_cookie(response, request, portal_auth.get_settings())
    return {"ok": True}
