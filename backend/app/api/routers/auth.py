"""REST API для простой портальной аутентификации (логин/пароль).

Без БД пользователей и ролей. Учётки и хеши паролей задаются через env
(`PORTAL_AUTH_USERS`). Пароль НИКОГДА не логируется.
"""
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.app.core import portal_auth

router = APIRouter(prefix="/api/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
async def login(payload: LoginRequest, request: Request, response: Response):
    """Проверить логин/пароль и выдать session-cookie."""
    settings = portal_auth.get_settings()
    if not settings.enabled:
        # Auth выключен — вход не требуется, ведём себя как авторизованные.
        return {"authenticated": True, "username": payload.username, "auth_enabled": False}

    username = (payload.username or "").strip()
    if not portal_auth.verify_credentials(username, payload.password or "", settings):
        return JSONResponse(
            {"authenticated": False, "detail": "Неверный логин или пароль"},
            status_code=401,
        )

    token = portal_auth.issue_token(username, settings)
    portal_auth.set_session_cookie(response, token, request, settings)
    return {"authenticated": True, "username": username, "auth_enabled": True}


@router.post("/logout")
async def logout(request: Request, response: Response):
    """Очистить session-cookie."""
    settings = portal_auth.get_settings()
    portal_auth.clear_session_cookie(response, request, settings)
    return {"ok": True}


@router.get("/me")
async def me(request: Request):
    """Статус текущей сессии. Доступен без авторизации (фронт опрашивает)."""
    settings = portal_auth.get_settings()
    if not settings.enabled:
        return {"authenticated": True, "username": None, "auth_enabled": False}
    username = portal_auth.request_username(request, settings)
    return {"authenticated": bool(username), "username": username, "auth_enabled": True}
