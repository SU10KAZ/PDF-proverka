"""
REST API для управления пользователями (сотрудниками-экспертами).
"""
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional

import backend.app.services.common.user_service as user_service
from backend.app.core import portal_auth

router = APIRouter(prefix="/api/users", tags=["users"])


class AddUserRequest(BaseModel):
    surname: str
    initials: str = ""
    role: str = "expert"
    login: str = ""


class UpdateUserRequest(BaseModel):
    surname: Optional[str] = None
    initials: Optional[str] = None
    role: Optional[str] = None
    login: Optional[str] = None


class SwitchUserRequest(BaseModel):
    id: str


def _session_username(request: Request) -> Optional[str]:
    """Логин из активной портальной сессии (None, если auth выключен)."""
    settings = portal_auth.get_settings()
    if not settings.enabled:
        return None
    return portal_auth.request_username(request, settings)


@router.get("")
async def list_users(request: Request):
    """Список всех пользователей + текущий активный.

    Если портальная авторизация включена, активный сотрудник = залогиненный
    пользователь (матч по login/id). Глобальный current_id из файла
    используется только когда auth выключен (локальная разработка) или логин
    не сопоставлен ни с одним сотрудником.
    """
    username = _session_username(request)
    auth_enabled = portal_auth.get_settings().enabled

    matched = user_service.get_user_by_login(username) if username else None
    if matched:
        current_id = matched["id"]
    else:
        current_id = user_service.get_current_id()

    return {
        "users": user_service.list_users(),
        "current_id": current_id,
        "auth_enabled": auth_enabled,
        "logged_in_username": username,
        "logged_in_matched": bool(matched),
    }


@router.post("")
async def add_user(req: AddUserRequest):
    """Добавить пользователя."""
    try:
        user = user_service.add_user(req.surname, req.initials, req.role, req.login)
        return {"status": "ok", "user": user}
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/switch")
async def switch_user(req: SwitchUserRequest):
    """Сделать пользователя текущим активным (от его имени сохраняются решения)."""
    try:
        return {"status": "ok", "user": user_service.switch_user(req.id)}
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.put("/{user_id}")
async def update_user(user_id: str, req: UpdateUserRequest):
    """Обновить данные пользователя."""
    try:
        return {"status": "ok", "user": user_service.update_user(
            user_id, req.surname, req.initials, req.role, req.login)}
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.delete("/{user_id}")
async def delete_user(user_id: str):
    """Удалить пользователя (его записи в логе решений сохраняются)."""
    user_service.delete_user(user_id)
    return {"status": "ok"}


@router.get("/{user_id}/activity")
async def user_activity(user_id: str):
    """Активность пользователя: проекты, в которых он принял/отклонил
    замечания и оптимизации."""
    try:
        return user_service.get_user_activity(user_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
