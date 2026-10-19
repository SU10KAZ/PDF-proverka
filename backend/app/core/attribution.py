"""Серверная атрибуция экспертных действий.

Автор нового решения — сотрудник, связанный с вошедшим аккаунтом
(``AccountContext.staff_id`` → ``users.json``), с канонизацией имени через
``employee_identity``. Глобальный «текущий сотрудник» (``users.json →
current_id``) и автор из тела запроса для подмены вошедшего не используются.

Аккаунт без связи с сотрудником экспертное действие сохранить не может:
``409 account_not_linked_to_staff`` вместо записи с пустым автором.
Исключение — выключенная авторизация (локальная разработка, тесты): там
субъекта нет, и, как раньше, используется имя из запроса или текущий сотрудник.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException, Request

from backend.app.core.access import MODE_DISABLED, request_account, require_account


@dataclass(frozen=True)
class Author:
    name: str
    staff_id: Optional[str]
    account_id: Optional[int]
    login: str


NOT_LINKED = {
    "code": "account_not_linked_to_staff",
    "message": (
        "Аккаунт не связан с сотрудником — экспертное действие не сохранено. "
        "Администратор назначает связь на странице «Пользователи»."
    ),
}


def server_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def resolve_author(request: Request, *, body_reviewer: str = "") -> Author:
    """Автор экспертного действия этого запроса. Бросает 401/409."""
    from backend.app.services.common import user_service
    from backend.app.services.common.employee_identity import canonicalize_user_record

    ctx = request_account(request)
    if ctx is not None and ctx.mode == MODE_DISABLED:
        current = user_service.get_current_user() or {}
        name = body_reviewer or current.get("name") or ""
        return Author(name=name, staff_id=current.get("id"), account_id=None, login="")
    ctx = require_account(request)
    staff = user_service.get_user_by_id(ctx.staff_id) if ctx.staff_id else None
    if not staff:
        raise HTTPException(409, NOT_LINKED)
    staff = canonicalize_user_record(staff)
    name = staff.get("name") or ""
    if not name:
        raise HTTPException(409, NOT_LINKED)
    return Author(name=name, staff_id=staff.get("id"), account_id=ctx.account_id, login=ctx.login)


__all__ = ["Author", "NOT_LINKED", "resolve_author", "server_now_iso"]
