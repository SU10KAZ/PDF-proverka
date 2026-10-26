"""Серверная атрибуция экспертных действий.

Автор нового решения — сотрудник, связанный с вошедшим аккаунтом
(``AccountContext.staff_id`` → ``users.json``), с канонизацией имени через
``employee_identity``. Глобальный «текущий сотрудник» (``users.json →
current_id``) и автор из тела запроса для подмены вошедшего не используются.

Аккаунт без связи с сотрудником экспертное действие сохранить не может:
``409 account_not_linked_to_staff`` вместо записи с пустым автором.
При импорте Excel для аккаунта без staff_id используется логин, проверенный
сервером; это отдельное исключение, остальные экспертные действия требуют
связи с сотрудником. При выключенной авторизации (локальная разработка, тесты)
субъекта нет, и, как раньше, используется имя из запроса или текущий сотрудник.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException, Request

from backend.app.core.access import MODE_ACCOUNTS, MODE_DISABLED, current_account, request_account


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


def resolve_author(request: Optional[Request] = None, *, body_reviewer: str = "") -> Author:
    """Автор экспертного действия этого запроса. Бросает 401/409.

    ``request`` можно не передавать — тогда берётся контекст текущего запроса
    (``access.current_account``), который ставит PortalAuthMiddleware.
    """
    from backend.app.services.common import user_service
    from backend.app.services.common.employee_identity import canonicalize_user_record

    ctx = request_account(request) if request is not None else current_account()
    if ctx is not None and ctx.mode == MODE_DISABLED:
        current = user_service.get_current_user() or {}
        name = body_reviewer or current.get("name") or ""
        return Author(name=name, staff_id=current.get("id"), account_id=None, login="")
    if ctx is None:
        raise HTTPException(401, {"code": "not_authenticated", "message": "Требуется вход."})
    staff = user_service.get_user_by_id(ctx.staff_id) if ctx.staff_id else None
    if not staff:
        raise HTTPException(409, NOT_LINKED)
    staff = canonicalize_user_record(staff)
    name = staff.get("name") or ""
    if not name:
        raise HTTPException(409, NOT_LINKED)
    return Author(name=name, staff_id=staff.get("id"), account_id=ctx.account_id, login=ctx.login)


def actor_label(request: Optional[Request] = None, *, prefer: str = "name") -> str:
    """Подпись инициатора НЕ экспертного действия (запуск, загрузка): сотрудник
    аккаунта, иначе логин. Никогда — глобальный «текущий сотрудник» при
    включённой авторизации."""
    from backend.app.services.common import user_service

    ctx = request_account(request) if request is not None else current_account()
    if ctx is None:
        return ""
    if ctx.mode == MODE_DISABLED:
        cur = user_service.get_current_user() or {}
        return str(cur.get(prefer) or cur.get("login") or cur.get("id") or "")
    staff = user_service.get_user_by_id(ctx.staff_id) if ctx.staff_id else None
    if staff:
        return str(staff.get(prefer) or staff.get("login") or staff.get("id") or ctx.login)
    return ctx.login


def resolve_import_author(request: Request) -> Author:
    """Автор импорта Excel: ФИО связанного сотрудника, иначе логин аккаунта.

    Новый одобренный аккаунт может вернуть решения без старой записи users.json.
    Логин и account_id берутся из сессии; автора из Excel/формы не используем.
    """
    ctx = request_account(request)
    if (ctx is not None and ctx.mode == MODE_ACCOUNTS and ctx.can_import_decisions
            and not ctx.staff_id):
        return Author(name=ctx.login, staff_id=None, account_id=ctx.account_id, login=ctx.login)
    return resolve_author(request)


__all__ = ["Author", "NOT_LINKED", "actor_label", "resolve_author", "resolve_import_author", "server_now_iso"]
