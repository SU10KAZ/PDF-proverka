"""Кто выполняет запрос и что ему можно — единый серверный контекст.

Портал знает четыре режима (`PORTAL_AUTH_MODE` при `PORTAL_AUTH_ENABLED=true`):

* ``disabled``        — авторизация выключена (локальная разработка, тесты):
  проверки прав не применяются, как и раньше;
* ``env_users``       — прежний контур: логины и хеши из ``PORTAL_AUTH_USERS``.
  Вошедший сотрудник делает всё, как до появления ролей, КРОМЕ действий,
  помеченных ``require_admin`` (управление сотрудниками, деньги, журнал,
  учётная запись модели): для них нужен сотрудник с ``users.json role=admin``;
* ``accounts``        — аккаунты в SQLite (``services/accounts``): роли
  admin / viewer, права по разделам, статусы, версия сессии;
* ``emergency_owner`` — аварийный режим отката: входит ровно один владелец из
  ``PORTAL_EMERGENCY_OWNER``, он admin, остальные и регистрация закрыты.

Контекст собирает ``PortalAuthMiddleware`` один раз на запрос и кладёт в
``request.state.portal_account``. Роль и права НИКОГДА не берутся из тела,
query или заголовков запроса — только из подписанной cookie и хранилища.
"""
from __future__ import annotations

import contextvars
import os
from dataclasses import dataclass, field
from typing import Mapping, Optional

from fastapi import HTTPException, Request

MODE_DISABLED = "disabled"
MODE_ENV_USERS = "env_users"
MODE_ACCOUNTS = "accounts"
MODE_EMERGENCY = "emergency_owner"
_MODES = {MODE_ENV_USERS, MODE_ACCOUNTS, MODE_EMERGENCY}

ROLE_ADMIN = "admin"
ROLE_VIEWER = "viewer"

#: Функциональные разделы портала первой версии (план § 3.3).
SECTIONS: tuple[str, ...] = (
    "audit_results",
    "version_comparison",
    "stage_comparison",
    "knowledge_base",
    "schedule",
)
#: Разделы, в которых сейчас нет ни одной выгрузки: право скачивания в них не
#: назначается (план, поправка 0.5).
SECTIONS_WITHOUT_DOWNLOADS: frozenset[str] = frozenset({"knowledge_base", "schedule"})

SECTION_TITLES: Mapping[str, str] = {
    "audit_results": "Результаты аудита",
    "version_comparison": "Сравнение версий",
    "stage_comparison": "Сравнение стадий",
    "knowledge_base": "База знаний",
    "schedule": "График работ",
}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def configured_mode() -> str:
    """Режим из PORTAL_AUTH_MODE без учёта флага включения."""
    raw = (os.environ.get("PORTAL_AUTH_MODE") or MODE_ENV_USERS).strip().lower()
    # Неизвестное значение — не «пропустить», а самый строгий из рабочих
    # режимов: аккаунты (без БД и администратора вход просто закрыт).
    return raw if raw in _MODES else MODE_ACCOUNTS


def portal_mode() -> str:
    """Текущий режим. Читается из окружения на каждый вызов (дёшево)."""
    if not _env_bool("PORTAL_AUTH_ENABLED", False):
        return MODE_DISABLED
    return configured_mode()


@dataclass(frozen=True)
class AccountContext:
    """Действующее лицо запроса. Собирается только сервером."""

    mode: str
    login: str
    role: str
    account_id: Optional[int] = None
    status: str = "active"
    staff_id: Optional[str] = None
    session_version: int = 0
    must_change_password: bool = False
    #: section → (can_view, can_download)
    sections: Mapping[str, tuple[bool, bool]] = field(default_factory=dict)
    #: env_users: сотрудник users.json с role=admin (прежний «администратор»).
    legacy_staff_admin: bool = False
    upload_projects: bool = False

    @property
    def is_admin(self) -> bool:
        if self.mode in (MODE_DISABLED, MODE_EMERGENCY):
            return True
        if self.mode == MODE_ENV_USERS:
            return self.legacy_staff_admin
        return self.role == ROLE_ADMIN and self.status == "active"

    @property
    def enforces_roles(self) -> bool:
        """Применяется ли таблица политик (роль viewer существует только тут)."""
        return self.mode == MODE_ACCOUNTS

    def can_view(self, section: str) -> bool:
        if not self.enforces_roles or self.is_admin:
            return True
        return bool(self.sections.get(section, (False, False))[0])

    def can_download(self, section: str) -> bool:
        if not self.enforces_roles or self.is_admin:
            return True
        view, download = self.sections.get(section, (False, False))
        return bool(view and download)

    @property
    def can_import_decisions(self) -> bool:
        """Активный viewer с доступом к результатам может вернуть решения из Excel."""
        if not self.enforces_roles or self.is_admin:
            return True
        return (self.role == ROLE_VIEWER and self.status == "active"
                and self.can_view("audit_results"))

    @property
    def can_upload_projects(self) -> bool:
        if not self.enforces_roles or self.is_admin:
            return True
        return (self.role == ROLE_VIEWER and self.status == "active"
                and self.upload_projects and self.can_view("audit_results"))

    def as_public(self) -> dict:
        """Проекция для UI и /api/auth/me. Ни хешей, ни секретов."""
        return {
            "mode": self.mode,
            "login": self.login,
            "account_id": self.account_id,
            "role": self.role if self.mode == MODE_ACCOUNTS else (
                ROLE_ADMIN if self.is_admin else "staff"),
            "is_admin": self.is_admin,
            "can_import_decisions": self.can_import_decisions,
            "can_upload_projects": self.can_upload_projects,
            "staff_id": self.staff_id,
            "must_change_password": self.must_change_password,
            "sections": {
                s: {"view": self.can_view(s),
                    "download": self.can_download(s) and s not in SECTIONS_WITHOUT_DOWNLOADS}
                for s in SECTIONS
            },
        }


DISABLED_CONTEXT = AccountContext(mode=MODE_DISABLED, login="", role=ROLE_ADMIN)

# Контекст текущего HTTP-запроса для кода, которому не передан Request
# (помощники атрибуции в сервисах). Ставит PortalAuthMiddleware.
_CURRENT: contextvars.ContextVar[Optional[AccountContext]] = contextvars.ContextVar(
    "portal_current_account", default=None
)


def bind_current(ctx: Optional[AccountContext]):
    return _CURRENT.set(ctx)


def unbind_current(token) -> None:
    try:
        _CURRENT.reset(token)
    except (ValueError, RuntimeError):
        pass


def current_account() -> Optional[AccountContext]:
    """Контекст текущего запроса; при выключенной авторизации — DISABLED."""
    ctx = _CURRENT.get()
    if ctx is None and portal_mode() == MODE_DISABLED:
        return DISABLED_CONTEXT
    return ctx


def read_only_request() -> bool:
    """True для viewer в режиме аккаунтов: просмотр без записи и запусков."""
    ctx = current_account()
    return bool(ctx is not None and ctx.enforces_roles and not ctx.is_admin)


def request_account(request: Request) -> Optional[AccountContext]:
    """Контекст запроса. Для exempt-путей (их middleware пропускает) —
    разрешается здесь же из cookie, чтобы, например, `/api/info` знал админа."""
    state_ctx = getattr(getattr(request, "state", None), "portal_account", None)
    if isinstance(state_ctx, AccountContext):
        return state_ctx
    bound = _CURRENT.get()
    if bound is not None:
        return bound
    if portal_mode() == MODE_DISABLED:
        return DISABLED_CONTEXT
    from backend.app.core import portal_auth

    resolved = portal_auth.resolve_request_account_sync(request)
    return resolved.context if resolved is not None else None


def require_account(request: Request) -> AccountContext:
    ctx = request_account(request)
    if ctx is None:
        raise HTTPException(401, {"code": "not_authenticated", "message": "Требуется вход."})
    return ctx


def require_admin(request: Request) -> AccountContext:
    """Действие только для администратора. В режиме ``env_users`` — сотрудник
    с ``users.json role=admin`` (так уже работал ``schedule._require_admin``)."""
    ctx = require_account(request)
    if not ctx.is_admin:
        raise HTTPException(403, {"code": "admin_only",
                                  "message": "Действие доступно только администратору."})
    return ctx


async def require_admin_dep(request: Request) -> AccountContext:
    """FastAPI-зависимость для целых роутеров (usage, action_log, …)."""
    return require_admin(request)


__all__ = [
    "AccountContext",
    "DISABLED_CONTEXT",
    "MODE_ACCOUNTS",
    "MODE_DISABLED",
    "MODE_EMERGENCY",
    "MODE_ENV_USERS",
    "ROLE_ADMIN",
    "ROLE_VIEWER",
    "SECTIONS",
    "SECTIONS_WITHOUT_DOWNLOADS",
    "SECTION_TITLES",
    "portal_mode",
    "request_account",
    "require_account",
    "require_admin",
    "require_admin_dep",
]
