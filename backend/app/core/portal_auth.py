"""
Portal auth — вход в веб-портал и серверный контекст аккаунта.

Режимы (`PORTAL_AUTH_MODE`, см. `core/access.py`):

* ``env_users`` — прежний контур: логины и хеши из ``PORTAL_AUTH_USERS``,
  cookie ``{"u": login, "exp"}``;
* ``accounts`` — аккаунты в SQLite (`services/accounts`), cookie
  ``{"v": 2, "a": account_id, "sv": session_version, "iat", "exp"}``. На КАЖДОМ
  защищённом запросе аккаунт и права читаются из БД (в потоке, без
  межзапросного кэша): блокировка, сброс пароля, понижение роли и «завершить
  сеансы» (`session_version+1`) действуют со следующего запроса — и для правок
  через API, и для правок через CLI;
* ``emergency_owner`` — аварийный режим отката: входит только владелец из
  ``PORTAL_EMERGENCY_OWNER`` (``login:hash``), строка ``PORTAL_AUTH_USERS`` не
  читается, регистрация и вход остальных закрыты.

Подпись cookie — HMAC-SHA256 (stdlib), cookie `HttpOnly`, `SameSite=Lax`,
`Secure` по схеме запроса или `PORTAL_COOKIE_SECURE`.

Middleware дополнительно:
* отклоняет `Sec-Fetch-Site: cross-site` на `/api/*` и изменяющие запросы с
  чужим `Origin`; в режимах accounts/emergency изменяющие `/api/*` требуют
  заголовок `X-Requested-With: AuditManager` (ставит portal_auth.js);
* при `must_change_password` пропускает только собственные действия и
  страницу `/change-password`.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlsplit

from passlib.hash import pbkdf2_sha256
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

from backend.app.core.access import (
    DISABLED_CONTEXT,
    MODE_ACCOUNTS,
    MODE_DISABLED,
    MODE_EMERGENCY,
    MODE_ENV_USERS,
    ROLE_ADMIN,
    AccountContext,
    bind_current,
    configured_mode,
    portal_mode,
    unbind_current,
)

LOGIN_PATH = "/login"
CHANGE_PASSWORD_PATH = "/change-password"

# Доступны без входа:
#   /login, /register      — автономные страницы входа и заявки
#   /api/auth/login        — проверка логина/пароля
#   /api/auth/register     — заявка на доступ (только режим accounts и флаг)
#   /api/auth/logout       — выход (idempotent)
#   /api/auth/me           — статус сессии: аноним / действующая / отозванная
#   /api/info              — healthcheck watchdog'а (анониму — без путей)
#   /favicon.ico
EXEMPT_PATHS = frozenset(
    {
        LOGIN_PATH,
        "/register",
        "/api/auth/login",
        "/api/auth/register",
        "/api/auth/logout",
        "/api/auth/me",
        "/api/info",
        "/favicon.ico",
    }
)

# Свой контур аутентификации (bearer-токен исполнителя), портальная cookie
# здесь не работает и не должна (техпроект §20.2).
EXEMPT_PREFIXES: tuple[str, ...] = ("/api/v1/worker/",)

# Что доступно при обязательной смене пароля.
MUST_CHANGE_ALLOWED = frozenset(
    {"/api/auth/me", "/api/auth/password", "/api/auth/logout", CHANGE_PASSWORD_PATH}
)

# Документация API — только администратору (режим accounts).
DOCS_PATHS = frozenset({"/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect"})

CSRF_HEADER = "x-requested-with"
CSRF_VALUE = "AuditManager"
_UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

_DUMMY_HASH = pbkdf2_sha256.hash("portal-auth-dummy-password")
_EPHEMERAL_SECRET: str | None = None


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def _parse_users(raw: str) -> dict[str, str]:
    """`'ivan:HASH,petr:HASH'` → {ivan: HASH, ...}. Делим по ПЕРВОМУ ':'."""
    users: dict[str, str] = {}
    if not raw:
        return users
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        name, _, pw_hash = entry.partition(":")
        name = name.strip()
        pw_hash = pw_hash.strip()
        if name and pw_hash:
            users[name] = pw_hash
    return users


@dataclass(frozen=True)
class PortalSettings:
    enabled: bool
    users: dict[str, str]          # env_users: PORTAL_AUTH_USERS; emergency: владелец; accounts: {}
    secret: str
    ttl_seconds: int
    cookie_secure_mode: str        # auto | true | false
    cookie_name: str
    mode: str = MODE_ENV_USERS


def get_settings() -> PortalSettings:
    """Актуальные настройки из окружения (читаются на каждый запрос, дёшево)."""
    global _EPHEMERAL_SECRET
    enabled = _env_bool("PORTAL_AUTH_ENABLED", False)
    mode = portal_mode()
    # Строка логинов зависит от НАСТРОЕННОГО режима, а не от флага включения:
    # при выключенной авторизации прежний разбор cookie (журнал) не ломается.
    configured = configured_mode()
    if configured == MODE_ENV_USERS:
        users = _parse_users(os.environ.get("PORTAL_AUTH_USERS", ""))
    elif configured == MODE_EMERGENCY:
        # Ровно одна запись; больше — не «пустить всех», а никого.
        owner = _parse_users(os.environ.get("PORTAL_EMERGENCY_OWNER", ""))
        users = owner if len(owner) == 1 else {}
    else:
        users = {}

    secret = (os.environ.get("PORTAL_SESSION_SECRET", "") or "").strip()
    if not secret:
        if _EPHEMERAL_SECRET is None:
            _EPHEMERAL_SECRET = secrets.token_urlsafe(48)
        secret = _EPHEMERAL_SECRET

    try:
        ttl_hours = int(os.environ.get("PORTAL_SESSION_TTL_HOURS", "24") or "24")
    except ValueError:
        ttl_hours = 24
    if ttl_hours <= 0:
        ttl_hours = 24

    cookie_secure_mode = (os.environ.get("PORTAL_COOKIE_SECURE", "auto").strip().lower() or "auto")
    if cookie_secure_mode not in {"auto", "true", "false"}:
        cookie_secure_mode = "auto"

    cookie_name = (os.environ.get("PORTAL_SESSION_COOKIE_NAME", "portal_session").strip() or "portal_session")

    return PortalSettings(
        enabled=enabled,
        users=users,
        secret=secret,
        ttl_seconds=ttl_hours * 3600,
        cookie_secure_mode=cookie_secure_mode,
        cookie_name=cookie_name,
        mode=mode,
    )


def registration_enabled() -> bool:
    return portal_mode() == MODE_ACCOUNTS and _env_bool("PORTAL_REGISTRATION_ENABLED", False)


# ─── Пароли (env_users / emergency) ─────────────────────────────────────────
def hash_password(password: str) -> str:
    """pbkdf2_sha256 хеш (helper-скрипт, аварийный владелец)."""
    return pbkdf2_sha256.hash(password)


def verify_credentials(username: str, password: str, settings: PortalSettings) -> bool:
    """Логин+пароль по строке настроек; при неизвестном логине — dummy-verify."""
    stored = settings.users.get(username) if username else None
    if stored is None:
        try:
            pbkdf2_sha256.verify(password, _DUMMY_HASH)
        except (ValueError, TypeError):
            pass
        return False
    try:
        return pbkdf2_sha256.verify(password, stored)
    except (ValueError, TypeError):
        return False


# ─── Токены ──────────────────────────────────────────────────────────────────
def _sign(body: dict, settings: PortalSettings) -> str:
    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    body_b64 = _b64url_encode(raw)
    sig = hmac.new(settings.secret.encode("utf-8"), body_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{body_b64}.{_b64url_encode(sig)}"


def _decode(token: str, settings: PortalSettings) -> Optional[dict]:
    """Payload при верной подписи, иначе None. Срок НЕ проверяет."""
    if not token or "." not in token:
        return None
    body_b64, _, sig_b64 = token.partition(".")
    if not body_b64 or not sig_b64:
        return None
    expected = hmac.new(settings.secret.encode("utf-8"), body_b64.encode("ascii"), hashlib.sha256).digest()
    try:
        provided = _b64url_decode(sig_b64)
    except (ValueError, TypeError):
        return None
    if not hmac.compare_digest(expected, provided):
        return None
    try:
        payload = json.loads(_b64url_decode(body_b64))
    except (ValueError, TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def issue_token(username: str, settings: PortalSettings) -> str:
    """Cookie прежнего формата (env_users / emergency)."""
    return _sign({"u": username, "exp": int(time.time()) + settings.ttl_seconds}, settings)


def issue_token_v2(ctx: AccountContext, settings: PortalSettings) -> str:
    now = int(time.time())
    return _sign({"v": 2, "a": int(ctx.account_id), "sv": int(ctx.session_version),
                  "iat": now, "exp": now + settings.ttl_seconds}, settings)


def verify_token(token: str, settings: PortalSettings) -> str | None:
    """Логин из cookie прежнего формата, если она валидна, не истекла и логин
    есть в настройках режима. Cookie v2 этой функцией не принимается."""
    payload = _decode(token, settings)
    if payload is None or payload.get("v") is not None:
        return None
    username = payload.get("u")
    exp = payload.get("exp")
    if not isinstance(username, str) or not isinstance(exp, int):
        return None
    if exp < int(time.time()):
        return None
    if username not in settings.users:
        return None
    return username


# ─── Cookie helpers ──────────────────────────────────────────────────────────
def _cookie_secure(request: Request, settings: PortalSettings) -> bool:
    mode = settings.cookie_secure_mode
    if mode == "true":
        return True
    if mode == "false":
        return False
    proto = request.headers.get("x-forwarded-proto", "").split(",")[0].strip().lower()
    if proto:
        return proto == "https"
    return request.url.scheme == "https"


def set_session_cookie(response: Response, token: str, request: Request, settings: PortalSettings) -> None:
    response.set_cookie(
        key=settings.cookie_name,
        value=token,
        max_age=settings.ttl_seconds,
        httponly=True,
        samesite="lax",
        secure=_cookie_secure(request, settings),
        path="/",
    )


def clear_session_cookie(response: Response, request: Request, settings: PortalSettings) -> None:
    response.delete_cookie(
        key=settings.cookie_name,
        path="/",
        httponly=True,
        samesite="lax",
        secure=_cookie_secure(request, settings),
    )


# ─── Контекст аккаунта ───────────────────────────────────────────────────────
@dataclass(frozen=True)
class ResolvedAccount:
    """Результат разбора cookie: контекст либо причина отказа."""

    context: Optional[AccountContext]
    reason: str = ""
    exp: int = 0


def _legacy_context(username: str) -> AccountContext:
    """env_users: сотрудник из users.json по логину."""
    from backend.app.services.common import user_service

    staff = user_service.get_user_by_login(username)
    return AccountContext(
        mode=MODE_ENV_USERS,
        login=username,
        role=ROLE_ADMIN,
        staff_id=(staff or {}).get("id"),
        legacy_staff_admin=bool(staff) and (staff.get("role") or "") == "admin",
    )


def _emergency_context(username: str) -> AccountContext:
    from backend.app.services.common import user_service

    staff = user_service.get_user_by_login(username)
    return AccountContext(mode=MODE_EMERGENCY, login=username, role=ROLE_ADMIN,
                          staff_id=(staff or {}).get("id"))


def resolve_token_sync(token: str, settings: PortalSettings) -> ResolvedAccount:
    """Cookie → контекст. В режиме accounts ходит в SQLite (вызывать из потока)."""
    mode = settings.mode
    if mode in (MODE_ENV_USERS, MODE_EMERGENCY):
        username = verify_token(token, settings)
        if not username:
            return ResolvedAccount(None, "session_expired")
        payload = _decode(token, settings) or {}
        ctx = _legacy_context(username) if mode == MODE_ENV_USERS else _emergency_context(username)
        return ResolvedAccount(ctx, exp=int(payload.get("exp") or 0))
    payload = _decode(token, settings)
    if payload is None or payload.get("v") != 2:
        return ResolvedAccount(None, "session_invalid")
    account_id, sv, exp = payload.get("a"), payload.get("sv"), payload.get("exp")
    if not isinstance(account_id, int) or not isinstance(sv, int) or not isinstance(exp, int):
        return ResolvedAccount(None, "session_invalid")
    if exp < int(time.time()):
        return ResolvedAccount(None, "session_expired")
    from backend.app.services.accounts import store

    try:
        ctx = store.get_context(account_id)
    except FileNotFoundError:
        return ResolvedAccount(None, "session_revoked")
    if ctx is None or ctx.status != "active" or ctx.session_version != sv:
        return ResolvedAccount(None, "session_revoked")
    return ResolvedAccount(ctx, exp=exp)


def resolve_request_account_sync(request: Request) -> ResolvedAccount | None:
    """None — cookie нет (аноним); иначе результат разбора."""
    settings = get_settings()
    if not settings.enabled:
        return None
    token = request.cookies.get(settings.cookie_name)
    if not token:
        return None
    return resolve_token_sync(token, settings)


async def resolve_request_account(request: Request) -> ResolvedAccount | None:
    """Async-обёртка: в режиме accounts чтение SQLite уходит в поток."""
    existing = getattr(request.state, "portal_account", None)
    if isinstance(existing, AccountContext):
        return ResolvedAccount(existing)
    settings = get_settings()
    if not settings.enabled:
        return None
    token = request.cookies.get(settings.cookie_name)
    if not token:
        return None
    if settings.mode == MODE_ACCOUNTS:
        return await asyncio.to_thread(resolve_token_sync, token, settings)
    return resolve_token_sync(token, settings)


def request_username(request: Request, settings: PortalSettings) -> str | None:
    """Логин вошедшего. Сначала — контекст, уже проверенный middleware (так
    работает и режим accounts, где в cookie нет логина), иначе — cookie
    прежнего формата."""
    ctx = getattr(getattr(request, "state", None), "portal_account", None)
    if isinstance(ctx, AccountContext):
        return ctx.login or None
    token = request.cookies.get(settings.cookie_name) if hasattr(request, "cookies") else None
    if not token:
        return None
    return verify_token(token, settings)


def is_path_exempt(path: str) -> bool:
    return path in EXEMPT_PATHS or path.startswith(EXEMPT_PREFIXES)


# ─── Защита изменяющих запросов ─────────────────────────────────────────────
def _request_host(request: Request) -> str:
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
    return host.split(",")[0].strip().lower()


def _deny(code: str, message: str, status: int = 403) -> JSONResponse:
    return JSONResponse({"detail": {"code": code, "message": message}}, status_code=status)


def request_guard_violation(request: Request, mode: str) -> Optional[JSONResponse]:
    path = request.url.path
    if not path.startswith("/api/") or path.startswith(EXEMPT_PREFIXES):
        return None
    if request.headers.get("sec-fetch-site", "").strip().lower() == "cross-site":
        return _deny("cross_site_request", "Запрос с чужого сайта отклонён.")
    if request.method.upper() not in _UNSAFE_METHODS:
        return None
    origin = request.headers.get("origin")
    if origin and origin != "null":
        if urlsplit(origin).netloc.lower() != _request_host(request):
            return _deny("bad_origin", "Запрос с чужого сайта отклонён.")
    elif origin == "null":
        return _deny("bad_origin", "Запрос с чужого сайта отклонён.")
    if mode in (MODE_ACCOUNTS, MODE_EMERGENCY):
        if request.headers.get(CSRF_HEADER, "") != CSRF_VALUE:
            return _deny("csrf_header_required", "Запрос отклонён: нет заголовка портала.")
    return None


# ─── Middleware ───────────────────────────────────────────────────────────────
_REASON_TEXT = {
    "not_authenticated": "Требуется вход.",
    "session_expired": "Сеанс истёк — войдите снова.",
    "session_revoked": "Сеанс завершён — войдите снова.",
    "session_invalid": "Сеанс недействителен — войдите снова.",
}


class PortalAuthMiddleware(BaseHTTPMiddleware):
    """Ставит ``request.state.portal_account`` либо отказывает.

    Только HTTP; WebSocket-обработчики проверяют cookie сами (`main.py`).
    """

    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        if not settings.enabled:
            request.state.portal_account = DISABLED_CONTEXT
            return await call_next(request)
        bound = bind_current(None)
        try:
            return await self._dispatch(request, call_next, settings)
        finally:
            unbind_current(bound)

    async def _dispatch(self, request: Request, call_next, settings: PortalSettings):

        violation = request_guard_violation(request, settings.mode)
        if violation is not None:
            return violation

        path = request.url.path
        if is_path_exempt(path):
            return await call_next(request)

        token = request.cookies.get(settings.cookie_name)
        resolved = None
        if token:
            if settings.mode == MODE_ACCOUNTS:
                resolved = await asyncio.to_thread(resolve_token_sync, token, settings)
            else:
                resolved = resolve_token_sync(token, settings)
        ctx = resolved.context if resolved else None
        if ctx is None:
            reason = (resolved.reason if resolved else "") or "not_authenticated"
            if path.startswith("/api/") or path.startswith("/ws/"):
                response: Response = JSONResponse(
                    {"detail": {"code": reason, "message": _REASON_TEXT.get(reason, "Требуется вход.")}},
                    status_code=401,
                )
            else:
                target = LOGIN_PATH if reason == "not_authenticated" else f"{LOGIN_PATH}?reason={reason}"
                response = RedirectResponse(target, status_code=302)
            if token:
                clear_session_cookie(response, request, settings)
            return response

        request.state.portal_account = ctx
        request.state.portal_user = ctx.login
        bind_current(ctx)

        if ctx.must_change_password and path not in MUST_CHANGE_ALLOWED:
            if path.startswith("/api/"):
                return _deny("password_change_required", "Сначала смените временный пароль.")
            return RedirectResponse(CHANGE_PASSWORD_PATH, status_code=302)
        if path in DOCS_PATHS and ctx.enforces_roles and not ctx.is_admin:
            return _deny("admin_only", "Действие доступно только администратору.")
        return await call_next(request)
