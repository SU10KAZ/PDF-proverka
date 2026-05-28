"""
Portal auth — простая защита веб-портала логином/паролем.

Минимально надёжный вариант для 3-4 сотрудников: без БД пользователей, ролей,
регистрации и восстановления пароля. Логины/хеши паролей задаются через env.

Дизайн (без новых зависимостей):
* Хеши паролей — pbkdf2_sha256 (passlib, чистый Python, без bcrypt-расширения).
* Сессия — self-contained подписанный cookie (stdlib hmac), без серверного
  стораджа. Payload: {"u": username, "exp": epoch}. Подпись HMAC-SHA256.
* Middleware блокирует всё, кроме явного exempt-списка, когда auth включён.

Настройки читаются из os.environ при каждом запросе (дёшево; портал
малонагруженный), что упрощает тестирование через monkeypatch.setenv.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass

from passlib.hash import pbkdf2_sha256
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

# Куда редиректить неаутентифицированного пользователя на HTML-страницах.
LOGIN_PATH = "/login"

# Эти пути доступны без аутентификации:
#   /login            — страница входа (self-contained, без внешних ассетов)
#   /api/auth/login   — проверка логина/пароля
#   /api/auth/logout  — выход (idempotent, безопасно открыть)
#   /api/auth/me      — статус сессии (фронт опрашивает до входа)
#   /api/info         — healthcheck, который дёргает cron-watchdog (curl -f)
#   /favicon.ico      — иконка вкладки на странице входа
EXEMPT_PATHS = frozenset(
    {
        LOGIN_PATH,
        "/api/auth/login",
        "/api/auth/logout",
        "/api/auth/me",
        "/api/info",
        "/favicon.ico",
    }
)

# Фиксированный dummy-хеш: verify против него при неизвестном логине, чтобы
# время ответа не выдавало, существует пользователь или нет.
_DUMMY_HASH = pbkdf2_sha256.hash("portal-auth-dummy-password")

# Эфемерный секрет на процесс — fallback, если PORTAL_SESSION_SECRET не задан.
# Стабилен в пределах процесса, но сбрасывается при рестарте (сессии слетают).
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
    """`'ivan:HASH,petr:HASH'` → {ivan: HASH, ...}.

    Username делится по ПЕРВОМУ ':' (pbkdf2-хеш не содержит ':' и ',').
    """
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
    users: dict[str, str]
    secret: str
    ttl_seconds: int
    cookie_secure_mode: str  # auto | true | false
    cookie_name: str


def get_settings() -> PortalSettings:
    """Собрать актуальные настройки auth из окружения."""
    global _EPHEMERAL_SECRET
    enabled = _env_bool("PORTAL_AUTH_ENABLED", False)
    users = _parse_users(os.environ.get("PORTAL_AUTH_USERS", ""))

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
    )


# ─── Пароли ─────────────────────────────────────────────────────────────────
def hash_password(password: str) -> str:
    """Сгенерировать pbkdf2_sha256 хеш (для helper-скрипта)."""
    return pbkdf2_sha256.hash(password)


def verify_credentials(username: str, password: str, settings: PortalSettings) -> bool:
    """Проверить логин+пароль. Constant-time-ish: при неизвестном логине
    выполняется verify против dummy-хеша, чтобы не палить наличие юзера."""
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
        # битый/неподдерживаемый хеш в конфиге
        return False


# ─── Session token (подписанный cookie) ─────────────────────────────────────
def issue_token(username: str, settings: PortalSettings) -> str:
    exp = int(time.time()) + settings.ttl_seconds
    body = json.dumps({"u": username, "exp": exp}, separators=(",", ":")).encode("utf-8")
    body_b64 = _b64url_encode(body)
    sig = hmac.new(settings.secret.encode("utf-8"), body_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{body_b64}.{_b64url_encode(sig)}"


def verify_token(token: str, settings: PortalSettings) -> str | None:
    """Вернуть username, если токен валиден и не истёк, иначе None."""
    if not token or "." not in token:
        return None
    body_b64, _, sig_b64 = token.partition(".")
    if not body_b64 or not sig_b64:
        return None
    expected = hmac.new(
        settings.secret.encode("utf-8"), body_b64.encode("ascii"), hashlib.sha256
    ).digest()
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
    username = payload.get("u")
    exp = payload.get("exp")
    if not isinstance(username, str) or not isinstance(exp, int):
        return None
    if exp < int(time.time()):
        return None
    # Юзер мог быть удалён из конфига после выпуска токена.
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


def request_username(request: Request, settings: PortalSettings) -> str | None:
    """Достать и проверить session-cookie из запроса."""
    token = request.cookies.get(settings.cookie_name)
    if not token:
        return None
    return verify_token(token, settings)


def is_path_exempt(path: str) -> bool:
    return path in EXEMPT_PATHS


# ─── Middleware ───────────────────────────────────────────────────────────────
class PortalAuthMiddleware(BaseHTTPMiddleware):
    """Блокирует доступ ко всему, кроме exempt-путей, пока пользователь не вошёл.

    Работает только для HTTP. WebSocket-обработчики проверяют cookie сами
    (BaseHTTPMiddleware не перехватывает ws-scope).
    """

    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        if not settings.enabled:
            return await call_next(request)

        path = request.url.path
        if is_path_exempt(path):
            return await call_next(request)

        username = request_username(request, settings)
        if username:
            request.state.portal_user = username
            return await call_next(request)

        # Не аутентифицирован.
        if path.startswith("/api/") or path.startswith("/ws/"):
            return JSONResponse({"detail": "Not authenticated"}, status_code=401)
        # HTML / прочее → на страницу входа.
        return RedirectResponse(LOGIN_PATH, status_code=302)
