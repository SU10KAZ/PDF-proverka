"""Общие помощники тестов распределённых воркеров.

`SyncASGITransport` позволяет гонять НАСТОЯЩЕГО синхронного агента против
НАСТОЯЩЕГО FastAPI-приложения без сокетов и портов: каждый запрос исполняется
ASGI-приложением в собственном event loop.

Зачем свой транспорт, а не httpx.ASGITransport: тот асинхронный (aclose), а
агент по проекту синхронный — он живёт в потоках, а не в event loop.
"""
from __future__ import annotations

import asyncio
from typing import Any

import httpx


class SyncASGITransport(httpx.BaseTransport):
    """Синхронный мост к ASGI-приложению."""

    def __init__(self, app: Any):
        self.app = app

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        body = request.read()
        rebuilt = httpx.Request(
            request.method,
            request.url,
            headers=request.headers,
            content=body,
        )

        async def run() -> httpx.Response:
            transport = httpx.ASGITransport(app=self.app)
            response = await transport.handle_async_request(rebuilt)
            try:
                payload = await response.aread()
            finally:
                await response.aclose()
            return httpx.Response(
                response.status_code,
                headers=response.headers,
                content=payload,
                request=request,
            )

        return asyncio.run(run())

    def close(self) -> None:  # httpx.Client.close() зовёт именно это
        return None


def make_center_app():
    """Приложение центра только с роутерами подсистемы (без остального портала)."""
    from fastapi import FastAPI

    from backend.app.api.routers import audit_worker_agent, audit_workers_admin

    app = FastAPI()
    app.include_router(audit_workers_admin.status_router)
    app.include_router(audit_worker_agent.router)
    app.include_router(audit_workers_admin.router)
    return app


def make_disabled_center_app():
    """Сборка при ВЫКЛЮЧЕННОМ флаге — ровно как в main.py: только status."""
    from fastapi import FastAPI

    from backend.app.api.routers import audit_workers_admin

    app = FastAPI()
    app.include_router(audit_workers_admin.status_router)
    return app


# ─── Портальная аутентификация и роли подсистемы ─────────────────────────────
# С пред-пайплайнового этапа операторский контур закрыт ролевой моделью, и
# анонимный клиент больше не может ничего изменить. Тесты поэтому ходят так же,
# как настоящий оператор: с портальной session-cookie реального пользователя.
# Отдельного «тестового обхода» нет намеренно — он и был бы той самой дырой,
# которую этот этап закрывает.
ADMIN_USER = "dw-admin"
OPERATOR_USER = "dw-operator"
VIEWER_USER = "dw-viewer"
STRANGER_USER = "dw-stranger"          # аутентифицирован, но роли не имеет
PORTAL_PASSWORD = "dw-test-password"
PORTAL_SECRET = "test-portal-session-secret-0123456789abcdef"

_PASSWORD_HASH: str | None = None


def password_hash() -> str:
    """pbkdf2 считается один раз на процесс: он намеренно медленный."""
    global _PASSWORD_HASH
    if _PASSWORD_HASH is None:
        from backend.app.core import portal_auth

        _PASSWORD_HASH = portal_auth.hash_password(PORTAL_PASSWORD)
    return _PASSWORD_HASH


def enable_portal_roles(
    monkeypatch,
    *,
    admins: tuple[str, ...] = (ADMIN_USER,),
    operators: tuple[str, ...] = (OPERATOR_USER,),
    viewers: tuple[str, ...] = (VIEWER_USER,),
    users: tuple[str, ...] = (ADMIN_USER, OPERATOR_USER, VIEWER_USER, STRANGER_USER),
) -> None:
    """Включить портальную аутентификацию и роли подсистемы для теста."""
    from backend.app.services.distributed_workers import authorization

    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "true")
    monkeypatch.setenv(
        "PORTAL_AUTH_USERS",
        ",".join(f"{name}:{password_hash()}" for name in users),
    )
    monkeypatch.setenv("PORTAL_SESSION_SECRET", PORTAL_SECRET)
    monkeypatch.setenv(authorization.ENV_ADMINS, ",".join(admins))
    monkeypatch.setenv(authorization.ENV_OPERATORS, ",".join(operators))
    monkeypatch.setenv(authorization.ENV_VIEWERS, ",".join(viewers))


def portal_role_env(
    *,
    admins: tuple[str, ...] = (ADMIN_USER,),
    operators: tuple[str, ...] = (OPERATOR_USER,),
    viewers: tuple[str, ...] = (VIEWER_USER,),
    users: tuple[str, ...] = (ADMIN_USER, OPERATOR_USER, VIEWER_USER, STRANGER_USER),
) -> dict[str, str]:
    """Те же переменные окружения — для ОТДЕЛЬНОГО процесса (uvicorn, smoke)."""
    from backend.app.services.distributed_workers import authorization

    return {
        "PORTAL_AUTH_ENABLED": "true",
        "PORTAL_AUTH_USERS": ",".join(f"{n}:{password_hash()}" for n in users),
        "PORTAL_SESSION_SECRET": PORTAL_SECRET,
        authorization.ENV_ADMINS: ",".join(admins),
        authorization.ENV_OPERATORS: ",".join(operators),
        authorization.ENV_VIEWERS: ",".join(viewers),
    }


def session_cookie(username: str) -> str:
    """Настоящий подписанный токен сессии портала (тот же код, что в проде)."""
    from backend.app.core import portal_auth

    return portal_auth.issue_token(username, portal_auth.get_settings())


def portal_client(app, *, username: str = ADMIN_USER, base_url: str = "http://center"):
    """httpx-клиент с сессией конкретного пользователя и заголовком намерения."""
    import httpx

    from backend.app.core import portal_auth

    settings = portal_auth.get_settings()
    client = httpx.Client(
        transport=SyncASGITransport(app),
        base_url=base_url,
        headers={"X-Requested-With": "audit-workers"},
    )
    client.cookies.set(settings.cookie_name, session_cookie(username))
    return client
