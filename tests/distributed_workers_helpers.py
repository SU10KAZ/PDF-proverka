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
