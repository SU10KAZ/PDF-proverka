"""
WebSocket Connection Manager.
Управляет подключениями, комнатами (по проектам), broadcast.

Каждое соединение помнит, кто его открыл (`AccountContext`, срок cookie).
Отзыв доступа:
* через API аккаунтов — `revoke_account(account_id)` закрывает соединения
  сразу (код 4401);
* через CLI или по истечении срока — фоновая перепроверка раз в
  `WS_RECHECK_SECONDS` читает аккаунт из БД заново (статус, роль, версия
  сессии, обязательная смена пароля, срок) и закрывает устаревшие.
"""
import asyncio
import json
import time
from dataclasses import dataclass
from typing import Optional

from fastapi import WebSocket

from backend.app.models.websocket import WSMessage

WS_RECHECK_SECONDS = 5.0
CLOSE_REVOKED = 4401
CLOSE_FORBIDDEN = 4403


@dataclass
class _Meta:
    ctx: object                 # AccountContext
    token: str
    exp: int


class ConnectionManager:
    """Синглтон-менеджер WebSocket-подключений."""

    def __init__(self):
        self._project_connections: dict[str, list[WebSocket]] = {}
        self._global_connections: list[WebSocket] = []
        self._meta: dict[int, _Meta] = {}          # id(ws) → кто открыл
        self._sockets: dict[int, WebSocket] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._recheck_task: Optional[asyncio.Task] = None

    # ─── учёт владельцев ────────────────────────────────────────────────
    def _remember(self, websocket: WebSocket, ctx, token: str, exp: int) -> None:
        if ctx is None:
            return
        self._meta[id(websocket)] = _Meta(ctx=ctx, token=token or "", exp=int(exp or 0))
        self._sockets[id(websocket)] = websocket
        if self._recheck_task is None or self._recheck_task.done():
            self._recheck_task = asyncio.get_running_loop().create_task(self._recheck_loop())

    def _forget(self, websocket: WebSocket) -> None:
        self._meta.pop(id(websocket), None)
        self._sockets.pop(id(websocket), None)

    async def _close(self, websocket: WebSocket, code: int) -> None:
        self._forget(websocket)
        self.disconnect_global(websocket)
        for pid in list(self._project_connections):
            self.disconnect_project(websocket, pid)
        try:
            await websocket.close(code=code)
        except Exception:
            pass

    async def revoke_account(self, account_id: Optional[int]) -> int:
        """Закрыть все соединения аккаунта (правка через API). → сколько закрыто."""
        if account_id is None:
            return 0
        victims = [self._sockets[k] for k, m in list(self._meta.items())
                   if getattr(m.ctx, "account_id", None) == account_id and k in self._sockets]
        for ws in victims:
            await self._close(ws, CLOSE_REVOKED)
        return len(victims)

    async def recheck_once(self) -> int:
        """Перепроверить все соединения по актуальному состоянию. → сколько закрыто."""
        from backend.app.core import portal_auth

        closed = 0
        settings = portal_auth.get_settings()
        now = int(time.time())
        for key, meta in list(self._meta.items()):
            ws = self._sockets.get(key)
            if ws is None:
                continue
            ok = meta.exp == 0 or meta.exp >= now
            if ok and settings.enabled:
                resolved = await asyncio.to_thread(portal_auth.resolve_token_sync, meta.token, settings)
                ctx = resolved.context
                ok = bool(ctx) and ws_allowed(ctx)
            if not ok:
                await self._close(ws, CLOSE_REVOKED)
                closed += 1
        return closed

    async def _recheck_loop(self) -> None:
        while self._meta:
            await asyncio.sleep(WS_RECHECK_SECONDS)
            try:
                await self.recheck_once()
            except Exception:  # noqa: BLE001 — перепроверка не должна умирать
                pass

    # ─── подключение ────────────────────────────────────────────────────
    async def connect_project(self, websocket: WebSocket, project_id: str, *, ctx=None, token: str = "", exp: int = 0):
        """Подключиться к комнате проекта."""
        await websocket.accept()
        self._loop = asyncio.get_running_loop()
        self._project_connections.setdefault(project_id, []).append(websocket)
        self._remember(websocket, ctx, token, exp)

    async def connect_global(self, websocket: WebSocket, *, ctx=None, token: str = "", exp: int = 0):
        """Подключиться к глобальной комнате."""
        await websocket.accept()
        self._loop = asyncio.get_running_loop()
        self._global_connections.append(websocket)
        self._remember(websocket, ctx, token, exp)

    def schedule_broadcast_to_project(self, project_id: str, message: WSMessage) -> None:
        """Запланировать broadcast_to_project из синхронного кода (в т.ч. из
        рабочего потока — на loop, запомненный при подключении клиента)."""
        coro = self.broadcast_to_project(project_id, message)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            loop = self._loop
            if loop is not None and not loop.is_closed():
                asyncio.run_coroutine_threadsafe(coro, loop)
            else:
                coro.close()
            return
        asyncio.ensure_future(coro)

    def disconnect_project(self, websocket: WebSocket, project_id: str):
        conns = self._project_connections.get(project_id, [])
        if websocket in conns:
            conns.remove(websocket)
        if not any(websocket in c for c in self._project_connections.values()) \
                and websocket not in self._global_connections:
            self._forget(websocket)

    def disconnect_global(self, websocket: WebSocket):
        if websocket in self._global_connections:
            self._global_connections.remove(websocket)
        if not any(websocket in c for c in self._project_connections.values()):
            self._forget(websocket)

    async def broadcast_to_project(self, project_id: str, message: WSMessage):
        """Отправить сообщение всем подписчикам проекта + глобальным."""
        json_str = json.dumps(message.model_dump(), ensure_ascii=False)
        dead = []
        for ws in list(self._project_connections.get(project_id, [])):
            try:
                await ws.send_text(json_str)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect_project(ws, project_id)
        dead_global = []
        for ws in list(self._global_connections):
            try:
                await ws.send_text(json_str)
            except Exception:
                dead_global.append(ws)
        for ws in dead_global:
            self.disconnect_global(ws)

    async def broadcast_global(self, message: WSMessage):
        """Отправить только глобальным подписчикам."""
        json_str = json.dumps(message.model_dump(), ensure_ascii=False)
        dead = []
        for ws in list(self._global_connections):
            try:
                await ws.send_text(json_str)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect_global(ws)

    @property
    def total_connections(self) -> int:
        total = len(self._global_connections)
        for conns in self._project_connections.values():
            total += len(conns)
        return total


def ws_allowed(ctx) -> bool:
    """Кому можно держать WebSocket: живые журналы и события всех проектов —
    служебный канал администратора (viewer'у не нужен, план Д-6)."""
    if ctx is None or getattr(ctx, "must_change_password", False):
        return False
    return bool(getattr(ctx, "is_admin", False)) or not getattr(ctx, "enforces_roles", False)


# Глобальный экземпляр
ws_manager = ConnectionManager()
