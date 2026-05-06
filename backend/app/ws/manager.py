"""
WebSocket Connection Manager.
Управляет подключениями, комнатами (по проектам), broadcast.
"""
import json
import asyncio
from typing import Optional
from fastapi import WebSocket
from backend.app.models.websocket import WSMessage


class ConnectionManager:
    """Синглтон-менеджер WebSocket-подключений."""

    def __init__(self):
        # Подключения по проектам: {project_id: [ws1, ws2, ...]}
        self._project_connections: dict[str, list[WebSocket]] = {}
        # Глобальные подключения (получают все события)
        self._global_connections: list[WebSocket] = []

    async def connect_project(self, websocket: WebSocket, project_id: str):
        """Подключиться к комнате проекта."""
        await websocket.accept()
        if project_id not in self._project_connections:
            self._project_connections[project_id] = []
        self._project_connections[project_id].append(websocket)

    async def connect_global(self, websocket: WebSocket):
        """Подключиться к глобальной комнате."""
        await websocket.accept()
        self._global_connections.append(websocket)

    def disconnect_project(self, websocket: WebSocket, project_id: str):
        """Отключиться от комнаты проекта."""
        conns = self._project_connections.get(project_id, [])
        if websocket in conns:
            conns.remove(websocket)

    def disconnect_global(self, websocket: WebSocket):
        """Отключиться от глобальной комнаты."""
        if websocket in self._global_connections:
            self._global_connections.remove(websocket)

    async def broadcast_to_project(self, project_id: str, message: WSMessage):
        """Отправить сообщение всем подписчикам проекта + глобальным."""
        data = message.model_dump()
        json_str = json.dumps(data, ensure_ascii=False)

        # Отправить подписчикам проекта
        dead = []
        for ws in self._project_connections.get(project_id, []):
            try:
                await ws.send_text(json_str)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect_project(ws, project_id)

        # Отправить глобальным подписчикам
        dead_global = []
        for ws in self._global_connections:
            try:
                await ws.send_text(json_str)
            except Exception:
                dead_global.append(ws)
        for ws in dead_global:
            self.disconnect_global(ws)

    async def broadcast_global(self, message: WSMessage):
        """Отправить только глобальным подписчикам."""
        data = message.model_dump()
        json_str = json.dumps(data, ensure_ascii=False)

        dead = []
        for ws in self._global_connections:
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


# Глобальный экземпляр
ws_manager = ConnectionManager()
