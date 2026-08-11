"""Ephemeral stream sessions; durable fencing lives in SQLite."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from contracts.agent_stream.v1 import agent_stream_pb2 as stream_pb

from backend.app.agent_gateway.metrics import GatewayMetrics


@dataclass
class GatewaySession:
    worker_id: str
    instance_id: str
    connection_id: str
    connection_epoch: int
    protocol_version: int
    max_slots: int
    outbound: asyncio.Queue[stream_pb.CenterToAgent]
    connected_at: float = field(default_factory=time.time)
    last_message_at: float = field(default_factory=time.time)
    last_heartbeat_at: float = field(default_factory=time.time)
    last_stream_sequence: int = 0
    outbound_sequence: int = 0
    active_slots: int = 0
    accepting_jobs: bool = True
    closing: asyncio.Event = field(default_factory=asyncio.Event)
    superseded: bool = False
    sent_offers: set[str] = field(default_factory=set)
    sent_commands: set[str] = field(default_factory=set)
    sent_results: set[str] = field(default_factory=set)

    @property
    def free_slots(self) -> int:
        if not self.accepting_jobs:
            return 0
        return max(0, int(self.max_slots) - int(self.active_slots) - len(self.sent_offers))

    def enqueue(self, message: stream_pb.CenterToAgent) -> None:
        self.outbound.put_nowait(message)


class GatewayConnectionRegistry:
    def __init__(self, metrics: GatewayMetrics) -> None:
        self._sessions: dict[str, GatewaySession] = {}
        self._lock = asyncio.Lock()
        self.metrics = metrics

    async def register(self, session: GatewaySession) -> GatewaySession | None:
        async with self._lock:
            old = self._sessions.get(session.worker_id)
            self._sessions[session.worker_id] = session
            if old is not None and old.connection_id != session.connection_id:
                old.superseded = True
            self.metrics.set_active(len(self._sessions))
            return old

    async def unregister(self, session: GatewaySession) -> None:
        async with self._lock:
            if self._sessions.get(session.worker_id) is session:
                self._sessions.pop(session.worker_id, None)
            self.metrics.set_active(len(self._sessions))

    async def get(self, worker_id: str) -> GatewaySession | None:
        async with self._lock:
            return self._sessions.get(worker_id)

    async def drain(self) -> None:
        async with self._lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            session.closing.set()

    async def count(self) -> int:
        async with self._lock:
            return len(self._sessions)
