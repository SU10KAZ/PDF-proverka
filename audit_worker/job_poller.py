"""Получение следующего задания через long-poll.

Тянем ТОЛЬКО когда есть свободный слот: иначе воркер заберёт работу, которую
не сможет выполнить, и она зависнет в assigned у него, а не у центра.
"""
from __future__ import annotations

from typing import Any, Optional

from audit_worker.client import CenterClient, CenterError


class JobPullClient:
    def __init__(self, client: CenterClient, *, wait_sec: int = 25):
        self._client = client
        self._wait_sec = max(0, min(60, wait_sec))

    def poll(
        self,
        *,
        free_slots: int,
        compressions: list[str],
        executor_status: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """Вернуть JobAssignment или None (204 / нет слотов / временная ошибка).

        `executor_status` — состояние ЛОКАЛЬНОГО исполнителя на момент запроса.
        Центр учитывает его в своём расчёте ёмкости: снимок из heartbeat может
        отставать на весь его период, и «исполнитель был офлайн полминуты
        назад» — плохое основание отказать в работе, которую уже есть кому
        делать.
        """
        if free_slots <= 0:
            return None
        body: dict[str, Any] = {
            "free_slots": free_slots,
            "accepts": {"compressions": compressions},
            "wait_sec": self._wait_sec,
        }
        if executor_status:
            body["executor_status"] = executor_status
        try:
            return self._client.next_job(body)
        except CenterError as exc:
            if exc.status in (204, 409):
                return None
            raise
