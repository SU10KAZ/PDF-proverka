"""Получение следующего задания через long-poll.

Тянем ТОЛЬКО когда есть свободный слот: иначе воркер заберёт работу, которую
не сможет выполнить, и она зависнет в assigned у него, а не у центра.
"""
from __future__ import annotations

import uuid
from typing import Any, Optional

from audit_worker.client import CenterClient, CenterError


class JobPullClient:
    def __init__(self, client: CenterClient, *, wait_sec: int = 25):
        self._client = client
        self._wait_sec = max(0, min(60, wait_sec))
        # Ключ незавершённого запроса. Живёт ровно до первого определённого
        # ответа центра, см. `poll`.
        self._retry_key: Optional[str] = None

    def poll(
        self,
        *,
        free_slots: int,
        compressions: list[str],
        executor_status: Optional[str] = None,
        busy_slots: Optional[int] = None,
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
        if busy_slots is not None:
            body["busy_slots"] = max(0, int(busy_slots))

        # Ключ идемпотентности нужен ровно для одного случая: запрос дошёл,
        # центр выдал задание и ЗАНЯЛ слот, а ответ потерялся по дороге. Воркер
        # о задании не знает и никогда о нём не спросит — слот остаётся занятым.
        # Центр умел разбирать повтор по ключу давно, но агент ключа не слал,
        # и вся эта ветка была мёртвой.
        #
        # Поэтому ключ НОВЫЙ на каждый обычный опрос (это разные запросы работы)
        # и ТОТ ЖЕ, если прошлый запрос закончился обрывом: только там ответ мог
        # потеряться. Любой определённый ответ центра — задание, 204, 409 —
        # означает, что судьба прошлого запроса известна, и ключ сбрасывается.
        key = self._retry_key or uuid.uuid4().hex
        try:
            assignment = self._client.next_job(body, idempotency_key=key)
        except CenterError as exc:
            self._retry_key = None
            if exc.status in (204, 409):
                return None
            raise
        except Exception:
            # Обрыв соединения или таймаут: повторим ТЕМ ЖЕ ключом.
            self._retry_key = key
            raise
        self._retry_key = None
        return assignment
