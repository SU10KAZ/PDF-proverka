"""Приложение нагрузочного стенда: боевое приложение + заглушка длительной
задачи администратора. ТОЛЬКО для стенда (scripts/load), в релиз не входит:
``backend.app.main`` этот модуль не импортирует.

``POST /api/_stand/long-admin-task?seconds=N`` (только admin по таблице
политик: неизвестный маршрут — admin) имитирует длительную работу
администратора без моделей:
* поток, который кусками держит общий замок сравнения стадий
  (``store._lock``) и загружает CPU — как пересчёт сравнения;
* поток с хешированием больших буферов — как сборка/проверка артефактов;
* ``slots`` задач, занимающих поток пула anyio на всё время — как
  production run, держащий поток пула весь прогон.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import time

from fastapi.concurrency import run_in_threadpool

from backend.app.main import app

_STATE = {"running": False, "started_at": None, "seconds": 0}


def _lock_and_cpu(seconds: float) -> None:
    from backend.app.services.stage_comparison import store

    end = time.time() + seconds
    while time.time() < end:
        with store._lock:
            t0 = time.time()
            x = 0
            while time.time() - t0 < 1.5:
                x += sum(i * i for i in range(2000))
        time.sleep(0.5)


def _hashing(seconds: float) -> None:
    end = time.time() + seconds
    buf = os.urandom(8 * 1024 * 1024)
    while time.time() < end:
        hashlib.sha256(buf).hexdigest()


@app.post("/api/_stand/long-admin-task")
async def long_admin_task(seconds: int = 300, slots: int = 4):
    if _STATE["running"]:
        return {"status": "already_running", **_STATE}
    _STATE.update(running=True, started_at=time.time(), seconds=seconds)
    threading.Thread(target=_lock_and_cpu, args=(seconds,), daemon=True, name="stand-lock-cpu").start()
    threading.Thread(target=_hashing, args=(seconds,), daemon=True, name="stand-hash").start()
    for _ in range(max(0, slots)):
        asyncio.get_running_loop().create_task(run_in_threadpool(time.sleep, seconds))

    async def _done():
        await asyncio.sleep(seconds)
        _STATE["running"] = False

    asyncio.get_running_loop().create_task(_done())
    return {"status": "started", **_STATE}
