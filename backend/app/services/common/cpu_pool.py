"""Общий пул процессов для CPU-тяжёлой работы конвейера.

Зачем отдельный модуль
──────────────────────
Когда очередь гоняет несколько проектов ОДНОВРЕМЕННО, вся CPU-работа
(разбор вектор-слоя, рендер PDF, геометрия профилей) конкурирует за одни и те
же ядра. Два антипаттерна, которых этот модуль избегает:

1. `asyncio.to_thread` для чистого CPU — упирается в GIL: N проектов
   «выполняются», а реально считает одно ядро (бенч на 16 ядрах: 85% CPU у
   бэкенда и 5–22 с на блок при чистых 1–1.5 с; см. block_context/builder.py).
2. Свой пул на каждый проект — N проектов × M воркеров выносят машину.

Отсюда правило: **пул ровно один на процесс бэкенда и общий для всех
проектов очереди**. Он и есть тот бюджет ядер, который делится между
параллельными проектами; планировщик ОС раскидывает воркеры сам.

Распределение по ядрам
──────────────────────
`CPU_POOL_PIN_CORES=true` включает жёсткую привязку: воркер i садится на ядро
`cores[i % len(cores)]` через `os.sched_setaffinity`. Это даёт локальность кэша
и убирает миграцию задач между ядрами под нагрузкой. По умолчанию ВЫКЛЮЧЕНО:
для смешанной нагрузки (CPU + сеть) свободный планировщик обычно не хуже, а
пиннинг мешает ему балансировать. Включать осмысленно, когда на машине
одновременно живут несколько проектов и профиль нагрузки ровный.

Метод старта — spawn: fork из многопоточного uvicorn-процесса рискует
дедлоком в дочернем.

Всё fail-soft: если пул недоступен (нет прав на форк, сломанный executor) —
`run()` считает в текущем потоке, стадия не падает.

Переменные окружения:
  CPU_POOL_WORKERS    — размер пула (0/пусто → авто: min(8, ядра − 2))
  CPU_POOL_PIN_CORES  — true/1/yes → привязать воркеры к ядрам
"""
from __future__ import annotations

import asyncio
import multiprocessing
import os
import threading
from concurrent.futures import BrokenExecutor, ProcessPoolExecutor
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")

# Столько ядер оставляем бэкенду: HTTP/WS и сам event loop не должны голодать
# на фоне CPU-пула.
RESERVED_CORES = 2
DEFAULT_MAX_WORKERS = 8

_POOL_LOCK = threading.Lock()
_POOL: Optional[ProcessPoolExecutor] = None
_POOL_DISABLED = False
_POOL_WORKERS = 0


def _env_flag(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def available_cores() -> list[int]:
    """Ядра, доступные ЭТОМУ процессу (учитывает cgroup/taskset контейнера)."""
    try:
        return sorted(os.sched_getaffinity(0))
    except AttributeError:  # не Linux
        return list(range(os.cpu_count() or 1))


def pool_workers() -> int:
    """Размер пула. CPU_POOL_WORKERS=1 → работа в текущем потоке (пул не нужен)."""
    # BLOCK_CONTEXT_WORKERS — legacy-имя: до появления общего пула им задавался
    # размер пула в block_context/builder.py. Держим как запасной ключ, чтобы у
    # тех, кто его уже настроил, размер пула не поехал молча.
    for key in ("CPU_POOL_WORKERS", "BLOCK_CONTEXT_WORKERS"):
        raw = (os.environ.get(key) or "").strip()
        if not raw:
            continue
        try:
            requested = int(raw)
        except ValueError:
            continue
        if requested > 0:
            return requested
    cores = len(available_cores())
    return max(1, min(DEFAULT_MAX_WORKERS, cores - RESERVED_CORES))


_PIN_COUNTER: Any = None  # multiprocessing.Value, создаётся вместе с пулом


def _pin_initializer(counter, cores: list[int]) -> None:
    """Инициализатор воркера: посадить процесс на своё ядро.

    Порядковый номер воркера берём из общего счётчика — ProcessPoolExecutor не
    сообщает индекс воркера, а имя процесса (`SpawnProcess-N`) не гарантирует
    плотную нумерацию при перезапуске упавшего воркера.
    """
    try:
        with counter.get_lock():
            idx = counter.value
            counter.value += 1
        core = cores[idx % len(cores)]
        os.sched_setaffinity(0, {core})
    except Exception:
        # Пиннинг — оптимизация, а не требование: не смогли — считаем как есть.
        pass


def _get_pool() -> Optional[ProcessPoolExecutor]:
    """Ленивый общий пул. None → считать в текущем потоке."""
    global _POOL, _POOL_DISABLED, _POOL_WORKERS, _PIN_COUNTER
    if _POOL_DISABLED:
        return None
    workers = pool_workers()
    if workers <= 1:
        return None
    with _POOL_LOCK:
        if _POOL_DISABLED:
            return None
        if _POOL is None:
            ctx = multiprocessing.get_context("spawn")
            kwargs: dict[str, Any] = {"max_workers": workers, "mp_context": ctx}
            if _env_flag("CPU_POOL_PIN_CORES"):
                cores = available_cores()
                if cores:
                    _PIN_COUNTER = ctx.Value("i", 0)
                    kwargs["initializer"] = _pin_initializer
                    kwargs["initargs"] = (_PIN_COUNTER, cores)
            try:
                _POOL = ProcessPoolExecutor(**kwargs)
                _POOL_WORKERS = workers
            except Exception as exc:  # окружение без права форка и т.п.
                _POOL_DISABLED = True
                print(f"[cpu_pool] пул процессов недоступен ({exc}); считаем в потоке")
                return None
        return _POOL


def get_executor() -> Optional[ProcessPoolExecutor]:
    """Сырой executor для вызывающих со своей логикой подачи задач.

    Нужен там, где мало `run()`: например block_context подаёт блоки окном и
    сам обрабатывает падение отдельного блока. Владелец пула всё равно один —
    этот модуль, поэтому N проектов делят общий бюджет ядер.
    """
    return _get_pool()


def disable_pool(reason: str) -> None:
    """Пул сломался — дальше считаем в потоке, стадия не падает."""
    global _POOL, _POOL_DISABLED
    with _POOL_LOCK:
        _POOL_DISABLED = True
        pool, _POOL = _POOL, None
    print(f"[cpu_pool] пул процессов отключён: {reason}")
    if pool is not None:
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass


def shutdown_pool() -> None:
    """Погасить пул (вызывается на shutdown бэкенда)."""
    global _POOL
    with _POOL_LOCK:
        pool, _POOL = _POOL, None
    if pool is not None:
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass


def pool_info() -> dict:
    """Диагностика для /api — что реально поднято."""
    return {
        "workers": _POOL_WORKERS if _POOL is not None else 0,
        "configured": pool_workers(),
        "cores": len(available_cores()),
        "pinned": _env_flag("CPU_POOL_PIN_CORES"),
        "disabled": _POOL_DISABLED,
        "alive": _POOL is not None,
    }


async def run(fn: Callable[..., T], *args: Any) -> T:
    """Выполнить CPU-функцию в общем пуле (fallback — поток).

    `fn` и аргументы должны быть picklable: пул стартует через spawn.
    """
    pool = _get_pool()
    if pool is None:
        return await asyncio.to_thread(fn, *args)
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(pool, fn, *args)
    except (BrokenExecutor, OSError) as exc:
        disable_pool(f"{type(exc).__name__}: {exc}")
        return await asyncio.to_thread(fn, *args)
