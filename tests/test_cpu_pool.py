"""Тесты общего пула процессов для CPU-тяжёлых этапов.

Пул один на процесс бэкенда и общий для всех проектов очереди — это тот бюджет
ядер, который делится между параллельными проектами. Проверяем:

  1. Размер пула считается от доступных ядер с резервом под сам бэкенд.
  2. CPU_POOL_WORKERS переопределяет авто-расчёт.
  3. CPU_POOL_WORKERS=1 → пул не поднимается, работа идёт в потоке.
  4. Работа реально уходит в РАЗНЫЕ процессы (то, чего не даёт to_thread из-за GIL).
  5. CPU_POOL_PIN_CORES=true сажает каждый воркер на своё ядро.
  6. Сломанный пул не роняет стадию: disable_pool → fallback в поток.
  7. pool_info честно отражает состояние.

Run: python -m pytest tests/test_cpu_pool.py -v
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import cpu_pool  # noqa: E402


# Модульного уровня — чтобы функция была picklable для spawn-воркеров.
def _probe(marker: int):
    """Вернуть pid и привязку к ядрам — по ним видно, где реально считалось."""
    try:
        aff = tuple(sorted(os.sched_getaffinity(0)))
    except Exception:
        aff = None
    return os.getpid(), aff, marker


@pytest.fixture(autouse=True)
def _reset_pool():
    """Каждый тест стартует с чистым пулом и без унаследованных env."""
    saved = {k: os.environ.get(k) for k in ("CPU_POOL_WORKERS", "CPU_POOL_PIN_CORES")}
    cpu_pool.shutdown_pool()
    cpu_pool._POOL_DISABLED = False
    yield
    cpu_pool.shutdown_pool()
    cpu_pool._POOL_DISABLED = False
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def test_pool_size_reserves_cores_for_backend(monkeypatch):
    """Авто-размер оставляет ядра под HTTP/WS и не превышает потолок."""
    monkeypatch.delenv("CPU_POOL_WORKERS", raising=False)
    monkeypatch.setattr(cpu_pool, "available_cores", lambda: list(range(16)))
    assert cpu_pool.pool_workers() == min(cpu_pool.DEFAULT_MAX_WORKERS, 16 - cpu_pool.RESERVED_CORES)

    # Маленькая машина: не уходим в ноль или отрицательное.
    monkeypatch.setattr(cpu_pool, "available_cores", lambda: [0])
    assert cpu_pool.pool_workers() == 1


def test_env_overrides_pool_size(monkeypatch):
    monkeypatch.setenv("CPU_POOL_WORKERS", "3")
    assert cpu_pool.pool_workers() == 3

    # Мусор в переменной не должен ронять расчёт — падаем на авто.
    monkeypatch.setenv("CPU_POOL_WORKERS", "не-число")
    monkeypatch.setattr(cpu_pool, "available_cores", lambda: list(range(8)))
    assert cpu_pool.pool_workers() == min(cpu_pool.DEFAULT_MAX_WORKERS, 8 - cpu_pool.RESERVED_CORES)


def test_single_worker_runs_inline_without_pool(monkeypatch):
    """CPU_POOL_WORKERS=1 → пул не поднимаем, считаем в потоке (как до параллели)."""
    monkeypatch.setenv("CPU_POOL_WORKERS", "1")
    assert cpu_pool._get_pool() is None

    pid, _, marker = asyncio.run(cpu_pool.run(_probe, 42))
    assert marker == 42
    assert pid == os.getpid()  # тот же процесс — работа не уехала в пул


def test_work_spreads_across_processes(monkeypatch):
    """Главное свойство: задачи считаются в РАЗНЫХ процессах, а не под одним GIL."""
    monkeypatch.setenv("CPU_POOL_WORKERS", "4")
    monkeypatch.delenv("CPU_POOL_PIN_CORES", raising=False)

    async def _run():
        return await asyncio.gather(*[cpu_pool.run(_probe, i) for i in range(12)])

    res = asyncio.run(_run())
    pids = {pid for pid, _, _ in res}
    assert len(pids) > 1, "работа не разошлась по процессам — пул не задействован"
    assert os.getpid() not in pids, "считали в родительском процессе вместо пула"
    assert sorted(m for _, _, m in res) == list(range(12))


@pytest.mark.skipif(
    not hasattr(os, "sched_setaffinity"), reason="привязка к ядрам только на Linux"
)
def test_pin_cores_gives_each_worker_own_core(monkeypatch):
    """CPU_POOL_PIN_CORES=true → каждый воркер садится на одно (своё) ядро."""
    if len(cpu_pool.available_cores()) < 2:
        pytest.skip("нужно минимум 2 ядра")

    monkeypatch.setenv("CPU_POOL_WORKERS", "2")
    monkeypatch.setenv("CPU_POOL_PIN_CORES", "true")

    async def _run():
        return await asyncio.gather(*[cpu_pool.run(_probe, i) for i in range(8)])

    res = asyncio.run(_run())
    affinities = {aff for _, aff, _ in res}
    assert affinities, "не получили привязок"
    for aff in affinities:
        assert aff is not None and len(aff) == 1, f"воркер не привязан к одному ядру: {aff}"
    # Два воркера — два РАЗНЫХ ядра, а не оба на нулевом.
    assert len(affinities) == 2, f"воркеры сели на одно ядро: {affinities}"


def test_broken_pool_falls_back_to_thread(monkeypatch):
    """Сломанный executor не роняет этап — досчитываем в потоке."""
    from concurrent.futures import BrokenExecutor

    monkeypatch.setenv("CPU_POOL_WORKERS", "2")

    class _BrokenPool:
        def submit(self, *a, **kw):
            raise BrokenExecutor("подопытный сбой")

    monkeypatch.setattr(cpu_pool, "_get_pool", lambda: _BrokenPool())

    # run_in_executor поднимет BrokenExecutor — ждём тихий fallback, не исключение.
    loop_run = asyncio.AbstractEventLoop.run_in_executor

    def _raising(self, executor, func, *args):
        fut = asyncio.get_event_loop().create_future()
        fut.set_exception(BrokenExecutor("подопытный сбой"))
        return fut

    monkeypatch.setattr(asyncio.AbstractEventLoop, "run_in_executor", _raising)
    try:
        pid, _, marker = asyncio.run(cpu_pool.run(_probe, 7))
    finally:
        monkeypatch.setattr(asyncio.AbstractEventLoop, "run_in_executor", loop_run)

    assert marker == 7
    assert pid == os.getpid()  # посчитали здесь же, в потоке
    assert cpu_pool._POOL_DISABLED is True


def test_pool_info_reports_state(monkeypatch):
    monkeypatch.setenv("CPU_POOL_WORKERS", "2")
    info = cpu_pool.pool_info()
    assert info["configured"] == 2
    assert info["alive"] is False  # ленивый: ещё не поднят

    asyncio.run(cpu_pool.run(_probe, 1))
    info = cpu_pool.pool_info()
    assert info["alive"] is True
    assert info["workers"] == 2
    assert info["cores"] >= 1
