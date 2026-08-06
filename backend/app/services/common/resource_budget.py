"""Глобальные бюджеты внешних ресурсов — общие на ВЕСЬ бэкенд.

Зачем
─────
Все ограничители параллелизма в конвейере — «на проект»: семафор создаётся
внутри функции этапа и живёт ровно один аудит. Пока очередь вела один проект,
это и был глобальный лимит. При пяти проектах каждый лимит умножается на пять:

  • `claude -p`   — absence_guard 4 + decision_carryover 4 → до 20 процессов;
  • `codex exec`  — Stage 01 (2 блока × 3 ноги + gap-search) → около 20;
  • норм-MCP      — поднимается НА КАЖДЫЙ процесс CLI и весит ~2,8 ГБ;
                    на этапе optimization это до 10 серверов ≈ 28 ГБ RAM;
  • локальная LLM — одна на машину, и перезагрузка модели под чужой
                    context_length рвёт inflight-запросы соседних проектов.

Первый же параллельный прогон без бюджетов упирается не в скорость, а в
rate-limit подписок и OOM-killer. Поэтому лимит должен быть один на процесс
бэкенда, а не на проект.

Как пользоваться
────────────────
    from backend.app.services.common import resource_budget

    async with resource_budget.slot("norms_mcp"):
        ...  # запуск CLI, который поднимет норм-MCP

Слот берётся на время ОДНОГО внешнего вызова. Не оборачивайте им целую стадию:
проект будет держать слот, пока считает локально, и соседи встанут зря.

Значения по умолчанию рассчитаны на 16 ядер / 62 ГБ и BATCH_MAX_PARALLEL=5.
Каждый переопределяется переменной окружения `BUDGET_<ИМЯ>` и читается на
каждый запрос слота — можно менять без рестарта.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncIterator, Optional

# Имя → сколько одновременных операций разрешено на весь бэкенд.
#
# claude_cli / codex_cli: измерено, что подписка держит десятки одновременных
#   сессий (см. эксперимент 06.08.2026), поэтому потолок здесь не про
#   работоспособность, а про расход квоты и RAM на Node-процессы.
# norms_mcp: жёстко про RAM. 2 × 2,8 ГБ ≈ 5,6 ГБ — предел, за которым на
#   11-гигабайтном хосте начинается OOM.
# local_llm: модель на машине одна; больше 1 — это не параллель, а пинг-понг
#   перезагрузок контекста, рвущий чужие запросы.
DEFAULTS: dict[str, int] = {
    "claude_cli": 6,
    "codex_cli": 6,
    "norms_mcp": 2,
    "local_llm": 1,
}

# Имена переменных окружения записаны литералами намеренно: реестр флагов
# (scripts/audit_env_flags.py) ищет читателей грепом, и вычисляемое
# `os.environ[f"BUDGET_{name.upper()}"]` он бы не увидел — флаг попал бы в
# orphan'ы. Заодно это единственное место, где видно полный список ручек.
ENV_KEYS: dict[str, str] = {
    "claude_cli": "BUDGET_CLAUDE_CLI",
    "codex_cli": "BUDGET_CODEX_CLI",
    "norms_mcp": "BUDGET_NORMS_MCP",
    "local_llm": "BUDGET_LOCAL_LLM",
}

# Семафоры привязаны к event loop: asyncio.Semaphore, созданный в одном loop,
# в другом не работает (тесты поднимают loop на каждый прогон).
_semaphores: dict[tuple[int, str], asyncio.Semaphore] = {}
_limits: dict[tuple[int, str], int] = {}


def limit_for(name: str) -> int:
    """Текущий лимит для ресурса (env `BUDGET_<ИМЯ>` перекрывает дефолт)."""
    env_key = ENV_KEYS.get(name) or f"BUDGET_{name.upper()}"
    raw = (os.environ.get(env_key) or "").strip()
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return DEFAULTS.get(name, 1)


def _key(name: str) -> tuple[int, str]:
    try:
        loop_id = id(asyncio.get_running_loop())
    except RuntimeError:
        loop_id = 0
    return loop_id, name


def _get_semaphore(name: str) -> asyncio.Semaphore:
    key = _key(name)
    limit = limit_for(name)
    sem = _semaphores.get(key)
    # Лимит поменяли в env на живой системе — пересоздаём. Уже занятые слоты
    # доработают на старом объекте, новый вступит в силу со следующего входа.
    if sem is None or _limits.get(key) != limit:
        sem = asyncio.Semaphore(limit)
        _semaphores[key] = sem
        _limits[key] = limit
    return sem


# Какие слоты уже удерживает ТЕКУЩАЯ задача. Нужен для реентерабельности:
# локальные транспорты после перезагрузки модели вызывают сами себя, и
# повторный вход в тот же семафор (asyncio.Semaphore не реентерабелен)
# намертво заклинил бы задачу на самой себе.
_held: ContextVar[frozenset[str]] = ContextVar("resource_budget_held", default=frozenset())


@asynccontextmanager
async def slot(name: str) -> AsyncIterator[None]:
    """Занять один слот ресурса на время блока.

    Реентерабелен: если эта же задача уже держит слот с таким именем, вложенный
    вход проходит без ожидания. Неизвестное имя не блокирует ничего — лучше
    пропустить лимит, чем уронить стадию из-за опечатки.
    """
    if name not in DEFAULTS:
        yield
        return
    held = _held.get()
    if name in held:
        yield
        return
    sem = _get_semaphore(name)
    async with sem:
        token = _held.set(held | {name})
        try:
            yield
        finally:
            _held.reset(token)


def snapshot() -> dict[str, dict[str, Optional[int]]]:
    """Диагностика для /api: лимиты и сколько слотов свободно."""
    out: dict[str, dict[str, Optional[int]]] = {}
    for name in DEFAULTS:
        key = _key(name)
        sem = _semaphores.get(key)
        out[name] = {
            "limit": limit_for(name),
            "free": None if sem is None else sem._value,  # noqa: SLF001 — только для диагностики
            "active": sem is not None,
        }
    return out


def reset_for_tests() -> None:
    """Сбросить кэш семафоров (тесты меняют лимиты между прогонами)."""
    _semaphores.clear()
    _limits.clear()
