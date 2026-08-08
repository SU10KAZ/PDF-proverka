"""Подключение к SQLite подсистемы воркеров.

Дисциплина использования (ADR-007 техпроекта) — нарушение любого пункта даёт
новый класс инцидента, поэтому она вынесена в один модуль:

  * WAL + busy_timeout + foreign_keys выставляются на каждом соединении;
  * ОДИН writer-коннект под threading.Lock; читатели получают свои соединения;
  * из async-кода ходить ТОЛЬКО через `run_db()` (asyncio.to_thread).
    Синхронный sqlite3 в event loop — известная причина смерти бэкенда:
    watchdog убивает процесс, если /api/info не отвечает (см. первый аудит);
  * база НЕ создаётся, пока подсистема выключена флагом.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional, TypeVar

from backend.app.services.distributed_workers import schema
from backend.app.services.distributed_workers.settings import (
    DistributedWorkersSettings,
    get_settings,
)

T = TypeVar("T")

_write_lock = threading.Lock()
_local = threading.local()
# Путь → уже мигрированная база. Миграция один раз на процесс и на путь
# (в тестах путей много: каждый tmp_path — своя база).
_migrated: set[str] = set()
_migrate_lock = threading.Lock()


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    schema.apply_pragmas(conn)
    return conn


def ensure_ready(settings: DistributedWorkersSettings | None = None) -> Path:
    """Создать каталоги и применить миграции. Идемпотентно.

    Вызывается лениво из первой же операции с БД, а не на импорте: при
    выключенном флаге база не должна появляться на диске вовсе.
    """
    st = settings or get_settings()
    st.require_enabled()
    for directory in (
        st.data_dir,
        st.source_packages_dir,
        st.incoming_dir,
        st.result_staging_dir,
        st.validated_results_dir,
        st.rejected_results_dir,
        st.superseded_results_dir,
        st.job_logs_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    key = str(st.db_path)
    if key in _migrated:
        return st.db_path
    with _migrate_lock:
        if key not in _migrated:
            conn = _connect(st.db_path)
            try:
                _backup_before_migration(conn, st.db_path)
                schema.migrate(conn)
            finally:
                conn.close()
            _migrated.add(key)
    return st.db_path


def _backup_before_migration(conn: sqlite3.Connection, db_path: Path) -> Optional[Path]:
    """Снять копию базы ПЕРЕД применением миграций. Возвращает путь копии.

    Миграция 3 перестраивает хранение заданий и удаляет таблицу remote_jobs.
    Forward-only миграция без копии означала бы «откатиться нечем»; копия
    снимается через VACUUM INTO — это согласованный снимок, а не cp файла
    рядом с живым WAL.

    Копия НЕ снимается, если мигрировать нечего или база ещё пуста: плодить
    файлы на каждом старте незачем.
    """
    pending = schema.pending_migrations(conn)
    if not pending:
        return None
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','view')"
        " AND name NOT LIKE 'sqlite_%'"
    ).fetchone()
    if not row or int(row[0]) <= 1:      # только schema_migrations → новая база
        return None
    version = schema.current_version(conn)
    target = db_path.with_name(f"{db_path.name}.before_v{version}_to_v{max(pending)}")
    if target.exists():
        return target
    try:
        conn.execute("VACUUM INTO ?", (str(target),))
    except sqlite3.Error:
        # Резервная копия — страховка, а не условие работы. Отсутствие места
        # или старый SQLite не должны блокировать запуск подсистемы; факт
        # неудачи виден в логе, а миграция всё равно транзакционная.
        logging.getLogger(__name__).warning(
            "Не удалось снять резервную копию %s перед миграцией", db_path
        )
        return None
    return target


def _thread_conn(path: Path) -> sqlite3.Connection:
    """Соединение, закреплённое за потоком (sqlite3 не любит шаринг между потоками)."""
    cache: dict[str, sqlite3.Connection] = getattr(_local, "conns", None) or {}
    key = str(path)
    conn = cache.get(key)
    if conn is None:
        conn = _connect(path)
        cache[key] = conn
        _local.conns = cache
    return conn


@contextmanager
def read_conn(settings: DistributedWorkersSettings | None = None):
    """Читающее соединение. Без глобального лока — WAL допускает многих читателей."""
    st = settings or get_settings()
    path = ensure_ready(st)
    yield _thread_conn(path)


@contextmanager
def write_txn(settings: DistributedWorkersSettings | None = None):
    """Пишущая транзакция под глобальным локом.

    SQLite допускает одного писателя; явная сериализация избавляет от
    SQLITE_BUSY вместо надежды на busy_timeout. Транзакции обязаны быть
    короткими: никаких сетевых ожиданий и long-poll внутри.
    """
    st = settings or get_settings()
    path = ensure_ready(st)
    with _write_lock:
        conn = _thread_conn(path)
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")


async def run_db(fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Выполнить синхронную работу с БД вне event loop.

    ЕДИНСТВЕННЫЙ разрешённый способ обращения к базе из async-обработчиков.
    """
    return await asyncio.to_thread(fn, *args, **kwargs)


def reset_state_for_tests() -> None:
    """Сбросить закэшированные соединения и отметки миграций (только для тестов)."""
    cache: dict[str, sqlite3.Connection] = getattr(_local, "conns", None) or {}
    for conn in cache.values():
        try:
            conn.close()
        except Exception:  # noqa: BLE001 — закрытие на тестовом пути не критично
            pass
    _local.conns = {}
    with _migrate_lock:
        _migrated.clear()
