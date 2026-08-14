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
import os
import sqlite3
import stat
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
from backend.app.services.distributed_workers.state_permissions import (
    STATIC_DIRECTORY_NAMES,
    validate_runtime_shared_state,
)

T = TypeVar("T")

_write_lock = threading.Lock()
_local = threading.local()
# Путь → уже мигрированная база. Миграция один раз на процесс и на путь
# (в тестах путей много: каждый tmp_path — своя база).
_migrated: set[str] = set()
_migrate_lock = threading.Lock()


def _connect(st: DistributedWorkersSettings) -> sqlite3.Connection:
    conn = sqlite3.connect(str(st.db_path), timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    schema.apply_pragmas(conn)
    _secure_database_files(st)
    return conn


def _validate_plain_path(path: Path, *, directory: bool) -> os.stat_result:
    """Reject symlinks and unexpected file types in the durable state tree."""
    value = path.lstat()
    expected = stat.S_ISDIR(value.st_mode) if directory else stat.S_ISREG(value.st_mode)
    if stat.S_ISLNK(value.st_mode) or not expected:
        kind = "directory" if directory else "file"
        raise RuntimeError(f"distributed Worker state {kind} is not a plain path: {path}")
    return value


def _enforce_private_permissions(
    path: Path,
    *,
    directory: bool,
) -> None:
    """Retain historical caller-owned 0700/0600 behavior in private mode."""
    _validate_plain_path(path, directory=directory)
    expected_mode = 0o700 if directory else 0o600
    try:
        os.chmod(path, expected_mode)
    except OSError as exc:
        raise RuntimeError(
            f"cannot enforce distributed Worker state mode {expected_mode:o}: {path}"
        ) from exc
    final = _validate_plain_path(path, directory=directory)
    if stat.S_IMODE(final.st_mode) != expected_mode:
        raise RuntimeError(
            f"distributed Worker state mode verification failed for {path}"
        )


def _state_directories(st: DistributedWorkersSettings) -> tuple[Path, ...]:
    return tuple(st.data_dir / name if name else st.data_dir for name in STATIC_DIRECTORY_NAMES)


def _prepare_or_validate_directories(st: DistributedWorkersSettings) -> None:
    if st.shared_state_enabled:
        assert st.shared_state_owner_uid is not None
        assert st.shared_state_gid is not None
        assert st.shared_state_receipt_path is not None
        validate_runtime_shared_state(
            data_dir=st.data_dir,
            owner_uid=st.shared_state_owner_uid,
            shared_gid=st.shared_state_gid,
            receipt_path=st.shared_state_receipt_path,
        )
        return
    for directory in _state_directories(st):
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        _enforce_private_permissions(directory, directory=True)


def _secure_database_files(st: DistributedWorkersSettings) -> None:
    """Retain only the historical private-state mode normalization.

    Shared-state metadata is fully deployment-owned.  In shared mode this
    request-path helper is deliberately a no-op: runtime never chmods/chowns or
    repairs persistent state.
    """
    if st.shared_state_enabled:
        return
    for path in (
        st.db_path,
        st.db_path.with_name(st.db_path.name + "-wal"),
        st.db_path.with_name(st.db_path.name + "-shm"),
    ):
        if path.exists() or path.is_symlink():
            _enforce_private_permissions(path, directory=False)


def ensure_ready(settings: DistributedWorkersSettings | None = None) -> Path:
    """Создать каталоги и применить миграции. Идемпотентно.

    Вызывается лениво из первой же операции с БД, а не на импорте: при
    выключенном флаге база не должна появляться на диске вовсе.
    """
    st = settings or get_settings()
    st.require_enabled()
    key = str(st.db_path)
    # The shared contract is a startup/readiness gate.  Once the exact state
    # has been accepted for this process, normal request paths do not perform
    # filesystem policy work.
    if st.shared_state_enabled and key in _migrated:
        return st.db_path
    _prepare_or_validate_directories(st)

    _secure_database_files(st)

    if key in _migrated:
        _secure_database_files(st)
        return st.db_path
    with _migrate_lock:
        if key not in _migrated:
            conn = _connect(st)
            try:
                _secure_database_files(st)
                backup = _backup_before_migration(conn, st.db_path)
                if backup is not None:
                    if not st.shared_state_enabled:
                        _enforce_private_permissions(backup, directory=False)
                schema.migrate(conn)
                _secure_database_files(st)
            finally:
                conn.close()
            _secure_database_files(st)
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


def _thread_conn(st: DistributedWorkersSettings) -> sqlite3.Connection:
    """Соединение, закреплённое за потоком (sqlite3 не любит шаринг между потоками)."""
    cache: dict[str, sqlite3.Connection] = getattr(_local, "conns", None) or {}
    key = str(st.db_path)
    conn = cache.get(key)
    if conn is None:
        conn = _connect(st)
        cache[key] = conn
        _local.conns = cache
    return conn


@contextmanager
def read_conn(settings: DistributedWorkersSettings | None = None):
    """Читающее соединение. Без глобального лока — WAL допускает многих читателей."""
    st = settings or get_settings()
    path = ensure_ready(st)
    yield _thread_conn(st)


def _drop_thread_conn(path: Path) -> None:
    """Выбросить потоко-локальное соединение: следующий вызов откроет новое."""
    cache: dict[str, sqlite3.Connection] = getattr(_local, "conns", None) or {}
    conn = cache.pop(str(path), None)
    if conn is not None:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 — закрываем как получится
            pass
    _local.conns = cache


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
        conn = _thread_conn(st)
        conn.execute("BEGIN IMMEDIATE")
        _secure_database_files(st)
            # COMMIT и ROLLBACK — внутри try, и оба умеют не сработать.
            # Раньше COMMIT стоял в `else`, снаружи защиты: отказ на коммите
            # (SQLITE_BUSY, кончившийся диск, ошибка ввода-вывода) оставлял
            # транзакцию ОТКРЫТОЙ на потоко-локальном соединении, а лок при
            # этом отпускался. Соединение кэшируется в threading.local и не
            # пересоздаётся, поэтому каждая следующая запись в этом потоке
            # падала с «cannot start a transaction within a transaction» —
            # навсегда. В центре потоки берутся из пула asyncio.to_thread,
            # то есть один отравленный поток ронял произвольные запросы.
            #
            # Лечение — закрыть соединение: следующий вызов откроет новое.
            # ROLLBACK тоже завёрнут, иначе его собственная ошибка («no
            # transaction is active») подменяла бы исходную причину сбоя.
        try:
            try:
                yield conn
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
            _secure_database_files(st)
        except Exception:
            if conn.in_transaction:
                _drop_thread_conn(path)
            raise


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
