"""Реестр дочерних процессов воркера.

Нужен для трёх вещей: честно считать занятость, уметь остановить работу по
команде отмены и — главное — пережить рестарт агента. Первый аудит показал,
почему второй источник обязателен: `has_live_processes()` в основном
репозитории покрывает не все пути запуска, поэтому «живо ли что-то» стоит
проверять и по /proc, а не только по своему словарю.

Реестр персистится в runtime/processes.json вместе с временем старта процесса:
голый pid переиспользуется системой, и без метки времени после рестарта можно
принять чужой процесс за свой.
"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

from audit_worker.local_store import atomic_write_json, read_json


def process_start_time(pid: int) -> Optional[float]:
    """Тик старта процесса из /proc/<pid>/stat (поле 22). None, если процесса нет."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as fh:
            data = fh.read().decode("utf-8", "replace")
    except (OSError, ValueError):
        return None
    try:
        tail = data[data.rindex(")") + 2:].split()
        return float(tail[19])
    except (ValueError, IndexError):
        return None


def pid_exists(pid: int) -> bool:
    """Существует ли процесс с таким pid. НЕ доказательство принадлежности."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True          # чужой процесс — но существует
    except OSError:
        return False
    return True


def is_alive(pid: int, start_time: Optional[float]) -> bool:
    """Жив ли ИМЕННО тот процесс (pid + метка старта), а не тёзка по pid.

    `start_time is None` означает «идентичность неизвестна». Раньше здесь
    возвращалось True, то есть один голый pid признавался доказательством
    живости — буквально запрещённый сценарий I-17. Теперь неизвестность
    трактуется как «не доказано»: хуже застрявшая попытка, чем сигнал чужому
    процессу.
    """
    if pid <= 0 or start_time is None:
        return False
    current = process_start_time(pid)
    if current is None:
        return False
    return abs(current - start_time) < 1e-6


def process_cmdline(pid: int) -> Optional[list[str]]:
    """argv процесса из /proc/<pid>/cmdline. None, если прочитать нельзя."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            raw = fh.read()
    except OSError:
        return None
    if not raw:
        return None
    parts = raw.split(b"\x00")
    if parts and parts[-1] == b"":
        parts = parts[:-1]
    return [p.decode("utf-8", "replace") for p in parts]


def live_command_fingerprint(pid: int) -> Optional[str]:
    """Отпечаток команды ЖИВОГО процесса — независимая проверка (I-17).

    Считается так же, как `test_runner.command_fingerprint`, но по данным
    ядра, а не по нашей же записи. Сверка реестра с metadata.json — это
    сверка двух копий одной записи; здесь спрашивают у самого процесса.
    """
    argv = process_cmdline(pid)
    if not argv:
        return None
    import hashlib

    return hashlib.sha256("\x00".join(argv).encode("utf-8")).hexdigest()[:32]


class ProcessRegistry:
    def __init__(self, runtime_dir: Path):
        self.path = runtime_dir / "processes.json"
        self._lock = threading.Lock()
        self._items: dict[str, dict[str, Any]] = read_json(self.path, {}) or {}

    def register(
        self,
        pid: int,
        *,
        job_id: str,
        attempt_id: str,
        kind: str = "test",
        command_fingerprint: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._items[str(pid)] = {
                "pid": pid,
                "job_id": job_id,
                "attempt_id": attempt_id,
                "kind": kind,
                # Три независимых признака «это наш живой процесс»:
                # pid (переиспользуется), метка старта (защищает от тёзки по
                # pid) и отпечаток команды (защищает от чужого процесса,
                # случайно совпавшего по обоим).
                "start_time": process_start_time(pid),
                "command_fingerprint": command_fingerprint,
                "registered_at": time.time(),
            }
            self._flush()

    def unregister(self, pid: int) -> None:
        with self._lock:
            self._items.pop(str(pid), None)
            self._flush()

    def for_job(self, job_id: str, attempt_id: str) -> list[dict[str, Any]]:
        return [
            item
            for item in self._items.values()
            if item.get("job_id") == job_id and item.get("attempt_id") == attempt_id
        ]

    def alive_for_job(
        self, job_id: str, attempt_id: str, *, command_fingerprint: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """Живые процессы задания.

        `command_fingerprint` — дополнительный фильтр: при рестарте агента он
        отсекает чужой процесс, занявший освободившийся pid.
        """
        out = []
        for item in self.for_job(job_id, attempt_id):
            if not is_alive(int(item["pid"]), item.get("start_time")):
                continue
            if command_fingerprint and item.get("command_fingerprint") not in (
                None, command_fingerprint
            ):
                continue
            out.append(item)
        return out

    def live_count(self) -> int:
        return sum(
            1
            for item in self._items.values()
            if is_alive(int(item["pid"]), item.get("start_time"))
        )

    def prune_dead(self) -> int:
        """Убрать записи об умерших процессах. Возвращает число вычищенных."""
        with self._lock:
            dead = [
                key
                for key, item in self._items.items()
                if not is_alive(int(item["pid"]), item.get("start_time"))
            ]
            for key in dead:
                self._items.pop(key, None)
            if dead:
                self._flush()
        return len(dead)

    # Метода `terminate_job` здесь больше нет. Он бил `os.kill` по голому pid,
    # без единого доказательства принадлежности: ни сверки тика старта, ни
    # отпечатка команды, ни группы процессов. В подсистеме, которая обещает
    # «сигнал только своему процессу», такой запасной путь опасен сам по себе —
    # достаточно одного вызова из будущего кода, чтобы обещание перестало
    # действовать, а pid к тому моменту мог быть переиспользован системой.
    # Единственная точка остановки процесса — process_control.terminate:
    # доказанная группа, SIGTERM → пауза → SIGKILL. Вызовов у удалённого метода
    # не было ни одного (проверено grep по репозиторию).

    def _flush(self) -> None:
        atomic_write_json(self.path, self._items)
