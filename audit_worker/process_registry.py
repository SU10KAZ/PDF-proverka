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
import signal
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


def is_alive(pid: int, start_time: Optional[float]) -> bool:
    """Жив ли ИМЕННО тот процесс (pid + метка старта), а не тёзка по pid."""
    if pid <= 0:
        return False
    current = process_start_time(pid)
    if current is None:
        return False
    if start_time is None:
        return True
    return abs(current - start_time) < 1e-6


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

    def terminate_job(self, job_id: str, attempt_id: str, *, grace_sec: float = 10.0) -> int:
        """SIGTERM → пауза → SIGKILL. Возвращает число задетых процессов."""
        targets = self.alive_for_job(job_id, attempt_id)
        for item in targets:
            try:
                os.kill(int(item["pid"]), signal.SIGTERM)
            except OSError:
                continue
        deadline = time.time() + grace_sec
        while time.time() < deadline:
            if not self.alive_for_job(job_id, attempt_id):
                break
            time.sleep(0.2)
        for item in self.alive_for_job(job_id, attempt_id):
            try:
                os.kill(int(item["pid"]), signal.SIGKILL)
            except OSError:
                continue
        self.prune_dead()
        return len(targets)

    def _flush(self) -> None:
        atomic_write_json(self.path, self._items)
