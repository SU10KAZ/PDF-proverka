"""Атомарная потокобезопасная запись JSON для shared-стораджей.

reserc.md #7/#81/#87/#101/#107. Раньше высокоценные shared-файлы (decisions_log
на 140 проектов, patterns) писались plain `open('w')` без atomic/lock → краш или
гонка параллельных писателей портили весь KB. Здесь: запись во временный файл в
той же папке + `os.replace` (атомарная подмена, не оставляет полу-записанный
файл) под per-path `threading.Lock` (сериализует писателей одного процесса).

Аналог уже существовал в stage_comparison.store._atomic_write_json; этот модуль —
единый общий writer, переиспользуемый KB / grounding / findings.
"""
from __future__ import annotations

import copy
import json
import os
import threading
from pathlib import Path
from typing import Any, Callable

try:  # pragma: no cover - platform-specific; fallback is tested via monkeypatch.
    import fcntl as _fcntl
except Exception:  # pragma: no cover
    _fcntl = None

# Per-path локи: один объект Lock на абсолютный путь.
_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _locks_guard:
        lk = _locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _locks[key] = lk
        return lk


def _write_json_unlocked(path: Path, data: Any, *, indent: int = 2) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_write_json(path: Path, data: Any, *, indent: int = 2) -> None:
    """Атомарно и потокобезопасно записать JSON.

    tmp в той же директории → fsync → os.replace, всё под per-path локом.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock_for(path):
        _write_json_unlocked(path, data, indent=indent)


def _load_json_locked(path: Path, default: Any) -> Any:
    if not path.exists():
        return copy.deepcopy(default)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_modify_save(
    path: Path,
    mutate_fn: Callable[[Any], Any],
    *,
    default: Any = None,
    indent: int = 2,
) -> Any:
    """Atomically run read -> mutate -> write under one per-file lock.

    Missing files are initialized from a deep copy of default. Existing but
    invalid JSON is not replaced silently: json.JSONDecodeError propagates and
    the original file stays untouched. mutate_fn may mutate and return the
    passed object or return a replacement payload; the returned payload is what
    gets written and returned to the caller.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")

    with _lock_for(path):
        lock_file = None
        try:
            lock_file = open(lock_path, "a+", encoding="utf-8")
            if _fcntl is not None:
                _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_EX)
            data = _load_json_locked(path, default)
            new_data = mutate_fn(data)
            if new_data is None:
                new_data = data
            _write_json_unlocked(path, new_data, indent=indent)
            return new_data
        finally:
            if lock_file is not None:
                try:
                    if _fcntl is not None:
                        _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_UN)
                finally:
                    lock_file.close()
