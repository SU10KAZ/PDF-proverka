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

import json
import os
import threading
from pathlib import Path
from typing import Any

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


def atomic_write_json(path: Path, data: Any, *, indent: int = 2) -> None:
    """Атомарно и потокобезопасно записать JSON.

    tmp в той же директории → fsync → os.replace, всё под per-path локом.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock_for(path):
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
