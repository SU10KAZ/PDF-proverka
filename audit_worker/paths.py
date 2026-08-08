"""Пути воркера строятся только из UUID.

Копия правила центра (`services/distributed_workers/identifiers.py`), но без
зависимости от backend: пакет `audit_worker` ставится на чужой VPS отдельно и
про backend ничего не знает.

Почему это отдельный модуль, а не пара `os.path.join` по месту: внешний код
проекта в этом репозитории выглядит как `13АВ/РД-АР3-К7` — со слэшем внутри.
Один join с таким кодом и запись уходит за пределы каталога данных. Ключ
хранения обязан быть UUID, и это проверяется до того, как строка попадёт в Path.
"""
from __future__ import annotations

import uuid
from pathlib import Path


class UnsafeStorageKey(ValueError):
    """Значение не годится как сегмент пути."""


def is_storage_key(value: str) -> bool:
    try:
        uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return False
    return True


def require_storage_key(value: str, *, field: str) -> str:
    text = str(value or "").strip()
    if not is_storage_key(text):
        raise UnsafeStorageKey(
            f"{field}: сегментом пути может быть только UUID (получено {text[:64]!r})"
        )
    return text


def attempt_dir(jobs_root: Path, job_id: str, attempt_id: str) -> Path:
    """jobs/<job_uuid>/<attempt_uuid> — единственная допустимая раскладка."""
    return (
        Path(jobs_root)
        / require_storage_key(job_id, field="job_id")
        / require_storage_key(attempt_id, field="attempt_id")
    )
