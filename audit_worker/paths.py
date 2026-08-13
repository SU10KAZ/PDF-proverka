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

import re
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


# Ключи этапа 0 («att_1a2b3c4d»): не UUID, но выданы центром и безопасны как
# сегмент пути. Допускаются только для уже существующих каталогов.
_LEGACY_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def is_legacy_key(value: str) -> bool:
    text = str(value or "").strip()
    return bool(_LEGACY_KEY_RE.match(text)) and text not in (".", "..")


def require_storage_key(value: str, *, field: str, allow_legacy: bool = True) -> str:
    text = str(value or "").strip()
    if is_storage_key(text):
        return text
    if allow_legacy and is_legacy_key(text):
        return text
    raise UnsafeStorageKey(
        f"{field}: сегментом пути может быть только UUID (получено {text[:64]!r})"
    )


def attempt_dir(
    jobs_root: Path, job_id: str, attempt_id: str, *, allow_legacy: bool = True
) -> Path:
    """jobs/<job_uuid>/<attempt_uuid> — единственная допустимая раскладка.

    `allow_legacy` по умолчанию включён: на воркере уже могут лежать каталоги
    попыток этапа 0, и терять к ним доступ нельзя. Внешний код проекта не
    проходит ни при каком значении флага — в нём есть «/» и пробелы.
    """
    return (
        Path(jobs_root)
        / require_storage_key(job_id, field="job_id", allow_legacy=allow_legacy)
        / require_storage_key(attempt_id, field="attempt_id", allow_legacy=allow_legacy)
    )
