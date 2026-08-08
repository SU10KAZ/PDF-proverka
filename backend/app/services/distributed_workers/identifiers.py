"""Разделение «как проект называется» и «где лежат его файлы».

Три сущности, которые на этапе 0 были одной строкой:

  * `project_external_id`   — пользовательский код проекта. Реальные коды в
    этом репозитории выглядят как `13АВ/РД-АР3-К7` и `ЖК «Событие 6.2» /
    корпус 3`: кириллица, пробелы, кавычки и `/`. Это МЕТАДАННЫЕ;
  * `project_display_name`  — человеческое название для экрана;
  * `job_id` / `attempt_id` — UUID. ТОЛЬКО они попадают в файловые пути.

Почему это отдельный модуль, а не пара regex по месту: правило «внешний
идентификатор никогда не является компонентом пути» (I-11) должно проверяться
одинаково на центре и на воркере, а `/` во внешнем коде — не редкость, а норма.
Один `os.path.join` с таким кодом даёт запись за пределами каталога данных.
"""
from __future__ import annotations

import re
import unicodedata
import uuid
from pathlib import Path

MAX_EXTERNAL_ID_LEN = 200
MAX_DISPLAY_NAME_LEN = 300

# Управляющие символы (кроме отсутствующих здесь табов/переводов строк — они
# тоже запрещены: это идентификатор, а не текст).
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")


class UnsafeIdentifier(ValueError):
    """Идентификатор непригоден: NUL, управляющие символы, пустота или длина."""


def normalize_external_id(
    value: str, *, field: str = "project_external_id", max_length: int = MAX_EXTERNAL_ID_LEN
) -> str:
    """Привести внешний идентификатор к каноническому виду и проверить его.

    Что делаем: NFC-нормализация (иначе «й» в двух разных кодировках дают два
    разных проекта), схлопывание пробелов по краям.
    Что запрещаем: NUL, управляющие символы, пустую строку, чрезмерную длину.
    Что НЕ запрещаем: кириллицу, пробелы, кавычки, скобки и `/` — это
    легальная часть кода проекта, а не попытка обхода.
    """
    if not isinstance(value, str):
        raise UnsafeIdentifier(f"{field}: ожидается строка")
    text = unicodedata.normalize("NFC", value).strip()
    if not text:
        raise UnsafeIdentifier(f"{field}: пустое значение")
    if _CONTROL_RE.search(text):
        raise UnsafeIdentifier(
            f"{field}: управляющие символы и NUL недопустимы"
        )
    if len(text) > max_length:
        raise UnsafeIdentifier(
            f"{field}: длина {len(text)} превышает {max_length} символов"
        )
    return text


def normalize_display_name(value: str, *, fallback: str = "") -> str:
    """Отображаемое название. Пустое допустимо — подставляется fallback."""
    if not value:
        return fallback
    text = unicodedata.normalize("NFC", str(value)).strip()
    text = _CONTROL_RE.sub(" ", text)
    return text[:MAX_DISPLAY_NAME_LEN] or fallback


def is_storage_key(value: str) -> bool:
    """UUID ли это. Ключ хранения обязан быть UUID и ничем иным."""
    try:
        uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return False
    return True


def require_storage_key(value: str, *, field: str) -> str:
    """Проверить, что значение годится как СЕГМЕНТ ПУТИ.

    Единственный допустимый вид — UUID. Ни внешний код проекта, ни имя
    воркера, ни имя теста сюда попасть не могут: у них нет формы UUID, и
    проверка отвергнет их до того, как строка окажется в Path.
    """
    text = str(value or "").strip()
    if not is_storage_key(text):
        raise UnsafeIdentifier(
            f"{field}: ключом хранения может быть только UUID (получено {text[:64]!r})"
        )
    return text


def attempt_dir(root: Path, job_id: str, attempt_id: str) -> Path:
    """jobs/<job_uuid>/<attempt_uuid> — единственный способ строить путь.

    Никаких project_external_id, display_name, version_external_id, имён
    воркера или теста в пути нет и быть не может.
    """
    return (
        Path(root)
        / require_storage_key(job_id, field="job_id")
        / require_storage_key(attempt_id, field="attempt_id")
    )


_FILENAME_BAD = re.compile(r'[\\/:*?"<>|\x00-\x1f\x7f]+')


def safe_download_filename(display: str, *, fallback: str, suffix: str = "") -> str:
    """Имя файла ДЛЯ ПОКАЗА в браузере, не для чтения с диска.

    Файл на диске всегда открывается по UUID из БД; сюда попадает только то,
    что уйдёт в заголовок Content-Disposition. Разделители пути вырезаются:
    даже в заголовке `../` — лишний повод для сюрприза у клиента.
    """
    text = unicodedata.normalize("NFC", str(display or "")).strip()
    text = _FILENAME_BAD.sub("_", text).strip(". ")
    if not text:
        text = fallback
    return (text[:120] + suffix) if suffix else text[:120]
