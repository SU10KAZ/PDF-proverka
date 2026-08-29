"""Кэш ответов модели по содержанию доказательств.

Ключ — не «этот элемент», а «эти доказательства этой моделью на этом уровне
рассуждения по ЭТОМУ ТЕКСТУ промпта и ЭТОЙ схеме». Поэтому кэш переживает
перенумерацию элементов и перезапуск прогона, но честно промахивается, как
только изменился хоть один из входов.

Почему в ключе и номер версии, и отпечаток содержимого. Номер версии —
намерение автора: «я изменил смысл промпта». Отпечаток — факт: «текст стал
другим». Одного номера мало, потому что он поднимается вручную, а забытый
bump превращает кэш в тихого лжеца: ответ на прежний вопрос выдаётся за ответ
на новый, и заметить это по артефактам невозможно. Одного отпечатка мало,
потому что он не отличает правку опечатки от смены контракта. Поэтому оба.

Кэш живёт рядом с артефактами пары, а не в run-каталоге: иначе повторный
прогон той же пары платил бы второй раз за те же вопросы.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Mapping

from ..production_artifacts import content_signature
from . import settings

CACHE_KIND = "stage_comparison_ai_cache"
CACHE_SCHEMA_VERSION = "ai-cache.v1"


def digest_prompt(prompt: str, system_prompt: str | None = None) -> str:
    """Отпечаток ровно того текста, который уедет модели."""
    return content_signature({
        "system_prompt": system_prompt or "",
        "prompt": prompt,
    })


def digest_schema(schema: Mapping[str, Any]) -> str:
    """Отпечаток схемы по содержанию.

    Сериализация каноническая (ключи сортируются), поэтому перестановка полей
    в объявлении схемы отпечаток не меняет: кэш обесценивается сменой
    контракта, а не переносом строки.
    """
    return content_signature(schema)


def cache_key(
    *,
    evidence_digest: str,
    model: str,
    reasoning_level: str | None,
    prompt_version: str,
    schema_version: str,
    role: str,
    prompt_digest: str,
    schema_digest: str,
) -> str:
    # Оба отпечатка обязательны, а не «по умолчанию пусто»: вызов, забывший
    # их передать, — это ровно та дыра, которую они закрывают.
    return content_signature({
        "evidence_digest": evidence_digest,
        "model": model,
        "reasoning_level": reasoning_level or "",
        "prompt_version": prompt_version,
        "prompt_digest": prompt_digest,
        "schema_version": schema_version,
        "schema_digest": schema_digest,
        "role": role,
    })


class ResponseCache:
    """Файловый кэш «ключ → ответ модели». Промах никогда не роняет прогон."""

    def __init__(self, directory: Path | str | None) -> None:
        self.directory = Path(directory) if directory else None
        # Партии читают и пишут кэш параллельно; счётчики без замка врут.
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.writes = 0

    @property
    def enabled(self) -> bool:
        return self.directory is not None and settings.cache_enabled()

    def _path(self, key: str) -> Path:
        assert self.directory is not None
        return self.directory / f"{key[:32]}.json"

    def load(self, key: str) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        path = self._path(key)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            with self._lock:
                self.misses += 1
            return None
        if (
            not isinstance(payload, Mapping)
            or payload.get("kind") != CACHE_KIND
            or payload.get("schema_version") != CACHE_SCHEMA_VERSION
            or payload.get("cache_key") != key
        ):
            with self._lock:
                self.misses += 1
            return None
        with self._lock:
            self.hits += 1
        value = payload.get("response")
        return dict(value) if isinstance(value, Mapping) else None

    def store(self, key: str, response: Mapping[str, Any], meta: Mapping[str, Any]) -> None:
        if not self.enabled:
            return
        assert self.directory is not None
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            payload = {
                "kind": CACHE_KIND,
                "schema_version": CACHE_SCHEMA_VERSION,
                "cache_key": key,
                "meta": dict(meta),
                "response": dict(response),
            }
            # Запись атомарная: половина JSON в кэше хуже пустого кэша.
            handle, temporary = tempfile.mkstemp(
                dir=str(self.directory), suffix=".tmp"
            )
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=False)
            os.replace(temporary, self._path(key))
            with self._lock:
                self.writes += 1
        except OSError:
            return

    def statistics(self) -> dict[str, int | bool]:
        return {
            "enabled": self.enabled,
            "hits": self.hits,
            "misses": self.misses,
            "writes": self.writes,
        }


__all__ = [
    "CACHE_KIND",
    "CACHE_SCHEMA_VERSION",
    "ResponseCache",
    "cache_key",
    "digest_prompt",
    "digest_schema",
]
