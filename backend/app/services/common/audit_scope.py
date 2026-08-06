"""Область видимости путей аудита: какой проект/версия «активны» прямо сейчас.

Зачем
─────
Пути записи артефактов (`_output/`, каталог версии, project_id, version_id)
раньше передавались через `os.environ`: `_scoped_audit_paths` в
`services/llm/claude_runner.py` делал `os.environ.update(...)` вокруг `await`.
Пока проекты шли строго по одному, это работало. При ПАРАЛЛЕЛЬНОЙ обработке
нескольких проектов это порча данных, а не гонка на секунду:

  1. Проект A входит в блок и уходит в `await` LLM на минуты.
  2. Проект B перетирает `AUDIT_OUTPUT_DIR` своим значением.
  3. A просыпается и пишет `03_findings.json` в `_output/` проекта B.

Плюс save/restore на общем `os.environ` не стек-безопасен: восстановление в
чужом порядке оставляет переменную испорченной до конца жизни процесса.

Решение
───────
Внутри процесса источник истины — `ContextVar`. `asyncio.create_task` копирует
контекст, поэтому каждая задача-проект видит СВОИ пути, а `set()` соседа её не
касается. Тот же приём уже применён в проекте для version/object
(`version_service.bind_version`, `project_service.bind_object`).

`os.environ` остаётся ТОЛЬКО как канал для дочерних процессов: `claude -p`,
`codex exec`, `process_project.py` получают значения явным `env_overrides` при
запуске (см. `manager._make_audit_env_for_job`). Поэтому все геттеры ниже
читают ContextVar, а затем — env: код, исполняемый ВНУТРИ дочернего процесса,
продолжает работать без единой правки.

Правило простое: **писать — только через `bind_audit_scope`, читать — только
через геттеры этого модуля**. Прямой `os.environ["AUDIT_OUTPUT_DIR"] = ...` в
коде бэкенда — баг.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Iterator, Optional

# Имена переменных окружения сохранены прежними: их читают дочерние процессы.
ENV_OUTPUT_DIR = "AUDIT_OUTPUT_DIR"
ENV_VERSION_DIR = "AUDIT_VERSION_DIR"
ENV_PROJECT_ID = "AUDIT_PROJECT_ID"
ENV_VERSION_ID = "AUDIT_VERSION_ID"

_output_dir: ContextVar[Optional[str]] = ContextVar("audit_output_dir", default=None)
_version_dir: ContextVar[Optional[str]] = ContextVar("audit_version_dir", default=None)
_project_id: ContextVar[Optional[str]] = ContextVar("audit_project_id", default=None)
_version_id: ContextVar[Optional[str]] = ContextVar("audit_version_id", default=None)

_VARS = {
    ENV_OUTPUT_DIR: _output_dir,
    ENV_VERSION_DIR: _version_dir,
    ENV_PROJECT_ID: _project_id,
    ENV_VERSION_ID: _version_id,
}


def _read(env_key: str) -> Optional[str]:
    """ContextVar (этот процесс) → env (мы внутри дочернего процесса)."""
    value = _VARS[env_key].get()
    if value:
        return value
    return os.environ.get(env_key) or None


def get_output_dir() -> Optional[str]:
    """Каталог `_output/` активной версии активного проекта."""
    return _read(ENV_OUTPUT_DIR)


def get_version_dir() -> Optional[str]:
    return _read(ENV_VERSION_DIR)


def get_project_id() -> Optional[str]:
    return _read(ENV_PROJECT_ID)


def get_version_id() -> Optional[str]:
    return _read(ENV_VERSION_ID)


def as_env(**overrides: Optional[str]) -> dict[str, str]:
    """Снимок области видимости для передачи в ДОЧЕРНИЙ процесс.

    Дочерний процесс не наследует ContextVar, поэтому значения нужно передать
    ему явно через `env`. Пустые значения не попадают в результат.
    """
    snapshot = {
        ENV_OUTPUT_DIR: overrides.get("output_dir") or get_output_dir(),
        ENV_VERSION_DIR: overrides.get("version_dir") or get_version_dir(),
        ENV_PROJECT_ID: overrides.get("project_id") or get_project_id(),
        ENV_VERSION_ID: overrides.get("version_id") or get_version_id(),
    }
    return {k: str(v) for k, v in snapshot.items() if v}


@contextmanager
def bind_audit_scope(
    *,
    output_dir: str | os.PathLike | None = None,
    version_dir: str | os.PathLike | None = None,
    project_id: str | None = None,
    version_id: str | None = None,
) -> Iterator[None]:
    """Назначить пути аудита для текущего async-контекста.

    Стек-безопасно: каждый `set()` возвращает свой token, и `reset(token)`
    восстанавливает ровно предыдущее значение — в отличие от save/restore на
    общем `os.environ`, где параллельные входы затирали друг друга.
    """
    tokens: list[tuple[ContextVar, Token]] = []
    values = {
        ENV_OUTPUT_DIR: output_dir,
        ENV_VERSION_DIR: version_dir,
        ENV_PROJECT_ID: project_id,
        ENV_VERSION_ID: version_id,
    }
    try:
        for env_key, value in values.items():
            if value is None:
                continue
            tokens.append((_VARS[env_key], _VARS[env_key].set(str(value))))
        yield
    finally:
        # В обратном порядке — вложенные привязки снимаются корректно.
        for var, token in reversed(tokens):
            try:
                var.reset(token)
            except ValueError:
                # Токен из другого контекста (сработало бы при попытке снять
                # привязку не в той задаче) — молча пропускаем, иначе уронили бы
                # успешно завершившийся этап.
                pass


def current_scope() -> dict[str, Optional[str]]:
    """Диагностика: что видит текущая задача."""
    return {
        "output_dir": get_output_dir(),
        "version_dir": get_version_dir(),
        "project_id": get_project_id(),
        "version_id": get_version_id(),
    }
