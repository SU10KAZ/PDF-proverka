"""Изоляция путей аудита между параллельными проектами.

Инцидент, который эти тесты не дают повторить: пути записи артефактов
(`AUDIT_OUTPUT_DIR` и соседи) передавались через process-global `os.environ`,
причём `os.environ.update(...)` стоял ВОКРУГ `await`. Пока очередь шла строго
по одному проекту, это работало. При параллельной обработке:

  1. Проект A входит в блок и уходит в await LLM на минуты.
  2. Проект B перетирает AUDIT_OUTPUT_DIR своим значением.
  3. A просыпается и пишет 03_findings.json в _output/ проекта B.

Это порча данных, а не гонка на секунду. Плюс save/restore на общем env не
стек-безопасен: восстановление в чужом порядке оставляло переменную битой
до конца жизни процесса.

Проверяем:
  1. Две одновременные задачи видят СВОИ пути через await.
  2. Значение соседа не протекает после его выхода из блока.
  3. Вложенные привязки снимаются в правильном порядке.
  4. `os.environ` больше не мутируется (иначе «чёрный ход» остаётся).
  5. Внутри дочернего процесса (ContextVar пуст) читается env — совместимость.
  6. `as_env()` отдаёт снимок для передачи в subprocess.
  7. `claude_runner._scoped_audit_paths` изолирует параллельные вызовы.

Run: python -m pytest tests/test_audit_scope_isolation.py -v
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import audit_scope  # noqa: E402


@pytest.mark.asyncio
async def test_parallel_tasks_keep_own_paths_across_await():
    """Ядро фикса: сосед не уводит артефакты в свой каталог."""
    seen: dict[str, tuple[str, str]] = {}

    async def project(name: str, delay: float):
        with audit_scope.bind_audit_scope(
            output_dir=f"/projects/{name}/_output",
            project_id=name,
        ):
            # Пока спим — соседняя задача успевает выставить своё значение.
            await asyncio.sleep(delay)
            seen[name] = (audit_scope.get_output_dir(), audit_scope.get_project_id())

    # B стартует позже, но заканчивает раньше — худший порядок для общего env.
    await asyncio.gather(project("A", 0.05), project("B", 0.01))

    assert seen["A"] == ("/projects/A/_output", "A")
    assert seen["B"] == ("/projects/B/_output", "B")


@pytest.mark.asyncio
async def test_neighbour_value_does_not_leak_after_exit():
    """После выхода соседа область видимости текущей задачи не меняется."""
    async def neighbour():
        with audit_scope.bind_audit_scope(output_dir="/projects/B/_output"):
            await asyncio.sleep(0)

    with audit_scope.bind_audit_scope(output_dir="/projects/A/_output"):
        await neighbour()
        assert audit_scope.get_output_dir() == "/projects/A/_output"


def test_nested_bind_restores_outer_value():
    """Вложенная привязка снимается стек-безопасно (чего не давал save/restore)."""
    with audit_scope.bind_audit_scope(output_dir="/outer", project_id="P1"):
        assert audit_scope.get_output_dir() == "/outer"
        with audit_scope.bind_audit_scope(output_dir="/inner"):
            assert audit_scope.get_output_dir() == "/inner"
            # project_id внутренним блоком не переопределялся — виден внешний.
            assert audit_scope.get_project_id() == "P1"
        assert audit_scope.get_output_dir() == "/outer"
    assert audit_scope.get_output_dir() is None


def test_environ_is_not_mutated(monkeypatch):
    """Прямой записи в os.environ быть не должно — иначе баг возвращается."""
    monkeypatch.delenv(audit_scope.ENV_OUTPUT_DIR, raising=False)
    with audit_scope.bind_audit_scope(output_dir="/projects/A/_output"):
        assert audit_scope.get_output_dir() == "/projects/A/_output"
        assert audit_scope.ENV_OUTPUT_DIR not in os.environ, (
            "область видимости снова пишет в общий os.environ"
        )


def test_env_fallback_for_subprocess(monkeypatch):
    """Дочерний процесс не наследует ContextVar — там читаем env."""
    monkeypatch.setenv(audit_scope.ENV_OUTPUT_DIR, "/from/parent/_output")
    monkeypatch.setenv(audit_scope.ENV_PROJECT_ID, "DOC-1")
    # ContextVar пуст (как в свежем процессе) → берём env.
    assert audit_scope.get_output_dir() == "/from/parent/_output"
    assert audit_scope.get_project_id() == "DOC-1"

    # ContextVar приоритетнее env: внутри бэкенда решает задача, а не процесс.
    with audit_scope.bind_audit_scope(output_dir="/task/_output"):
        assert audit_scope.get_output_dir() == "/task/_output"


def test_as_env_snapshot_for_child_process(monkeypatch):
    monkeypatch.delenv(audit_scope.ENV_OUTPUT_DIR, raising=False)
    monkeypatch.delenv(audit_scope.ENV_VERSION_DIR, raising=False)
    monkeypatch.delenv(audit_scope.ENV_PROJECT_ID, raising=False)
    monkeypatch.delenv(audit_scope.ENV_VERSION_ID, raising=False)

    with audit_scope.bind_audit_scope(
        output_dir="/p/_output", project_id="DOC-2", version_id="v003"
    ):
        env = audit_scope.as_env()

    assert env[audit_scope.ENV_OUTPUT_DIR] == "/p/_output"
    assert env[audit_scope.ENV_PROJECT_ID] == "DOC-2"
    assert env[audit_scope.ENV_VERSION_ID] == "v003"
    # Незаданное не попадает в снимок — не затираем чужое пустой строкой.
    assert audit_scope.ENV_VERSION_DIR not in env


@pytest.mark.asyncio
async def test_claude_runner_scoped_paths_isolated():
    """Тот же инвариант на реальной точке входа, которой пользуется конвейер."""
    import backend.app.services.llm.claude_runner as claude_runner

    async def project(name: str, delay: float) -> str:
        with claude_runner._scoped_audit_paths(
            output_dir=f"/projects/{name}/_output", project_id=name
        ):
            await asyncio.sleep(delay)
            return claude_runner._resolve_output_dir(name, None).as_posix()

    a, b = await asyncio.gather(project("A", 0.04), project("B", 0.01))
    assert a == "/projects/A/_output"
    assert b == "/projects/B/_output"
