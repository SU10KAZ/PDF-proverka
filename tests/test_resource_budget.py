"""Глобальные бюджеты внешних ресурсов.

Все ограничители параллелизма в конвейере исторически «на проект»: семафор
создаётся внутри функции этапа. Пока очередь вела один проект, это и было
глобальным лимитом. При пяти проектах каждый умножается на пять — до 20
одновременных CLI-процессов и до 10 норм-MCP по ~2,8 ГБ (это OOM, а не
ускорение). Бюджет обязан быть один на процесс бэкенда.

Покрытие:
  1. Лимит соблюдается: одновременных входов не больше разрешённого.
  2. Разные ресурсы независимы (norms_mcp не держит claude_cli).
  3. env BUDGET_<ИМЯ> перекрывает дефолт и подхватывается без рестарта.
  4. Реентерабельность: рекурсивный вход той же задачи не даёт дедлок
     (локальные транспорты после reload вызывают сами себя).
  5. Неизвестное имя не блокирует — опечатка не должна ронять этап.
  6. Слот освобождается при исключении внутри блока.
  7. snapshot() отражает лимит и занятость.

Run: python -m pytest tests/test_resource_budget.py -v
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import resource_budget  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_budget():
    resource_budget.reset_for_tests()
    yield
    resource_budget.reset_for_tests()


@pytest.mark.asyncio
async def test_limit_is_enforced(monkeypatch):
    monkeypatch.setenv("BUDGET_CLAUDE_CLI", "3")
    resource_budget.reset_for_tests()

    now = 0
    peak = 0

    async def worker():
        nonlocal now, peak
        async with resource_budget.slot("claude_cli"):
            now += 1
            peak = max(peak, now)
            await asyncio.sleep(0.02)
            now -= 1

    await asyncio.gather(*[worker() for _ in range(12)])
    assert peak == 3, f"лимит 3, а одновременно входило {peak}"


@pytest.mark.asyncio
async def test_resources_are_independent(monkeypatch):
    monkeypatch.setenv("BUDGET_NORMS_MCP", "1")
    monkeypatch.setenv("BUDGET_CLAUDE_CLI", "4")
    resource_budget.reset_for_tests()

    order: list[str] = []

    async def mcp_hog():
        async with resource_budget.slot("norms_mcp"):
            order.append("mcp-start")
            await asyncio.sleep(0.05)
            order.append("mcp-end")

    async def cli_call():
        await asyncio.sleep(0.01)
        async with resource_budget.slot("claude_cli"):
            order.append("cli")

    await asyncio.gather(mcp_hog(), cli_call())
    # cli не обязан ждать освобождения norms_mcp.
    assert order.index("cli") < order.index("mcp-end")


@pytest.mark.asyncio
async def test_reentrant_does_not_deadlock(monkeypatch):
    """Рекурсивный вход той же задачи обязан пройти без ожидания.

    Рекурсивный повтор внутри уже занятого слота не должен заклинивать задачу
    на самой себе — обычный asyncio.Semaphore это сделал бы.
    """
    monkeypatch.setenv("BUDGET_NORMS_MCP", "1")
    resource_budget.reset_for_tests()

    async def nested():
        async with resource_budget.slot("norms_mcp"):
            async with resource_budget.slot("norms_mcp"):
                return "готово"

    result = await asyncio.wait_for(nested(), timeout=2.0)
    assert result == "готово"


@pytest.mark.asyncio
async def test_unknown_name_does_not_block():
    async def call():
        async with resource_budget.slot("_none"):
            return True

    # Много одновременных входов в несуществующий ресурс — все проходят сразу.
    assert all(await asyncio.gather(*[call() for _ in range(20)]))


@pytest.mark.asyncio
async def test_slot_released_on_exception(monkeypatch):
    monkeypatch.setenv("BUDGET_NORMS_MCP", "1")
    resource_budget.reset_for_tests()

    with pytest.raises(RuntimeError):
        async with resource_budget.slot("norms_mcp"):
            raise RuntimeError("подопытный сбой")

    # Слот должен быть свободен, иначе следующий вызов зависнет.
    async def after():
        async with resource_budget.slot("norms_mcp"):
            return "свободно"

    assert await asyncio.wait_for(after(), timeout=2.0) == "свободно"


def test_limit_for_reads_env(monkeypatch):
    monkeypatch.delenv("BUDGET_NORMS_MCP", raising=False)
    assert resource_budget.limit_for("norms_mcp") == resource_budget.DEFAULTS["norms_mcp"]

    monkeypatch.setenv("BUDGET_NORMS_MCP", "5")
    assert resource_budget.limit_for("norms_mcp") == 5

    # Мусор и ноль не должны обнулять лимит.
    monkeypatch.setenv("BUDGET_NORMS_MCP", "не-число")
    assert resource_budget.limit_for("norms_mcp") == resource_budget.DEFAULTS["norms_mcp"]
    monkeypatch.setenv("BUDGET_NORMS_MCP", "0")
    assert resource_budget.limit_for("norms_mcp") == resource_budget.DEFAULTS["norms_mcp"]


@pytest.mark.asyncio
async def test_snapshot_reports_state(monkeypatch):
    monkeypatch.setenv("BUDGET_CLAUDE_CLI", "2")
    resource_budget.reset_for_tests()

    snap = resource_budget.snapshot()
    assert snap["claude_cli"]["limit"] == 2
    assert snap["claude_cli"]["active"] is False

    async with resource_budget.slot("claude_cli"):
        snap = resource_budget.snapshot()
        assert snap["claude_cli"]["active"] is True
        assert snap["claude_cli"]["free"] == 1


def test_defaults_are_conservative_for_ram():
    """norms_mcp — про RAM: ~2,8 ГБ на сервер, дефолт не должен грозить OOM."""
    assert resource_budget.DEFAULTS["norms_mcp"] <= 2
    # Локальная модель на машине одна — параллелить её нечем.
