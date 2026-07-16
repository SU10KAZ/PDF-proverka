"""Тесты CodexExecStreamFilter — белый список для транскрипта codex exec.

Причина: codex exec (plain-режим) стримит в лог аудита весь транскрипт —
эхо промпта, вывод команд, дампы читаемых файлов. На живом прогоне
оптимизации АИ1 (2026-07-16) это дало ~2500 записей `[OPT codex] …` и
вытеснило остальные секции вкладки «Лог». Строки взяты из того прогона.
"""
import asyncio

import pytest

from backend.app.services.common.codex_stream_filter import (
    CodexExecStreamFilter,
    wrap_codex_on_output,
)
from backend.app.pipeline.stages.optimization.ensemble import _make_provider_on_output


# ─── Подавление транскрипта ─────────────────────────────────────────


@pytest.mark.parametrize("raw", [
    # эхо промпта
    "You are running as OpenAI Codex exec inside the Audit Manager classic pipeline.",
    "Stage: optimization",
    "# Task: Design Solution Optimization",
    "### 1. CHEAPER ANALOG REPLACEMENT",
    "- **Vendor list is mandatory:** propose ONLY manufacturers from the vendor list",
    "**STRICTLY FOLLOW THE SCHEMA. Each item is a flat object with these fields:**",
    "<PIPELINE_TASK>",
    # дампы читаемых файлов (маркдаун-таблицы проектного MD)
    "| Инженерная система | Тип оборудования | Универсальные решения в РД |",
    "| ОЗ-6 |  | Стрелиция Н170 | 1 |",
    "|---|---|---|---|",
    # команды и их вывод
    "/bin/bash -lc \"node -e 'const fs=require(...)'\"",
    "index 0000000000000000000000000000000000000000..3caf047d4fd4",
    "--- /dev/null",
    "+        \"Поз. НП5.1 — Керамогранит по стяжке М200, 5,47 м²\"",
    # маркеры секций и баннер
    "user",
    "thinking",
    "codex",
    "exec",
    "OpenAI Codex v0.144.2",
    "workdir: /home/coder/projects",
    # web search без запроса — шум
    "web search:",
    "",
])
def test_transcript_noise_is_suppressed(raw):
    assert CodexExecStreamFilter().feed(raw) is None


# ─── Белый список ───────────────────────────────────────────────────


def test_web_search_with_query_passes():
    out = CodexExecStreamFilter().feed(
        "web search: СП 7.13130.2013 решетки дымоудаления пункт 7.11"
    )
    assert out == "Codex: веб-поиск — СП 7.13130.2013 решетки дымоудаления пункт 7.11"


def test_full_json_line_passes_to_humanizer():
    """Однострочный JSON не глушим здесь: log_humanizer превратит тело ошибки
    API в error-строку, а машинное событие подавит."""
    raw = '{"error":{"message":"usage limit reached"}}'
    assert CodexExecStreamFilter().feed(raw) == raw


@pytest.mark.parametrize("raw", [
    "ERROR: stream disconnected before completion",
    "stream error: retrying request",
    "fatal: repository not found",
])
def test_error_lines_pass(raw):
    assert CodexExecStreamFilter().feed(raw) == raw


def test_tokens_used_same_line():
    out = CodexExecStreamFilter().feed("tokens used: 12345")
    assert out == "Codex: токенов использовано 12345"


def test_tokens_used_number_on_next_line():
    f = CodexExecStreamFilter()
    assert f.feed("tokens used") is None
    assert f.feed("128 431") == "Codex: токенов использовано 128 431"


def test_tokens_used_next_line_not_number_falls_through():
    f = CodexExecStreamFilter()
    assert f.feed("tokens used") is None
    # следующая строка — не число: обрабатывается обычными правилами (глушится)
    assert f.feed("| таблица | из | файла |") is None


def test_runner_service_line_passes():
    """Служебная строка НАШЕГО runner-кода (claude_runner шлёт через тот же
    on_output до старта codex) — не транскрипт, глушить нельзя (находка ревью)."""
    raw = "Codex optimization vision: attached 3 drawing block image(s)"
    assert CodexExecStreamFilter().feed(raw) == raw


def test_wrap_codex_on_output_filters_and_none_passthrough():
    """Хелпер для прямого (не-ансамблевого) codex-пути оптимизации."""
    assert wrap_codex_on_output(None) is None

    lines = []

    async def sink(message, level="info"):
        lines.append(message)

    wrapped = wrap_codex_on_output(sink)

    async def run():
        await wrapped("# Task: Design Solution Optimization")  # эхо промпта
        await wrapped("web search: СП 29.13330.2011 полы")

    asyncio.run(run())
    assert lines == ["Codex: веб-поиск — СП 29.13330.2011 полы"]


# ─── Прокладка ансамбля ─────────────────────────────────────────────


def _collect_on_output(provider):
    lines = []

    async def log(message):
        lines.append(message)

    return _make_provider_on_output(log, provider), lines


def test_ensemble_codex_leg_is_filtered():
    on_output, lines = _collect_on_output("codex")

    async def run():
        await on_output("# Task: Design Solution Optimization")   # эхо промпта
        await on_output("| ОЗ-6 |  | Стрелиция Н170 | 1 |")       # дамп файла
        await on_output("web search: ГОСТ Р 58324-2018 статус")   # активность

    asyncio.run(run())
    assert lines == ["[OPT codex] Codex: веб-поиск — ГОСТ Р 58324-2018 статус"]


def test_ensemble_claude_leg_is_passthrough():
    """Claude стримит stream-json — его разбирает manager._log, фильтр
    ансамбля не вмешивается."""
    on_output, lines = _collect_on_output("claude")

    async def run():
        await on_output('{"type":"result","result":"ok"}')

    asyncio.run(run())
    assert lines == ['[OPT claude] {"type":"result","result":"ok"}']
