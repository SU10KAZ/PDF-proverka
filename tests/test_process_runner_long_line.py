"""Сверхдлинная строка от CLI не должна ронять этап.

Живой случай 15.07.2026: 13АВ-РД-ВК1-К1 V1, findings_merge на ансамбле GPT+Codex.
Агент напечатал JSON одной строкой, asyncio.StreamReader (дефолтный лимит 64 КиБ)
бросил ValueError «Separator is found, but chunk is longer than limit», исключение
всплыло наружу и убило весь прогон проекта.
"""
import sys

import pytest

from backend.app.services.common.process_runner import run_command

# Дефолтный лимит asyncio.StreamReader — ровно на нём всё и сломалось.
_ASYNCIO_DEFAULT_LIMIT = 64 * 1024


@pytest.mark.asyncio
async def test_long_line_does_not_kill_the_stage():
    """Строка в 1 МБ — заведомо больше старого лимита 64 КиБ."""
    payload_len = 1024 * 1024
    code = (
        "import sys;"
        f"sys.stdout.write('x' * {payload_len});"
        "sys.stdout.write('\\nГОТОВО\\n')"
    )
    exit_code, stdout, stderr = await run_command([sys.executable, "-c", code])

    assert exit_code == 0, f"этап упал: {stderr[:200]}"
    # Строка помещается в новый лимит → доезжает целиком, ничего не потеряно.
    assert "ГОТОВО" in stdout
    assert len(stdout) >= payload_len


@pytest.mark.asyncio
async def test_stage_survives_line_beyond_any_limit():
    """Даже строка в 128 МБ не должна ронять прогон.

    Такую реплику лога спасти нельзя (readline() чистит буфер до того, как
    отдаст управление), но чтение обязано продолжиться: маркер после
    чудовищной строки доходит, exit_code нулевой.
    """
    code = (
        "import sys;"
        "sys.stdout.write('x' * (128 * 1024 * 1024));"
        "sys.stdout.write('\\nПОСЛЕ-ОГРОМНОЙ\\n')"
    )
    exit_code, stdout, stderr = await run_command([sys.executable, "-c", code])

    assert exit_code == 0, f"этап упал на строке больше лимита: {stderr[:200]}"
    assert "ПОСЛЕ-ОГРОМНОЙ" in stdout


@pytest.mark.asyncio
async def test_stdin_path_survives_long_line():
    """Ветка с stdin (её использует Claude/Codex CLI) — тот же ридер."""
    code = (
        "import sys;"
        "sys.stdin.read();"
        "sys.stdout.write('y' * (1024 * 1024));"
        "sys.stdout.write('\\nSTDIN-OK\\n')"
    )
    exit_code, stdout, _ = await run_command(
        [sys.executable, "-c", code], input_text="задача",
    )

    assert exit_code == 0
    assert "STDIN-OK" in stdout


@pytest.mark.asyncio
async def test_normal_output_is_unchanged():
    """Обычный вывод не должен пострадать от правки."""
    code = "print('строка 1'); print('строка 2')"
    exit_code, stdout, _ = await run_command([sys.executable, "-c", code])

    assert exit_code == 0
    assert "строка 1" in stdout and "строка 2" in stdout


def test_limit_is_generous_enough_for_llm_json():
    """64 КиБ не хватало на JSON от агента — лимит должен быть на порядки больше."""
    from backend.app.services.common.process_runner import _STREAM_LIMIT

    assert _STREAM_LIMIT >= 16 * 1024 * 1024
    assert _STREAM_LIMIT > _ASYNCIO_DEFAULT_LIMIT
