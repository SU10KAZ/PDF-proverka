"""Regression: norm_verify не должен считать exit=0 без файла успехом.

Покрывает кейс, когда Claude CLI отвечает success (exit_code=0), но по какой-
либо причине не создаёт ожидаемый `_output/norm_checks_llm.json` (например,
печатает JSON в чат вместо вызова Write, или пишет в неправильный путь).
До фикса pipeline тихо пропускал merge и этап отчитывался как выполненный.
После фикса должен быть ровно один контролируемый retry, а потом явный raise.
"""
from __future__ import annotations

import asyncio
import json
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# ─── fake pipeline harness ────────────────────────────────────────────────
# Мы не хотим поднимать весь PipelineManager (там AuditJob/websockets/...).
# Вместо этого дублируем ровно ту петлю post-check'а, что лежит в
# pipeline_service._run_norm_verification, и тестируем её.


async def _post_check_loop(
    *,
    claude_call,
    norm_checks_llm_path: Path,
    max_cli_retries: int = 5,
):
    """Минимальная копия post-check семантики из pipeline_service.

    Используем для unit-теста без запуска webapp.
    """
    # clear stale
    if norm_checks_llm_path.exists():
        norm_checks_llm_path.unlink()

    # основной цикл (сейчас у нас одна попытка = success)
    for attempt in range(1, max_cli_retries + 1):
        exit_code = await claude_call()
        if exit_code == 0:
            break
        if attempt >= max_cli_retries:
            raise RuntimeError(f"max_retries исчерпано (exit={exit_code})")

    # post-check — ровно 1 ретрай
    if not norm_checks_llm_path.exists():
        exit_code = await claude_call()
        if exit_code != 0 or not norm_checks_llm_path.exists():
            raise RuntimeError(
                f"norm_verify: paragraph verification не выполнена — "
                f"{norm_checks_llm_path.name} не создан (retry exit={exit_code})"
            )


# ─── tests ────────────────────────────────────────────────────────────────

def _make_call(side_effects: list[tuple[int, bool]], out: Path):
    """Фабрика fake claude_call.

    side_effects — список (exit_code, create_file), по одному элементу на
    попытку. На каждый вызов возвращаем соответствующий exit и при
    create_file=True создаём out.
    """
    iterator = iter(side_effects)

    async def _call():
        ec, create = next(iterator)
        if create:
            out.write_text('{"checks":[],"paragraph_checks":[]}', encoding="utf-8")
        return ec

    return _call


def test_exit_zero_without_file_raises_after_retry(tmp_path):
    """exit=0 дважды подряд, но файл не создан → RuntimeError."""
    out = tmp_path / "norm_checks_llm.json"
    call = _make_call([(0, False), (0, False)], out)

    with pytest.raises(RuntimeError, match="не создан"):
        asyncio.run(_post_check_loop(claude_call=call, norm_checks_llm_path=out))

    assert not out.exists(), "файл не должен был появиться"


def test_exit_zero_without_file_then_success_on_retry(tmp_path):
    """Первый exit=0 без файла, retry — файл создан. Должно проходить."""
    out = tmp_path / "norm_checks_llm.json"
    call = _make_call([(0, False), (0, True)], out)

    asyncio.run(_post_check_loop(claude_call=call, norm_checks_llm_path=out))
    assert out.exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert "paragraph_checks" in data


def test_exit_zero_with_file_no_retry(tmp_path):
    """Нормальный успех — файл создан на первой попытке, retry не нужен."""
    out = tmp_path / "norm_checks_llm.json"
    calls_made = []

    async def _call():
        calls_made.append(True)
        out.write_text('{"checks":[],"paragraph_checks":[]}', encoding="utf-8")
        return 0

    asyncio.run(_post_check_loop(claude_call=_call, norm_checks_llm_path=out))
    assert out.exists()
    assert len(calls_made) == 1, "ретрая быть не должно"


def test_stale_file_gets_cleared_before_run(tmp_path):
    """Если файл уже лежит от прошлого прогона, он удаляется ДО новой CLI-попытки.

    Это чтобы post-check реально оценивал текущую попытку, а не наследство.
    """
    out = tmp_path / "norm_checks_llm.json"
    out.write_text('{"stale":true}', encoding="utf-8")

    call = _make_call([(0, False), (0, False)], out)

    with pytest.raises(RuntimeError, match="не создан"):
        asyncio.run(_post_check_loop(claude_call=call, norm_checks_llm_path=out))

    # stale был удалён и не был восстановлен
    assert not out.exists()


# ─── prompt invariant: Write-инструкция для пустого задания ───────────────

def test_prompt_demands_file_even_when_empty():
    """Промпт должен явно требовать создать файл даже при пустом задании."""
    ru = (ROOT / "prompts/pipeline/ru/norm_verify_task.md").read_text(encoding="utf-8")
    en = (ROOT / "prompts/pipeline/en/norm_verify_task.md").read_text(encoding="utf-8")

    for name, text in (("ru", ru), ("en", en)):
        assert "norm_checks_llm.json" in text, name
        # RU — "даже если список для верификации пуст"
        # EN — "even if the verification assignment is empty"
        assert any(
            phrase.lower() in text.lower()
            for phrase in (
                "даже если список для верификации пуст",
                "даже при пустом задании",
                "even if the verification assignment is empty",
            )
        ), f"{name}: нет требования создавать файл при пустом задании"
