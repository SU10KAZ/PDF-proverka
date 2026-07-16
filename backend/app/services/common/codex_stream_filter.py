"""
Фильтр живого стрима `codex exec` (plain-режим) для лога аудита.

codex exec печатает в stdout/stderr ВЕСЬ транскрипт сессии: баннер, эхо
промпта, размышления, выполняемые команды и их полный вывод — содержимое
читаемых файлов, diff'ы apply_patch, дампы маркдаун-таблиц из проектных MD.
На живом прогоне оптимизации (АИ1, 2026-07-16) это дало ~2500 строк на один
запуск: лавина вытесняла остальные секции вкладки «Лог» и выглядела для
пользователя как «JSON-мусор и обрывки таблиц».

Построчный log_humanizer такое не отфильтрует: строки контента человекочитаемы
(таблицы, прозаические абзацы документа) и неотличимы от легитимных сообщений
пайплайна. Поэтому для codex-стрима действует БЕЛЫЙ список:

  - ``web search: <запрос>`` → «Codex: веб-поиск — <запрос>» (сигнал
    активности; голый ``web search:`` без запроса глушится);
  - однострочный полный JSON ``{...}`` — пропускается дальше: log_humanizer
    превратит тело ошибки API в error-строку, а машинные события подавит;
  - строки об ошибках (``error`` / ``stream error`` / ``fatal`` / ``panic``);
  - ``tokens used`` (+ число на той же или следующей строке) →
    «Codex: токенов использовано N»;
  - служебные строки НАШЕГО runner-кода, идущие через тот же on_output ДО
    старта процесса codex («Codex optimization vision: attached N …»).

Всё остальное — эхо промпта, вывод команд, финальное JSON-сообщение агента —
подавляется. Итоговые сводки этапа логирует вызывающий код (ensemble).

Фильтр stateful (число токенов может прийти строкой ниже «tokens used») —
создавайте отдельный экземпляр на каждый запуск codex.
"""
from __future__ import annotations

import re
from typing import Awaitable, Callable

_WEB_SEARCH_RE = re.compile(r"^web search:\s*(\S.*)$", re.IGNORECASE)
_ERROR_RE = re.compile(r"^(stream\s+)?error\b|^fatal\b|^panic\b", re.IGNORECASE)
_TOKENS_USED_RE = re.compile(r"^tokens used\b[:\s]*([\d][\d\s ,._]*)?$", re.IGNORECASE)
_BARE_NUMBER_RE = re.compile(r"^[\d][\d\s ,._]*$")

# Служебные сообщения нашего runner-кода: claude_runner шлёт их через тот же
# on_output ДО старта процесса codex — это не транскрипт, глушить нельзя.
_RUNNER_SERVICE_RE = re.compile(r"^Codex optimization vision:")


def _format_tokens(raw: str) -> str:
    return re.sub(r"[\s ]+", " ", raw).strip(" ,._")


class CodexExecStreamFilter:
    """Белый список для построчного стрима codex exec.

    ``feed(line)`` возвращает строку для лога или None (подавить).
    """

    def __init__(self) -> None:
        self._awaiting_token_count = False

    def feed(self, line: str) -> str | None:
        body = (line or "").strip()
        if not body:
            self._awaiting_token_count = False
            return None

        # «tokens used» → число либо в той же строке, либо в следующей.
        if self._awaiting_token_count:
            self._awaiting_token_count = False
            if _BARE_NUMBER_RE.match(body):
                return f"Codex: токенов использовано {_format_tokens(body)}"
            # не число — строка проходит обычную обработку ниже

        tokens = _TOKENS_USED_RE.match(body)
        if tokens:
            count = (tokens.group(1) or "").strip()
            if count:
                return f"Codex: токенов использовано {_format_tokens(count)}"
            self._awaiting_token_count = True
            return None

        search = _WEB_SEARCH_RE.match(body)
        if search:
            return f"Codex: веб-поиск — {search.group(1).strip()}"

        if _RUNNER_SERVICE_RE.match(body):
            return line

        # Полный однострочный JSON — дальше решает log_humanizer
        # (тело ошибки API → error-строка, машинное событие → подавить).
        if body.startswith("{") and body.endswith("}"):
            return line

        if _ERROR_RE.match(body):
            return line

        return None


def wrap_codex_on_output(
    on_output: Callable[[str], Awaitable[None]] | None,
) -> Callable[[str], Awaitable[None]] | None:
    """Обернуть on_output белым списком codex exec (None → None).

    Для вызывающих, которые знают, что этап пойдёт через codex-модель
    (is_codex_model): прямой не-ансамблевый запуск оптимизации и т.п.
    Каждый вызов создаёт СВОЙ экземпляр фильтра (stateful).
    """
    if on_output is None:
        return None

    stream_filter = CodexExecStreamFilter()

    async def _filtered(message: str) -> None:
        kept = stream_filter.feed(message)
        if kept is not None:
            await on_output(kept)

    return _filtered
