"""
Гуманизация строк живого лога аудита.

Единственная точка входа — humanize_log_line(): принимает сырую строку от
подпроцесса (Claude CLI, Codex CLI, python-скрипты) и возвращает, что с ней
делать. Цель — в audit_log.jsonl и WS-лог не должны попадать сырые JSON-события
и построчные JSON/diff-фрагменты артефактов, которые модель пишет на диск.

Классы мусора (наблюдались в живых логах, см. проекты 214_Alia/13АВ-РД-ВК2-К4):
  1. stream-события Codex CLI: {"type":"thread.started"}, {"type":"turn.started"},
     {"type":"item.completed","item":{...}}, {"type":"turn.completed","usage":{...}}
  2. Построчный diff/JSON, когда агент пишет артефакт (apply_patch):
     '+      "id": "OPT-1",'  /  '"F-001",'  /  '],'  /  '}'
  3. Баннер Codex exec: 'OpenAI Codex v0.144.2', 'workdir: ...', 'model: ...'
Строки могут приходить с префиксами '[OPT codex] ', '[OPT claude] ', '[ERR] '.

При этом ОШИБКИ не глушатся: однострочные JSON-тела ошибок API
({"error":{"message":"usage limit reached"}}) превращаются в человеческую
error-строку, а не выбрасываются.

Claude CLI stream-json (type=result/assistant/user/...) обрабатывается ДО этого
модуля в manager._log (result → cli_summary карточка) и сюда не доходит.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass

# Префиксы, которые оборачивают строки подпроцессов. Снимаем для анализа,
# возвращаем обратно, если строка выжила.
_KNOWN_PREFIX_RE = re.compile(r"^(\[(?:OPT [a-z]+|ERR)\]\s+)+")

# Событийные типы Codex CLI (codex exec --json), которые не несут пользы человеку.
_CODEX_SILENT_TYPES = {
    "thread.started",
    "turn.started",
    "item.started",
    "item.updated",
    "item.completed",
}

# Баннер codex exec (печатается в начале каждого запуска).
_CODEX_BANNER_RE = re.compile(
    r"^(OpenAI Codex v[\d.]+"
    r"|-{3,}\s*$"
    r"|workdir: "
    r"|model: "
    r"|provider: "
    r"|approval: "
    r"|sandbox: "
    r"|reasoning effort: "
    r"|reasoning summaries: "
    r"|session id: "
    r"|rollout: "
    r"|logs: "
    r"|tokens used[:\s]"
    r"|mcp startup:"
    r")",
)

# Маркеры секций вывода codex exec без полезного содержимого.
_CODEX_MARKER_LINES = {"exec", "thinking", "codex", "user", "assistant"}

# Построчный JSON/diff-фрагмент записываемого документа. Осторожно с ложными
# срабатываниями: человеческие строки тоже могут начинаться с кавычки
# («"03_findings.json" создан») или с '- "…"' (маркдаун-буллет с цитатой) —
# такие НЕ давим. JSON-фрагмент отличают ключ с двоеточием ("key":) либо
# структурный хвост (',', '{', '[', '}', ']' в конце строки).
_JSON_KEY_RE = re.compile(r'^[+-]?\s*"[^"]*"\s*:')
_QUOTE_START_RE = re.compile(r'^[+-]?\s*"')
_BRACE_START_RE = re.compile(r"^[+-]?\s*[{}]")
_BRACKET_FRAGMENT_RE = re.compile(
    r"^[+-]?\s*("
    r"\],?\s*$"      # ] или ],
    r'|\[\s*["{\]]'  # [" / [{ / []
    r"|\[\s*$"       # одинокая [
    r")"
)
# Голые значения из pretty-printed JSON-массивов: '  5,' / 'true,' / 'null'
_BARE_JSON_VALUE_RE = re.compile(
    r"^[+-]?\s*(null|true|false|-?\d+(\.\d+)?)\s*,\s*$"
)
_STRUCTURAL_TAIL_RE = re.compile(r"[,{\[\]}]\s*$")

# Маркеры apply_patch (codex пишет файл): *** Begin Patch / *** Update File: / @@
_PATCH_MARKER_RE = re.compile(
    r"^(\*\*\* (Begin|End) Patch"
    r"|\*\*\* (Update|Add|Delete) File:"
    r"|@@( |$))"
)


def _fmt_int(value: object) -> str:
    try:
        return f"{int(value):,}".replace(",", " ")
    except (TypeError, ValueError):
        return "0"


@dataclass
class HumanizedLine:
    """Результат гуманизации: text=None → строку не логировать."""

    text: str | None
    level: str = "info"


def split_known_prefix(message: str) -> tuple[str, str]:
    """Отделить известные префиксы ('[OPT codex] ', '[ERR] ') от тела строки."""
    match = _KNOWN_PREFIX_RE.match(message or "")
    if not match:
        return "", (message or "")
    prefix = match.group(0)
    return prefix, message[len(prefix):]


def _extract_error_message(payload: dict) -> str | None:
    """Достать человеческий текст из JSON-тела ошибки API, если это оно."""
    for key in ("error", "detail", "message"):
        if key not in payload:
            continue
        value = payload[key]
        if isinstance(value, dict):
            inner = value.get("message") or value.get("detail")
            if inner:
                return str(inner)
            return json.dumps(value, ensure_ascii=False)
        if value:
            return str(value)
    return None


def _humanize_codex_event(payload: dict) -> HumanizedLine:
    """Событие Codex CLI (--json) → человеческая строка или подавление."""
    event_type = str(payload.get("type") or "")

    if event_type in _CODEX_SILENT_TYPES:
        return HumanizedLine(text=None)

    if event_type == "turn.completed":
        usage = payload.get("usage") or {}
        if isinstance(usage, dict) and usage:
            parts = [
                f"in {_fmt_int(usage.get('input_tokens'))}",
                f"out {_fmt_int(usage.get('output_tokens'))}",
            ]
            cached = usage.get("cached_input_tokens") or 0
            if cached:
                parts.append(f"кэш {_fmt_int(cached)}")
            reasoning = usage.get("reasoning_output_tokens") or 0
            if reasoning:
                parts.append(f"рассуждения {_fmt_int(reasoning)}")
            return HumanizedLine(
                text=f"Codex: ход завершён ({' / '.join(parts)} токенов)",
            )
        return HumanizedLine(text="Codex: ход завершён")

    if event_type in ("turn.failed", "error"):
        detail = _extract_error_message(payload) or str(
            payload.get("message") or "без деталей"
        )
        return HumanizedLine(text=f"Codex: ошибка — {detail}", level="error")

    # Неизвестное событие — не показываем сырой JSON.
    return HumanizedLine(text=None)


def _humanize_full_json(body: str) -> HumanizedLine | None:
    """Однострочный полный JSON-объект: событие → гуманизировать, тело ошибки
    API → error-строка, прочий машинный JSON → подавить. None = не JSON."""
    if not (body.startswith("{") and body.rstrip().endswith("}")):
        return None
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if "type" in payload:
        return _humanize_codex_event(payload)
    error_text = _extract_error_message(payload)
    if error_text:
        return HumanizedLine(text=f"Ошибка API: {error_text}", level="error")
    return HumanizedLine(text=None)


def _is_json_fragment(body: str) -> bool:
    """Похоже ли на кусок записываемого JSON-документа/diff'а."""
    if _JSON_KEY_RE.match(body):                     # "key": ...
        return True
    if _BRACE_START_RE.match(body):                  # { / } / },
        return True
    if _BRACKET_FRAGMENT_RE.match(body):             # ] / ], / [{ / ["
        return True
    if _BARE_JSON_VALUE_RE.match(body):              # 5, / true, / null,
        return True
    if _QUOTE_START_RE.match(body) and _STRUCTURAL_TAIL_RE.search(body):
        return True                                  # "F-001", / "x"],
    return False


def humanize_log_line(message: str, level: str = "info") -> HumanizedLine:
    """Решить судьбу строки live-лога.

    Возвращает HumanizedLine: .text — что писать (None = подавить),
    .level — возможно повышенный уровень (например, error для turn.failed).
    """
    if not message:
        return HumanizedLine(text=None)

    prefix, rest = split_known_prefix(message)
    body = rest.strip()

    if not body:
        return HumanizedLine(text=None)

    # 1. Однострочный полный JSON: событие CLI / тело ошибки API / машинный JSON.
    full_json = _humanize_full_json(body)
    if full_json is not None:
        if full_json.text is None:
            return full_json
        return HumanizedLine(text=f"{prefix}{full_json.text}", level=full_json.level)

    # 2. Построчные JSON/diff-фрагменты записываемых артефактов.
    if _is_json_fragment(body) or _PATCH_MARKER_RE.match(body):
        return HumanizedLine(text=None)

    # 3. Баннер и служебные маркеры codex exec.
    if _CODEX_BANNER_RE.match(body) or body.lower() in _CODEX_MARKER_LINES:
        return HumanizedLine(text=None)

    return HumanizedLine(text=message, level=level)
