"""
Утилиты для работы с Claude CLI.
Детекция ошибок, парсинг вывода, визуализация сеток тайлов.
"""
import json
import re
from pathlib import Path
from typing import Optional, Callable, Awaitable

from webapp.models.usage import CLIResult

# ─── Паттерны rate limit ошибок Claude CLI ───
_RATE_LIMIT_PATTERNS = [
    re.compile(r"rate.?limit", re.IGNORECASE),
    re.compile(r"429", re.IGNORECASE),
    re.compile(r"too many requests", re.IGNORECASE),
    re.compile(r"overloaded", re.IGNORECASE),
    re.compile(r"resource.?exhausted", re.IGNORECASE),
    re.compile(r"token.?limit", re.IGNORECASE),
    re.compile(r"capacity", re.IGNORECASE),
    re.compile(r"quota.?exceeded", re.IGNORECASE),
    re.compile(r"hit your limit", re.IGNORECASE),
    re.compile(r"you.ve hit", re.IGNORECASE),
]


def is_cancelled(exit_code: int) -> bool:
    """Exit code -2 = asyncio CancelledError (отмена пользователем/системой)."""
    return exit_code == -2


def is_timeout(exit_code: int) -> bool:
    """Exit code -1 = таймаут Claude CLI."""
    return exit_code == -1


def is_prompt_too_long(exit_code: int, stdout: str, stderr: str) -> bool:
    """Определить, вызвана ли ошибка превышением размера промпта (нерепетируемая)."""
    if exit_code == 0:
        return False
    combined = f"{stdout or ''}\n{stderr or ''}"
    return bool(re.search(r"prompt is too long", combined, re.IGNORECASE))


def is_rate_limited(exit_code: int, stdout: str, stderr: str) -> bool:
    """
    Определить, вызвана ли ошибка исчерпанием rate limit.

    Проверяет stdout, stderr и JSON-вывод Claude CLI на наличие
    характерных маркеров: 429, rate limit, overloaded, hit your limit и т.д.
    """
    if exit_code == 0 or exit_code == -2:
        return False

    combined = f"{stdout or ''}\n{stderr or ''}"
    for pattern in _RATE_LIMIT_PATTERNS:
        if pattern.search(combined):
            return True
    return False


def parse_rate_limit_reset(text: str) -> int | None:
    """
    Извлечь время сброса лимита из сообщения Claude CLI.

    Примеры:
        "You've hit your limit · resets 11pm (Europe/Moscow)" → секунды до 23:00 MSK
        "resets 3am (Europe/Moscow)" → секунды до 03:00 MSK

    Returns:
        Количество секунд до сброса, или None если не удалось распарсить.
    """
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    m = re.search(r"resets?\s+(\d{1,2})(am|pm)\s*\(([^)]+)\)", text, re.IGNORECASE)
    if not m:
        return None

    hour = int(m.group(1))
    ampm = m.group(2).lower()

    # Конвертируем 12-часовой формат
    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0

    # MSK = UTC+3
    msk = _tz(_td(hours=3))
    now_msk = _dt.now(msk)
    reset_time = now_msk.replace(hour=hour, minute=0, second=0, microsecond=0)

    # Если reset уже прошёл сегодня — это завтра
    if reset_time <= now_msk:
        reset_time += _td(days=1)

    wait_sec = int((reset_time - now_msk).total_seconds())
    return max(wait_sec, 60)  # минимум 60 секунд


def load_template(template_path: Path) -> str:
    """Загрузить шаблон задачи."""
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


def parse_cli_json_output(stdout: str) -> CLIResult:
    """
    Парсить JSON-вывод Claude CLI (--output-format json).
    Извлекает result (текст), cost_usd, session_id и другие метаданные.
    При ошибке парсинга — fallback на сырой stdout как текст.
    """
    if not stdout or not stdout.strip():
        return CLIResult(result_text="", is_error=True)

    try:
        data = json.loads(stdout)
        usage = data.get("usage") or {}
        return CLIResult(
            result_text=data.get("result", stdout),
            is_error=data.get("is_error", False),
            cost_usd=data.get("total_cost_usd", 0.0) or 0.0,
            duration_ms=data.get("duration_ms", 0) or 0,
            duration_api_ms=data.get("duration_api_ms", 0) or 0,
            num_turns=data.get("num_turns", 0) or 0,
            session_id=data.get("session_id"),
            input_tokens=int(usage.get("input_tokens", 0) or 0),
            output_tokens=int(usage.get("output_tokens", 0) or 0),
            cache_creation_tokens=int(usage.get("cache_creation_input_tokens", 0) or 0),
            cache_read_tokens=int(usage.get("cache_read_input_tokens", 0) or 0),
        )
    except (json.JSONDecodeError, KeyError, TypeError):
        # Fallback: stdout не является валидным JSON — вернуть как текст
        return CLIResult(result_text=stdout, is_error=True)


async def send_output(
    on_output: Optional[Callable[[str], Awaitable[None]]],
    text: str,
):
    """Отправить текст в on_output callback построчно."""
    if not on_output or not text:
        return
    for line in text.splitlines():
        try:
            await on_output(line)
        except Exception:
            pass


def build_grid_visual(grid: str, tiles: list) -> str:
    """Построить ASCII-визуализацию сетки тайлов для промпта.

    Пример для 2x3:
    ```
    +----------+----------+----------+
    | r1c1     | r1c2     | r1c3     |
    | (лев-верх)| (центр-верх)| (прав-верх)|
    +----------+----------+----------+
    | r2c1     | r2c2     | r2c3     |
    | (лев-низ) | (центр-низ) | (прав-низ) |
    +----------+----------+----------+
    ```
    """
    try:
        parts = grid.split("x")
        rows, cols = int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return ""

    if rows < 1 or cols < 1 or rows > 10 or cols > 10:
        return ""

    # Маппинг позиций
    pos_labels = {}
    row_labels = {1: "верх", 2: "низ"} if rows == 2 else {i: str(i) for i in range(1, rows + 1)}
    col_labels = {1: "лев", 2: "прав"} if cols == 2 else (
        {1: "лев", 2: "центр", 3: "прав"} if cols == 3 else {i: str(i) for i in range(1, cols + 1)}
    )

    for r in range(1, rows + 1):
        for c in range(1, cols + 1):
            rl = row_labels.get(r, str(r))
            cl = col_labels.get(c, str(c))
            pos_labels[(r, c)] = f"{cl}-{rl}"

    cell_w = 14
    sep = "+" + (("-" * cell_w + "+") * cols)
    lines = [sep]
    for r in range(1, rows + 1):
        row1 = "|"
        row2 = "|"
        for c in range(1, cols + 1):
            cell_id = f"r{r}c{c}"
            pos = pos_labels.get((r, c), "")
            row1 += f" {cell_id:<{cell_w - 1}}|"
            row2 += f" ({pos}){' ' * max(0, cell_w - len(pos) - 3)}|"
        lines.append(row1)
        lines.append(row2)
        lines.append(sep)

    return "\n".join(lines)
