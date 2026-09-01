"""Prompt surface for a selector that cannot author engineering facts."""
from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

SYSTEM_PROMPT = (
    "Ты — инженерный селектор. Все факты, ссылки, значения и варианты уже "
    "неизменяемо подготовлены backend. Для каждой задачи выбери ровно один "
    "переданный candidate_id. Не создавай и не исправляй доказательства."
)


def selector_prompt(
    *,
    shared_context: Mapping[str, Any],
    tasks: Sequence[Mapping[str, Any]],
) -> str:
    return "\n\n".join([
        "AI ANALYST V3 — BOUNDED SELECTOR",
        (
            "Правила: выбери только candidate_id из options данной задачи; "
            "NONE/INSUFFICIENT выбирай при недоказанности. optional_short_reason "
            "— только диагностика и никогда не доказательство. Ответь один раз "
            "на каждый task_id, ничего не цитируй и не добавляй."
        ),
        "COMPACT SHEET CONTEXT\n" + json.dumps(
            shared_context, ensure_ascii=False, separators=(",", ":")
        ),
        "BOUNDED TASKS\n" + json.dumps(
            list(tasks), ensure_ascii=False, separators=(",", ":")
        ),
        "Ответь строго по JSON Schema.",
    ])


__all__ = ["SYSTEM_PROMPT", "selector_prompt"]
