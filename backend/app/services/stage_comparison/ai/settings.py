"""Настройки ИИ-слоя сравнения. Читаются в момент вызова, а не при импорте.

Модульная константа, посчитанная на импорте, не переключается ни тестом, ни
перезапуском этапа — а весь смысл этих флагов в том, чтобы прогон можно было
провести в трёх режимах подряд. Поэтому здесь функции-читатели, как у
stage02_paid_cache.cache_enabled().

С дефолтами ниже поведение системы совпадает со сборкой без ИИ вообще:
STAGE_COMPARISON_AI_MODE=OFF ничего не запускает и ничего не пишет.
"""
from __future__ import annotations

import os

#: Только детерминированный конвейер. Модели не вызываются.
MODE_OFF = "OFF"
#: Детерминированный конвейер + массовый аналитик Codex по неоднозначным.
MODE_STANDARD = "STANDARD"
#: STANDARD + выборочный критик Claude + визуальный резерв.
MODE_DEEP = "DEEP"
MODES = (MODE_OFF, MODE_STANDARD, MODE_DEEP)

CLAUDE_SESSION = "CLAUDE_SESSION"
CODEX_SESSION = "CODEX_SESSION"
PROVIDER_FAMILIES = (CLAUDE_SESSION, CODEX_SESSION)


def _env(name: str, default: str) -> str:
    return (os.environ.get(name) or "").strip() or default


def _env_int(name: str, default: int, *, low: int, high: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(low, min(high, int(raw)))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def mode() -> str:
    """OFF / STANDARD / DEEP. Неизвестное значение читается как OFF."""
    value = _env("STAGE_COMPARISON_AI_MODE", MODE_OFF).upper()
    return value if value in MODES else MODE_OFF


def enabled() -> bool:
    return mode() != MODE_OFF


def deep() -> bool:
    return mode() == MODE_DEEP


# ── Модели и уровни рассуждения ────────────────────────────────────────────
# Массовый аналитик выбран по замеру: на подтверждённых листах Codex
# gpt-5.6-sol/low закрывал 92 % элементов при 100 % прохождении верификатора,
# а более высокие уровни рассуждения давали то же качество за большее время.

def analyst_model() -> str:
    return _env("STAGE_COMPARISON_AI_ANALYST_MODEL", "gpt-5.6-sol")


def analyst_effort() -> str:
    return _env("STAGE_COMPARISON_AI_ANALYST_EFFORT", "low")


def retry_effort() -> str:
    """Уровень второй попытки: включается только после отказа верификатора."""
    return _env("STAGE_COMPARISON_AI_RETRY_EFFORT", "high")


def critic_model() -> str:
    return _env("STAGE_COMPARISON_AI_CRITIC_MODEL", "claude-opus-5")


def vision_model() -> str:
    return _env("STAGE_COMPARISON_AI_VISION_MODEL", "gpt-5.6-sol")


def vision_effort() -> str:
    return _env("STAGE_COMPARISON_AI_VISION_EFFORT", "medium")


# ── Пределы ────────────────────────────────────────────────────────────────
# Исчерпание любого предела не роняет конвейер: остаток честно уезжает
# человеку с причиной, а не исчезает.

def max_items() -> int:
    return _env_int("STAGE_COMPARISON_AI_MAX_ITEMS", 400, low=0, high=100000)


def max_batches() -> int:
    return _env_int("STAGE_COMPARISON_AI_MAX_BATCHES", 120, low=0, high=10000)


def max_retries() -> int:
    return _env_int("STAGE_COMPARISON_AI_MAX_RETRIES", 1, low=0, high=3)


def max_critic_passes() -> int:
    return _env_int("STAGE_COMPARISON_AI_MAX_CRITIC_PASSES", 40, low=0, high=10000)


def max_vision_items() -> int:
    return _env_int("STAGE_COMPARISON_AI_MAX_VISION_ITEMS", 20, low=0, high=1000)


def max_session_seconds() -> int:
    return _env_int(
        "STAGE_COMPARISON_AI_MAX_SESSION_SECONDS", 1800, low=30, high=86400
    )


def call_timeout_seconds() -> int:
    return _env_int(
        "STAGE_COMPARISON_AI_CALL_TIMEOUT_SECONDS", 420, low=10, high=3600
    )


def batch_size() -> int:
    return _env_int("STAGE_COMPARISON_AI_BATCH_SIZE", 10, low=1, high=50)


def concurrency() -> int:
    return _env_int("STAGE_COMPARISON_AI_CONCURRENCY", 4, low=1, high=16)


def cache_enabled() -> bool:
    return _env_bool("STAGE_COMPARISON_AI_CACHE_ENABLED", True)


def codex_binary() -> str:
    return _env(
        "STAGE_COMPARISON_AI_CODEX_BIN",
        _env("AUDIT_CODEX_CLI_PATH", ""),
    )


def claude_binary() -> str:
    return _env("STAGE_COMPARISON_AI_CLAUDE_BIN", "claude")


def snapshot() -> dict:
    """Полный слепок настроек для аудитного следа прогона."""
    return {
        "mode": mode(),
        "analyst": {"model": analyst_model(), "reasoning_level": analyst_effort()},
        "retry": {"reasoning_level": retry_effort()},
        "critic": {"model": critic_model()},
        "vision": {"model": vision_model(), "reasoning_level": vision_effort()},
        "limits": {
            "max_items": max_items(),
            "max_batches": max_batches(),
            "max_retries": max_retries(),
            "max_critic_passes": max_critic_passes(),
            "max_vision_items": max_vision_items(),
            "max_session_seconds": max_session_seconds(),
            "call_timeout_seconds": call_timeout_seconds(),
            "batch_size": batch_size(),
            "concurrency": concurrency(),
        },
        "cache_enabled": cache_enabled(),
    }


__all__ = [
    "CLAUDE_SESSION",
    "CODEX_SESSION",
    "MODES",
    "MODE_DEEP",
    "MODE_OFF",
    "MODE_STANDARD",
    "PROVIDER_FAMILIES",
    "analyst_effort",
    "analyst_model",
    "batch_size",
    "cache_enabled",
    "call_timeout_seconds",
    "claude_binary",
    "codex_binary",
    "concurrency",
    "critic_model",
    "deep",
    "enabled",
    "max_batches",
    "max_critic_passes",
    "max_items",
    "max_retries",
    "max_session_seconds",
    "max_vision_items",
    "mode",
    "retry_effort",
    "snapshot",
    "vision_effort",
    "vision_model",
]
