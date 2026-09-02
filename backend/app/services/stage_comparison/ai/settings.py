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
from typing import Any

#: Только детерминированный конвейер. Модели не вызываются.
MODE_OFF = "OFF"
#: То же самое на языке инженера: «Быстро». Инженеру не показывают «выключено»
#: как режим работы — он выбирает глубину анализа, а не состояние подсистемы.
MODE_FAST = "FAST"
#: Детерминированный конвейер + массовый аналитик Codex по неоднозначным.
MODE_STANDARD = "STANDARD"
#: STANDARD + выборочный критик Claude + визуальный резерв.
MODE_DEEP = "DEEP"
MODES = (MODE_OFF, MODE_STANDARD, MODE_DEEP)

#: Что может выбрать инженер при запуске прогона.
RUN_MODES = (MODE_FAST, MODE_STANDARD, MODE_DEEP)

_RUN_MODE_ALIASES = {
    MODE_FAST: MODE_OFF,
    MODE_OFF: MODE_OFF,
    MODE_STANDARD: MODE_STANDARD,
    MODE_DEEP: MODE_DEEP,
}


def normalize_mode(value: Any) -> str:
    """Привести режим к внутреннему имени. Неизвестное читается как OFF."""
    return _RUN_MODE_ALIASES.get(str(value or "").strip().upper(), MODE_OFF)


def run_mode_label(value: Any) -> str:
    """Внутреннее имя → то, что видит инженер."""
    return MODE_FAST if normalize_mode(value) == MODE_OFF else normalize_mode(value)


def allowed_run_modes() -> tuple[str, ...]:
    """Что этой установке разрешено запускать. Политика сервера, не клиента.

    Клиент присылает пожелание; разрешает его сервер. Иначе «глубокая
    проверка» на машине без Claude означала бы тихую деградацию критика.
    """
    raw = _env("STAGE_COMPARISON_AI_ALLOWED_MODES", ",".join(RUN_MODES))
    allowed = [
        value.strip().upper() for value in raw.split(",") if value.strip()
    ]
    ordered = tuple(mode for mode in RUN_MODES if mode in allowed)
    # «Быстро» отключить нельзя: это работа без моделей вообще.
    return ordered if MODE_FAST in ordered else (MODE_FAST, *ordered)


def resolve_run_mode(requested: Any = None) -> str:
    """Режим конкретного прогона: пожелание клиента в рамках политики сервера.

    Без пожелания действует прежний путь — переменная окружения, — чтобы
    установки, которые ею пользуются, не поменяли поведение молча.
    """
    if requested in (None, ""):
        return mode()
    label = run_mode_label(requested)
    if label not in allowed_run_modes():
        raise ValueError(f"режим анализа {label!r} на этой установке запрещён")
    return normalize_mode(requested)

CLAUDE_SESSION = "CLAUDE_SESSION"
CODEX_SESSION = "CODEX_SESSION"
PROVIDER_FAMILIES = (CLAUDE_SESSION, CODEX_SESSION)

# Function Lineage is an independently deployable shadow contour.  The flag
# only arms the contour: a production pair or run must also be explicitly
# allowlisted.  The materialization flag reserves a future gate; current
# production code records it for diagnostics but deliberately has no
# materialization path.
FUNCTION_LINEAGE_SHADOW_FEATURE_FLAG = "AI_FUNCTION_LINEAGE_SHADOW_ENABLED"
FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST = (
    "AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST"
)
FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST = (
    "AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST"
)
FUNCTION_LINEAGE_MATERIALIZATION_FEATURE_FLAG = (
    "AI_FUNCTION_LINEAGE_MATERIALIZATION_ENABLED"
)


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


def _env_allowlist(name: str) -> frozenset[str]:
    """Read an exact, case-sensitive comma-separated identifier allowlist."""
    return frozenset(
        value.strip()
        for value in (os.environ.get(name) or "").split(",")
        if value.strip()
    )


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


def context_window() -> int:
    """Сколько строк документа показывать вокруг изменившейся с каждой стороны.

    Слишком узкое окно — и модель честно отвечает EVIDENCE_TRUNCATED вместо
    вывода: на паре АР так закончились 29 % отказов. Слишком широкое — и
    соседняя строка таблицы начинает выглядеть тем же объектом.
    """
    return _env_int("STAGE_COMPARISON_AI_CONTEXT_WINDOW", 6, low=1, high=40)


def concurrency() -> int:
    return _env_int("STAGE_COMPARISON_AI_CONCURRENCY", 4, low=1, high=16)


def cache_enabled() -> bool:
    return _env_bool("STAGE_COMPARISON_AI_CACHE_ENABLED", True)


def function_lineage_shadow_enabled() -> bool:
    """Whether the allowlisted STANDARD shadow contour is armed."""
    return _env_bool(FUNCTION_LINEAGE_SHADOW_FEATURE_FLAG, False)


def function_lineage_shadow_pair_allowlist() -> frozenset[str]:
    """Production pair identifiers explicitly allowed to run the shadow."""
    return _env_allowlist(FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST)


def function_lineage_shadow_run_allowlist() -> frozenset[str]:
    """Production run identifiers explicitly allowed to run the shadow."""
    return _env_allowlist(FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST)


def function_lineage_shadow_target_allowed(*, pair_id: str, run_id: str) -> bool:
    """Return true only when either exact production identifier is allowed.

    Empty allowlists deliberately return false.  The global feature flag is
    checked separately so setting it cannot opt every STANDARD run in.
    """
    return (
        str(pair_id) in function_lineage_shadow_pair_allowlist()
        or str(run_id) in function_lineage_shadow_run_allowlist()
    )


def function_lineage_materialization_enabled() -> bool:
    """Reserved future gate; no current code materializes lineage output."""
    return _env_bool(FUNCTION_LINEAGE_MATERIALIZATION_FEATURE_FLAG, False)


def codex_binary() -> str:
    return _env(
        "STAGE_COMPARISON_AI_CODEX_BIN",
        _env("AUDIT_CODEX_CLI_PATH", ""),
    )


def claude_binary() -> str:
    return _env("STAGE_COMPARISON_AI_CLAUDE_BIN", "claude")


def snapshot(run_mode: str | None = None) -> dict:
    """Полный слепок настроек для аудитного следа прогона."""
    effective = normalize_mode(run_mode) if run_mode else mode()
    return {
        "mode": effective,
        "run_mode": run_mode_label(effective),
        "allowed_run_modes": list(allowed_run_modes()),
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
            "context_window": context_window(),
            "concurrency": concurrency(),
        },
        "cache_enabled": cache_enabled(),
    }


__all__ = [
    "CLAUDE_SESSION",
    "CODEX_SESSION",
    "FUNCTION_LINEAGE_MATERIALIZATION_FEATURE_FLAG",
    "FUNCTION_LINEAGE_SHADOW_FEATURE_FLAG",
    "FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST",
    "FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST",
    "MODES",
    "MODE_DEEP",
    "MODE_FAST",
    "MODE_OFF",
    "MODE_STANDARD",
    "RUN_MODES",
    "allowed_run_modes",
    "normalize_mode",
    "resolve_run_mode",
    "run_mode_label",
    "PROVIDER_FAMILIES",
    "analyst_effort",
    "analyst_model",
    "batch_size",
    "cache_enabled",
    "call_timeout_seconds",
    "claude_binary",
    "codex_binary",
    "concurrency",
    "context_window",
    "critic_model",
    "deep",
    "enabled",
    "function_lineage_materialization_enabled",
    "function_lineage_shadow_enabled",
    "function_lineage_shadow_pair_allowlist",
    "function_lineage_shadow_run_allowlist",
    "function_lineage_shadow_target_allowed",
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
