"""
rate_limit_retry.py
-------------------
Pure, testable конфигурация и backoff-логика для bounded rate-limit retry
в text_analysis.

Контекст бага: раньше `run_text_analysis` делал ОДИН wait+retry. Если после
него лимит сохранялся (CLI снова возвращал rate-limit код), этап немедленно
падал как `Текстовый анализ: код 1`. Особенно плохо, когда reset time из CLI
не парсился (`Сброс через ~`) — `wait_for_rate_limit` мог вернуть False, и
проект жёстко фейлился вместо retry.

Этот модуль НЕ ходит в сеть и НЕ зависит от manager/scanner — только считает
параметры backoff и решения, поэтому полностью покрывается unit-тестами.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.environ.get(name, "")).strip())
        return v if v >= 0 else default
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = float(str(os.environ.get(name, "")).strip())
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class RateLimitRetryConfig:
    """Параметры bounded rate-limit retry (читаются из env с дефолтами)."""

    max_retries: int = 3
    fallback_backoff_sec: int = 300       # старт fallback-backoff, когда reset time не распарсился
    backoff_multiplier: float = 2.0       # экспоненциальный рост по попыткам
    max_backoff_sec: int = 3600           # верхняя граница одного fallback-ожидания
    pause_on_exhausted: bool = False      # ставить очередь на паузу при rate_limit_exhausted


def load_rate_limit_config() -> RateLimitRetryConfig:
    """Собрать конфиг из env. Безопасные дефолты сохраняют прежнее поведение,
    кроме главного отличия: теперь до `max_retries` попыток вместо одной."""
    return RateLimitRetryConfig(
        max_retries=_env_int("TEXT_ANALYSIS_RATE_LIMIT_MAX_RETRIES", 3),
        fallback_backoff_sec=_env_int("TEXT_ANALYSIS_RATE_LIMIT_FALLBACK_BACKOFF_SEC", 300),
        backoff_multiplier=_env_float("TEXT_ANALYSIS_RATE_LIMIT_BACKOFF_MULT", 2.0),
        max_backoff_sec=_env_int("TEXT_ANALYSIS_RATE_LIMIT_MAX_BACKOFF_SEC", 3600),
        pause_on_exhausted=_env_bool("TEXT_ANALYSIS_PAUSE_ON_RATE_LIMIT", False),
    )


def compute_fallback_backoff(attempt: int, cfg: RateLimitRetryConfig) -> int:
    """Fallback-ожидание (сек) для попытки `attempt` (1-based), когда reset time
    из CLI не распарсился.

    Экспоненциально: base * mult^(attempt-1), но не больше max_backoff_sec и не
    меньше 1. Ограничение сверху гарантирует, что очередь не зависнет навсегда.
    """
    if attempt < 1:
        attempt = 1
    raw = cfg.fallback_backoff_sec * (cfg.backoff_multiplier ** (attempt - 1))
    bounded = min(int(raw), cfg.max_backoff_sec)
    return max(1, bounded)


# Канонические reason-коды итогового статуса этапа (для pipeline_log / StageResult.data)
REASON_RATE_LIMIT_EXHAUSTED = "rate_limit_exhausted"
REASON_RATE_LIMIT_WAIT_ABORTED = "rate_limit_wait_aborted"
