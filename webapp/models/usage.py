"""Pydantic-модели для трекинга потребления токенов."""

from pydantic import BaseModel
from typing import Optional


class UsageRecord(BaseModel):
    """Одна запись о потреблении (одна Claude CLI сессия)."""

    timestamp: str  # ISO datetime
    session_id: Optional[str] = None
    project_id: str = ""
    stage: str = ""  # tile_batch, main_audit, triage, etc.
    model: str = ""
    # Из JSON output (мгновенные):
    cost_usd: float = 0.0
    duration_ms: int = 0
    num_turns: int = 0
    # Из JSONL post-parse (точные, заполняются позже):
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_tokens: int = 0
    cache_read_tokens: int = 0


class UsageCounters(BaseModel):
    """Три счётчика для фронтенда."""

    # Сессионный счётчик (ручной сброс)
    session_cost_usd: float = 0.0
    session_input_tokens: int = 0
    session_output_tokens: int = 0
    session_total_tokens: int = 0
    session_calls: int = 0
    session_started_at: Optional[str] = None

    # 5-часовое окно (скользящее)
    window_5h_cost_usd: float = 0.0
    window_5h_input_tokens: int = 0
    window_5h_output_tokens: int = 0
    window_5h_total_tokens: int = 0
    window_5h_calls: int = 0
    window_5h_limit_tokens: int = 220_000
    window_5h_remaining_tokens: int = 220_000
    window_5h_percent_used: float = 0.0
    window_5h_resets_at: Optional[str] = None

    # Недельный счётчик (сброс по понедельникам)
    weekly_cost_usd: float = 0.0
    weekly_input_tokens: int = 0
    weekly_output_tokens: int = 0
    weekly_total_tokens: int = 0
    weekly_calls: int = 0
    weekly_limit_tokens: int = 7_400_000
    weekly_remaining_tokens: int = 7_400_000
    weekly_percent_used: float = 0.0
    weekly_started_at: Optional[str] = None


class ModelUsage(BaseModel):
    """Токены по одной модели за период."""

    model: str = ""
    output_tokens: int = 0
    input_tokens: int = 0
    cache_read_tokens: int = 0
    cache_create_tokens: int = 0
    total_tokens: int = 0
    messages: int = 0


class GlobalUsageCounters(BaseModel):
    """Счётчики на основе ВСЕХ сессий Claude Code (парсинг JSONL).

    Формат как на дашборде Anthropic:
    - Текущая сессия (5ч окно) — "Сброс через X ч Y мин"
    - Все модели (недельный) — "Сброс в четверг в 20:00"
    - Только Sonnet (недельный) — отдельно
    """

    # 5ч скользящее окно ("Текущая сессия" на дашборде)
    session_5h_output_tokens: int = 0
    session_5h_input_tokens: int = 0
    session_5h_cache_read_tokens: int = 0
    session_5h_cache_create_tokens: int = 0
    session_5h_total_tokens: int = 0
    session_5h_messages: int = 0
    session_5h_percent: float = 0.0
    session_5h_limit: int = 0
    session_5h_resets_in_sec: int = 0
    session_5h_resets_in_text: str = ""

    # Недельный — все модели
    weekly_all_output_tokens: int = 0
    weekly_all_input_tokens: int = 0
    weekly_all_total_tokens: int = 0
    weekly_all_messages: int = 0
    weekly_all_percent: float = 0.0
    weekly_all_limit: int = 0
    weekly_resets_at: str = ""
    weekly_resets_in_sec: int = 0

    # Недельный — по моделям (sonnet, opus, haiku)
    weekly_by_model: dict[str, dict] = {}

    # Метаданные сканирования
    scanned_files: int = 0
    scanned_messages: int = 0
    scan_duration_ms: int = 0
    last_scan_at: str = ""
    covers_all_usage: bool = False  # True = видим только Claude Code


class CLIResult(BaseModel):
    """Результат парсинга JSON-вывода Claude CLI."""

    result_text: str = ""
    is_error: bool = False
    cost_usd: float = 0.0
    duration_ms: int = 0
    duration_api_ms: int = 0
    num_turns: int = 0
    session_id: Optional[str] = None
