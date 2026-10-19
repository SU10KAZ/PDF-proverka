"""Настройки отчёта по MD-сводке: секция ``md_report`` файла ``ai_settings.json`` и окружение.

Модель по умолчанию — Sol 6.1 (``gpt-6.1-sol``, выбор Андрея Ивановича для этого отчёта).
Подмены модели при отказе нет: ошибка исполнителя — ошибка этапа с причиной.
CLI — закреплённый путь способа «Изменения в листе» (``sc_codex_bin``), если своего нет:
список моделей сервер отдаёт под версию клиента, Sol есть только у 0.162.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from .. import settings as ai_settings

DEFAULT_MODEL = "gpt-6.1-sol"
DEFAULT_EFFORT = "high"
# Версия контракта отчёта и промптов: входит в ключ повторного использования результатов этапов.
PROMPT_VERSION = "mdr-p1"
SCHEMA_VERSION = "mdr-s2"
REPORT_SCHEMA = "report_v2"
MAX_UPLOAD_BYTES = 20 * 1024 * 1024
# Русский текст: ~3,5–4 символа на токен; для запаса считаем 2 символа на токен.
CHARS_PER_TOKEN = 2.0


def _file() -> dict:
    try:
        data = json.loads(ai_settings.switch_file().read_text(encoding="utf-8")).get("md_report")
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, AttributeError):
        return {}


def _get(name: str, env: str, default: str = "") -> str:
    raw = (os.environ.get(env) or "").strip()
    if raw:
        return raw
    value = _file().get(name)
    return str(value).strip() if value not in (None, "") else default


def _int(name: str, env: str, default: int, low: int, high: int) -> int:
    try:
        return max(low, min(high, int(_get(name, env, str(default)))))
    except ValueError:
        return default


def model() -> str:
    return _get("model", "VC_MR_MODEL", DEFAULT_MODEL)


def effort() -> str:
    return _get("effort", "VC_MR_EFFORT", DEFAULT_EFFORT)


def codex_bin() -> str:
    return _get("codex_bin", "VC_MR_CODEX_BIN", "") or ai_settings.sc_codex_bin()


def service_tier() -> str:
    """Тариф скорости Codex: пусто — обычный; ``priority`` («Fast», быстрее, расход лимита выше) или ``ultrafast``."""
    raw = _get("service_tier", "VC_MR_SERVICE_TIER", "")
    return raw if raw in ("priority", "ultrafast") else ""


def parallel() -> int:
    """Одновременных обращений: каждое — отдельная эфемерная сессия codex exec, общего состояния нет."""
    return _int("parallel", "VC_MR_PARALLEL", 4, 1, 32)


def block_chars() -> int:
    """Желаемый объём текста источника в одном задании аналитика (символы)."""
    return _int("block_chars", "VC_MR_BLOCK_CHARS", 70000, 2000, 400000)


def verify_full_chars() -> int:
    """До этого объёма проверяющий получает полный текст своих переходов, больше — выдержки."""
    return _int("verify_full_chars", "VC_MR_VERIFY_FULL_CHARS", 150000, 0, 400000)


def verify_unit() -> str:
    """``transition`` (по умолчанию) — проверка по переходу целиком; ``group`` — по группам аналитика."""
    raw = _get("verify_unit", "VC_MR_VERIFY_UNIT", "transition")
    return raw if raw in ("transition", "group") else "transition"


def call_timeout_s() -> int:
    return _int("call_timeout_s", "VC_MR_CALL_TIMEOUT_S", 2400, 120, 7200)


def retries() -> int:
    """Повторы обращения при ответе не по схеме или временном отказе (квота и вход не повторяются)."""
    return _int("retries", "VC_MR_RETRIES", 1, 0, 3)


def models_cache_path() -> Path:
    home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")
    return home / "models_cache.json"


def model_limits(slug: str | None = None) -> dict:
    """Окно контекста модели из кэша моделей Codex (то, что сервер отдал текущему клиенту).

    Нет файла или модели — ``context_window`` = None, применяется консервативный предел 272 000.
    """
    slug = slug or model()
    out = {"model": slug, "context_window": None, "client_version": None, "listed": False,
           "source": str(models_cache_path())}
    try:
        data = json.loads(models_cache_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return out
    out["client_version"] = data.get("client_version")
    for m in data.get("models") or []:
        if m.get("slug") == slug:
            out.update(listed=True, context_window=m.get("context_window"), display_name=m.get("display_name"),
                       efforts=[x.get("effort") for x in m.get("supported_reasoning_levels") or []])
    return out


def budget(block: int | None = None, slug: str | None = None) -> dict:
    """Предел объёма одного задания: желаемый размер блока и жёсткий предел от окна контекста.

    ``block`` и ``slug`` — из снимка настроек отчёта: продолжение режет файл так же, как первый запуск."""
    lim = model_limits(slug)
    window = int(lim.get("context_window") or 272000)
    # Под входной текст — не больше половины окна: место под инструкции, ответ и рассуждение.
    hard = int(window * 0.5 * CHARS_PER_TOKEN)
    return {"context_window": window, "context_source": lim["source"] if lim.get("context_window") else "по умолчанию",
            "chars_per_token_assumed": CHARS_PER_TOKEN, "block_chars": min(block or block_chars(), hard),
            "hard_chars": hard}


def snapshot() -> dict:
    return {"model": model(), "effort": effort(), "codex_bin": codex_bin() or None, "parallel": parallel(),
            "service_tier": service_tier() or None,
            "block_chars": block_chars(), "verify_full_chars": verify_full_chars(), "verify_unit": verify_unit(),
            "call_timeout_s": call_timeout_s(), "retries": retries(), "prompt_version": PROMPT_VERSION,
            "schema_version": SCHEMA_VERSION}
