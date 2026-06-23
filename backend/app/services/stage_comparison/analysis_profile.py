"""
analysis_profile.py — явные профили анализа Stage Comparison.

Проблема: качество сравнения ГРЩ/плотных однолинейных схем зависит от трёх
env-флагов глубокого графического извлечения. Эталонный прогон шёл с флагами ON
(rich, пофидерное извлечение ГРЩ → больше отличий), массовый прогон — с дефолтными
OFF (fast → меньше отличий). Раньше профиль не фиксировался, и пользователь не
понимал, почему было 38, а стало 15, плюс быстрый прогон мог молча затереть
богатый результат.

Этот модуль вводит ДВА именованных профиля и помечает каждый результат:

* ``default``   — «Быстрый режим»: все три флага OFF (массовый прогон).
* ``rich_grsh`` — «Глубокий ГРЩ»: все три флага ON (выбранные пары/листы).

Кроме env-флагов поддерживается **per-run override** через contextvar: можно
прогнать одну пару в rich-режиме, не включая флаги глобально в ``.env``. Override
видят и flag-reader'ы извлечения (`asyncio.to_thread` копирует контекст), и запись
метаданных результата.

Модуль — leaf (только stdlib), не импортирует другие stage_comparison модули,
чтобы flag-reader'ы могли его импортировать без циклов.
"""
from __future__ import annotations

import contextvars
import os
from datetime import datetime, timezone
from typing import Optional

# ─── Имена env-флагов (единый источник правды) ──────────────────────────────
GRAPHIC_STRUCTURED_FLAG = "STAGE_COMPARISON_GRAPHIC_STRUCTURED_EXTRACTION_ENABLED"
BLOCK_PDF_SOURCE_FLAG = "STAGE_COMPARISON_BLOCK_PDF_SOURCE_ENABLED"
GRSH_FEEDER_FLAG = "STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED"

PROFILE_FLAG_NAMES = (GRAPHIC_STRUCTURED_FLAG, BLOCK_PDF_SOURCE_FLAG, GRSH_FEEDER_FLAG)

# Короткие ключи, под которыми флаги пишутся в метаданные результата.
_SHORT_KEY = {
    GRAPHIC_STRUCTURED_FLAG: "graphic_structured_extraction",
    BLOCK_PDF_SOURCE_FLAG: "block_pdf_source",
    GRSH_FEEDER_FLAG: "grsh_feeder_extraction",
}
_SHORT_KEYS = tuple(_SHORT_KEY.values())

DEFAULT_PROFILE = "default"
RICH_GRSH_PROFILE = "rich_grsh"

# Каноничные профили: short-key → bool.
PROFILES: dict[str, dict] = {
    DEFAULT_PROFILE: {
        "label": "Быстрый режим",
        "flags": {k: False for k in _SHORT_KEYS},
    },
    RICH_GRSH_PROFILE: {
        "label": "Глубокий ГРЩ",
        "flags": {k: True for k in _SHORT_KEYS},
    },
}

# Per-run override: dict {env_flag_name: bool} либо None (= использовать env).
_override: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar(
    "sc_analysis_profile_override", default=None
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# ─── Резолв флагов (override → env) ─────────────────────────────────────────
def flag_enabled(name: str, default: bool = False) -> bool:
    """Значение флага с учётом per-run override (override приоритетнее env)."""
    ov = _override.get()
    if ov is not None and name in ov:
        return bool(ov[name])
    return _env_bool(name, default)


def current_flags() -> dict:
    """Резолвнутые (override-aware) флаги профиля в short-key форме."""
    return {
        "graphic_structured_extraction": flag_enabled(GRAPHIC_STRUCTURED_FLAG),
        "block_pdf_source": flag_enabled(BLOCK_PDF_SOURCE_FLAG),
        "grsh_feeder_extraction": flag_enabled(GRSH_FEEDER_FLAG),
    }


def flags_for_profile(name: str) -> Optional[dict]:
    """env-name → bool для именованного профиля (для установки override). None если профиль неизвестен."""
    prof = PROFILES.get(name)
    if not prof:
        return None
    short = prof["flags"]
    return {env: bool(short[_SHORT_KEY[env]]) for env in PROFILE_FLAG_NAMES}


def classify_profile(flags: dict) -> str:
    """short-key dict → 'default' | 'rich_grsh' | 'custom'."""
    norm = {k: bool((flags or {}).get(k)) for k in _SHORT_KEYS}
    for name in (DEFAULT_PROFILE, RICH_GRSH_PROFILE):
        if norm == PROFILES[name]["flags"]:
            return name
    return "custom"


def profile_label(name: Optional[str]) -> str:
    if name in PROFILES:
        return PROFILES[name]["label"]
    if name == "custom":
        return "Кастомный профиль"
    return "Неизвестен"


def profile_metadata(source: Optional[str] = None, flags: Optional[dict] = None) -> dict:
    """Метаданные профиля для записи в comparison_result.json (плоские поля)."""
    f = current_flags() if flags is None else {k: bool((flags or {}).get(k)) for k in _SHORT_KEYS}
    name = classify_profile(f)
    if source is None:
        if _override.get() is not None:
            source = "ui"
        elif name == DEFAULT_PROFILE:
            source = "default"
        else:
            source = "env"
    return {
        "analysis_profile": name,
        "analysis_profile_label": profile_label(name),
        "profile_flags": f,
        "profile_created_at": _utc_now(),
        "profile_source": source,
    }


# ─── Override как контекст-менеджер ─────────────────────────────────────────
class profile_override:
    """Временно выставить флаги профиля (per-run, без правки .env).

    Использование::

        with profile_override(flags_for_profile("rich_grsh")):
            ... enrichment + comparison ...

    contextvars копируются в ``asyncio.to_thread``, поэтому override виден и
    blocking-сравнению, и async-обогащению в рамках одной задачи.
    """

    def __init__(self, flags: Optional[dict]):
        self._flags = dict(flags) if flags else None
        self._token = None

    def __enter__(self):
        self._token = _override.set(self._flags)
        return self

    def __exit__(self, *exc):
        if self._token is not None:
            _override.reset(self._token)
        return False


def profile_override_for(name: Optional[str]):
    """Контекст-менеджер override по имени профиля. Неизвестное имя/None → no-op
    (НЕ трогает текущий override, чтобы не затереть внешний rich-контекст)."""
    flags = flags_for_profile(name) if name else None
    if flags is None:
        from contextlib import nullcontext
        return nullcontext()
    return profile_override(flags)


# ─── Dense-graphics сигнал (для warning о fast-профиле) ──────────────────────
_DENSE_MARKERS = ("dense_grsh", "dense_scheme", "GRSH_FEEDERS", "GRSH_CORE_SYSTEMS")


def has_dense_graphics(*md_texts: Optional[str]) -> bool:
    """True если в enriched MD есть маркеры плотных однолинейных схем (ГРЩ/ВРУ)."""
    for t in md_texts:
        if t and any(m in t for m in _DENSE_MARKERS):
            return True
    return False


DENSE_DEFAULT_WARNING = (
    "В этой паре есть плотные однолинейные схемы (ГРЩ/ВРУ). Быстрый профиль "
    "может пропустить часть графических отличий. Для эталонной проверки "
    "запустите «Глубокий ГРЩ» (analysis_profile=rich_grsh)."
)
