"""Флаги Sheet Matcher v4. Читаются в момент вызова, не при импорте.

Sheet Matcher v4 — четыре доказанных исправления одного алгоритма (паспорт
листа из тела Markdown, окно pass-1 = deep top-K, ось фасада/разреза без
предлога «в осях», страж неоднозначности HIGH).  Исследование 2026-09-06
показало на 23 парах объекта 272: UNKNOWN 2137→132, HIGH 53→296, ложных
HIGH 0 — но вопросов 121→383 и «на проверку» 722→2234, потому что
сопоставленных листов стало больше.  Поэтому v4 не включается сам: прод
остаётся на v3 байт-в-байт, пока флаг выключен, а v4 сначала считается
тенью для allowlist-пар и пишется отдельным диагностическим артефактом.

Семантика:

* ``STAGE_COMPARISON_SHEET_MATCHER_V4_ENABLED`` (по умолчанию ``false``) —
  боевой алгоритм.  ``false`` → v3, результат побайтово равен базе;
  ``true`` → замороженная v4 (все четыре исправления вместе, по отдельности
  они не проверялись).
* ``STAGE_COMPARISON_SHEET_MATCHER_V4_SHADOW_ENABLED`` (``false``) — тень:
  после боевого v3 для allowlist-пары считается ещё и v4 и сохраняется
  артефактом ``sheet_matcher_v4_shadow``.  Тень не меняет ни область
  листов, ни вопросы, ни синтез, ни отчёт.  При включённом боевом v4 тень
  не считается — сравнивать не с чем.
* ``..._SHADOW_PAIR_ALLOWLIST`` / ``..._SHADOW_RUN_ALLOWLIST`` — точные
  идентификаторы пар/прогонов через запятую.  Пустые списки означают
  «никто»: одним флагом тень на все пары не включить.

Ни один из режимов не обращается к модели.
"""
from __future__ import annotations

import os

FEATURE_FLAG = "STAGE_COMPARISON_SHEET_MATCHER_V4_ENABLED"
SHADOW_FLAG = "STAGE_COMPARISON_SHEET_MATCHER_V4_SHADOW_ENABLED"
SHADOW_PAIR_ALLOWLIST = "STAGE_COMPARISON_SHEET_MATCHER_V4_SHADOW_PAIR_ALLOWLIST"
SHADOW_RUN_ALLOWLIST = "STAGE_COMPARISON_SHEET_MATCHER_V4_SHADOW_RUN_ALLOWLIST"

ALGORITHM_V3 = "production-sheet-matcher.v3"
ALGORITHM_V4 = "production-sheet-matcher.v4"
ALGORITHMS = (ALGORITHM_V3, ALGORITHM_V4)


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_allowlist(name: str) -> frozenset[str]:
    return frozenset(
        value.strip()
        for value in (os.environ.get(name) or "").split(",")
        if value.strip()
    )


def v4_enabled() -> bool:
    """Боевой алгоритм — v4?  По умолчанию нет."""
    return _env_bool(FEATURE_FLAG, False)


def resolve_algorithm(algorithm: str | None = None) -> str:
    """Явный выбор вызывающего важнее флага; без него — флаг."""
    if algorithm is None:
        return ALGORITHM_V4 if v4_enabled() else ALGORITHM_V3
    if algorithm not in ALGORITHMS:
        raise ValueError("unsupported sheet matcher algorithm")
    return algorithm


def shadow_enabled() -> bool:
    return _env_bool(SHADOW_FLAG, False)


def shadow_pair_allowlist() -> frozenset[str]:
    return _env_allowlist(SHADOW_PAIR_ALLOWLIST)


def shadow_run_allowlist() -> frozenset[str]:
    return _env_allowlist(SHADOW_RUN_ALLOWLIST)


def snapshot() -> dict[str, object]:
    """Слепок флагов для аудитного следа; идентификаторы allowlist не раскрываются."""
    return {
        "algorithm": resolve_algorithm(),
        "v4_enabled": v4_enabled(),
        "shadow_enabled": shadow_enabled(),
        "shadow_pair_allowlist_configured": bool(shadow_pair_allowlist()),
        "shadow_run_allowlist_configured": bool(shadow_run_allowlist()),
    }


__all__ = [
    "ALGORITHMS",
    "ALGORITHM_V3",
    "ALGORITHM_V4",
    "FEATURE_FLAG",
    "SHADOW_FLAG",
    "SHADOW_PAIR_ALLOWLIST",
    "SHADOW_RUN_ALLOWLIST",
    "resolve_algorithm",
    "shadow_enabled",
    "shadow_pair_allowlist",
    "shadow_run_allowlist",
    "snapshot",
    "v4_enabled",
]
