"""Каналы проверки и статус пары листов (план §6, §6.1).

«Совпадает» выдаётся, только если ВСЕ применимые каналы в состоянии
``checked``. Нереализованный или невыполненный канал — ``unchecked`` с
причиной, а не ``not_applicable``.
"""
from __future__ import annotations

from typing import Any

CHANNELS = ("geometry", "color", "text", "text_coverage", "zones", "annotations", "warnings", "spec_positions")
MAIN_CHANNELS = ("geometry", "color", "text")
SERVICE_CHANNELS = ("zones", "annotations", "warnings")

# Причины, при которых итог — «Нужна проверка» независимо от числа отличий.
FORCING_REASONS = {
    "MATCH_LOW_CONFIDENCE", "MATCH_AMBIGUOUS", "MATCH_UNRESOLVED", "ALIGNMENT_UNRELIABLE", "ALIGNMENT_PARTIAL",
    "PARTIAL_COVERAGE", "DUPLICATE_SHEET", "RENDER_FAILED", "NO_TEXT_LAYER", "TEXT_UNREADABLE_IN_DIFF_AREA",
    "STAMP_NOT_RECOGNIZED", "SPEC_NOT_PARSED",
}

STATUS_LABELS = {
    "same": "Совпадает",
    "same_zones_changed": "Совпадает, изменены служебные зоны",
    "different": "Есть отличия",
    "added": "Лист добавлен",
    "removed": "Лист удалён",
    "unmatched": "Не сопоставлен",
    "needs_review": "Нужна проверка",
}


def checked(diffs: int = 0, **extra: Any) -> dict[str, Any]:
    return {"state": "checked", "diffs": int(diffs), **extra}


def unchecked(reason: str, **extra: Any) -> dict[str, Any]:
    return {"state": "unchecked", "reason": reason, **extra}


def not_applicable(reason: str) -> dict[str, Any]:
    return {"state": "not_applicable", "reason": reason}


def decide(channels: dict[str, dict[str, Any]], reasons: list[str]) -> tuple[str, list[str]]:
    """Итоговый статус и причины по каналам и принудительным причинам."""
    missing = [c for c in CHANNELS if c not in channels]
    if missing:
        # Защита от ошибки кода: канал забыли посчитать — это не «совпадает».
        for name in missing:
            channels[name] = unchecked("канал не вычислен")
    reasons = list(dict.fromkeys(reasons))
    unchecked_names = [n for n in CHANNELS if channels[n]["state"] == "unchecked"]
    for name in unchecked_names:
        reasons.append(f"UNCHECKED_CHANNEL:{name}")
    main_diffs = sum(int(channels[n].get("diffs", 0)) for n in MAIN_CHANNELS if channels[n]["state"] == "checked")
    service_diffs = sum(int(channels[n].get("diffs", 0)) for n in SERVICE_CHANNELS if channels[n]["state"] == "checked")
    if any(r.split(":")[0] in FORCING_REASONS for r in reasons):
        return "needs_review", reasons
    if main_diffs > 0:
        return "different", reasons
    if unchecked_names:
        return "needs_review", reasons
    if service_diffs > 0:
        return "same_zones_changed", reasons
    return "same", reasons
