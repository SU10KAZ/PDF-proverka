"""reserc.md #48 — не отдавать из кеша непригодные блоки при включённом retry.

Кеш-write теперь хранит usable_for_diff; cache-read при usable_for_diff=False И
включённом problem-block retry байпасит кеш (перегенерация). Старые записи без
поля и retry-off → поведение не меняется.
"""
from __future__ import annotations

from backend.app.services.stage_comparison.md_image_enrichment import (
    _cache_unusable_for_retry,
)


def test_unusable_and_retry_enabled_bypasses():
    assert _cache_unusable_for_retry({"usable_for_diff": False}, True) is True


def test_retry_disabled_never_bypasses():
    # При выключенном retry поведение не меняется (backward-safe).
    assert _cache_unusable_for_retry({"usable_for_diff": False}, False) is False


def test_usable_true_not_bypassed():
    assert _cache_unusable_for_retry({"usable_for_diff": True}, True) is False


def test_missing_field_not_bypassed():
    # Старые кеш-записи без поля → не бастим массово.
    assert _cache_unusable_for_retry({"status": "done"}, True) is False


def test_none_cache_not_bypassed():
    assert _cache_unusable_for_retry(None, True) is False
