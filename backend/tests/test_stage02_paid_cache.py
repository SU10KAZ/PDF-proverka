"""Tests for Stage 02 paid response cache.

Назначение cache: в инциденте 2026-05-16 один и тот же блок M31A платился
9-15 раз ($0.32 за вызов), потому что retry Stage 02 дёргал OpenRouter заново.
Cache на (model, block_id, prompt, image) делает retry zero-cost.

Покрытие:
  - cache miss → None
  - cache hit возвращает сохранённый response с from_cache=True, cost_usd=0
  - смена model инвалидирует cache
  - смена image_bytes инвалидирует cache
  - смена system_prompt инвалидирует cache
  - смена enrichment инвалидирует cache (через каноническую сериализацию)
  - STAGE02_PAID_CACHE_ENABLED=false → cache_enabled()=False
  - try_load_cached на повреждённом JSON → None (no crash)
  - try_load_cached на неверной схеме → None
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.app.pipeline.stages.block_analysis import stage02_paid_cache as cache_mod


# ─── Базовые свойства ключа ─────────────────────────────────────────


def _make_key(**overrides) -> str:
    base = dict(
        model="openai/gpt-5.4",
        block_id="block_007_1",
        system_prompt="SYSTEM",
        user_text="USER",
        enrichment={"label": "lighting plan", "page": 4},
        page_text="Сводный план освещения, 1-й этаж",
        image_bytes=b"\x89PNG\r\n\x1a\nFAKE",
    )
    base.update(overrides)
    return cache_mod.compute_cache_key(**base)


def test_same_inputs_produce_same_key():
    k1 = _make_key()
    k2 = _make_key()
    assert k1 == k2
    assert len(k1) == 64  # sha256 hex


def test_different_model_invalidates_key():
    k1 = _make_key()
    k2 = _make_key(model="openai/gpt-4o")
    assert k1 != k2


def test_different_image_invalidates_key():
    k1 = _make_key()
    k2 = _make_key(image_bytes=b"DIFFERENT_PNG_BYTES")
    assert k1 != k2


def test_different_system_prompt_invalidates_key():
    k1 = _make_key()
    k2 = _make_key(system_prompt="OTHER SYSTEM PROMPT")
    assert k1 != k2


def test_enrichment_dict_order_doesnt_change_key():
    """Каноническая сериализация: dict ordering не должен ломать hash."""
    k1 = _make_key(enrichment={"a": 1, "b": 2, "c": 3})
    k2 = _make_key(enrichment={"c": 3, "a": 1, "b": 2})
    assert k1 == k2


def test_different_enrichment_value_invalidates_key():
    k1 = _make_key(enrichment={"label": "lighting"})
    k2 = _make_key(enrichment={"label": "power"})
    assert k1 != k2


# ─── Cache enable flag ───────────────────────────────────────────────


def test_cache_enabled_default_true(monkeypatch):
    monkeypatch.delenv("STAGE02_PAID_CACHE_ENABLED", raising=False)
    assert cache_mod.cache_enabled() is True


def test_cache_disabled_via_env(monkeypatch):
    monkeypatch.setenv("STAGE02_PAID_CACHE_ENABLED", "false")
    assert cache_mod.cache_enabled() is False


def test_cache_enabled_via_env_truthy(monkeypatch):
    monkeypatch.setenv("STAGE02_PAID_CACHE_ENABLED", "yes")
    assert cache_mod.cache_enabled() is True


# ─── try_load / save round-trip ─────────────────────────────────────


def test_save_and_load_round_trip(tmp_path):
    cache_key = _make_key()
    response = {
        "ok": True,
        "raw_content": '{"findings":[{"id":"F-001"}]}',
        "parsed": {"findings": [{"id": "F-001"}]},
        "input_tokens": 40517,
        "output_tokens": 17118,
        "elapsed_ms": 9123,
        "from_cache": False,
    }
    cache_mod.save_to_cache(
        tmp_path,
        cache_key,
        response=response,
        model="openai/gpt-5.4",
        block_id="block_007_1",
        original_cost_usd=0.3227,
        source_job_id="j1",
    )

    loaded = cache_mod.try_load_cached(tmp_path, cache_key)
    assert loaded is not None
    assert loaded["from_cache"] is True
    assert loaded["cost_usd"] == 0.0
    assert loaded["cache_key"] == cache_key
    assert loaded["original_cost_usd"] == 0.3227
    # Содержимое исходного response сохранилось:
    assert loaded["parsed"] == {"findings": [{"id": "F-001"}]}
    assert loaded["input_tokens"] == 40517


def test_load_returns_none_on_cache_miss(tmp_path):
    assert cache_mod.try_load_cached(tmp_path, "deadbeef" * 8) is None


def test_load_returns_none_on_corrupted_json(tmp_path):
    cache_key = "abc" + "1" * 61
    file_path = cache_mod.cache_file_for_key(tmp_path, cache_key)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("not a json {{{", encoding="utf-8")
    assert cache_mod.try_load_cached(tmp_path, cache_key) is None


def test_load_returns_none_on_wrong_schema_version(tmp_path):
    cache_key = "abc" + "2" * 61
    file_path = cache_mod.cache_file_for_key(tmp_path, cache_key)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(
        json.dumps({"schema_version": 999, "response": {"ok": True}}),
        encoding="utf-8",
    )
    assert cache_mod.try_load_cached(tmp_path, cache_key) is None


def test_save_is_atomic_no_tmp_leftover(tmp_path):
    """save_to_cache использует tmp + os.replace — в каталоге не должно остаться .tmp."""
    cache_key = _make_key()
    cache_mod.save_to_cache(
        tmp_path,
        cache_key,
        response={"ok": True, "raw_content": "x"},
        model="m",
        block_id="b",
        original_cost_usd=0.0,
    )
    tmp_files = list((tmp_path / cache_mod.CACHE_DIRNAME).glob("*.tmp"))
    assert tmp_files == []


# ─── Sanity: повторный hit того же блока не платит ──────────────────


def test_double_hit_simulates_retry_savings(tmp_path):
    """Симуляция инцидента M31A: тот же блок отвечает 2 раза.

    Первый раз — save (paid). Второй раз — load (free).
    """
    cache_key = _make_key()

    # 1) Первый успешный ответ (после OpenRouter 2xx):
    paid_response = {
        "ok": True,
        "raw_content": '{"findings":[]}',
        "parsed": {"findings": []},
        "input_tokens": 40517,
        "output_tokens": 17118,
        "from_cache": False,
    }
    cache_mod.save_to_cache(
        tmp_path,
        cache_key,
        response=paid_response,
        model="openai/gpt-5.4",
        block_id="block_007_1",
        original_cost_usd=0.3227,
    )

    # 2) Retry с тем же model/block/prompt/image — должен поднять cached.
    second = cache_mod.try_load_cached(tmp_path, cache_key)
    assert second is not None
    assert second["from_cache"] is True
    assert second["cost_usd"] == 0.0
    # Контентно совпадает с первым ответом:
    assert second["raw_content"] == '{"findings":[]}'
    assert second["input_tokens"] == 40517
