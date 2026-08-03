"""reserc.md #26 — юнит-тесты платного кеша Stage 02 + адаптера findings.

Покрывает то, что было без тестов: cache hit → from_cache + zero cost, miss → None,
save→load round-trip, инвалидация по prompt/image, защита от битого/чужого кеша,
adapt_findings_to_production (G-NNN нумерация, мердж recommendation).
Интеграционные части (#26: paid_api_blocked не кешируется, billable-vs-cached в
summary) требуют мока сети — остаются для отдельного интеграционного теста.
"""
from __future__ import annotations

from backend.app.pipeline.stages.block_analysis import stage02_paid_cache as cache
from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
    adapt_findings_to_production,
)


def _key(**over):
    base = dict(
        model="openai/gpt-5.4", block_id="b1", system_prompt="sys",
        user_text="find issues", enrichment={"a": 1}, page_text="page",
        image_identity="block_id=b1|page=1|crop_px=[0, 0, 10, 10]",
    )
    base.update(over)
    return cache.compute_cache_key(**base)


def test_cache_key_deterministic_and_order_independent():
    k1 = _key(enrichment={"a": 1, "b": 2})
    k2 = _key(enrichment={"b": 2, "a": 1})  # другой порядок ключей
    assert k1 == k2 and len(k1) == 64


def test_cache_key_changes_on_prompt_or_image():
    base = _key()
    assert _key(user_text="other") != base       # сменился prompt → новый ключ
    # сменилась картинка (другие координаты кропа) → новый ключ
    assert _key(image_identity="block_id=b1|page=1|crop_px=[9, 9, 99, 99]") != base


def test_save_then_load_roundtrip_marks_cache_hit(tmp_path):
    key = _key()
    resp = {"ok": True, "raw_content": "x", "parsed": {"findings": []},
            "input_tokens": 100, "output_tokens": 50, "elapsed_ms": 1200}
    cache.save_to_cache(tmp_path, key, response=resp, model="openai/gpt-5.4",
                        block_id="b1", original_cost_usd=0.3227, source_job_id="job1")
    loaded = cache.try_load_cached(tmp_path, key)
    assert loaded is not None
    assert loaded["from_cache"] is True
    assert loaded["cost_usd"] == 0.0            # cache hit = zero billable
    assert loaded["original_cost_usd"] == 0.3227
    assert loaded["input_tokens"] == 100        # полезная нагрузка сохранена


def test_load_miss_returns_none(tmp_path):
    assert cache.try_load_cached(tmp_path, _key()) is None


def test_load_ignores_corrupt_and_foreign_schema(tmp_path):
    key = _key()
    p = cache.cache_file_for_key(tmp_path, key)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{ broken json", encoding="utf-8")
    assert cache.try_load_cached(tmp_path, key) is None     # не бросает
    p.write_text('{"schema_version": 999, "response": {}}', encoding="utf-8")
    assert cache.try_load_cached(tmp_path, key) is None     # чужая схема


def test_cache_enabled_env(monkeypatch):
    monkeypatch.delenv("STAGE02_PAID_CACHE_ENABLED", raising=False)
    assert cache.cache_enabled() is True                    # дефолт
    monkeypatch.setenv("STAGE02_PAID_CACHE_ENABLED", "false")
    assert cache.cache_enabled() is False
    monkeypatch.setenv("STAGE02_PAID_CACHE_ENABLED", "on")
    assert cache.cache_enabled() is True


def test_adapt_findings_g_numbering_and_recommendation_merge():
    counter = [0]
    raw = [
        {"finding": "Нет заземления", "recommendation": "Добавить PE", "severity": "КРИТИЧЕСКОЕ"},
        {"finding": "Сечение мало"},
    ]
    out = adapt_findings_to_production(raw, "block_007_1", counter)
    assert [f["id"] for f in out] == ["G-001", "G-002"]     # сквозная G-нумерация
    assert out[0]["block_evidence"] == "block_007_1"
    assert "Рекомендация: Добавить PE" in out[0]["finding"]  # recommendation вмёржена
    assert out[0]["provenance"]["found_by"] == ["gpt_openrouter"]
    assert out[0]["provenance"]["detections"][0]["raw_finding_id"] == "G-001"
    assert counter[0] == 2                                   # счётчик продвинут


def test_adapt_does_not_duplicate_recommendation_already_in_text():
    counter = [5]
    raw = [{"finding": "Проблема. Добавить PE проводник", "recommendation": "добавить PE"}]
    out = adapt_findings_to_production(raw, "b1", counter)
    assert out[0]["id"] == "G-006"
    assert out[0]["finding"].lower().count("добавить pe") == 1  # не задвоено
