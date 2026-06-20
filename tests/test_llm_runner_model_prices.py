"""reserc.md #75 — цены моделей в конфиге + наблюдаемость под-учёта.

_MODEL_PRICES грузятся из data/model_prices.json (fallback — встроенные). При
неизвестной модели стоимость 0.0, но теперь с warning (раньше молча → под-учёт
незаметен). (Backoff на APITimeoutError — отдельная правка той же находки,
зеркалит уже протестированную ветку RateLimitError.)
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from backend.app.services.llm import llm_runner as lr


def test_model_prices_loaded_from_config():
    prices = lr._load_model_prices()
    assert "openai/gpt-5.4" in prices
    assert prices["openai/gpt-5.4"]["input"] > 0
    assert prices["openai/gpt-5.4"]["output"] > 0


def test_config_file_parses_and_has_models():
    p = Path(lr.__file__).resolve().parents[2] / "data" / "model_prices.json"
    data = json.loads(p.read_text(encoding="utf-8"))
    assert "openai/gpt-5.4" in data and "anthropic/claude-opus-4-7" in data


def test_estimate_cost_known_model():
    # 1M input @2.5 + 1M output @15 = 17.5 USD
    c = lr._estimate_cost("openai/gpt-5.4", 1_000_000, 1_000_000)
    assert abs(c - 17.5) < 1e-6


def test_estimate_cost_unknown_model_warns_once(caplog):
    lr._WARNED_UNKNOWN_PRICE_MODELS.discard("zzz/unknown-xyz")
    with caplog.at_level(logging.WARNING):
        assert lr._estimate_cost("zzz/unknown-xyz", 1000, 1000) == 0.0
        lr._estimate_cost("zzz/unknown-xyz", 1000, 1000)  # повтор — без нового warning
    warns = [r for r in caplog.records if "zzz/unknown-xyz" in r.getMessage()]
    assert len(warns) == 1


def test_fallback_to_builtin_when_config_missing(monkeypatch):
    # Если файла нет — используем встроенные цены (поведение не ломается).
    import builtins
    real_open = builtins.open

    def _no_file(path, *a, **k):
        if "model_prices.json" in str(path):
            raise FileNotFoundError(path)
        return real_open(path, *a, **k)

    monkeypatch.setattr(builtins, "open", _no_file)
    prices = lr._load_model_prices()
    assert prices == lr._MODEL_PRICES_BUILTIN
