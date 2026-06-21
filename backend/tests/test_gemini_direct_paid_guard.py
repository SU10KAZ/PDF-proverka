"""reserc.md #3/#88/#105 — gemini_direct проходит через paid-API guard + record_paid.

Раньше платный gemini_direct обходил и kill-switch PAID_API_ENABLED, и учёт
стоимости (paid_cost.json/daily/dashboard). Тесты фиксируют оба контура:
  1) при блокировке guard'ом платный вызов НЕ делается и расход НЕ пишется;
  2) при успехе record_paid вызывается с реальным cost/source/токенами.

Без сети: worker, prompt_builder, guard и paid_cost_tracker замоканы.
"""
from __future__ import annotations

import asyncio

import pytest

from backend.app.services.llm import gemini_direct_runner as gdr
from backend.app.services.llm import paid_api_guard as pag
from backend.app.services.common import usage_service


def _batch():
    return {"batch_id": 7, "blocks": [{"block_id": "b1"}]}


def test_gemini_direct_blocked_by_killswitch(monkeypatch):
    def _block(ctx):
        raise pag.PaidApiBlockedError("PAID_API_ENABLED=false", ctx)

    monkeypatch.setattr(pag, "assert_paid_api_allowed", _block)

    worker_called = {"v": False}

    async def _worker(*a, **k):
        worker_called["v"] = True
        return gdr.GeminiBlockBatchResult(is_error=False, cost_usd=1.0)

    monkeypatch.setattr(gdr, "run_gemini_direct_block_batch", _worker)

    rec = {"v": False}
    monkeypatch.setattr(
        usage_service.paid_cost_tracker, "record_paid",
        lambda *a, **k: rec.__setitem__("v", True),
    )

    exit_code, text, result = asyncio.run(gdr.run_block_batch_gemini_direct(
        _batch(), {"project_id": "p"}, "p", 1, model_id="gemini-2.5-flash"))

    assert exit_code == 1
    assert result.is_error is True
    assert "paid_api_blocked" in text
    assert worker_called["v"] is False   # платный вызов НЕ сделан
    assert rec["v"] is False             # расход НЕ записан


def test_gemini_direct_records_paid_on_success(monkeypatch):
    monkeypatch.setattr(pag, "assert_paid_api_allowed", lambda ctx: None)

    import backend.app.pipeline.stages.prepare.prompt_builder as pb
    monkeypatch.setattr(pb, "build_block_batch_messages", lambda *a, **k: [])

    async def _worker(*a, **k):
        return gdr.GeminiBlockBatchResult(
            batch_id=7, model_id="gemini-2.5-flash", is_error=False,
            parsed_data=None, cost_usd=0.0123,
            prompt_tokens=100, output_tokens=50,
        )

    monkeypatch.setattr(gdr, "run_gemini_direct_block_batch", _worker)

    captured: dict = {}

    def _rec(cost, **k):
        captured["cost"] = cost
        captured.update(k)

    monkeypatch.setattr(usage_service.paid_cost_tracker, "record_paid", _rec)

    exit_code, text, result = asyncio.run(gdr.run_block_batch_gemini_direct(
        _batch(), {"project_id": "p"}, "p", 1, model_id="gemini-2.5-flash"))

    assert exit_code == 0
    assert result.is_error is False
    assert captured["cost"] == pytest.approx(0.0123)
    assert captured["source"] == "gemini_direct"
    assert captured["project_id"] == "p"
    assert captured["input_tokens"] == 100
    assert captured["output_tokens"] == 50
