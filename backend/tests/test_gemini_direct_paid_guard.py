"""reserc.md #3/#88/#105/#73 — gemini_direct проходит через paid-API guard +
record_paid + РЕЗЕРВИРОВАНИЕ бюджета.

Раньше платный gemini_direct обходил и kill-switch PAID_API_ENABLED, и учёт
стоимости (paid_cost.json/daily/dashboard). #73: вдобавок gemini_direct звал
только assert_ (без резервации), и конкурентные батчи (Semaphore+gather)
гонкой проскакивали дневной лимит. Теперь путь идёт через reserve_paid_api +
release_reservation в finally. Тесты фиксируют контуры:
  1) при блокировке guard'ом платный вызов НЕ делается и расход НЕ пишется;
  2) при успехе record_paid вызывается с реальным cost/source/токенами;
  3) резервация ДЕРЖИТСЯ во время платного вызова и освобождается после (#73).

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

    # #73: gemini_direct теперь резервирует через reserve_paid_api, не assert_.
    monkeypatch.setattr(pag, "reserve_paid_api", _block)

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
    # #73: путь идёт через reserve_paid_api + release_reservation (finally).
    monkeypatch.setattr(pag, "reserve_paid_api", lambda ctx: None)
    monkeypatch.setattr(pag, "release_reservation", lambda res: None)

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


def test_gemini_direct_holds_reservation_during_call(monkeypatch):
    """#73: на реальном guard резервация ДЕРЖИТСЯ во время платного вызова
    (конкурентные батчи видят её сумму) и освобождается после (finally)."""
    # реальный guard: kill-switch on, лимит задан, потрачено 0, чистый ledger
    monkeypatch.setenv("PAID_API_ENABLED", "true")
    monkeypatch.setenv("PAID_API_DAILY_LIMIT_USD", "10.0")
    monkeypatch.setattr(pag, "_today_spent_usd", lambda: 0.0)
    with pag._reservation_lock:
        pag._reservations.clear()

    import backend.app.pipeline.stages.prepare.prompt_builder as pb
    monkeypatch.setattr(
        pb, "build_block_batch_messages",
        lambda *a, **k: [{"role": "user", "content": "x" * 4000}],
    )

    seen = {"during": 0}

    async def _worker(*a, **k):
        # во время платного вызова резервация активна → конкурент её увидит
        seen["during"] = pag.active_reservation_count()
        return gdr.GeminiBlockBatchResult(
            batch_id=7, model_id="gemini-2.5-flash", is_error=False,
            parsed_data=None, cost_usd=0.01, prompt_tokens=1000, output_tokens=50,
        )

    monkeypatch.setattr(gdr, "run_gemini_direct_block_batch", _worker)
    monkeypatch.setattr(usage_service.paid_cost_tracker, "record_paid", lambda *a, **k: None)

    exit_code, _text, result = asyncio.run(gdr.run_block_batch_gemini_direct(
        _batch(), {"project_id": "OBJ/PROJ"}, "OBJ/PROJ", 1,
        model_id="gemini-2.5-flash"))

    assert exit_code == 0
    assert result.is_error is False
    assert seen["during"] == 1            # резервация была активна во время вызова
    assert pag.active_reservation_count() == 0  # освобождена в finally
