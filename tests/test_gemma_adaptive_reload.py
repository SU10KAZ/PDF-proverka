"""
test_gemma_adaptive_reload.py
-----------------------------
Unit-тесты для двухпроходной схемы Gemma enrichment с adaptive reload.

Покрытие:
  - _adaptive_reload_to_context: unload → load → verify контракт.
  - context_guard: остаётся 2+ инстанса → GemmaAdaptiveReloadError.
  - context_guard: loaded_context_length < requested → GemmaAdaptiveReloadError.
  - skip-reload если уже загружен один инстанс с подходящим ctx (для skip/resume).
  - _preflight_loaded_context: без reload, только warning.
  - _base_result_candidate_reasons: помечает base_context_overflow.
  - _enrich_block_single_pass отдаёт EnrichResult.context_overflow=True
    после исчерпания всех scale-tier'ов с "Context size has been exceeded".

Все тесты НЕ делают сетевых вызовов: lms_service.* мокается, httpx тоже.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.stages.gemma_enrichment import gemma_enrich as ge


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_lms(monkeypatch):
    """Подменяет lms_service полностью на in-memory mock.

    Возвращает SimpleNamespace со счётчиками вызовов и контролем состояния.
    """
    state = SimpleNamespace(
        loaded=[],  # текущий список загруженных инстансов
        unload_calls=[],  # список model_key, для которых вызвали unload_all_for
        load_calls=[],  # список (model_key, context_length)
        invalidate_calls=0,
    )

    def fake_list_loaded():
        return list(state.loaded)

    def fake_unload_all_for(model_key):
        state.unload_calls.append(model_key)
        # Снимаем все инстансы с этим именем
        before = len(state.loaded)
        state.loaded = [
            entry for entry in state.loaded
            if not (
                entry["identifier"] == model_key
                or entry["identifier"].startswith(f"{model_key}:")
            )
        ]
        return before - len(state.loaded)

    def fake_load_model(model_key, *, context_length=16384, **kwargs):
        state.load_calls.append((model_key, int(context_length)))
        identifier = model_key
        if any(
            (entry["identifier"] == model_key or entry["identifier"].startswith(f"{model_key}:"))
            for entry in state.loaded
        ):
            identifier = f"{model_key}:{len(state.loaded) + 1}"
        new_entry = {
            "identifier": identifier,
            "type": "llm",
            "context_length": int(context_length),
        }
        state.loaded.append(new_entry)
        return {
            "identifier": identifier,
            "context_length": int(context_length),
            "model_key": model_key,
        }

    def fake_invalidate():
        state.invalidate_calls += 1

    import backend.app.services.llm.lms_service as lms
    monkeypatch.setattr(lms, "list_loaded", fake_list_loaded)
    monkeypatch.setattr(lms, "unload_all_for", fake_unload_all_for)
    monkeypatch.setattr(lms, "load_model", fake_load_model)
    monkeypatch.setattr(lms, "invalidate_loaded_cache", fake_invalidate)
    return state


# ─── _adaptive_reload_to_context: happy path ─────────────────────────────────


def test_adaptive_reload_base_unloads_and_loads(mock_lms):
    """Base pass: пустой LM Studio → unload(0) → load → verify ровно один инстанс."""
    event = asyncio.run(
        ge._adaptive_reload_to_context(
            model="google/gemma-4-26b-a4b",
            target_context_length=8192,
            phase="base",
        )
    )
    assert event["ok"] is True
    assert event["loaded_context_length"] == 8192
    assert event["identifier"] == "google/gemma-4-26b-a4b"
    assert event["instances_after"] == 1
    assert ("google/gemma-4-26b-a4b", 8192) in mock_lms.load_calls
    # Унлоада не было: список был пустой → unload_calls пуст
    assert mock_lms.unload_calls == []


def test_adaptive_reload_skips_when_already_loaded_with_sufficient_ctx(mock_lms):
    """Если уже загружен один инстанс с ctx >= target → skip reload (skip/resume)."""
    mock_lms.loaded = [{
        "identifier": "google/gemma-4-26b-a4b",
        "type": "llm",
        "context_length": 16000,
    }]
    event = asyncio.run(
        ge._adaptive_reload_to_context(
            model="google/gemma-4-26b-a4b",
            target_context_length=8192,
            phase="base",
        )
    )
    assert event["ok"] is True
    assert event["skipped"] is True
    assert event["loaded_context_length"] == 16000
    assert mock_lms.unload_calls == []
    assert mock_lms.load_calls == []


def test_adaptive_reload_high_detail_switches_after_base(mock_lms):
    """Полный цикл base → high-detail: оба unload+load вызываются."""
    asyncio.run(
        ge._adaptive_reload_to_context(
            model="google/gemma-4-26b-a4b", target_context_length=8192, phase="base",
        )
    )
    asyncio.run(
        ge._adaptive_reload_to_context(
            model="google/gemma-4-26b-a4b", target_context_length=16000, phase="high_detail",
        )
    )
    # base load + high_detail load
    assert mock_lms.load_calls == [
        ("google/gemma-4-26b-a4b", 8192),
        ("google/gemma-4-26b-a4b", 16000),
    ]
    # Перед high_detail должен быть unload (т.к. модель уже была загружена)
    assert "google/gemma-4-26b-a4b" in mock_lms.unload_calls
    # Финально остаётся один инстанс с 16000
    assert len(mock_lms.loaded) == 1
    assert mock_lms.loaded[0]["context_length"] == 16000


# ─── context_guard: ошибки ───────────────────────────────────────────────────


def test_adaptive_reload_raises_when_two_instances_remain(mock_lms, monkeypatch):
    """Если list_loaded после load возвращает 2 инстанса той же модели → ошибка."""
    # Пустой стейт перед reload, но list_loaded после load возвращает 2 инстанса
    call_count = SimpleNamespace(n=0)
    real_list = ge._instances_for_model  # не подменяем helper

    import backend.app.services.llm.lms_service as lms

    def fake_list_loaded_two():
        call_count.n += 1
        # Первый вызов (before reload) → пусто; второй (after reload) → 2 инстанса
        if call_count.n == 1:
            return []
        return [
            {"identifier": "google/gemma-4-26b-a4b", "type": "llm", "context_length": 16000},
            {"identifier": "google/gemma-4-26b-a4b:2", "type": "llm", "context_length": 4096},
        ]
    monkeypatch.setattr(lms, "list_loaded", fake_list_loaded_two)

    with pytest.raises(ge.GemmaAdaptiveReloadError, match="multiple Gemma instances"):
        asyncio.run(
            ge._adaptive_reload_to_context(
                model="google/gemma-4-26b-a4b", target_context_length=16000, phase="high_detail",
            )
        )


def test_adaptive_reload_raises_when_loaded_ctx_too_small(mock_lms, monkeypatch):
    """Если load_model вернул ctx < requested → terminal error."""
    import backend.app.services.llm.lms_service as lms

    def fake_load_small_ctx(model_key, *, context_length=16384, **kwargs):
        mock_lms.load_calls.append((model_key, int(context_length)))
        # Эмулируем: оператор задал лимит и LM Studio loaded только 4096
        mock_lms.loaded = [{
            "identifier": model_key,
            "type": "llm",
            "context_length": 4096,
        }]
        return {"identifier": model_key, "context_length": 4096}
    monkeypatch.setattr(lms, "load_model", fake_load_small_ctx)

    with pytest.raises(ge.GemmaAdaptiveReloadError, match="loaded_context_length=4096"):
        asyncio.run(
            ge._adaptive_reload_to_context(
                model="google/gemma-4-26b-a4b", target_context_length=16000, phase="high_detail",
            )
        )


# ─── _preflight_loaded_context: warning без raise ────────────────────────────


def test_preflight_returns_warning_when_no_instance(mock_lms):
    """Adaptive reload off + ничего не загружено → preflight возвращает warning."""
    event = asyncio.run(
        ge._preflight_loaded_context(
            model="google/gemma-4-26b-a4b",
            required_context_length=8192,
            phase="base",
        )
    )
    assert event["ok"] is False
    assert event["skipped"] is True
    assert "no loaded instance" in (event["error"] or "")


def test_preflight_returns_warning_when_two_instances(mock_lms):
    """Adaptive reload off + 2 инстанса (как сейчас у оператора) → warning."""
    mock_lms.loaded = [
        {"identifier": "google/gemma-4-26b-a4b", "type": "llm", "context_length": 4096},
        {"identifier": "google/gemma-4-26b-a4b:2", "type": "llm", "context_length": 16000},
    ]
    event = asyncio.run(
        ge._preflight_loaded_context(
            model="google/gemma-4-26b-a4b",
            required_context_length=8192,
            phase="base",
        )
    )
    # Первый matching выбирается → его ctx=4096 < 8192 → warning
    assert event["ok"] is False
    assert "instances of google/gemma-4-26b-a4b loaded" in (event["error"] or "") or \
           "loaded_context_length=4096" in (event["error"] or "")
    # preflight не должен делать ни unload, ни load
    assert mock_lms.unload_calls == []
    assert mock_lms.load_calls == []


def test_preflight_ok_when_single_instance_sufficient(mock_lms):
    mock_lms.loaded = [
        {"identifier": "google/gemma-4-26b-a4b", "type": "llm", "context_length": 16000},
    ]
    event = asyncio.run(
        ge._preflight_loaded_context(
            model="google/gemma-4-26b-a4b",
            required_context_length=8192,
            phase="base",
        )
    )
    assert event["ok"] is True
    assert event["loaded_context_length"] == 16000


# ─── context_overflow signal на base pass ────────────────────────────────────


def test_candidate_reasons_marks_context_overflow():
    """Failed base result с context_overflow=True должен получить отдельный reason."""
    result = ge.EnrichResult(
        block_id="b1", page=1, ok=False,
        error="context exceeded",
        context_overflow=True,
    )
    block = {"block_id": "b1", "ocr_label": "", "ocr_text_len": 0, "render_size": [800, 500]}
    reasons = ge._base_result_candidate_reasons(block, result)
    assert "base_context_overflow" in reasons
    assert "base_failed" in reasons


def test_candidate_reasons_no_context_overflow_for_ok_result():
    """Успешный base result не получает context_overflow reason."""
    result = ge.EnrichResult(
        block_id="b1", page=1, ok=True,
        enrichment={"block_type": "схема", "subject": "x" * 80, "notes": "y" * 80},
        context_overflow=False,
    )
    block = {"block_id": "b1", "ocr_label": "", "ocr_text_len": 0, "render_size": [800, 500]}
    reasons = ge._base_result_candidate_reasons(block, result)
    assert "base_context_overflow" not in reasons


# ─── _enrich_block_single_pass: context_overflow при исчерпании tier'ов ─────


def test_single_pass_marks_context_overflow_when_all_tiers_exhausted(monkeypatch, tmp_path):
    """Если на КАЖДОМ scale-tier'е возвращается context_exceeded — result.ok=False
    и result.context_overflow=True."""
    blocks_dir = tmp_path / "blocks_gemma_100"
    blocks_dir.mkdir()
    # минимальный PNG, чтобы Image.open мог его прочитать
    from PIL import Image as _PIL
    png_path = blocks_dir / "block_test.png"
    _PIL.new("RGB", (800, 500), color="white").save(png_path, format="PNG")

    block = {"block_id": "b-ctx", "page": 7, "file": "block_test.png", "ocr_label": ""}
    graph = {"pages": [{"page": 7, "text_blocks": [], "sheet_no_normalized": "AR-1"}]}

    async def fake_call(*args, **kwargs):
        # status=400 + body указывающее на context overflow → каждый scale_idx++
        data = {"error": {"message": "request (4570 tokens) exceeds the available context size (4096 tokens)"}}
        return 400, data, "exceeds the available context", 10
    monkeypatch.setattr(ge, "_gemma_call_attempt", fake_call)

    import httpx
    async def _run():
        async with httpx.AsyncClient() as client:
            return await ge._enrich_block_single_pass(
                client, "http://fake", block, graph, blocks_dir,
                model="google/gemma-4-26b-a4b", timeout=30,
                max_output_tokens=ge.DEFAULT_MAX_OUTPUT_TOKENS,
            )
    result = asyncio.run(_run())
    assert result.ok is False
    assert result.context_overflow is True


def test_single_pass_does_not_mark_context_overflow_on_other_errors(monkeypatch, tmp_path):
    """Если HTTP-ошибка не context overflow (например HTTP 500) — context_overflow=False."""
    blocks_dir = tmp_path / "blocks_gemma_100"
    blocks_dir.mkdir()
    from PIL import Image as _PIL
    png_path = blocks_dir / "block_test.png"
    _PIL.new("RGB", (800, 500), color="white").save(png_path, format="PNG")

    block = {"block_id": "b-500", "page": 7, "file": "block_test.png", "ocr_label": ""}
    graph = {"pages": [{"page": 7, "text_blocks": [], "sheet_no_normalized": "AR-1"}]}

    async def fake_call(*args, **kwargs):
        return 500, {"error": {"message": "internal error"}}, "internal error", 10
    monkeypatch.setattr(ge, "_gemma_call_attempt", fake_call)

    import httpx
    async def _run():
        async with httpx.AsyncClient() as client:
            return await ge._enrich_block_single_pass(
                client, "http://fake", block, graph, blocks_dir,
                model="google/gemma-4-26b-a4b", timeout=30,
                max_output_tokens=ge.DEFAULT_MAX_OUTPUT_TOKENS,
            )
    result = asyncio.run(_run())
    assert result.ok is False
    assert result.context_overflow is False


# ─── _resolve_*_context_length: env → config → fallback ──────────────────────


def test_resolve_context_lengths_pick_up_env(monkeypatch):
    """env GEMMA_BASE_CONTEXT_LENGTH должен реально влиять на _resolve_base_context_length.
    Это и есть тот самый «мёртвый env-bypass», который мы чинили."""
    # Сбросим кеш — но config — top-level constants, нужно перечитать модуль.
    monkeypatch.setenv("GEMMA_BASE_CONTEXT_LENGTH", "12345")
    monkeypatch.setenv("GEMMA_HIGH_DETAIL_CONTEXT_LENGTH", "23456")
    import importlib, backend.app.core.config as cfg
    importlib.reload(cfg)
    importlib.reload(ge)
    assert ge._resolve_base_context_length() == 12345
    assert ge._resolve_high_detail_context_length() == 23456
    # restore default for downstream tests
    monkeypatch.delenv("GEMMA_BASE_CONTEXT_LENGTH", raising=False)
    monkeypatch.delenv("GEMMA_HIGH_DETAIL_CONTEXT_LENGTH", raising=False)
    importlib.reload(cfg)
    importlib.reload(ge)
