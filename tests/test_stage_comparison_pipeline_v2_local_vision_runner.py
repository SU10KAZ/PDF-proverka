# -*- coding: utf-8 -*-
"""Тесты адаптера local vision runner (контракт, без сети/моделей).

Транспорт (compare_images_local) мокается — реальный endpoint не дёргается.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional

import pytest

from backend.app.services.stage_comparison import (
    pipeline_v2_local_vision_runner as lvr,
)


@dataclass
class _FakeCompareResult:
    status: str = "done"
    model: str = "qwen/test"
    model_used: str = "qwen/test"
    parsed: Optional[dict] = None
    raw_response_excerpt: str = ""
    duration_sec: float = 1.5
    error: Optional[str] = None
    differences: list = field(default_factory=list)


@pytest.fixture()
def patched(monkeypatch):
    """Подменить транспорт и конфиг graphic_llm_local."""
    from backend.app.services.stage_comparison import graphic_llm_local as gl

    state: dict[str, Any] = {"result": _FakeCompareResult(), "calls": []}

    async def _fake_compare(left, right, prompt=None, *, model=None,
                            cfg=None, **kw):
        state["calls"].append({"left": left, "right": right,
                               "prompt": prompt, "model": model})
        return state["result"]

    class _Cfg:
        max_tokens = 5500
        timeout_sec = 300

    monkeypatch.setattr(gl, "compare_images_local", _fake_compare)
    monkeypatch.setattr(gl, "load_local_graphic_llm_config", lambda: _Cfg())
    # dataclasses.replace не работает с _Cfg-классом — отключаем override path
    return state


def _runner(**kw):
    return lvr.build_local_vision_runner(**kw)


def test_requires_both_images(patched):
    runner = _runner()
    with pytest.raises(ValueError):
        runner("p", None, "/r.png", {})
    with pytest.raises(ValueError):
        runner("p", "/l.png", None, {})
    assert patched["calls"] == []   # транспорт не дёргался


def test_old_new_order_preserved(patched):
    runner = _runner()
    patched["result"] = _FakeCompareResult(parsed={
        "old_description": "OLD", "new_description": "NEW",
        "confidence": "high"})
    out = runner("PROMPT", "/old.png", "/new.png", {})
    call = patched["calls"][0]
    # OLD строго первым аргументом, NEW вторым — без перестановок
    assert call["left"] == "/old.png"
    assert call["right"] == "/new.png"
    assert call["prompt"] == "PROMPT"
    assert out["old_description"] == "OLD"
    assert out["new_description"] == "NEW"
    assert out["model_used"] == "qwen/test"
    assert out["duration_sec"] == 1.5


def test_transport_error_raises_runtime_error(patched):
    runner = _runner()
    for status in ("error", "timeout", "provider_unavailable"):
        patched["result"] = _FakeCompareResult(status=status, error="boom")
        with pytest.raises(RuntimeError):
            runner("p", "/l.png", "/r.png", {})


def test_unparsed_response_falls_back_to_salvage_and_raw_text(patched):
    runner = _runner()
    raw = 'мусор до {"old_description": "A", "new_description": "B"} хвост'
    patched["result"] = _FakeCompareResult(status="invalid_json", parsed=None,
                                           raw_response_excerpt=raw)
    out = runner("p", "/l.png", "/r.png", {})
    # salvage достал JSON из мусора; raw_text сохранён для диагностики
    assert out.get("old_description") == "A"
    assert out["raw_text"] == raw


def test_totally_unparsable_keeps_raw_text_only(patched):
    runner = _runner()
    patched["result"] = _FakeCompareResult(status="invalid_json", parsed=None,
                                           raw_response_excerpt="no json here")
    out = runner("p", "/l.png", "/r.png", {})
    assert out["raw_text"] == "no json here"
    assert "old_description" not in out
    # gv-слой пометит такой item failed (нет описаний) — отчёт не падает
    from backend.app.services.stage_comparison.pipeline_v2_graphic_vision_enrichment import (
        normalize_vision_runner_result,
    )
    result, _ = normalize_vision_runner_result(out)
    assert result is None


def test_result_wrapper_unwrapped(patched):
    runner = _runner()
    patched["result"] = _FakeCompareResult(parsed={
        "result": {"old_description": "X", "new_description": "Y"}})
    out = runner("p", "/l.png", "/r.png", {})
    assert out["old_description"] == "X"


def test_no_forbidden_imports():
    from pathlib import Path
    src = Path(lvr.__file__).read_text(encoding="utf-8").lower()
    for forbidden in ("claude", "opus", "text_llm_provider", "subprocess"):
        assert forbidden not in src, f"adapter references {forbidden!r}"
