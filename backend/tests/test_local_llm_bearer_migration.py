"""Тесты миграции локальных LLM на bearer/OpenAI-транспорт (новый LM Studio).

Покрывают:
  * URL-нормализатор (обе формы base URL — с /v1 и без);
  * bearer/basic заголовки в трёх точках (llm_runner, gemma_enrich, graphic_llm_local);
  * OpenAI-vision payload gemma при CHANDRA_CHAT_TRANSPORT=openai_completions;
  * fail-soft health-probe при ENABLE_MODEL_LOAD=false (без native /api/v1/models).

Стиль — синхронные тесты с asyncio.run(), как в test_stage_comparison_graphic_local_llm.py.
"""
from __future__ import annotations

import asyncio
import io

import pytest
from PIL import Image

from backend.app.core.config import _normalize_local_base_url


# ─── URL-нормализатор ─────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("https://01.vibe.cloud-ip.cc", "https://01.vibe.cloud-ip.cc"),
        ("https://01.vibe.cloud-ip.cc/", "https://01.vibe.cloud-ip.cc"),
        ("https://01.vibe.cloud-ip.cc/v1", "https://01.vibe.cloud-ip.cc"),
        ("https://01.vibe.cloud-ip.cc/v1/", "https://01.vibe.cloud-ip.cc"),
        ("", ""),
        ("  https://host/v1  ", "https://host"),
    ],
)
def test_normalize_local_base_url(raw, expected):
    assert _normalize_local_base_url(raw) == expected


# ─── llm_runner._build_chandra_headers (bearer|basic) ─────────────────────

def test_build_chandra_headers_bearer(monkeypatch):
    import backend.app.services.llm.llm_runner as runner

    monkeypatch.setattr(runner, "CHANDRA_AUTH_MODE", "bearer")
    monkeypatch.setattr(runner, "CHANDRA_BEARER_TOKEN", "secret-token")
    headers = runner._build_chandra_headers()
    assert headers["Authorization"] == "Bearer secret-token"
    assert "ngrok-skip-browser-warning" not in headers  # новый сервер — не ngrok


def test_build_chandra_headers_basic(monkeypatch):
    import backend.app.services.llm.llm_runner as runner

    monkeypatch.setattr(runner, "CHANDRA_AUTH_MODE", "basic")
    monkeypatch.setattr(runner, "CHANDRA_BASIC_USER", "u")
    monkeypatch.setattr(runner, "CHANDRA_BASIC_PASS", "p")
    headers = runner._build_chandra_headers()
    assert headers["Authorization"].startswith("Basic ")
    assert headers["ngrok-skip-browser-warning"] == "true"


def test_build_chandra_headers_bearer_missing_token_raises(monkeypatch):
    import backend.app.services.llm.llm_runner as runner

    monkeypatch.setattr(runner, "CHANDRA_AUTH_MODE", "bearer")
    monkeypatch.setattr(runner, "CHANDRA_BEARER_TOKEN", "")
    with pytest.raises(RuntimeError):
        runner._build_chandra_headers()


def test_local_auth_missing_bearer(monkeypatch):
    import backend.app.services.llm.llm_runner as runner

    monkeypatch.setattr(runner, "CHANDRA_AUTH_MODE", "bearer")
    monkeypatch.setattr(runner, "CHANDRA_BEARER_TOKEN", "")
    assert runner._local_auth_missing() is not None
    monkeypatch.setattr(runner, "CHANDRA_BEARER_TOKEN", "tok")
    assert runner._local_auth_missing() is None


# ─── gemma_enrich._auth_header (bearer|basic) ─────────────────────────────

def test_gemma_auth_header_bearer(monkeypatch):
    import backend.app.pipeline.stages.gemma_enrichment.gemma_enrich as g

    monkeypatch.setenv("CHANDRA_AUTH_MODE", "bearer")
    monkeypatch.setenv("CHANDRA_BEARER_TOKEN", "tok-abc")
    monkeypatch.delenv("NGROK_AUTH_USER", raising=False)
    monkeypatch.delenv("NGROK_AUTH_PASS", raising=False)
    headers = g._auth_header()  # НЕ должен требовать NGROK_AUTH_* в bearer
    assert headers["Authorization"] == "Bearer tok-abc"
    assert "ngrok-skip-browser-warning" not in headers


def test_gemma_auth_header_basic(monkeypatch):
    import backend.app.pipeline.stages.gemma_enrichment.gemma_enrich as g

    monkeypatch.setenv("CHANDRA_AUTH_MODE", "basic")
    monkeypatch.setenv("NGROK_AUTH_USER", "u")
    monkeypatch.setenv("NGROK_AUTH_PASS", "p")
    headers = g._auth_header()
    assert headers["Authorization"].startswith("Basic ")
    assert headers["ngrok-skip-browser-warning"] == "true"


# ─── graphic_llm_local bearer (config + headers + availability) ───────────

def _graphic_bearer_env(monkeypatch, token="tok"):
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "https://01.vibe.cloud-ip.cc/v1")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "qwen36-27b-mtp")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_AUTH", "bearer")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_ENABLE_MODEL_LOAD", "false")
    if token is None:
        monkeypatch.delenv("STAGE_COMPARISON_GRAPHIC_LLM_BEARER_TOKEN", raising=False)
        monkeypatch.delenv("CHANDRA_BEARER_TOKEN", raising=False)
        monkeypatch.delenv("LMSTUDIO_API_KEY", raising=False)
    else:
        monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BEARER_TOKEN", token)


def test_graphic_config_normalizes_url_and_reads_bearer(monkeypatch):
    import backend.app.services.stage_comparison.graphic_llm_local as g

    _graphic_bearer_env(monkeypatch, token="tok-xyz")
    cfg = g.load_local_graphic_llm_config()
    assert cfg.base_url == "https://01.vibe.cloud-ip.cc"  # /v1 снят нормализатором
    assert cfg.bearer_token == "tok-xyz"
    assert cfg.auth == "bearer"
    assert cfg.auth_configured is True


def test_graphic_build_headers_bearer(monkeypatch):
    import backend.app.services.stage_comparison.graphic_llm_local as g

    _graphic_bearer_env(monkeypatch, token="tok-xyz")
    cfg = g.load_local_graphic_llm_config()
    headers = g._build_headers(cfg)
    assert headers["Authorization"] == "Bearer tok-xyz"
    assert "ngrok-skip-browser-warning" not in headers


def test_graphic_available_bearer_missing_token(monkeypatch):
    import backend.app.services.stage_comparison.graphic_llm_local as g

    _graphic_bearer_env(monkeypatch, token=None)
    cfg = g.load_local_graphic_llm_config()
    ok, reason = g.check_local_graphic_llm_available(cfg)
    assert ok is False
    assert reason == "bearer_auth_credentials_missing"


# ─── gemma OpenAI-vision payload при openai_completions ────────────────────

class _FakeResp:
    def __init__(self):
        self.status_code = 200
        self.text = '{"choices":[{"message":{"content":"{}"},"finish_reason":"stop"}]}'

    def json(self):
        return {"choices": [{"message": {"content": "{}"}, "finish_reason": "stop"}]}


class _CapturingClient:
    def __init__(self):
        self.calls = []

    async def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "json": json})
        return _FakeResp()


def _make_png(tmp_path):
    p = tmp_path / "blk.png"
    Image.new("RGB", (4, 4), (255, 255, 255)).save(p, format="PNG")
    return p


def test_gemma_call_attempt_openai_transport(monkeypatch, tmp_path):
    import backend.app.pipeline.stages.gemma_enrichment.gemma_enrich as g

    monkeypatch.setenv("CHANDRA_CHAT_TRANSPORT", "openai_completions")
    monkeypatch.setenv("CHANDRA_AUTH_MODE", "bearer")
    monkeypatch.setenv("CHANDRA_BEARER_TOKEN", "tok")
    png = _make_png(tmp_path)
    client = _CapturingClient()

    status, data, raw, _elapsed = asyncio.run(
        g._gemma_call_attempt(client, "https://01.vibe.cloud-ip.cc", "hello", png, 1.0, "chandra-ocr-2", 60)
    )
    assert status == 200
    call = client.calls[0]
    assert call["url"].endswith("/v1/chat/completions")
    payload = call["json"]
    assert "messages" in payload and "input" not in payload
    assert payload["max_tokens"] >= 1 and "max_output_tokens" not in payload
    user_content = payload["messages"][-1]["content"]
    types = {part["type"] for part in user_content}
    assert types == {"text", "image_url"}
    assert call["headers"]["Authorization"] == "Bearer tok"


def test_gemma_call_attempt_native_transport_default(monkeypatch, tmp_path):
    import backend.app.pipeline.stages.gemma_enrichment.gemma_enrich as g

    monkeypatch.delenv("CHANDRA_CHAT_TRANSPORT", raising=False)  # default native
    monkeypatch.setenv("CHANDRA_AUTH_MODE", "basic")
    monkeypatch.setenv("NGROK_AUTH_USER", "u")
    monkeypatch.setenv("NGROK_AUTH_PASS", "p")
    png = _make_png(tmp_path)
    client = _CapturingClient()

    asyncio.run(
        g._gemma_call_attempt(client, "https://host", "hello", png, 1.0, "gemma", 60)
    )
    call = client.calls[0]
    assert call["url"].endswith("/api/v1/chat")
    assert "input" in call["json"] and "messages" not in call["json"]


# ─── probe_qwen_health fail-soft при ENABLE_MODEL_LOAD=false ───────────────

def test_probe_qwen_health_skips_native_models_when_load_disabled(monkeypatch):
    import backend.app.services.stage_comparison.graphic_llm_local as g

    _graphic_bearer_env(monkeypatch, token="tok")
    cfg = g.load_local_graphic_llm_config()
    assert cfg.enable_model_load is False

    async def _fake_live(_cfg):
        return {"ok": True, "reason": "ok", "status_code": 200}

    async def _boom(*_a, **_k):
        raise AssertionError("loaded_models_diagnostics НЕ должен вызываться при enable_model_load=false")

    monkeypatch.setattr(g, "_live_completion_probe", _fake_live)
    monkeypatch.setattr(g, "loaded_models_diagnostics", _boom)

    res = asyncio.run(g.probe_qwen_health(cfg, do_live_test=True))
    assert res["ok"] is True
    assert res["details"]["model_load_enabled"] is False
