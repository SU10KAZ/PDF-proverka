"""Unit tests для нового local OpenAI-compatible graphic LLM провайдера.

Покрывает:
  1. load_local_graphic_llm_config() читает все env vars
  2. check_local_graphic_llm_available — корректная диагностика
  3. внешние paid URL'ы (openrouter / openai / anthropic / gemini) блокируются
  4. Basic Auth header формируется только при наличии user+pass
  5. _resize_png_to_long_side уменьшает длинную сторону
  6. compare_images_local payload содержит две картинки
  7. parse_diff_json: чистый JSON / markdown fence / invalid → None
  8. timeout пути / HTTP error
  9. ensure_lmstudio_model_loaded:
       — primary load success
       — primary fail → fallback success
       — load endpoint недоступен → endpoint_available=False, не падаем
  10. single graphic-diff: provider=local_openai_compatible сохраняет entry с
      provider/model/model_used/fallback_used
  11. batch graphic job: provider=local сохраняет model_used/fallback_used
  12. run_paid=false не вызывает local provider
"""
from __future__ import annotations

import asyncio
import base64
import io
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ─── Изолируем COMPARISON_ROOT во временный каталог ───────────────────────

@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_test"))
    (tmp_path / "comparison_test").mkdir(exist_ok=True)
    yield


@pytest.fixture
def _local_env(monkeypatch):
    """Полностью настроенный local provider env.

    Явно изолируем от production .env: PROTECT_MODELS, UNLOAD_AFTER_REQUEST,
    UNLOAD_AFTER_BATCH тоже устанавливаются явно, иначе load_dotenv() в
    config.py подтянет production-значения и тесты станут флапающими.
    """
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "https://test-ngrok.example.com")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "qwen/qwen3.6-35b-a3b")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_FALLBACK_MODEL", "qwen3.6-35b-a3b-mtp")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TEMPERATURE", "0.0")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS", "1800")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_TIMEOUT_SEC", "300")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_IMAGE_LONG_SIDE", "1100")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_AUTH", "basic")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_ENABLE_MODEL_LOAD", "true")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_LOAD_CONTEXT_LENGTH", "16000")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROTECT_MODELS", "chandra-ocr-2")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST", "false")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_BATCH", "false")
    monkeypatch.setenv("NGROK_AUTH_USER", "test_user")
    monkeypatch.setenv("NGROK_AUTH_PASS", "test_pass")


def _png_bytes(width: int = 16, height: int = 16) -> bytes:
    from PIL import Image
    img = Image.new("RGB", (width, height), color=(255, 255, 255))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _write_png(path: Path, width: int = 16, height: int = 16) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_png_bytes(width, height))
    return path


# ─── 1. Config loading ────────────────────────────────────────────────────


def test_load_local_graphic_llm_config_reads_env(_local_env):
    from backend.app.services.stage_comparison import graphic_llm_local as g

    cfg = g.load_local_graphic_llm_config()
    assert cfg.is_active
    assert cfg.provider == "local_openai_compatible"
    assert cfg.base_url == "https://test-ngrok.example.com"
    assert cfg.model == "qwen/qwen3.6-35b-a3b"
    assert cfg.fallback_model == "qwen3.6-35b-a3b-mtp"
    assert cfg.temperature == 0.0
    assert cfg.max_tokens == 1800
    assert cfg.timeout_sec == 300
    assert cfg.image_long_side == 1100
    assert cfg.auth == "basic"
    assert cfg.enable_model_load is True
    assert cfg.load_context_length == 16000
    assert cfg.auth_configured is True


def test_load_local_graphic_llm_config_default_provider(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", raising=False)
    from backend.app.services.stage_comparison import graphic_llm_local as g
    cfg = g.load_local_graphic_llm_config()
    assert cfg.provider == "existing"
    assert not cfg.is_active


# ─── 2-3. External paid URLs blocked ──────────────────────────────────────


@pytest.mark.parametrize("base_url", [
    "https://openrouter.ai/api/v1",
    "https://api.openrouter.ai/v1",
    "https://generativelanguage.googleapis.com",
    "https://api.openai.com/v1",
    "https://openai.com",
    "https://anthropic.com",
    "https://api.anthropic.com/v1",
])
def test_external_paid_urls_blocked(monkeypatch, base_url):
    """Local provider должен явно отвергать URL'ы внешних paid провайдеров."""
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", base_url)
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "x")
    monkeypatch.setenv("NGROK_AUTH_USER", "u")
    monkeypatch.setenv("NGROK_AUTH_PASS", "p")

    cfg = g.load_local_graphic_llm_config()
    ok, reason = g.check_local_graphic_llm_available(cfg)
    assert not ok
    assert reason is not None and "external_paid_host_blocked" in reason


def test_local_provider_unavailable_without_auth(monkeypatch):
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "https://x.ngrok-free.dev")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "qwen/qwen3.6-35b-a3b")
    monkeypatch.delenv("NGROK_AUTH_USER", raising=False)
    monkeypatch.delenv("NGROK_AUTH_PASS", raising=False)
    cfg = g.load_local_graphic_llm_config()
    ok, reason = g.check_local_graphic_llm_available(cfg)
    assert not ok
    assert reason == "basic_auth_credentials_missing"


def test_local_provider_available_when_fully_configured(_local_env):
    from backend.app.services.stage_comparison import graphic_llm_local as g
    cfg = g.load_local_graphic_llm_config()
    ok, reason = g.check_local_graphic_llm_available(cfg)
    assert ok, f"expected available, got reason={reason}"
    assert reason is None


# ─── 4. Basic Auth header ─────────────────────────────────────────────────


def test_build_headers_basic_auth(_local_env):
    from backend.app.services.stage_comparison import graphic_llm_local as g
    cfg = g.load_local_graphic_llm_config()
    hdrs = g._build_headers(cfg)
    assert hdrs.get("Authorization", "").startswith("Basic ")
    encoded = hdrs["Authorization"].split(" ", 1)[1]
    decoded = base64.b64decode(encoded).decode()
    assert decoded == "test_user:test_pass"
    assert hdrs.get("ngrok-skip-browser-warning") == "true"
    assert hdrs.get("Content-Type") == "application/json"


# ─── 5. Image resize ──────────────────────────────────────────────────────


def test_resize_png_to_long_side_downscales(tmp_path):
    from PIL import Image
    from backend.app.services.stage_comparison import graphic_llm_local as g

    p = tmp_path / "big.png"
    Image.new("RGB", (3000, 2000), color=(0, 0, 255)).save(p, format="PNG")
    out = g._resize_png_to_long_side(p, 1100)
    with Image.open(io.BytesIO(out)) as img:
        assert max(img.size) == 1100
        # пропорция сохранена
        w, h = img.size
        assert abs((w / h) - (3000 / 2000)) < 0.01


def test_resize_png_to_long_side_skips_small(tmp_path):
    from PIL import Image
    from backend.app.services.stage_comparison import graphic_llm_local as g

    p = tmp_path / "small.png"
    Image.new("RGB", (300, 200), color=(0, 0, 255)).save(p, format="PNG")
    out = g._resize_png_to_long_side(p, 1100)
    with Image.open(io.BytesIO(out)) as img:
        assert img.size == (300, 200)


# ─── 7. parse_diff_json ───────────────────────────────────────────────────


def test_parse_diff_json_clean():
    from backend.app.services.stage_comparison.graphic_llm_local import parse_diff_json
    s = '{"has_significant_difference": true, "summary": "X", "differences": [], "confidence": 0.9}'
    parsed = parse_diff_json(s)
    assert parsed["has_significant_difference"] is True
    assert parsed["summary"] == "X"


def test_parse_diff_json_markdown_fence():
    from backend.app.services.stage_comparison.graphic_llm_local import parse_diff_json
    s = "```json\n{\"summary\": \"Y\", \"differences\": []}\n```"
    parsed = parse_diff_json(s)
    assert parsed is not None
    assert parsed["summary"] == "Y"


def test_parse_diff_json_invalid():
    from backend.app.services.stage_comparison.graphic_llm_local import parse_diff_json
    assert parse_diff_json("") is None
    assert parse_diff_json("this is not json") is None


# ─── 6+8+9. compare_images_local payload, timeout, JSON status ─────────────


class _MockHTTPResponse:
    def __init__(self, status_code: int, json_payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._json = json_payload
        self.text = text or (json.dumps(json_payload) if json_payload else "")

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json


class _MockAsyncClient:
    """Базовый mock httpx.AsyncClient."""

    def __init__(self, *_, **__):
        self.last_call: dict = {}
        # Можно навязать конкретный response через class attribute
        self._response = getattr(self.__class__, "_response_override", None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, headers=None, json=None):
        self.last_call = {"url": url, "headers": headers, "json": json}
        type(self).last_call_post = self.last_call
        if self._response is not None:
            return self._response
        return _MockHTTPResponse(200, {
            "choices": [{"message": {"content": '{"has_significant_difference": true, "summary": "s", "differences": [{"type": "added", "severity": "low", "description": "d", "evidence": "e"}], "confidence": 0.8}'}}]
        })

    async def get(self, url, headers=None):
        type(self).last_call_get = {"url": url, "headers": headers}
        if self._response is not None:
            return self._response
        return _MockHTTPResponse(200, {"models": []})


def test_compare_images_local_payload_has_two_images(_local_env, tmp_path):
    from backend.app.services.stage_comparison import graphic_llm_local as g

    left = _write_png(tmp_path / "L.png")
    right = _write_png(tmp_path / "R.png")

    with patch.object(g.httpx, "AsyncClient", _MockAsyncClient):
        res = asyncio.run(g.compare_images_local(left, right))

    assert res.status == "done"
    assert res.provider == "local_openai_compatible"
    assert res.model == "qwen/qwen3.6-35b-a3b"
    assert res.has_significant_difference is True
    assert res.differences and res.differences[0]["type"] == "added"
    assert res.confidence == pytest.approx(0.8)

    payload = _MockAsyncClient.last_call_post["json"]
    assert payload["model"] == "qwen/qwen3.6-35b-a3b"
    assert payload["temperature"] == 0.0
    assert payload["max_tokens"] == 1800
    msg = payload["messages"][0]
    content = msg["content"]
    images = [c for c in content if c.get("type") == "image_url"]
    assert len(images) == 2
    assert all(img["image_url"]["url"].startswith("data:image/png;base64,") for img in images)


def test_compare_images_local_invalid_json_status(_local_env, tmp_path):
    from backend.app.services.stage_comparison import graphic_llm_local as g

    left = _write_png(tmp_path / "L.png")
    right = _write_png(tmp_path / "R.png")

    class _BadJsonClient(_MockAsyncClient):
        _response_override = _MockHTTPResponse(200, {
            "choices": [{"message": {"content": "this is not JSON at all"}}]
        })

    with patch.object(g.httpx, "AsyncClient", _BadJsonClient):
        res = asyncio.run(g.compare_images_local(left, right))

    assert res.status == "invalid_json"
    assert res.error == "json_parse_failed"


def test_compare_images_local_timeout(_local_env, tmp_path):
    from backend.app.services.stage_comparison import graphic_llm_local as g

    left = _write_png(tmp_path / "L.png")
    right = _write_png(tmp_path / "R.png")

    class _TimeoutClient:
        def __init__(self, *_, **__):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, *a, **kw):
            raise g.httpx.TimeoutException("simulated timeout")

    with patch.object(g.httpx, "AsyncClient", _TimeoutClient):
        res = asyncio.run(g.compare_images_local(left, right))

    assert res.status == "timeout"
    assert "timeout" in (res.error or "")


def test_compare_images_local_unavailable_when_no_auth(monkeypatch, tmp_path):
    """Если provider не сконфигурирован — status=provider_unavailable, ничего не шлём."""
    from backend.app.services.stage_comparison import graphic_llm_local as g

    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL", "https://x.ngrok-free.dev")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "qwen/qwen3.6-35b-a3b")
    monkeypatch.delenv("NGROK_AUTH_USER", raising=False)
    monkeypatch.delenv("NGROK_AUTH_PASS", raising=False)

    left = _write_png(tmp_path / "L.png")
    right = _write_png(tmp_path / "R.png")
    res = asyncio.run(g.compare_images_local(left, right))
    assert res.status == "provider_unavailable"
    assert "local_graphic_llm_unavailable" in (res.error or "")


# ─── 9. ensure_lmstudio_model_loaded ─────────────────────────────────────


def test_ensure_lmstudio_model_loaded_already_loaded(_local_env):
    """Backward-compat: instance без config — считаем OK, не трогаем."""
    from backend.app.services.stage_comparison import graphic_llm_local as g

    class _AlreadyLoadedClient(_MockAsyncClient):
        async def get(self, url, headers=None):
            return _MockHTTPResponse(200, {
                "models": [
                    {"key": "qwen/qwen3.6-35b-a3b", "loaded_instances": [{"id": "inst-1"}]}
                ]
            })

    with patch.object(g.httpx, "AsyncClient", _AlreadyLoadedClient):
        res = asyncio.run(g.ensure_lmstudio_model_loaded("qwen/qwen3.6-35b-a3b"))

    assert res["ok"] is True
    assert res["model_used"] == "qwen/qwen3.6-35b-a3b"
    assert res["fallback_used"] is False
    assert any(m.startswith("already_loaded") for m in res["messages"])


def test_ensure_lmstudio_model_loaded_already_loaded_with_sufficient_ctx(_local_env):
    """ctx >= desired → already_loaded OK без reload."""
    from backend.app.services.stage_comparison import graphic_llm_local as g

    state = {"posts": 0}

    class _Client(_MockAsyncClient):
        async def get(self, url, headers=None):
            return _MockHTTPResponse(200, {
                "models": [{
                    "key": "qwen/qwen3.6-35b-a3b",
                    "loaded_instances": [{"id": "inst-1", "config": {"context_length": 16000}}],
                }]
            })

        async def post(self, url, headers=None, json=None):
            state["posts"] += 1
            return _MockHTTPResponse(200, {"ok": True})

    with patch.object(g.httpx, "AsyncClient", _Client):
        res = asyncio.run(g.ensure_lmstudio_model_loaded("qwen/qwen3.6-35b-a3b"))

    assert res["ok"] is True
    assert res["actual_ctx"] == 16000
    assert res["desired_ctx"] == 16000
    assert state["posts"] == 0, "should NOT unload/reload when ctx is sufficient"


def test_ensure_lmstudio_model_loaded_low_ctx_triggers_unload_and_reload(_local_env):
    """ctx < desired → unload bad instance + reload + verify ctx=16000."""
    from backend.app.services.stage_comparison import graphic_llm_local as g

    state = {"get_calls": 0, "post_calls": []}

    class _Client(_MockAsyncClient):
        async def get(self, url, headers=None):
            state["get_calls"] += 1
            if state["get_calls"] == 1:
                # first GET: bad ctx instance
                return _MockHTTPResponse(200, {
                    "models": [{
                        "key": "qwen/qwen3.6-35b-a3b",
                        "loaded_instances": [{"id": "inst-bad", "config": {"context_length": 4096}}],
                    }]
                })
            # post-reload verify: good ctx
            return _MockHTTPResponse(200, {
                "models": [{
                    "key": "qwen/qwen3.6-35b-a3b",
                    "loaded_instances": [{"id": "inst-good", "config": {"context_length": 16000}}],
                }]
            })

        async def post(self, url, headers=None, json=None):
            state["post_calls"].append({"url": url, "json": json})
            return _MockHTTPResponse(200, {"ok": True})

    with patch.object(g.httpx, "AsyncClient", _Client):
        res = asyncio.run(g.ensure_lmstudio_model_loaded("qwen/qwen3.6-35b-a3b"))

    assert res["ok"] is True
    assert res["actual_ctx"] == 16000
    assert res["desired_ctx"] == 16000
    urls = [c["url"] for c in state["post_calls"]]
    assert any(u.endswith("/api/v1/models/unload") for u in urls)
    assert any(u.endswith("/api/v1/models/load") for u in urls)
    load_payload = next(c["json"] for c in state["post_calls"] if c["url"].endswith("/load"))
    assert load_payload["context_length"] == 16000
    assert load_payload["model"] == "qwen/qwen3.6-35b-a3b"


def test_ensure_lmstudio_model_loaded_ctx_mismatch_after_reload(_local_env):
    """После reload ctx всё ещё < desired → status=error, reason=context_length_mismatch."""
    from backend.app.services.stage_comparison import graphic_llm_local as g

    class _Client(_MockAsyncClient):
        async def get(self, url, headers=None):
            # каждый раз: ctx=4096 (LM Studio упорно сидит на дефолте)
            return _MockHTTPResponse(200, {
                "models": [{
                    "key": "qwen/qwen3.6-35b-a3b",
                    "loaded_instances": [{"id": "inst-x", "config": {"context_length": 4096}}],
                }]
            })

        async def post(self, url, headers=None, json=None):
            # unload и load оба «успешны», но эффекта нет
            return _MockHTTPResponse(200, {"ok": True})

    with patch.object(g.httpx, "AsyncClient", _Client):
        res = asyncio.run(g.ensure_lmstudio_model_loaded("qwen/qwen3.6-35b-a3b"))

    assert res["ok"] is False
    assert res["status"] == "error"
    assert res["reason"] == "context_length_mismatch"
    assert res["desired_ctx"] == 16000
    assert res["actual_ctx"] == 4096


def test_ensure_lmstudio_protected_model_low_ctx_returns_protected_error(
    monkeypatch, _local_env,
):
    """Если model в protect_models и ctx < desired — НЕ выгружаем,
    отдаём ошибку context_length_mismatch_protected."""
    from backend.app.services.stage_comparison import graphic_llm_local as g

    # Назначим chandra primary через env override
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_MODEL", "chandra-ocr-2")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROTECT_MODELS", "chandra-ocr-2")

    state = {"posts": 0}

    class _Client(_MockAsyncClient):
        async def get(self, url, headers=None):
            return _MockHTTPResponse(200, {
                "models": [{
                    "key": "chandra-ocr-2",
                    "loaded_instances": [{"id": "c-1", "config": {"context_length": 4096}}],
                }]
            })

        async def post(self, url, headers=None, json=None):
            state["posts"] += 1
            return _MockHTTPResponse(200, {"ok": True})

    with patch.object(g.httpx, "AsyncClient", _Client):
        res = asyncio.run(g.ensure_lmstudio_model_loaded("chandra-ocr-2"))

    assert res["ok"] is False
    assert res["status"] == "error"
    assert res["reason"] == "context_length_mismatch_protected"
    assert res["actual_ctx"] == 4096
    assert res["desired_ctx"] == 16000
    assert state["posts"] == 0, "protected model must NOT be unloaded/reloaded"


def test_loaded_models_diagnostics_reports_primary_ctx_ok_false(_local_env):
    """loaded_models_diagnostics: primary qwen ctx=4096 → primary_context_ok=false."""
    from backend.app.services.stage_comparison import graphic_llm_local as g

    class _Client(_MockAsyncClient):
        async def get(self, url, headers=None):
            return _MockHTTPResponse(200, {
                "models": [
                    {"key": "qwen/qwen3.6-35b-a3b",
                     "loaded_instances": [{"id": "q", "config": {"context_length": 4096}}]},
                    {"key": "chandra-ocr-2",
                     "loaded_instances": [{"id": "c", "config": {"context_length": 16000}}]},
                ]
            })

    with patch.object(g.httpx, "AsyncClient", _Client):
        diag = asyncio.run(g.loaded_models_diagnostics())

    assert diag["endpoint_available"] is True
    assert diag["desired_context_length"] == 16000
    assert diag["primary_loaded_ctx"] == 4096
    assert diag["primary_context_ok"] is False
    keys = {e["key"]: e for e in diag["loaded_models"]}
    assert keys["qwen/qwen3.6-35b-a3b"]["ctx_ok"] is False
    assert keys["qwen/qwen3.6-35b-a3b"]["is_primary"] is True
    assert keys["chandra-ocr-2"]["protected"] is True
    assert keys["chandra-ocr-2"]["ctx_ok"] is True


def test_ensure_lmstudio_model_loaded_primary_load_success(_local_env):
    from backend.app.services.stage_comparison import graphic_llm_local as g

    class _Client(_MockAsyncClient):
        async def get(self, url, headers=None):
            # empty loaded list — нужно грузить
            return _MockHTTPResponse(200, {"models": []})

        async def post(self, url, headers=None, json=None):
            # primary load OK
            assert url.endswith("/api/v1/models/load")
            assert json["model"] == "qwen/qwen3.6-35b-a3b"
            assert json["context_length"] == 16000
            return _MockHTTPResponse(200, {"ok": True})

    with patch.object(g.httpx, "AsyncClient", _Client):
        res = asyncio.run(g.ensure_lmstudio_model_loaded("qwen/qwen3.6-35b-a3b"))

    assert res["ok"] is True
    assert res["model_used"] == "qwen/qwen3.6-35b-a3b"
    assert res["fallback_used"] is False


def test_ensure_lmstudio_model_loaded_primary_fail_fallback_success(_local_env):
    from backend.app.services.stage_comparison import graphic_llm_local as g

    state = {"post_calls": []}

    class _Client(_MockAsyncClient):
        async def get(self, url, headers=None):
            return _MockHTTPResponse(200, {"models": [{"key": "other", "loaded_instances": [{"id": "x"}]}]})

        async def post(self, url, headers=None, json=None):
            state["post_calls"].append(json["model"])
            # primary fails, fallback succeeds
            if json["model"] == "qwen/qwen3.6-35b-a3b":
                return _MockHTTPResponse(500, {"error": "OOM"}, text='{"error":"OOM"}')
            return _MockHTTPResponse(200, {"ok": True})

    with patch.object(g.httpx, "AsyncClient", _Client):
        res = asyncio.run(g.ensure_lmstudio_model_loaded("qwen/qwen3.6-35b-a3b"))

    assert res["ok"] is True
    assert res["fallback_used"] is True
    assert res["model_used"] == "qwen3.6-35b-a3b-mtp"
    assert state["post_calls"] == ["qwen/qwen3.6-35b-a3b", "qwen3.6-35b-a3b-mtp"]


def test_ensure_lmstudio_model_loaded_endpoint_unreachable(_local_env):
    from backend.app.services.stage_comparison import graphic_llm_local as g

    class _DownClient(_MockAsyncClient):
        async def get(self, url, headers=None):
            raise g.httpx.ConnectError("connection refused")

        async def post(self, url, headers=None, json=None):
            raise g.httpx.ConnectError("connection refused")

    with patch.object(g.httpx, "AsyncClient", _DownClient):
        res = asyncio.run(g.ensure_lmstudio_model_loaded("qwen/qwen3.6-35b-a3b"))

    # Не падаем: caller сможет попробовать chat completion как есть
    assert res["ok"] is False
    assert res["endpoint_available"] is False


# ─── 10. Single graphic-diff via local provider ──────────────────────────


def _make_session(session_id: str = "sess_local_test") -> Path:
    from backend.app.services.stage_comparison import paths as paths_mod
    sd = paths_mod.session_dir(session_id)
    paths_mod.session_json_path(session_id).write_text(json.dumps({
        "id": session_id,
        "stage_a_path": "/tmp/a",
        "stage_b_path": "/tmp/b",
        "pair_order": ["p1"],
        "warnings": [],
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }, ensure_ascii=False), encoding="utf-8")
    pair = {
        "id": "p1",
        "status": "matched",
        "left":  {"filename": "a.pdf", "pdf_path": "/dev/null/a.pdf", "md_path": "/tmp/a.md"},
        "right": {"filename": "b.pdf", "pdf_path": "/dev/null/b.pdf"},
    }
    paths_mod.pair_json_path(session_id, "p1").write_text(json.dumps(pair, ensure_ascii=False), encoding="utf-8")
    paths_mod.findings_path(session_id).write_text(
        json.dumps({"version": 1, "items": []}), encoding="utf-8",
    )
    return sd


def test_single_graphic_diff_local_provider_saves_entry(_local_env, tmp_path):
    """run_paid=true + provider=local_openai_compatible → entry с provider/model_used."""
    from backend.app.api.routers import stage_comparison as router_mod
    from backend.app.services.stage_comparison import store, graphic_llm_local as g

    _make_session()
    fake_png = _write_png(tmp_path / "x.png")

    # Сбросим кеш модели между тестами
    router_mod._LOCAL_MODEL_LOAD_CACHE.clear()

    async def _run():
        req = router_mod.GraphicDiffRequest(
            left_block_id="lid", right_block_id="rid", run_paid=True,
        )
        return await router_mod.graphic_diff_endpoint("sess_local_test", "p1", req)

    fake_result = g.CompareResult(
        status="done",
        provider="local_openai_compatible",
        model="qwen/qwen3.6-35b-a3b",
        model_used="qwen/qwen3.6-35b-a3b",
        fallback_used=False,
        has_significant_difference=True,
        summary="Added a new label",
        differences=[{"type": "added", "severity": "low", "description": "label", "evidence": "evidence"}],
        confidence=0.9,
        parsed={"has_significant_difference": True, "summary": "Added a new label"},
        raw_response_excerpt="…",
        duration_sec=2.5,
        error=None,
    )

    with patch.object(store, "render_block_crop", lambda *a, **kw: fake_png), \
         patch.object(g, "ensure_lmstudio_model_loaded", AsyncMock(return_value={
             "ok": True, "model_used": "qwen/qwen3.6-35b-a3b", "fallback_used": False,
             "endpoint_available": True, "messages": [],
         })), \
         patch.object(g, "compare_images_local", AsyncMock(return_value=fake_result)):
        resp = asyncio.run(_run())

    assert resp["status"] == "done"
    assert resp["provider"] == "local_openai_compatible"
    assert resp["model"] == "qwen/qwen3.6-35b-a3b"
    assert resp["model_used"] == "qwen/qwen3.6-35b-a3b"
    assert resp["fallback_used"] is False
    entry = resp["entry"]
    assert entry["provider"] == "local_openai_compatible"
    assert entry["model_used"] == "qwen/qwen3.6-35b-a3b"
    assert entry["fallback_used"] is False
    assert entry["summary"] == "Added a new label"
    assert entry["has_significant_difference"] is True


def test_single_graphic_diff_run_paid_false_does_not_call_llm(_local_env, tmp_path):
    """run_paid=false: только crop preview, LLM не вызывается."""
    from backend.app.api.routers import stage_comparison as router_mod
    from backend.app.services.stage_comparison import store, graphic_llm_local as g

    _make_session()
    fake_png = _write_png(tmp_path / "x.png")

    called = {"compare": 0, "load": 0}

    async def _spy_compare(*a, **kw):
        called["compare"] += 1
        raise AssertionError("compare should NOT be called when run_paid=false")

    async def _spy_load(*a, **kw):
        called["load"] += 1
        raise AssertionError("ensure_lmstudio_model_loaded should NOT be called when run_paid=false")

    async def _run():
        req = router_mod.GraphicDiffRequest(
            left_block_id="lid", right_block_id="rid", run_paid=False,
        )
        return await router_mod.graphic_diff_endpoint("sess_local_test", "p1", req)

    with patch.object(store, "render_block_crop", lambda *a, **kw: fake_png), \
         patch.object(g, "compare_images_local", _spy_compare), \
         patch.object(g, "ensure_lmstudio_model_loaded", _spy_load):
        resp = asyncio.run(_run())

    assert resp["status"] == "prepared"
    assert resp.get("provider") == "local_openai_compatible"
    assert called == {"compare": 0, "load": 0}


# ─── 11. Batch graphic job via local provider ────────────────────────────


def test_batch_graphic_job_local_provider_saves_model_used(_local_env, tmp_path):
    """run_job() с local provider — items записывают model_used/fallback_used."""
    from backend.app.services.stage_comparison import jobs, store, graphic_llm_local as g

    _make_session()
    fake_png = _write_png(tmp_path / "x.png")

    # Создаём job вручную в queued
    job = {
        "id": "job_local_1",
        "session_id": "sess_local_test",
        "type": "graphic_llm_batch",
        "scope": "selected",
        "provider": "local_openai_compatible",
        "model": "qwen/qwen3.6-35b-a3b",
        "status": "queued",
        "created_at": "x", "updated_at": "x",
        "items": [
            {"pair_id": "p1", "left_block_id": "lid", "right_block_id": "rid",
             "status": "queued", "error": "", "graphic_diff_id": ""},
        ],
        "warnings": [],
        "progress": {"total": 1, "done": 0, "failed": 0, "skipped": 0},
        "run_paid": True, "confirm_paid": True,
    }
    jobs._write_job("sess_local_test", job)

    fake_result = g.CompareResult(
        status="done",
        provider="local_openai_compatible",
        model="qwen/qwen3.6-35b-a3b",
        model_used="qwen3.6-35b-a3b-mtp",  # эмулируем fallback
        fallback_used=True,
        has_significant_difference=False,
        summary="No differences",
        differences=[],
        confidence=0.95,
        parsed={"has_significant_difference": False, "summary": "No differences"},
        duration_sec=1.2,
    )

    with patch.object(store, "render_block_crop", lambda *a, **kw: fake_png), \
         patch.object(g, "ensure_lmstudio_model_loaded", AsyncMock(return_value={
             "ok": True, "model_used": "qwen3.6-35b-a3b-mtp", "fallback_used": True,
             "endpoint_available": True, "messages": [],
         })), \
         patch.object(g, "compare_images_local", AsyncMock(return_value=fake_result)):
        finished = asyncio.run(jobs.run_job("sess_local_test", "job_local_1", auto_rebuild_findings=False))

    assert finished["status"] == "done"
    assert finished["provider"] == "local_openai_compatible"
    it = finished["items"][0]
    assert it["status"] == "done"
    assert it["model_used"] == "qwen3.6-35b-a3b-mtp"
    assert it["fallback_used"] is True

    # Verify entry persisted
    diffs = store._load_graphic_diffs("sess_local_test", "p1")
    assert len(diffs) == 1
    entry = diffs[0]
    assert entry["status"] == "done"
    assert entry["provider"] == "local_openai_compatible"
    assert entry["model_used"] == "qwen3.6-35b-a3b-mtp"
    assert entry["fallback_used"] is True


# ─── 12. config_info_for_endpoint не возвращает пароли ───────────────────


def test_config_info_endpoint_hides_secrets(_local_env):
    from backend.app.services.stage_comparison import graphic_llm_local as g

    info = g.config_info_for_endpoint()
    # Ни пароль, ни логин не должны утекать
    s = json.dumps(info, ensure_ascii=False)
    assert "test_pass" not in s
    assert "test_user" not in s
    assert info["provider"] == "local_openai_compatible"
    assert info["auth"] == "basic"
    assert info["auth_configured"] is True
    assert info["base_url_present"] is True
    assert info["model"] == "qwen/qwen3.6-35b-a3b"
    assert info["fallback_model"] == "qwen3.6-35b-a3b-mtp"
    assert info["available"] is True
    assert info["reason"] is None
    # Защита LM Studio: дефолты + видимость в endpoint
    assert info["protect_models"] == ["chandra-ocr-2"]
    assert info["unload_after_request"] is False
    assert info["unload_after_batch"] is False


# ─── 13. LM Studio model protection (PROTECT_MODELS / UNLOAD_AFTER_*) ────


def test_protect_models_default_is_chandra(monkeypatch):
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.delenv("STAGE_COMPARISON_GRAPHIC_LLM_PROTECT_MODELS", raising=False)
    cfg = g.load_local_graphic_llm_config()
    assert cfg.protect_models == ["chandra-ocr-2"]


def test_protect_models_env_csv_overrides(monkeypatch):
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv(
        "STAGE_COMPARISON_GRAPHIC_LLM_PROTECT_MODELS",
        "chandra-ocr-2, google/gemma-4-26b-a4b",
    )
    cfg = g.load_local_graphic_llm_config()
    assert cfg.protect_models == ["chandra-ocr-2", "google/gemma-4-26b-a4b"]


def test_unload_after_request_and_batch_env(monkeypatch):
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "local_openai_compatible")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST", "true")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_BATCH", "yes")
    cfg = g.load_local_graphic_llm_config()
    assert cfg.unload_after_request is True
    assert cfg.unload_after_batch is True


class _ScriptedClient:
    """Универсальный mock httpx.AsyncClient, сценарии задаются классом.

    Subclasses переопределяют _get_response(url) и _post_response(url, body).
    Записывают каждый вызов в class attribute `calls`.
    """

    calls: list = []

    def __init__(self, *_, **__):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None):
        type(self).calls.append({"method": "GET", "url": url})
        return self._get_response(url)

    async def post(self, url, headers=None, json=None):
        type(self).calls.append({"method": "POST", "url": url, "body": json})
        return self._post_response(url, json)

    def _get_response(self, url):
        return _MockHTTPResponse(200, {"models": []})

    def _post_response(self, url, body):
        return _MockHTTPResponse(200, {"ok": True})


def test_cleanup_skips_unload_when_disabled(_local_env, monkeypatch):
    """unload_after_request=false → ничего не выгружаем (даже primary)."""
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST", "false")
    cfg = g.load_local_graphic_llm_config()

    # Snapshot: chandra + qwen уже loaded
    pre_snapshot = [
        {"model_key": "chandra-ocr-2", "instance_id": "chandra-ocr-2",
         "config": {"context_length": 16000}},
        {"model_key": "qwen/qwen3.6-35b-a3b", "instance_id": "qwen/qwen3.6-35b-a3b",
         "config": {"context_length": 16000}},
    ]

    class _NoUnloadClient(_ScriptedClient):
        calls = []

        def _get_response(self, url):
            return _MockHTTPResponse(200, {"models": [
                {"key": "chandra-ocr-2", "loaded_instances": [{"id": "chandra-ocr-2", "config": {"context_length": 16000}}]},
                {"key": "qwen/qwen3.6-35b-a3b", "loaded_instances": [{"id": "qwen/qwen3.6-35b-a3b", "config": {"context_length": 16000}}]},
            ]})

    _NoUnloadClient.calls = []
    with patch.object(g.httpx, "AsyncClient", _NoUnloadClient):
        info = asyncio.run(g.cleanup_local_graphic_llm(cfg, pre_snapshot, scope="request"))

    assert info["skipped_unload"] is True
    assert info["unloaded"] == []
    assert "chandra-ocr-2" in info["protected_kept"]
    # Никаких POST unload не было
    posts = [c for c in _NoUnloadClient.calls if c["method"] == "POST"]
    assert all("/unload" not in c["url"] for c in posts)


def test_cleanup_unload_after_request_unloads_qwen_keeps_chandra(_local_env, monkeypatch):
    """unload_after_request=true → выгружаем qwen primary/fallback, оставляем chandra."""
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST", "true")
    cfg = g.load_local_graphic_llm_config()

    pre_snapshot = [
        {"model_key": "chandra-ocr-2", "instance_id": "chandra-ocr-2",
         "config": {"context_length": 16000, "parallel": 2}},
    ]

    state = {"loaded": ["chandra-ocr-2", "qwen/qwen3.6-35b-a3b"]}

    class _Client(_ScriptedClient):
        calls = []

        def _get_response(self, url):
            return _MockHTTPResponse(200, {"models": [
                {"key": k, "loaded_instances": [{"id": k, "config": {"context_length": 16000}}]}
                for k in state["loaded"]
            ]})

        def _post_response(self, url, body):
            if "/unload" in url:
                iid = body.get("instance_id")
                if iid in state["loaded"]:
                    state["loaded"].remove(iid)
                return _MockHTTPResponse(200, {"ok": True})
            if "/load" in url:
                # никто не должен переподгружать chandra — она не пропадала
                model = body.get("model")
                if model not in state["loaded"]:
                    state["loaded"].append(model)
                return _MockHTTPResponse(200, {"ok": True})
            return _MockHTTPResponse(404, {}, text="not found")

    _Client.calls = []
    with patch.object(g.httpx, "AsyncClient", _Client):
        info = asyncio.run(g.cleanup_local_graphic_llm(cfg, pre_snapshot, scope="request"))

    assert "qwen/qwen3.6-35b-a3b" in info["unloaded"]
    assert info["restored"] == []  # chandra не пропадала, восстанавливать не нужно
    assert "chandra-ocr-2" in info["protected_kept"]
    assert state["loaded"] == ["chandra-ocr-2"]


def test_cleanup_restores_chandra_if_lost(_local_env, monkeypatch):
    """Если chandra пропала после load qwen — cleanup её восстанавливает."""
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST", "true")
    cfg = g.load_local_graphic_llm_config()

    # До запроса chandra была loaded
    pre_snapshot = [
        {"model_key": "chandra-ocr-2", "instance_id": "chandra-ocr-2",
         "config": {"context_length": 16000, "parallel": 2, "flash_attention": True,
                     "offload_kv_cache_to_gpu": True, "eval_batch_size": 512}},
    ]
    # Но LM Studio её evicted, сейчас loaded только qwen
    state = {"loaded": ["qwen/qwen3.6-35b-a3b"], "load_history": []}

    class _Client(_ScriptedClient):
        calls = []

        def _get_response(self, url):
            return _MockHTTPResponse(200, {"models": [
                {"key": k, "loaded_instances": [{"id": k, "config": {"context_length": 16000}}]}
                for k in state["loaded"]
            ]})

        def _post_response(self, url, body):
            if "/unload" in url:
                iid = body.get("instance_id")
                if iid in state["loaded"]:
                    state["loaded"].remove(iid)
                return _MockHTTPResponse(200, {"ok": True})
            if "/load" in url:
                model = body.get("model")
                state["load_history"].append({"model": model, "body": dict(body)})
                if model not in state["loaded"]:
                    state["loaded"].append(model)
                return _MockHTTPResponse(200, {"ok": True})
            return _MockHTTPResponse(404, {}, text="not found")

    _Client.calls = []
    with patch.object(g.httpx, "AsyncClient", _Client):
        info = asyncio.run(g.cleanup_local_graphic_llm(cfg, pre_snapshot, scope="request"))

    # Сначала выгрузили qwen, потом обнаружили что chandra пропала и восстановили её
    assert "qwen/qwen3.6-35b-a3b" in info["unloaded"]
    assert "chandra-ocr-2" in info["restored"]
    assert any("restore_verified:chandra-ocr-2" in e for e in info["events"])
    # Восстановление пошло с config из snapshot
    load_calls = [h for h in state["load_history"] if h["model"] == "chandra-ocr-2"]
    assert len(load_calls) == 1
    body = load_calls[0]["body"]
    assert body["context_length"] == 16000
    assert body["eval_batch_size"] == 512
    assert body["flash_attention"] is True
    assert body["offload_kv_cache_to_gpu"] is True


def test_cleanup_never_unloads_protected_model(_local_env, monkeypatch):
    """protect_models не входит в provider-owned — НИКОГДА не выгружается."""
    from backend.app.services.stage_comparison import graphic_llm_local as g
    # PROTECT_MODELS перекрывает primary — даже если оператор поставил qwen в
    # protect-list, его не выгружают.
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST", "true")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROTECT_MODELS",
                        "chandra-ocr-2, qwen/qwen3.6-35b-a3b")
    cfg = g.load_local_graphic_llm_config()
    assert "qwen/qwen3.6-35b-a3b" in cfg.protect_models

    pre_snapshot = [
        {"model_key": "chandra-ocr-2", "instance_id": "chandra-ocr-2", "config": {}},
        {"model_key": "qwen/qwen3.6-35b-a3b", "instance_id": "qwen/qwen3.6-35b-a3b",
         "config": {"context_length": 16000}},
    ]
    state = {"loaded": ["chandra-ocr-2", "qwen/qwen3.6-35b-a3b"]}

    class _Client(_ScriptedClient):
        calls = []

        def _get_response(self, url):
            return _MockHTTPResponse(200, {"models": [
                {"key": k, "loaded_instances": [{"id": k, "config": {}}]}
                for k in state["loaded"]
            ]})

        def _post_response(self, url, body):
            if "/unload" in url:
                iid = body.get("instance_id")
                if iid in state["loaded"]:
                    state["loaded"].remove(iid)
                return _MockHTTPResponse(200, {"ok": True})
            return _MockHTTPResponse(200, {"ok": True})

    _Client.calls = []
    with patch.object(g.httpx, "AsyncClient", _Client):
        info = asyncio.run(g.cleanup_local_graphic_llm(cfg, pre_snapshot, scope="request"))

    # qwen в protect-list → не выгружается
    assert info["unloaded"] == []
    assert "qwen/qwen3.6-35b-a3b" in info["protected_kept"]
    # Никаких POST /unload не было
    posts = [c for c in _Client.calls if c["method"] == "POST"]
    assert posts == [], f"unexpected POSTs: {posts}"


def test_cleanup_warning_when_restore_fails(_local_env, monkeypatch):
    """Если restore чандры падает с http 500 — пишем warning, не падаем."""
    from backend.app.services.stage_comparison import graphic_llm_local as g
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST", "true")
    cfg = g.load_local_graphic_llm_config()
    pre_snapshot = [
        {"model_key": "chandra-ocr-2", "instance_id": "chandra-ocr-2",
         "config": {"context_length": 16000}},
    ]
    state = {"loaded": []}

    class _Client(_ScriptedClient):
        calls = []

        def _get_response(self, url):
            return _MockHTTPResponse(200, {"models": [
                {"key": k, "loaded_instances": [{"id": k, "config": {}}]}
                for k in state["loaded"]
            ]})

        def _post_response(self, url, body):
            if "/load" in url:
                return _MockHTTPResponse(500, {"error": "OOM"}, text='{"error":"OOM"}')
            return _MockHTTPResponse(200, {"ok": True})

    _Client.calls = []
    with patch.object(g.httpx, "AsyncClient", _Client):
        info = asyncio.run(g.cleanup_local_graphic_llm(cfg, pre_snapshot, scope="request"))

    assert info["restored"] == []
    assert any("restore:chandra-ocr-2" in w for w in info["warnings"])


def test_single_graphic_diff_runs_cleanup(_local_env, monkeypatch, tmp_path):
    """run_paid=true с unload_after_request=true вызывает cleanup и возвращает поля."""
    from backend.app.api.routers import stage_comparison as router_mod
    from backend.app.services.stage_comparison import store, graphic_llm_local as g

    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST", "true")
    _make_session()
    fake_png = _write_png(tmp_path / "x.png")
    router_mod._LOCAL_MODEL_LOAD_CACHE.clear()

    fake_result = g.CompareResult(
        status="done", provider="local_openai_compatible",
        model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
        fallback_used=False, has_significant_difference=False,
        summary="No diff", differences=[], confidence=0.9,
        parsed={"summary": "No diff"}, duration_sec=1.0,
    )
    fake_cleanup = {
        "scope": "request",
        "events": ["unload:qwen/qwen3.6-35b-a3b:unloaded"],
        "warnings": [],
        "unloaded": ["qwen/qwen3.6-35b-a3b"],
        "restored": [],
        "protected_kept": ["chandra-ocr-2"],
        "skipped_unload": False,
    }
    fake_snapshot = [{"model_key": "chandra-ocr-2", "instance_id": "chandra-ocr-2", "config": {}}]

    async def _run():
        req = router_mod.GraphicDiffRequest(
            left_block_id="lid", right_block_id="rid", run_paid=True,
        )
        return await router_mod.graphic_diff_endpoint("sess_local_test", "p1", req)

    with patch.object(store, "render_block_crop", lambda *a, **kw: fake_png), \
         patch.object(g, "snapshot_loaded_models", AsyncMock(return_value=fake_snapshot)), \
         patch.object(g, "ensure_lmstudio_model_loaded", AsyncMock(return_value={
             "ok": True, "model_used": "qwen/qwen3.6-35b-a3b", "fallback_used": False,
             "endpoint_available": True, "messages": [],
         })), \
         patch.object(g, "compare_images_local", AsyncMock(return_value=fake_result)), \
         patch.object(g, "cleanup_local_graphic_llm", AsyncMock(return_value=fake_cleanup)):
        resp = asyncio.run(_run())

    assert resp["status"] == "done"
    cu = resp.get("cleanup") or {}
    assert cu["unloaded"] == ["qwen/qwen3.6-35b-a3b"]
    assert cu["protected_kept"] == ["chandra-ocr-2"]
    assert cu["warnings"] == []
    # Кеш должен быть очищен после unload — следующий call повторно загрузит модель
    assert router_mod._LOCAL_MODEL_LOAD_CACHE == {}


def test_batch_graphic_job_runs_cleanup_with_warnings(_local_env, monkeypatch, tmp_path):
    """Batch job вызывает cleanup в финализации; warnings уходят в job.warnings."""
    from backend.app.services.stage_comparison import jobs, store, graphic_llm_local as g

    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_BATCH", "true")
    _make_session()
    fake_png = _write_png(tmp_path / "x.png")

    job = {
        "id": "job_local_cleanup",
        "session_id": "sess_local_test",
        "type": "graphic_llm_batch",
        "scope": "selected",
        "provider": "local_openai_compatible",
        "model": "qwen/qwen3.6-35b-a3b",
        "status": "queued",
        "created_at": "x", "updated_at": "x",
        "items": [
            {"pair_id": "p1", "left_block_id": "lid", "right_block_id": "rid",
             "status": "queued", "error": "", "graphic_diff_id": ""},
        ],
        "warnings": [],
        "progress": {"total": 1, "done": 0, "failed": 0, "skipped": 0},
        "run_paid": True, "confirm_paid": True,
    }
    jobs._write_job("sess_local_test", job)

    fake_result = g.CompareResult(
        status="done", provider="local_openai_compatible",
        model="qwen/qwen3.6-35b-a3b", model_used="qwen/qwen3.6-35b-a3b",
        fallback_used=False, has_significant_difference=False,
        summary="No diff", differences=[], confidence=0.9,
        parsed={"summary": "No diff"}, duration_sec=1.0,
    )
    fake_cleanup = {
        "scope": "batch",
        "events": ["unload:qwen/qwen3.6-35b-a3b:unloaded", "restore:chandra-ocr-2:http_500"],
        "warnings": ["restore:chandra-ocr-2:http_500:OOM"],
        "unloaded": ["qwen/qwen3.6-35b-a3b"],
        "restored": [],
        "protected_kept": [],
        "skipped_unload": False,
    }

    with patch.object(store, "render_block_crop", lambda *a, **kw: fake_png), \
         patch.object(g, "snapshot_loaded_models", AsyncMock(return_value=[])), \
         patch.object(g, "ensure_lmstudio_model_loaded", AsyncMock(return_value={
             "ok": True, "model_used": "qwen/qwen3.6-35b-a3b", "fallback_used": False,
             "endpoint_available": True, "messages": [],
         })), \
         patch.object(g, "compare_images_local", AsyncMock(return_value=fake_result)), \
         patch.object(g, "cleanup_local_graphic_llm", AsyncMock(return_value=fake_cleanup)):
        finished = asyncio.run(jobs.run_job(
            "sess_local_test", "job_local_cleanup", auto_rebuild_findings=False,
        ))

    assert finished["status"] == "done"
    cu = finished.get("cleanup") or {}
    assert cu["unloaded"] == ["qwen/qwen3.6-35b-a3b"]
    assert cu["warnings"] == ["restore:chandra-ocr-2:http_500:OOM"]
    # Warnings также проброшены в job.warnings (с prefix cleanup:)
    assert any(w.startswith("cleanup:") for w in finished.get("warnings") or [])
