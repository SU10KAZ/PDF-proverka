"""Ограниченные повторы GPT/OpenRouter до остановки Stage 01.

Инцидент 26.08.2026: пять проектов упали после одного транспортного
исключения httpx. Прежний код сохранял только ``str(exc)``, который у
ReadError/ReadTimeout может быть пустым, и не делал ни одного повтора.
"""
from __future__ import annotations

import httpx
import pytest

from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo


class _Response:
    def __init__(self, status_code: int, *, headers: dict | None = None):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = ""


@pytest.fixture
def no_retry_sleep(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(delay: float):
        sleeps.append(delay)

    monkeypatch.setattr(gfo.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(gfo.random, "uniform", lambda _a, _b: 0.0)
    monkeypatch.setenv("STAGE01_OPENROUTER_TRANSIENT_RETRIES", "2")
    monkeypatch.setenv("STAGE01_OPENROUTER_RETRY_BASE_DELAY_S", "2")
    return sleeps


@pytest.mark.asyncio
async def test_transport_error_is_retried_and_keeps_exception_type(no_retry_sleep):
    class Client:
        def __init__(self):
            self.calls = 0

        async def post(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                # Именно такой пустой str(exc) попал в боевые артефакты.
                raise httpx.ReadError("")
            return _Response(200)

    client = Client()
    response, meta = await gfo._post_openrouter_with_transient_retry(
        client,
        headers={},
        payload={},
        timeout=30,
        label="DOC:block",
    )

    assert response is not None and response.status_code == 200
    assert client.calls == 2
    assert meta["attempts"] == 2 and meta["retry_count"] == 1
    assert meta["retry_errors"] == ["httpx.ReadError"]
    assert no_retry_sleep == [2.0]


@pytest.mark.asyncio
async def test_transport_failure_reports_type_and_attempt_count(no_retry_sleep):
    class Client:
        def __init__(self):
            self.calls = 0

        async def post(self, *_args, **_kwargs):
            self.calls += 1
            raise httpx.ReadError("")

    client = Client()
    response, meta = await gfo._post_openrouter_with_transient_retry(
        client,
        headers={},
        payload={},
        timeout=30,
        label="DOC:block",
    )

    assert response is None
    assert client.calls == 3
    assert meta["attempts"] == 3 and meta["retry_count"] == 2
    assert meta["error"] == "httpx.ReadError"
    assert no_retry_sleep == [2.0, 4.0]


@pytest.mark.asyncio
async def test_fatal_4xx_is_not_retried(no_retry_sleep):
    class Client:
        def __init__(self):
            self.calls = 0

        async def post(self, *_args, **_kwargs):
            self.calls += 1
            return _Response(402)

    client = Client()
    response, meta = await gfo._post_openrouter_with_transient_retry(
        client,
        headers={},
        payload={},
        timeout=30,
        label="DOC:block",
    )

    assert response is not None and response.status_code == 402
    assert client.calls == 1 and meta["attempts"] == 1
    assert no_retry_sleep == []


@pytest.mark.asyncio
async def test_retryable_status_uses_retry_after(no_retry_sleep):
    class Client:
        def __init__(self):
            self.calls = 0

        async def post(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return _Response(429, headers={"Retry-After": "7"})
            return _Response(200)

    client = Client()
    response, meta = await gfo._post_openrouter_with_transient_retry(
        client,
        headers={},
        payload={},
        timeout=30,
        label="DOC:block",
    )

    assert response is not None and response.status_code == 200
    assert meta["retry_errors"] == ["HTTP 429"]
    assert no_retry_sleep == [7.0]


@pytest.mark.asyncio
async def test_full_timeout_gets_at_most_one_retry(no_retry_sleep):
    class Client:
        def __init__(self):
            self.calls = 0

        async def post(self, *_args, **_kwargs):
            self.calls += 1
            raise httpx.ReadTimeout("")

    client = Client()
    response, meta = await gfo._post_openrouter_with_transient_retry(
        client,
        headers={},
        payload={},
        timeout=200,
        label="DOC:block",
    )

    assert response is None
    assert client.calls == 2
    assert meta["attempts"] == 2
    assert meta["error"] == "httpx.ReadTimeout"
    assert no_retry_sleep == [2.0]


def test_retry_config_is_bounded_and_falls_back(monkeypatch):
    monkeypatch.setenv("STAGE01_OPENROUTER_TRANSIENT_RETRIES", "99")
    assert gfo._openrouter_retry_attempts() == 5
    monkeypatch.setenv("STAGE01_OPENROUTER_TRANSIENT_RETRIES", "broken")
    assert gfo._openrouter_retry_attempts() == 2
    monkeypatch.setenv("STAGE01_OPENROUTER_RETRY_BASE_DELAY_S", "-1")
    assert gfo._openrouter_retry_base_delay() == 0.0


@pytest.mark.asyncio
async def test_terminal_transport_detail_reaches_leg_result(monkeypatch, tmp_path):
    from backend.app.services.llm import paid_api_guard

    monkeypatch.setattr(paid_api_guard, "assert_paid_api_allowed", lambda _ctx: None)
    monkeypatch.setenv("STAGE02_PAID_CACHE_ENABLED", "false")
    monkeypatch.setenv("STAGE01_OPENROUTER_TRANSIENT_RETRIES", "0")

    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    (blocks_dir / "b1.png").write_bytes(b"\x89PNG\r\n\x1a\nFAKE")

    class Client:
        async def post(self, *_args, **_kwargs):
            raise httpx.ReadError("")

    result = await gfo.call_gpt_for_block(
        client=Client(),
        block={"block_id": "b1", "page": 1, "file": "b1.png"},
        enrichment={},
        page_text="",
        blocks_dir=blocks_dir,
        api_key="sk-test",
        model="openai/gpt-5.4",
        reasoning_effort="low",
        max_tokens=100,
        system_prompt="system",
        timeout=30,
        project_id="DOC",
        output_dir=tmp_path,
    )

    assert result["ok"] is False
    assert result["error"] == "httpx: httpx.ReadError; attempts=1"
    assert result["attempts"] == 1
    assert result["retry_errors"] == ["httpx.ReadError"]
