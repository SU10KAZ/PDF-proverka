"""Synthetic-only control of every transport; sockets forbidden in this module."""
import asyncio
import json
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from backend.app.services.llm import openrouter_gate as gate


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("A kill-switch test attempted a real network connection")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.delenv(gate.FLAG, raising=False)


@pytest.mark.parametrize("raw", [None, "1", "true", "yes", "on", "TRUE", "YeS"])
def test_enabled_values(raw, monkeypatch):
    if raw is not None:
        monkeypatch.setenv(gate.FLAG, raw)
    gate.assert_openrouter_enabled()
    assert gate.openrouter_state() == {
        "openrouter_enabled": True, "source": "default" if raw is None else "environment",
    }


@pytest.mark.parametrize("raw", ["0", "false", "no", "off", "FALSE", "oFf", "banana", "", " true "])
def test_closed_values(raw, monkeypatch):
    monkeypatch.setenv(gate.FLAG, raw)
    code = "OPENROUTER_DISABLED" if raw.lower() in {"0", "false", "no", "off"} else "openrouter_invalid_enable_flag"
    with pytest.raises(gate.OpenRouterGateError) as exc:
        gate.assert_openrouter_enabled()
    assert exc.value.code == code
    assert gate.openrouter_state() == {"openrouter_enabled": False, "source": "environment"}


@pytest.mark.asyncio
@pytest.mark.parametrize("raw, count", [("1", 1), ("0", 0), ("banana", 0)])
async def test_stage01_fake_transport(raw, count, monkeypatch):
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo
    client = SimpleNamespace(post=AsyncMock(return_value=httpx.Response(200)))
    monkeypatch.setenv(gate.FLAG, raw)
    call = gfo._post_openrouter_with_transient_retry(client, headers={}, payload={}, timeout=1, label="synthetic")
    if count:
        await call
    else:
        with pytest.raises(gate.OpenRouterGateError):
            await call
    assert client.post.await_count == count


@pytest.mark.asyncio
async def test_stage01_disable_before_retry(monkeypatch):
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo
    client = SimpleNamespace(post=AsyncMock(side_effect=httpx.ReadError("synthetic")))
    async def disable(_):
        monkeypatch.setenv(gate.FLAG, "0")
    monkeypatch.setattr(gfo.asyncio, "sleep", disable)
    with pytest.raises(gate.OpenRouterGateError, match="OPENROUTER_DISABLED"):
        await gfo._post_openrouter_with_transient_retry(client, headers={}, payload={}, timeout=1, label="synthetic")
    assert client.post.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("raw,count", [("1", 1), ("0", 0), ("banana", 0)])
async def test_sdk_transport_and_internal_retries(raw, count, monkeypatch):
    from openai import AsyncOpenAI
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={"id":"fake", "object":"chat.completion", "created":0,
            "model":"synthetic", "choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]})
    monkeypatch.setenv(gate.FLAG, raw)
    async with gate.guarded_openai_class(AsyncOpenAI)(api_key="fake", base_url="https://openrouter.ai/api/v1", http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond))) as client:
        if count:
            await client.chat.completions.create(model="synthetic", messages=[])
        else:
            with pytest.raises(gate.OpenRouterGateError):
                await client.chat.completions.create(model="synthetic", messages=[])
    assert len(calls) == count


@pytest.mark.asyncio
async def test_sdk_disable_between_internal_attempts(monkeypatch):
    from openai import AsyncOpenAI
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(429, json={"error":{"message":"synthetic"}})
    async with gate.guarded_openai_class(AsyncOpenAI)(api_key="fake", http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond))) as client:
        async def disable(**kwargs):
            monkeypatch.setenv(gate.FLAG, "0")
        monkeypatch.setattr(client, "_sleep_for_retry", disable)
        with pytest.raises(gate.OpenRouterGateError, match="OPENROUTER_DISABLED"):
            await client.chat.completions.create(model="synthetic", messages=[])
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_runner_blocks_before_budget_client_and_fallback(monkeypatch, stream):
    from backend.app.services.llm import llm_runner as runner
    monkeypatch.setenv(gate.FLAG, "0")
    client = Mock(side_effect=AssertionError("client created"))
    reserve = Mock(side_effect=AssertionError("budget reserved"))
    monkeypatch.setattr(runner, "_get_client", client)
    monkeypatch.setattr(runner, "reserve_paid_api", reserve)
    if stream:
        events = [event async for event in runner.run_llm_stream([], "synthetic")]
        assert len(events) == 1 and events[0]["code"] == "OPENROUTER_DISABLED"
    else:
        with pytest.raises(gate.OpenRouterGateError):
            await runner.run_llm("text_analysis", [])
    client.assert_not_called()
    reserve.assert_not_called()


@pytest.mark.asyncio
async def test_running_job_keeps_first_result_and_never_completes(monkeypatch, tmp_path):
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.models.audit import JobStatus
    manager = PipelineManager.__new__(PipelineManager)
    manager._log = AsyncMock()
    job = SimpleNamespace(status=JobStatus.RUNNING, error_message=None, completed_at=None)
    transport = AsyncMock(return_value="saved first response")
    first = tmp_path / "completed_step.json"
    fallback = Mock()
    @gate.inference_entrypoint
    def direct_provider():
        fallback()
    async def work(*args):
        gate.assert_openrouter_enabled()
        first.write_text(json.dumps({"result": await transport()}))
        monkeypatch.setenv(gate.FLAG, "0")
        # Includes a child task and a legacy fail-soft catcher.
        async def next_step():
            gate.assert_openrouter_enabled()
            await transport()
        await asyncio.gather(next_step(), return_exceptions=True)
        with pytest.raises(gate.OpenRouterGateError):
            direct_provider()
        job.status = JobStatus.COMPLETED
    manager._dispatch_action_impl = work
    await manager._dispatch_action(None, job)
    assert json.loads(first.read_text()) == {"result": "saved first response"}
    assert transport.await_count == 1
    fallback.assert_not_called()
    assert job.status == JobStatus.FAILED
    assert job.error_message.startswith("OPENROUTER_DISABLED:")
    # An unrelated direct-provider job is not poisoned by the stopped job.
    direct_provider()
    fallback.assert_called_once()


def test_worker_blocks_before_secret_transport_and_grant(monkeypatch):
    from audit_worker.providers.openrouter_adapter import OpenRouterProviderAdapter
    adapter = OpenRouterProviderAdapter.__new__(OpenRouterProviderAdapter)
    adapter.home = SimpleNamespace(auth_mode="synthetic")
    monkeypatch.setenv(gate.FLAG, "0")
    result = adapter._inference_gate(confirmed_by_caller=True, purpose="synthetic")
    assert result.error_code == "OPENROUTER_DISABLED"
    result = adapter._request(prompt="synthetic", images=[], purpose="test", timeout_sec=1, model="synthetic", reasoning_effort=None)
    assert result.error_code == "OPENROUTER_DISABLED"


def test_benchmark_transport_blocked(monkeypatch):
    from backend.scripts import benchmark_rejected_audit_openrouter as benchmark
    monkeypatch.setenv(gate.FLAG, "0")
    fake = Mock()
    monkeypatch.setattr(benchmark.urllib.request, "urlopen", fake)
    with pytest.raises(gate.OpenRouterGateError):
        benchmark._call_openrouter({}, "fake", 1)
    fake.assert_not_called()


def test_remote_plan_and_non_openrouter(monkeypatch):
    monkeypatch.setenv(gate.FLAG, "0")
    gate.assert_openrouter_plan_enabled({"legs":[{"provider":"claude"}, {"provider":"codex"}]})
    with pytest.raises(gate.OpenRouterGateError):
        gate.assert_openrouter_plan_enabled({"legs":[{"provider":"openrouter"}]})


@pytest.mark.asyncio
async def test_read_only_model_status(monkeypatch):
    from backend.app.api.routers.audit import get_model
    monkeypatch.setenv(gate.FLAG, "0")
    result = await get_model()
    assert result["openrouter"] == {"openrouter_enabled":False, "source":"environment"}


def test_critic_blocks_before_auth_and_transport(monkeypatch):
    from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import OpenRouterProvider
    import requests
    fake = Mock()
    monkeypatch.setenv(gate.FLAG, "0")
    monkeypatch.setattr(requests, "post", fake)
    with pytest.raises(gate.OpenRouterGateError):
        OpenRouterProvider(model="synthetic")([], {}, "synthetic")
    fake.assert_not_called()


@pytest.mark.asyncio
async def test_safe_api_error_has_no_traceback():
    from backend.app.main import openrouter_gate_error_handler
    response = await openrouter_gate_error_handler(None, gate.OpenRouterGateError())
    assert response.status_code == 503
    assert json.loads(response.body) == {
        "code": "OPENROUTER_DISABLED", "message": "Обработка через OpenRouter временно отключена",
    }


@pytest.mark.asyncio
async def test_sdk_redirect_cannot_bypass_gate(monkeypatch):
    from openai import AsyncOpenAI
    calls = []
    def redirect(request):
        calls.append(request)
        monkeypatch.setenv(gate.FLAG, "0")
        return httpx.Response(307, headers={"location":"https://openrouter.ai/redirected"})
    async with gate.guarded_openai_class(AsyncOpenAI)(api_key="fake", http_client=httpx.AsyncClient(transport=httpx.MockTransport(redirect), follow_redirects=True)) as client:
        with pytest.raises(gate.OpenRouterGateError, match="OPENROUTER_DISABLED"):
            await client.chat.completions.create(model="synthetic", messages=[])
    assert len(calls) == 1


def test_requests_redirect_cannot_bypass_gate(monkeypatch):
    import requests
    calls = []
    def redirect(self, request, **kwargs):
        calls.append(request)
        monkeypatch.setenv(gate.FLAG, "0")
        response = requests.Response()
        response.status_code = 307
        response.headers['location'] = 'https://openrouter.ai/redirected'
        response._content = b''
        response.url = request.url
        response.request = request
        return response
    monkeypatch.setattr(requests.adapters.HTTPAdapter, 'send', redirect)
    with pytest.raises(gate.OpenRouterGateError, match='OPENROUTER_DISABLED'):
        gate.openrouter_requests_post('https://openrouter.ai/api/v1/chat/completions', json={})
    assert len(calls) == 1


def test_worker_environment_preserves_switch(monkeypatch, tmp_path):
    from audit_worker import audit_runner
    monkeypatch.setenv(gate.FLAG, "0")
    env = audit_runner.build_env(config=SimpleNamespace(pipeline_root=tmp_path), job_dir=tmp_path / "job", provider_dir=None)
    assert env[gate.FLAG] == "0"
    assert "OPENROUTER_API_KEY" not in env


@pytest.mark.asyncio
async def test_stream_fallback_checked_at_iteration(monkeypatch):
    sent = []
    @gate.inference_entrypoint
    async def direct_stream():
        sent.append(1)
        yield "fake"
    with gate.openrouter_workflow():
        stream = direct_stream()
        monkeypatch.setenv(gate.FLAG, "0")
        with pytest.raises(gate.OpenRouterGateError):
            gate.assert_openrouter_enabled()
        with pytest.raises(gate.OpenRouterGateError):
            await anext(stream)
        assert sent == []
