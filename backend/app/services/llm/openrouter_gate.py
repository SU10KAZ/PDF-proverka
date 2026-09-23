"""One runtime policy for every OpenRouter inference transport.

The environment is process-scoped: operators must restart idle services after
changing their EnvironmentFile. No flag is cached at import or client creation.
This gate never grants permission: existing paid/worker authorization still applies.
"""
from __future__ import annotations

import os
from contextlib import aclosing, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from inspect import isasyncgenfunction, iscoroutinefunction

FLAG = "AUDIT_OPENROUTER_ENABLED"
_ENABLED = frozenset(("1", "true", "yes", "on"))
_DISABLED = frozenset(("0", "false", "no", "off"))


class OpenRouterGateError(RuntimeError):
    """Terminal policy denial, never a transient provider failure."""

    def __init__(self, code: str = "OPENROUTER_DISABLED"):
        self.code = code
        self.reason = code
        self.message = (
            "Обработка через OpenRouter временно отключена"
            if code == "OPENROUTER_DISABLED"
            else "Некорректная настройка доступности OpenRouter"
        )
        super().__init__(f"{code}: {self.message}")


def openrouter_state() -> dict:
    """Read-only, secret-free effective state; invalid configuration is off."""
    raw = os.environ.get(FLAG)
    return {
        "openrouter_enabled": raw is None or raw.lower() in _ENABLED,
        "source": "default" if raw is None else "environment",
    }


@dataclass
class WorkflowState:
    # Shared by child asyncio tasks, but isolated between independent jobs.
    error: OpenRouterGateError | None = None


_workflow: ContextVar[WorkflowState | None] = ContextVar("openrouter_workflow", default=None)


@contextmanager
def openrouter_workflow():
    state = WorkflowState()
    token = _workflow.set(state)
    try:
        yield state
    finally:
        _workflow.reset(token)


def raise_if_openrouter_stopped() -> None:
    """A denied workflow cannot silently continue through another provider."""
    state = _workflow.get()
    if state is not None and state.error is not None:
        raise state.error


def assert_openrouter_enabled() -> None:
    raise_if_openrouter_stopped()
    raw = os.environ.get(FLAG)
    if raw is None or raw.lower() in _ENABLED:
        return
    error = OpenRouterGateError(
        "OPENROUTER_DISABLED" if raw.lower() in _DISABLED
        else "openrouter_invalid_enable_flag"
    )
    state = _workflow.get()
    if state is not None:
        state.error = error
    raise error


def inference_entrypoint(function):
    """Prevent fallback ONLY after a denial in this job; other jobs are unaffected."""
    if isasyncgenfunction(function):
        @wraps(function)
        async def stream_call(*args, **kwargs):
            raise_if_openrouter_stopped()
            async with aclosing(function(*args, **kwargs)) as stream:
                async for event in stream:
                    yield event
            raise_if_openrouter_stopped()
        return stream_call

    if iscoroutinefunction(function):
        @wraps(function)
        async def async_call(*args, **kwargs):
            raise_if_openrouter_stopped()
            result = await function(*args, **kwargs)
            raise_if_openrouter_stopped()
            return result
        return async_call

    @wraps(function)
    def sync_call(*args, **kwargs):
        raise_if_openrouter_stopped()
        result = function(*args, **kwargs)
        raise_if_openrouter_stopped()
        return result
    return sync_call


def guarded_openai_class(base):
    """SDK hook runs outside SDK's retry catcher, before EVERY HTTP attempt.

    Keeping the SDK's retry settings preserves enabled-state behavior. Unlike a
    transport error, a denial here cannot be wrapped and retried by the SDK.
    """
    from openai import OpenAIError

    class SDKGateError(OpenRouterGateError, OpenAIError):
        pass

    async def before_send(_request):
        try:
            assert_openrouter_enabled()
        except OpenRouterGateError as exc:
            # OpenAIError is propagated without wrapping/retrying by the SDK.
            # The request hook also runs before httpx-followed redirects.
            raise SDKGateError(exc.code) from None

    class GuardedOpenRouterClient(base):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._client.event_hooks["request"].append(before_send)

        async def _prepare_request(self, request):
            await super()._prepare_request(request)
            assert_openrouter_enabled()
    return GuardedOpenRouterClient


def assert_openrouter_plan_enabled(value) -> None:
    """Reject remote OpenRouter work before dispatch, including frozen plans."""
    if isinstance(value, dict):
        if value.get("provider") == "openrouter":
            assert_openrouter_enabled()
        for child in value.values():
            assert_openrouter_plan_enabled(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            assert_openrouter_plan_enabled(child)


def openrouter_requests_post(url, **kwargs):
    """Requests transport with a gate on redirects as well as the first send."""
    import requests

    class GuardedSession(requests.Session):
        def send(self, request, **send_kwargs):
            assert_openrouter_enabled()
            return super().send(request, **send_kwargs)

    assert_openrouter_enabled()
    with GuardedSession() as session:
        return session.post(url, **kwargs)
