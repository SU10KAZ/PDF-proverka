"""Production provider adapter for ProjectChange V3."""
from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol

from .contracts import MODEL, REASONING

_lock = threading.Lock()
_test_provider = None


class ProviderError(RuntimeError):
    def __init__(self, code: str, message: str = ""):
        super().__init__(message or code)
        self.code = code
        self.message = message or code


class V3Provider(Protocol):
    def complete(
        self,
        *,
        stage: str,
        call_id: str,
        pair_id: str,
        prompt: str,
        data: Any,
        schema: dict[str, Any],
        images: list[dict[str, Any]],
    ) -> dict[str, Any]:
        ...


def build_codex_payload(
    prompt: str,
    data: Any,
    images: list[dict[str, Any]],
) -> tuple[str, list[str], list[dict[str, Any]]]:
    """Exact model-visible payload of the frozen V3 transport.

    Images are identified by CONTENT (SHA256), not by path: two crops with
    identical bytes are one model image, as in the frozen experiment.  Labels
    are numbered in first-seen order.
    """
    labels: list[dict[str, Any]] = []
    image_paths: list[str] = []
    seen: set[str] = set()
    for row in images:
        path = str(row.get("path") or "")
        if not path:
            raise ProviderError("image_path_missing", "V3 image row has no path")
        digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        image_paths.append(path)
        labels.append({**dict(row.get("label") or {}), "image": len(image_paths)})
    payload = (
        prompt
        + "\nIMAGES:\n"
        + json.dumps(labels, ensure_ascii=False)
        + "\nSOURCE DATA:\n"
        + json.dumps(data, ensure_ascii=False)
    )
    return payload, image_paths, labels


_USAGE_KEYS = {
    "input_tokens": ("input_tokens", "inputTokens"),
    "cached_input_tokens": ("cached_input_tokens", "cachedInputTokens"),
    "output_tokens": ("output_tokens", "outputTokens"),
    "reasoning_output_tokens": ("reasoning_output_tokens", "reasoningOutputTokens"),
}


def normalized_usage(usage: dict[str, Any] | None) -> dict[str, int] | None:
    """Token usage of one call in one shape for both transports (None: not reported)."""
    if not usage:
        return None
    return {
        key: int(next((usage[a] for a in aliases if usage.get(a) is not None), 0) or 0)
        for key, aliases in _USAGE_KEYS.items()
    }


@dataclass
class FakeProvider:
    handlers: dict[str, Callable[..., dict[str, Any]]] = field(default_factory=dict)
    calls: list[dict[str, Any]] = field(default_factory=list)

    def complete(
        self,
        *,
        stage: str,
        call_id: str,
        pair_id: str,
        prompt: str,
        data: Any,
        schema: dict[str, Any],
        images: list[dict[str, Any]],
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "stage": stage,
                "call_id": call_id,
                "pair_id": pair_id,
                "images": len(images),
                "prompt_chars": len(prompt),
            }
        )
        handler = self.handlers.get(stage)
        if handler is None:
            raise ProviderError("fake_handler_missing", f"No fake handler for stage={stage}")
        return handler(
            call_id=call_id,
            pair_id=pair_id,
            data=data,
            schema=schema,
            images=images,
        )


@dataclass
class CodexProvider:
    model: str = MODEL
    reasoning: str = REASONING
    timeout_s: int = 3600
    call_count: int = 0
    # Receipt of the latest call (set before the provider is contacted, so a
    # failed call is receipted too): transport version, lossless-split hashes.
    last_transport: dict[str, Any] | None = None
    # Run control of the pair (gateway CancelToken): a user cancel kills the
    # CLI session of the call in flight.  Never part of what the model sees.
    cancel_token: Any = None

    def complete(
        self,
        *,
        stage: str,
        call_id: str,
        pair_id: str,
        prompt: str,
        data: Any,
        schema: dict[str, Any],
        images: list[dict[str, Any]],
    ) -> dict[str, Any]:
        from backend.app.services.stage_comparison.ai.gateway import (
            GatewayCancelled,
            GatewayError,
            call_codex,
            call_codex_app_server,
        )

        from . import transport

        self.last_transport = None
        try:
            import jsonschema
        except ImportError as exc:
            # The answer could not be validated: refuse before any model call.
            raise ProviderError(
                "schema_validator_unavailable",
                "jsonschema is not installed in this runtime; no model call was made",
            ) from exc
        payload, image_paths, _labels = build_codex_payload(prompt, data, images)
        try:
            plan = transport.plan(payload)
        except transport.TransportIntegrityError as exc:
            raise ProviderError("transport_integrity", str(exc)) from exc
        self.last_transport = plan.receipt(image_paths)
        try:
            if plan.oversize:
                # Never shortened: exact ordered chunks, see transport.py.
                result, wire = call_codex_app_server(
                    plan.chunks[:-1],
                    plan.chunks[-1],
                    model=self.model,
                    expected_text_sha256=plan.payload_sha256,
                    schema=schema,
                    reasoning_level=self.reasoning,
                    timeout_s=self.timeout_s,
                    images=image_paths,
                    run_id=call_id,
                    cyber_access_program=transport.OVERSIZE_CYBER_ACCESS_PROGRAM,
                    cancel=self.cancel_token,
                )
                self.last_transport["wire"] = wire
            else:
                result = call_codex(
                    payload,
                    model=self.model,
                    schema=schema,
                    reasoning_level=self.reasoning,
                    timeout_s=self.timeout_s,
                    images=image_paths,
                    retries=0,
                    run_id=call_id,
                    json_events=True,  # token usage for the receipt; answer only from -o
                    cancel=self.cancel_token,
                )
        except GatewayCancelled as exc:
            raise ProviderError("provider_cancelled", str(exc)) from exc
        except GatewayError as exc:
            raise ProviderError("provider_gateway_error", str(exc)) from exc
        except Exception as exc:
            raise ProviderError("provider_exception", f"{type(exc).__name__}: {exc}") from exc

        self.call_count += 1
        self.last_transport["usage"] = normalized_usage(result.usage)
        self.last_transport["provider_ok"] = bool(result.ok)
        if not result.ok or not isinstance(result.parsed, dict):
            raise ProviderError(
                result.error_kind or "provider_failed",
                result.error or "empty_or_invalid_provider_response",
            )
        try:
            jsonschema.validate(result.parsed, schema)
        except Exception as exc:
            raise ProviderError("schema_invalid", str(exc)) from exc
        return result.parsed


def set_test_provider(provider) -> None:
    global _test_provider
    with _lock:
        _test_provider = provider


def get_provider():
    with _lock:
        if _test_provider is not None:
            return _test_provider
    return CodexProvider()


def reset_test_provider() -> None:
    set_test_provider(None)
