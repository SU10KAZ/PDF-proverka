"""Production provider adapter for ProjectChange V3."""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
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
        )

        labels = []
        image_paths: list[str] = []
        seen: set[str] = set()
        for row in images:
            path = str(row.get("path") or "")
            if not path or path in seen:
                continue
            seen.add(path)
            image_paths.append(path)
            label = dict(row.get("label") or {})
            label["image"] = len(image_paths)
            labels.append(label)

        payload = (
            prompt
            + "\nIMAGES:\n"
            + json.dumps(labels, ensure_ascii=False)
            + "\nSOURCE DATA:\n"
            + json.dumps(data, ensure_ascii=False)
        )
        try:
            result = call_codex(
                payload,
                model=self.model,
                schema=schema,
                reasoning_level=self.reasoning,
                timeout_s=self.timeout_s,
                images=image_paths,
                retries=0,
                run_id=call_id,
            )
        except GatewayCancelled as exc:
            raise ProviderError("provider_cancelled", str(exc)) from exc
        except GatewayError as exc:
            raise ProviderError("provider_gateway_error", str(exc)) from exc
        except Exception as exc:
            raise ProviderError("provider_exception", f"{type(exc).__name__}: {exc}") from exc

        self.call_count += 1
        if not result.ok or not isinstance(result.parsed, dict):
            raise ProviderError(
                result.error_kind or "provider_failed",
                result.error or "empty_or_invalid_provider_response",
            )
        try:
            import jsonschema

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
