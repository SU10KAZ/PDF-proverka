"""
Единый клиент LLM для OpenRouter и локального Chandra/LM Studio.

OpenRouter остаётся основным удалённым провайдером.
Локальный QWEN использует два transport-режима:
  - `/v1/chat/completions` для structured JSON-этапов;
  - `/api/v1/chat` для свободного текста и multimodal block_batch.
Старый Claude CLI пайплайн НЕ затрагивается.
"""
import asyncio
import base64
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import httpx
from openai import AsyncOpenAI, RateLimitError, APITimeoutError, APIError

from webapp.config import (
    OPENROUTER_API_KEY, OPENROUTER_BASE_URL,
    OPENROUTER_SITE_URL, OPENROUTER_SITE_NAME,
    STAGE_MODELS_OPENROUTER, GEMINI_MAX_OUTPUT_TOKENS, GPT_MAX_OUTPUT_TOKENS,
    DEFAULT_TEMPERATURE, SCHEMAS_DIR,
    CHANDRA_BASE_URL, CHANDRA_API_BASE_URL, CHANDRA_BASIC_USER, CHANDRA_BASIC_PASS,
    LOCAL_QWEN_CONTEXT_LENGTH, LOCAL_QWEN_MAX_OUTPUT_TOKENS,
    LOCAL_QWEN_FINDINGS_MAX_OUTPUT_TOKENS,
    is_local_llm_model,
    get_stage_model,
)
from webapp.models.usage import LLMResult
from webapp.services import model_control_service

logger = logging.getLogger(__name__)

# Sentinel: "не задано" (отличает от явного None = "без формата")
_UNSET = object()
_LOCAL_CONTEXT_ERROR_RE = re.compile(r"n_keep:\s*(\d+)\s*>=\s*n_ctx:\s*(\d+)", re.I)
_LOCAL_CONTEXT_LENGTH_TIERS = (4096, 8192, 16384, 32768, 65536, 98304, 131072, 262144)
_LOCAL_MODEL_RELOAD_LOCKS: dict[str, asyncio.Lock] = {}
_LOCAL_STRUCTURED_COMPLETION_STAGES = {
    "text_analysis",
    "findings_merge",
    "findings_critic",
    "findings_corrector",
    "optimization",
    "optimization_critic",
    "optimization_corrector",
}

# Единый клиент -- создаётся лениво (чтобы не падать при импорте без ключа)
_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            base_url=OPENROUTER_BASE_URL,
            api_key=OPENROUTER_API_KEY,
        )
    return _client


def _build_chandra_headers() -> dict[str, str]:
    """Заголовки для Chandra ngrok endpoint."""
    token = base64.b64encode(
        f"{CHANDRA_BASIC_USER}:{CHANDRA_BASIC_PASS}".encode("utf-8")
    ).decode("ascii")
    return {
        "Authorization": f"Basic {token}",
        "content-type": "application/json",
        "ngrok-skip-browser-warning": "true",
    }


def _coerce_message_content(content: Any) -> str:
    """Нормализовать content из OpenAI-compatible ответа в строку."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(part for part in parts if part)
    return str(content or "")


def _try_parse_json_content(content: str) -> dict | list | None:
    """Попытаться извлечь JSON из сырого текста ответа."""
    if not content:
        return None

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass

    md_match = re.search(r"```(?:json)?\s*\n(.*?)\n```", content, re.DOTALL)
    if md_match:
        try:
            return json.loads(md_match.group(1))
        except json.JSONDecodeError:
            pass

    brace_match = re.search(r"\{.*\}", content, re.DOTALL)
    if brace_match:
        try:
            return json.loads(brace_match.group(0))
        except json.JSONDecodeError:
            pass

    return None


def _usage_value(usage: dict[str, Any] | Any, key: str) -> int:
    """Безопасно взять integer usage field из dict/object."""
    if isinstance(usage, dict):
        return int(usage.get(key, 0) or 0)
    return int(getattr(usage, key, 0) or 0)


def _extract_local_message_text(message: dict[str, Any]) -> tuple[str, str]:
    """Достать content и reasoning_content из local OpenAI-compatible ответа."""
    content = _coerce_message_content(message.get("content", ""))
    reasoning = _coerce_message_content(message.get("reasoning_content", ""))
    return content, reasoning


def _local_chat_input_item(text: str) -> dict[str, str]:
    return {"type": "text", "content": text}


def _convert_local_chat_content(content: Any) -> list[dict[str, str]]:
    """Преобразовать OpenAI-style content в LM Studio /api/v1/chat input."""
    if isinstance(content, str):
        return [_local_chat_input_item(content)]

    items: list[dict[str, str]] = []
    if isinstance(content, list):
        for item in content:
            if isinstance(item, str):
                items.append(_local_chat_input_item(item))
                continue
            if not isinstance(item, dict):
                items.append(_local_chat_input_item(str(item)))
                continue

            item_type = item.get("type")
            if item_type == "text":
                text = item.get("text")
                if isinstance(text, str) and text:
                    items.append(_local_chat_input_item(text))
                continue
            if item_type == "image_url":
                image_url = item.get("image_url") or {}
                data_url = image_url.get("url")
                if isinstance(data_url, str) and data_url:
                    items.append({"type": "image", "data_url": data_url})
                continue

            text = item.get("text")
            if isinstance(text, str) and text:
                items.append(_local_chat_input_item(text))

    return items


def _build_local_chat_payload(
    *,
    model: str,
    messages: list[dict],
    max_tokens: int,
    temperature: float,
) -> dict[str, Any]:
    """Собрать payload для нативного Chandra /api/v1/chat."""
    system_parts: list[str] = []
    input_items: list[dict[str, str]] = []

    for message in messages:
        role = str(message.get("role") or "user")
        content = message.get("content", "")
        if role == "system":
            system_text = _coerce_message_content(content)
            if system_text:
                system_parts.append(system_text)
            continue

        converted = _convert_local_chat_content(content)
        if role != "user":
            input_items.append(_local_chat_input_item(f"[{role.upper()}]"))
        input_items.extend(converted)

    input_payload: str | list[dict[str, str]]
    if not input_items:
        input_payload = ""
    elif len(input_items) == 1 and input_items[0].get("type") == "text":
        input_payload = input_items[0]["content"]
    else:
        input_payload = input_items

    return {
        "model": model,
        "system_prompt": "\n\n".join(part for part in system_parts if part).strip(),
        "input": input_payload,
        "temperature": temperature,
        "max_output_tokens": max_tokens,
        "reasoning": "off",
        "store": False,
    }


def _build_local_chat_completions_payload(
    *,
    model: str,
    messages: list[dict],
    max_tokens: int,
    temperature: float,
    response_format: dict[str, Any] | None,
) -> dict[str, Any]:
    """Собрать payload для OpenAI-compatible `/v1/chat/completions`."""
    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if response_format is not None:
        payload["response_format"] = response_format
    return payload


def _extract_local_chat_output(data: dict[str, Any]) -> tuple[str, str]:
    """Извлечь финальный текст и reasoning из /api/v1/chat ответа."""
    output_items = data.get("output") or []
    messages: list[str] = []
    reasoning: list[str] = []
    for item in output_items:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        content = str(item.get("content") or "")
        if item_type == "message":
            messages.append(content)
        elif item_type == "reasoning":
            reasoning.append(content)
    return "\n".join(part for part in messages if part).strip(), "\n".join(part for part in reasoning if part).strip()


def _extract_local_error_message(data: Any, fallback_text: str = "") -> str:
    """Извлечь удобочитаемое сообщение об ошибке из ответа local endpoint."""
    if isinstance(data, dict):
        error = data.get("error")
        if isinstance(error, dict):
            return str(error.get("message") or error.get("type") or error)
        if error:
            return str(error)
    return fallback_text[:400]


_LOCAL_SCHEMA_STAGE_MAP = {
    "text_analysis": "text_analysis",
    "block_batch": "block_batch",
    "findings_merge": "findings",
    "findings_critic": "findings_review",
    "findings_corrector": "findings",
    "optimization": "optimization",
    "optimization_critic": "optimization_review",
    "optimization_corrector": "optimization",
}


def _sanitize_json_schema(node: Any) -> Any:
    """Убрать ключи, которые LM Studio/structured outputs часто отвергают."""
    if isinstance(node, dict):
        cleaned: dict[str, Any] = {}
        for key, value in node.items():
            if key in {"$schema", "format", "default"}:
                continue
            cleaned[key] = _sanitize_json_schema(value)
        return cleaned
    if isinstance(node, list):
        return [_sanitize_json_schema(item) for item in node]
    return node


def _build_local_response_format(stage_key: str) -> dict[str, Any] | None:
    """Подготовить response_format для локального Chandra endpoint."""
    schema_stage = _LOCAL_SCHEMA_STAGE_MAP.get(stage_key)
    if not schema_stage:
        return {"type": "text"}
    schema = load_schema(schema_stage)
    if not schema:
        return {"type": "text"}
    return {
        "type": "json_schema",
        "json_schema": {
            "name": schema_stage,
            "schema": _sanitize_json_schema(schema),
        },
    }


def _extract_local_context_error(error_text: str) -> tuple[int, int] | None:
    """Извлечь n_keep/n_ctx из LM Studio ошибки про слишком маленький контекст."""
    match = _LOCAL_CONTEXT_ERROR_RE.search(error_text or "")
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _recommended_local_context_length(prompt_tokens: int, max_tokens: int) -> int:
    """Подобрать следующий разумный context_length для локального QWEN."""
    required = max(
        LOCAL_QWEN_CONTEXT_LENGTH,
        prompt_tokens + max(1024, max_tokens),
    )
    for tier in _LOCAL_CONTEXT_LENGTH_TIERS:
        if tier >= required:
            return tier
    return _LOCAL_CONTEXT_LENGTH_TIERS[-1]


def _get_local_max_output_tokens(stage_key: str) -> int:
    """Stage-aware max_output_tokens для локального QWEN."""
    if stage_key == "findings_merge":
        return max(LOCAL_QWEN_MAX_OUTPUT_TOKENS, LOCAL_QWEN_FINDINGS_MAX_OUTPUT_TOKENS)
    return LOCAL_QWEN_MAX_OUTPUT_TOKENS


def _get_local_model_reload_lock(model: str) -> asyncio.Lock:
    lock = _LOCAL_MODEL_RELOAD_LOCKS.get(model)
    if lock is None:
        lock = asyncio.Lock()
        _LOCAL_MODEL_RELOAD_LOCKS[model] = lock
    return lock


def _format_model_control_error(result: dict[str, Any]) -> str:
    error = result.get("error")
    if isinstance(error, dict):
        return str(error.get("message") or error.get("type") or error)
    if error:
        return str(error)
    response = result.get("response")
    if isinstance(response, dict):
        nested = response.get("error")
        if isinstance(nested, dict):
            return str(nested.get("message") or nested.get("type") or nested)
        if nested:
            return str(nested)
    return "unknown model-control error"


def _load_kwargs_from_status(status: dict[str, Any], model: str) -> dict[str, Any]:
    for item in status.get("loaded_instances", []) or []:
        if item.get("model_key") != model:
            continue
        config = item.get("config") or {}
        return {
            "flash_attention": bool(config.get("flash_attention", True)),
            "offload_kv_cache_to_gpu": bool(config.get("offload_kv_cache_to_gpu", True)),
            "eval_batch_size": config.get("eval_batch_size"),
            "num_experts": config.get("num_experts"),
        }
    return {
        "flash_attention": True,
        "offload_kv_cache_to_gpu": True,
        "eval_batch_size": None,
        "num_experts": None,
    }


async def _reload_local_model_with_context(model: str, target_context_length: int) -> tuple[bool, str]:
    """Оставить один instance модели с нужным context_length."""
    async with _get_local_model_reload_lock(model):
        status = await asyncio.to_thread(model_control_service.get_status)
        load_kwargs = _load_kwargs_from_status(status, model)
        loaded_instances = [
            item for item in (status.get("loaded_instances", []) or [])
            if item.get("model_key") == model
        ]

        unload_failures: list[str] = []
        for item in loaded_instances:
            instance_id = item.get("instance_id")
            if not instance_id:
                continue
            unload_result = await asyncio.to_thread(
                model_control_service.unload_instance,
                instance_id=instance_id,
            )
            if not unload_result.get("ok"):
                unload_failures.append(
                    f"{instance_id}: {_format_model_control_error(unload_result)}"
                )

        if unload_failures:
            logger.warning(
                "Failed to unload some local model instances for %s: %s",
                model,
                "; ".join(unload_failures),
            )

        load_result = await asyncio.to_thread(
            model_control_service.load_model,
            model=model,
            context_length=target_context_length,
            flash_attention=bool(load_kwargs.get("flash_attention", True)),
            offload_kv_cache_to_gpu=bool(load_kwargs.get("offload_kv_cache_to_gpu", True)),
            eval_batch_size=load_kwargs.get("eval_batch_size"),
            num_experts=load_kwargs.get("num_experts"),
        )
        if load_result.get("ok"):
            return True, f"context_length={target_context_length}"
        return False, _format_model_control_error(load_result)


async def _run_local_chandra_chat(
    *,
    model: str,
    messages: list[dict],
    max_tokens: int,
    temperature: float,
    timeout: int,
    response_format: dict | None,
    _allow_context_reload: bool = True,
) -> LLMResult:
    """Запуск локальной модели через Chandra OpenAI-compatible endpoint."""
    if not CHANDRA_BASE_URL:
        return LLMResult(
            text="",
            is_error=True,
            error_message="CHANDRA_BASE_URL не задан",
            model=model,
            cost_source="local",
        )
    if not CHANDRA_BASIC_USER or not CHANDRA_BASIC_PASS:
        return LLMResult(
            text="",
            is_error=True,
            error_message="NGROK_AUTH_USER/NGROK_AUTH_PASS не заданы",
            model=model,
            cost_source="local",
        )

    payload = _build_local_chat_payload(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
    )

    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{CHANDRA_BASE_URL}/api/v1/chat",
                headers=_build_chandra_headers(),
                json=payload,
            )
    except httpx.TimeoutException as exc:
        return LLMResult(
            text="",
            is_error=True,
            error_message=f"Local model timeout: {exc}",
            model=model,
            cost_source="local",
        )
    except Exception as exc:
        return LLMResult(
            text="",
            is_error=True,
            error_message=f"Local model error: {exc}",
            model=model,
            cost_source="local",
        )

    elapsed_ms = int((time.monotonic() - started) * 1000)

    try:
        data = response.json()
    except Exception:
        data = {"raw_text": response.text[:4000]}

    if response.status_code >= 400:
        error = _extract_local_error_message(data, response.text)

        context_error = _extract_local_context_error(error)
        if context_error and _allow_context_reload:
            prompt_tokens, loaded_context = context_error
            target_context = _recommended_local_context_length(prompt_tokens, max_tokens)
            logger.warning(
                "Local model context overflow for %s: n_keep=%s n_ctx=%s; reloading with %s",
                model,
                prompt_tokens,
                loaded_context,
                target_context,
            )
            reloaded, reload_message = await _reload_local_model_with_context(model, target_context)
            if reloaded:
                return await _run_local_chandra_chat(
                    model=model,
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    timeout=timeout,
                    response_format=response_format,
                    _allow_context_reload=False,
                )
            error = (
                f"{error} | auto-reload failed for context_length={target_context}: "
                f"{reload_message}"
            )

        return LLMResult(
            text="",
            is_error=True,
            error_message=f"Local model HTTP {response.status_code}: {error}",
            model=model,
            duration_ms=elapsed_ms,
            cost_source="local",
        )

    content = ""
    reasoning_content = ""
    json_data = None
    stats = data.get("stats", {}) if isinstance(data, dict) else {}
    if isinstance(data, dict):
        content, reasoning_content = _extract_local_chat_output(data)
        json_data = _try_parse_json_content(content)
        if json_data is None and reasoning_content:
            reasoning_json = _try_parse_json_content(reasoning_content)
            if reasoning_json is not None:
                content = reasoning_content
                json_data = reasoning_json
        if not content and reasoning_content:
            content = reasoning_content

    return LLMResult(
        text=content,
        json_data=json_data,
        input_tokens=_usage_value(stats, "input_tokens"),
        output_tokens=_usage_value(stats, "total_output_tokens"),
        cost_usd=0.0,
        duration_ms=elapsed_ms,
        model=model,
        reasoning_tokens=_usage_value(stats, "reasoning_output_tokens"),
        cost_source="local",
        response_id=(data.get("model_instance_id", "") if isinstance(data, dict) else "") or "",
        finish_reason="stop" if content else "",
    )


async def _run_local_chat_completions(
    *,
    model: str,
    messages: list[dict],
    max_tokens: int,
    temperature: float,
    timeout: int,
    response_format: dict[str, Any] | None,
    _allow_context_reload: bool = True,
) -> LLMResult:
    """Запуск локальной модели через OpenAI-compatible `/v1/chat/completions`."""
    if not CHANDRA_BASE_URL:
        return LLMResult(
            text="",
            is_error=True,
            error_message="CHANDRA_BASE_URL не задан",
            model=model,
            cost_source="local",
        )
    if not CHANDRA_BASIC_USER or not CHANDRA_BASIC_PASS:
        return LLMResult(
            text="",
            is_error=True,
            error_message="NGROK_AUTH_USER/NGROK_AUTH_PASS не заданы",
            model=model,
            cost_source="local",
        )

    payload = _build_local_chat_completions_payload(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
        response_format=response_format,
    )

    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{CHANDRA_BASE_URL}/v1/chat/completions",
                headers=_build_chandra_headers(),
                json=payload,
            )
    except httpx.TimeoutException as exc:
        return LLMResult(
            text="",
            is_error=True,
            error_message=f"Local model timeout: {exc}",
            model=model,
            cost_source="local",
        )
    except Exception as exc:
        return LLMResult(
            text="",
            is_error=True,
            error_message=f"Local model error: {exc}",
            model=model,
            cost_source="local",
        )

    elapsed_ms = int((time.monotonic() - started) * 1000)

    try:
        data = response.json()
    except Exception:
        data = {"raw_text": response.text[:4000]}

    if response.status_code >= 400:
        error = _extract_local_error_message(data, response.text)

        context_error = _extract_local_context_error(error)
        if context_error and _allow_context_reload:
            prompt_tokens, loaded_context = context_error
            target_context = _recommended_local_context_length(prompt_tokens, max_tokens)
            logger.warning(
                "Local chat.completions context overflow for %s: n_keep=%s n_ctx=%s; reloading with %s",
                model,
                prompt_tokens,
                loaded_context,
                target_context,
            )
            reloaded, reload_message = await _reload_local_model_with_context(model, target_context)
            if reloaded:
                return await _run_local_chat_completions(
                    model=model,
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    timeout=timeout,
                    response_format=response_format,
                    _allow_context_reload=False,
                )
            error = (
                f"{error} | auto-reload failed for context_length={target_context}: "
                f"{reload_message}"
            )

        return LLMResult(
            text="",
            is_error=True,
            error_message=f"Local model HTTP {response.status_code}: {error}",
            model=model,
            duration_ms=elapsed_ms,
            cost_source="local",
        )

    message: dict[str, Any] = {}
    usage = {}
    finish_reason = ""
    if isinstance(data, dict):
        choices = data.get("choices") or []
        if choices and isinstance(choices[0], dict):
            choice0 = choices[0]
            message = choice0.get("message") or {}
            finish_reason = str(choice0.get("finish_reason") or "")
        usage = data.get("usage") or {}

    content, reasoning_content = _extract_local_message_text(message)
    json_data = _try_parse_json_content(content)
    if json_data is None and reasoning_content:
        reasoning_json = _try_parse_json_content(reasoning_content)
        if reasoning_json is not None:
            content = reasoning_content
            json_data = reasoning_json
    if not content and reasoning_content:
        content = reasoning_content

    return LLMResult(
        text=content,
        json_data=json_data,
        input_tokens=_usage_value(usage, "prompt_tokens"),
        output_tokens=_usage_value(usage, "completion_tokens"),
        cost_usd=0.0,
        duration_ms=elapsed_ms,
        model=model,
        reasoning_tokens=_usage_value(
            usage.get("completion_tokens_details", {}) if isinstance(usage, dict) else {},
            "reasoning_tokens",
        ),
        cost_source="local",
        response_id=(data.get("id", "") if isinstance(data, dict) else "") or "",
        finish_reason=finish_reason or ("stop" if content else ""),
        is_error=bool(finish_reason == "length" and json_data is None),
        error_message=(
            "Local model output truncated before valid JSON was completed"
            if finish_reason == "length" and json_data is None
            else ""
        ),
    )


# Цены моделей OpenRouter ($/1M токенов) — обновлять при изменении
# Fallback only: если в response.usage пришла usage.cost — используется она.
_MODEL_PRICES = {
    "google/gemini-2.5-pro":          {"input": 1.25,  "output": 10.0},
    "google/gemini-2.5-flash":        {"input": 0.30,  "output": 2.50},
    "google/gemini-3.1-pro-preview":  {"input": 2.0,   "output": 12.0},
    "anthropic/claude-opus-4-7":      {"input": 15.0,  "output": 75.0},
    "anthropic/claude-sonnet-4-6":    {"input": 3.0,   "output": 15.0},
    "openai/gpt-5.4":                {"input": 2.50,  "output": 15.0},
    "openai/gpt-4.1":               {"input": 2.00,  "output": 8.0},
}


def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Оценить стоимость запроса на основе токенов и цен модели."""
    prices = _MODEL_PRICES.get(model)
    if not prices:
        return 0.0
    cost = (input_tokens * prices["input"] + output_tokens * prices["output"]) / 1_000_000
    return round(cost, 6)


async def run_llm(
    stage: str,
    messages: list[dict],
    response_format: dict | None = _UNSET,
    temperature: float | None = None,
    timeout: int = 600,
    max_retries: int = 3,
    model_override: str | None = None,
    *,
    strict_schema: dict | None = None,
    schema_name: str = "response",
    response_healing: bool = False,
    require_parameters: bool = False,
    provider_data_collection: str | None = None,
    max_tokens_override: int | None = None,
    extra_body: dict | None = None,
) -> LLMResult:
    """Единый вызов LLM через OpenRouter или локальный Chandra.

    Args:
        stage: ключ этапа конвейера (text_analysis, block_batch, findings_merge и т.д.)
        messages: список сообщений [{role, content}, ...]
        response_format: формат ответа (по умолчанию json_object)
        temperature: температура генерации (по умолчанию из config)
        timeout: таймаут запроса в секундах
        max_retries: макс. число повторов при rate limit / timeout
        model_override: явная модель (если задана — игнорирует stage config)

    Returns:
        LLMResult с текстом, распарсенным JSON, токенами и метриками.
    """
    # Нормализация: block_batch_001 -> block_batch
    stage_key = stage
    if stage.startswith("block_batch"):
        stage_key = "block_batch"

    model = model_override or get_stage_model(stage_key)
    if max_tokens_override is not None:
        max_tokens = max_tokens_override
    elif is_local_llm_model(model):
        max_tokens = _get_local_max_output_tokens(stage_key)
    else:
        max_tokens = (
            GEMINI_MAX_OUTPUT_TOKENS if "gemini" in model
            else GPT_MAX_OUTPUT_TOKENS
        )
    temp = temperature if temperature is not None else DEFAULT_TEMPERATURE

    # Build response_format: strict_schema wins, then explicit, then default json_object.
    if strict_schema is not None:
        effective_format = {
            "type": "json_schema",
            "json_schema": {
                "name": schema_name,
                "strict": True,
                "schema": strict_schema,
            },
        }
    else:
        effective_format = (
            {"type": "json_object"} if response_format is _UNSET
            else response_format  # None или явный dict
        )

    if is_local_llm_model(model):
        # Local path:
        # - text-only structured stages -> /v1/chat/completions (json_schema)
        # - multimodal/freeform stages -> /api/v1/chat
        local_format = _build_local_response_format(stage_key)
        if (
            stage_key in _LOCAL_STRUCTURED_COMPLETION_STAGES
            and isinstance(local_format, dict)
            and local_format.get("type") == "json_schema"
        ):
            return await _run_local_chat_completions(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temp,
                timeout=timeout,
                response_format=local_format,
            )
        return await _run_local_chandra_chat(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temp,
            timeout=timeout,
            response_format=local_format,
        )

    client = _get_client()

    # Build extra_body for OpenRouter-specific knobs (plugins, provider)
    built_extra_body: dict = {}
    plugins: list[dict] = []
    if response_healing:
        plugins.append({"id": "response-healing"})
    if plugins:
        built_extra_body["plugins"] = plugins

    provider_block: dict = {}
    if require_parameters:
        provider_block["require_parameters"] = True
    if provider_data_collection in ("allow", "deny"):
        provider_block["data_collection"] = provider_data_collection
    if provider_block:
        built_extra_body["provider"] = provider_block

    # User-supplied extra_body merges on top (deep merge for plugins/provider)
    if extra_body:
        for k, v in extra_body.items():
            if k == "plugins" and isinstance(v, list):
                built_extra_body.setdefault("plugins", []).extend(v)
            elif k == "provider" and isinstance(v, dict):
                built_extra_body.setdefault("provider", {}).update(v)
            else:
                built_extra_body[k] = v

    for attempt in range(1, max_retries + 1):
        start = time.monotonic()
        try:
            create_kwargs = dict(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temp,
                timeout=timeout,
                extra_headers={
                    "HTTP-Referer": OPENROUTER_SITE_URL,
                    "X-Title": OPENROUTER_SITE_NAME,
                },
            )
            if effective_format is not None:
                create_kwargs["response_format"] = effective_format
            if built_extra_body:
                create_kwargs["extra_body"] = built_extra_body
            response = await client.chat.completions.create(**create_kwargs)
        except RateLimitError as e:
            if attempt < max_retries:
                wait = min(60, 2 ** attempt * 5)
                logger.warning(
                    "[%s] Rate limit (attempt %d/%d), waiting %ds: %s",
                    stage, attempt, max_retries, wait, e,
                )
                await asyncio.sleep(wait)
                continue
            return LLMResult(
                text="", is_error=True,
                error_message=f"Rate limit after {max_retries} retries: {e}",
                model=model,
            )
        except APITimeoutError as e:
            if attempt < max_retries:
                logger.warning(
                    "[%s] Timeout (attempt %d/%d): %s",
                    stage, attempt, max_retries, e,
                )
                continue
            return LLMResult(
                text="", is_error=True,
                error_message=f"Timeout after {max_retries} retries: {e}",
                model=model,
            )
        except APIError as e:
            return LLMResult(
                text="", is_error=True,
                error_message=f"API error: {e}",
                model=model,
            )
        except Exception as e:
            return LLMResult(
                text="", is_error=True,
                error_message=str(e),
                model=model,
            )

        elapsed_ms = int((time.monotonic() - start) * 1000)
        content = response.choices[0].message.content or ""

        json_data = _try_parse_json_content(content)

        # Usage: extract extended fields + actual cost if present
        usage = response.usage
        input_tokens = usage.prompt_tokens if usage else 0
        output_tokens = usage.completion_tokens if usage else 0
        cached_tokens = 0
        cache_write_tokens = 0
        reasoning_tokens = 0
        actual_cost = None
        cost_source = "estimated"

        if usage is not None:
            pt_details = getattr(usage, "prompt_tokens_details", None)
            if pt_details is not None:
                cached_tokens = getattr(pt_details, "cached_tokens", 0) or 0
                cache_write_tokens = getattr(pt_details, "cache_write_tokens", 0) or 0
            ct_details = getattr(usage, "completion_tokens_details", None)
            if ct_details is not None:
                reasoning_tokens = getattr(ct_details, "reasoning_tokens", 0) or 0
            # OpenRouter-specific: usage.cost (in USD)
            actual_cost = getattr(usage, "cost", None)

        if actual_cost is not None and actual_cost > 0:
            cost = round(float(actual_cost), 8)
            cost_source = "actual"
        else:
            cost = _estimate_cost(model, input_tokens, output_tokens)
            cost_source = "estimated"

        finish_reason = ""
        try:
            finish_reason = response.choices[0].finish_reason or ""
        except Exception:
            finish_reason = ""

        return LLMResult(
            text=content,
            json_data=json_data,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
            duration_ms=elapsed_ms,
            model=model,
            cached_tokens=cached_tokens,
            cache_write_tokens=cache_write_tokens,
            reasoning_tokens=reasoning_tokens,
            cost_source=cost_source,
            response_id=getattr(response, "id", "") or "",
            finish_reason=finish_reason,
        )

    # Safety net (shouldn't reach here)
    return LLMResult(
        text="", is_error=True,
        error_message="Max retries exhausted",
        model=model,
    )


from typing import AsyncGenerator


async def run_llm_stream(
    messages: list[dict],
    model_override: str,
    temperature: float | None = None,
    timeout: int = 120,
) -> AsyncGenerator[dict, None]:
    """Стриминг ответа через OpenRouter (SSE).

    Yields:
        {"type": "delta", "text": "..."} — фрагмент текста
        {"type": "done", "text": "...", "input_tokens": N, "output_tokens": N, "cost_usd": F}
    """
    model = model_override
    if is_local_llm_model(model):
        yield {"type": "error", "message": "Local QWEN streaming is not supported in this UI yet"}
        return
    max_tokens = (
        GEMINI_MAX_OUTPUT_TOKENS if "gemini" in model
        else GPT_MAX_OUTPUT_TOKENS
    )
    temp = temperature if temperature is not None else DEFAULT_TEMPERATURE
    client = _get_client()

    full_text = ""
    input_tokens = 0
    output_tokens = 0

    try:
        stream = await client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temp,
            stream=True,
            stream_options={"include_usage": True},
            timeout=timeout,
            extra_headers={
                "HTTP-Referer": OPENROUTER_SITE_URL,
                "X-Title": OPENROUTER_SITE_NAME,
            },
        )

        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                delta = chunk.choices[0].delta.content
                full_text += delta
                yield {"type": "delta", "text": delta}
            # Некоторые провайдеры отдают usage в последнем chunk
            if hasattr(chunk, 'usage') and chunk.usage:
                input_tokens = chunk.usage.prompt_tokens or 0
                output_tokens = chunk.usage.completion_tokens or 0

    except Exception as e:
        yield {"type": "error", "message": str(e)}
        return

    cost = _estimate_cost(model, input_tokens, output_tokens)
    yield {
        "type": "done",
        "text": full_text,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost,
    }


def make_image_content(
    image_path: str | Path,
    detail: str = "high",
    *,
    scale: float = 1.0,
) -> dict:
    """PNG -> base64 content block для multimodal сообщений.

    Args:
        image_path: путь к PNG-файлу
        detail: уровень детализации ("high" или "low")
        scale: множитель ресайза (0<scale<=1). При scale<1 PNG перед base64
            уменьшается (LANCZOS) — используется как fallback для локального
            QWEN, который отвергает слишком большие изображения.
    """
    if scale >= 0.999:
        raw = Path(image_path).read_bytes()
    else:
        from PIL import Image
        import io
        with Image.open(image_path) as img:
            new_w = max(1, int(img.width * scale))
            new_h = max(1, int(img.height * scale))
            resized = img.resize((new_w, new_h), Image.LANCZOS)
            buf = io.BytesIO()
            save_kwargs = {"format": "PNG", "optimize": True}
            resized.save(buf, **save_kwargs)
            raw = buf.getvalue()
    b64 = base64.b64encode(raw).decode()
    return {
        "type": "image_url",
        "image_url": {
            "url": f"data:image/png;base64,{b64}",
            "detail": detail,
        },
    }


def build_interleaved_content(
    blocks: list[dict],
    page_contexts: dict[int, str],
    project_dir: Path,
    *,
    image_scale: float = 1.0,
) -> list[dict]:
    """Interleaved text<->PNG по страницам.

    image_scale<1.0 — ресайз PNG блоков перед base64 (fallback для QWEN).
    """
    content: list[dict] = []
    current_page = None

    for block in blocks:
        page = block.get("page", 0)
        if page != current_page:
            current_page = page
            ctx = page_contexts.get(page, f"Page {page}")
            content.append({
                "type": "text",
                "text": f"=== PAGE {page} ===\n{ctx}",
            })

        block_path = project_dir / "_output" / "blocks" / block["file"]
        if block_path.exists():
            content.append(make_image_content(block_path, scale=image_scale))

        content.append({
            "type": "text",
            "text": f"[{block['block_id']}] {block.get('ocr_label', '')}",
        })

    return content


def load_schema(stage: str) -> dict | None:
    """Загрузить JSON Schema для этапа.

    Args:
        stage: ключ этапа (text_analysis, block_batch, findings и т.д.)

    Returns:
        dict со схемой или None если файл не найден.
    """
    schema_path = SCHEMAS_DIR / f"{stage}.json"
    if schema_path.exists():
        return json.loads(schema_path.read_text(encoding="utf-8"))
    return None
