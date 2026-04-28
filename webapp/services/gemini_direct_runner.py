"""
Direct Gemini Developer API runner for stage 02 block_batch.

Parallel path alongside OpenRouter (llm_runner.py) — does NOT modify
existing llm_runner.py or Claude CLI path.

Activation:
  env GEMINI_DIRECT_API_KEY must be set.
  Pass provider="gemini_direct" to run_block_batch or use in experiment runner.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ─── Model ID mapping ─────────────────────────────────────────────────────────
# Internal / OpenRouter model IDs → Gemini Developer API native model IDs
GEMINI_DIRECT_MODEL_MAP: dict[str, str] = {
    "google/gemini-2.5-flash":       "gemini-2.5-flash",
    "google/gemini-2.5-flash-lite":  "gemini-2.5-flash-lite",
    "google/gemini-3.1-pro-preview": "gemini-3.1-pro-preview",
    # Native IDs pass through unchanged
    "gemini-2.5-flash":              "gemini-2.5-flash",
    "gemini-2.5-flash-lite":         "gemini-2.5-flash-lite",
    "gemini-3.1-pro-preview":        "gemini-3.1-pro-preview",
    "gemini-2.5-pro":                "gemini-2.5-pro",
    "gemini-2.0-flash":              "gemini-2.0-flash",
}

# Models that support thinking config
_THINKING_MODELS = {"gemini-2.5-flash", "gemini-2.5-pro", "gemini-3.1-pro-preview"}

# ─── Pricing table ────────────────────────────────────────────────────────────
# USD per 1M tokens (Gemini Developer API pricing, 2025)
# standard = regular API tier; flex = reduced-latency/bulk tier
_GEMINI_DIRECT_PRICES: dict[str, dict[str, dict[str, float]]] = {
    "gemini-2.5-flash": {
        "standard": {
            "input":    0.15,    # ≤200K context
            "input_long": 0.15,  # >200K context
            "output":   0.60,
            "thinking": 3.50,
            "cached":   0.0375,  # 1/4 of input price
        },
        "flex": {
            "input":    0.04,
            "output":   0.15,
            "thinking": 0.35,
            "cached":   0.01,
        },
    },
    "gemini-2.5-flash-lite": {
        "standard": {
            "input":    0.10,
            "output":   0.40,
            "thinking": 1.00,
            "cached":   0.025,
        },
        "flex": {
            "input":    0.025,
            "output":   0.10,
            "thinking": 0.25,
            "cached":   0.006,
        },
    },
    "gemini-3.1-pro-preview": {
        "standard": {
            "input":      1.25,
            "input_long": 2.50,   # >200K context
            "output":     10.0,
            "thinking":   3.50,
            "cached":     0.3125,
        },
        "flex": {
            "input":    0.50,
            "output":   4.00,
            "thinking": 1.50,
            "cached":   0.125,
        },
    },
    "gemini-2.5-pro": {
        "standard": {
            "input":      1.25,
            "input_long": 2.50,
            "output":     10.0,
            "thinking":   3.50,
            "cached":     0.3125,
        },
        "flex": {
            "input":    0.50,
            "output":   4.00,
            "thinking": 1.50,
            "cached":   0.125,
        },
    },
}

LONG_CONTEXT_THRESHOLD_TOKENS = 200_000


# ─── Result dataclass ─────────────────────────────────────────────────────────

@dataclass
class GeminiBlockBatchResult:
    """Full result from one direct Gemini block batch request."""

    batch_id: int = 0
    model_id: str = ""
    tier: str = "standard"

    raw_text: str = ""
    parsed_data: dict | None = None

    is_error: bool = False
    error_type: str = ""   # provider | schema | completeness | timeout | network
    error_message: str = ""
    retry_count: int = 0

    # Token telemetry
    predicted_prompt_tokens: int = 0  # preflight estimate via countTokens
    prompt_tokens: int = 0            # actual from usage_metadata
    output_tokens: int = 0
    thought_tokens: int = 0
    cached_tokens: int = 0
    total_tokens: int = 0             # prompt + output + thought

    # Cost
    cost_usd: float = 0.0

    # Timing
    duration_ms: int = 0

    # Completeness
    input_block_ids: list[str] = field(default_factory=list)
    returned_block_ids: list[str] = field(default_factory=list)
    missing_block_ids: list[str] = field(default_factory=list)
    duplicate_block_ids: list[str] = field(default_factory=list)
    extra_block_ids: list[str] = field(default_factory=list)
    block_id_inferred_from_input: bool = False
    inferred_block_id_count: int = 0

    # Cache
    cache_name: str | None = None
    cache_hit: bool = False


# ─── Public helpers ───────────────────────────────────────────────────────────

def resolve_direct_model_id(model_id: str) -> str:
    """Resolve an internal/OpenRouter model ID to a Gemini Developer API model ID."""
    return GEMINI_DIRECT_MODEL_MAP.get(model_id, model_id)


def estimate_gemini_direct_cost(
    model_id: str,
    prompt_tokens: int,
    output_tokens: int,
    thought_tokens: int = 0,
    cached_tokens: int = 0,
    tier: str = "standard",
) -> float:
    """Estimate cost in USD for a Gemini Developer API request.

    Uses actual usage metadata when available.  Falls back to 0.0 for unknown models.
    """
    native_id = resolve_direct_model_id(model_id)
    prices = _GEMINI_DIRECT_PRICES.get(native_id) or _GEMINI_DIRECT_PRICES.get(model_id)
    if not prices:
        return 0.0

    tier_prices = prices.get(tier) or prices.get("standard", {})
    input_price = tier_prices.get("input", 0.0)
    output_price = tier_prices.get("output", 0.0)
    thinking_price = tier_prices.get("thinking", output_price)
    cached_price = tier_prices.get("cached", input_price * 0.25)

    # Use long-context pricing when above threshold
    if prompt_tokens > LONG_CONTEXT_THRESHOLD_TOKENS:
        input_price = tier_prices.get("input_long", input_price)

    uncached_input = max(0, prompt_tokens - cached_tokens)
    cost = (
        uncached_input * input_price
        + cached_tokens * cached_price
        + output_tokens * output_price
        + thought_tokens * thinking_price
    ) / 1_000_000
    return round(cost, 8)


def is_gemini_direct_model(model_id: str) -> bool:
    """Return True if this model ID should use the direct Gemini provider."""
    native = resolve_direct_model_id(model_id)
    return native.startswith("gemini-")


# ─── Schema loading ───────────────────────────────────────────────────────────

def _load_gemini_schema() -> dict:
    schema_path = (
        Path(__file__).resolve().parent.parent / "schemas" / "block_batch.gemini_direct.json"
    )
    if schema_path.exists():
        return json.loads(schema_path.read_text(encoding="utf-8"))
    # Minimal inline fallback
    return {
        "type": "object",
        "required": ["block_analyses"],
        "properties": {
            "block_analyses": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["block_id", "page", "label", "summary", "key_values_read", "findings"],
                    "properties": {
                        "block_id":        {"type": "string"},
                        "page":            {"type": "integer"},
                        "label":           {"type": "string"},
                        "summary":         {"type": "string"},
                        "key_values_read": {"type": "array", "items": {"type": "string"}},
                        "findings":        {"type": "array", "items": {"type": "object"}},
                    },
                },
            },
        },
    }


# ─── Message format conversion ────────────────────────────────────────────────

def openrouter_messages_to_gemini(
    messages: list[dict],
) -> tuple[str, list]:
    """Convert OpenRouter-format messages to Gemini API contents.

    Returns:
        (system_instruction_text, user_contents_list_of_Content_objects)

    OpenRouter format:
        [{"role": "system", "content": "..."},
         {"role": "user",   "content": [{type: text|image_url, ...}]}]

    Gemini format:
        system_instruction = "..."
        contents = [Content(role="user", parts=[Part(text=...), Part.from_bytes(...)])]
    """
    try:
        from google.genai import types as _gt
    except ImportError as exc:
        raise RuntimeError(
            "google-genai SDK not installed. Run: pip install google-genai"
        ) from exc

    system_text = ""
    gemini_contents: list = []

    for msg in messages:
        role = msg.get("role", "")
        content = msg.get("content", "")

        if role == "system":
            if isinstance(content, str):
                system_text = content
            elif isinstance(content, list):
                system_text = "\n".join(
                    p.get("text", "") for p in content if p.get("type") == "text"
                )
            continue

        if role in ("user", "assistant"):
            parts: list = []
            if isinstance(content, str):
                parts.append(_gt.Part(text=content))
            elif isinstance(content, list):
                for item in content:
                    t = item.get("type", "")
                    if t == "text":
                        txt = item.get("text", "")
                        if txt:
                            parts.append(_gt.Part(text=txt))
                    elif t == "image_url":
                        url = item.get("image_url", {}).get("url", "")
                        if url.startswith("data:image/png;base64,"):
                            raw = base64.b64decode(url[len("data:image/png;base64,"):])
                            parts.append(_gt.Part.from_bytes(data=raw, mime_type="image/png"))
                        elif url.startswith("data:image/jpeg;base64,"):
                            raw = base64.b64decode(url[len("data:image/jpeg;base64,"):])
                            parts.append(_gt.Part.from_bytes(data=raw, mime_type="image/jpeg"))
            if parts:
                gemini_contents.append(_gt.Content(role="user", parts=parts))

    return system_text, gemini_contents


# ─── Completeness validation ──────────────────────────────────────────────────

def check_completeness(
    input_block_ids: list[str],
    parsed_data: dict | None,
) -> tuple[list[str], list[str], list[str], bool, int]:
    """Validate block ID completeness in parsed response.

    Returns:
        (missing_ids, duplicate_ids, extra_ids, block_id_inferred, inferred_count)

    Single-block inference rule:
      If exactly 1 input_block_id, 1 returned analysis, and the returned block_id
      is empty/missing but all other required fields are valid → inject input block_id.
      Mark block_id_inferred_from_input=True and count it.
      This is NOT applied for batches with 2+ blocks.
    """
    if not parsed_data:
        return list(input_block_ids), [], [], False, 0

    analyses = parsed_data.get("block_analyses", [])
    if not isinstance(analyses, list):
        return list(input_block_ids), [], [], False, 0

    returned_ids = [
        str(a.get("block_id", "")).strip()
        for a in analyses
        if isinstance(a, dict)
    ]

    block_id_inferred = False
    inferred_count = 0

    # Single-block inference: only when input has exactly 1 block
    if (
        len(input_block_ids) == 1
        and len(analyses) == 1
        and not returned_ids[0]
        and _is_valid_analysis_sans_block_id(analyses[0])
    ):
        analyses[0]["block_id"] = input_block_ids[0]
        returned_ids = [input_block_ids[0]]
        block_id_inferred = True
        inferred_count = 1
        logger.debug("block_id inferred from single-block input: %s", input_block_ids[0])

    input_set = set(input_block_ids)
    returned_set = set(returned_ids)

    missing = [bid for bid in input_block_ids if bid not in returned_set]

    seen: set[str] = set()
    duplicates: list[str] = []
    for bid in returned_ids:
        if bid in seen and bid not in duplicates:
            duplicates.append(bid)
        seen.add(bid)

    extra = [bid for bid in returned_set if bid and bid not in input_set]

    return missing, duplicates, extra, block_id_inferred, inferred_count


def _is_valid_analysis_sans_block_id(analysis: dict) -> bool:
    """True if analysis has all required fields (except possibly block_id)."""
    for f in ("page", "label", "summary", "key_values_read", "findings"):
        if f not in analysis:
            return False
    return isinstance(analysis["key_values_read"], list) and isinstance(analysis["findings"], list)


# ─── Context cache manager ────────────────────────────────────────────────────

class GeminiCacheManager:
    """Manages Gemini context caches (system prompt caching).

    Creates one cache per unique (model, system_prompt) pair and reuses it
    across requests for the duration of the experiment.
    """

    def __init__(self, api_key: str, ttl_seconds: int = 600):
        self._api_key = api_key
        self._ttl = f"{ttl_seconds}s"
        self._cache_map: dict[str, str] = {}  # hash → cache_name

    def _prompt_hash(self, model_id: str, system_text: str) -> str:
        import hashlib
        return hashlib.sha256(f"{model_id}|{system_text}".encode()).hexdigest()[:16]

    async def get_or_create(self, model_id: str, system_text: str) -> str | None:
        """Return existing cache name or create a new one. None on failure."""
        if not system_text:
            return None
        key = self._prompt_hash(model_id, system_text)
        if key in self._cache_map:
            return self._cache_map[key]

        try:
            from google import genai
            from google.genai import types as _gt

            client = genai.Client(api_key=self._api_key)
            cache_resp = await asyncio.to_thread(
                client.caches.create,
                model=model_id,
                config=_gt.CreateCachedContentConfig(
                    system_instruction=system_text,
                    ttl=self._ttl,
                    display_name=f"gemini_exp_{key}",
                ),
            )
            cache_name = cache_resp.name
            self._cache_map[key] = cache_name
            logger.debug("Created cache %s for model %s (hash=%s)", cache_name, model_id, key)
            return cache_name
        except Exception as e:
            logger.debug("Cache creation failed (non-fatal): %s", e)
            return None

    async def cleanup(self) -> None:
        """Delete all managed caches."""
        if not self._cache_map:
            return
        try:
            from google import genai
            client = genai.Client(api_key=self._api_key)
            for cache_name in self._cache_map.values():
                try:
                    await asyncio.to_thread(client.caches.delete, name=cache_name)
                    logger.debug("Deleted cache %s", cache_name)
                except Exception:
                    pass
        except Exception:
            pass
        self._cache_map.clear()


# ─── Main async runner ────────────────────────────────────────────────────────

async def run_gemini_direct_block_batch(
    messages: list[dict],
    input_block_ids: list[str],
    *,
    batch_id: int = 0,
    model_id: str = "google/gemini-2.5-flash",
    tier: str = "standard",
    api_key: str | None = None,
    cache_manager: GeminiCacheManager | None = None,
    cache_name: str | None = None,
    max_retries: int = 3,
    timeout: int = 600,
) -> GeminiBlockBatchResult:
    """Run one block batch through the direct Gemini Developer API.

    Args:
        messages:          OpenRouter-format messages list.
        input_block_ids:   Expected block IDs in the response (for completeness check).
        batch_id:          Numeric batch identifier.
        model_id:          Model (OpenRouter or native Gemini format).
        tier:              "standard" or "flex" (affects cost accounting).
        api_key:           Gemini API key; falls back to GEMINI_DIRECT_API_KEY env var.
        cache_manager:     Shared GeminiCacheManager instance for system prompt caching.
        cache_name:        Pre-created cache name to use (overrides cache_manager).
        max_retries:       Maximum retry attempts.
        timeout:           Per-request timeout in seconds.

    Returns:
        GeminiBlockBatchResult with full telemetry.
    """
    try:
        from google import genai
        from google.genai import types as _gt
    except ImportError as exc:
        return GeminiBlockBatchResult(
            batch_id=batch_id, model_id=model_id, is_error=True,
            error_type="provider",
            error_message=f"google-genai not installed: {exc}",
            input_block_ids=input_block_ids,
        )

    key = api_key or os.environ.get("GEMINI_DIRECT_API_KEY", "")
    if not key:
        return GeminiBlockBatchResult(
            batch_id=batch_id, model_id=model_id, is_error=True,
            error_type="provider",
            error_message="GEMINI_DIRECT_API_KEY not set",
            input_block_ids=input_block_ids,
        )

    native_model = resolve_direct_model_id(model_id)
    schema = _load_gemini_schema()
    system_text, user_contents = openrouter_messages_to_gemini(messages)

    client = genai.Client(api_key=key)

    # ── Thinking config ───────────────────────────────────────────────────────
    native_base = native_model.split("-preview")[0].split("-exp")[0]
    if native_base in _THINKING_MODELS or any(m in native_model for m in _THINKING_MODELS):
        if "flash" in native_model and "pro" not in native_model:
            thinking_cfg = _gt.ThinkingConfig(thinking_budget=0)
        else:
            thinking_cfg = _gt.ThinkingConfig(thinking_budget=512)
    else:
        thinking_cfg = None

    # ── Preflight token count ─────────────────────────────────────────────────
    predicted_prompt_tokens = 0
    try:
        count_resp = await asyncio.to_thread(
            client.models.count_tokens,
            model=native_model,
            contents=user_contents,
            config=_gt.CountTokensConfig(system_instruction=system_text) if system_text else None,
        )
        predicted_prompt_tokens = getattr(count_resp, "total_tokens", 0) or 0
        logger.debug("[batch %03d] Predicted prompt tokens: %d", batch_id, predicted_prompt_tokens)
    except Exception as e:
        logger.debug("[batch %03d] countTokens failed (non-fatal): %s", batch_id, e)

    # ── Resolve context cache ─────────────────────────────────────────────────
    active_cache_name = cache_name
    if active_cache_name is None and cache_manager is not None and system_text:
        active_cache_name = await cache_manager.get_or_create(native_model, system_text)

    # ── Retry loop ────────────────────────────────────────────────────────────
    last_error_type = "provider"
    last_error_msg = "Max retries exhausted"
    retry_count = 0

    for attempt in range(1, max_retries + 1):
        t0 = time.monotonic()
        try:
            if active_cache_name:
                gen_config = _gt.GenerateContentConfig(
                    cached_content=active_cache_name,
                    response_mime_type="application/json",
                    response_schema=schema,
                    **({"thinking_config": thinking_cfg} if thinking_cfg else {}),
                    max_output_tokens=65536,
                    temperature=0.2,
                )
            else:
                gen_config = _gt.GenerateContentConfig(
                    system_instruction=system_text or None,
                    response_mime_type="application/json",
                    response_schema=schema,
                    **({"thinking_config": thinking_cfg} if thinking_cfg else {}),
                    max_output_tokens=65536,
                    temperature=0.2,
                )

            response = await asyncio.to_thread(
                client.models.generate_content,
                model=native_model,
                contents=user_contents,
                config=gen_config,
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)

        except Exception as exc:
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            err = str(exc)
            retry_count = attempt

            if any(x in err for x in ("429", "RESOURCE_EXHAUSTED", "quota")):
                last_error_type = "provider"
                last_error_msg = f"Rate limit (429): {err[:300]}"
                wait = min(120, 2 ** attempt * 10)
            elif any(x in err.lower() for x in ("timeout", "deadline")):
                last_error_type = "timeout"
                last_error_msg = f"Timeout: {err[:300]}"
                wait = min(60, 2 ** attempt * 5)
            elif any(x in err for x in ("503", "502", "UNAVAILABLE", "INTERNAL")):
                last_error_type = "provider"
                last_error_msg = f"Server error: {err[:300]}"
                wait = min(60, 2 ** attempt * 5)
            else:
                last_error_type = "provider"
                last_error_msg = f"Exception: {err[:300]}"
                wait = min(30, 2 ** attempt * 3)

            logger.warning(
                "[batch %03d] attempt %d/%d %s: %s",
                batch_id, attempt, max_retries, last_error_type, last_error_msg[:120],
            )
            if attempt < max_retries:
                await asyncio.sleep(wait)
            continue

        # ── Extract usage metadata ────────────────────────────────────────────
        usage = getattr(response, "usage_metadata", None)
        prompt_tokens  = int(getattr(usage, "prompt_token_count", 0) or 0)
        output_tokens  = int(getattr(usage, "candidates_token_count", 0) or 0)
        thought_tokens = int(getattr(usage, "thoughts_token_count", 0) or 0)
        cached_tokens  = int(getattr(usage, "cached_content_token_count", 0) or 0)
        total_tokens   = prompt_tokens + output_tokens + thought_tokens

        cost = estimate_gemini_direct_cost(
            native_model, prompt_tokens, output_tokens,
            thought_tokens=thought_tokens,
            cached_tokens=cached_tokens,
            tier=tier,
        )

        # ── Extract text ──────────────────────────────────────────────────────
        raw_text = ""
        candidates = getattr(response, "candidates", None) or []
        if candidates:
            cand = candidates[0]
            content_obj = getattr(cand, "content", None)
            if content_obj:
                raw_parts = getattr(content_obj, "parts", []) or []
                raw_text = "".join(
                    getattr(p, "text", "") or ""
                    for p in raw_parts
                    if hasattr(p, "text") and not getattr(p, "thought", False)
                )

        if not raw_text:
            last_error_type = "completeness"
            last_error_msg = "Empty response from model"
            retry_count = attempt
            if attempt < max_retries:
                await asyncio.sleep(min(30, 2 ** attempt * 2))
            continue

        # ── Parse JSON ────────────────────────────────────────────────────────
        parsed_data = _try_parse_json(raw_text)
        if parsed_data is None:
            last_error_type = "schema"
            last_error_msg = f"JSON parse failed. Raw[:200]: {raw_text[:200]!r}"
            retry_count = attempt
            if attempt < max_retries:
                await asyncio.sleep(min(30, 2 ** attempt * 2))
            continue

        # ── Completeness check ────────────────────────────────────────────────
        missing, duplicates, extra, inferred, inferred_count = check_completeness(
            input_block_ids, parsed_data
        )
        returned_ids = [
            str(a.get("block_id", "")).strip()
            for a in parsed_data.get("block_analyses", [])
            if isinstance(a, dict)
        ]

        return GeminiBlockBatchResult(
            batch_id=batch_id,
            model_id=native_model,
            tier=tier,
            raw_text=raw_text,
            parsed_data=parsed_data,
            is_error=False,
            retry_count=attempt - 1,
            predicted_prompt_tokens=predicted_prompt_tokens,
            prompt_tokens=prompt_tokens,
            output_tokens=output_tokens,
            thought_tokens=thought_tokens,
            cached_tokens=cached_tokens,
            total_tokens=total_tokens,
            cost_usd=cost,
            duration_ms=elapsed_ms,
            input_block_ids=input_block_ids,
            returned_block_ids=returned_ids,
            missing_block_ids=missing,
            duplicate_block_ids=duplicates,
            extra_block_ids=extra,
            block_id_inferred_from_input=inferred,
            inferred_block_id_count=inferred_count,
            cache_name=active_cache_name,
            cache_hit=cached_tokens > 0,
        )

    # All retries exhausted
    return GeminiBlockBatchResult(
        batch_id=batch_id,
        model_id=native_model,
        tier=tier,
        is_error=True,
        error_type=last_error_type,
        error_message=last_error_msg,
        retry_count=retry_count,
        input_block_ids=input_block_ids,
        cache_name=active_cache_name,
    )


# ─── JSON parsing helpers ─────────────────────────────────────────────────────

def _try_parse_json(text: str) -> dict | None:
    """Try to parse JSON from text, including markdown-wrapped variants."""
    import re

    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    md = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    if md:
        try:
            return json.loads(md.group(1))
        except json.JSONDecodeError:
            pass

    brace = re.search(r"\{.*\}", text, re.DOTALL)
    if brace:
        try:
            return json.loads(brace.group(0))
        except json.JSONDecodeError:
            pass

    return None


# ─── Production integration helper ───────────────────────────────────────────

async def run_block_batch_gemini_direct(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
    *,
    model_id: str = "google/gemini-2.5-flash",
    tier: str = "standard",
    api_key: str | None = None,
    cache_manager: GeminiCacheManager | None = None,
) -> tuple[int, str, GeminiBlockBatchResult]:
    """Production-integration wrapper: build messages + run Gemini Direct for one batch.

    Returns (exit_code, raw_text, result) to match the claude_runner tuple convention.
    """
    from webapp.services import prompt_builder

    batch_id = batch_data.get("batch_id", 0)
    input_block_ids = [b["block_id"] for b in batch_data.get("blocks", [])]

    messages = prompt_builder.build_block_batch_messages(
        batch_data, project_info, project_id, total_batches
    )

    result = await run_gemini_direct_block_batch(
        messages,
        input_block_ids,
        batch_id=batch_id,
        model_id=model_id,
        tier=tier,
        api_key=api_key,
        cache_manager=cache_manager,
    )

    if not result.is_error and result.parsed_data:
        from webapp.services.claude_runner import _resolve_output_dir, _write_json
        output_path = _resolve_output_dir(project_id) / f"block_batch_{batch_id:03d}.json"
        _write_json(output_path, result.parsed_data)

    exit_code = 0 if not result.is_error else 1
    return exit_code, result.raw_text, result
