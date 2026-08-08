"""
Единый клиент LLM для OpenRouter.

Локальные LLM-мощности (LM Studio за ngrok / 01.vibe) с платформы удалены —
единственный сетевой транспорт здесь OpenRouter. Старый Claude CLI пайплайн
НЕ затрагивается.
"""
import asyncio
import base64
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, TYPE_CHECKING


# Ленивый импорт openai: модуль не должен падать при импорте, если пакет не
# установлен (например, в окружениях, где Claude CLI используется без LLM-runner).
# Реальный импорт выполняется только при первом обращении к API через
# _get_client(). Для except-блоков используется _openai_exc() — он либо
# возвращает классы исключений openai, либо безопасный fallback-tuple.
if TYPE_CHECKING:
    from openai import AsyncOpenAI


def _import_openai():
    """Ленивый импорт openai с понятным сообщением, если пакета нет."""
    try:
        import openai as _openai  # noqa: F401
        from openai import (
            AsyncOpenAI,
            RateLimitError,
            APITimeoutError,
            APIError,
        )
        return AsyncOpenAI, RateLimitError, APITimeoutError, APIError
    except ImportError as e:
        raise RuntimeError(
            "Пакет 'openai' не установлен, но requested LLM-runner call requires it. "
            "Установите `pip install openai` или используйте только Claude CLI-пайплайн."
        ) from e


try:
    # Получаем классы исключений на уровне модуля. Если openai не установлен,
    # except-блоки получат no-op заглушки, которые никогда не сработают —
    # это безопасно, потому что openai-функции в этом случае всё равно не
    # запустятся (фейл произойдёт на _import_openai() в _get_client()).
    from openai import (
        RateLimitError as RateLimitError,
        APITimeoutError as APITimeoutError,
        APIError as APIError,
    )
except ImportError:
    class _OpenAIUnavailable(Exception):
        """No-op заглушка для except-блоков, когда openai не установлен."""
        pass

    RateLimitError = _OpenAIUnavailable  # type: ignore[assignment,misc]
    APITimeoutError = _OpenAIUnavailable  # type: ignore[assignment,misc]
    APIError = _OpenAIUnavailable  # type: ignore[assignment,misc]


from backend.app.core.config import (
    OPENROUTER_API_KEY, OPENROUTER_BASE_URL,
    OPENROUTER_SITE_URL, OPENROUTER_SITE_NAME,
    STAGE_MODELS_OPENROUTER, GEMINI_MAX_OUTPUT_TOKENS, GPT_MAX_OUTPUT_TOKENS,
    DEFAULT_TEMPERATURE, SCHEMAS_DIR,
    get_stage_model,
)
from backend.app.models.usage import LLMResult
from backend.app.services.llm.paid_api_guard import (
    PaidApiBlockedError,
    PaidApiContext,
    assert_paid_api_allowed,
    release_reservation,
    reserve_paid_api,
)
from backend.app.services.llm import paid_api_events

logger = logging.getLogger(__name__)

# Sentinel: "не задано" (отличает от явного None = "без формата")
_UNSET = object()
# Единый клиент -- создаётся лениво (чтобы не падать при импорте без ключа)
_client: "AsyncOpenAI | None" = None


def _get_client() -> "AsyncOpenAI":
    global _client
    if _client is None:
        AsyncOpenAI, _, _, _ = _import_openai()
        _client = AsyncOpenAI(
            base_url=OPENROUTER_BASE_URL,
            api_key=OPENROUTER_API_KEY,
        )
    return _client


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


# Цены моделей OpenRouter ($/1M токенов) — обновлять при изменении
# Fallback only: если в response.usage пришла usage.cost — используется она.
# Встроенные цены (USD за 1M токенов) — fallback, если data/model_prices.json
# недоступен. #75: основной источник — конфиг-файл, чтобы цены правились без
# редактирования кода.
_MODEL_PRICES_BUILTIN = {
    "google/gemini-2.5-pro":          {"input": 1.25,  "output": 10.0},
    "google/gemini-2.5-flash":        {"input": 0.30,  "output": 2.50},
    "google/gemini-3.1-pro-preview":  {"input": 2.0,   "output": 12.0},
    "anthropic/claude-opus-5":        {"input": 5.0,   "output": 25.0},
    "anthropic/claude-sonnet-5":      {"input": 3.0,   "output": 15.0},
    "anthropic/claude-opus-4-7":      {"input": 15.0,  "output": 75.0},
    "anthropic/claude-sonnet-4-6":    {"input": 3.0,   "output": 15.0},
    "openai/gpt-5.4":                {"input": 2.50,  "output": 15.0},
    "openai/gpt-4.1":               {"input": 2.00,  "output": 8.0},
}


def _load_model_prices() -> dict:
    """#75: загрузить цены из data/model_prices.json (fallback — встроенные)."""
    path = Path(__file__).resolve().parents[2] / "data" / "model_prices.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        prices = {
            k: v for k, v in data.items()
            if isinstance(v, dict) and "input" in v and "output" in v
        }
        if prices:
            return prices
    except (OSError, json.JSONDecodeError):
        pass
    return dict(_MODEL_PRICES_BUILTIN)


_MODEL_PRICES = _load_model_prices()
_WARNED_UNKNOWN_PRICE_MODELS: set[str] = set()


def _estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Оценить стоимость запроса на основе токенов и цен модели."""
    prices = _MODEL_PRICES.get(model)
    if not prices:
        # #75: неизвестная цена → под-учёт (cost=0.0). Делаем это наблюдаемым:
        # warning один раз на модель, чтобы не спамить лог.
        if model and model not in _WARNED_UNKNOWN_PRICE_MODELS:
            _WARNED_UNKNOWN_PRICE_MODELS.add(model)
            logger.warning(
                "[cost] неизвестная цена для модели '%s' → стоимость считается 0.0 "
                "(под-учёт). Добавьте её в backend/app/data/model_prices.json.",
                model,
            )
        return 0.0
    cost = (input_tokens * prices["input"] + output_tokens * prices["output"]) / 1_000_000
    return round(cost, 6)


def _estimate_input_tokens(messages: list[dict]) -> int:
    """Грубая оценка входных токенов до запроса (~4 символа на токен).

    Учитывает как строковый, так и multimodal content (берём только текстовые
    части — картинки в daily-limit оценке игнорируем, их токены непредсказуемы)."""
    total_chars = 0
    for m in messages or []:
        c = m.get("content")
        if isinstance(c, str):
            total_chars += len(c)
        elif isinstance(c, list):
            for part in c:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    total_chars += len(part["text"])
    return total_chars // 4


def _estimate_request_cost(model: str, messages: list[dict], max_tokens: int) -> float:
    """#73: консервативная оценка стоимости запроса для резервирования бюджета.

    Output считаем по max_tokens (верхняя граница), input — по длине сообщений.
    Завышение безопасно: резервируем «не больше потолка», реальный cost из
    record_paid обычно меньше."""
    return _estimate_cost(model, _estimate_input_tokens(messages), int(max_tokens or 0))


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
    project_id: str = "",
    version_id: str = "",
    job_id: str = "",
    source: str = "llm_runner",
) -> LLMResult:
    """Единый вызов LLM через OpenRouter.

    Args:
        stage: ключ этапа конвейера (text_analysis, block_batch, findings_merge и т.д.)
        messages: список сообщений [{role, content}, ...]
        response_format: формат ответа (по умолчанию json_object)
        temperature: температура генерации (по умолчанию из config)
        timeout: таймаут запроса в секундах
        max_retries: макс. число повторов при rate limit / timeout
        model_override: явная модель (если задана — игнорирует stage config)
        project_id/version_id/job_id/source: контекст для paid_api_guard.
            Если PAID_API_ENABLED=false или превышен daily limit, внешний
            платный вызов будет заблокирован.

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

    # ─── Paid API guard: проверка ДО network request ────────────────────
    # Все вызовы идут во внешних платных провайдеров (OpenRouter и т.п.).
    paid_ctx = PaidApiContext(
        source=source or "llm_runner",
        model=model,
        project_id=project_id,
        version_id=version_id,
        stage=stage_key,
        job_id=job_id,
        estimated_cost_usd=_estimate_request_cost(model, messages, max_tokens),
    )
    # #73: резервируем оценку под локом — конкурентные вызовы не перебирают лимит.
    reservation = None
    try:
        reservation = reserve_paid_api(paid_ctx)
    except PaidApiBlockedError as e:
        return LLMResult(
            text="",
            is_error=True,
            error_message=f"paid_api_blocked: {e.reason}",
            model=model,
        )

    # #73: освобождаем резервацию на ЛЮБОМ выходе из функции после reserve.
    # TTL в guard самозалечивает пропущенный release, но явный — точнее.
    def _done(result: LLMResult) -> LLMResult:
        release_reservation(reservation)
        return result

    # _get_client()/extra_body вне retry-цикла → их исключение раньше уходило
    # МИМО _done() и подвешивало резервацию до TTL (pre-deploy review). Оборачиваем,
    # чтобы release был гарантирован на ВСЕХ путях.
    try:
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
    except Exception as e:  # noqa: BLE001
        return _done(LLMResult(text="", is_error=True, error_message=str(e), model=model))

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
            return _done(LLMResult(
                text="", is_error=True,
                error_message=f"Rate limit after {max_retries} retries: {e}",
                model=model,
            ))
        except APITimeoutError as e:
            if attempt < max_retries:
                # #75: backoff перед повтором (как у RateLimitError) — без паузы
                # ретраи били в тот же таймаут-шторм.
                wait = min(60, 2 ** attempt * 5)
                logger.warning(
                    "[%s] Timeout (attempt %d/%d), waiting %ds: %s",
                    stage, attempt, max_retries, wait, e,
                )
                await asyncio.sleep(wait)
                continue
            return _done(LLMResult(
                text="", is_error=True,
                error_message=f"Timeout after {max_retries} retries: {e}",
                model=model,
            ))
        except APIError as e:
            return _done(LLMResult(
                text="", is_error=True,
                error_message=f"API error: {e}",
                model=model,
            ))
        except Exception as e:
            return _done(LLMResult(
                text="", is_error=True,
                error_message=str(e),
                model=model,
            ))

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

        # Учёт платных вызовов (OpenRouter возвращает actual cost).
        # Claude CLI не идёт через этот путь, поэтому каждый ненулевой cost
        # здесь — реальный платный API.
        # Единый helper paid_cost_tracker.record_paid гарантирует, что paid_cost.json
        # и paid_cost_events.jsonl увеличиваются вместе. Раньше эти две записи
        # были независимыми вызовами, что привело к расхождению 9 vs 15 в инциденте.
        if cost > 0:
            try:
                from backend.app.services.common.usage_service import paid_cost_tracker as _paid
                _paid.record_paid(
                    cost,
                    model=model,
                    project_id=project_id,
                    stage=stage_key,
                    source=source or "llm_runner",
                    job_id=job_id,
                    version_id=version_id,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    response_id=getattr(response, "id", "") or "",
                )
            except Exception:
                logger.exception("paid_cost_tracker.record_paid failed")

        finish_reason = ""
        try:
            finish_reason = response.choices[0].finish_reason or ""
        except Exception:
            finish_reason = ""

        return _done(LLMResult(
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
        ))

    # Safety net (shouldn't reach here)
    return _done(LLMResult(
        text="", is_error=True,
        error_message="Max retries exhausted",
        model=model,
    ))


from typing import AsyncGenerator


async def run_llm_stream(
    messages: list[dict],
    model_override: str,
    temperature: float | None = None,
    timeout: int = 120,
    *,
    project_id: str = "",
    stage: str = "discussion",
    version_id: str = "",
    job_id: str = "",
    source: str = "llm_runner.stream",
) -> AsyncGenerator[dict, None]:
    """Стриминг ответа через OpenRouter (SSE).

    Yields:
        {"type": "delta", "text": "..."} — фрагмент текста
        {"type": "done", "text": "...", "input_tokens": N, "output_tokens": N, "cost_usd": F}
        {"type": "error", "message": "..."} — в т.ч. paid_api_blocked
    """
    model = model_override
    # ─── Paid API guard: проверка ДО network request ────────────────────
    max_tokens = (
        GEMINI_MAX_OUTPUT_TOKENS if "gemini" in model
        else GPT_MAX_OUTPUT_TOKENS
    )
    temp = temperature if temperature is not None else DEFAULT_TEMPERATURE

    paid_ctx = PaidApiContext(
        source=source or "llm_runner.stream",
        model=model,
        project_id=project_id,
        version_id=version_id,
        stage=stage or "discussion",
        job_id=job_id,
        estimated_cost_usd=_estimate_request_cost(model, messages, max_tokens),
    )
    # #73: резервируем оценку, чтобы стрим-вызовы тоже учитывались в лимите.
    reservation = None
    try:
        reservation = reserve_paid_api(paid_ctx)
    except PaidApiBlockedError as e:
        yield {"type": "error", "message": f"paid_api_blocked: {e.reason}"}
        return

    full_text = ""
    input_tokens = 0
    output_tokens = 0

    # try/finally гарантирует release резервации на всех путях (включая error,
    # сбой _get_client() и преждевременный обрыв генератора потребителем).
    try:
        client = _get_client()
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
        if cost > 0:
            # Единый helper: paid_cost.json + paid_cost_events.jsonl одной записью.
            try:
                from backend.app.services.common.usage_service import paid_cost_tracker as _paid
                _paid.record_paid(
                    cost,
                    model=model,
                    project_id=project_id,
                    stage=stage,
                    source=source or "llm_runner.stream",
                    job_id=job_id,
                    version_id=version_id,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                )
            except Exception:
                logger.exception("paid_cost_tracker.record_paid failed (stream)")
        yield {
            "type": "done",
            "text": full_text,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost_usd": cost,
        }
    finally:
        release_reservation(reservation)


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
            уменьшается (LANCZOS) — fallback для моделей, отвергающих
            слишком большие изображения.
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

    image_scale<1.0 — ресайз PNG блоков перед base64 (fallback для моделей
    с ограничением на размер изображения).
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
