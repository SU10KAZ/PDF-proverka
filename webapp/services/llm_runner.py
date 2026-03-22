"""
Единый клиент OpenRouter для GPT-5.4 и Gemini 3.1 Pro.

Использует OpenAI SDK с base_url=openrouter.ai.
Старый Claude CLI пайплайн НЕ затрагивается.
"""
import asyncio
import base64
import json
import logging
import time
from pathlib import Path

from openai import AsyncOpenAI, RateLimitError, APITimeoutError, APIError

from webapp.config import (
    OPENROUTER_API_KEY, OPENROUTER_BASE_URL,
    OPENROUTER_SITE_URL, OPENROUTER_SITE_NAME,
    STAGE_MODELS_OPENROUTER, GEMINI_MAX_OUTPUT_TOKENS, GPT_MAX_OUTPUT_TOKENS,
    DEFAULT_TEMPERATURE, SCHEMAS_DIR,
    get_stage_model,
)
from webapp.models.usage import LLMResult

logger = logging.getLogger(__name__)

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


async def run_llm(
    stage: str,
    messages: list[dict],
    response_format: dict | None = None,
    temperature: float | None = None,
    timeout: int = 600,
    max_retries: int = 3,
) -> LLMResult:
    """Единый вызов LLM через OpenRouter.

    Args:
        stage: ключ этапа конвейера (text_analysis, block_batch, findings_merge и т.д.)
        messages: список сообщений [{role, content}, ...]
        response_format: формат ответа (по умолчанию json_object)
        temperature: температура генерации (по умолчанию из config)
        timeout: таймаут запроса в секундах
        max_retries: макс. число повторов при rate limit / timeout

    Returns:
        LLMResult с текстом, распарсенным JSON, токенами и метриками.
    """
    # Нормализация: block_batch_001 -> block_batch
    stage_key = stage
    if stage.startswith("block_batch"):
        stage_key = "block_batch"

    model = get_stage_model(stage_key)
    max_tokens = (
        GEMINI_MAX_OUTPUT_TOKENS if "gemini" in model
        else GPT_MAX_OUTPUT_TOKENS
    )
    temp = temperature if temperature is not None else DEFAULT_TEMPERATURE
    client = _get_client()

    for attempt in range(1, max_retries + 1):
        start = time.monotonic()
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temp,
                response_format=response_format or {"type": "json_object"},
                timeout=timeout,
                extra_headers={
                    "HTTP-Referer": OPENROUTER_SITE_URL,
                    "X-Title": OPENROUTER_SITE_NAME,
                },
            )
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

        # Парсинг JSON
        json_data = None
        try:
            json_data = json.loads(content)
        except json.JSONDecodeError:
            pass

        # Usage
        usage = response.usage
        input_tokens = usage.prompt_tokens if usage else 0
        output_tokens = usage.completion_tokens if usage else 0

        return LLMResult(
            text=content,
            json_data=json_data,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=0.0,
            duration_ms=elapsed_ms,
            model=model,
        )

    # Safety net (shouldn't reach here)
    return LLMResult(
        text="", is_error=True,
        error_message="Max retries exhausted",
        model=model,
    )


def make_image_content(image_path: str | Path, detail: str = "high") -> dict:
    """PNG -> base64 content block для multimodal сообщений.

    Args:
        image_path: путь к PNG-файлу
        detail: уровень детализации ("high" или "low")

    Returns:
        dict с type=image_url для включения в messages content.
    """
    b64 = base64.b64encode(Path(image_path).read_bytes()).decode()
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
) -> list[dict]:
    """Interleaved text<->PNG по страницам.

    Формирует массив content-блоков для user message:
    - Текстовый контекст страницы перед первым блоком этой страницы
    - PNG блока (base64)
    - Текстовая метка блока (block_id + ocr_label)

    Args:
        blocks: список блоков из batch_data["blocks"]
        page_contexts: {page_num: context_text} из document_graph
        project_dir: корневая папка проекта (resolve_project_dir)

    Returns:
        Список content-блоков для user message.
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
            content.append(make_image_content(block_path))

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
