"""
gemma_enrich.py
--------------
Production-модуль Gemma-multimodal enrichment image-блоков с augment-merge в MD.

Логика:
1. Гарантирует base crops в `_output/blocks_gemma_100/`.
2. Прогоняет base Gemma pass по всем image-блокам на 100 DPI.
3. Выбирает кандидатов на targeted high-detail retry и, если нужно,
   доготавливает 300 DPI crops в `_output/blocks_gemma_300/`.
4. Открывает MD-файл проекта и для каждого `### BLOCK [IMAGE]: <id>`
   вставляет ЛУЧШИЙ итоговый enrichment (high-detail или base).
4. Делает backup MD: `*_document.md.pre_enrichment.bak` (один, первый — не перезаписывает).
5. Записывает MD-маркер первой строкой:
   `<!-- ENRICHMENT: gemma3.6-35b @ <ts> blocks=42/45 ok -->`

Использование:
    # CLI
    python gemma_enrich.py projects/<name>
    python gemma_enrich.py projects/<name> --force      # перезатереть
    python gemma_enrich.py projects/<name> --parallelism 3

    # Программно
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrich import enrich_project
    asyncio.run(enrich_project(Path("projects/<name>")))

ENV (из .env):
    CHANDRA_BASE_URL   — ngrok-tunnel до lm-studio
    NGROK_AUTH_USER    — Basic Auth username
    NGROK_AUTH_PASS    — Basic Auth password
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import os
import re
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import httpx
from dotenv import load_dotenv
from PIL import Image

from backend.app.pipeline.stages.crop_blocks.block_markdown import extract_block_sections, strip_enrichment_in_block
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    ENRICHMENT_MARKER_RE,
    GEMMA_BASE_BLOCKS_DIRNAME,
    GEMMA_BASE_PROFILE,
    GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME,
    GEMMA_HIGH_DETAIL_PROFILE,
    GEMMA_HIGH_DETAIL_CROP_POLICY,
    WARNING_LARGE_BLOCK,
    blocks_index_hash,
    build_gemma_summary,
    crop_index_matches_policy,
    load_json,
    gemma_base_blocks_dir,
    gemma_base_blocks_index_path,
    gemma_base_crop_policy,
    gemma_blocks_dir,
    gemma_blocks_index_path,
    gemma_high_detail_blocks_dir,
    gemma_high_detail_blocks_index_path,
    gemma_high_detail_crop_policy,
    strip_gemma_enrichment,
    utc_now_iso,
    validate_gemma_summary,
)

from backend.app.core.config import ROOT_DIR as _ROOT_DIR
load_dotenv(_ROOT_DIR / ".env")


# ─── Constants ─────────────────────────────────────────────────────────────

DEFAULT_MODEL = "google/gemma-4-26b-a4b"
DEFAULT_PARALLELISM = 1
HIGH_DETAIL_PARALLELISM = 1
# context_length для двух проходов (адаптивный reload)
BASE_CONTEXT_LENGTH = 16000
HIGH_DETAIL_CONTEXT_LENGTH = 16000
DEFAULT_TIMEOUT_S = 300
# Gemma3 с reasoning тратит ~1500-2000 токенов на размышления до JSON.
# 2048 обрезало reasoning и JSON не успевал дописаться → шаблон в MD.
DEFAULT_MAX_OUTPUT_TOKENS = 8192
# При обрыве (truncated) автоматически повторяем со следующим тиром.
RETRY_OUTPUT_TOKEN_TIERS = [DEFAULT_MAX_OUTPUT_TOKENS, 16384]
# После основного прохода — повторный прогон упавших блоков. Лотерейные сбои
# модели (битый JSON в середине, не truncation) часто проходят со 2-3 попытки.
# Итого максимум 3 попытки на блок: 1 исходная + 2 retry-pass.
PROJECT_RETRY_PASSES = 2
# Split-fallback: для очень вытянутых блоков (разрезы лестниц и т.п.) модель
# зацикливается на повторах. Если truncated на всех тирах И aspect выше порога —
# режем картинку пополам по высоте и обрабатываем половины отдельно.
SPLIT_ASPECT_THRESHOLD = 2.5  # height/width >= 2.5 → split-eligible
SPLIT_PARTS = 2  # на сколько кусков делим (тестирование показало что 2 хватает)
# Максимальная длинная сторона при отправке в Gemma. Блоки крупнее обрезаются до этого размера
# независимо от scale-тира. Исключает ситуацию «5400px блок на scale=1.0» → битый JSON.
MAX_INPUT_LONG_SIDE_PX = 1500
HIGH_DETAIL_MAX_SIZE_KB = 300
HIGH_DETAIL_MAX_LONG_SIDE_PX = 3500
HIGH_DETAIL_MAX_IMAGE_TOKENS = 3500
REASONING_TAIL_MAX_CHARS = 1000
SHORT_RESULT_THRESHOLD = 60

_IMAGE_SCALE_TIERS = [1.0, 0.6, 0.35, 0.2]
_NGROK_HTML_RETRIES = 2
_NGROK_HTML_BACKOFF_S = 1.5
_WEAK_RESULT_MARKERS = (
    "unreadable",
    "нечитаемо",
    "неразборчиво",
    "blurred",
    "cannot read",
    "text too small",
    "partially readable",
)
_DETAIL_TEXT_HINTS = (
    "табл",
    "table",
    "специф",
    "ведомост",
    "экспликац",
    "schedule",
    "legend",
    "detail",
)


# ─── Prompts ───────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Ты — инженер. Извлеки факты из блока чертежа: что нарисовано, марки, размеры, ссылки.
Отвечай строго JSON по схеме, без преамбулы и markdown. Не ищи ошибок. Не выдумывай.
Если поле не видно — [] или null.
"""

USER_INSTRUCTION = """JSON по схеме (максимум 15 элементов в каждом массиве, ответ до 600 токенов):
{{
"block_type":"план|план_армирования|план_опалубки|разрез|сечение|ведомость|схема|таблица|узел|другое",
"subject":"до 120 симв — что изображено",
"marks":["марки/позиции"],
"rebar_specs":["параметры арматуры"],
"dimensions":["размеры"],
"references_on_block":["ссылки на листы/разрезы"],
"axes":["оси"],
"level_marks":["до 5 ключевых отметок"],
"concrete_class":"класс бетона|null",
"notes":"1-2 предл — контекст без картинки"
}}

block_id={block_id} page={page} sheet={sheet_no} ocr="{ocr_label}"

Текст со страницы (подсказка, не копируй):
---
{page_text}
---
"""


# ─── Helpers ───────────────────────────────────────────────────────────────

def _env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"Переменная окружения {name} не установлена (см. .env). "
            f"Требуются: CHANDRA_BASE_URL, NGROK_AUTH_USER, NGROK_AUTH_PASS"
        )
    return val


def _auth_header() -> dict[str, str]:
    user = _env("NGROK_AUTH_USER")
    pwd = _env("NGROK_AUTH_PASS")
    token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "ngrok-skip-browser-warning": "true",
        "content-type": "application/json",
    }


def _png_to_data_url(path: Path, scale: float = 1.0) -> str:
    img = Image.open(path)
    long_side = max(img.width, img.height)
    # Ограничиваем длинную сторону: кап применяется ДО scale-множителя.
    # Без этого блоки >5000px отправляются в Gemma целиком и ломают JSON на ~450 символе.
    cap_scale = MAX_INPUT_LONG_SIDE_PX / long_side if long_side > MAX_INPUT_LONG_SIDE_PX else 1.0
    effective_scale = scale * cap_scale
    if abs(effective_scale - 1.0) < 0.01:
        data = path.read_bytes()
    else:
        new_size = (max(1, int(img.width * effective_scale)), max(1, int(img.height * effective_scale)))
        img = img.resize(new_size, Image.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="PNG", optimize=True)
        data = buf.getvalue()
    b64 = base64.b64encode(data).decode()
    return f"data:image/png;base64,{b64}"


def _is_invalid_image_error(data: dict | None, raw: str) -> bool:
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict) and "Invalid image" in str(err.get("message", "")):
            return True
    return "Invalid image" in (raw or "")


def _is_context_exceeded_error(data: dict | None, raw: str) -> bool:
    """Сервер gemma-VL отклонил запрос: input не лезет в n_ctx (типично 4096).
    Сообщение вида: 'request (4570 tokens) exceeds the available context size (4096 tokens)'.
    Лечится только уменьшением картинки (page_text уже capped до 4000 chars)."""
    msg = ""
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            msg = str(err.get("message", ""))
    haystack = f"{msg}\n{raw or ''}"
    return "exceeds the available context" in haystack or "context length" in haystack


def _is_ngrok_html(data: dict | None, raw: str) -> bool:
    if data is not None:
        return False
    head = (raw or "")[:500].lower()
    return head.lstrip().startswith("<!doctype") or "<html" in head


def _load_page_text(graph: dict, page: int) -> str:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            parts: list[str] = []
            sheet = p.get("sheet_name") or ""
            if sheet:
                parts.append(f"[SHEET] {sheet}")
            for tb in p.get("text_blocks", [])[:10]:
                txt = (tb.get("text") or "").strip()
                if txt:
                    parts.append(txt[:250])
            return "\n".join(parts)[:800]
    return ""


def _load_sheet_no(graph: dict, page: int) -> str:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            return str(p.get("sheet_no_normalized") or p.get("sheet_no_raw") or "")
    return ""


# ─── Adaptive model reload ──────────────────────────────────────────────────

async def _reload_model_for_high_detail(model: str) -> dict:
    """Unload → reload модели с HIGH_DETAIL_CONTEXT_LENGTH перед high-detail pass.

    Вызывается только если GEMMA_ADAPTIVE_RELOAD_ENABLED=true.
    Возвращает dict с полями ok, context_length, error (если не ok).
    """
    import backend.app.services.llm.lms_service as lms_service

    logger = _get_logger()
    try:
        logger.info("Adaptive reload: unloading %s before high-detail pass", model)
        unloaded = await asyncio.to_thread(lms_service.unload_all_for, model)
        logger.info("Adaptive reload: unloaded %d instance(s)", unloaded)
    except Exception as exc:
        logger.warning("Adaptive reload: unload failed: %s", exc)

    try:
        logger.info(
            "Adaptive reload: loading %s with context_length=%d", model, HIGH_DETAIL_CONTEXT_LENGTH
        )
        result = await asyncio.to_thread(
            lms_service.load_model,
            model,
            context_length=HIGH_DETAIL_CONTEXT_LENGTH,
        )
        lms_service.invalidate_loaded_cache()
        logger.info(
            "Adaptive reload: loaded %s context_length=%d",
            result.get("identifier"), result.get("context_length"),
        )
        return {"ok": True, "context_length": result.get("context_length")}
    except Exception as exc:
        logger.warning("Adaptive reload: load failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def _get_logger():
    import logging
    return logging.getLogger(__name__)


# ─── Gemma call ─────────────────────────────────────────────────────────────

@dataclass
class EnrichResult:
    block_id: str
    page: int
    ok: bool
    elapsed_ms: int = 0
    enrichment: dict | None = None
    error: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_tokens: int | None = None
    truncated: bool = False  # True если обрыв из-за max_output_tokens (Unterminated string)
    response_source: str = "content"
    finish_reason: str = ""
    partial_ok: bool = False


def _coerce_message_text(value) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
                continue
            if not isinstance(item, dict):
                parts.append(str(item))
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
                continue
            content = item.get("content")
            if isinstance(content, str) and content.strip():
                parts.append(content.strip())
        return "\n".join(part for part in parts if part).strip()
    if isinstance(value, dict):
        return _coerce_message_text(value.get("text") or value.get("content") or "")
    return str(value or "").strip()


def _extract_response_texts(data: dict | None) -> tuple[str, str, str]:
    if not isinstance(data, dict):
        return "", "", "error"

    content_parts: list[str] = []
    reasoning_parts: list[str] = []
    finish_reason = ""

    choices = data.get("choices") or []
    if choices and isinstance(choices[0], dict):
        choice = choices[0]
        message = choice.get("message") or {}
        content = _coerce_message_text(message.get("content", ""))
        reasoning = _coerce_message_text(message.get("reasoning_content", ""))
        if content:
            content_parts.append(content)
        if reasoning:
            reasoning_parts.append(reasoning)
        finish_reason = str(choice.get("finish_reason") or "")

    for item in data.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type") or "")
        if item_type == "message":
            content = _coerce_message_text(item.get("content", ""))
            reasoning = _coerce_message_text(item.get("reasoning_content", ""))
            if content:
                content_parts.append(content)
            if reasoning:
                reasoning_parts.append(reasoning)
            if not finish_reason:
                finish_reason = str(item.get("finish_reason") or "")
            continue
        if item_type in {"reasoning", "reasoning_content"}:
            reasoning = _coerce_message_text(item.get("content", ""))
            if reasoning:
                reasoning_parts.append(reasoning)

    content = "\n".join(part for part in content_parts if part).strip()
    reasoning = "\n".join(part for part in reasoning_parts if part).strip()
    if not finish_reason:
        finish_reason = "stop" if (content or reasoning) else "error"
    return content, reasoning, finish_reason


def _extract_reasoning_tail(reasoning_text: str) -> tuple[str, str]:
    text = (reasoning_text or "").strip()
    if not text:
        return "", "empty"

    section_patterns = [
        r"(?is)(final\s*check\s*[:\-]?\s*)(?P<body>.+)$",
        r"(?is)(final\s*answer\s*[:\-]?\s*)(?P<body>.+)$",
        r"(?is)(читаемо\s*[:\-]?\s*)(?P<body>.+)$",
    ]
    for pattern in section_patterns:
        match = re.search(pattern, text)
        if match:
            body = str(match.group("body") or "").strip()
            if body:
                return body[-REASONING_TAIL_MAX_CHARS:].strip(), "section"

    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    structured_tail: list[str] = []
    for line in reversed(lines):
        stripped = line.strip()
        if re.match(r"^[-*]\s+\S", stripped) or re.match(r"^\d+[.)]\s+\S", stripped):
            structured_tail.append(stripped)
            continue
        if structured_tail:
            if ":" in stripped and len(stripped) <= 160:
                structured_tail.append(stripped)
                continue
            break
    if structured_tail:
        structured_tail.reverse()
        return "\n".join(structured_tail)[-REASONING_TAIL_MAX_CHARS:].strip(), "structured_tail"

    return text[-REASONING_TAIL_MAX_CHARS:].strip(), "fallback_tail"


def _empty_enrichment() -> dict[str, object]:
    return {
        "block_type": "другое",
        "subject": "",
        "marks": [],
        "rebar_specs": [],
        "dimensions": [],
        "references_on_block": [],
        "axes": [],
        "level_marks": [],
        "concrete_class": None,
        "notes": "",
    }


def _parse_reasoning_bullets(text: str) -> dict | None:
    """Парсит bullet-список из reasoning когда модель не успела сформировать JSON.

    Формат который пишет Gemma3 в reasoning:
        - `block_type`: "схема"
        - `marks`: ["К1", "К2"]
        - `concrete_class`: null
    """
    field_map = {
        "block_type": "block_type",
        "subject": "subject",
        "marks": "marks",
        "rebar_specs": "rebar_specs",
        "dimensions": "dimensions",
        "references_on_block": "references_on_block",
        "axes": "axes",
        "level_marks": "level_marks",
        "concrete_class": "concrete_class",
        "notes": "notes",
    }
    result: dict = {}
    # Ищем строки вида:   - `field`: value   или   - field: value
    pattern = re.compile(
        r"[-*]\s+`?(\w+)`?\s*:\s*(.+)$", re.MULTILINE
    )
    for m in pattern.finditer(text):
        key = m.group(1).strip()
        raw_val = m.group(2).strip()
        if key not in field_map:
            continue
        # Пробуем распарсить значение как JSON
        try:
            val = json.loads(raw_val)
        except json.JSONDecodeError:
            # Убираем trailing комментарии и пробуем ещё раз
            clean_val = re.sub(r"\s*#.*$", "", raw_val).strip().rstrip(",")
            try:
                val = json.loads(clean_val)
            except json.JSONDecodeError:
                val = raw_val.strip('"').strip("'")
        result[field_map[key]] = val

    if len(result) >= 3:  # минимум 3 поля чтобы считать валидным
        enrichment = _empty_enrichment()
        enrichment.update(result)
        return enrichment
    return None


def _build_textual_fallback_enrichment(text: str) -> dict | None:
    clean = (text or "").strip()
    if not clean:
        return None
    # Сначала пробуем bullet-парсер из reasoning
    bullets = _parse_reasoning_bullets(clean)
    if bullets is not None:
        return bullets
    lines = [line.strip(" -\t") for line in clean.splitlines() if line.strip()]
    if not lines:
        return None
    subject = lines[0][:120]
    enrichment = _empty_enrichment()
    enrichment["subject"] = subject
    enrichment["notes"] = clean[:2000]
    return enrichment


def _extract_json_from_text(text: str) -> dict | None:
    """Ищет первый валидный JSON-объект в тексте (жадный поиск с конца)."""
    # Ищем все вхождения { ... } и пробуем с самого длинного
    matches = list(re.finditer(r"\{", text))
    if not matches:
        return None
    for m in reversed(matches):
        candidate = text[m.start():]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
        # Пробуем найти закрывающую } от этой {
        inner = re.search(r"\{.*\}", candidate, re.DOTALL)
        if inner:
            try:
                return json.loads(inner.group(0))
            except json.JSONDecodeError:
                repaired = _repair_json(inner.group(0))
                if repaired is not None:
                    return repaired
    return None


def _parse_gemma_payload(
    *,
    content_text: str,
    reasoning_text: str,
    finish_reason: str,
) -> tuple[dict | None, str | None, str, bool]:
    source = "content"
    text = (content_text or "").strip()
    reasoning_tail = ""
    reasoning_tail_kind = ""

    # Шаг 1: пробуем content напрямую
    if text:
        try:
            return json.loads(text), None, source, False
        except json.JSONDecodeError as exc:
            first_err = str(exc)
        result = _extract_json_from_text(text)
        if result is not None:
            return result, None, source, False
        repaired = _repair_json(text)
        if repaired is not None:
            return repaired, None, source, False
    else:
        first_err = "empty content"

    # Шаг 2: ищем JSON во всём reasoning_content целиком (не только в хвосте)
    if reasoning_text:
        full_reasoning = reasoning_text.strip()
        result = _extract_json_from_text(full_reasoning)
        if result is not None:
            return result, None, "reasoning_content", False
        # Шаг 2b: bullet-парсер — когда модель перечислила поля но не успела написать JSON
        bullets = _parse_reasoning_bullets(full_reasoning)
        if bullets is not None:
            return bullets, None, "reasoning_bullets", False

    # Шаг 3: fallback на хвост reasoning (старая логика)
    if reasoning_text:
        reasoning_tail, reasoning_tail_kind = _extract_reasoning_tail(reasoning_text)
        if reasoning_tail:
            repaired = _repair_json(reasoning_tail)
            if repaired is not None:
                return repaired, None, "reasoning_tail", finish_reason == "length"
            textual = _build_textual_fallback_enrichment(reasoning_tail)
            if textual is not None:
                return textual, None, "reasoning_tail", finish_reason == "length"

    partial_ok = finish_reason == "length" and bool(reasoning_tail)
    if partial_ok:
        return (
            _build_textual_fallback_enrichment(reasoning_tail) or _empty_enrichment(),
            None,
            "reasoning_tail",
            True,
        )

    reason = first_err or "empty message"
    if reasoning_tail_kind:
        reason = f"{reason}; reasoning={reasoning_tail_kind}"
    return None, reason, source, False


def _stats_usage(data: dict | None) -> tuple[int | None, int | None, int | None]:
    if not isinstance(data, dict):
        return None, None, None
    stats = data.get("stats")
    if isinstance(stats, dict):
        return (
            stats.get("input_tokens"),
            stats.get("total_output_tokens"),
            stats.get("reasoning_output_tokens"),
        )
    usage = data.get("usage")
    if isinstance(usage, dict):
        completion_details = usage.get("completion_tokens_details") or {}
        return (
            usage.get("prompt_tokens"),
            usage.get("completion_tokens"),
            completion_details.get("reasoning_tokens"),
        )
    return None, None, None


async def _gemma_call_attempt(
    client: httpx.AsyncClient,
    base_url: str,
    user_text: str,
    png_path: Path,
    scale: float,
    model: str,
    timeout: int,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
) -> tuple[int, dict | None, str, int]:
    data_url = _png_to_data_url(png_path, scale=scale)
    payload = {
        "model": model,
        "system_prompt": SYSTEM_PROMPT,
        "input": [
            {"type": "text", "content": user_text},
            {"type": "image", "data_url": data_url},
        ],
        "temperature": 0.1,
        "max_output_tokens": max_output_tokens,
        "store": False,
    }
    started = time.monotonic()
    try:
        resp = await client.post(
            f"{base_url}/api/v1/chat",
            headers=_auth_header(),
            json=payload,
            timeout=timeout,
        )
    except Exception as exc:
        return -1, None, f"HTTP exception: {exc}", int((time.monotonic() - started) * 1000)
    elapsed = int((time.monotonic() - started) * 1000)
    raw = resp.text
    try:
        data = resp.json()
    except Exception:
        data = None
    return resp.status_code, data, raw, elapsed


def _repair_json(text: str) -> dict | None:
    """Попытка починить частично валидный JSON.

    Стратегия: ищем позицию ошибки, берём текст до неё и закрываем
    все открытые фигурные скобки. Помогает при Unterminated string и
    неожиданном конце объекта.
    """
    # Найти самый длинный валидный JSON-объект, последовательно укорачивая
    bracket_depth = 0
    in_string = False
    escape_next = False
    last_close = -1
    for i, ch in enumerate(text):
        if escape_next:
            escape_next = False
            continue
        if ch == "\\" and in_string:
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
        if in_string:
            continue
        if ch == "{":
            bracket_depth += 1
        elif ch == "}":
            bracket_depth -= 1
            if bracket_depth == 0:
                last_close = i
    # Если есть полное закрытое {...} — попробуем его
    if last_close > 0:
        try:
            return json.loads(text[:last_close + 1])
        except json.JSONDecodeError:
            pass
    # Иначе — закрыть недостающие скобки
    if bracket_depth > 0 and "{" in text:
        # Обрезаем до последней запятой или полного значения, потом закрываем
        close_text = text.rstrip().rstrip(",").rstrip()
        # Удалить незакрытую строку (всё после последней нечётной кавычки)
        quote_count = close_text.count('"') - close_text.count('\\"')
        if quote_count % 2 == 1:
            last_q = close_text.rfind('"')
            close_text = close_text[:last_q].rstrip().rstrip(",").rstrip()
        close_text += "}" * bracket_depth
        try:
            return json.loads(close_text)
        except json.JSONDecodeError:
            pass
    return None


def _parse_gemma_json(data: dict) -> tuple[dict | None, str | None]:
    """Извлечь и распарсить JSON из output Gemma."""
    content_text, reasoning_text, finish_reason = _extract_response_texts(data)
    parsed, parse_err, _source, _partial_ok = _parse_gemma_payload(
        content_text=content_text,
        reasoning_text=reasoning_text,
        finish_reason=finish_reason,
    )
    return parsed, parse_err


async def _enrich_block_single_pass(
    client: httpx.AsyncClient,
    base_url: str,
    block: dict,
    graph: dict,
    blocks_dir: Path,
    model: str,
    timeout: int,
    max_output_tokens: int,
) -> EnrichResult:
    """Один проход enrichment с заданным max_output_tokens (без auto-retry).
    Содержит scale-tier retry на Invalid image и ngrok HTML retry.
    """
    block_id = block["block_id"]
    page = block["page"]
    file_name = block["file"]
    png_path = blocks_dir / file_name

    page_text = _load_page_text(graph, page)
    sheet_no = _load_sheet_no(graph, page)

    user_text = USER_INSTRUCTION.format(
        block_id=block_id,
        page=page,
        sheet_no=sheet_no or "(не определён)",
        ocr_label=str(block.get("ocr_label", ""))[:250],
        page_text=page_text or "(текст страницы недоступен)",
    )

    ngrok_retries_left = _NGROK_HTML_RETRIES
    scale_idx = 0
    last_status = 0
    last_data: dict | None = None
    last_raw = ""
    total_elapsed_ms = 0

    while scale_idx < len(_IMAGE_SCALE_TIERS):
        scale = _IMAGE_SCALE_TIERS[scale_idx]
        status, data, raw, elapsed = await _gemma_call_attempt(
            client, base_url, user_text, png_path, scale, model, timeout,
            max_output_tokens=max_output_tokens,
        )
        total_elapsed_ms += elapsed
        last_status, last_data, last_raw = status, data, raw

        if _is_ngrok_html(data, raw):
            if ngrok_retries_left > 0:
                ngrok_retries_left -= 1
                await asyncio.sleep(_NGROK_HTML_BACKOFF_S)
                continue
            break

        if status >= 400 and _is_invalid_image_error(data, raw):
            scale_idx += 1
            continue

        if status >= 400 and _is_context_exceeded_error(data, raw):
            scale_idx += 1
            continue

        # JSON parse error после успешного HTTP — пробуем уменьшить изображение.
        # Это лечит блоки где модель «ломает» JSON при большом input, даже если
        # MAX_INPUT_LONG_SIDE_PX уже применён (разные контексты страниц могут мешать).
        if status == 200 and data is not None:
            content_text, reasoning_text, finish_reason = _extract_response_texts(data)
            parsed_check, parse_err_check, _source, partial_ok = _parse_gemma_payload(
                content_text=content_text,
                reasoning_text=reasoning_text,
                finish_reason=finish_reason,
            )
            if parse_err_check is not None and not partial_ok and scale_idx < len(_IMAGE_SCALE_TIERS) - 1:
                scale_idx += 1
                continue

        break

    if last_status < 0 or last_status >= 400 or last_data is None:
        err = (last_raw or "")[:500] if last_status >= 0 else last_raw
        return EnrichResult(
            block_id=block_id, page=page, ok=False,
            elapsed_ms=total_elapsed_ms, error=err or f"http {last_status}",
        )

    content_text, reasoning_text, finish_reason = _extract_response_texts(last_data)
    parsed, parse_err, response_source, partial_ok = _parse_gemma_payload(
        content_text=content_text,
        reasoning_text=reasoning_text,
        finish_reason=finish_reason,
    )
    input_tokens, output_tokens_value, reasoning_tokens = _stats_usage(last_data)

    output_tokens = output_tokens_value or 0
    truncated = False
    if parse_err and "Unterminated" in parse_err:
        truncated = True
    # Truncation определяется относительно ТЕКУЩЕГО лимита (max_output_tokens),
    # а не глобального DEFAULT — иначе retry с большим лимитом всегда будет «truncated».
    if output_tokens and output_tokens >= int(max_output_tokens * 0.95):
        truncated = True

    if parsed is None:
        return EnrichResult(
            block_id=block_id, page=page, ok=False,
            elapsed_ms=total_elapsed_ms,
            error=f"JSON parse failed: {parse_err}",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            reasoning_tokens=reasoning_tokens,
            truncated=truncated,
            response_source=response_source,
            finish_reason=finish_reason or "error",
        )

    return EnrichResult(
        block_id=block_id, page=page, ok=True,
        elapsed_ms=total_elapsed_ms,
        enrichment=parsed,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        reasoning_tokens=reasoning_tokens,
        truncated=truncated,
        response_source=response_source,
        finish_reason=finish_reason,
        partial_ok=partial_ok,
    )


def _split_png_to_grid(png_path: Path, rows: int, cols: int) -> list[Path]:
    """Разрезает PNG в сетку rows×cols, пишет временные файлы.

    Покрывает три случая:
      rows=2, cols=1 — вертикальный split (для вытянутых блоков, разрезов лестниц)
      rows=1, cols=2 — горизонтальный split (для широких блоков, длинных таблиц)
      rows=2, cols=2 — quad split (для плотных квадратных блоков — арматурные схемы)

    Caller обязан удалить файлы и tmpdir.
    """
    img = Image.open(png_path)
    W, H = img.width, img.height
    cw, ch = W // cols, H // rows
    paths: list[Path] = []
    tmpdir = Path(tempfile.mkdtemp(prefix=f"gemma_split_{png_path.stem}_"))
    parts_total = rows * cols
    idx = 0
    for r in range(rows):
        for c in range(cols):
            idx += 1
            left = c * cw
            top = r * ch
            right = (c + 1) * cw if c < cols - 1 else W
            bottom = (r + 1) * ch if r < rows - 1 else H
            crop = img.crop((left, top, right, bottom))
            out_path = tmpdir / f"{png_path.stem}_r{r+1}c{c+1}_of_{rows}x{cols}.png"
            crop.save(out_path, format="PNG", optimize=True)
            paths.append(out_path)
    return paths


def _split_png_vertically(png_path: Path, parts: int = 2) -> list[Path]:
    """Backward-compatible: вертикальный split (parts строк × 1 колонка)."""
    return _split_png_to_grid(png_path, rows=parts, cols=1)


def _merge_split_enrichments(parts: list[dict | None]) -> dict:
    """Объединить результаты enrich N половин одного блока в одну структуру.

    Списки (marks/dimensions/...) — конкатенация с дедупликацией с сохранением порядка.
    Скаляры (block_type, concrete_class) — берём первый непустой.
    subject/notes — склеиваем через разделитель.
    """
    list_keys = ("marks", "rebar_specs", "dimensions",
                 "references_on_block", "axes", "level_marks")
    merged: dict = {k: [] for k in list_keys}
    merged.update({"block_type": "", "subject": "", "concrete_class": None, "notes": ""})
    seen: dict[str, set] = {k: set() for k in list_keys}
    subjects: list[str] = []
    notes_parts: list[str] = []
    for p in parts:
        if not isinstance(p, dict):
            continue
        if not merged["block_type"] and p.get("block_type"):
            merged["block_type"] = p["block_type"]
        if not merged["concrete_class"] and p.get("concrete_class"):
            merged["concrete_class"] = p["concrete_class"]
        if p.get("subject"):
            subjects.append(str(p["subject"]))
        if p.get("notes"):
            notes_parts.append(str(p["notes"]))
        for key in list_keys:
            for v in (p.get(key) or []):
                k_ser = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False, sort_keys=True)
                if k_ser in seen[key]:
                    continue
                seen[key].add(k_ser)
                merged[key].append(v)
    merged["subject"] = " | ".join(subjects) if subjects else ""
    merged["notes"] = "\n".join(notes_parts) if notes_parts else ""
    return merged


async def _enrich_block_via_split(
    client: httpx.AsyncClient,
    base_url: str,
    block: dict,
    graph: dict,
    blocks_dir: Path,
    model: str,
    timeout: int,
    rows: int,
    cols: int,
) -> EnrichResult:
    """Fallback: режем PNG в сетку rows×cols, обрабатываем каждый кусок, мерджим.

    Применяется когда все токенные тиры исходного блока упали (truncated/cycle).
    Стратегия выбирается по форме блока в enrich_one_block:
      rows=2, cols=1 — вертикальный (вытянутые блоки)
      rows=1, cols=2 — горизонтальный (широкие блоки)
      rows=2, cols=2 — quad 2×2 (плотные квадратные — арматурные схемы)

    Каждый кусок проходит **полный** tier-цикл (2048 → 4096 → 8192). Повторный
    split на кусках НЕ делаем (disable_split=True предотвращает рекурсию).
    """
    block_id = block["block_id"]
    page = block.get("page", 0)
    png_path = blocks_dir / block["file"]
    parts_total = rows * cols

    split_paths = _split_png_to_grid(png_path, rows=rows, cols=cols)
    cumulative_ms = 0
    results: list[EnrichResult] = []
    try:
        for i, sp in enumerate(split_paths):
            sub_block = dict(block)
            # Уникальный block_id чтобы лог различал, но page/ocr_label/text — те же.
            sub_block["block_id"] = f"{block_id}#part{i+1}/{parts_total}"
            sub_block["file"] = sp.name
            res = await enrich_one_block(
                client, base_url, sub_block, graph, sp.parent, model, timeout,
                progress_cb=None, disable_split=True,
            )
            cumulative_ms += res.elapsed_ms
            results.append(res)
    finally:
        # Чистим временные файлы и директорию
        for sp in split_paths:
            try:
                sp.unlink()
            except OSError:
                pass
        if split_paths:
            try:
                split_paths[0].parent.rmdir()
            except OSError:
                pass

    # Если хоть одна часть не распарсилась — фейл (но не truncated, чтоб не зациклить retry)
    if not all(r.ok for r in results):
        errs = "; ".join(
            f"part{i+1}: {(r.error or '')[:80]}" for i, r in enumerate(results) if not r.ok
        )
        return EnrichResult(
            block_id=block_id, page=page, ok=False,
            elapsed_ms=cumulative_ms,
            error=f"split({rows}x{cols}) failed: {errs}"[:300],
            input_tokens=sum((r.input_tokens or 0) for r in results),
            output_tokens=sum((r.output_tokens or 0) for r in results),
            reasoning_tokens=sum((r.reasoning_tokens or 0) for r in results),
            truncated=False,
        )

    merged = _merge_split_enrichments([r.enrichment for r in results])
    return EnrichResult(
        block_id=block_id, page=page, ok=True,
        elapsed_ms=cumulative_ms,
        enrichment=merged,
        input_tokens=sum((r.input_tokens or 0) for r in results),
        output_tokens=sum((r.output_tokens or 0) for r in results),
        reasoning_tokens=sum((r.reasoning_tokens or 0) for r in results),
        truncated=False,
    )


def _block_aspect(blocks_dir: Path, block: dict) -> float:
    """Высота / ширина PNG. 0.0 если файл недоступен."""
    try:
        with Image.open(blocks_dir / block["file"]) as img:
            return img.height / max(1, img.width)
    except Exception:
        return 0.0


async def enrich_one_block(
    client: httpx.AsyncClient,
    base_url: str,
    block: dict,
    graph: dict,
    blocks_dir: Path,
    model: str,
    timeout: int,
    progress_cb=None,
    disable_split: bool = False,
) -> EnrichResult:
    """Enrichment блока с авторетраем по тирам max_output_tokens.

    Стратегия:
      1) Первая попытка — DEFAULT_MAX_OUTPUT_TOKENS (быстро, ~95% блоков).
      2) Если truncated — повтор с 4096.
      3) Если ещё truncated — повтор с 8192 (потолок при context=16K).
      4) Если все три попытки truncated И блок «вытянутый» (aspect ≥ SPLIT_ASPECT_THRESHOLD)
         → fallback split: режем картинку пополам, обрабатываем по отдельности и мерджим.
      5) Иначе возвращаем последний результат с truncated=True.

    Не-truncated ошибки (HTTP, JSON parse без Unterminated) НЕ ретраим — они
    не лечатся увеличением лимита.

    disable_split=True — отключает split-fallback (используется при рекурсии из
    самого split, чтобы половины не пытались split-нуться повторно).
    """
    last_result: EnrichResult | None = None
    cumulative_elapsed_ms = 0
    # Был ли truncation на предыдущих тирах. Если был — а на следующем тире
    # модель не truncated, но JSON всё равно битый — это паттерн «cycle-генерации»
    # на вытянутых блоках. В этом случае идём к split-fallback вместо немедленного возврата.
    had_truncation = False

    for attempt_idx, max_tokens in enumerate(RETRY_OUTPUT_TOKEN_TIERS):
        if attempt_idx > 0 and progress_cb is not None:
            await _emit(progress_cb, {
                "type": "block_retry",
                "block_id": block["block_id"],
                "page": block.get("page"),
                "attempt": attempt_idx + 1,
                "max_tokens": max_tokens,
                "previous_output_tokens": last_result.output_tokens if last_result else None,
            })

        result = await _enrich_block_single_pass(
            client, base_url, block, graph, blocks_dir, model, timeout,
            max_output_tokens=max_tokens,
        )
        cumulative_elapsed_ms += result.elapsed_ms
        result.elapsed_ms = cumulative_elapsed_ms  # отображаем суммарное время
        last_result = result

        if result.ok:
            return result  # Чистый успех — выходим
        if result.truncated:
            had_truncation = True
            continue  # Truncation — пробуем следующий tier
        # Не-truncated parse error: если до этого было truncation на меньшем тире,
        # это «cycle» на длинном блоке — прорываемся к split-fallback.
        if had_truncation:
            break
        # Иначе — лотерейная parse-ошибка с первой попытки, возвращаем как есть.
        return result

    # Сюда попадаем когда: (a) все тиры truncated; (b) был truncation, потом cycle.
    # Выбираем split-стратегию по геометрии блока:
    #   aspect ≥ 2.5  → вертикальный 2×1 (вытянутые: разрезы лестниц)
    #   aspect ≤ 0.4  → горизонтальный 1×2 (широкие: длинные таблицы/планы)
    #   иначе         → quad 2×2 (плотные квадратные: арматурные схемы)
    if not disable_split and last_result is not None and not last_result.ok:
        aspect = _block_aspect(blocks_dir, block)
        if aspect >= SPLIT_ASPECT_THRESHOLD:
            rows, cols = 2, 1
            strategy = "vertical_2x1"
        elif aspect > 0 and aspect <= (1.0 / SPLIT_ASPECT_THRESHOLD):
            rows, cols = 1, 2
            strategy = "horizontal_1x2"
        elif aspect > 0:
            # Квадратный плотный блок — пробуем quad 2x2
            rows, cols = 2, 2
            strategy = "quad_2x2"
        else:
            rows, cols = 0, 0  # PNG не открылся — split не пробуем
            strategy = "skip"

        if rows > 0:
            if progress_cb is not None:
                await _emit(progress_cb, {
                    "type": "block_split",
                    "block_id": block["block_id"],
                    "page": block.get("page"),
                    "aspect": round(aspect, 2),
                    "strategy": strategy,
                    "parts": rows * cols,
                })
            try:
                split_res = await _enrich_block_via_split(
                    client, base_url, block, graph, blocks_dir, model, timeout,
                    rows=rows, cols=cols,
                )
            except Exception as exc:
                # Split упал по непонятной причине — возвращаем оригинальный truncated.
                if progress_cb is not None:
                    await _emit(progress_cb, {
                        "type": "block_split_failed",
                        "block_id": block["block_id"],
                        "error": f"{exc}"[:200],
                    })
                return last_result
            # split_res.elapsed_ms — только время split-вызовов; добавим время предыдущих тиров.
            split_res.elapsed_ms += cumulative_elapsed_ms
            return split_res

    # Иначе возвращаем последний (truncated) результат как есть.
    return last_result if last_result is not None else EnrichResult(
        block_id=block["block_id"], page=block.get("page", 0), ok=False,
        error="enrich_one_block: no result",
    )


# ─── MD manipulation ───────────────────────────────────────────────────────

def _enrichment_text(enrichment: dict | None) -> str:
    if not isinstance(enrichment, dict):
        return ""
    parts: list[str] = []
    for key in (
        "block_type", "subject", "notes", "concrete_class",
        "marks", "rebar_specs", "dimensions", "references_on_block",
        "axes", "level_marks",
    ):
        value = enrichment.get(key)
        if isinstance(value, list):
            parts.extend(str(item).strip() for item in value if str(item).strip())
        elif value not in (None, ""):
            parts.append(str(value).strip())
    return "\n".join(part for part in parts if part).strip()


def _estimate_image_tokens(render_size: list[int] | tuple[int, int] | None) -> int:
    if not render_size or len(render_size) < 2:
        return 0
    width = max(1, int(render_size[0] or 0))
    height = max(1, int(render_size[1] or 0))
    return int(math.ceil(width / 64) * math.ceil(height / 64))


def _base_result_candidate_reasons(block: dict, result: EnrichResult) -> list[str]:
    reasons: list[str] = []
    text = _enrichment_text(result.enrichment).lower()
    label = str(block.get("ocr_label") or "").lower()
    ocr_text_len = int(block.get("ocr_text_len") or 0)
    render_size = block.get("render_size") or [0, 0]
    long_side = max(render_size) if render_size else 0
    short_side = min(render_size) if render_size else 0

    if not result.ok or not result.enrichment:
        reasons.append("base_failed")
        return reasons
    if result.partial_ok:
        reasons.append("base_partial_ok")
    if len(text.strip()) < SHORT_RESULT_THRESHOLD:
        reasons.append("base_too_short")
    if any(marker in text for marker in _WEAK_RESULT_MARKERS):
        reasons.append("base_low_readability")
    if any(hint in label for hint in _DETAIL_TEXT_HINTS):
        reasons.append("detail_text_block")
    # Текстонасыщенность сама по себе не повод для 300 DPI —
    # только если base pass дал слабый/обрезанный результат.
    base_weak = bool(reasons)  # уже есть partial_ok / too_short / low_readability / detail_text
    if base_weak:
        if ocr_text_len >= 180:
            reasons.append("ocr_text_heavy")
        if ocr_text_len >= 120 and short_side and short_side < 700:
            reasons.append("dense_small_text")
        if ocr_text_len >= 220 and long_side and long_side < 1300:
            reasons.append("text_dense_medium_crop")
    return list(dict.fromkeys(reasons))


def _high_detail_safety_meta(block: dict) -> dict[str, Any]:
    size_kb = float(block.get("size_kb") or 0.0)
    render_size = block.get("render_size") or [0, 0]
    long_side = max(render_size) if render_size else 0
    estimated_tokens = _estimate_image_tokens(render_size)
    safe = (
        size_kb <= HIGH_DETAIL_MAX_SIZE_KB
        and long_side <= HIGH_DETAIL_MAX_LONG_SIDE_PX
        and (estimated_tokens == 0 or estimated_tokens <= HIGH_DETAIL_MAX_IMAGE_TOKENS)
    )
    reason = ""
    if not safe:
        if size_kb > HIGH_DETAIL_MAX_SIZE_KB:
            reason = f"size_kb_300>{HIGH_DETAIL_MAX_SIZE_KB}"
        elif long_side > HIGH_DETAIL_MAX_LONG_SIDE_PX:
            reason = f"long_side_300>{HIGH_DETAIL_MAX_LONG_SIDE_PX}"
        else:
            reason = f"estimated_image_tokens>{HIGH_DETAIL_MAX_IMAGE_TOKENS}"
    return {
        "safe": safe,
        "reason": reason,
        "size_kb_300": round(size_kb, 1),
        "long_side_300": int(long_side or 0),
        "estimated_image_tokens": estimated_tokens,
    }


def _build_index_payload(
    *,
    existing: dict[str, Any] | None,
    blocks: list[dict[str, Any]],
    policy: dict[str, Any],
    output_dir_name: str,
) -> dict[str, Any]:
    existing = existing or {}
    merged_source = sorted(set(existing.get("source_result_json") or []))
    payload = {
        "total_blocks": len(blocks),
        "total_expected": max(int(existing.get("total_expected") or 0), len(blocks)),
        "errors": int(existing.get("errors") or 0),
        "profile": policy["profile"],
        "compact": policy["compact"],
        "dpi": policy["dpi"],
        "min_long_side": policy["min_long_side"],
        "skip_small": policy["skip_small"],
        "output_dir_name": output_dir_name,
        "source_result_json": merged_source,
        "blocks": blocks,
    }
    return payload


def _write_merged_crop_index(
    index_path: Path,
    *,
    existing: dict[str, Any] | None,
    new_blocks: list[dict[str, Any]],
    policy: dict[str, Any],
    output_dir_name: str,
) -> dict[str, Any]:
    existing = existing or {}
    blocks_by_id: dict[str, dict[str, Any]] = {}
    for block in existing.get("blocks") or []:
        if isinstance(block, dict) and block.get("block_id"):
            blocks_by_id[str(block["block_id"])] = block
    for block in new_blocks:
        if isinstance(block, dict) and block.get("block_id"):
            blocks_by_id[str(block["block_id"])] = block
    merged_blocks = sorted(blocks_by_id.values(), key=lambda block: (int(block.get("page") or 0), str(block.get("block_id") or "")))
    payload = _build_index_payload(
        existing=existing,
        blocks=merged_blocks,
        policy=policy,
        output_dir_name=output_dir_name,
    )
    index_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _ensure_crop_index(
    project_dir: Path,
    *,
    policy: dict[str, Any],
    output_dir_name: str,
    block_ids: list[str] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    from backend.app.pipeline.stages.crop_blocks.blocks import crop_blocks

    project_dir = Path(project_dir)
    output_dir = project_dir / "_output" / output_dir_name
    index_path = output_dir / "index.json"
    existing = load_json(index_path)

    if not block_ids:
        stale_existing_dir = (
            not index_path.exists()
            and output_dir.exists()
            and any(output_dir.glob("block_*.png"))
        )
        needs_crop = (
            force
            or not index_path.exists()
            or not crop_index_matches_policy(index_path, policy)
            or stale_existing_dir
        )
        if needs_crop:
            result = crop_blocks(
                str(project_dir),
                force=force or stale_existing_dir or (index_path.exists() and not crop_index_matches_policy(index_path, policy)),
                compact=policy["compact"],
                dpi=policy["dpi"],
                skip_small=policy["skip_small"],
                output_dir_name=output_dir_name,
            )
            if result.get("error"):
                raise RuntimeError(str(result["error"]))
        return load_json(index_path)

    wanted_ids = sorted({str(block_id) for block_id in block_ids if str(block_id)})
    if not wanted_ids:
        return existing

    existing_policy_ok = index_path.exists() and crop_index_matches_policy(index_path, policy)
    existing_by_id = {
        str(block.get("block_id") or ""): block
        for block in existing.get("blocks") or []
        if isinstance(block, dict) and block.get("block_id")
    }
    already_present = (
        existing_policy_ok
        and all(
            existing_by_id.get(block_id)
            and (output_dir / str(existing_by_id[block_id].get("file") or "")).exists()
            for block_id in wanted_ids
        )
    )
    if already_present and not force:
        return existing

    result = crop_blocks(
        str(project_dir),
        block_ids=wanted_ids,
        force=force,
        compact=policy["compact"],
        dpi=policy["dpi"],
        skip_small=policy["skip_small"],
        output_dir_name=output_dir_name,
    )
    if result.get("error"):
        raise RuntimeError(str(result["error"]))
    new_index = load_json(index_path)
    return _write_merged_crop_index(
        index_path,
        existing=existing if existing_policy_ok else None,
        new_blocks=list(new_index.get("blocks") or []),
        policy=policy,
        output_dir_name=output_dir_name,
    )

def _format_enrichment_md(record: dict, model: str, ts: str) -> str:
    """Форматировать enrichment-словарь в markdown-секцию."""
    lines = [f"\n\n**[ENRICHED {model} @ {ts}]**"]

    def _add(label: str, value):
        if value is None or value == "" or value == []:
            return
        if isinstance(value, list):
            value = ", ".join(str(x) for x in value if x)
            if not value:
                return
        lines.append(f"- **{label}:** {value}")

    _add("Тип блока", record.get("block_type"))
    _add("Содержание", record.get("subject"))
    _add("Марки", record.get("marks"))
    _add("Арматура", record.get("rebar_specs"))
    _add("Размеры", record.get("dimensions"))
    _add("Оси", record.get("axes"))
    _add("Отметки", record.get("level_marks"))
    _add("Бетон", record.get("concrete_class"))
    _add("Ссылки", record.get("references_on_block"))
    _add("Заметки", record.get("notes"))

    return "\n".join(lines)


def inject_enrichment_meta_into_graph(graph_path: Path, meta: dict) -> bool:
    """Дописать `meta.enrichment` в document_graph.json.

    Вызывается после augment-merge MD. Не перестраивает граф.
    Возвращает True если файл был обновлён, False если граф не существует.
    """
    if not graph_path.exists():
        return False
    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    graph.setdefault("meta", {})["enrichment"] = meta
    graph_path.write_text(json.dumps(graph, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def _augment_md(
    md_text: str,
    enrichments_by_id: dict[str, dict],
    model: str,
    ts: str,
) -> str:
    """Вставить [ENRICHED] секции в конец каждого `### BLOCK [IMAGE]: <id>` блока."""
    sections = extract_block_sections(md_text)
    if not sections:
        return md_text

    # Соберём результирующий текст по сегментам
    result: list[str] = []
    cursor = 0
    for section in sections:
        # Часть до этого блока (включая заголовок и предыдущий контекст)
        result.append(md_text[cursor:section.header_start])
        block_body = section.text

        # Снять старый ENRICHED (на случай force re-enrich или повторного вызова)
        block_body = strip_enrichment_in_block(block_body)

        if section.type == "IMAGE":
            enr = enrichments_by_id.get(section.id)
            if enr is not None:
                # Пустая строка между Chandra-описанием и ENRICHED + пустая после
                block_body = block_body.rstrip() + _format_enrichment_md(enr, model, ts) + "\n\n"

        result.append(block_body)
        cursor = section.body_end

    # Остаток файла
    result.append(md_text[cursor:])
    return "".join(result)


def _backup_md_once(md_path: Path) -> Path:
    """Создать backup *.pre_enrichment.bak (один раз, не перезаписывает)."""
    bak = md_path.with_suffix(md_path.suffix + ".pre_enrichment.bak")
    if not bak.exists():
        bak.write_bytes(md_path.read_bytes())
    return bak


# ─── Main entry ────────────────────────────────────────────────────────────

ProgressCb = Callable[[dict], Awaitable[None] | None]


async def _emit(progress_cb: Optional[ProgressCb], event: dict) -> None:
    if progress_cb is None:
        return
    res = progress_cb(event)
    if asyncio.iscoroutine(res):
        await res


def _result_status(result: EnrichResult | None) -> str:
    if result is None:
        return "missing"
    if result.ok and result.partial_ok:
        return "partial_ok"
    if result.ok:
        return "ok"
    return "failed"


async def _run_blocks_pass(
    *,
    image_blocks: list[dict],
    graph: dict,
    blocks_dir: Path,
    base_url: str,
    model: str,
    timeout: int,
    parallelism: int,
    progress_cb: Optional[ProgressCb],
    pause_event: Optional[asyncio.Event],
    cancel_event: Optional[asyncio.Event],
    phase: str,
) -> tuple[list[EnrichResult], dict[str, Any]]:
    phase = phase.strip() or "base"
    sem = asyncio.Semaphore(max(1, parallelism))
    completed = 0
    completed_lock = asyncio.Lock()

    if phase == "base":
        event_names = {
            "started": "block_started",
            "done": "block_done",
            "retry_pass_started": "retry_pass_started",
            "retry_done": "retry_block_done",
            "retry_pass_done": "retry_pass_done",
        }
    else:
        event_names = {
            "started": f"{phase}_block_started",
            "done": f"{phase}_block_done",
            "retry_pass_started": f"{phase}_retry_pass_started",
            "retry_done": f"{phase}_retry_block_done",
            "retry_pass_done": f"{phase}_retry_pass_done",
        }

    async with httpx.AsyncClient(timeout=timeout + 30) as client:
        async def _process_block(block: dict, *, count_progress: bool, attempt: int) -> EnrichResult:
            nonlocal completed
            if cancel_event is not None and cancel_event.is_set():
                return EnrichResult(
                    block_id=block["block_id"],
                    page=block.get("page", 0),
                    ok=False,
                    error="cancelled by user",
                    finish_reason="error",
                )
            if pause_event is not None:
                while not pause_event.is_set():
                    if cancel_event is not None and cancel_event.is_set():
                        return EnrichResult(
                            block_id=block["block_id"],
                            page=block.get("page", 0),
                            ok=False,
                            error="cancelled during pause",
                            finish_reason="error",
                        )
                    await asyncio.sleep(0.5)
            async with sem:
                if count_progress:
                    await _emit(progress_cb, {
                        "type": event_names["started"],
                        "phase": phase,
                        "block_id": block["block_id"],
                        "page": block.get("page"),
                        "completed": completed,
                        "total": len(image_blocks),
                    })
                rec = await enrich_one_block(
                    client,
                    base_url,
                    block,
                    graph,
                    blocks_dir,
                    model,
                    timeout,
                    progress_cb=progress_cb,
                )
                if count_progress:
                    async with completed_lock:
                        completed += 1
                    await _emit(progress_cb, {
                        "type": event_names["done"],
                        "phase": phase,
                        "block_id": rec.block_id,
                        "page": rec.page,
                        "ok": rec.ok,
                        "partial_ok": rec.partial_ok,
                        "elapsed_ms": rec.elapsed_ms,
                        "completed": completed,
                        "total": len(image_blocks),
                        "error": rec.error,
                        "truncated": rec.truncated,
                        "output_tokens": rec.output_tokens,
                        "response_source": rec.response_source,
                        "finish_reason": rec.finish_reason,
                    })
                else:
                    await _emit(progress_cb, {
                        "type": event_names["retry_done"],
                        "phase": phase,
                        "block_id": rec.block_id,
                        "page": rec.page,
                        "ok": rec.ok,
                        "partial_ok": rec.partial_ok,
                        "elapsed_ms": rec.elapsed_ms,
                        "attempt": attempt,
                        "max_attempts": PROJECT_RETRY_PASSES + 1,
                        "error": rec.error,
                        "truncated": rec.truncated,
                        "output_tokens": rec.output_tokens,
                        "response_source": rec.response_source,
                        "finish_reason": rec.finish_reason,
                    })
                return rec

        gathered = await asyncio.gather(
            *(_process_block(block, count_progress=True, attempt=1) for block in image_blocks),
            return_exceptions=True,
        )
        results: list[EnrichResult] = []
        for block, result in zip(image_blocks, gathered):
            if isinstance(result, Exception):
                results.append(EnrichResult(
                    block_id=block["block_id"],
                    page=block.get("page", 0),
                    ok=False,
                    error=f"Unhandled Gemma {phase} exception: {type(result).__name__}: {result}",
                    finish_reason="error",
                ))
            else:
                results.append(result)

        retry_stats: dict[str, Any] = {
            "passes_planned": PROJECT_RETRY_PASSES,
            "passes_executed": 0,
            "attempts_total": 0,
            "recovered_total": 0,
            "still_failed_after_retries": 0,
            "passes": [],
        }
        for retry_pass in range(PROJECT_RETRY_PASSES):
            if cancel_event is not None and cancel_event.is_set():
                break
            failed_idx = [
                idx for idx, result in enumerate(results)
                if not result.ok and not (result.error or "").startswith("cancelled")
            ]
            if not failed_idx:
                break
            attempt_no = retry_pass + 2
            blocks_by_id = {block["block_id"]: block for block in image_blocks}
            retry_blocks = [blocks_by_id[results[idx].block_id] for idx in failed_idx]
            await _emit(progress_cb, {
                "type": event_names["retry_pass_started"],
                "phase": phase,
                "attempt": attempt_no,
                "max_attempts": PROJECT_RETRY_PASSES + 1,
                "to_retry": len(retry_blocks),
            })
            retry_gathered = await asyncio.gather(
                *(
                    _process_block(block, count_progress=False, attempt=attempt_no)
                    for block in retry_blocks
                ),
                return_exceptions=True,
            )
            recovered = 0
            for original_idx, block, retry_result in zip(failed_idx, retry_blocks, retry_gathered):
                if isinstance(retry_result, Exception):
                    retry_result = EnrichResult(
                        block_id=block["block_id"],
                        page=block.get("page", 0),
                        ok=False,
                        error=f"Unhandled Gemma {phase} retry exception: {type(retry_result).__name__}: {retry_result}",
                        finish_reason="error",
                    )
                if retry_result.ok:
                    recovered += 1
                results[original_idx] = retry_result
            retry_stats["passes_executed"] += 1
            retry_stats["attempts_total"] += len(retry_blocks)
            retry_stats["recovered_total"] += recovered
            retry_stats["passes"].append({
                "attempt": attempt_no,
                "blocks_retried": len(retry_blocks),
                "recovered": recovered,
                "still_failed": len(retry_blocks) - recovered,
            })
            await _emit(progress_cb, {
                "type": event_names["retry_pass_done"],
                "phase": phase,
                "attempt": attempt_no,
                "max_attempts": PROJECT_RETRY_PASSES + 1,
                "recovered": recovered,
                "still_failed": len(retry_blocks) - recovered,
            })
        retry_stats["still_failed_after_retries"] = sum(
            1 for result in results
            if not result.ok and not (result.error or "").startswith("cancelled")
        )
        return results, retry_stats


async def enrich_project(
    project_dir: Path,
    *,
    force: bool = False,
    model: str = DEFAULT_MODEL,
    parallelism: int = DEFAULT_PARALLELISM,
    timeout: int = DEFAULT_TIMEOUT_S,
    progress_cb: Optional[ProgressCb] = None,
    pause_event: Optional[asyncio.Event] = None,
    cancel_event: Optional[asyncio.Event] = None,
) -> dict:
    """Gemma enrichment проекта: base 100 DPI + targeted high-detail 300 DPI."""
    project_dir = Path(project_dir).resolve()
    out_dir = project_dir / "_output"
    base_policy = gemma_base_crop_policy()
    high_detail_policy = gemma_high_detail_crop_policy()
    base_blocks_dir = gemma_base_blocks_dir(project_dir)
    base_index_path = gemma_base_blocks_index_path(project_dir)
    high_detail_blocks_dir = gemma_high_detail_blocks_dir(project_dir)
    graph_path = out_dir / "document_graph.json"

    md_files = sorted(project_dir.glob("*_document.md"))
    if not md_files:
        raise FileNotFoundError(f"MD-файл не найден в {project_dir}")
    md_path = md_files[0]

    existing_summary = validate_gemma_summary(project_dir, md_path=md_path, min_coverage=0.0)
    if existing_summary.get("valid") and not force:
        await _emit(progress_cb, {
            "type": "skipped",
            "reason": "summary_valid",
            "existing": existing_summary.get("summary"),
        })
        skipped = dict(existing_summary.get("summary") or {})
        skipped.update({
            "status": "skipped",
            "reason": "summary_valid",
            "existing": existing_summary.get("summary"),
            "md_path": str(md_path),
        })
        return skipped

    _ensure_crop_index(
        project_dir,
        policy=base_policy,
        output_dir_name=GEMMA_BASE_BLOCKS_DIRNAME,
        force=force,
    )
    if not base_index_path.exists():
        raise FileNotFoundError(
            f"{base_index_path} не найден — сначала создайте base Gemma crops"
        )
    if not crop_index_matches_policy(base_index_path, base_policy):
        raise RuntimeError(
            f"{GEMMA_BASE_BLOCKS_DIRNAME}/index.json не соответствует Gemma base crop policy "
            f"{base_policy}. Перекропайте base crops."
        )

    base_url = _env("CHANDRA_BASE_URL").rstrip("/")
    base_index = load_json(base_index_path)
    blocks = base_index.get("blocks", [])
    image_blocks = [
        block for block in blocks
        if isinstance(block, dict) and (block.get("block_type") or "").lower() == "image"
    ]
    total = len(image_blocks)

    if total == 0:
        summary = build_gemma_summary(
            status="no_blocks",
            project_dir=project_dir,
            md_path=md_path,
            model=model,
            blocks_total=0,
            blocks_ok=0,
            blocks_failed=0,
            blocks_skipped=0,
            extra={
                "blocks": [],
                "md_path": str(md_path),
                "timestamp": utc_now_iso(),
                "uncovered_block_ids": [],
                "uncovered_blocks": [],
                "high_detail_blocks_index_hash": None,
            },
        )
        (out_dir / "gemma_enrichment_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        await _emit(progress_cb, {"type": "no_blocks"})
        return summary

    # graph (опционально — для page_text/sheet_no)
    graph: dict = {}
    if graph_path.exists():
        try:
            graph = json.loads(graph_path.read_text(encoding="utf-8"))
        except Exception:
            graph = {}

    # Backup MD (один раз)
    bak_path = _backup_md_once(md_path)

    # Если force и старая enrichment секция есть — снимем её перед re-augment
    md_text = strip_gemma_enrichment(md_path.read_text(encoding="utf-8"))

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    started = time.monotonic()

    # Если включён адаптивный reload — загружаем модель с BASE_CONTEXT_LENGTH (4K)
    # перед base pass. Если уже загружена с нужным ctx — unload+reload всё равно,
    # потому что оператор явно включил эту опцию.
    try:
        from backend.app.core.config import GEMMA_ADAPTIVE_RELOAD_ENABLED
        _adaptive_reload_enabled = GEMMA_ADAPTIVE_RELOAD_ENABLED
    except ImportError:
        _adaptive_reload_enabled = False

    if _adaptive_reload_enabled:
        import backend.app.services.llm.lms_service as _lms
        logger = _get_logger()
        try:
            logger.info("Adaptive reload: loading %s with context_length=%d for base pass", model, BASE_CONTEXT_LENGTH)
            await asyncio.to_thread(_lms.unload_all_for, model)
            await asyncio.to_thread(
                _lms.load_model, model, context_length=BASE_CONTEXT_LENGTH
            )
            _lms.invalidate_loaded_cache()
            logger.info("Adaptive reload: base pass model loaded at %d ctx", BASE_CONTEXT_LENGTH)
        except Exception as _exc:
            logger.warning("Adaptive reload: base pass load failed (continuing): %s", _exc)

    await _emit(progress_cb, {
        "type": "started",
        "total": total,
        "model": model,
        "parallelism": parallelism,
        "base_profile": GEMMA_BASE_PROFILE,
        "base_blocks_dir": f"_output/{GEMMA_BASE_BLOCKS_DIRNAME}",
        "high_detail_profile": GEMMA_HIGH_DETAIL_PROFILE,
        "high_detail_blocks_dir": f"_output/{GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME}",
        "timestamp": ts,
    })

    base_results, base_retry_stats = await _run_blocks_pass(
        image_blocks=image_blocks,
        graph=graph,
        blocks_dir=base_blocks_dir,
        base_url=base_url,
        model=model,
        timeout=timeout,
        parallelism=parallelism,
        progress_cb=progress_cb,
        pause_event=pause_event,
        cancel_event=cancel_event,
        phase="base",
    )
    base_results_by_id = {result.block_id: result for result in base_results}
    base_blocks_ok = sum(1 for result in base_results if result.ok)

    candidate_reason_map: dict[str, list[str]] = {}
    candidate_ids: list[str] = []
    for block in image_blocks:
        reasons = _base_result_candidate_reasons(block, base_results_by_id.get(block["block_id"]))
        if reasons:
            candidate_ids.append(block["block_id"])
            candidate_reason_map[block["block_id"]] = reasons

    high_detail_results_by_id: dict[str, EnrichResult] = {}
    high_detail_retry_stats: dict[str, Any] = {
        "passes_planned": PROJECT_RETRY_PASSES,
        "passes_executed": 0,
        "attempts_total": 0,
        "recovered_total": 0,
        "still_failed_after_retries": 0,
        "passes": [],
    }
    high_detail_safety: dict[str, dict[str, Any]] = {}
    skipped_large_ids: list[str] = []
    high_detail_index_hash: str | None = None

    if candidate_ids:
        await _emit(progress_cb, {
            "type": "high_detail_candidates",
            "total": total,
            "candidates": len(candidate_ids),
            "candidate_block_ids": sorted(candidate_ids),
        })
        high_detail_index = _ensure_crop_index(
            project_dir,
            policy=high_detail_policy,
            output_dir_name=GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME,
            block_ids=candidate_ids,
            force=force,
        )
        high_detail_index_hash = blocks_index_hash(gemma_high_detail_blocks_index_path(project_dir)) or None
        high_detail_blocks = {
            str(block.get("block_id") or ""): block
            for block in high_detail_index.get("blocks", [])
            if isinstance(block, dict) and block.get("block_id")
        }
        safe_candidates: list[dict] = []
        for block_id in candidate_ids:
            block_300 = high_detail_blocks.get(block_id)
            if block_300 is None:
                high_detail_safety[block_id] = {
                    "safe": False,
                    "reason": "missing_high_detail_crop",
                    "size_kb_300": 0.0,
                    "long_side_300": 0,
                    "estimated_image_tokens": 0,
                }
                continue
            safety = _high_detail_safety_meta(block_300)
            high_detail_safety[block_id] = safety
            if safety["safe"]:
                safe_candidates.append(block_300)
            else:
                skipped_large_ids.append(block_id)
        await _emit(progress_cb, {
            "type": "high_detail_prefilter",
            "candidates": len(candidate_ids),
            "safe_candidates": len(safe_candidates),
            "skipped_large_ids": sorted(skipped_large_ids),
        })
        if safe_candidates:
            # Адаптивная перезагрузка: base прошёл на 4K ctx, high-detail нужен 16K.
            # Reload только если оператор включил GEMMA_ADAPTIVE_RELOAD_ENABLED.
            reload_result: dict = {"ok": False, "skipped": True}
            try:
                from backend.app.core.config import GEMMA_ADAPTIVE_RELOAD_ENABLED
                adaptive_reload = GEMMA_ADAPTIVE_RELOAD_ENABLED
            except ImportError:
                adaptive_reload = False
            if adaptive_reload:
                reload_result = await _reload_model_for_high_detail(model)
                await _emit(progress_cb, {
                    "type": "adaptive_reload",
                    "ok": reload_result.get("ok"),
                    "context_length": reload_result.get("context_length"),
                    "error": reload_result.get("error"),
                })
            high_detail_results, high_detail_retry_stats = await _run_blocks_pass(
                image_blocks=safe_candidates,
                graph=graph,
                blocks_dir=high_detail_blocks_dir,
                base_url=base_url,
                model=model,
                timeout=timeout,
                parallelism=HIGH_DETAIL_PARALLELISM,
                progress_cb=progress_cb,
                pause_event=pause_event,
                cancel_event=cancel_event,
                phase="high_detail",
            )
            high_detail_results_by_id = {result.block_id: result for result in high_detail_results}

    final_enrichments: dict[str, dict] = {}
    summary_blocks: list[dict[str, Any]] = []
    uncovered_blocks: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    base_only_ids: list[str] = []
    upgraded_ids: list[str] = []
    high_detail_ok = 0

    for block in image_blocks:
        block_id = block["block_id"]
        base_result = base_results_by_id.get(block_id)
        high_detail_result = high_detail_results_by_id.get(block_id)
        base_status = _result_status(base_result)
        high_detail_status = "not_needed"
        warnings: list[str] = []
        candidate_reasons = candidate_reason_map.get(block_id, [])
        if candidate_reasons:
            warnings.extend(candidate_reasons)
            if block_id in skipped_large_ids:
                high_detail_status = "skipped_large_block"
                warnings.append(WARNING_LARGE_BLOCK)
            elif high_detail_result is not None:
                high_detail_status = _result_status(high_detail_result)
            else:
                high_detail_status = "missing"

        final_profile = "none"
        coverage_status = "missing_gemma_enrichment"
        final_enrichment = None

        if high_detail_result is not None and high_detail_result.ok and high_detail_result.enrichment:
            final_enrichment = high_detail_result.enrichment
            final_profile = GEMMA_HIGH_DETAIL_PROFILE
            coverage_status = "partial" if high_detail_result.partial_ok else "ok"
            high_detail_ok += 1
            upgraded_ids.append(block_id)
            if high_detail_result.partial_ok:
                warnings.append("high_detail_partial_ok")
        elif base_result is not None and base_result.ok and base_result.enrichment:
            final_enrichment = base_result.enrichment
            final_profile = GEMMA_BASE_PROFILE
            base_only_ids.append(block_id)
            if base_result.partial_ok:
                coverage_status = "partial"
                warnings.append("base_partial_ok")
            elif high_detail_status == "skipped_large_block":
                coverage_status = "high_detail_skipped_large_block"
            elif candidate_reasons and high_detail_status in {"failed", "missing"}:
                coverage_status = "partial"
                warnings.append("high_detail_not_upgraded")
            else:
                coverage_status = "ok"
        else:
            error_text = ""
            if high_detail_result is not None and high_detail_result.error:
                error_text = high_detail_result.error
            elif base_result is not None and base_result.error:
                error_text = base_result.error
            uncovered_blocks.append({
                "block_id": block_id,
                "page": block.get("page"),
                "reason": "gemma_enrichment_failed",
                "error": error_text[:300] if error_text else "",
            })
            failed.append({
                "block_id": block_id,
                "page": block.get("page"),
                "error": error_text[:300] if error_text else "",
            })

        if final_enrichment is not None:
            final_enrichments[block_id] = final_enrichment

        summary_block = {
            "block_id": block_id,
            "base_status": base_status,
            "high_detail_status": high_detail_status,
            "final_profile": final_profile,
            "coverage_status": coverage_status,
            "warnings": sorted(set(warnings)),
            "base_response_source": (base_result.response_source if base_result else ""),
            "base_finish_reason": (base_result.finish_reason if base_result else ""),
            "high_detail_response_source": (high_detail_result.response_source if high_detail_result else ""),
            "high_detail_finish_reason": (high_detail_result.finish_reason if high_detail_result else ""),
        }
        if block_id in high_detail_safety:
            summary_block["high_detail_safety"] = high_detail_safety[block_id]
        summary_blocks.append(summary_block)

    new_md = _augment_md(md_text, final_enrichments, model, ts)
    ok_count = len(final_enrichments)
    marker = f"<!-- ENRICHMENT: {model} @ {ts} blocks={ok_count}/{total} ok -->\n"
    new_md = marker + new_md

    md_path.write_text(new_md, encoding="utf-8")

    elapsed_s = time.monotonic() - started
    all_results = list(base_results) + list(high_detail_results_by_id.values())
    truncated_blocks = [result for result in all_results if result.truncated]
    total_input_tokens = sum((result.input_tokens or 0) for result in all_results)
    total_output_tokens = sum((result.output_tokens or 0) for result in all_results)
    total_reasoning_tokens = sum((result.reasoning_tokens or 0) for result in all_results)
    summary_status = "ok" if all(block["coverage_status"] == "ok" for block in summary_blocks) else "partial"
    summary = build_gemma_summary(
        status=summary_status,
        project_dir=project_dir,
        md_path=md_path,
        model=model,
        blocks_total=total,
        blocks_ok=base_blocks_ok,
        blocks_failed=len(failed),
        extra={
            "blocks": summary_blocks,
            "md_path": str(md_path),
            "backup_path": str(bak_path),
            "timestamp": ts,
            "parallelism": parallelism,
            "base_parallelism": parallelism,
            "high_detail_parallelism": HIGH_DETAIL_PARALLELISM,
            "max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS,
            "blocks_truncated": len(truncated_blocks),
            "wall_clock_s": round(elapsed_s, 1),
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_reasoning_tokens": total_reasoning_tokens,
            "max_output_tokens_seen": max((result.output_tokens or 0) for result in all_results) if all_results else 0,
            "failed": failed,
            "uncovered_block_ids": [b["block_id"] for b in uncovered_blocks],
            "uncovered_blocks": uncovered_blocks,
            "truncated": [
                {"block_id": result.block_id, "page": result.page, "output_tokens": result.output_tokens}
                for result in truncated_blocks
            ],
            "base_retry_stats": base_retry_stats,
            "high_detail_retry_stats": high_detail_retry_stats,
            "high_detail_candidates": len(candidate_ids),
            "high_detail_candidate_block_ids": sorted(candidate_ids),
            "high_detail_ok": high_detail_ok,
            "high_detail_skipped_large": len(skipped_large_ids),
            "high_detail_blocks_index_hash": high_detail_index_hash,
            "blocks_analyzed_only_with_base_100": sorted(base_only_ids),
            "blocks_upgraded_to_300": sorted(upgraded_ids),
        },
    )

    summary_path = out_dir / "gemma_enrichment_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Записать meta.enrichment в document_graph.json (если граф существует)
    inject_enrichment_meta_into_graph(graph_path, {
        "source": model,
        "timestamp": ts,
        "blocks_ok": ok_count,
        "blocks_total": total,
        "base_profile": GEMMA_BASE_PROFILE,
        "high_detail_profile": GEMMA_HIGH_DETAIL_PROFILE,
        "high_detail_candidates": len(candidate_ids),
        "high_detail_ok": high_detail_ok,
    })

    await _emit(progress_cb, {"type": "completed", "summary": summary})
    return summary


async def retry_failed_blocks(
    project_dir: Path,
    *,
    model: str = DEFAULT_MODEL,
    timeout: int = DEFAULT_TIMEOUT_S,
    progress_cb: Optional[ProgressCb] = None,
    pause_event: Optional[asyncio.Event] = None,
    cancel_event: Optional[asyncio.Event] = None,
    force_base_rerun: bool = False,
    force_high_detail_rerun: bool = False,
    only_failed_missing_high_detail: bool = False,
) -> dict:
    """Retry helper for the split Gemma architecture.

    Modes:
      - `force_base_rerun=True` → full `enrich_project(force=True)`.
      - unresolved base failures present → full base rerun.
      - otherwise rerun only failed/missing high-detail candidates or, with
        `force_high_detail_rerun=True`, all existing high-detail candidates.
    """
    project_dir = Path(project_dir).resolve()
    out_dir = project_dir / "_output"
    summary_path = out_dir / "gemma_enrichment_summary.json"
    graph_path = out_dir / "document_graph.json"

    md_files = sorted(project_dir.glob("*_document.md"))
    if not md_files:
        return {"status": "error", "error": "MD-файл не найден"}
    md_path = md_files[0]

    if not summary_path.exists():
        return {"status": "error", "error": "gemma_enrichment_summary.json не найден — нечего ретраить"}
    prev_summary = load_json(summary_path)
    validation = validate_gemma_summary(project_dir, md_path=md_path, summary=prev_summary, min_coverage=0.0)
    if not validation.get("valid"):
        return {"status": "error", "error": f"gemma summary invalid: {validation.get('reason') or validation.get('reason_code')}"}

    prev_blocks = prev_summary.get("blocks") or []
    prev_blocks_by_id = {
        str(block.get("block_id") or ""): dict(block)
        for block in prev_blocks
        if isinstance(block, dict) and block.get("block_id")
    }
    base_failed_ids = sorted(
        block_id for block_id, block in prev_blocks_by_id.items()
        if str(block.get("base_status") or "") in {"failed", "missing"}
    )
    candidate_ids = sorted(
        block_id for block_id, block in prev_blocks_by_id.items()
        if str(block.get("high_detail_status") or "") != "not_needed"
    )
    unresolved_high_detail_ids = sorted(
        block_id for block_id, block in prev_blocks_by_id.items()
        if str(block.get("high_detail_status") or "") in {"failed", "missing"}
    )

    if force_base_rerun or base_failed_ids:
        await _emit(progress_cb, {
            "type": "retry_failed_started",
            "mode": "force_base_rerun" if force_base_rerun else "base_failed_full_rerun",
            "to_retry": len(base_failed_ids) or int(prev_summary.get("blocks_total") or 0),
            "missing": [],
            "model": model,
        })
        rerun = await enrich_project(
            project_dir,
            force=True,
            model=model,
            parallelism=DEFAULT_PARALLELISM,
            timeout=timeout,
            progress_cb=progress_cb,
            pause_event=pause_event,
            cancel_event=cancel_event,
        )
        rerun["retry_mode"] = "force_base_rerun" if force_base_rerun else "base_failed_full_rerun"
        rerun["retry_request"] = {
            "force_base_rerun": force_base_rerun,
            "force_high_detail_rerun": force_high_detail_rerun,
            "only_failed_missing_high_detail": only_failed_missing_high_detail,
        }
        await _emit(progress_cb, {"type": "retry_failed_completed", "summary": rerun})
        return rerun

    target_ids: list[str]
    if force_high_detail_rerun:
        target_ids = candidate_ids
        retry_mode = "force_high_detail_rerun"
    elif only_failed_missing_high_detail:
        target_ids = unresolved_high_detail_ids
        retry_mode = "failed_missing_high_detail_only"
    else:
        target_ids = unresolved_high_detail_ids
        retry_mode = "failed_missing_high_detail_only"

    if not target_ids:
        return {
            "status": "no_failed",
            "reason": "Нет base-failed или unresolved high-detail кандидатов — нечего ретраить",
            "summary": prev_summary,
        }

    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import parse_enrichment_from_md

    base_index = load_json(gemma_base_blocks_index_path(project_dir))
    base_blocks = [
        block for block in base_index.get("blocks", [])
        if isinstance(block, dict) and block.get("block_id")
    ]
    base_blocks_by_id = {str(block["block_id"]): block for block in base_blocks}
    missing_base_ids = [block_id for block_id in target_ids if block_id not in base_blocks_by_id]

    graph: dict = {}
    if graph_path.exists():
        try:
            graph = json.loads(graph_path.read_text(encoding="utf-8"))
        except Exception:
            graph = {}

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    started_mono = time.monotonic()
    await _emit(progress_cb, {
        "type": "retry_failed_started",
        "mode": retry_mode,
        "to_retry": len(target_ids),
        "missing": missing_base_ids,
        "model": model,
    })

    high_detail_policy = gemma_high_detail_crop_policy()
    high_detail_index = _ensure_crop_index(
        project_dir,
        policy=high_detail_policy,
        output_dir_name=GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME,
        block_ids=target_ids,
        force=force_high_detail_rerun,
    )
    high_detail_blocks = {
        str(block.get("block_id") or ""): block
        for block in high_detail_index.get("blocks", [])
        if isinstance(block, dict) and block.get("block_id")
    }
    safe_candidates: list[dict[str, Any]] = []
    high_detail_safety: dict[str, dict[str, Any]] = {}
    skipped_large_ids: list[str] = []
    for block_id in target_ids:
        block_300 = high_detail_blocks.get(block_id)
        if block_300 is None:
            high_detail_safety[block_id] = {
                "safe": False,
                "reason": "missing_high_detail_crop",
                "size_kb_300": 0.0,
                "long_side_300": 0,
                "estimated_image_tokens": 0,
            }
            continue
        safety = _high_detail_safety_meta(block_300)
        high_detail_safety[block_id] = safety
        if safety["safe"]:
            safe_candidates.append(block_300)
        else:
            skipped_large_ids.append(block_id)

    results_by_id: dict[str, EnrichResult] = {}
    retry_stats: dict[str, Any] = {
        "passes_planned": PROJECT_RETRY_PASSES,
        "passes_executed": 0,
        "attempts_total": 0,
        "recovered_total": 0,
        "still_failed_after_retries": 0,
        "passes": [],
    }
    if safe_candidates:
        base_url = _env("CHANDRA_BASE_URL").rstrip("/")
        retry_results, retry_stats = await _run_blocks_pass(
            image_blocks=safe_candidates,
            graph=graph,
            blocks_dir=gemma_high_detail_blocks_dir(project_dir),
            base_url=base_url,
            model=model,
            timeout=timeout,
            parallelism=HIGH_DETAIL_PARALLELISM,
            progress_cb=progress_cb,
            pause_event=pause_event,
            cancel_event=cancel_event,
            phase="retry_failed",
        )
        results_by_id = {result.block_id: result for result in retry_results}

    current_md_text = md_path.read_text(encoding="utf-8")
    current_final_enrichments = {
        block_id: parse_enrichment_from_md(current_md_text, block_id)
        for block_id in prev_blocks_by_id
    }

    updated_blocks: list[dict[str, Any]] = []
    final_enrichments: dict[str, dict] = {}
    uncovered_blocks: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    base_only_ids: list[str] = []
    upgraded_ids: list[str] = []

    for block_id, prev_block in prev_blocks_by_id.items():
        block = dict(prev_block)
        existing_enrichment = current_final_enrichments.get(block_id)
        warnings = list(block.get("warnings") or [])
        high_detail_status = str(block.get("high_detail_status") or "not_needed")
        coverage_status = str(block.get("coverage_status") or "missing_gemma_enrichment")
        final_profile = str(block.get("final_profile") or "none")

        if block_id in target_ids:
            if block_id in skipped_large_ids:
                high_detail_status = "skipped_large_block"
                warnings.append(WARNING_LARGE_BLOCK)
                if final_profile == GEMMA_BASE_PROFILE:
                    coverage_status = "high_detail_skipped_large_block"
            else:
                result = results_by_id.get(block_id)
                if result is not None and result.ok and result.enrichment:
                    existing_enrichment = result.enrichment
                    final_profile = GEMMA_HIGH_DETAIL_PROFILE
                    high_detail_status = _result_status(result)
                    coverage_status = "partial" if result.partial_ok else "ok"
                elif result is not None:
                    high_detail_status = _result_status(result)
                    if final_profile == GEMMA_BASE_PROFILE:
                        coverage_status = "partial"
                    elif final_profile == "none":
                        coverage_status = "missing_gemma_enrichment"
                else:
                    high_detail_status = "missing"

        if existing_enrichment is not None and final_profile != "none":
            final_enrichments[block_id] = existing_enrichment
            if final_profile == GEMMA_HIGH_DETAIL_PROFILE:
                upgraded_ids.append(block_id)
            elif final_profile == GEMMA_BASE_PROFILE:
                base_only_ids.append(block_id)
        else:
            final_profile = "none"
            coverage_status = "missing_gemma_enrichment"
            error_text = ""
            if block_id in results_by_id and results_by_id[block_id].error:
                error_text = str(results_by_id[block_id].error or "")
            uncovered_blocks.append({
                "block_id": block_id,
                "page": base_blocks_by_id.get(block_id, {}).get("page"),
                "reason": "gemma_enrichment_failed",
                "error": error_text[:300] if error_text else "",
            })
            failed.append({
                "block_id": block_id,
                "page": base_blocks_by_id.get(block_id, {}).get("page"),
                "error": error_text[:300] if error_text else "",
            })

        block["high_detail_status"] = high_detail_status
        block["final_profile"] = final_profile
        block["coverage_status"] = coverage_status
        block["warnings"] = sorted(set(warnings))
        if block_id in high_detail_safety:
            block["high_detail_safety"] = high_detail_safety[block_id]
        if block_id in results_by_id:
            block["high_detail_response_source"] = results_by_id[block_id].response_source
            block["high_detail_finish_reason"] = results_by_id[block_id].finish_reason
        updated_blocks.append(block)

    updated_blocks.sort(key=lambda block: str(block.get("block_id") or ""))
    md_clean = strip_gemma_enrichment(current_md_text)
    new_md = _augment_md(md_clean, final_enrichments, model, ts)
    marker = f"<!-- ENRICHMENT: {model} @ {ts} blocks={len(final_enrichments)}/{len(updated_blocks)} ok (retry-failed) -->\n"
    md_path.write_text(marker + new_md, encoding="utf-8")

    elapsed_s = time.monotonic() - started_mono
    all_results = list(results_by_id.values())
    new_summary = build_gemma_summary(
        status="ok" if all(str(block.get("coverage_status") or "") == "ok" for block in updated_blocks) else "partial",
        project_dir=project_dir,
        md_path=md_path,
        model=model,
        blocks_total=len(updated_blocks),
        blocks_ok=int(prev_summary.get("base_blocks_ok") or 0),
        blocks_failed=len(failed),
        extra={
            "blocks": updated_blocks,
            "timestamp_retry_failed": ts,
            "wall_clock_s": round(elapsed_s, 1),
            "failed": failed,
            "uncovered_blocks": uncovered_blocks,
            "high_detail_candidates": int(prev_summary.get("high_detail_candidates") or len(candidate_ids)),
            "high_detail_candidate_block_ids": prev_summary.get("high_detail_candidate_block_ids") or candidate_ids,
            "high_detail_ok": len(upgraded_ids),
            "high_detail_skipped_large": len([block for block in updated_blocks if block.get("high_detail_status") == "skipped_large_block"]),
            "high_detail_blocks_index_hash": blocks_index_hash(gemma_high_detail_blocks_index_path(project_dir)) or None,
            "blocks_analyzed_only_with_base_100": sorted(base_only_ids),
            "blocks_upgraded_to_300": sorted(upgraded_ids),
            "retry_failed_stats": {
                "ts": ts,
                "mode": retry_mode,
                "to_retry": len(target_ids),
                "safe_candidates": len(safe_candidates),
                "skipped_large_ids": sorted(skipped_large_ids),
                "missing_in_index": missing_base_ids,
                "recovered": len([result for result in all_results if result.ok]),
                "still_failed": len([result for result in all_results if not result.ok]),
                "elapsed_s": round(elapsed_s, 1),
                "input_tokens": sum((result.input_tokens or 0) for result in all_results),
                "output_tokens": sum((result.output_tokens or 0) for result in all_results),
            },
            "high_detail_retry_stats": retry_stats,
        },
    )
    new_summary["retry_mode"] = retry_mode
    new_summary["retry_request"] = {
        "force_base_rerun": force_base_rerun,
        "force_high_detail_rerun": force_high_detail_rerun,
        "only_failed_missing_high_detail": only_failed_missing_high_detail,
    }
    summary_path.write_text(json.dumps(new_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    inject_enrichment_meta_into_graph(graph_path, {
        "source": model,
        "timestamp": ts,
        "blocks_ok": len(final_enrichments),
        "blocks_total": len(updated_blocks),
        "retry_failed": True,
        "retry_mode": retry_mode,
    })

    await _emit(progress_cb, {"type": "retry_failed_completed", "summary": new_summary})
    return new_summary


# ─── CLI ───────────────────────────────────────────────────────────────────

async def _cli() -> int:
    parser = argparse.ArgumentParser(description="Gemma enrichment проекта (augment в MD)")
    parser.add_argument("project_dir", help="Путь к папке проекта")
    parser.add_argument("--force", action="store_true", help="Перезапустить даже если уже enriched")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Gemma модель")
    parser.add_argument("--parallelism", type=int, default=DEFAULT_PARALLELISM)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S, help="Таймаут per-request, сек")
    args = parser.parse_args()

    project_dir = Path(args.project_dir).resolve()
    if not project_dir.exists():
        print(f"Project dir not found: {project_dir}", file=sys.stderr)
        return 2

    # project_info.json overrides
    info_path = project_dir / "project_info.json"
    enrichment_cfg: dict = {}
    if info_path.exists():
        try:
            info = json.loads(info_path.read_text(encoding="utf-8"))
            enrichment_cfg = info.get("enrichment") or {}
        except Exception:
            pass

    model = enrichment_cfg.get("model") or args.model
    parallelism = enrichment_cfg.get("parallelism") or args.parallelism
    timeout = enrichment_cfg.get("timeout") or args.timeout

    def _print_progress(event: dict) -> None:
        t = event.get("type")
        if t == "started":
            print(f"[start] {event['total']} blocks, model={event['model']}, parallelism={event['parallelism']}")
        elif t == "block_done":
            mark = "OK " if event["ok"] else "FAIL"
            err = f" — {event['error'][:80]}" if event.get("error") else ""
            print(f"  [{event['completed']:>3}/{event['total']}] {mark} {event['block_id']} p={event['page']} t={event['elapsed_ms']/1000:.1f}s{err}")
        elif t == "skipped":
            existing = event.get("existing") or {}
            print(f"[skip] already enriched: {existing.get('model')} @ {existing.get('timestamp')} ({existing.get('blocks_ok')}/{existing.get('blocks_total')})")
            print("       use --force to re-enrich")
        elif t == "completed":
            s = event["summary"]
            print(f"[done] {s['blocks_ok']}/{s['blocks_total']} OK in {s['wall_clock_s']}s")
            print(f"       in={s['total_input_tokens']:,}  out={s['total_output_tokens']:,}  reason={s['total_reasoning_tokens']:,}")
        elif t == "no_blocks":
            print("[no-blocks] index.json содержит 0 image-блоков")

    summary = await enrich_project(
        project_dir,
        force=args.force,
        model=model,
        parallelism=parallelism,
        timeout=timeout,
        progress_cb=_print_progress,
    )

    if summary["status"] in ("ok", "partial", "skipped", "no_blocks"):
        return 0 if summary["status"] != "partial" else 1
    return 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_cli()))
