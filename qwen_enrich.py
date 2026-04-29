"""
qwen_enrich.py
--------------
Production-модуль Qwen-multimodal enrichment image-блоков с augment-merge в MD.

Логика:
1. Читает _output/blocks/index.json (создан blocks.py crop).
2. Для каждого image-блока вызывает Qwen multimodal через ngrok-tunnel,
   получает структурированное JSON-описание (block_type/marks/dimensions/etc).
3. Открывает MD-файл проекта, для каждого `### BLOCK [IMAGE]: <id>`
   добавляет markdown-секцию `**[ENRICHED qwen3.6-35b @ <ts>]**` с описанием.
4. Делает backup MD: `*_document.md.pre_enrichment.bak` (один, первый — не перезаписывает).
5. Записывает MD-маркер первой строкой:
   `<!-- ENRICHMENT: qwen3.6-35b @ <ts> blocks=42/45 ok -->`

Использование:
    # CLI
    python qwen_enrich.py projects/<name>
    python qwen_enrich.py projects/<name> --force      # перезатереть
    python qwen_enrich.py projects/<name> --parallelism 3

    # Программно
    from qwen_enrich import enrich_project
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
import os
import re
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Awaitable, Callable, Optional

import httpx
from dotenv import load_dotenv
from PIL import Image

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")


# ─── Constants ─────────────────────────────────────────────────────────────

DEFAULT_MODEL = "qwen/qwen3.6-35b-a3b"
DEFAULT_PARALLELISM = 1  # Qwen 3.6 35B не тянет параллель — последовательно
DEFAULT_TIMEOUT_S = 300
DEFAULT_MAX_OUTPUT_TOKENS = 2048  # типичный JSON ответ ~500-1500 токенов; 2048 = безопасный лимит
# При обрыве (truncated) автоматически повторяем со следующим тиром.
# 8192 — потолок (на context=16K есть ~10K input+output буфера).
RETRY_OUTPUT_TOKEN_TIERS = [DEFAULT_MAX_OUTPUT_TOKENS, 4096, 8192]
# После основного прохода — повторный прогон упавших блоков. Лотерейные сбои
# модели (битый JSON в середине, не truncation) часто проходят со 2-3 попытки.
# Итого максимум 3 попытки на блок: 1 исходная + 2 retry-pass.
PROJECT_RETRY_PASSES = 2
# Split-fallback: для очень вытянутых блоков (разрезы лестниц и т.п.) модель
# зацикливается на повторах. Если truncated на всех тирах И aspect выше порога —
# режем картинку пополам по высоте и обрабатываем половины отдельно.
SPLIT_ASPECT_THRESHOLD = 2.5  # height/width >= 2.5 → split-eligible
SPLIT_PARTS = 2  # на сколько кусков делим (тестирование показало что 2 хватает)
# Максимальная длинная сторона при отправке в Qwen. Блоки крупнее обрезаются до этого размера
# независимо от scale-тира. Исключает ситуацию «5400px блок на scale=1.0» → битый JSON.
MAX_INPUT_LONG_SIDE_PX = 1500

_IMAGE_SCALE_TIERS = [1.0, 0.6, 0.35, 0.2]
_NGROK_HTML_RETRIES = 2
_NGROK_HTML_BACKOFF_S = 1.5


# ─── Prompts ───────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Ты — инженер. Извлеки факты из блока чертежа: что нарисовано, марки, размеры, ссылки.
Отвечай строго JSON по схеме, без преамбулы и markdown. Не ищи ошибок. Не выдумывай.
Если поле не видно — [] или null.
"""

USER_INSTRUCTION = """JSON по схеме:
{{
"block_type":"план|план_армирования|план_опалубки|разрез|сечение|ведомость|схема|таблица|узел|другое",
"subject":"до 120 симв — что изображено",
"marks":["марки/позиции"],
"rebar_specs":["параметры арматуры"],
"dimensions":["размеры"],
"references_on_block":["ссылки на листы/разрезы"],
"axes":["оси"],
"level_marks":["отметки"],
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
    # Без этого блоки >5000px отправляются в Qwen целиком и ломают JSON на ~450 символе.
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
    """Сервер qwen-VL отклонил запрос: input не лезет в n_ctx (типично 4096).
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
            return "\n".join(parts)[:1500]
    return ""


def _load_sheet_no(graph: dict, page: int) -> str:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            return str(p.get("sheet_no_normalized") or p.get("sheet_no_raw") or "")
    return ""


# ─── Qwen call ─────────────────────────────────────────────────────────────

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


async def _qwen_call_attempt(
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
        "reasoning": "off",
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


def _parse_qwen_json(data: dict) -> tuple[dict | None, str | None]:
    """Извлечь и распарсить JSON из output Qwen."""
    msg_parts: list[str] = []
    for item in data.get("output", []) or []:
        if isinstance(item, dict) and item.get("type") == "message":
            msg_parts.append(str(item.get("content") or ""))
    text = "\n".join(msg_parts).strip()
    if not text:
        return None, "empty message"
    # Прямой JSON
    try:
        return json.loads(text), None
    except json.JSONDecodeError as e:
        first_err = str(e)
    # Извлечь {...} из текста
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0)), None
        except json.JSONDecodeError as e:
            # Попробовать ремонт перед тем как сдаться
            repaired = _repair_json(m.group(0))
            if repaired is not None:
                return repaired, None
            return None, f"{first_err}; extracted: {e}"
    # Попробовать ремонт всего текста (на случай если {} не нашёлся из-за обрыва)
    repaired = _repair_json(text)
    if repaired is not None:
        return repaired, None
    return None, first_err


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
        status, data, raw, elapsed = await _qwen_call_attempt(
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
            parsed_check, parse_err_check = _parse_qwen_json(data)
            if parse_err_check is not None and scale_idx < len(_IMAGE_SCALE_TIERS) - 1:
                scale_idx += 1
                continue

        break

    if last_status < 0 or last_status >= 400 or last_data is None:
        err = (last_raw or "")[:500] if last_status >= 0 else last_raw
        return EnrichResult(
            block_id=block_id, page=page, ok=False,
            elapsed_ms=total_elapsed_ms, error=err or f"http {last_status}",
        )

    parsed, parse_err = _parse_qwen_json(last_data)
    stats = last_data.get("stats", {}) if isinstance(last_data, dict) else {}

    output_tokens = stats.get("total_output_tokens") or 0
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
            input_tokens=stats.get("input_tokens"),
            output_tokens=output_tokens,
            reasoning_tokens=stats.get("reasoning_output_tokens"),
            truncated=truncated,
        )

    return EnrichResult(
        block_id=block_id, page=page, ok=True,
        elapsed_ms=total_elapsed_ms,
        enrichment=parsed,
        input_tokens=stats.get("input_tokens"),
        output_tokens=output_tokens,
        reasoning_tokens=stats.get("reasoning_output_tokens"),
        truncated=truncated,
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
    tmpdir = Path(tempfile.mkdtemp(prefix=f"qwen_split_{png_path.stem}_"))
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

ENRICHMENT_MARKER_RE = re.compile(
    r"^<!--\s*ENRICHMENT:\s*(?P<model>\S+)\s*@\s*(?P<ts>\S+)\s+blocks=(?P<ok>\d+)/(?P<total>\d+).*?-->\s*\n",
    re.MULTILINE,
)
BLOCK_HEADER_RE = re.compile(r"^### BLOCK \[(IMAGE|TEXT)\]:\s*(\S+)\s*$", re.MULTILINE)
# Регекс работает ТОЛЬКО внутри тела одного блока (между ### заголовками),
# так что жадная match всех строк безопасна.
ENRICHED_IN_BLOCK_RE = re.compile(
    r"\n*\*\*\[ENRICHED [^\]]+\]\*\*\n(?:.*\n)*",
    re.MULTILINE,
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


def _strip_enrichment_in_block(block_body: str) -> str:
    """Удалить [ENRICHED ...] секцию ВНУТРИ одного блока (между ### заголовками).

    После удаления нормализуем trailing newlines до `\\n\\n`, чтобы
    между блоками сохранялась пустая строка (regex может съесть лишние \\n).
    """
    new_body = ENRICHED_IN_BLOCK_RE.sub("", block_body)
    if new_body != block_body:
        new_body = new_body.rstrip() + "\n\n"
    return new_body


def _strip_existing_enrichment(md_text: str) -> str:
    """Убрать все [ENRICHED ...] секции из MD (для force re-enrich).

    Идём по блокам по `### BLOCK` границам и чистим тело каждого блока,
    чтобы не задеть соседние блоки.
    """
    headers = list(BLOCK_HEADER_RE.finditer(md_text))
    if not headers:
        return md_text

    page_starts = [m.start() for m in re.finditer(r"^## СТРАНИЦА\s+\d+\s*$", md_text, re.MULTILINE)]
    parts: list[str] = []
    cursor = 0
    for i, h in enumerate(headers):
        block_start = h.start()
        next_block_pos = headers[i + 1].start() if i + 1 < len(headers) else len(md_text)
        next_page_pos = next((p for p in page_starts if p > block_start), len(md_text))
        block_end = min(next_block_pos, next_page_pos)
        parts.append(md_text[cursor:block_start])
        parts.append(_strip_enrichment_in_block(md_text[block_start:block_end]))
        cursor = block_end
    parts.append(md_text[cursor:])
    return "".join(parts)


def _strip_existing_marker(md_text: str) -> str:
    """Убрать существующий маркер ENRICHMENT в начале файла."""
    return ENRICHMENT_MARKER_RE.sub("", md_text, count=1)


def get_enrichment_meta(md_path: Path) -> Optional[dict]:
    """Прочитать MD-маркер enrichment (если есть). Возвращает dict или None."""
    if not md_path.exists():
        return None
    head = md_path.read_text(encoding="utf-8")[:1024]
    m = ENRICHMENT_MARKER_RE.search(head)
    if not m:
        return None
    return {
        "model": m.group("model"),
        "timestamp": m.group("ts"),
        "blocks_ok": int(m.group("ok")),
        "blocks_total": int(m.group("total")),
    }


def is_enriched(md_path: Path) -> bool:
    return get_enrichment_meta(md_path) is not None


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
    headers = list(BLOCK_HEADER_RE.finditer(md_text))
    if not headers:
        return md_text

    # Найдём также все ## СТРАНИЦА для границ блоков
    page_re = re.compile(r"^## СТРАНИЦА\s+\d+\s*$", re.MULTILINE)
    page_starts = [m.start() for m in page_re.finditer(md_text)]

    # Соберём результирующий текст по сегментам
    result: list[str] = []
    cursor = 0
    for i, h in enumerate(headers):
        kind = h.group(1)
        bid = h.group(2)
        block_start = h.start()

        # Найти конец блока: следующий ### BLOCK или ## СТРАНИЦА после block_start
        next_block_pos = headers[i + 1].start() if i + 1 < len(headers) else len(md_text)
        next_page_pos = next((p for p in page_starts if p > block_start), len(md_text))
        block_end = min(next_block_pos, next_page_pos)

        # Часть до этого блока (включая заголовок и предыдущий контекст)
        result.append(md_text[cursor:block_start])
        block_body = md_text[block_start:block_end]

        # Снять старый ENRICHED (на случай force re-enrich или повторного вызова)
        block_body = _strip_enrichment_in_block(block_body)

        if kind == "IMAGE":
            enr = enrichments_by_id.get(bid)
            if enr is not None:
                # Пустая строка между Chandra-описанием и ENRICHED + пустая после
                block_body = block_body.rstrip() + _format_enrichment_md(enr, model, ts) + "\n\n"

        result.append(block_body)
        cursor = block_end

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
    """Enrichment проекта. Возвращает summary dict.

    Шаги:
      - Проверить MD и blocks/index.json
      - Если уже enriched и не force — выйти
      - Backup MD (один первый)
      - Прогнать Qwen на всех image-блоках с заданным parallelism
      - Augment-merge в MD + добавить маркер
      - Записать summary.json в _output/qwen_enrichment_summary.json
    """
    project_dir = Path(project_dir).resolve()
    out_dir = project_dir / "_output"
    blocks_dir = out_dir / "blocks"
    index_path = blocks_dir / "index.json"
    graph_path = out_dir / "document_graph.json"

    md_files = sorted(project_dir.glob("*_document.md"))
    if not md_files:
        raise FileNotFoundError(f"MD-файл не найден в {project_dir}")
    md_path = md_files[0]

    if not index_path.exists():
        raise FileNotFoundError(
            f"{index_path} не найден — сначала запустите blocks.py crop"
        )

    # Проверка: уже enriched?
    existing_meta = get_enrichment_meta(md_path)
    if existing_meta and not force:
        await _emit(progress_cb, {
            "type": "skipped",
            "reason": "already_enriched",
            "existing": existing_meta,
        })
        return {
            "status": "skipped",
            "reason": "already_enriched",
            "existing": existing_meta,
            "md_path": str(md_path),
        }

    base_url = _env("CHANDRA_BASE_URL").rstrip("/")

    index = json.loads(index_path.read_text(encoding="utf-8"))
    blocks = index.get("blocks", [])
    image_blocks = [b for b in blocks if (b.get("block_type") or "").lower() == "image"]
    total = len(image_blocks)

    if total == 0:
        await _emit(progress_cb, {"type": "no_blocks"})
        return {"status": "no_blocks", "md_path": str(md_path)}

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
    md_text = md_path.read_text(encoding="utf-8")
    if force:
        md_text = _strip_existing_marker(md_text)
        md_text = _strip_existing_enrichment(md_text)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    started = time.monotonic()
    sem = asyncio.Semaphore(max(1, parallelism))

    await _emit(progress_cb, {
        "type": "started",
        "total": total,
        "model": model,
        "parallelism": parallelism,
        "timestamp": ts,
    })

    completed = 0
    completed_lock = asyncio.Lock()
    results: list[EnrichResult] = []

    async with httpx.AsyncClient(timeout=timeout + 30) as client:
        async def _process_block(block: dict, *, count_progress: bool, attempt: int) -> EnrichResult:
            """Один прогон блока. count_progress=True — увеличиваем счётчик и шлём
            block_done; False — это retry-pass, шлём retry_block_done."""
            nonlocal completed
            # Cancel — пропускаем блок (будет помечен как cancelled в результате)
            if cancel_event is not None and cancel_event.is_set():
                return EnrichResult(
                    block_id=block["block_id"], page=block.get("page", 0),
                    ok=False, error="cancelled by user",
                )
            # Pause — ждём пока не разблокируют (или не отменят)
            if pause_event is not None:
                while not pause_event.is_set():
                    if cancel_event is not None and cancel_event.is_set():
                        return EnrichResult(
                            block_id=block["block_id"], page=block.get("page", 0),
                            ok=False, error="cancelled during pause",
                        )
                    await asyncio.sleep(0.5)
            async with sem:
                if count_progress:
                    await _emit(progress_cb, {
                        "type": "block_started",
                        "block_id": block["block_id"],
                        "page": block.get("page"),
                        "completed": completed,
                        "total": total,
                    })
                rec = await enrich_one_block(
                    client, base_url, block, graph, blocks_dir, model, timeout,
                    progress_cb=progress_cb,
                )
                if count_progress:
                    async with completed_lock:
                        completed += 1
                    await _emit(progress_cb, {
                        "type": "block_done",
                        "block_id": rec.block_id,
                        "page": rec.page,
                        "ok": rec.ok,
                        "elapsed_ms": rec.elapsed_ms,
                        "completed": completed,
                        "total": total,
                        "error": rec.error,
                        "truncated": rec.truncated,
                        "output_tokens": rec.output_tokens,
                    })
                else:
                    await _emit(progress_cb, {
                        "type": "retry_block_done",
                        "block_id": rec.block_id,
                        "page": rec.page,
                        "ok": rec.ok,
                        "elapsed_ms": rec.elapsed_ms,
                        "attempt": attempt,
                        "max_attempts": PROJECT_RETRY_PASSES + 1,
                        "error": rec.error,
                        "truncated": rec.truncated,
                        "output_tokens": rec.output_tokens,
                    })
                return rec

        async def _runner(block: dict) -> EnrichResult:
            return await _process_block(block, count_progress=True, attempt=1)

        results = await asyncio.gather(*(_runner(b) for b in image_blocks))

        # ─── Project-level retry passes для упавших блоков ───
        # Cancel — пользователь явно остановил; не ретраим.
        # Отказы по причине cancellation тоже не ретраим (это была воля юзера).
        retry_stats: dict = {
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
                i for i, r in enumerate(results)
                if not r.ok and not (r.error or "").startswith("cancelled")
            ]
            if not failed_idx:
                break
            attempt_no = retry_pass + 2  # человеческая нумерация: 2-я / 3-я попытка
            blocks_by_id = {b["block_id"]: b for b in image_blocks}
            retry_blocks = [blocks_by_id[results[i].block_id] for i in failed_idx]
            await _emit(progress_cb, {
                "type": "retry_pass_started",
                "attempt": attempt_no,
                "max_attempts": PROJECT_RETRY_PASSES + 1,
                "to_retry": len(retry_blocks),
            })
            retry_results = await asyncio.gather(*(
                _process_block(b, count_progress=False, attempt=attempt_no)
                for b in retry_blocks
            ))
            recovered = 0
            for orig_i, new_res in zip(failed_idx, retry_results):
                if new_res.ok:
                    results[orig_i] = new_res
                    recovered += 1
                else:
                    # Сохраняем последнюю ошибку retry — может быть информативнее.
                    results[orig_i] = new_res
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
                "type": "retry_pass_done",
                "attempt": attempt_no,
                "max_attempts": PROJECT_RETRY_PASSES + 1,
                "recovered": recovered,
                "still_failed": len(retry_blocks) - recovered,
            })
        retry_stats["still_failed_after_retries"] = sum(
            1 for r in results
            if not r.ok and not (r.error or "").startswith("cancelled")
        )

    elapsed_s = time.monotonic() - started
    ok_count = sum(1 for r in results if r.ok)
    failed = [r for r in results if not r.ok]

    enrichments_by_id = {r.block_id: r.enrichment for r in results if r.ok and r.enrichment}

    # Если все блоки упали — НЕ записываем MD/маркер. Иначе skip-логика
    # будет считать проект обогащённым и пропустит при следующем запуске.
    if ok_count == 0:
        await _emit(progress_cb, {
            "type": "all_failed",
            "total": total,
            "failed": [{"block_id": r.block_id, "error": (r.error or "")[:300]} for r in failed],
        })
        result_summary = {
            "status": "failed",
            "md_path": str(md_path),
            "model": model,
            "timestamp": ts,
            "blocks_total": total,
            "blocks_ok": 0,
            "blocks_failed": len(failed),
            "wall_clock_s": round(elapsed_s, 1),
            "failed": [
                {"block_id": r.block_id, "page": r.page, "error": (r.error or "")[:300]}
                for r in failed
            ],
            "reason": "Все блоки упали — MD не изменён, маркер не записан",
            "retry_stats": retry_stats,
        }
        # Запишем только summary (для дебага), без trogan'я MD/graph
        (out_dir / "qwen_enrichment_summary.json").write_text(
            json.dumps(result_summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        await _emit(progress_cb, {"type": "completed", "summary": result_summary})
        return result_summary

    # Augment-merge + маркер
    new_md = _augment_md(md_text, enrichments_by_id, model, ts)
    marker = f"<!-- ENRICHMENT: {model} @ {ts} blocks={ok_count}/{total} ok -->\n"
    new_md = marker + new_md

    md_path.write_text(new_md, encoding="utf-8")

    truncated_blocks = [r for r in results if r.truncated]
    summary = {
        "status": "ok" if ok_count == total else "partial",
        "md_path": str(md_path),
        "backup_path": str(bak_path),
        "model": model,
        "timestamp": ts,
        "parallelism": parallelism,
        "max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS,
        "blocks_total": total,
        "blocks_ok": ok_count,
        "blocks_failed": len(failed),
        "blocks_truncated": len(truncated_blocks),
        "wall_clock_s": round(elapsed_s, 1),
        "total_input_tokens": sum((r.input_tokens or 0) for r in results),
        "total_output_tokens": sum((r.output_tokens or 0) for r in results),
        "total_reasoning_tokens": sum((r.reasoning_tokens or 0) for r in results),
        "max_output_tokens_seen": max((r.output_tokens or 0) for r in results) if results else 0,
        "failed": [
            {"block_id": r.block_id, "page": r.page, "error": (r.error or "")[:300]}
            for r in failed
        ],
        "truncated": [
            {"block_id": r.block_id, "page": r.page, "output_tokens": r.output_tokens}
            for r in truncated_blocks
        ],
        "retry_stats": retry_stats,
    }

    summary_path = out_dir / "qwen_enrichment_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Записать meta.enrichment в document_graph.json (если граф существует)
    inject_enrichment_meta_into_graph(graph_path, {
        "source": model,
        "timestamp": ts,
        "blocks_ok": ok_count,
        "blocks_total": total,
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
) -> dict:
    """Перепрогнать только упавшие блоки прошлого enrichment'а.

    Читает qwen_enrichment_summary.json → берёт failed_block_ids → прогоняет
    каждый через enrich_one_block (с новой логикой retry tier + split fallback).
    Восстановленные блоки вшиваются в существующий MD через _augment_md.
    Маркер MD и summary обновляются: blocks_ok ↑, failed список ↓.

    Не трогает успешные блоки и не делает backup MD (он уже есть от прошлого прогона).
    """
    project_dir = Path(project_dir).resolve()
    out_dir = project_dir / "_output"
    blocks_dir = out_dir / "blocks"
    summary_path = out_dir / "qwen_enrichment_summary.json"
    index_path = blocks_dir / "index.json"
    graph_path = out_dir / "document_graph.json"

    md_files = sorted(project_dir.glob("*_document.md"))
    if not md_files:
        return {"status": "error", "error": "MD-файл не найден"}
    md_path = md_files[0]

    if not summary_path.exists():
        return {"status": "error", "error": "qwen_enrichment_summary.json не найден — нечего ретраить"}
    if not index_path.exists():
        return {"status": "error", "error": "blocks/index.json не найден"}

    prev_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    failed_records = prev_summary.get("failed") or []
    failed_ids = [f["block_id"] for f in failed_records if "block_id" in f]
    if not failed_ids:
        return {
            "status": "no_failed",
            "reason": "В summary нет упавших блоков — нечего ретраить",
            "summary": prev_summary,
        }

    # Подгружаем блоки только для упавших ID
    index = json.loads(index_path.read_text(encoding="utf-8"))
    blocks_all = index.get("blocks", [])
    blocks_by_id = {b["block_id"]: b for b in blocks_all}
    retry_blocks = [blocks_by_id[bid] for bid in failed_ids if bid in blocks_by_id]
    missing = [bid for bid in failed_ids if bid not in blocks_by_id]

    graph: dict = {}
    if graph_path.exists():
        try:
            graph = json.loads(graph_path.read_text(encoding="utf-8"))
        except Exception:
            graph = {}

    base_url = _env("CHANDRA_BASE_URL").rstrip("/")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    started_mono = time.monotonic()

    await _emit(progress_cb, {
        "type": "retry_failed_started",
        "to_retry": len(retry_blocks),
        "missing": missing,
        "model": model,
    })

    recovered: dict[str, dict] = {}  # block_id → enrichment
    still_failed: list[dict] = []
    new_truncated: list[dict] = []
    in_tok = out_tok = 0

    async with httpx.AsyncClient(timeout=timeout + 30) as client:
        for idx_i, block in enumerate(retry_blocks, start=1):
            if cancel_event is not None and cancel_event.is_set():
                break
            if pause_event is not None:
                while not pause_event.is_set():
                    if cancel_event is not None and cancel_event.is_set():
                        break
                    await asyncio.sleep(0.5)
            await _emit(progress_cb, {
                "type": "retry_failed_block_started",
                "block_id": block["block_id"],
                "page": block.get("page"),
                "index": idx_i,
                "total": len(retry_blocks),
            })
            rec = await enrich_one_block(
                client, base_url, block, graph, blocks_dir,
                model, timeout, progress_cb=progress_cb,
            )
            in_tok += rec.input_tokens or 0
            out_tok += rec.output_tokens or 0
            await _emit(progress_cb, {
                "type": "retry_failed_block_done",
                "block_id": rec.block_id,
                "page": rec.page,
                "ok": rec.ok,
                "elapsed_ms": rec.elapsed_ms,
                "index": idx_i,
                "total": len(retry_blocks),
                "error": rec.error,
                "truncated": rec.truncated,
                "output_tokens": rec.output_tokens,
            })
            if rec.ok and rec.enrichment:
                recovered[rec.block_id] = rec.enrichment
            else:
                still_failed.append({
                    "block_id": rec.block_id,
                    "page": rec.page,
                    "error": (rec.error or "")[:300],
                })
            if rec.truncated:
                new_truncated.append({
                    "block_id": rec.block_id,
                    "page": rec.page,
                    "output_tokens": rec.output_tokens,
                })

    elapsed_s = time.monotonic() - started_mono

    # Вшиваем восстановленные в MD (если есть что вшивать)
    if recovered:
        md_text = md_path.read_text(encoding="utf-8")
        # _augment_md трогает только блоки чьи id есть в recovered — остальные не меняет.
        new_md = _augment_md(md_text, recovered, model, ts)
        # Обновляем маркер: blocks=newOk/total
        prev_marker = ENRICHMENT_MARKER_RE.search(new_md[:1024])
        if prev_marker:
            prev_ok = int(prev_marker.group("ok"))
            total = int(prev_marker.group("total"))
            new_ok = prev_ok + len(recovered)
            new_md = ENRICHMENT_MARKER_RE.sub("", new_md, count=1)
            marker = f"<!-- ENRICHMENT: {model} @ {ts} blocks={new_ok}/{total} ok (retry-failed) -->\n"
            new_md = marker + new_md
        md_path.write_text(new_md, encoding="utf-8")

    # Обновляем summary
    new_blocks_ok = (prev_summary.get("blocks_ok") or 0) + len(recovered)
    new_blocks_failed = len(still_failed)
    blocks_total = prev_summary.get("blocks_total") or 0
    # truncated — обновляем только новый список (не суммируем со старым: те блоки,
    # что были truncated и теперь успешны, не должны числиться truncated)
    new_summary = dict(prev_summary)
    new_summary.update({
        "status": "ok" if new_blocks_ok == blocks_total else "partial",
        "timestamp_retry_failed": ts,
        "blocks_ok": new_blocks_ok,
        "blocks_failed": new_blocks_failed,
        "blocks_truncated": (prev_summary.get("blocks_truncated") or 0)
                          - sum(1 for f in failed_records if f["block_id"] in recovered)
                          + sum(1 for t in new_truncated if t["block_id"] not in recovered),
        "wall_clock_s": round(elapsed_s, 1),  # время этого retry-прохода
        "failed": still_failed,
        "retry_failed_stats": {
            "ts": ts,
            "to_retry": len(retry_blocks),
            "recovered": len(recovered),
            "still_failed": len(still_failed),
            "missing_in_index": missing,
            "elapsed_s": round(elapsed_s, 1),
            "input_tokens": in_tok,
            "output_tokens": out_tok,
        },
    })
    new_summary["blocks_truncated"] = max(0, new_summary["blocks_truncated"])
    summary_path.write_text(json.dumps(new_summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Обновим meta.enrichment в graph
    if recovered:
        inject_enrichment_meta_into_graph(graph_path, {
            "source": model,
            "timestamp": ts,
            "blocks_ok": new_blocks_ok,
            "blocks_total": blocks_total,
            "retry_failed": True,
        })

    await _emit(progress_cb, {"type": "retry_failed_completed", "summary": new_summary})
    return new_summary


# ─── CLI ───────────────────────────────────────────────────────────────────

async def _cli() -> int:
    parser = argparse.ArgumentParser(description="Qwen enrichment проекта (augment в MD)")
    parser.add_argument("project_dir", help="Путь к папке проекта")
    parser.add_argument("--force", action="store_true", help="Перезапустить даже если уже enriched")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Qwen модель")
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
