"""
gemma_findings_only.py
---------------------
Production-модуль stage 02 в режиме findings-only + Gemma-enrichment.

Поддерживает transport'ы:
  - OpenRouter (GPT-5.4, Gemini Flash/Pro)  — HTTP + json_schema
  - Claude CLI (Sonnet/Opus через subscription) — subprocess `claude -p`
  - Codex CLI (subscription) — `codex exec --image`, JSON-only

Режим `ensemble/gpt-codex` запускает GPT и Codex независимо на одинаковом
single-block payload и сохраняет оба набора findings до Stage 03.

Выбирается по model id: `claude-*`, `codex/*`, `ensemble/gpt-codex` или OpenRouter.

Используется и из CLI-скрипта (scripts/run_stage02_findings_only_gpt54.py),
и из webapp pipeline_service (вместо batched stage 02). Оба пути делятся
одной функцией `run_findings_only_for_project()`.

Per-block flow:
  PNG из _output/blocks_stage02_100/  +  gemma-описание (JSON или MD-парсинг)  +  page text
  → модель single-block + findings-only + extended <SECTION>/finding_categories.md
  → {"findings": [...]}
  → адаптируется под production block_analyses[] формат stage 03.

Перезапись _output/01_blocks_analysis.json опциональна (write_target=True).
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    BLOCKS_META_KEY,
    BLOCK_CONTEXT_SUMMARY_FILENAME,
)
from backend.app.pipeline.stages.block_context.contract import (
    load_block_context_summary,
    validate_block_context_summary,
)
from backend.app.pipeline.stages.crop_blocks.block_markdown import ENRICHED_LINE_RE, extract_block_sections
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BLOCKS_DIRNAME,
    STAGE02_BLOCKS_DIRNAME,
    crop_index_matches_policy,
    gemma_output_root,
    stage02_blocks_dir,
    stage02_blocks_index_path,
    stage02_crop_policy,
)

from backend.app.core.config import (
    CODEX_STAGE_MODEL_ID,
    PROMPTS_DIR as _PROMPTS_DIR,
    STAGE02_DUAL_MODEL_ID,
    is_codex_model,
)
from backend.app.services.storage.projects_v2_source_resolver import (
    load_version_project_info,
    resolve_version_source_files,
)
from backend.app.pipeline.stages.block_analysis.provenance import (
    STAGE01_PROMPT_VERSION,
    build_finding_provenance,
    detector_for_model,
)

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "openai/gpt-5.4"
DEFAULT_EFFORT = "low"
DEFAULT_MAX_TOKENS = 16000
DEFAULT_PARALLELISM = 3
DEFAULT_TIMEOUT_S = 200
RUNTIME_PLAN_SCHEMA_VERSION = 1

PRICE_IN = 2.50
PRICE_OUT = 15.0

# Claude CLI binary (subscription transport). Можно переопределить через env.
CLAUDE_CLI_BIN = os.environ.get("CLAUDE_CLI_BIN", str(Path.home() / ".local" / "bin" / "claude"))

# clean_cwd: запуск `claude -p` из чистой папки + урезанным env, чтобы не подгружать
# CLAUDE.md проекта, .claude/settings.json, hooks, project memory, skills manifest.
# Эмпирически даёт −44% input/блок и −52% cli_cost для Stage 01.
_CLEAN_CWD_PATH = "/tmp/sonnet_clean"
_CLEAN_ENV_KEEP = {"HOME", "PATH", "LANG", "LC_ALL", "USER", "SHELL"}


def _ensure_clean_cwd() -> str:
    """Создать (если нужно) и очистить /tmp/sonnet_clean. Возвращает путь."""
    p = _CLEAN_CWD_PATH
    os.makedirs(p, exist_ok=True)
    for entry in os.listdir(p):
        full = os.path.join(p, entry)
        if os.path.isfile(full):
            try:
                os.unlink(full)
            except OSError:
                pass
    return p


def _build_clean_env() -> dict:
    """Минимальный env (HOME/PATH/LANG/LC_ALL/USER/SHELL/XDG_*) — исключает project memory,
    skills manifest и прочие context-dependent артефакты Claude CLI."""
    keep = {}
    for k, v in os.environ.items():
        if k in _CLEAN_ENV_KEEP or k.startswith("XDG_"):
            keep[k] = v
    return keep


def is_claude_cli_model(model: str) -> bool:
    """Sonnet/Opus через Claude CLI subscription (`claude-sonnet-4-6`, `claude-opus-4-7`, …)."""
    return model.startswith("claude-")


# ─── Prompt ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT_BASE = """Ты — инженер-проверяющий проектную документацию жилого здания, проверяющий чертёж на ошибки.

На вход ты получишь:
  1. ИЗОБРАЖЕНИЕ одного блока чертежа.
  2. Уже извлечённое структурированное ОПИСАНИЕ блока (block_type, marks, dimensions, references, level_marks, rebar_specs и т.п.) — считай его корректным контекстом.
  3. Текстовый контекст страницы (общие указания, спецификации и т.д.).

Твоя ЕДИНСТВЕННАЯ задача — вернуть массив findings[] с найденными проблемами.
НЕ описывай что видишь на блоке. НЕ пересказывай описание. НЕ делай summary.
Если проблем не нашёл — верни {"findings": []}.

Каждое finding:
  - severity: одно из "КРИТИЧЕСКОЕ" | "ЭКОНОМИЧЕСКОЕ" | "ЭКСПЛУАТАЦИОННОЕ" | "РЕКОМЕНДАТЕЛЬНОЕ" | "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
  - category: короткий тег (snake_case)
  - finding: суть замечания (конкретно, с цифрами и марками, 1-3 предложения)
  - norm_quote: цитата или ссылка на пункт нормы РФ если применимо, иначе null
  - value_found: точная цитата с чертежа (значение, марка, размер) — или пустая строка
  - recommendation: что делать (1 предложение)

Строго JSON, без markdown-обёртки, без преамбулы.
"""

_EXTENDED_HEADER = """

## Категории замечаний (пройди мысленно по ВСЕМУ списку — это чек-лист направлений поиска)

Для КАЖДОЙ категории ниже проверь, применима ли она к этому блоку, и если применима — нет ли в блоке соответствующей проблемы. НЕ пропускай категории «для красоты» — особенно cross-discipline и cross-section. Эти категории часто выпадают из фокуса, но именно там находятся важнейшие замечания.

"""


def load_categories_for_section(section: str) -> str:
    """Подгрузить prompts/disciplines/<SECTION>/finding_categories.md (или пусто, если нет)."""
    path = _PROMPTS_DIR / "disciplines" / section / "finding_categories.md"
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""


def build_system_prompt(section: str, extended: bool) -> str:
    if not extended:
        return SYSTEM_PROMPT_BASE
    cats = load_categories_for_section(section)
    if not cats:
        return SYSTEM_PROMPT_BASE
    return SYSTEM_PROMPT_BASE + _EXTENDED_HEADER + cats + "\n"


RESPONSE_SCHEMA = {
    "name": "findings_only",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "findings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "severity": {"type": "string"},
                        "category": {"type": "string"},
                        "finding": {"type": "string"},
                        "norm_quote": {"type": ["string", "null"]},
                        "value_found": {"type": "string"},
                        "recommendation": {"type": "string"},
                    },
                    "required": [
                        "severity", "category", "finding",
                        "norm_quote", "value_found", "recommendation",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["findings"],
        "additionalProperties": False,
    },
}


# ─── Enrichment loaders ──────────────────────────────────────────────────────

def latest_gemma_enrichment(project_dir: Path, block_id: str) -> Optional[dict]:
    """Свежий enrichment-JSON из _experiments/gemma_enrichment/<latest>/block_<id>.json."""
    root = project_dir / "_experiments" / "gemma_enrichment"
    if not root.exists():
        return None
    for run_dir in sorted(root.iterdir(), reverse=True):
        if not run_dir.is_dir():
            continue
        path = run_dir / f"block_{block_id}.json"
        if path.exists():
            try:
                rec = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if rec.get("ok") and rec.get("enrichment"):
                return rec["enrichment"]
    return None


_ENRICHED_BULLET_RE = re.compile(r"^- \*\*(?P<key>[^:*]+):\*\*\s*(?P<val>.+)$")


def parse_enrichment_from_md(md_text: str, block_id: str) -> Optional[dict]:
    """Fallback: из MD-секции **[ENRICHED ...]** для конкретного block_id."""
    image_sections = [s for s in extract_block_sections(md_text) if s.type == "IMAGE"]
    target = next((s for s in image_sections if s.id == block_id), None)
    if target is None:
        # Exact match wins. A unique case-insensitive fallback helps when a legacy
        # index and MD differ only by case, without normalizing underscores/dots.
        casefold_matches = [s for s in image_sections if s.id.casefold() == block_id.casefold()]
        if len(casefold_matches) == 1:
            target = casefold_matches[0]
    if target is None:
        return None
    body = target.body

    er_match = ENRICHED_LINE_RE.search(body)
    if not er_match:
        return None
    section = body[er_match.end():]

    label_to_key = {
        "Тип блока": "block_type", "Содержание": "subject",
        "Марки": "marks", "Арматура": "rebar_specs",
        "Размеры": "dimensions", "Оси": "axes",
        "Отметки": "level_marks", "Бетон": "concrete_class",
        "Ссылки": "references_on_block", "Заметки": "notes",
    }
    list_keys = {"marks", "rebar_specs", "dimensions", "axes", "level_marks", "references_on_block"}

    out: dict[str, Any] = {}
    for line in section.splitlines():
        m = _ENRICHED_BULLET_RE.match(line.strip())
        if not m:
            continue
        label = m.group("key").strip()
        val = m.group("val").strip()
        key = label_to_key.get(label)
        if not key:
            continue
        if key in list_keys:
            out[key] = [v.strip() for v in val.split(",") if v.strip()]
        else:
            out[key] = val
    return out or None


def _resolve_md_path(project_dir: Path, project_info: dict) -> Optional[Path]:
    try:
        document_code = project_info.get("document_code") or project_info.get("project_id") or project_dir.name
        sources = resolve_version_source_files(project_dir, document_code, project_info=project_info)
        if sources.md_path is not None:
            return sources.md_path
    except Exception:
        pass

    md_name = project_info.get("md_file")
    if md_name:
        cand = project_dir / md_name
        if cand.exists():
            return cand
    for p in project_dir.glob("*_document.md"):
        return p
    return None


def get_enrichment(
    project_dir: Path,
    md_text_cache: dict,
    project_info: dict,
    block_id: str,
) -> tuple[Optional[dict], str]:
    """Возвращает (enrichment, source) — source = 'md' | 'experiments' | 'none'."""
    md_text = md_text_cache.get("text")
    if md_text is None:
        md_path = _resolve_md_path(project_dir, project_info)
        if md_path is None:
            md_text_cache["text"] = ""
            return None, "none"
        md_text = md_path.read_text(encoding="utf-8")
        md_text_cache["text"] = md_text

    enr = parse_enrichment_from_md(md_text, block_id)
    if enr is not None:
        return enr, "md"

    enr = latest_gemma_enrichment(project_dir, block_id)
    if enr is not None:
        return enr, "experiments"
    return None, "none"


def write_single_block_runtime_plan(
    output_dir: Path,
    blocks: list[dict],
    *,
    blocks_dir: Path | None = None,
    source: str = "gemma_findings_only_blocks_index",
) -> dict:
    """Persist the actual single-block Stage 01 execution plan."""
    batches = []
    for idx, block in enumerate(blocks, start=1):
        block_copy = dict(block)
        file_name = block_copy.get("file")
        if blocks_dir is not None and file_name:
            rel_dir = f"_output/{blocks_dir.name}"
            block_copy["image_dir"] = rel_dir
            block_copy["image_path"] = f"{rel_dir}/{file_name}"
            block_copy["image_crop_policy"] = stage02_crop_policy()
        page = block_copy.get("page")
        batches.append({
            "batch_id": idx,
            "blocks": [block_copy],
            "pages_included": [page] if page is not None else [],
            "block_count": 1,
            "total_size_kb": block_copy.get("size_kb", 0),
            "single_block_mode": True,
            "source_block_id": block_copy.get("block_id"),
        })
    plan = {
        "schema_version": RUNTIME_PLAN_SCHEMA_VERSION,
        "mode": "single_block",
        "source": source,
        "total_batches": len(batches),
        "total_blocks": len(batches),
        "batches": batches,
    }
    path = output_dir / "block_batches.runtime.json"
    path.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
    return plan


# ─── Page text from document graph ──────────────────────────────────────────

def load_page_text(graph: dict, page: int) -> str:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            parts = []
            if p.get("sheet_name"):
                parts.append(f"[SHEET] {p['sheet_name']}")
            for tb in p.get("text_blocks", [])[:20]:
                txt = (tb.get("text") or "").strip()
                if txt:
                    parts.append(txt[:500])
            return "\n".join(parts)[:4000]
    return ""


def build_block_user_text(block_id: str, page, enrichment: Optional[dict], page_text: str) -> str:
    """Текст блока, уходящий в LLM на Stage 01 (без изображения).

    ЕДИНЫЙ источник: эту же функцию вызывает реальный анализ блока (call_gpt_for_block)
    и UI-endpoint предпросмотра «что отправляем в нейронку», чтобы они не разъезжались.
    """
    return (
        f"# Блок {block_id} | страница PDF {page}\n\n"
        f"## Уже извлечённое описание блока (контекст, считай верным):\n"
        f"```json\n{json.dumps(enrichment, ensure_ascii=False, indent=2)}\n```\n\n"
        f"## Текст страницы (общие указания, спецификации и т.д.):\n"
        f"{page_text or '(недоступен)'}\n\n"
        f"## Задача:\n"
        f"Посмотри на изображение блока и верни findings[]. Только проблемы. "
        f"Не описывай что видишь. Если всё корректно — пустой массив."
    )


def sheet_for_page(graph: dict, page: int) -> Optional[str]:
    for p in graph.get("pages", []):
        if p.get("page") == page:
            sno = p.get("sheet_no")
            if sno:
                return f"Лист {sno}"
            return p.get("sheet_name")
    return None


# ─── PNG → data URL ──────────────────────────────────────────────────────────

def png_to_data_url(path: Path) -> str:
    return f"data:image/png;base64,{base64.b64encode(path.read_bytes()).decode()}"


def _context_source_from_enrichment(enrichment: dict) -> str:
    canonical = str((enrichment or {}).get("_block_context_source") or "")
    if canonical:
        return canonical
    marker = str((enrichment or {}).get("_gemma_skipped") or "")
    if marker == "vector_layer":
        return "raw_vector"
    if marker == "stage_disabled":
        return "image_only"
    return "legacy_enrichment"


def build_effective_block_context(
    block: dict,
    enrichment: dict,
    page_text: str,
    *,
    output_dir: Optional[Path] = None,
) -> tuple[str, str]:
    """Build the Stage 01 prompt text and its normalized context source."""
    user_text = build_block_user_text(block["block_id"], block["page"], enrichment, page_text)
    context_source = _context_source_from_enrichment(enrichment)

    # The source router is the canonical Stage 01 path. A block without vector text keeps
    # the image-only placeholder text and is still analyzed from its attached PNG.
    _router_applied = False
    if output_dir is not None:
        try:
            from backend.app.pipeline.stages.block_grounding.block_source_router import (
                resolve_block_source as _resolve_block_source,
            )
            _rtext, _rkind = _resolve_block_source(
                output_dir, block.get("block_id", ""), block.get("page"))
            context_source = "image_only" if _rkind == "gemma_fallback" else _rkind
            if _rtext:
                user_text = _rtext
            _router_applied = True
        except Exception:
            context_source = "error"
            _router_applied = True

    # ─── SINGLELINE граф-энричмент: для СХЕМНЫХ блоков — курируемый ГИБРИД ─────
    # Вместо скудного enrichment+page_text подаём render_graph_for_prompt (гибрид: ввод+ТТ
    # без хардкода ВА-305, детерминированная проверка защиты Iкз/сечение, компактные отходящие
    # линии, анти-boilerplate). ЕДИНЫЙ вариант (rich схлопнут в него, решение 07-04). Флаг
    # SINGLELINE_RICH_PROMPT_ENABLED = вкл/выкл инъекции (default OFF → прод не меняется до
    # осознанного включения). Меняет user_text ДО cache_key. fail-soft (ошибка → базовый текст).
    try:
        from backend.app.core import config as _slcfg
        if (not _router_applied
                and getattr(_slcfg, "SINGLELINE_RICH_PROMPT_ENABLED", False)
                and output_dir is not None):
            from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
                resolve_singleline_prompt as _resolve_sl_prompt,
            )
            _graph_prompt = _resolve_sl_prompt(Path(output_dir).parent, block.get("block_id", ""),
                                               block.get("page"), rich=False)
            if _graph_prompt:
                user_text = _graph_prompt
    except Exception:
        pass

    # ─── OCR-ПОДМЕНА («зеркало»): чистый вектор-текст блока как приоритетный источник ЧИСЕЛ ──
    # Аддитивно к user_text (не удаляет enrichment). Бьёт по «нейронка не так прочитала графику»
    # (замер: OCR путает 3х1.5→3x15, вектор-слой — нет). Флаг MIRROR_OCR_ENABLED (default OFF →
    # прод не меняется). Меняет user_text ДО cache_key. fail-soft. Скан без слоя → None → no-op.
    try:
        from backend.app.core import config as _mcfg
        if (not _router_applied
                and getattr(_mcfg, "MIRROR_OCR_ENABLED", False)
                and output_dir is not None):
            from backend.app.pipeline.stages.block_grounding.mirror_block_text import (
                resolve_mirror_block_text as _resolve_mirror,
                inject_mirror_text as _inject_mirror,
            )
            _vtext = _resolve_mirror(Path(output_dir).parent, block.get("block_id", ""))
            if _vtext:
                user_text = _inject_mirror(user_text, _vtext)
    except Exception:
        pass

    return user_text, context_source


def build_effective_block_user_text(
    block: dict,
    enrichment: dict,
    page_text: str,
    *,
    output_dir: Optional[Path] = None,
) -> str:
    """Compatibility wrapper for callers that only need the Stage 01 text."""
    return build_effective_block_context(
        block, enrichment, page_text, output_dir=output_dir
    )[0]


# ─── OpenRouter call ────────────────────────────────────────────────────────

async def call_gpt_for_block(
    client: httpx.AsyncClient,
    block: dict,
    enrichment: dict,
    page_text: str,
    blocks_dir: Path,
    *,
    api_key: str,
    model: str,
    reasoning_effort: str,
    max_tokens: int,
    system_prompt: str,
    timeout: int,
    project_id: str = "",
    version_id: str = "",
    job_id: str = "",
    output_dir: Optional[Path] = None,
) -> dict:
    png_path = blocks_dir / block["file"]
    if not png_path.exists():
        return {"ok": False, "error": f"PNG missing: {png_path.name}", "elapsed_ms": 0}

    user_text, context_source = build_effective_block_context(
        block,
        enrichment,
        page_text,
        output_dir=output_dir,
    )

    # ─── Paid response cache check (до guard и до сети) ────────────
    # Если этот блок с этим model/prompt/image уже отвечал — берём из
    # cache, никаких paid_event и денег. Спасает в инциденте 2026-05-16,
    # где retry платил $0.32 за повтор того же блока.
    from backend.app.pipeline.stages.block_analysis import stage02_paid_cache
    cache_key = ""
    image_bytes_for_cache = b""
    if stage02_paid_cache.cache_enabled() and output_dir is not None:
        try:
            image_bytes_for_cache = png_path.read_bytes()
            cache_key = stage02_paid_cache.compute_cache_key(
                model=model,
                block_id=str(block.get("block_id", "")),
                system_prompt=system_prompt,
                user_text=user_text,
                enrichment=enrichment,
                page_text=page_text,
                image_bytes=image_bytes_for_cache,
            )
            cached = stage02_paid_cache.try_load_cached(output_dir, cache_key)
            if cached is not None:
                cached.setdefault("context_source", context_source)
                return cached
        except OSError:
            # disk error при чтении PNG — пусть дальше упадёт на той же ошибке
            cache_key = ""

    # ─── Paid API guard (defence-in-depth) ──────────────────────────
    # Cache miss → нужен сетевой вызов → нужен guard.
    try:
        from backend.app.services.llm.paid_api_guard import (
            PaidApiBlockedError as _PaidApiBlockedError,
            PaidApiContext as _PaidApiContext,
            assert_paid_api_allowed as _assert_paid_api_allowed,
        )
        _assert_paid_api_allowed(_PaidApiContext(
            source="manager.stage02.call_gpt_for_block",
            model=model,
            project_id=project_id,
            version_id=version_id,
            stage="block_analysis",
            job_id=job_id,
        ))
    except _PaidApiBlockedError as _e:
        return {
            "ok": False,
            "error": f"paid_api_blocked: {_e.reason}",
            "elapsed_ms": 0,
            "paid_api_blocked": True,
        }

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": user_text},
                {"type": "image_url", "image_url": {"url": png_to_data_url(png_path)}},
            ],
        },
    ]

    payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_schema", "json_schema": RESPONSE_SCHEMA},
    }
    if reasoning_effort:
        payload["reasoning"] = {"effort": reasoning_effort}

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost",
        "X-Title": "stage01-findings-only",
    }

    # Последняя защитная стена: без api_key не пускаем в сеть, даже если guard
    # каким-то образом пропустил вызов (тест мокает guard, кто-то обошёл через
    # monkeypatch, dev-конфиг). Без ключа OpenRouter всё равно вернёт 401, но
    # отдельная цена за tokens не списывается — это явный refusal.
    if not (api_key or "").strip():
        return {
            "ok": False,
            "error": "paid_api_blocked: missing_api_key (OpenRouter key empty)",
            "elapsed_ms": 0,
            "paid_api_blocked": True,
        }

    started = time.monotonic()
    try:
        resp = await client.post(OPENROUTER_URL, headers=headers, json=payload, timeout=timeout)
    except Exception as exc:
        return {"ok": False, "error": f"httpx: {exc}", "elapsed_ms": int((time.monotonic() - started) * 1000)}
    elapsed_ms = int((time.monotonic() - started) * 1000)

    if resp.status_code >= 400:
        return {
            "ok": False,
            "http_status": resp.status_code,
            "error": resp.text[:500],
            "elapsed_ms": elapsed_ms,
        }

    data = resp.json()
    choice = (data.get("choices") or [{}])[0]
    msg = choice.get("message") or {}
    raw = msg.get("content") or ""
    finish_reason = choice.get("finish_reason")
    usage = data.get("usage") or {}
    completion_details = usage.get("completion_tokens_details") or {}

    try:
        parsed = json.loads(raw) if raw else None
        parse_err = None
    except Exception as e:
        parsed = None
        parse_err = str(e)

    # Усечённый ответ (finish_reason=length) нельзя считать успехом, даже если
    # обрезанный JSON случайно распарсился — часть findings потеряна. Помечаем
    # truncated и НЕ ставим ok, чтобы блок попал в failed/coverage, а не молча
    # принялся за валидный (reserc.md #25/#14).
    truncated = finish_reason == "length"
    if truncated and parse_err is None and parsed is not None:
        parse_err = "truncated_output (finish_reason=length)"

    response_dict = {
        "ok": parsed is not None and not truncated,
        "parse_error": parse_err,
        "finish_reason": finish_reason,
        "truncated": truncated,
        "elapsed_ms": elapsed_ms,
        "input_tokens": usage.get("prompt_tokens"),
        "output_tokens": usage.get("completion_tokens"),
        "reasoning_tokens": completion_details.get("reasoning_tokens"),
        "raw_content": raw,
        "parsed": parsed,
        "from_cache": False,
        "context_source": context_source,
    }

    # ─── Сохранить в cache СРАЗУ после успешного 2xx ────────────────
    # Даже если parsed is None (parse_error), raw_content есть и за него уже
    # заплачено — при retry хотим получить тот же ответ без новой оплаты.
    if cache_key and output_dir is not None:
        try:
            in_tok = int(usage.get("prompt_tokens") or 0)
            out_tok = int(usage.get("completion_tokens") or 0)
            cost_est = (in_tok * PRICE_IN + out_tok * PRICE_OUT) / 1_000_000
            stage02_paid_cache.save_to_cache(
                output_dir,
                cache_key,
                response=response_dict,
                model=model,
                block_id=str(block.get("block_id", "")),
                original_cost_usd=cost_est,
                source_job_id=job_id,
            )
        except Exception:  # noqa: BLE001
            # cache save — best-effort; ошибка не должна валить stage
            logging.getLogger(__name__).warning(
                "stage02_paid_cache.save_to_cache failed", exc_info=True
            )

    return response_dict


# ─── Codex CLI transport (subscription) ────────────────────────────────────

async def call_codex_for_block(
    block: dict,
    enrichment: dict,
    page_text: str,
    blocks_dir: Path,
    *,
    model: str,
    system_prompt: str,
    timeout: int,
    project_id: str = "",
    output_dir: Optional[Path] = None,
) -> dict:
    """Run the same single-block payload through a Codex subscription session."""
    png_path = blocks_dir / block["file"]
    if not png_path.exists():
        return {"ok": False, "error": f"PNG missing: {png_path.name}", "elapsed_ms": 0}

    from backend.app.services.llm.codex_runner import run_codex_json_messages

    user_text, context_source = build_effective_block_context(
        block,
        enrichment,
        page_text,
        output_dir=output_dir,
    )
    result = await run_codex_json_messages(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        timeout=timeout,
        stage="block_analysis",
        project_id=project_id,
        model=model,
        image_paths=[png_path],
    )
    parsed = result.json_data if isinstance(result.json_data, dict) else None
    findings = parsed.get("findings") if parsed else None
    ok = not result.is_error and isinstance(findings, list)
    return {
        "ok": ok,
        "error": result.error_message or (None if ok else "codex_findings_missing"),
        "parse_error": None if ok else "codex_findings_missing",
        "elapsed_ms": result.duration_ms,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "reasoning_tokens": None,
        "cost_usd": 0.0,
        "cost_source": "subscription",
        "raw_content": result.text,
        "parsed": parsed if ok else None,
        "model": result.model or model,
        "context_source": context_source,
    }


# ─── Claude CLI transport (subscription) ────────────────────────────────────

def _build_claude_cli_task_text(
    *,
    system_prompt: str,
    block_id: str,
    page: int,
    sheet_no: str,
    enrichment: dict,
    page_text: str,
    png_path: Path,
    output_path: Path,
) -> str:
    """Промпт-текст для `claude -p` (Claude CLI сам читает PNG через Read tool и пишет findings через Write tool)."""
    enrichment_section = (
        "## Подготовленный контекст блока:\n"
        f"```json\n{json.dumps(enrichment, ensure_ascii=False, indent=2)}\n```\n"
    )
    page_text_section = f"## Текст страницы:\n{page_text or '(недоступен)'}\n"
    block_header = f"# Блок {block_id} | страница PDF {page} | лист {sheet_no or '(не определён)'}\n\n"
    steps_block = (
        f"1. Прочитай изображение блока через Read tool: `{png_path}`\n"
        "2. Используй приведённый ниже контекст блока и текст страницы.\n"
        "3. Найди проблемы согласно правилам выше.\n"
        f"4. Запиши результат через Write tool в файл: `{output_path}`\n"
    )
    return f"""{system_prompt}

# ЗАДАЧА

Шаги:
{steps_block}   Формат файла: один JSON объект `{{"findings": [...]}}`.
   Никаких других файлов не создавай. Никакого markdown-обёртывания JSON в файле.

{block_header}{enrichment_section}{page_text_section}"""


def _parse_claude_cli_stdout(stdout: str) -> dict:
    """Claude CLI с `--output-format json` возвращает структурированный JSON в stdout."""
    try:
        return json.loads(stdout)
    except Exception:
        m = re.search(r"\{[\s\S]*\}\s*$", stdout)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
        return {}


async def call_claude_cli_for_block(
    block: dict,
    enrichment: dict,
    page_text: str,
    blocks_dir: Path,
    sheet_no: str,
    *,
    model: str,
    system_prompt: str,
    timeout: int,
    clean_cwd: bool = True,
) -> dict:
    """Вызов Claude CLI через subprocess `claude -p --model X --allowedTools Read,Write --output-format json`.

    PNG читается через Read tool, findings пишутся через Write tool в temp-файл,
    из которого мы парсим результат.

    clean_cwd=True (default): subprocess запускается из /tmp/sonnet_clean с минимальным env
    (без project CLAUDE.md, hooks, memory, skills manifest). Даёт −44% input/блок и −52% cost.
    """
    png_path = (blocks_dir / block["file"]).resolve()
    if not png_path.exists():
        return {"ok": False, "error": f"PNG missing: {png_path.name}", "elapsed_ms": 0}

    # Временный output файл — Claude CLI запишет туда findings.json.
    tmp_fd, tmp_name = tempfile.mkstemp(suffix=".findings.json", prefix=f"block_{block['block_id']}_")
    os.close(tmp_fd)
    output_path = Path(tmp_name)
    try:
        output_path.unlink()  # удалим пустой файл — CLI напишет свой
    except FileNotFoundError:
        pass

    task_text = _build_claude_cli_task_text(
        system_prompt=system_prompt,
        block_id=block["block_id"],
        page=block["page"],
        sheet_no=sheet_no,
        enrichment=enrichment,
        page_text=page_text,
        png_path=png_path,
        output_path=output_path,
    )

    cmd = [
        CLAUDE_CLI_BIN, "-p",
        "--model", model,
        "--allowedTools", "Read,Write",
        "--output-format", "json",
    ]

    if clean_cwd:
        proc_cwd = _ensure_clean_cwd()
        proc_env = _build_clean_env()
    else:
        proc_cwd = None
        proc_env = {**os.environ, **{k: "" for k in os.environ if k.startswith("CLAUDE_CODE")}}

    started = time.monotonic()
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=proc_cwd,
            env=proc_env,
        )
    except FileNotFoundError as exc:
        return {"ok": False, "error": f"Claude CLI not found: {exc}", "elapsed_ms": 0}

    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(task_text.encode("utf-8")),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return {"ok": False, "error": f"Claude CLI timeout after {timeout}s",
                "elapsed_ms": int((time.monotonic() - started) * 1000)}

    elapsed_ms = int((time.monotonic() - started) * 1000)
    stdout_text = stdout_b.decode("utf-8", errors="replace")
    stderr_text = stderr_b.decode("utf-8", errors="replace")
    exit_code = proc.returncode or 0

    cli_meta = _parse_claude_cli_stdout(stdout_text)
    usage = cli_meta.get("usage", {}) or {}
    in_tokens = usage.get("input_tokens") or cli_meta.get("input_tokens")
    out_tokens = usage.get("output_tokens") or cli_meta.get("output_tokens")
    total_cost = cli_meta.get("total_cost_usd") or cli_meta.get("cost_usd")

    findings = None
    parse_err = None
    if output_path.exists():
        try:
            data = json.loads(output_path.read_text(encoding="utf-8"))
            findings = data.get("findings") if isinstance(data, dict) else (data if isinstance(data, list) else None)
        except Exception as e:
            parse_err = f"output JSON parse failed: {e}"
        finally:
            try:
                output_path.unlink()
            except FileNotFoundError:
                pass
    elif exit_code != 0:
        parse_err = f"exit code {exit_code}: {stderr_text[-200:]}"

    parsed = {"findings": findings or []} if findings is not None else None
    return {
        "ok": parsed is not None,
        "parse_error": parse_err,
        "elapsed_ms": elapsed_ms,
        "input_tokens": in_tokens,
        "output_tokens": out_tokens,
        "reasoning_tokens": None,
        "cli_reported_cost_usd": total_cost,
        "raw_content": json.dumps(parsed, ensure_ascii=False) if parsed else "",
        "parsed": parsed,
        "exit_code": exit_code,
        "context_source": _context_source_from_enrichment(enrichment),
    }


# ─── Adapter: pilot finding → production format ─────────────────────────────

def adapt_findings_to_production(
    raw_findings: list[dict],
    block_id: str,
    finding_id_counter: list[int],
    *,
    model: str = DEFAULT_MODEL,
    run_id: str = "stage01",
    detection_mode: str = "independent",
    detected_at: str | None = None,
    context_source: str | None = None,
) -> list[dict]:
    """Адаптируем findings из findings-only schema под формат stage 03."""
    out = []
    for f in raw_findings:
        finding_id_counter[0] += 1
        recommendation = (f.get("recommendation") or "").strip()
        finding_text = (f.get("finding") or "").strip()
        if recommendation and recommendation.lower() not in finding_text.lower():
            finding_text = f"{finding_text}\n\nРекомендация: {recommendation}"
        raw_finding_id = f"G-{finding_id_counter[0]:03d}"
        out.append({
            "id": raw_finding_id,
            "severity": f.get("severity") or "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
            "category": f.get("category") or "uncategorized",
            "finding": finding_text,
            "norm": None,
            "norm_quote": f.get("norm_quote"),
            "block_evidence": block_id,
            "value_found": f.get("value_found") or "",
            "highlight_regions": [],
            "provenance": build_finding_provenance(
                model=str(f.get("_detector_model") or model),
                run_id=str(f.get("_detector_run_id") or run_id),
                raw_finding_id=raw_finding_id,
                mode=detection_mode,
                detected_at=detected_at,
                context_source=context_source,
            ),
        })
    return out


# ─── Main runner ────────────────────────────────────────────────────────────

class FindingsOnlyError(Exception):
    """Прерывание прогона (отсутствие prerequisites, отмена и т.п.)."""


def combine_detector_results(
    detector_results: list[tuple[str, dict]],
    *,
    run_id: str,
) -> dict:
    """Combine independent detector payloads without deduplicating findings."""
    combined_findings: list[dict] = []
    ok_models: list[str] = []
    failed_models: list[str] = []
    paid_input_tokens = 0
    paid_output_tokens = 0
    paid_cached_input_tokens = 0
    paid_cached_output_tokens = 0

    for detector_model, result in detector_results:
        if result.get("ok"):
            ok_models.append(detector_model)
            for raw in (result.get("parsed") or {}).get("findings") or []:
                if not isinstance(raw, dict):
                    continue
                tagged = dict(raw)
                tagged["_detector_model"] = detector_model
                tagged["_detector_run_id"] = f"{run_id}:{detector_for_model(detector_model)}"
                combined_findings.append(tagged)
        else:
            failed_models.append(detector_model)

        if detector_for_model(detector_model) == "gpt_openrouter":
            in_tokens = int(result.get("input_tokens") or 0)
            out_tokens = int(result.get("output_tokens") or 0)
            paid_input_tokens += in_tokens
            paid_output_tokens += out_tokens
            if result.get("from_cache"):
                paid_cached_input_tokens += in_tokens
                paid_cached_output_tokens += out_tokens

    ok = bool(ok_models)
    errors = [
        f"{model}: {result.get('error') or result.get('parse_error') or 'failed'}"
        for model, result in detector_results
        if not result.get("ok")
    ]
    context_sources = [
        str(result.get("context_source") or "")
        for _, result in detector_results
        if result.get("context_source")
    ]
    context_source = (
        context_sources[0]
        if context_sources and len(set(context_sources)) == 1
        else ("mixed" if context_sources else None)
    )
    return {
        "ok": ok,
        "partial": ok and bool(failed_models),
        "detectors_complete": len(ok_models) == len(detector_results),
        "detectors_ok": ok_models,
        "detectors_failed": failed_models,
        "detector_results": [
            {"model": model, "result": result}
            for model, result in detector_results
        ],
        "error": "; ".join(errors) if errors else None,
        "parse_error": None if ok else "; ".join(errors),
        "elapsed_ms": max((int(result.get("elapsed_ms") or 0) for _, result in detector_results), default=0),
        "input_tokens": sum(int(result.get("input_tokens") or 0) for _, result in detector_results),
        "output_tokens": sum(int(result.get("output_tokens") or 0) for _, result in detector_results),
        "reasoning_tokens": sum(int(result.get("reasoning_tokens") or 0) for _, result in detector_results),
        "paid_input_tokens": paid_input_tokens,
        "paid_output_tokens": paid_output_tokens,
        "paid_cached_input_tokens": paid_cached_input_tokens,
        "paid_cached_output_tokens": paid_cached_output_tokens,
        "context_source": context_source,
        "parsed": {"findings": combined_findings} if ok else None,
    }


async def run_findings_only_for_project(
    project_dir: Path,
    *,
    output_dir_override: Optional[Path] = None,
    model: str = DEFAULT_MODEL,
    reasoning_effort: str = DEFAULT_EFFORT,
    extended_prompt: bool = True,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    parallelism: int = DEFAULT_PARALLELISM,
    blocks_filter: Optional[list[str]] = None,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    api_key: Optional[str] = None,
    on_progress: Optional[Callable[[dict], None]] = None,
    cancel_event: Optional[asyncio.Event] = None,
    write_target: bool = True,
    write_run_log: bool = True,
    claude_clean_cwd: bool = True,
    # ─── Paid API guard context ─────────────────────────────────────
    # Обязательно для платных моделей (OpenRouter/GPT). Для Claude CLI
    # (модели "claude-..." без слэша) — не требуется.
    project_id: str = "",
    version_id: str = "",
    job_id: str = "",
    detection_mode: str = "independent",
) -> dict:
    """Прогнать Stage 01 findings-only для проекта.

    Возвращает dict:
      {"output_doc": <01_blocks_analysis.json content>,
       "summary": <metrics dict>,
       "plan": <per-block plan list>,
       "run_dir": Path | None}

    on_progress(event) callback — webapp может подписаться:
      {"type": "started",  "blocks_total": N, "model": ..., "section": ...}
      {"type": "block_done", "block_id": ..., "page": ..., "ok": True, "findings": N,
       "input_tokens": ..., "output_tokens": ..., "reasoning_tokens": ...,
       "elapsed_ms": ..., "completed": K, "total": N}
      {"type": "block_skip", "block_id": ..., "reason": "no_enrichment", ...}
      {"type": "completed", "summary": {...}}

    cancel_event — webapp может set() для прерывания между блоками.
    """
    output_dir = Path(output_dir_override) if output_dir_override is not None else gemma_output_root(project_dir)
    run_started_at = datetime.now(timezone.utc).isoformat()
    run_id = (
        "stage01-"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + re.sub(r"[^a-zA-Z0-9_-]+", "_", model).strip("_")
    )
    blocks_dir = output_dir / STAGE02_BLOCKS_DIRNAME
    index_path = blocks_dir / "index.json"
    context_summary_path = output_dir / BLOCK_CONTEXT_SUMMARY_FILENAME
    graph_path = output_dir / "document_graph.json"
    target_path = output_dir / BLOCKS_ANALYSIS_FILENAME

    if not index_path.exists():
        raise FindingsOnlyError(f"no _output/{STAGE02_BLOCKS_DIRNAME}/index.json — сначала: blocks.py crop --output-dir {STAGE02_BLOCKS_DIRNAME}")
    if not crop_index_matches_policy(index_path, stage02_crop_policy()):
        raise FindingsOnlyError(f"_output/{STAGE02_BLOCKS_DIRNAME}/index.json не соответствует Stage 01 crop policy {stage02_crop_policy()}")
    if not graph_path.exists():
        raise FindingsOnlyError("no _output/document_graph.json — сначала: process_project.py")
    context_validation = validate_block_context_summary(output_dir)
    if not context_validation.get("valid"):
        raise FindingsOnlyError(
            f"block context summary invalid: {context_validation.get('reason')}"
        )

    project_info = load_version_project_info(project_dir)
    section = (project_info.get("section") or "_generic").strip() or "_generic"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    context_summary = load_block_context_summary(output_dir)
    context_blocks = {
        str(item.get("block_id")): item
        for item in context_summary.get("blocks") or []
        if isinstance(item, dict) and item.get("block_id")
    }

    by_id = {b["block_id"]: b for b in index.get("blocks", [])}
    crop_index_warnings = {
        "context_blocks_without_stage01_crop": [
            {
                "block_id": bid,
                "page": context_blocks.get(bid, {}).get("page"),
                "reason": "missing_stage01_crop",
            }
            for bid in sorted(set(context_blocks) - set(by_id))
        ],
    }
    if blocks_filter:
        unknown = [b for b in blocks_filter if b not in by_id]
        if unknown:
            raise FindingsOnlyError(f"unknown block_ids: {unknown}")
        wanted = list(blocks_filter)
    else:
        wanted = [b["block_id"] for b in index.get("blocks", [])]

    runtime_blocks = [by_id[bid] for bid in wanted]
    runtime_plan = write_single_block_runtime_plan(output_dir, runtime_blocks, blocks_dir=blocks_dir)
    runtime_batches = runtime_plan.get("batches", [])
    wanted = [
        batch["blocks"][0]["block_id"]
        for batch in runtime_batches
        if batch.get("blocks")
    ]

    use_claude_cli = is_claude_cli_model(model)
    use_codex_cli = is_codex_model(model)
    use_dual = model == STAGE02_DUAL_MODEL_ID
    detector_models = (
        [DEFAULT_MODEL, CODEX_STAGE_MODEL_ID]
        if use_dual
        else [model]
    )
    if not use_claude_cli and not use_codex_cli:
        if api_key is None:
            api_key = os.environ.get("OPENROUTER_API_KEY")
        if not api_key:
            raise FindingsOnlyError("OPENROUTER_API_KEY not set")

    system_prompt = build_system_prompt(section, extended=extended_prompt)
    cats_loaded = bool(load_categories_for_section(section)) and extended_prompt

    enr_sources: dict[str, int] = {}
    plan: list[dict] = []
    for bid in wanted:
        block = by_id[bid]
        context = context_blocks.get(bid) or {}
        source = str(context.get("source_kind") or "missing")
        coverage_status = str(context.get("coverage_status") or "error")
        warnings = list(context.get("warnings") or [])
        missing_reason = "missing_block_context" if coverage_status == "error" else None
        enrichment = {
            "block_type": "image",
            "subject": "Контекст блока подготовлен из PDF/Vectograph или изображения",
            "notes": "Stage 01 использует точный векторный контекст либо приложенный PNG.",
            "_block_context_source": source,
        }
        enr_sources[source] = enr_sources.get(source, 0) + 1
        plan.append({
            "block_id": bid,
            "page": block["page"],
            "enrichment": enrichment,
            "src": source,
            "coverage_status": coverage_status,
            "warnings": warnings,
            "missing_reason": missing_reason,
        })

    skip_no_enrich: list[dict] = []
    uncovered_blocks = [
        {"block_id": p["block_id"], "page": p["page"], "reason": p.get("missing_reason")}
        for p in plan if p.get("missing_reason")
    ]
    stage02_crop_missing_blocks = crop_index_warnings["context_blocks_without_stage01_crop"]
    coverage_uncovered_blocks_by_id: dict[str, dict[str, Any]] = {}
    for item in uncovered_blocks:
        if not isinstance(item, dict) or not item.get("block_id"):
            continue
        block_id = str(item["block_id"])
        normalized = dict(item)
        if normalized.get("page") is None and block_id in by_id:
            normalized["page"] = by_id[block_id].get("page")
        coverage_uncovered_blocks_by_id[block_id] = normalized
    for item in uncovered_blocks:
        coverage_uncovered_blocks_by_id[str(item["block_id"])] = dict(item)
    coverage_uncovered_blocks = sorted(
        coverage_uncovered_blocks_by_id.values(),
        key=lambda item: (int(item.get("page") or 0), str(item.get("block_id") or "")),
    )

    if on_progress:
        on_progress({
            "type": "started",
            "blocks_total": len(wanted),
            "model": model,
            "reasoning_effort": reasoning_effort,
            "extended_prompt": cats_loaded,
            "section": section,
            "enrichment_sources": dict(enr_sources),
            "skipped_no_enrichment": len(skip_no_enrich),
            "uncovered_blocks": coverage_uncovered_blocks,
            "stage02_crop_missing_blocks": stage02_crop_missing_blocks,
            "crop_index_warnings": crop_index_warnings,
            "context_coverage": context_summary,
            "runtime_plan_path": str(output_dir / "block_batches.runtime.json"),
        })

    run_dir: Optional[Path] = None
    if write_run_log:
        model_tag = model.replace("/", "_").replace(":", "_")
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        run_dir = output_dir / "_stage01_findings_only_runs" / f"{ts}__{model_tag}_{reasoning_effort or 'none'}"
        run_dir.mkdir(parents=True, exist_ok=True)

    sem = asyncio.Semaphore(parallelism)
    completed_count = 0
    completed_lock = asyncio.Lock()
    results: list[dict] = []

    async def _one(item: dict, client: Optional[httpx.AsyncClient]) -> Optional[dict]:
        nonlocal completed_count
        if item["enrichment"] is None:
            async with completed_lock:
                completed_count += 1
                cur = completed_count
            if on_progress:
                on_progress({
                    "type": "block_skip", "block_id": item["block_id"],
                    "page": item["page"], "reason": item.get("missing_reason") or "no_enrichment",
                    "completed": cur, "total": len(wanted),
                })
            return None
        if cancel_event is not None and cancel_event.is_set():
            return None
        async with sem:
            if cancel_event is not None and cancel_event.is_set():
                return None
            block = by_id[item["block_id"]]
            page_text = load_page_text(graph, block["page"])
            if use_dual:
                assert client is not None
                gpt_result, codex_result = await asyncio.gather(
                    call_gpt_for_block(
                        client, block, item["enrichment"], page_text, blocks_dir,
                        api_key=api_key or "", model=DEFAULT_MODEL,
                        reasoning_effort=reasoning_effort,
                        max_tokens=max_tokens, system_prompt=system_prompt,
                        timeout=timeout_s, project_id=project_id,
                        version_id=version_id, job_id=job_id,
                        output_dir=output_dir,
                    ),
                    call_codex_for_block(
                        block, item["enrichment"], page_text, blocks_dir,
                        model=CODEX_STAGE_MODEL_ID,
                        system_prompt=system_prompt, timeout=timeout_s,
                        project_id=project_id, output_dir=output_dir,
                    ),
                )
                res = combine_detector_results(
                    [(DEFAULT_MODEL, gpt_result), (CODEX_STAGE_MODEL_ID, codex_result)],
                    run_id=run_id,
                )
            elif use_codex_cli:
                res = await call_codex_for_block(
                    block, item["enrichment"], page_text, blocks_dir,
                    model=model, system_prompt=system_prompt, timeout=timeout_s,
                    project_id=project_id, output_dir=output_dir,
                )
            elif use_claude_cli:
                sheet = sheet_for_page(graph, block["page"]) or ""
                res = await call_claude_cli_for_block(
                    block, item["enrichment"], page_text, blocks_dir, sheet,
                    model=model, system_prompt=system_prompt, timeout=timeout_s,
                    clean_cwd=claude_clean_cwd,
                )
            else:
                res = await call_gpt_for_block(
                    client, block, item["enrichment"], page_text, blocks_dir,
                    api_key=api_key, model=model,
                    reasoning_effort=reasoning_effort,
                    max_tokens=max_tokens, system_prompt=system_prompt,
                    timeout=timeout_s,
                    project_id=project_id,
                    version_id=version_id,
                    job_id=job_id,
                    output_dir=output_dir,
                )
            n = len((res.get("parsed") or {}).get("findings", [])) if res.get("ok") else 0
            record = {
                "block_id": item["block_id"],
                "page": block["page"],
                "size_kb": block.get("size_kb"),
                "enrichment_source": item["src"],
                "context_source": res.get("context_source") or _context_source_from_enrichment(item["enrichment"]),
                "result": res,
            }
            if run_dir is not None:
                (run_dir / f"block_{item['block_id']}.json").write_text(
                    json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
                )
            async with completed_lock:
                completed_count += 1
                cur = completed_count
            if on_progress:
                on_progress({
                    "type": "block_done",
                    "block_id": item["block_id"],
                    "page": block["page"],
                    "ok": res.get("ok"),
                    "findings": n,
                    "input_tokens": res.get("input_tokens"),
                    "output_tokens": res.get("output_tokens"),
                    "reasoning_tokens": res.get("reasoning_tokens"),
                    "elapsed_ms": res.get("elapsed_ms"),
                    "partial": bool(res.get("partial")),
                    "detectors_ok": res.get("detectors_ok") or [],
                    "detectors_failed": res.get("detectors_failed") or [],
                    "completed": cur,
                    "total": len(wanted),
                    "error": res.get("error") or res.get("parse_error") if not res.get("ok") else None,
                })
            return record

    started_at = time.monotonic()
    if use_claude_cli or use_codex_cli:
        # Subscription CLI transports work through subprocess; no HTTP client.
        gathered = await asyncio.gather(
            *(_one(p, None) for p in plan),
            return_exceptions=True,
        )
    else:
        async with httpx.AsyncClient(timeout=timeout_s + 20) as client:
            gathered = await asyncio.gather(
                *(_one(p, client) for p in plan),
                return_exceptions=True,
            )
    wall_clock_s = round(time.monotonic() - started_at, 1)
    task_exceptions: list[dict[str, Any]] = []
    results: list[dict] = []
    for item, result in zip(plan, gathered):
        if isinstance(result, Exception):
            err = f"{type(result).__name__}: {result}"
            completed_count += 1
            cur = completed_count
            task_exceptions.append({
                "block_id": item["block_id"],
                "page": item["page"],
                "error": err,
                "exception_type": type(result).__name__,
            })
            results.append({
                "block_id": item["block_id"],
                "page": item["page"],
                "size_kb": by_id[item["block_id"]].get("size_kb"),
                "enrichment_source": item["src"],
                "result": {
                    "ok": False,
                    "error": f"Unhandled single-block exception: {err}",
                    "exception_type": type(result).__name__,
                },
            })
            if on_progress:
                on_progress({
                    "type": "block_done",
                    "block_id": item["block_id"],
                    "page": item["page"],
                    "ok": False,
                    "findings": 0,
                    "completed": cur,
                    "total": len(wanted),
                    "error": f"Unhandled single-block exception: {err}",
                })
        elif result is not None:
            results.append(result)

    cancelled = cancel_event is not None and cancel_event.is_set()

    # Build production-format 01_blocks_analysis.json
    finding_id_counter = [0]
    block_analyses = []
    for p in plan:
        bid = p["block_id"]
        block = by_id[bid]
        sheet = sheet_for_page(graph, block["page"])
        rec = next((r for r in results if r["block_id"] == bid), None)

        if rec is None:
            missing_gemma = p["enrichment"] is None
            status = (p.get("coverage_status") or "missing_gemma_enrichment") if missing_gemma else "cancelled"
            missing_reason = p.get("missing_reason") or "no_enrichment"
            details = (
                "Блок не анализировался полноценно: отсутствует подготовленный контекст "
                "(запустите подготовку контекста повторно)."
                if missing_gemma else "Прерывание/отмена"
            )
            if missing_reason == "missing_gemma_index":
                details = (
                    "Блок есть в Stage 01 100 DPI index, но отсутствует в compatibility index; "
                    "он не анализировался как полноценно обогащённый."
                )
            block_analyses.append({
                "block_id": bid, "page": block["page"], "sheet": sheet,
                "label": block.get("ocr_label", ""), "sheet_type": None,
                "unreadable_text": True,
                "unreadable_details": details,
                "not_enriched": missing_gemma,
                "coverage_status": status,
                "analysis_status": "not_analyzed",
                "context_source": _context_source_from_enrichment(p.get("enrichment") or {}),
                "summary": "", "key_values_read": [], "evidence_text_refs": [],
                "findings": [],
                "_skip_reason": missing_reason if missing_gemma else "cancelled",
            })
            continue

        res = rec["result"]
        if not res.get("ok"):
            err_text = res.get("error") or res.get("parse_error") or "unknown error"
            block_analyses.append({
                "block_id": bid, "page": block["page"], "sheet": sheet,
                "label": block.get("ocr_label", ""), "sheet_type": None,
                "unreadable_text": True,
                "unreadable_details": f"Single-block analysis failed: {err_text}",
                "not_enriched": False,
                "coverage_status": "single_block_analysis_failed",
                "analysis_status": "failed",
                "context_source": res.get("context_source") or _context_source_from_enrichment(p.get("enrichment") or {}),
                "summary": "", "key_values_read": [], "evidence_text_refs": [],
                "findings": [],
                "_error": err_text,
            })
            continue

        raw_findings = (res.get("parsed") or {}).get("findings", [])
        coverage_status = p.get("coverage_status") or "ok"
        if res.get("partial"):
            coverage_status = "partial_detector_failure"
        block_analyses.append({
            "block_id": bid, "page": block["page"], "sheet": sheet,
            "label": block.get("ocr_label", ""), "sheet_type": None,
            "unreadable_text": False, "unreadable_details": None,
            "not_enriched": False,
            "coverage_status": coverage_status,
            "analysis_status": "partial" if res.get("partial") else "analyzed",
            "context_source": res.get("context_source") or _context_source_from_enrichment(p.get("enrichment") or {}),
            "detectors_ok": res.get("detectors_ok") or detector_models,
            "detectors_failed": res.get("detectors_failed") or [],
            "summary": "", "key_values_read": [], "evidence_text_refs": [],
            "findings": adapt_findings_to_production(
                raw_findings,
                bid,
                finding_id_counter,
                model=model,
                run_id=run_id,
                detection_mode=detection_mode,
                detected_at=run_started_at,
                context_source=res.get("context_source") or _context_source_from_enrichment(p.get("enrichment") or {}),
            ),
        })

    context_source_counts: dict[str, int] = {}
    for analysis in block_analyses:
        source = str(analysis.get("context_source") or "unknown")
        context_source_counts[source] = context_source_counts.get(source, 0) + 1

    output_doc = {
        "batch_id": 0,
        "project_id": project_info.get("project_id", project_dir.name),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage01_mode": "findings_only_block_context",
        BLOCKS_META_KEY: {
            "model": model,
            "run_id": run_id,
            "detection_mode": detection_mode,
            "prompt_version": STAGE01_PROMPT_VERSION,
            "detectors": [
                {
                    "detector": detector_for_model(detector_model),
                    "model": detector_model,
                    "mode": detection_mode,
                    "run_id": f"{run_id}:{detector_for_model(detector_model)}",
                    "prompt_version": STAGE01_PROMPT_VERSION,
                }
                for detector_model in detector_models
            ],
            "reasoning_effort": reasoning_effort,
            "extended_prompt": cats_loaded,
            "section": section,
            "context_source_counts": context_source_counts,
            "context_coverage": {
                "blocks_total": context_summary.get("blocks_total", 0),
                "blocks_ready": context_summary.get("blocks_ready", 0),
                "blocks_failed": context_summary.get("blocks_failed", 0),
                "source_counts": context_summary.get("source_counts", {}),
            },
            "blocks_total": len(wanted),
            "blocks_ok": sum(1 for r in results if r["result"].get("ok")),
            "blocks_failed": sum(1 for r in results if not r["result"].get("ok")),
            "blocks_partial": sum(1 for r in results if r["result"].get("partial")),
            "blocks_skipped_no_context": len(skip_no_enrich),
            "uncovered_blocks": coverage_uncovered_blocks,
            "stage02_crop_missing_blocks": stage02_crop_missing_blocks,
            "crop_index_warnings": crop_index_warnings,
            "failed_blocks": [
                {
                    "block_id": r["block_id"],
                    "page": r.get("page"),
                    "reason": "single_block_analysis_failed",
                    "error": r["result"].get("error") or r["result"].get("parse_error"),
                }
                for r in results
                if not r["result"].get("ok")
            ],
            "partial_detector_blocks": [
                {
                    "block_id": r["block_id"],
                    "page": r.get("page"),
                    "detectors_ok": r["result"].get("detectors_ok") or [],
                    "detectors_failed": r["result"].get("detectors_failed") or [],
                    "error": r["result"].get("error"),
                }
                for r in results
                if r["result"].get("partial")
            ],
            "task_exceptions": task_exceptions,
            "runtime_plan_path": str(output_dir / "block_batches.runtime.json"),
            "blocks_crop_dir": f"_output/{STAGE02_BLOCKS_DIRNAME}",
            "wall_clock_s": wall_clock_s,
            "cancelled": cancelled,
        },
        "block_analyses": block_analyses,
    }

    if write_target:
        if target_path.exists():
            bak = target_path.with_suffix(".classic.bak.json")
            if not bak.exists():
                bak.write_text(target_path.read_text(encoding="utf-8"), encoding="utf-8")
        # Атомарно (tmp + os.replace): финальная запись итога многочасовой
        # стадии — kill посреди прямого write_text корёжил 02_blocks_analysis.
        _tmp = target_path.with_suffix(target_path.suffix + ".tmp")
        _tmp.write_text(json.dumps(output_doc, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(_tmp, target_path)

    # Run summary
    ok = [r for r in results if r["result"].get("ok")]
    fail = [r for r in results if not r["result"].get("ok")]
    total_in = sum((r["result"].get("input_tokens") or 0) for r in results)
    total_out = sum((r["result"].get("output_tokens") or 0) for r in results)
    total_reason = sum((r["result"].get("reasoning_tokens") or 0) for r in results)
    total_findings = sum(len(b["findings"]) for b in block_analyses)
    # Paid-token accounting excludes Codex/Claude subscription tokens in dual
    # mode. Cache hits remain visible in total tokens but are not billed.
    paid_in = sum(
        int(r["result"].get("paid_input_tokens", r["result"].get("input_tokens") or 0))
        for r in results
    ) if not (use_claude_cli or use_codex_cli) else 0
    paid_out = sum(
        int(r["result"].get("paid_output_tokens", r["result"].get("output_tokens") or 0))
        for r in results
    ) if not (use_claude_cli or use_codex_cli) else 0
    cached_in = sum(
        int(r["result"].get("paid_cached_input_tokens", r["result"].get("input_tokens") or 0))
        for r in results if r["result"].get("from_cache") or r["result"].get("paid_cached_input_tokens")
    )
    cached_out = sum(
        int(r["result"].get("paid_cached_output_tokens", r["result"].get("output_tokens") or 0))
        for r in results if r["result"].get("from_cache") or r["result"].get("paid_cached_output_tokens")
    )
    billable_in = max(0, paid_in - cached_in)
    billable_out = max(0, paid_out - cached_out)
    if use_claude_cli:
        # Claude CLI subscription: суммируем cost_usd, отчитанный самим CLI.
        # CLI не кешируется здесь, так что cached_* = 0.
        cost_total = sum((r["result"].get("cli_reported_cost_usd") or 0.0) for r in results)
        cost_in = 0.0
        cost_out = 0.0
    elif use_codex_cli:
        cost_in = 0.0
        cost_out = 0.0
        cost_total = 0.0
    else:
        cost_in = billable_in * PRICE_IN / 1_000_000
        cost_out = billable_out * PRICE_OUT / 1_000_000
        cost_total = cost_in + cost_out

    summary = {
        "project_dir": str(project_dir),
        "model": model,
        "reasoning_effort": reasoning_effort,
        "extended_prompt": cats_loaded,
        "blocks_total": len(wanted),
        "blocks_with_context": sum(1 for p in plan if p["enrichment"] is not None),
        "blocks_ok": len(ok),
        "blocks_failed": len(fail),
        "blocks_partial": sum(1 for r in results if r["result"].get("partial")),
        "blocks_skipped_no_context": len(skip_no_enrich),
        "context_coverage": {
            "blocks_total": context_summary.get("blocks_total", 0),
            "blocks_ready": context_summary.get("blocks_ready", 0),
            "blocks_failed": context_summary.get("blocks_failed", 0),
            "source_counts": context_summary.get("source_counts", {}),
        },
        "uncovered_blocks": coverage_uncovered_blocks,
        "stage02_crop_missing_blocks": stage02_crop_missing_blocks,
        "crop_index_warnings": crop_index_warnings,
        "failed_blocks": [
            {
                "block_id": r["block_id"],
                "page": r.get("page"),
                "reason": "single_block_analysis_failed",
                "error": r["result"].get("error") or r["result"].get("parse_error"),
            }
            for r in fail
        ],
        "task_exceptions": task_exceptions,
        "runtime_plan_path": str(output_dir / "block_batches.runtime.json"),
        "blocks_crop_dir": f"_output/{STAGE02_BLOCKS_DIRNAME}",
        "wall_clock_s": wall_clock_s,
        "cancelled": cancelled,
        "totals": {
            "input_tokens": total_in,
            "output_tokens": total_out,
            "reasoning_tokens": total_reason,
            "findings": total_findings,
            "estimated_cost_usd_in": round(cost_in, 4),
            "estimated_cost_usd_out": round(cost_out, 4),
            "estimated_cost_usd_total": round(cost_total, 4),
            "estimated_cost_per_block_usd": round(cost_total / max(1, len(ok)), 4),
            "cache_hits": sum(1 for r in results if r["result"].get("from_cache")),
            "cached_input_tokens": cached_in,
            "cached_output_tokens": cached_out,
            "billable_input_tokens": billable_in,
            "billable_output_tokens": billable_out,
        },
        "context_sources": enr_sources,
    }

    if run_dir is not None:
        (run_dir / "summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    if on_progress:
        on_progress({"type": "completed", "summary": summary})

    return {
        "output_doc": output_doc,
        "summary": summary,
        "plan": plan,
        "run_dir": run_dir,
    }


def check_prerequisites(project_dir: Path, *, output_dir_override: Optional[Path] = None) -> dict:
    """Проверить готовность проекта к findings_only_block_context.

    Возвращает dict {"ok": bool, "reasons": [...], "blocks_total": N, "with_context": M}.
    """
    output_dir = Path(output_dir_override) if output_dir_override is not None else gemma_output_root(project_dir)
    index_path = output_dir / STAGE02_BLOCKS_DIRNAME / "index.json"
    graph_path = output_dir / "document_graph.json"

    reasons: list[str] = []
    if not index_path.exists():
        reasons.append(f"Нет _output/{STAGE02_BLOCKS_DIRNAME}/index.json (запустите Stage 01 crop)")
    elif not crop_index_matches_policy(index_path, stage02_crop_policy()):
        reasons.append(f"_output/{STAGE02_BLOCKS_DIRNAME}/index.json не соответствует Stage 01 crop policy")
    context_validation = validate_block_context_summary(output_dir)
    if not context_validation.get("valid"):
        reasons.append(f"Контекст блоков не готов: {context_validation.get('reason')}")
    if not graph_path.exists():
        reasons.append("Нет _output/document_graph.json")

    if reasons:
        return {
            "ok": False,
            "reasons": reasons,
            "blocks_total": 0,
            "with_context": 0,
            "uncovered_blocks": [],
        }

    index = json.loads(index_path.read_text(encoding="utf-8"))
    context_summary = load_block_context_summary(output_dir)
    context_by_id = {
        str(item.get("block_id")): item
        for item in context_summary.get("blocks") or []
        if isinstance(item, dict) and item.get("block_id")
    }
    blocks = index.get("blocks", [])
    with_context = 0
    uncovered_blocks: list[dict[str, Any]] = []
    for b in blocks:
        summary_block = context_by_id.get(str(b.get("block_id"))) or {}
        if summary_block.get("coverage_status") != "error":
            with_context += 1
        else:
            uncovered_blocks.append({
                "block_id": b.get("block_id"),
                "page": b.get("page"),
                "reason": "missing_block_context",
            })

    if blocks and with_context == 0:
        reasons.append("Ни у одного блока нет подготовленного контекста")
    elif with_context < len(blocks):
        reasons.append(f"Контекст готов для {with_context}/{len(blocks)} блоков")

    return {
        "ok": not blocks or with_context > 0,
        "reasons": reasons,
        "blocks_total": len(blocks),
        "with_context": with_context,
        "uncovered_blocks": uncovered_blocks,
    }
