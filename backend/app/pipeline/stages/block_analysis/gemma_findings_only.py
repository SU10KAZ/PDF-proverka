"""
gemma_findings_only.py
---------------------
Production-модуль stage 02 в режиме findings-only + Gemma-enrichment.

Поддерживает transport'ы:
  - OpenRouter (GPT-5.4, Gemini Flash/Pro)  — HTTP + json_schema
  - Claude CLI (Sonnet/Opus через subscription) — subprocess `claude -p`
  - Codex CLI (subscription) — `codex exec --image`, JSON-only

Режим `ensemble/gpt-codex` запускает GPT и Codex независимо на одинаковом
single-block payload, затем Codex-review классифицирует смысловые отношения
(match/extension/new/disputed) и опционально ищет проблемы, пропущенные обоими.
Исходные наборы и авторство каждой детекции сохраняются до Stage 03.

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
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    BLOCKS_META_KEY,
    BLOCK_CONTEXT_SUMMARY_FILENAME,
)
from backend.app.pipeline.stages.block_context.contract import (
    crops_materialized,
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
    STAGE01_ABORT_ON_LEG_FAILURE_ENABLED,
    STAGE01_DUAL_GAP_SEARCH_ENABLED,
    STAGE01_DUAL_REVIEW_ENABLED,
    STAGE01_DUAL_REVIEW_MODEL,
    STAGE01_LEG_FAILURE_THRESHOLD,
    STAGE01_PROTECTION_TABLE_CHECK_ENABLED,
    STAGE02_DUAL_MODEL_ID,
    is_codex_model,
)
from backend.app.services.storage.projects_v2_source_resolver import (
    load_version_project_info,
    resolve_version_source_files,
)
from backend.app.services.common import cpu_pool
from backend.app.pipeline.stages.block_analysis.provenance import (
    STAGE01_PROMPT_VERSION,
    build_finding_provenance,
    detector_for_model,
)
from backend.app.pipeline.stages.block_analysis.protection_table_check import (
    DETECTOR_MODEL as PROTECTION_DETECTOR_MODEL,
    run_protection_table_detector,
)

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "openai/gpt-5.4"
DEFAULT_EFFORT = "low"
DEFAULT_MAX_TOKENS = 16000
DEFAULT_PARALLELISM = 3
DEFAULT_TIMEOUT_S = 200
# Жёсткий потолок на ОДИН блок (backstop поверх per-transport timeout). Нужен,
# т.к. httpx read-timeout меряет паузу между чтениями, а не общее время: при
# «капающем» keepalive от провайдера (OpenRouter во время долгого reasoning)
# read-timeout может не сработать и блок повиснет на часы, заморозив весь батч
# (cleanup_zombies не снимает «формально живой» asyncio-таск). Потолок берётся с
# запасом над timeout_s, чтобы легитимные (даже долгие) ответы успевали, а реально
# залипший блок падал с ошибкой и стадия шла дальше. Настройка — через env.
BLOCK_HARD_TIMEOUT_BUFFER_S = int(
    os.environ.get("STAGE02_BLOCK_HARD_TIMEOUT_BUFFER_S", "300") or "300"
)
RUNTIME_PLAN_SCHEMA_VERSION = 1

PRICE_IN = 2.50
PRICE_OUT = 15.0

# Claude CLI binary (subscription transport). Можно переопределить через env.
CLAUDE_CLI_BIN = os.environ.get("CLAUDE_CLI_BIN", str(Path.home() / ".local" / "bin" / "claude"))

# clean_cwd: запуск `claude -p` из чистой папки + урезанным env, чтобы не подгружать
# CLAUDE.md проекта, .claude/settings.json, hooks, project memory, skills manifest.
# Эмпирически даёт −44% input/блок и −52% cli_cost для Stage 01.
def _clean_cwd_root() -> str:
    """Корень «чистых» каталогов запуска. См. config.clean_cli_cwd_root."""
    from backend.app.core.config import clean_cli_cwd_root
    return clean_cli_cwd_root()
_CLEAN_ENV_KEEP = {"HOME", "PATH", "LANG", "LC_ALL", "USER", "SHELL"}


def _resolve_block_package_in_worker(output_dir: str, block_id: str, page: Any) -> dict[str, Any]:
    """Точка входа дочернего процесса пула (уровень модуля — требование pickle).

    prefer_prepared=False сохранён намеренно: при смене профильных маппингов
    (например AI → architecture) готовый пакет со старой маршрутизацией должен
    быть пересчитан, иначе он молча переживёт перезапуск.
    """
    from pathlib import Path as _Path

    from backend.app.pipeline.stages.block_grounding.block_source_router import (
        resolve_block_package as _resolve,
    )

    return _resolve(_Path(output_dir), block_id, page, prefer_prepared=False)


def _ensure_clean_cwd() -> str:
    """Чистый рабочий каталог на ОДИН запуск `claude -p`.

    Раньше здесь был дубль реализации из claude_runner: общий каталог, который
    каждый вызов вычищал целиком. При параллельных проектах старт одного
    вызова стирал рабочие файлы уже бегущих. Делегируем единственной
    реализации, чтобы дефект не разъезжался по копиям.
    """
    from backend.app.services.llm.claude_runner import _ensure_clean_cwd as _impl

    return _impl()


def _release_clean_cwd(path: str | None) -> None:
    """Удалить каталог запуска (см. _ensure_clean_cwd)."""
    from backend.app.services.llm.claude_runner import _release_clean_cwd as _impl

    _impl(path)


def _build_clean_env() -> dict:
    """Минимальный env (HOME/PATH/LANG/LC_ALL/USER/SHELL/XDG_*) — исключает project memory,
    skills manifest и прочие context-dependent артефакты Claude CLI."""
    keep = {}
    for k, v in os.environ.items():
        if k in _CLEAN_ENV_KEEP or k.startswith("XDG_"):
            keep[k] = v
    return keep


def provider_bridge_active() -> bool:
    """Активен ли мост провайдеров воркера в ЭТОМ процессе (этап 11F).

    Отсутствие пакета `audit_worker` — законный случай: на центре его может не
    быть вовсе, и там поведение обязано остаться прежним. А вот заданная
    переменная привязки при отсутствующем файле — ошибка развёртывания, и она
    поднимается наверх исключением: тихий возврат к прежнему транспорту
    означал бы вызов неавторизованного CLI из-под изолированного HOME.
    """
    try:
        from audit_worker.providers import pipeline_bridge
    except ModuleNotFoundError:
        return False
    return bool(pipeline_bridge.active())


def is_claude_cli_model(model: str) -> bool:
    """Sonnet/Opus через Claude CLI subscription (`claude-sonnet-5`, `claude-opus-5`, …)."""
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

## Evidence-first правила

Большинство блоков корректны. Пустой findings[] — нормальный и ожидаемый результат.
Сначала определи тип блока и проверяй ТОЛЬКО применимые к нему категории. Не обязан
находить замечание в каждой категории и не обязан исчерпывать чек-лист.

Finding допустим только когда:
  1. указан конкретный affected_entity (марка, помещение, узел, размер или решение);
  2. есть наблюдаемое evidence_quote из изображения/векторного текста/контекста;
  3. контекст достаточен, контрдоказательства на листе и в результатах поиска проверены;
  4. confidence не ниже 0.80;
  5. это прямое нарушение, явное противоречие двух значений или явное отсутствие
     обязательного элемента — не совет по улучшению и не гипотеза.

Если данных не хватает, НЕ превращай это в замечание об отсутствии. Верни кандидата
с context_status="needs_retrieval" или "external_only", claim_type="context_gap" и
перечисли required_context. Такой кандидат сохранится для аудита, но не будет опубликован.
Не используй условные формулировки «если», «возможно», «может», «требуется проверить»
для готового замечания. Не выдумывай норму: norm_quote=null лучше ложной ссылки.

Заголовки «Эталонная текстовая разметка», название профиля/типа фрагмента,
``document_type``, квитанция поиска и прочие служебные пояснения подготовлены
системой анализа, а НЕ автором проекта. Они нужны только для навигации. Никогда не
создавай замечание о противоречии чертежа этим служебным метаданным и не предлагай
исправлять «текстовую разметку», «индексацию», «контекст» или тип профиля.

Каждое finding:
  - severity: одно из "КРИТИЧЕСКОЕ" | "ЭКОНОМИЧЕСКОЕ" | "ЭКСПЛУАТАЦИОННОЕ" | "РЕКОМЕНДАТЕЛЬНОЕ" | "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
  - category: короткий тег (snake_case)
  - finding: суть замечания (конкретно, с цифрами и марками, 1-3 предложения)
  - norm_quote: цитата или ссылка на пункт нормы РФ если применимо, иначе null
  - value_found: точная цитата с чертежа (значение, марка, размер) — или пустая строка
  - recommendation: что делать (1 предложение)
  - claim_type: "direct_violation" | "contradiction" | "explicit_omission" | "context_gap" | "recommendation"
  - problem_class: устойчивое краткое имя типа проблемы (snake_case)
  - affected_entity: конкретный объект замечания
  - evidence_quote: точная цитата либо однозначное описание видимого графического факта
  - evidence_kind: "block_image" | "block_vector" | "same_page" | "document_retrieval" | "none"
  - context_status: "sufficient" | "needs_retrieval" | "external_only"
  - confidence: число от 0 до 1
  - counterevidence_checked: true только если проверены переданные контексты
  - required_context: список недостающих источников; [] при достаточном контексте

В полях finding и recommendation НЕ упоминай внутренние идентификаторы
(block_id вида RUXD-WP4R-6C3, номера G-NNN/T-NNN) — их читают сторонние
эксперты. Ссылайся на источник словами: тип фрагмента + название + лист.

Строго JSON, без markdown-обёртки, без преамбулы.
"""

_EXTENDED_HEADER = """

## Категории замечаний (справочник применимых направлений поиска)

Сначала классифицируй блок, затем выбери только категории, которые подтверждаются
содержанием этого блока. Cross-discipline и cross-section разрешены только при наличии
двух явно переданных и противоречащих друг другу фактов; отсутствие смежного раздела
в контексте не является доказательством дефекта.

"""


def load_categories_for_section(section: str) -> str:
    """Подгрузить `finding_categories.md` профиля дисциплины (или пусто).

    Сегмент пути берётся не из аргумента, а из реестра: `section` приходит из
    `project_info` пользователя и может быть чем угодно — кириллическим кодом,
    для которого каталога с таким именем нет вовсе, или сегментом пути.
    """
    from backend.app.services.common import discipline_identity as _identity

    try:
        code = _identity.normalize_discipline_code(section)
        if code is None:
            return ""
        profile_dir = _identity.profile_dir_name(code)
    except _identity.DisciplineError:
        return ""
    path = _PROMPTS_DIR / "disciplines" / profile_dir / "finding_categories.md"
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
                        "claim_type": {
                            "type": "string",
                            "enum": [
                                "direct_violation", "contradiction", "explicit_omission",
                                "context_gap", "recommendation",
                            ],
                        },
                        "problem_class": {"type": "string"},
                        "affected_entity": {"type": "string"},
                        "evidence_quote": {"type": "string"},
                        "evidence_kind": {
                            "type": "string",
                            "enum": [
                                "block_image", "block_vector", "same_page",
                                "document_retrieval", "none",
                            ],
                        },
                        "context_status": {
                            "type": "string",
                            "enum": ["sufficient", "needs_retrieval", "external_only"],
                        },
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "counterevidence_checked": {"type": "boolean"},
                        "required_context": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": [
                        "severity", "category", "finding",
                        "norm_quote", "value_found", "recommendation",
                        "claim_type", "problem_class", "affected_entity",
                        "evidence_quote", "evidence_kind", "context_status",
                        "confidence", "counterevidence_checked", "required_context",
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

def load_page_text(
    graph: dict,
    page: int,
    *,
    per_block: int = 500,
    total: int = 4000,
    max_blocks: int = 20,
) -> str:
    """Текст листа для контекста блока.

    Лимиты параметризованы: дефолты (500/4000) — историческое поведение, при
    котором СПЕЦИФИКАЦИЯ режется в огрызок. Замер на 133-23-ГК-АИ2: полный текст
    страницы до 7567 симв, самый большой одиночный text_block — 5615 (таблица
    спецификации дверей с пределами EI); при per_block=500 в промпт уходило 28%
    текста, и модель репортила «не указан предел огнестойкости», хотя EI есть.
    Полный контекст стоит ~609 токенов/блок (~1% прогона) — см.
    STAGE01_PAGE_CONTEXT_ENABLED.
    """
    for p in graph.get("pages", []):
        if p.get("page") == page:
            parts = []
            if p.get("sheet_name"):
                parts.append(f"[SHEET] {p['sheet_name']}")
            for tb in p.get("text_blocks", [])[:max_blocks]:
                txt = (tb.get("text") or "").strip()
                if txt:
                    parts.append(txt[:per_block])
            return "\n".join(parts)[:total]
    return ""


# Лимиты контекста листа при STAGE01_PAGE_CONTEXT_ENABLED: двукратный запас над
# максимумом, замеренным на реальном разделе (страница 7567 / блок 5615), и
# одновременно защита от аномально длинных страниц в других проектах.
PAGE_CONTEXT_PER_BLOCK = 8000
PAGE_CONTEXT_TOTAL = 16000

# ── Контекст листа: РАЗДЕЛЕНИЕ данных и оговорки (07-17) ──────────────────────
# Историческая _page_context_section склеивала в одну строку ДВЕ разные вещи:
#   (D) ДАННЫЕ листа  — текст соседних text_blocks + описания/вектор соседних блоков;
#   (C) анти-FP ОГОВОРКУ — «не считай замечанием отсутствие расшифровки в блоке».
# Из-за склейки замер «global-ON вредит ЭОМ» смешал две оси: неизвестно, что навредило —
# данные или оговорка. Разводим: ДАННЫЕ подаём всем дисциплинам, ОГОВОРКУ — только там,
# где замер показал пользу (AI, планы интерьеров); на ЭОМ/СС оговорка глушит инженерную
# критику «не указано сечение/защиту/EI» (shadow: КРИТ 7→4, жёсткие 13→5).
#
# Флаги читаем из env ПРЯМО ЗДЕСЬ, а не в config.py, чтобы не коснуться строки, которую
# в рабочем дереве правит соседняя сессия (config.py:769). После её коммита можно перенести.
def _gfo_env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _gfo_env_set(name: str, default_set) -> set:
    raw = os.environ.get(name)
    if raw is None:
        return set(default_set)
    return {t.strip() for t in raw.replace(";", ",").split(",") if t.strip()}


def _gfo_env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name) or default)
    except (TypeError, ValueError):
        return default


# Режим анти-FP оговорки: off | all | disciplines (по коду section из project_info).
STAGE01_ABSENCE_CAVEAT_MODE = (
    os.environ.get("STAGE01_ABSENCE_CAVEAT_MODE", "disciplines").strip().lower()
    or "disciplines"
)
# Дисциплины (section-код), где оговорка ВКЛючена при mode=disciplines. Дефолт консервативный:
# только AI (замерено). AR (архитектура) — кандидат на добавление после замера, не по догадке.
STAGE01_ABSENCE_CAVEAT_DISCIPLINES = _gfo_env_set(
    "STAGE01_ABSENCE_CAVEAT_DISCIPLINES", {"AI"}
)
# Секция соседних блоков листа (описания из MD + вектор-текст соседей). Подфича ДАННЫХ листа:
# работает только когда STAGE01_PAGE_CONTEXT_ENABLED (иначе данные листа вообще не подаются).
STAGE01_PAGE_NEIGHBORS_ENABLED = _gfo_env_bool("STAGE01_PAGE_NEIGHBORS_ENABLED", True)
NEIGHBOR_MAX_BLOCKS = _gfo_env_int("STAGE01_NEIGHBOR_MAX_BLOCKS", 6)
NEIGHBOR_VECTOR_PER_BLOCK = _gfo_env_int("STAGE01_NEIGHBOR_VECTOR_PER_BLOCK", 800)
NEIGHBOR_MD_PER_BLOCK = _gfo_env_int("STAGE01_NEIGHBOR_MD_PER_BLOCK", 400)
NEIGHBOR_TOTAL = _gfo_env_int("STAGE01_NEIGHBOR_TOTAL", 6000)

# ── Координатно-привязанный вектор-текст соседей (07-18) ──────────────────────
# Замер на силовой однолинейке ЭМ_1-1: плоский вектор соседей (vector_covered_block_ids →
# _block_text, координаты потеряны) заставлял модель принимать РЕАЛЬНЫЕ внутрипанельные дубли
# позиционных обозначений (1QF6 дважды, 4QF32 D50 vs D40) за «склейку соседних блоков» → она
# УБИРАЛА реальную критику (10/11 подавленных находок были реальны, вкл. КРИТ 2QF8 16А<26,4А).
# Фикс: повторяющиеся обозначения аннотируются позицией @(gx,gy) в 30pt-сетке — два одинаковых
# кода на разных X ИЛИ Y читаются как РАЗНЫЕ аппараты. Полный Вектограф-граф здесь НЕ годится:
# он схлопывает идентичные метки QF в один узел (прячет ровно тот дубль). Флаг default OFF.
STAGE01_NEIGHBOR_COORD_TEXT_ENABLED = _gfo_env_bool("STAGE01_NEIGHBOR_COORD_TEXT_ENABLED", False)
# Ограничить координатный рендер профилем электрики/схематики (по block_type или QF-регэкспу):
# проблема дублей-обозначений специфична для схем, на планах/таблицах координаты = лишний шум.
STAGE01_NEIGHBOR_COORD_ELECTRICAL_ONLY = _gfo_env_bool("STAGE01_NEIGHBOR_COORD_ELECTRICAL_ONLY", True)
NEIGHBOR_COORD_GRID_PT = _gfo_env_int("STAGE01_NEIGHBOR_COORD_GRID_PT", 30)
NEIGHBOR_COORD_PER_BLOCK = _gfo_env_int("STAGE01_NEIGHBOR_COORD_PER_BLOCK", 1200)

# ── Третья нога ансамбля блок-анализа (07-20, за флагом) ──────────────────────
# Замер на силовой однолинейке ЭМ_1-1 (блок ГРЩ): разные модели ловят РАЗНЫЕ реальные
# находки (GPT-5.4 → надёжный PEN; codex-5.6-sol → больший объём, изредка ТТ), поэтому
# объединение трёх независимых ног шире двух. Добавляется к GPT-5.4 + codex-5.4, все на том
# же reasoning_effort (low в проде). codex-нога = subscription ($0). Default OFF — прод не
# трогаем до проверки на блоке; тяжёлые числовые (секционник/уставки) сюда НЕ закрываются
# ансамблем — под них отдельный детерминированный табличный чек.
STAGE01_THIRD_LEG_ENABLED = _gfo_env_bool("STAGE01_THIRD_LEG_ENABLED", False)
STAGE01_THIRD_LEG_MODEL = (
    os.environ.get("STAGE01_THIRD_LEG_MODEL", "codex/gpt-5.6-sol").strip()
    or "codex/gpt-5.6-sol"
)

# Позиционное обозначение аппарата/марки: 1QF6, 4QF32, РП1, ВА105, QF8, П2.2 и т.п.
_NEIGHBOR_DESIG_RE = re.compile(r"^\d?[A-ZА-Я]{1,3}\d")
# Электрический сосед по вектор-тексту (fallback, если block_type в графе пуст).
_NEIGHBOR_ELEC_RE = re.compile(r"\bQF\d|ВА\d{2,}|кВт|кВА|Iр=|РУНН|ГРЩ|ВРУ|\bРП\d", re.I)


def _render_neighbor_coord_text(clipped_words: list, grid: int = 30) -> str:
    """Координатно-привязанный текст блока: повторяющиеся обозначения → `КОД@(gx,gy)`.

    clipped_words — кортежи (x0,y0,x1,y1,text,block_no,line_no,word_no) в pt (из
    _clip_words_to_polygon). Порядок чтения: по строке-бэнду Y, затем X. Аннотируются ТОЛЬКО
    обозначения, встречающиеся в блоке ≥2 раз (экономия + именно они — источник ложной
    «склейки»). Детерминизм: round(x/grid), round(y/grid), стабильная сортировка → cache_key
    стабилен. Дедупа НЕТ (иначе воспроизвели бы баг схлопывания Вектографа).
    """
    if not clipped_words:
        return ""
    counts: dict[str, int] = {}
    for w in clipped_words:
        t = w[4]
        if _NEIGHBOR_DESIG_RE.match(t):
            counts[t] = counts.get(t, 0) + 1
    band = max(1, grid)
    ordered = sorted(clipped_words, key=lambda w: (round(w[1] / band), round(w[0])))
    out: list[str] = []
    for w in ordered:
        t = w[4]
        if counts.get(t, 0) >= 2:
            gx = int(w[0] // grid)
            gy = int(w[1] // grid)
            out.append(f"{t}@({gx},{gy})")
        else:
            out.append(t)
    return " ".join(out)


def _neighbor_is_electrical(block: dict, vec_text: str) -> bool:
    """Профиль соседа = электрика/схематика (для координатного рендера). Дёшево, без PDF."""
    bt = str(block.get("block_type") or block.get("ocr_label") or "").lower()
    if any(k in bt for k in ("single", "singleline", "electr", "grsh", "схем", "щит", "рп")):
        return True
    return bool(_NEIGHBOR_ELEC_RE.search(vec_text or ""))


def build_neighbor_coord_map(output_dir, graph: dict) -> dict:
    """{block_id: координатный текст} для ВСЕХ image-блоков — ОДИН проход по PDF на прогон.

    Тот же паттерн «один fitz.open на батч», что vector_covered_block_ids. Клип к полигону
    блока обязателен (иначе чужой текст листа протечёт). fail-soft → {} (тогда координатный
    режим тихо откатывается на плоский vector_map). Хелперы клипа импортируются read-only из
    singleline_graph_geometry (мой код), block_source_router НЕ редактируется.
    """
    try:
        import fitz  # локально, как в block_grounding
        from backend.app.pipeline.stages.block_grounding.block_source_router import _locate
        from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
            _clip_words_to_polygon,
            _clip_words_to_bbox,
        )
    except Exception:
        return {}
    try:
        pdf, dgp = _locate(output_dir)
        if pdf is None or dgp is None:
            return {}
        dg = json.loads(dgp.read_text(encoding="utf-8"))
        doc = fitz.open(str(pdf))
    except Exception:
        return {}
    try:
        out: dict = {}
        for p in dg.get("pages", []):
            pi = p.get("page_index", p.get("page"))
            if pi is None or pi >= doc.page_count:
                continue
            page = doc[pi]
            pw, ph = float(page.rect.width), float(page.rect.height)
            words = page.get_text("words")
            for b in p.get("image_blocks", []) or []:
                bid = b.get("id") or b.get("block_id")
                if not bid:
                    continue
                poly = b.get("polygon_points_norm")
                clipped = (
                    _clip_words_to_polygon(words, poly, pw, ph)
                    if poly
                    else _clip_words_to_bbox(words, b.get("coords_norm"), pw, ph)
                )
                txt = _render_neighbor_coord_text(clipped, grid=NEIGHBOR_COORD_GRID_PT)
                if txt.strip():
                    out[str(bid)] = txt
        return out
    except Exception:
        return {}
    finally:
        try:
            doc.close()
        except Exception:
            pass


def _caveat_enabled_for_section(section: str) -> bool:
    """Включать ли анти-FP оговорку для данной дисциплины (section-код)."""
    mode = STAGE01_ABSENCE_CAVEAT_MODE
    if mode == "off":
        return False
    if mode == "all":
        return True
    return (section or "").strip() in STAGE01_ABSENCE_CAVEAT_DISCIPLINES


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
    # Номер листа читаем через общий v1/v2-helper: в v2-графе sheet_no всегда
    # None, а номер лежит в sheet_no_raw. Прямое чтение sheet_no уводило в
    # fallback на sheet_name → в block_analyses[].sheet попадало НАЗВАНИЕ листа
    # («Корпус 14.6. Маркировочные планы 1 этажа»), merge копировал его в
    # finding["sheet"], и в UI/Excel вместо «Лист 2» выводилось название.
    from backend.app.pipeline.stages.prepare.graph_builder import get_page_sheet_no

    for p in graph.get("pages", []):
        if p.get("page") == page:
            sno = get_page_sheet_no(p)
            if sno:
                return f"Лист {sno}"
            return p.get("sheet_name")
    return None


@lru_cache(maxsize=32)
def _blocks_index_top_cached(index_path: str, mtime_ns: int, size: int) -> dict:
    try:
        data = json.loads(Path(index_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {k: v for k, v in data.items() if k != "blocks"}


def _blocks_index_top(blocks_dir: Path) -> dict:
    """Шапка index.json (политика рендера) — для ключа платного кэша.

    Мемоизируем по (путь, mtime, размер): функция зовётся на каждый блок, а
    index.json бывает на мегабайты.
    """
    index_path = Path(blocks_dir) / "index.json"
    try:
        st = index_path.stat()
    except OSError:
        return {}
    return _blocks_index_top_cached(str(index_path), st.st_mtime_ns, st.st_size)


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


def _page_context_section(page_text: str) -> str:
    """Секция контекста листа для промпта блока + анти-FP оговорка.

    Блок — это ФРАГМЕНТ листа. Легенда, примечания и спецификации физически
    находятся вне его полигона (обычно на том же листе, иногда на соседнем —
    это норма РД, а не дефект). Без этой секции модель считает недостачу
    аномалией документа и репортит «расшифровка отсутствует».

    ⚠️ ЭТО ВЕРСИЯ v1. Не «улучшать» её оговорками — проверено замером 07-17.
    Версия v2 (противоречие выведено из-под фильтра + защита «инженерных дефектов»
    от глушения + анти-crowding-out) дала на том же прогоне РЕГРЕСС:
      * чистый шум 24 → 51 (8% → 16%), findings 273 → 313;
      * КРИТ 18 → 28, но точность КРИТ = 7/28 = 25%; из 7 подтверждённых лишь ОДИН
        уникален для v2 (и с выдуманной нормой), остальные 6 уже были в v1 другой
        категорией = инфляция severity;
      * «противоречие вне фильтра» породило ИНВЕРСНЫЙ класс FP — модель конструирует
        расхождение там, где план и ведомость совпадают (П-1 стр.5: 16.05 м² сходится);
      * «защита инженерных дефектов» превратила подрезки плитки (735/890/895, стр.10)
        в фиктивное КРИТ по доступности — в v1 они корректно = РЕКОМЕНДАТЕЛЬНОЕ;
      * потерян кросс-детекторный дедуп: один ложный тезис про клапаны выдан 4× как КРИТ.
    Единственную реальную потерю v1 (доводчик с фиксацией на дверях EI/EIS) забирать
    точечным правилом, а не откатом на v2.
    """
    return _page_context_data_section(page_text) + _absence_caveat_section()


def _page_context_data_section(page_text: str, neighbors_section: str = "") -> str:
    """ТОЛЬКО ДАННЫЕ листа (без единой фразы оговорки): текст листа + опц. соседние блоки.

    Подаётся ВСЕМ дисциплинам при STAGE01_PAGE_CONTEXT_ENABLED. Не содержит инструкций
    «не считай замечанием…» — они вынесены в _absence_caveat_section и включаются отдельно.
    """
    out = (
        "\n## Контекст ЛИСТА (условные обозначения, примечания, спецификации, ведомость):\n"
        f"```\n{page_text}\n```\n"
    )
    if neighbors_section:
        out += neighbors_section
    return out


def _absence_caveat_section() -> str:
    """ТОЛЬКО анти-FP оговорка о границах фрагмента (без данных).

    ⚠️ Замер 07-17: помогает на AI (планы интерьеров), ВРЕДИТ на ЭОМ/СС (глушит инженерную
    критику «не указано сечение/защиту/EI»). Поэтому включается per-discipline, не глобально.
    См. _caveat_enabled_for_section и STAGE01_ABSENCE_CAVEAT_*.
    """
    return (
        "\n## Границы фрагмента (важно):\n"
        "Анализируемый блок — ФРАГМЕНТ листа, а не весь лист. Условные обозначения, "
        "расшифровки марок, сноски («*»), спецификации и ведомости размещаются вне блока — "
        "выше по листу или на отдельных листах спецификаций. Это НОРМА рабочей документации.\n"
        "- НЕ считай замечанием то, что расшифровка/легенда/сноска/спецификация отсутствует "
        "В САМОМ БЛОКЕ: сначала проверь контекст листа выше.\n"
        "- Если расшифровка есть в контексте листа — замечания НЕТ.\n"
        "- Сообщай о недостающей расшифровке ТОЛЬКО если её нет ни в блоке, ни в контексте "
        "листа, и при этом она нужна для однозначного чтения чертежа. Тогда формулируй по "
        "существу («в проекте не найдена ведомость X»), а не «на фрагменте не показано».\n"
    )


def build_page_neighbors_section(
    graph: dict,
    target_block_id: str,
    page,
    vector_map: dict,
    by_id: dict,
    coord_map: Optional[dict] = None,
) -> str:
    """Секция «соседние блоки этого листа» для контекста анализируемого блока.

    Требование Андрея Ивановича (07-17): если на странице есть другие блоки — подать их
    ОПИСАНИЯ из MD (ocr_raw) + ТЕКСТОВЫЙ СЛОЙ (вектор PDF). Так блок «не указано X»
    видит, что X есть на соседнем блоке того же листа (78% ложняков судьи — именно это).

    Детерминизм обязателен (cache_key завязан на user_text): соседи ранжируются по
    геометрической близости центроидов coords_norm, тай-брейк по block_id. Жёсткие лимиты
    (NEIGHBOR_*) против crowding-out: на листе из 20 блоков берём только ближайшие 6.

    Приоритет источников: вектор-текст (точный, из PDF) выше MD-описания (подсказка
    нейросети портала — врёт: транслитерация Д→D, выдуманные размеры). MD помечается как
    подсказка, при конфликте — верить чертежу/PDF (правило проекта PDF > MD).
    """
    pg = next((p for p in graph.get("pages", []) if p.get("page") == page), None)
    if not pg:
        return ""
    tgt_center = None
    cands: list[dict] = []
    for b in pg.get("image_blocks", []) or []:
        bid = str(b.get("id") or b.get("block_id") or "")
        if not bid:
            continue
        coords = b.get("coords_norm") or []
        cx = (coords[0] + coords[2]) / 2 if len(coords) >= 4 else 0.5
        cy = (coords[1] + coords[3]) / 2 if len(coords) >= 4 else 0.5
        if bid == target_block_id:
            tgt_center = (cx, cy)
            continue
        vec = str(vector_map.get(bid) or "")
        md = str(b.get("ocr_raw") or b.get("ocr_text_normalized") or "")
        if not vec and not md:
            continue  # штамп/пустой блок — нечего показывать
        cands.append({"bid": bid, "cx": cx, "cy": cy, "vec": vec, "md": md, "block": b})
    if not cands:
        return ""
    if tgt_center is not None:
        for c in cands:
            c["dist"] = ((c["cx"] - tgt_center[0]) ** 2 + (c["cy"] - tgt_center[1]) ** 2) ** 0.5
    else:
        for c in cands:
            c["dist"] = 0.0
    cands.sort(key=lambda c: (round(c["dist"], 6), c["bid"]))

    parts = [
        "\n## Соседние блоки на этом листе (для контекста; при конфликте — верь чертежу/PDF):\n"
    ]
    total = 0
    used = 0
    for c in cands:
        if used >= NEIGHBOR_MAX_BLOCKS:
            break
        lines = [f"### Соседний блок ({c['bid'][:12]}…):"]
        # Для электрических/схемных соседей — координатно-привязанный текст (повторы обозначений
        # с позициями @(gx,gy)), чтобы реальные внутрипанельные дубли не читались как «склейка».
        _coord = ""
        if (
            STAGE01_NEIGHBOR_COORD_TEXT_ENABLED
            and coord_map
            and (not STAGE01_NEIGHBOR_COORD_ELECTRICAL_ONLY
                 or _neighbor_is_electrical(c.get("block") or {}, c["vec"]))
        ):
            _coord = str(coord_map.get(c["bid"]) or "")
        if _coord:
            lines.append(
                "Точный текст (вектор-слой PDF, привязка по координатам; повтор обозначения "
                "на разной позиции @(x,y) = РАЗНЫЕ аппараты, НЕ артефакт склейки): "
                f"{_coord[:NEIGHBOR_COORD_PER_BLOCK]}"
            )
        elif c["vec"]:
            lines.append(f"Точный текст (вектор-слой PDF): {c['vec'][:NEIGHBOR_VECTOR_PER_BLOCK]}")
        if c["md"]:
            lines.append(
                "Описание (подсказка нейросети, НЕ истина; при конфликте — PDF): "
                f"{c['md'][:NEIGHBOR_MD_PER_BLOCK]}"
            )
        chunk = "\n".join(lines) + "\n"
        if total + len(chunk) > NEIGHBOR_TOTAL:
            break
        parts.append(chunk)
        total += len(chunk)
        used += 1
    if used == 0:
        return ""
    return "\n".join(parts)


def build_effective_block_context(
    block: dict,
    enrichment: dict,
    page_text: str,
    *,
    output_dir: Optional[Path] = None,
    routed_context: Optional[tuple[str, str]] = None,
    document_context: str = "",
    document_type: str = "",
    page_neighbors: str = "",
    include_absence_caveat: bool = False,
) -> tuple[str, str]:
    """Build the Stage 01 prompt text and its normalized context source.

    ``page_neighbors`` — предсобранная секция соседних блоков листа (ДАННЫЕ, добавляются
    к тексту листа). ``include_absence_caveat`` — добавлять ли анти-FP оговорку (per-discipline,
    решается вызывающим через _caveat_enabled_for_section). Обе применяются ТОЛЬКО когда
    STAGE01_PAGE_CONTEXT_ENABLED и есть page_text.
    """
    user_text = build_block_user_text(block["block_id"], block["page"], enrichment, page_text)
    context_source = _context_source_from_enrichment(enrichment)

    # The source router is the canonical Stage 01 path. A block without vector text keeps
    # the image-only placeholder text and is still analyzed from its attached PNG.
    _router_applied = False
    if routed_context is not None:
        _rtext, _rkind = routed_context
        context_source = "image_only" if _rkind == "gemma_fallback" else _rkind
        if _rtext:
            user_text = _rtext
            from backend.app.core import config as _pcfg
            if _pcfg.STAGE01_PAGE_CONTEXT_ENABLED and page_text:
                user_text += _page_context_data_section(page_text, page_neighbors)
                if include_absence_caveat:
                    user_text += _absence_caveat_section()
        _router_applied = True
    elif output_dir is not None:
        try:
            from backend.app.pipeline.stages.block_grounding.block_source_router import (
                resolve_block_source as _resolve_block_source,
            )
            _rtext, _rkind = _resolve_block_source(
                output_dir, block.get("block_id", ""), block.get("page"))
            context_source = "image_only" if _rkind == "gemma_fallback" else _rkind
            if _rtext:
                # Роутер возвращает ТОЛЬКО текст самого блока и затирает user_text,
                # собранный build_block_user_text вместе с page_text. Контекст листа
                # (легенда/примечания/спецификации) терялся молча, хотя system-промпт
                # обещает его модели — отсюда documentation-шум «расшифровка
                # отсутствует». Возвращаем его поверх роутерного текста.
                user_text = _rtext
                from backend.app.core import config as _pcfg
                if _pcfg.STAGE01_PAGE_CONTEXT_ENABLED and page_text:
                    user_text += _page_context_data_section(page_text, page_neighbors)
                    if include_absence_caveat:
                        user_text += _absence_caveat_section()
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

    if document_type:
        user_text += (
            "\n\n## Тип проверяемого документа\n"
            f"document_type={document_type}. Для полного комплекта РД отсутствие данных "
            "на одном фрагменте не доказывает отсутствие в документе.\n"
        )
    if document_context:
        user_text += "\n\n" + document_context
        user_text += (
            "\n\nРезультаты поиска выше являются дополнительным контекстом. "
            "Если нужного источника нет и утверждение нельзя доказать, поставь "
            "context_status=needs_retrieval или external_only; не публикуй гипотезу как факт.\n"
        )

    return user_text, context_source


def build_effective_block_user_text(
    block: dict,
    enrichment: dict,
    page_text: str,
    *,
    output_dir: Optional[Path] = None,
    routed_context: Optional[tuple[str, str]] = None,
    document_context: str = "",
    document_type: str = "",
) -> str:
    """Compatibility wrapper for callers that only need the Stage 01 text."""
    return build_effective_block_context(
        block, enrichment, page_text, output_dir=output_dir,
        routed_context=routed_context, document_context=document_context,
        document_type=document_type,
    )[0]


# ─── OpenRouter call ────────────────────────────────────────────────────────

async def call_provider_for_block(
    block: dict,
    enrichment: dict,
    page_text: str,
    blocks_dir: Path,
    *,
    system_prompt: str,
    timeout: int,
    output_dir: Optional[Path] = None,
    routed_context: Optional[tuple[str, str]] = None,
    document_context: str = "",
    document_type: str = "",
    page_neighbors: str = "",
    include_absence_caveat: bool = False,
) -> dict:
    """Нога Stage 01 через ProviderAdapter воркера (этап 11F).

    Контракт возврата — тот же словарь, что у остальных ног
    (`ok`/`parsed`/`elapsed_ms`/токены/`context_source`), поэтому сводящий код
    (`combine_detector_results`, провенанс, счётчики) не меняется ни строкой.

    Отличий от `call_claude_cli_for_block` ровно три, и все транспортные:
    изображение уходит байтами в теле запроса, инструментов у модели нет,
    результат возвращается объектом, а не файлом.
    """
    import asyncio as _asyncio

    from audit_worker.providers import pipeline_bridge
    from audit_worker.providers.pipeline_bridge import ProviderBridgeError
    from backend.app.pipeline.stages.block_analysis import provider_transport as _pt

    started = time.monotonic()
    block_id = str(block.get("block_id") or "")
    try:
        image = _pt.read_crop(blocks_dir, block.get("file") or "")
    except _pt.BlockInputError as exc:
        return {"ok": False, "error": str(exc), "elapsed_ms": 0}

    user_text, context_source = build_effective_block_context(
        block,
        enrichment,
        page_text,
        output_dir=output_dir,
        routed_context=routed_context,
        document_context=document_context,
        document_type=document_type,
        page_neighbors=page_neighbors,
        include_absence_caveat=include_absence_caveat,
    )
    built = _pt.build_provider_prompt(
        system_prompt=system_prompt, user_text=user_text,
    )

    try:
        attempt_dir = pipeline_bridge.attempt_dir()
        outcome = await _asyncio.to_thread(
            lambda: pipeline_bridge.run_stage_inference(
                job_dir=attempt_dir,
                stage="block_analysis",
                prompt=built["prompt"],
                # `purpose` включает block_id: иначе два блока одной попытки
                # получили бы соседние ключи только за счёт отпечатка вложения,
                # а в журнале их было бы не различить глазом.
                purpose=f"block_analysis:{block_id}",
                required_result_fields=_pt.REQUIRED_RESULT_FIELDS,
                field_types=_pt.FIELD_TYPES,
                timeout_sec=float(timeout),
                images=[(_pt.CROP_MEDIA_TYPE, image)],
            )
        )
    except ProviderBridgeError as exc:
        return {
            "ok": False,
            "error": f"provider_bridge: {exc}",
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "context_source": context_source,
        }

    elapsed_ms = int((time.monotonic() - started) * 1000)
    result = outcome.provider_result
    usage = dict(result.usage)
    payload = result.result if isinstance(result.result, dict) else {}
    if not outcome.ok:
        return {
            "ok": False,
            "error": _pt.failure_detail(outcome),
            "elapsed_ms": elapsed_ms,
            "input_tokens": usage.get("input_tokens"),
            "output_tokens": usage.get("output_tokens"),
            "cli_reported_cost_usd": usage.get("total_cost_usd"),
            "exit_code": result.exit_code,
            "context_source": context_source,
            "provider_performed": bool(outcome.performed),
        }
    parsed = {"findings": _pt.result_findings(payload)}
    return {
        "ok": True,
        "parse_error": None,
        "elapsed_ms": elapsed_ms,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "reasoning_tokens": None,
        "cli_reported_cost_usd": usage.get("total_cost_usd"),
        "raw_content": "",
        "parsed": parsed,
        "exit_code": result.exit_code,
        "context_source": context_source,
        "provider_performed": bool(outcome.performed),
        "provider_model": result.model,
        "provider_prompt_sha256": pipeline_bridge.sha256_text(built["prompt"]),
        "provider_prompt_map": built["map"],
        "provider_soft_contract": _pt.soft_contract_report(payload),
    }


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
    routed_context: Optional[tuple[str, str]] = None,
    document_context: str = "",
    document_type: str = "",
    page_neighbors: str = "",
    include_absence_caveat: bool = False,
) -> dict:
    png_path = blocks_dir / block["file"]
    if not png_path.exists():
        return {"ok": False, "error": f"PNG missing: {png_path.name}", "elapsed_ms": 0}

    user_text, context_source = build_effective_block_context(
        block,
        enrichment,
        page_text,
        output_dir=output_dir,
        routed_context=routed_context,
        document_context=document_context,
        document_type=document_type,
        page_neighbors=page_neighbors,
        include_absence_caveat=include_absence_caveat,
    )

    # ─── Paid response cache check (до guard и до сети) ────────────
    # Если этот блок с этим model/prompt/image уже отвечал — берём из
    # cache, никаких paid_event и денег. Спасает в инциденте 2026-05-16,
    # где retry платил $0.32 за повтор того же блока.
    from backend.app.pipeline.stages.block_analysis import stage02_paid_cache
    cache_key = ""
    if stage02_paid_cache.cache_enabled() and output_dir is not None:
        try:
            # Идентичность картинки берём из index.json, а НЕ из байтов PNG:
            # восстановленный после эвакуации кроп выглядит так же, но байт-в-байт
            # не совпадает — на байтах кэш промахивался бы всегда и блок платился
            # бы повторно (ровно то, ради чего кэш и заводили).
            cache_key = stage02_paid_cache.compute_cache_key(
                model=model,
                block_id=str(block.get("block_id", "")),
                system_prompt=system_prompt,
                user_text=user_text,
                enrichment=enrichment,
                page_text=page_text,
                image_identity=stage02_paid_cache.build_image_identity(
                    block, _blocks_index_top(blocks_dir)
                ),
            )
            cached = stage02_paid_cache.try_load_cached(output_dir, cache_key)
            if cached is not None:
                cached.setdefault("context_source", context_source)
                return cached
        except OSError:
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
    reasoning_effort: str = "",
    project_id: str = "",
    output_dir: Optional[Path] = None,
    routed_context: Optional[tuple[str, str]] = None,
    document_context: str = "",
    document_type: str = "",
    page_neighbors: str = "",
    include_absence_caveat: bool = False,
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
        routed_context=routed_context,
        document_context=document_context,
        document_type=document_type,
        page_neighbors=page_neighbors,
        include_absence_caveat=include_absence_caveat,
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
        reasoning_effort=reasoning_effort,
        output_schema=RESPONSE_SCHEMA["schema"],
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
        "cached_input_tokens": result.cached_tokens,
        "output_tokens": result.output_tokens,
        "reasoning_tokens": result.reasoning_tokens,
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
        item = {
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
                mode=str(f.get("_detection_mode") or detection_mode),
                detected_at=detected_at,
                context_source=context_source,
            ),
        }
        detector_ref = str(f.get("_detector_ref") or "").strip()
        if detector_ref:
            item["comparison_ref"] = detector_ref
        comparison = f.get("_comparison")
        if isinstance(comparison, dict):
            item["detector_comparison"] = dict(comparison)
        observations = f.get("_finding_evidence_observations")
        if isinstance(observations, list):
            item["_finding_evidence_observations"] = [
                dict(observation)
                for observation in observations
                if isinstance(observation, dict)
            ]
        out.append(item)
    return out


# ─── Main runner ────────────────────────────────────────────────────────────

class FindingsOnlyError(Exception):
    """Прерывание прогона (отсутствие prerequisites, отмена и т.п.)."""


def combine_detector_results(
    detector_results: list[tuple[str, dict]],
    *,
    run_id: str,
) -> dict:
    """Combine independent payloads without deduplication and assign stable refs."""
    combined_findings: list[dict] = []
    ok_models: list[str] = []
    failed_models: list[str] = []
    paid_input_tokens = 0
    paid_output_tokens = 0
    paid_cached_input_tokens = 0
    paid_cached_output_tokens = 0

    _ref_offsets: dict[str, int] = {}
    for detector_model, result in detector_results:
        _det_key = detector_for_model(detector_model)
        _ref_base = _ref_offsets.get(_det_key, 0)
        _last_ref_index = 0
        if result.get("ok"):
            ok_models.append(detector_model)
            for raw_index, raw in enumerate(
                (result.get("parsed") or {}).get("findings") or [], start=1
            ):
                _last_ref_index = raw_index
                if not isinstance(raw, dict):
                    continue
                tagged = dict(raw)
                tagged["_detector_model"] = detector_model
                tagged["_detector_run_id"] = f"{run_id}:{_det_key}"
                tagged["_detector_ref"] = (
                    f"{_det_key}:{_ref_base + raw_index:03d}"
                )
                combined_findings.append(tagged)
        else:
            failed_models.append(detector_model)
        # Сквозная нумерация ref для ног ОДНОГО детектора (две codex-ноги): без смещения обе
        # дали бы «codex:001…», и dual_review (словарь по _detector_ref) потерял бы находки
        # одной. Для 2-ногого случая (gpt+codex) смещение=0 → поведение прежнее (обр. совм.).
        _ref_offsets[_det_key] = _ref_base + _last_ref_index

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
    md_text_for_type = ""
    md_path_for_type = _resolve_md_path(project_dir, project_info)
    if md_path_for_type is not None:
        try:
            md_text_for_type = md_path_for_type.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            md_text_for_type = ""
    from backend.app.services.text_analysis.document_type_detector import (
        detect_document_type,
    )
    document_type, document_type_confidence = detect_document_type(
        project_info, md_text_for_type or None
    )
    index = json.loads(index_path.read_text(encoding="utf-8"))
    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    context_summary = load_block_context_summary(output_dir)

    # Вектор-текст ВСЕХ блоков документа для секции «соседние блоки листа» — ОДИН проход
    # по PDF на весь прогон (не resolve поблочно: там каждый вызов заново открывает PDF).
    # Считаем только когда контекст листа и соседи включены, иначе — 0 стоимости (прод OFF).
    from backend.app.core import config as _pcfg_nb
    neighbor_vector_map: dict = {}
    symbol_vector_index: dict = {}
    symbol_evidence_enabled = bool(
        getattr(_pcfg_nb, "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED", False)
    )
    page_neighbors_need_vector = (
        _pcfg_nb.STAGE01_PAGE_CONTEXT_ENABLED
        and STAGE01_PAGE_NEIGHBORS_ENABLED
        and output_dir is not None
    )
    if symbol_evidence_enabled and output_dir is not None:
        try:
            from backend.app.pipeline.stages.block_grounding.block_source_router import (
                vector_text_block_index,
            )
            symbol_vector_index = vector_text_block_index(output_dir) or {}
            if page_neighbors_need_vector:
                neighbor_vector_map = {
                    block_id: record.get("text")
                    for block_id, record in symbol_vector_index.items()
                    if isinstance(record, dict)
                    and record.get("router_eligible") is True
                }
        except Exception:
            symbol_vector_index = {}
            neighbor_vector_map = {}
    elif page_neighbors_need_vector:
        try:
            from backend.app.pipeline.stages.block_grounding.block_source_router import (
                vector_covered_block_ids,
            )
            neighbor_vector_map = vector_covered_block_ids(output_dir) or {}
        except Exception:
            neighbor_vector_map = {}

    # Координатно-привязанный текст соседей (позиции повторов обозначений) — ОДИН проход по PDF.
    # Только когда координатный режим включён поверх соседей (иначе 0 стоимости).
    neighbor_coord_map: dict = {}
    if (
        _pcfg_nb.STAGE01_PAGE_CONTEXT_ENABLED
        and STAGE01_PAGE_NEIGHBORS_ENABLED
        and STAGE01_NEIGHBOR_COORD_TEXT_ENABLED
        and output_dir is not None
    ):
        try:
            neighbor_coord_map = build_neighbor_coord_map(output_dir, graph) or {}
        except Exception:
            neighbor_coord_map = {}
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

    # Мост воркера (этап 11F) перекрывает ЛЮБОЙ выбор транспорта: когда
    # исполнитель выписал привязку, единственный разрешённый путь к модели —
    # провайдерский слой. Развилка стоит выше остальных намеренно, ровно как в
    # `run_text_analysis`/`run_findings_merge`: решать «каким CLI» после сборки
    # промпта было бы поздно.
    use_provider_bridge = provider_bridge_active()
    use_claude_cli = (not use_provider_bridge) and is_claude_cli_model(model)
    use_codex_cli = (not use_provider_bridge) and is_codex_model(model)
    use_dual = (not use_provider_bridge) and model == STAGE02_DUAL_MODEL_ID
    detector_models = (
        [DEFAULT_MODEL, CODEX_STAGE_MODEL_ID]
        if use_dual
        else [model]
    )
    configured_detector_models = list(detector_models)
    if STAGE01_PROTECTION_TABLE_CHECK_ENABLED:
        configured_detector_models.append(PROTECTION_DETECTOR_MODEL)

    if not use_provider_bridge and not use_claude_cli and not use_codex_cli:
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
            "document_type": document_type,
            "document_type_confidence": document_type_confidence,
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
    # Backstop-потолок на блок: гарантирует завершение даже если транспортный
    # timeout обойдён (trickle keepalive). При превышении блок помечается
    # неудачным, семафор освобождается, стадия продолжается.
    block_hard_timeout_s = timeout_s + BLOCK_HARD_TIMEOUT_BUFFER_S
    completed_count = 0
    completed_lock = asyncio.Lock()
    results: list[dict] = []

    # ── Остановка стадии при выпадении ноги ансамбля ──────────────────────
    # Отдельное событие, а НЕ cancel_event: отмена пользователем и аварийная
    # остановка должны различаться на выходе (cancel → StageResult.cancel,
    # выпавшая нога → StageResult.fail с текстом, какая именно упала).
    # Проверяется в тех же двух точках, что и отмена: уже запущенные блоки
    # доработают, новые не начнутся — это и есть «не продолжаем».
    abort_event = asyncio.Event()
    leg_failures: list[dict] = []
    leg_failure_lock = asyncio.Lock()

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
        async def _skip_after_abort() -> None:
            """Отметить брошенный блок, чтобы прогресс не замер на месте.

            Без этого счётчик «сделано N из M» останавливается и в интерфейсе
            остановка неотличима от зависания.
            """
            nonlocal completed_count
            async with completed_lock:
                completed_count += 1
                cur_ = completed_count
            if on_progress:
                on_progress({
                    "type": "block_skip", "block_id": item["block_id"],
                    "page": item["page"], "reason": "leg_failure_abort",
                    "completed": cur_, "total": len(wanted),
                })

        if cancel_event is not None and cancel_event.is_set():
            return None
        if abort_event.is_set():
            await _skip_after_abort()
            return None
        async with sem:
            if cancel_event is not None and cancel_event.is_set():
                return None
            # Нога упала у соседнего блока, пока мы стояли за семафором —
            # не начинаем ещё один платный вызов ради заведомо брошенной стадии.
            if abort_event.is_set():
                await _skip_after_abort()
                return None
            block = by_id[item["block_id"]]
            # При включённом контексте листа берём его целиком: дефолтные 500 симв
            # на text_block режут таблицу спецификации в огрызок (28% текста), и
            # блок «не видит» EI/марки, которые в проекте есть.
            from backend.app.core import config as _pcfg2
            if _pcfg2.STAGE01_PAGE_CONTEXT_ENABLED:
                page_text = load_page_text(
                    graph, block["page"],
                    per_block=PAGE_CONTEXT_PER_BLOCK, total=PAGE_CONTEXT_TOTAL,
                )
            else:
                page_text = load_page_text(graph, block["page"])

            # ДАННЫЕ: секция соседних блоков листа (описания MD + вектор-слой).
            # ОГОВОРКА: анти-FP, per-discipline (section из project_info) — только где помогает.
            page_neighbors = ""
            include_caveat = False
            if _pcfg2.STAGE01_PAGE_CONTEXT_ENABLED:
                include_caveat = _caveat_enabled_for_section(section)
                if STAGE01_PAGE_NEIGHBORS_ENABLED and neighbor_vector_map:
                    try:
                        page_neighbors = build_page_neighbors_section(
                            graph, str(block.get("block_id") or ""),
                            block.get("page"), neighbor_vector_map, by_id,
                            coord_map=neighbor_coord_map,
                        )
                    except Exception:
                        page_neighbors = ""

            # Resolve the canonical block package once.  ``prefer_prepared=False``
            # deliberately refreshes profile routing when code/profile mappings
            # change (notably AI -> architecture); otherwise an old raw-vector
            # prepared package would silently survive a rerun.
            routed_context: Optional[tuple[str, str]] = None
            retrieval_query_text = ""
            package: Optional[dict[str, Any]] = None
            try:
                from backend.app.pipeline.stages.block_grounding.block_source_router import (
                    resolve_block_package,
                )
                # Разбор вектор-слоя — чистый CPU (fitz + геометрия профиля):
                # замерено 0,7 с/блок на лёгкой дисциплине и до 40 с на одном
                # блоке тяжёлой. Раньше он считался ПРЯМО НА event loop внутри
                # `async def _one`: при нескольких параллельных проектах это не
                # деградация латентности, а остановка бэкенда — health-проверка
                # не отвечает, и вотчдог убивает живой аудит.
                # Уводим в общий пул процессов (common/cpu_pool.py): пул один на
                # бэкенд, поэтому проекты делят бюджет ядер, а не плодят свои.
                package = await cpu_pool.run(
                    _resolve_block_package_in_worker,
                    str(output_dir),
                    str(block.get("block_id") or ""),
                    block.get("page"),
                )
                package_text = str(package.get("user_text") or "")
                package_kind = str(package.get("source_kind") or "error")
                if package_text:
                    routed_context = (package_text, package_kind)
                classification = package.get("classification") or {}
                retrieval_query_text = str(
                    classification.get("block_title")
                    or classification.get("description")
                    or package_text
                )
            except Exception:
                routed_context = None

            protection_pair: Optional[tuple[str, dict]] = None
            if STAGE01_PROTECTION_TABLE_CHECK_ENABLED:
                try:
                    protection_result = run_protection_table_detector(
                        package,
                        output_dir=output_dir,
                        block_id=str(block.get("block_id") or ""),
                    )
                    if protection_result is not None:
                        protection_pair = (PROTECTION_DETECTOR_MODEL, protection_result)
                except Exception:
                    # The optional deterministic leg is strictly fail-soft.
                    protection_pair = None

            from backend.app.pipeline.stages.block_analysis.document_retrieval import (
                retrieve_document_context,
            )
            retrieval_query = retrieval_query_text or (
                routed_context[0] if routed_context else page_text
            )
            document_context, retrieval_receipt = retrieve_document_context(
                graph, retrieval_query, int(block.get("page") or 0)
            )

            async def _dispatch() -> dict:
                """Один вызов на блок по выбранному транспорту (без backstop)."""
                if use_dual:
                    assert client is not None
                    _dispatch_calls = [
                        call_gpt_for_block(
                            client, block, item["enrichment"], page_text, blocks_dir,
                            api_key=api_key or "", model=DEFAULT_MODEL,
                            reasoning_effort=reasoning_effort,
                            max_tokens=max_tokens, system_prompt=system_prompt,
                            timeout=timeout_s, project_id=project_id,
                            version_id=version_id, job_id=job_id,
                            output_dir=output_dir,
                            routed_context=routed_context,
                            document_context=document_context,
                            document_type=document_type,
                            page_neighbors=page_neighbors,
                            include_absence_caveat=include_caveat,
                        ),
                        call_codex_for_block(
                            block, item["enrichment"], page_text, blocks_dir,
                            model=CODEX_STAGE_MODEL_ID,
                            system_prompt=system_prompt, timeout=timeout_s,
                            reasoning_effort=reasoning_effort,
                            project_id=project_id, output_dir=output_dir,
                            routed_context=routed_context,
                            document_context=document_context,
                            document_type=document_type,
                            page_neighbors=page_neighbors,
                            include_absence_caveat=include_caveat,
                        ),
                    ]
                    # Третья нога (за флагом): ещё одна независимая codex-модель (по умолчанию
                    # codex/gpt-5.6-sol) на том же low. Разные модели ловят РАЗНЫЕ находки —
                    # combine_detector_results делает union по списку ног любой длины.
                    _use_third_leg = (
                        STAGE01_THIRD_LEG_ENABLED
                        and STAGE01_THIRD_LEG_MODEL
                        and STAGE01_THIRD_LEG_MODEL != CODEX_STAGE_MODEL_ID
                    )
                    if _use_third_leg:
                        _dispatch_calls.append(
                            call_codex_for_block(
                                block, item["enrichment"], page_text, blocks_dir,
                                model=STAGE01_THIRD_LEG_MODEL,
                                system_prompt=system_prompt, timeout=timeout_s,
                                reasoning_effort=reasoning_effort,
                                project_id=project_id, output_dir=output_dir,
                                routed_context=routed_context,
                                document_context=document_context,
                                document_type=document_type,
                                page_neighbors=page_neighbors,
                                include_absence_caveat=include_caveat,
                            )
                        )
                    # return_exceptions обязателен: у ног есть НЕобёрнутые raise
                    # (png_to_data_url на эвакуированном кропе — OSError;
                    # resp.json() при 2xx с не-JSON телом от шлюза OpenRouter).
                    # Без него исключение вылетало из _dispatch мимо ветки
                    # TimeoutError, ловилось внешним gather и превращало блок в
                    # «Unhandled single-block exception» БЕЗ detectors_failed —
                    # то есть выпавшая нога переставала быть видна, а уже
                    # оплаченный ответ соседней ноги выбрасывался.
                    _dispatch_results = await asyncio.gather(
                        *_dispatch_calls, return_exceptions=True
                    )
                    _normalized: list = []
                    for _r in _dispatch_results:
                        # CancelledError НЕ глушим: на нём держатся backstop-таймаут
                        # блока (wait_for) и отмена аудита пользователем.
                        if isinstance(_r, asyncio.CancelledError):
                            raise _r
                        if isinstance(_r, BaseException):
                            _normalized.append({
                                "ok": False,
                                "error": f"{type(_r).__name__}: {_r}",
                                "parse_error": "leg_exception",
                                "elapsed_ms": 0,
                            })
                        else:
                            _normalized.append(_r)
                    _dispatch_results = _normalized
                    _detector_pairs = [
                        (DEFAULT_MODEL, _dispatch_results[0]),
                        (CODEX_STAGE_MODEL_ID, _dispatch_results[1]),
                    ]
                    if _use_third_leg:
                        _detector_pairs.append(
                            (STAGE01_THIRD_LEG_MODEL, _dispatch_results[2])
                        )
                    if protection_pair is not None:
                        _detector_pairs.append(protection_pair)
                    combined = combine_detector_results(
                        _detector_pairs,
                        run_id=run_id,
                    )
                    if not combined.get("detectors_complete"):
                        combined["dual_review"] = {
                            "schema_version": 1,
                            "status": "skipped",
                            "reason": "partial_detector_failure",
                            "counts": {
                                "matches": 0, "extensions": 0, "new": 0,
                                "disputed": 0, "gap_findings": 0,
                            },
                            "gap_search": {
                                "enabled": bool(STAGE01_DUAL_GAP_SEARCH_ENABLED),
                                "performed": False,
                                "status": "skipped",
                                "findings_added": 0,
                            },
                        }
                        return combined
                    if not STAGE01_DUAL_REVIEW_ENABLED:
                        combined["dual_review"] = {
                            "schema_version": 1,
                            "status": "disabled",
                            "reviewer_model": STAGE01_DUAL_REVIEW_MODEL,
                            "counts": {
                                "matches": 0, "extensions": 0,
                                "new": len((combined.get("parsed") or {}).get("findings") or []),
                                "disputed": 0, "gap_findings": 0,
                            },
                            "gap_search": {
                                "enabled": bool(STAGE01_DUAL_GAP_SEARCH_ENABLED),
                                "performed": False,
                                "status": "disabled",
                                "findings_added": 0,
                            },
                        }
                        return combined

                    from backend.app.pipeline.stages.block_analysis.dual_review import (
                        fallback_dual_review,
                        review_dual_findings,
                    )

                    review_context, _ = build_effective_block_context(
                        block,
                        item["enrichment"],
                        page_text,
                        output_dir=output_dir,
                        routed_context=routed_context,
                        document_context=document_context,
                        document_type=document_type,
                        page_neighbors=page_neighbors,
                        include_absence_caveat=include_caveat,
                    )
                    try:
                        review = await review_dual_findings(
                            (combined.get("parsed") or {}).get("findings") or [],
                            block_id=str(block.get("block_id") or ""),
                            page=int(block.get("page") or 0),
                            block_context=review_context,
                            image_path=blocks_dir / block["file"],
                            reviewer_model=STAGE01_DUAL_REVIEW_MODEL,
                            run_id=run_id,
                            project_id=project_id,
                            timeout=timeout_s,
                            gap_search_enabled=STAGE01_DUAL_GAP_SEARCH_ENABLED,
                        )
                    except Exception as exc:  # fail-soft: raw detections survive
                        review = fallback_dual_review(
                            (combined.get("parsed") or {}).get("findings") or [],
                            reviewer_model=STAGE01_DUAL_REVIEW_MODEL,
                            run_id=run_id,
                            gap_search_enabled=STAGE01_DUAL_GAP_SEARCH_ENABLED,
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    combined["parsed"] = {"findings": review["findings"]}
                    combined["dual_review"] = review["report"]
                    combined["dual_review_raw_content"] = review.get("raw_content") or ""
                    combined["dual_review_calls"] = 1
                    combined["dual_review_input_tokens"] = int(review.get("input_tokens") or 0)
                    combined["dual_review_output_tokens"] = int(review.get("output_tokens") or 0)
                    combined["input_tokens"] += int(review.get("input_tokens") or 0)
                    combined["output_tokens"] += int(review.get("output_tokens") or 0)
                    combined["elapsed_ms"] += int(review.get("elapsed_ms") or 0)
                    return combined
                if use_codex_cli:
                    return await call_codex_for_block(
                        block, item["enrichment"], page_text, blocks_dir,
                        model=model, system_prompt=system_prompt, timeout=timeout_s,
                        reasoning_effort=reasoning_effort,
                        project_id=project_id, output_dir=output_dir,
                        routed_context=routed_context,
                        document_context=document_context,
                        document_type=document_type,
                        page_neighbors=page_neighbors,
                        include_absence_caveat=include_caveat,
                    )
                if use_provider_bridge:
                    # Мост воркера (11F). Стоит ПЕРВЫМ намеренно: когда
                    # исполнитель выписал привязку, ни одна другая нога не имеет
                    # права выполниться — все они идут мимо провайдерского слоя
                    # (OpenRouter по HTTPS, `codex exec`, прямой `claude -p`), то
                    # есть без авторизации по режиму, без журнала вызовов и без
                    # сверки фактической модели.
                    return await call_provider_for_block(
                        block, item["enrichment"], page_text, blocks_dir,
                        system_prompt=system_prompt, timeout=timeout_s,
                        output_dir=output_dir,
                        routed_context=routed_context,
                        document_context=document_context,
                        document_type=document_type,
                        page_neighbors=page_neighbors,
                        include_absence_caveat=include_caveat,
                    )
                if use_claude_cli:
                    sheet = sheet_for_page(graph, block["page"]) or ""
                    claude_page_text = (
                        page_text + "\n\n" + document_context
                        + f"\n\ndocument_type={document_type}"
                    )
                    if _pcfg2.STAGE01_PAGE_CONTEXT_ENABLED and page_neighbors:
                        claude_page_text += page_neighbors
                    if _pcfg2.STAGE01_PAGE_CONTEXT_ENABLED and include_caveat:
                        claude_page_text += _absence_caveat_section()
                    return await call_claude_cli_for_block(
                        block, item["enrichment"], claude_page_text, blocks_dir, sheet,
                        model=model, system_prompt=system_prompt, timeout=timeout_s,
                        clean_cwd=claude_clean_cwd,
                    )
                return await call_gpt_for_block(
                    client, block, item["enrichment"], page_text, blocks_dir,
                    api_key=api_key, model=model,
                    reasoning_effort=reasoning_effort,
                    max_tokens=max_tokens, system_prompt=system_prompt,
                    timeout=timeout_s,
                    project_id=project_id,
                    version_id=version_id,
                    job_id=job_id,
                    output_dir=output_dir,
                    routed_context=routed_context,
                    document_context=document_context,
                    document_type=document_type,
                    page_neighbors=page_neighbors,
                    include_absence_caveat=include_caveat,
                )

            try:
                res = await asyncio.wait_for(_dispatch(), timeout=block_hard_timeout_s)
            except asyncio.TimeoutError:
                # Backstop сработал: транспортный timeout обойдён (напр. trickle
                # keepalive), блок реально завис. wait_for уже отменил вложенный
                # вызов (httpx-запрос/subprocess убит через CancelledError).
                # Помечаем блок неудачным и продолжаем стадию, а не морозим батч.
                # detectors_failed заполняем ЯВНО: без этого блок, где сдохли
                # ВСЕ ноги, не вызывал остановку, а блок, потерявший одну, —
                # вызывал. Инверсия строгости: молчим на тяжёлом случае и
                # останавливаемся на лёгком.
                res = {
                    "ok": False,
                    "error": f"block_hard_timeout_{block_hard_timeout_s}s",
                    "parse_error": "block_hard_timeout",
                    "elapsed_ms": block_hard_timeout_s * 1000,
                    "detectors_ok": [],
                    "detectors_failed": list(configured_detector_models),
                    "partial": False,
                }

            if not use_dual and protection_pair is not None:
                # Single-model transports use the same arbitrary-length union
                # contract as ensemble mode; a failed LLM leg remains visible as
                # a partial result while exact deterministic findings survive.
                res = combine_detector_results(
                    [(model, res), protection_pair],
                    run_id=run_id,
                )

            # ── Нога ансамбля не ответила → останавливаем стадию ───────────
            # Признак уже посчитан в combine_detector_results: непустой
            # detectors_failed (он же partial). Городить новую детекцию не надо
            # — надо перестать игнорировать то, что и так известно.
            # У одномодельных режимов список пуст: ансамбля нет, падать нечему.
            if STAGE01_ABORT_ON_LEG_FAILURE_ENABLED:
                _failed_legs = list(res.get("detectors_failed") or [])
                if _failed_legs:
                    async with leg_failure_lock:
                        leg_failures.append({
                            "block_id": item["block_id"],
                            # sheet (номер из штампа) и page (страница PDF) —
                            # РАЗНЫЕ вещи (CLAUDE.md). Пишем оба: «лист 7» для
                            # номера страницы дезинформирует инженера.
                            "sheet": sheet_for_page(graph, block["page"]) or "",
                            "page": block["page"],
                            "failed_legs": _failed_legs,
                            "error": (
                                res.get("error") or res.get("parse_error") or ""
                            )[:500],
                        })
                        _reached = len(leg_failures) >= STAGE01_LEG_FAILURE_THRESHOLD
                    if _reached:
                        abort_event.set()

            # Publication is evidence-first, but no candidate is destroyed:
            # deferred_findings stays in the per-block audit record.
            if res.get("ok"):
                candidates = list((res.get("parsed") or {}).get("findings") or [])
                if symbol_evidence_enabled:
                    try:
                        from backend.app.pipeline.stages.block_analysis.finding_evidence_gate import (
                            observe_symbol_token_evidence,
                        )
                        candidates, observer_report = observe_symbol_token_evidence(
                            candidates,
                            vector_sources=symbol_vector_index,
                            document_graph=graph,
                            target_block_id=item["block_id"],
                            target_page=block["page"],
                            enabled=True,
                        )
                        res["parsed"] = {"findings": candidates}
                        res["finding_evidence_observer"] = observer_report
                    except Exception:
                        # Shadow observer is fail-soft by contract.
                        pass
                from backend.app.core import config as _gate_cfg
                if getattr(_gate_cfg, "STAGE01_EVIDENCE_GATE_ENABLED", True):
                    from backend.app.pipeline.stages.block_analysis.finding_evidence_gate import (
                        gate_findings,
                    )
                    _block_cap = getattr(_gate_cfg, "STAGE01_BLOCK_MAX_FINDINGS", 0) or None
                    published, deferred, gate_report = gate_findings(
                        candidates, max_published=_block_cap
                    )
                    res["parsed"] = {"findings": published}
                    res["deferred_findings"] = deferred
                    res["evidence_gate"] = gate_report
            res["document_retrieval"] = retrieval_receipt
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
                    "deferred_findings": len(res.get("deferred_findings") or []),
                    "input_tokens": res.get("input_tokens"),
                    "cached_input_tokens": res.get("cached_input_tokens"),
                    "output_tokens": res.get("output_tokens"),
                    "reasoning_tokens": res.get("reasoning_tokens"),
                    "elapsed_ms": res.get("elapsed_ms"),
                    "partial": bool(res.get("partial")),
                    "detectors_ok": res.get("detectors_ok") or [],
                    "detectors_failed": res.get("detectors_failed") or [],
                    "dual_review_status": (res.get("dual_review") or {}).get("status"),
                    "dual_review_counts": (res.get("dual_review") or {}).get("counts") or {},
                    "gap_findings": int(
                        ((res.get("dual_review") or {}).get("counts") or {}).get("gap_findings") or 0
                    ),
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
    # Аварийная остановка по выпавшей ноге — НЕ отмена пользователем: обёртка
    # этапа обязана развести их и вернуть fail с текстом, а не cancel.
    aborted_on_leg_failure = abort_event.is_set()

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
        analysis = {
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
        }
        if isinstance(res.get("evidence_gate"), dict):
            analysis["evidence_gate"] = res["evidence_gate"]
            analysis["deferred_findings"] = list(res.get("deferred_findings") or [])
        if isinstance(res.get("finding_evidence_observer"), dict):
            analysis["finding_evidence_observer"] = res["finding_evidence_observer"]
        if isinstance(res.get("document_retrieval"), dict):
            analysis["document_retrieval"] = res["document_retrieval"]
        if use_dual and isinstance(res.get("dual_review"), dict):
            analysis["dual_review"] = res["dual_review"]
        block_analyses.append(analysis)

    context_source_counts: dict[str, int] = {}
    for analysis in block_analyses:
        source = str(analysis.get("context_source") or "unknown")
        context_source_counts[source] = context_source_counts.get(source, 0) + 1

    dual_review_meta: dict[str, Any] | None = None
    if use_dual:
        reports = [
            r["result"].get("dual_review")
            for r in results
            if isinstance(r.get("result", {}).get("dual_review"), dict)
        ]
        aggregate_counts = {
            "matches": 0,
            "extensions": 0,
            "new": 0,
            "disputed": 0,
            "gap_findings": 0,
        }
        for report in reports:
            for key in aggregate_counts:
                aggregate_counts[key] += int((report.get("counts") or {}).get(key) or 0)
        dual_review_meta = {
            "schema_version": 1,
            "enabled": bool(STAGE01_DUAL_REVIEW_ENABLED),
            "reviewer_model": STAGE01_DUAL_REVIEW_MODEL,
            "gap_search_enabled": bool(STAGE01_DUAL_GAP_SEARCH_ENABLED),
            "blocks_reviewed": sum(1 for report in reports if report.get("status") == "ok"),
            "blocks_fallback": sum(1 for report in reports if report.get("status") == "fallback"),
            "blocks_skipped": sum(1 for report in reports if report.get("status") == "skipped"),
            "review_calls": sum(int(r["result"].get("dual_review_calls") or 0) for r in results),
            "gap_search_blocks": sum(
                1 for report in reports if (report.get("gap_search") or {}).get("performed")
            ),
            "counts": aggregate_counts,
        }

    gate_reports = [
        r["result"].get("evidence_gate")
        for r in results
        if isinstance(r.get("result", {}).get("evidence_gate"), dict)
    ]
    gate_reason_counts: dict[str, int] = {}
    for report in gate_reports:
        for reason, count in (report.get("reason_counts") or {}).items():
            gate_reason_counts[str(reason)] = gate_reason_counts.get(str(reason), 0) + int(count or 0)
    evidence_gate_meta = {
        "schema_version": 1,
        "enabled": bool(gate_reports),
        "blocks_gated": len(gate_reports),
        "candidates": sum(int(r.get("candidates") or 0) for r in gate_reports),
        "published": sum(int(r.get("published") or 0) for r in gate_reports),
        "deferred": sum(int(r.get("deferred") or 0) for r in gate_reports),
        "reason_counts": dict(sorted(gate_reason_counts.items())),
    }
    retrieval_reports = [
        r["result"].get("document_retrieval")
        for r in results
        if isinstance(r.get("result", {}).get("document_retrieval"), dict)
    ]
    document_retrieval_meta = {
        "scope": "all_document_vector_text_other_pages",
        "blocks_searched": len(retrieval_reports),
        "blocks_with_hits": sum(1 for r in retrieval_reports if r.get("status") == "hits"),
        "selected_hits": sum(int(r.get("selected_hits") or 0) for r in retrieval_reports),
    }

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
                for detector_model in configured_detector_models
            ],
            **({"dual_review": dual_review_meta} if dual_review_meta is not None else {}),
            "reasoning_effort": reasoning_effort,
            "extended_prompt": cats_loaded,
            "section": section,
            "document_type": document_type,
            "document_type_confidence": document_type_confidence,
            "evidence_gate": evidence_gate_meta,
            "document_retrieval": document_retrieval_meta,
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
            "aborted_on_leg_failure": aborted_on_leg_failure,
            "leg_failures": leg_failures,
        },
        "block_analyses": block_analyses,
    }

    if write_target and aborted_on_leg_failure:
        # НЕ перезаписываем итог стадии огрызком аварийно оборванного прогона.
        # Сценарий потери данных: полный аудит на 205 блоков есть → retry этапа
        # 01 → на 6-м блоке выпала нога → сюда приходит output_doc из 6 блоков.
        # Страховка .classic.bak.json не спасает: она пишется только `if not
        # bak.exists()`, то есть относится к самому первому прогону.
        # Результаты самого оборванного прогона не теряются — они лежат
        # поблочно в run_dir/block_<id>.json.
        write_target = False
        logging.getLogger(__name__).warning(
            "Stage 01 остановлен из-за выпавшей ноги: %s НЕ перезаписан, "
            "прежний результат сохранён.", target_path.name,
        )

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
        "document_type": document_type,
        "document_type_confidence": document_type_confidence,
        "evidence_gate": evidence_gate_meta,
        "document_retrieval": document_retrieval_meta,
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
        # Аварийная остановка по выпавшей ноге. leg_failures — что именно
        # упало: обёртка этапа собирает из этого текст для пользователя.
        "aborted_on_leg_failure": aborted_on_leg_failure,
        "leg_failures": leg_failures,
        "detector_calls": sum(
            len(r["result"].get("detector_results") or []) or 1
            for r in results
        ),
        "dual_review_calls": sum(
            int(r["result"].get("dual_review_calls") or 0) for r in results
        ),
        "api_calls_total": sum(
            (len(r["result"].get("detector_results") or []) or 1)
            + int(r["result"].get("dual_review_calls") or 0)
            for r in results
        ),
        **({"dual_review": dual_review_meta} if dual_review_meta is not None else {}),
        "totals": {
            "input_tokens": total_in,
            "output_tokens": total_out,
            "reasoning_tokens": total_reason,
            "findings": total_findings,
            "finding_candidates": evidence_gate_meta["candidates"],
            "findings_deferred": evidence_gate_meta["deferred"],
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
    else:
        # Последний рубеж: даже при прямом вызове стадии (минуя проверки готовности
        # в manager/prepare) анализ блоков не должен стартовать без картинок —
        # иначе каждый блок вернёт "PNG missing", а прогон будет выглядеть успешным.
        _ok, _missing = crops_materialized(index_path.parent)
        if _missing:
            reasons.append(
                f"_output/{STAGE02_BLOCKS_DIRNAME}: отсутствует {len(_missing)} PNG "
                f"из index.json (кропы не материализованы — нужен пере-кроп)"
            )
    context_validation = validate_block_context_summary(output_dir)
    if not context_validation.get("valid"):
        reasons.append(f"Векторные графы блоков не готовы: {context_validation.get('reason')}")
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
