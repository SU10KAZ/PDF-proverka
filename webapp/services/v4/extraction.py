"""
extraction.py — Stage 1 v4 pipeline.

Запускает LLM extraction для одного batch image-блоков → typed_facts_batch_NNN.json.

Поддерживает два провайдера:
- OpenRouter (GPT-5.4, Gemini 3.1 Pro) — через webapp.services.llm_runner
- Claude CLI (Opus, Sonnet) — через webapp.services.claude_runner._run_cli

Выбор провайдера определяется моделью, настроенной для stage "block_batch"
в webapp.config.STAGE_MODEL_CONFIG. Если модель начинается с "claude-" →
CLI, иначе → OpenRouter.
"""
from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Awaitable, Callable, Optional

log = logging.getLogger(__name__)


_PROMPTS_DIR = Path(__file__).resolve().parents[3] / "prompts" / "v4"
TEMPLATE_PATH = _PROMPTS_DIR / "v4_extraction_template.md"


def _render_v4_config_sections(config: dict) -> dict:
    """Сгенерировать текстовые секции из v4_config для подстановки в template."""
    ext = config.get("extraction", {})

    # Priority tasks
    tasks_lines = []
    for task in ext.get("priority_tasks", []):
        critical = " (КРИТИЧНО)" if task.get("critical") else ""
        tasks_lines.append(f"### Задача {task.get('id', '?')}: {task.get('title', '')}{critical}\n")
        tasks_lines.append(f"{task.get('description', '')}\n")
        # Few-shot examples
        for ex in task.get("examples", []):
            tasks_lines.append(f"\n```\n\"{ex['input']}\"\n→ {ex['output']}\n```\n")
        # Key phrases
        for kp in task.get("key_phrases", []):
            tasks_lines.append(f"- \"{kp}\"\n")
        tasks_lines.append("")
    priority_tasks = "\n".join(tasks_lines) if tasks_lines else "Извлеки все видимые сущности с их атрибутами."

    # Scope
    scope_label = ext.get("scope_label", "Все разделы")
    included = ext.get("scope_included", ["all"])
    excluded = ext.get("scope_excluded", [])
    scope_text = f"**В scope:** {scope_label}\n\n"
    if excluded:
        scope_text += f"**Вне scope (помечай `out_of_scope_ref: true`):** {', '.join(excluded)}"

    # Entity types enum
    entity_types = ext.get("entity_types", ["item", "spec_row", "note", "room", "other"])
    entity_types_enum = " | ".join(entity_types)
    entity_types_list = "\n".join(f"- `{et}`" for et in entity_types)

    # Exact keys mapping: per-entity example.
    # Пишем ТОЛЬКО релевантный ключ для каждого entity_type, чтобы модель не
    # копировала 6 лишних null-полей в каждый mention.
    key_fields = ext.get("exact_key_fields", {})
    if key_fields:
        mapping_lines = [
            f'- `{et}` → `{{ "{field}": "..." }}`'
            for et, field in key_fields.items()
        ]
        exact_keys_example = (
            "Мэппинг `entity_type` → релевантное поле в `exact_keys` "
            "(включай только это поле, без прочих null):\n" + "\n".join(mapping_lines)
        )
    else:
        exact_keys_example = '{ "item_id": "..." }'

    # Attributes list
    attr_enum = ext.get("attribute_enum", {})
    attr_lines = []
    for et, attrs in attr_enum.items():
        attr_lines.append(f"\n**Для `{et}`:**")
        for a in attrs:
            attr_lines.append(f"- `{a}`")
    attributes_list = "\n".join(attr_lines) if attr_lines else "Любые атрибуты, которые видишь на чертеже."

    return {
        "{V4_PRIORITY_TASKS}": priority_tasks,
        "{V4_SCOPE}": scope_text,
        "{V4_SCOPE_INCLUDED}": json.dumps(included, ensure_ascii=False),
        "{V4_SCOPE_EXCLUDED}": json.dumps(excluded, ensure_ascii=False),
        "{V4_ENTITY_TYPES_ENUM}": entity_types_enum,
        "{V4_ENTITY_TYPES_LIST}": entity_types_list,
        "{V4_EXACT_KEYS_EXAMPLE}": exact_keys_example,
        "{V4_ATTRIBUTES_LIST}": attributes_list,
    }


def _build_prompt(
    batch_id: int,
    total_batches: int,
    project_id: str,
    section: str,
    blocks: list[dict],
    block_lines: list[str],
    md_context: str,
    output_dir: Path,
    config: dict,
) -> str:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    for placeholder, value in _render_v4_config_sections(config).items():
        template = template.replace(placeholder, value)

    return (
        template
        .replace("{BATCH_ID}", str(batch_id))
        .replace("{BATCH_ID_PADDED}", f"{batch_id:03d}")
        .replace("{TOTAL_BATCHES}", str(total_batches))
        .replace("{PROJECT_ID}", project_id)
        .replace("{SECTION}", section)
        .replace("{BLOCK_COUNT}", str(len(blocks)))
        .replace("{BLOCK_LIST}", "\n".join(block_lines))
        .replace("{BLOCK_MD_CONTEXT}", md_context or "(no context)")
        .replace("{OUTPUT_PATH}", str(output_dir).replace("\\", "/"))
    )


def _strip_cli_instructions(prompt: str) -> str:
    """Удалить инструкции про Read/Write tool — для OpenRouter пути."""
    cli_patterns = [
        r"^.*Read tool.*$",
        r"^.*Write tool.*$",
        r"^.*WRITE via Write.*$",
        r"^.*прочитать КАЖДЫЙ через Read.*$",
        r"^.*Пиши JSON через Write.*$",
    ]
    for pat in cli_patterns:
        prompt = re.sub(pat, "", prompt, flags=re.IGNORECASE | re.MULTILINE)
    return prompt


async def run_extraction_batch(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
) -> tuple[int, str, dict]:
    """Запустить v4 extraction для одного batch.

    Args:
        batch_data: dict с batch_id и blocks[].
        project_info: project_info.json содержимое.
        project_id: ID проекта.
        total_batches: всего batch'ей.
        on_output: callback для live-log.

    Returns:
        (exit_code, combined_output, {"typed_facts_path": str, "mentions": int})
        exit_code == 0 на успех, иначе ошибка.
    """
    from webapp.config import get_stage_model, is_claude_stage
    from webapp.services.project_service import resolve_project_dir

    batch_id = batch_data.get("batch_id", 0)
    blocks = batch_data.get("blocks", [])
    section = (project_info or {}).get("section", "EOM")

    out_dir = resolve_project_dir(project_id) / "_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Контекст и block_lines — те же вспомогательные функции, что и в legacy
    from webapp.services.task_builder import (
        _load_document_graph,
        _build_structured_block_context,
        _get_md_file_path,
        _extract_page_to_sheet_map,
    )

    md_file_path = _get_md_file_path(project_info, project_id)
    page_to_sheet = _extract_page_to_sheet_map(md_file_path)

    block_ids = [b["block_id"] for b in blocks]
    block_pages = [b.get("page") for b in blocks if b.get("page")]

    graph = _load_document_graph(project_id)
    if graph:
        md_context = _build_structured_block_context(graph, block_ids, block_pages)
    else:
        md_context = "(document_graph.json not available)"

    # Определяем провайдера по модели
    model = get_stage_model("block_batch")
    use_cli = is_claude_stage("block_batch")

    log.info(
        "[v4] Batch %d: extraction starting (model=%s, provider=%s, blocks=%d)",
        batch_id, model, "cli" if use_cli else "openrouter", len(blocks),
    )

    # Формируем block_lines: формат зависит от провайдера
    if use_cli:
        # CLI читает файлы напрямую → нужны пути к PNG (скрываем от LLM-вывода)
        block_lines = []
        for block in blocks:
            block_path = str(
                resolve_project_dir(project_id) / "_output" / "blocks" / block["file"]
            )
            pdf_page = block.get("page", "?")
            sheet_info = page_to_sheet.get(pdf_page, "")
            sheet_suffix = f", Лист {sheet_info}" if sheet_info else ""
            block_lines.append(
                f"- `{block_path}` (стр. {pdf_page}{sheet_suffix}, "
                f"block_id: {block['block_id']}, "
                f"OCR: {block.get('ocr_label', 'image')})"
            )
    else:
        # OpenRouter передаёт изображения через messages — путь не нужен
        block_lines = []
        for block in blocks:
            pdf_page = block.get("page", "?")
            sheet_info = page_to_sheet.get(pdf_page, "")
            sheet_suffix = f", Лист {sheet_info}" if sheet_info else ""
            block_lines.append(
                f"- block_id: {block['block_id']}, стр. {pdf_page}{sheet_suffix}, "
                f"OCR: {block.get('ocr_label', 'image')}"
            )

    # Загружаем дисциплинарный v4 config для extraction template
    from webapp.services.v4.pipeline import load_v4_config
    v4_config = load_v4_config(section)

    extraction_prompt = _build_prompt(
        batch_id=batch_id,
        total_batches=total_batches,
        project_id=project_id,
        section=section,
        blocks=blocks,
        block_lines=block_lines,
        md_context=md_context,
        output_dir=out_dir,
        config=v4_config,
    )

    out_path = out_dir / f"typed_facts_batch_{batch_id:03d}.json"

    if use_cli:
        return await _run_cli_extraction(
            extraction_prompt, model, project_id, batch_id,
            out_path, on_output,
        )
    else:
        return await _run_openrouter_extraction(
            extraction_prompt, project_info, project_id, batch_id,
            total_batches, batch_data, out_path, on_output,
        )


async def _run_cli_extraction(
    prompt: str,
    model: str,
    project_id: str,
    batch_id: int,
    out_path: Path,
    on_output,
) -> tuple[int, str, dict]:
    """Запустить extraction через Claude CLI. CLI сам читает PNG и пишет JSON."""
    from webapp.config import BLOCK_ANALYSIS_TOOLS, CLAUDE_BLOCK_ANALYSIS_TIMEOUT
    from webapp.services.claude_runner import _run_cli

    start = time.monotonic()
    try:
        exit_code, combined, cli_result = await _run_cli(
            prompt,
            BLOCK_ANALYSIS_TOOLS,
            CLAUDE_BLOCK_ANALYSIS_TIMEOUT,
            on_output=on_output,
            stage=f"v4_extraction_{batch_id:03d}",
            project_id=project_id,
            model=model,
        )
    except Exception as e:
        log.error("[v4] Batch %d: CLI call failed: %s", batch_id, e)
        return (1, str(e), {})

    duration = time.monotonic() - start

    if out_path.exists():
        try:
            data = json.loads(out_path.read_text(encoding="utf-8"))
            num_mentions = len(data.get("entity_mentions", []))
            log.info(
                "[v4] Batch %d: CLI wrote %s (%.0fs, exit=%s, mentions=%d)",
                batch_id, out_path.name, duration, exit_code, num_mentions,
            )
            return (0, combined, {
                "typed_facts_path": str(out_path),
                "mentions": num_mentions,
                "duration_s": duration,
            })
        except Exception as e:
            log.error("[v4] Batch %d: CLI wrote broken JSON: %s", batch_id, e)
            return (1, combined, {})
    else:
        log.error(
            "[v4] Batch %d: CLI did NOT write %s (exit=%s, %.0fs)",
            batch_id, out_path.name, exit_code, duration,
        )
        return (1, combined, {})


async def _run_openrouter_extraction(
    prompt: str,
    project_info: dict,
    project_id: str,
    batch_id: int,
    total_batches: int,
    batch_data: dict,
    out_path: Path,
    on_output,
) -> tuple[int, str, dict]:
    """Запустить extraction через OpenRouter (GPT/Gemini).
    Использует prompt_builder для формирования messages с изображениями.
    """
    from webapp.services import prompt_builder
    from webapp.services.llm_runner import run_llm

    # Убираем инструкции про Read/Write tool — они для CLI
    prompt_clean = _strip_cli_instructions(prompt)

    try:
        messages = prompt_builder.build_block_batch_messages(
            batch_data, project_info, project_id, total_batches
        )
    except Exception as e:
        log.error("[v4] Batch %d: failed to build messages: %s", batch_id, e)
        return (1, str(e), {})

    # Заменяем system сообщение на v4 prompt
    messages[0] = {"role": "system", "content": prompt_clean}

    start = time.monotonic()
    try:
        result = await run_llm(
            stage=f"v4_extraction_{batch_id:03d}",
            messages=messages,
            timeout=900,
            response_format=None,  # Free-form — Gemini иногда валит SDK при json_object
        )
    except Exception as e:
        log.error("[v4] Batch %d: LLM call failed: %s", batch_id, e)
        return (1, str(e), {})

    duration = time.monotonic() - start

    if result.is_error:
        log.error("[v4] Batch %d: LLM error: %s", batch_id, result.error_message)
        return (1, result.error_message or "LLM error", {})

    if not result.json_data:
        # Для диагностики: сохраняем сырой ответ LLM в .raw.txt рядом с output
        raw_text = result.text or ""
        raw_path = out_path.with_suffix(".raw.txt")
        try:
            raw_path.write_text(raw_text[:50000], encoding="utf-8")
        except Exception:
            pass
        err_hint = (raw_text[:200] or "empty response").replace("\n", " ")
        log.error(
            "[v4] Batch %d: LLM returned no JSON (cost=$%s, raw_len=%d, hint: %s)",
            batch_id, result.cost_usd, len(raw_text), err_hint,
        )
        return (1, f"no JSON from LLM: {err_hint}", {
            "cost_usd": result.cost_usd,
            "input_tokens": result.input_tokens,
            "output_tokens": result.output_tokens,
        })

    # Валидация: должен быть entity_mentions (даже если пустой список)
    if "entity_mentions" not in result.json_data:
        err_msg = f"JSON не содержит entity_mentions (keys={list(result.json_data.keys())[:5]})"
        log.error("[v4] Batch %d: %s", batch_id, err_msg)
        return (1, err_msg, {
            "cost_usd": result.cost_usd,
            "input_tokens": result.input_tokens,
            "output_tokens": result.output_tokens,
        })

    out_path.write_text(
        json.dumps(result.json_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    num_mentions = len(result.json_data.get("entity_mentions", []))
    log.info(
        "[v4] Batch %d: saved %s (%.0fs, $%.2f, %d→%d tokens, mentions=%d)",
        batch_id, out_path.name, duration, result.cost_usd,
        result.input_tokens, result.output_tokens, num_mentions,
    )

    return (0, result.text or "", {
        "typed_facts_path": str(out_path),
        "mentions": num_mentions,
        "duration_s": duration,
        "cost_usd": result.cost_usd,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
    })
