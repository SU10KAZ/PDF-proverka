"""
Построение задач для Claude CLI из шаблонов.
Подготовка текста промтов с подстановкой плейсхолдеров и инъекцией дисциплин.
"""
import json
import re
from pathlib import Path
from typing import Optional

from webapp.config import (
    BASE_DIR, PROJECTS_DIR,
    NORM_VERIFY_TASK_TEMPLATE, NORM_FIX_TASK_TEMPLATE,
    OPTIMIZATION_TASK_TEMPLATE,
    TEXT_ANALYSIS_TASK_TEMPLATE, BLOCK_ANALYSIS_TASK_TEMPLATE,
    FINDINGS_MERGE_TASK_TEMPLATE,
)
from webapp.services.cli_utils import load_template
from webapp.services import discipline_service


# ─── Prompt Overrides ───

def _overrides_path(project_id: str) -> Path:
    return PROJECTS_DIR / project_id / "_output" / "prompt_overrides.json"


def _load_all_overrides(project_id: str) -> dict:
    p = _overrides_path(project_id)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _load_prompt_override(project_id: str, stage: str) -> str | None:
    """Загрузить кастомный промпт для этапа, если есть."""
    overrides = _load_all_overrides(project_id)
    val = overrides.get(stage)
    return val if val else None


def save_prompt_override(project_id: str, stage: str, content: str | None):
    """Сохранить или сбросить кастомный промпт."""
    overrides = _load_all_overrides(project_id)
    if content:
        overrides[stage] = content
    else:
        overrides.pop(stage, None)
    p = _overrides_path(project_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(overrides, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_project_info(project_id: str) -> dict:
    """Загрузить project_info.json."""
    info_path = PROJECTS_DIR / project_id / "project_info.json"
    if info_path.exists():
        try:
            return json.loads(info_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def get_resolved_prompts(project_id: str, discipline_override: str | None = None) -> list[dict]:
    """Получить все промпты (resolved) для отображения в UI.

    discipline_override — код дисциплины (EM, OV и т.д.) для подмены section.
    Если None — используется section из project_info.json.
    """
    project_info = _load_project_info(project_id)
    # Подмена дисциплины для предпросмотра промптов другой системы
    if discipline_override:
        project_info = {**project_info, "section": discipline_override}
    overrides = _load_all_overrides(project_id)

    stages = [
        ("text_analysis", "Анализ текста", lambda: prepare_text_analysis_task(project_info, project_id)),
        ("block_analysis", "Анализ блоков", lambda: _get_block_analysis_example(project_info, project_id)),
        ("findings_merge", "Свод замечаний", lambda: prepare_findings_merge_task(project_info, project_id)),
        ("optimization", "Оптимизация", lambda: prepare_optimization_task(project_info, project_id)),
    ]

    result = []
    for stage_key, label, resolver in stages:
        is_custom = stage_key in overrides and overrides[stage_key]
        try:
            content = overrides[stage_key] if is_custom else resolver()
        except Exception as e:
            content = f"[Ошибка формирования промпта: {e}]"
        result.append({
            "stage": stage_key,
            "label": label,
            "content": content,
            "is_custom": bool(is_custom),
            "char_count": len(content),
        })

    return result


# ─── Шаблоны (raw templates) ───

_STAGE_TEMPLATE_MAP = {
    "text_analysis": TEXT_ANALYSIS_TASK_TEMPLATE,
    "block_analysis": BLOCK_ANALYSIS_TASK_TEMPLATE,
    "findings_merge": FINDINGS_MERGE_TASK_TEMPLATE,
    "optimization": OPTIMIZATION_TASK_TEMPLATE,
}

_STAGE_LABELS = {
    "text_analysis": "Анализ текста",
    "block_analysis": "Анализ блоков",
    "findings_merge": "Свод замечаний",
    "optimization": "Оптимизация",
}


def get_template_prompts(discipline_code: str | None = None) -> list[dict]:
    """Получить сырые шаблоны с плейсхолдерами (без подстановки путей проекта).

    discipline_code — если указан, инъектировать дисциплину в плейсхолдеры.
    """
    result = []
    for stage_key, template_path in _STAGE_TEMPLATE_MAP.items():
        try:
            content = load_template(template_path)
            # Инъекция дисциплины если указана
            if discipline_code:
                profile = discipline_service.load_discipline(discipline_code)
                content = discipline_service.inject_discipline(content, profile)
        except Exception as e:
            content = f"[Ошибка загрузки шаблона: {e}]"
        result.append({
            "stage": stage_key,
            "label": _STAGE_LABELS.get(stage_key, stage_key),
            "content": content,
            "char_count": len(content),
        })
    return result


def save_template(stage: str, content: str):
    """Сохранить шаблон промпта в .claude/*.md файл."""
    template_path = _STAGE_TEMPLATE_MAP.get(stage)
    if not template_path:
        raise ValueError(f"Неизвестный этап: {stage}")
    Path(template_path).write_text(content, encoding="utf-8")


def _get_block_analysis_example(project_info: dict, project_id: str) -> str:
    """Пример промпта для анализа блоков (первый пакет или шаблон)."""
    batches_file = PROJECTS_DIR / project_id / "_output" / "block_batches.json"
    if batches_file.exists():
        try:
            data = json.loads(batches_file.read_text(encoding="utf-8"))
            batches = data.get("batches", [])
            if batches:
                return prepare_block_batch_task(
                    batches[0], project_info, project_id, len(batches)
                )
        except Exception:
            pass
    # Если батчей нет — вернуть шаблон с незаполненными batch-плейсхолдерами
    return prepare_block_batch_task(
        {"batch_id": 1, "blocks": []}, project_info, project_id, 1
    )


def _inject_discipline(template: str, project_info: dict) -> str:
    """Инъекция дисциплинарного контента в шаблон."""
    section = (project_info or {}).get("section", "EM")
    profile = discipline_service.load_discipline(section)
    return discipline_service.inject_discipline(template, profile)


def _get_md_file_path(project_info: dict, project_id: str) -> str:
    """Получить путь к MD-файлу проекта."""
    md_file = project_info.get("md_file")
    if md_file:
        return str(PROJECTS_DIR / project_id / md_file)
    return "(нет)"


def _get_project_paths(project_id: str) -> tuple[str, str]:
    """Получить пути к проекту и выходной папке."""
    return (
        str(PROJECTS_DIR / project_id),
        str(PROJECTS_DIR / project_id / "_output"),
    )


# ─── Legacy stubs (для обратной совместимости с claude_runner.py) ───

def prepare_tile_batch_task(*args, **kwargs) -> str:
    """Legacy stub — тайловый пайплайн заменён на блочный."""
    return prepare_block_batch_task(*args, **kwargs)

def prepare_main_audit_task(project_id: str, project_info: dict = None, **kwargs) -> str:
    """Legacy stub — основной аудит заменён на конвейер."""
    return prepare_text_analysis_task(project_id, project_info)

def prepare_triage_task(project_id: str, project_info: dict = None, **kwargs) -> str:
    """Legacy stub — триаж теперь часть text_analysis."""
    return prepare_text_analysis_task(project_id, project_info)

def prepare_smart_merge_task(project_id: str, project_info: dict = None, **kwargs) -> str:
    """Legacy stub — smart merge заменён на findings_merge."""
    return prepare_findings_merge_task(project_id, project_info)


# ─── Верификация нормативных ссылок ───

def prepare_norm_verify_task(
    norms_list_text: str,
    project_id: str,
    project_info: Optional[dict] = None,
) -> str:
    """Подготовить задачу для верификации нормативных ссылок."""
    template = load_template(NORM_VERIFY_TASK_TEMPLATE)
    template = _inject_discipline(template, project_info or {})

    project_path, _ = _get_project_paths(project_id)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{PROJECT_PATH}", project_path)
        .replace("{BASE_DIR}", str(BASE_DIR))
        .replace("{NORMS_LIST}", norms_list_text)
    )
    return task


def prepare_norm_fix_task(
    findings_to_fix_text: str,
    project_id: str,
    project_info: Optional[dict] = None,
) -> str:
    """Подготовить задачу для пересмотра замечаний с устаревшими нормами."""
    template = load_template(NORM_FIX_TASK_TEMPLATE)
    template = _inject_discipline(template, project_info or {})

    project_path, _ = _get_project_paths(project_id)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{PROJECT_PATH}", project_path)
        .replace("{BASE_DIR}", str(BASE_DIR))
        .replace("{FINDINGS_TO_FIX}", findings_to_fix_text)
    )
    return task


# ─── Анализ текста ───

def prepare_text_analysis_task(
    project_info: dict,
    project_id: str,
) -> str:
    """Подготовить задачу для текстового анализа MD-файла."""
    override = _load_prompt_override(project_id, "text_analysis")
    if override:
        return override
    template = load_template(TEXT_ANALYSIS_TASK_TEMPLATE)

    _, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{MD_FILE_PATH}", md_file_path)
    )
    return task


# ─── Извлечение [IMAGE] контекста из MD ───

def _extract_image_context_for_blocks(md_file_path: str, block_ids: list[str]) -> str:
    """Извлечь из MD-файла секции [IMAGE] только для указанных block_id.

    Вместо того чтобы Claude CLI читал весь MD (100-500 KB),
    мы извлекаем только релевантные секции (~2-5 KB на пакет).
    """
    md_path = Path(md_file_path)
    if not md_path.exists() or md_file_path == "(нет)":
        return ""

    try:
        content = md_path.read_text(encoding="utf-8")
    except OSError:
        return ""

    block_ids_set = set(block_ids)
    if not block_ids_set:
        return ""

    # Парсим MD: ищем блоки ### BLOCK [IMAGE]: <block_id>
    # Каждый блок заканчивается перед следующим ### BLOCK или ## СТРАНИЦА
    sections = []
    current_page_header = ""

    for line in content.split("\n"):
        # Трекинг текущей страницы
        if line.startswith("## СТРАНИЦА "):
            current_page_header = line
            continue

        # Начало IMAGE-блока
        if line.startswith("### BLOCK [IMAGE]:"):
            # Извлекаем block_id
            bid = line.split(":", 1)[-1].strip()
            if bid in block_ids_set:
                sections.append({
                    "block_id": bid,
                    "page_header": current_page_header,
                    "lines": [line],
                    "active": True,
                })
            continue

        # Начало другого блока — закрываем активный
        if line.startswith("### BLOCK ") or line.startswith("## СТРАНИЦА "):
            for s in sections:
                s["active"] = False
            if line.startswith("## СТРАНИЦА "):
                current_page_header = line
            continue

        # Добавляем строки к активным секциям
        for s in sections:
            if s.get("active"):
                s["lines"].append(line)

    if not sections:
        return ""

    # Формируем компактный контекст
    parts = []
    for s in sections:
        text = "\n".join(s["lines"]).strip()
        if text:
            parts.append(text)

    return "\n\n".join(parts)


# ─── Анализ пакета image-блоков (OCR-пайплайн) ───

def prepare_block_batch_task(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
) -> str:
    """Подготовить задачу для одного пакета image-блоков."""
    override = _load_prompt_override(project_id, "block_analysis")
    if override:
        return override
    template = load_template(BLOCK_ANALYSIS_TASK_TEMPLATE)

    batch_id = batch_data["batch_id"]
    blocks = batch_data.get("blocks", [])

    # Формируем список блоков
    block_lines = []
    for block in blocks:
        block_path = str(
            PROJECTS_DIR / project_id / "_output" / "blocks" / block["file"]
        )
        block_lines.append(
            f"- `{block_path}` (стр. {block.get('page', '?')}, "
            f"block_id: {block['block_id']}, "
            f"OCR: {block.get('ocr_label', 'image')})"
        )

    _, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    # Извлекаем inline MD-контекст только для блоков этого пакета
    batch_block_ids = [b["block_id"] for b in blocks]
    md_context = _extract_image_context_for_blocks(md_file_path, batch_block_ids)

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("{BATCH_ID}", str(batch_id))
        .replace("{BATCH_ID_PADDED}", f"{batch_id:03d}")
        .replace("{TOTAL_BATCHES}", str(total_batches))
        .replace("{PROJECT_ID}", project_id)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{BLOCK_COUNT}", str(len(blocks)))
        .replace("{BLOCK_LIST}", "\n".join(block_lines))
        .replace("{MD_FILE_PATH}", md_file_path)
        .replace("{BLOCK_MD_CONTEXT}", md_context if md_context else "(нет IMAGE-описаний для блоков этого пакета)")
    )
    return task


# ─── Компактификация данных для findings_merge ───

def _prepare_compact_findings_input(project_id: str) -> Path | None:
    """Создать компактный JSON из 01+02 для findings_merge.

    Убирает: полные block summaries, дублирующий контент.
    Оставляет: findings, key_values, project_params, verified items.

    Типичное сжатие: 800 KB → 100-200 KB (4-8x меньше).
    """
    output_dir = PROJECTS_DIR / project_id / "_output"
    stage01 = output_dir / "01_text_analysis.json"
    stage02 = output_dir / "02_blocks_analysis.json"
    compact_path = output_dir / "_findings_compact.json"

    compact = {}

    # Из 01: project_params, normative_refs, text_findings
    if stage01.exists():
        try:
            data01 = json.loads(stage01.read_text(encoding="utf-8"))
            compact["project_params"] = data01.get("project_params", {})
            compact["normative_refs_found"] = data01.get("normative_refs_found", [])
            compact["text_findings"] = data01.get("text_findings", [])
            # Информация о пропущенных блоках (для полноты картины)
            skipped = data01.get("blocks_skipped", [])
            compact["blocks_skipped_count"] = len(skipped)
        except (json.JSONDecodeError, OSError):
            return None

    # Из 02: preliminary_findings, items_verified, key_values (без полных summary)
    if stage02.exists():
        try:
            data02 = json.loads(stage02.read_text(encoding="utf-8"))

            # Preliminary findings — полностью
            compact["preliminary_findings"] = data02.get("preliminary_findings", [])

            # Items verified — полностью
            compact["items_verified_from_stage_01"] = data02.get(
                "items_verified_from_stage_01", []
            )

            # Из block_analyses: только block_id, page, sheet_type, key_values_read, findings
            # Без полных summary и label (экономия ~60% объёма 02)
            block_analyses = data02.get("block_analyses", [])
            compact["blocks_compact"] = [
                {
                    "block_id": ba.get("block_id", ""),
                    "page": ba.get("page", 0),
                    "sheet_type": ba.get("sheet_type", ""),
                    "key_values_read": ba.get("key_values_read", []),
                    "findings_count": len(ba.get("findings", [])),
                    # findings уже в preliminary_findings — не дублируем
                }
                for ba in block_analyses
            ]
            compact["total_blocks_analyzed"] = len(block_analyses)
        except (json.JSONDecodeError, OSError):
            return None
    else:
        compact["preliminary_findings"] = []
        compact["blocks_compact"] = []
        compact["total_blocks_analyzed"] = 0

    # Записываем компактный файл
    try:
        compact_path.write_text(
            json.dumps(compact, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return compact_path
    except OSError:
        return None


# ─── Свод замечаний (OCR-пайплайн) ───

def prepare_findings_merge_task(
    project_info: dict,
    project_id: str,
) -> str:
    """Подготовить задачу для свода замечаний из текста + блоков."""
    override = _load_prompt_override(project_id, "findings_merge")
    if override:
        return override
    template = load_template(FINDINGS_MERGE_TASK_TEMPLATE)

    _, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    # Создаём компактный input
    compact_path = _prepare_compact_findings_input(project_id)

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{MD_FILE_PATH}", md_file_path)
    )

    # Если компактный файл создан — заменяем ссылки на полные файлы
    if compact_path and compact_path.exists():
        task = task.replace(
            f"`{output_path}/01_text_analysis.json`",
            f"`{compact_path}` *(компактная версия)*",
        )
        task = task.replace(
            f"`{output_path}/02_blocks_analysis.json`",
            f"`{compact_path}` *(уже включено выше)*",
        )

    return task


# ─── Оптимизация проектных решений ───

def prepare_optimization_task(
    project_info: dict,
    project_id: str,
) -> str:
    """Подготовить задачу для анализа оптимизации проектной документации."""
    override = _load_prompt_override(project_id, "optimization")
    if override:
        return override
    template = load_template(OPTIMIZATION_TASK_TEMPLATE)

    _, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{MD_FILE_PATH}", md_file_path)
    )
    return task
