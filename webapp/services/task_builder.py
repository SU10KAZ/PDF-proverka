"""
Построение задач для Claude CLI из шаблонов.
Подготовка текста промтов с подстановкой плейсхолдеров и инъекцией дисциплин.
"""
import json
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

from webapp.config import (
    BASE_DIR, PROJECTS_DIR,
    NORM_VERIFY_TASK_TEMPLATE, NORM_FIX_TASK_TEMPLATE,
    OPTIMIZATION_TASK_TEMPLATE,
    TEXT_ANALYSIS_TASK_TEMPLATE, BLOCK_ANALYSIS_TASK_TEMPLATE,
    FINDINGS_MERGE_TASK_TEMPLATE,
    FINDINGS_CRITIC_TASK_TEMPLATE, FINDINGS_CORRECTOR_TASK_TEMPLATE,
    OPTIMIZATION_CRITIC_TASK_TEMPLATE, OPTIMIZATION_CORRECTOR_TASK_TEMPLATE,
)
from webapp.services.cli_utils import load_template
from webapp.services import discipline_service
from webapp.services.project_service import resolve_project_dir


# ─── Prompt Overrides ───

def _overrides_path(project_id: str) -> Path:
    return resolve_project_dir(project_id) / "_output" / "prompt_overrides.json"


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
    info_path = resolve_project_dir(project_id) / "project_info.json"
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
    batches_file = resolve_project_dir(project_id) / "_output" / "block_batches.json"
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
        return str(resolve_project_dir(project_id) / md_file)
    return "(нет)"


def _get_project_paths(project_id: str) -> tuple[str, str]:
    """Получить пути к проекту и выходной папке."""
    return (
        str(resolve_project_dir(project_id)),
        str(resolve_project_dir(project_id) / "_output"),
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
    llm_out_filename: str = "norm_checks_llm.json",
) -> str:
    """Подготовить задачу для верификации нормативных ссылок.

    norms_list_text — отформатированная LLM-работа (unknown нормы + параграфы).
    Подставляется в плейсхолдер {LLM_WORK}.
    llm_out_filename — имя выходного файла LLM (для chunked mode).
    """
    template = load_template(NORM_VERIFY_TASK_TEMPLATE)
    template = _inject_discipline(template, project_info or {})

    project_path, _ = _get_project_paths(project_id)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{PROJECT_PATH}", project_path)
        .replace("{BASE_DIR}", str(BASE_DIR))
        .replace("{LLM_WORK}", norms_list_text)
        .replace("{NORMS_LIST}", norms_list_text)
        .replace("norm_checks_llm.json", llm_out_filename)
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


# ─── Извлечение контекста страниц из MD ───

def _extract_page_context_for_blocks(
    md_file_path: str,
    block_ids: list[str],
    block_pages: list[int],
) -> str:
    """Извлечь из MD-файла полный контекст страниц для блоков пакета.

    Для каждой страницы, на которой есть блоки пакета, извлекает:
    1. Метаданные страницы (лист, наименование листа)
    2. Все [TEXT] блоки (текст, таблицы, примечания)
    3. [IMAGE] описания только для блоков этого пакета

    Это даёт Claude полный контекст: что написано рядом с чертежом.
    Типичный объём: 3-10 KB на пакет (vs 100-500 KB за весь MD).
    """
    md_path = Path(md_file_path)
    if not md_path.exists() or md_file_path == "(нет)":
        return ""

    try:
        content = md_path.read_text(encoding="utf-8")
    except OSError:
        return ""

    block_ids_set = set(block_ids)
    target_pages = set(block_pages)
    if not block_ids_set and not target_pages:
        return ""

    # Два режима релевантности страниц:
    # 1) target_pages задан → фильтр по номерам страниц (основной путь)
    # 2) target_pages пуст → "ленивый" режим: страница релевантна,
    #    если на ней встретится IMAGE-блок из block_ids_set
    filter_by_pages = bool(target_pages)

    # Парсим MD постранично
    # Структура: ## СТРАНИЦА N → метаданные → ### BLOCK [TEXT/IMAGE]: ID
    pages: dict[int, dict] = {}  # page_num → {meta, texts, images}
    current_page_num = 0
    current_page_relevant = False
    current_block_type = None  # "text" | "image" | None
    current_block_id = ""
    current_block_lines: list[str] = []
    current_block_relevant = False  # для IMAGE — только если block_id в пакете
    # Буфер для ленивого режима: накапливаем блоки до определения релевантности
    _lazy_buffer: list[str] = []  # метаданные + TEXT-блоки до первого совпадения

    def _ensure_page():
        """Создать запись страницы если её ещё нет."""
        if current_page_num not in pages:
            pages[current_page_num] = {
                "num": current_page_num,
                "meta": [],
                "texts": [],
                "images": [],
            }

    def _flush_block():
        """Сохранить накопленный блок в структуру страницы."""
        nonlocal current_block_type, current_block_lines, current_block_relevant
        if not current_block_type:
            current_block_lines = []
            return

        text = "\n".join(current_block_lines).strip()
        if not text:
            current_block_type = None
            current_block_lines = []
            return

        if current_page_num in pages:
            # Страница уже активирована — пишем напрямую
            page = pages[current_page_num]
            if current_block_type == "text":
                if "[Ошибка" not in text and "*(нет данных)*" not in text:
                    page["texts"].append(text)
            elif current_block_type == "image" and current_block_relevant:
                page["images"].append(text)
        elif not filter_by_pages and current_block_type == "text":
            # Ленивый режим: страница ещё не активирована — TEXT в буфер
            if "[Ошибка" not in text and "*(нет данных)*" not in text:
                _lazy_buffer.append(text)

        current_block_type = None
        current_block_lines = []
        current_block_relevant = False

    def _flush_lazy_buffer():
        """Перенести буфер ленивого режима в структуру страницы."""
        nonlocal _lazy_buffer
        if not _lazy_buffer or current_page_num not in pages:
            _lazy_buffer = []
            return
        page = pages[current_page_num]
        for item in _lazy_buffer:
            if item.startswith("**Лист:**") or item.startswith("**Наименование листа:**"):
                page["meta"].append(item)
            else:
                page["texts"].append(item)
        _lazy_buffer = []

    for line in content.split("\n"):
        # Начало новой страницы
        if line.startswith("## СТРАНИЦА "):
            _flush_block()
            try:
                current_page_num = int(line.split("СТРАНИЦА")[1].strip())
            except (ValueError, IndexError):
                current_page_num = 0
            if filter_by_pages:
                current_page_relevant = current_page_num in target_pages
                if current_page_relevant:
                    _ensure_page()
            else:
                # Ленивый режим: пока не знаем, релевантна ли страница
                current_page_relevant = True  # парсим всё, но в буфер
                _lazy_buffer = []
            continue

        if not current_page_relevant:
            continue

        # Метаданные страницы (Лист, Наименование листа)
        if line.startswith("**Лист:**") or line.startswith("**Наименование листа:**"):
            if filter_by_pages:
                if current_page_num in pages:
                    pages[current_page_num]["meta"].append(line)
            else:
                _lazy_buffer.append(line)
            continue

        # Начало TEXT-блока
        if line.startswith("### BLOCK [TEXT]:"):
            _flush_block()
            current_block_type = "text"
            current_block_id = line.split(":", 1)[-1].strip()
            current_block_lines = [line]
            continue

        # Начало IMAGE-блока
        if line.startswith("### BLOCK [IMAGE]:"):
            _flush_block()
            bid = line.split(":", 1)[-1].strip()
            current_block_type = "image"
            current_block_id = bid
            current_block_relevant = bid in block_ids_set
            # Ленивый режим: при первом совпадении — активируем страницу
            if not filter_by_pages and current_block_relevant:
                _ensure_page()
                _flush_lazy_buffer()
            current_block_lines = [line]
            continue

        # Начало неизвестного блока — закрываем текущий
        if line.startswith("### BLOCK "):
            _flush_block()
            continue

        # Накапливаем строки текущего блока
        if current_block_type:
            current_block_lines.append(line)
        elif not filter_by_pages and current_block_type is None:
            # Ленивый режим: строки вне блоков (между метаданными и блоками)
            pass

    _flush_block()  # последний блок

    if not pages:
        if block_ids_set:
            logger.warning(
                "MD-контекст пуст для %d блоков (block_ids: %s). "
                "Возможно, формат MD изменился — проверьте префиксы "
                "'## СТРАНИЦА ', '### BLOCK [TEXT]:', '### BLOCK [IMAGE]:'",
                len(block_ids_set),
                ", ".join(list(block_ids_set)[:5]),
            )
        return ""

    # Формируем контекст: по страницам, в порядке возрастания
    parts = []
    for page_num in sorted(pages):
        page = pages[page_num]
        section_lines = [f"## СТРАНИЦА {page_num}"]

        # Метаданные
        for m in page["meta"]:
            section_lines.append(m)

        # Текстовые блоки
        if page["texts"]:
            section_lines.append("")
            section_lines.append("### Текст на странице:")
            for t in page["texts"]:
                section_lines.append(t)
                section_lines.append("")

        # IMAGE описания (только для блоков пакета)
        if page["images"]:
            section_lines.append("")
            section_lines.append("### OCR-описания блоков:")
            for img in page["images"]:
                section_lines.append(img)
                section_lines.append("")

        parts.append("\n".join(section_lines))

    return "\n\n---\n\n".join(parts)


def _extract_page_to_sheet_map(md_file_path: str) -> dict[int, str]:
    """Извлечь маппинг PDF-страница → номер листа из MD-файла.

    Парсит строки '**Лист:** N' внутри '## СТРАНИЦА M'.
    Возвращает {pdf_page: sheet_number_str}, например {5: "1 (из 15)", 6: "2"}.
    """
    md_path = Path(md_file_path)
    if not md_path.exists() or md_file_path == "(нет)":
        return {}

    try:
        content = md_path.read_text(encoding="utf-8")
    except OSError:
        return {}

    mapping: dict[int, str] = {}
    current_page = 0

    for line in content.split("\n"):
        if line.startswith("## СТРАНИЦА "):
            try:
                current_page = int(line.split("СТРАНИЦА")[1].strip())
            except (ValueError, IndexError):
                current_page = 0
        elif line.startswith("**Лист:**") and current_page > 0:
            sheet_val = line.replace("**Лист:**", "").strip()
            if sheet_val:
                mapping[current_page] = sheet_val

    return mapping


def _extract_image_context_for_blocks(md_file_path: str, block_ids: list[str]) -> str:
    """Legacy wrapper — вызывает новую функцию без привязки к страницам.

    Используется только если нет информации о страницах (fallback).
    """
    return _extract_page_context_for_blocks(md_file_path, block_ids, [])


def _load_document_graph(project_id: str) -> dict | None:
    """Загрузить document_graph.json если он существует."""
    graph_path = resolve_project_dir(project_id) / "_output" / "document_graph.json"
    if not graph_path.exists():
        return None
    try:
        return json.loads(graph_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _build_structured_block_context(
    graph: dict,
    block_ids: list[str],
    block_pages: list[int],
) -> str:
    """Построить структурированный per-block контекст из document_graph.

    Формирует 3-уровневый контекст для КАЖДОГО блока:
    1. LOCAL TEXT CONTEXT — top-K ближайших text blocks (по geometry)
    2. TARGET BLOCK OCR — normalized OCR этого image block
    3. PAGE GLOBAL CONTEXT — fallback если local context бедный

    graph v2: использует coords_norm и local_text_links для locality binding.
    graph v1: fallback на старое поведение (все text_blocks страницы).

    Типичный объём: 3-10 KB.
    """
    # Импортируем compatibility-функции
    try:
        from graph_builder import (
            is_graph_v2, get_page_sheet_no,
            get_text_block_text, get_image_block_ocr,
            is_good_local_candidate,
        )
        graph_is_v2 = is_graph_v2(graph)
    except ImportError:
        graph_is_v2 = False
        is_good_local_candidate = None
        logger.warning(
            "graph_builder not importable — using v1 flat context for all blocks"
        )

    block_ids_set = set(block_ids)
    target_pages = set(block_pages)

    # Индекс страниц графа
    pages_index: dict[int, dict] = {}
    for page in graph.get("pages", []):
        pages_index[page["page"]] = page

    # Определяем релевантные страницы
    relevant_pages: set[int] = set()
    if target_pages:
        relevant_pages = target_pages
    else:
        for page_num, page in pages_index.items():
            for img in page.get("image_blocks", []):
                if img["id"] in block_ids_set:
                    relevant_pages.add(page_num)

    if not relevant_pages:
        return ""

    # Индекс text blocks для быстрого доступа
    text_block_index: dict[str, dict] = {}
    for page in graph.get("pages", []):
        for tb in page.get("text_blocks", []):
            text_block_index[tb["id"]] = tb

    parts = []

    for page_num in sorted(relevant_pages):
        page = pages_index.get(page_num)
        if not page:
            continue

        page_block_ids = [
            img["id"] for img in page.get("image_blocks", [])
            if img["id"] in block_ids_set
        ]

        if not page_block_ids and not target_pages:
            continue

        # Заголовок страницы
        if graph_is_v2:
            sheet_no = page.get("sheet_no_raw") or page.get("sheet_no_normalized")
        else:
            sheet_no = page.get("sheet_no")
        sheet_name = page.get("sheet_name")

        section_lines = [f"## СТРАНИЦА {page_num}"]
        if sheet_no:
            section_lines.append(f"**Лист:** {sheet_no}")
        if sheet_name:
            section_lines.append(f"**Наименование листа:** {sheet_name}")

        # Per-block 3-уровневый контекст (v2) или flat (v1 fallback)
        if graph_is_v2 and page_block_ids:
            local_links = page.get("local_text_links", {})
            text_blocks = page.get("text_blocks", [])

            for bid in page_block_ids:
                section_lines.append("")
                section_lines.append(f"#### block_id: {bid}")

                img_entry = next(
                    (img for img in page.get("image_blocks", []) if img["id"] == bid),
                    None,
                )

                # ── LEVEL 2: TARGET BLOCK OCR ──
                section_lines.append("")
                section_lines.append("**[TARGET BLOCK OCR]**")
                if img_entry:
                    if img_entry.get("type"):
                        section_lines.append(f"Тип: {img_entry['type']}")
                    section_lines.append(f"page: {page_num}, sheet: {sheet_no or 'N/A'}")
                    ocr_text = get_image_block_ocr(img_entry) if graph_is_v2 else (img_entry.get("ocr") or "")
                    if ocr_text:
                        section_lines.append(ocr_text)
                    else:
                        section_lines.append("(OCR отсутствует)")

                # ── LEVEL 1: LOCAL TEXT CONTEXT ──
                candidates = local_links.get(bid, [])
                local_text_ids = set()

                if candidates:
                    section_lines.append("")
                    section_lines.append("**[LOCAL TEXT CONTEXT]**")
                    for cand in candidates:
                        tb_id = cand["text_block_id"]
                        local_text_ids.add(tb_id)
                        tb = text_block_index.get(tb_id)
                        if tb:
                            tb_text = get_text_block_text(tb) if graph_is_v2 else tb.get("text", "")
                            score = cand.get("score", 0)
                            reason = cand.get("reason", "")
                            section_lines.append(f"[text_block_id: {tb_id}, score: {score}, reason: {reason}]")
                            section_lines.append(tb_text)
                            section_lines.append("")

                # ── LEVEL 3: PAGE GLOBAL CONTEXT (fallback) ──
                # Используем strict check: далёкие/слабые кандидаты НЕ отключают fallback
                if is_good_local_candidate is not None:
                    has_good_local = any(is_good_local_candidate(c) for c in candidates)
                else:
                    has_good_local = any(c.get("score", 0) > 0.15 for c in candidates)

                if not has_good_local and text_blocks:
                    # Local context бедный — даём page-level fallback
                    remaining = [tb for tb in text_blocks if tb["id"] not in local_text_ids]
                    if remaining:
                        section_lines.append("")
                        section_lines.append("**[PAGE GLOBAL CONTEXT — fallback, local context insufficient]**")
                        # Ограничиваем fallback: макс 3 блока, 2000 символов
                        char_budget = 2000
                        chars_used = 0
                        for tb in remaining[:3]:
                            tb_text = get_text_block_text(tb) if graph_is_v2 else tb.get("text", "")
                            if chars_used + len(tb_text) > char_budget:
                                tb_text = tb_text[:char_budget - chars_used] + "..."
                            section_lines.append(f"[text_block_id: {tb['id']}]")
                            section_lines.append(tb_text)
                            section_lines.append("")
                            chars_used += len(tb_text)
                            if chars_used >= char_budget:
                                break

            # Другие блоки на этой странице
            other_blocks = [
                img for img in page.get("image_blocks", [])
                if img["id"] not in block_ids_set
            ]
            if other_blocks:
                section_lines.append("")
                section_lines.append("### Другие блоки на этой странице:")
                for ob in other_blocks:
                    type_info = f" ({ob.get('type', '')})" if ob.get("type") else ""
                    section_lines.append(f"- {ob['id']}{type_info}")

        else:
            # ── v1 FALLBACK: старое поведение ──
            logger.warning(
                "Using v1 flat context for page %d (graph version: %s) — "
                "all text blocks sent to every image block",
                page_num, graph.get("version", "unknown"),
            )
            text_blocks = page.get("text_blocks", [])
            if text_blocks:
                section_lines.append("")
                section_lines.append("### Текст на странице:")
                for tb in text_blocks:
                    tb_id = tb.get("id", "")
                    if tb_id:
                        section_lines.append(f"[text_block_id: {tb_id}]")
                    section_lines.append(tb["text"])
                    section_lines.append("")

            if page_block_ids:
                section_lines.append("")
                section_lines.append("### Блоки этого пакета:")
                for bid in page_block_ids:
                    img_entry = next(
                        (img for img in page.get("image_blocks", []) if img["id"] == bid),
                        None,
                    )
                    section_lines.append(f"")
                    section_lines.append(f"#### block_id: {bid}")
                    if img_entry:
                        if img_entry.get("type"):
                            section_lines.append(f"**Тип:** {img_entry['type']}")
                        if img_entry.get("ocr"):
                            section_lines.append(f"**OCR-описание:**")
                            section_lines.append(img_entry["ocr"])

                other_blocks = [
                    img for img in page.get("image_blocks", [])
                    if img["id"] not in block_ids_set
                ]
                if other_blocks:
                    section_lines.append("")
                    section_lines.append("### Другие блоки на этой странице:")
                    for ob in other_blocks:
                        type_info = f" ({ob['type']})" if ob.get("type") else ""
                        section_lines.append(f"- {ob['id']}{type_info}")

        parts.append("\n".join(section_lines))

    return "\n\n---\n\n".join(parts)


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

    _, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    # Маппинг PDF-страница → номер листа (для grounding)
    page_to_sheet = _extract_page_to_sheet_map(md_file_path)

    # Формируем список блоков с указанием и страницы PDF, и листа документа
    block_lines = []
    for block in blocks:
        block_path = str(
            resolve_project_dir(project_id) / "_output" / "blocks" / block["file"]
        )
        pdf_page = block.get("page", "?")
        sheet_info = page_to_sheet.get(pdf_page, "")
        sheet_suffix = f", Лист {sheet_info}" if sheet_info else ""
        block_lines.append(
            f"- `{block_path}` (стр. PDF {pdf_page}{sheet_suffix}, "
            f"block_id: {block['block_id']}, "
            f"OCR: {block.get('ocr_label', 'image')})"
        )

    # Извлекаем inline контекст: приоритет document_graph → fallback raw MD
    batch_block_ids = [b["block_id"] for b in blocks]
    batch_pages = [b["page"] for b in blocks if b.get("page")]

    graph = _load_document_graph(project_id)
    if graph:
        md_context = _build_structured_block_context(
            graph, batch_block_ids, batch_pages
        )
    else:
        md_context = _extract_page_context_for_blocks(
            md_file_path, batch_block_ids, batch_pages
        )

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
        .replace("{SECTION}", (project_info or {}).get("section", "EM"))
    )
    return task


# ─── Компактификация данных для findings_merge ───

def _enrich_compact_from_graph_v2(compact: dict, graph: dict):
    """Обогатить blocks_compact данными locality из document_graph v2.

    Для каждого block_id находит:
    - selected_text_block_ids из local_text_links
    - local_text_excerpt (первые 300 символов ближайшего text-блока)
    - block_ocr_excerpt (первые 200 символов OCR)
    - sheet_no из страницы
    """
    # Индекс: page → page_data
    pages_by_num = {p["page"]: p for p in graph.get("pages", [])}

    # Индекс: block_id → page_num
    block_to_page: dict[str, int] = {}
    for pg in graph.get("pages", []):
        for img in pg.get("image_blocks", []):
            block_to_page[img["id"]] = pg["page"]

    # Индекс text blocks
    text_index: dict[str, dict] = {}
    for pg in graph.get("pages", []):
        for tb in pg.get("text_blocks", []):
            text_index[tb["id"]] = tb

    for bc in compact.get("blocks_compact", []):
        bid = bc.get("block_id", "")
        if not bid:
            continue

        page_num = block_to_page.get(bid, bc.get("page", 0))
        pg = pages_by_num.get(page_num)
        if not pg:
            continue

        # sheet_no
        bc["sheet_no"] = pg.get("sheet_no_raw") or pg.get("sheet_no_normalized") or ""

        # local_text_links
        local_links = pg.get("local_text_links", {})
        candidates = local_links.get(bid, [])

        # Enrich only missing fields — don't overwrite primary data from block_analyses
        if not bc.get("selected_text_block_ids") and candidates:
            bc["selected_text_block_ids"] = [c["text_block_id"] for c in candidates]

        # local_text_excerpt — текст лучшего кандидата (always enrich if empty)
        if not bc.get("local_text_excerpt") and candidates:
            best_tb_id = candidates[0]["text_block_id"]
            best_tb = text_index.get(best_tb_id)
            if best_tb:
                bc["local_text_excerpt"] = (best_tb.get("text") or "")[:300]

        # block_ocr_excerpt (always enrich if empty)
        if not bc.get("block_ocr_excerpt"):
            for img in pg.get("image_blocks", []):
                if img["id"] == bid:
                    ocr = img.get("ocr_text_normalized") or img.get("ocr") or ""
                    bc["block_ocr_excerpt"] = ocr[:200]
                    break


def _prepare_compact_findings_input(project_id: str) -> Path | None:
    """Создать компактный JSON из 01+02 для findings_merge.

    Убирает: полные block summaries, дублирующий контент.
    Оставляет: findings, key_values, project_params, verified items.

    Типичное сжатие: 800 KB → 100-200 KB (4-8x меньше).
    """
    output_dir = resolve_project_dir(project_id) / "_output"
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

    # Из 02: findings из block_analyses, items_verified, key_values (без полных summary)
    if stage02.exists():
        try:
            data02 = json.loads(stage02.read_text(encoding="utf-8"))

            # Items verified — полностью
            compact["items_verified_from_stage_01"] = data02.get(
                "items_verified_from_stage_01", []
            )

            # Собираем findings из block_analyses[].findings (основной источник)
            # + legacy preliminary_findings
            block_analyses = data02.get("block_analyses", [])
            all_block_findings = []
            for ba in block_analyses:
                for f in ba.get("findings", []):
                    if "block_evidence" not in f:
                        f["block_evidence"] = ba.get("block_id", "")
                    # Normalize block identity → bare id
                    try:
                        from graph_builder import normalize_block_ids_in_finding
                        normalize_block_ids_in_finding(f)
                    except ImportError:
                        pass
                    all_block_findings.append(f)
            legacy_findings = data02.get("preliminary_findings", [])
            compact["preliminary_findings"] = all_block_findings + legacy_findings

            # Из block_analyses: block_id, page, sheet_type, key_values_read
            # + discipline-поля для discipline gate в merge
            # + locality fields (primary source: block_analyses after backfill)
            compact["blocks_compact"] = [
                {
                    "block_id": ba.get("block_id", ""),
                    "page": ba.get("page", 0),
                    "sheet_type": ba.get("sheet_type", ""),
                    "key_values_read": ba.get("key_values_read", []),
                    "findings_count": len(ba.get("findings", [])),
                    "discipline_detected": ba.get("discipline_detected", ""),
                    "discipline_mismatch": ba.get("discipline_mismatch", False),
                    "discipline_note": ba.get("discipline_note", ""),
                    # Locality contract — primary source from block_analyses
                    "selected_text_block_ids": ba.get("selected_text_block_ids", []),
                    "evidence_text_refs": ba.get("evidence_text_refs", []),
                    "local_text_excerpt": ba.get("local_text_excerpt", ""),
                    "block_ocr_excerpt": ba.get("block_ocr_excerpt", ""),
                    "sheet_no": ba.get("sheet_no", ""),
                    "related_block_ids": ba.get("related_block_ids", []),
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

    # Обогащаем blocks_compact из document_graph v2 (locality contract)
    graph_path = output_dir / "document_graph.json"
    graph = None
    if graph_path.exists() and compact.get("blocks_compact"):
        try:
            graph = json.loads(graph_path.read_text(encoding="utf-8"))
            if graph.get("version", 1) >= 2:
                _enrich_compact_from_graph_v2(compact, graph)
            else:
                logger.warning(
                    "[%s] document_graph v%s — locality enrichment skipped, "
                    "run process_project.py --force to upgrade",
                    project_id, graph.get("version", 1),
                )
        except (json.JSONDecodeError, OSError):
            pass

    # page_sheet_map из document_graph.json — для merge-этапа
    if graph_path.exists():
        try:
            if graph is None:
                graph = json.loads(graph_path.read_text(encoding="utf-8"))
            page_sheet_map = {}
            for pg in graph.get("pages", []):
                page_num = pg.get("page")
                sheet_no = (
                    pg.get("sheet_no_raw")
                    or pg.get("sheet_no_normalized")
                    or pg.get("sheet_no")
                )
                if page_num is not None and sheet_no:
                    page_sheet_map[str(page_num)] = sheet_no
            compact["page_sheet_map"] = page_sheet_map
        except (json.JSONDecodeError, OSError):
            compact["page_sheet_map"] = {}
    else:
        compact["page_sheet_map"] = {}

    # Fallback для v1: если page_sheet_map пуст, строим из MD-файла
    if not compact["page_sheet_map"]:
        md_file_path = _get_md_file_path(
            _load_project_info(project_id), project_id
        )
        if md_file_path and md_file_path != "(нет)":
            fallback_map = _extract_page_to_sheet_map(md_file_path)
            if fallback_map:
                compact["page_sheet_map"] = {
                    str(k): v for k, v in fallback_map.items()
                }
                logger.warning(
                    "[%s] page_sheet_map built from MD fallback (%d entries) — "
                    "upgrade to graph v2 for stamp-based mapping",
                    project_id, len(fallback_map),
                )

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


# ─── Critic + Corrector (проверка замечаний) ───

def prepare_findings_critic_task(
    project_info: dict,
    project_id: str,
    chunk_suffix: str = "",
) -> str:
    """Подготовить задачу для критической проверки замечаний.

    chunk_suffix: если задан (напр. "_001") — подменяет имена input/output файлов
    для параллельного запуска чанков без файловых конфликтов.
    """
    template = load_template(FINDINGS_CRITIC_TASK_TEMPLATE)

    _, output_path = _get_project_paths(project_id)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{OUTPUT_PATH}", output_path)
    )

    if chunk_suffix:
        task = task.replace(
            "03_findings_review_input.json",
            f"03_findings_review_input{chunk_suffix}.json",
        )
        task = task.replace(
            "03_findings_review.json",
            f"03_findings_review{chunk_suffix}.json",
        )

    return task


def prepare_findings_corrector_task(
    project_info: dict,
    project_id: str,
) -> str:
    """Подготовить задачу для корректировки замечаний по вердиктам критика."""
    template = load_template(FINDINGS_CORRECTOR_TASK_TEMPLATE)

    _, output_path = _get_project_paths(project_id)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{OUTPUT_PATH}", output_path)
    )
    return task


# ─── Оптимизация проектных решений ───

# Маппинг дисциплин → разделы вендор-листа (по первой колонке таблицы)
_VENDOR_SECTIONS_BY_DISCIPLINE: dict[str, list[str]] = {
    "OV": [
        "Вентиляция и кондиционирование",
        "Отопление и теплоснабжение",
        "Холодоснабжение",
        "Автоматизация и диспетчеризация",
    ],
    "EM": [
        "Электроснабжение и освещение",
        "Автоматизация и диспетчеризация",
    ],
    "VK": [
        "Системы водоснабжения",
        "Система водоотведения",
    ],
    "PB": [
        "Автоматическое пожаротушение",
        "Газовое пожаротушение",
        "Автоматика систем ППЗ",
    ],
    "SS": [
        "Системы безопасности",
        "Автоматика систем ППЗ",
        "Автоматизация и диспетчеризация",
    ],
}


def _load_vendor_list_for_discipline(section: str) -> str:
    """Загрузить и отфильтровать вендор-лист по дисциплине.

    Парсит MD-таблицу, оставляет только строки с разделами,
    относящимися к указанной дисциплине.
    """
    vendor_path = PROJECTS_DIR / "DOC" / "вендор лист.md"
    if not vendor_path.exists():
        return "(вендор-лист не найден)"

    try:
        content = vendor_path.read_text(encoding="utf-8")
    except OSError:
        return "(ошибка чтения вендор-листа)"

    allowed = _VENDOR_SECTIONS_BY_DISCIPLINE.get(section, [])
    if not allowed:
        return "(нет маппинга вендор-листа для дисциплины " + section + ")"

    # Парсим MD-таблицу: строки начинаются с |
    lines = content.split("\n")
    header_lines: list[str] = []
    data_lines: list[str] = []
    current_section = ""

    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        # Заголовок и разделитель таблицы (первые 2 строки с |)
        if not header_lines or (len(header_lines) == 1 and ":---" in stripped):
            header_lines.append(stripped)
            continue

        # Определяем текущий раздел — первая колонка содержит **жирный** текст
        cols = [c.strip() for c in stripped.split("|")]
        # cols[0] пустой (до первого |), cols[1] = первая колонка
        if len(cols) >= 2 and cols[1] and "**" in cols[1]:
            current_section = cols[1].replace("**", "").strip()

        # Проверяем, подходит ли текущий раздел
        if any(a.lower() in current_section.lower() for a in allowed):
            data_lines.append(stripped)

    if not data_lines:
        return "(нет позиций вендор-листа для дисциплины " + section + ")"

    result = "\n".join(header_lines + data_lines)
    return result


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

    # Вендор-лист — отфильтрованный по дисциплине
    section = (project_info or {}).get("section", "EM")
    vendor_list_text = _load_vendor_list_for_discipline(section)

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{MD_FILE_PATH}", md_file_path)
        .replace("{VENDOR_LIST}", vendor_list_text)
    )
    return task


def prepare_optimization_critic_task(
    project_info: dict,
    project_id: str,
) -> str:
    """Подготовить задачу для критической проверки оптимизации (Critic)."""
    template = load_template(OPTIMIZATION_CRITIC_TASK_TEMPLATE)

    _, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    # Вендор-лист — отфильтрованный по дисциплине
    section = (project_info or {}).get("section", "EM")
    vendor_list_text = _load_vendor_list_for_discipline(section)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{MD_FILE_PATH}", md_file_path)
        .replace("{VENDOR_LIST}", vendor_list_text)
    )
    return task


def prepare_optimization_corrector_task(
    project_info: dict,
    project_id: str,
) -> str:
    """Подготовить задачу для корректировки оптимизации по вердиктам критика (Corrector)."""
    template = load_template(OPTIMIZATION_CORRECTOR_TASK_TEMPLATE)

    _, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    # Вендор-лист — отфильтрованный по дисциплине
    section = (project_info or {}).get("section", "EM")
    vendor_list_text = _load_vendor_list_for_discipline(section)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{MD_FILE_PATH}", md_file_path)
        .replace("{VENDOR_LIST}", vendor_list_text)
    )
    return task
