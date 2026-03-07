"""
Построение задач для Claude CLI из шаблонов.
Подготовка текста промтов с подстановкой плейсхолдеров и инъекцией дисциплин.
"""
from typing import Optional

from webapp.config import (
    BASE_DIR, PROJECTS_DIR,
    AUDIT_TASK_TEMPLATE, TILE_BATCH_TASK_TEMPLATE,
    NORM_VERIFY_TASK_TEMPLATE, NORM_FIX_TASK_TEMPLATE,
    TRIAGE_TASK_TEMPLATE, SMART_MERGE_TASK_TEMPLATE,
    OPTIMIZATION_TASK_TEMPLATE,
)
from webapp.services.cli_utils import load_template, build_grid_visual
from webapp.services import discipline_service


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


# ─── Пакет тайлов ───

def prepare_tile_batch_task(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
) -> str:
    """
    Подготовить задачу для одного пакета тайлов.
    Подставляет плейсхолдеры из шаблона tile_batch_task.md.
    """
    template = load_template(TILE_BATCH_TASK_TEMPLATE)

    batch_id = batch_data["batch_id"]
    tiles = batch_data.get("tiles", [])
    pages = batch_data.get("pages_included", [])
    batch_type = batch_data.get("batch_type", "multi_page")
    page_grid = batch_data.get("page_grid", "")

    # Формируем список тайлов
    tile_lines = []
    for tile in tiles:
        tile_path = str(PROJECTS_DIR / project_id / "_output" / "tiles" / tile["file"])
        tile_lines.append(
            f"- `{tile_path}` (стр. {tile.get('page', '?')}, "
            f"r{tile.get('row', '?')}c{tile.get('col', '?')})"
        )

    project_path, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    # ─── Тип батча и описание ───
    if batch_type == "single_page" and page_grid:
        batch_type_desc = (
            f"ОДИН ЛИСТ (стр. {pages[0]}), сетка {page_grid} — "
            f"все тайлы являются фрагментами одного чертежа"
        )
    else:
        batch_type_desc = (
            f"СБОРНЫЙ — {len(pages)} отдельных страниц с 1 тайлом каждая"
        )

    # ─── Информация о сетке (для single_page) ───
    if batch_type == "single_page" and page_grid:
        grid_visual = build_grid_visual(page_grid, tiles)
        page_grid_info = f"""## Раскладка тайлов на листе (стр. {pages[0]})

Этот пакет содержит **один чертёж**, разрезанный на сетку **{page_grid}**.
Тайлы перекрываются на ~5% по краям. Мысленно собери лист целиком перед анализом.

```
{grid_visual}
```

**Порядок чтения:** сначала прочитай ВСЕ тайлы, затем анализируй лист как единое целое.
Обращай внимание на элементы, пересекающие границы тайлов:
- Кабельные трассы, идущие из одного тайла в соседний
- Подписи и обозначения рядом с границей тайла
- Однолинейные связи между элементами на разных тайлах
- Таблицы, разделённые между тайлами"""
    else:
        page_grid_info = (
            "Этот пакет содержит одиночные страницы (по 1 тайлу). "
            "Каждый тайл — отдельный самостоятельный лист."
        )

    # ─── Инструкции по реконструкции ───
    if batch_type == "single_page" and page_grid:
        reconstruction = """**Этот батч — ОДИН ЛИСТ.** Алгоритм анализа:
1. Прочитай ВСЕ тайлы последовательно (r1c1 → r1c2 → ... → r2c1 → r2c2 → ...)
2. Мысленно собери полный лист из фрагментов по сетке выше
3. Определи тип чертежа (однолинейная, план, схема щита, спецификация)
4. Анализируй лист ЦЕЛИКОМ — ищи связи между элементами на разных тайлах
5. Проверяй кабельные трассы и цепи, проходящие через границы тайлов
6. Если текст или обозначение обрезано на краю тайла — ищи продолжение на соседнем"""
    else:
        reconstruction = (
            "Каждый тайл — отдельная страница. Анализируй их независимо друг от друга."
        )

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("{BATCH_ID}", str(batch_id))
        .replace("{BATCH_ID_PADDED}", f"{batch_id:03d}")
        .replace("{TOTAL_BATCHES}", str(total_batches))
        .replace("{PROJECT_ID}", project_id)
        .replace("{PROJECT_NAME}", project_info.get("name", project_id))
        .replace("{PROJECT_PATH}", project_path)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{TILE_COUNT}", str(len(tiles)))
        .replace("{PAGES_LIST}", ", ".join(str(p) for p in pages))
        .replace("{TILE_LIST}", "\n".join(tile_lines))
        .replace("{BASE_DIR}", str(BASE_DIR))
        .replace("{MD_FILE_PATH}", md_file_path)
        .replace("{BATCH_TYPE_DESCRIPTION}", batch_type_desc)
        .replace("{PAGE_GRID_INFO}", page_grid_info)
        .replace("{RECONSTRUCTION_INSTRUCTIONS}", reconstruction)
    )

    return task


# ─── Основной аудит ───

def prepare_main_audit_task(
    project_info: dict,
    project_id: str,
) -> str:
    """
    Подготовить задачу для основного аудита.
    Подставляет пути проекта в audit_task.md.
    """
    template = load_template(AUDIT_TASK_TEMPLATE)

    project_path, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("project/document_pdf_extracted.txt", f"{output_path}\\extracted_text.txt")
        .replace("project/tiles", f"{output_path}\\tiles")
        .replace("133/23-ГК-ЭМ1", project_info.get("name", project_id))
        .replace("{MD_FILE_PATH}", md_file_path)
    )

    return task


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


# ─── Триаж страниц ───

def prepare_triage_task(
    project_info: dict,
    project_id: str,
) -> str:
    """Подготовить задачу для триажа страниц (определение приоритетов)."""
    template = load_template(TRIAGE_TASK_TEMPLATE)

    project_path, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{PROJECT_PATH}", project_path)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{BASE_DIR}", str(BASE_DIR))
        .replace("{MD_FILE_PATH}", md_file_path)
    )
    return task


# ─── Свод замечаний ───

def prepare_smart_merge_task(
    project_info: dict,
    project_id: str,
) -> str:
    """Подготовить задачу для свода замечаний после параллельного анализа тайлов."""
    template = load_template(SMART_MERGE_TASK_TEMPLATE)

    project_path, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("{PROJECT_ID}", project_id)
        .replace("{PROJECT_PATH}", project_path)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{BASE_DIR}", str(BASE_DIR))
        .replace("{MD_FILE_PATH}", md_file_path)
    )
    return task


# ─── Оптимизация проектных решений ───

def prepare_optimization_task(
    project_info: dict,
    project_id: str,
) -> str:
    """Подготовить задачу для анализа оптимизации проектной документации."""
    template = load_template(OPTIMIZATION_TASK_TEMPLATE)

    project_path, output_path = _get_project_paths(project_id)
    md_file_path = _get_md_file_path(project_info, project_id)

    template = _inject_discipline(template, project_info)

    task = (
        template
        .replace("{PROJECT_PATH}", project_path)
        .replace("{OUTPUT_PATH}", output_path)
        .replace("{MD_FILE_PATH}", md_file_path)
    )
    return task
