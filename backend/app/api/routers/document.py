"""
REST API для просмотра MD-документа проекта (постранично).
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
import backend.app.services.common.project_service as project_service
from backend.app.services.common import version_service

router = APIRouter(prefix="/api/document", tags=["document"])


def _validate_version(project_id: str, version_id: Optional[str]) -> None:
    if not version_id:
        return
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    try:
        version_service.get_version_entry(proj_dir, project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))


@router.get("/{project_id:path}/pages")
async def get_document_pages(
    project_id: str,
    version_id: Optional[str] = Query(None, description="Конкретная версия, по умолчанию latest"),
):
    """Оглавление MD-документа: список страниц с метаданными (без содержимого блоков)."""
    _validate_version(project_id, version_id)
    doc = project_service.parse_md_document(project_id, version_id=version_id)
    if not doc:
        raise HTTPException(404, f"MD-файл не найден для '{project_id}'")
    # Возвращаем без содержимого блоков (только счётчики)
    pages_light = []
    for p in doc["pages"]:
        pages_light.append({
            "page_num": p["page_num"],
            "sheet_info": p["sheet_info"],
            "sheet_label": p["sheet_label"],
            "text_blocks": p["text_blocks"],
            "image_blocks": p["image_blocks"],
        })
    return {
        "project_id": doc["project_id"],
        "md_file": doc["md_file"],
        "total_pages": doc["total_pages"],
        "pages": pages_light,
    }


@router.get("/{project_id:path}/page/{page_num}")
async def get_document_page(
    project_id: str,
    page_num: int,
    version_id: Optional[str] = Query(None),
):
    """Содержимое одной страницы MD-документа (все блоки)."""
    _validate_version(project_id, version_id)
    page = project_service.get_document_page(project_id, page_num, version_id=version_id)
    if not page:
        raise HTTPException(404, f"Страница {page_num} не найдена для '{project_id}'")
    return page
