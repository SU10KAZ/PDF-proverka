"""
REST API для просмотра MD-документа проекта (постранично).
"""
from fastapi import APIRouter, HTTPException
from webapp.services import project_service

router = APIRouter(prefix="/api/document", tags=["document"])


@router.get("/{project_id}/pages")
async def get_document_pages(project_id: str):
    """Оглавление MD-документа: список страниц с метаданными (без содержимого блоков)."""
    doc = project_service.parse_md_document(project_id)
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


@router.get("/{project_id}/page/{page_num}")
async def get_document_page(project_id: str, page_num: int):
    """Содержимое одной страницы MD-документа (все блоки)."""
    page = project_service.get_document_page(project_id, page_num)
    if not page:
        raise HTTPException(404, f"Страница {page_num} не найдена для '{project_id}'")
    return page
