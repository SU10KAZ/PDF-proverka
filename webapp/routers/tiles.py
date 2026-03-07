"""
REST API для тайлов (блоков чертежей).
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from webapp.services import project_service

router = APIRouter(prefix="/api/tiles", tags=["tiles"])


@router.get("/{project_id}/pages")
async def get_tile_pages(project_id: str):
    """Список страниц с тайлами."""
    pages = project_service.get_tile_pages(project_id)
    if not pages:
        raise HTTPException(404, f"Тайлы не найдены для '{project_id}'")
    return {"project_id": project_id, "pages": pages}


@router.get("/{project_id}/analysis")
async def get_tile_analysis(project_id: str):
    """Агрегированные данные анализа тайлов из tile_batch_*.json."""
    data = project_service.get_tile_analysis(project_id)
    if not data:
        return {"project_id": project_id, "total_analyzed": 0, "tiles": {}}
    return data


@router.get("/{project_id}/page-summaries")
async def get_page_summaries(project_id: str):
    """Все page_summaries проекта (без full_text_content)."""
    data = project_service.get_all_page_summaries(project_id)
    if not data:
        return {"project_id": project_id, "page_summaries": []}
    return data


@router.get("/{project_id}/page-analysis/{page_num}")
async def get_page_analysis(project_id: str, page_num: int):
    """Полный анализ одной страницы: page_summary + тайлы."""
    data = project_service.get_page_analysis(project_id, page_num)
    if not data:
        raise HTTPException(404, f"Анализ страницы {page_num} не найден для '{project_id}'")
    return data


@router.get("/{project_id}/image/{page_num}/{row}_{col}")
async def get_tile_image(project_id: str, page_num: str, row: int, col: int):
    """PNG-файл тайла."""
    path = project_service.get_tile_path(project_id, page_num, row, col)
    if not path:
        raise HTTPException(404, f"Тайл page_{page_num}_r{row}c{col}.png не найден")
    return FileResponse(str(path), media_type="image/png")
