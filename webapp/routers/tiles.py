"""
REST API для тайлов и OCR-блоков чертежей.
"""
import json
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from webapp.services import project_service
from webapp.config import PROJECTS_DIR

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


# ─── OCR-блоки ───

@router.get("/{project_id}/blocks")
async def get_blocks(project_id: str):
    """Список image-блоков, сгруппированных по страницам."""
    blocks_dir = PROJECTS_DIR / project_id / "_output" / "blocks"
    index_path = blocks_dir / "index.json"
    if not index_path.exists():
        raise HTTPException(404, f"Блоки не найдены для '{project_id}'")

    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)

    # Группируем по страницам
    pages_map: dict[int, list] = {}
    for block in index_data.get("blocks", []):
        page = block.get("page", 0)
        pages_map.setdefault(page, []).append(block)

    pages = []
    for page_num in sorted(pages_map.keys()):
        blocks = pages_map[page_num]
        pages.append({
            "page_num": page_num,
            "block_count": len(blocks),
            "blocks": blocks,
        })

    return {
        "project_id": project_id,
        "total_blocks": index_data.get("total_blocks", 0),
        "pages": pages,
    }


@router.get("/{project_id}/blocks/analysis")
async def get_blocks_analysis(project_id: str):
    """Агрегированные данные анализа блоков из block_batch_*.json."""
    output_dir = PROJECTS_DIR / project_id / "_output"
    batch_files = sorted(output_dir.glob("block_batch_*.json"))

    blocks_map = {}
    for bf in batch_files:
        try:
            with open(bf, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Поддержка обоих форматов: blocks_reviewed (legacy) и block_analyses (OCR)
            block_list = data.get("blocks_reviewed") or data.get("block_analyses") or []
            for block_info in block_list:
                bid = block_info.get("block_id", "")
                if bid:
                    blocks_map[bid] = block_info
        except Exception:
            continue

    return {
        "project_id": project_id,
        "total_analyzed": len(blocks_map),
        "blocks": blocks_map,
    }


@router.get("/{project_id}/blocks/image/{block_id}")
async def get_block_image(project_id: str, block_id: str):
    """PNG-файл кропнутого блока."""
    block_path = PROJECTS_DIR / project_id / "_output" / "blocks" / f"block_{block_id}.png"
    if not block_path.exists():
        raise HTTPException(404, f"Блок {block_id} не найден")
    return FileResponse(str(block_path), media_type="image/png")
