"""
REST API для OCR-блоков чертежей.
"""
import json
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from webapp.services.project_service import resolve_project_dir

router = APIRouter(prefix="/api/tiles", tags=["blocks"])


# ─── OCR-блоки ───

@router.get("/{project_id:path}/blocks")
async def get_blocks(project_id: str):
    """Список image-блоков, сгруппированных по страницам."""
    blocks_dir = resolve_project_dir(project_id) / "_output" / "blocks"
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
        "total_expected": index_data.get("total_expected", 0),
        "errors": index_data.get("errors", 0),
        "pages": pages,
    }


@router.get("/{project_id:path}/blocks/analysis")
async def get_blocks_analysis(project_id: str):
    """Агрегированные данные анализа блоков из block_batch_*.json."""
    output_dir = resolve_project_dir(project_id) / "_output"
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


@router.get("/{project_id:path}/blocks/image/{block_id}")
async def get_block_image(project_id: str, block_id: str):
    """PNG-файл кропнутого блока."""
    block_path = resolve_project_dir(project_id) / "_output" / "blocks" / f"block_{block_id}.png"
    if not block_path.exists():
        raise HTTPException(404, f"Блок {block_id} не найден")
    return FileResponse(str(block_path), media_type="image/png")
