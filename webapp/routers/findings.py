"""
REST API для замечаний аудита.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from webapp.services import findings_service

router = APIRouter(prefix="/api/findings", tags=["findings"])


@router.get("/summary")
async def get_all_summaries():
    """Сводка замечаний по всем проектам."""
    summaries = findings_service.get_all_summaries()
    return {"summaries": [s.model_dump() for s in summaries]}


@router.get("/{project_id:path}/block-map")
async def get_finding_block_map(project_id: str):
    """Маппинг finding_id → [block_ids] для подсветки блоков при наведении."""
    result = findings_service.get_finding_block_map(project_id)
    if result is None:
        raise HTTPException(404, f"Данные не найдены для '{project_id}'")
    return result


@router.get("/{project_id:path}/finding/{finding_id}")
async def get_finding(project_id: str, finding_id: str):
    """Одно замечание по ID."""
    finding = findings_service.get_finding_by_id(project_id, finding_id)
    if finding is None:
        raise HTTPException(404, f"Замечание '{finding_id}' не найдено")
    return finding


@router.get("/{project_id:path}")
async def get_findings(
    project_id: str,
    severity: Optional[str] = Query(None, description="Фильтр по критичности"),
    category: Optional[str] = Query(None, description="Фильтр по категории"),
    sheet: Optional[str] = Query(None, description="Фильтр по листу"),
    search: Optional[str] = Query(None, description="Полнотекстовый поиск"),
    limit: Optional[int] = Query(None, ge=1, le=500, description="Макс. замечаний"),
    offset: Optional[int] = Query(None, ge=0, description="Смещение"),
):
    """Замечания проекта с фильтрацией и пагинацией."""
    result = findings_service.get_findings(
        project_id,
        severity=severity,
        category=category,
        sheet=sheet,
        search=search,
        limit=limit,
        offset=offset,
    )
    if result is None:
        raise HTTPException(404, f"Замечания не найдены для '{project_id}'. Возможно, аудит ещё не проводился.")
    return result.model_dump()
