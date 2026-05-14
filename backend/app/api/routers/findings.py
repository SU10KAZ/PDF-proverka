"""
REST API для замечаний аудита.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import backend.app.services.findings.findings_service as findings_service

router = APIRouter(prefix="/api/findings", tags=["findings"])


@router.get("/summary")
async def get_all_summaries():
    """Сводка замечаний по всем проектам."""
    summaries = findings_service.get_all_summaries()
    return {"summaries": [s.model_dump() for s in summaries]}


def _validate_version_id(project_id: str, version_id: Optional[str]) -> None:
    """Если version_id задан, проверить, что он существует в манифесте."""
    if not version_id:
        return
    from backend.app.services.common import project_service, version_service
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    try:
        version_service.get_version_entry(proj_dir, project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))


@router.get("/{project_id:path}/block-map")
async def get_finding_block_map(
    project_id: str,
    version_id: Optional[str] = Query(None, description="Конкретная версия (v1/v2/...), по умолчанию latest"),
):
    """Маппинг finding_id → [block_ids] для подсветки блоков при наведении."""
    _validate_version_id(project_id, version_id)
    result = findings_service.get_finding_block_map(project_id, version_id=version_id)
    if result is None:
        raise HTTPException(404, f"Данные не найдены для '{project_id}'")
    return result


@router.get("/{project_id:path}/finding/{finding_id}")
async def get_finding(
    project_id: str,
    finding_id: str,
    version_id: Optional[str] = Query(None),
):
    """Одно замечание по ID."""
    _validate_version_id(project_id, version_id)
    finding = findings_service.get_finding_by_id(project_id, finding_id, version_id=version_id)
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
    group: bool = Query(False, description="Группировать похожие замечания"),
    version_id: Optional[str] = Query(None, description="Конкретная версия (v1/v2/...), по умолчанию latest"),
):
    """Замечания проекта с фильтрацией и пагинацией."""
    _validate_version_id(project_id, version_id)
    result = findings_service.get_findings(
        project_id,
        severity=severity,
        category=category,
        sheet=sheet,
        search=search,
        limit=limit,
        offset=offset,
        group=group,
        version_id=version_id,
    )
    if result is None:
        raise HTTPException(404, f"Замечания не найдены для '{project_id}'. Возможно, аудит ещё не проводился.")
    return result.model_dump()
