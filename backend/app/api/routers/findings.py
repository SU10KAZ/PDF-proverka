"""
REST API для замечаний аудита.
"""
import asyncio

from fastapi import APIRouter, HTTPException, Query, Request
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
    try:
        from backend.app.services.storage.storage_read_facade import production_uses_v2
        if production_uses_v2():
            from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
            adapter = ProjectsV2Adapter()
            doc = adapter.find_document_by_project_id(project_id)
            if doc is not None and adapter.resolve_version_id(doc, version_id):
                return
    except Exception:
        pass
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
    request: Request,
    version_id: Optional[str] = Query(None, description="Конкретная версия (v1/v2/...), по умолчанию latest"),
):
    """Маппинг finding_id → [block_ids] для подсветки блоков при наведении.

    opt-in/default read canary: при v2-backend строится из projects_v2
    (read-only, та же строгая логика). `?storage=legacy` форсит legacy.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_block_map(request, project_id)
    _validate_version_id(project_id, version_id)
    result = findings_service.get_finding_block_map(project_id, version_id=version_id)
    if result is None:
        raise HTTPException(404, f"Данные не найдены для '{project_id}'")
    return result


@router.get("/{project_id:path}/finding/{finding_id}")
async def get_finding(
    project_id: str,
    finding_id: str,
    request: Request,
    version_id: Optional[str] = Query(None),
):
    """Одно замечание по ID.

    opt-in read canary: `?storage=projects_v2` (или header) + флаг → замечание из
    projects_v2 (read-only). Без opt-in — legacy как прежде.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_finding_by_id(request, project_id, finding_id)
    _validate_version_id(project_id, version_id)
    finding = findings_service.get_finding_by_id(project_id, finding_id, version_id=version_id)
    if finding is None:
        raise HTTPException(404, f"Замечание '{finding_id}' не найдено")
    return finding




# KB validation



@router.get("/{project_id:path}/evidence-validation")
async def get_evidence_validation(
    project_id: str,
    version_id: Optional[str] = Query(None),
):
    """Return saved Evidence Verifier decisions."""
    import backend.app.services.findings.evidence_validation_service as evsvc
    data = evsvc.get_evidence_validation(project_id, version_id=version_id)
    if data is None:
        raise HTTPException(404, "Evidence validation has not been generated for this project")
    return data


@router.post("/{project_id:path}/evidence-validation/run")
async def run_evidence_validation(
    project_id: str,
    version_id: Optional[str] = Query(None),
    section: str = Query("TX"),
    graphic_model: Optional[str] = Query(None),
    text_model: Optional[str] = Query(None),
    force: bool = Query(False),
):
    """Run Evidence Verifier (document + graphic blocks). May take a long time."""
    import backend.app.services.findings.evidence_validation_service as evsvc
    try:
        # Сервис синхронный (внутри asyncio.run + локальные vision-вызовы) —
        # выносим в поток, чтобы не блокировать event loop и не падать на
        # вложенном asyncio.run внутри уже работающего loop.
        return await asyncio.to_thread(
            evsvc.run_evidence_validation,
            project_id,
            version_id,
            section,
            graphic_model=graphic_model,
            text_model=text_model,
            force=force,
        )
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, f"Evidence validation error: {e}")

@router.get("/{project_id:path}/kb-validation")
async def get_kb_validation(
    project_id: str,
    version_id: Optional[str] = Query(None),
):
    """Return saved KB validation decisions for project findings."""
    import backend.app.services.findings.kb_validation_service as kbsvc
    data = kbsvc.get_kb_validation(project_id, version_id=version_id)
    if data is None:
        raise HTTPException(404, "KB validation has not been generated for this project")
    return data


@router.post("/{project_id:path}/kb-validation/run")
async def run_kb_validation(
    project_id: str,
    version_id: Optional[str] = Query(None),
    section: str = Query("TX"),
    model: str = Query("sonnet"),
):
    """Run KB validation. This can take several minutes."""
    import backend.app.services.findings.kb_validation_service as kbsvc
    try:
        data = kbsvc.run_kb_validation(
            project_id,
            version_id=version_id,
            section=section,
            model=model,
        )
        return data
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, f"KB validation error: {e}")

@router.get("/{project_id:path}")
async def get_findings(
    project_id: str,
    request: Request,
    severity: Optional[str] = Query(None, description="Фильтр по критичности"),
    category: Optional[str] = Query(None, description="Фильтр по категории"),
    sheet: Optional[str] = Query(None, description="Фильтр по листу"),
    search: Optional[str] = Query(None, description="Полнотекстовый поиск"),
    limit: Optional[int] = Query(None, ge=1, le=500, description="Макс. замечаний"),
    offset: Optional[int] = Query(None, ge=0, description="Смещение"),
    group: bool = Query(False, description="Группировать похожие замечания"),
    version_id: Optional[str] = Query(None, description="Конкретная версия (v1/v2/...), по умолчанию latest"),
):
    """Замечания проекта с фильтрацией и пагинацией.

    opt-in read canary: при `?storage=projects_v2` (или header
    `X-Audit-Storage: projects_v2`) И включённом `AUDIT_PROJECTS_V2_READ_CANARY_ENABLED`
    findings/counts отдаются из projects_v2 (read-only, без silent fallback).
    Без opt-in — legacy как прежде.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_findings(request, project_id)
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
