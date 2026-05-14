"""
REST API для модуля оптимизации проектных решений.
"""
import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.app.pipeline.manager import pipeline_manager
import backend.app.services.common.project_service as project_service
from backend.app.services.common import version_service
from backend.app.services.common.project_service import resolve_project_dir

router = APIRouter(prefix="/api/optimization", tags=["optimization"])


def _resolve_version_output(project_id: str, version_id: Optional[str]) -> Path:
    """Резолв `_output` нужной версии + 404 на невалидный version_id."""
    try:
        return version_service.resolve_version_output_dir(project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    except FileNotFoundError:
        raise HTTPException(404, f"Проект '{project_id}' не найден")


@router.get("/summary/all")
async def get_all_optimization_summaries():
    """Сводка оптимизаций по всем проектам."""
    from backend.app.services.findings.findings_service import get_all_optimization_summaries as _get_all
    summaries = _get_all()
    return {"summaries": summaries}


@router.post("/{project_id:path}/run")
async def start_optimization(
    project_id: str,
    version_id: Optional[str] = Query(None, description="Версия проекта, по умолчанию latest"),
):
    """Запустить анализ оптимизации проектной документации (для нужной версии)."""
    _check_project(project_id, version_id)
    try:
        job = await pipeline_manager.start_optimization(project_id, version_id=version_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.get("/{project_id:path}/block-map")
async def get_optimization_block_map(
    project_id: str,
    version_id: Optional[str] = Query(None),
):
    """Маппинг optimization_id → [block_ids] для подсветки блоков."""
    from backend.app.services.findings.findings_service import get_optimization_block_map as _get_map
    # validate version_id existence
    _resolve_version_output(project_id, version_id) if version_id else None
    result = _get_map(project_id, version_id=version_id)
    if result is None:
        raise HTTPException(404, f"Данные оптимизации не найдены для '{project_id}'")
    return result


@router.get("/{project_id:path}/status")
async def get_optimization_status(
    project_id: str,
    version_id: Optional[str] = Query(None),
):
    """Статус оптимизации проекта."""
    status = project_service.get_project_status(project_id, version_id=version_id)
    if not status:
        raise HTTPException(404, f"Проект '{project_id}' не найден")

    job = pipeline_manager.get_job(project_id)
    is_running = (
        job is not None
        and job.stage.value == "optimization"
        and job.status.value == "running"
    )

    output_dir = _resolve_version_output(project_id, version_id)
    opt_path = output_dir / "optimization.json"
    has_results = opt_path.exists() and opt_path.stat().st_size > 100

    return {
        "project_id": project_id,
        "version_id": status.version_id,
        "pipeline_status": status.pipeline.optimization,
        "is_running": is_running,
        "has_results": has_results,
    }


@router.get("/{project_id:path}")
async def get_optimization(
    project_id: str,
    version_id: Optional[str] = Query(None, description="Конкретная версия, по умолчанию latest"),
):
    """Получить результаты оптимизации (optimization.json) для указанной версии."""
    output_dir = _resolve_version_output(project_id, version_id)
    opt_path = output_dir / "optimization.json"
    if not opt_path.exists():
        return {"project_id": project_id, "version_id": version_id, "has_data": False, "data": None}

    try:
        with open(opt_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {"project_id": project_id, "version_id": version_id, "has_data": True, "data": data}
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(500, f"Ошибка чтения optimization.json: {e}")


@router.delete("/{project_id:path}/cancel")
async def cancel_optimization(project_id: str):
    """Отменить запущенную оптимизацию."""
    success = await pipeline_manager.cancel(project_id)
    if not success:
        raise HTTPException(404, f"Нет запущенной задачи для '{project_id}'")
    return {"status": "cancelled"}


def _check_project(project_id: str, version_id: Optional[str] = None):
    """Проверка существования проекта и (опционально) валидности версии.

    Запуск оптимизации требует, чтобы у нужной версии были PDF/MD-исходники.
    """
    status = project_service.get_project_status(project_id, version_id=version_id)
    if not status:
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    if version_id:
        proj_dir = project_service.resolve_project_dir(project_id)
        try:
            version_service.get_version_entry(proj_dir, project_id, version_id)
        except version_service.VersionNotFoundError as e:
            raise HTTPException(404, str(e))

    effective_vid = version_id or status.version_id
    readiness = version_service.version_audit_readiness(project_id, effective_vid)
    if not readiness["can_run_audit"]:
        if effective_vid in (None, "v1"):
            raise HTTPException(
                400, f"В проекте '{project_id}' отсутствует PDF файл"
            )
        raise HTTPException(
            409,
            f"В версии '{effective_vid}' проекта '{project_id}' нет исходных "
            f"PDF/MD файлов. Загрузите их через POST /api/projects/{{id}}/"
            f"versions/{effective_vid}/files перед запуском оптимизации."
        )
