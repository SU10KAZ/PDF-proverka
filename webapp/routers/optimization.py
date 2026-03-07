"""
REST API для модуля оптимизации проектных решений.
"""
import json
from pathlib import Path
from fastapi import APIRouter, HTTPException
from webapp.services.pipeline_service import pipeline_manager
from webapp.services import project_service
from webapp.config import PROJECTS_DIR

router = APIRouter(prefix="/api/optimization", tags=["optimization"])


@router.post("/{project_id}/run")
async def start_optimization(project_id: str):
    """Запустить анализ оптимизации проектной документации."""
    _check_project(project_id)
    try:
        job = await pipeline_manager.start_optimization(project_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.get("/{project_id}")
async def get_optimization(project_id: str):
    """Получить результаты оптимизации (optimization.json)."""
    opt_path = PROJECTS_DIR / project_id / "_output" / "optimization.json"
    if not opt_path.exists():
        return {"project_id": project_id, "has_data": False, "data": None}

    try:
        with open(opt_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {"project_id": project_id, "has_data": True, "data": data}
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(500, f"Ошибка чтения optimization.json: {e}")


@router.get("/{project_id}/status")
async def get_optimization_status(project_id: str):
    """Статус оптимизации проекта."""
    status = project_service.get_project_status(project_id)
    if not status:
        raise HTTPException(404, f"Проект '{project_id}' не найден")

    job = pipeline_manager.get_job(project_id)
    is_running = (
        job is not None
        and job.stage.value == "optimization"
        and job.status.value == "running"
    )

    opt_path = PROJECTS_DIR / project_id / "_output" / "optimization.json"
    has_results = opt_path.exists() and opt_path.stat().st_size > 100

    return {
        "project_id": project_id,
        "pipeline_status": status.pipeline.optimization,
        "is_running": is_running,
        "has_results": has_results,
    }


@router.delete("/{project_id}/cancel")
async def cancel_optimization(project_id: str):
    """Отменить запущенную оптимизацию."""
    success = await pipeline_manager.cancel(project_id)
    if not success:
        raise HTTPException(404, f"Нет запущенной задачи для '{project_id}'")
    return {"status": "cancelled"}


def _check_project(project_id: str):
    """Проверка существования проекта."""
    status = project_service.get_project_status(project_id)
    if not status:
        raise HTTPException(404, f"Проект '{project_id}' не найден")
