"""
REST API для модуля оптимизации проектных решений.
"""
import json
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from webapp.services.pipeline_service import pipeline_manager
from webapp.services import project_service
from webapp.services.project_service import resolve_project_dir

router = APIRouter(prefix="/api/optimization", tags=["optimization"])


def _reject_v2_for_legacy_runner(version_id: Optional[str]) -> None:
    """Safety-gate: legacy pipeline_manager не умеет работать с V2+.

    См. webapp/routers/audit.py: same gate, same message. Дублируем рядом,
    чтобы не вытаскивать helper в отдельный модуль ради одного use case.
    `None` и 'v1' пропускаем — это default legacy-поведение.
    """
    if version_id is None:
        return
    if str(version_id).strip().lower() == "v1":
        return
    raise HTTPException(
        409,
        f"Запуск оптимизации версии '{version_id}' временно недоступен в "
        f"legacy runner. Версия создана и файлы загружены, но проверку V2+ "
        f"нужно запускать через version-aware backend runner "
        f"(backend.app.main:app)."
    )


@router.get("/summary/all")
async def get_all_optimization_summaries():
    """Сводка оптимизаций по всем проектам."""
    from webapp.services.findings_service import get_all_optimization_summaries as _get_all
    summaries = _get_all()
    return {"summaries": summaries}


@router.post("/{project_id:path}/run")
async def start_optimization(
    project_id: str,
    version_id: Optional[str] = Query(None, description="Версия (v1/v2/...). V2+ временно не поддерживается legacy runner'ом"),
):
    """Запустить анализ оптимизации проектной документации."""
    _reject_v2_for_legacy_runner(version_id)
    _check_project(project_id)
    try:
        job = await pipeline_manager.start_optimization(project_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.get("/{project_id:path}/block-map")
async def get_optimization_block_map(project_id: str):
    """Маппинг optimization_id → [block_ids] для подсветки блоков."""
    from webapp.services.findings_service import get_optimization_block_map as _get_map
    result = _get_map(project_id)
    if result is None:
        raise HTTPException(404, f"Данные оптимизации не найдены для '{project_id}'")
    return result


@router.get("/{project_id:path}/status")
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

    opt_path = resolve_project_dir(project_id) / "_output" / "optimization.json"
    has_results = opt_path.exists() and opt_path.stat().st_size > 100

    return {
        "project_id": project_id,
        "pipeline_status": status.pipeline.optimization,
        "is_running": is_running,
        "has_results": has_results,
    }


@router.get("/{project_id:path}")
async def get_optimization(project_id: str):
    """Получить результаты оптимизации (optimization.json)."""
    opt_path = resolve_project_dir(project_id) / "_output" / "optimization.json"
    if not opt_path.exists():
        return {"project_id": project_id, "has_data": False, "data": None}

    try:
        with open(opt_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {"project_id": project_id, "has_data": True, "data": data}
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(500, f"Ошибка чтения optimization.json: {e}")


@router.delete("/{project_id:path}/cancel")
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
