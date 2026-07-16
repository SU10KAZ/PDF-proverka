"""
REST API для модуля оптимизации проектных решений.
"""
import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from backend.app.pipeline.manager import pipeline_manager
import backend.app.services.common.project_service as project_service
from backend.app.services.common import version_service
from backend.app.services.common.project_service import resolve_project_dir

router = APIRouter(prefix="/api/optimization", tags=["optimization"])


class SectionReplicationStartRequest(BaseModel):
    signal_id: str = Field(min_length=1, max_length=80)
    target_project_ids: Optional[list[str]] = None



def _normalize_version_query(version_id: Optional[str]) -> Optional[str]:
    return version_id if isinstance(version_id, str) and version_id else None

def _production_uses_v2() -> bool:
    try:
        from backend.app.services.storage.storage_read_facade import production_uses_v2
        return production_uses_v2()
    except Exception:
        return False


def _read_v2_optimization(project_id: str, version_id: Optional[str] = None):
    from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

    version_id = _normalize_version_query(version_id)
    adapter = ProjectsV2Adapter()
    if not adapter.is_available():
        raise FileNotFoundError(f"projects_v2 root not available: {adapter.objects_root}")
    doc = adapter.find_document_by_project_id(project_id)
    if doc is None:
        return None, version_id
    vid = adapter.resolve_version_id(doc, version_id)
    if not vid:
        return None, version_id
    data = adapter.read_analysis_artifact(Path(doc["doc_dir"]), vid, "optimization.json")
    return data, vid

def _resolve_version_output(project_id: str, version_id: Optional[str]) -> Path:
    """Резолв `_output` нужной версии + 404 на невалидный version_id."""
    version_id = _normalize_version_query(version_id)
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


def _section_code_or_400(section_code: str) -> str:
    code = (section_code or "").strip().upper()
    if not code or len(code) > 32 or not all(ch.isalnum() or ch in "_-" for ch in code):
        raise HTTPException(400, "Недопустимый код раздела")
    return code


@router.post("/section/{section_code}/pipeline/run")
async def start_section_optimization_pipeline(
    section_code: str,
    object_id: Optional[str] = Query(None, description="Объект, выбранный в интерфейсе"),
):
    """Запустить реальные этапы «Сбор — Нормализация — Синтез» в фоне."""
    code = _section_code_or_400(section_code)
    from backend.app.services.section_optimization_pipeline_service import (
        SectionPipelineConflict,
        start_pipeline,
    )
    try:
        return {"status": "started", "pipeline": start_pipeline(code, object_id=object_id)}
    except SectionPipelineConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/section/{section_code}/pipeline/status")
async def get_section_optimization_pipeline_status(
    section_code: str,
    object_id: Optional[str] = Query(None, description="Объект, выбранный в интерфейсе"),
):
    """Вернуть сохраняемый статус этапов section-level pipeline."""
    code = _section_code_or_400(section_code)
    from backend.app.services.section_optimization_pipeline_service import get_pipeline_state
    try:
        return get_pipeline_state(code, object_id=object_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/section/{section_code}/pipeline/graphics-plan")
async def create_section_optimization_graphics_plan(
    section_code: str,
    object_id: Optional[str] = Query(None, description="Объект, выбранный в интерфейсе"),
):
    """Точечно подготовить план проверки графики, без автозапуска vision/LLM."""
    code = _section_code_or_400(section_code)
    from backend.app.services.section_optimization_pipeline_service import (
        SectionPipelineConflict,
        SectionPipelineNotFound,
        request_graphics_plan,
    )
    try:
        return {"status": "ready", "pipeline": request_graphics_plan(code, object_id=object_id)}
    except SectionPipelineNotFound as exc:
        raise HTTPException(409, str(exc)) from exc
    except SectionPipelineConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/section/{section_code}/replications/start")
async def start_section_replication(
    section_code: str,
    payload: SectionReplicationStartRequest,
    object_id: Optional[str] = Query(None, description="Объект, выбранный в интерфейсе"),
):
    """Зафиксировать кандидат и запустить подготовку досье тиражирования."""
    code = _section_code_or_400(section_code)
    from backend.app.services.section_optimization_replication_service import (
        SectionReplicationConflict,
        SectionReplicationNotFound,
        start_replication,
    )
    try:
        return {
            "status": "started",
            "replication": start_replication(
                code,
                payload.signal_id,
                object_id=object_id,
                target_project_ids=payload.target_project_ids,
            ),
        }
    except SectionReplicationConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except SectionReplicationNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/section/{section_code}/replications/start-all")
async def start_all_section_replications(
    section_code: str,
    object_id: Optional[str] = Query(None, description="Объект, выбранный в интерфейсе"),
):
    """Одной командой запустить все ещё не подготовленные тиражирования раздела."""
    code = _section_code_or_400(section_code)
    from backend.app.services.section_optimization_replication_service import (
        SectionReplicationNotFound,
        start_all_replications,
    )
    try:
        result = start_all_replications(code, object_id=object_id)
        return {
            "status": "started" if result["started_count"] else "nothing_to_start",
            **result,
        }
    except SectionReplicationNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/section/{section_code}/replications")
async def get_section_replications(
    section_code: str,
    object_id: Optional[str] = Query(None, description="Объект, выбранный в интерфейсе"),
):
    """Список сохраняемых процессов тиражирования раздела."""
    code = _section_code_or_400(section_code)
    from backend.app.services.section_optimization_replication_service import list_replications
    try:
        return {"replications": list_replications(code, object_id=object_id)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/section/{section_code}/replications/{replication_id}")
async def get_section_replication(
    section_code: str,
    replication_id: str,
    object_id: Optional[str] = Query(None, description="Объект, выбранный в интерфейсе"),
    include_dossier: bool = Query(False),
):
    """Статус процесса; полное досье отдаётся только по явному запросу."""
    code = _section_code_or_400(section_code)
    from backend.app.services.section_optimization_replication_service import (
        SectionReplicationNotFound,
        get_replication,
    )
    try:
        return get_replication(
            code,
            replication_id,
            object_id=object_id,
            include_dossier=include_dossier,
        )
    except SectionReplicationNotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/section/{section_code:path}")
async def get_section_optimization(
    section_code: str,
    object_id: Optional[str] = Query(None, description="Объект, выбранный в интерфейсе"),
):
    """Общая спецификация и принятые оптимизации всех проектов раздела.

    Эндпоинт read-only. Тяжёлое чтение Markdown выполняется вне event loop,
    чтобы открытие сводной карточки не задерживало остальные запросы портала.
    """
    code = _section_code_or_400(section_code)
    from backend.app.services.section_optimization_service import build_section_optimization
    from backend.app.services.section_optimization_pipeline_service import (
        get_latest_snapshot,
        get_pipeline_state,
        store_latest_snapshot,
    )
    from backend.app.services.section_optimization_replication_service import list_replications

    try:
        pipeline = get_pipeline_state(code, object_id=object_id)
        snapshot = get_latest_snapshot(code, object_id=object_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    data = snapshot
    if data is None:
        data = await run_in_threadpool(build_section_optimization, code, object_id=object_id)
        store_latest_snapshot(code, data, object_id=object_id)
        pipeline = get_pipeline_state(code, object_id=object_id)
    if data["meta"]["project_count"] == 0:
        raise HTTPException(404, f"В разделе '{code}' нет проектов")
    stages = list(data.get("analysis_stages") or [])
    if not any(stage.get("key") == "agent" for stage in stages):
        insert_at = next(
            (index for index, stage in enumerate(stages) if stage.get("key") == "graphics"),
            len(stages),
        )
        stages.insert(insert_at, {
            "key": "agent",
            "title": "Умный агент",
            "description": "Инженерная оценка применимости решения отдельно для каждого целевого проекта.",
        })
        data["analysis_stages"] = stages
    data["pipeline"] = pipeline
    data["replications"] = list_replications(code, object_id=object_id)
    return data


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
    version_id = _normalize_version_query(version_id)
    status = project_service.get_project_status(project_id, version_id=version_id)
    if not status:
        raise HTTPException(404, f"Проект '{project_id}' не найден")

    job = pipeline_manager.get_job(project_id)
    is_running = (
        job is not None
        and job.stage.value == "optimization"
        and job.status.value == "running"
    )

    if _production_uses_v2():
        try:
            data, resolved_vid = _read_v2_optimization(project_id, version_id)
            return {
                "project_id": project_id,
                "version_id": resolved_vid or status.version_id,
                "pipeline_status": status.pipeline.optimization,
                "is_running": is_running,
                "has_results": bool(data),
            }
        except Exception as exc:
            print(f"[projects_v2 read] optimization status fallback to legacy: {exc}")

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
    version_id = _normalize_version_query(version_id)
    if _production_uses_v2():
        try:
            data, resolved_vid = _read_v2_optimization(project_id, version_id)
            if data is None:
                return {"project_id": project_id, "version_id": resolved_vid, "has_data": False, "data": None}
            return {"project_id": project_id, "version_id": resolved_vid, "has_data": True, "data": data}
        except Exception as exc:
            print(f"[projects_v2 read] optimization payload fallback to legacy: {exc}")

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
