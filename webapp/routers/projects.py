"""
REST API для проектов.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from webapp.services import project_service, discipline_service

router = APIRouter(prefix="/api/projects", tags=["projects"])


class RegisterProjectRequest(BaseModel):
    """Запрос на регистрацию проекта из папки projects/."""
    folder: str
    pdf_file: str
    md_file: Optional[str] = None
    name: Optional[str] = None
    section: str = "EM"
    description: str = ""


# ─── Дисциплины ───

@router.get("/disciplines")
async def list_disciplines():
    """Список поддерживаемых дисциплин для UI."""
    return {"disciplines": discipline_service.get_supported_disciplines()}


class DetectDisciplineRequest(BaseModel):
    folder_name: str
    text_sample: str = ""


@router.post("/detect-discipline")
async def detect_discipline(req: DetectDisciplineRequest):
    """Автодетекция дисциплины по имени папки и/или тексту."""
    code = discipline_service.detect_discipline(req.folder_name, req.text_sample)
    return {"code": code}


# ─── Статичные роуты (ПЕРЕД динамическими /{project_id}/...) ───

@router.get("")
async def list_projects():
    """Список всех проектов с их статусом."""
    projects = project_service.list_projects()
    return {"projects": [p.model_dump() for p in projects]}


@router.get("/scan")
async def scan_unregistered():
    """Сканировать папку projects/ — найти папки с PDF, но без project_info.json."""
    folders = project_service.scan_unregistered_folders()
    return {"folders": folders}


@router.post("/register")
async def register_project(req: RegisterProjectRequest):
    """Зарегистрировать проект — создать project_info.json для папки из projects/."""
    try:
        info = project_service.register_project(
            folder=req.folder,
            pdf_file=req.pdf_file,
            md_file=req.md_file,
            name=req.name,
            section=req.section,
            description=req.description,
        )
        return {"status": "ok", "project_info": info}
    except ValueError as e:
        raise HTTPException(400, str(e))


# ─── Динамические роуты /{project_id}/... ───

@router.get("/{project_id}")
async def get_project(project_id: str):
    """Детали одного проекта."""
    status = project_service.get_project_status(project_id)
    if not status:
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    return status.model_dump()


@router.get("/{project_id}/config")
async def get_project_config(project_id: str):
    """Сырой project_info.json."""
    info = project_service.get_project_info(project_id)
    if not info:
        raise HTTPException(404, f"project_info.json не найден для '{project_id}'")
    return info


@router.delete("/{project_id}/clean")
async def clean_project(project_id: str):
    """Очистить все результаты аудита (сохраняет PDF, MD, project_info.json).

    Удаляет всю папку _output/ и сбрасывает авто-поля в project_info.json.
    """
    # Проверка что аудит не запущен
    from webapp.services.pipeline_service import pipeline_manager
    if pipeline_manager.is_running(project_id):
        raise HTTPException(409, f"Аудит проекта '{project_id}' сейчас выполняется. Сначала отмените.")

    try:
        result = project_service.clean_project_data(project_id)
        return {"status": "ok", "project_id": project_id, **result}
    except ValueError as e:
        raise HTTPException(404, str(e))
