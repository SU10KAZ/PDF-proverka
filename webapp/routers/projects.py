"""
REST API для проектов.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from webapp.services import project_service, discipline_service, group_service

router = APIRouter(prefix="/api/projects", tags=["projects"])


class RegisterProjectRequest(BaseModel):
    """Запрос на регистрацию проекта из папки projects/."""
    folder: str
    pdf_file: str                          # основной PDF (обратная совместимость)
    pdf_files: list[str] = []              # все PDF (если несколько)
    md_file: Optional[str] = None
    md_files: list[str] = []               # все MD (если несколько)
    name: Optional[str] = None
    section: str = "EOM"
    description: str = ""


# ─── Дисциплины ───

@router.get("/disciplines")
async def list_disciplines():
    """Список поддерживаемых дисциплин для UI."""
    return {"disciplines": discipline_service.get_supported_disciplines()}


class DetectDisciplineRequest(BaseModel):
    folder_name: str
    text_sample: str = ""


class AddDisciplineRequest(BaseModel):
    code: str
    name: str
    color: str = "#666"


@router.post("/detect-discipline")
async def detect_discipline(req: DetectDisciplineRequest):
    """Автодетекция дисциплины по имени папки и/или тексту."""
    code = discipline_service.detect_discipline(req.folder_name, req.text_sample)
    return {"code": code}


@router.post("/disciplines")
async def add_discipline(req: AddDisciplineRequest):
    """Добавить пользовательский раздел."""
    try:
        disc = discipline_service.add_discipline(req.code, req.name, req.color)
        return {"status": "ok", "discipline": disc}
    except ValueError as e:
        raise HTTPException(400, str(e))


class UpdateDisciplineRequest(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None


@router.put("/disciplines/{code}")
async def update_discipline(code: str, req: UpdateDisciplineRequest):
    """Обновить параметры раздела."""
    try:
        disc = discipline_service.update_discipline(code, req.name, req.color)
        return {"status": "ok", "discipline": disc}
    except ValueError as e:
        raise HTTPException(400, str(e))


class ReorderDisciplinesRequest(BaseModel):
    codes: list[str]


@router.post("/disciplines/reorder")
async def reorder_disciplines(req: ReorderDisciplinesRequest):
    """Переупорядочить разделы."""
    discipline_service.reorder_disciplines(req.codes)
    return {"status": "ok"}


@router.delete("/disciplines/{code}")
async def delete_discipline(code: str):
    """Удалить раздел."""
    try:
        discipline_service.delete_discipline(code)
        return {"status": "ok"}
    except ValueError as e:
        raise HTTPException(400, str(e))


# ─── Группы проектов ───

groups_router = APIRouter(prefix="/api/project-groups", tags=["groups"])


@groups_router.get("")
async def list_groups():
    """Все группы всех секций."""
    return {"groups": group_service.load_groups()}


class SaveSectionGroupsRequest(BaseModel):
    groups: list[dict]


@groups_router.put("/{section}")
async def save_section_groups(section: str, req: SaveSectionGroupsRequest):
    """Сохранить группы секции целиком."""
    group_service.save_section_groups(section, req.groups)
    return {"status": "ok"}


@groups_router.delete("/{section}/{group_id}")
async def delete_group(section: str, group_id: str):
    """Удалить одну группу."""
    if not group_service.delete_group(section, group_id):
        raise HTTPException(404, "Group not found")
    return {"status": "ok"}


# ─── Статичные роуты (ПЕРЕД динамическими /{project_id}/...) ───

@router.get("")
async def list_projects():
    """Список всех проектов с их статусом."""
    from webapp.services.object_service import get_current_object
    current_obj = get_current_object()
    object_name = current_obj["name"] if current_obj else "Объект"
    projects = project_service.list_projects()
    return {"projects": [p.model_dump() for p in projects], "object_name": object_name}


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
            pdf_files=req.pdf_files or [req.pdf_file],
            md_file=req.md_file,
            md_files=req.md_files or ([req.md_file] if req.md_file else []),
            name=req.name,
            section=req.section,
            description=req.description,
        )
        return {"status": "ok", "project_info": info}
    except ValueError as e:
        raise HTTPException(400, str(e))


class ScanExternalRequest(BaseModel):
    path: str


class RegisterExternalRequest(BaseModel):
    source_path: str
    pdf_file: str
    pdf_files: list[str] = []
    md_file: Optional[str] = None
    md_files: list[str] = []
    name: Optional[str] = None
    section: str = "EOM"
    description: str = ""


@router.post("/scan-external")
async def scan_external(req: ScanExternalRequest):
    """Сканировать внешнюю папку — найти подпапки с PDF."""
    folders = project_service.scan_external_folder(req.path)
    return {"folders": folders}


@router.post("/register-external")
async def register_external(req: RegisterExternalRequest):
    """Скопировать проект из внешней папки в projects/ и зарегистрировать."""
    try:
        info = project_service.register_external_project(
            source_path=req.source_path,
            pdf_file=req.pdf_file,
            pdf_files=req.pdf_files or [req.pdf_file],
            md_file=req.md_file,
            md_files=req.md_files or ([req.md_file] if req.md_file else []),
            name=req.name,
            section=req.section,
            description=req.description,
        )
        return {"status": "ok", "project_info": info}
    except ValueError as e:
        raise HTTPException(400, str(e))


# ─── Динамические роуты /{project_id}/... ───

@router.get("/{project_id:path}")
async def get_project(project_id: str):
    """Детали одного проекта."""
    status = project_service.get_project_status(project_id)
    if not status:
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    return status.model_dump()


@router.get("/{project_id:path}/config")
async def get_project_config(project_id: str):
    """Сырой project_info.json."""
    info = project_service.get_project_info(project_id)
    if not info:
        raise HTTPException(404, f"project_info.json не найден для '{project_id}'")
    return info


class PipelineVersionRequest(BaseModel):
    pipeline_version: str  # "legacy" | "v4"


@router.put("/{project_id:path}/pipeline-version")
async def set_pipeline_version(project_id: str, req: PipelineVersionRequest):
    """Переключить pipeline_version проекта (legacy / v4).
    Сохраняется в project_info.json.
    """
    if req.pipeline_version not in ("legacy", "v4"):
        raise HTTPException(400, f"Неверный pipeline_version: {req.pipeline_version}. Допустимо: legacy, v4")

    from webapp.services.pipeline_service import pipeline_manager
    if pipeline_manager.is_running(project_id):
        raise HTTPException(409, "Аудит проекта сейчас выполняется. Сначала отмените.")

    info = project_service.get_project_info(project_id)
    if not info:
        raise HTTPException(404, f"project_info.json не найден для '{project_id}'")

    info["pipeline_version"] = req.pipeline_version
    project_service.save_project_info(project_id, info)

    return {"status": "ok", "project_id": project_id, "pipeline_version": req.pipeline_version}


@router.delete("/{project_id:path}/clean")
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
