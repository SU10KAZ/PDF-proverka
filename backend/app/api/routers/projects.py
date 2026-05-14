"""
REST API для проектов.
"""
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from typing import Optional
import backend.app.services.common.project_service as project_service
import backend.app.services.common.discipline_service as discipline_service
import backend.app.services.common.group_service as group_service

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
async def list_groups(object_id: Optional[str] = None):
    """Группы секций для объекта (object_id query param)."""
    return {"groups": group_service.load_groups(object_id)}


class SaveSectionGroupsRequest(BaseModel):
    groups: list[dict]
    object_id: Optional[str] = None


@groups_router.put("/{section}")
async def save_section_groups(section: str, req: SaveSectionGroupsRequest):
    """Сохранить группы секции целиком."""
    group_service.save_section_groups(section, req.groups, req.object_id)
    return {"status": "ok"}


@groups_router.delete("/{section}/{group_id}")
async def delete_group(section: str, group_id: str, object_id: Optional[str] = None):
    """Удалить одну группу."""
    if not group_service.delete_group(section, group_id, object_id):
        raise HTTPException(404, "Group not found")
    return {"status": "ok"}


# ─── Статичные роуты (ПЕРЕД динамическими /{project_id}/...) ───

@router.get("")
async def list_projects():
    """Список всех проектов с их статусом."""
    from backend.app.services.common.object_service import get_current_object
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

@router.get("/{project_id:path}/versions")
async def list_project_versions(project_id: str):
    """Список версий проекта.

    Для legacy-проектов (без project_versions.json) возвращает синтетическую
    единственную версию V1, указывающую на корневую папку проекта.
    """
    from backend.app.services.common import version_service
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    return version_service.get_versions_summary(proj_dir, project_id)


@router.post("/{project_id:path}/versions/ensure-manifest")
async def ensure_versions_manifest(project_id: str):
    """Создать project_versions.json для legacy-проекта (если ещё нет).

    Идемпотентно: если манифест уже есть, возвращает текущий.
    """
    from backend.app.services.common import version_service
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    manifest = version_service.ensure_project_versions_manifest(proj_dir, project_id)
    return {"status": "ok", "project_id": project_id, "manifest": manifest}


class CreateVersionRequest(BaseModel):
    """Запрос на создание новой версии проекта."""
    comment: Optional[str] = None
    label: Optional[str] = None
    source: str = "manual"
    status: str = "new"


@router.post("/{project_id:path}/versions")
async def create_project_version(project_id: str, req: CreateVersionRequest):
    """Создать следующую версию проекта (V2, V3, ...).

    - физически создаёт `_versions/v{N}/_output/`;
    - кладёт минимальный `project_info.json` внутрь новой версии;
    - обновляет `project_versions.json` (latest_version_id → новая версия);
    - НЕ копирует и НЕ переносит существующий `_output` старой версии.
    """
    from backend.app.services.common import version_service
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")

    try:
        new_entry = version_service.create_next_version(
            proj_dir,
            project_id,
            label=req.label,
            source=req.source,
            status=req.status,
            comment=req.comment,
        )
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))

    summary = version_service.get_versions_summary(proj_dir, project_id)
    return {
        "status": "ok",
        "project_id": project_id,
        "version": new_entry,
        "latest_version_id": summary["latest_version_id"],
        "version_count": summary["version_count"],
    }


# ─── Загрузка / просмотр исходных файлов версии ─────────────────────────────


@router.get("/{project_id:path}/versions/{version_id}/files")
async def list_version_files_endpoint(project_id: str, version_id: str):
    """Список исходных файлов конкретной версии проекта.

    Не включает `_output/`, манифест и `project_info.json`. Возвращает также
    текущий `project_info.json` версии.
    """
    from backend.app.services.common import version_service
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    try:
        return version_service.list_version_files(project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))


@router.post("/{project_id:path}/versions/{version_id}/files")
async def upload_version_files_endpoint(
    project_id: str,
    version_id: str,
    files: list[UploadFile] = File(..., description="Файлы для загрузки в версию"),
    replace_existing: bool = Form(False),
    comment: Optional[str] = Form(None),
    allow_v1_upload: bool = Form(False),
):
    """Загрузить исходные PDF/MD-файлы в папку конкретной версии.

    Поведение:
    - V1 запрещён по умолчанию (set `allow_v1_upload=true` для override);
    - неизвестная версия → 404;
    - конфликт имени без `replace_existing` → 409;
    - path traversal / запрещённое расширение → 400;
    - после загрузки `project_info.json` версии обновляется (pdf_files, md_files, updated_at).
    """
    from backend.app.services.common import version_service
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")

    if not files:
        raise HTTPException(400, "Не передан ни один файл")

    # Читаем содержимое заранее — UploadFile поток после ответа закрывается.
    payload: list[tuple[str, bytes]] = []
    for f in files:
        content = await f.read()
        payload.append((f.filename or "", content))

    try:
        result = version_service.save_files_to_version(
            project_id,
            version_id,
            payload,
            replace_existing=replace_existing,
            comment=comment,
            allow_v1_upload=allow_v1_upload,
        )
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    except version_service.VersionUploadForbiddenError as e:
        raise HTTPException(403, str(e))
    except version_service.VersionFileConflictError as e:
        raise HTTPException(409, str(e))
    except version_service.VersionFileError as e:
        raise HTTPException(400, str(e))

    return {"status": "ok", **result}


@router.get("/{project_id:path}")
async def get_project(project_id: str, version_id: Optional[str] = None):
    """Детали одного проекта.

    Query param `version_id` (опционально): получить статус конкретной версии.
    По умолчанию возвращается latest. Для legacy-проектов без манифеста
    допустим только `v1` (это и есть корень проекта).
    """
    from backend.app.services.common import version_service
    # Заранее проверяем валидность version_id, чтобы 404 не путал
    # «проекта нет» и «версии нет».
    if version_id:
        proj_dir = project_service.resolve_project_dir(project_id)
        if not proj_dir.exists():
            raise HTTPException(404, f"Проект '{project_id}' не найден")
        try:
            version_service.get_version_entry(proj_dir, project_id, version_id)
        except version_service.VersionNotFoundError as e:
            raise HTTPException(404, str(e))

    status = project_service.get_project_status(project_id, version_id=version_id)
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
    pipeline_version: str  # "legacy"


@router.put("/{project_id:path}/pipeline-version")
async def set_pipeline_version(project_id: str, req: PipelineVersionRequest):
    """Переключить pipeline_version проекта. Сохраняется в project_info.json."""
    if req.pipeline_version not in ("legacy",):
        raise HTTPException(400, f"Неверный pipeline_version: {req.pipeline_version}. Допустимо: legacy")

    # Смена pipeline_version влияет только на следующий запуск аудита: текущий
    # прогон держит project_info в памяти и файл не перечитывает, поэтому
    # блокировать запись во время аудита не нужно.

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
    from backend.app.pipeline.manager import pipeline_manager
    if pipeline_manager.is_running(project_id):
        raise HTTPException(409, f"Аудит проекта '{project_id}' сейчас выполняется. Сначала отмените.")

    try:
        result = project_service.clean_project_data(project_id)
        return {"status": "ok", "project_id": project_id, **result}
    except ValueError as e:
        raise HTTPException(404, str(e))
