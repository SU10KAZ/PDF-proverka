"""
Compatibility-router для legacy webapp.main:app: проксирует к новому
`backend.app.services.common.version_service` те эндпоинты, которых у legacy
roouters нет. Бизнес-логику не дублирует — только тонкие FastAPI-обёртки.

Подключать ДО webapp.routers.projects.router, чтобы catch-all `GET /{project_id:path}`
не перехватил `/{project_id}/versions/...`.

Endpoints:
    GET    /api/projects/{project_id}/versions
    POST   /api/projects/{project_id}/versions
    POST   /api/projects/{project_id}/versions/ensure-manifest
    GET    /api/projects/{project_id}/versions/{version_id}/files
    POST   /api/projects/{project_id}/versions/{version_id}/files
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from backend.app.services.common import project_service, version_service

router = APIRouter(
    prefix="/api/projects",
    tags=["version_compat"],
)


class CreateVersionRequest(BaseModel):
    """Тело POST /versions."""
    comment: Optional[str] = None
    label: Optional[str] = None
    source: str = "manual"
    status: str = "new"


@router.get("/{project_id:path}/versions")
async def list_project_versions(project_id: str):
    """Список версий проекта (для legacy-проектов синтезирует V1)."""
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    return version_service.get_versions_summary(proj_dir, project_id)


@router.post("/{project_id:path}/versions/ensure-manifest")
async def ensure_versions_manifest(project_id: str):
    """Создать project_versions.json для legacy-проекта (идемпотентно)."""
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    manifest = version_service.ensure_project_versions_manifest(proj_dir, project_id)
    return {"status": "ok", "project_id": project_id, "manifest": manifest}


@router.post("/{project_id:path}/versions")
async def create_project_version(project_id: str, req: CreateVersionRequest):
    """Создать V{N+1} проекта (физически создаёт `_versions/v{N+1}/_output/`).

    `_output` старой версии не копируется.
    """
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


@router.get("/{project_id:path}/versions/{version_id}/files")
async def list_version_files_endpoint(project_id: str, version_id: str):
    """Список исходных файлов конкретной версии (без _output/)."""
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
    """Загрузить PDF/MD в папку версии.

    V1 запрещён без `allow_v1_upload=true`. Conflict (имя занято) → 409.
    Path traversal / запрещённое расширение → 400.
    """
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")

    if not files:
        raise HTTPException(400, "Не передан ни один файл")

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
