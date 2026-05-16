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
    POST   /api/projects/{project_id}/versions/from-candidate
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from backend.app.services.common import project_service, version_service

router = APIRouter(
    prefix="/api/projects",
    tags=["version_compat"],
)


# ─── Flat endpoints (target_project_id в body) ─────────────────────────────
# Эти роуты определены ПЕРЕД динамическими `/{project_id:path}/...`, чтобы
# их перехватывал FastAPI первым. Главное: они не содержат project_id в URL,
# что важно для project_id со слешами (`KJ/M31A`) — иначе `%2F` блокируется
# Cloudflare/прокси с ответом 405 Method Not Allowed.

class FlatVersionFromProjectRequest(BaseModel):
    target_project_id: str
    source_project_id: str
    comment: Optional[str] = None
    source: str = "edit_projects_modal"
    delete_source: bool = True
    discard_source_output: bool = False


@router.post("/versions/from-project")
async def flat_create_version_from_project(req: FlatVersionFromProjectRequest):
    """Flat-вариант POST /{target}/versions/from-project: target в body."""
    from backend.app.services.common import version_service as _vs

    src_dir = project_service.resolve_project_dir(req.source_project_id)
    tgt_dir = project_service.resolve_project_dir(req.target_project_id)
    if not src_dir.exists():
        raise HTTPException(404, f"Source проект '{req.source_project_id}' не найден")
    if not tgt_dir.exists():
        raise HTTPException(404, f"Target проект '{req.target_project_id}' не найден")

    try:
        from webapp.services.pipeline_service import pipeline_manager as _pm
        if _pm.is_running(req.source_project_id):
            raise HTTPException(
                409,
                f"Аудит source проекта '{req.source_project_id}' выполняется. Сначала отмените.",
            )
        if _pm.is_running(req.target_project_id):
            raise HTTPException(
                409,
                f"Target проект '{req.target_project_id}' сейчас находится в обработке. "
                f"Привязка версии невозможна, пока активный аудит не завершится.",
            )
    except (ImportError, AttributeError):
        pass

    try:
        result = _vs.merge_project_as_version(
            req.source_project_id,
            req.target_project_id,
            comment=req.comment,
            source=req.source,
            delete_source=req.delete_source,
            discard_source_output=req.discard_source_output,
        )
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except _vs.SourceOutputNotEmptyError as e:
        raise HTTPException(
            409,
            {
                "code": "source_output_not_empty",
                "message": str(e),
                "needs_flag": "discard_source_output",
            },
        )
    except _vs.VersionFileConflictError as e:
        raise HTTPException(409, str(e))
    except _vs.VersionFileError as e:
        raise HTTPException(400, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))

    return result


class FlatVersionFromCandidateRequest(BaseModel):
    target_project_id: str
    candidate_pdf_path: str
    candidate_md_path: Optional[str] = None
    candidate_result_json_path: Optional[str] = None
    candidate_extra_paths: list[str] = []
    expected_section: Optional[str] = None
    external_root: Optional[str] = None
    comment: Optional[str] = None
    source: str = "section_add_project_modal"


@router.post("/versions/from-candidate")
async def flat_create_version_from_candidate(req: FlatVersionFromCandidateRequest):
    """Flat-вариант POST /{target}/versions/from-candidate: target в body."""
    from backend.app.services.common import version_service as _vs

    tgt_dir = project_service.resolve_project_dir(req.target_project_id)
    if not tgt_dir.exists():
        raise HTTPException(404, f"Проект '{req.target_project_id}' не найден")

    allowed_roots: list = [project_service._get_projects_dir()]
    if req.external_root:
        try:
            ext = Path(req.external_root).expanduser().resolve(strict=True)
        except (OSError, RuntimeError):
            raise HTTPException(400, f"external_root не существует: {req.external_root!r}")
        if not ext.is_dir():
            raise HTTPException(400, f"external_root не является папкой: {req.external_root!r}")
        allowed_roots.append(ext)

    try:
        result = _vs.create_version_from_existing_files(
            req.target_project_id,
            candidate_files={
                "pdf": req.candidate_pdf_path,
                "md": req.candidate_md_path,
                "result_json": req.candidate_result_json_path,
                "extra": req.candidate_extra_paths,
            },
            expected_section=req.expected_section,
            comment=req.comment,
            source=req.source,
            allowed_roots=allowed_roots,
        )
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except _vs.VersionFileConflictError as e:
        raise HTTPException(409, str(e))
    except _vs.VersionFileError as e:
        raise HTTPException(400, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))

    return result


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


class VersionFromCandidateRequest(BaseModel):
    """Создать новую версию у существующего проекта из найденных файлов."""
    candidate_pdf_path: str
    candidate_md_path: Optional[str] = None
    candidate_result_json_path: Optional[str] = None
    candidate_extra_paths: list[str] = []
    expected_section: Optional[str] = None
    external_root: Optional[str] = None
    comment: Optional[str] = None
    source: str = "section_add_project_modal"


@router.post("/{target_project_id:path}/versions/from-candidate")
async def create_version_from_candidate(
    target_project_id: str,
    req: VersionFromCandidateRequest,
):
    """Создать V{N+1} target-проекта из уже найденных PDF/MD/result.json."""
    proj_dir = project_service.resolve_project_dir(target_project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{target_project_id}' не найден")

    # PROJECTS_DIR через project_service — поддерживает monkeypatch в тестах.
    allowed_roots: list = [project_service._get_projects_dir()]
    if req.external_root:
        try:
            ext = Path(req.external_root).expanduser().resolve(strict=True)
        except (OSError, RuntimeError):
            raise HTTPException(400, f"external_root не существует: {req.external_root!r}")
        if not ext.is_dir():
            raise HTTPException(400, f"external_root не является папкой: {req.external_root!r}")
        allowed_roots.append(ext)

    try:
        result = version_service.create_version_from_existing_files(
            target_project_id,
            candidate_files={
                "pdf": req.candidate_pdf_path,
                "md": req.candidate_md_path,
                "result_json": req.candidate_result_json_path,
                "extra": req.candidate_extra_paths,
            },
            expected_section=req.expected_section,
            comment=req.comment,
            source=req.source,
            allowed_roots=allowed_roots,
        )
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except version_service.VersionFileConflictError as e:
        raise HTTPException(409, str(e))
    except version_service.VersionFileError as e:
        raise HTTPException(400, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))

    return result


class VersionFromProjectRequest(BaseModel):
    """Слить source-проект в target как новую версию (V{N+1})."""
    source_project_id: str
    comment: Optional[str] = None
    source: str = "edit_projects_modal"
    delete_source: bool = True
    discard_source_output: bool = False


@router.post("/{target_project_id:path}/versions/from-project")
async def create_version_from_project(
    target_project_id: str,
    req: VersionFromProjectRequest,
):
    """Слить source-проект в target как новую версию."""
    src_dir = project_service.resolve_project_dir(req.source_project_id)
    tgt_dir = project_service.resolve_project_dir(target_project_id)
    if not src_dir.exists():
        raise HTTPException(404, f"Source проект '{req.source_project_id}' не найден")
    if not tgt_dir.exists():
        raise HTTPException(404, f"Target проект '{target_project_id}' не найден")

    try:
        from webapp.services.pipeline_service import pipeline_manager as _pm
        if _pm.is_running(req.source_project_id):
            raise HTTPException(
                409,
                f"Аудит source проекта '{req.source_project_id}' выполняется. Сначала отмените.",
            )
        if _pm.is_running(target_project_id):
            raise HTTPException(
                409,
                f"Target проект '{target_project_id}' сейчас находится в обработке. "
                f"Привязка версии невозможна, пока активный аудит не завершится.",
            )
    except (ImportError, AttributeError):
        pass

    try:
        result = version_service.merge_project_as_version(
            req.source_project_id,
            target_project_id,
            comment=req.comment,
            source=req.source,
            delete_source=req.delete_source,
            discard_source_output=req.discard_source_output,
        )
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except version_service.SourceOutputNotEmptyError as e:
        raise HTTPException(
            409,
            {
                "code": "source_output_not_empty",
                "message": str(e),
                "needs_flag": "discard_source_output",
            },
        )
    except version_service.VersionFileConflictError as e:
        raise HTTPException(409, str(e))
    except version_service.VersionFileError as e:
        raise HTTPException(400, str(e))
    except ValueError as e:
        raise HTTPException(400, str(e))

    return result
