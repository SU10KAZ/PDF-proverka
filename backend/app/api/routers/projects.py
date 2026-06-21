"""
REST API для проектов.
"""
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
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
async def list_projects(request: Request):
    """Список всех проектов с их статусом.

    opt-in read canary: при `?storage=projects_v2` (или header
    `X-Audit-Storage: projects_v2`) И включённом `AUDIT_PROJECTS_V2_READ_CANARY_ENABLED`
    список отдаётся из projects_v2 (read-only). Без opt-in — legacy как прежде.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_projects_list()
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


@router.post("/upload-folder")
async def upload_folder(
    discipline: str = Form(...),
    project_name: str = Form(...),
    object_id: Optional[str] = Form(None),
    object_name: Optional[str] = Form(None),
    description: str = Form(""),
    upload_mode: str = Form("new_project"),
    target_project_id: Optional[str] = Form(None),
    files: list[UploadFile] = File(..., description="Файлы папки проекта"),
):
    """Загрузить папку проекта с компьютера инженера (browser folder upload).

    `upload_mode=new_project` (default) — создать новый проект. `new_version` —
    добавить новую версию существующего `target_project_id` (в этом же объекте/
    разделе) через проверенный version-flow (без orphan, с пере-зеркалированием
    target в projects_v2). Сначала legacy (авторитетно), затем dual_write_shadow.
    """
    oid = object_id
    if not oid and object_name:
        from backend.app.services.common import object_service
        for o in object_service.list_objects():
            if o.get("name") == object_name:
                oid = o.get("id")
                break
    if not oid:
        raise HTTPException(422, "Не указан объект (object_id или object_name)")
    if not files:
        raise HTTPException(422, "Не передан ни один файл")

    if upload_mode == "new_version":
        if not target_project_id:
            raise HTTPException(422, "Не указан target_project_id для режима new_version")
        from backend.app.pipeline.manager import pipeline_manager
        if pipeline_manager.is_running(target_project_id):
            raise HTTPException(409, f"Проект-основание «{target_project_id}» сейчас в аудите. Сначала отмените.")

    payload: list[tuple[str, bytes]] = []
    for f in files:
        payload.append((f.filename or "", await f.read()))

    try:
        result = project_service.save_uploaded_project_folder(
            object_id=oid,
            discipline=discipline,
            project_name=project_name,
            files=payload,
            description=description,
            upload_mode=upload_mode,
            target_project_id=target_project_id,
        )
    except project_service.UploadFolderConflict as e:
        raise HTTPException(409, str(e))
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except project_service.UploadFolderError as e:
        raise HTTPException(422, str(e))
    return {"status": "ok", **result}


@router.post("/upload-folder/precheck")
async def upload_folder_precheck(
    project_name: str = Form(...),
    discipline: Optional[str] = Form(None),
    object_id: Optional[str] = Form(None),
    object_name: Optional[str] = Form(None),
    folder_name: Optional[str] = Form(None),
    files: list[UploadFile] = File(..., description="Файлы папки проекта"),
):
    """Dry-run проверка папки перед загрузкой — НИЧЕГО не пишет.

    Если `discipline` не передана — определяется автоматически (имя папки → имя
    PDF → текст document.md → fallback). Возвращает verdict (ready|warning|
    duplicate|error) + blocks[]/warnings[] + fingerprint + detected_discipline/
    source + suggested_target_project/version (предложение версии). Backend всё
    равно повторяет hard-проверки при фактической загрузке (race guard).
    """
    oid = object_id
    if not oid and object_name:
        from backend.app.services.common import object_service
        for o in object_service.list_objects():
            if o.get("name") == object_name:
                oid = o.get("id")
                break
    if not oid:
        raise HTTPException(422, "Не указан объект (object_id или object_name)")
    if not files:
        raise HTTPException(422, "Не передан ни один файл")

    payload: list[tuple[str, bytes]] = []
    for f in files:
        payload.append((f.filename or "", await f.read()))

    verdict = project_service.precheck_uploaded_project_folder(
        object_id=oid, discipline=discipline, project_name=project_name,
        files=payload, folder_name=folder_name,
    )
    return {"status": "ok", "precheck": verdict}


# ─── Flat endpoints (target_project_id в body) — ДО динамических роутов ───
# Эти роуты нужны для проектов со слешами в project_id (`KJ/M31A`): URL-форма
# `/{p:path}/versions/from-project` после encodeURIComponent даёт `%2F`,
# который блокируется Cloudflare/прокси с ответом 405. Поэтому target_project_id
# передаём в теле, а URL остаётся flat.

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
    from backend.app.pipeline.manager import pipeline_manager

    src_dir = project_service.resolve_project_dir(req.source_project_id)
    tgt_dir = project_service.resolve_project_dir(req.target_project_id)
    if not src_dir.exists():
        raise HTTPException(404, f"Source проект '{req.source_project_id}' не найден")
    if not tgt_dir.exists():
        raise HTTPException(404, f"Target проект '{req.target_project_id}' не найден")

    try:
        if pipeline_manager.is_running(req.source_project_id):
            raise HTTPException(
                409,
                f"Аудит source проекта '{req.source_project_id}' выполняется. Сначала отмените.",
            )
        if pipeline_manager.is_running(req.target_project_id):
            raise HTTPException(
                409,
                f"Target проект '{req.target_project_id}' сейчас находится в обработке. "
                f"Привязка версии невозможна, пока активный аудит не завершится.",
            )
    except AttributeError:
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


# ─── Динамические роуты /{project_id}/... ───

@router.get("/{project_id:path}/versions")
async def list_project_versions(project_id: str, request: Request):
    """Список версий проекта.

    Для legacy-проектов (без project_versions.json) возвращает синтетическую
    единственную версию V1, указывающую на корневую папку проекта.

    opt-in read canary: `?storage=projects_v2` (или header) + флаг → versions
    из projects_v2 (read-only). Без opt-in — legacy как прежде.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_project_versions(request, project_id)
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

    # Промоут в контейнер мог переместить папку V1 → перерезолвим её.
    proj_dir = project_service.resolve_project_dir(project_id)
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
async def list_version_files_endpoint(project_id: str, version_id: str, request: Request):
    """Список исходных файлов конкретной версии проекта.

    Не включает `_output/`, манифест и `project_info.json`. Возвращает также
    текущий `project_info.json` версии.

    opt-in/default read canary: при v2-backend список берётся из projects_v2
    01_input (read-only). `?storage=legacy` форсит legacy.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_version_files(request, project_id, version_id)
    from backend.app.services.common import version_service
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    try:
        return version_service.list_version_files(project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))


@router.delete("/{project_id:path}/versions/{version_id}")
async def delete_project_version(project_id: str, version_id: str):
    """Удалить версию проекта: папку с диска + запись из манифеста.

    Нельзя удалить единственную версию. После удаления latest переключается
    на предыдущую. Записи decisions_log, ссылающиеся на удалённые findings,
    станут orphan-записями (очищаются через KB-утилиты).

    Операция работает на legacy-сторе (источник истины при
    READ_DEFAULT_ENABLED=false). projects_v2-shadow остаётся теневой копией.
    """
    from backend.app.services.common import version_service
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    try:
        result = version_service.delete_version(proj_dir, project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    except version_service.VersionFileError as e:
        raise HTTPException(400, str(e))
    return {"status": "ok", **result}


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


@router.delete("/{project_id:path}/versions/{version_id}")
async def delete_project_version(project_id: str, version_id: str):
    """Удалить версию проекта: папку с диска + запись из манифеста.

    Нельзя удалить единственную версию. После удаления latest переключается
    на предыдущую. Записи decisions_log, ссылающиеся на удалённые findings,
    станут orphan-записями (очищаются через KB-утилиты).
    """
    from backend.app.services.common import version_service
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    try:
        result = version_service.delete_version(proj_dir, project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    except version_service.VersionFileError as e:
        raise HTTPException(400, str(e))
    return {"status": "ok", **result}


class VersionFromCandidateRequest(BaseModel):
    """Создать новую версию у существующего проекта из найденных файлов.

    Все пути — server-side. Файлы должны находиться внутри PROJECTS_DIR или
    другого `allowed_root`; path traversal отклоняется.
    """
    candidate_pdf_path: str
    candidate_md_path: Optional[str] = None
    candidate_result_json_path: Optional[str] = None
    candidate_extra_paths: list[str] = []
    expected_section: Optional[str] = None  # защита от cross-section мисс-клика
    external_root: Optional[str] = None     # дополнительный allowed_root (Из другой папки)
    comment: Optional[str] = None
    source: str = "section_add_project_modal"


@router.post("/{target_project_id:path}/versions/from-candidate")
async def create_version_from_candidate(
    target_project_id: str,
    req: VersionFromCandidateRequest,
):
    """Создать V{N+1} target-проекта из уже найденных PDF/MD/result.json.

    Не создаёт новой карточки проекта: вся новая версия привязывается к
    target_project_id. `expected_section` нужен, чтобы UI текущего раздела не
    мог случайно «вставить» V2 в проект другого раздела.
    """
    from backend.app.services.common import version_service

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
    """Слить source-проект в target как новую версию.

    - source ≠ target;
    - section должны совпадать;
    - PDF/MD source переносятся в `_versions/v{N+1}/` target;
    - V1 target не трогается;
    - `_output/` source НЕ копируется (новая версия начинается с нуля);
    - если source имеет непустой `_output` → 409 без флага `discard_source_output`;
    - source-папка удаляется (delete_source=True по умолчанию);
    - ни source, ни target не должны иметь активного аудита.
    """
    from backend.app.services.common import version_service
    from backend.app.pipeline.manager import pipeline_manager

    # source и target существуют?
    src_dir = project_service.resolve_project_dir(req.source_project_id)
    tgt_dir = project_service.resolve_project_dir(target_project_id)
    if not src_dir.exists():
        raise HTTPException(404, f"Source проект '{req.source_project_id}' не найден")
    if not tgt_dir.exists():
        raise HTTPException(404, f"Target проект '{target_project_id}' не найден")

    # Не сливаем активный source/target
    try:
        if pipeline_manager.is_running(req.source_project_id):
            raise HTTPException(
                409,
                f"Аудит source проекта '{req.source_project_id}' выполняется. Сначала отмените.",
            )
        if pipeline_manager.is_running(target_project_id):
            raise HTTPException(
                409,
                f"Target проект '{target_project_id}' сейчас находится в обработке. "
                f"Привязка версии невозможна, пока активный аудит не завершится.",
            )
    except AttributeError:
        # pipeline_manager может не быть готов в тестах — пропускаем
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


class RenameProjectRequest(BaseModel):
    """Запрос на переименование загруженной папки проекта."""
    name: str
    object_id: Optional[str] = None


@router.patch("/{project_id:path}/rename")
async def rename_project_endpoint(project_id: str, req: RenameProjectRequest):
    """Переименовать папку проекта (и project_id) безопасно.

    Меняет имя папки на диске, обновляет project_info.json / version_group.json
    и синхронно переписывает ссылки в decisions_log / usage_data /
    project_groups / missing_norms_vault. Внутренние data-файлы (PDF/MD/
    артефакты) не переименовываются. Аудит не запускается.
    """
    from backend.app.services.common import project_rename_service as prs
    try:
        return prs.rename_project(project_id, req.name, object_id=req.object_id)
    except prs.InvalidProjectNameError as e:
        raise HTTPException(400, str(e))
    except prs.ProjectNotFoundError as e:
        raise HTTPException(404, str(e))
    except prs.RenameConflictError as e:
        raise HTTPException(409, str(e))
    except prs.ProjectBusyError as e:
        raise HTTPException(409, str(e))
    except prs.RenameError as e:
        raise HTTPException(500, str(e))


@router.get("/{project_id:path}")
async def get_project(project_id: str, request: Request, version_id: Optional[str] = None):
    """Детали одного проекта.

    Query param `version_id` (опционально): получить статус конкретной версии.
    По умолчанию возвращается latest. Для legacy-проектов без манифеста
    допустим только `v1` (это и есть корень проекта).

    opt-in read canary: `?storage=projects_v2` (или header) + флаг → детали из
    projects_v2 (read-only). Без opt-in — legacy как прежде.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_project_details(request, project_id)
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
async def clean_project(project_id: str, version_id: Optional[str] = None):
    """Очистить результаты аудита ТОЛЬКО активной версии.

    Удаляет `_output/` версии (`version_id`; при None — активной/latest) и
    сбрасывает авто-поля в её project_info.json. Другие версии проекта
    остаются нетронутыми. PDF, MD, project_info.json сохраняются.
    """
    # Проверка что аудит не запущен
    from backend.app.pipeline.manager import pipeline_manager
    if pipeline_manager.is_running(project_id):
        raise HTTPException(409, f"Аудит проекта '{project_id}' сейчас выполняется. Сначала отмените.")

    try:
        result = project_service.clean_project_data(project_id, version_id=version_id)
        return {"status": "ok", "project_id": project_id, **result}
    except ValueError as e:
        raise HTTPException(404, str(e))


class SetSectionRequest(BaseModel):
    section: str


@router.put("/{project_id:path}/section")
async def set_project_section_endpoint(project_id: str, req: SetSectionRequest):
    """Сменить дисциплину проекта (только запись `section` в project_info.json)."""
    section = (req.section or "").strip()
    if not section:
        raise HTTPException(400, "Не задан раздел")
    try:
        info = project_service.set_project_section(project_id, section)
        return {"status": "ok", "project_id": project_id, "section": info.get("section")}
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post("/{project_id:path}/hide")
async def hide_project_endpoint(project_id: str):
    """Скрыть проект из UI (файлы на диске не трогаем)."""
    project_service.hide_project(project_id)
    return {"status": "ok", "project_id": project_id, "hidden": True}


@router.post("/{project_id:path}/unhide")
async def unhide_project_endpoint(project_id: str):
    """Вернуть скрытый проект в UI."""
    project_service.unhide_project(project_id)
    return {"status": "ok", "project_id": project_id, "hidden": False}


# Жёсткое удаление проекта. Зарегистрировано ПОСЛЕ `/{project_id:path}/clean`,
# чтобы жадный path-конвертер не перехватывал `…/clean` (тот матчится первым).
@router.delete("/{project_id:path}")
async def delete_project_endpoint(project_id: str):
    """Безвозвратно удалить проект: legacy-папку (все версии, если контейнер) +
    документ(ы) в projects_v2 + записи old_to_new_map. Гард: нельзя во время
    запущенного аудита.
    """
    from backend.app.pipeline.manager import pipeline_manager
    if pipeline_manager.is_running(project_id):
        raise HTTPException(409, f"Аудит проекта '{project_id}' сейчас выполняется. Сначала отмените.")
    try:
        result = project_service.delete_project(project_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"status": "ok", **result}
