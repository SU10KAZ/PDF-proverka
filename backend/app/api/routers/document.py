"""
REST API для просмотра MD-документа проекта (постранично).
"""
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
import backend.app.services.common.project_service as project_service
from backend.app.services.common import version_service

router = APIRouter(prefix="/api/document", tags=["document"])


def _validate_version(project_id: str, version_id: Optional[str]) -> None:
    if not version_id:
        return
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    try:
        version_service.get_version_entry(proj_dir, project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))


@router.get("/{project_id:path}/pages")
async def get_document_pages(
    project_id: str,
    request: Request,
    version_id: Optional[str] = Query(None, description="Конкретная версия, по умолчанию latest"),
):
    """Оглавление MD-документа: список страниц с метаданными (без содержимого блоков).

    opt-in/default read canary: при v2-backend читается из projects_v2 MD
    (read-only). `?storage=legacy` форсит legacy.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_document_pages(request, project_id)
    _validate_version(project_id, version_id)
    doc = project_service.parse_md_document(project_id, version_id=version_id)
    if not doc:
        raise HTTPException(404, f"MD-файл не найден для '{project_id}'")
    # Возвращаем без содержимого блоков (только счётчики)
    pages_light = []
    for p in doc["pages"]:
        pages_light.append({
            "page_num": p["page_num"],
            "sheet_info": p["sheet_info"],
            "sheet_label": p["sheet_label"],
            "text_blocks": p["text_blocks"],
            "image_blocks": p["image_blocks"],
        })
    return {
        "project_id": doc["project_id"],
        "md_file": doc["md_file"],
        "total_pages": doc["total_pages"],
        "pages": pages_light,
    }


@router.get("/{project_id:path}/page/{page_num}")
async def get_document_page(
    project_id: str,
    page_num: int,
    request: Request,
    version_id: Optional[str] = Query(None),
):
    """Содержимое одной страницы MD-документа (все блоки).

    opt-in/default read canary: при v2-backend читается из projects_v2 MD
    (read-only). `?storage=legacy` форсит legacy.
    """
    from backend.app.services.storage import read_canary
    if read_canary.resolve_read_backend(request) == read_canary.BACKEND_V2:
        return read_canary.v2_document_page(request, project_id, page_num)
    _validate_version(project_id, version_id)
    page = project_service.get_document_page(project_id, page_num, version_id=version_id)
    if not page:
        raise HTTPException(404, f"Страница {page_num} не найдена для '{project_id}'")
    return page


@router.get("/{project_id:path}/pdf")
async def get_document_pdf(
    project_id: str,
    request: Request,
    version_id: Optional[str] = Query(None, description="Конкретная версия, по умолчанию latest"),
):
    """Отдать исходный PDF нужной версии (inline, со стримингом HTTP Range).

    Резолвит PDF конкретной версии (projects_v2: `02_work/document.pdf` →
    `01_input/*.pdf`; legacy: `*.pdf` в корне / `project_info.pdf_file`) и отдаёт
    как `application/pdf`. `FileResponse` (Starlette) выставляет
    `Accept-Ranges: bytes` и обрабатывает `Range` → браузер тянет файл частями по
    мере листания, не загружая фронтенд. Просмотрщик встраивается в `<iframe>`,
    поэтому disposition = inline.
    """
    from backend.app.services.storage.projects_v2_source_resolver import (
        resolve_version_source_files,
    )

    _validate_version(project_id, version_id)
    try:
        ctx = version_service.resolve_project_version_context(
            project_id,
            version_id,
            resolve_project_dir_fn=project_service.resolve_project_dir,
        )
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))

    sources = resolve_version_source_files(ctx["version_dir"], project_id)
    pdf_path = sources.pdf_path
    if not pdf_path or not Path(pdf_path).is_file():
        raise HTTPException(404, f"PDF не найден для версии '{ctx.get('version_id')}'")

    return FileResponse(
        str(pdf_path),
        media_type="application/pdf",
        filename=Path(pdf_path).name,
        content_disposition_type="inline",
    )
