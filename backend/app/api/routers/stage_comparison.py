"""API for the source-upload and vector-viewer comparison shell."""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from pydantic import BaseModel, Field

from backend.app.services.stage_comparison import objects as objects_mod
from backend.app.services.stage_comparison import stage_upload as stage_upload_mod
from backend.app.services.stage_comparison import store


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/stage-comparison", tags=["stage-comparison"])


class CreateSessionRequest(BaseModel):
    stage_a_path: str = Field(min_length=1)
    stage_b_path: str = Field(min_length=1)


class CreatePairRequest(BaseModel):
    left_pdf: str = Field(min_length=1)
    right_pdf: str = Field(min_length=1)


class SheetLinkRequest(BaseModel):
    id: str | None = None
    left_pages: list[int]
    right_pages: list[int]
    source: str = "manual"
    confidence: str = "manual"
    reason: list[str] = Field(default_factory=list)


class SaveSheetLinksRequest(BaseModel):
    links: list[SheetLinkRequest] = Field(default_factory=list)
    unlinked_left_pages: list[int] = Field(default_factory=list)


@router.get("/objects")
async def list_comparison_objects():
    return objects_mod.list_objects()


@router.post("/objects/{object_id}/stages/{stage_name}/upload")
async def upload_stage_archive(object_id: str, stage_name: str, file: UploadFile = File(...)):
    if stage_name not in stage_upload_mod.VALID_STAGES:
        raise HTTPException(400, "Разрешены только stage_1 и stage_2")
    try:
        return await run_in_threadpool(
            stage_upload_mod.replace_stage_from_zip, object_id, stage_name, file.file, file.filename,
        )
    except stage_upload_mod.StageUploadError as exc:
        raise HTTPException(400, str(exc)) from exc
    except OSError as exc:
        logger.exception("stage archive upload failed: %s/%s", object_id, stage_name)
        raise HTTPException(500, f"Не удалось сохранить архив стадии: {exc}") from exc


@router.post("/objects/{object_id}/stages/{stage_name}/upload-folder")
async def upload_stage_folder(
    object_id: str,
    stage_name: str,
    files: list[UploadFile] = File(...),
    relative_paths: str = Form("[]"),
    folder_name: str = Form(""),
    retain_backup: bool = Form(True),
):
    if stage_name not in stage_upload_mod.VALID_STAGES:
        raise HTTPException(400, "Разрешены только stage_1 и stage_2")
    try:
        paths = json.loads(relative_paths or "[]")
    except json.JSONDecodeError as exc:
        raise HTTPException(422, "Некорректный список путей файлов") from exc
    if not files or not isinstance(paths, list) or len(paths) != len(files):
        raise HTTPException(422, "Количество файлов и относительных путей не совпадает")
    uploads = [(upload.file, str(paths[index] or upload.filename or "")) for index, upload in enumerate(files)]
    try:
        return await run_in_threadpool(
            stage_upload_mod.replace_stage_from_folder,
            object_id,
            stage_name,
            uploads,
            folder_name,
            retain_backup,
        )
    except stage_upload_mod.StageUploadError as exc:
        raise HTTPException(400, str(exc)) from exc
    except OSError as exc:
        logger.exception("stage folder upload failed: %s/%s", object_id, stage_name)
        raise HTTPException(500, f"Не удалось сохранить папку стадии: {exc}") from exc


@router.post("/sessions")
async def create_session(request: CreateSessionRequest):
    try:
        store.assert_path_in_allowlist(request.stage_a_path)
        store.assert_path_in_allowlist(request.stage_b_path)
        session, warnings = await run_in_threadpool(
            store.create_session, request.stage_a_path, request.stage_b_path,
        )
    except PermissionError as exc:
        raise HTTPException(403, str(exc)) from exc
    except OSError as exc:
        raise HTTPException(400, f"Ошибка доступа к папкам: {exc}") from exc
    return {**session, "session_id": session["id"], "warnings": warnings}


@router.get("/sessions")
async def list_sessions():
    return {"sessions": await run_in_threadpool(store.list_sessions)}


@router.get("/sessions/{session_id}")
async def get_session(session_id: str):
    session = await run_in_threadpool(store.get_session, session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    return session


@router.post("/sessions/{session_id}/pairs")
async def create_pair(session_id: str, request: CreatePairRequest):
    try:
        return await run_in_threadpool(
            store.create_pair, session_id, request.left_pdf, request.right_pdf,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}")
async def get_pair(session_id: str, pair_id: str):
    pair = await run_in_threadpool(store.get_pair_view, session_id, pair_id)
    if pair is None:
        raise HTTPException(404, "Пара не найдена")
    return pair


@router.post("/sessions/{session_id}/pairs/{pair_id}/sheet-match-suggestions")
async def rebuild_sheet_match_suggestions(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(store.run_sheet_matching, session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(400, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise HTTPException(400, f"Не удалось прочитать Markdown: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/sheet-matches")
async def get_sheet_matches(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(store.get_sheet_matching_state, session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/sessions/{session_id}/pairs/{pair_id}/sheet-links")
async def save_sheet_links(session_id: str, pair_id: str, request: SaveSheetLinksRequest):
    try:
        return await run_in_threadpool(
            store.save_sheet_links,
            session_id,
            pair_id,
            [link.model_dump() for link in request.links],
            request.unlinked_left_pages,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/page-svg")
async def get_page_svg(
    request: Request,
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    page: int = Query(1, ge=1),
):
    accept_gzip = "gzip" in (request.headers.get("accept-encoding") or "").lower()
    try:
        payload = await run_in_threadpool(
            store.page_svg_payload, session_id, pair_id, side, page, accept_gzip
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("page-svg render failed")
        raise HTTPException(500, f"Ошибка векторного рендера страницы: {exc}") from exc

    headers = {"Cache-Control": "private, max-age=3600", "ETag": payload["etag"]}
    # Просмотрщик листает страницы туда-обратно; 304 экономит мегабайты вектора.
    if request.headers.get("if-none-match") == payload["etag"]:
        return Response(status_code=304, headers=headers)
    if payload["encoding"]:
        headers["Content-Encoding"] = payload["encoding"]
        headers["Vary"] = "Accept-Encoding"
    return Response(payload["body"], media_type="image/svg+xml", headers=headers)
