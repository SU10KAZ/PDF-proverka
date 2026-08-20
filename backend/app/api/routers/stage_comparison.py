"""API for the source-upload and vector-viewer comparison shell."""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
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


@router.get("/sessions/{session_id}/pairs/{pair_id}/page-svg")
async def get_page_svg(
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    page: int = Query(1, ge=1),
):
    try:
        payload = await run_in_threadpool(store.render_pdf_page_svg, session_id, pair_id, side, page)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("page-svg render failed")
        raise HTTPException(500, f"Ошибка векторного рендера страницы: {exc}") from exc
    return Response(payload, media_type="image/svg+xml", headers={"Cache-Control": "private, max-age=3600"})
