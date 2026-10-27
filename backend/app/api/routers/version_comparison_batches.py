"""Folder-batch endpoints under the version-comparison router."""
from typing import Literal

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from backend.app.services.version_comparison import batches, settings, store


async def _upload_only(request: Request):
    """Upload permission never resumes a batch configured to run comparisons."""
    from backend.app.core.access import require_account

    ctx = require_account(request)
    batch_id = request.path_params.get("batch_id")
    if ctx.enforces_roles and not ctx.is_admin and batch_id and request.method != "GET":
        data = await run_in_threadpool(guarded, batches.get, request.path_params["object_id"], batch_id)
        if data.get("compare_neighbors"):
            raise HTTPException(403, "Управление пакетом со сравнением доступно только администратору.")


router = APIRouter(dependencies=[Depends(_upload_only)])


def guarded(fn, *args):
    if not settings.enabled():
        raise HTTPException(404, "Раздел «Сравнение версий» выключен")
    try:
        return fn(*args)
    except (store.CatalogError, ValueError) as exc:
        raise HTTPException(400, str(exc)) from exc


class BatchFile(BaseModel):
    file_id: str = Field(min_length=1, max_length=80)
    relative_path: str = Field(min_length=1, max_length=2000)
    size: int = Field(gt=0)
    last_modified: int = Field(default=0, ge=0)


class BatchItem(BaseModel):
    item_id: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=1, max_length=300)
    source_root: str = Field(default="", max_length=300)
    files: list[BatchFile] = Field(min_length=1, max_length=batches.MAX_FILES)


class BatchCreate(BaseModel):
    batch_id: str = Field(min_length=1, max_length=80)
    compare_neighbors: bool = False
    items: list[BatchItem] = Field(min_length=1, max_length=batches.MAX_PROJECTS)


class PauseRequest(BaseModel):
    paused: bool


class ItemAction(BaseModel):
    command: Literal["skip", "retry"]


class FileChoice(BaseModel):
    file_id: str = Field(min_length=1, max_length=80)
    label: str = Field(default="", max_length=300)


class ConfirmRequest(BaseModel):
    files: list[FileChoice] = Field(min_length=1, max_length=batches.MAX_FILES)


@router.get("/objects/{object_id}/batches")
def list_batches(object_id: str):
    return {"batches": guarded(batches.list_batches, object_id)}


@router.post("/objects/{object_id}/batches")
def create_batch(object_id: str, body: BatchCreate, request: Request):
    from backend.app.core.access import require_account

    ctx = require_account(request)
    if body.compare_neighbors and ctx.enforces_roles and not ctx.is_admin:
        raise HTTPException(403, "Право загрузки проектов не разрешает запуск сравнения.")
    return guarded(batches.create, object_id, body.model_dump())


@router.get("/objects/{object_id}/batches/{batch_id}")
def get_batch(object_id: str, batch_id: str):
    return guarded(batches.get, object_id, batch_id)


@router.delete("/objects/{object_id}/batches/{batch_id}")
def remove_batch(object_id: str, batch_id: str):
    return guarded(batches.remove, object_id, batch_id)


@router.post("/objects/{object_id}/batches/{batch_id}/items/{item_id}/files/{file_id}/chunks/{index}")
async def receive_chunk(object_id: str, batch_id: str, item_id: str, file_id: str,
                        index: int, file: UploadFile = File(...)):
    return await run_in_threadpool(guarded, batches.receive_chunk, object_id, batch_id,
                                  item_id, file_id, index, file.file)


@router.post("/objects/{object_id}/batches/{batch_id}/items/{item_id}/files/{file_id}")
async def receive(object_id: str, batch_id: str, item_id: str, file_id: str, file: UploadFile = File(...)):
    return await run_in_threadpool(guarded, batches.receive, object_id, batch_id, item_id, file_id, file.file)


@router.post("/objects/{object_id}/batches/{batch_id}/start")
def start(object_id: str, batch_id: str):
    return guarded(batches.start, object_id, batch_id)


@router.patch("/objects/{object_id}/batches/{batch_id}")
def pause(object_id: str, batch_id: str, body: PauseRequest):
    return guarded(batches.pause, object_id, batch_id, body.paused)


@router.post("/objects/{object_id}/batches/{batch_id}/items/{item_id}/action")
def action(object_id: str, batch_id: str, item_id: str, body: ItemAction):
    return guarded(batches.action, object_id, batch_id, item_id, body.command)


@router.post("/objects/{object_id}/batches/{batch_id}/items/{item_id}/confirm")
def confirm(object_id: str, batch_id: str, item_id: str, body: ConfirmRequest):
    return guarded(batches.confirm, object_id, batch_id, item_id, [f.model_dump() for f in body.files])
