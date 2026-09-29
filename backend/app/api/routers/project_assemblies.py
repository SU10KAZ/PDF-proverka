"""REST API for immutable project assemblies."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, ConfigDict, Field

from backend.app.api.routers.knowledge_base import _resolve_reviewer
from backend.app.services.project_assemblies import service

router = APIRouter(prefix="/api/project-assemblies", tags=["project-assemblies"])


class PreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    object_id: str = Field(min_length=1)
    source_refs: list[str] = Field(min_length=1, max_length=service.MAX_SOURCES)


class CreateRequest(PreviewRequest):
    name: str = Field(min_length=1, max_length=240)
    section: str = Field(default="OTHER", max_length=32)
    composition_completeness: str = Field(default="UNKNOWN", pattern="^(COMPLETE|INCOMPLETE|UNKNOWN)$")


class AttachmentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    object_id: str = Field(min_length=1)
    side: str = Field(pattern="^stage_[12]$")


def _gate() -> None:
    if not service.enabled():
        raise HTTPException(404, "Модуль сборок отключён")


def _http_error(exc: service.AssemblyError) -> HTTPException:
    status = 404 if exc.code.endswith("NOT_FOUND") else 409 if exc.code in {"SOURCE_CHANGED", "INSUFFICIENT_DISK"} else 400
    return HTTPException(status, {"code": exc.code, "message": str(exc), "details": exc.details})


@router.get("/sources")
async def sources(object_id: str):
    _gate()
    try:
        return await run_in_threadpool(service.list_sources, object_id)
    except service.AssemblyError as exc:
        raise _http_error(exc) from exc


@router.post("/preview")
async def preview(body: PreviewRequest):
    _gate()
    try:
        return await run_in_threadpool(service.preview, body.object_id, body.source_refs)
    except service.AssemblyError as exc:
        raise _http_error(exc) from exc


@router.post("")
async def create(body: CreateRequest, request: Request):
    _gate()
    try:
        return await run_in_threadpool(
            service.create_assembly,
            object_id=body.object_id,
            name=body.name,
            section=body.section,
            composition_completeness=body.composition_completeness,
            source_refs=body.source_refs,
            author=_resolve_reviewer(request),
            idempotency_key=request.headers.get("Idempotency-Key", ""),
            build=False,
        )
    except service.AssemblyError as exc:
        raise _http_error(exc) from exc


@router.get("")
async def list_all(object_id: str):
    _gate()
    return await run_in_threadpool(service.list_assemblies, object_id)


@router.get("/{assembly_id}/versions/{version_id}")
async def get_one(assembly_id: str, version_id: str, object_id: str):
    _gate()
    try:
        return await run_in_threadpool(service.get_assembly, object_id, assembly_id, version_id)
    except service.AssemblyError as exc:
        raise _http_error(exc) from exc


@router.post("/{assembly_id}/versions/{version_id}/attachment")
async def attach(assembly_id: str, version_id: str, body: AttachmentRequest):
    _gate()
    try:
        return await run_in_threadpool(service.attach, body.object_id, assembly_id, version_id, body.side)
    except service.AssemblyError as exc:
        raise _http_error(exc) from exc


@router.delete("/{assembly_id}/versions/{version_id}/attachment")
async def detach(assembly_id: str, version_id: str, object_id: str, side: str):
    _gate()
    try:
        return await run_in_threadpool(service.detach, object_id, assembly_id, version_id, side)
    except service.AssemblyError as exc:
        raise _http_error(exc) from exc


@router.post("/{assembly_id}/versions/{version_id}/retry")
async def retry(assembly_id: str, version_id: str, object_id: str):
    _gate()
    try:
        return await run_in_threadpool(service.retry_version, object_id, assembly_id, version_id)
    except service.AssemblyError as exc:
        raise _http_error(exc) from exc


@router.post("/{assembly_id}/versions/{version_id}/cancel")
async def cancel(assembly_id: str, version_id: str, object_id: str):
    _gate()
    try:
        return await run_in_threadpool(service.cancel_version, object_id, assembly_id, version_id)
    except service.AssemblyError as exc:
        raise _http_error(exc) from exc


@router.get("/{assembly_id}/versions/{version_id}/pages/{page}/origin")
async def page_origin(assembly_id: str, version_id: str, page: int, object_id: str):
    _gate()
    try:
        return await run_in_threadpool(service.origin_for_page, object_id, assembly_id, version_id, page)
    except service.AssemblyError as exc:
        raise _http_error(exc) from exc
