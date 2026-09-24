"""Read-only API of the stage-2 semantic block workspace (GET only).

Contracts stage-block-mapping-*/1; nothing here writes. Human decisions are
written only through the existing Human Mapping API (/api/human-mapping/...).
"""
from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from backend.app.services.stage_block_mapping import service

router = APIRouter(prefix="/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/block-mapping",
                   tags=["stage-block-mapping"])

NO_STORE = {"Cache-Control": "no-store"}
FROZEN = "private, max-age=86400"
REVALIDATE = "private, no-cache"


def _call(fn: Callable[..., Any], *args: Any) -> Any:
    try:
        return fn(*args)
    except service.BlockMappingError as exc:
        raise HTTPException(exc.status, detail={"error": exc.code, "ok": False, **exc.extra}) from exc


def _tagged(request: Request, payload: dict[str, Any], tag: str, cache_control: str) -> Response:
    etag = f'"{tag}"'
    headers = {"ETag": etag, "Cache-Control": cache_control}
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    return JSONResponse(payload, headers=headers)


@router.get("/status")
def block_mapping_status(session_id: str, pair_id: str):
    return JSONResponse(_call(service.status, session_id, pair_id), headers=NO_STORE)


@router.get("/runs/{run_id}/region-index")
def region_index(session_id: str, pair_id: str, run_id: str, request: Request):
    payload, tag = _call(service.region_index, session_id, pair_id, run_id)
    return _tagged(request, payload, tag, FROZEN)


@router.get("/runs/{run_id}/page-blocks")
def run_page_blocks(session_id: str, pair_id: str, run_id: str, request: Request,
                    side: str = Query(...), pages: str = Query(...)):
    page_list = _call(service.parse_pages, pages)
    payload, tag = _call(service.page_blocks_run, session_id, pair_id, run_id, side, page_list)
    return _tagged(request, payload, tag, FROZEN)


@router.get("/runs/{run_id}/blocks/{side}/{block_id}")
def run_block(session_id: str, pair_id: str, run_id: str, side: str, block_id: str,
              page: int | None = Query(default=None, ge=1)):
    return JSONResponse(_call(service.block_run, session_id, pair_id, run_id, side, block_id, page),
                        headers={"Cache-Control": FROZEN})


@router.get("/source/page-blocks")
def source_page_blocks(session_id: str, pair_id: str, request: Request,
                       side: str = Query(...), pages: str = Query(...)):
    page_list = _call(service.parse_pages, pages)
    payload, tag = _call(service.page_blocks_source, session_id, pair_id, side, page_list)
    return _tagged(request, payload, tag, REVALIDATE)


@router.get("/source/blocks/{side}/{block_id}")
def source_block(session_id: str, pair_id: str, side: str, block_id: str):
    return JSONResponse(_call(service.block_source, session_id, pair_id, side, block_id),
                        headers={"Cache-Control": REVALIDATE})


@router.get("/runs/{run_id}/bridge-check")
def bridge_check(session_id: str, pair_id: str, run_id: str):
    return JSONResponse(_call(service.bridge_check, session_id, pair_id, run_id), headers=NO_STORE)
