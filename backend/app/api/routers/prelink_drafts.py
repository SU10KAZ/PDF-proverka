"""Pre-analysis human block links of a pair (``human-prelink-drafts/1``).

The ONLY writer of ``prelink_drafts.json``.  These links are never shown to any
model; after an analysis they are reconciled with the AI regions and a person
decides explicitly (existing Human Mapping API).  GET and DELETE work at any
flag so leftover drafts can always be seen and removed; POST/PUT need
STAGE_PRELINK_DRAFTS and the live recognition of both sides.
"""
from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from backend.app.services.stage_comparison import prelink_drafts

router = APIRouter(prefix="/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/prelinks",
                   tags=["stage-prelink-drafts"])

NO_STORE = {"Cache-Control": "no-store"}


class PrelinkBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=0)
    old_block_ids: list[str] = Field(default_factory=list, max_length=64)
    new_block_ids: list[str] = Field(default_factory=list, max_length=64)
    note: str = Field(default="", max_length=2000)


def _call(fn: Callable[..., Any], *args: Any, status_code: int = 200, **kwargs: Any) -> JSONResponse:
    try:
        payload = fn(*args, **kwargs)
    except prelink_drafts.PrelinkError as exc:
        raise HTTPException(exc.status, detail={"error": exc.code, "ok": False, **exc.extra}) from exc
    return JSONResponse(payload, status_code=status_code, headers=NO_STORE)


@router.get("")
def get_prelinks(session_id: str, pair_id: str):
    return _call(prelink_drafts.view, session_id, pair_id)


@router.post("")
def create_prelink(session_id: str, pair_id: str, body: PrelinkBody):
    return _call(prelink_drafts.create, session_id, pair_id, status_code=201,
                 expected_revision=body.expected_revision, old_block_ids=body.old_block_ids,
                 new_block_ids=body.new_block_ids, note=body.note)


@router.put("/{prelink_id}")
def replace_prelink(session_id: str, pair_id: str, prelink_id: str, body: PrelinkBody):
    return _call(prelink_drafts.replace, session_id, pair_id, prelink_id,
                 expected_revision=body.expected_revision, old_block_ids=body.old_block_ids,
                 new_block_ids=body.new_block_ids, note=body.note)


@router.delete("/{prelink_id}")
def delete_prelink(session_id: str, pair_id: str, prelink_id: str, expected_revision: int = Query(..., ge=0)):
    return _call(prelink_drafts.delete, session_id, pair_id, prelink_id, expected_revision=expected_revision)
