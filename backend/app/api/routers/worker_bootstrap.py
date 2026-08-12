"""Admin API for the resumable one-click worker bootstrap state machine."""
from __future__ import annotations

import asyncio

from typing import Any

from fastapi import APIRouter, BackgroundTasks, Body, Depends, Header, HTTPException, Query
from pydantic import ValidationError

from backend.app.services.distributed_workers.authorization import Actor, require_admin
from backend.app.services.worker_bootstrap.manager import BootstrapManager
from backend.app.services.worker_bootstrap.models import (
    BootstrapSessionView,
    CreateBootstrapSession,
    UpdateBootstrapSession,
)
from backend.app.services.worker_bootstrap.store import SessionNotFound, SessionUpdateConflict


router = APIRouter(prefix="/api/workers/bootstrap", tags=["worker-bootstrap"])


def _manager() -> BootstrapManager:
    return BootstrapManager()


def _intent(value: str | None) -> None:
    if value != "execute":
        raise HTTPException(
            status_code=428,
            detail="Требуется X-Worker-Bootstrap-Intent: execute",
        )


def _validated_payload(raw: Any) -> CreateBootstrapSession:
    """Validate without reflecting secret-bearing invalid inputs.

    FastAPI's default Pydantic 422 includes `input`; a rejected URL such as
    `https://user:password@host` would therefore echo the password into the
    center API response and possibly access logs. We retain typed domain
    models but expose only location/type/message from validation failures.
    """
    try:
        return CreateBootstrapSession.model_validate(raw)
    except ValidationError as exc:
        safe_errors = [
            {
                "loc": list(item.get("loc") or ()),
                "type": item.get("type"),
                "message": item.get("msg"),
            }
            for item in exc.errors()
        ]
        raise HTTPException(status_code=422, detail=safe_errors) from exc


def _validated_update(raw: Any) -> UpdateBootstrapSession:
    try:
        return UpdateBootstrapSession.model_validate(raw)
    except ValidationError as exc:
        safe_errors = [
            {
                "loc": list(item.get("loc") or ()),
                "type": item.get("type"),
                "message": item.get("msg"),
            }
            for item in exc.errors()
        ]
        raise HTTPException(status_code=422, detail=safe_errors) from exc


@router.post("/sessions", response_model=BootstrapSessionView, status_code=202)
async def create_session(
    background: BackgroundTasks,
    raw_payload: Any = Body(...),
    execute: bool = Query(default=True),
    x_worker_bootstrap_intent: str | None = Header(default=None),
    actor: Actor = Depends(require_admin),
) -> dict:
    del actor
    _intent(x_worker_bootstrap_intent)
    payload = _validated_payload(raw_payload)
    manager = _manager()
    session = await asyncio.to_thread(
        manager.create,
        operation=payload.operation,
        request=payload.request,
        idempotency_key=payload.idempotency_key,
    )
    if execute and session["state"] not in {"succeeded", "running"}:
        background.add_task(manager.run, session["session_id"])
    return session


@router.get("/sessions", response_model=list[BootstrapSessionView])
async def list_sessions(
    limit: int = Query(default=100, ge=1, le=500),
    actor: Actor = Depends(require_admin),
) -> list[dict]:
    del actor
    return await asyncio.to_thread(_manager().list, limit=limit)


@router.get("/sessions/{session_id}", response_model=BootstrapSessionView)
async def get_session(
    session_id: str, actor: Actor = Depends(require_admin)
) -> dict:
    del actor
    try:
        return await asyncio.to_thread(_manager().get, session_id)
    except SessionNotFound as exc:
        raise HTTPException(status_code=404, detail="Bootstrap session не найдена") from exc


@router.patch("/sessions/{session_id}", response_model=BootstrapSessionView)
async def update_session(
    session_id: str,
    raw_payload: Any = Body(...),
    x_worker_bootstrap_intent: str | None = Header(default=None),
    actor: Actor = Depends(require_admin),
) -> dict:
    """Update the transport endpoint of the same resumable bootstrap session."""
    del actor
    _intent(x_worker_bootstrap_intent)
    payload = _validated_update(raw_payload)
    try:
        return await asyncio.to_thread(
            _manager().update_center_url, session_id, payload.center_url
        )
    except SessionNotFound as exc:
        raise HTTPException(status_code=404, detail="Bootstrap session не найдена") from exc
    except SessionUpdateConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/sessions/{session_id}/resume", response_model=BootstrapSessionView)
async def resume_session(
    session_id: str,
    x_worker_bootstrap_intent: str | None = Header(default=None),
    actor: Actor = Depends(require_admin),
) -> dict:
    del actor
    _intent(x_worker_bootstrap_intent)
    try:
        return await asyncio.to_thread(_manager().run, session_id)
    except SessionNotFound as exc:
        raise HTTPException(status_code=404, detail="Bootstrap session не найдена") from exc
