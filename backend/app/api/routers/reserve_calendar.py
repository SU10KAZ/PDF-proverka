"""Reserve calendar API. Reads use schedule access; writes follow existing operate rights."""
import asyncio
from datetime import date
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from backend.app.core.access import require_account
from backend.app.core.attribution import actor_label
from backend.app.services.common import reserve_service as service

router = APIRouter(prefix='/api/schedule/reserve', tags=['schedule'])


class ProjectLink(BaseModel):
    object_id: str
    project_id: str
    version_id: str
    check_type: Literal['expert_review'] = 'expert_review'


class QueueItem(BaseModel):
    engineer_id: str
    title: str | None = Field(default=None, max_length=4000)
    object_id: str | None = None
    project_id: str | None = None
    version_id: str | None = None
    check_type: Literal['expert_review'] = 'expert_review'


class Operation(BaseModel):
    operation_id: UUID
    revision: int = Field(ge=0)
    kind: Literal['move', 'delete', 'add_preparation', 'assign', 'link', 'start', 'undo']
    items: list[QueueItem] = Field(default_factory=list, max_length=1000)
    task_ids: list[str] = Field(default_factory=list, max_length=1000)
    task_id: str | None = None
    engineer_id: str | None = None
    anchor_id: str | None = None
    side: Literal['before', 'after'] = 'before'
    forecast_date: str | None = None
    project: ProjectLink | None = None
    undo_id: str | None = None


def _actor(request):
    ctx = require_account(request)
    return f'account:{ctx.account_id}' if ctx.account_id is not None else ctx.login or actor_label(request) or 'local'


@router.get('')
async def get_reserve(request: Request, view_start: date | None = None):
    actor = _actor(request)
    try:
        result = await asyncio.to_thread(service.snapshot, actor, view_start)
    except (ValueError, OSError) as exc:
        raise HTTPException(503, {'code': 'source_unavailable', 'message': str(exc)})
    ctx = require_account(request)
    result['can_edit'] = not ctx.enforces_roles or ctx.can_edit_schedule
    return result


@router.post('/operations')
async def reserve_operation(body: Operation, request: Request, view_start: date | None = None):
    ctx = require_account(request)
    # Legacy staff retain their rights; accounts require calendar editing access.
    if ctx.enforces_roles and not ctx.can_edit_schedule:
        raise HTTPException(403, 'Нет права изменять очередь.')
    try:
        payload = body.model_dump(mode='json')
        # Transferring by the engineer name preserves dates. An explicit null
        # still means clearing a date, rather than omitting it accidentally.
        if body.kind == 'move' and 'forecast_date' not in body.model_fields_set:
            payload.pop('forecast_date')
        result = await asyncio.to_thread(service.mutate, payload, _actor(request), view_start)
        result['can_edit'] = True
        return result
    except service.ReserveError as exc:
        raise HTTPException(exc.status, {'code': exc.code, 'message': str(exc)})
    except (ValueError, OSError) as exc:
        raise HTTPException(503, {'code': 'source_unavailable', 'message': str(exc)})
