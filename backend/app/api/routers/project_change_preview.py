"""Opt-in, object-272-only research presentation and isolated decision journal."""
import os
import threading
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from backend.app.services.project_change_preview.service import PreviewService
from backend.app.services.project_change_preview.sources import OBJECT, SourceUnavailable
from backend.app.services.project_change_preview.decisions import DecisionConflict

router = APIRouter(prefix='/api/project-change-preview/objects/{object_id}', tags=['ProjectChange preview'])
_initialization = threading.Lock()


def service(request: Request, object_id: str):
    enabled = getattr(request.app.state, 'project_change_preview_enabled', False)
    if not enabled and os.environ.get('PROJECT_CHANGE_PREVIEW_ENABLED') != '1':
        raise HTTPException(404, 'ProjectChange preview disabled')
    if object_id != OBJECT:
        raise HTTPException(404, 'Object outside preview scope')
    with _initialization:
        current = getattr(request.app.state, 'project_change_preview_service', None)
        if current is None:
            directory = os.environ.get('PROJECT_CHANGE_PREVIEW_STATE_DIR')
            if not directory:
                raise HTTPException(503, 'Isolated preview state directory is required')
            try:
                current = PreviewService(directory)
            except (ValueError, OSError) as error:
                raise HTTPException(503, 'Preview sources unavailable') from error
            request.app.state.project_change_preview_service = current
    return current


def invoke(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except DecisionConflict as error:
        raise HTTPException(409, str(error)) from error
    except (SourceUnavailable, OSError) as error:
        raise HTTPException(503, 'Источники preview недоступны или изменились. ' + str(error)) from error
    except KeyError as error:
        raise HTTPException(404, 'Preview resource not found') from error


class DecisionRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    change_id: str = Field(min_length=1, max_length=160)
    decision_key: str = Field(pattern=r'^pcbridge1_[0-9a-f]{64}$')
    binding_signature: str = Field(pattern=r'^[0-9a-f]{64}$')
    action: Literal['CONFIRM', 'NOT_A_CHANGE', 'UNSURE', 'BROKEN_CASE']
    comment: str = Field(default='', max_length=4000)
    expected_source_revision: str = Field(pattern=r'^[0-9a-f]{64}$')
    expected_decision_revision: int = Field(ge=0, strict=True)


@router.get('')
def presentation(s: PreviewService = Depends(service)):
    return invoke(s.envelope)


@router.get('/manifest')
def manifest(s: PreviewService = Depends(service)):
    invoke(s.sources.assert_current)
    return s.sources.manifest


@router.get('/report')
def report(s: PreviewService = Depends(service)):
    return invoke(s.envelope, report_only=True)


@router.post('/decisions')
def decide(body: DecisionRequest, request: Request, s: PreviewService = Depends(service)):
    # Custom header prevents cross-site form writes to the loopback preview.
    if request.headers.get('X-ProjectChange-Preview') != '1':
        raise HTTPException(403, 'Preview request header required')
    actor = getattr(request.app.state, 'project_change_preview_actor', None)
    if not actor:
        from backend.app.core import portal_auth
        settings = portal_auth.get_settings()
        actor = portal_auth.request_username(request, settings) if settings.enabled else None
    if not actor:
        raise HTTPException(401, 'Authenticated actor required')
    return invoke(s.decide, **body.model_dump(), actor=actor)


@router.get('/decisions/{decision_key}')
def history(decision_key: str, s: PreviewService = Depends(service)):
    return {'items': invoke(s.history, decision_key)}


@router.get('/evidence/{evidence_id}/crop')
def crop(evidence_id: str, s: PreviewService = Depends(service)):
    return Response(invoke(s.crop, evidence_id), media_type='image/png', headers={'Cache-Control': 'no-store'})


@router.get('/viewer/pairs/{pair_id}')
def pair(pair_id: str, s: PreviewService = Depends(service)):
    return invoke(s.pair, pair_id)


@router.get('/viewer/pairs/{pair_id}/page-info')
def page_info(pair_id: str, side: Literal['left', 'right'], page: int = Query(ge=1),
              s: PreviewService = Depends(service)):
    return invoke(s.page_info, pair_id, 'old' if side == 'left' else 'new', page)


@router.get('/viewer/pairs/{pair_id}/page-preview')
@router.get('/viewer/pairs/{pair_id}/page-thumb')
def page_preview(pair_id: str, side: Literal['left', 'right'], page: int = Query(ge=1),
                 width: int = Query(default=1400, ge=64, le=4096), s: PreviewService = Depends(service)):
    return Response(invoke(s.page_raster, pair_id, 'old' if side == 'left' else 'new', page, width),
                    media_type='image/png', headers={'Cache-Control': 'no-store'})


@router.get('/viewer/pairs/{pair_id}/page-tile')
def page_tile(pair_id: str, side: Literal['left', 'right'], page: int = Query(ge=1),
              level: int = Query(ge=0, le=6), x: int = Query(ge=0, le=4096), y: int = Query(ge=0, le=4096),
              s: PreviewService = Depends(service)):
    return Response(invoke(s.page_raster, pair_id, 'old' if side == 'left' else 'new', page, tile=(level,x,y)),
                    media_type='image/png', headers={'Cache-Control': 'no-store'})
