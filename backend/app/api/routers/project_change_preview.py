"""Object-scoped ProjectChange availability and the frozen read-only snapshot."""
import threading
from fastapi import APIRouter,Depends,HTTPException,Query,Request,Response
from typing import Literal
from backend.app.services.project_change_preview.service import OBJECT,PreviewService,SourceUnavailable

router=APIRouter(prefix='/api/project-change-preview/objects/{object_id}',tags=['ProjectChange preview'])
availability_router=APIRouter(prefix='/api/stage-comparison/objects/{object_id}/project-changes',
                             tags=['Stage comparison'])
_initialization=threading.Lock()

def service(request:Request,object_id:str):
    # UI availability is global; this guard protects only the snapshot's data.
    if object_id!=OBJECT: raise HTTPException(404,'Object outside snapshot scope')
    with _initialization:
        current=getattr(request.app.state,'project_change_preview_service',None)
        if current is None:
            try: current=PreviewService()
            except (ValueError,OSError,KeyError) as e: raise HTTPException(503,'Frozen preview unavailable') from e
            request.app.state.project_change_preview_service=current
    return current

def invoke(method,*args,**kwargs):
    try: return method(*args,**kwargs)
    except KeyError as e: raise HTTPException(404,'Resource outside preview snapshot') from e
    except (SourceUnavailable,OSError) as e: raise HTTPException(503,'Frozen preview unavailable') from e

@router.get('')
def presentation(s:PreviewService=Depends(service)): return invoke(s.envelope)

def _v3_scope(object_id):
    from backend.app.services.human_mapping_production.storage import InvalidScopeId,require_safe_id
    try: require_safe_id(object_id,'object')
    except InvalidScopeId as e: raise HTTPException(400,'Invalid object id') from e
    from backend.app.services.project_change_v3 import presentation
    return presentation

@availability_router.get('')
def available_presentation(request:Request,object_id:str):
    # Only an admitted dataset may supply ProjectChangeView: the sealed snapshot
    # of its object, or persisted V3 ProjectChanges of the object's own
    # comparisons.  Legacy differences are not a semantic contract.
    v3=_v3_scope(object_id)
    if object_id==OBJECT:
        envelope=invoke(service(request,object_id).envelope)
        return v3.merge_into_snapshot(envelope,object_id) if v3.v3_presentation_enabled() else envelope
    envelope=v3.object_envelope(object_id) if v3.v3_presentation_enabled() else None
    if envelope is not None:
        return envelope
    return {'schema_version':'project-change-view/1','object_id':object_id,
            'availability':'UNAVAILABLE','items':[],
            'capabilities':{'decisions':False,'history':False}}

@availability_router.get('/evidence/{evidence_id}/crop')
def v3_evidence_crop(object_id:str,evidence_id:str):
    v3=_v3_scope(object_id)
    if not v3.v3_presentation_enabled(): raise HTTPException(404,'V3 presentation disabled')
    try: data=v3.evidence_crop(object_id,evidence_id)
    except v3.EvidenceUnavailable as e: raise HTTPException(404,'Evidence unavailable') from e
    return Response(data,media_type='image/png',headers={'Cache-Control':'no-store'})

@router.get('/manifest')
def manifest(s:PreviewService=Depends(service)):
    invoke(s.assert_current)
    return {k:v for k,v in s.manifest.items() if k!='files'}

@router.get('/report')
def report(s:PreviewService=Depends(service)): return invoke(s.envelope,report_only=True)

@router.api_route('/decisions',methods=['POST','PUT','PATCH','DELETE'])
def decide(s:PreviewService=Depends(service)):
    raise HTTPException(403,'PROJECT_CHANGE_PREVIEW is READ ONLY; decisions are disabled')

@router.get('/decisions/{decision_key}')
def history(decision_key:str,s:PreviewService=Depends(service)):
    return {'items':[],'mode':'READ_ONLY'}

@router.get('/evidence/{evidence_id}/crop')
def crop(evidence_id:str,s:PreviewService=Depends(service)):
    return Response(invoke(s.crop,evidence_id),media_type='image/png',headers={'Cache-Control':'no-store'})

@router.get('/viewer/pairs/{pair_id}')
def pair(pair_id:str,s:PreviewService=Depends(service)): return invoke(s.pair,pair_id)

@router.get('/viewer/pairs/{pair_id}/page-info')
def page_info(pair_id:str,side:Literal['left','right'],page:int=Query(ge=1),s:PreviewService=Depends(service)):
    return invoke(s.page_info,pair_id,'old' if side=='left' else 'new',page)

@router.get('/viewer/pairs/{pair_id}/page-preview')
@router.get('/viewer/pairs/{pair_id}/page-thumb')
def page_preview(pair_id:str,side:Literal['left','right'],page:int=Query(ge=1),width:int=Query(default=1400,ge=64,le=4096),s:PreviewService=Depends(service)):
    return Response(invoke(s.page_raster,pair_id,'old' if side=='left' else 'new',page,width),media_type='image/png',headers={'Cache-Control':'no-store'})

@router.get('/viewer/pairs/{pair_id}/page-tile')
def page_tile(pair_id:str,side:Literal['left','right'],page:int=Query(ge=1),level:int=Query(ge=0,le=6),x:int=Query(ge=0,le=4096),y:int=Query(ge=0,le=4096),s:PreviewService=Depends(service)):
    return Response(invoke(s.page_raster,pair_id,'old' if side=='left' else 'new',page,tile=(level,x,y)),media_type='image/png',headers={'Cache-Control':'no-store'})
