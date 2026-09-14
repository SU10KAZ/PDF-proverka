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

@availability_router.get('')
def available_presentation(request:Request,object_id:str):
    # Only an admitted dataset may supply ProjectChangeView. Legacy differences
    # are not a semantic contract, and no other object inherits this snapshot.
    if object_id==OBJECT:
        return invoke(service(request,object_id).envelope)
    return {'schema_version':'project-change-view/1','object_id':object_id,
            'availability':'UNAVAILABLE','items':[],
            'capabilities':{'decisions':False,'history':False}}

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
