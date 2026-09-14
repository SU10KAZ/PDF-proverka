"""Loopback acceptance server with real snapshot and Stage Comparison HTTP routes.

Only the portal registry and storage locations are isolated. Ordinary documents
are generated UI fixtures, not research evidence or ProjectChange datasets.
No full application startup, worker or inference. All writes go to a fresh tmpdir.
"""
import argparse
import io
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
STATE = Path(tempfile.mkdtemp(prefix='project-change-ui-acceptance-'))
os.environ['COMPARISON_ROOT'] = str(STATE / 'sessions')
os.environ['AUDIT_STAGE_COMPARISON_ROOTS'] = str(STATE)

# Fail the acceptance run if any code attempts external network access.
def local_network_only(event, args):
    if event == 'socket.connect':
        address = args[1]
        if isinstance(address, tuple) and address[0] not in {'127.0.0.1', '::1', 'localhost'}:
            raise RuntimeError('Acceptance forbids external connections')
sys.addaudithook(local_network_only)

import fitz
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware
import uvicorn
from backend.app.api.routers import project_change_preview, stage_comparison
from backend.app.services.project_change_preview.service import OBJECT
from backend.app.services.stage_comparison import stage_upload
from backend.app.services.common import object_service

REGISTRY = [{'id': OBJECT, 'name': '272. Садовническая 76 / Балчуг Эстейт'},
            {'id': 'OTHER_OBJECT', 'name': 'Проверка совместимости · обычный объект'},
            {'id': 'EMPTY_OBJECT', 'name': 'Проверка совместимости · без документов'}]
object_service.list_objects = lambda: REGISTRY
object_service.get_object_by_id = lambda oid: next((o for o in REGISTRY if o['id'] == oid), None)
stage_upload._projects_v2_root = lambda: STATE / 'projects_v2'
def resolve_object_dir(oid, *, create=False):
    obj = object_service.get_object_by_id(oid)
    if obj is None:
        raise stage_upload.StageUploadError('Unknown acceptance object')
    directory = STATE / 'projects_v2' / 'objects' / oid / 'comparison'
    if create:
        directory.mkdir(parents=True, exist_ok=True)
    return obj, directory
stage_upload.resolve_object_dir = resolve_object_dir

for stage, side in [('stage_1', 'OLD'), ('stage_2', 'NEW')]:
    for label in ['Ventilation', 'Heating']:
        with fitz.open() as doc:
            page = doc.new_page(width=640, height=420)
            page.insert_text((40, 50), f'{label} - {side}', fontsize=24)
            page.insert_text((40, 95), 'UI acceptance fixture: no ProjectChange analysis', fontsize=12)
            page.draw_rect(fitz.Rect(50, 130, 540, 340), color=(0.2, 0.3, 0.5))
            data = doc.tobytes()
        stage_upload.replace_stage_from_folder('OTHER_OBJECT', stage,
            [(io.BytesIO(data), f'{label}.pdf')], label, False)

app = FastAPI()
app.add_middleware(TrustedHostMiddleware, allowed_hosts=['127.0.0.1', 'localhost', 'testserver'])
app.include_router(project_change_preview.router)
app.include_router(project_change_preview.availability_router)
app.include_router(stage_comparison.router)
app.mount('/static', StaticFiles(directory=ROOT / 'frontend/static'), name='static')

@app.get('/')
def index():
    return FileResponse(ROOT / 'frontend/index.html')

@app.websocket('/ws/global')
async def shell_notifications(socket: WebSocket):
    await socket.accept()
    try:
        while True:
            await socket.receive_text()
    except WebSocketDisconnect:
        pass

@app.get('/api/acceptance/upload.pdf')
def upload_fixture():
    return Response(data, media_type='application/pdf')

@app.get('/api/usage/projects-summary')
def project_usage():
    return {}

@app.get('/api/objects')
def objects():
    return {'objects': REGISTRY, 'current_id': OBJECT}

@app.post('/api/objects/switch')
async def switch_object(request: Request):
    oid = (await request.json()).get('id')
    obj = object_service.get_object_by_id(oid)
    if obj is None:
        raise HTTPException(404, 'Unknown acceptance object')
    return {'current_id': oid, 'object': obj}

@app.get('/api/audit/live-status')
@app.get('/api/projects/disciplines')
@app.get('/api/usage/global')
@app.get('/api/audit/account')
@app.get('/api/usage/paid-api/status')
@app.get('/api/usage/paid-cost/events')
@app.get('/api/usage/paid-cost/blocked-events')
@app.get('/api/usage/paid-cost')
@app.get('/api/usage/paid-cost/daily')
@app.get('/api/objects/stats')
@app.get('/api/project-groups')
@app.get('/api/auth/me')
@app.get('/api/auth/status')
@app.get('/api/users')
@app.get('/api/projects')
@app.get('/api/disciplines')
@app.get('/api/objects/{object_id}/disciplines')
def shell():
    return {'items': [], 'projects': [], 'users': [], 'disciplines': [], 'groups': [], 'auth_enabled': False}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8991)
    args = parser.parse_args()
    print(f'Acceptance state: {STATE}', flush=True)
    uvicorn.run(app, host='127.0.0.1', port=args.port, log_level='warning')
