"""Loopback stand for the stage-2 semantic block workspace (sheets + Human Mapping).

Real routers stage_comparison, project_change_preview, project_comparison_catalog,
human_mapping and stage_block_mapping over a COPY of a comparison root placed in a
fresh tmpdir. Upload PDFs/Markdown referenced by the copied pairs are only read.
Mutating requests are accepted only by the sheet-link and Human Mapping routes and
land in the tmpdir copy. No full application startup, worker or inference: any
external connection or subprocess launch aborts the request.

    python scripts/stage_block_mapping_smoke_server.py --copy-from <comparison root copy>
    # or a minimal subset read from any root (the source is only read):
    python scripts/stage_block_mapping_smoke_server.py --copy-from comparison \
        --pairs e6fc8a2725eb4a67/p290a06df79 e6fc8a2725eb4a67/p11ad4a09d9
"""
import argparse
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
STATE = Path(tempfile.mkdtemp(prefix='stage-block-mapping-stand-'))
os.environ['COMPARISON_ROOT'] = str(STATE / 'comparison')
os.environ['AUDIT_STAGE_COMPARISON_ROOTS'] = str(STATE)
os.environ['AUDIT_DISABLE_DOTENV'] = '1'
os.environ['PROJECT_COMPARISON_V3_ALLOW_INFERENCE'] = '0'
os.environ['PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE'] = '1'
os.environ.pop('STAGE_BLOCK_MAPPING_WRITES', None)

# The stand never talks to the outside world and never starts model CLIs.
def loopback_only(event, args):
    if event == 'socket.connect':
        address = args[1]
        if isinstance(address, tuple) and address[0] not in {'127.0.0.1', '::1', 'localhost'}:
            raise RuntimeError('Stand forbids external connections')
    if event in {'subprocess.Popen', 'os.posix_spawn', 'os.exec', 'os.system'}:
        raise RuntimeError(f'Stand forbids process launch: {args[0]!r}')
sys.addaudithook(loopback_only)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware
import uvicorn
from backend.app.api.routers import (human_mapping, project_change_preview, project_comparison_catalog,
                                     stage_block_mapping, stage_comparison)
from backend.app.services.common import object_service
from backend.app.services.project_change_v3 import provider

provider.set_test_provider(provider.FakeProvider(handlers={}))

WRITABLE = [re.compile(p) for p in (
    r'^/api/stage-comparison/sessions$',  # opens the existing shell session (writes only COMPARISON_ROOT)
    r'^/api/stage-comparison/sessions/[^/]+/pairs$',
    r'^/api/stage-comparison/sessions/[^/]+/pairs/[^/]+/sheet-links$',
    r'^/api/stage-comparison/sessions/[^/]+/pairs/[^/]+/sheet-link-repairs/[^/]+/undo$',
    r'^/api/human-mapping/objects/[^/]+/(pairs|comparisons)/[^/]+/(reviews|block-links)$',
    r'^/api/objects/switch$',
)]

app = FastAPI()
app.add_middleware(TrustedHostMiddleware, allowed_hosts=['127.0.0.1', 'localhost', 'testserver'])


@app.middleware('http')
async def read_only_outside_the_copy(request, call_next):
    if request.method not in {'GET', 'HEAD', 'OPTIONS'} and not any(p.match(request.url.path) for p in WRITABLE):
        return JSONResponse({'detail': {'error': 'STAND_READ_ONLY', 'ok': False}}, status_code=403)
    return await call_next(request)


app.include_router(stage_comparison.router)
app.include_router(project_change_preview.router)
app.include_router(project_change_preview.availability_router)
app.include_router(project_comparison_catalog.router)
app.include_router(human_mapping.router)
app.include_router(human_mapping.api_router)
app.include_router(stage_block_mapping.router)
app.mount('/static', StaticFiles(directory=ROOT / 'frontend/static'), name='static')
REGISTRY = []


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


@app.get('/api/objects')
def objects():
    return {'objects': REGISTRY, 'current_id': REGISTRY[0]['id'] if REGISTRY else None}


@app.post('/api/objects/switch')
def switch_object():
    return {'current_id': REGISTRY[0]['id'], 'object': REGISTRY[0]}


@app.get('/api/usage/projects-summary')
def project_usage():
    return {}


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


SUBSET_FILE_LIMIT = 16 * 1024 * 1024
SUBSET_PRODUCTION_DIRS = ('runs', 'project_change_v3')


def copy_subset(source: Path, target: Path, pairs: list[str]) -> None:
    """index.json, the sessions and pairs asked for (sheet map, V3 runs with Human Mapping, small production
    files) and their legacy Human Mapping history; old-pipeline artefacts of hundreds of MB are left out."""
    target.mkdir(parents=True)
    if (source / 'index.json').is_file():
        shutil.copy2(source / 'index.json', target / 'index.json')
    for item in pairs:
        session_id, pair_id = item.split('/')
        session_dir, pair_dir = source / 'sessions' / session_id, source / 'sessions' / session_id / 'pairs' / pair_id
        (target / 'sessions' / session_id / 'pairs').mkdir(parents=True, exist_ok=True)
        shutil.copy2(session_dir / 'session.json', target / 'sessions' / session_id / 'session.json')
        out = target / 'sessions' / session_id / 'pairs' / pair_id
        out.mkdir()
        for entry in pair_dir.iterdir():
            if entry.is_file():
                shutil.copy2(entry, out / entry.name)
        production = pair_dir / 'production'
        if production.is_dir():
            (out / 'production').mkdir()
            for entry in production.iterdir():
                if entry.is_file() and entry.stat().st_size <= SUBSET_FILE_LIMIT:
                    shutil.copy2(entry, out / 'production' / entry.name)
                elif entry.is_dir() and entry.name in SUBSET_PRODUCTION_DIRS:
                    shutil.copytree(entry, out / 'production' / entry.name, symlinks=True)
        for legacy in sorted((source / 'human_mapping').glob(f'*/{pair_id}')):
            shutil.copytree(legacy, target / 'human_mapping' / legacy.parent.name / pair_id, symlinks=True)


def copied_objects(comparison: Path) -> list[dict]:
    """Registry objects (read-only) whose stage folders are those of the copied sessions."""
    import json
    from backend.app.services.stage_comparison import objects as objects_mod
    stage_paths = set()
    for session_json in sorted((comparison / 'sessions').glob('*/session.json')):
        session = json.loads(session_json.read_text(encoding='utf-8'))
        stage_paths.add(str(Path(session.get('stage_a_path') or '/').resolve()))
    owners = {str(item['id']) for item in objects_mod.list_objects().get('items') or []
              if any(str(Path(s.get('path') or '/').resolve()) in stage_paths for s in item.get('stages') or [])}
    return [dict(obj) for obj in object_service.list_objects() if str(obj.get('id')) in owners]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--copy-from', type=Path, required=True,
                        help='comparison root to copy from; it is only read')
    parser.add_argument('--pairs', nargs='+', metavar='SESSION/PAIR',
                        help='copy only these pairs (and their legacy Human Mapping history)')
    parser.add_argument('--port', type=int, default=8992)
    parser.add_argument('--writes', action='store_true', help='STAGE_BLOCK_MAPPING_WRITES=1 inside the stand')
    args = parser.parse_args()
    if args.pairs:
        copy_subset(args.copy_from, STATE / 'comparison', args.pairs)
    else:
        shutil.copytree(args.copy_from, STATE / 'comparison', symlinks=True)
    REGISTRY.extend(copied_objects(STATE / 'comparison'))
    object_service.list_objects = lambda: list(REGISTRY)
    object_service.get_object_by_id = lambda oid: next((o for o in REGISTRY if o['id'] == oid), None)
    if args.writes:
        os.environ['STAGE_BLOCK_MAPPING_WRITES'] = '1'
    print(f'Stand state: {STATE}  objects: {[o["id"] for o in REGISTRY]}', flush=True)
    uvicorn.run(app, host='127.0.0.1', port=args.port, log_level='warning')
