"""Real frozen-source bridge, loopback only; no production startup or fixture events.

python scripts/project_change_bridge_preview.py --state-dir /tmp/project-change-bridge-v1
"""
import argparse
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware
from backend.app.api.routers.project_change_preview import router
from backend.app.services.project_change_preview.service import PreviewService
from backend.app.services.project_change_preview.sources import OBJECT


def create_app(state_dir, actor='codex-local-preview'):
    app = FastAPI(title='ProjectChange RESEARCH / PREVIEW')
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=['127.0.0.1', 'localhost', 'testserver'])
    app.state.project_change_preview_enabled = True
    app.state.project_change_preview_service = PreviewService(state_dir)
    app.state.project_change_preview_actor = actor
    app.include_router(router)
    app.mount('/static', StaticFiles(directory=REPO/'frontend/static'), name='static')

    @app.get('/')
    def index():
        return FileResponse(REPO/'frontend/index.html')

    # Shared portal shell only. ProjectChanges, PDFs and decisions are served
    # exclusively by the real bridge router above, never fixture JSON.
    @app.get('/api/objects')
    def objects():
        return {'objects':[{'id':OBJECT,'name':'272. Садовническая 76 / Балчуг Эстейт'}], 'current_id':OBJECT}

    @app.get('/api/auth/me')
    @app.get('/api/auth/status')
    @app.get('/api/users')
    @app.get('/api/projects')
    @app.get('/api/disciplines')
    @app.get('/api/objects/{object_id}/disciplines')
    def shell():
        return {'items':[],'projects':[],'users':[],'disciplines':[],'auth_enabled':False}

    return app


if __name__ == '__main__':
    import uvicorn
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8974)
    parser.add_argument('--state-dir', type=Path, default=Path('/tmp/project-change-bridge-v1'))
    parser.add_argument('--actor', default='codex-local-preview', help='Explicit local preview identity, not a portal employee')
    args = parser.parse_args()
    if not args.actor.strip():
        parser.error('actor must not be empty')
    app = create_app(args.state_dir, args.actor)
    print(f'RESEARCH / PREVIEW: http://127.0.0.1:{args.port}/?projectChangeUi=1#/stage-comparison', flush=True)
    uvicorn.run(app, host='127.0.0.1', port=args.port, log_level='warning')
