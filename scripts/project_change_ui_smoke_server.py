"""Isolated local UI smoke: real read-only preview router; minimal portal shell.

No full application startup, workers, decision stores or external inference.
Run from the repository root with python scripts/project_change_ui_smoke_server.py.
"""
import argparse
from pathlib import Path
import sys

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.app.api.routers.project_change_preview import router
from backend.app.services.project_change_preview.service import OBJECT

app = FastAPI()
app.include_router(router)
app.mount('/static', StaticFiles(directory=ROOT / 'frontend/static'), name='static')

@app.get('/')
def index():
    return FileResponse(ROOT / 'frontend/index.html')

@app.get('/api/objects')
def objects():
    return {'objects': [{'id': OBJECT, 'name': '272. Садовническая 76 / Балчуг Эстейт'},
                        {'id': 'OTHER_OBJECT', 'name': 'Другой объект'}], 'current_id': OBJECT}

@app.get('/api/auth/me')
@app.get('/api/auth/status')
@app.get('/api/users')
@app.get('/api/projects')
@app.get('/api/disciplines')
@app.get('/api/objects/{object_id}/disciplines')
def shell():
    return {'items': [], 'projects': [], 'users': [], 'disciplines': [], 'auth_enabled': False}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8991)
    args = parser.parse_args()
    uvicorn.run(app, host='127.0.0.1', port=args.port, log_level='warning')
