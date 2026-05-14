"""
Audit Manager — точка входа FastAPI (backend).
Запуск: uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload
"""
import sys
import os
from pathlib import Path

# Принудительно UTF-8 для stdout/stderr
if sys.stdout is not None and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr is not None and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Добавляем корень проекта в sys.path чтобы norms.*, backend.* работали
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

# Загружаем .env из корня проекта
_env_file = ROOT_DIR / ".env"
if _env_file.exists():
    for _line in _env_file.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse

from backend.app.core.config import APP_HOST, APP_PORT
from backend.app.api.routers import (
    projects,
    findings,
    blocks,
    audit,
    export,
    usage,
    optimization,
    document,
    discussions,
    knowledge_base,
    objects,
    model_control,
    lms,
    critic_v2_ui,
    critic_v2_assisted_round1,
    migrated_findings,
)
from backend.app.ws.manager import ws_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle."""
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    from backend.app.pipeline.manager import pipeline_manager
    pipeline_manager.cleanup_zombies()
    pipeline_manager._recover_stale_pipelines()
    pipeline_manager.load_persisted_queue()
    from backend.app.pipeline.stages.prepare.prepare_service import load_persisted_queue as load_prepare_queue
    load_prepare_queue()
    yield


app = FastAPI(
    title="Audit Manager",
    description="Управление аудитом проектной документации жилых зданий",
    version="1.0.0",
    lifespan=lifespan,
)

# ─── REST Routers ───────────────────────────────────────────
# migrated_findings регистрируется ДО projects.router, потому что в projects
# зарегистрирован catch-all `GET /api/projects/{project_id:path}`, который
# иначе перехватит более специфичные эндпоинты с тем же префиксом.
app.include_router(migrated_findings.router)
app.include_router(projects.router)
app.include_router(projects.groups_router)
app.include_router(findings.router)
app.include_router(blocks.router)
app.include_router(audit.router)
app.include_router(export.router)
app.include_router(usage.router)
app.include_router(optimization.router)
app.include_router(document.router)
app.include_router(discussions.router)
app.include_router(knowledge_base.router)
app.include_router(objects.router)
app.include_router(model_control.router)
app.include_router(lms.router)
app.include_router(critic_v2_ui.router)
app.include_router(critic_v2_assisted_round1.router)
# migrated_findings уже подключён выше — повторно не подключаем.

# ─── WebSocket Endpoints ────────────────────────────────────
@app.websocket("/ws/audit/{project_id}")
async def ws_audit(websocket: WebSocket, project_id: str):
    """WebSocket для live-лога аудита конкретного проекта."""
    await ws_manager.connect_project(websocket, project_id)
    try:
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect_project(websocket, project_id)


@app.websocket("/ws/global")
async def ws_global(websocket: WebSocket):
    """WebSocket для глобальных событий (все проекты)."""
    await ws_manager.connect_global(websocket)
    try:
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect_global(websocket)


# ─── API Info ───────────────────────────────────────────────
@app.get("/api/info")
async def api_info():
    """Информация о сервере."""
    from backend.app.core.config import ROOT_DIR, PROJECTS_DIR, get_claude_cli
    return {
        "app": "Audit Manager",
        "version": "1.0.0",
        "base_dir": str(ROOT_DIR),
        "projects_dir": str(PROJECTS_DIR),
        "claude_cli": get_claude_cli(),
        "ws_connections": ws_manager.total_connections,
    }


# ─── Static Files & SPA ────────────────────────────────────
# HTML-страницы берём из frontend/ (рядом с index.html / model-control.html).
# /static монтируем из frontend/static/ (js/ и css/ лежат там).
# Fallback на webapp/static/ для обратной совместимости.
_frontend_dir = ROOT_DIR / "frontend"
_frontend_static_dir = _frontend_dir / "static"
_webapp_static_dir = ROOT_DIR / "webapp" / "static"

if _frontend_static_dir.exists():
    _static_mount_dir = _frontend_static_dir
elif _webapp_static_dir.exists():
    _static_mount_dir = _webapp_static_dir
else:
    _static_mount_dir = None

if _static_mount_dir is not None:
    app.mount("/static", StaticFiles(directory=str(_static_mount_dir)), name="static")

_html_dir = _frontend_dir if _frontend_dir.exists() else _webapp_static_dir


@app.get("/")
async def serve_spa():
    """Отдать SPA index.html."""
    index_path = _html_dir / "index.html"
    if not index_path.exists():
        return {"message": "Audit Manager API. Frontend not found. Use /docs for Swagger."}
    css_path = (_static_mount_dir / "css" / "styles.css") if _static_mount_dir else None
    js_path = (_static_mount_dir / "js" / "app.js") if _static_mount_dir else None
    css_ver = int(css_path.stat().st_mtime) if css_path and css_path.exists() else 0
    js_ver = int(js_path.stat().st_mtime) if js_path and js_path.exists() else 0
    html = index_path.read_text(encoding="utf-8")
    html = html.replace("{{css_version}}", str(css_ver)).replace("{{js_version}}", str(js_ver))
    return HTMLResponse(html)


@app.get("/model-control")
async def serve_model_control():
    """Отдать страницу управления моделями."""
    page_path = _html_dir / "model-control.html"
    if not page_path.exists():
        return {"message": "Model control page not found"}
    css_path = (_static_mount_dir / "css" / "model-control.css") if _static_mount_dir else None
    js_path = (_static_mount_dir / "js" / "model-control.js") if _static_mount_dir else None
    css_ver = int(css_path.stat().st_mtime) if css_path and css_path.exists() else 0
    js_ver = int(js_path.stat().st_mtime) if js_path and js_path.exists() else 0
    html = page_path.read_text(encoding="utf-8")
    html = html.replace("{{css_version}}", str(css_ver)).replace("{{js_version}}", str(js_ver))
    return HTMLResponse(html)


# ─── Запуск ─────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n  Audit Manager запускается на http://localhost:{APP_PORT}")
    print(f"  Swagger UI: http://localhost:{APP_PORT}/docs")
    print(f"  Папка проектов: {ROOT_DIR / 'projects'}\n")

    import platform
    use_reload = platform.system() != "Windows"

    uvicorn.run(
        "backend.app.main:app",
        host=APP_HOST,
        port=APP_PORT,
        reload=use_reload,
        reload_dirs=[str(ROOT_DIR / "backend")] if use_reload else None,
    )
