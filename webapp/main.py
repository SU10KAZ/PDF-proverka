"""
Audit Manager — точка входа FastAPI.
Запуск: cd webapp && python main.py
"""
import sys
import os
from pathlib import Path

# Принудительно UTF-8 для stdout/stderr (Windows cp1251 ломает Unicode-вывод)
# pythonw запускается без stdout/stderr (None) — нужна проверка
if sys.stdout is not None and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr is not None and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Добавляем корень проекта в sys.path чтобы webapp.* работал
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from webapp.config import APP_HOST, APP_PORT
from webapp.routers import projects, findings, tiles, audit, export, usage, optimization, document
from webapp.ws.manager import ws_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle."""
    # Startup: создать папку данных usage
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    # Startup: очистить зомби-задачи
    from webapp.services.pipeline_service import pipeline_manager
    pipeline_manager.cleanup_zombies()
    yield
    # Shutdown: ничего особого


# ─── FastAPI App ────────────────────────────────────────────
app = FastAPI(
    title="Audit Manager",
    description="Управление аудитом проектной документации жилых зданий",
    version="1.0.0",
    lifespan=lifespan,
)

# ─── REST Routers ───────────────────────────────────────────
app.include_router(projects.router)
app.include_router(findings.router)
app.include_router(tiles.router)
app.include_router(audit.router)
app.include_router(export.router)
app.include_router(usage.router)
app.include_router(optimization.router)
app.include_router(document.router)

# ─── WebSocket Endpoints ────────────────────────────────────
@app.websocket("/ws/audit/{project_id}")
async def ws_audit(websocket: WebSocket, project_id: str):
    """WebSocket для live-лога аудита конкретного проекта."""
    await ws_manager.connect_project(websocket, project_id)
    try:
        while True:
            # Клиент может отправлять ping/команды
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
    from webapp.config import BASE_DIR, PROJECTS_DIR, CLAUDE_CLI
    return {
        "app": "Audit Manager",
        "version": "1.0.0",
        "base_dir": str(BASE_DIR),
        "projects_dir": str(PROJECTS_DIR),
        "claude_cli": CLAUDE_CLI,
        "ws_connections": ws_manager.total_connections,
    }


# ─── Static Files & SPA ────────────────────────────────────
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
async def serve_spa():
    """Отдать SPA index.html."""
    index_path = static_dir / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {"message": "Audit Manager API. Frontend пока не создан. Используйте /docs для Swagger."}


# ─── Запуск ─────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n  Audit Manager запускается на http://localhost:{APP_PORT}")
    print(f"  Swagger UI: http://localhost:{APP_PORT}/docs")
    print(f"  Папка проектов: {ROOT_DIR / 'projects'}\n")

    # На Windows uvicorn --reload ломается при изменении файлов (WinError 6).
    # Отключаем reload на Windows; на Linux/Mac — можно включить.
    import platform
    use_reload = platform.system() != "Windows"

    uvicorn.run(
        "webapp.main:app",
        host=APP_HOST,
        port=APP_PORT,
        reload=use_reload,
        reload_dirs=[str(Path(__file__).parent)] if use_reload else None,
    )
