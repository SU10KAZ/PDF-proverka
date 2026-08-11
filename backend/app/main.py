"""
Audit Manager — точка входа FastAPI (backend).
Запуск: uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload
"""
import sys
import os
import re
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
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from backend.app.core.config import APP_HOST, APP_PORT
from backend.app.core import portal_auth
from backend.app.core import current_object as current_object_mw
from backend.app.core import action_log as action_log_core
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
    users,
    model_control,
    lms,
    critic_v2_ui,
    critic_v2_assisted_round1,
    migrated_findings,
    external_register,
    stage_comparison,
    auth,
    projects_v2_shadow,
    schedule,
    action_log,
)
from backend.app.ws.manager import ws_manager


def default_thread_pool_size() -> int:
    """Сколько потоков отдать под asyncio.to_thread.

    THREAD_POOL_WORKERS перекрывает расчёт. По умолчанию — вчетверо больше
    ядер, но не меньше 32: работа здесь блокирующая и в основном ждёт диск
    или дочерний процесс, поэтому потоков нужно больше, чем ядер.
    """
    raw = (os.environ.get("THREAD_POOL_WORKERS") or "").strip()
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return max(32, (os.cpu_count() or 4) * 4)


def _install_default_thread_executor() -> None:
    """Заменить дефолтный executor цикла на пул нужного размера (fail-soft)."""
    try:
        import asyncio
        from concurrent.futures import ThreadPoolExecutor

        workers = default_thread_pool_size()
        asyncio.get_running_loop().set_default_executor(
            ThreadPoolExecutor(max_workers=workers, thread_name_prefix="audit")
        )
        print(f"[startup] пул потоков to_thread: {workers}")
    except Exception as exc:  # noqa: BLE001 — размер пула не повод не стартовать
        print(f"[startup] не удалось расширить пул потоков: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle."""
    # Пул потоков под asyncio.to_thread. По умолчанию Python даёт
    # min(32, ядра+4) ≈ 20 потоков, и этот пул ОБЩИЙ: в нём же сидят
    # длинные норм-задачи (run_in_executor(None, ...)) и весь блокирующий
    # sync-IO конвейера. При нескольких параллельных проектах он выедается,
    # to_thread из других стадий встаёт в очередь, event loop залипает,
    # health-проверка не отвечает — и вотчдог убивает живой аудит.
    # Расширяем заранее: потоки дешёвые, а голодание здесь стоит часов работы.
    _install_default_thread_executor()

    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    from backend.app.pipeline.manager import pipeline_manager
    pipeline_manager.cleanup_zombies()
    pipeline_manager._recover_stale_pipelines()
    pipeline_manager.load_persisted_queue()
    from backend.app.pipeline.stages.prepare.prepare_service import load_persisted_queue as load_prepare_queue
    load_prepare_queue()
    # Авто-возобновление прерванной батч-очереди (инцидент 03.07: после kill
    # вотчдогом очередь простаивала до ручного POST /batch/resume). Отложенный
    # фоновый таск; kill-switch BATCH_AUTO_RESUME_ENABLED=false.
    import asyncio as _asyncio
    _auto_resume_task = _asyncio.create_task(
        pipeline_manager.auto_resume_interrupted_batch()
    )
    # Журнал действий: мост logging (WARNING+ всех модулей → журнал) и метка
    # старта сервера — по ним в анализе видны рестарты/падения.
    action_log_core.install_logging_bridge()
    action_log_core.log_event("system", event="startup")
    yield
    action_log_core.log_event("system", event="shutdown")
    # Воркеры векторных графов блоков живут дольше запроса — гасим явно,
    # чтобы после рестарта не оставались осиротевшие python-процессы.
    from backend.app.pipeline.stages.block_context.builder import shutdown_pool
    shutdown_pool()
    # Снять мост с process-global root: в проде безвредно, а в тестах
    # `with TestClient(app)` хендлер не переживает выход из контекста.
    action_log_core.uninstall_logging_bridge()


_GZIP_SKIP_PATH_RE = re.compile(
    r"^/api/(tiles/.+/blocks/(image|region-image)/|document/.+/page/)"
)


class _ImageSafeGZipMiddleware(GZipMiddleware):
    """GZip для всего, кроме бинарных картинок.

    Starlette решает, сжимать ли ответ, ТОЛЬКО по размеру тела (>= minimum_size)
    и не смотрит на content-type. PNG/WebP уже сжаты: deflate уровня 9 на них
    даёт околонулевую экономию, но тратит CPU прямо в event loop. При открытии
    вкладки «Блоки» это десятки кропов подряд, вплоть до ~2 МБ каждый.
    """

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http" and _GZIP_SKIP_PATH_RE.match(scope.get("path") or ""):
            await self.app(scope, receive, send)
            return
        await super().__call__(scope, receive, send)


app = FastAPI(
    title="Audit Manager",
    description="Управление аудитом проектной документации жилых зданий",
    version="1.0.0",
    lifespan=lifespan,
)

# ─── Per-request «текущий объект» ───────────────────────────
# Добавлен ПЕРВЫМ → innermost: напрямую оборачивает роутер (между ним и
# обработчиком нет BaseHTTPMiddleware), поэтому ContextVar, который он ставит из
# заголовка X-Object-Id, гарантированно виден в эндпоинте. Делает выбор объекта
# per-request вместо глобального current_id (иначе переключение объекта одним
# инженером «прячет» проекты у остальных). Fail-soft, тело не трогает.
app.add_middleware(current_object_mw.CurrentObjectMiddleware)

# ─── Сжатие ответов (gzip) ──────────────────────────────────
# Фронт-бандл app.js ~920 КБ; без сжатия каждый некэшированный заход (hard
# refresh) тянет его целиком по WAN (сервер за cloudflared в KZ) → «долго
# грузится». gzip сжимает JS/HTML/JSON в ~4–6 раз. minimum_size, чтобы не
# тратить CPU на мелочь. Добавлен раньше PortalAuth → PortalAuth остаётся
# внешним, gzip сжимает финальные ответы приложения и статики.
#
# ВАЖНО: GZipMiddleware смотрит только на размер тела, НЕ на content-type, и
# жмёт уровнем 9 в event loop. Для уже сжатых PNG/WebP это нулевая экономия и
# чистая нагрузка на цикл — а кропы бывают под 2 МБ и грузятся десятками при
# открытии вкладки «Блоки». Исключаем бинарные картинки по пути.
app.add_middleware(_ImageSafeGZipMiddleware, minimum_size=1000)

# ─── Portal auth ────────────────────────────────────────────
# Простая защита портала логином/паролем. Включается через PORTAL_AUTH_ENABLED.
# При выключенном auth middleware — no-op (поведение портала не меняется).
app.add_middleware(portal_auth.PortalAuthMiddleware)

# ─── Журнал действий ────────────────────────────────────────
# Сквозной журнал: каждый HTTP-запрос (кроме шумового поллинга) пишется в
# logs/actions/actions-YYYY-MM-DD.jsonl — кто, что, когда, статус, длительность,
# ошибки с трейсбеком. Добавлен ПОСЛЕДНИМ → внешний: видит и 401 от PortalAuth
# (неавторизованные попытки — тоже действия). Fail-soft, тело запроса/ответа
# не читает (безопасен для стриминга и загрузок). Kill-switch: ACTION_LOG_ENABLED.
app.add_middleware(action_log_core.ActionLogMiddleware)

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
app.include_router(users.router)
app.include_router(schedule.router)
app.include_router(model_control.router)
app.include_router(lms.router)
app.include_router(critic_v2_ui.router)
app.include_router(critic_v2_assisted_round1.router)
app.include_router(external_register.router)
app.include_router(stage_comparison.router)
app.include_router(auth.router)
# Read-only shadow API над projects_v2. Все endpoint'ы gated флагом
# AUDIT_PROJECTS_V2_SHADOW_API_ENABLED (default false → 404). При выключенном
# флаге роутер инертен, production/UI не меняется.
app.include_router(projects_v2_shadow.router)
# Чтение журнала действий (сам журнал пишет ActionLogMiddleware + хуки).
app.include_router(action_log.router)
# migrated_findings уже подключён выше — повторно не подключаем.

# ─── Распределённые audit-worker (этап 0) ───────────────────
# Роутеры регистрируются ТОЛЬКО при DISTRIBUTED_WORKERS_ENABLED=true. При
# выключенном флаге путей нет вовсе (404), SQLite-база не создаётся и фоновых
# задач не появляется — существующая платформа работает без изменений.
# Исключение: /api/workers/status отдаётся всегда, чтобы фронт мог честно
# показать «функция отключена» вместо пустого экрана.
from backend.app.services.distributed_workers.settings import (  # noqa: E402
    get_settings as _dw_settings,
)

from backend.app.api.routers import audit_workers_admin  # noqa: E402

# /api/workers/status отвечает всегда — фронт по нему понимает, что показывать.
app.include_router(audit_workers_admin.status_router)

if _dw_settings().enabled:
    from backend.app.api.routers import audit_worker_agent  # noqa: E402
    from backend.app.core import portal_auth as _portal_auth  # noqa: E402

    app.include_router(audit_worker_agent.router)

    # Операторский контур собственной аутентификации не имеет — он целиком
    # опирается на портальную. Без неё `POST /api/workers/{id}/rotate-token`
    # отдал бы живой токен воркера любому, кто дотянулся до порта. Поднимаем
    # его только при включённой портальной защите либо при ЯВНОМ признании
    # риска (локальный пилот).
    _dw_admin_ok = (
        _portal_auth.get_settings().enabled or _dw_settings().allow_insecure_admin
    )
    if _dw_admin_ok:
        app.include_router(audit_workers_admin.router)
        from backend.app.api.routers import worker_bootstrap  # noqa: E402
        app.include_router(worker_bootstrap.router)
        print("[startup] распределённые audit-worker: ВКЛЮЧЕНЫ")
    else:
        print(
            "[startup] распределённые audit-worker: контур воркеров включён, "
            "ОПЕРАТОРСКИЙ контур НЕ поднят — PORTAL_AUTH_ENABLED=false. "
            "Включите портальную авторизацию или, для локального пилота, "
            "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN=true."
        )

# ─── WebSocket Endpoints ────────────────────────────────────
def _ws_authorized(websocket: WebSocket) -> bool:
    """Проверить session-cookie на WebSocket (middleware ws не перехватывает)."""
    settings = portal_auth.get_settings()
    if not settings.enabled:
        return True
    token = websocket.cookies.get(settings.cookie_name)
    return bool(token and portal_auth.verify_token(token, settings))


@app.websocket("/ws/audit/{project_id}")
async def ws_audit(websocket: WebSocket, project_id: str):
    """WebSocket для live-лога аудита конкретного проекта."""
    if not _ws_authorized(websocket):
        await websocket.close(code=1008)
        return
    await ws_manager.connect_project(websocket, project_id)
    try:
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect_project(websocket, project_id)


@app.websocket("/ws/global")
async def ws_global(websocket: WebSocket):
    """WebSocket для глобальных событий (все проекты)."""
    if not _ws_authorized(websocket):
        await websocket.close(code=1008)
        return
    await ws_manager.connect_global(websocket)
    try:
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect_global(websocket)


def _disk_stats(path) -> dict:
    """Свободное место на ФС данных. Fail-soft: /api/info обязан отвечать всегда."""
    import shutil

    try:
        usage = shutil.disk_usage(str(path))
    except OSError:
        return {"available": False}
    return {
        "available": True,
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "used_percent": round(usage.used * 100 / usage.total, 1) if usage.total else None,
    }


# ─── API Info ───────────────────────────────────────────────
@app.get("/api/info")
async def api_info():
    """Информация о сервере.

    ``data_roots`` явно раскрывает, какие РУНТАЙМ-data-роуты реально читает
    backend (а не только ``base_dir`` = worktree кода). Это нужно для
    диагностики active-root drift: ``base_dir`` может указывать на deploy
    worktree, тогда как данные (projects / app_data / comparison) редиректятся
    в MAIN через env (AUDIT_DATA_DIR / COMPARISON_ROOT). Только пути — без
    секретов, токенов и env целиком.
    """
    from backend.app.core.config import (
        ROOT_DIR, PROJECTS_DIR, DATA_DIR, APP_DATA_DIR, get_claude_cli,
    )
    from backend.app.services.stage_comparison.paths import comparison_root_path
    try:
        comparison_root = str(comparison_root_path())
    except Exception:  # noqa: BLE001 — /api/info обязан оставаться быстрым/живым
        comparison_root = None
    return {
        "app": "Audit Manager",
        "version": "1.0.0",
        "base_dir": str(ROOT_DIR),
        "projects_dir": str(PROJECTS_DIR),
        "claude_cli": get_claude_cli(),
        "ws_connections": ws_manager.total_connections,
        # явные runtime data-роуты (для root-drift диагностики)
        "data_roots": {
            "audit_data_dir": str(DATA_DIR),
            "audit_app_data_dir": str(APP_DATA_DIR),
            "projects_dir": str(PROJECTS_DIR),
            "comparison_root": comparison_root,
        },
        # Контроля свободного места до сих пор не было нигде, хотя диск дважды
        # доводили до 100% (инцидент с обнулением stage_models.json). Отдаём
        # прямо в health-эндпоинте — дёшево (один statvfs) и всегда под рукой.
        "disk": _disk_stats(DATA_DIR),
    }


# ─── Static Files & SPA ────────────────────────────────────
# HTML-страницы берём из frontend/ (рядом с index.html / model-control.html).
# /static монтируем из frontend/static/ (js/ и css/ лежат там).
_frontend_dir = ROOT_DIR / "frontend"
_frontend_static_dir = _frontend_dir / "static"

_static_mount_dir = _frontend_static_dir if _frontend_static_dir.exists() else None

if _static_mount_dir is not None:
    app.mount("/static", StaticFiles(directory=str(_static_mount_dir)), name="static")

_html_dir = _frontend_dir


@app.get("/login")
async def serve_login(request: Request):
    """Отдать self-contained страницу входа.

    Когда auth выключен — вход не нужен, редирект на `/`. Если пользователь
    уже залогинен — тоже на `/`.
    """
    settings = portal_auth.get_settings()
    if not settings.enabled:
        return RedirectResponse("/", status_code=302)
    if portal_auth.request_username(request, settings):
        return RedirectResponse("/", status_code=302)
    login_path = _html_dir / "login.html"
    if login_path.exists():
        return HTMLResponse(login_path.read_text(encoding="utf-8"))
    return HTMLResponse(
        "<!doctype html><meta charset=utf-8><title>Вход</title>"
        "<p>Страница входа не найдена (login.html).</p>",
        status_code=200,
    )


@app.get("/")
async def serve_spa():
    """Отдать SPA index.html.

    Подменяет cache-bust токены `{{css_version}}` и `{{js_version}}` на mtime
    `styles.css` / `app.js`. Считается max(mtime app.js, mtime version_api.js),
    чтобы правка любого из двух JS принудительно инвалидировала кеш браузера.
    """
    index_path = _html_dir / "index.html"
    if not index_path.exists():
        return {"message": "Audit Manager API. Frontend not found. Use /docs for Swagger."}
    css_path = (_static_mount_dir / "css" / "styles.css") if _static_mount_dir else None
    js_path = (_static_mount_dir / "js" / "app.js") if _static_mount_dir else None
    vapi_path = (_static_mount_dir / "js" / "version_api.js") if _static_mount_dir else None
    pauth_path = (_static_mount_dir / "js" / "portal_auth.js") if _static_mount_dir else None
    css_ver = int(css_path.stat().st_mtime) if css_path and css_path.exists() else 0
    js_mtimes = [int(p.stat().st_mtime) for p in (js_path, vapi_path, pauth_path) if p and p.exists()]
    js_ver = max(js_mtimes) if js_mtimes else 0
    html = index_path.read_text(encoding="utf-8")
    html = html.replace("{{css_version}}", str(css_ver)).replace("{{js_version}}", str(js_ver))
    # SPA-точка входа не должна кэшироваться: иначе браузер держит старый index.html
    # со старым ?v= и не подхватывает свежий CSS/JS. Сами css/js версионируются mtime
    # и кэшируются нормально — no-cache нужен только для HTML-обёртки.
    return HTMLResponse(html, headers={"Cache-Control": "no-cache, must-revalidate"})


@app.get("/audit-workers")
async def serve_audit_workers():
    """Отдать экран «Аудит-воркеры».

    Отдельная страница по образцу /model-control: так экран не требует правок
    в 19-тысячестрочном app.js и не может сломать основной SPA. Сама страница
    сначала спрашивает /api/workers/status и при выключенном флаге честно
    показывает «функция отключена».
    """
    page_path = _html_dir / "audit-workers.html"
    if not page_path.exists():
        return {"message": "Audit workers page not found"}
    css_path = (_static_mount_dir / "css" / "audit-workers.css") if _static_mount_dir else None
    js_path = (_static_mount_dir / "js" / "audit-workers.js") if _static_mount_dir else None
    css_ver = int(css_path.stat().st_mtime) if css_path and css_path.exists() else 0
    js_ver = int(js_path.stat().st_mtime) if js_path and js_path.exists() else 0
    html = page_path.read_text(encoding="utf-8")
    html = html.replace("{{css_version}}", str(css_ver)).replace("{{js_version}}", str(js_ver))
    return HTMLResponse(html, headers={"Cache-Control": "no-cache, must-revalidate"})


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
    return HTMLResponse(html, headers={"Cache-Control": "no-cache, must-revalidate"})


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
