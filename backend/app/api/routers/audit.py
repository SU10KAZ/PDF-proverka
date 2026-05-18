"""
REST API для запуска и управления аудитом.
"""
import asyncio
import json
import re
import subprocess
import traceback
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, Body, HTTPException, Query
from backend.app.pipeline.manager import pipeline_manager
import backend.app.services.common.project_service as project_service
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.core.config import (
    CLAUDE_CLI,
    get_claude_cli,
    get_claude_model, set_claude_model, CLAUDE_MODEL_OPTIONS,
    get_stage_models, set_stage_model, get_model_for_stage,
    STAGE_MODELS_OPENROUTER, GEMINI_MODEL, GPT_MODEL,
    STAGE_MODEL_CONFIG, AVAILABLE_MODELS, get_stage_model, is_claude_stage,
    _save_stage_model_config,
    validate_current_stage_model_config, validate_stage_model_choice,
)

router = APIRouter(prefix="/api/audit", tags=["audit"])


# ─── Paid API guard: helper для endpoint'ов запуска ──────────────────
def _issue_manual_run_if_allowed(
    project_ids: list[str] | str,
    *,
    paid_api_allowed: bool,
    batch_id: str = "",
) -> Optional[str]:
    """Если пользователь нажал кнопку с галкой "Разрешить платные API",
    выдать manual_run_id для конкретного scope. Иначе None.

    Auto-resume/retry/orphan/фоновые задачи НЕ имеют права передавать
    paid_api_allowed=True — это контролируется на уровне endpoint'а.
    """
    if not paid_api_allowed:
        return None
    try:
        from backend.app.services.llm.paid_api_guard import issue_manual_run
        return issue_manual_run(project_ids=project_ids, batch_id=batch_id)
    except (ValueError, ImportError):
        return None


# ─── prepare-data queue control ──────────────────────────────────
# Регистрируются ПЕРВЫМИ, потому что ниже есть `/{project_id:path}/resume`
# (для audit job pause/resume), который перехватывает любой `/audit/.../resume`.
# Префикс /prepare-data/queue/ — фиксированный, не path-параметр.

@router.post("/prepare-data/queue/pause")
async def prepare_data_pause():
    """Поставить очередь prepare-data на паузу. Текущий блок дойдёт, потом ожидание."""
    from backend.app.pipeline.stages.prepare.prepare_service import pause_queue
    return await pause_queue()


@router.post("/prepare-data/queue/resume")
async def prepare_data_resume():
    """Снять паузу с очереди prepare-data."""
    from backend.app.pipeline.stages.prepare.prepare_service import resume_queue
    return await resume_queue()


@router.post("/prepare-data/queue/cancel")
async def prepare_data_cancel():
    """Отменить очередь prepare-data: pending → skipped, текущий блок дойдёт до конца."""
    from backend.app.pipeline.stages.prepare.prepare_service import cancel_queue
    return await cancel_queue()


@router.post("/prepare-data/{project_id:path}/retry-failed")
async def prepare_data_retry_failed(project_id: str):
    """Перепрогнать только упавшие блоки прошлого enrichment'а данного проекта.

    Использует тот же Gemma-лок что и обычный prepare. Не делает full re-enrich,
    обрабатывает только block_id'ы из summary.failed.
    """
    from backend.app.pipeline.stages.prepare.prepare_service import start_retry_failed
    return await start_retry_failed(project_id)


async def _safe_task(coro, name: str = "task"):
    """Обёртка для asyncio.create_task — логирует ошибки в stdout."""
    try:
        return await coro
    except asyncio.CancelledError:
        print(f"[AUDIT] {name}: отменено")
        raise
    except Exception as e:
        print(f"[AUDIT] {name}: ИСКЛЮЧЕНИЕ: {e}")
        traceback.print_exc()
        raise


# ─── Статичные роуты (ПЕРЕД динамическими /{project_id}/...) ───

@router.get("/model")
async def get_model():
    """Текущая модель (default) и доступные опции.

    Возвращает как legacy Claude модели, так и OpenRouter модели.
    """
    openrouter_options = sorted(set(STAGE_MODELS_OPENROUTER.values()))
    return {
        "model": get_claude_model(),
        "options": CLAUDE_MODEL_OPTIONS,
        "openrouter_models": openrouter_options,
        "openrouter_default": GPT_MODEL,
    }


@router.post("/model")
async def switch_model(model: str = Query(..., description="ID модели")):
    """Переключить модель (legacy Claude CLI)."""
    if model not in CLAUDE_MODEL_OPTIONS:
        raise HTTPException(400, f"Неизвестная модель. Доступны: {CLAUDE_MODEL_OPTIONS}")
    set_claude_model(model)
    return {"model": get_claude_model()}


@router.get("/model/stages")
async def get_stage_model_config():
    """Настройки per-stage моделей (унифицированный конфиг).

    Возвращает текущий маппинг этап → модель (Claude CLI + OpenRouter).
    """
    from backend.app.core.config import STAGE_MODEL_RESTRICTIONS, STAGE_MODEL_HINTS
    return {
        "stages": dict(STAGE_MODEL_CONFIG),
        "available_models": AVAILABLE_MODELS,
        "restrictions": STAGE_MODEL_RESTRICTIONS,
        "hints": STAGE_MODEL_HINTS,
        "config_errors": validate_current_stage_model_config(),
    }


@router.post("/model/stages")
async def set_stage_model_config(request: dict):
    """Установить модели для всех этапов (bulk update).

    Body: {"text_analysis": "claude-opus-4-7", "block_batch": "openai/gpt-5.4", ...}
    """
    updated = {}
    rejected = {}
    for stage, model in request.items():
        reason = validate_stage_model_choice(stage, model)
        if reason:
            rejected[stage] = reason
            continue
        STAGE_MODEL_CONFIG[stage] = model
        # Синхронизация с legacy конфигами
        STAGE_MODELS_OPENROUTER[stage] = model
        if model.startswith("claude-"):
            set_stage_model(stage, model)
        else:
            set_stage_model(stage, None)
        updated[stage] = model
    # Персистим на диск — переживёт рестарт сервера
    if updated:
        _save_stage_model_config()
    return {
        "status": "partial" if rejected else "ok",
        "updated": updated,
        "rejected": rejected,
        "stages": dict(STAGE_MODEL_CONFIG),
        "config_errors": validate_current_stage_model_config(),
    }


@router.get("/model/batch-modes")
async def get_stage_batch_modes_config():
    """Текущие batch-режимы этапов (расширенные режимы поверх per-stage модели).

    Сейчас используется только block_batch:
      - "findings_only_gemma_pair"   — production single-block GPT-5.4 + Gemma enrichment
    """
    from backend.app.core.config import STAGE_BATCH_MODES, STAGE_BATCH_MODE_CHOICES
    return {
        "modes": dict(STAGE_BATCH_MODES),
        "choices": STAGE_BATCH_MODE_CHOICES,
    }


@router.post("/model/batch-modes")
async def set_stage_batch_modes_config(request: dict):
    """Установить batch-режимы этапов.

    Body: {"block_batch": "findings_only_gemma_pair"}.
    """
    from backend.app.core.config import set_stage_batch_mode, STAGE_BATCH_MODES, STAGE_BATCH_MODE_CHOICES
    updated = {}
    rejected = {}
    for stage, mode in request.items():
        if stage not in STAGE_BATCH_MODE_CHOICES:
            rejected[stage] = f"unknown stage (choices: {list(STAGE_BATCH_MODE_CHOICES)})"
            continue
        if mode not in STAGE_BATCH_MODE_CHOICES[stage]:
            rejected[stage] = f"invalid mode (choices: {STAGE_BATCH_MODE_CHOICES[stage]})"
            continue
        if set_stage_batch_mode(stage, mode):
            updated[stage] = mode
    if rejected:
        return {"status": "partial", "updated": updated, "rejected": rejected,
                "modes": dict(STAGE_BATCH_MODES)}
    return {"status": "ok", "updated": updated, "modes": dict(STAGE_BATCH_MODES)}


@router.get("/account")
async def get_claude_account():
    """Получить информацию о текущем аккаунте Claude CLI."""
    try:
        cli = get_claude_cli()
        result = subprocess.run(
            [cli, "auth", "status", "--json"],
            capture_output=True, text=True, timeout=10,
            encoding="utf-8", errors="replace",
            shell=cli.endswith(".cmd"),
        )
        if result.returncode == 0 and result.stdout.strip():
            data = json.loads(result.stdout.strip())
            return {
                "email": data.get("email", "—"),
                "org": data.get("orgName", "—"),
                "plan": data.get("subscriptionType", "—"),
                "loggedIn": data.get("loggedIn", False),
            }
        return {"email": "—", "org": "—", "plan": "—", "loggedIn": False, "error": "CLI не авторизован"}
    except Exception as e:
        return {"email": "—", "org": "—", "plan": "—", "loggedIn": False, "error": str(e)}


# ─── Смена аккаунта ───
# Хранит текущий процесс login и auth URL
_login_state: dict = {"proc": None, "url": None, "done": False}


@router.post("/account/switch")
async def switch_claude_account():
    """Выйти из текущего аккаунта и начать логин в новый. Возвращает auth URL."""
    global _login_state
    cli = get_claude_cli()
    shell = cli.endswith(".cmd")

    # 1. Logout
    subprocess.run(
        [cli, "auth", "logout"],
        capture_output=True, text=True, timeout=10,
        encoding="utf-8", errors="replace", shell=shell,
    )

    # 2. Запустить login в фоне
    import platform
    kwargs = {}
    if platform.system() == "Windows":
        kwargs["creationflags"] = 0x08000000  # CREATE_NO_WINDOW

    proc = subprocess.Popen(
        [cli, "auth", "login"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace",
        shell=shell, **kwargs,
    )
    _login_state = {"proc": proc, "url": None, "done": False}

    # 3. Прочитать вывод до появления URL (макс 5 сек)
    import threading

    def _read_output():
        for line in proc.stdout:
            url_match = re.search(r'(https://claude\.ai/oauth/authorize\S+)', line)
            if url_match:
                _login_state["url"] = url_match.group(1)
            if "Login successful" in line:
                _login_state["done"] = True
                break

    t = threading.Thread(target=_read_output, daemon=True)
    t.start()
    t.join(timeout=5)

    if _login_state["url"]:
        return {"status": "waiting", "auth_url": _login_state["url"]}
    return {"status": "started", "message": "Ожидание URL авторизации..."}


@router.get("/account/switch/status")
async def switch_account_status():
    """Проверить статус login — завершён ли."""
    if _login_state.get("done"):
        # Получить данные нового аккаунта
        try:
            cli = get_claude_cli()
            result = subprocess.run(
                [cli, "auth", "status", "--json"],
                capture_output=True, text=True, timeout=10,
                encoding="utf-8", errors="replace",
                shell=cli.endswith(".cmd"),
            )
            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout.strip())
                return {
                    "status": "done",
                    "email": data.get("email", "—"),
                    "plan": data.get("subscriptionType", "—"),
                }
        except Exception:
            pass
        return {"status": "done"}

    if _login_state.get("url"):
        return {"status": "waiting", "auth_url": _login_state["url"]}
    return {"status": "pending"}


@router.post("/all/full")
async def start_all_projects():
    """Запустить полный конвейер для ВСЕХ проектов последовательно."""
    if pipeline_manager.is_running("__ALL__"):
        raise HTTPException(409, "Массовый аудит уже запущен")

    asyncio.create_task(
        _safe_task(pipeline_manager.start_all_projects(), "start_all_projects")
    )
    return {"status": "started", "message": "Полный конвейер запущен для всех проектов"}


@router.post("/batch")
async def start_batch_action(request: dict):
    """Запустить групповое действие для выбранных проектов.

    Если в request передано "paid_api_allowed": true, выдаётся один общий
    batch_manual_run_id для всех проектов batch — все Stage 02/discussion
    внутри этих job будут разрешены guard'ом. Без галки — все платные
    этапы заблокируются ДО network request (auto-resume safety).
    """
    from backend.app.models.audit import BatchRequest
    req = BatchRequest(**request)

    if pipeline_manager.is_running("__BATCH__"):
        raise HTTPException(409, "Групповое действие уже выполняется")
    if pipeline_manager.is_running("__ALL__"):
        raise HTTPException(409, "Массовый аудит уже запущен")

    # Валидация проектов
    valid_ids = []
    for pid in req.project_ids:
        status = project_service.get_project_status(pid)
        if status and status.has_pdf:
            valid_ids.append(pid)

    if not valid_ids:
        raise HTTPException(400, "Нет валидных проектов для обработки")

    # Paid-API: один batch_manual_run_id на весь batch.
    batch_manual_run_id = _issue_manual_run_if_allowed(
        valid_ids,
        paid_api_allowed=bool(getattr(req, "paid_api_allowed", False)),
        batch_id=f"batch_{req.action.value}",
    )

    queue = await pipeline_manager.start_batch(
        valid_ids, req.action.value, manual_run_id=batch_manual_run_id,
    )
    return {
        "status": "started",
        "queue": queue.model_dump(),
        "manual_run_id": batch_manual_run_id,
    }


@router.get("/batch/status")
async def get_batch_status():
    """Статус текущей batch-очереди.

    active=True  — очередь работает прямо сейчас.
    active=False — очередь есть, но не запущена (история/прервана/завершена).
    queue=None   — очереди нет вовсе.
    """
    queue = pipeline_manager.get_batch_queue()
    active = bool(queue and queue.status == "running")
    return {"active": active, "queue": queue.model_dump() if queue else None}


@router.delete("/batch/history")
async def clear_batch_history():
    """Удалить историю очереди (прерванные/завершённые).

    Нельзя очистить работающую очередь — вернёт 409.
    """
    try:
        pipeline_manager.clear_queue_history()
        return {"status": "cleared"}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/batch/resume")
async def resume_batch():
    """Продолжить прерванную batch-очередь."""
    try:
        queue = await pipeline_manager.resume_interrupted_batch()
        return {"status": "resumed", "queue": queue.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/batch/add")
async def add_to_batch(request: dict):
    """Добавить проекты в работающую batch-очередь."""
    project_ids = request.get("project_ids", [])
    action = request.get("action")  # None = использовать action очереди

    if not project_ids:
        raise HTTPException(400, "Список проектов пуст")

    valid_ids = []
    for pid in project_ids:
        status = project_service.get_project_status(pid)
        if status and status.has_pdf:
            valid_ids.append(pid)

    if not valid_ids:
        raise HTTPException(400, "Нет валидных проектов для добавления")

    try:
        queue = await pipeline_manager.add_to_batch(valid_ids, action)
        return {"status": "added", "added": len(valid_ids), "queue": queue.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Ошибка добавления в очередь: {e}")


@router.post("/batch/add-retry")
async def add_retry_to_batch(
    request: dict,
    paid_api_allowed: bool = Query(
        False,
        description="True только если пользователь явно поставил галку «Разрешить платные API» в UI. "
                    "По умолчанию False (fail-closed). Auto-retry/orphan = False.",
    ),
):
    """Добавить retry конкретного этапа в очередь (создаёт новую если нет)."""
    project_id = request.get("project_id")
    stage = request.get("stage")
    if not project_id or not stage:
        raise HTTPException(400, "project_id и stage обязательны")

    _check_project(project_id)
    manual_run_id = _issue_manual_run_if_allowed(
        project_id, paid_api_allowed=paid_api_allowed,
    )

    try:
        queue = await pipeline_manager.add_retry_to_batch(
            project_id, stage, manual_run_id=manual_run_id,
        )
        return {"status": "added", "queue": queue.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    except TypeError:
        # Backward-compat: если pipeline_manager.add_retry_to_batch ещё не
        # принимает manual_run_id, вызываем без него. guard всё равно сработает
        # на стадии перед network request.
        try:
            queue = await pipeline_manager.add_retry_to_batch(project_id, stage)
            return {"status": "added", "queue": queue.model_dump()}
        except RuntimeError as e:
            raise HTTPException(409, str(e))


@router.post("/batch/add-resume")
async def add_resume_to_batch(request: dict):
    """Добавить resume (продолжение) проекта в очередь (создаёт новую если нет)."""
    project_id = request.get("project_id")
    if not project_id:
        raise HTTPException(400, "project_id обязателен")

    _check_project(project_id)

    try:
        queue = await pipeline_manager.add_resume_to_batch(project_id)
        return {"status": "added", "queue": queue.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.delete("/batch/cancel")
async def cancel_batch():
    """Отменить текущую batch-очередь."""
    success = await pipeline_manager.cancel_batch()
    if not success:
        raise HTTPException(404, "Нет активной групповой очереди")
    return {"status": "cancelled"}


# ─── Пауза / Возобновление ───

@router.post("/pause")
async def pause_pipeline(request: dict = {}):
    """Поставить на паузу все активные процессы.

    Body: {"mode": "finish_current" | "interrupt"}
    - finish_current: дождаться завершения текущего Claude CLI, не запускать следующий
    - interrupt: прервать текущий процесс немедленно
    """
    mode = request.get("mode", "finish_current")
    if mode not in ("finish_current", "interrupt"):
        raise HTTPException(400, f"Неизвестный режим: {mode}")
    result = await pipeline_manager.pause(mode)
    return result


@router.post("/resume")
async def resume_pipeline():
    """Снять паузу — продолжить работу."""
    result = await pipeline_manager.unpause()
    return result


@router.get("/pause/status")
async def pause_status():
    """Текущий статус паузы."""
    return pipeline_manager.get_pause_status()


@router.post("/batch/reorder")
async def reorder_batch(request: dict):
    """Переупорядочить pending-элементы очереди."""
    new_order = request.get("order", [])
    if not new_order:
        raise HTTPException(400, "Пустой список порядка")
    try:
        queue = await pipeline_manager.reorder_batch(new_order)
        return {"status": "reordered", "queue": queue.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/batch/remove")
async def remove_batch_item(request: dict):
    """Удалить pending-элемент из очереди."""
    project_id = request.get("project_id")
    if not project_id:
        raise HTTPException(400, "project_id не указан")
    try:
        queue = await pipeline_manager.remove_from_batch(project_id)
        return {"status": "removed", "queue": queue.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/batch/update-action")
async def update_batch_item(request: dict):
    """Изменить действие для pending-элемента очереди."""
    project_id = request.get("project_id")
    action = request.get("action")
    if not project_id or not action:
        raise HTTPException(400, "project_id и action обязательны")
    try:
        queue = await pipeline_manager.update_batch_item_action(project_id, action)
        return {"status": "updated", "queue": queue.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.get("/disciplines")
async def get_disciplines():
    """Получить список поддерживаемых дисциплин для UI."""
    from backend.app.services.common.discipline_service import get_supported_disciplines
    return {"disciplines": get_supported_disciplines()}


@router.get("/templates")
async def get_templates(
    discipline: str = Query(None, description="Код дисциплины (EOM, OV)"),
):
    """Получить сырые шаблоны промптов (с плейсхолдерами)."""
    from backend.app.pipeline.stages.prepare.task_builder import get_template_prompts
    templates = get_template_prompts(discipline_code=discipline)
    return {"templates": templates}


@router.put("/templates/{stage}")
async def save_template_endpoint(stage: str, body: dict):
    """Сохранить русский шаблон промпта в .claude/*.md (глобально для всех проектов)."""
    valid_stages = {"text_analysis", "block_analysis", "findings_merge", "optimization"}
    if stage not in valid_stages:
        raise HTTPException(400, f"Неизвестный этап: {stage}")
    content = body.get("content")
    if not content:
        raise HTTPException(400, "Пустой контент")
    from backend.app.pipeline.stages.prepare.task_builder import save_template
    save_template(stage, content)
    return {"status": "saved", "stage": stage}


@router.put("/templates/{stage}/en")
async def save_en_template_endpoint(stage: str, body: dict):
    """Сохранить английскую версию шаблона в .claude/en/*.md."""
    content = body.get("content")
    if not content:
        raise HTTPException(400, "Empty content")
    from backend.app.pipeline.stages.prepare.task_builder import save_en_template
    try:
        save_en_template(stage, content)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "saved", "stage": stage, "lang": "en"}


@router.get("/templates/sync")
async def get_templates_sync():
    """Статус синхронизации русских и английских шаблонов."""
    from backend.app.pipeline.stages.prepare.task_builder import check_template_sync
    sync = check_template_sync()
    out_of_sync = [s for s in sync if s["en_exists"] and not s["synced"]]
    missing_en = [s for s in sync if not s["en_exists"]]
    return {
        "templates": sync,
        "out_of_sync_count": len(out_of_sync),
        "missing_en_count": len(missing_en),
    }


@router.get("/live-status")
async def get_all_live_status():
    """Быстрый polling: live-статус всех запущенных задач + обновлённые batches."""
    # Ленивая очистка зомби-задач при каждом polling
    pipeline_manager.cleanup_zombies()

    running = {}
    for pid, job in pipeline_manager.active_jobs.items():
        running[pid] = {
            "stage": job.stage.value,
            "status": job.status.value,
            "progress_current": job.progress_current,
            "progress_total": job.progress_total,
            "started_at": job.started_at,
            # Heartbeat & ETA
            "last_heartbeat": job.last_heartbeat,
            "batch_started_at": job.batch_started_at,
            "eta_sec": pipeline_manager._calculate_eta(job),
        }

    # Очередённые проекты (pending в _batch_queue) — фронту тоже надо их видеть,
    # иначе после клика "Запустить аудит" дашборд молчит до того, как worker
    # дойдёт до проекта. stage="queued" — отдельный sentinel, фронт рендерит
    # как "В очереди" и не показывает спиннер активного этапа.
    queue = pipeline_manager.get_batch_queue()
    if queue and queue.status == "running":
        for it in queue.items:
            if it.status == "pending" and it.project_id not in running:
                running[it.project_id] = {
                    "stage": "queued",
                    "status": "queued",
                    "progress_current": 0,
                    "progress_total": 0,
                    "started_at": None,
                    "last_heartbeat": None,
                    "batch_started_at": None,
                    "eta_sec": None,
                    "action": it.action,
                    "retry_stage": it.retry_stage,
                }

    # Также отдаём актуальные completed_batches для всех проектов
    batches_info = {}
    for pid, entry in project_service.iter_project_dirs():
        output_dir = entry / "_output"
        batches_file = output_dir / "block_batches.json"
        batch_prefix = "block_batch"
        if not batches_file.exists():
            batches_file = output_dir / "tile_batches.json"
            batch_prefix = "tile_batch"
        if not batches_file.exists():
            continue
        try:
            with open(batches_file, "r", encoding="utf-8") as f:
                bd = json.load(f)
            total = bd.get("total_batches", len(bd.get("batches", [])))
            completed = 0
            for i in range(1, total + 1):
                bf = output_dir / f"{batch_prefix}_{i:03d}.json"
                if bf.exists() and bf.stat().st_size > 100:
                    completed += 1
            batches_info[pid] = {"total": total, "completed": completed}
        except Exception:
            pass

    # Данные о потреблении токенов
    from backend.app.services.common.usage_service import usage_tracker
    try:
        usage = usage_tracker.get_counters().model_dump()
    except Exception:
        usage = None

    pause = pipeline_manager.get_pause_status()

    return {"running": running, "batches": batches_info, "usage": usage, "paused": pause["paused"], "pause_mode": pause.get("mode")}


# ─── Логи проектов ───


def _resolve_log_path_for_version(
    project_id: str,
    version_id: Optional[str],
) -> Path:
    """Версия-aware путь к `audit_log.jsonl`.

    - `version_id=None` → latest version (для нового V2-проекта это V2);
    - `version_id="v1"` → root `_output/audit_log.jsonl`;
    - `version_id="v2"` → `_versions/v2/_output/audit_log.jsonl`;
    - неизвестная версия → 404.

    Без fallback на V1: если V2 audit ещё не писал лог, GET вернёт пустой
    результат, но НЕ покажет V1-лог пользователю (это сбивает с толку и
    создаёт впечатление, что V2 audit что-то делал, хотя нет).
    """
    from backend.app.services.common import version_service
    try:
        output_dir = version_service.resolve_version_output_dir(
            project_id, version_id=version_id,
        )
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    return output_dir / "audit_log.jsonl"


@router.get("/{project_id:path}/log")
async def get_project_log(
    project_id: str,
    limit: int = 500,
    offset: int = 0,
    version_id: Optional[str] = Query(
        None,
        description="Версия лога (v1/v2/...); по умолчанию latest. "
                    "Без fallback на V1: V2 без лога вернёт пустой результат.",
    ),
):
    """Получить персистентный лог аудита из audit_log.jsonl (version-aware)."""
    log_path = _resolve_log_path_for_version(project_id, version_id)
    if not log_path.exists():
        return {"entries": [], "total": 0, "has_more": False}

    entries = []
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        total = len(all_lines)
        # Берём последние `limit` записей (или с offset)
        if offset == 0:
            # По умолчанию — последние N записей
            start = max(0, total - limit)
            selected = all_lines[start:]
        else:
            selected = all_lines[offset:offset + limit]

        for line in selected:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    except OSError:
        return {"entries": [], "total": 0, "has_more": False}

    return {"entries": entries, "total": total, "has_more": total > limit}


@router.delete("/{project_id:path}/log")
async def clear_project_log(
    project_id: str,
    version_id: Optional[str] = Query(
        None,
        description="Версия лога (v1/v2/...); по умолчанию latest. "
                    "DELETE V2 НЕ удаляет V1-лог.",
    ),
):
    """Очистить лог аудита проекта (version-aware)."""
    log_path = _resolve_log_path_for_version(project_id, version_id)
    if log_path.exists():
        log_path.unlink()
    return {"status": "ok"}


# ─── Динамические роуты /{project_id}/... ───

@router.get("/{project_id:path}/prompts")
async def get_prompts(
    project_id: str,
    discipline: str = Query(None, description="Код дисциплины (EOM, OV и т.д.)"),
):
    """Получить все промпты (resolved) для проекта."""
    _check_project(project_id)
    from backend.app.pipeline.stages.prepare.task_builder import get_resolved_prompts
    prompts = get_resolved_prompts(project_id, discipline_override=discipline)
    return {"prompts": prompts}


@router.put("/{project_id:path}/prompts/{stage}")
async def save_prompt(project_id: str, stage: str, body: dict):
    """Сохранить кастомный промпт для этапа."""
    _check_project(project_id)
    valid_stages = {"text_analysis", "block_analysis", "findings_merge", "optimization"}
    if stage not in valid_stages:
        raise HTTPException(400, f"Неизвестный этап: {stage}")
    from backend.app.pipeline.stages.prepare.task_builder import save_prompt_override
    content = body.get("content")
    save_prompt_override(project_id, stage, content)
    return {"status": "saved", "stage": stage}


@router.delete("/{project_id:path}/prompts/{stage}")
async def reset_prompt(project_id: str, stage: str):
    """Сбросить кастомный промпт к стандартному."""
    _check_project(project_id)
    from backend.app.pipeline.stages.prepare.task_builder import save_prompt_override
    save_prompt_override(project_id, stage, None)
    return {"status": "reset", "stage": stage}


@router.post("/{project_id:path}/prepare")
async def prepare_project(
    project_id: str,
    version_id: Optional[str] = Query(None, description="Версия (v1/v2/...), по умолчанию latest"),
):
    """Запустить подготовку проекта (текст + тайлы)."""
    _check_project(project_id, version_id)
    try:
        job = await pipeline_manager.start_prepare(project_id, version_id=version_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id:path}/tile-audit")
async def start_tile_audit(
    project_id: str,
    start_from: int = Query(1, description="Начать с пакета N"),
    version_id: Optional[str] = Query(None),
):
    """Запустить пакетный анализ тайлов."""
    _check_project(project_id, version_id)
    try:
        job = await pipeline_manager.start_tile_audit(
            project_id, start_from=start_from, version_id=version_id,
        )
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id:path}/main-audit")
async def start_main_audit(project_id: str, version_id: Optional[str] = Query(None)):
    """Запустить основной аудит (Claude CLI)."""
    _check_project(project_id, version_id)
    try:
        job = await pipeline_manager.start_main_audit(project_id, version_id=version_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id:path}/smart-audit")
async def start_smart_audit(project_id: str, version_id: Optional[str] = Query(None)):
    """Запустить интеллектуальный аудит (текст → триаж → выборочная нарезка → анализ → Excel)."""
    _check_project(project_id, version_id)
    try:
        job = await pipeline_manager.start_smart_audit(project_id, version_id=version_id)
        return {"status": "started", "mode": "smart", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id:path}/full-audit")
async def start_audit(
    project_id: str,
    version_id: Optional[str] = Query(None),
    paid_api_allowed: bool = Query(
        False,
        description="True если пользователь явно разрешил платные API "
                    "(чекбокс в UI). Без этого Stage 02/discussion блокируются.",
    ),
):
    """Аудит (OCR): кроп блоков → текст → все блоки → свод → нормы."""
    _check_project(project_id, version_id)
    manual_run_id = _issue_manual_run_if_allowed(
        project_id, paid_api_allowed=paid_api_allowed,
    )
    try:
        job = await pipeline_manager.start_audit(
            project_id, version_id=version_id, manual_run_id=manual_run_id,
        )
        return {
            "status": "started",
            "job": job.model_dump(),
            "manual_run_id": manual_run_id,
        }
    except RuntimeError as e:
        raise HTTPException(409, str(e))


# Legacy aliases
@router.post("/{project_id:path}/standard-audit")
async def start_standard_audit(project_id: str, version_id: Optional[str] = Query(None)):
    return await start_audit(project_id, version_id)

@router.post("/{project_id:path}/pro-audit")
async def start_pro_audit(project_id: str, version_id: Optional[str] = Query(None)):
    return await start_audit(project_id, version_id)


@router.get("/{project_id:path}/resume-info")
async def get_resume_info(project_id: str, version_id: Optional[str] = Query(None)):
    """Определить, с какого этапа можно продолжить пайплайн."""
    _check_project(project_id, version_id)
    info = pipeline_manager.detect_resume_stage(project_id, version_id=version_id)
    return info


@router.post("/{project_id:path}/resume")
async def resume_pipeline(
    project_id: str,
    version_id: Optional[str] = Query(None),
    paid_api_allowed: bool = Query(
        False,
        description="True если пользователь явно разрешил платные API при resume. "
                    "AUTO-resume (без участия пользователя) ВСЕГДА передаёт False.",
    ),
):
    """Продолжить пайплайн с места ошибки/остановки."""
    _check_project(project_id, version_id)
    manual_run_id = _issue_manual_run_if_allowed(
        project_id, paid_api_allowed=paid_api_allowed,
    )
    try:
        job = await pipeline_manager.resume_pipeline(
            project_id, version_id=version_id, manual_run_id=manual_run_id,
        )
        return {
            "status": "started",
            "job": job.model_dump(),
            "manual_run_id": manual_run_id,
        }
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id:path}/start-from")
async def start_from_stage(
    project_id: str,
    stage: str = Query(..., description="Этап: prepare, gemma_enrichment, text_analysis, block_analysis, findings_merge, norm_verify, excel"),
    version_id: Optional[str] = Query(None),
    paid_api_allowed: bool = Query(
        False,
        description="См. start_audit. True только если пользователь нажал галку.",
    ),
):
    """Запустить конвейер с указанного этапа (все последующие пересчитываются)."""
    _check_project(project_id, version_id)
    manual_run_id = _issue_manual_run_if_allowed(
        project_id, paid_api_allowed=paid_api_allowed,
    )
    try:
        job = await pipeline_manager.start_from_stage(
            project_id, stage, version_id=version_id,
            manual_run_id=manual_run_id,
        )
        return {
            "status": "started",
            "job": job.model_dump(),
            "manual_run_id": manual_run_id,
        }
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id:path}/verify-norms")
async def start_norm_verification(project_id: str, version_id: Optional[str] = Query(None)):
    """Запустить верификацию нормативных ссылок через WebSearch."""
    _check_project(project_id, version_id)
    try:
        job = await pipeline_manager.start_norm_verify(project_id, version_id=version_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id:path}/prepare-data")
async def prepare_data_endpoint(
    project_id: str,
    force: bool = False,
    parallelism: int | None = None,
    model: str | None = None,
    timeout: int | None = None,
):
    """Запустить «Подготовить данные» = crop PNG + Gemma enrichment.

    Прогресс публикуется в WebSocket (ws/audit/{project_id}) с stage="prepare_data".
    Возвращает immediately с status=started — клиент следит по WS.
    """
    _check_project(project_id)
    from backend.app.pipeline.stages.prepare.prepare_service import start_prepare_data
    result = await start_prepare_data(
        project_id,
        force=force,
        parallelism=parallelism,
        model=model,
        timeout=timeout,
    )
    if result.get("status") == "already_running":
        raise HTTPException(409, "prepare_data уже запущен для этого проекта")
    return result


@router.get("/{project_id:path}/prepare-data/status")
async def prepare_data_status(project_id: str):
    """Текущий статус prepare_data (running + позиция в очереди + последний результат)."""
    from backend.app.pipeline.stages.prepare.prepare_service import get_prepare_status
    return get_prepare_status(project_id)


@router.get("/prepare-data/queue")
async def prepare_data_queue():
    """Глобальная очередь prepare-задач (с per-project прогрессом)."""
    from backend.app.pipeline.stages.prepare.prepare_service import get_global_queue
    return get_global_queue()


@router.post("/prepare-data/queue/clear")
async def prepare_data_queue_clear():
    """Очистить из очереди завершённые/упавшие/пропущенные задачи."""
    from backend.app.pipeline.stages.prepare.prepare_service import clear_completed_from_queue
    removed = clear_completed_from_queue()
    return {"removed": removed}


# prepare-data/queue/{pause,resume,cancel} зарегистрированы вверху файла,
# чтобы /{project_id:path}/resume не перехватывал их


@router.post("/{project_id:path}/crop-blocks-only")
async def crop_blocks_only(project_id: str, force: bool = False):
    """Запустить только кроп графических блоков (без полного аудита).

    Используется для предпросмотра качества блоков перед запуском аудита.
    Не пишет в pipeline_log — это утилитарная операция.
    """
    _check_project(project_id)
    project_dir = resolve_project_dir(project_id)
    from backend.app.core.config import BLOCKS_SCRIPT, BASE_DIR
    args = ["python", str(BLOCKS_SCRIPT), "crop", str(project_dir)]
    if force:
        args.append("--force")

    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(BASE_DIR),
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
    except asyncio.TimeoutError:
        proc.kill()
        raise HTTPException(504, "Таймаут кропа блоков (>5 мин)")

    out = stdout.decode("utf-8", errors="replace") if stdout else ""
    err = stderr.decode("utf-8", errors="replace") if stderr else ""

    if proc.returncode != 0:
        raise HTTPException(500, f"blocks.py crop вернул код {proc.returncode}: {err[-500:] or out[-500:]}")

    # Парсим итоговую строку JSON-вывода
    summary = {"total_blocks": 0, "cropped": 0, "skipped": 0, "errors": 0}
    for line in reversed(out.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                summary = json.loads(line)
                break
            except json.JSONDecodeError:
                pass

    return {"status": "ok", "project_id": project_id, "summary": summary}


@router.get("/{project_id:path}/status")
async def get_audit_status(project_id: str):
    """Получить текущий статус аудита."""
    job = pipeline_manager.get_job(project_id)
    status = project_service.get_project_status(project_id)
    return {
        "project_id": project_id,
        "is_running": pipeline_manager.is_running(project_id),
        "current_job": job.model_dump() if job else None,
        "pipeline": status.pipeline.model_dump() if status else None,
    }


@router.post("/{project_id:path}/retry/{stage}")
async def retry_stage(
    project_id: str,
    stage: str,
    paid_api_allowed: bool = Query(
        False,
        description="True только при ручном retry с галкой. Auto-retry/orphan = False.",
    ),
):
    """Повторить конкретный этап конвейера."""
    _check_project(project_id)
    manual_run_id = _issue_manual_run_if_allowed(
        project_id, paid_api_allowed=paid_api_allowed,
    )

    # Имя метода → этап для start_from_stage (или специальный starter).
    stage_to_pipeline_stage = {
        "crop_blocks": "prepare",
        "gemma_enrichment": "gemma_enrichment",
        "text_analysis": "text_analysis",
        "block_analysis": "block_analysis",
        "findings_merge": "findings_merge",
        "findings_critic": "findings_review",
        "findings_review": "findings_review",
        "findings_corrector": "findings_review",
        "prepare": "prepare",
        "tile_audit": "block_analysis",
        "main_audit": "findings_merge",
    }
    special_starters = {
        "norm_verify",
        "norm_requote",
        "optimization",
        "optimization_critic",
        "optimization_corrector",
    }

    if stage in stage_to_pipeline_stage:
        async def _starter():
            return await pipeline_manager.start_from_stage(
                project_id, stage_to_pipeline_stage[stage],
                manual_run_id=manual_run_id,
            )
    elif stage == "norm_verify" or stage == "norm_requote":
        async def _starter():
            return await pipeline_manager.start_norm_verify(project_id)
    elif stage == "optimization":
        async def _starter():
            return await pipeline_manager.start_optimization(project_id)
    elif stage == "optimization_critic" or stage == "optimization_corrector":
        async def _starter():
            return await pipeline_manager.start_optimization_review(project_id)
    else:
        raise HTTPException(400, f"Этап '{stage}' не поддерживает повтор")

    try:
        job = await _starter()
        # У старых start_* методов нет manual_run_id-параметра — там
        # paid_api_guard заблокирует на уровне run_llm. Сообщаем UI scope:
        return {
            "status": "started",
            "stage": stage,
            "job": job.model_dump(),
            "manual_run_id": manual_run_id,
        }
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id:path}/skip/{stage}")
async def skip_stage(project_id: str, stage: str):
    """Пропустить ошибочный этап (пометить как skipped)."""
    _check_project(project_id)

    valid_stages = {"crop_blocks", "text_analysis", "block_analysis", "findings_merge", "norm_verify", "excel",
                     "tile_audit", "main_audit", "prepare"}  # + legacy aliases
    if stage not in valid_stages:
        raise HTTPException(400, f"Этап '{stage}' нельзя пропустить")

    pipeline_manager._update_pipeline_log(
        project_id, stage, "skipped", message="Пропущен пользователем"
    )
    return {"status": "skipped", "stage": stage}


@router.delete("/{project_id:path}/cancel")
async def cancel_audit(project_id: str):
    """Отменить запущенный аудит."""
    success = await pipeline_manager.cancel(project_id)
    if not success:
        raise HTTPException(404, f"Нет запущенного аудита для '{project_id}'")
    return {"status": "cancelled"}


def _check_project(project_id: str, version_id: Optional[str] = None):
    """Проверка существования проекта (и опционально — валидной версии).

    Запуск аудита требует наличия PDF в папке выбранной версии:
    - V1 / legacy: PDF в корне проекта (как и раньше);
    - V2+: PDF/MD в `_versions/v{N}/`; если их нет — `409`, чтобы UI мог
      подсказать пользователю сначала загрузить файлы через
      `POST /api/projects/{id}/versions/{vid}/files`.

    fallback на PDF из V1 запрещён.
    """
    from backend.app.services.common import version_service
    status = project_service.get_project_status(project_id, version_id=version_id)
    if not status:
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    # Валидируем version_id явно
    if version_id:
        proj_dir = project_service.resolve_project_dir(project_id)
        try:
            version_service.get_version_entry(proj_dir, project_id, version_id)
        except version_service.VersionNotFoundError as e:
            raise HTTPException(404, str(e))

    # Проверяем именно ту версию, которую планируется запускать.
    effective_vid = version_id or status.version_id
    readiness = version_service.version_audit_readiness(project_id, effective_vid)
    if not readiness["can_run_audit"]:
        # Для V1 формулировка такая же, как была (совместимость UI).
        if effective_vid in (None, "v1"):
            raise HTTPException(
                400, f"В проекте '{project_id}' отсутствует PDF файл"
            )
        raise HTTPException(
            409,
            f"В версии '{effective_vid}' проекта '{project_id}' нет исходных "
            f"PDF/MD файлов. Загрузите их через POST /api/projects/{{id}}/"
            f"versions/{effective_vid}/files перед запуском аудита."
        )
