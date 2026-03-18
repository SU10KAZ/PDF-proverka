"""
REST API для запуска и управления аудитом.
"""
import asyncio
import json
import re
import subprocess
import traceback
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query
from webapp.services.pipeline_service import pipeline_manager
from webapp.services import project_service
from webapp.services.project_service import resolve_project_dir
from webapp.config import (
    CLAUDE_CLI,
    get_claude_model, set_claude_model, CLAUDE_MODEL_OPTIONS,
    get_stage_models, set_stage_model, get_model_for_stage,
)

router = APIRouter(prefix="/api/audit", tags=["audit"])


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
    """Текущая модель Claude CLI."""
    return {"model": get_claude_model(), "options": CLAUDE_MODEL_OPTIONS}


@router.post("/model")
async def switch_model(model: str = Query(..., description="ID модели")):
    """Переключить модель Claude CLI."""
    if model not in CLAUDE_MODEL_OPTIONS:
        raise HTTPException(400, f"Неизвестная модель. Доступны: {CLAUDE_MODEL_OPTIONS}")
    set_claude_model(model)
    return {"model": get_claude_model()}


@router.get("/model/stages")
async def get_stage_model_config():
    """Настройки per-stage моделей (гибридный режим)."""
    stages = get_stage_models()
    default = get_claude_model()
    return {
        "default_model": default,
        "stages": {k: (v or default) for k, v in stages.items()},
        "options": CLAUDE_MODEL_OPTIONS,
    }


@router.post("/model/stages")
async def set_stage_model_config(
    stage: str = Query(..., description="Этап: text_analysis, block_batch, findings_merge, norm_verify, norm_fix, optimization"),
    model: str = Query(..., description="Модель или 'default'"),
):
    """Установить модель для конкретного этапа ('default' = использовать общую)."""
    if model == "default":
        set_stage_model(stage, None)
    elif model not in CLAUDE_MODEL_OPTIONS:
        raise HTTPException(400, f"Неизвестная модель. Доступны: {CLAUDE_MODEL_OPTIONS}")
    else:
        set_stage_model(stage, model)
    return {"stage": stage, "model": get_model_for_stage(stage)}


@router.get("/account")
async def get_claude_account():
    """Получить информацию о текущем аккаунте Claude CLI."""
    try:
        result = subprocess.run(
            [CLAUDE_CLI, "auth", "status", "--json"],
            capture_output=True, text=True, timeout=10,
            encoding="utf-8", errors="replace",
            shell=CLAUDE_CLI.endswith(".cmd"),
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
    shell = CLAUDE_CLI.endswith(".cmd")

    # 1. Logout
    subprocess.run(
        [CLAUDE_CLI, "auth", "logout"],
        capture_output=True, text=True, timeout=10,
        encoding="utf-8", errors="replace", shell=shell,
    )

    # 2. Запустить login в фоне
    import platform
    kwargs = {}
    if platform.system() == "Windows":
        kwargs["creationflags"] = 0x08000000  # CREATE_NO_WINDOW

    proc = subprocess.Popen(
        [CLAUDE_CLI, "auth", "login"],
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
            result = subprocess.run(
                [CLAUDE_CLI, "auth", "status", "--json"],
                capture_output=True, text=True, timeout=10,
                encoding="utf-8", errors="replace",
                shell=CLAUDE_CLI.endswith(".cmd"),
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
    """Запустить групповое действие для выбранных проектов."""
    from webapp.models.audit import BatchRequest
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

    queue = await pipeline_manager.start_batch(valid_ids, req.action.value)
    return {"status": "started", "queue": queue.model_dump()}


@router.get("/batch/status")
async def get_batch_status():
    """Статус текущей batch-очереди."""
    queue = pipeline_manager.get_batch_queue()
    if not queue:
        return {"active": False}
    return {"active": True, "queue": queue.model_dump()}


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


@router.post("/batch/add-retry")
async def add_retry_to_batch(request: dict):
    """Добавить retry конкретного этапа в очередь (создаёт новую если нет)."""
    project_id = request.get("project_id")
    stage = request.get("stage")
    if not project_id or not stage:
        raise HTTPException(400, "project_id и stage обязательны")

    _check_project(project_id)

    try:
        queue = await pipeline_manager.add_retry_to_batch(project_id, stage)
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
    from webapp.services.discipline_service import get_supported_disciplines
    return {"disciplines": get_supported_disciplines()}


@router.get("/templates")
async def get_templates(
    discipline: str = Query(None, description="Код дисциплины (EM, OV)"),
):
    """Получить сырые шаблоны промптов (с плейсхолдерами)."""
    from webapp.services.task_builder import get_template_prompts
    templates = get_template_prompts(discipline_code=discipline)
    return {"templates": templates}


@router.put("/templates/{stage}")
async def save_template_endpoint(stage: str, body: dict):
    """Сохранить шаблон промпта в .claude/*.md (глобально для всех проектов)."""
    valid_stages = {"text_analysis", "block_analysis", "findings_merge", "optimization"}
    if stage not in valid_stages:
        raise HTTPException(400, f"Неизвестный этап: {stage}")
    content = body.get("content")
    if not content:
        raise HTTPException(400, "Пустой контент")
    from webapp.services.task_builder import save_template
    save_template(stage, content)
    return {"status": "saved", "stage": stage}


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
    from webapp.services.usage_service import usage_tracker
    try:
        usage = usage_tracker.get_counters().model_dump()
    except Exception:
        usage = None

    pause = pipeline_manager.get_pause_status()

    return {"running": running, "batches": batches_info, "usage": usage, "paused": pause["paused"], "pause_mode": pause.get("mode")}


# ─── Логи проектов ───

@router.get("/{project_id}/log")
async def get_project_log(project_id: str, limit: int = 500, offset: int = 0):
    """Получить персистентный лог аудита из audit_log.jsonl."""
    log_path = resolve_project_dir(project_id) / "_output" / "audit_log.jsonl"
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


@router.delete("/{project_id}/log")
async def clear_project_log(project_id: str):
    """Очистить лог аудита проекта."""
    log_path = resolve_project_dir(project_id) / "_output" / "audit_log.jsonl"
    if log_path.exists():
        log_path.unlink()
    return {"status": "ok"}


# ─── Динамические роуты /{project_id}/... ───

@router.get("/{project_id}/prompts")
async def get_prompts(
    project_id: str,
    discipline: str = Query(None, description="Код дисциплины (EM, OV и т.д.)"),
):
    """Получить все промпты (resolved) для проекта."""
    _check_project(project_id)
    from webapp.services.task_builder import get_resolved_prompts
    prompts = get_resolved_prompts(project_id, discipline_override=discipline)
    return {"prompts": prompts}


@router.put("/{project_id}/prompts/{stage}")
async def save_prompt(project_id: str, stage: str, body: dict):
    """Сохранить кастомный промпт для этапа."""
    _check_project(project_id)
    valid_stages = {"text_analysis", "block_analysis", "findings_merge", "optimization"}
    if stage not in valid_stages:
        raise HTTPException(400, f"Неизвестный этап: {stage}")
    from webapp.services.task_builder import save_prompt_override
    content = body.get("content")
    save_prompt_override(project_id, stage, content)
    return {"status": "saved", "stage": stage}


@router.delete("/{project_id}/prompts/{stage}")
async def reset_prompt(project_id: str, stage: str):
    """Сбросить кастомный промпт к стандартному."""
    _check_project(project_id)
    from webapp.services.task_builder import save_prompt_override
    save_prompt_override(project_id, stage, None)
    return {"status": "reset", "stage": stage}


@router.post("/{project_id}/prepare")
async def prepare_project(project_id: str):
    """Запустить подготовку проекта (текст + тайлы)."""
    _check_project(project_id)
    try:
        job = await pipeline_manager.start_prepare(project_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id}/tile-audit")
async def start_tile_audit(
    project_id: str,
    start_from: int = Query(1, description="Начать с пакета N"),
):
    """Запустить пакетный анализ тайлов."""
    _check_project(project_id)
    try:
        job = await pipeline_manager.start_tile_audit(project_id, start_from=start_from)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id}/main-audit")
async def start_main_audit(project_id: str):
    """Запустить основной аудит (Claude CLI)."""
    _check_project(project_id)
    try:
        job = await pipeline_manager.start_main_audit(project_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id}/smart-audit")
async def start_smart_audit(project_id: str):
    """Запустить интеллектуальный аудит (текст → триаж → выборочная нарезка → анализ → Excel)."""
    _check_project(project_id)
    try:
        job = await pipeline_manager.start_smart_audit(project_id)
        return {"status": "started", "mode": "smart", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id}/full-audit")
async def start_audit(project_id: str):
    """Аудит (OCR): кроп блоков → текст → все блоки → свод → нормы."""
    _check_project(project_id)
    try:
        job = await pipeline_manager.start_audit(project_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


# Legacy aliases
@router.post("/{project_id}/standard-audit")
async def start_standard_audit(project_id: str):
    return await start_audit(project_id)

@router.post("/{project_id}/pro-audit")
async def start_pro_audit(project_id: str):
    return await start_audit(project_id)


@router.get("/{project_id}/resume-info")
async def get_resume_info(project_id: str):
    """Определить, с какого этапа можно продолжить пайплайн."""
    _check_project(project_id)
    info = pipeline_manager.detect_resume_stage(project_id)
    return info


@router.post("/{project_id}/resume")
async def resume_pipeline(project_id: str):
    """Продолжить пайплайн с места ошибки/остановки."""
    _check_project(project_id)
    try:
        job = await pipeline_manager.resume_pipeline(project_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id}/start-from")
async def start_from_stage(project_id: str, stage: str = Query(..., description="Этап: prepare, text_analysis, block_analysis, findings_merge, norm_verify, excel")):
    """Запустить конвейер с указанного этапа (все последующие пересчитываются)."""
    _check_project(project_id)
    try:
        job = await pipeline_manager.start_from_stage(project_id, stage)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id}/verify-norms")
async def start_norm_verification(project_id: str):
    """Запустить верификацию нормативных ссылок через WebSearch."""
    _check_project(project_id)
    try:
        job = await pipeline_manager.start_norm_verify(project_id)
        return {"status": "started", "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.get("/{project_id}/status")
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


@router.post("/{project_id}/retry/{stage}")
async def retry_stage(project_id: str, stage: str):
    """Повторить конкретный этап конвейера."""
    _check_project(project_id)

    stage_methods = {
        "crop_blocks": lambda: pipeline_manager.start_from_stage(project_id, "prepare"),
        "text_analysis": lambda: pipeline_manager.start_from_stage(project_id, "text_analysis"),
        "block_analysis": lambda: pipeline_manager.start_from_stage(project_id, "block_analysis"),
        "findings_merge": lambda: pipeline_manager.start_from_stage(project_id, "findings_merge"),
        "findings_critic": lambda: pipeline_manager.start_from_stage(project_id, "findings_review"),
        "findings_review": lambda: pipeline_manager.start_from_stage(project_id, "findings_review"),
        "findings_corrector": lambda: pipeline_manager.start_from_stage(project_id, "findings_review"),
        "norm_verify": lambda: pipeline_manager.start_norm_verify(project_id),
        "optimization": lambda: pipeline_manager.start_optimization(project_id),
        "optimization_critic": lambda: pipeline_manager.start_optimization_review(project_id),
        "optimization_corrector": lambda: pipeline_manager.start_optimization_review(project_id),
        # Legacy aliases
        "prepare": lambda: pipeline_manager.start_from_stage(project_id, "prepare"),
        "tile_audit": lambda: pipeline_manager.start_from_stage(project_id, "block_analysis"),
        "main_audit": lambda: pipeline_manager.start_from_stage(project_id, "findings_merge"),
    }

    starter = stage_methods.get(stage)
    if not starter:
        raise HTTPException(400, f"Этап '{stage}' не поддерживает повтор")

    try:
        job = await starter()
        return {"status": "started", "stage": stage, "job": job.model_dump()}
    except RuntimeError as e:
        raise HTTPException(409, str(e))


@router.post("/{project_id}/skip/{stage}")
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


@router.delete("/{project_id}/cancel")
async def cancel_audit(project_id: str):
    """Отменить запущенный аудит."""
    success = await pipeline_manager.cancel(project_id)
    if not success:
        raise HTTPException(404, f"Нет запущенного аудита для '{project_id}'")
    return {"status": "cancelled"}


def _check_project(project_id: str):
    """Проверка существования проекта."""
    status = project_service.get_project_status(project_id)
    if not status:
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    if not status.has_pdf:
        raise HTTPException(400, f"В проекте '{project_id}' отсутствует PDF файл")
