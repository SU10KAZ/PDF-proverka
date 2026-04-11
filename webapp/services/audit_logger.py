"""
Логирование аудита.
Персистентные логи (pipeline_log.json, audit_log.jsonl) и WebSocket broadcast.
"""
import asyncio
import json
from datetime import datetime
from pathlib import Path

from webapp.services.project_service import resolve_project_dir
from webapp.models.audit import AuditJob
from webapp.models.websocket import WSMessage
from webapp.ws.manager import ws_manager


def update_pipeline_log(
    project_id: str,
    stage_key: str,
    status: str,
    message: str = "",
    error: str = "",
    detail: dict | None = None,
):
    """Записать статус этапа в pipeline_log.json и отправить WS-обновление."""
    output_dir = resolve_project_dir(project_id) / "_output"
    output_dir.mkdir(exist_ok=True)

    log_path = output_dir / "pipeline_log.json"
    if log_path.exists():
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                log_data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            log_data = {"version": 1, "stages": {}}
    else:
        log_data = {"version": 1, "stages": {}}

    now = datetime.now().isoformat()
    log_data["last_updated"] = now

    stage_info = log_data["stages"].get(stage_key, {})
    stage_info["status"] = status

    if status == "running":
        stage_info["started_at"] = now
        stage_info.pop("error", None)
        stage_info.pop("detail", None)
        stage_info.pop("interrupted_at", None)
    elif status in ("done", "skipped"):
        stage_info["completed_at"] = now
        # Очистить ложные ошибки от recovery (если этап успешно завершился)
        if not error:
            stage_info.pop("error", None)
            stage_info.pop("interrupted_at", None)
    elif status == "error":
        stage_info["completed_at"] = now

    if message:
        stage_info["message"] = message
    if error:
        stage_info["error"] = error
    if detail:
        stage_info["detail"] = detail

    log_data["stages"][stage_key] = stage_info

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)

    # WS-broadcast для реактивного обновления UI
    try:
        from webapp.services.project_service import _get_pipeline_status
        pipeline = _get_pipeline_status(output_dir)
        asyncio.ensure_future(
            ws_manager.broadcast_to_project(
                project_id,
                WSMessage.status_change(project_id, pipeline.model_dump()),
            )
        )
    except Exception:
        pass  # WS broadcast не должен ломать основной процесс


def persist_log(project_id: str, message: str, level: str, stage: str,
                extras: dict | None = None):
    """Сохранить запись лога в audit_log.jsonl проекта.

    extras: опциональные доп. поля (kind, result_md, duration_sec и т.п.) —
    используются для структурированных записей типа cli_summary, которые
    нужно восстанавливать после refresh браузера.
    """
    try:
        output_dir = resolve_project_dir(project_id) / "_output"
        output_dir.mkdir(parents=True, exist_ok=True)
        log_path = output_dir / "audit_log.jsonl"
        entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "stage": stage,
            "message": message,
        }
        if extras:
            entry.update(extras)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass  # Не ломаем основной процесс


async def log_to_project(job: AuditJob, message: str, level: str = "info"):
    """Записать лог в консоль, файл и WebSocket."""
    tag = f"[{job.project_id}:{job.stage.value}]"
    if level in ("error", "warn"):
        print(f"{tag} [{level.upper()}] {message}")
    persist_log(job.project_id, message, level, job.stage.value)
    await ws_manager.broadcast_to_project(
        job.project_id,
        WSMessage.log(job.project_id, message, level, job.stage.value),
    )


async def send_progress(job: AuditJob, current: int, total: int):
    """Отправить обновление прогресса по WebSocket."""
    job.progress_current = current
    job.progress_total = total
    await ws_manager.broadcast_to_project(
        job.project_id,
        WSMessage.progress(job.project_id, current, total, job.stage.value),
    )
