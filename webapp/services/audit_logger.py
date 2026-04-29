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

# Канонический порядок этапов конвейера — дубликат _PIPELINE_STAGE_ORDER,
# чтобы не создавать цикл импорта project_service ↔ audit_logger.
_PIPELINE_STAGE_ORDER_KEYS = [
    "crop_blocks",
    "text_analysis",
    "block_analysis",
    "block_retry",
    "findings_merge",
    "findings_critic",
    "findings_corrector",
    "norm_verify",
    "optimization",
    "optimization_critic",
    "optimization_corrector",
    "excel",
]
_TERMINAL_STATUSES = {"done", "skipped", "error", "interrupted"}

# Этапы, которые выполняются параллельно с findings_critic/findings_corrector
# и не должны сбрасываться при их перезапуске.
_PARALLEL_TO_FINDINGS_REVIEW = {
    "norm_verify", "optimization", "optimization_critic", "optimization_corrector",
}


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
        # completed_at от прошлого прогона нельзя оставлять: с ним UI считает, что
        # этап «свежесделан», пока фактически только что запустился.
        stage_info.pop("completed_at", None)
        stage_info.pop("error", None)
        stage_info.pop("detail", None)
        stage_info.pop("interrupted_at", None)
        # Новый запуск этапа не должен наследовать usage от прошлого прогона.
        stage_info.pop("input_tokens", None)
        stage_info.pop("output_tokens", None)
        stage_info.pop("model", None)
        # Cascade: этапы ниже по конвейеру, завершённые в прошлом прогоне,
        # больше не валидны. Удаляем только терминальные (done/error/skipped/
        # interrupted) — running/pending не трогаем, чтобы не мешать параллельным
        # этапам, которые стартуют одновременно.
        if stage_key in _PIPELINE_STAGE_ORDER_KEYS:
            idx = _PIPELINE_STAGE_ORDER_KEYS.index(stage_key)
            for downstream in _PIPELINE_STAGE_ORDER_KEYS[idx + 1:]:
                # Параллельные этапы (norm_verify, optimization и их critic/corrector)
                # выполняются одновременно с findings_critic/corrector, поэтому их
                # статус не сбрасывается при перезапуске findings-review.
                if (downstream in _PARALLEL_TO_FINDINGS_REVIEW
                        and stage_key in ("findings_critic", "findings_corrector")):
                    continue
                ds_info = log_data["stages"].get(downstream)
                if ds_info and ds_info.get("status") in _TERMINAL_STATUSES:
                    log_data["stages"].pop(downstream, None)
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


def reset_audit_log(project_id: str) -> None:
    """Архивировать audit_log.jsonl при старте свежего прогона.

    Старый файл переименовывается в audit_log_<timestamp>.jsonl (timestamp =
    время первой записи прошлого прогона, чтобы имя отражало когда он начат).
    Вызывается из start_audit / start_smart_audit / start_flash_pro_triage
    и из batch-loop для fresh-start экшнов (full/audit/standard/pro).
    Resume / retry / optimization / prepare-data не архивируют — продолжают
    писать в тот же файл (это «дозапуски» текущего прогона).
    """
    try:
        log_path = resolve_project_dir(project_id) / "_output" / "audit_log.jsonl"
        if not log_path.exists():
            return
        ts = _read_first_timestamp(log_path) or datetime.fromtimestamp(
            log_path.stat().st_mtime
        ).isoformat()
        # Безопасно для FS: убрать двоеточия и точки
        slug = ts.replace(":", "-").replace(".", "-")
        archive = log_path.with_name(f"audit_log_{slug}.jsonl")
        # На случай коллизии (двойной reset в одну секунду) добавим суффикс
        n = 1
        while archive.exists():
            archive = log_path.with_name(f"audit_log_{slug}_{n}.jsonl")
            n += 1
        log_path.rename(archive)
    except OSError:
        pass


def _read_first_timestamp(path: Path) -> str | None:
    """Достать timestamp первой валидной записи jsonl. None если файл пустой/битый."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = obj.get("timestamp")
                if isinstance(ts, str) and ts:
                    return ts
                return None
    except OSError:
        return None
    return None


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
