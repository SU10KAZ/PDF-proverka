"""
Pipeline Manager — оркестрация конвейера аудита.
Запуск, отмена, отслеживание прогресса.
"""
import asyncio
import json
import os
import random
import shutil
import time
from uuid import uuid4
from datetime import datetime
from pathlib import Path
from typing import Optional

from backend.app.core.config import (
    BLOCK_BATCH_MODE_FINDINGS_ONLY,
    BASE_DIR, PROJECTS_DIR,
    PROCESS_PROJECT_SCRIPT, GENERATE_EXCEL_SCRIPT,
    BLOCKS_SCRIPT, NORMS_SCRIPT,
    MAX_PARALLEL_BATCHES,
    get_block_batch_parallelism,
    get_stage_model,
    get_stage_batch_mode,
    is_local_llm_model,
    RATE_LIMIT_THRESHOLD_PCT, RATE_LIMIT_CHECK_INTERVAL,
    RATE_LIMIT_MAX_WAIT, RATE_LIMIT_MAX_RETRIES,
    validate_current_stage_model_config,
    BATCH_QUEUE_FILE,
)
from backend.app.models.audit import AuditJob, AuditStage, JobStatus, BatchQueueStatus, BatchQueueItem, BatchAction
from backend.app.models.websocket import WSMessage
from backend.app.core.config import get_claude_model, get_model_for_stage
from backend.app.models.usage import UsageRecord
from backend.app.services.common.process_runner import (
    run_script,
    kill_all_processes,
    has_live_processes,
    active_process_pids,
)
import backend.app.services.llm.claude_runner as claude_runner
from backend.app.services.common.usage_service import usage_tracker, global_scanner, paid_cost_tracker
from backend.app.pipeline.resume_detector import detect_resume_stage as _detect_resume_stage
import backend.app.services.common.audit_logger as audit_logger
from backend.app.services.common.log_humanizer import humanize_log_line, split_known_prefix
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_ALL_NAMES,
    BLOCKS_ANALYSIS_FILENAME,
    BLOCKS_FOR_TEXT_ALL_NAMES,
    BLOCKS_FOR_TEXT_FILENAME,
    BLOCK_CONTEXT_SUMMARY_ALL_NAMES,
    TEXT_ANALYSIS_ALL_NAMES,
    TEXT_ANALYSIS_FILENAME,
    resolve_existing,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import (
    GEMMA_STAGE_LABEL,
    evaluate_gemma_enrichment,
    find_project_markdown,
    load_project_info,
    gemma_gate_error,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BLOCKS_DIRNAME,
    STAGE02_BLOCKS_DIRNAME,
    crop_index_matches_policy,
    gemma_blocks_dir,
    gemma_blocks_index_path,
    gemma_enrichment_crop_policy,
    stage02_blocks_dir,
    stage02_blocks_index_path,
    stage02_crop_policy,
    gemma_output_root,
)

# ── Stage runner imports (extracted pure helpers) ──────────────────────────
from backend.app.pipeline.stages.crop_blocks.runner import (
    build_crop_args as _build_crop_args,
    existing_crop_matches_policy as _existing_crop_matches_policy,
    crop_policy_label as _crop_policy_label,
    run_crop_blocks as _run_crop_blocks,
    run_policy_recrop as _run_policy_recrop,
    sync_v2_read_canary_blocks_alias as _sync_v2_read_canary_blocks_alias,
)

from backend.app.pipeline.stages.block_analysis.runner import (
    RUNTIME_BATCHES_FILE,
    expand_block_batches_for_single_block_mode as _expand_block_batches_for_local_model,
    build_single_block_runtime_plan as _build_single_block_runtime_plan,
    write_single_block_runtime_plan as _write_single_block_runtime_plan,
    load_or_create_single_block_runtime_plan as _load_or_create_single_block_runtime_plan,
    runtime_batch_failure_entry as _runtime_batch_failure_entry,
    write_block_analysis_runtime_summary as _write_block_analysis_runtime_summary,
)
from backend.app.pipeline.stages.findings_merge.runner import (
    run_findings_merge as _run_findings_merge_stage,
)
from backend.app.pipeline.stages.norms.runner import (
    run_norm_verification as _run_norm_verification_stage,
)
from backend.app.pipeline.stages.findings_verify.runner import (
    run_findings_verify as _run_findings_verify_stage,
)
from backend.app.pipeline.stages.critic_v2_triage import (
    run_critic_v2_triage as _run_critic_v2_triage_stage,
)

from backend.app.pipeline.stages.block_analysis.runner import (
    run_block_analysis_findings_only as _run_block_analysis_findings_only_stage,
)
from backend.app.pipeline.stages.text_analysis.runner import (
    run_text_analysis as _run_text_analysis_stage,
)
from backend.app.pipeline.stages.block_context.runner import (
    run_block_context_stage as _run_block_context_stage_fn,
)
# ──────────────────────────────────────────────────────────────────────────


def _has_ocr_result_json(project_dir: Path) -> bool:
    try:
        from backend.app.services.storage.projects_v2_source_resolver import (
            load_version_project_info,
            resolve_version_source_files,
        )

        info = load_version_project_info(project_dir)
        document_code = info.get("document_code") or info.get("project_id") or Path(project_dir).name
        sources = resolve_version_source_files(project_dir, document_code, project_info=info)
        if sources.layout == "projects_v2":
            if sources.result_json_paths:
                return True
            # Новый комплект портала: result.json синтезируется из blocks.json.
            # Ленивое самолечение версий, загруженных до подключения синтеза
            # в их контур приёма (идемпотентно, fail-soft); кропы — фоном.
            from backend.app.services.common.blocks_json import ensure_result_json_for_version
            if ensure_result_json_for_version(project_dir):
                try:
                    from backend.app.services.common.crop_cache import ensure_crops_for_version
                    ensure_crops_for_version(project_dir)
                except Exception:
                    pass
                return True
            return False
    except Exception:
        pass
    return bool(list(Path(project_dir).glob("*_result.json")))


def _project_path(pid: str, version_id: Optional[str] = None) -> str:
    """Относительный путь к папке проекта c учётом версии.

    - `version_id` пустой/"v1" → root project_dir (legacy V1 поведение).
    - "v2", "v3" … → `<root>/_versions/v{N}/`.
    - неизвестная версия → fallback root (legacy), чтобы случайный
      mismatch не падал stack trace'ом из subprocess argv-builder'ов.

    ВАЖНО: long-running job-ы должны передавать `job.version_id` явно либо
    использовать `PipelineManager._project_path_for_job(job)`. Не полагаться
    на дефолт «latest version» внутри версионо-зависимых stages.
    """
    resolved = resolve_project_dir(pid)
    if version_id:
        try:
            from backend.app.services.common import version_service
            resolved = version_service.get_version_dir(resolved, pid, version_id)
        except Exception:
            # VersionNotFoundError / любая ошибка → legacy fallback на root.
            pass
    try:
        return str(resolved.relative_to(BASE_DIR))
    except ValueError:
        return str(resolved)


def _extract_error_detail(exit_code: int, output: str, max_len: int = 120) -> str:
    """Извлечь полезное сообщение об ошибке из CLI output.

    Ищет последние значимые строки stderr/stdout, убирает мусор.
    Возвращает строку до max_len символов.
    """
    if not output:
        return f"Exit code {exit_code}"

    lines = output.strip().splitlines()
    # Фильтруем пустые и мусорные строки
    useful = []
    skip_prefixes = ("╭", "╰", "│", "─", "⎿", "⏎", "\\", "  ", "Usage:", "Duration:")
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if any(stripped.startswith(p) for p in skip_prefixes):
            continue
        # Ищем строки с реальным содержанием ошибки
        lower = stripped.lower()
        if any(kw in lower for kw in ("error", "ошибка", "failed", "timeout", "timed out",
                                       "rate limit", "overloaded", "connection", "refused",
                                       "exception", "traceback", "permission", "not found",
                                       "invalid", "json", "unable", "cannot")):
            useful.insert(0, stripped)
            if len(useful) >= 3:
                break
        elif not useful:
            # Берём последнюю непустую строку как fallback
            useful.append(stripped)

    if useful:
        msg = " | ".join(useful)
        if len(msg) > max_len:
            msg = msg[:max_len - 3] + "..."
        return msg
    return f"Exit code {exit_code}"


# BATCH_QUEUE_FILE imported from backend.app.core.config
# RUNTIME_BATCHES_FILE imported from backend.app.pipeline.stages.block_analysis.runner
# _build_crop_args, _existing_crop_matches_policy, _crop_policy_label
#   imported from backend.app.pipeline.stages.crop_blocks.runner
# _expand_block_batches_for_local_model, _build_single_block_runtime_plan,
# _write_single_block_runtime_plan, _load_or_create_single_block_runtime_plan,
# _runtime_batch_failure_entry, _write_block_analysis_runtime_summary
#   imported from backend.app.pipeline.stages.block_analysis.runner

from backend.app.services.common.project_service import resolve_project_dir, bind_object, unbind_object
from backend.app.ws.manager import ws_manager


def _current_object_id_or_none() -> Optional[str]:
    """Helper: ID текущего объекта (None, если objects.json недоступен)."""
    try:
        from backend.app.services.common.object_service import get_current_id
        return get_current_id()
    except Exception:
        return None


class BatchResumeBlockedError(RuntimeError):
    """Resume очереди временно недоступен — текущий проект ещё выполняется.

    Отличается от обычного RuntimeError, чтобы API мог вернуть структурированный
    409 с reason=current_project_running вместо общего сообщения.
    """


# Сколько проектов вперёд (после текущего running) готовит pre-crop. Раньше
# pre-crop кропил ВСЮ очередь без ограничения и racing'ом с основным pipeline
# жёг CPU/IO. Окно делает опережение предсказуемым и скользит вместе с
# current_index.
BATCH_PRECROP_WINDOW = max(1, int(os.environ.get("BATCH_PRECROP_WINDOW", "6")))

# Порог числа реальных ошибок, после которого Stage 02 прекращает запускать
# новые блоки. На production single-block пути остаток скипнутых блоков теперь
# помечается failed (reserc.md #1) — раньше был тихий return и блоки исчезали
# из coverage. Конфигурируемо через env (раньше было захардкожено «5»).
STAGE02_ERROR_ABORT_THRESHOLD = max(
    1, int(os.environ.get("STAGE02_ERROR_ABORT_THRESHOLD", "5") or "5")
)


class PipelineManager:
    """Управляет запущенными аудитами. Singleton."""

    def __init__(self):
        self.active_jobs: dict[str, AuditJob] = {}      # project_id -> job
        self._tasks: dict[str, asyncio.Task] = {}        # project_id -> asyncio.Task
        self._heartbeat_tasks: dict[str, asyncio.Task] = {}  # project_id -> heartbeat Task

        # Пауза: Event set = работа, Event clear = пауза
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # изначально НЕ на паузе
        self._paused = False
        self._pause_mode: str | None = None  # "finish_current" | "interrupt"

        # Лок-handshake между _enqueue_single и завершением batch worker:
        # без него возможна гонка, когда worker уже вышел из while-цикла, но
        # ещё не успел перевести queue.status в "completed" — _enqueue_single
        # увидит running-очередь и допишет item, который никто не подберёт.
        self._enqueue_lock = asyncio.Lock()

    ZOMBIE_TIMEOUT_SEC = 600  # 10 минут без heartbeat = зомби

    # ─── Привязка job к объекту ────────────────────────────────────────
    # Каждый job, идущий через PipelineManager, обязан быть привязан к
    # object_id, под которым он создавался. _create_bound_task оборачивает
    # coroutine в per-task ContextVar set, чтобы все вложенные вызовы
    # resolve_project_dir() видели именно тот projects_dir, а не текущий
    # активный объект из objects.json.

    @staticmethod
    def _resolve_object_id(object_id: Optional[str]) -> Optional[str]:
        """Вычислить object_id для нового job. None → current_id."""
        return object_id if object_id is not None else _current_object_id_or_none()

    @staticmethod
    def _resolve_object_id_for_project(
        object_id: Optional[str],
        project_id: Optional[str],
        version_id: Optional[str] = None,
    ) -> Optional[str]:
        """Resolve job object by the actual v2 document before falling back to current_id."""
        if object_id is not None:
            return object_id
        if project_id:
            try:
                from backend.app.services.storage.storage_write_facade import (
                    StorageWriteFacade,
                    v2_is_primary,
                )

                if v2_is_primary():
                    v2_root = StorageWriteFacade().v2_root()
                    if v2_root is not None:
                        from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter

                        doc = ProjectsV2Adapter(v2_root).find_document_by_project_id(project_id)
                        if doc and doc.get("object_id"):
                            return str(doc["object_id"])
            except Exception:
                pass
        return _current_object_id_or_none()

    @staticmethod
    def _v2_object_id_from_doc_dir(doc_dir: Path) -> Optional[str]:
        try:
            object_json = Path(doc_dir).parents[3] / "object.json"
            data = json.loads(object_json.read_text(encoding="utf-8"))
            value = data.get("object_id") if isinstance(data, dict) else None
            return str(value) if value else None
        except Exception:
            return None

    @staticmethod
    def _create_bound_task(coro, job: AuditJob) -> asyncio.Task:
        """Запустить coroutine с биндингом object_id из job.

        Если у job.object_id нет — запускает как обычный task (совместимо со
        старыми путями). Если object_id есть — внутри task выставляет
        ContextVar, и все resolve_project_dir() под ним используют именно
        projects_dir этого объекта.
        """
        bound_id = job.object_id
        if not bound_id:
            return asyncio.create_task(coro)

        async def _bound():
            token = bind_object(bound_id)
            try:
                return await coro
            finally:
                unbind_object(token)

        return asyncio.create_task(_bound())

    # ─── Пауза/Возобновление ───

    async def pause(self, mode: str = "finish_current") -> dict:
        """
        Поставить на паузу.

        mode:
          - "finish_current": дождаться завершения текущего этапа, не запускать следующий
          - "interrupt": прервать текущий Claude CLI процесс
        """
        if self._paused:
            return {"status": "already_paused"}

        self._paused = True
        self._pause_mode = mode
        self._pause_event.clear()  # блокировать _check_pause()

        # Логируем во все активные проекты
        for pid, job in self.active_jobs.items():
            await self._log(job, f"⏸ ПАУЗА ({mode})", "warn")

        await ws_manager.broadcast_global(
            WSMessage.log("__SYSTEM__", f"⏸ Пауза: {mode}", "warn")
        )

        if mode == "interrupt":
            # Убить все активные Claude CLI процессы
            for pid in list(self.active_jobs.keys()):
                killed = await kill_all_processes(pid)
                if killed:
                    await self._log(
                        self.active_jobs[pid],
                        f"Прервано: {killed} процессов убито",
                        "warn",
                    )

        return {
            "status": "paused",
            "mode": mode,
            "active_projects": list(self.active_jobs.keys()),
        }

    async def unpause(self) -> dict:
        """Снять паузу — продолжить работу."""
        if not self._paused:
            return {"status": "not_paused"}

        self._paused = False
        self._pause_mode = None
        self._pause_event.set()  # разблокировать _check_pause()

        for pid, job in self.active_jobs.items():
            await self._log(job, "▶ Продолжение работы", "info")
            # Восстановить pause_total_sec
            if hasattr(job, '_pause_started_at') and job._pause_started_at:
                pause_duration = (datetime.now() - job._pause_started_at).total_seconds()
                job.pause_total_sec += pause_duration
                job._pause_started_at = None

        await ws_manager.broadcast_global(
            WSMessage.log("__SYSTEM__", "▶ Продолжение работы", "info")
        )

        return {"status": "resumed"}

    def get_pause_status(self) -> dict:
        """Текущий статус паузы."""
        return {
            "paused": self._paused,
            "mode": self._pause_mode,
        }

    # ─── Персистентность очереди ───────────────────────────────────────

    def _persist_queue(self) -> None:
        """Сохранить текущую очередь на диск (batch_queue.json).

        Вызывается после каждого изменения состояния очереди. Если очереди
        нет — файл не трогаем (старая история остаётся видимой).

        Запись атомарная (tmp + os.replace), чтобы сбой процесса не оставил
        повреждённый JSON.
        """
        if self._batch_queue is None:
            return
        try:
            BATCH_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
            payload = self._batch_queue.model_dump_json(indent=2)
            tmp = BATCH_QUEUE_FILE.with_suffix(BATCH_QUEUE_FILE.suffix + ".tmp")
            tmp.write_text(payload, encoding="utf-8")
            os.replace(tmp, BATCH_QUEUE_FILE)
        except Exception as e:
            print(f"[PipelineManager] Ошибка сохранения очереди: {e}")

    def is_project_in_active_batch(self, pid: str) -> bool:
        """True если проект участвует в активной batch-очереди (pending или running).
        Используется prepare_service для блокировки ручного prepare/retry.
        """
        q = self._batch_queue
        if q is None or q.status != "running":
            return False
        return any(
            it.project_id == pid and it.status in ("pending", "running")
            for it in q.items
        )

    # ─── Живость worker'а очереди и текущего аудита ─────────────────────
    # Ключевой инвариант инцидента: batch-worker __BATCH__ ВЫПОЛНЯЕТ текущий
    # project audit внутри своей же корутины (см. _run_batch_queue:
    # self._tasks[pid] = asyncio.current_task()). Поэтому «worker потерян, но
    # проект ещё жив» на практике = у живой корутины ошибочно сняли регистрацию
    # в self._tasks (это делал cleanup_zombies). Эти helper'ы дают надёжный
    # признак живости, не завязанный на in-memory job/heartbeat-трекинг.

    def _batch_worker_alive(self) -> bool:
        """True если asyncio-таск worker'а очереди __BATCH__ реально жив."""
        task = self._tasks.get("__BATCH__")
        return task is not None and not task.done()

    def _current_batch_item_pid(self) -> Optional[str]:
        """project_id текущего (current_index) элемента очереди, либо None."""
        q = self._batch_queue
        if q is None:
            return None
        idx = q.current_index
        if 0 <= idx < len(q.items):
            return q.items[idx].project_id
        return None

    def _has_live_project_audit(self) -> bool:
        """True если реально выполняется аудит проекта этой очереди.

        Любого сигнала достаточно (от самого надёжного к запасному):
          1. worker-таск __BATCH__ жив (он же исполняет текущий project audit);
          2. в active_jobs есть не-__BATCH__ job со статусом RUNNING;
          3. у текущего элемента очереди есть живые дочерние процессы
             (ground-truth, переживает in-memory GC джоба/heartbeat'а).
        """
        if self._batch_worker_alive():
            return True
        for pid, job in self.active_jobs.items():
            if pid == "__BATCH__":
                continue
            if job.status == JobStatus.RUNNING:
                return True
        cur = self._current_batch_item_pid()
        if cur is not None and has_live_processes(cur):
            return True
        return False

    def _protected_pids(self) -> set[str]:
        """pid, которые cleanup_zombies НЕ имеет права снимать как зомби.

        Защищаем worker очереди и текущий выполняющийся проект, пока аудит жив,
        а также любой проект с живыми дочерними процессами. Иначе живой аудит
        ошибочно признаётся зомби → очередь демотируется в interrupted.
        """
        protected: set[str] = set(active_process_pids())
        if self._batch_worker_alive():
            protected.add("__BATCH__")
            cur = self._current_batch_item_pid()
            if cur is not None:
                protected.add(cur)
        return protected

    def get_batch_diagnostics(self) -> dict:
        """Диагностика для UI/API: разделяет состояние очереди, текущего проекта
        и worker'а, чтобы не показывать ложный «полный сбой».
        """
        q = self._batch_queue
        worker_alive = self._batch_worker_alive()
        live_audit = self._has_live_project_audit()
        cur = self._current_batch_item_pid()
        status = q.status if q is not None else None
        # worker потерян, но очередь формально running
        batch_worker_lost = bool(q is not None and status == "running" and not worker_alive)
        # resume безопасен только когда нет живого аудита и очередь прервана
        # (или running с потерянным worker'ом и без живого проекта)
        resume_available = bool(
            q is not None
            and not live_audit
            and (status == "interrupted" or batch_worker_lost)
        )
        if live_audit and batch_worker_lost:
            display_status = "degraded_but_current_running"
        elif live_audit and status == "running":
            display_status = "running"
        else:
            display_status = status
        return {
            "queue_status": status,
            "display_status": display_status,
            "worker_alive": worker_alive,
            "current_project_running": live_audit,
            "current_project_id": cur if live_audit else None,
            "batch_worker_lost": batch_worker_lost,
            "resume_available": resume_available,
        }

    def load_persisted_queue(self) -> None:
        """Загрузить очередь после перезапуска сервера.

        running-элементы → interrupted (процесс был прерван рестартом).
        pending-элементы → остаются pending (не были запущены).
        Статус очереди → "interrupted" (не "running") чтобы worker не запустился.

        Pipeline сам управляет платными вызовами — после рестарта resume
        продолжает работу без галок (kill-switch PAID_API_ENABLED — глобальный).
        """
        if not BATCH_QUEUE_FILE.exists():
            return
        try:
            data = json.loads(BATCH_QUEUE_FILE.read_text(encoding="utf-8"))
            # Legacy очистка: старые batch_queue.json могли содержать
            # manual_run_id — поле удалено, не передаём в BatchQueueItem.
            for it in data.get("items", []) or []:
                if isinstance(it, dict):
                    it.pop("manual_run_id", None)
            queue = BatchQueueStatus(**data)
        except Exception as e:
            print(f"[Recovery] Ошибка загрузки batch_queue.json: {e}")
            return

        # Не восстанавливать уже завершённые (completed/cancelled) без прерванных
        has_interrupted_or_pending = any(
            it.status in ("running", "pending") for it in queue.items
        )
        if not has_interrupted_or_pending and queue.status != "interrupted":
            # Очередь уже была полностью завершена — тем не менее показываем историю
            pass

        changed = False
        for item in queue.items:
            if item.status == "running":
                item.status = "interrupted"
                # Понятная причина для UI/диагностики (раньше reason отсутствовал).
                if not item.error:
                    item.error = "Прервано рестартом сервера"
                changed = True

        if queue.status == "running":
            queue.status = "interrupted"
            changed = True

        if changed:
            interrupted_count = sum(1 for it in queue.items if it.status == "interrupted")
            pending_count = sum(1 for it in queue.items if it.status == "pending")
            print(
                f"[Recovery] Восстановлена очередь: {interrupted_count} прервано, "
                f"{pending_count} ожидало, всего {len(queue.items)} элементов"
            )

        self._batch_queue = queue
        # Сохранить обновлённые статусы
        self._persist_queue()

    def clear_queue_history(self) -> None:
        """Удалить историю очереди (файл + in-memory, только если не running)."""
        if self._batch_queue and self._batch_queue.status == "running":
            raise RuntimeError("Нельзя очистить работающую очередь")
        self._batch_queue = None
        try:
            if BATCH_QUEUE_FILE.exists():
                BATCH_QUEUE_FILE.unlink()
        except Exception as e:
            print(f"[PipelineManager] Ошибка удаления batch_queue.json: {e}")

    async def auto_resume_interrupted_batch(self, delay_sec: float = 20.0) -> None:
        """Авто-возобновление прерванной очереди после старта бэкенда.

        Инцидент 03.07.2026: вотчдог дважды убивал бэкенд посреди ночного
        батча, и до ручного POST /api/audit/batch/resume очередь простаивала.
        Этот метод вызывается фоновым таском из lifespan-startup.

        Безопасность:
          - kill-switch: BATCH_AUTO_RESUME_ENABLED=false отключает полностью;
          - задержка delay_sec даёт серверу подняться (health, роутеры),
            чтобы тяжёлый пайплайн не стартовал в момент инициализации;
          - вся логика гонок/идемпотентности — внутри resume_interrupted_batch
            (лок, проверка живого worker'а/аудита); здесь только мягкие
            отказы в лог, никаких исключений наружу.
        """
        enabled = (os.environ.get("BATCH_AUTO_RESUME_ENABLED", "true").strip().lower()
                   not in ("false", "0", "no", "off"))
        if not enabled:
            print("[Recovery] Авто-resume батча отключён (BATCH_AUTO_RESUME_ENABLED=false)")
            return
        try:
            await asyncio.sleep(delay_sec)
            queue = self._batch_queue
            if queue is None or queue.status != "interrupted":
                return
            if not any(it.status in ("pending", "interrupted") for it in queue.items):
                return
            resumed = await self.resume_interrupted_batch()
            print(
                f"[Recovery] Авто-resume батча: очередь {resumed.queue_id} "
                f"возобновлена ({resumed.completed}/{resumed.total} уже готово)"
            )
        except BatchResumeBlockedError as e:
            print(f"[Recovery] Авто-resume батча отложен: {e}")
        except Exception as e:
            print(f"[Recovery] Авто-resume батча не удался: {e}")

    async def resume_interrupted_batch(self) -> BatchQueueStatus:
        """Restart a persisted interrupted queue from unfinished items.

        Безопасность resume:
          - идемпотентность: если worker очереди уже жив — НЕ создаём второй
            worker, просто возвращаем текущую очередь (повторный клик безвреден);
          - НЕ запускаем resume, пока жив текущий аудит проекта — иначе
            дубль-worker мог бы перезапустить уже идущий проект, убить его
            claude-процессы и перезаписать 01/02/03. Возвращаем понятный 409.
          - НЕ делаем kill/pkill активных процессов и НЕ удаляем артефакты.
        """
        async with self._enqueue_lock:
            queue = self._batch_queue
            if not queue:
                raise RuntimeError("Нет прерванной очереди")
            # Идемпотентность: worker уже жив → очередь и так работает.
            if self._batch_worker_alive():
                return queue
            # Текущий проект ещё реально выполняется (worker-регистрацию могли
            # ошибочно снять, но корутина/процессы живы) — resume небезопасен.
            if self._has_live_project_audit():
                raise BatchResumeBlockedError(
                    "Текущий проект ещё выполняется — продолжение очереди будет "
                    "доступно после его завершения"
                )
            if queue.status == "running":
                # status running, но worker мёртв и живого аудита нет —
                # нормализуем, чтобы можно было пересоздать worker.
                queue.status = "interrupted"
            if queue.status != "interrupted":
                raise RuntimeError("Очередь не находится в состоянии interrupted")

            resumable = False
            for item in queue.items:
                if item.status in ("interrupted", "running"):
                    item.status = "pending"
                    item.error = None
                    resumable = True
                elif item.status == "pending":
                    resumable = True

            if not resumable:
                raise RuntimeError("В очереди нет задач для продолжения")

            first_pending = next(
                (idx for idx, item in enumerate(queue.items) if item.status == "pending"),
                0,
            )
            queue.current_index = first_pending
            queue.status = "running"

            meta_job = AuditJob(
                job_id=queue.queue_id,
                object_id=self._resolve_object_id(None),
                project_id="__BATCH__",
                stage=AuditStage.PREPARE,
                status=JobStatus.RUNNING,
                started_at=datetime.now().isoformat(),
                progress_total=queue.total,
                progress_current=queue.completed + queue.failed,
            )
            self.active_jobs["__BATCH__"] = meta_job
            self._tasks["__BATCH__"] = self._create_bound_task(
                self._run_batch_queue(queue, meta_job),
                meta_job,
            )

        await self._broadcast_batch_progress(queue)
        return queue

    async def _check_pause(self, job: AuditJob) -> bool:
        """
        Проверить паузу между этапами pipeline.

        Вызывается перед каждым новым этапом. Если на паузе — ждёт.
        Returns: True = можно продолжать, False = job отменён.
        """
        if not self._paused:
            return job.status != JobStatus.CANCELLED

        # Запомнить время начала паузы для ETA
        job._pause_started_at = datetime.now()

        await self._log(job, "⏸ Пауза — ожидание команды 'Продолжить'...", "warn")

        # Отправляем WS-обновление
        await ws_manager.broadcast_to_project(
            job.project_id,
            WSMessage.status_change(job.project_id, {"status": "paused"}),
        )

        # Ждём unpause
        await self._pause_event.wait()

        await self._log(job, "▶ Возобновлено", "info")

        return job.status != JobStatus.CANCELLED

    # ─── Rate Limit: ожидание сброса лимита ───

    async def _wait_for_rate_limit(self, job: AuditJob, reason: str = "", cli_output: str = "") -> bool:
        """
        Ожидать сброса rate limit. Периодически проверяет usage.

        Args:
            job: текущий AuditJob (для логирования и проверки отмены)
            reason: причина паузы (для лога)
            cli_output: сырой вывод Claude CLI (для парсинга времени сброса)

        Returns:
            True если лимит сбросился и можно продолжать,
            False если job отменён или превышен макс. таймаут ожидания.
        """
        pause_start = datetime.now()
        total_waited = 0

        # Попытка извлечь точное время сброса из вывода CLI
        parsed_wait = None
        if cli_output:
            parsed_wait = claude_runner.parse_rate_limit_reset(cli_output)

        check = global_scanner.check_rate_limit(RATE_LIMIT_THRESHOLD_PCT)

        # Если CLI дал точное время — используем его, иначе из scanner
        if parsed_wait:
            wait_sec = parsed_wait
            hours = wait_sec // 3600
            mins_remaining = (wait_sec % 3600) // 60
            resets_text = f"{hours} ч {mins_remaining} мин" if hours > 0 else f"{mins_remaining} мин"
        else:
            wait_sec = check.get("wait_seconds", RATE_LIMIT_CHECK_INTERVAL)
            resets_text = check.get("resets_in_text", "?")

        usage_pct = check.get("usage_pct", 0)

        await self._log(
            job,
            f"ПАУЗА: {reason or check.get('reason', 'rate limit')}. "
            f"Сброс через ~{resets_text}. "
            f"Ожидание...",
            "warn",
        )
        # Уведомляем фронтенд о паузе
        await ws_manager.broadcast_to_project(
            job.project_id,
            WSMessage.log(
                job.project_id,
                f"Rate limit пауза: сброс через ~{resets_text}",
                level="warn",
            ),
        )

        try:
            while total_waited < RATE_LIMIT_MAX_WAIT:
                if job.status == JobStatus.CANCELLED:
                    return False

                # Спим порциями, чтобы можно было отменить
                sleep_chunk = min(RATE_LIMIT_CHECK_INTERVAL, RATE_LIMIT_MAX_WAIT - total_waited)
                await asyncio.sleep(sleep_chunk)
                total_waited += sleep_chunk

                # Если есть точное время из CLI — просто ждём до него
                if parsed_wait and total_waited >= parsed_wait:
                    await self._log(
                        job,
                        f"Время сброса rate limit достигнуто (ждали {total_waited // 60} мин). Продолжаем.",
                        "info",
                    )
                    return True

                # Без точного времени — проверяем scanner
                if not parsed_wait:
                    global_scanner.invalidate_cache()
                    check = global_scanner.check_rate_limit(RATE_LIMIT_THRESHOLD_PCT)

                    if check["can_proceed"]:
                        mins = total_waited // 60
                        await self._log(
                            job,
                            f"Rate limit сброшен после {mins} мин ожидания. Продолжаем.",
                            "info",
                        )
                        return True

                # Каждые 5 минут логируем статус ожидания
                if total_waited % 300 == 0:
                    remaining = (parsed_wait - total_waited) if parsed_wait else None
                    if remaining and remaining > 0:
                        r_min = remaining // 60
                        await self._log(
                            job,
                            f"Ожидание rate limit: осталось ~{r_min} мин "
                            f"(ждём {total_waited // 60} мин)",
                            "warn",
                        )
                    else:
                        await self._log(
                            job,
                            f"Ожидание rate limit "
                            f"(ждём {total_waited // 60} мин)",
                            "warn",
                        )

            await self._log(job, f"Превышено макс. время ожидания rate limit ({RATE_LIMIT_MAX_WAIT // 3600} ч)", "error")
            return False
        finally:
            # Накапливаем реальное время паузы (для вычисления чистого времени)
            paused_sec = (datetime.now() - pause_start).total_seconds()
            job.pause_total_sec += paused_sec

    async def _check_before_launch(self, job: AuditJob) -> bool:
        """
        Превентивная проверка паузы перед запуском LLM.

        OpenRouter имеет встроенные retries при rate limit (в llm_runner),
        поэтому проверка global_scanner больше не нужна.

        Returns:
            True если можно запускать, False если job отменён.
        """
        # Проверка паузы (ждёт если на паузе)
        if not await self._check_pause(job):
            return False

        return True

    def _record_cli_usage(self, job: AuditJob, cli_result, stage: str, is_retry: bool = False):
        """Записать использование токенов после LLM вызова.

        Работает как с LLMResult (OpenRouter), так и с CLIResult (legacy).
        Токены берутся напрямую из result — обогащение из JSONL не требуется.
        Также обогащает pipeline_log.json полями model/input_tokens/output_tokens.

        CLI-модели (подписка) — cost_usd=0 (бесплатно), оригинал в cost_usd_notional.
        """
        if not cli_result:
            return

        # LLMResult имеет input_tokens/output_tokens напрямую
        input_tokens = getattr(cli_result, "input_tokens", 0) or 0
        output_tokens = getattr(cli_result, "output_tokens", 0) or 0
        cache_creation_tokens = getattr(cli_result, "cache_creation_tokens", 0) or 0
        cache_read_tokens = getattr(cli_result, "cache_read_tokens", 0) or 0
        model = getattr(cli_result, "model", "") or get_model_for_stage(stage)

        # CLI-модели работают по подписке — реальная стоимость = $0
        raw_cost = cli_result.cost_usd or 0.0
        is_cli = model.startswith("claude-") and "/" not in model
        actual_cost = 0.0 if is_cli else raw_cost

        record = UsageRecord(
            timestamp=datetime.now().isoformat(),
            session_id=cli_result.session_id,
            project_id=job.project_id,
            stage=stage,
            model=model,
            cost_usd=actual_cost,
            cost_usd_notional=raw_cost if is_cli else 0.0,
            duration_ms=cli_result.duration_ms,
            duration_api_ms=cli_result.duration_api_ms,
            num_turns=cli_result.num_turns,
            api_calls=1,
            is_retry=is_retry,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_tokens=cache_creation_tokens,
            cache_read_tokens=cache_read_tokens,
        )
        usage_tracker.record_usage(record)
        # paid_cost_tracker инкрементируется внутри llm_runner.run_llm — здесь не дублируем.
        job.cost_usd += actual_cost
        job.cli_calls += 1

        # Обогатить pipeline_log.json полями model/tokens для текущего этапа
        self._enrich_pipeline_log(job.project_id, stage, model, input_tokens, output_tokens)

    def _enrich_pipeline_log(self, project_id: str, stage: str, model: str,
                              input_tokens: int, output_tokens: int):
        """Добавить model и tokens в запись pipeline_log.json для этапа.

        Агрегирует токены для batch-этапов (block_batch_001..N → block_analysis).
        """
        import re
        # Нормализуем stage key для pipeline_log
        _batch_re = re.compile(r"(block_batch|tile_batch)_\d+")
        _norm_re = re.compile(r"norm_verify(_chunk_\d+|_retry_\d+)")
        _critic_re = re.compile(r"findings_critic(_chunk\d+)?")
        _corrector_re = re.compile(r"findings_corrector(_chunk\d+)?")
        _opt_critic_re = re.compile(r"optimization_critic(_retry_\d+)?")
        _opt_corrector_re = re.compile(r"optimization_corrector(_retry_\d+)?")
        _retry_re = re.compile(r"^(.+?)_retry(_\d+)?$")

        log_key = stage
        if _batch_re.match(stage):
            log_key = "block_analysis"
        elif _norm_re.match(stage):
            log_key = "norm_verify"
        elif _critic_re.match(stage):
            log_key = "findings_critic"
        elif _corrector_re.match(stage):
            log_key = "findings_corrector"
        elif _opt_critic_re.match(stage):
            log_key = "optimization_critic"
        elif _opt_corrector_re.match(stage):
            log_key = "optimization_corrector"
        else:
            m = _retry_re.match(stage)
            if m:
                log_key = m.group(1)

        try:
            output_dir = self._output_dir_for_project(project_id)
            log_path = output_dir / "pipeline_log.json"
            if not log_path.exists():
                return

            with open(log_path, "r", encoding="utf-8") as f:
                log_data = json.load(f)

            stage_info = log_data.get("stages", {}).get(log_key, {})
            if not stage_info:
                return

            # Для batch-этапов: агрегируем токены
            prev_in = stage_info.get("input_tokens", 0)
            prev_out = stage_info.get("output_tokens", 0)
            is_aggregate = log_key in ("block_analysis", "norm_verify",
                                        "findings_critic", "optimization_critic")
            if is_aggregate and (prev_in > 0 or prev_out > 0):
                stage_info["input_tokens"] = prev_in + input_tokens
                stage_info["output_tokens"] = prev_out + output_tokens
            else:
                stage_info["input_tokens"] = input_tokens
                stage_info["output_tokens"] = output_tokens

            stage_info["model"] = model

            log_data["stages"][log_key] = stage_info
            with open(log_path, "w", encoding="utf-8") as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass  # Не ронять pipeline из-за обогащения лога

    async def _enrich_usage_async(self, session_id: str, record_timestamp: str):
        """Legacy no-op. Обогащение из JSONL больше не требуется (токены приходят из API)."""
        pass

    @staticmethod
    def job_key(project_id: str, version_id: Optional[str] = None) -> str:
        """Сформировать ключ active_jobs с учётом версии.

        Для legacy V1 (или version_id=None) ключом остаётся `project_id` —
        это сохраняет обратную совместимость с уже работающими job-ами.
        Для V2+ ключ строится как `<project_id>:<version_id>`, чтобы V1 и V2
        одного проекта не конфликтовали.
        """
        if not version_id or version_id == "v1":
            return project_id
        return f"{project_id}:{version_id}"

    def is_running(self, project_id: str, version_id: Optional[str] = None) -> bool:
        """Проверить, бежит ли job для (project_id[, version_id]).

        Без `version_id` — старая семантика (любая активная job по project_id).
        """
        if version_id and version_id != "v1":
            return self.job_key(project_id, version_id) in self.active_jobs
        return project_id in self.active_jobs

    def is_queued(self, project_id: str) -> bool:
        """Проверить, стоит ли проект в очереди со статусом pending."""
        if not self._batch_queue or self._batch_queue.status != "running":
            return False
        return any(
            it.project_id == project_id and it.status == "pending"
            for it in self._batch_queue.items
        )

    def has_active_or_queued_work(self) -> bool:
        if any(task is not None and not task.done() for task in self._tasks.values()):
            return True
        if any(job.status in {JobStatus.RUNNING, JobStatus.QUEUED} for job in self.active_jobs.values()):
            return True
        if self._batch_queue and self._batch_queue.status == "running":
            if any(item.status in ("pending", "running") for item in self._batch_queue.items):
                return True
        return False

    def is_idle(self) -> bool:
        return not self.has_active_or_queued_work()

    def get_job(self, project_id: str) -> Optional[AuditJob]:
        """Текущий job проекта.

        Если проект бежит — возвращает реальный job из active_jobs.
        Если стоит в очереди — возвращает placeholder со status=QUEUED, чтобы
        фронт между моментом enqueue и реальным стартом не видел "ничего".
        """
        job = self.active_jobs.get(project_id)
        if job is not None:
            return job
        # Проект в очереди?
        if self._batch_queue and self._batch_queue.status == "running":
            for it in self._batch_queue.items:
                if it.project_id == project_id and it.status == "pending":
                    return AuditJob(
                        job_id=it.job_id or "",
                        object_id=self._resolve_object_id(None),
                        project_id=project_id,
                        stage=AuditStage.PREPARE,
                        status=JobStatus.QUEUED,
                    )
        return None

    def cleanup_zombies(self):
        """Очистить зомби-задачи (нет heartbeat более ZOMBIE_TIMEOUT_SEC).

        Защита: НИКОГДА не снимаем worker очереди __BATCH__ и текущий живой
        проект, пока аудит реально выполняется. Раньше __BATCH__ (у него нет
        собственного heartbeat-цикла) после ZOMBIE_TIMEOUT_SEC ложно
        признавался зомби, снимался из self._tasks — и _reconcile_stale_queue
        затем демотировал всю очередь в interrupted, хотя проект ещё шёл.
        """
        now = datetime.now()
        # pid, которые нельзя трогать пока жив batch-worker / есть живые процессы.
        protected = self._protected_pids()
        zombies = []
        for pid, job in list(self.active_jobs.items()):
            if pid in protected:
                continue  # живой worker / текущий проект — не зомби
            # __BATCH__ судим по живости таска, а не по heartbeat (его нет).
            if pid == "__BATCH__":
                if not self._batch_worker_alive():
                    zombies.append(pid)
                continue
            # Любой проект с живыми дочерними процессами — реально работает.
            if has_live_processes(pid):
                continue
            # Живой asyncio-таск = НЕ зомби, независимо от status/heartbeat.
            # Раньше job снимался без task.cancel(): корутина оставалась жить
            # «призраком» (невидима в active_jobs/_tasks), а повторный запуск
            # проекта мог поднять ПАРАЛЛЕЛЬНЫЙ аудит того же проекта. Сетевые
            # стадии (OpenRouter/critic между claude-вызовами) не имеют живых
            # дочерних процессов — has_live_processes ложно давал False.
            _task = self._tasks.get(pid)
            if _task is not None and not _task.done():
                continue
            if job.status != JobStatus.RUNNING:
                zombies.append(pid)
                continue
            # Определяем последнюю активность
            last_activity = job.last_heartbeat or job.started_at
            if last_activity:
                try:
                    last_time = datetime.fromisoformat(last_activity)
                    elapsed = (now - last_time).total_seconds()
                    if elapsed > self.ZOMBIE_TIMEOUT_SEC:
                        zombies.append(pid)
                except (ValueError, TypeError):
                    zombies.append(pid)
            else:
                zombies.append(pid)

        for pid in zombies:
            print(f"[PipelineManager] Очистка зомби-задачи: {pid}")
            self._cleanup(pid)

        # Заодно приводим batch-очередь к консистентному виду: мёртвый worker
        # не должен оставлять фантомные 'running' item'ы (живой спиннер в UI).
        self._reconcile_stale_queue()

    def _recover_stale_pipelines(self):
        """Сканирует все pipeline_log.json и помечает зависшие 'running' как 'interrupted'.

        Вызывается при старте сервера. Если сервер был перезапущен во время
        активного аудита, процессы Claude CLI уже завершились, но pipeline_log
        остался в состоянии 'running'. Помечаем как 'interrupted' чтобы:
        1. UI показывал корректный статус (не вечный спиннер)
        2. Resume мог подхватить с прерванного этапа
        """
        from backend.app.services.common.project_service import iter_project_dirs
        from backend.app.services.common.version_service import is_version_container

        # Собрать project_id активных задач, чтобы не трогать их
        active_pids = set(self.active_jobs.keys())

        def _recover_one_log(log_path: Path, label: str) -> bool:
            if not log_path.exists():
                return False
            try:
                data = json.loads(log_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                print(f"[Recovery] Ошибка чтения {log_path}: {e}")
                return False
            stages = data.get("stages", {})
            changed = False
            for stage_key, stage_info in stages.items():
                if stage_info.get("status") == "running":
                    # Этот этап остался "running" после рестарта — прерван
                    stage_info["status"] = "interrupted"
                    stage_info["error"] = "Сервер перезапущен во время выполнения"
                    stage_info["interrupted_at"] = datetime.now().isoformat()
                    changed = True
                    print(f"[Recovery] {label}: этап '{stage_key}' running → interrupted")
            if changed:
                data["last_updated"] = datetime.now().isoformat()
                log_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            return changed

        recovered = 0
        for _pid, project_dir in iter_project_dirs():
            # Не трогать проекты с активным аудитом
            if _pid in active_pids:
                continue
            # Сканируем primary + ВСЕ версии контейнера `<база>(main)/` — раньше
            # бралась только primary _output, и V2-аудит оставался вечным
            # 'running' после рестарта (reserc.md #10). Filesystem-обход надёжнее
            # манифеста версий.
            scan_dirs = [project_dir]
            parent = project_dir.parent
            try:
                if is_version_container(parent):
                    for sib in sorted(parent.iterdir()):
                        if sib.is_dir() and sib != project_dir and not sib.name.startswith("_"):
                            scan_dirs.append(sib)
            except OSError:
                pass
            for vdir in scan_dirs:
                if _recover_one_log(vdir / "_output" / "pipeline_log.json", vdir.name):
                    recovered += 1

        # projects_v2-primary хранит live-снимок не в legacy `_output`, а в
        # `versions/<vid>/03_analysis/latest`. Старый recovery его не видел:
        # после watchdog-restart UI оставался с вечными running-этапами, а
        # resume-detector мог принять старые latest-артефакты за завершённый run.
        try:
            from backend.app.services.storage.storage_write_facade import StorageWriteFacade

            v2_root = StorageWriteFacade().v2_root()
        except Exception:
            v2_root = None
        if v2_root is not None:
            try:
                v2_logs = sorted(
                    Path(v2_root).glob(
                        "objects/*/disciplines/*/documents/*/versions/*/"
                        "03_analysis/latest/pipeline_log.json"
                    )
                )
            except OSError:
                v2_logs = []
            for log_path in v2_logs:
                try:
                    doc_code = log_path.parents[4].name
                except IndexError:
                    doc_code = ""
                if doc_code and doc_code in active_pids:
                    continue
                try:
                    label = str(log_path.relative_to(v2_root))
                except ValueError:
                    label = str(log_path)
                if _recover_one_log(log_path, label):
                    recovered += 1

        if recovered:
            print(f"[Recovery] Восстановлено {recovered} проектов с зависшими этапами")

    async def cancel(self, project_id: str) -> bool:
        """Отменить запущенный или очередённый аудит.

        Для running — убивает дочерние процессы и снимает задачу.
        Для pending — удаляет item из очереди (без убийства, т.к. ничего не
        запущено).
        """
        job = self.active_jobs.get(project_id)
        if job:
            job.status = JobStatus.CANCELLED
            killed = await kill_all_processes(project_id)
            if killed:
                print(f"[{project_id}] Убито {killed} дочерних процессов")
            task = self._tasks.get(project_id)
            # _tasks[pid] у батч-item'а указывает на корутину ВСЕГО worker'а
            # очереди (см. `self._tasks[pid] = asyncio.current_task()` в
            # _run_batch_queue). task.cancel() в этом случае убивал бы всю
            # очередь: CancelledError — BaseException, пролетает мимо
            # `except Exception` item-цикла, worker умирает, очередь виснет
            # в 'running' (фантом). Отменяем таск только если он НЕ worker:
            # для item'а достаточно job.status=CANCELLED + убитых процессов —
            # стадия завершится сама, item-цикл штатно пометит cancelled и
            # перейдёт к следующему проекту.
            if task is not None and task is not self._tasks.get("__BATCH__"):
                task.cancel()
            self._cleanup(project_id)
            await ws_manager.broadcast_to_project(
                project_id,
                WSMessage.log(project_id, f"Аудит отменён пользователем (убито {killed} процессов)", "warn"),
            )
            return True

        # Не бежит сейчас — может быть в очереди?
        if self._batch_queue and self._batch_queue.status == "running":
            for it in self._batch_queue.items:
                if it.project_id == project_id and it.status == "pending":
                    it.status = "cancelled"
                    await ws_manager.broadcast_global(
                        WSMessage.log(
                            "__BATCH__",
                            f"⊘ {project_id}: убран из очереди",
                            "warn",
                        )
                    )
                    await self._broadcast_batch_progress(self._batch_queue)
                    return True
        return False

    def _cleanup(self, project_id: str):
        self._stop_heartbeat(project_id)
        self.active_jobs.pop(project_id, None)
        self._tasks.pop(project_id, None)

    def _cleanup_batch_worker(self, meta_job: "AuditJob") -> None:
        """Identity-aware финализация worker'а очереди (__BATCH__).

        Снимаем регистрацию __BATCH__ ТОЛЬКО если она всё ещё принадлежит этому
        worker'у. Гонка close+enqueue: пока старый worker доходит до finally,
        enqueue под _enqueue_lock мог уже поднять НОВЫЙ worker (новый task/meta_job
        под тем же ключом __BATCH__). Безусловный `_cleanup("__BATCH__")` снёс бы
        регистрацию нового worker'а → он осиротел бы (жив, но _batch_worker_alive
        врёт False → дубль-worker на следующем enqueue). Сверяем по identity.
        """
        current = asyncio.current_task()
        if self._tasks.get("__BATCH__") is current:
            self._tasks.pop("__BATCH__", None)
        if self.active_jobs.get("__BATCH__") is meta_job:
            self.active_jobs.pop("__BATCH__", None)
        self._stop_heartbeat("__BATCH__")

    async def _run_script(self, project_id: str, *args, **kwargs):
        """Обёртка run_script с автоматическим project_id для трекинга процессов."""
        return await run_script(*args, project_id=project_id, **kwargs)

    async def _run_script_for_job(
        self,
        job: "AuditJob",
        script: str,
        args: list[str] = None,
        **kwargs,
    ):
        """run_script с автоинъекцией version-aware AUDIT_* env.

        Использовать вместо `_run_script(pid, script, ...)` для всех subprocess
        invocations, привязанных к конкретному job. Subprocess получает в env
        достоверную идентификацию version, что позволяет скриптам логировать /
        ветвиться по версии без необходимости парсить argv.

        Если caller передал свои `env_overrides`, AUDIT_* добавляются поверх
        (caller-overrides побеждают на коллизии).
        """
        env_extra = self._make_audit_env_for_job(job)
        env_overrides = kwargs.pop("env_overrides", None) or {}
        merged_env = {**env_extra, **env_overrides}
        return await self._run_script(
            job.project_id,
            script,
            args,
            env_overrides=merged_env,
            **kwargs,
        )

    def _make_audit_env_for_job(self, job: "AuditJob") -> dict:
        """Собрать AUDIT_PROJECT_ID/VERSION_ID/VERSION_DIR/OUTPUT_DIR для subprocess env."""
        _root, version_dir, output_dir = self._resolve_job_paths(job)
        return {
            "AUDIT_PROJECT_ID": str(job.project_id),
            "AUDIT_VERSION_ID": str(job.version_id or "v1"),
            "AUDIT_VERSION_DIR": str(version_dir),
            "AUDIT_OUTPUT_DIR": str(output_dir),
        }

    def _project_path_for_job(self, job: "AuditJob") -> str:
        """Version-aware path к папке проекта для subprocess argv.

        Возвращает путь к `version_dir` (V1 → root project_dir; V2+ →
        `<root>/_versions/v{N}/`), относительный к BASE_DIR, если возможно.

        Использовать вместо `_project_path(job.project_id)` во всех subprocess
        invocations внутри pipeline stages, чтобы V2 audit не передавал V1 root
        в скрипты вроде process_project.py / blocks.py — иначе скрипт
        перезапишет V1 `_output/`.
        """
        _root, version_dir, _output = self._resolve_job_paths(job)
        try:
            return str(version_dir.relative_to(BASE_DIR))
        except ValueError:
            return str(version_dir)

    def _resolve_job_paths(self, job: "AuditJob") -> tuple[Path, Path, Path]:
        """Вернуть version-aware пути для job: (root_project_dir, version_dir, output_dir).

        - `root_project_dir`: корневая папка проекта (там, где лежит project_versions.json
          и V1 source-файлы). Использовать только для root-level manifest операций.
        - `version_dir`: папка активной версии (для V1 это = root_project_dir,
          для V2+ это `root/_versions/v{N}/`). Здесь ищутся PDF, MD, project_info.json
          для исполняемого аудита.
        - `output_dir`: `version_dir / _output`.

        Если `job.version_id` отсутствует или невалиден — возвращаем root в качестве
        version_dir (legacy V1 поведение). Стартовые endpoint'ы валидируют версию
        раньше и возвращают 404, поэтому сюда обычно доходит валидный version_id.
        """
        # Шаг 6B: v2-primary ветка активна ТОЛЬКО при WRITE_MODE=projects_v2_primary
        # (в проде WRITE_MODE=dual_write_shadow → ветка НЕ исполняется). Источник
        # читается из v2 01_input/02_work, output → 03_analysis/runs/<run_id>
        # (эквивалент legacy _output). Адаптация source-reading стадий под v2 —
        # отдельный шаг (см. отчёт 6B blockers). legacy/dual_shadow путь — ниже,
        # без изменений.
        from backend.app.services.storage import storage_write_facade as _swf
        if _swf.v2_is_primary():
            from backend.app.services.storage.v2_primary_wiring import resolve_v2_job_paths
            try:
                _legacy_root = resolve_project_dir(job.project_id)
            except Exception:
                _legacy_root = None  # legacy может отсутствовать в v2-primary мире
            _object_id = getattr(job, "object_id", None)
            _paths = resolve_v2_job_paths(
                job.project_id, job.version_id,
                run_id=getattr(job, "job_id", None),
                object_id=_object_id,
                legacy_project_dir=_legacy_root,
            )
            if _paths is None and _object_id:
                _paths = resolve_v2_job_paths(
                    job.project_id, job.version_id,
                    run_id=getattr(job, "job_id", None),
                    object_id=None,
                    legacy_project_dir=_legacy_root,
                )
                if _paths is not None:
                    _resolved_object_id = self._v2_object_id_from_doc_dir(_paths[0])
                    if _resolved_object_id:
                        job.object_id = _resolved_object_id
            if _paths is None:
                raise RuntimeError(
                    f"v2-primary: не удалось разрешить v2-пути для "
                    f"{job.project_id}/{job.version_id}"
                )
            return _paths

        from backend.app.services.common import version_service
        root_dir = resolve_project_dir(job.project_id)
        try:
            version_dir = version_service.get_version_dir(
                root_dir, job.project_id, job.version_id,
            )
        except version_service.VersionNotFoundError:
            version_dir = root_dir
        return root_dir, version_dir, version_dir / "_output"

    def _load_project_info_for_paths(self, pid: str, root_dir: Path, version_dir: Path) -> dict:
        """Version-aware project_info for audit stages.

        Legacy reads the active version project_info with root fallback. In
        projects_v2-primary, source metadata lives in 01_input/project_info.json
        and mutable audit metadata is stored in version.json.project_info.
        """
        from backend.app.services.storage.storage_write_facade import v2_is_primary

        root_dir = Path(root_dir)
        version_dir = Path(version_dir)
        if v2_is_primary():
            info: dict = {}
            input_info = version_dir / "01_input" / "project_info.json"
            if input_info.is_file():
                try:
                    data = json.loads(input_info.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        info.update(data)
                except Exception:
                    pass
            version_json = version_dir / "version.json"
            if version_json.is_file():
                try:
                    data = json.loads(version_json.read_text(encoding="utf-8"))
                    version_info = data.get("project_info") if isinstance(data, dict) else None
                    if isinstance(version_info, dict):
                        info.update(version_info)
                except Exception:
                    pass
            info.setdefault("project_id", pid)
            info.setdefault("pdf_file", "document.pdf")
            return info

        info_path = version_dir / "project_info.json"
        if not info_path.exists():
            info_path = root_dir / "project_info.json"
        try:
            return json.loads(info_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _project_info_path_for_paths(self, root_dir: Path, version_dir: Path) -> Path:
        """Best-effort path for callers that still need a project_info filename."""
        from backend.app.services.storage.storage_write_facade import v2_is_primary

        version_dir = Path(version_dir)
        if v2_is_primary():
            input_info = version_dir / "01_input" / "project_info.json"
            return input_info if input_info.exists() else version_dir / "version.json"
        root_dir = Path(root_dir)
        version_info = version_dir / "project_info.json"
        return version_info if version_info.exists() else root_dir / "project_info.json"

    def _require_project_md(self, pid: str, root_dir: Path, version_dir: Path, info_path: Path):
        """Version-aware MD-gate. Бросает RuntimeError с понятной диагностикой
        (md_not_found / ambiguous_md_candidates), если MD не разрешается в папке
        активной версии. Возвращает MdResolution при успехе.

        Заменяет прежний наивный glob: учитывает latest_version_id, не доверяет
        битому project_info (md_file=None / pdf_file на чужой проект), даёт
        cross-version подсказку, не угадывает при неоднозначности.
        """
        from backend.app.services.storage.storage_write_facade import v2_is_primary
        if v2_is_primary():
            from backend.app.services.common.md_resolver import MdResolution, STATUS_OK
            from backend.app.services.storage.projects_v2_source_resolver import resolve_v2_source_files

            sources = resolve_v2_source_files(Path(version_dir), pid)
            if sources.md_path is None:
                raise RuntimeError(
                    f"v2_md_not_found: MD-файл не найден для проекта {pid} "
                    f"в {Path(version_dir) / '01_input'} или {Path(version_dir) / '02_work'}. "
                    "Анализ без MD не поддерживается."
                )
            return MdResolution(
                status=STATUS_OK,
                md_name=sources.md_path.name,
                md_path=sources.md_path,
                searched_dir=Path(version_dir),
                candidates=[str(sources.md_path.relative_to(Path(version_dir)))],
                diagnostics={"selected_by": "projects_v2_source_resolver"},
            )

        from backend.app.services.common.md_resolver import resolve_project_md
        from backend.app.services.common import version_service as _vs
        info = {}
        try:
            p = Path(info_path)
            if p.exists():
                info = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            info = {}
        latest_vid = None
        try:
            latest_vid = _vs.get_latest_version_id(Path(root_dir), pid)
        except Exception:
            latest_vid = None
        res = resolve_project_md(
            Path(version_dir), pid,
            project_info=info, root_dir=Path(root_dir), latest_version_id=latest_vid,
        )
        if not res.ok:
            raise RuntimeError(res.error_message(pid))
        return res

    def _make_stage_context(self, job: "AuditJob",
                            stage_override: str | None = None) -> "PipelineStageContext":
        """Построить PipelineStageContext из текущего job для передачи в stage runner-ы.

        stage_override — явная секция лога для ctx.log: в параллельной группе
        (верификатор ∥ нормы) job общий, и job.stage гоняется — без явного
        stage строки одного этапа помечались секцией другого.
        """
        from backend.app.pipeline.context import PipelineStageContext
        pid = job.project_id
        # ctx.project_dir сейчас — это version_dir (V1: root; V2+: _versions/v{N}/).
        # Это нужно, чтобы stage runner-ы видели правильные source-файлы версии.
        _root, version_dir, output_dir = self._resolve_job_paths(job)
        project_dir = version_dir

        async def _log(msg: str, level: str = "info") -> None:
            await self._log(job, msg, level, stage_override=stage_override)

        async def _check_before_launch() -> bool:
            return await self._check_before_launch(job)

        async def _check_pause() -> bool:
            return await self._check_pause(job)

        async def _wait_for_rate_limit(reason: str, cli_output: str) -> bool:
            return await self._wait_for_rate_limit(job, reason, cli_output)

        def _record_cli_usage(cli_result, stage: str, is_retry: bool = False) -> None:
            self._record_cli_usage(job, cli_result, stage, is_retry)

        def _update_pipeline_log(stage_key: str, status: str, **kwargs) -> None:
            self._update_pipeline_log(pid, stage_key, status, **kwargs)

        async def _run_subprocess(*args, **kwargs):
            return await self._run_script_for_job(job, *args, **kwargs)

        project_info = self._load_project_info_for_paths(pid, _root, version_dir)

        async def _stream_findings_events(stage: str) -> None:
            await self._stream_findings_events(job, stage)

        def _reset_job_progress() -> None:
            self._reset_job_progress(job)

        def _refresh_finding_quality() -> None:
            self._refresh_finding_quality(pid)

        def _progress_sync(current: int, total: int) -> None:
            """Синхронный progress callback для block_analysis executor thread."""
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(self._progress(job, current, total), loop)

        def _record_block_analysis_usage(summary: dict) -> None:
            self._record_findings_only_usage(job, summary)

        def _is_cancelled() -> bool:
            return job.status == JobStatus.CANCELLED

        return PipelineStageContext(
            project_dir=project_dir,
            project_id=pid,
            output_dir=output_dir,
            log=_log,
            check_before_launch=_check_before_launch,
            check_pause=_check_pause,
            wait_for_rate_limit=_wait_for_rate_limit,
            record_cli_usage=_record_cli_usage,
            update_pipeline_log=_update_pipeline_log,
            run_subprocess=_run_subprocess,
            project_info=project_info,
            object_id=getattr(job, "object_id", None),
            stream_findings_events=_stream_findings_events,
            reset_job_progress=_reset_job_progress,
            refresh_finding_quality=_refresh_finding_quality,
            progress_sync=_progress_sync,
            record_block_analysis_usage=_record_block_analysis_usage,
            is_cancelled=_is_cancelled,
            version_id=getattr(job, "version_id", None),
            job_id=getattr(job, "job_id", None),
        )

    def _reset_job_progress(self, job: AuditJob):
        """Сбросить прогресс и ETA-данные при переходе между этапами пайплайна."""
        job.progress_current = 0
        job.progress_total = 0
        job.batch_durations = []
        job.batch_started_at = None

    def _backfill_highlight_regions(self, project_id: str):
        """Восстановить highlight_regions в 03_findings.json из 01_blocks_analysis.json.

        При findings_merge LLM иногда теряет highlight_regions из G-замечаний.
        Этот метод сначала подтягивает координаты обратно по
        source_block_ids/related_block_ids, затем (под отдельным флагом)
        запускает детерминированный grounding по текстовому слою PDF.

        Version-aware: output берётся из `_output_dir_for_project`, а source-
        директория активной job — из `_resolve_job_paths`. Иначе V2-primary run
        мог бы прочитать PDF/result.json не той версии.
        """
        from backend.app.pipeline.stages.findings_merge.backfill_highlights import backfill_project
        from backend.app.pipeline.stages.findings_merge.ground_highlights_textlayer import (
            backfill_textlayer_highlights,
        )

        output_dir = self._output_dir_for_project(project_id)
        job = self.active_jobs.get(project_id)
        if job is not None:
            _root, project_dir, _resolved_output = self._resolve_job_paths(job)
        else:
            project_dir = output_dir.parent
        result = backfill_project(project_dir, output_dir=output_dir)
        if result["fixed"] > 0:
            print(f"[{project_id}] highlight_regions restored: {result['fixed']}")
        textlayer = backfill_textlayer_highlights(project_dir, output_dir=output_dir)
        if textlayer.get("enabled"):
            mode = "shadow" if textlayer.get("shadow") else "live"
            print(
                f"[{project_id}] text-layer highlights ({mode}): "
                f"grounded={textlayer.get('grounded', 0)}/{textlayer.get('checked', 0)}, "
                f"written={textlayer.get('fixed', 0)}"
            )

    @staticmethod
    def _attach_stage02_coverage_to_findings(
        project_id: str, output_dir: Optional[Path] = None
    ) -> dict:
        """Attach deterministic Stage 02 coverage warnings to final findings."""
        from backend.app.pipeline.stages.block_analysis.runner import (
            attach_stage02_coverage_to_findings,
        )
        return attach_stage02_coverage_to_findings(project_id, output_dir=output_dir)

    @staticmethod
    def _backfill_text_evidence_in_findings(project_id: str):
        """Backfill text-evidence + sheet в 03_findings.json."""
        from backend.app.pipeline.stages.findings_merge.runner import (
            backfill_text_evidence_in_findings,
        )
        return backfill_text_evidence_in_findings(project_id)

    @staticmethod
    def _refresh_finding_quality(
        project_id: str,
        filename: str = "03_findings.json",
    ) -> dict | None:
        """Refresh deterministic practicality metadata for findings."""
        from backend.app.pipeline.stages.findings_merge.runner import (
            refresh_finding_quality,
        )
        return refresh_finding_quality(project_id, filename)

    @staticmethod
    def _merge_similar_findings(project_id: str) -> dict | None:
        """Объединить похожие замечания в 03_findings.json."""
        from backend.app.pipeline.stages.findings_merge.runner import (
            merge_similar_findings,
        )
        return merge_similar_findings(project_id)

    async def _build_document_graph_v2(self, job: AuditJob):
        """Построить document_graph v2 из *_result.json (Python, без LLM)."""
        pid = job.project_id
        try:
            from backend.app.pipeline.stages.prepare.graph_builder import build_document_graph_v2, generate_locality_debug
            from backend.app.services.storage.storage_write_facade import v2_is_primary

            # Version-aware: V1 = root, V2+ = _versions/v{N}/, projects_v2 = versions/<vid>/.
            _root, project_dir, output_dir = self._resolve_job_paths(job)
            graph_source_dir = project_dir
            result_json_paths = None
            if v2_is_primary():
                try:
                    from backend.app.services.storage.projects_v2_source_resolver import resolve_v2_source_files

                    sources = resolve_v2_source_files(project_dir, pid)
                    if sources.result_json_path is not None:
                        graph_source_dir = sources.result_json_path.parent
                        result_json_paths = [sources.result_json_path]
                except Exception as exc:
                    await self._log(job, f"v2 source resolver для document_graph не сработал: {exc}", "warn")

            graph = build_document_graph_v2(
                graph_source_dir,
                output_dir,
                result_json_paths=result_json_paths,
            )
            if graph:
                debug_path = generate_locality_debug(graph, output_dir)
                self._promote_v2_analysis_artifacts(
                    job,
                    ("document_graph.json", "step1_locality_debug.json"),
                )
                await self._log(
                    job,
                    f"document_graph v{graph['version']}: "
                    f"{graph['total_pages']} стр., "
                    f"{graph['total_text_blocks']} текст., "
                    f"{graph['total_image_blocks']} граф."
                    + (f", debug: {debug_path.name}" if debug_path else ""),
                )
            elif result_json_paths:
                # Текст-слой (*_result.json) НАЙДЕН — граф ОЖИДАЛСЯ, но не
                # построился. Это НЕ чистый скан: без document_graph роутер
                # источника блока / Gemma дадут 0 covered, и весь лист уедет в
                # placeholder ПРИ status=ok — тихий провал вектор-покрытия
                # (residual «0 из вектор-слоя»). Фиксируем как деградацию стадии,
                # чтобы аномалия всплыла в финальной сводке _log_stage_degradations,
                # а не тонула в мягком warn. Без hard-fail: провал может быть
                # транзиентным (resume/retry достроит граф), а жёсткий останов
                # рисковал бы ложными фейлами.
                self._update_pipeline_log(
                    pid, "document_graph", "partial",
                    message="Граф не построен, хотя *_result.json найден — вектор-покрытие будет 0",
                )
                await self._log(
                    job,
                    "document_graph v2 НЕ построен, хотя *_result.json найден — "
                    "вектор-покрытие будет 0 (аномалия сборки графа, НЕ чистый скан)",
                    "error",
                )
            else:
                await self._log(
                    job,
                    "document_graph v2 не построен (*_result.json не найден) — "
                    "используется MD fallback",
                    "warn",
                )
        except ImportError:
            await self._log(
                job, "graph_builder не найден — document_graph v2 недоступен", "warn"
            )
        except Exception as e:
            await self._log(
                job, f"document_graph v2 ошибка: {e}", "warn"
            )


    def _v2_promotion_context(self, job: AuditJob):
        """Resolve v2 promotion target for the current job, or None outside v2-primary."""
        from backend.app.services.storage.storage_write_facade import (
            StorageWriteFacade,
            v2_is_primary,
        )

        if not v2_is_primary():
            return None
        try:
            _doc_dir, _version_dir, output_dir = self._resolve_job_paths(job)
            facade = StorageWriteFacade()
            v2_root = facade.v2_root()
            if v2_root is None:
                return None
            try:
                legacy_root = resolve_project_dir(job.project_id)
            except Exception:
                legacy_root = None
            from backend.app.services.storage.v2_primary_wiring import resolve_v2_target_by_id

            _object_id = getattr(job, "object_id", None)
            target = resolve_v2_target_by_id(
                job.project_id,
                getattr(job, "version_id", None) or "v001",
                v2_root=v2_root,
                object_id=_object_id,
                legacy_project_dir=legacy_root,
            )
            if target is None and _object_id:
                target = resolve_v2_target_by_id(
                    job.project_id,
                    getattr(job, "version_id", None) or "v001",
                    v2_root=v2_root,
                    object_id=None,
                    legacy_project_dir=legacy_root,
                )
                if target is not None:
                    resolved_id = self._v2_object_id_from_doc_dir(target.doc_dir(v2_root))
                    if resolved_id:
                        job.object_id = resolved_id
            if target is None:
                return None
            return facade, target, output_dir
        except Exception as exc:
            print(f"[{job.project_id}] v2 promotion context failed: {exc}")
            return None

    def _promote_v2_analysis_artifacts(self, job: AuditJob, artifact_names) -> dict:
        """Copy selected artifacts from the job run dir into projects_v2 latest.

        No-op outside projects_v2_primary. Fail-soft by design: the stage output
        in runs/<job_id> remains authoritative even if latest promotion fails.
        """
        ctx = self._v2_promotion_context(job)
        if ctx is None:
            return {}
        facade, target, output_dir = ctx
        run_id = getattr(job, "job_id", None)
        results = {}
        for name in artifact_names:
            source = Path(output_dir) / str(name)
            if not source.is_file():
                continue
            try:
                results[str(name)] = facade.save_analysis_artifact(
                    target,
                    str(name),
                    source.read_bytes(),
                    run_id=run_id,
                )
            except Exception as exc:
                print(f"[{job.project_id}] v2 promote {name} failed: {exc}")
        return results

    def _promote_completed_audit_v2(self, job: AuditJob) -> dict:
        """Bulk-promote late audit artifacts from runs/<job_id> to latest."""
        ctx = self._v2_promotion_context(job)
        if ctx is None:
            return {}
        facade, target, output_dir = ctx
        try:
            from backend.app.services.storage.v2_primary_prototype import (
                write_completed_audit_artifacts_v2,
            )

            return write_completed_audit_artifacts_v2(
                facade,
                target,
                output_dir,
                run_id=getattr(job, "job_id", None),
            )
        except Exception as exc:
            print(f"[{job.project_id}] v2 completed audit promotion failed: {exc}")
            return {}

    async def _run_gemma_enrichment_stage(self, job: AuditJob, *, force: bool = False) -> None:
        """Compatibility-named entry point for local block-context preparation."""
        job.stage = AuditStage.BLOCK_CONTEXT
        await self._ensure_stage02_crops(job)
        result = await _run_block_context_stage_fn(self._make_stage_context(job), force=force)
        if result.cancelled:
            job.status = JobStatus.CANCELLED
            return
        if not result.success:
            job.status = JobStatus.FAILED
            job.error_message = result.error
            raise RuntimeError(result.error or f"{GEMMA_STAGE_LABEL}: ошибка")

    async def _run_block_grounding_stage(self, job: AuditJob) -> None:
        """Усиление предобработки: Value Grounding (вектор-сверка значений gemma).

        OFF по умолчанию (BLOCK_VALUE_GROUNDING_ENABLED) → полный no-op. Fail-soft:
        ошибки усиления не роняют основной пайплайн. Запускается после gemma_enrichment,
        до text_analysis (значения уже прочитаны, вектор-слой доступен из result.json).
        """
        from backend.app.core.config import BLOCK_VALUE_GROUNDING_ENABLED
        if not BLOCK_VALUE_GROUNDING_ENABLED:
            return
        try:
            from backend.app.pipeline.stages.block_grounding.runner import (
                run_block_grounding_stage,
            )
            from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
                bind_output_root,
                unbind_output_root,
            )
            ctx = self._make_stage_context(job)
            _token = bind_output_root(ctx.output_dir)
            try:
                await run_block_grounding_stage(ctx)
            finally:
                unbind_output_root(_token)
        except Exception as exc:
            await self._log(job, f"Value Grounding пропущен (soft): {exc}", "warn")

    async def _log_stage_degradations(self, job: AuditJob) -> None:
        """Видимая сводка деградаций при «зелёном» финале аудита.

        Fail-soft стадии (debt_control, decision_carryover, critic-чанки,
        coverage и т.п.) пишут error/partial в pipeline_log, но аудит
        завершается COMPLETED — раньше эксперт узнавал о невыполненном
        переносе вердиктов только заметив пустые строки. Теперь финал явно
        перечисляет стадии с error/partial.
        """
        try:
            _root, _proj, output_dir = self._resolve_job_paths(job)
            log_path = output_dir / "pipeline_log.json"
            if not log_path.exists():
                return
            stages = (json.loads(log_path.read_text(encoding="utf-8"))
                      .get("stages") or {})
            degraded = [
                f"{name}: {info.get('error') or info.get('message') or info.get('status')}"
                for name, info in stages.items()
                if isinstance(info, dict) and info.get("status") in ("error", "partial")
            ]
            if degraded:
                await self._log(
                    job,
                    "⚠ Аудит завершён С ДЕГРАДАЦИЯМИ: " + "; ".join(degraded),
                    "error",
                )
        except Exception:
            pass  # сводка не должна ломать финализацию

    async def _run_debt_control(self, job: AuditJob) -> None:
        """«Контроль долгов»: согласованные замечания прошлой версии не теряются.

        Always-on для V2+ (kill-switch DEBT_CONTROL_ENABLED). Fail-soft. Обёртка
        над migrated_findings_service: дубли обогащаются origin-метой, потерянные
        аудитом «долги» добавляются в 03_findings.json как MIG-замечания, спорные
        уходят в отчёт migrated_findings_report.json. Ставится ПЕРЕД
        decision_carryover: добавленные MIG-замечания тут же получают вердикт
        «согласовано» переносом.
        """
        try:
            from backend.app.pipeline.stages.debt_control.runner import (
                run_debt_control_stage,
            )
            self._reset_job_progress(job)
            job.stage = AuditStage.DEBT_CONTROL
            job.status = JobStatus.RUNNING
            await run_debt_control_stage(self._make_stage_context(job))
            self._promote_v2_analysis_artifacts(
                job, ("03_findings.json", "migrated_findings_report.json",
                      "pipeline_log.json")
            )
        except Exception as exc:
            # Fail-soft сохранён (аудит не валим), но деградация видимая:
            # error в pipeline_log + error-уровень лога (раньше warn терялся
            # в потоке, а исключение ДО runner'а вообще не оставляло следа).
            self._update_pipeline_log(
                job.project_id, "debt_control", "error", error=str(exc)[:200],
            )
            await self._log(
                job, f"Контроль долгов НЕ выполнен (деградация): {exc}", "error",
            )

    async def _run_decision_carryover(self, job: AuditJob) -> None:
        """Перенос вердиктов эксперта из предыдущей проверенной версии (fail-soft).

        Always-on для V2+ (kill-switch DECISION_CARRYOVER_ENABLED). Ошибки НЕ роняют
        аудит. Sonnet сверяет каждое текущее замечание с решёнными замечаниями прошлой
        версии; при подтверждении переносит вердикт в expert_review.json (только там,
        где эксперт ещё не решил) + decisions_log.json и пишет
        _output/decision_carryover_report.json. Сервис внутри синхронный (claude -p),
        поэтому запускается через asyncio.to_thread внутри stage runner-а.
        """
        try:
            from backend.app.pipeline.stages.decision_carryover.runner import (
                run_decision_carryover_stage,
            )
            self._reset_job_progress(job)
            job.stage = AuditStage.DECISION_CARRYOVER
            job.status = JobStatus.RUNNING
            await run_decision_carryover_stage(self._make_stage_context(job))
        except Exception as exc:
            # Симметрично debt_control: fail-soft, но с видимым следом —
            # иначе эксперт молча получал пустые вердикты и решал всё заново.
            self._update_pipeline_log(
                job.project_id, "decision_carryover", "error", error=str(exc)[:200],
            )
            await self._log(
                job, f"Перенос вердиктов НЕ выполнен (деградация): {exc}", "error",
            )

    async def _ensure_stage02_crops(self, job: AuditJob) -> None:
        """Ensure findings-only Stage 01 has its 100 DPI crop index."""
        pid = job.project_id
        # Version-aware: V1 = root, V2+ = _versions/v{N}/.
        _root, project_dir, output_dir = self._resolve_job_paths(job)
        policy = stage02_crop_policy()
        # v2-primary: crop-субпроцесс пишет в output_dir (ему передаётся
        # AUDIT_OUTPUT_DIR = run dir). Проверку строим от того же output_dir, а не
        # через gemma_output_root(project_dir): без активного bind_output_root
        # (например, на пути resume) он падает на latest/_output — мимо run dir,
        # и готовый crop не находится. output_dir корректен и в legacy (= _output).
        index_path = output_dir / STAGE02_BLOCKS_DIRNAME / "index.json"
        blocks_dir = output_dir / STAGE02_BLOCKS_DIRNAME
        stale_existing_dir = (
            not index_path.exists()
            and blocks_dir.exists()
            and any(blocks_dir.glob("block_*.png"))
        )
        needs_crop = (
            force := (
                (index_path.exists() and not _existing_crop_matches_policy(index_path, policy))
                or stale_existing_dir
            )
        ) or not index_path.exists()
        if not needs_crop:
            await self._log(
                job,
                f"Stage 01 crops готовы: _output/{STAGE02_BLOCKS_DIRNAME} "
                f"({_crop_policy_label(policy)})",
            )
            return

        await self._log(
            job,
            f"Stage 01 crop: создаю _output/{STAGE02_BLOCKS_DIRNAME} "
            f"({_crop_policy_label(policy)})",
            "warn" if force else "info",
        )
        exit_code, _, stderr = await self._run_script_for_job(
            job,
            str(BLOCKS_SCRIPT),
            _build_crop_args(
                self._project_path_for_job(job),
                force=force,
                policy=policy,
                output_dir_name=STAGE02_BLOCKS_DIRNAME,
            ),
            on_output=lambda msg: self._log(job, msg),
        )
        if exit_code == 2 and index_path.exists():
            await self._log(
                job,
                "Stage 01 crop частично завершился с ошибками; продолжу с доступными "
                "100 DPI blocks, пропуски попадут в coverage",
                "warn",
            )
            return
        if exit_code != 0:
            raise RuntimeError(f"Stage 01 crop failed: {stderr or f'Exit code {exit_code}'}")
        if not index_path.exists():
            raise RuntimeError(f"Stage 01 crop не создал _output/{STAGE02_BLOCKS_DIRNAME}/index.json")

    async def _run_block_analysis_findings_only(self, job: AuditJob) -> None:
        """Тонкий оркестратор: делегирует в block_analysis/runner.py.

        Оркестраторная логика (prerequisites, job.stage, heartbeat, cleanup)
        остаётся здесь. Бизнес-логика анализа блоков — в runner.
        """
        pid = job.project_id
        # ─── Paid API guard: проверка ДО любого network request Stage 01 ────
        # Stage 01 block analysis идёт в OpenRouter напрямую и
        # тратит реальные деньги. Блокируем только если глобальный kill-switch
        # PAID_API_ENABLED=false или превышен daily limit.
        try:
            from backend.app.services.llm.paid_api_guard import (
                PaidApiBlockedError,
                PaidApiContext,
                assert_paid_api_allowed,
            )
            from backend.app.core.config import get_stage_model
            stage01_model = get_stage_model("block_analysis") or "openai/gpt-5.4"
            assert_paid_api_allowed(PaidApiContext(
                source="manager.stage01.orchestrator",
                model=stage01_model,
                project_id=pid,
                version_id=getattr(job, "version_id", None) or "",
                stage="block_analysis",
                job_id=getattr(job, "job_id", "") or "",
            ))
        except PaidApiBlockedError as _e:
            await self._log(
                job,
                f"Stage 01 заблокирован paid_api_guard: {_e.reason}. "
                f"Включите PAID_API_ENABLED=true либо проверьте daily limit.",
                "error",
            )
            job.status = JobStatus.FAILED
            job.error_message = f"paid_api_blocked: {_e.reason}"
            return
        # Version-aware: V1 = root, V2+ = _versions/v{N}/.
        _root, project_dir, _output = self._resolve_job_paths(job)
        await self._ensure_stage02_crops(job)
        from backend.app.pipeline.stages.block_context.contract import (
            validate_block_context_summary,
        )
        if not validate_block_context_summary(_output).get("valid"):
            await self._run_gemma_enrichment_stage(job, force=True)
            if job.status in {JobStatus.CANCELLED, JobStatus.FAILED}:
                return

        self._reset_job_progress(job)
        job.stage = AuditStage.BLOCK_ANALYSIS
        job.status = JobStatus.RUNNING
        job.progress_total = 0  # будет обновлён check_prerequisites внутри runner
        await self._start_heartbeat(job)

        ctx = self._make_stage_context(job)
        result = await _run_block_analysis_findings_only_stage(ctx)

        if result.cancelled:
            job.status = JobStatus.CANCELLED
            return

        if not result.success:
            job.status = JobStatus.FAILED
            job.error_message = result.error
            return

        job.status = JobStatus.COMPLETED
        self._promote_v2_analysis_artifacts(
            job,
            (
                BLOCKS_ANALYSIS_FILENAME,
                "block_analysis_summary.json",
                RUNTIME_BATCHES_FILE,
                "block_batches.json",
                "pipeline_log.json",
            ),
        )

        # Step 9/10 dual-write: ранний снимок после block_analysis (covers
        # standalone block_analysis-only прогон). ПОЛНЫЙ снимок после всего
        # конвейера делает _run_batch_queue по завершении (late artifacts).
        self._shadow_mirror_completed_audit(job.project_id, job)

    def _shadow_mirror_completed_audit(self, project_id: str, job=None) -> None:
        """Зеркалировать legacy-проект в projects_v2 после успешного этапа/аудита.

        no-op в legacy-режиме (default), fail-soft — НИКОГДА не влияет на статус
        аудита. Вызывается из двух точек: (1) после block_analysis (ранний снимок
        для standalone-прогона) и (2) из _run_batch_queue после ЗАВЕРШЕНИЯ всего
        конвейера, где legacy _output уже содержит все late-stage artifacts
        (03_findings/optimization/нормы) и финальный pipeline_log. Зеркалирование
        идемпотентно (migrate_project обновляет latest-снимок), поэтому повторный
        вызов после полного аудита перезаписывает неполный ранний снимок.
        """
        try:
            from backend.app.services.storage import storage_write_facade as _swf
            if _swf.v2_is_primary():
                if job is not None:
                    self._promote_completed_audit_v2(job)
                return
            run_id = getattr(job, "job_id", None) if job is not None else None
            _swf.shadow_mirror_project_id_safe(project_id, run_id=run_id)
        except Exception as e:
            # fail-soft: legacy уже записан и авторитетен; v2 — best-effort тень
            print(f"[{project_id}] shadow_mirror_completed_audit failed: {e}")

    def _record_findings_only_usage(self, job: AuditJob, summary: dict) -> None:
        """Учесть стоимость Stage 01 block analysis в usage tracker.

        Для OpenRouter-моделей (GPT/Gemini) — реальная плата → cost_usd.
        Для Claude CLI (sonnet/opus, без слэша) — подписка → cost_usd=0, notional=cost.
        """
        totals = summary.get("totals", {}) or {}
        model = summary.get("model", "") or ""
        cost = float(totals.get("estimated_cost_usd_total", 0.0) or 0.0)
        input_tokens = int(totals.get("input_tokens", 0) or 0)
        output_tokens = int(totals.get("output_tokens", 0) or 0)
        api_calls = int(
            summary.get("api_calls_total")
            or summary.get("blocks_ok", 0)
            or 1
        )
        duration_ms = int(float(summary.get("wall_clock_s", 0.0) or 0.0) * 1000)

        if input_tokens <= 0 and output_tokens <= 0 and cost <= 0:
            return

        is_cli = bool(model) and model.startswith("claude-") and "/" not in model
        actual_cost = 0.0 if is_cli else cost
        notional_cost = cost if is_cli else 0.0

        record = UsageRecord(
            timestamp=datetime.now().isoformat(),
            session_id=None,
            project_id=job.project_id,
            stage="block_analysis",
            model=model,
            cost_usd=actual_cost,
            cost_usd_notional=notional_cost,
            duration_ms=duration_ms,
            duration_api_ms=duration_ms,
            num_turns=api_calls,
            api_calls=api_calls,
            is_retry=False,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        usage_tracker.record_usage(record)
        # Stage 01 findings-only ходит в OpenRouter напрямую, в обход
        # llm_runner.run_llm — поэтому учёт paid_cost здесь обязателен.
        # Единый helper record_paid гарантирует, что paid_cost.json и
        # paid_cost_events.jsonl увеличиваются одной операцией (структурный
        # инвариант, заменяет два независимых вызова — root cause 9 vs 15).
        if actual_cost > 0:
            try:
                paid_cost_tracker.record_paid(
                    actual_cost,
                    model=model,
                    project_id=job.project_id,
                    stage="block_analysis",
                    source="manager.stage02",
                    job_id=getattr(job, "job_id", "") or "",
                    version_id=getattr(job, "version_id", None) or "",
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                )
            except Exception as _pae_err:
                # logger в manager.py не настроен — пишем в stderr, чтобы не
                # уронить пайплайн из-за ошибки журнала.
                print(
                    f"[paid_cost_tracker] record_paid failed (stage02): {_pae_err}",
                    flush=True,
                )
        job.cost_usd += actual_cost
        job.cli_calls += api_calls
        self._enrich_pipeline_log(
            job.project_id, "block_analysis", model, input_tokens, output_tokens
        )

    def _output_dir_for_project(self, project_id: str) -> Path:
        """Version-aware output_dir: если для project_id в active_jobs есть job
        с version_id, использует его; иначе fallback на root project_dir/_output.

        Защита от V2 audit, который случайно очищает V1 `_output/` через
        version-unaware helper'ы (_clean_stage_files / _backup_findings_before_restart).
        """
        job = self.active_jobs.get(project_id)
        if job is not None:
            _root, _vdir, output_dir = self._resolve_job_paths(job)
            return output_dir
        return resolve_project_dir(project_id) / "_output"

    def _clean_stage_files(self, project_id: str, files: list[str]):
        """Удалить устаревшие JSON-файлы этапов перед перезапуском."""
        output_dir = self._output_dir_for_project(project_id)
        for filename in files:
            if "*" in filename:
                # glob-шаблон (например tile_batch_*.json)
                for path in output_dir.glob(filename):
                    path.unlink()
                    print(f"[{project_id}:clean] Удалён {path.name}")
            else:
                path = output_dir / filename
                if path.exists():
                    path.unlink()
                    print(f"[{project_id}:clean] Удалён {filename}")

    def _backup_findings_before_restart(self, project_id: str):
        """Сохранить 03_findings.json как _pre_restart бэкап перед полной очисткой."""
        import shutil
        output_dir = self._output_dir_for_project(project_id)
        findings_path = output_dir / "03_findings.json"
        if findings_path.exists():
            backup_path = output_dir / "03_findings_pre_restart.json"
            shutil.copy2(findings_path, backup_path)
            print(f"[{project_id}:clean] Бэкап findings → 03_findings_pre_restart.json")
        # Слепок решённых вердиктов ПЕРЕД удалением findings: findings_merge
        # перенумерует F-ID, слепок позволит перепривязать вердикты после merge
        # (verdict_preservation, fail-soft — ошибки не роняют пайплайн).
        try:
            from backend.app.services.findings import verdict_preservation as _vp
            job = self.active_jobs.get(project_id)
            _res = _vp.snapshot_for_project(
                project_id, version_id=getattr(job, "version_id", None),
            )
            if _res.get("status") == "ok":
                print(
                    f"[{project_id}:clean] Слепок вердиктов: {_res.get('items')} шт. "
                    "→ 04_review/verdict_preservation_snapshot.json"
                )
        except Exception as _vp_err:  # noqa: BLE001 — fail-soft, но наблюдаемо
            print(f"[{project_id}:clean] verdict_preservation snapshot failed: {_vp_err}")

    async def _run_verdict_rehydration(
        self, job: "AuditJob", item_types: tuple = ("finding",),
    ) -> None:
        """Перепривязать вердикты эксперта к новым ID после регенерации.

        Вызывается дважды: после findings_merge для ("finding",) и после
        post-findings блока (этап оптимизации) для ("optimization",).
        Fail-soft: любая ошибка логируется и не влияет на пайплайн. Первый
        вызов идёт ДО decision_carryover — carryover заполняет только пустые
        слоты и восстановленные здесь решения не перезапишет.
        """
        try:
            from backend.app.services.findings import verdict_preservation as _vp
            if not _vp.is_enabled():
                return
            res = await asyncio.to_thread(
                _vp.rehydrate_for_project,
                job.project_id,
                version_id=getattr(job, "version_id", None),
                item_types=item_types,
            )
            status = res.get("status")
            label = "/".join(item_types)
            if status == "ok":
                await self._log(
                    job,
                    f"Вердикты после переаудита ({label}): восстановлено "
                    f"{res.get('restored', 0)} из {res.get('snapshot_items', 0)} "
                    f"(exact {res.get('restored_exact', 0)}, fuzzy {res.get('restored_fuzzy', 0)}; "
                    f"не сматчено {res.get('unmatched', 0)}, неоднозначно {res.get('ambiguous', 0)}, "
                    f"снято устаревших {res.get('stale_removed', 0)})",
                    "info",
                )
            elif status not in {"disabled", "no_snapshot", "already_applied", "no_decisions"}:
                await self._log(
                    job, f"Восстановление вердиктов ({label}): {status}", "info",
                )
        except Exception as exc:  # noqa: BLE001 — fail-soft
            try:
                await self._log(
                    job, f"Восстановление вердиктов не выполнено: {exc}", "warn",
                )
            except Exception:
                pass

    @staticmethod
    def _codex_models_enabled() -> bool:
        """True когда текущий stage model config содержит Codex exec."""
        try:
            from backend.app.core.config import STAGE_MODEL_CONFIG, is_codex_model
            return any(is_codex_model(model) for model in STAGE_MODEL_CONFIG.values())
        except Exception:
            return False

    @staticmethod
    def _safe_backup_name(value: str) -> str:
        safe = []
        for ch in value:
            if ch.isalnum() or ch in ("-", "_", "."):
                safe.append(ch)
            else:
                safe.append("_")
        return "".join(safe).strip("_")[:120] or "project"

    def _snapshot_output_before_codex_run(self, job: AuditJob, action: str) -> Path | None:
        """Copy current result artifacts before a Codex experiment can overwrite them.

        This intentionally snapshots JSON/JSONL/MD/XLSX top-level artifacts and
        audit_trail only, not cropped block PNG folders, to keep backups compact
        while preserving Claude analysis results for restore/comparison.
        """
        if not self._codex_models_enabled():
            return None
        try:
            _root, _version_dir, _run_output = self._resolve_job_paths(job)
        except Exception:
            _version_dir = None
            _run_output = self._output_dir_for_project(job.project_id)
        # На projects_v2 живые результаты (findings / expert_review / *.xlsx)
        # лежат в 03_analysis/latest, а output_dir из _resolve_job_paths — это
        # per-run 03_analysis/runs/<run_id>, который на dispatch ещё ПУСТОЙ:
        # снимок молча бэкапил пустоту и не защищал данные. Для снимка берём
        # latest, если он существует (v2), иначе per-run / legacy _output.
        output_dir = _run_output
        if _version_dir is not None:
            _latest = _version_dir / "03_analysis" / "latest"
            if _latest.is_dir():
                output_dir = _latest
        if not output_dir.exists():
            return None

        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        version_id = getattr(job, "version_id", None) or "v1"
        project_key = self._safe_backup_name(job.project_id)
        backup_dir = (
            BASE_DIR / "comparison" / "classic_codex_ab" / "backups" /
            project_key / f"{version_id}_{action}_{timestamp}"
        )
        backup_dir.mkdir(parents=True, exist_ok=True)

        copied = []
        suffixes = {".json", ".jsonl", ".md", ".xlsx"}
        for src in sorted(output_dir.iterdir()):
            if src.is_file() and src.suffix.lower() in suffixes:
                shutil.copy2(src, backup_dir / src.name)
                copied.append(src.name)
        audit_trail = output_dir / "audit_trail"
        if audit_trail.is_dir():
            shutil.copytree(audit_trail, backup_dir / "audit_trail", dirs_exist_ok=True)
            copied.append("audit_trail/")

        marker = {
            "type": "pre_codex_snapshot",
            "project_id": job.project_id,
            "version_id": version_id,
            "action": action,
            "source_output_dir": str(output_dir),
            "created_at": datetime.now().isoformat(),
            "copied": copied,
            "restore_note": "To restore Claude results, copy the needed files from this backup directory back to source_output_dir.",
        }
        (backup_dir / "_snapshot_meta.json").write_text(
            json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        print(f"[{job.project_id}:codex] Snapshot текущих результатов → {backup_dir}")
        return backup_dir

    # ─── Валидация JSON после записи LLM ───

    @staticmethod
    def _validate_and_repair_json(file_path: Path) -> tuple[bool, str]:
        """Проверить JSON-файл и попытаться починить, если невалиден."""
        from backend.app.pipeline.stages.block_analysis.runner import (
            validate_and_repair_json,
        )
        return validate_and_repair_json(file_path)
    # ─── Логирование (делегирование в audit_logger) ───

    def _update_pipeline_log(self, project_id: str, stage_key: str, status: str,
                              message: str = "", error: str = "", detail: dict | None = None):
        """Записать статус этапа в pipeline_log.json и отправить WS-обновление."""
        audit_logger.update_pipeline_log(project_id, stage_key, status, message, error, detail)

    async def _log(self, job: AuditJob, message: str, level: str = "info",
                   stage_override: str | None = None):
        """Записать лог в консоль, файл и WebSocket.

        Перехватывает финальный JSON-ответ Claude CLI ({"type":"result",...})
        и превращает его в красивую cli_summary карточку вместо сырого JSON-мусора.
        Промежуточные stream-json сообщения (type=assistant/user/system) подавляются.
        Остальной технический мусор (события Codex CLI, построчные JSON/diff-
        фрагменты записываемых артефактов, баннер codex exec) гуманизируется
        или подавляется в log_humanizer — в лог для человека он не попадает.

        stage_override — явная секция лога для задач параллельной группы,
        где job общий и job.stage гоняется (см. audit_logger.log_to_project).
        """
        # Быстрый фильтр — обычные строки идут как есть. Префикс провайдера
        # ('[OPT claude] ' из ансамбля) срываем ДО проверки: иначе финальный
        # {"type":"result",...} Claude-ноги не превращался в cli_summary.
        _, body = split_known_prefix((message or "").lstrip())
        stripped = body.lstrip()
        if stripped.startswith('{"type":"'):
            try:
                payload = json.loads(stripped)
            except (json.JSONDecodeError, ValueError):
                payload = None
            if isinstance(payload, dict) and "type" in payload:
                msg_type = payload.get("type")
                if msg_type == "result":
                    await self._emit_cli_summary(job, payload, stage_override=stage_override)
                    return
                # Прочие технические типы stream-json не захламляют лог
                if msg_type in ("assistant", "user", "system", "tool_use", "tool_result"):
                    return

        humanized = humanize_log_line(message, level)
        if humanized.text is None:
            return

        await audit_logger.log_to_project(
            job, humanized.text, humanized.level, stage_override=stage_override,
        )

    async def _emit_cli_summary(self, job: AuditJob, payload: dict,
                                stage_override: str | None = None):
        """
        Преобразовать {"type":"result",...} JSON от Claude CLI в:
          1) короткую строку в persisted-лог (для истории),
          2) структурированное cli_summary WS-сообщение для красивой карточки.
        """
        pid = job.project_id
        stage_val = stage_override or (job.stage.value if job.stage else "")

        result_md = payload.get("result") or ""
        if not isinstance(result_md, str):
            result_md = str(result_md)

        is_error = bool(payload.get("is_error", False))
        duration_ms = payload.get("duration_ms", 0) or 0
        duration_sec = duration_ms / 1000.0 if duration_ms else 0
        cost_usd = payload.get("total_cost_usd", 0) or 0

        usage = payload.get("usage", {}) or {}
        input_tokens = usage.get("input_tokens", 0) or 0
        output_tokens = usage.get("output_tokens", 0) or 0
        cache_read = usage.get("cache_read_input_tokens", 0) or 0
        cache_creation = usage.get("cache_creation_input_tokens", 0) or 0

        # Извлекаем имя модели из modelUsage (берём первую — обычно там одна)
        model = ""
        model_usage = payload.get("modelUsage") or {}
        if isinstance(model_usage, dict) and model_usage:
            model = next(iter(model_usage.keys()), "")

        # 1. Структурированная запись в persisted log (для восстановления после refresh)
        short_duration = f"{int(duration_sec // 60)}м {int(duration_sec % 60)}с" if duration_sec >= 60 else f"{duration_sec:.1f}с"
        short_msg = (
            f"✓ Claude завершил: {short_duration}, ${cost_usd:.2f}, "
            f"{output_tokens} out / {cache_creation} cache_new / {cache_read} cache_hit"
        )
        if is_error:
            short_msg = "✗ Claude завершил с ошибкой — см. карточку сводки"

        level = "error" if is_error else "info"
        # Пишем структурированную запись в audit_log.jsonl —
        # loadProjectLog восстановит красивую карточку при refresh
        audit_logger.persist_log(
            pid,
            short_msg,
            level,
            stage_val,
            extras={
                "kind": "cli_summary",
                "result_md": result_md,
                "duration_sec": round(duration_sec, 1),
                "cost_usd": round(cost_usd, 4),
                "output_tokens": output_tokens,
                "cache_read": cache_read,
                "cache_creation": cache_creation,
                "model": model,
                "is_error": is_error,
            },
        )

        # 2. Красивая карточка через отдельный WS-тип
        try:
            await ws_manager.broadcast_to_project(
                pid,
                WSMessage.cli_summary(
                    project=pid,
                    stage=stage_val,
                    result_md=result_md,
                    duration_sec=duration_sec,
                    cost_usd=cost_usd,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cache_read=cache_read,
                    cache_creation=cache_creation,
                    model=model,
                    is_error=is_error,
                ),
            )
        except Exception as e:
            print(f"[{pid}] _emit_cli_summary failed: {e}")

    async def _stream_findings_events(self, job: AuditJob, stage: str):
        """
        Публикует структурированные события в WebSocket для «размышления модели».

        stage:
          - "merge"     — читает 03_findings.json → finding_added[] (по одному, с паузой)
          - "critic"    — читает 03_findings_review.json → finding_verdict[] (с паузой)
          - "corrector" — только finding_stage("corrector")
          - "done"      — финальный finding_stage("done") + final_count из 03_findings.json

        Все данные берутся из уже готовых JSON-файлов, LLM не вовлекается.
        Ошибки чтения подавляются — это «косметический» стрим, он не должен ломать конвейер.
        """
        pid = job.project_id
        try:
            # Version-aware: V1 = root/_output, V2+ = _versions/v{N}/_output.
            _root, _project_dir, output_dir = self._resolve_job_paths(job)

            if stage == "merge":
                findings_path = output_dir / "03_findings.json"
                if not findings_path.exists():
                    return
                try:
                    data = json.loads(findings_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    return
                findings = data.get("findings", []) or []
                await ws_manager.broadcast_to_project(
                    pid, WSMessage.finding_stage(pid, "merge", {"total": len(findings)}),
                )
                for f in findings:
                    if job.status == JobStatus.CANCELLED:
                        return
                    await ws_manager.broadcast_to_project(
                        pid, WSMessage.finding_added(pid, f),
                    )
                    await asyncio.sleep(0.15)
                return

            if stage == "critic":
                review_path = output_dir / "03_findings_review.json"
                if not review_path.exists():
                    return
                try:
                    data = json.loads(review_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    return
                reviews = data.get("reviews", []) or []
                await ws_manager.broadcast_to_project(
                    pid, WSMessage.finding_stage(pid, "critic", {"total": len(reviews)}),
                )
                for r in reviews:
                    if job.status == JobStatus.CANCELLED:
                        return
                    fid = r.get("finding_id") or r.get("id", "")
                    if not fid:
                        continue
                    await ws_manager.broadcast_to_project(
                        pid,
                        WSMessage.finding_verdict(
                            pid,
                            finding_id=fid,
                            verdict=r.get("verdict", "pass"),
                            details=r.get("details", "") or "",
                            suggested_action=r.get("suggested_action"),
                        ),
                    )
                    await asyncio.sleep(0.2)
                return

            if stage == "corrector":
                await ws_manager.broadcast_to_project(
                    pid, WSMessage.finding_stage(pid, "corrector"),
                )
                return

            if stage == "done":
                final_count = 0
                findings_path = output_dir / "03_findings.json"
                if findings_path.exists():
                    try:
                        data = json.loads(findings_path.read_text(encoding="utf-8"))
                        final_count = len(data.get("findings", []) or [])
                    except (json.JSONDecodeError, OSError):
                        pass
                await ws_manager.broadcast_to_project(
                    pid, WSMessage.finding_stage(pid, "done", {"final_count": final_count}),
                )
                return
        except Exception as e:
            # Никогда не ломаем конвейер из-за косметического стрима
            print(f"[{pid}] _stream_findings_events({stage}) failed: {e}")

    async def _progress(self, job: AuditJob, current: int, total: int):
        """Отправить обновление прогресса."""
        await audit_logger.send_progress(job, current, total)

    # ─── Heartbeat ─────────────────────────────────────────────
    async def _start_heartbeat(self, job: AuditJob):
        """Запустить heartbeat-цикл для задачи."""
        self._stop_heartbeat(job.project_id)
        task = asyncio.create_task(self._heartbeat_loop(job))
        self._heartbeat_tasks[job.project_id] = task

    def _stop_heartbeat(self, project_id: str):
        """Остановить heartbeat-цикл."""
        task = self._heartbeat_tasks.pop(project_id, None)
        if task and not task.done():
            task.cancel()

    async def _heartbeat_loop(self, job: AuditJob):
        """Отправлять heartbeat каждые 15 секунд.

        Устойчивость: исключение В ОДНОЙ итерации (сбой broadcast / парсинга
        времени / расчёта ETA) логируется и НЕ убивает цикл — следующий тик
        продолжается. Раньше `except Exception` оборачивал весь `while` →
        единичный сбой молча гасил heartbeat, джоба «замолкала», и
        cleanup_zombies через ZOMBIE_TIMEOUT_SEC ложно признавал живой аудит
        зомби (инцидент 10.06.2026). last_heartbeat обновляется ПЕРВЫМ — даже
        если тело тика частично упадёт, маркер живости свежий.
        """
        try:
            while True:
                await asyncio.sleep(15)
                if job.status != JobStatus.RUNNING:
                    break
                try:
                    now = datetime.now()
                    # Обновляем маркер живости ПЕРВЫМ — чтобы даже при сбое
                    # broadcast/ETA джоба не выглядела «замолчавшей» для зомби-чистки.
                    job.last_heartbeat = now.isoformat()

                    # Вычислить elapsed (чистое время без пауз на rate limit)
                    ref_time = job.batch_started_at or job.started_at
                    if ref_time:
                        started = datetime.fromisoformat(ref_time)
                        elapsed_sec = (now - started).total_seconds() - job.pause_total_sec
                        elapsed_sec = max(0, elapsed_sec)
                    else:
                        elapsed_sec = 0

                    # Вычислить ETA
                    eta_sec = self._calculate_eta(job)

                    # Получить текущие счётчики usage
                    try:
                        counters = usage_tracker.get_counters()
                        tokens_data = counters.model_dump()
                    except Exception:
                        tokens_data = None

                    await ws_manager.broadcast_to_project(
                        job.project_id,
                        WSMessage.heartbeat(
                            project=job.project_id,
                            stage=job.stage.value,
                            elapsed_sec=elapsed_sec,
                            process_alive=True,
                            batch_current=job.progress_current,
                            batch_total=job.progress_total,
                            eta_sec=eta_sec,
                            tokens=tokens_data,
                        ),
                    )
                except asyncio.CancelledError:
                    raise  # отмена task'а — выходим штатно (внешний handler)
                except Exception as e:
                    # Один сбойный тик не должен гасить heartbeat. Логируем и
                    # продолжаем — иначе джоба «замолкнет» и попадёт в зомби-чистку.
                    print(f"[heartbeat] {job.project_id}: сбой итерации (продолжаю): {e}")
                    continue
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # Совсем неожиданный сбой вне тела тика — логируем (раньше глоталось молча).
            print(f"[heartbeat] {job.project_id}: цикл остановлен из-за ошибки: {e}")

    def _calculate_eta(self, job: AuditJob) -> Optional[float]:
        """Рассчитать ETA на основе среднего времени пакетов."""
        if not job.batch_durations or job.progress_total <= 0:
            return None
        avg_duration = sum(job.batch_durations) / len(job.batch_durations)
        remaining = job.progress_total - job.progress_current
        if remaining <= 0:
            return 0
        return avg_duration * remaining

    # ─── Определение точки возобновления ───

    def detect_resume_stage(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
    ) -> dict:
        """Делегирует в resume_detector.detect_resume_stage() для нужной версии."""
        return _detect_resume_stage(project_id, version_id=version_id)

    @staticmethod
    def _normalize_ocr_stage(stage: str) -> str:
        aliases = {
            "crop_blocks": "prepare",
            "block_context": "gemma_enrichment",
            "blocks_analysis": "block_analysis",
            "tile_audit": "block_analysis",
            "findings": "findings_merge",
            "main_audit": "findings_merge",
            "norms_verified": "norm_verify",
        }
        normalized = aliases.get(stage, stage)
        valid_stages = {
            "prepare", "gemma_enrichment", "text_analysis", "block_analysis",
            "findings_merge", "findings_review", "norm_verify",
            "optimization", "optimization_review", "debt_control",
            "decision_carryover", "excel",
        }
        if normalized not in valid_stages:
            raise RuntimeError(f"Неизвестный этап: {stage}")
        return normalized

    def _validate_start_from_stage_now(
        self, project_id: str, stage: str, *, version_id: Optional[str] = None,
    ) -> str:
        """Fail fast when a manual start/retry would bypass mandatory stages.

        Version-aware (reserc.md #4): валидируем против _output АКТИВНОЙ версии,
        а не корня проекта — иначе V2-retry проверялся против stale V1-состояния
        (ложные прохождения/блокировки gemma-гейта). version_id=None → latest.
        """
        normalized = self._normalize_ocr_stage(stage)
        self._assert_stage_model_config_ready()
        from backend.app.services.common import version_service
        try:
            ctx = version_service.resolve_project_version_context(project_id, version_id)
            project_dir = ctx["version_dir"]
            output_dir = ctx["output_dir"]
        except (version_service.VersionNotFoundError, FileNotFoundError):
            project_dir = resolve_project_dir(project_id)
            output_dir = project_dir / "_output"
        project_info = self._load_project_info_for_paths(project_id, project_dir, project_dir)
        self._seed_latest_gemma_artifacts_from_recent_run(project_dir, output_dir, project_info)
        gemma_state = evaluate_gemma_enrichment(project_dir, project_info)

        # evaluate_gemma_enrichment смотрит на legacy-индекс blocks_gemma_100/index.json.
        # Вектор-конвейер (block_context) его не создаёт — контекст блоков собирается из
        # вектор-слоя в канонический block_context_summary.json, а Gemma OCR идёт «всухую».
        # Поэтому у полностью готового вектор-проекта gemma_state=missing_blocks, хотя
        # блоки на месте. Рантайм-путь (_assert_gemma_ready_for_stage, resume) уже гейтит
        # по block_context_summary.json — здесь зеркалим ТОТ ЖЕ контракт, чтобы ручной
        # start-from-stage / add-retry не расходились с resume. Полный аудит (/batch) этот
        # валидатор не вызывает и строит контекст сам, так что его защита не ослабляется;
        # ниже остаются файловые проверки 02_text/01_blocks/03_findings.
        from backend.app.pipeline.stages.block_context.contract import (
            validate_block_context_summary,
        )
        block_context_ready = bool(
            validate_block_context_summary(output_dir).get("valid")
        )

        if normalized == "gemma_enrichment":
            if gemma_state.get("status") in {"missing_md", "missing_blocks"}:
                raise RuntimeError(gemma_gate_error(gemma_state, "gemma_enrichment"))
            return normalized

        if normalized in {
            "text_analysis", "block_analysis", "findings_merge",
            "findings_review", "norm_verify", "decision_carryover", "excel",
        }:
            if gemma_state.get("status") == "missing_md":
                raise RuntimeError(gemma_gate_error(gemma_state, normalized))
            if gemma_state.get("status") == "missing_blocks" and not block_context_ready:
                raise RuntimeError(gemma_gate_error(gemma_state, normalized))

        # Первый LLM-этап может встать в очередь при неполной Gemma: _run_resumed_pipeline()
        # сначала догонит gemma_enrichment. Остальные этапы — нет. Первый LLM-этап зависит от
        # порядка: text_analysis (старый порядок) или block_analysis (новый порядок block→text).
        blocks_before_text = self._blocks_before_text_enabled()
        _first_llm_stage = "block_analysis" if blocks_before_text else "text_analysis"
        strict_gemma = {"findings_merge", "findings_review", "norm_verify", "debt_control", "decision_carryover", "excel"} | (
            {"text_analysis", "block_analysis"} - {_first_llm_stage}
        )
        if normalized in strict_gemma and not gemma_state.get("ready") and not block_context_ready:
            raise RuntimeError(gemma_gate_error(gemma_state, normalized))

        # Требование 01 (текст) — у этапов ниже текста по потоку.
        needs_text = {"findings_merge", "findings_review", "norm_verify", "debt_control", "decision_carryover", "excel"}
        if not blocks_before_text:
            needs_text = needs_text | {"block_analysis"}
        if normalized in needs_text and not resolve_existing(output_dir, TEXT_ANALYSIS_FILENAME).exists():
            raise RuntimeError(
                f"Нельзя запускать {normalized}: 02_text_analysis.json отсутствует. "
                "Сначала выполните text_analysis."
            )

        # Требование 02 (блоки) — у findings_merge+ и (в новом порядке) у text_analysis.
        needs_blocks = {"findings_merge", "findings_review", "norm_verify", "debt_control", "decision_carryover", "excel"}
        if blocks_before_text:
            needs_blocks = needs_blocks | {"text_analysis"}
        if normalized in needs_blocks and not resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME).exists():
            raise RuntimeError(
                f"Нельзя запускать {normalized}: 01_blocks_analysis.json отсутствует. "
                "Сначала выполните block_analysis."
            )

        if normalized in {"findings_review", "norm_verify", "optimization", "optimization_review", "debt_control", "decision_carryover", "excel"}:
            if not (output_dir / "03_findings.json").exists():
                raise RuntimeError(
                    "Нельзя запускать этот этап: 03_findings.json отсутствует. "
                    "Сначала выполните findings_merge."
                )

        return normalized

    @staticmethod
    def _gemma_state_for_output_root(
        project_dir: Path,
        project_info: dict,
        output_dir: Path,
    ) -> dict:
        from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
            bind_output_root,
            unbind_output_root,
        )

        token = bind_output_root(output_dir)
        try:
            return evaluate_gemma_enrichment(project_dir, project_info)
        finally:
            unbind_output_root(token)

    def _seed_latest_gemma_artifacts_from_recent_run(
        self,
        project_dir: Path,
        output_dir: Path,
        project_info: dict,
    ) -> int:
        """Recover light Gemma prereq files in latest from the newest valid run.

        Failed v2-primary retries can leave `03_analysis/latest` with Stage 02
        JSONs but without Gemma index/summary, while the actual run directory
        already has them. Manual start-from-stage validates against latest, so
        copy only missing lightweight JSON/index files from a run that still
        passes the Gemma gate for the current MD.
        """
        project_dir = Path(project_dir)
        output_dir = Path(output_dir)
        runs_dir = project_dir / "03_analysis" / "runs"
        if not runs_dir.is_dir():
            return 0

        from backend.app.pipeline.stages.block_context.contract import (
            resolve_blocks_index,
            validate_block_context_summary,
        )
        need_context_index = not resolve_blocks_index(output_dir).is_file()
        need_summary = not validate_block_context_summary(output_dir).get("valid")
        need_stage02 = not resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME).is_file()
        if not (need_context_index or need_summary or need_stage02):
            return 0

        try:
            candidates = sorted(
                [p for p in runs_dir.iterdir() if p.is_dir()],
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
        except OSError:
            return 0

        output_resolved = None
        try:
            output_resolved = output_dir.resolve()
        except OSError:
            pass

        top_level_names = (
            *BLOCK_CONTEXT_SUMMARY_ALL_NAMES,
            "block_grounding_summary.json",
            "document_graph.json",
            *BLOCKS_ANALYSIS_ALL_NAMES,
            *BLOCKS_FOR_TEXT_ALL_NAMES,
            "block_analysis_summary.json",
            RUNTIME_BATCHES_FILE,
            "block_batches.json",
            "step1_locality_debug.json",
        )
        index_dirs = (
            STAGE02_BLOCKS_DIRNAME,
            GEMMA_BLOCKS_DIRNAME,
            "blocks",
        )

        for candidate in candidates:
            try:
                if output_resolved is not None and candidate.resolve() == output_resolved:
                    continue
            except OSError:
                pass
            if not resolve_blocks_index(candidate).is_file():
                continue
            if not validate_block_context_summary(candidate).get("valid"):
                continue

            copied = 0
            output_dir.mkdir(parents=True, exist_ok=True)
            for name in top_level_names:
                src = candidate / name
                dst = output_dir / name
                if src.is_file() and not dst.exists():
                    try:
                        shutil.copy2(src, dst)
                        copied += 1
                    except OSError:
                        pass
            for dirname in index_dirs:
                src = candidate / dirname / "index.json"
                dst = output_dir / dirname / "index.json"
                if src.is_file() and not dst.exists():
                    try:
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(src, dst)
                        copied += 1
                    except OSError:
                        pass
            if copied:
                print(
                    f"[resume] Восстановлены prereq-артефакты Gemma: "
                    f"{copied} файлов из {candidate} -> {output_dir}"
                )
            return copied

        return 0

    @staticmethod
    def _assert_stage_model_config_ready() -> None:
        rejected = validate_current_stage_model_config()
        if not rejected:
            return
        details = "; ".join(f"{stage}: {reason}" for stage, reason in rejected.items())
        raise RuntimeError(
            "Некорректная конфигурация моделей этапов. "
            f"Исправьте Stage Models перед запуском аудита: {details}"
        )

    async def _ensure_gemma_ready_or_run(
        self,
        job: AuditJob,
        project_info: dict,
        target_stage: str,
    ) -> dict:
        del project_info
        _root, _project_dir, output_dir = self._resolve_job_paths(job)
        from backend.app.pipeline.stages.block_context.contract import (
            validate_block_context_summary,
        )
        state = validate_block_context_summary(output_dir)
        if state.get("valid"):
            return {"ready": True, **state}
        await self._log(
            job,
            f"{target_stage}: контекст блоков не готов — выполняю локальную подготовку",
            "warn",
        )
        await self._run_gemma_enrichment_stage(job, force=True)
        state = validate_block_context_summary(output_dir, canonical_only=True)
        if not state.get("valid"):
            raise RuntimeError(
                f"{target_stage}: block_context_summary.json не создан: {state.get('reason')}"
            )
        return {"ready": True, **state}

    async def _assert_gemma_ready_for_stage(
        self,
        job: AuditJob,
        project_info: dict,
        target_stage: str,
    ) -> dict:
        del project_info
        _root, _project_dir, output_dir = self._resolve_job_paths(job)
        from backend.app.pipeline.stages.block_context.contract import (
            validate_block_context_summary,
        )
        state = validate_block_context_summary(output_dir)
        if not state.get("valid"):
            raise RuntimeError(
                f"{target_stage}: контекст блоков не готов: {state.get('reason')}"
            )
        return {"ready": True, **state}

    @staticmethod
    def _assert_text_analysis_exists(output_dir: Path, target_stage: str) -> None:
        if not resolve_existing(output_dir, TEXT_ANALYSIS_FILENAME).exists():
            raise RuntimeError(
                f"Нельзя запускать {target_stage}: 02_text_analysis.json отсутствует. "
                "Сначала выполните text_analysis."
            )

    @staticmethod
    def _assert_block_analysis_exists(output_dir: Path, target_stage: str) -> None:
        """Симметрично _assert_text_analysis_exists — для порядка block→text."""
        if not resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME).exists():
            raise RuntimeError(
                f"Нельзя запускать {target_stage}: 01_blocks_analysis.json отсутствует. "
                "Сначала выполните block_analysis."
            )

    def _reconcile_completed_block_analysis_for_resume(
        self,
        job: AuditJob,
        output_dir: Path,
    ) -> bool:
        """Снять ложный ``interrupted`` с полностью сохранённого block_analysis.

        После рестарта startup-recovery честно помечает любой оставшийся
        ``running`` как ``interrupted``. Но при явном resume с более позднего
        этапа (например, text_analysis) канонический 01_blocks_analysis.json
        может уже содержать полный результат предыдущего успешного прогона.
        В таком случае красный статус не должен мешать UI/resume-detector и
        создавать впечатление, что блоковые замечания потеряны.

        Статус восстанавливается только когда артефакт сам доказывает полноту:
        есть запись по каждому блоку, все счётчики закрывают total и прогон не
        был cancelled. Частичный файл никогда не маскируется как успешный.
        """
        log_path = Path(output_dir) / "pipeline_log.json"
        try:
            log_data = json.loads(log_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return False

        stage_info = (log_data.get("stages") or {}).get("block_analysis") or {}
        if stage_info.get("status") != "interrupted":
            return False

        artifact_path = resolve_existing(Path(output_dir), BLOCKS_ANALYSIS_FILENAME)
        try:
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return False

        analyses = artifact.get("block_analyses")
        meta = artifact.get("stage01_meta")
        if not isinstance(analyses, list) or not isinstance(meta, dict):
            return False

        try:
            total = int(meta.get("blocks_total") or len(analyses))
            ok = int(meta.get("blocks_ok") or 0)
            failed = int(meta.get("blocks_failed") or 0)
            partial = int(meta.get("blocks_partial") or 0)
            skipped = int(meta.get("blocks_skipped_no_context") or 0)
        except (TypeError, ValueError):
            return False

        processed = ok + failed + partial + skipped
        if (
            bool(meta.get("cancelled"))
            or total <= 0
            or len(analyses) < total
            or processed < total
        ):
            return False

        clean = failed == 0 and partial == 0 and skipped == 0 and ok >= total
        status = "done" if clean else "partial"
        message = (
            f"OK ({ok}/{total} блоков; восстановлено из сохранённого результата)"
            if clean
            else f"Завершено частично ({ok}/{total} блоков; восстановлено из артефакта)"
        )
        self._update_pipeline_log(
            job.project_id,
            "block_analysis",
            status,
            message=message,
            detail={
                "reconciled_from_artifact": True,
                "blocks_total": total,
                "blocks_ok": ok,
                "blocks_failed": failed,
                "blocks_partial": partial,
                "blocks_skipped": skipped,
            },
        )
        return True

    async def start_from_stage(
        self,
        project_id: str,
        stage: str,
        *,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        """Запустить конвейер с указанного этапа (ручной перезапуск цепочки).

        Кладёт single-task в общую очередь — фактический запуск произойдёт,
        когда worker дойдёт до элемента (см. `_enqueue_single`/`_dispatch_action`).
        """
        stage = self._validate_start_from_stage_now(project_id, stage, version_id=version_id)
        return await self._enqueue_single(
            project_id, action="retry_stage", retry_stage=stage,
            version_id=version_id,
        )

    async def resume_pipeline(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        """Продолжить пайплайн с места ошибки."""
        self._assert_stage_model_config_ready()
        resume_info = self.detect_resume_stage(project_id, version_id=version_id)
        if not resume_info.get("can_resume"):
            raise RuntimeError("Все этапы уже завершены — нечего возобновлять")
        resume_stage = str(resume_info.get("stage") or "")
        if resume_stage in {"optimization", "optimization_review"}:
            return await self._enqueue_single(
                project_id,
                action="retry_stage",
                retry_stage=resume_stage,
                version_id=version_id,
            )
        return await self._enqueue_single(
            project_id, action="resume", version_id=version_id,
        )

    async def _run_resumed_pipeline(self, job: AuditJob, start_stage: str, resume_info: dict):
        """Запуск OCR-пайплайна с указанного этапа."""
        start_time = datetime.now()
        pid = job.project_id
        _resume_output_root_token = None
        try:
            # Нормализация stage: legacy aliases → OCR stages
            normalized = self._normalize_ocr_stage(start_stage)

            # Порядок этапов OCR-пайплайна (без дублей). По флагу
            # PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED блоки идут ПЕРЕД текстом.
            blocks_before_text = self._blocks_before_text_enabled()
            _mid = (
                ["block_analysis", "text_analysis"]
                if blocks_before_text else
                ["text_analysis", "block_analysis"]
            )
            ocr_stages = [
                "prepare",
                "gemma_enrichment",
                *_mid,
                "findings_merge",
                "findings_review",
                "norm_verify",
                "debt_control",
                "decision_carryover",
                "excel",
            ]
            if normalized in ocr_stages:
                start_idx = ocr_stages.index(normalized)
            else:
                # Раньше: тихий фолбэк в 0 (=prepare) → «resume оптимизации»
                # молча перезапускал весь конвейер. Такие стадии должны роутиться
                # в свои ветки ДО вызова (см. action="resume"/retry_stage).
                raise RuntimeError(
                    f"_run_resumed_pipeline не поддерживает этап '{normalized}' — "
                    "он выполняется отдельной веткой, а не OCR-конвейером"
                )
            idx_text = ocr_stages.index("text_analysis")
            idx_block = ocr_stages.index("block_analysis")

            await self._log(
                job,
                f"Возобновление конвейера с этапа: {resume_info.get('stage_label', start_stage)} "
                f"({resume_info.get('detail', '')})",
                "info",
            )

            # Version-aware пути: V1 = root, V2+ = _versions/v{N}/.
            _root_dir, project_dir, output_dir = self._resolve_job_paths(job)
            project_info = self._load_project_info_for_paths(pid, _root_dir, project_dir)

            _latest_dir = project_dir / "03_analysis" / "latest"
            _recovered_latest = self._seed_latest_gemma_artifacts_from_recent_run(
                project_dir, _latest_dir, project_info,
            )
            if _recovered_latest:
                await self._log(
                    job,
                    f"Resume: latest восстановлен {_recovered_latest} prereq-артефактами Gemma",
                    "info",
                )

            # Баг B1 аудита пайплайна: retry/resume на v2-primary исполняется
            # в СВЕЖЕМ ПУСТОМ runs/<новый job_id>, а валидатор проверял
            # артефакты в 03_analysis/latest — retry поздних стадий либо
            # падал на prereq-проверках ниже, либо шёл без входов. Сидируем
            # run dir JSON-артефактами из latest (только отсутствующие;
            # тяжёлые PNG-каталоги не копируем — их пересоздаёт pre-crop).
            if (
                _latest_dir.is_dir()
                and output_dir != _latest_dir
                and "03_analysis" in output_dir.parts
            ):
                output_dir.mkdir(parents=True, exist_ok=True)
                # ПЕР-ПРОГОННЫЕ файлы НЕ сидируем: они отражают статус/лог
                # ИМЕННО этого прогона, а не прошлого. Иначе старые записи
                # (напр. debt_control:error от прогона до фикса версий)
                # «переезжают» в новый run и промоутятся обратно в latest —
                # UI показывает ложную ошибку при успешном перезапуске.
                _NO_SEED = {"pipeline_log.json"}
                _seeded = 0
                for _src in sorted(_latest_dir.iterdir()):
                    if not _src.is_file():
                        continue
                    if _src.suffix not in (".json", ".jsonl", ".md"):
                        continue
                    # audit_log.jsonl и его архивы audit_log_*.jsonl — пер-прогонный лог.
                    if _src.name in _NO_SEED or _src.name.startswith("audit_log"):
                        continue
                    _dst = output_dir / _src.name
                    if _dst.exists():
                        continue
                    try:
                        shutil.copy2(_src, _dst)
                        _seeded += 1
                    except OSError as _copy_err:
                        await self._log(
                            job, f"Seed run dir: не скопирован {_src.name}: {_copy_err}",
                            "warn",
                        )
                # Для позднего resume копируем только лёгкие index.json; PNG этим
                # стадиям не нужны, а context-gate получает canonical summary.
                _seeded_idx = 0
                for _sub in ("blocks_stage02_100", "blocks_gemma_100", "blocks"):
                    _src_idx = _latest_dir / _sub / "index.json"
                    _dst_idx = output_dir / _sub / "index.json"
                    if _src_idx.is_file() and not _dst_idx.exists():
                        try:
                            _dst_idx.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(_src_idx, _dst_idx)
                            _seeded_idx += 1
                        except OSError:
                            pass
                if _seeded or _seeded_idx:
                    await self._log(
                        job,
                        f"Resume: run dir засеян {_seeded} артефактами + "
                        f"{_seeded_idx} индексами блоков из latest",
                        "info",
                    )

            # Симметрично _run_ocr_pipeline: все резолвы через
            # gemma_output_root/AUDIT_OUTPUT_DIR внутри стадий должны видеть
            # run dir этого job'а, а не latest.
            from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
                bind_output_root as _bind_or,
            )
            _resume_output_root_token = _bind_or(output_dir)

            if start_idx >= 4:
                await self._assert_gemma_ready_for_stage(job, project_info, normalized)
                self._assert_text_analysis_exists(output_dir, normalized)
                if not resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME).exists():
                    raise RuntimeError(
                        f"Нельзя запускать {normalized}: 01_blocks_analysis.json отсутствует. "
                        "Сначала выполните block_analysis."
                    )
                self._reconcile_completed_block_analysis_for_resume(job, output_dir)
            if start_idx >= 5 and not (output_dir / "03_findings.json").exists():
                raise RuntimeError(
                    f"Нельзя запускать {normalized}: 03_findings.json отсутствует. "
                    "Сначала выполните findings_merge."
                )

            # ═══ ЭТАП 1: Кроп image-блоков ═══
            if start_idx <= 0:
                # Полный перезапуск — бэкап findings перед очисткой
                self._backup_findings_before_restart(pid)
                # Очистить все промежуточные файлы
                self._clean_stage_files(pid, [
                    *TEXT_ANALYSIS_ALL_NAMES, *BLOCKS_ANALYSIS_ALL_NAMES,
                    *BLOCK_CONTEXT_SUMMARY_ALL_NAMES,
                    "03_findings.json", "block_batch_*.json", "block_batches.json",
                    RUNTIME_BATCHES_FILE, "block_analysis_summary.json",
                ])
                job.stage = AuditStage.CROP_BLOCKS
                print(f"[{pid}:resume] ═══ ЭТАП 1: Кроп image-блоков ═══")
                await self._ensure_stage02_crops(job)

                # Построить document_graph v2 (Python, без LLM)
                await self._build_document_graph_v2(job)

                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ Векторные графы блоков (локально, без OCR-модели) ═══
            if start_idx <= 1:
                if start_idx == 1:
                    # Перезапуск контекста меняет вход Stage 01 и downstream-этапов.
                    self._backup_findings_before_restart(pid)
                    self._clean_stage_files(pid, [
                        *TEXT_ANALYSIS_ALL_NAMES, *BLOCKS_ANALYSIS_ALL_NAMES,
                        *BLOCK_CONTEXT_SUMMARY_ALL_NAMES,
                        "03_findings.json", "block_batch_*.json", "block_batches.json",
                        RUNTIME_BATCHES_FILE, "block_analysis_summary.json",
                    ])
                await self._run_gemma_enrichment_stage(job, force=start_idx == 1)

                if job.status == JobStatus.CANCELLED:
                    return

                # Усиление предобработки: Value Grounding. OFF по умолчанию, fail-soft.
                await self._run_block_grounding_stage(job)

            # ═══ Блоки (01) и текст (02) ═══
            async def _resume_run_text_stage() -> bool:
                """Текстовый анализ. Возвращает False если джоба отменена (нужно return)."""
                if start_idx == idx_text:
                    await self._ensure_gemma_ready_or_run(job, project_info, "text_analysis")
                    if blocks_before_text:
                        # Новый порядок: текст читает финальный 02 — он обязан существовать.
                        self._assert_block_analysis_exists(output_dir, "text_analysis")
                        self._reconcile_completed_block_analysis_for_resume(job, output_dir)
                    self._backup_findings_before_restart(pid)
                    if blocks_before_text:
                        # Не трогаем блоки (посчитаны на block-этапе выше).
                        clean = [*TEXT_ANALYSIS_ALL_NAMES, "03_findings.json"]
                    else:
                        clean = [
                            *TEXT_ANALYSIS_ALL_NAMES, *BLOCKS_ANALYSIS_ALL_NAMES,
                            "03_findings.json", "block_batch_*.json", "block_batches.json",
                            RUNTIME_BATCHES_FILE, "block_analysis_summary.json",
                        ]
                    self._clean_stage_files(pid, clean)
                self._reset_job_progress(job)
                job.stage = AuditStage.TEXT_ANALYSIS
                job.status = JobStatus.RUNNING
                print(f"[{pid}:resume] ═══ Текстовый анализ MD ═══")
                await self._log(job, "═══ Текстовый анализ MD (Claude) ═══")
                await self._start_heartbeat(job)
                _ta_result = await _run_text_analysis_stage(
                    self._make_stage_context(job),
                    with_rate_limit_retry=False,
                )
                if _ta_result.cancelled:
                    job.status = JobStatus.CANCELLED
                    return False
                if not _ta_result.success:
                    raise RuntimeError(_ta_result.error or "Текстовый анализ: ошибка")
                self._promote_v2_analysis_artifacts(
                    job, (TEXT_ANALYSIS_FILENAME, "pipeline_log.json")
                )
                return job.status != JobStatus.CANCELLED

            async def _resume_run_block_stage() -> bool:
                """Анализ блоков. Возвращает False если джоба отменена (нужно return)."""
                await self._assert_gemma_ready_for_stage(job, project_info, "block_analysis")
                if not blocks_before_text:
                    self._assert_text_analysis_exists(output_dir, "block_analysis")
                if blocks_before_text:
                    if start_idx == idx_block:
                        # Resume прямо с блоков (новый порядок) — почистить 02/03, но не 01.
                        self._backup_findings_before_restart(pid)
                        self._clean_stage_files(pid, [
                            *BLOCKS_ANALYSIS_ALL_NAMES, "block_analysis_summary.json",
                            RUNTIME_BATCHES_FILE, "03_findings.json",
                        ])
                    # Полноценный блок-этап (findings_only + retry + promote 02 + компакт для текста).
                    await self._ocr_block_analysis_and_retry(job, pid, project_info, output_dir)
                    if job.status == JobStatus.CANCELLED:
                        return False
                elif get_stage_batch_mode("block_batch") == BLOCK_BATCH_MODE_FINDINGS_ONLY:
                    # Старый порядок: findings_only без retry (unreadable_text не помечается).
                    await self._run_block_analysis_findings_only(job)
                    if job.status == JobStatus.CANCELLED:
                        return False
                    if job.status == JobStatus.FAILED:
                        # Симметрично _ocr_block_analysis_and_retry: FAILED нельзя
                        # проглатывать — иначе merge пойдёт без 02_blocks_analysis.
                        raise RuntimeError(
                            job.error_message or "Stage 01 (block_analysis) failed"
                        )
                    self.active_jobs[pid] = job
                    self._tasks[pid] = asyncio.current_task()
                return job.status != JobStatus.CANCELLED

            if blocks_before_text:
                if start_idx <= idx_block:
                    if not await _resume_run_block_stage():
                        return
                if start_idx <= idx_text:
                    if not await _resume_run_text_stage():
                        return
            else:
                if start_idx <= idx_text:
                    if not await _resume_run_text_stage():
                        return
                if start_idx <= idx_block:
                    if not await _resume_run_block_stage():
                        return

            # ═══ ЭТАП 5: Свод замечаний ═══
            if start_idx <= 4:
                if start_idx == 4:
                    await self._assert_gemma_ready_for_stage(job, project_info, "findings_merge")
                    self._assert_text_analysis_exists(output_dir, "findings_merge")
                    if not resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME).exists():
                        raise RuntimeError(
                            "Нельзя запускать findings_merge: 01_blocks_analysis.json отсутствует. "
                            "Сначала выполните block_analysis."
                        )
                # Retry merge перенумерует F-ID — бэкап findings + слепок вердиктов.
                self._backup_findings_before_restart(pid)
                self._clean_stage_files(pid, [
                    "03_findings.json", "03_findings_review.json", "03_findings_pre_review.json",
                ])
                self._reset_job_progress(job)
                job.stage = AuditStage.FINDINGS_MERGE
                job.status = JobStatus.RUNNING

                print(f"[{pid}:resume] ═══ ЭТАП 5: Свод замечаний ═══")
                await self._start_heartbeat(job)
                _fm_result = await _run_findings_merge_stage(self._make_stage_context(job))
                if _fm_result.cancelled:
                    job.status = JobStatus.CANCELLED
                    return
                if not _fm_result.success:
                    raise RuntimeError(_fm_result.error or "Свод замечаний: ошибка")

                self._promote_v2_analysis_artifacts(
                    job, ("03_findings.json", "pipeline_log.json")
                )

                # Перепривязать вердикты эксперта к новым F-ID (fail-soft).
                await self._run_verdict_rehydration(job)

                # «Размышление модели»: стрим найденных замечаний в live-лог (WS)
                await self._stream_findings_events(job, "merge")

                if job.status == JobStatus.CANCELLED:
                    return

                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАПЫ 5.5-6: Параллельный запуск critic + norms (+ optimization) ═══
            # output_dir уже version-aware (см. начало _run_resumed_pipeline).
            if start_idx < 5:
                # Полный post-findings: critic + norms + optimization (параллельно).
                # AUDIT_RESUME_SKIP_OPTIMIZATION=true (default false) обрезает каскад
                # resume до верификации норм: тяжёлый Opus-этап оптимизации и его
                # review НЕ запускаются. Нужно для re-run проектов, где переделывается
                # только block_analysis→findings→нормы (Stage 02 был заблокирован
                # дневным лимитом платного API), а оптимизация по ним не пересобирается.
                # Флаг действует ТОЛЬКО на resume-путь; полный аудит (_run_ocr_pipeline)
                # не затрагивается.
                findings_path = output_dir / "03_findings.json"
                if findings_path.exists():
                    _skip_opt = os.environ.get(
                        "AUDIT_RESUME_SKIP_OPTIMIZATION", "",
                    ).strip().lower() in {"1", "true", "yes", "on"}
                    if _skip_opt:
                        await self._log(
                            job,
                            "Оптимизация пропущена (AUDIT_RESUME_SKIP_OPTIMIZATION=true): "
                            "resume только до верификации норм.",
                            "info",
                        )
                    await self._run_post_findings_parallel(
                        job, project_info, include_optimization=not _skip_opt,
                    )

                    if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                        return

                    self.active_jobs[pid] = job
                    self._tasks[pid] = asyncio.current_task()

                    # Вердикты по оптимизациям: optimization.json пересобран
                    # внутри post-findings блока — восстановить их можно теперь.
                    if not _skip_opt:
                        await self._run_verdict_rehydration(job, item_types=("optimization",))
                else:
                    await self._log(job, "03_findings.json не найден — пропуск верификации", "warn")

            # Resume только этап «Верификатор» — без повтора norms/optimization
            if start_idx == 5:
                findings_path = output_dir / "03_findings.json"
                if findings_path.exists():
                    await self._start_heartbeat(job)
                    await self._run_findings_verify(job, project_info)

                    if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                        return

                    # Верификатор fail-soft: 'error' в логе означает сбой ОБЕИХ
                    # внутренних фаз (структура + присутствие) — это не должно
                    # молча маскироваться на resume-пути.
                    _plog_path = output_dir / "pipeline_log.json"
                    try:
                        _plog = json.loads(_plog_path.read_text(encoding="utf-8")) if _plog_path.exists() else {}
                    except Exception:
                        _plog = {}
                    _critic_status = _plog.get("stages", {}).get("findings_critic", {}).get("status")
                    if _critic_status == "error":
                        job.status = JobStatus.FAILED
                        job.error_message = "Верификатор (структурная проверка) провалился"
                        return

                    self.active_jobs[pid] = job
                    self._tasks[pid] = asyncio.current_task()
                else:
                    await self._log(job, "03_findings.json не найден — пропуск review", "warn")

            # Если resume начался с norm_verify (start_idx=6) — запускать только norms
            if start_idx == 6:
                self._clean_stage_files(pid, [
                    "03a_norms_verified.json", "norm_checks.json", "norm_checks_llm.json",
                    "missing_norms_queue.json", "missing_norms_report.json",
                    "missing_norms_queue.md",
                ])
                self._reset_job_progress(job)
                findings_path = output_dir / "03_findings.json"
                if findings_path.exists():
                    job.stage = AuditStage.NORM_VERIFY
                    job.status = JobStatus.RUNNING
                    print(f"[{pid}:resume] ═══ Верификация норм ═══")
                    await self._log(job, "═══ Верификация нормативных ссылок ═══")
                    await self._run_norm_verification(job, standalone=False)

                    if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                        return

                    self.active_jobs[pid] = job
                    self._tasks[pid] = asyncio.current_task()

            # ═══ Контроль долгов + перенос вердиктов (по start_idx) ═══
            if start_idx <= ocr_stages.index("debt_control"):
                await self._run_debt_control(job)
            if start_idx <= ocr_stages.index("decision_carryover"):
                await self._run_decision_carryover(job)

            # ═══ ЭТАП 7: Excel ═══
            self._reset_job_progress(job)
            job.stage = AuditStage.EXCEL
            job.status = JobStatus.RUNNING
            print(f"[{pid}:resume] ═══ ЭТАП 7: Excel ═══")
            from backend.app.pipeline.stages.report.runner import run_excel_report as _run_excel
            _xls_result = await _run_excel(self._make_stage_context(job))
            if not _xls_result.success:
                # Excel-ошибка не прерывает pipeline: аудит считается завершённым,
                # но pipeline_log уже содержит excel:error для диагностики.
                await self._log(job, f"Excel-отчёт не создан: {_xls_result.error}", "warn")

            wall_sec = (datetime.now() - start_time).total_seconds()
            net_sec = max(0, wall_sec - job.pause_total_sec)
            duration = round(net_sec / 60, 1)
            wall_duration = round(wall_sec / 60, 1)
            job.status = JobStatus.COMPLETED
            self._promote_completed_audit_v2(job)
            pause_note = f" (паузы: {round(job.pause_total_sec / 60, 1)} мин)" if job.pause_total_sec > 60 else ""
            print(f"[{pid}:resume] ═══ Конвейер завершён за {duration} мин{pause_note} ═══")
            await self._log(job, f"Конвейер завершён за {duration} мин{pause_note}.", "info")
            await self._log_stage_degradations(job)

            await ws_manager.broadcast_to_project(
                pid, WSMessage.complete(pid, duration_minutes=duration,
                                        pause_minutes=round(job.pause_total_sec / 60, 1)),
            )

        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            await self._log(job, f"Исключение: {e}", "error")
        finally:
            if _resume_output_root_token is not None:
                from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
                    unbind_output_root as _unbind_or,
                )
                try:
                    _unbind_or(_resume_output_root_token)
                except Exception:
                    pass
            job.completed_at = datetime.now().isoformat()
            self._cleanup(pid)

    # ─── Запуск подготовки ───
    async def start_prepare(self, project_id: str, *, version_id: Optional[str] = None) -> AuditJob:
        return await self._enqueue_single(project_id, action="prepare", version_id=version_id)

    async def _run_prepare(self, job: AuditJob):
        """Подготовка проекта — оркестратор делегирует в prepare/runner.py."""
        from backend.app.pipeline.stages.prepare.runner import run_prepare as _prepare_runner
        from backend.app.pipeline.stage_result import StageResult
        pid = job.project_id
        try:
            await self._start_heartbeat(job)
            ctx = self._make_stage_context(job)
            result: StageResult = await _prepare_runner(ctx)

            if result.cancelled:
                job.status = JobStatus.CANCELLED
            elif result.success:
                job.status = JobStatus.COMPLETED
            else:
                job.status = JobStatus.FAILED
                job.error_message = result.error or "prepare failed"

        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            self._update_pipeline_log(pid, "prepare", "error", error="Отменено")
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            await self._log(job, f"Исключение: {e}", "error")
            self._update_pipeline_log(pid, "prepare", "error", error=str(e))
        finally:
            job.completed_at = datetime.now().isoformat()
            self._cleanup(pid)

    # ─── Запуск пакетного анализа тайлов ───
    async def start_tile_audit(
        self,
        project_id: str,
        start_from: int = 1,
        *,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        return await self._enqueue_single(
            project_id, action="tile_audit",
            extra_params={"start_from": start_from},
            version_id=version_id,
        )

    async def _run_tile_audit(self, job: AuditJob, start_from: int = 1, pages_filter: list[int] | None = None, standalone: bool = True):
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "tile_audit", "running")
            # Version-aware пути: V1 = root, V2+ = _versions/v{N}/.
            _root_dir, project_dir, output_dir = self._resolve_job_paths(job)
            batches_file = output_dir / "tile_batches.json"

            # Шаг 1: Генерация пакетов (если нет или устарели)
            regenerate = False
            if not batches_file.exists():
                print(f"[{pid}:tile] tile_batches.json не существует → regenerate")
                regenerate = True
            else:
                # Проверяем актуальность по двум критериям:
                # 1) tile_config_source должен совпадать
                # 2) количество тайлов в батчах = реальному количеству на диске
                info = self._load_project_info_for_paths(pid, _root_dir, project_dir)
                current_source = info.get("tile_config_source", "")
                with open(batches_file, "r", encoding="utf-8") as f:
                    bdata = json.load(f)
                old_source = bdata.get("tile_config_source", "")
                old_tile_count = bdata.get("total_tiles", 0)

                # Подсчитать реальные тайлы на диске
                tiles_dir = output_dir / "tiles"
                real_tile_count = 0
                if tiles_dir.is_dir():
                    for page_dir in tiles_dir.iterdir():
                        if page_dir.is_dir() and page_dir.name.startswith("page_"):
                            real_tile_count += sum(1 for f in page_dir.iterdir() if f.suffix == ".png")

                print(f"[{pid}:tile] tile_config_source: файл={old_source}, проект={current_source}")
                print(f"[{pid}:tile] tile_count: батчи={old_tile_count}, диск={real_tile_count}")

                stale_reason = None
                if current_source != old_source:
                    stale_reason = f"tile_config_source изменился ({old_source} → {current_source})"
                elif old_tile_count != real_tile_count:
                    stale_reason = f"количество тайлов изменилось ({old_tile_count} → {real_tile_count})"

                if stale_reason:
                    regenerate = True
                    await self._log(job, f"{stale_reason}, пересоздаём пакеты...")
                    # Удалить старые tile_batch_NNN.json
                    deleted_count = 0
                    for f_old in output_dir.glob("tile_batch_*.json"):
                        f_old.unlink()
                        deleted_count += 1
                    print(f"[{pid}:tile] Удалено {deleted_count} старых tile_batch_*.json")

            # При фильтре по страницам — всегда пересоздаём батчи
            if pages_filter:
                regenerate = True

            if regenerate:
                job.stage = AuditStage.TILE_BATCHES
                gen_args = [self._project_path_for_job(job)]
                if pages_filter:
                    pages_str = ",".join(str(p) for p in pages_filter)
                    gen_args += ["--pages", pages_str]
                    await self._log(job, f"Генерация пакетов тайлов (страницы: {pages_str})...")
                else:
                    await self._log(job, "Генерация пакетов тайлов...")
                exit_code, _, stderr = await self._run_script_for_job(
                    job,
                    str(BLOCKS_SCRIPT),
                    ["batches"] + gen_args,
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code != 0:
                    raise RuntimeError(f"blocks.py batches: {stderr}")
                await self._log(job, "Пакеты сгенерированы")

            # Загружаем пакеты
            with open(batches_file, "r", encoding="utf-8") as f:
                batches_data = json.load(f)

            batches = batches_data.get("batches", [])
            total = len(batches)
            job.progress_total = total

            # Свежий запуск (не resume) — удалить старые результаты батчей
            if start_from <= 1:
                deleted_batch_count = 0
                for old_file in output_dir.glob("tile_batch_*.json"):
                    old_file.unlink()
                    deleted_batch_count += 1
                if deleted_batch_count:
                    print(f"[{pid}:tile] Свежий запуск — удалено {deleted_batch_count} старых tile_batch_*.json")
                    await self._log(job, f"Очистка: удалено {deleted_batch_count} старых результатов батчей")

            # Загружаем project_info (project_dir уже version-aware)
            project_info = self._load_project_info_for_paths(pid, _root_dir, project_dir)

            # Шаг 2: Параллельная обработка пакетов
            job.stage = AuditStage.TILE_AUDIT
            parallel = MAX_PARALLEL_BATCHES
            print(f"[{pid}:tile] Запуск пакетного анализа: {total} пакетов, start_from={start_from}, parallel={parallel}")
            await self._log(job, f"Запуск пакетного анализа тайлов: {total} пакетов (x{parallel} параллельно)")
            await self._start_heartbeat(job)

            semaphore = asyncio.Semaphore(parallel)
            completed_count = 0
            error_count = 0
            rate_limit_paused = False  # флаг: система на паузе из-за rate limit

            async def _process_batch(batch):
                nonlocal completed_count, error_count, rate_limit_paused
                batch_id = batch["batch_id"]

                # Пропуск уже обработанных
                if batch_id < start_from:
                    return

                result_file = output_dir / f"tile_batch_{batch_id:03d}.json"
                if result_file.exists() and result_file.stat().st_size > 100:
                    completed_count += 1
                    job.progress_current = completed_count
                    await self._progress(job, completed_count, total)
                    return

                async with semaphore:
                    if job.status == JobStatus.CANCELLED:
                        return
                    # Остановка при слишком большом числе реальных ошибок
                    if error_count >= STAGE02_ERROR_ABORT_THRESHOLD:
                        return

                    # ── Превентивная проверка rate limit перед запуском ──
                    can_go = await self._check_before_launch(job)
                    if not can_go:
                        # Job отменён или макс. ожидание превышено
                        return

                    tile_count = batch.get("tile_count", len(batch.get("tiles", [])))
                    print(f"[{pid}:tile] Пакет {batch_id}/{total}: {tile_count} тайлов...")
                    await self._log(job, f"Пакет {batch_id}/{total}: {tile_count} тайлов...")

                    # ── Запуск с retry при rate limit ──
                    retries = 0
                    pause_before_batch = job.pause_total_sec
                    while retries <= RATE_LIMIT_MAX_RETRIES:
                        batch_start_time = datetime.now()
                        job.batch_started_at = batch_start_time.isoformat()

                        exit_code, output, cli_result = await claude_runner.run_tile_batch(
                            batch, project_info, job.project_id, total,
                            on_output=lambda msg: self._log(job, msg),
                        )
                        self._record_cli_usage(job, cli_result, f"tile_batch_{batch_id:03d}")
                        print(f"[{pid}:tile] Пакет {batch_id}/{total}: exit_code={exit_code}")

                        batch_wall = (datetime.now() - batch_start_time).total_seconds()
                        batch_pause = job.pause_total_sec - pause_before_batch
                        batch_duration = max(0, batch_wall - batch_pause)
                        job.batch_durations.append(batch_duration)

                        # Успех
                        if exit_code == 0:
                            if result_file.exists():
                                size_kb = round(result_file.stat().st_size / 1024, 1)
                                await self._log(job, f"Пакет {batch_id}/{total}: OK ({size_kb} KB)", "info")
                            else:
                                await self._log(job, f"Пакет {batch_id}/{total}: файл не создан", "warn")
                                if output and output.strip():
                                    await self._log(job, f"  Вывод: {output.strip()[:500]}", "warn")
                            break  # выход из retry-цикла

                        # Отмена — выходим без retry и без ошибки
                        if claude_runner.is_cancelled(exit_code):
                            await self._log(job, f"Пакет {batch_id}/{total}: отменён", "warn")
                            break

                        # Проверяем: это rate limit или реальная ошибка?
                        stdout_text = output or ""
                        stderr_text = cli_result.result_text if cli_result and cli_result.is_error else ""
                        if claude_runner.is_rate_limited(exit_code, stdout_text, stderr_text):
                            retries += 1
                            rate_limit_paused = True
                            await self._log(
                                job,
                                f"Пакет {batch_id}/{total}: rate limit (попытка {retries}/{RATE_LIMIT_MAX_RETRIES})",
                                "warn",
                            )

                            if retries > RATE_LIMIT_MAX_RETRIES:
                                await self._log(
                                    job,
                                    f"Пакет {batch_id}/{total}: превышено макс. попыток после rate limit",
                                    "error",
                                )
                                error_count += 1
                                break

                            # Ждём сброса rate limit
                            can_continue = await self._wait_for_rate_limit(
                                job, f"rate limit при обработке пакета {batch_id}",
                                cli_output=f"{stdout_text}\n{stderr_text}",
                            )
                            if not can_continue:
                                error_count += 1
                                break
                            # После ожидания — повторяем этот же батч
                            continue
                        else:
                            # Реальная ошибка (не rate limit)
                            error_count += 1
                            error_snippet = (output or "").strip()[:500]
                            await self._log(job, f"Пакет {batch_id}/{total}: ОШИБКА (код {exit_code})", "error")
                            if error_snippet:
                                await self._log(job, f"  Детали: {error_snippet}", "error")
                            if error_count >= STAGE02_ERROR_ABORT_THRESHOLD:
                                await self._log(job, f"{error_count} ошибок — пакетный анализ остановлен", "error")
                            break  # не retry для реальных ошибок

                    completed_count += 1
                    job.progress_current = completed_count
                    await self._progress(job, completed_count, total)

            # Запуск всех батчей параллельно (семафор ограничивает одновременность)
            tasks = [_process_batch(batch) for batch in batches]
            gathered = await asyncio.gather(*tasks, return_exceptions=True)
            for batch, result in zip(batches, gathered):
                if isinstance(result, Exception):
                    error_count += 1
                    batch_id = batch.get("batch_id", "?")
                    await self._log(
                        job,
                        f"Пакет {batch_id}/{total}: необработанное исключение task — "
                        f"{type(result).__name__}: {result}",
                        "error",
                    )

            # Проверка: если ВСЕ батчи провалились — это FAILED, не COMPLETED
            if error_count >= total:
                job.status = JobStatus.FAILED
                job.error_message = f"Все {total} пакетов завершились с ошибкой"
                await self._log(job, f"Все {total} пакетов завершились с ошибкой — этап FAILED", "error")
                self._update_pipeline_log(pid, "tile_audit", "error",
                                           error=f"Все {total} пакетов с ошибкой",
                                           detail={"completed_batches": 0,
                                                   "total_batches": total,
                                                   "error_count": error_count})
                return

            # Шаг 3: Слияние результатов
            if job.status != JobStatus.CANCELLED:
                job.stage = AuditStage.MERGE
                await self._log(job, "Слияние результатов пакетного анализа...")
                exit_code, _, stderr = await self._run_script_for_job(
                    job,
                    str(BLOCKS_SCRIPT),
                    ["merge", self._project_path_for_job(job)],
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code == 0:
                    await self._log(job, "02_tiles_analysis.json создан", "info")
                else:
                    await self._log(job, f"Ошибка слияния: {stderr}", "error")

            if error_count > 0:
                await self._log(job, f"Пакетный анализ завершён с ошибками ({error_count}/{total} пакетов)", "warn")
                self._update_pipeline_log(pid, "tile_audit", "error",
                                           error=f"{error_count} из {total} пакетов с ошибками",
                                           detail={"completed_batches": total - error_count,
                                                   "total_batches": total,
                                                   "error_count": error_count})
            else:
                self._update_pipeline_log(pid, "tile_audit", "done",
                                           message=f"Все {total} пакетов OK")
            job.status = JobStatus.COMPLETED
            await self._log(job, "Пакетный анализ тайлов завершён", "info")

        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            self._update_pipeline_log(pid, "tile_audit", "error", error="Отменено")
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            await self._log(job, f"Исключение: {e}", "error")
            self._update_pipeline_log(pid, "tile_audit", "error", error=str(e))
        finally:
            job.completed_at = datetime.now().isoformat()
            if standalone:
                self._cleanup(job.project_id)

    # ─── Запуск основного аудита ───
    async def start_main_audit(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        # cleanup stage-файлов и установка stage перенесены в `_dispatch_action`
        return await self._enqueue_single(
            project_id, action="main_audit", version_id=version_id,
        )

    async def _run_main_audit(self, job: AuditJob, standalone: bool = True):
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "main_audit", "running")
            # Version-aware project_info: V1 = root, projects_v2 = 01_input/version.json.
            _root_dir, _project_dir, _output_dir = self._resolve_job_paths(job)
            project_info = self._load_project_info_for_paths(pid, _root_dir, _project_dir)

            await self._log(job, "Запуск основного аудита Claude...")
            await self._start_heartbeat(job)

            # ── Проверка rate limit перед запуском ──
            can_go = await self._check_before_launch(job)
            if not can_go:
                job.status = JobStatus.FAILED
                job.error_message = "Rate limit: ожидание превышено или отменено"
                self._update_pipeline_log(pid, "main_audit", "error",
                                           error="Rate limit: ожидание превышено")
                return

            exit_code, output, cli_result = await claude_runner.run_main_audit(
                project_info, pid,
                on_output=lambda msg: self._log(job, msg),
                output_dir=_output_dir,
                version_dir=_project_dir,
                version_id=job.version_id,
            )
            self._record_cli_usage(job, cli_result, "main_audit")

            if exit_code == 0:
                await self._log(job, "Аудит завершён", "info")
                job.status = JobStatus.COMPLETED
                self._update_pipeline_log(pid, "main_audit", "done", message="OK")
            elif claude_runner.is_cancelled(exit_code):
                await self._log(job, "Основной аудит отменён", "warn")
                job.status = JobStatus.CANCELLED
                self._update_pipeline_log(pid, "main_audit", "error", error="Отменено")
            elif claude_runner.is_rate_limited(exit_code, output or "", ""):
                # Rate limit во время основного аудита — ждём и retry
                await self._log(job, "Rate limit при основном аудите, ожидание...", "warn")
                can_continue = await self._wait_for_rate_limit(job, "rate limit при основном аудите", cli_output=output or "")
                if can_continue:
                    # Повторный запуск
                    exit_code, output, cli_result = await claude_runner.run_main_audit(
                        project_info, pid,
                        on_output=lambda msg: self._log(job, msg),
                        output_dir=_output_dir,
                        version_dir=_project_dir,
                        version_id=job.version_id,
                    )
                    self._record_cli_usage(job, cli_result, "main_audit_retry")
                    if exit_code == 0:
                        await self._log(job, "Аудит завершён (после паузы)", "info")
                        job.status = JobStatus.COMPLETED
                        self._update_pipeline_log(pid, "main_audit", "done", message="OK (после rate limit паузы)")
                    else:
                        await self._log(job, f"Ошибка аудита после retry (код {exit_code})", "error")
                        job.status = JobStatus.FAILED
                        job.error_message = f"Exit code: {exit_code} (после rate limit retry)"
                        self._update_pipeline_log(pid, "main_audit", "error",
                                                   error=_extract_error_detail(exit_code, output))
                else:
                    job.status = JobStatus.FAILED
                    job.error_message = "Rate limit: ожидание превышено или отменено"
                    self._update_pipeline_log(pid, "main_audit", "error",
                                               error="Rate limit: ожидание превышено")
            else:
                await self._log(job, f"Ошибка аудита (код {exit_code})", "error")
                job.status = JobStatus.FAILED
                job.error_message = f"Exit code: {exit_code}"
                self._update_pipeline_log(pid, "main_audit", "error",
                                           error=_extract_error_detail(exit_code, output))

        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            self._update_pipeline_log(pid, "main_audit", "error", error="Отменено")
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            await self._log(job, f"Исключение: {e}", "error")
            self._update_pipeline_log(pid, "main_audit", "error", error=str(e))
        finally:
            job.completed_at = datetime.now().isoformat()
            if standalone:
                self._cleanup(pid)

    # ─── Верификация нормативных ссылок ───
    async def start_norm_verify(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        # cleanup перенесён в `_dispatch_action`
        return await self._enqueue_single(
            project_id, action="norm_verify", version_id=version_id,
        )


    async def _run_findings_verify(self, job: AuditJob, project_info: dict):
        """Тонкий оркестратор этапа «Верификатор» (findings_verify).

        Пришёл на смену LLM-критику (03b/03c). Делает две полезные вещи над слитым
        03_findings.json: детерминированные структурные проверки (перенос из критика)
        + LLM-проверку присутствия («страж отсутствия»). Пишет ТЕ ЖЕ pipeline_log-ключи
        (findings_critic/findings_corrector) — совместимость со статус-машинерией и
        объединённым чипом фронта. Бизнес-логика — в stages/findings_verify/runner.py.
        """
        job.stage = AuditStage.FINDINGS_REVIEW
        job.status = JobStatus.RUNNING
        # stage_override: в параллельной группе job общий с norm_verify —
        # без явной секции строки верификатора красились в norm_verify.
        ctx = self._make_stage_context(job, stage_override="findings_review")
        # Верификатор fail-soft: не валит job (замечания не теряются). job.status
        # остаётся RUNNING — critic_v2 и promote выполняются как раньше.
        await _run_findings_verify_stage(ctx)

        # ─── Critic v2 post-processing (experimental, OFF by default) ──────
        # Запускается после готовых findings; не трогает production-артефакты.
        # Fail-open: ошибка stage не валит аудит (если не включён CRITIC_V2_FAILS_PIPELINE).
        if job.status not in (JobStatus.CANCELLED, JobStatus.FAILED):
            await self._run_critic_v2_post_review(job)

        if job.status not in (JobStatus.CANCELLED, JobStatus.FAILED):
            self._promote_v2_analysis_artifacts(
                job,
                (
                    "03_findings.json",
                    "03_findings_review.json",
                    "03_findings_pre_review.json",
                    "pipeline_log.json",
                ),
            )

    async def _run_critic_v2_post_review(self, job: AuditJob) -> None:
        """Post-processing critic v2 triage над уже готовыми 03_findings.json.

        Не модифицирует production artifacts. Пишет только в
        <project>/_output/<CRITIC_V2_OUTPUT_SUBDIR>/. LLM не вызывается.
        Контроль через env (см. backend.app.core.config):
          CRITIC_V2_ENABLED, CRITIC_V2_PROFILE, CRITIC_V2_LLM_ENABLED,
          CRITIC_V2_FAILS_PIPELINE, CRITIC_V2_OUTPUT_SUBDIR.
        """
        from backend.app.core import config as cfg

        pid = job.project_id
        if not getattr(cfg, "CRITIC_V2_ENABLED", False):
            self._update_pipeline_log(
                pid, "critic_v2_triage", "skipped",
                detail={"reason": "CRITIC_V2_ENABLED=false"},
            )
            return

        profile = getattr(cfg, "CRITIC_V2_PROFILE", "conservative") or "conservative"
        output_subdir = getattr(cfg, "CRITIC_V2_OUTPUT_SUBDIR", "critic_v2") or "critic_v2"
        # llm_enabled из конфига передаём как hint, но runner внутри игнорирует
        # его (см. runner.py) — это вторая линия защиты.
        llm_enabled = bool(getattr(cfg, "CRITIC_V2_LLM_ENABLED", False))
        fails_pipeline = bool(getattr(cfg, "CRITIC_V2_FAILS_PIPELINE", False))

        from backend.app.services.storage.storage_write_facade import v2_is_primary as _v2_primary_for_critic
        if _v2_primary_for_critic():
            _root_dir, _version_dir, _output_dir = self._resolve_job_paths(job)
            project_dir = _output_dir
        else:
            project_dir = Path(_project_path(pid, getattr(job, "version_id", None)))

        await self._log(job, f"Critic v2 triage: запуск (profile={profile}, "
                              f"subdir={output_subdir}, llm=False)",
                        stage_override="findings_review")
        self._update_pipeline_log(
            pid, "critic_v2_triage", "running",
            detail={"profile": profile, "output_subdir": output_subdir},
        )
        try:
            # Runner синхронный и быстрый — вынесем в thread, чтобы не блокировать loop.
            result = await asyncio.to_thread(
                _run_critic_v2_triage_stage,
                project_dir,
                output_subdir=output_subdir,
                profile=profile,
                llm_enabled=False,  # явно False — fail-safe независимо от cfg
                project_id=pid,
            )
        except Exception as exc:  # noqa: BLE001 — fail-open
            await self._log(job, f"Critic v2 triage упал: {exc}", "warn",
                            stage_override="findings_review")
            self._update_pipeline_log(
                pid, "critic_v2_triage", "error",
                error=str(exc),
                detail={"non_blocking": not fails_pipeline},
            )
            if fails_pipeline:
                job.status = JobStatus.FAILED
                job.error_message = f"Critic v2 triage failed: {exc}"
                raise
            return

        if not result.success:
            await self._log(job, f"Critic v2 triage: {result.error}", "warn",
                            stage_override="findings_review")
            self._update_pipeline_log(
                pid, "critic_v2_triage", "error",
                error=result.error or "unknown",
                detail={"non_blocking": not fails_pipeline},
            )
            if fails_pipeline:
                job.status = JobStatus.FAILED
                job.error_message = f"Critic v2 triage failed: {result.error}"
            return

        await self._log(
            job,
            f"Critic v2 triage готов: {result.triage_total}/{result.findings_total} "
            f"замечаний обработано, артефакты в {result.artifacts_dir}",
            stage_override="findings_review",
        )
        self._update_pipeline_log(
            pid, "critic_v2_triage", "done",
            detail={
                "profile": result.profile,
                "findings_total": result.findings_total,
                "triage_total": result.triage_total,
                "artifacts_dir": str(result.artifacts_dir) if result.artifacts_dir else None,
                "llm_called": False,
            },
        )

    # ─── Параллельный запуск post-findings этапов ───

    async def _run_post_findings_parallel(
        self,
        job: AuditJob,
        project_info: dict,
        include_optimization: bool = True,
        include_norms: bool = True,
    ):
        """
        Параллельный запуск после findings_merge:

        ┌─ findings_critic → corrector ──────────────┐
        ├─ norm_verify ──────────────────────────────┼─→ (done)
        └─ optimization → (ждёт corrector) → opt_review ─┘

        include_norms=False (флаг PIPELINE_NORMS_AFTER_MERGE_ENABLED): norm_verify
        НЕ запускается здесь — вызывающий код прогонит его последовательно ПОСЛЕ
        debt_control (merge/stable-id), чтобы нормы верифицировались против финальных F-ID.

        Файловая безопасность:
        - critic/corrector пишут: 03_findings_review*.json, 03_findings.json
        - norm_verify пишет: norm_checks*.json, norm_fix пишет 03_findings.json
        - optimization пишет: optimization*.json
        Corrector и norm_fix оба пишут в 03_findings.json →
        norm_fix ждёт corrector_done перед записью (через wait_before_fix).
        """
        pid = job.project_id
        corrector_done = asyncio.Event()
        review_error = False

        async def _task_findings_review():
            """Задача A: Верификатор (findings_verify) → signal corrector_done.

            Верификатор fail-soft (замечания не теряются), но если сам ОРКЕСТРАТОР
            упадёт — norm_fix ждёт corrector_done, поэтому Event ставим в finally.
            """
            nonlocal review_error
            try:
                await self._run_findings_verify(job, project_info)
            except Exception as e:
                await self._log(job, f"Верификатор (оркестратор) ошибка: {e}", "error",
                                stage_override="findings_review")
                review_error = True
            finally:
                corrector_done.set()

        async def _task_norm_verify():
            """Задача B: Верификация норм (параллельно с critic).

            Шаги 1-2 + MCP paragraph verification работают параллельно с critic/corrector.
            Шаг norm_fix ждёт corrector_done (оба пишут в 03_findings.json).
            """
            try:
                self._clean_stage_files(pid, [
                    "03a_norms_verified.json", "norm_checks.json", "norm_checks_llm.json",
                    "missing_norms_queue.json", "missing_norms_report.json",
                    "missing_norms_queue.md",
                ])
                print(f"[{pid}] ═══ Верификация норм (параллельно) ═══")
                await self._log(job, "═══ Верификация нормативных ссылок (параллельно с Critic) ═══",
                                stage_override="norm_verify")
                await self._run_norm_verification(
                    job, standalone=False, wait_before_fix=corrector_done,
                )
            except Exception as e:
                await self._log(job, f"Norm verify ошибка: {e}", "error",
                                stage_override="norm_verify")
                self._update_pipeline_log(pid, "norm_verify", "error", error=str(e))

        async def _task_optimization():
            """Задача C: Optimization → ждёт corrector → opt_critic → opt_corrector."""
            print(f"[{pid}] _task_optimization STARTED")
            try:
                # Optimization сам по себе НЕ зависит от corrector
                from backend.app.services.storage.storage_write_facade import v2_is_primary

                if v2_is_primary():
                    opt_job_id = job.job_id
                    opt_object_id = getattr(job, "object_id", None)
                    opt_version_id = getattr(job, "version_id", None)
                else:
                    opt_job_id = job.job_id + "_opt"
                    opt_object_id = self._resolve_object_id(None)
                    opt_version_id = None
                opt_job = AuditJob(
                    job_id=opt_job_id,
                    object_id=opt_object_id,
                    project_id=pid,
                    version_id=opt_version_id,
                    stage=AuditStage.OPTIMIZATION,
                    status=JobStatus.RUNNING,
                    started_at=datetime.now().isoformat(),
                )
                print(f"[{pid}] ═══ Оптимизация (параллельно) ═══")
                await self._log(job, "═══ Оптимизация (параллельно с Critic) ═══",
                                stage_override="optimization")

                await self._run_optimization(opt_job, standalone=False)

                if opt_job.status != JobStatus.COMPLETED:
                    await self._log(
                        job,
                        f"Оптимизация: {opt_job.status.value}"
                        + (f" — {opt_job.error_message}" if opt_job.error_message else ""),
                        "warn",
                        stage_override="optimization",
                    )
                    return

                # Opt_critic ЖДЁТ corrector (нужны финальные findings для проверки конфликтов)
                await self._log(job, "Оптимизация готова, ожидание Corrector для opt_critic...",
                                stage_override="optimization")
                await corrector_done.wait()

                if job.status == JobStatus.CANCELLED:
                    return

                # Запускаем opt_critic → opt_corrector
                await self._run_optimization_review(opt_job)

                if opt_job.status == JobStatus.FAILED:
                    await self._log(
                        job,
                        f"Optimization review: {opt_job.error_message or 'ошибка'}",
                        "warn",
                        stage_override="optimization",
                    )
            except Exception as e:
                await self._log(job, f"Optimization ошибка: {e}", "error",
                                stage_override="optimization")
                self._update_pipeline_log(pid, "optimization", "error", error=str(e))

        # Запускаем параллельные задачи. Список (имя, корутина) строим динамически —
        # так метки ошибок в gather остаются корректными при любом составе.
        _specs = [
            ("findings_review", _task_findings_review()),
        ]
        if include_norms:
            # include_norms=False (флаг): нормы уйдут в последовательный прогон
            # после debt_control — тут corrector_done всё равно ставит Верификатор.
            _specs.append(("norm_verify", _task_norm_verify()))
        if include_optimization:
            _specs.append(("optimization", _task_optimization()))

        _task_names = [name for name, _ in _specs]
        tasks = [asyncio.create_task(coro) for _, coro in _specs]

        await self._log(
            job,
            "═══ Параллельный запуск: Critic"
            + (" + Нормы" if include_norms else "")
            + (" + Оптимизация" if include_optimization else "")
            + " ═══",
            stage_override="findings_review",
        )

        print(f"[{pid}] Parallel tasks created: {len(tasks)} ({', '.join(_task_names)})")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        print(f"[{pid}] Parallel tasks completed: {[type(r).__name__ if isinstance(r, Exception) else 'ok' for r in results]}")
        # Логируем ошибки из параллельных задач
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                task_name = _task_names[i] if i < len(_task_names) else f"task_{i}"
                await self._log(job, f"Параллельная задача {task_name} упала: {result}", "error",
                                stage_override="findings_review")
                print(f"[{pid}] Parallel task {task_name} exception: {result}")

        # Баг A4 аудита пайплайна: review_error выставлялся, но НИГДЕ не
        # читался — promote выполнялся безусловно, и аудит завершался
        # COMPLETED без вердиктов критика (findings уходили эксперту
        # непроверенными, а latest выглядел «как будто review прошёл»).
        if review_error:
            job.error_message = (
                job.error_message
                or "findings_review упал — вердикты критика отсутствуют"
            )
            raise RuntimeError(job.error_message)

        # coverage — в run dir job'а (класс бага B2: резолв по pid уходил в
        # v2 latest, правки затирались promote'ом run→latest).
        _cov_root, _cov_proj, _cov_out = self._resolve_job_paths(job)
        coverage = self._attach_stage02_coverage_to_findings(pid, output_dir=_cov_out)
        excluded_count = (coverage.get("summary") or {}).get("excluded_from_full_analysis_count", 0)
        if excluded_count:
            await self._log(
                job,
                f"Финальная coverage-сводка обновлена: {excluded_count} блоков вне полноценного анализа",
                "warn",
            )

        # (Evidence Verifier удалён как мёртвая подсистема — был default OFF/no-op.)

        self._promote_v2_analysis_artifacts(
            job,
            (
                "03_findings.json",
                "03_findings_review.json",
                "03_findings_pre_review.json",
                "03a_norms_verified.json",
                "norm_checks.json",
                "norm_checks_llm.json",
                "optimization.json",
                "optimization_claude.json",
                "optimization_codex.json",
                "optimization_merge_report.json",
                "optimization_review.json",
                "optimization_pre_review.json",
                "pipeline_log.json",
            ),
        )
        self._promote_completed_audit_v2(job)

    async def _run_norm_verification(
        self,
        job: AuditJob,
        standalone: bool = True,
        wait_before_fix: asyncio.Event | None = None,
    ):
        """Тонкий оркестратор: делегирует в norms/runner.py run_norm_verification.

        Оркестраторная логика (job.stage, job.status, heartbeat, cleanup)
        остаётся здесь. Бизнес-логика верификации норм — в runner.
        """
        pid = job.project_id
        try:
            job.stage = AuditStage.NORM_VERIFY
            await self._start_heartbeat(job)

            # stage_override: при standalone=False job общий с верификатором —
            # без явной секции строки норм могли краситься в findings_review.
            ctx = self._make_stage_context(job, stage_override="norm_verify")
            result = await _run_norm_verification_stage(
                ctx,
                wait_before_fix=wait_before_fix,
            )

            if result.cancelled:
                job.status = JobStatus.CANCELLED
                return

            if not result.success:
                # standalone=False: job ОБЩИЙ с critic/corrector/optimization —
                # мутация статуса здесь затирала бы их состояние (см. ниже).
                if standalone:
                    job.status = JobStatus.FAILED
                    job.error_message = result.error
                await self._log(job, f"Верификация норм: {result.error}", "error",
                                stage_override="norm_verify")
                return

            # ГОНКА (баг C1 аудита пайплайна): при standalone=False нормы
            # финишируют первыми и ставили COMPLETED на общем job, пока
            # critic/optimization ещё работают часами. Heartbeat-цикл выходит
            # по status != RUNNING → live-status видел «зомби», cleanup мог
            # убить живой аудит; а FAILED от critic затирался COMPLETED.
            # Статус общего job'а ставит только оркестратор после gather.
            if standalone:
                job.status = JobStatus.COMPLETED
            self._promote_v2_analysis_artifacts(
                job,
                (
                    "03_findings.json",
                    "03a_norms_verified.json",
                    "norm_checks.json",
                    "norm_checks_llm.json",
                    "pipeline_log.json",
                ),
            )

        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            self._update_pipeline_log(pid, "norm_verify", "error", error="Отменено")
        except Exception as e:
            if standalone:
                job.status = JobStatus.FAILED
                job.error_message = str(e)
            await self._log(job, f"Исключение: {e}", "error",
                            stage_override="norm_verify")
            self._update_pipeline_log(pid, "norm_verify", "error", error=str(e))
        finally:
            job.completed_at = datetime.now().isoformat()
            if standalone:
                self._cleanup(pid)

    async def _update_norms_db(self, job: AuditJob):
        """No-op: локальный norms_db.json больше не authoritative.

        Источник истины — Norms-main (status_index.json). Мы его не
        модифицируем и не дублируем. Метод оставлен для обратной совместимости
        вызова в _run_norm_verification — чтобы не ломать чужие forks.
        """
        await self._log(
            job,
            "norms_db.json: пропуск обновления — authoritative источник Norms-main",
            "info",
            stage_override="norm_verify",
        )

    @staticmethod
    def _enrich_norm_quotes_from_checks(output_dir: Path) -> int:
        """Обогатить findings из norm_checks.json."""
        from backend.app.pipeline.stages.norms.runner import enrich_norm_quotes_from_checks
        return enrich_norm_quotes_from_checks(output_dir)

    @staticmethod
    def _fix_paragraph_refs(output_dir: Path) -> int:
        """Исправить неверные номера пунктов норм по данным paragraph_checks."""
        from backend.app.pipeline.stages.norms.runner import fix_paragraph_refs
        return fix_paragraph_refs(output_dir)

    @staticmethod
    def _count_manual_check_flags(output_dir: Path) -> int:
        """Подсчитать количество findings с флагом [Пункт нормы ... ручной сверки]."""
        from backend.app.pipeline.stages.norms.runner import count_manual_check_flags
        return count_manual_check_flags(output_dir)

    # ─── Запуск аудита (OCR-пайплайн) ───
    async def start_audit(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        """Аудит: кроп блоков → текстовый анализ → ВСЕ блоки → свод.

        Single-start — кладёт задачу в общую очередь. Реальный запуск
        случится, когда worker возьмёт её.

        `version_id`: фиксированная версия проекта, в которую пойдут все
        write-операции. None → latest на момент enqueue.
        """
        self._assert_stage_model_config_ready()
        return await self._enqueue_single(
            project_id, action="full", version_id=version_id,
        )

    # Legacy aliases
    start_standard_audit = start_audit
    start_pro_audit = start_audit

    async def _run_block_retry(
        self,
        job: AuditJob,
        pid: str,
        project_info: dict,
        output_dir: Path,
    ) -> None:
        """Перекроп нечитаемых блоков с увеличенным разрешением и повторный анализ.

        Собирает все блоки с unreadable_text=true из 01_blocks_analysis.json,
        перекропает их (до MAX_RECROP_ITERATIONS раз, ×2 на итерации),
        создаёт мини-батч только для них и повторно прогоняет через блок-анализ
        (Gemini через OpenRouter). Результаты merge'атся поверх существующего
        01_blocks_analysis.json — перезаписываются только затронутые block_id.

        При ошибке скриптов/CLI логируем warn и продолжаем: unreadable=true
        сохраняется, пайплайн идёт дальше на findings_merge.
        """
        from backend.app.pipeline.stages.crop_blocks.blocks import (
            find_unreadable_blocks,
            recrop_blocks,
            promote_to_full,
            MAX_RECROP_ITERATIONS,
        )

        _index_path = output_dir / "blocks" / "index.json"
        _is_compact = False
        if _index_path.exists():
            try:
                with open(_index_path, "r", encoding="utf-8") as f:
                    _idx = json.load(f)
                _is_compact = _idx.get("compact", False)
            except Exception:
                pass

        max_retry = 1 if _is_compact else MAX_RECROP_ITERATIONS
        had_unreadable = False

        # Version-aware path для V1/V2 — все retry-helper'ы и BLOCKS_SCRIPT
        # должны видеть тот же `version_dir`, что и остальные стадии.
        proj_path = self._project_path_for_job(job)

        for retry_iter in range(1, max_retry + 1):
            unreadable = find_unreadable_blocks(proj_path)
            if not unreadable:
                if retry_iter == 1:
                    await self._log(job, "Block retry: все блоки читаемы, пропуск")
                break

            had_unreadable = True
            block_ids = [u["block_id"] for u in unreadable]

            if _is_compact:
                await self._log(job, f"Block retry: {len(block_ids)} нечитаемых → promote compact→full")
                self._update_pipeline_log(pid, "block_retry", "running",
                                          message=f"Promote {len(block_ids)} блоков")
                # to_thread: внутри — fitz-рендер PDF (sync). Прямой вызов
                # блокировал event loop → health-check /api/info молчал →
                # вотчдог убивал живой бэкенд (инциденты 03.07).
                promote_result = await asyncio.to_thread(
                    promote_to_full, proj_path, block_ids
                )
                if promote_result.get("promoted", 0) == 0:
                    await self._log(job, "Block retry: нет full-версий для промоута")
                    break
            else:
                await self._log(job, f"Block retry (итерация {retry_iter}): {len(block_ids)} нечитаемых блоков → перекачка ×2")
                self._update_pipeline_log(pid, "block_retry", "running",
                                          message=f"Итерация {retry_iter}: {len(block_ids)} блоков")
                # to_thread: внутри — urllib-скачивание с retry/backoff
                # (time.sleep) и fitz-рендер, всё синхронное. На большом
                # наборе блоков блокировало event loop на десятки секунд.
                recrop_result = await asyncio.to_thread(
                    recrop_blocks, proj_path, block_ids, scale_multiplier=2.0
                )
                if recrop_result.get("recropped", 0) == 0:
                    await self._log(job, "Block retry: все блоки уже на максимальном разрешении, стоп")
                    break

            exit_code, _, _ = await self._run_script_for_job(
                job, str(BLOCKS_SCRIPT),
                # --solo: 1 блок = 1 пакет, модель фокусируется на одной картинке
                # (retry именно по ней и шёл, контекст других блоков уже есть в 01_blocks_analysis.json)
                ["batches", proj_path, "--block-ids", ",".join(block_ids), "--solo"],
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code != 0:
                await self._log(job, "Block retry: ошибка создания пакетов", "warn")
                break

            batches_file = output_dir / "block_batches.json"
            if batches_file.exists():
                with open(batches_file, "r", encoding="utf-8") as f:
                    retry_batches_data = json.load(f)
                runtime_path = output_dir / RUNTIME_BATCHES_FILE
                previous_runtime_text = runtime_path.read_text(encoding="utf-8") if runtime_path.exists() else None
                retry_runtime_plan = _write_single_block_runtime_plan(
                    output_dir,
                    retry_batches_data.get("batches", []),
                    source="expanded_from_block_retry_batches",
                )
                retry_batches = retry_runtime_plan.get("batches", [])
                retry_total = len(retry_batches)
                retry_failed: list[dict] = []

                for rb in retry_batches:
                    batch_id = rb.get("batch_id", 0)
                    old_result = output_dir / f"block_batch_{batch_id:03d}.json"
                    if old_result.exists():
                        old_result.unlink()

                    can_go = await self._check_before_launch(job)
                    if not can_go:
                        break

                    exit_code, output, cli_result = await claude_runner.run_block_batch(
                        rb, project_info, pid, retry_total,
                    )
                    self._record_cli_usage(job, cli_result, f"block_retry_iter{retry_iter}")
                    if exit_code != 0:
                        err_detail = _extract_error_detail(exit_code, output or "", max_len=160)
                        retry_failed.append(
                            _runtime_batch_failure_entry(
                                rb, err_detail, reason="block_retry_failed",
                            )
                        )
                        await self._log(job, f"Block retry batch {batch_id}: ошибка (код {exit_code})", "warn")

                _write_block_analysis_runtime_summary(
                    output_dir,
                    retry_runtime_plan,
                    failed_batches=retry_failed,
                    completed_batches=max(0, retry_total - len(retry_failed)),
                )
                if previous_runtime_text is not None:
                    runtime_path.write_text(previous_runtime_text, encoding="utf-8")
                elif runtime_path.exists():
                    runtime_path.unlink()

            exit_code, _, _ = await self._run_script_for_job(
                job, str(BLOCKS_SCRIPT),
                ["merge", proj_path],
                on_output=lambda msg: self._log(job, msg),
            )
            await self._log(job, f"Block retry итерация {retry_iter}: merge завершён")

        final_unreadable = find_unreadable_blocks(proj_path)
        if had_unreadable:
            if final_unreadable:
                self._update_pipeline_log(pid, "block_retry", "done",
                                          message=f"Осталось {len(final_unreadable)} нечитаемых (макс разрешение)")
            else:
                self._update_pipeline_log(pid, "block_retry", "done", message="OK")
        else:
            self._update_pipeline_log(pid, "block_retry", "skipped",
                                      message="Все блоки читаемы")

    @staticmethod
    def _blocks_before_text_enabled() -> bool:
        """Флаг порядка: блоки (Stage 01) перед текстом (Stage 02)."""
        from backend.app.core import config as cfg
        return bool(getattr(cfg, "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", False))

    @staticmethod
    def _norms_after_merge_enabled() -> bool:
        """Флаг: norm_verify вне параллельного блока, ПОСЛЕ merge/stable-id.

        ON → нормы верифицируются против финальных findings (см. config).
        Действует только на полный аудит (_run_ocr_pipeline); resume — легаси.
        """
        from backend.app.core import config as cfg
        return bool(getattr(cfg, "PIPELINE_NORMS_AFTER_MERGE_ENABLED", False))

    def _write_blocks_for_text_compact(self, output_dir: Path) -> None:
        """Записать компактный view 02 (01_blocks_for_text.json) для текстового этапа. Fail-soft."""
        try:
            from backend.app.pipeline.stages.block_analysis.blocks_for_text import (
                write_blocks_for_text_compact,
            )
            write_blocks_for_text_compact(Path(output_dir))
        except Exception:
            pass

    async def _ocr_block_analysis_and_retry(
        self, job: AuditJob, pid: str, project_info: dict, output_dir: Path,
    ) -> None:
        """Stage 01: анализ блоков (findings_only) + block_retry + promote 01.

        При PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED дополнительно пишет компактный view
        01_blocks_for_text.json (после retry — уже финальный 02) для текстового этапа.
        Единый источник для обоих порядков (block→text и text→block).
        """
        if get_stage_batch_mode("block_batch") == BLOCK_BATCH_MODE_FINDINGS_ONLY:
            # findings_only_block_context: single-block GPT-5.4 + PDF/Vectograph context.
            # Пишет 01_blocks_analysis.json напрямую, без block_batches.json.
            await self._run_block_analysis_findings_only(job)
            if job.status == JobStatus.CANCELLED:
                return
            if job.status == JobStatus.FAILED:
                # Иначе провал Stage 01 (paid_api_guard / «все блоки упали» /
                # result.success=False) молча проглатывается: следующая стадия
                # перетирает статус RUNNING и аудит доезжает до COMPLETED без
                # ~60% визуальных замечаний. Роняем job штатно, как text/merge.
                raise RuntimeError(
                    job.error_message or "Stage 01 (block_analysis) failed"
                )
            self.active_jobs[pid] = job
            self._tasks[pid] = asyncio.current_task()

        if job.status == JobStatus.CANCELLED:
            return

        # Re-register
        self.active_jobs[pid] = job
        self._tasks[pid] = asyncio.current_task()

        # ═══ Block Retry — перекачка нечитаемых блоков ═══
        await self._run_block_retry(job, pid, project_info, output_dir)

        promote = [
            BLOCKS_ANALYSIS_FILENAME,
            "block_analysis_summary.json",
            RUNTIME_BATCHES_FILE,
            "pipeline_log.json",
        ]
        # Порядок block→text: компактный view финальных блоков для текстового этапа.
        if self._blocks_before_text_enabled():
            self._write_blocks_for_text_compact(output_dir)
            promote.append(BLOCKS_FOR_TEXT_FILENAME)
        self._promote_v2_analysis_artifacts(job, tuple(promote))

    async def _run_ocr_pipeline(self, job: AuditJob, include_optimization: bool = True):
        """
        Полный аудит блоков.

        Этапы:
        1. Stage 01 crop → _output/blocks_stage02_100/
        2. Локальный PDF/Vectograph context → block_context_summary.json
        3. findings_only_block_context → 01_blocks_analysis.json
        4. Claude: text_analysis → 02_text_analysis.json
        5. Claude: findings_merge → 03_findings.json
        6. norm_verify
        7. Excel
        """
        start_time = datetime.now()
        pid = job.project_id
        output_root_token = None
        try:
            # Version-aware пути: V1 = root, projects_v2 = versions/<vid>/.
            _root_dir, project_dir, output_dir = self._resolve_job_paths(job)
            from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import bind_output_root
            output_root_token = bind_output_root(output_dir)
            info_path = self._project_info_path_for_paths(_root_dir, project_dir)
            project_info = self._load_project_info_for_paths(pid, _root_dir, project_dir)

            # ═══ Проверка MD-файла (version-aware, обязательный источник текста) ═══
            self._require_project_md(pid, _root_dir, project_dir, info_path)

            # ═══ ЭТАП 1: Кроп image-блоков ═══
            job.stage = AuditStage.CROP_BLOCKS
            self._update_pipeline_log(pid, "crop_blocks", "running")
            await self._ensure_stage02_crops(job)
            self._update_pipeline_log(pid, "crop_blocks", "done", message="Stage 01 crops ready")

            # Построить document_graph v2 (Python, без LLM)
            await self._build_document_graph_v2(job)

            if job.status == JobStatus.CANCELLED:
                return

            # Локальная подготовка PDF/Vectograph контекста.
            await self._run_gemma_enrichment_stage(job)

            if job.status == JobStatus.CANCELLED:
                return

            # Усиление предобработки: Value Grounding (вектор-сверка). OFF по умолчанию, fail-soft.
            await self._run_block_grounding_stage(job)

            # ═══ Порядок block↔text по флагу PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED ═══
            blocks_before_text = self._blocks_before_text_enabled()

            if blocks_before_text:
                # Новый порядок: блоки (GPT) + retry ПЕРЕД текстом; текст читает компактный view 02.
                await self._ocr_block_analysis_and_retry(job, pid, project_info, output_dir)
                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══
            files_to_clean = [
                *TEXT_ANALYSIS_ALL_NAMES,
                "03_findings.json", "03_findings_review.json", "03_findings_pre_review.json",
            ]
            if not blocks_before_text:
                # Старый порядок: текст первый — чистим и блочные артефакты (пересоберутся ниже).
                files_to_clean += [
                    "block_batch_*.json", "block_batches.json", RUNTIME_BATCHES_FILE,
                    "block_analysis_summary.json", *BLOCKS_ANALYSIS_ALL_NAMES,
                ]
            # Полный аудит перенумерует F-ID — бэкап findings + слепок вердиктов.
            self._backup_findings_before_restart(pid)
            self._clean_stage_files(pid, files_to_clean)
            self._reset_job_progress(job)
            job.stage = AuditStage.TEXT_ANALYSIS
            job.status = JobStatus.RUNNING
            print(f"[{pid}] ═══ ЭТАП 2: Текстовый анализ MD ═══")
            await self._log(job, "═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══")
            await self._start_heartbeat(job)

            _ta_result = await _run_text_analysis_stage(
                self._make_stage_context(job),
                with_rate_limit_retry=True,
            )
            if _ta_result.cancelled:
                job.status = JobStatus.CANCELLED
                return
            if not _ta_result.success:
                # rate_limit_exhausted + pause_on_rate_limit: не давать очереди
                # бесконтрольно стартовать следующий проект — ставим паузу.
                if (
                    _ta_result.data
                    and _ta_result.data.get("pause_on_rate_limit")
                    and not self._paused
                ):
                    await self._log(
                        job,
                        "⏸ Очередь на паузе: rate_limit_exhausted "
                        "(TEXT_ANALYSIS_PAUSE_ON_RATE_LIMIT=true)",
                        "warn",
                    )
                    await self.pause("finish_current")
                raise RuntimeError(_ta_result.error or "Текстовый анализ: ошибка")

            self._promote_v2_analysis_artifacts(
                job, (TEXT_ANALYSIS_FILENAME, "pipeline_log.json")
            )
            print(f"[{pid}] ЭТАП 2 OK")

            if job.status == JobStatus.CANCELLED:
                return

            if not blocks_before_text:
                # Legacy-порядок: блоки (Stage 01 artifact) + retry после текста.
                await self._ocr_block_analysis_and_retry(job, pid, project_info, output_dir)
                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ ЭТАП 6: Свод замечаний ═══
            self._reset_job_progress(job)
            job.stage = AuditStage.FINDINGS_MERGE
            job.status = JobStatus.RUNNING
            print(f"[{pid}] ═══ ЭТАП 6: Свод замечаний ═══")
            _fm_result = await _run_findings_merge_stage(self._make_stage_context(job))
            if _fm_result.cancelled:
                job.status = JobStatus.CANCELLED
                return
            if not _fm_result.success:
                raise RuntimeError(_fm_result.error or "Свод замечаний: ошибка")

            self._promote_v2_analysis_artifacts(
                job, ("03_findings.json", "pipeline_log.json")
            )

            # Перепривязать вердикты эксперта к новым F-ID (fail-soft).
            await self._run_verdict_rehydration(job)

            # «Размышление модели»: стрим найденных замечаний в live-лог (WS)
            await self._stream_findings_events(job, "merge")

            if job.status == JobStatus.CANCELLED:
                return

            # Re-register
            self.active_jobs[pid] = job
            self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАПЫ 6.5-7-OPT: Параллельный запуск после findings_merge ═══
            # Critic+Corrector, Norm verify и Optimization — независимы.
            # Optimization_critic ждёт corrector (нужны финальные findings).
            # output_dir уже version-aware (см. начало _run_ocr_pipeline).
            #
            # Флаг PIPELINE_NORMS_AFTER_MERGE_ENABLED (default OFF): norm_verify
            # выходит из параллели и запускается ПОСЛЕ debt_control (merge/stable-id) —
            # тогда нормы верифицируются против финальных F-ID. optimization остаётся
            # параллельным. Порядок (флаг ON):
            #   Верификатор ∥ optimization → debt_control → нормы → carryover.
            norms_after_merge = self._norms_after_merge_enabled()
            findings_path = output_dir / "03_findings.json"
            if findings_path.exists():
                await self._run_post_findings_parallel(
                    job, project_info,
                    include_optimization=include_optimization,
                    include_norms=not norms_after_merge,
                )

                if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                    return

                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()

                # Вердикты по оптимизациям: optimization.json пересобран внутри
                # post-findings блока — восстановить их можно только теперь.
                if include_optimization:
                    await self._run_verdict_rehydration(job, item_types=("optimization",))
            else:
                await self._log(job, "03_findings.json не найден — пропуск верификации", "warn")

            # ═══ ЭТАП 7.7: Перенос вердиктов из предыдущей версии ═══
            # debt_control (merge-similar + stable-id) ДО норм при флаге ON —
            # чтобы нормы легли на финальные, стабильные F-ID.
            await self._run_debt_control(job)

            if norms_after_merge:
                if findings_path.exists() and job.status not in (
                    JobStatus.CANCELLED, JobStatus.FAILED,
                ):
                    await self._log(
                        job,
                        "═══ Верификация норм (последовательно, после merge/stable-id) ═══",
                    )
                    # standalone=False: НЕ ставит COMPLETED и не делает _cleanup —
                    # впереди ещё carryover и excel. Верификатор уже завершён →
                    # wait_before_fix не нужен (corrector_done уже наступил).
                    await self._run_norm_verification(
                        job, standalone=False, wait_before_fix=None,
                    )
                    if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                        return

            await self._run_decision_carryover(job)

            # ═══ ЭТАП 8: Excel ═══
            self._reset_job_progress(job)
            job.stage = AuditStage.EXCEL
            job.status = JobStatus.RUNNING
            print(f"[{pid}] ═══ ЭТАП 8: Excel ═══")
            from backend.app.pipeline.stages.report.runner import run_excel_report as _run_excel
            _xls_result = await _run_excel(self._make_stage_context(job))
            if not _xls_result.success:
                await self._log(job, f"Excel-отчёт не создан: {_xls_result.error}", "warn")

            wall_sec = (datetime.now() - start_time).total_seconds()
            net_sec = max(0, wall_sec - job.pause_total_sec)
            duration = round(net_sec / 60, 1)
            job.status = JobStatus.COMPLETED
            self._promote_completed_audit_v2(job)
            pause_note = f" (паузы: {round(job.pause_total_sec / 60, 1)} мин)" if job.pause_total_sec > 60 else ""
            print(f"[{pid}] ═══ Аудит завершён за {duration} мин{pause_note} ═══")
            await self._log(job, f"Аудит завершён за {duration} мин{pause_note}.", "info")
            await self._log_stage_degradations(job)

            await ws_manager.broadcast_to_project(
                pid, WSMessage.complete(pid, duration_minutes=duration,
                                        pause_minutes=round(job.pause_total_sec / 60, 1)),
            )

        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
        except Exception as e:
            import traceback
            traceback.print_exc()
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            await self._log(job, f"Исключение: {e}", "error")
        finally:
            if output_root_token is not None:
                try:
                    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import unbind_output_root
                    unbind_output_root(output_root_token)
                except Exception:
                    pass
            job.completed_at = datetime.now().isoformat()
            self._cleanup(pid)

    # ─── Запуск ВСЕХ проектов последовательно ───
    # ─── Batch (групповые действия для выбранных проектов) ───

    _batch_queue: Optional[BatchQueueStatus] = None

    async def start_batch(
        self,
        project_ids: list[str],
        action: str,
    ) -> BatchQueueStatus:
        """Запустить групповое действие для списка проектов.

        Дописывает items в общую очередь (создавая её если нужно). Если в
        очереди уже есть single-task'и от `start_audit`, новые items
        добавляются после них — всё бежит последовательно.
        """
        if self.is_running("__ALL__"):
            raise RuntimeError("Запуск всех проектов уже выполняется")
        if not project_ids:
            raise RuntimeError("Список проектов пуст")

        async with self._enqueue_lock:
            existing_pending = set()
            if self._batch_queue and self._batch_queue.status == "running":
                existing_pending = {
                    it.project_id for it in self._batch_queue.items
                    if it.status in ("pending", "running")
                }

            # Не дублируем те, что уже стоят в очереди или активно работают
            filtered = [
                pid for pid in project_ids
                if pid not in existing_pending and pid not in self.active_jobs
            ]
            # Версионность очереди: фиксируем effective_version_id на каждый
            # item ОДИН раз при enqueue (как это делает _enqueue_single для
            # single-start). Иначе version_id=None заставляет prefetch/resolve
            # резолвить проект к PRIMARY (V1) папке, и для проекта, чья latest =
            # V2, V1-состояние «протекает» в V2-прогон (Gemma помечается как
            # prefetched по V1 → main worker пропускает её для V2).
            from backend.app.services.common import version_service as _vs
            new_items = []
            for pid in filtered:
                try:
                    eff_vid = _vs.resolve_effective_version_id(
                        resolve_project_dir(pid), pid, None,
                    )
                except Exception:
                    eff_vid = None
                new_items.append(
                    BatchQueueItem(
                        project_id=pid,
                        version_id=eff_vid,
                        action=action,
                        status="pending",
                        job_id=str(uuid4()),
                    )
                )

            queue = self._ensure_batch_worker(action_for_label=action)
            queue.items.extend(new_items)
            queue.total = len(queue.items)

            meta_job = self.active_jobs.get("__BATCH__")
            if meta_job:
                meta_job.progress_total = queue.total

        await self._broadcast_batch_progress(queue)
        return queue

    # ─── Pre-crop: фоновая загрузка блоков для следующих проектов в очереди ───

    async def _precrop_project(self, pid: str, version_id: Optional[str] = None) -> bool:
        """Кроп блоков для одного проекта (фоновая задача). Возвращает True при успехе.

        Version-aware: использует папку активной версии (V1 = root, V2+ = _versions/v{N}/).
        """
        try:
            root_dir = resolve_project_dir(pid)
            # Резолвим версионную папку — для V1 это root, для V2+ это _versions/v{N}/.
            from backend.app.services.common import version_service as _vs
            try:
                proj_dir = _vs.get_version_dir(root_dir, pid, version_id)
            except Exception:
                proj_dir = root_dir

            # Пропустить если блоки уже есть
            index_file = stage02_blocks_index_path(proj_dir)
            if index_file.exists() and _existing_crop_matches_policy(
                index_file, stage02_crop_policy()
            ):
                print(f"[PRE-CROP] {pid} ({version_id}): блоки уже есть, пропуск")
                return True
            # Пропустить если нет result.json (не OCR-проект)
            if not _has_ocr_result_json(proj_dir):
                return False

            print(f"[PRE-CROP] {pid} ({version_id}): начинаю фоновый кроп блоков...")
            await ws_manager.broadcast_global(
                WSMessage.log("__BATCH__", f"  ⚡ Pre-crop: {pid} ({version_id})", "info")
            )
            # Путь для blocks.py выводим из ТОГО ЖЕ proj_dir, что и проверки выше —
            # единый источник истины. Иначе version_id=None (latest) рассинхронил бы
            # check-dir (get_version_dir→latest) и crop-path (_project_path→V1 root).
            try:
                crop_rel_path = str(proj_dir.relative_to(BASE_DIR))
            except ValueError:
                crop_rel_path = str(proj_dir)
            exit_code, _, stderr = await run_script(
                str(BLOCKS_SCRIPT),
                _build_crop_args(
                    crop_rel_path,
                    policy=stage02_crop_policy(),
                    output_dir_name=STAGE02_BLOCKS_DIRNAME,
                ),
                project_id=f"__PRECROP_{pid}__",
            )
            if exit_code == 0:
                print(f"[PRE-CROP] {pid} ({version_id}): OK")
                return True
            else:
                print(f"[PRE-CROP] {pid} ({version_id}): ошибка (код {exit_code})")
                return False
        except Exception as e:
            print(f"[PRE-CROP] {pid} ({version_id}): исключение: {e}")
            return False

    def _select_precrop_candidate(
        self,
        queue: BatchQueueStatus,
        precropped: set[tuple[str, Optional[str]]],
    ) -> Optional[BatchQueueItem]:
        """Следующий pending-проект для pre-crop в окне lookahead (или None).

        Используется bounded lookahead до
        BATCH_PRECROP_WINDOW проектов вперёд от current_index, в порядке очереди
        (ближайший pending первым → предсказуемое опережение B → C → D). Skip-ahead
        тихо пропускает проекты без result.json (OCR ещё не готов), не «залипая».
        Чистая (без async/IO над состоянием очереди) — детерминированно тестируема.
        """
        from backend.app.services.common import version_service as _vs

        running_idx = queue.current_index
        for idx, item in enumerate(queue.items):
            if item.status != "pending":
                continue
            # Bounded lookahead: не уезжаем дальше окна впереди running.
            # distance может быть 0, когда очередь ещё не стартовала и
            # current_index указывает на сам pending item — это валидно (кропим).
            if idx - running_idx > BATCH_PRECROP_WINDOW:
                break  # дальше items только ещё дальше — выходим из скана
            dedup_key = (item.project_id, item.version_id)
            if dedup_key in precropped:
                continue
            action = item.action or queue.action
            if action == "optimization":
                continue  # оптимизация не нуждается в кропе
            # Ищем result.json в папке ВЕРСИИ, а не в V1-root.
            root_dir = resolve_project_dir(item.project_id)
            try:
                item_dir = _vs.get_version_dir(root_dir, item.project_id, item.version_id)
            except Exception:
                item_dir = root_dir
            if _has_ocr_result_json(item_dir):
                return item
        return None

    async def _run_precrop_loop(self, queue: BatchQueueStatus):
        """Фоновый цикл: кропит блоки для pending-проектов из очереди.

        Lookahead bounded: готовит только проекты в пределах BATCH_PRECROP_WINDOW
        вперёд от current_index. Опережение предсказуемо (ровно N следующих
        pending) и не racing'ует всю очередь, конкурируя с основным pipeline.
        Окно скользит вместе с current_index — далёкие проекты кропятся just-in-time.
        """
        # Ключ дедупа: (project_id, version_id) — V1 и V2 одного проекта независимы.
        precropped: set[tuple[str, Optional[str]]] = set()
        while queue.status == "running":
            target_item = self._select_precrop_candidate(queue, precropped)

            if not target_item:
                # Нет проектов для pre-crop в окне, подождём и проверим снова
                # (окно сдвинется когда main advance'нёт current_index).
                await asyncio.sleep(5)
                continue

            precropped.add((target_item.project_id, target_item.version_id))
            await self._precrop_project(target_item.project_id, target_item.version_id)
            # Небольшая пауза между кропами
            await asyncio.sleep(1)

    async def _run_batch_queue(self, queue: BatchQueueStatus, meta_job: AuditJob):
        """Последовательная обработка очереди проектов."""
        precrop_task = None
        try:
            await ws_manager.broadcast_global(
                WSMessage.log(
                    "__BATCH__",
                    f"═══ Групповое действие ({queue.action}) для {queue.total} проектов ═══",
                    "info",
                )
            )

            # Локальная подготовка контекста не требует отдельного prefetch.
            # Оставляем только pre-crop для следующих pending проектов.
            if queue.total > 1:
                precrop_task = asyncio.create_task(self._run_precrop_loop(queue))

            idx = 0
            while True:
                # Проверяем условие выхода под локом, чтобы _enqueue_single
                # не успел дописать item в момент перехода в "completed".
                if idx >= len(queue.items):
                    async with self._enqueue_lock:
                        if idx >= len(queue.items):
                            queue.status = "completed"
                            break
                    # под локом увидели свежие items — продолжаем цикл

                item = queue.items[idx]
                if item.status in ("completed", "failed", "skipped", "cancelled"):
                    idx += 1
                    continue
                if item.status == "interrupted":
                    item.status = "pending"

                if queue.status == "cancelled":
                    item.status = "cancelled"
                    idx += 1
                    continue

                # Проверка паузы перед следующим проектом
                if self._paused:
                    await self._log(
                        meta_job,
                        f"⏸ Очередь на паузе (перед проектом {idx + 1}/{queue.total})",
                        "warn",
                    )
                    await self._pause_event.wait()
                    await self._log(meta_job, "▶ Очередь продолжена", "info")

                queue.current_index = idx
                meta_job.progress_current = idx
                item.status = "running"
                # Тайминги item'а: при повторном запуске (resume interrupted)
                # перезаписываем — показываем фактический последний прогон.
                item.started_at = time.time()
                item.finished_at = None

                pid = item.project_id
                print(f"[BATCH] ▶ Проект {idx + 1}/{queue.total}: {pid} ({queue.action})")
                await ws_manager.broadcast_global(
                    WSMessage.log("__BATCH__", f"▶ Проект {idx + 1}/{queue.total}: {pid}", "info")
                )
                await self._broadcast_batch_progress(queue)

                # Пропуск уже запущенных
                if self.is_running(pid):
                    item.status = "skipped"
                    item.finished_at = time.time()
                    item.error = "Уже выполняется"
                    await ws_manager.broadcast_global(
                        WSMessage.log("__BATCH__", f"  ⏭ Пропуск {pid}: уже выполняется", "warn")
                    )
                    idx += 1
                    continue

                # version_id зафиксирован на момент enqueue (см. _enqueue_single).
                # Закрепляем его в ContextVar на весь срок жизни этого job —
                # любые service-функции внутри pipeline, которые читают
                # bind_version, увидят правильную версию.
                from backend.app.services.common import version_service
                version_token = version_service.bind_version(item.version_id)
                object_token = None
                try:
                    item_object_id = self._resolve_object_id_for_project(
                        None, pid, item.version_id,
                    )
                    if item_object_id:
                        object_token = bind_object(item_object_id)
                    job = AuditJob(
                        job_id=item.job_id or str(uuid4()),
                        object_id=item_object_id,
                        project_id=pid,
                        version_id=item.version_id,
                        stage=AuditStage.PREPARE,
                        status=JobStatus.RUNNING,
                        started_at=datetime.now().isoformat(),
                    )
                    self.active_jobs[pid] = job
                    self._tasks[pid] = asyncio.current_task()

                    await self._dispatch_action(item, job, default_action=queue.action)

                    if job.status == JobStatus.COMPLETED:
                        item.status = "completed"
                        queue.completed += 1
                        # projects_v2 dual-write: зеркалим ПОЛНЫЙ проект (включая
                        # late-stage artifacts — 03_findings/optimization/нормы и
                        # финальный pipeline_log) после ЗАВЕРШЕНИЯ всего конвейера.
                        # Это единая точка завершения любого action (full/resume/
                        # retry/optimization), поэтому раньше v2-снимок обрывался
                        # на block_analysis (мирор внутри block-стадии), а поздние
                        # этапы в v2 не попадали. no-op в legacy-режиме, fail-soft.
                        self._shadow_mirror_completed_audit(pid, job)
                        await ws_manager.broadcast_global(
                            WSMessage.log("__BATCH__", f"  ✓ {pid}: завершён", "info")
                        )
                    elif job.status == JobStatus.CANCELLED:
                        item.status = "cancelled"
                        item.error = job.error_message or "cancelled"
                        await ws_manager.broadcast_global(
                            WSMessage.log("__BATCH__", f"  ⊘ {pid}: отменён", "warn")
                        )
                    else:
                        item.status = "failed"
                        item.error = job.error_message or job.status.value
                        queue.failed += 1
                        await ws_manager.broadcast_global(
                            WSMessage.log("__BATCH__", f"  ✗ {pid}: {job.status.value}", "error")
                        )

                except Exception as e:
                    if job.status == JobStatus.CANCELLED:
                        # Остановлено пользователем во время этапа — это не сбой.
                        item.status = "cancelled"
                        item.error = "Остановлено пользователем"
                        await ws_manager.broadcast_global(
                            WSMessage.log("__BATCH__", f"  ⊘ {pid}: остановлен", "warn")
                        )
                    else:
                        item.status = "failed"
                        item.error = str(e)
                        queue.failed += 1
                        import traceback
                        traceback.print_exc()
                        await ws_manager.broadcast_global(
                            WSMessage.log("__BATCH__", f"  ✗ {pid}: исключение: {e}", "error")
                        )
                finally:
                    # Фиксируем время окончания для любого терминального статуса
                    if item.status in ("completed", "failed", "cancelled", "skipped"):
                        item.finished_at = time.time()
                    self._stop_heartbeat(pid)
                    self.active_jobs.pop(pid, None)
                    self._tasks.pop(pid, None)
                    # Итог item'а — на диск сразу: иначе он живёт только в
                    # памяти (persist был лишь side-effect prefetch-тасков),
                    # и kill бэкенда в окне терял completed/failed-статусы.
                    self._persist_queue()
                    await self._broadcast_batch_progress(queue)
                    # Снимаем bind_object/bind_version, выставленные перед dispatch
                    try:
                        if object_token is not None:
                            unbind_object(object_token)
                    except Exception:
                        pass
                    try:
                        version_service.unbind_version(version_token)
                    except Exception:
                        pass

                idx += 1

            # Итог (queue.status уже выставлен в "completed" под локом выше)
            meta_job.progress_current = queue.total
            meta_job.status = JobStatus.COMPLETED

            await ws_manager.broadcast_global(
                WSMessage.log(
                    "__BATCH__",
                    f"═══ Групповое действие завершено: {queue.completed}/{queue.total} OK, "
                    f"{queue.failed} ошибок ═══",
                    "info",
                )
            )
            await self._broadcast_batch_progress(queue, complete=True)

        except Exception as e:
            meta_job.status = JobStatus.FAILED
            # Не оставлять текущий item в 'running' — иначе UI вечно покажет
            # «Выполняется» при мёртвом воркере.
            for _it in queue.items:
                if _it.status == "running":
                    _it.status = "failed"
                    if not _it.error:
                        _it.error = f"Сбой воркера очереди: {e}"
                    queue.failed += 1
            # Раньше здесь безусловно ставился 'completed' — при живых pending
            # это делало очередь невозобновимой (resume требует interrupted),
            # и остаток батча застревал навсегда. Честный статус: interrupted,
            # если есть кого продолжать.
            has_pending = any(_it.status == "pending" for _it in queue.items)
            queue.status = "interrupted" if has_pending else "completed"
            print(f"[BATCH] КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Зафиксировать финальное состояние очереди на диске: до этого
            # persist происходил только как side-effect prefetch-тасков, и
            # kill процесса в окне оставлял batch_queue.json со стейл-статусами.
            self._persist_queue()
            # Остановить фоновый pre-crop.
            for _task in (precrop_task,):
                if _task is not None and not _task.done():
                    _task.cancel()
                    try:
                        await _task
                    except (asyncio.CancelledError, Exception):
                        pass
            # Identity-aware: не сносим регистрацию нового worker'а, если enqueue
            # успел поднять его пока мы доходили до finally (гонка close+enqueue).
            self._cleanup_batch_worker(meta_job)

    # ─── Единый dispatcher action'ов ───────────────────────────────────
    async def _dispatch_action(
        self,
        item: BatchQueueItem,
        job: AuditJob,
        default_action: str = "full",
    ) -> None:
        """Выполнить action из item, мутируя job на месте.

        Caller (`_run_batch_queue`) уже зарегистрировал job в active_jobs и
        обработает cleanup. Этот метод — единая точка диспетчеризации:
        kill зомби-процессов + сброс usage/audit_log + cleanup stage-файлов
        + вызов соответствующего `_run_*` пайплайна.

        Любой single-start (start_audit, ...) проходит
        через очередь и попадает сюда же — `start_*` не запускают coroutine
        самостоятельно.
        """
        pid = job.project_id
        action = item.action or default_action or "full"
        extra = item.extra_params or {}

        # Pre-action cleanup — убить зомби от прошлых запусков того же проекта
        try:
            killed = await kill_all_processes(pid)
            if killed:
                print(f"[{pid}] Убито {killed} зомби-процессов от предыдущего запуска")
        except Exception as e:
            print(f"[{pid}] kill_all_processes исключение: {e}")

        # Сброс audit-log/usage только для свежих прогонов (не retry/resume/optimization-only)
        fresh_actions = {
            "full", "audit", "standard", "pro",
            "audit+optimization", "standard+optimization", "pro+optimization",
            "main_audit", "tile_audit", "prepare",
        }
        if action in fresh_actions and not item.retry_stage:
            try:
                usage_tracker.clear_project_usage(pid)
            except Exception:
                pass
            try:
                await audit_logger.reset_audit_log_async(pid)
            except Exception:
                pass

        snapshot_actions = fresh_actions | {"optimization", "optimization_review", "norm_verify"}
        if not item.retry_stage and action in snapshot_actions:
            self._snapshot_output_before_codex_run(job, action)

        # Per-action cleanup stage-файлов (миррор старых start_* helpers)
        if not item.retry_stage:
            if action == "main_audit":
                self._clean_stage_files(pid, [
                    "00_init.json", *TEXT_ANALYSIS_ALL_NAMES, "03_findings.json",
                ])
                job.stage = AuditStage.MAIN_AUDIT
            elif action == "norm_verify":
                self._clean_stage_files(pid, [
                    "03a_norms_verified.json", "norm_checks.json", "norm_checks_llm.json",
                    "missing_norms_queue.json", "missing_norms_report.json",
                    "missing_norms_queue.md",
                ])
                job.stage = AuditStage.NORM_VERIFY
            elif action == "optimization":
                self._clean_stage_files(pid, ["optimization.json"])
                job.stage = AuditStage.OPTIMIZATION
            elif action == "optimization_review":
                # Version-aware: V1 = root/_output, V2+ = _versions/v{N}/_output.
                _root_dir, _proj_dir, _output_dir = self._resolve_job_paths(job)
                opt_path = _output_dir / "optimization.json"
                if not opt_path.exists():
                    job.status = JobStatus.FAILED
                    job.error_message = "optimization.json не найден — сначала запустите оптимизацию"
                    job.completed_at = datetime.now().isoformat()
                    return
                self._clean_stage_files(pid, [
                    "optimization_review.json", "optimization_pre_review.json",
                ])
                job.stage = AuditStage.OPTIMIZATION
            elif action == "tile_audit":
                job.stage = AuditStage.TILE_AUDIT

        # ── Dispatch ───
        if item.retry_stage:
            stage_label = {
                "prepare": "Кроп блоков",
                "gemma_enrichment": GEMMA_STAGE_LABEL,
                "text_analysis": "Анализ текста",
                "block_analysis": "Анализ блоков",
                "findings_merge": "Свод замечаний",
                "findings_review": "Проверка замечаний",
                "norm_verify": "Верификация норм",
                "optimization": "Оптимизация",
                "optimization_review": "Проверка оптимизации",
                "debt_control": "Контроль долгов",
                "decision_carryover": "Перенос вердиктов",
                "excel": "Excel-отчёт",
            }.get(item.retry_stage, item.retry_stage)

            if item.retry_stage == "optimization":
                await self._log(job, f"▶ Повтор: {stage_label}", "info")
                await self._run_optimization(job, standalone=False)
                if job.status == JobStatus.COMPLETED:
                    await self._run_optimization_review(job)
            elif item.retry_stage == "optimization_review":
                await self._log(job, f"▶ Повтор: {stage_label}", "info")
                await self._start_heartbeat(job)
                await self._run_optimization_review(job)
                if job.status == JobStatus.RUNNING:
                    job.status = JobStatus.COMPLETED
            else:
                resume_info = {
                    "stage": item.retry_stage,
                    "stage_label": stage_label,
                    "detail": "Повтор этапа из очереди",
                    "can_resume": True,
                    "is_stage_retry": True,
                }
                await self._run_resumed_pipeline(job, item.retry_stage, resume_info)
            return

        if action == "resume":
            resume_info = self.detect_resume_stage(pid, version_id=job.version_id)
            if not resume_info.get("can_resume"):
                job.status = JobStatus.FAILED
                job.error_message = "Нечего возобновлять"
                job.completed_at = datetime.now().isoformat()
                return
            # Стадии оптимизации НЕ входят в ocr_stages конвейера: без этого роутинга
            # _run_resumed_pipeline падал в start_idx=0 → «resume оптимизации»
            # перезапускал ВЕСЬ конвейер с prepare (часы + деньги + перегенерация
            # findings). resume_pipeline() роутит их при enqueue (retry_stage), но
            # авто-resume и add_resume_to_batch кладут action="resume" и попадают
            # сюда. Ветки зеркалят retry_stage-обработку выше.
            _resume_stage = str(resume_info.get("stage") or "")
            if _resume_stage == "optimization":
                await self._log(job, "▶ Возобновление: Оптимизация", "info")
                await self._run_optimization(job, standalone=False)
                if job.status == JobStatus.COMPLETED:
                    await self._run_optimization_review(job)
                return
            if _resume_stage == "optimization_review":
                await self._log(job, "▶ Возобновление: Проверка оптимизации", "info")
                await self._start_heartbeat(job)
                await self._run_optimization_review(job)
                if job.status == JobStatus.RUNNING:
                    job.status = JobStatus.COMPLETED
                return
            await self._run_resumed_pipeline(job, resume_info["stage"], resume_info)
            return

        if action == "main_audit":
            await self._run_main_audit(job)
            return
        if action == "norm_verify":
            await self._run_norm_verification(job)
            return
        if action == "prepare":
            await self._run_prepare(job)
            return
        if action == "tile_audit":
            await self._run_tile_audit(job, start_from=int(extra.get("start_from", 1)))
            return
        if action == "optimization":
            await self._run_optimization(job)
            if job.status == JobStatus.COMPLETED:
                await self._run_optimization_review(job)
            return
        if action == "optimization_review":
            await self._start_heartbeat(job)
            await self._run_optimization_review(job)
            if job.status == JobStatus.RUNNING:
                job.status = JobStatus.COMPLETED
            return

        # Полный аудит / batch-actions. Для V2+ ищем _result.json в V2 dir.
        # Smart-ветка (тайловый триаж) удалена: неподготовленный проект (нет
        # result.json от Qwen-prepare) — это ошибка конфигурации, а не повод
        # уходить в legacy-конвейер. Сообщаем явно.
        _root, proj_dir, _od = self._resolve_job_paths(job)
        is_ocr = _has_ocr_result_json(proj_dir)

        if not is_ocr:
            job.status = JobStatus.FAILED
            job.error_message = (
                "Проект не подготовлен: нет result.json (Qwen enrichment). "
                "Запустите подготовку проекта, затем полный аудит "
                "(smart-режим удалён)."
            )
            job.completed_at = datetime.now().isoformat()
            await self._log(job, job.error_message, "error")
            return

        if action == "full":
            await self._run_ocr_pipeline(job, include_optimization=True)
            return
        if action in ("audit", "standard", "pro"):
            await self._run_ocr_pipeline(job)
            return
        if action in ("audit+optimization", "standard+optimization", "pro+optimization"):
            await self._run_ocr_pipeline(job, include_optimization=True)
            return

        # fallback
        await self._run_ocr_pipeline(job)

    # ─── Единая очередь: enqueue single-project ───────────────────────
    def _ensure_batch_worker(self, action_for_label: str = "full") -> BatchQueueStatus:
        """Гарантировать, что _batch_queue существует и worker запущен.

        Не добавляет items. Возвращает текущую очередь либо создаёт пустую и
        поднимает worker.
        """
        queue = self._batch_queue
        # Идемпотентность по ЖИВОСТИ таска, а не по наличию ключа в active_jobs:
        # cleanup_zombies мог снять __BATCH__ из active_jobs у живой корутины —
        # тогда наличие ключа лгало бы об отсутствии worker'а и мы создали бы
        # дубль. Доверяем _batch_worker_alive().
        if queue is not None and queue.status == "running" and self._batch_worker_alive():
            return queue

        # Прерванная очередь (interrupted, либо running с мёртвым worker'ом —
        # рестарт бэкенда) с незавершёнными items НЕ затирается: раньше здесь
        # безусловно создавалась новая пустая очередь, и один одиночный запуск
        # после рестарта безвозвратно стирал items ночного батча. Вместо этого
        # переиспользуем её — нормализуем зависшие статусы и поднимаем worker
        # над ТОЙ ЖЕ очередью (эквивалент resume_interrupted_batch), а новый
        # item enqueue-caller допишет в хвост.
        if queue is not None and not self._batch_worker_alive():
            unfinished = any(
                it.status in ("pending", "running", "interrupted")
                for it in queue.items
            )
            if unfinished and self._has_live_project_audit():
                # Worker-регистрацию могли ошибочно снять (cleanup_zombies),
                # но корутина/процессы живы — поднимать второй worker нельзя
                # (как в resume_interrupted_batch). Возвращаем очередь как
                # есть: caller допишет item, живая корутина его подхватит.
                return queue
            if unfinished:
                for it in queue.items:
                    if it.status in ("running", "interrupted"):
                        it.status = "pending"
                        it.error = None
                queue.current_index = next(
                    (i for i, it in enumerate(queue.items) if it.status == "pending"),
                    0,
                )
                queue.status = "running"

                meta_job = AuditJob(
                    job_id=queue.queue_id,
                    object_id=self._resolve_object_id(None),
                    project_id="__BATCH__",
                    stage=AuditStage.PREPARE,
                    status=JobStatus.RUNNING,
                    started_at=datetime.now().isoformat(),
                    progress_total=queue.total,
                    progress_current=queue.completed + queue.failed,
                )
                self.active_jobs["__BATCH__"] = meta_job
                self._tasks["__BATCH__"] = self._create_bound_task(
                    self._run_batch_queue(queue, meta_job),
                    meta_job,
                )
                self._persist_queue()
                return queue

        queue = BatchQueueStatus(
            queue_id=str(uuid4()),
            action=action_for_label,
            items=[],
            total=0,
            status="running",
        )
        self._batch_queue = queue

        meta_job = AuditJob(
            job_id=queue.queue_id,
            object_id=self._resolve_object_id(None),
            project_id="__BATCH__",
            stage=AuditStage.PREPARE,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
            progress_total=0,
        )
        self.active_jobs["__BATCH__"] = meta_job

        task = self._create_bound_task(
            self._run_batch_queue(queue, meta_job),
            meta_job,
        )
        self._tasks["__BATCH__"] = task
        return queue

    async def _enqueue_single(
        self,
        project_id: str,
        action: str,
        *,
        retry_stage: Optional[str] = None,
        extra_params: Optional[dict] = None,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        """Поставить single-project задачу в общую очередь.

        Возвращает placeholder AuditJob со status=QUEUED. Реальный pipeline
        запустится, когда worker дойдёт до этого item. Это единственный путь
        запуска одиночного проекта — start_audit/... все
        теперь делегируют сюда.

        `version_id` фиксируется на момент enqueue. Если None — берётся
        latest_version_id проекта (один раз). После этого пользователь может
        создать V_{N+1}, на запущенный job-а это не повлияет.
        """
        # Один раз резолвим effective_version_id — не каждый раз внутри стадии.
        from backend.app.services.common import version_service
        try:
            project_dir_for_resolve = resolve_project_dir(project_id)
            effective_vid = version_service.resolve_effective_version_id(
                project_dir_for_resolve, project_id, version_id,
            )
            # Валидируем, что версия существует
            version_service.get_version_entry(
                project_dir_for_resolve, project_id, effective_vid,
            )
        except version_service.VersionNotFoundError as e:
            raise RuntimeError(str(e)) from e

        async with self._enqueue_lock:
            try:
                from backend.app.pipeline.stages.prepare.prepare_service import is_prepare_active_or_queued
                if is_prepare_active_or_queued(project_id):
                    raise RuntimeError(
                        f"Проект {project_id} уже выполняется или ожидает в prepare-очереди"
                    )
            except ImportError:
                pass

            jkey = self.job_key(project_id, effective_vid)

            # Уже бежит прямо сейчас (по этой версии)?
            if jkey in self.active_jobs or (effective_vid in (None, "v1") and project_id in self.active_jobs):
                raise RuntimeError(f"Аудит уже запущен для {project_id} ({effective_vid})")

            # Уже стоит в очереди (pending/running) по той же версии?
            if self._batch_queue and self._batch_queue.status == "running":
                for it in self._batch_queue.items:
                    same_version = (it.version_id or "v1") == (effective_vid or "v1")
                    if (
                        it.project_id == project_id
                        and same_version
                        and it.status in ("pending", "running")
                    ):
                        raise RuntimeError(
                            f"Проект {project_id} ({effective_vid}) уже в очереди"
                        )

            job_id = str(uuid4())
            item = BatchQueueItem(
                project_id=project_id,
                version_id=effective_vid,
                action=action,
                retry_stage=retry_stage,
                extra_params=extra_params or {},
                status="pending",
                job_id=job_id,
            )

            queue = self._ensure_batch_worker(action_for_label=action)
            queue.items.append(item)
            queue.total = len(queue.items)

            meta_job = self.active_jobs.get("__BATCH__")
            if meta_job:
                meta_job.progress_total = queue.total

            placeholder = AuditJob(
                job_id=job_id,
                object_id=self._resolve_object_id_for_project(
                    None, project_id, effective_vid,
                ),
                project_id=project_id,
                version_id=effective_vid,
                stage=AuditStage.PREPARE,
                status=JobStatus.QUEUED,
            )

        # Broadcast делаем вне лока (там тоже awaits) — на корректность не влияет.
        await self._broadcast_batch_progress(queue)
        return placeholder

    async def cancel_batch(self) -> bool:
        """Отменить текущую batch-очередь."""
        if not self._batch_queue or self._batch_queue.status != "running":
            return False
        self._batch_queue.status = "cancelled"
        # Отменить текущий активный проект
        current_item = self._batch_queue.items[self._batch_queue.current_index]
        if current_item.status == "running":
            await self.cancel(current_item.project_id)
        self._persist_queue()
        return True

    async def add_to_batch(self, project_ids: list[str], action: str | None = None) -> BatchQueueStatus:
        """Добавить проекты в общую очередь.

        Сохраняет совместимость с прежним API роутера. Под капотом — то же,
        что `start_batch`: проекты дописываются в running-очередь либо
        поднимается новая.
        """
        if not project_ids:
            queue = self._batch_queue
            if queue:
                return queue
            raise RuntimeError("Нет активной групповой очереди")
        effective_action = action or (
            self._batch_queue.action if self._batch_queue else "full"
        )
        return await self.start_batch(project_ids, effective_action)

    async def add_retry_to_batch(
        self,
        project_id: str,
        stage: str,
        *,
        version_id: Optional[str] = None,
    ) -> BatchQueueStatus:
        """Добавить retry конкретного этапа в очередь."""
        # Маппинг ключей pipeline_summary → внутренних ключей этапов
        stage_map = {
            "crop_blocks": "prepare",
            "block_context": "gemma_enrichment",
            "gemma_enrichment": "gemma_enrichment",
            "text_analysis": "text_analysis",
            "block_analysis": "block_analysis",
            "findings_merge": "findings_merge",
            "findings_critic": "findings_review",
            "findings_review": "findings_review",
            "findings_corrector": "findings_review",
            "norm_verify": "norm_verify",
            "optimization": "optimization",
            "optimization_critic": "optimization_review",
            "optimization_corrector": "optimization_review",
            "debt_control": "debt_control",
            "decision_carryover": "decision_carryover",
            "prepare": "prepare",
            "tile_audit": "block_analysis",
            "main_audit": "findings_merge",
        }
        internal_stage = stage_map.get(stage, stage)
        internal_stage = self._validate_start_from_stage_now(
            project_id, internal_stage, version_id=version_id,
        )

        await self._enqueue_single(
            project_id, action="retry_stage", retry_stage=internal_stage,
            version_id=version_id,
        )
        stage_label = {
            "prepare": "Кроп блоков", "gemma_enrichment": GEMMA_STAGE_LABEL,
            "text_analysis": "Анализ текста",
            "block_analysis": "Анализ блоков", "findings_merge": "Свод замечаний",
            "findings_review": "Critic замечаний", "norm_verify": "Верификация норм",
            "optimization": "Оптимизация", "optimization_review": "Проверка оптимизации",
            "debt_control": "Контроль долгов",
            "decision_carryover": "Перенос вердиктов",
        }.get(internal_stage, internal_stage)
        await ws_manager.broadcast_global(
            WSMessage.log("__BATCH__", f"+ В очередь: {project_id} → {stage_label}", "info")
        )
        return self._batch_queue

    async def add_resume_to_batch(
        self, project_id: str, *, version_id: Optional[str] = None,
    ) -> BatchQueueStatus:
        """Добавить resume проекта в очередь."""
        await self._enqueue_single(project_id, action="resume", version_id=version_id)
        await ws_manager.broadcast_global(
            WSMessage.log("__BATCH__", f"+ В очередь: {project_id} → Продолжить", "info")
        )
        return self._batch_queue

    def _reconcile_stale_queue(self) -> bool:
        """Привести очередь к консистентному виду, если worker уже не жив.

        Batch-worker регистрируется как task под ключом "__BATCH__". Если его
        нет или он завершён, ни один item не может реально «выполняться».
        Зависшие 'running' элементы (после отмены, исключения воркера или гонки
        очистки) демотируются в 'interrupted', а сама очередь — из 'running' в
        'interrupted'. Иначе UI бесконечно показывает «Выполняется» при
        простаивающем конвейере.
        """
        q = self._batch_queue
        if q is None:
            return False
        if self._batch_worker_alive():
            return False  # воркер жив — доверяем его учёту
        # Worker-таск не зарегистрирован/завершён, НО текущий проект мог ещё
        # реально выполняться (его корутина = тот же __BATCH__ таск, у которого
        # cleanup_zombies мог снять регистрацию; либо живы дочерние процессы).
        # В этом случае НЕ демотируем очередь в interrupted — иначе фоновые
        # циклы (gated на status == "running") умрут, а UI покажет ложный
        # «полный сбой». Демотируем только когда проект реально завершился.
        if self._has_live_project_audit():
            return False
        changed = False
        demoted_pids = []
        for it in q.items:
            if it.status == "running":
                it.status = "interrupted"
                if not it.error:
                    it.error = "Прервано (воркер очереди не активен)"
                demoted_pids.append(it.project_id)
                changed = True
        if q.status == "running":
            q.status = "interrupted"
            changed = True
        if changed:
            # Диагностика: почему очередь вдруг стала interrupted (worker мёртв,
            # живого аудита нет). Без лога «зависшая running» расследуется вслепую.
            print(
                f"[BATCH] _reconcile_stale_queue: worker не активен, очередь → "
                f"interrupted; демотировано running-итемов: {len(demoted_pids)}"
                + (f" ({', '.join(demoted_pids)})" if demoted_pids else "")
            )
            try:
                self._persist_queue()
            except Exception:
                pass
        return changed

    def get_batch_queue(self) -> Optional[BatchQueueStatus]:
        """Получить текущую batch-очередь."""
        self._reconcile_stale_queue()
        return self._batch_queue

    async def reorder_batch(self, new_order: list[str]) -> BatchQueueStatus:
        """Переупорядочить pending-элементы очереди. new_order — список project_id в новом порядке."""
        queue = self._batch_queue
        if not queue or queue.status != "running":
            raise RuntimeError("Нет активной групповой очереди")

        # Инвариант (баг C3 аудита пайплайна): ЛЮБОЙ не-pending элемент
        # (completed/failed/skipped/running, а также cancelled/interrupted)
        # остаётся ровно на своей позиции — worker-цикл держит локальный
        # индекс, и сдвиг позиций running/обработанных рассинхронивал его
        # (пропуск pending-элемента, «выполняется» не тот проект в UI).
        # Раньше cancelled/interrupted считались «pending» и уезжали в хвост,
        # двигая всё после себя. Переставляем pending только между их же
        # слотами.
        pending_map = {
            item.project_id: item
            for item in queue.items
            if item.status == "pending"
        }
        reordered_pending = [
            pending_map.pop(pid) for pid in new_order if pid in pending_map
        ]
        reordered_pending.extend(pending_map.values())

        _pending_iter = iter(reordered_pending)
        queue.items = [
            item if item.status != "pending" else next(_pending_iter)
            for item in queue.items
        ]
        queue.total = len(queue.items)
        self._persist_queue()
        await self._broadcast_batch_progress(queue)
        return queue

    async def remove_from_batch(self, project_id: str) -> BatchQueueStatus:
        """Удалить pending-элемент из очереди."""
        queue = self._batch_queue
        if not queue or queue.status != "running":
            raise RuntimeError("Нет активной групповой очереди")

        original_len = len(queue.items)
        queue.items = [item for item in queue.items
                       if not (item.project_id == project_id and item.status == "pending")]

        if len(queue.items) == original_len:
            raise RuntimeError(f"Проект {project_id} не найден в очереди или уже обрабатывается")

        queue.total = len(queue.items)
        # Скорректировать current_index если удалённый элемент был до текущего
        if queue.current_index >= len(queue.items):
            queue.current_index = max(0, len(queue.items) - 1)

        await ws_manager.broadcast_global(
            WSMessage.log("__BATCH__", f"- Удалён из очереди: {project_id}", "info")
        )
        await self._broadcast_batch_progress(queue)
        return queue

    async def update_batch_item_action(self, project_id: str, action: str) -> BatchQueueStatus:
        """Изменить действие (audit/optimization/audit+optimization) для pending-элемента."""
        queue = self._batch_queue
        if not queue or queue.status != "running":
            raise RuntimeError("Нет активной групповой очереди")

        for item in queue.items:
            if item.project_id == project_id and item.status == "pending":
                item.action = action
                await self._broadcast_batch_progress(queue)
                return queue

        raise RuntimeError(f"Проект {project_id} не найден в очереди или уже обрабатывается")

    async def _broadcast_batch_progress(self, queue: BatchQueueStatus, complete: bool = False):
        """WS-уведомление о прогрессе batch-очереди."""
        current_project = None
        if queue.current_index < len(queue.items):
            current_project = queue.items[queue.current_index].project_id

        await ws_manager.broadcast_global(WSMessage(
            type="batch_progress",
            project="__BATCH__",
            timestamp=datetime.now().isoformat(),
            data={
                "queue_id": queue.queue_id,
                "action": queue.action,
                "status": queue.status,
                "current_index": queue.current_index,
                "total": queue.total,
                "completed": queue.completed,
                "failed": queue.failed,
                "current_project": current_project,
                "items": [item.model_dump() for item in queue.items],
                "complete": complete,
            },
        ))
        self._persist_queue()

    async def start_all_projects(self, project_ids: list[str] | None = None) -> dict:
        """Поставить полный аудит для всех проектов в общую очередь.

        После рефакторинга на единую очередь это просто обёртка над
        `start_batch(all_ids, action="full")`. __ALL__ meta-job больше не
        используется — UI видит обычный batch-индикатор.
        """
        from backend.app.services.common.project_service import list_projects

        if project_ids:
            all_ids = list(project_ids)
        else:
            projects = list_projects()
            all_ids = [p.project_id for p in projects if p.has_pdf]

        if not all_ids:
            return {"error": "Нет проектов для обработки"}

        queue = await self.start_batch(all_ids, action="full")
        await ws_manager.broadcast_global(
            WSMessage.log(
                "__BATCH__",
                f"═══ В очередь поставлен аудит {len(all_ids)} проектов ═══",
                "info",
            )
        )
        return {
            "total": len(all_ids),
            "queue_id": queue.queue_id,
            "queue_total": queue.total,
            "status": "queued",
        }

    # ─── Запуск оптимизации проектных решений ───
    async def start_optimization(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        """Запустить анализ оптимизации проектной документации."""
        return await self._enqueue_single(
            project_id, action="optimization", version_id=version_id,
        )

    async def start_optimization_review(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        """Запустить только critic + corrector оптимизации (без перезапуска самой оптимизации)."""
        # Sanity-check на момент enqueue, чтобы не плодить заведомо ломанные
        # items в очереди. Повторная проверка существования файла происходит
        # внутри `_dispatch_action` на момент реального запуска.
        from backend.app.services.common import version_service
        try:
            output_dir = version_service.resolve_version_output_dir(project_id, version_id)
        except version_service.VersionNotFoundError as e:
            raise RuntimeError(str(e)) from e
        opt_path = output_dir / "optimization.json"
        if not opt_path.exists():
            raise RuntimeError("optimization.json не найден — сначала запустите оптимизацию")
        return await self._enqueue_single(
            project_id, action="optimization_review", version_id=version_id,
        )

    async def _run_optimization_review_standalone(self, job: AuditJob):
        """Critic + Corrector оптимизации (standalone запуск)."""
        try:
            await self._start_heartbeat(job)
            await self._run_optimization_review(job)
            if job.status == JobStatus.RUNNING:
                job.status = JobStatus.COMPLETED
        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            await self._log(job, f"Исключение: {e}", "error")
        finally:
            job.completed_at = datetime.now().isoformat()
            self._cleanup(job.project_id)

    async def _run_optimization_with_review(self, job: AuditJob):
        """Оптимизация + critic/corrector."""
        await self._run_optimization(job)
        if job.status == JobStatus.COMPLETED:
            await self._run_optimization_review(job)

    async def _run_optimization(self, job: AuditJob, standalone: bool = True):
        """Запуск оптимизации — оркестратор делегирует в optimization/runner.py.

        standalone=False: не делать cleanup в finally (для параллельного запуска).
        """
        from backend.app.pipeline.stages.optimization.runner import (
            run_optimization as _opt_runner,
            OptimizationResult,
        )
        pid = job.project_id
        try:
            if standalone:
                await self._start_heartbeat(job)

            ctx = self._make_stage_context(job)
            result: OptimizationResult = await _opt_runner(ctx)

            if result.cancelled:
                job.status = JobStatus.CANCELLED
            elif result.success:
                job.status = JobStatus.COMPLETED
                self._promote_v2_analysis_artifacts(
                    job,
                    (
                        "optimization.json",
                        "optimization_claude.json",
                        "optimization_codex.json",
                        "optimization_merge_report.json",
                        "pipeline_log.json",
                    ),
                )
            else:
                job.status = JobStatus.FAILED
                job.error_message = result.error or "optimization failed"

        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            self._update_pipeline_log(pid, "optimization", "error", error="Отменено")
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            await self._log(job, f"Исключение: {e}", "error")
            self._update_pipeline_log(pid, "optimization", "error", error=str(e))
        finally:
            job.completed_at = datetime.now().isoformat()
            if standalone:
                self._cleanup(pid)

    async def _run_optimization_review(self, job: AuditJob):
        """Critic + Corrector оптимизации — оркестратор делегирует в optimization/runner.py.

        Результат пробрасывается в job.status (reserc.md #40): раньше он
        игнорировался, поэтому проверки opt_job.status==FAILED (call-site 3659)
        были мёртвыми. Консервативно: статус выставляем ТОЛЬКО на cancelled/error
        (не форсим COMPLETED — чтобы не затереть статус в параллельных потоках).
        Возвращаем результат для возможного использования вызывающим.
        """
        from backend.app.pipeline.stages.optimization.runner import (
            run_optimization_review as _opt_review_runner,
            OptimizationReviewResult,
        )
        pid = job.project_id
        try:
            ctx = self._make_stage_context(job)
            result: OptimizationReviewResult = await _opt_review_runner(ctx)
            if result.cancelled:
                job.status = JobStatus.CANCELLED
            elif result.error:
                job.status = JobStatus.FAILED
                job.error_message = result.error
            else:
                self._promote_v2_analysis_artifacts(
                    job,
                    (
                        "optimization.json",
                        "optimization_claude.json",
                        "optimization_codex.json",
                        "optimization_merge_report.json",
                        "optimization_review.json",
                        "optimization_pre_review.json",
                        "pipeline_log.json",
                    ),
                )
            return result
        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            self._update_pipeline_log(pid, "optimization_review", "error", error="Отменено")
            raise
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            await self._log(job, f"Optimization review исключение: {e}", "error")
            self._update_pipeline_log(pid, "optimization_review", "error", error=str(e))
            return None


# Глобальный экземпляр
pipeline_manager = PipelineManager()
