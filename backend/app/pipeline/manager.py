"""
Pipeline Manager — оркестрация конвейера аудита.
Запуск, отмена, отслеживание прогресса.
"""
import asyncio
import json
import os
import random
from uuid import uuid4
from datetime import datetime
from pathlib import Path
from typing import Optional

from backend.app.core.config import (
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
    CRITIC_CHUNK_SIZE,
    CORRECTOR_CHUNK_SIZE,
    validate_current_stage_model_config,
    BATCH_QUEUE_FILE,
)
from backend.app.models.audit import AuditJob, AuditStage, JobStatus, BatchQueueStatus, BatchQueueItem, BatchAction
from backend.app.models.websocket import WSMessage
from backend.app.core.config import get_claude_model, get_model_for_stage
from backend.app.models.usage import UsageRecord
from backend.app.services.common.process_runner import run_script, kill_all_processes
import backend.app.services.llm.claude_runner as claude_runner
from backend.app.services.common.usage_service import usage_tracker, global_scanner, paid_cost_tracker
from backend.app.services.llm.lmstudio_lifecycle_service import (
    note_activity as _lmstudio_note_activity,
    register_idle_probe as _register_lmstudio_idle_probe,
    schedule_post_queue_cleanup as _schedule_lmstudio_post_queue_cleanup,
)
from backend.app.pipeline.resume_detector import detect_resume_stage as _detect_resume_stage
import backend.app.services.common.audit_logger as audit_logger
from backend.app.services.common.project_service import resolve_project_dir
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
)

# ── Stage runner imports (extracted pure helpers) ──────────────────────────
from backend.app.pipeline.stages.crop_blocks.runner import (
    build_crop_args as _build_crop_args,
    existing_crop_matches_policy as _existing_crop_matches_policy,
    crop_policy_label as _crop_policy_label,
    run_crop_blocks as _run_crop_blocks,
    run_policy_recrop as _run_policy_recrop,
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
from backend.app.pipeline.stages.findings_review.runner import (
    run_findings_review as _run_findings_review_stage,
)
from backend.app.pipeline.stages.block_analysis.runner import (
    run_block_analysis_findings_only as _run_block_analysis_findings_only_stage,
)
from backend.app.pipeline.stages.text_analysis.runner import (
    run_text_analysis as _run_text_analysis_stage,
)
from backend.app.pipeline.stages.gemma_enrichment.runner import (
    run_gemma_enrichment_stage as _run_gemma_enrichment_stage_fn,
)
# ──────────────────────────────────────────────────────────────────────────


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

        Paid-API guard: manual_run_id из item'ов СТИРАЕТСЯ перед записью.
        Это политика fail-closed для restart — после рестарта resumed jobs
        становятся orphan и платные этапы блокируются. Пользователь должен
        заново нажать Start с галкой "Разрешить платные API".
        """
        if self._batch_queue is None:
            return
        try:
            BATCH_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
            data = self._batch_queue.model_dump()
            # Стираем manual_run_id из items при persist.
            for it in data.get("items", []) or []:
                if isinstance(it, dict):
                    it["manual_run_id"] = None
            BATCH_QUEUE_FILE.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            print(f"[PipelineManager] Ошибка сохранения очереди: {e}")

    def load_persisted_queue(self) -> None:
        """Загрузить очередь после перезапуска сервера.

        running-элементы → interrupted (процесс был прерван рестартом).
        pending-элементы → остаются pending (не были запущены).
        Статус очереди → "interrupted" (не "running") чтобы worker не запустился.

        Paid-API guard: manual_run_id из persisted items ИГНОРИРУЕТСЯ
        (обнуляется). Даже если файл содержит старые значения (legacy/тест),
        registry в памяти пуст — guard заблокирует.
        """
        if not BATCH_QUEUE_FILE.exists():
            return
        try:
            data = json.loads(BATCH_QUEUE_FILE.read_text(encoding="utf-8"))
            # Зачистка manual_run_id ДО построения pydantic-модели —
            # на случай если в файле хранились старые значения.
            for it in data.get("items", []) or []:
                if isinstance(it, dict) and "manual_run_id" in it:
                    it["manual_run_id"] = None
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

    async def resume_interrupted_batch(self) -> BatchQueueStatus:
        """Restart a persisted interrupted queue from unfinished items."""
        async with self._enqueue_lock:
            queue = self._batch_queue
            if not queue:
                raise RuntimeError("Нет прерванной очереди")
            if queue.status == "running":
                return queue
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
        """Очистить зомби-задачи (нет heartbeat более ZOMBIE_TIMEOUT_SEC)."""
        now = datetime.now()
        zombies = []
        for pid, job in list(self.active_jobs.items()):
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

    def _recover_stale_pipelines(self):
        """Сканирует все pipeline_log.json и помечает зависшие 'running' как 'interrupted'.

        Вызывается при старте сервера. Если сервер был перезапущен во время
        активного аудита, процессы Claude CLI уже завершились, но pipeline_log
        остался в состоянии 'running'. Помечаем как 'interrupted' чтобы:
        1. UI показывал корректный статус (не вечный спиннер)
        2. Resume мог подхватить с прерванного этапа
        """
        from backend.app.services.common.project_service import iter_project_dirs

        # Собрать project_id активных задач, чтобы не трогать их
        active_pids = set(self.active_jobs.keys())

        recovered = 0
        for _pid, project_dir in iter_project_dirs():
            # Не трогать проекты с активным аудитом
            if _pid in active_pids:
                continue
            log_path = project_dir / "_output" / "pipeline_log.json"
            if not log_path.exists():
                continue
            try:
                data = json.loads(log_path.read_text(encoding="utf-8"))
                stages = data.get("stages", {})
                changed = False
                for stage_key, stage_info in stages.items():
                    if stage_info.get("status") == "running":
                        # Этот этап остался "running" после рестарта — прерван
                        stage_info["status"] = "interrupted"
                        stage_info["error"] = "Сервер перезапущен во время выполнения"
                        stage_info["interrupted_at"] = datetime.now().isoformat()
                        changed = True
                        print(f"[Recovery] {project_dir.name}: этап '{stage_key}' running → interrupted")
                if changed:
                    data["last_updated"] = datetime.now().isoformat()
                    log_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                    recovered += 1
            except (json.JSONDecodeError, OSError) as e:
                print(f"[Recovery] Ошибка чтения {log_path}: {e}")

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
            if task:
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
        _schedule_lmstudio_post_queue_cleanup("pipeline queue drained")

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
        from backend.app.services.common import version_service
        root_dir = resolve_project_dir(job.project_id)
        try:
            version_dir = version_service.get_version_dir(
                root_dir, job.project_id, job.version_id,
            )
        except version_service.VersionNotFoundError:
            version_dir = root_dir
        return root_dir, version_dir, version_dir / "_output"

    def _make_stage_context(self, job: "AuditJob") -> "PipelineStageContext":
        """Построить PipelineStageContext из текущего job для передачи в stage runner-ы."""
        from backend.app.pipeline.context import PipelineStageContext
        pid = job.project_id
        # ctx.project_dir сейчас — это version_dir (V1: root; V2+: _versions/v{N}/).
        # Это нужно, чтобы stage runner-ы видели правильные source-файлы версии.
        _root, version_dir, output_dir = self._resolve_job_paths(job)
        project_dir = version_dir

        async def _log(msg: str, level: str = "info") -> None:
            await self._log(job, msg, level)

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
            return await self._run_script(pid, *args, **kwargs)

        # project_info: предпочтительно из папки версии (создаётся
        # create_next_version'ом для V2+), fallback — корневой info.
        info_path_version = version_dir / "project_info.json"
        info_path_root = project_dir / "project_info.json"
        info_path = info_path_version if info_path_version.exists() else info_path_root
        try:
            project_info = json.loads(info_path.read_text(encoding="utf-8"))
        except Exception:
            project_info = {}

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
            manual_run_id=getattr(job, "manual_run_id", None),
            job_id=getattr(job, "job_id", None),
        )

    def _reset_job_progress(self, job: AuditJob):
        """Сбросить прогресс и ETA-данные при переходе между этапами пайплайна."""
        job.progress_current = 0
        job.progress_total = 0
        job.batch_durations = []
        job.batch_started_at = None

    def _backfill_highlight_regions(self, project_id: str):
        """Восстановить highlight_regions в 03_findings.json из 02_blocks_analysis.json.

        При findings_merge LLM иногда теряет highlight_regions из G-замечаний.
        Этот метод подтягивает координаты обратно по source_block_ids/related_block_ids.

        Version-aware: использует `_output_dir_for_project`, parent которого =
        version_dir; для V2 это `<root>/_versions/v{N}`. Иначе backfill_project
        ушёл бы на V1 root и переписал V1 03_findings.json.
        """
        from backend.app.pipeline.stages.findings_merge.backfill_highlights import backfill_project
        # backfill_project работает с `project_dir / _output`, поэтому передаём
        # parent от version-aware output_dir.
        project_dir = self._output_dir_for_project(project_id).parent
        result = backfill_project(project_dir)
        if result["fixed"] > 0:
            print(f"[{project_id}] highlight_regions restored: {result['fixed']}")

    @staticmethod
    def _attach_stage02_coverage_to_findings(project_id: str) -> dict:
        """Attach deterministic Stage 02 coverage warnings to final findings."""
        from backend.app.pipeline.stages.block_analysis.runner import (
            attach_stage02_coverage_to_findings,
        )
        return attach_stage02_coverage_to_findings(project_id)

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

            # Version-aware: V1 = root, V2+ = _versions/v{N}/.
            _root, project_dir, output_dir = self._resolve_job_paths(job)

            graph = build_document_graph_v2(project_dir, output_dir)
            if graph:
                debug_path = generate_locality_debug(graph, output_dir)
                await self._log(
                    job,
                    f"document_graph v{graph['version']}: "
                    f"{graph['total_pages']} стр., "
                    f"{graph['total_text_blocks']} текст., "
                    f"{graph['total_image_blocks']} граф."
                    + (f", debug: {debug_path.name}" if debug_path else ""),
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

    async def _run_gemma_enrichment_stage(self, job: AuditJob, *, force: bool = False) -> None:
        """Тонкий оркестратор: делегирует в gemma_enrichment/runner.py.

        Оркестраторная логика (job.stage, job.status, heartbeat, cleanup)
        остаётся здесь. Бизнес-логика Gemma enrichment — в runner.
        """
        pid = job.project_id
        job.stage = AuditStage.GEMMA_ENRICHMENT

        ctx = self._make_stage_context(job)
        result = await _run_gemma_enrichment_stage_fn(ctx, force=force)

        if result.cancelled:
            job.status = JobStatus.CANCELLED
            return

        if not result.success:
            job.status = JobStatus.FAILED
            job.error_message = result.error
            raise RuntimeError(result.error or "Gemma enrichment: ошибка")

        # Успех или partial (допускается продолжение)

    async def _ensure_stage02_crops(self, job: AuditJob) -> None:
        """Ensure findings_only Stage 02 has its own 100 DPI crop index."""
        pid = job.project_id
        # Version-aware: V1 = root, V2+ = _versions/v{N}/.
        _root, project_dir, _output = self._resolve_job_paths(job)
        policy = stage02_crop_policy()
        index_path = stage02_blocks_index_path(project_dir)
        blocks_dir = stage02_blocks_dir(project_dir)
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
                f"Stage 02 crops готовы: _output/{STAGE02_BLOCKS_DIRNAME} "
                f"({_crop_policy_label(policy)})",
            )
            return

        await self._log(
            job,
            f"Stage 02 crop: создаю _output/{STAGE02_BLOCKS_DIRNAME} "
            f"({_crop_policy_label(policy)}); Gemma base/high-detail indexes не трогаю",
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
                "Stage 02 crop частично завершился с ошибками; продолжу с доступными "
                "100 DPI blocks, пропуски попадут в coverage",
                "warn",
            )
            return
        if exit_code != 0:
            raise RuntimeError(f"Stage 02 crop failed: {stderr or f'Exit code {exit_code}'}")
        if not index_path.exists():
            raise RuntimeError(f"Stage 02 crop не создал _output/{STAGE02_BLOCKS_DIRNAME}/index.json")

    async def _run_block_analysis_findings_only(self, job: AuditJob) -> None:
        """Тонкий оркестратор: делегирует в block_analysis/runner.py.

        Оркестраторная логика (prerequisites, job.stage, heartbeat, cleanup)
        остаётся здесь. Бизнес-логика анализа блоков — в runner.
        """
        pid = job.project_id
        # ─── Paid API guard: проверка ДО любого network request Stage 02 ────
        # Stage 02 (findings_only_gemma_pair) идёт в OpenRouter напрямую и
        # тратит реальные деньги. Без manual_run_id (auto-resume, orphan job,
        # retry без галки UI) — блокируем перед prerequisites/crops.
        try:
            from backend.app.services.llm.paid_api_guard import (
                PaidApiBlockedError,
                PaidApiContext,
                assert_paid_api_allowed,
            )
            # Модель — текущая stage02 модель из настроек. Используем самую
            # популярную как метку; реальная модель проверится в runner ещё раз.
            from backend.app.core.config import get_stage_model
            stage02_model = get_stage_model("block_analysis") or "openai/gpt-5.4"
            assert_paid_api_allowed(PaidApiContext(
                source="manager.stage02.orchestrator",
                model=stage02_model,
                project_id=pid,
                version_id=getattr(job, "version_id", None) or "",
                stage="block_analysis",
                manual_run_id=getattr(job, "manual_run_id", None) or "",
                job_id=getattr(job, "job_id", "") or "",
            ))
        except PaidApiBlockedError as _e:
            await self._log(
                job,
                f"Stage 02 заблокирован paid_api_guard: {_e.reason}. "
                f"Запустите аудит вручную с галкой «Разрешить платные API».",
                "error",
            )
            job.status = JobStatus.FAILED
            job.error_message = f"paid_api_blocked: {_e.reason}"
            return
        # Version-aware: V1 = root, V2+ = _versions/v{N}/.
        _root, project_dir, _output = self._resolve_job_paths(job)
        project_info = load_project_info(project_dir)
        await self._assert_gemma_ready_for_stage(job, project_info, "block_analysis")
        await self._ensure_stage02_crops(job)

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

    def _record_findings_only_usage(self, job: AuditJob, summary: dict) -> None:
        """Учесть стоимость stage 02 в режиме findings_only_gemma_pair в usage tracker.

        Для OpenRouter-моделей (GPT/Gemini) — реальная плата → cost_usd.
        Для Claude CLI (sonnet/opus, без слэша) — подписка → cost_usd=0, notional=cost.
        """
        totals = summary.get("totals", {}) or {}
        model = summary.get("model", "") or ""
        cost = float(totals.get("estimated_cost_usd_total", 0.0) or 0.0)
        input_tokens = int(totals.get("input_tokens", 0) or 0)
        output_tokens = int(totals.get("output_tokens", 0) or 0)
        api_calls = int(summary.get("blocks_ok", 0) or 0) or 1
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
        # Stage 02 (gemma_findings_only) ходит в OpenRouter напрямую, в обход
        # llm_runner.run_llm — поэтому учёт paid_cost здесь обязателен.
        if actual_cost > 0:
            paid_cost_tracker.add(
                actual_cost,
                model=model,
                project_id=job.project_id,
                stage="block_analysis",
            )
            # Append-only forensic-event: который job/manual_run потратил.
            try:
                from backend.app.services.llm import paid_api_events as _pae
                _pae.record_paid_event(
                    cost_usd=actual_cost,
                    model=model,
                    project_id=job.project_id,
                    version_id=getattr(job, "version_id", None) or "",
                    stage="block_analysis",
                    source="manager.stage02",
                    manual_run_id=getattr(job, "manual_run_id", None) or "",
                    job_id=getattr(job, "job_id", "") or "",
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                )
            except Exception as _pae_err:
                # logger в manager.py не настроен — пишем в stderr, чтобы не
                # уронить запись paid_cost из-за ошибки журнала.
                print(
                    f"[paid_api_events] record_paid_event failed (stage02): {_pae_err}",
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

    async def _log(self, job: AuditJob, message: str, level: str = "info"):
        """Записать лог в консоль, файл и WebSocket.

        Перехватывает финальный JSON-ответ Claude CLI ({"type":"result",...})
        и превращает его в красивую cli_summary карточку вместо сырого JSON-мусора.
        Промежуточные stream-json сообщения (type=assistant/user/system) подавляются.
        """
        # Быстрый фильтр — обычные строки идут как есть
        stripped = (message or "").lstrip()
        if stripped.startswith('{"type":"'):
            try:
                payload = json.loads(stripped)
            except (json.JSONDecodeError, ValueError):
                payload = None
            if isinstance(payload, dict) and "type" in payload:
                msg_type = payload.get("type")
                if msg_type == "result":
                    await self._emit_cli_summary(job, payload)
                    return
                # Прочие технические типы stream-json не захламляют лог
                if msg_type in ("assistant", "user", "system", "tool_use", "tool_result"):
                    return

        await audit_logger.log_to_project(job, message, level)

    async def _emit_cli_summary(self, job: AuditJob, payload: dict):
        """
        Преобразовать {"type":"result",...} JSON от Claude CLI в:
          1) короткую строку в persisted-лог (для истории),
          2) структурированное cli_summary WS-сообщение для красивой карточки.
        """
        pid = job.project_id
        stage_val = job.stage.value if job.stage else ""

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
        """Отправлять heartbeat каждые 15 секунд."""
        try:
            while True:
                await asyncio.sleep(15)
                if job.status != JobStatus.RUNNING:
                    break

                now = datetime.now()
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
            pass
        except Exception:
            pass  # Heartbeat не должен ронять основной процесс

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
            "optimization", "optimization_review", "excel",
        }
        if normalized not in valid_stages:
            raise RuntimeError(f"Неизвестный этап: {stage}")
        return normalized

    def _validate_start_from_stage_now(self, project_id: str, stage: str) -> str:
        """Fail fast when a manual start/retry would bypass mandatory stages."""
        normalized = self._normalize_ocr_stage(stage)
        self._assert_stage_model_config_ready()
        project_dir = resolve_project_dir(project_id)
        output_dir = project_dir / "_output"
        project_info = load_project_info(project_dir)
        gemma_state = evaluate_gemma_enrichment(project_dir, project_info)

        if normalized == "gemma_enrichment":
            if gemma_state.get("status") in {"missing_md", "missing_blocks"}:
                raise RuntimeError(gemma_gate_error(gemma_state, "gemma_enrichment"))
            return normalized

        if normalized in {
            "text_analysis", "block_analysis", "findings_merge",
            "findings_review", "norm_verify", "excel",
        }:
            if gemma_state.get("status") == "missing_md":
                raise RuntimeError(gemma_gate_error(gemma_state, normalized))
            if gemma_state.get("status") == "missing_blocks":
                raise RuntimeError(gemma_gate_error(gemma_state, normalized))

        # text_analysis may enqueue when Gemma is incomplete: _run_resumed_pipeline()
        # will run gemma_enrichment first. block_analysis and later stages cannot.
        if normalized in {
            "block_analysis", "findings_merge", "findings_review",
            "norm_verify", "excel",
        } and not gemma_state.get("ready"):
            raise RuntimeError(gemma_gate_error(gemma_state, normalized))

        if normalized in {
            "block_analysis", "findings_merge", "findings_review",
            "norm_verify", "excel",
        } and not (output_dir / "01_text_analysis.json").exists():
            raise RuntimeError(
                "Нельзя запускать block_analysis: 01_text_analysis.json отсутствует. "
                "Сначала выполните text_analysis."
            )

        if normalized in {"findings_merge", "findings_review", "norm_verify", "excel"}:
            if not (output_dir / "02_blocks_analysis.json").exists():
                raise RuntimeError(
                    "Нельзя запускать findings_merge: 02_blocks_analysis.json отсутствует. "
                    "Сначала выполните block_analysis."
                )

        if normalized in {"findings_review", "norm_verify", "optimization", "optimization_review", "excel"}:
            if not (output_dir / "03_findings.json").exists():
                raise RuntimeError(
                    "Нельзя запускать этот этап: 03_findings.json отсутствует. "
                    "Сначала выполните findings_merge."
                )

        return normalized

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
        # Version-aware: V1 = root, V2+ = _versions/v{N}/.
        _root, project_dir, _output = self._resolve_job_paths(job)
        state = evaluate_gemma_enrichment(project_dir, project_info)
        if state.get("ready"):
            if state.get("status") in {"partial_allowed", "partial"}:
                self._update_pipeline_log(
                    job.project_id,
                    "gemma_enrichment",
                    "partial",
                    message=state.get("detail", "Partial Gemma enrichment разрешён"),
                    detail={
                        "partial_allowed": True,
                        "blocks_ok": state.get("blocks_ok", 0),
                        "blocks_total": state.get("blocks_total", 0),
                    },
                )
                await self._log(job, state.get("detail", "Partial Gemma enrichment разрешён"), "warn")
            return state

        if state.get("status") in {"missing_md", "missing_blocks"}:
            raise RuntimeError(gemma_gate_error(state, target_stage))

        await self._log(
            job,
            f"{target_stage}: {GEMMA_STAGE_LABEL} не готов — сначала запускаю gemma_enrichment",
            "warn",
        )
        await self._run_gemma_enrichment_stage(job, force=True)
        state = evaluate_gemma_enrichment(project_dir, project_info)
        if not state.get("ready"):
            raise RuntimeError(gemma_gate_error(state, target_stage))
        return state

    async def _assert_gemma_ready_for_stage(
        self,
        job: AuditJob,
        project_info: dict,
        target_stage: str,
    ) -> dict:
        # Version-aware: V1 = root, V2+ = _versions/v{N}/.
        _root, project_dir, _output = self._resolve_job_paths(job)
        state = evaluate_gemma_enrichment(project_dir, project_info)
        if not state.get("ready"):
            raise RuntimeError(gemma_gate_error(state, target_stage))
        if state.get("status") in {"partial_allowed", "partial"}:
            self._update_pipeline_log(
                job.project_id,
                "gemma_enrichment",
                "partial",
                message=state.get("detail", "Partial Gemma enrichment разрешён"),
                detail={
                    "partial_allowed": True,
                    "blocks_ok": state.get("blocks_ok", 0),
                    "blocks_total": state.get("blocks_total", 0),
                },
            )
            await self._log(job, state.get("detail", "Partial Gemma enrichment разрешён"), "warn")
        return state

    @staticmethod
    def _assert_text_analysis_exists(output_dir: Path, target_stage: str) -> None:
        if not (output_dir / "01_text_analysis.json").exists():
            raise RuntimeError(
                f"Нельзя запускать {target_stage}: 01_text_analysis.json отсутствует. "
                "Сначала выполните text_analysis."
            )

    async def start_from_stage(
        self,
        project_id: str,
        stage: str,
        *,
        version_id: Optional[str] = None,
        manual_run_id: Optional[str] = None,
    ) -> AuditJob:
        """Запустить конвейер с указанного этапа (ручной перезапуск цепочки).

        Кладёт single-task в общую очередь — фактический запуск произойдёт,
        когда worker дойдёт до элемента (см. `_enqueue_single`/`_dispatch_action`).

        `manual_run_id` выдаётся endpoint'ом, если пользователь нажал кнопку
        "Разрешить платные API". Без него платные этапы блокируются guard'ом.
        """
        stage = self._validate_start_from_stage_now(project_id, stage)
        return await self._enqueue_single(
            project_id, action="retry_stage", retry_stage=stage,
            version_id=version_id, manual_run_id=manual_run_id,
        )

    async def resume_pipeline(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
        manual_run_id: Optional[str] = None,
    ) -> AuditJob:
        """Продолжить пайплайн с места ошибки.

        resume-проверка детектит этап на момент непосредственного запуска
        (внутри `_dispatch_action`) — на момент enqueue достаточно знать
        проект.
        """
        # Быстрая проверка чтобы не пускать в очередь заведомо нечего возобновлять
        self._assert_stage_model_config_ready()
        resume_info = self.detect_resume_stage(project_id, version_id=version_id)
        if not resume_info.get("can_resume"):
            raise RuntimeError("Все этапы уже завершены — нечего возобновлять")
        return await self._enqueue_single(
            project_id, action="resume", version_id=version_id,
            manual_run_id=manual_run_id,
        )

    async def _run_resumed_pipeline(self, job: AuditJob, start_stage: str, resume_info: dict):
        """Запуск OCR-пайплайна с указанного этапа."""
        start_time = datetime.now()
        pid = job.project_id
        try:
            # Нормализация stage: legacy aliases → OCR stages
            normalized = self._normalize_ocr_stage(start_stage)

            # Порядок этапов OCR-пайплайна (без дублей)
            ocr_stages = [
                "prepare",
                "gemma_enrichment",
                "text_analysis",
                "block_analysis",
                "findings_merge",
                "findings_review",
                "norm_verify",
                "excel",
            ]
            start_idx = ocr_stages.index(normalized) if normalized in ocr_stages else 0

            await self._log(
                job,
                f"Возобновление конвейера с этапа: {resume_info.get('stage_label', start_stage)} "
                f"({resume_info.get('detail', '')})",
                "info",
            )

            # Version-aware пути: V1 = root, V2+ = _versions/v{N}/.
            _root_dir, project_dir, output_dir = self._resolve_job_paths(job)
            info_path = project_dir / "project_info.json"
            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            if start_idx >= 4:
                await self._assert_gemma_ready_for_stage(job, project_info, normalized)
                self._assert_text_analysis_exists(output_dir, normalized)
                if not (output_dir / "02_blocks_analysis.json").exists():
                    raise RuntimeError(
                        f"Нельзя запускать {normalized}: 02_blocks_analysis.json отсутствует. "
                        "Сначала выполните block_analysis."
                    )
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
                    "01_text_analysis.json", "02_blocks_analysis.json",
                    "03_findings.json", "block_batch_*.json", "block_batches.json",
                    RUNTIME_BATCHES_FILE, "block_analysis_summary.json",
                ])
                job.stage = AuditStage.CROP_BLOCKS
                print(f"[{pid}:resume] ═══ ЭТАП 1: Кроп image-блоков ═══")
                gemma_crop_policy = gemma_enrichment_crop_policy()
                # project_dir уже version-aware (см. начало _run_resumed_pipeline).
                blocks_index = gemma_blocks_index_path(project_dir)
                blocks_dir = gemma_blocks_dir(project_dir)
                force_gemma_crop = (
                    (blocks_index.exists() and not _existing_crop_matches_policy(blocks_index, gemma_crop_policy))
                    or (not blocks_index.exists() and blocks_dir.exists() and any(blocks_dir.glob("block_*.png")))
                )
                _crop_result = await _run_crop_blocks(
                    self._make_stage_context(job),
                    project_rel_path=self._project_path_for_job(job),
                    force=force_gemma_crop,
                    policy=gemma_crop_policy,
                    output_dir_name=GEMMA_BLOCKS_DIRNAME,
                )
                if not _crop_result.success:
                    raise RuntimeError(_crop_result.error or "Crop blocks failed")

                # Построить document_graph v2 (Python, без LLM)
                await self._build_document_graph_v2(job)

                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ ЭТАП 2: Gemma OCR enrichment ═══
            if start_idx <= 1:
                if start_idx == 1:
                    # Перезапуск Gemma меняет входной MD для всех downstream-этапов.
                    self._backup_findings_before_restart(pid)
                    self._clean_stage_files(pid, [
                        "01_text_analysis.json", "02_blocks_analysis.json",
                        "03_findings.json", "block_batch_*.json", "block_batches.json",
                        RUNTIME_BATCHES_FILE, "block_analysis_summary.json",
                    ])
                await self._run_gemma_enrichment_stage(job, force=start_idx == 1)

                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ ЭТАП 3: Текстовый анализ MD (Claude) ═══
            if start_idx <= 2:
                if start_idx == 2:
                    await self._ensure_gemma_ready_or_run(job, project_info, "text_analysis")
                    # Resume с этого этапа — бэкап findings перед очисткой
                    self._backup_findings_before_restart(pid)
                    # Очистить старые результаты
                    self._clean_stage_files(pid, [
                        "01_text_analysis.json", "02_blocks_analysis.json",
                        "03_findings.json", "block_batch_*.json", "block_batches.json",
                        RUNTIME_BATCHES_FILE, "block_analysis_summary.json",
                    ])
                self._reset_job_progress(job)
                job.stage = AuditStage.TEXT_ANALYSIS
                job.status = JobStatus.RUNNING
                print(f"[{pid}:resume] ═══ ЭТАП 3: Текстовый анализ MD ═══")
                await self._log(job, "═══ ЭТАП 3: Текстовый анализ MD (Claude) ═══")
                await self._start_heartbeat(job)

                _ta_result = await _run_text_analysis_stage(
                    self._make_stage_context(job),
                    with_rate_limit_retry=False,
                )
                if _ta_result.cancelled:
                    job.status = JobStatus.CANCELLED
                    return
                if not _ta_result.success:
                    raise RuntimeError(_ta_result.error or "Текстовый анализ: ошибка")

                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ ЭТАП 4-5: Генерация пакетов + анализ блоков (Claude) ═══
            if start_idx <= 3:
                await self._assert_gemma_ready_for_stage(job, project_info, "block_analysis")
                self._assert_text_analysis_exists(output_dir, "block_analysis")

            if start_idx <= 3 and get_stage_batch_mode("block_batch") == "findings_only_gemma_pair":
                # Ветвь findings_only_gemma_pair — single-block через GPT-5.4 + gemma-enrichment.
                # Не использует blocks.py batches/merge — пишет 02_blocks_analysis.json напрямую.
                await self._run_block_analysis_findings_only(job)
                if job.status == JobStatus.CANCELLED:
                    return
                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()
                # Block retry пропускаем: findings-only не помечает unreadable_text=true.
            elif start_idx <= 3:
                batch_start_from = resume_info.get("start_from", 1) if start_idx == 3 else 1
                batches_file = output_dir / "block_batches.json"

                # Генерация пакетов (если нет или свежий старт)
                need_generate = not batches_file.exists() or start_idx < 3
                if need_generate:
                    self._reset_job_progress(job)
                    job.stage = AuditStage.CROP_BLOCKS  # reuse для генерации батчей

                    gen_args = [self._project_path_for_job(job)]
                    print(f"[{pid}:resume] ═══ ЭТАП 4: Генерация пакетов блоков ═══")
                    await self._log(job, "═══ ЭТАП 4: Генерация пакетов блоков ═══")

                    exit_code, _, stderr = await self._run_script_for_job(
                        job,
                        str(BLOCKS_SCRIPT),
                        ["batches"] + gen_args,
                        on_output=lambda msg: self._log(job, msg),
                    )
                    if exit_code != 0:
                        raise RuntimeError(f"Генерация пакетов: {stderr}")

                if not batches_file.exists():
                    raise RuntimeError("block_batches.json не создан")

                with open(batches_file, "r", encoding="utf-8") as f:
                    batches_data = json.load(f)

                runtime_plan = _load_or_create_single_block_runtime_plan(
                    output_dir,
                    batches_data.get("batches", []),
                    force_rebuild=need_generate,
                )
                batches = runtime_plan.get("batches", [])
                single_block_mode = runtime_plan.get("mode") == "single_block"
                total_batches = len(batches)

                if total_batches == 0:
                    await self._log(job, "Нет пакетов для анализа — переход к своду", "warn")
                else:
                    # Параллельный анализ блоков
                    self._reset_job_progress(job)
                    job.stage = AuditStage.BLOCK_ANALYSIS
                    job.status = JobStatus.RUNNING
                    job.progress_total = total_batches
                    self._update_pipeline_log(pid, "block_analysis", "running")

                    parallel = get_block_batch_parallelism("block_batch")
                    mode_label = (
                        f"{total_batches} single-block запросов"
                        if single_block_mode else f"{total_batches} пакетов"
                    )
                    print(f"[{pid}:resume] ═══ ЭТАП 4: Анализ блоков ({mode_label} x{parallel}) ═══")
                    await self._log(
                        job,
                        f"═══ ЭТАП 4: Анализ блоков ({mode_label}, x{parallel} параллельно) ═══"
                    )
                    if single_block_mode:
                        await self._log(
                            job,
                            f"Stage 02 runtime plan: {RUNTIME_BATCHES_FILE} "
                            f"({total_batches} single-block задач)",
                        )

                    semaphore = asyncio.Semaphore(parallel)
                    completed_count = 0
                    error_count = 0
                    failed_runtime_batches: list[dict] = []

                    # Время начала этапа — для фильтрации файлов от старых запусков
                    batch_stage_start = datetime.now().timestamp()
                    # Smart retry: при повторе конкретного этапа сохраняем успешные пакеты
                    _smart_retry = resume_info.get("is_stage_retry", False) and start_idx == 3
                    if _smart_retry:
                        _existing = sum(
                            1 for b in batches
                            if (output_dir / f"block_batch_{b['batch_id']:03d}.json").exists()
                            and (output_dir / f"block_batch_{b['batch_id']:03d}.json").stat().st_size > 100
                        )
                        _to_redo = total_batches - _existing
                        await self._log(
                            job,
                            f"Smart retry: {_existing} пакетов готовы, {_to_redo} будут перезапущены"
                        )

                    async def _process_batch(batch):
                        nonlocal completed_count, error_count
                        batch_id = batch["batch_id"]

                        result_file = output_dir / f"block_batch_{batch_id:03d}.json"
                        if result_file.exists() and result_file.stat().st_size > 100:
                            # Smart retry: при повторе этапа сохраняем валидные файлы от прошлого запуска
                            _is_from_current_run = result_file.stat().st_mtime >= batch_stage_start
                            if _smart_retry or _is_from_current_run:
                                completed_count += 1
                                job.progress_current = completed_count
                                await self._progress(job, completed_count, total_batches)
                                if _smart_retry and not _is_from_current_run:
                                    size_kb = round(result_file.stat().st_size / 1024, 1)
                                    await self._log(job, f"Пакет {batch_id}/{total_batches}: ✓ пропуск ({size_kb} KB из прошлого запуска)")
                                return
                            else:
                                # Файл от старого запуска — удаляем и обрабатываем заново
                                result_file.unlink()

                        async with semaphore:
                            if job.status == JobStatus.CANCELLED:
                                return
                            if error_count >= 5:
                                return

                            can_go = await self._check_before_launch(job)
                            if not can_go:
                                return

                            block_count = batch.get("block_count", len(batch.get("blocks", [])))
                            single_block_id = ""
                            if batch.get("single_block_mode") and batch.get("blocks"):
                                single_block_id = batch["blocks"][0].get("block_id", "")
                                await self._log(job, f"Блок {batch_id}/{total_batches}: {single_block_id}...")
                            else:
                                await self._log(job, f"Пакет {batch_id}/{total_batches}: {block_count} блоков...")

                            retries = 0
                            pause_before_batch = job.pause_total_sec
                            while retries <= RATE_LIMIT_MAX_RETRIES:
                                batch_start_time = datetime.now()
                                job.batch_started_at = batch_start_time.isoformat()

                                exit_code, output_text, cli_result = await claude_runner.run_block_batch(
                                    batch, project_info, pid, total_batches,
                                    on_output=lambda msg: self._log(job, msg),
                                )
                                self._record_cli_usage(job, cli_result, f"block_batch_{batch_id:03d}")

                                batch_wall = (datetime.now() - batch_start_time).total_seconds()
                                batch_pause = job.pause_total_sec - pause_before_batch
                                batch_duration = max(0, batch_wall - batch_pause)
                                job.batch_durations.append(batch_duration)

                                if exit_code == 0:
                                    if result_file.exists():
                                        size_kb = round(result_file.stat().st_size / 1024, 1)
                                        success_message = (
                                            f"Блок {batch_id}/{total_batches}: {single_block_id} — OK ({size_kb} KB)"
                                            if single_block_id else
                                            f"Пакет {batch_id}/{total_batches}: OK ({size_kb} KB)"
                                        )
                                        await self._log(job, success_message)
                                    break

                                if claude_runner.is_cancelled(exit_code):
                                    break

                                stdout_text = output_text or ""
                                stderr_text = cli_result.result_text if cli_result and cli_result.is_error else ""

                                # Таймаут + можно разбить → split & retry
                                if claude_runner.is_timeout(exit_code) and block_count > 3:
                                    await self._log(
                                        job,
                                        f"Пакет {batch_id}: таймаут ({block_count} блоков) — разбиваю пополам",
                                        "warn",
                                    )
                                    split_ok = await self._retry_batch_split(
                                        job, batch, project_info, pid,
                                        total_batches, batch_id, output_dir,
                                    )
                                    if split_ok:
                                        exit_code = 0  # считаем успехом
                                    break

                                # "Prompt is too long" — нерепетируемая, retry бесполезен
                                if claude_runner.is_prompt_too_long(exit_code, stdout_text, stderr_text):
                                    await self._log(job, f"Prompt is too long", "error")
                                    await self._log(job, f"Пакет {batch_id}: слишком много блоков ({block_count}), пропускаем", "warn")
                                    break

                                if claude_runner.is_rate_limited(exit_code, stdout_text, stderr_text):
                                    retries += 1
                                    if retries <= RATE_LIMIT_MAX_RETRIES:
                                        # Jitter 5-30 сек чтобы параллельные пакеты не retry одновременно
                                        jitter = random.uniform(5, 30)
                                        await asyncio.sleep(jitter)
                                        can_continue = await self._wait_for_rate_limit(
                                            job, f"пакет {batch_id}", cli_output=stdout_text
                                        )
                                        if not can_continue:
                                            error_count += 1
                                            break
                                        continue
                                else:
                                    break

                            if exit_code != 0 and not claude_runner.is_cancelled(exit_code):
                                error_count += 1
                                err_detail = _extract_error_detail(exit_code, output_text or "", max_len=160)
                                failed_runtime_batches.append(
                                    _runtime_batch_failure_entry(
                                        batch, err_detail, reason="single_block_analysis_failed",
                                    )
                                )
                                error_prefix = (
                                    f"Блок {batch_id}/{total_batches}: {single_block_id}"
                                    if single_block_id else
                                    f"Пакет {batch_id}"
                                )
                                await self._log(job, f"{error_prefix}: ошибка (код {exit_code}) — {err_detail}", "error")
                            else:
                                completed_count += 1
                                job.progress_current = completed_count
                                await self._progress(job, completed_count, total_batches)

                    # Запуск батчей (готовые пропустятся внутри _process_batch)
                    tasks = []
                    for batch in batches:
                        tasks.append(asyncio.create_task(_process_batch(batch)))

                    if tasks:
                        gathered = await asyncio.gather(*tasks, return_exceptions=True)
                        for batch, result in zip(batches, gathered):
                            if isinstance(result, Exception):
                                error_count += 1
                                err_detail = f"{type(result).__name__}: {result}"
                                failed_runtime_batches.append(
                                    _runtime_batch_failure_entry(
                                        batch, err_detail, reason="single_block_task_exception",
                                    )
                                )
                                await self._log(
                                    job,
                                    f"Single-block task exception "
                                    f"{_runtime_batch_failure_entry(batch, err_detail, reason='single_block_task_exception').get('block_id')}: "
                                    f"{err_detail}",
                                    "error",
                                )

                    _write_block_analysis_runtime_summary(
                        output_dir,
                        runtime_plan,
                        failed_batches=failed_runtime_batches,
                        completed_batches=completed_count,
                    )

                    if error_count > 0:
                        if error_count >= total_batches:
                            self._update_pipeline_log(pid, "block_analysis", "error",
                                                       error=f"{error_count} single-block задач с ошибками",
                                                       detail={"failed_blocks": failed_runtime_batches})
                            raise RuntimeError(f"Все пакеты завершились с ошибками")
                        self._update_pipeline_log(pid, "block_analysis", "partial",
                                                   message=f"{error_count} single-block задач с ошибками",
                                                   detail={"failed_blocks": failed_runtime_batches})
                    else:
                        self._update_pipeline_log(pid, "block_analysis", "done",
                                                   message=f"OK ({total_batches} пакетов)")

                # Слияние результатов block_batch_*.json → 02_blocks_analysis.json
                print(f"[{pid}:resume] Слияние block_batch_*.json → 02_blocks_analysis.json")
                await self._log(job, "Слияние результатов блоков...")
                exit_code, _, stderr = await self._run_script_for_job(
                    job,
                    str(BLOCKS_SCRIPT),
                    ["merge", self._project_path_for_job(job)],
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code != 0:
                    await self._log(job, f"Ошибка слияния: {stderr}", "warn")

                if job.status == JobStatus.CANCELLED:
                    return

                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()

                # ═══ ЭТАП 4b: Block Retry — перекачка нечитаемых блоков ═══
                await self._run_block_retry(job, pid, project_info, output_dir)

            # ═══ ЭТАП 5: Свод замечаний ═══
            if start_idx <= 4:
                if start_idx == 4:
                    await self._assert_gemma_ready_for_stage(job, project_info, "findings_merge")
                    self._assert_text_analysis_exists(output_dir, "findings_merge")
                    if not (output_dir / "02_blocks_analysis.json").exists():
                        raise RuntimeError(
                            "Нельзя запускать findings_merge: 02_blocks_analysis.json отсутствует. "
                            "Сначала выполните block_analysis."
                        )
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

                # «Размышление модели»: стрим найденных замечаний в live-лог (WS)
                await self._stream_findings_events(job, "merge")

                if job.status == JobStatus.CANCELLED:
                    return

                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАПЫ 5.5-6: Параллельный запуск critic + norms (+ optimization) ═══
            # output_dir уже version-aware (см. начало _run_resumed_pipeline).
            if start_idx < 5:
                # Полный post-findings: critic + norms + optimization (параллельно)
                findings_path = output_dir / "03_findings.json"
                if findings_path.exists():
                    await self._run_post_findings_parallel(job, project_info)

                    if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                        return

                    self.active_jobs[pid] = job
                    self._tasks[pid] = asyncio.current_task()
                else:
                    await self._log(job, "03_findings.json не найден — пропуск верификации", "warn")

            # Resume только findings_review (critic+corrector) — без повтора norms/optimization
            if start_idx == 5:
                findings_path = output_dir / "03_findings.json"
                if findings_path.exists():
                    await self._start_heartbeat(job)
                    await self._run_findings_review(job, project_info)

                    if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                        return

                    # Проверяем: если critic провалился — не маскировать ошибку
                    _plog_path = output_dir / "pipeline_log.json"
                    try:
                        _plog = json.loads(_plog_path.read_text(encoding="utf-8")) if _plog_path.exists() else {}
                    except Exception:
                        _plog = {}
                    _critic_status = _plog.get("stages", {}).get("findings_critic", {}).get("status")
                    if _critic_status == "error":
                        job.status = JobStatus.FAILED
                        job.error_message = "Findings critic провалился"
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
            pause_note = f" (паузы: {round(job.pause_total_sec / 60, 1)} мин)" if job.pause_total_sec > 60 else ""
            print(f"[{pid}:resume] ═══ Конвейер завершён за {duration} мин{pause_note} ═══")
            await self._log(job, f"Конвейер завершён за {duration} мин{pause_note}.", "info")

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
                info_path = project_dir / "project_info.json"
                with open(info_path, "r", encoding="utf-8") as f:
                    info = json.load(f)
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
            info_path = project_dir / "project_info.json"
            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

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
                    if error_count >= 5:
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
                            if error_count >= 5:
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
            # Version-aware project_info: V1 = root, V2+ = _versions/v{N}/.
            _root_dir, _project_dir, _output_dir = self._resolve_job_paths(job)
            info_path = _project_dir / "project_info.json"
            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

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

    async def _retry_batch_split(
        self,
        job: AuditJob,
        batch: dict,
        project_info: dict,
        pid: str,
        total_batches: int,
        original_batch_id: int,
        output_dir: Path,
    ) -> bool:
        """Разбить упавший пакет пополам и запустить обе части.

        Результаты записываются как block_batch_NNNa.json и block_batch_NNNb.json.
        Слияние (blocks.py merge) подхватит все block_batch_*.json.

        Returns: True если обе половины успешны.
        """
        blocks = batch.get("blocks", [])
        mid = len(blocks) // 2
        halves = [blocks[:mid], blocks[mid:]]
        suffixes = ["a", "b"]
        success = True

        # Удалить частичный результат от таймаута
        orig_file = output_dir / f"block_batch_{original_batch_id:03d}.json"
        if orig_file.exists():
            orig_file.unlink()

        for half_blocks, suffix in zip(halves, suffixes):
            if not half_blocks:
                continue

            sub_batch = {
                "batch_id": original_batch_id,
                "blocks": half_blocks,
                "block_count": len(half_blocks),
                "pages_included": sorted(set(b.get("page", 0) for b in half_blocks)),
            }

            sub_label = f"{original_batch_id}{suffix}"
            await self._log(
                job,
                f"Пакет {sub_label}/{total_batches}: {len(half_blocks)} блоков (retry)...",
            )

            exit_code, output_text, cli_result = await claude_runner.run_block_batch(
                sub_batch, project_info, pid, total_batches,
                on_output=lambda msg: self._log(job, msg),
            )
            self._record_cli_usage(job, cli_result, f"block_batch_{original_batch_id:03d}{suffix}")

            # Claude CLI пишет результат как block_batch_<batch_id>.json
            # Переименовываем: block_batch_003.json → block_batch_003a.json
            written_file = output_dir / f"block_batch_{original_batch_id:03d}.json"
            split_file = output_dir / f"block_batch_{original_batch_id:03d}{suffix}.json"

            if exit_code == 0 and written_file.exists():
                written_file.rename(split_file)
                size_kb = round(split_file.stat().st_size / 1024, 1)
                await self._log(job, f"Пакет {sub_label}: OK ({size_kb} KB)")
            elif exit_code == 0:
                await self._log(job, f"Пакет {sub_label}: OK (файл не создан)", "warn")
            else:
                await self._log(job, f"Пакет {sub_label}: ошибка (код {exit_code})", "error")
                success = False

        return success

    async def _run_findings_review(self, job: AuditJob, project_info: dict):
        """Тонкий оркестратор: делегирует в findings_review/runner.py.

        Оркестраторная логика (job.stage, job.status) остаётся здесь.
        Бизнес-логика critic + corrector — в runner.
        """
        job.stage = AuditStage.FINDINGS_REVIEW
        job.status = JobStatus.RUNNING
        ctx = self._make_stage_context(job)
        result = await _run_findings_review_stage(ctx)
        if result.cancelled:
            job.status = JobStatus.CANCELLED
        elif result.error and not result.critic_ok:
            job.status = JobStatus.FAILED
            job.error_message = result.error

    # ─── Параллельный запуск post-findings этапов ───

    async def _run_post_findings_parallel(
        self,
        job: AuditJob,
        project_info: dict,
        include_optimization: bool = True,
    ):
        """
        Параллельный запуск после findings_merge:

        ┌─ findings_critic → corrector ──────────────┐
        ├─ norm_verify ──────────────────────────────┼─→ (done)
        └─ optimization → (ждёт corrector) → opt_review ─┘

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
            """Задача A: Critic → Corrector → signal corrector_done."""
            nonlocal review_error
            try:
                await self._run_findings_review(job, project_info)
            except Exception as e:
                await self._log(job, f"Findings review ошибка: {e}", "error")
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
                await self._log(job, "═══ Верификация нормативных ссылок (параллельно с Critic) ═══")
                await self._run_norm_verification(
                    job, standalone=False, wait_before_fix=corrector_done,
                )
            except Exception as e:
                await self._log(job, f"Norm verify ошибка: {e}", "error")
                self._update_pipeline_log(pid, "norm_verify", "error", error=str(e))

        async def _task_optimization():
            """Задача C: Optimization → ждёт corrector → opt_critic → opt_corrector."""
            print(f"[{pid}] _task_optimization STARTED")
            try:
                # Optimization сам по себе НЕ зависит от corrector
                opt_job = AuditJob(
                    job_id=job.job_id + "_opt",
                    object_id=self._resolve_object_id(None),
                    project_id=pid,
                    stage=AuditStage.OPTIMIZATION,
                    status=JobStatus.RUNNING,
                    started_at=datetime.now().isoformat(),
                )
                print(f"[{pid}] ═══ Оптимизация (параллельно) ═══")
                await self._log(job, "═══ Оптимизация (параллельно с Critic) ═══")

                await self._run_optimization(opt_job, standalone=False)

                if opt_job.status != JobStatus.COMPLETED:
                    await self._log(
                        job,
                        f"Оптимизация: {opt_job.status.value}"
                        + (f" — {opt_job.error_message}" if opt_job.error_message else ""),
                        "warn",
                    )
                    return

                # Opt_critic ЖДЁТ corrector (нужны финальные findings для проверки конфликтов)
                await self._log(job, "Оптимизация готова, ожидание Corrector для opt_critic...")
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
                    )
            except Exception as e:
                await self._log(job, f"Optimization ошибка: {e}", "error")
                self._update_pipeline_log(pid, "optimization", "error", error=str(e))

        # Запускаем параллельные задачи
        tasks = [
            asyncio.create_task(_task_findings_review()),
            asyncio.create_task(_task_norm_verify()),
        ]

        if include_optimization:
            tasks.append(asyncio.create_task(_task_optimization()))

        await self._log(
            job,
            f"═══ Параллельный запуск: Critic + Нормы"
            + (" + Оптимизация" if include_optimization else "")
            + " ═══",
        )

        print(f"[{pid}] Parallel tasks created: {len(tasks)} (include_optimization={include_optimization})")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        print(f"[{pid}] Parallel tasks completed: {[type(r).__name__ if isinstance(r, Exception) else 'ok' for r in results]}")
        # Логируем ошибки из параллельных задач
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                task_name = ["findings_review", "norm_verify", "optimization"][i] if i < 3 else f"task_{i}"
                await self._log(job, f"Параллельная задача {task_name} упала: {result}", "error")
                print(f"[{pid}] Parallel task {task_name} exception: {result}")

        coverage = self._attach_stage02_coverage_to_findings(pid)
        excluded_count = (coverage.get("summary") or {}).get("excluded_from_full_analysis_count", 0)
        if excluded_count:
            await self._log(
                job,
                f"Финальная coverage-сводка обновлена: {excluded_count} блоков вне полноценного анализа",
                "warn",
            )

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

            ctx = self._make_stage_context(job)
            result = await _run_norm_verification_stage(
                ctx,
                wait_before_fix=wait_before_fix,
            )

            if result.cancelled:
                job.status = JobStatus.CANCELLED
                return

            if not result.success:
                job.status = JobStatus.FAILED
                job.error_message = result.error
                await self._log(job, f"Верификация норм: {result.error}", "error")
                return

            job.status = JobStatus.COMPLETED

        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            self._update_pipeline_log(pid, "norm_verify", "error", error="Отменено")
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            await self._log(job, f"Исключение: {e}", "error")
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

    # ─── Запуск интеллектуального аудита (smart) ───
    async def start_smart_audit(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
    ) -> AuditJob:
        """Интеллектуальный аудит: текст → триаж → выборочная нарезка → анализ."""
        return await self._enqueue_single(
            project_id, action="smart", version_id=version_id,
        )

    async def _run_smart_pipeline(self, job: AuditJob):
        """
        Smart Parallel Pipeline — параллельный интеллектуальный аудит.

        Этапы:
        1. Подготовка текста (process_project.py)
        2. Триаж страниц (отдельная Claude-сессия → 01_text_analysis.json)
        3. Выборочная нарезка тайлов (только HIGH+MEDIUM страницы)
        4. Параллельный анализ тайлов (N Claude-сессий одновременно)
        5. Свод замечаний (Claude-сессия → 03_findings.json + отчёт)
        6. [Опционально] Gap analysis → донарезка → доанализ (макс. 2 итерации)
        7. Верификация норм
        8. Excel
        """
        start_time = datetime.now()
        pid = job.project_id
        try:
            # Version-aware пути: для V1 это root проекта, для V2+ — _versions/v{N}/.
            # См. _resolve_job_paths() — единый helper, чтобы MD-check и source-файлы
            # больше не утекали из V1 root при V2-аудите.
            _root_dir, project_dir, output_dir = self._resolve_job_paths(job)
            info_path = project_dir / "project_info.json"

            # ═══ Проверка MD-файла (обязательный источник текста) ═══
            md_candidates = [
                f for f in project_dir.iterdir()
                if f.suffix == ".md" and f.name.endswith("_document.md")
            ]
            if not md_candidates:
                raise RuntimeError(
                    f"MD-файл не найден для проекта {pid}. "
                    f"Анализ без MD-файла не поддерживается. "
                    f"Создайте MD через Chandra OCR и положите в папку проекта."
                )

            # ═══ ЭТАП 1: Подготовка текста ═══
            job.stage = AuditStage.PREPARE
            self._update_pipeline_log(pid, "prepare", "running")
            print(f"[{pid}:smart] ═══ ЭТАП 1: Подготовка текста ═══")
            await self._log(job, "═══ ЭТАП 1: Подготовка текста ═══")

            exit_code, _, stderr = await self._run_script_for_job(
                job,
                str(PROCESS_PROJECT_SCRIPT),
                [self._project_path_for_job(job)],
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code != 0:
                self._update_pipeline_log(pid, "prepare", "error",
                                           error=stderr or f"Exit code: {exit_code}")
                raise RuntimeError(f"Подготовка: {stderr}")
            self._update_pipeline_log(pid, "prepare", "done", message="OK")
            print(f"[{pid}:smart] ЭТАП 1 OK")

            if job.status == JobStatus.CANCELLED:
                return

            # ═══ ЭТАП 2: Триаж страниц (отдельная Claude-сессия) ═══
            self._clean_stage_files(pid, [
                "00_init.json", "01_text_analysis.json",
                "02_tiles_analysis.json", "03_findings.json",
                "tile_batch_*.json", "tile_batches.json",
            ])
            self._reset_job_progress(job)
            job.stage = AuditStage.MAIN_AUDIT
            job.status = JobStatus.RUNNING
            print(f"[{pid}:smart] ═══ ЭТАП 2: Триаж страниц ═══")
            await self._log(job, "═══ ЭТАП 2: Триаж страниц (Claude определяет приоритеты) ═══")

            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            _triage_result = await _run_text_analysis_stage(
                self._make_stage_context(job),
                use_triage=True,
                with_rate_limit_retry=True,
                stage_label="text_analysis",
            )
            if _triage_result.cancelled:
                job.status = JobStatus.CANCELLED
                await self._log(job, "Триаж отменён", "warn")
                return
            if not _triage_result.success:
                raise RuntimeError(_triage_result.error or "Триаж: ошибка")

            # Прочитать результат триажа — оркестратор читает priority_pages сам
            triage_file = output_dir / "01_text_analysis.json"
            with open(triage_file, "r", encoding="utf-8") as f:
                triage_data = json.load(f)

            page_triage = triage_data.get("page_triage", [])
            priority_pages = [
                pt["page"] for pt in page_triage
                if pt.get("priority") in ("HIGH", "MEDIUM")
            ]
            # Обновить log message с количеством приоритетных страниц
            self._update_pipeline_log(pid, "text_analysis", "done",
                                       message=f"{len(priority_pages)} приоритетных из {len(page_triage)}")
            print(f"[{pid}:smart] Триаж: {len(priority_pages)} приоритетных страниц из {len(page_triage)}")
            await self._log(job, f"Триаж завершён: {len(priority_pages)} приоритетных страниц ({priority_pages})")

            if job.status == JobStatus.CANCELLED:
                return

            # ═══ ЭТАП 3: Выборочная нарезка тайлов ═══
            if priority_pages:
                # FIXME (Pass C, 2026-05-14): tile-нарезка через
                # process_project.py с `--pages`/`--quality` фактически НЕ
                # работала — process_project.py этих аргументов не понимает
                # (argparse → exit 2). Раньше код всё равно вызывал subprocess
                # и падал с traceback. Теперь — controlled failure: явно
                # сообщаем, что фича временно отключена, и job завершается
                # без запуска дорогих стадий.
                #
                # TODO: либо реализовать полноценный --pages в
                # process_project.py (отдельный pass), либо переписать
                # priority_pages branch на отдельный supported script.
                pages_str = ",".join(str(p) for p in priority_pages)
                msg = (
                    f"priority_pages smart-pipeline branch is temporarily "
                    f"disabled because process_project.py does not support "
                    f"--pages/--quality (запрошены страницы: {pages_str})"
                )
                print(f"[{pid}:smart] ═══ ЭТАП 3 SKIP: {msg} ═══")
                await self._log(job, msg, "error")
                self._update_pipeline_log(pid, "prepare", "error", error=msg)
                raise RuntimeError(msg)
            else:
                await self._log(job, "Нет приоритетных страниц — пропуск нарезки", "warn")

            if job.status == JobStatus.CANCELLED:
                return

            # ═══ ЭТАП 4: Параллельный анализ тайлов ═══
            max_iterations = 3
            all_analyzed_pages = list(priority_pages)

            for iteration in range(1, max_iterations + 1):
                current_pages = priority_pages if iteration == 1 else additional_pages

                if not current_pages:
                    break

                self._clean_stage_files(pid, ["tile_batch_*.json", "tile_batches.json"])
                self._reset_job_progress(job)
                job.status = JobStatus.RUNNING

                iter_label = f" (итерация {iteration})" if iteration > 1 else ""
                print(f"[{pid}:smart] ═══ ЭТАП 4{iter_label}: Параллельный анализ тайлов ═══")
                await self._log(job, f"═══ ЭТАП 4{iter_label}: Параллельный анализ тайлов ({len(current_pages)} стр.) ═══")

                # Re-register job (tile audit cleanup removes it)
                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()

                await self._run_tile_audit(job, start_from=1, pages_filter=current_pages)
                print(f"[{pid}:smart] ЭТАП 4{iter_label} завершён, status={job.status.value}")

                if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                    return

                # Re-register after tile audit
                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()

                # ═══ ЭТАП 5: Свод замечаний + Gap Analysis ═══
                self._reset_job_progress(job)
                job.stage = AuditStage.MAIN_AUDIT
                job.status = JobStatus.RUNNING
                self._update_pipeline_log(pid, "main_audit", "running")
                print(f"[{pid}:smart] ═══ ЭТАП 5{iter_label}: Свод замечаний ═══")
                await self._log(job, f"═══ ЭТАП 5{iter_label}: Свод замечаний + анализ пробелов ═══")

                # Перечитываем project_info (могли обновиться tile_config)
                with open(info_path, "r", encoding="utf-8") as f:
                    project_info = json.load(f)

                # ── Проверка rate limit перед сводом замечаний ──
                can_go = await self._check_before_launch(job)
                if not can_go:
                    raise RuntimeError("Rate limit: ожидание превышено или отменено")

                exit_code, output, cli_result = await claude_runner.run_smart_merge(
                    project_info, pid,
                    on_output=lambda msg: self._log(job, msg),
                )
                self._record_cli_usage(job, cli_result, "smart_merge")
                if claude_runner.is_cancelled(exit_code):
                    job.status = JobStatus.CANCELLED
                    await self._log(job, "Свод замечаний отменён", "warn")
                    return
                if claude_runner.is_rate_limited(exit_code, output or "", ""):
                    await self._log(job, "Rate limit при своде замечаний, ожидание...", "warn")
                    can_continue = await self._wait_for_rate_limit(job, "rate limit при своде замечаний", cli_output=output or "")
                    if can_continue:
                        exit_code, output, cli_result = await claude_runner.run_smart_merge(
                            project_info, pid,
                            on_output=lambda msg: self._log(job, msg),
                        )
                        self._record_cli_usage(job, cli_result, "smart_merge_retry")
                if exit_code != 0:
                    await self._log(job, f"Свод замечаний: код {exit_code}", "error")
                    self._update_pipeline_log(pid, "main_audit", "error",
                                               error=f"Свод: код {exit_code}")
                    # Не fatal — продолжаем
                else:
                    self._update_pipeline_log(pid, "main_audit", "done", message="OK")

                # Проверяем gap_analysis — нужны ли ещё страницы?
                additional_pages = []
                findings_path = output_dir / "03_findings.json"
                if findings_path.exists() and iteration < max_iterations:
                    try:
                        with open(findings_path, "r", encoding="utf-8") as f:
                            findings_data = json.load(f)
                        gap = findings_data.get("gap_analysis")
                        if gap and gap.get("additional_pages_needed"):
                            additional_pages = [
                                p for p in gap["additional_pages_needed"]
                                if p not in all_analyzed_pages
                            ]
                            if additional_pages:
                                all_analyzed_pages.extend(additional_pages)
                                pages_str = ",".join(str(p) for p in additional_pages)
                                await self._log(job, f"Gap analysis: нужны ещё страницы {pages_str}")

                                # FIXME (Pass C, 2026-05-14): см. priority_pages
                                # выше — process_project.py не понимает
                                # `--pages`/`--quality`. Раньше код всё равно
                                # вызывал subprocess и ловил exit 2 в warn.
                                # Теперь — controlled skip с понятным логом
                                # вместо генерации мусорного stderr.
                                await self._log(
                                    job,
                                    "Донарезка тайлов временно отключена: "
                                    "process_project.py не поддерживает "
                                    "--pages/--quality. Gap-страницы пропущены: "
                                    f"{pages_str}",
                                    "warn",
                                )
                                additional_pages = []
                    except Exception as e:
                        print(f"[{pid}:smart] Gap analysis error: {e}")

                if not additional_pages:
                    break

            if job.status == JobStatus.CANCELLED:
                return

            # Re-register
            self.active_jobs[pid] = job
            self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАПЫ 5.5-6: Параллельный запуск critic + norms ═══
            # output_dir уже version-aware (см. начало _run_smart_pipeline).
            findings_path = output_dir / "03_findings.json"
            if findings_path.exists():
                await self._run_post_findings_parallel(
                    job, project_info, include_optimization=False,
                )

                if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                    return

                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()
            else:
                await self._log(job, "03_findings.json не найден — пропуск верификации", "warn")

            # ═══ ЭТАП 7: Excel ═══
            self._reset_job_progress(job)
            job.stage = AuditStage.EXCEL
            job.status = JobStatus.RUNNING
            print(f"[{pid}:smart] ═══ ЭТАП 7: Excel ═══")
            from backend.app.pipeline.stages.report.runner import run_excel_report as _run_excel
            _xls_result = await _run_excel(self._make_stage_context(job))
            if not _xls_result.success:
                await self._log(job, f"Excel-отчёт не создан: {_xls_result.error}", "warn")

            wall_sec = (datetime.now() - start_time).total_seconds()
            net_sec = max(0, wall_sec - job.pause_total_sec)
            duration = round(net_sec / 60, 1)
            job.status = JobStatus.COMPLETED
            pause_note = f" (паузы: {round(job.pause_total_sec / 60, 1)} мин)" if job.pause_total_sec > 60 else ""
            print(f"[{pid}:smart] ═══ Smart Parallel завершён за {duration} мин{pause_note} ═══")
            await self._log(job, f"Smart Parallel конвейер завершён за {duration} мин{pause_note}.", "info")

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
            job.completed_at = datetime.now().isoformat()
            self._cleanup(pid)

    # ─── Запуск аудита (OCR-пайплайн) ───
    async def start_audit(
        self,
        project_id: str,
        *,
        version_id: Optional[str] = None,
        manual_run_id: Optional[str] = None,
    ) -> AuditJob:
        """Аудит: кроп блоков → текстовый анализ → ВСЕ блоки → свод.

        Single-start — кладёт задачу в общую очередь. Реальный запуск
        случится, когда worker возьмёт её. kill зомби, сброс audit_log/usage
        живут в `_dispatch_action`, чтобы не сбрасывать данные на ещё не
        стартовавший проект.

        `version_id`: фиксированная версия проекта, в которую пойдут все
        write-операции. None → latest на момент enqueue.

        `manual_run_id`: scope разрешения платных API. Без него Stage 02
        (GPT/OpenRouter) и discussions будут заблокированы paid_api_guard.
        """
        self._assert_stage_model_config_ready()
        return await self._enqueue_single(
            project_id, action="full", version_id=version_id,
            manual_run_id=manual_run_id,
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

        Собирает все блоки с unreadable_text=true из 02_blocks_analysis.json,
        перекропает их (до MAX_RECROP_ITERATIONS раз, ×2 на итерации),
        создаёт мини-батч только для них и повторно прогоняет через блок-анализ
        (Gemini через OpenRouter). Результаты merge'атся поверх существующего
        02_blocks_analysis.json — перезаписываются только затронутые block_id.

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
                promote_result = promote_to_full(proj_path, block_ids)
                if promote_result.get("promoted", 0) == 0:
                    await self._log(job, "Block retry: нет full-версий для промоута")
                    break
            else:
                await self._log(job, f"Block retry (итерация {retry_iter}): {len(block_ids)} нечитаемых блоков → перекачка ×2")
                self._update_pipeline_log(pid, "block_retry", "running",
                                          message=f"Итерация {retry_iter}: {len(block_ids)} блоков")
                recrop_result = recrop_blocks(proj_path, block_ids, scale_multiplier=2.0)
                if recrop_result.get("recropped", 0) == 0:
                    await self._log(job, "Block retry: все блоки уже на максимальном разрешении, стоп")
                    break

            exit_code, _, _ = await self._run_script_for_job(
                job, str(BLOCKS_SCRIPT),
                # --solo: 1 блок = 1 пакет, модель фокусируется на одной картинке
                # (retry именно по ней и шёл, контекст других блоков уже есть в 02_blocks_analysis.json)
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

    async def _run_ocr_pipeline(self, job: AuditJob, include_optimization: bool = True):
        """
        OCR-пайплайн: полный аудит всех блоков.

        Этапы:
        1. blocks.py crop → _output/blocks_gemma_100/
        2. Gemma base 100 DPI + optional targeted high-detail 300 DPI
        3. Claude: text_analysis → 01_text_analysis.json
        4. Stage 02 crop → _output/blocks_stage02_100/
        5. findings_only_gemma_pair → 02_blocks_analysis.json
        6. Claude: findings_merge → 03_findings.json
        7. norm_verify
        8. Excel
        """
        start_time = datetime.now()
        pid = job.project_id
        try:
            # Version-aware пути: V1 = root, V2+ = _versions/v{N}/.
            _root_dir, project_dir, output_dir = self._resolve_job_paths(job)
            info_path = project_dir / "project_info.json"

            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            # ═══ Проверка MD-файла (обязательный источник текста) ═══
            if find_project_markdown(project_dir, project_info) is None:
                raise RuntimeError(
                    f"MD-файл не найден для проекта {pid}. "
                    f"{GEMMA_STAGE_LABEL} и анализ без MD-файла не поддерживаются. "
                    f"Создайте MD через Chandra OCR и положите в папку проекта."
                )

            # ═══ ЭТАП 1: Кроп image-блоков ═══
            job.stage = AuditStage.CROP_BLOCKS
            blocks_index = gemma_blocks_index_path(project_dir)
            gemma_crop_policy = gemma_enrichment_crop_policy()
            blocks_dir = gemma_blocks_dir(project_dir)
            needs_recrop = (
                (blocks_index.exists() and not _existing_crop_matches_policy(blocks_index, gemma_crop_policy))
                or (not blocks_index.exists() and blocks_dir.exists() and any(blocks_dir.glob("block_*.png")))
            )
            if blocks_index.exists() and not needs_recrop:
                # Блоки уже скачаны (pre-crop из очереди) и совместимы с текущим режимом
                self._update_pipeline_log(pid, "crop_blocks", "done", message="Pre-cropped")
                print(f"[{pid}] ═══ ЭТАП 1: Кроп — уже готов (pre-crop) ═══")
                await self._log(job, "═══ ЭТАП 1: Кроп image-блоков — уже готов (pre-crop) ═══")
            else:
                self._update_pipeline_log(pid, "crop_blocks", "running")
                print(f"[{pid}] ═══ ЭТАП 1: Кроп image-блоков ═══")
                await self._log(job, "═══ ЭТАП 1: Кроп image-блоков из PDF ═══")
                if needs_recrop:
                    await self._log(
                        job,
                        "Существующий crop не совпадает с Gemma enrichment policy "
                        f"({_crop_policy_label(gemma_crop_policy)}) — перекропаем с --force",
                    )

                crop_args = _build_crop_args(
                    self._project_path_for_job(job),
                    force=needs_recrop,
                    policy=gemma_crop_policy,
                    output_dir_name=GEMMA_BLOCKS_DIRNAME,
                )
                exit_code, _, stderr = await self._run_script_for_job(
                    job,
                    str(BLOCKS_SCRIPT),
                    crop_args,
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code != 0:
                    self._update_pipeline_log(pid, "crop_blocks", "error",
                                               error=stderr or f"Exit code: {exit_code}")
                    raise RuntimeError(f"Кроп блоков: {stderr}")
                self._update_pipeline_log(
                    pid, "crop_blocks", "done",
                    message=f"OK (Gemma policy: {_crop_policy_label(gemma_crop_policy)})",
                )
                print(f"[{pid}] ЭТАП 1 OK (Gemma crop policy)")

            # Построить document_graph v2 (Python, без LLM)
            await self._build_document_graph_v2(job)

            if job.status == JobStatus.CANCELLED:
                return

            # ЭТАП 00: Gemma-обогащение MD (всегда выполняется, идемпотентно)
            await self._run_gemma_enrichment_stage(job)

            if job.status == JobStatus.CANCELLED:
                return

            # ═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══
            files_to_clean = [
                "01_text_analysis.json",
                "03_findings.json", "03_findings_review.json", "03_findings_pre_review.json",
                "block_batch_*.json", "block_batches.json", RUNTIME_BATCHES_FILE,
                "block_analysis_summary.json",
                "02_blocks_analysis.json",
            ]
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
                raise RuntimeError(_ta_result.error or "Текстовый анализ: ошибка")

            print(f"[{pid}] ЭТАП 2 OK")

            if job.status == JobStatus.CANCELLED:
                return

            if get_stage_batch_mode("block_batch") == "findings_only_gemma_pair":
                # findings_only_gemma_pair: single-block GPT-5.4 + gemma-enrichment.
                # Пишет 02_blocks_analysis.json напрямую, без block_batches.json.
                await self._run_block_analysis_findings_only(job)
                if job.status == JobStatus.CANCELLED:
                    return
                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()
            else:
                # ═══ ЭТАП 3: Генерация пакетов блоков ═══
                self._reset_job_progress(job)
                job.stage = AuditStage.CROP_BLOCKS  # reuse для генерации батчей

                # Все блоки — полное покрытие
                gen_args = [self._project_path_for_job(job)]
                await self._log(job, "Анализ ВСЕХ image-блоков")

                print(f"[{pid}] ═══ ЭТАП 3: Генерация пакетов блоков ═══")
                await self._log(job, "═══ ЭТАП 3: Генерация пакетов блоков ═══")

                exit_code, _, stderr = await self._run_script_for_job(
                    job,
                    str(BLOCKS_SCRIPT),
                    ["batches"] + gen_args,
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code != 0:
                    raise RuntimeError(f"Генерация пакетов: {stderr}")

                # Загружаем пакеты
                batches_file = output_dir / "block_batches.json"
                if not batches_file.exists():
                    raise RuntimeError("block_batches.json не создан")

                with open(batches_file, "r", encoding="utf-8") as f:
                    batches_data = json.load(f)

                runtime_plan = _load_or_create_single_block_runtime_plan(
                    output_dir,
                    batches_data.get("batches", []),
                    force_rebuild=True,
                )
                batches = runtime_plan.get("batches", [])
                single_block_mode = runtime_plan.get("mode") == "single_block"
                total_batches = len(batches)

                if total_batches == 0:
                    await self._log(job, "Нет пакетов для анализа — переход к своду", "warn")
                else:
                    # ═══ ЭТАП 4: Параллельный анализ блоков ═══
                    self._clean_stage_files(pid, ["block_batch_*.json"])
                    self._reset_job_progress(job)
                    job.stage = AuditStage.BLOCK_ANALYSIS
                    job.status = JobStatus.RUNNING
                    job.progress_total = total_batches
                    self._update_pipeline_log(pid, "block_analysis", "running")

                    parallel = get_block_batch_parallelism("block_batch")
                    mode_label = (
                        f"{total_batches} single-block запросов"
                        if single_block_mode else f"{total_batches} пакетов"
                    )
                    print(f"[{pid}] ═══ ЭТАП 4: Анализ блоков ({mode_label} x{parallel}) ═══")
                    await self._log(
                        job,
                        f"═══ ЭТАП 4: Анализ блоков ({mode_label}, x{parallel} параллельно) ═══"
                    )
                    if single_block_mode:
                        await self._log(
                            job,
                            f"Stage 02 runtime plan: {RUNTIME_BATCHES_FILE} "
                            f"({total_batches} single-block задач)",
                        )

                    semaphore = asyncio.Semaphore(parallel)
                    completed_count = 0
                    error_count = 0
                    failed_runtime_batches: list[dict] = []
                    # Время начала этапа — для фильтрации файлов от старых запусков
                    block_stage_start = datetime.now().timestamp()

                    async def _process_block_batch(batch):
                        nonlocal completed_count, error_count
                        batch_id = batch["batch_id"]

                        result_file = output_dir / f"block_batch_{batch_id:03d}.json"
                        if result_file.exists() and result_file.stat().st_size > 100:
                            # Проверяем что файл от ТЕКУЩЕГО запуска, а не от старого
                            if result_file.stat().st_mtime >= block_stage_start:
                                completed_count += 1
                                job.progress_current = completed_count
                                await self._progress(job, completed_count, total_batches)
                                return
                            else:
                                # Файл от старого запуска — удаляем и обрабатываем заново
                                result_file.unlink()

                        async with semaphore:
                            if job.status == JobStatus.CANCELLED:
                                return
                            if error_count >= 5:
                                return

                            can_go = await self._check_before_launch(job)
                            if not can_go:
                                return

                            block_count = batch.get("block_count", len(batch.get("blocks", [])))
                            single_block_id = ""
                            if batch.get("single_block_mode") and batch.get("blocks"):
                                single_block_id = batch["blocks"][0].get("block_id", "")
                                await self._log(job, f"Блок {batch_id}/{total_batches}: {single_block_id}...")
                            else:
                                await self._log(job, f"Пакет {batch_id}/{total_batches}: {block_count} блоков...")

                            retries = 0
                            pause_before_batch = job.pause_total_sec
                            while retries <= RATE_LIMIT_MAX_RETRIES:
                                batch_start_time = datetime.now()
                                job.batch_started_at = batch_start_time.isoformat()

                                exit_code, output_text, cli_result = await claude_runner.run_block_batch(
                                    batch, project_info, pid, total_batches,
                                    on_output=lambda msg: self._log(job, msg),
                                )
                                self._record_cli_usage(job, cli_result, f"block_batch_{batch_id:03d}")

                                batch_wall = (datetime.now() - batch_start_time).total_seconds()
                                batch_pause = job.pause_total_sec - pause_before_batch
                                batch_duration = max(0, batch_wall - batch_pause)
                                job.batch_durations.append(batch_duration)

                                if exit_code == 0:
                                    if result_file.exists():
                                        size_kb = round(result_file.stat().st_size / 1024, 1)
                                        success_message = (
                                            f"Блок {batch_id}/{total_batches}: {single_block_id} — OK ({size_kb} KB)"
                                            if single_block_id else
                                            f"Пакет {batch_id}/{total_batches}: OK ({size_kb} KB)"
                                        )
                                        await self._log(job, success_message)
                                    break

                                if claude_runner.is_cancelled(exit_code):
                                    break

                                stdout_text = output_text or ""
                                stderr_text = cli_result.result_text if cli_result and cli_result.is_error else ""

                                # Таймаут + можно разбить → split & retry
                                if claude_runner.is_timeout(exit_code) and block_count > 3:
                                    await self._log(
                                        job,
                                        f"Пакет {batch_id}: таймаут ({block_count} блоков) — разбиваю пополам",
                                        "warn",
                                    )
                                    split_ok = await self._retry_batch_split(
                                        job, batch, project_info, pid,
                                        total_batches, batch_id, output_dir,
                                    )
                                    if split_ok:
                                        exit_code = 0
                                    break

                                if claude_runner.is_rate_limited(exit_code, stdout_text, stderr_text):
                                    retries += 1
                                    if retries > RATE_LIMIT_MAX_RETRIES:
                                        error_count += 1
                                        break
                                    can_continue = await self._wait_for_rate_limit(
                                        job,
                                        f"rate limit при пакете {batch_id}",
                                        cli_output=f"{stdout_text}\n{stderr_text}",
                                    )
                                    if not can_continue:
                                        error_count += 1
                                        break
                                    continue
                                else:
                                    error_count += 1
                                    err_detail = _extract_error_detail(exit_code, output_text or "", max_len=160)
                                    failed_runtime_batches.append(
                                        _runtime_batch_failure_entry(
                                            batch, err_detail, reason="single_block_analysis_failed",
                                        )
                                    )
                                    await self._log(
                                        job,
                                        (
                                            f"Блок {batch_id}/{total_batches}: {single_block_id} — ОШИБКА (код {exit_code}) — {err_detail}"
                                            if single_block_id else
                                            f"Пакет {batch_id}/{total_batches}: ОШИБКА (код {exit_code}) — {err_detail}"
                                        ),
                                        "error",
                                    )
                                    break

                            completed_count += 1
                            job.progress_current = completed_count
                            await self._progress(job, completed_count, total_batches)

                    tasks = [_process_block_batch(batch) for batch in batches]
                    gathered = await asyncio.gather(*tasks, return_exceptions=True)
                    for batch, result in zip(batches, gathered):
                        if isinstance(result, Exception):
                            error_count += 1
                            err_detail = f"{type(result).__name__}: {result}"
                            failed_runtime_batches.append(
                                _runtime_batch_failure_entry(
                                    batch, err_detail, reason="single_block_task_exception",
                                )
                            )
                            await self._log(
                                job,
                                f"Single-block task exception "
                                f"{_runtime_batch_failure_entry(batch, err_detail, reason='single_block_task_exception').get('block_id')}: "
                                f"{err_detail}",
                                "error",
                            )

                    _write_block_analysis_runtime_summary(
                        output_dir,
                        runtime_plan,
                        failed_batches=failed_runtime_batches,
                        completed_batches=completed_count,
                    )

                    if error_count >= total_batches:
                        job.status = JobStatus.FAILED
                        job.error_message = f"Все {total_batches} пакетов с ошибкой"
                        self._update_pipeline_log(pid, "block_analysis", "error",
                                                   error=f"Все {total_batches} пакетов с ошибкой")
                        return

                    # Слияние block_batch_*.json → 02_blocks_analysis.json
                    await self._log(job, "Слияние результатов анализа блоков...")
                    exit_code, _, stderr = await self._run_script_for_job(
                        job,
                        str(BLOCKS_SCRIPT),
                        ["merge", self._project_path_for_job(job)],
                        on_output=lambda msg: self._log(job, msg),
                    )
                    if exit_code == 0:
                        await self._log(job, "02_blocks_analysis.json создан", "info")
                    else:
                        await self._log(job, f"Ошибка слияния: {stderr}", "error")

                    if error_count > 0:
                        self._update_pipeline_log(pid, "block_analysis", "partial",
                                                   message=f"{error_count} из {total_batches} single-block задач с ошибками",
                                                   detail={"failed_blocks": failed_runtime_batches})
                    else:
                        self._update_pipeline_log(pid, "block_analysis", "done",
                                                   message=f"Все {total_batches} пакетов OK")

            if job.status == JobStatus.CANCELLED:
                return

            # Re-register
            self.active_jobs[pid] = job
            self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАП 5b: Block Retry — перекачка нечитаемых блоков ═══
            await self._run_block_retry(job, pid, project_info, output_dir)

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
            findings_path = output_dir / "03_findings.json"
            if findings_path.exists():
                await self._run_post_findings_parallel(
                    job, project_info,
                    include_optimization=include_optimization,
                )

                if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                    return

                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()
            else:
                await self._log(job, "03_findings.json не найден — пропуск верификации", "warn")

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
            pause_note = f" (паузы: {round(job.pause_total_sec / 60, 1)} мин)" if job.pause_total_sec > 60 else ""
            print(f"[{pid}] ═══ Аудит завершён за {duration} мин{pause_note} ═══")
            await self._log(job, f"Аудит завершён за {duration} мин{pause_note}.", "info")

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
            job.completed_at = datetime.now().isoformat()
            self._cleanup(pid)

    # ─── Запуск ВСЕХ проектов последовательно ───
    # ─── Batch (групповые действия для выбранных проектов) ───

    _batch_queue: Optional[BatchQueueStatus] = None

    async def start_batch(
        self,
        project_ids: list[str],
        action: str,
        *,
        manual_run_id: Optional[str] = None,
    ) -> BatchQueueStatus:
        """Запустить групповое действие для списка проектов.

        Дописывает items в общую очередь (создавая её если нужно). Если в
        очереди уже есть single-task'и от `start_audit`, новые items
        добавляются после них — всё бежит последовательно.

        `manual_run_id`: общий scope разрешения платных API на весь batch.
        Без него все Stage 02/discussion внутри этих job блокируются guard'ом.
        """
        if self.is_running("__ALL__"):
            raise RuntimeError("Запуск всех проектов уже выполняется")
        if not project_ids:
            raise RuntimeError("Список проектов пуст")
        _lmstudio_note_activity(f"pipeline batch queued: {action}")

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
            new_items = [
                BatchQueueItem(
                    project_id=pid,
                    action=action,
                    status="pending",
                    job_id=str(uuid4()),
                    manual_run_id=manual_run_id,
                )
                for pid in filtered
            ]

            queue = self._ensure_batch_worker(action_for_label=action)
            queue.items.extend(new_items)
            queue.total = len(queue.items)

            meta_job = self.active_jobs.get("__BATCH__")
            if meta_job:
                meta_job.progress_total = queue.total

        await self._broadcast_batch_progress(queue)
        return queue

    # ─── Pre-crop: фоновая загрузка блоков для следующих проектов в очереди ───

    async def _precrop_project(self, pid: str) -> bool:
        """Кроп блоков для одного проекта (фоновая задача). Возвращает True при успехе.

        Safety guard: pre-crop loop V1-only. Если у проекта `latest_version_id`
        не V1 — skip с warn. Это защищает V1 _output/blocks_gemma_100 от
        перезаписи crop'ами под V2 source. Полная версионность batch queue
        отложена в отдельный pass (TODO: make pre-crop queue version-aware
        before enabling V2 batch pre-crop).
        """
        try:
            proj_dir = resolve_project_dir(pid)
            # Safety guard: skip V2+ projects from V1-only pre-crop path.
            try:
                from backend.app.services.common import version_service
                latest_vid = version_service.get_latest_version_id(proj_dir, pid) or "v1"
                if latest_vid != "v1":
                    msg = (
                        f"[PRE-CROP] {pid}: skip — latest_version_id={latest_vid} "
                        f"(pre-crop queue ещё не version-aware; V2+ pre-crop отключён)"
                    )
                    print(msg)
                    try:
                        await ws_manager.broadcast_global(
                            WSMessage.log("__BATCH__", msg, "warn")
                        )
                    except Exception:
                        pass
                    return False
            except Exception:
                # Manifest не найден — это V1-only project, продолжаем.
                pass

            # Пропустить если блоки уже есть
            blocks_dir = gemma_blocks_dir(proj_dir)
            index_file = gemma_blocks_index_path(proj_dir)
            if index_file.exists() and _existing_crop_matches_policy(
                index_file, gemma_enrichment_crop_policy()
            ):
                print(f"[PRE-CROP] {pid}: блоки уже есть, пропуск")
                return True
            # Пропустить если нет result.json (не OCR-проект)
            if not list(proj_dir.glob("*_result.json")):
                return False

            print(f"[PRE-CROP] {pid}: начинаю фоновый кроп блоков...")
            await ws_manager.broadcast_global(
                WSMessage.log("__BATCH__", f"  ⚡ Pre-crop: {pid}", "info")
            )
            # NOTE: pre-crop работает только над V1 root — batch queue ещё
            # не version-aware. V2 проекты сюда не попадают (queue не различает
            # версии), поэтому здесь явно используем V1 _project_path(pid).
            # Если в будущем queue научится таскать V2 — заменить на
            # version-aware path (см. _project_path_for_job).
            exit_code, _, stderr = await run_script(
                str(BLOCKS_SCRIPT),
                _build_crop_args(
                    _project_path(pid),
                    policy=gemma_enrichment_crop_policy(),
                    output_dir_name=GEMMA_BLOCKS_DIRNAME,
                ),
                project_id=f"__PRECROP_{pid}__",
            )
            if exit_code == 0:
                print(f"[PRE-CROP] {pid}: OK")
                return True
            else:
                print(f"[PRE-CROP] {pid}: ошибка (код {exit_code})")
                return False
        except Exception as e:
            print(f"[PRE-CROP] {pid}: исключение: {e}")
            return False

    async def _run_precrop_loop(self, queue: BatchQueueStatus):
        """Фоновый цикл: кропит блоки для pending-проектов из очереди."""
        precropped = set()
        while queue.status == "running":
            # Найти следующий pending OCR-проект для pre-crop
            target = None
            for item in queue.items:
                if item.status != "pending":
                    continue
                if item.project_id in precropped:
                    continue
                action = item.action or queue.action
                if action == "optimization":
                    continue  # оптимизация не нуждается в кропе
                proj_dir = resolve_project_dir(item.project_id)
                if list(proj_dir.glob("*_result.json")):
                    target = item.project_id
                    break

            if not target:
                # Нет проектов для pre-crop, подождём и проверим снова
                await asyncio.sleep(5)
                continue

            precropped.add(target)
            await self._precrop_project(target)
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

            # Запустить фоновый pre-crop для будущих проектов
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

                pid = item.project_id
                print(f"[BATCH] ▶ Проект {idx + 1}/{queue.total}: {pid} ({queue.action})")
                await ws_manager.broadcast_global(
                    WSMessage.log("__BATCH__", f"▶ Проект {idx + 1}/{queue.total}: {pid}", "info")
                )
                await self._broadcast_batch_progress(queue)

                # Пропуск уже запущенных
                if self.is_running(pid):
                    item.status = "skipped"
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
                try:
                    job = AuditJob(
                        job_id=item.job_id or str(uuid4()),
                        object_id=self._resolve_object_id(None),
                        project_id=pid,
                        version_id=item.version_id,
                        stage=AuditStage.PREPARE,
                        status=JobStatus.RUNNING,
                        started_at=datetime.now().isoformat(),
                        # Paid-API guard scope: manual_run_id выдан endpoint'ом
                        # при ручном старте с галкой "Разрешить платные API".
                        # None для auto-resume/orphan — Stage 02/discussion
                        # будут заблокированы paid_api_guard.
                        manual_run_id=getattr(item, "manual_run_id", None),
                    )
                    self.active_jobs[pid] = job
                    self._tasks[pid] = asyncio.current_task()

                    await self._dispatch_action(item, job, default_action=queue.action)

                    if job.status == JobStatus.COMPLETED:
                        item.status = "completed"
                        queue.completed += 1
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
                    item.status = "failed"
                    item.error = str(e)
                    queue.failed += 1
                    import traceback
                    traceback.print_exc()
                    await ws_manager.broadcast_global(
                        WSMessage.log("__BATCH__", f"  ✗ {pid}: исключение: {e}", "error")
                    )
                finally:
                    self._stop_heartbeat(pid)
                    self.active_jobs.pop(pid, None)
                    self._tasks.pop(pid, None)
                    await self._broadcast_batch_progress(queue)
                    # Снимаем bind_version, выставленный перед dispatch
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
            queue.status = "completed"
            meta_job.status = JobStatus.FAILED
            print(f"[BATCH] КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Остановить фоновый pre-crop
            if precrop_task and not precrop_task.done():
                precrop_task.cancel()
                try:
                    await precrop_task
                except (asyncio.CancelledError, Exception):
                    pass
            self._cleanup("__BATCH__")

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

        Любой single-start (start_audit, start_smart_audit, ...) проходит
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
            "full", "audit", "standard", "pro", "smart",
            "audit+optimization", "standard+optimization", "pro+optimization",
            "main_audit", "tile_audit", "prepare",
        }
        if action in fresh_actions and not item.retry_stage:
            try:
                usage_tracker.clear_project_usage(pid)
            except Exception:
                pass
            try:
                audit_logger.reset_audit_log(pid)
            except Exception:
                pass

        # Per-action cleanup stage-файлов (миррор старых start_* helpers)
        if not item.retry_stage:
            if action == "main_audit":
                self._clean_stage_files(pid, [
                    "00_init.json", "01_text_analysis.json", "03_findings.json",
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
            await self._run_resumed_pipeline(job, resume_info["stage"], resume_info)
            return

        if action == "smart":
            await self._run_smart_pipeline(job)
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
        _root, proj_dir, _od = self._resolve_job_paths(job)
        is_ocr = bool(list(proj_dir.glob("*_result.json")))

        if action == "full":
            # single-start "запустить аудит" — full audit + optimization для OCR, smart иначе
            if is_ocr:
                await self._run_ocr_pipeline(job, include_optimization=True)
            else:
                await self._run_smart_pipeline(job)
            return
        if action in ("audit", "standard", "pro"):
            if is_ocr:
                await self._run_ocr_pipeline(job)
            else:
                await self._run_smart_pipeline(job)
            return
        if action in ("audit+optimization", "standard+optimization", "pro+optimization"):
            if is_ocr:
                await self._run_ocr_pipeline(job, include_optimization=True)
            else:
                await self._run_smart_pipeline(job)
            return

        # fallback
        if is_ocr:
            await self._run_ocr_pipeline(job)
        else:
            await self._run_smart_pipeline(job)

    # ─── Единая очередь: enqueue single-project ───────────────────────
    def _ensure_batch_worker(self, action_for_label: str = "full") -> BatchQueueStatus:
        """Гарантировать, что _batch_queue существует и worker запущен.

        Не добавляет items. Возвращает текущую очередь либо создаёт пустую и
        поднимает worker.
        """
        queue = self._batch_queue
        if queue is not None and queue.status == "running" and "__BATCH__" in self.active_jobs:
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
        manual_run_id: Optional[str] = None,
    ) -> AuditJob:
        """Поставить single-project задачу в общую очередь.

        Возвращает placeholder AuditJob со status=QUEUED. Реальный pipeline
        запустится, когда worker дойдёт до этого item. Это единственный путь
        запуска одиночного проекта — start_audit/start_smart_audit/... все
        теперь делегируют сюда.

        `version_id` фиксируется на момент enqueue. Если None — берётся
        latest_version_id проекта (один раз). После этого пользователь может
        создать V_{N+1}, на запущенный job-а это не повлияет.
        """
        _lmstudio_note_activity(f"pipeline job queued: {project_id}/{action}")
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
                manual_run_id=manual_run_id,
            )

            queue = self._ensure_batch_worker(action_for_label=action)
            queue.items.append(item)
            queue.total = len(queue.items)

            meta_job = self.active_jobs.get("__BATCH__")
            if meta_job:
                meta_job.progress_total = queue.total

            placeholder = AuditJob(
                job_id=job_id,
                object_id=self._resolve_object_id(None),
                project_id=project_id,
                version_id=effective_vid,
                stage=AuditStage.PREPARE,
                status=JobStatus.QUEUED,
                manual_run_id=manual_run_id,
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

    async def add_retry_to_batch(self, project_id: str, stage: str) -> BatchQueueStatus:
        """Добавить retry конкретного этапа в очередь."""
        # Маппинг ключей pipeline_summary → внутренних ключей этапов
        stage_map = {
            "crop_blocks": "prepare",
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
            "prepare": "prepare",
            "tile_audit": "block_analysis",
            "main_audit": "findings_merge",
        }
        internal_stage = stage_map.get(stage, stage)
        internal_stage = self._validate_start_from_stage_now(project_id, internal_stage)

        await self._enqueue_single(
            project_id, action="retry_stage", retry_stage=internal_stage,
        )
        stage_label = {
            "prepare": "Кроп блоков", "gemma_enrichment": GEMMA_STAGE_LABEL,
            "text_analysis": "Анализ текста",
            "block_analysis": "Анализ блоков", "findings_merge": "Свод замечаний",
            "findings_review": "Critic замечаний", "norm_verify": "Верификация норм",
            "optimization": "Оптимизация", "optimization_review": "Проверка оптимизации",
        }.get(internal_stage, internal_stage)
        await ws_manager.broadcast_global(
            WSMessage.log("__BATCH__", f"+ В очередь: {project_id} → {stage_label}", "info")
        )
        return self._batch_queue

    async def add_resume_to_batch(self, project_id: str) -> BatchQueueStatus:
        """Добавить resume проекта в очередь."""
        await self._enqueue_single(project_id, action="resume")
        await ws_manager.broadcast_global(
            WSMessage.log("__BATCH__", f"+ В очередь: {project_id} → Продолжить", "info")
        )
        return self._batch_queue

    def get_batch_queue(self) -> Optional[BatchQueueStatus]:
        """Получить текущую batch-очередь."""
        return self._batch_queue

    async def reorder_batch(self, new_order: list[str]) -> BatchQueueStatus:
        """Переупорядочить pending-элементы очереди. new_order — список project_id в новом порядке."""
        queue = self._batch_queue
        if not queue or queue.status != "running":
            raise RuntimeError("Нет активной групповой очереди")

        # Разделяем: обработанные (уже не pending) и pending
        processed = []
        pending_map = {}
        for item in queue.items:
            if item.status in ("completed", "failed", "skipped", "running"):
                processed.append(item)
            else:
                pending_map[item.project_id] = item

        # Собираем новый порядок pending из new_order
        reordered_pending = []
        for pid in new_order:
            if pid in pending_map:
                reordered_pending.append(pending_map.pop(pid))
        # Добавляем оставшиеся pending (не упомянутые в new_order)
        for item in pending_map.values():
            reordered_pending.append(item)

        queue.items = processed + reordered_pending
        queue.total = len(queue.items)
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
        """Critic + Corrector оптимизации — оркестратор делегирует в optimization/runner.py."""
        from backend.app.pipeline.stages.optimization.runner import (
            run_optimization_review as _opt_review_runner,
        )
        ctx = self._make_stage_context(job)
        await _opt_review_runner(ctx)


# Глобальный экземпляр
pipeline_manager = PipelineManager()
_register_lmstudio_idle_probe("pipeline_queue", pipeline_manager.is_idle)
