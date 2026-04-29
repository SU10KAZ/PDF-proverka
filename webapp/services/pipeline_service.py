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

from webapp.config import (
    BASE_DIR, PROJECTS_DIR,
    PROCESS_PROJECT_SCRIPT, GENERATE_EXCEL_SCRIPT,
    BLOCKS_SCRIPT, NORMS_SCRIPT, DEFAULT_TILE_QUALITY,
    MAX_PARALLEL_BATCHES,
    get_block_batch_parallelism,
    get_stage_model,
    get_stage_batch_mode,
    is_local_llm_model,
    FLASH_PRO_TRIAGE_MODEL,
    RATE_LIMIT_THRESHOLD_PCT, RATE_LIMIT_CHECK_INTERVAL,
    RATE_LIMIT_MAX_WAIT, RATE_LIMIT_MAX_RETRIES,
    CRITIC_CHUNK_SIZE,
    CORRECTOR_CHUNK_SIZE,
)
from webapp.models.audit import AuditJob, AuditStage, JobStatus, BatchQueueStatus, BatchQueueItem, BatchAction
from webapp.models.websocket import WSMessage
from webapp.config import get_claude_model, get_model_for_stage
from webapp.models.usage import UsageRecord
from webapp.services.process_runner import run_script, kill_all_processes
from webapp.services import claude_runner
from webapp.services.usage_service import usage_tracker, global_scanner, paid_cost_tracker
from webapp.services.resume_detector import detect_resume_stage as _detect_resume_stage
from webapp.services import audit_logger
from webapp.services.project_service import resolve_project_dir


def _project_path(pid: str) -> str:
    """Относительный путь к папке проекта (с учётом подпапок-групп)."""
    resolved = resolve_project_dir(pid)
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


QWEN_CROP_DPI = 300
CLAUDE_CLI_CROP_DPI = 100

BATCH_QUEUE_FILE = BASE_DIR / "webapp" / "data" / "batch_queue.json"


def _block_batch_crop_mode() -> str:
    """Определить режим crop по текущей модели block_batch.

    qwen    — локальная QWEN (Chandra/LM Studio)
    claude  — Claude CLI (opus/sonnet)
    compact — OpenRouter (Gemini/GPT) и всё остальное
    """
    model = get_stage_model("block_batch")
    if is_local_llm_model(model):
        return "qwen"
    if model.startswith("claude-"):
        return "claude"
    return "compact"


def _build_crop_args(project_path: str, force: bool = False) -> list[str]:
    """Собрать аргументы для blocks.py crop с учётом модели block_batch.

    qwen   — DPI 300 + мелкие блоки включены
    claude — DPI 100, без compact-пары (один файл на блок)
    compact — 50 DPI compact + 100 DPI full (production default для OpenRouter)
    """
    mode = _block_batch_crop_mode()
    if mode == "qwen":
        args = ["crop", project_path, "--dpi", str(QWEN_CROP_DPI), "--no-skip-small"]
    elif mode == "claude":
        args = ["crop", project_path, "--dpi", str(CLAUDE_CLI_CROP_DPI)]
    else:
        args = ["crop", project_path, "--compact"]
    if force:
        args.append("--force")
    return args


def _existing_crop_matches_mode(blocks_index_path: Path) -> bool:
    """Проверить что существующий crop подходит текущему режиму block_batch."""
    try:
        idx = json.loads(blocks_index_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    mode = _block_batch_crop_mode()
    if mode == "qwen":
        return idx.get("dpi") == QWEN_CROP_DPI and idx.get("skip_small") is False
    if mode == "claude":
        return idx.get("dpi") == CLAUDE_CLI_CROP_DPI and not idx.get("compact")
    return bool(idx.get("compact"))


def _expand_block_batches_for_local_model(batches: list[dict]) -> tuple[list[dict], bool]:
    """Перевести stage 02 в single-block режим (для всех моделей и пресетов).

    После архитектурного решения (Идея 7 в ideas.md): один image-блок = один
    LLM-запрос для ВСЕХ моделей. Раньше единственно для локальных Qwen, теперь
    унифицировано — Qwen-enrichment перенесён в stage 1 prep, stage 02 работает
    только с одиночными блоками. Это даёт:
      - меньше blast radius при битом PNG;
      - стабильнее качество (нет деградации обдумывания при батчах графики);
      - симметрию между провайдерами (Sonnet/Opus/GPT/Gemini получают один блок).

    Генератор `blocks.py batches` остаётся общим для аллокации блоков, здесь мы
    адаптируем runtime-план перед запуском CLI.
    """
    single_block_batches: list[dict] = []
    next_batch_id = 1

    for batch in batches:
        source_batch_id = batch.get("batch_id")
        for block in batch.get("blocks", []):
            block_copy = dict(block)
            page = block_copy.get("page")
            single_block_batches.append({
                "batch_id": next_batch_id,
                "blocks": [block_copy],
                "pages_included": [page] if page is not None else [],
                "block_count": 1,
                "total_size_kb": block_copy.get("size_kb", 0),
                "single_block_mode": True,
                "source_batch_id": source_batch_id,
            })
            next_batch_id += 1

    return single_block_batches, True
from webapp.services.project_service import resolve_project_dir, bind_object, unbind_object
from webapp.ws.manager import ws_manager


def _current_object_id_or_none() -> Optional[str]:
    """Helper: ID текущего объекта (None, если objects.json недоступен)."""
    try:
        from webapp.services.object_service import get_current_id
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
        """
        if self._batch_queue is None:
            return
        try:
            BATCH_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
            BATCH_QUEUE_FILE.write_text(
                self._batch_queue.model_dump_json(indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            print(f"[PipelineManager] Ошибка сохранения очереди: {e}")

    def load_persisted_queue(self) -> None:
        """Загрузить очередь после перезапуска сервера.

        running-элементы → interrupted (процесс был прерван рестартом).
        pending-элементы → остаются pending (не были запущены).
        Статус очереди → "interrupted" (не "running") чтобы worker не запустился.
        """
        if not BATCH_QUEUE_FILE.exists():
            return
        try:
            data = json.loads(BATCH_QUEUE_FILE.read_text(encoding="utf-8"))
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
        if actual_cost > 0:
            paid_cost_tracker.add(actual_cost)
        job.cost_usd += actual_cost
        job.cli_calls += 1

        # Обогатить pipeline_log.json полями model/tokens для текущего этапа
        self._enrich_pipeline_log(job.project_id, stage, model, input_tokens, output_tokens)

    def _record_flash_pro_triage_usage(self, job: AuditJob, project_dir: Path):
        """Добавить агрегированную стоимость Stage 02 Flash+Pro triage в usage tracker."""
        output_path = project_dir / "_output" / "02_blocks_analysis.json"
        artifacts_dir: Path | None = None
        try:
            with open(output_path, encoding="utf-8") as f:
                output_data = json.load(f)
            meta = output_data.get("meta", {}) if isinstance(output_data, dict) else {}
            raw = meta.get("artifacts_dir")
            if raw:
                artifacts_dir = Path(raw)
                if not artifacts_dir.is_absolute():
                    artifacts_dir = project_dir / artifacts_dir
        except Exception:
            artifacts_dir = None

        if not artifacts_dir or not artifacts_dir.exists():
            candidates = sorted((project_dir / "_experiments" / "stage02_flash_pro_triage").glob("*"))
            artifacts_dir = candidates[-1] if candidates else None
        if not artifacts_dir or not artifacts_dir.exists():
            return

        for summary_name in ("flash_full_summary.json", "pro_selected_summary.json"):
            summary_path = artifacts_dir / summary_name
            if not summary_path.exists():
                continue
            try:
                with open(summary_path, encoding="utf-8") as f:
                    summary = json.load(f)
            except Exception:
                continue

            input_tokens = int(summary.get("total_prompt_tokens", 0) or 0)
            output_tokens = int(summary.get("total_output_tokens", 0) or 0)
            cost = float(summary.get("total_cost_usd", 0.0) or 0.0)
            if input_tokens <= 0 and output_tokens <= 0 and cost <= 0:
                continue

            api_calls = int(summary.get("completed_batches", 0) or summary.get("total_batches", 0) or 1)
            duration_ms = int(float(summary.get("elapsed_s", 0.0) or 0.0) * 1000)
            model = summary.get("model_id", "")
            record = UsageRecord(
                timestamp=datetime.now().isoformat(),
                session_id=None,
                project_id=job.project_id,
                stage="block_analysis",
                model=model,
                cost_usd=cost,
                cost_usd_notional=0.0,
                duration_ms=duration_ms,
                duration_api_ms=duration_ms,
                num_turns=api_calls,
                api_calls=api_calls,
                is_retry=False,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read_tokens=int(summary.get("total_cached_tokens", 0) or 0),
            )
            usage_tracker.record_usage(record)
            if cost > 0:
                paid_cost_tracker.add(cost)
            job.cost_usd += cost
            job.cli_calls += api_calls
            self._enrich_pipeline_log(job.project_id, "block_analysis", model, input_tokens, output_tokens)

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
            output_dir = resolve_project_dir(project_id) / "_output"
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

    def is_running(self, project_id: str) -> bool:
        return project_id in self.active_jobs

    def is_queued(self, project_id: str) -> bool:
        """Проверить, стоит ли проект в очереди со статусом pending."""
        if not self._batch_queue or self._batch_queue.status != "running":
            return False
        return any(
            it.project_id == project_id and it.status == "pending"
            for it in self._batch_queue.items
        )

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
        from webapp.services.project_service import iter_project_dirs

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

    async def _run_script(self, project_id: str, *args, **kwargs):
        """Обёртка run_script с автоматическим project_id для трекинга процессов."""
        return await run_script(*args, project_id=project_id, **kwargs)

    def _reset_job_progress(self, job: AuditJob):
        """Сбросить прогресс и ETA-данные при переходе между этапами пайплайна."""
        job.progress_current = 0
        job.progress_total = 0
        job.batch_durations = []
        job.batch_started_at = None

    @staticmethod
    def _backfill_highlight_regions(project_id: str):
        """Восстановить highlight_regions в 03_findings.json из 02_blocks_analysis.json.

        При findings_merge LLM иногда теряет highlight_regions из G-замечаний.
        Этот метод подтягивает координаты обратно по source_block_ids/related_block_ids.
        """
        from backfill_highlights import backfill_project
        project_dir = resolve_project_dir(project_id)
        result = backfill_project(project_dir)
        if result["fixed"] > 0:
            print(f"[{project_id}] highlight_regions restored: {result['fixed']}")

    @staticmethod
    def _backfill_text_evidence_in_findings(project_id: str):
        """Backfill text-evidence + sheet в 03_findings.json.

        1. selected_text_block_ids/evidence_text_refs — из 02_blocks_analysis.json
        2. sheet — детерминированно из document_graph.json page_sheet_map
        """
        output_dir = resolve_project_dir(project_id) / "_output"
        findings_path = output_dir / "03_findings.json"
        blocks_path = output_dir / "02_blocks_analysis.json"
        graph_path = output_dir / "document_graph.json"

        if not findings_path.exists():
            return

        try:
            fd = json.loads(findings_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return

        # Индекс block_id → block_analysis data из 02
        bc_index = {}
        if blocks_path.exists():
            try:
                data02 = json.loads(blocks_path.read_text(encoding="utf-8"))
                for ba in data02.get("block_analyses", []):
                    bid = ba.get("block_id", "")
                    if bid:
                        bc_index[bid] = ba
            except (json.JSONDecodeError, OSError):
                pass

        # page_sheet_map из document_graph.json
        psm = {}
        if graph_path.exists():
            try:
                graph = json.loads(graph_path.read_text(encoding="utf-8"))
                for pg in graph.get("pages", []):
                    page_num = pg.get("page")
                    sheet_no = (
                        pg.get("sheet_no_raw")
                        or pg.get("sheet_no_normalized")
                        or pg.get("sheet_no")
                    )
                    if page_num is not None and sheet_no:
                        psm[str(page_num)] = sheet_no
            except (json.JSONDecodeError, OSError):
                pass

        modified = 0
        for finding in fd.get("findings", []):
            # ── Text-evidence backfill ──
            if not finding.get("selected_text_block_ids"):
                source_blocks = finding.get("source_block_ids", [])
                related_blocks = finding.get("related_block_ids", [])
                lookup_blocks = source_blocks or related_blocks

                all_stbi = []
                all_etr = []
                for bid in lookup_blocks:
                    bc = bc_index.get(bid)
                    if not bc:
                        continue
                    for stbi in bc.get("selected_text_block_ids", []):
                        if stbi not in all_stbi:
                            all_stbi.append(stbi)
                    for etr in bc.get("evidence_text_refs", []):
                        if etr not in all_etr:
                            all_etr.append(etr)

                if all_stbi:
                    finding["selected_text_block_ids"] = all_stbi
                    modified += 1
                if all_etr and not finding.get("evidence_text_refs"):
                    finding["evidence_text_refs"] = all_etr

            # ── Sheet backfill (deterministic) ──
            sheet = finding.get("sheet")
            page = finding.get("page")
            sheet_empty = sheet is None or (isinstance(sheet, str) and not sheet.strip())

            if sheet_empty and page is not None and psm:
                # Resolve sheet from page_sheet_map
                pages_to_check = [page] if isinstance(page, int) else (
                    page if isinstance(page, list) else []
                )
                resolved_sheets = []
                for p in pages_to_check:
                    s = psm.get(str(p))
                    if s and s not in resolved_sheets:
                        resolved_sheets.append(s)

                if resolved_sheets:
                    if len(resolved_sheets) == 1:
                        finding["sheet"] = f"Лист {resolved_sheets[0]}"
                    else:
                        finding["sheet"] = "Листы " + ", ".join(resolved_sheets)
                    modified += 1
                else:
                    # Page exists but not in map — mark explicitly
                    finding["sheet_unavailable"] = True
                    finding["sheet_unavailable_reason"] = "page_not_in_map"
                    modified += 1

            elif sheet_empty and page is None:
                finding["sheet_unavailable"] = True
                finding["sheet_unavailable_reason"] = "no_page"
                modified += 1

        if modified > 0:
            findings_path.write_text(
                json.dumps(fd, ensure_ascii=False, indent=2), encoding="utf-8"
            )

    @staticmethod
    def _refresh_finding_quality(
        project_id: str,
        filename: str = "03_findings.json",
    ) -> dict | None:
        """Refresh deterministic practicality metadata for findings."""
        target_path = resolve_project_dir(project_id) / "_output" / filename
        if not target_path.exists():
            return None

        try:
            from webapp.services.finding_quality import enrich_findings_file
            return enrich_findings_file(target_path)
        except Exception:
            return None

    @staticmethod
    def _merge_similar_findings(project_id: str) -> dict | None:
        """Объединить похожие замечания в 03_findings.json.

        Группирует по нормализованному паттерну (severity + category + problem).
        Для каждой группы создаёт одно замечание-лидер с полным перечнем случаев
        в поле `sub_findings` и сводным описанием.
        """
        from webapp.services.findings_service import (
            _normalize_problem_pattern,
        )
        from collections import OrderedDict

        output_dir = resolve_project_dir(project_id) / "_output"
        findings_path = output_dir / "03_findings.json"
        if not findings_path.exists():
            return None

        try:
            fd = json.loads(findings_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

        items = fd.get("findings", fd.get("items", []))
        if len(items) < 2:
            return None

        import re as _re

        # Группировка
        groups: OrderedDict[str, list[dict]] = OrderedDict()
        for f in items:
            problem = f.get("problem") or f.get("description") or f.get("finding") or ""
            severity = f.get("severity", "")
            category = f.get("category", "")
            pattern = _normalize_problem_pattern(problem)
            key = f"{severity}||{category}||{pattern}"
            if key not in groups:
                groups[key] = []
            groups[key].append(f)

        # Построить новый список
        merged_items = []
        merge_count = 0
        new_id = 1
        for key, group_items in groups.items():
            if len(group_items) == 1:
                item = group_items[0]
                item["id"] = f"F-{new_id:03d}"
                merged_items.append(item)
                new_id += 1
            else:
                merge_count += 1
                leader = dict(group_items[0])  # копия лидера
                leader["id"] = f"F-{new_id:03d}"
                new_id += 1

                # Собрать все sheet/page/evidence/block_ids
                all_sheets = []
                all_pages = []
                all_block_ids = []
                all_evidence = []
                details_lines = []

                for i, it in enumerate(group_items, 1):
                    sh = it.get("sheet", "")
                    pg = it.get("page")
                    problem = it.get("problem") or it.get("description") or it.get("finding") or ""
                    details_lines.append(f"{i}) {problem}")

                    if sh and sh not in all_sheets:
                        all_sheets.append(sh)
                    if pg:
                        pgs = pg if isinstance(pg, list) else [pg]
                        for p in pgs:
                            if p not in all_pages:
                                all_pages.append(p)
                    for bid in (it.get("related_block_ids") or []):
                        if bid not in all_block_ids:
                            all_block_ids.append(bid)
                    for ev in (it.get("evidence") or []):
                        all_evidence.append(ev)

                # Сводное описание
                leader_problem = leader.get("problem") or leader.get("description") or ""
                summary = f"[Объединено {len(group_items)} замечаний] {leader_problem}"
                leader["problem"] = summary
                leader["description"] = "\n".join(details_lines)
                leader["sheet"] = ", ".join(all_sheets) if all_sheets else leader.get("sheet", "")
                leader["page"] = sorted(set(all_pages)) if all_pages else leader.get("page")
                leader["related_block_ids"] = all_block_ids
                leader["evidence"] = all_evidence
                leader["sub_findings"] = [
                    {
                        "original_id": it.get("id", ""),
                        "problem": it.get("problem") or it.get("description") or "",
                        "sheet": it.get("sheet", ""),
                        "page": it.get("page"),
                    }
                    for it in group_items
                ]

                merged_items.append(leader)

        if merge_count == 0:
            return {"merged_groups": 0}

        # Обновить meta
        meta = fd.get("meta", {})
        meta["total_findings"] = len(merged_items)
        meta["pre_merge_total"] = len(items)
        meta["merged_groups"] = merge_count

        # Пересчитать by_severity
        by_severity = {}
        for it in merged_items:
            sev = it.get("severity", "НЕИЗВЕСТНО")
            by_severity[sev] = by_severity.get(sev, 0) + 1
        meta["by_severity"] = by_severity

        fd["meta"] = meta
        fd["findings"] = merged_items

        # Бэкап оригинала
        backup_path = output_dir / "03_findings_pre_merge.json"
        if not backup_path.exists():
            import shutil
            shutil.copy2(findings_path, backup_path)

        findings_path.write_text(
            json.dumps(fd, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return {
            "merged_groups": merge_count,
            "before": len(items),
            "after": len(merged_items),
        }

    async def _build_document_graph_v2(self, job: AuditJob):
        """Построить document_graph v2 из *_result.json (Python, без LLM)."""
        pid = job.project_id
        try:
            import sys
            sys.path.insert(0, str(BASE_DIR))
            from graph_builder import build_document_graph_v2, generate_locality_debug

            project_dir = resolve_project_dir(pid)
            output_dir = project_dir / "_output"

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

    async def _run_qwen_enrichment_stage(self, job: AuditJob) -> None:
        """ЭТАП 00: Qwen-обогащение MD-файла (после crop, до text_analysis).

        Идемпотентен: если MD уже содержит маркер `<!-- ENRICHMENT: ... -->` — пропускает.
        Использует существующий `qwen_enrich.enrich_project()`. Все downstream-этапы
        (text_analysis, block_batch, findings_merge) автоматически читают обогащённый MD.

        Если все блоки упали (Qwen endpoint недоступен) — кидает RuntimeError
        и аудит останавливается. Если часть блоков ok — продолжаем (partial enrichment).
        """
        pid = job.project_id
        project_dir = resolve_project_dir(pid)
        job.stage = AuditStage.QWEN_ENRICHMENT
        self._update_pipeline_log(pid, "qwen_enrichment", "running")

        print(f"[{pid}] ═══ ЭТАП 00: Подготовка (Qwen-обогащение MD) ═══")
        await self._log(job, "═══ ЭТАП 00: Подготовка (Qwen-обогащение MD-файла) ═══")

        # Найти MD-файл проекта
        md_files = sorted([
            f for f in project_dir.iterdir()
            if f.suffix == ".md" and f.name.endswith("_document.md")
        ])
        if not md_files:
            await self._log(
                job, "MD-файл не найден — пропускаем enrichment", "warn"
            )
            self._update_pipeline_log(
                pid, "qwen_enrichment", "skipped", message="MD не найден"
            )
            return
        md_path = md_files[0]

        # Идемпотентность: если MD уже enriched, пропускаем
        try:
            import sys
            sys.path.insert(0, str(BASE_DIR))
            from qwen_enrich import is_enriched, get_enrichment_meta, enrich_project
        except ImportError as exc:
            await self._log(
                job, f"qwen_enrich модуль не найден: {exc}", "warn"
            )
            self._update_pipeline_log(
                pid, "qwen_enrichment", "skipped",
                message=f"qwen_enrich import error: {exc}"
            )
            return

        if is_enriched(md_path):
            meta = get_enrichment_meta(md_path) or {}
            msg = (f"MD уже обогащён "
                   f"({meta.get('blocks_ok', '?')}/{meta.get('blocks_total', '?')}, "
                   f"{meta.get('model', '?')}) — пропускаем")
            await self._log(job, msg)
            self._update_pipeline_log(pid, "qwen_enrichment", "done", message=msg)
            return

        # Прогрессбар через live-log
        async def progress_cb(event: dict) -> None:
            t = event.get("type")
            if t == "started":
                await self._log(
                    job,
                    f"  Qwen enrichment: {event['total']} блоков, "
                    f"model={event.get('model')}",
                )
            elif t == "block_done":
                completed = event.get("completed", 0)
                total = event.get("total", 0)
                bid = event.get("block_id", "?")
                pg = event.get("page", "?")
                ok = event.get("ok")
                ms = event.get("elapsed_ms", 0)
                if ok:
                    await self._log(
                        job,
                        f"  [{completed:>3}/{total}] OK {bid} p={pg} t={ms/1000:.1f}s"
                    )
                else:
                    err = (event.get("error") or "")[:80]
                    await self._log(
                        job,
                        f"  [{completed:>3}/{total}] FAIL {bid} p={pg}: {err}",
                        "warn"
                    )
            elif t == "no_blocks":
                await self._log(
                    job, "  Image-блоков для enrichment не найдено", "warn"
                )

        # Поддержка cancel через job.status
        cancel_event = asyncio.Event()
        pause_event = asyncio.Event()
        pause_event.set()  # не на паузе по умолчанию

        try:
            summary = await enrich_project(
                project_dir,
                force=False,
                parallelism=1,  # qwen3.6-35b не тянет параллель
                progress_cb=progress_cb,
                pause_event=pause_event,
                cancel_event=cancel_event,
            )
        except Exception as e:
            self._update_pipeline_log(
                pid, "qwen_enrichment", "error", error=f"qwen_enrich exception: {e}"
            )
            raise RuntimeError(f"Qwen enrichment упал: {e}") from e

        status = summary.get("status", "unknown")
        if status == "no_blocks":
            await self._log(job, "Image-блоков нет — этап пропущен")
            self._update_pipeline_log(
                pid, "qwen_enrichment", "skipped", message="image-блоков 0"
            )
            return
        if status == "failed":
            self._update_pipeline_log(
                pid, "qwen_enrichment", "error",
                error="Все блоки упали — Qwen endpoint недоступен?"
            )
            raise RuntimeError(
                "Qwen enrichment: все блоки упали. "
                "Проверьте CHANDRA_BASE_URL / NGROK_AUTH_USER / NGROK_AUTH_PASS."
            )

        ok = summary.get("blocks_ok", 0)
        total = summary.get("blocks_total", 0)
        wall = summary.get("wall_clock_s", 0)
        msg = f"OK ({ok}/{total} блоков, {wall:.0f}s)"
        if status == "partial":
            msg = f"partial: {msg} — {summary.get('blocks_failed', 0)} блоков упали"
            self._update_pipeline_log(pid, "qwen_enrichment", "done", message=msg)
            await self._log(job, f"  ⚠ {msg}", "warn")
        else:
            self._update_pipeline_log(pid, "qwen_enrichment", "done", message=msg)
            await self._log(job, f"  ✓ {msg}")

    async def _run_block_analysis_findings_only(self, job: AuditJob) -> None:
        """ЭТАП 02 в режиме findings_only_qwen_pair.

        Single-block: GPT-5.4 (low) + qwen-enrichment + extended categories на каждый блок.
        Пишет финальный _output/02_blocks_analysis.json напрямую (без block_batches.json и
        без blocks.py merge). Поддерживает cancel через job.status и progress через WS.
        """
        pid = job.project_id
        project_dir = resolve_project_dir(pid)

        try:
            import sys
            sys.path.insert(0, str(BASE_DIR))
            from qwen_findings_only import (
                run_findings_only_for_project,
                check_prerequisites,
                FindingsOnlyError,
                DEFAULT_MODEL, DEFAULT_EFFORT, DEFAULT_PARALLELISM,
            )
        except ImportError as exc:
            self._update_pipeline_log(
                pid, "block_analysis", "error",
                error=f"qwen_findings_only import error: {exc}"
            )
            raise RuntimeError(f"qwen_findings_only модуль не найден: {exc}") from exc

        check = check_prerequisites(project_dir)
        for r in check.get("reasons", []):
            await self._log(job, f"  · {r}", "warn" if not check["ok"] else "info")
        if not check["ok"]:
            self._update_pipeline_log(
                pid, "block_analysis", "error",
                error="findings_only_qwen_pair: prerequisites failed (нужен Qwen-enrichment)"
            )
            raise RuntimeError(
                "Stage 02 (findings_only_qwen_pair): нет Qwen-обогащения. "
                "Запустите 'Подготовить данные' с Qwen-enrichment."
            )

        # Модель берём из UI-конфига Stage 02. В findings_only_qwen_pair поддерживаются:
        # OpenRouter (GPT-5.4, Gemini Flash/Pro) и Claude CLI subscription (Sonnet/Opus).
        ui_model = get_stage_model("block_batch")
        findings_only_compatible = {
            "openai/gpt-5.4",
            "google/gemini-3.1-pro-preview",
            "claude-sonnet-4-6",
            "claude-opus-4-7",
        }
        if ui_model in findings_only_compatible:
            model = ui_model
        else:
            model = DEFAULT_MODEL
            await self._log(
                job,
                f"  · UI модель block_batch={ui_model} несовместима с findings_only режимом — "
                f"используем {DEFAULT_MODEL}",
                "warn",
            )
        effort = DEFAULT_EFFORT  # пока эффект reasoning оставляем по умолчанию

        self._reset_job_progress(job)
        job.stage = AuditStage.BLOCK_ANALYSIS
        job.status = JobStatus.RUNNING
        job.progress_total = check["blocks_total"]
        self._update_pipeline_log(pid, "block_analysis", "running")
        await self._start_heartbeat(job)

        await self._log(
            job,
            f"═══ ЭТАП 02 (findings_only_qwen_pair): {check['with_enrichment']}/{check['blocks_total']} "
            f"блоков, model={model}, effort={effort}, parallelism={DEFAULT_PARALLELISM} ═══"
        )

        cancel_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _on_progress(event: dict) -> None:
            t = event.get("type")
            if t == "started":
                asyncio.run_coroutine_threadsafe(
                    self._log(
                        job,
                        f"  Источники enrichment: {event.get('enrichment_sources')}, "
                        f"extended={event.get('extended_prompt')}, section={event.get('section')}"
                    ),
                    loop,
                )
            elif t == "block_done":
                completed = event.get("completed", 0)
                total = event.get("total", 0)
                bid = event.get("block_id", "?")
                pg = event.get("page", "?")
                ms = event.get("elapsed_ms") or 0
                ok = event.get("ok")
                n = event.get("findings", 0)
                job.progress_current = completed
                if ok:
                    msg = (
                        f"  [{completed:>3}/{total}] OK {bid} p={pg} t={ms/1000:.1f}s "
                        f"findings={n} in={event.get('input_tokens')} out={event.get('output_tokens')} "
                        f"reason={event.get('reasoning_tokens')}"
                    )
                    asyncio.run_coroutine_threadsafe(self._log(job, msg), loop)
                else:
                    err = (event.get("error") or "")[:80]
                    asyncio.run_coroutine_threadsafe(
                        self._log(job, f"  [{completed:>3}/{total}] FAIL {bid}: {err}", "warn"),
                        loop,
                    )
                asyncio.run_coroutine_threadsafe(
                    self._progress(job, completed, total),
                    loop,
                )
                if job.status == JobStatus.CANCELLED:
                    cancel_event.set()
            elif t == "block_skip":
                asyncio.run_coroutine_threadsafe(
                    self._log(
                        job,
                        f"  SKIP {event.get('block_id')} p={event.get('page')}: "
                        f"{event.get('reason')} findings=0",
                        "warn",
                    ),
                    loop,
                )

        try:
            from webapp.config import CLAUDE_BLOCK_BATCH_CLEAN_CWD
            result = await run_findings_only_for_project(
                project_dir,
                model=model,
                reasoning_effort=effort,
                claude_clean_cwd=CLAUDE_BLOCK_BATCH_CLEAN_CWD,
                on_progress=_on_progress,
                cancel_event=cancel_event,
            )
        except FindingsOnlyError as e:
            self._update_pipeline_log(
                pid, "block_analysis", "error",
                error=f"findings_only_qwen_pair: {e}"
            )
            raise RuntimeError(f"Stage 02 (findings_only_qwen_pair): {e}") from e

        summary = result["summary"]
        totals = summary["totals"]

        if summary.get("cancelled"):
            self._update_pipeline_log(
                pid, "block_analysis", "error",
                error="findings_only_qwen_pair: отменено пользователем"
            )
            return

        if summary["blocks_failed"] > 0 and summary["blocks_ok"] == 0:
            self._update_pipeline_log(
                pid, "block_analysis", "error",
                error=f"Все {summary['blocks_failed']} блоков упали"
            )
            raise RuntimeError(f"Stage 02 (findings_only_qwen_pair): все блоки упали")

        msg = (
            f"OK ({summary['blocks_ok']}/{summary['blocks_total']} блоков, "
            f"{summary['wall_clock_s']:.0f}s, "
            f"{totals['findings']} findings, "
            f"~${totals['estimated_cost_usd_total']:.3f})"
        )
        if summary["blocks_failed"] > 0:
            msg += f" — {summary['blocks_failed']} блоков упали"
            self._update_pipeline_log(pid, "block_analysis", "done", message=msg)
            await self._log(job, f"  ⚠ {msg}", "warn")
        else:
            self._update_pipeline_log(pid, "block_analysis", "done", message=msg)
            await self._log(job, f"  ✓ {msg}")

        self._record_findings_only_usage(job, summary)

    def _record_findings_only_usage(self, job: AuditJob, summary: dict) -> None:
        """Учесть стоимость stage 02 в режиме findings_only_qwen_pair в usage tracker.

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
        if actual_cost > 0:
            paid_cost_tracker.add(actual_cost)
        job.cost_usd += actual_cost
        job.cli_calls += api_calls
        self._enrich_pipeline_log(
            job.project_id, "block_analysis", model, input_tokens, output_tokens
        )

    def _clean_stage_files(self, project_id: str, files: list[str]):
        """Удалить устаревшие JSON-файлы этапов перед перезапуском."""
        output_dir = resolve_project_dir(project_id) / "_output"
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
        output_dir = resolve_project_dir(project_id) / "_output"
        findings_path = output_dir / "03_findings.json"
        if findings_path.exists():
            backup_path = output_dir / "03_findings_pre_restart.json"
            shutil.copy2(findings_path, backup_path)
            print(f"[{project_id}:clean] Бэкап findings → 03_findings_pre_restart.json")

    # ─── Валидация JSON после записи LLM ───

    @staticmethod
    def _validate_and_repair_json(file_path: Path) -> tuple[bool, str]:
        """
        Проверить JSON-файл и попытаться починить, если невалиден.

        Типичная проблема: LLM пишет неэкранированные кавычки внутри строк,
        например: "раздел ТХ.А "Технологические решения"" вместо
                  "раздел ТХ.А \"Технологические решения\""

        Returns:
            (is_valid, message) — True если файл валиден (или починен).
        """
        import re

        if not file_path.exists():
            return False, "Файл не существует"

        raw = file_path.read_text(encoding="utf-8")

        # 1. Пробуем валидный JSON
        try:
            json.loads(raw)
            return True, "OK"
        except json.JSONDecodeError as original_err:
            pass

        # 2. Бэкап перед ремонтом
        backup_path = file_path.with_suffix(".json.broken")
        backup_path.write_text(raw, encoding="utf-8")

        # 3. Ремонт: заменяем неэкранированные " внутри строковых значений
        # Стратегия: ищем паттерны ": "...внутренние "кавычки"..." и экранируем
        def _fix_inner_quotes(text: str) -> str:
            """Экранировать неэкранированные кавычки внутри JSON-строк."""
            result = []
            i = 0
            in_string = False
            escape_next = False

            while i < len(text):
                ch = text[i]

                if escape_next:
                    result.append(ch)
                    escape_next = False
                    i += 1
                    continue

                if ch == '\\' and in_string:
                    result.append(ch)
                    escape_next = True
                    i += 1
                    continue

                if ch == '"':
                    if not in_string:
                        in_string = True
                        result.append(ch)
                    else:
                        # Это " внутри строки — конец строки или внутренняя кавычка?
                        # Смотрим что после: если , ] } : или пробел+один из них — конец строки
                        rest = text[i + 1:].lstrip()
                        if not rest or rest[0] in (',', ']', '}', ':'):
                            in_string = False
                            result.append(ch)
                        else:
                            # Внутренняя кавычка — экранируем
                            result.append('\\"')
                    i += 1
                    continue

                result.append(ch)
                i += 1

            return ''.join(result)

        fixed = _fix_inner_quotes(raw)

        try:
            json.loads(fixed)
            file_path.write_text(fixed, encoding="utf-8")
            return True, f"Repaired (бэкап: {backup_path.name})"
        except json.JSONDecodeError:
            pass

        # 4. Fallback: замена типографских кавычек на экранированные
        fixed2 = raw.replace('\u201c', '\\"').replace('\u201d', '\\"')
        fixed2 = re.sub(
            r'(?<=": ")(.+?)(?="[,\s\n\r]*[}\]])',
            lambda m: m.group(0).replace('"', '\\"') if '"' in m.group(0) else m.group(0),
            fixed2,
        )

        try:
            json.loads(fixed2)
            file_path.write_text(fixed2, encoding="utf-8")
            return True, f"Repaired via fallback (бэкап: {backup_path.name})"
        except json.JSONDecodeError as e:
            # Не удалось починить — возвращаем оригинал
            file_path.write_text(raw, encoding="utf-8")
            return False, f"Ремонт не удался: {e}"

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
            output_dir = resolve_project_dir(pid) / "_output"

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

    def detect_resume_stage(self, project_id: str) -> dict:
        """Делегирует в resume_detector.detect_resume_stage()."""
        return _detect_resume_stage(project_id)

    async def start_from_stage(self, project_id: str, stage: str) -> AuditJob:
        """Запустить конвейер с указанного этапа (ручной перезапуск цепочки).

        Кладёт single-task в общую очередь — фактический запуск произойдёт,
        когда worker дойдёт до элемента (см. `_enqueue_single`/`_dispatch_action`).
        """
        valid_stages = [
            "prepare", "qwen_enrichment", "text_analysis", "block_analysis",
            "findings_merge", "findings_review", "norm_verify", "excel",
        ]
        if stage not in valid_stages:
            raise RuntimeError(f"Неизвестный этап: {stage}")
        return await self._enqueue_single(
            project_id, action="retry_stage", retry_stage=stage,
        )

    async def resume_pipeline(self, project_id: str) -> AuditJob:
        """Продолжить пайплайн с места ошибки.

        resume-проверка детектит этап на момент непосредственного запуска
        (внутри `_dispatch_action`) — на момент enqueue достаточно знать
        проект.
        """
        # Быстрая проверка чтобы не пускать в очередь заведомо нечего возобновлять
        resume_info = self.detect_resume_stage(project_id)
        if not resume_info.get("can_resume"):
            raise RuntimeError("Все этапы уже завершены — нечего возобновлять")
        return await self._enqueue_single(project_id, action="resume")

    async def _run_resumed_pipeline(self, job: AuditJob, start_stage: str, resume_info: dict):
        """Запуск OCR-пайплайна с указанного этапа."""
        start_time = datetime.now()
        pid = job.project_id
        try:
            # OCR-пайплайн: этапы в правильном порядке
            stages = [
                "prepare",          # 1: blocks.py crop
                "crop_blocks",      # 1: кроп блоков (alias prepare)
                "text_analysis",    # 2: Claude анализ текста MD
                "block_analysis",   # 3-4: генерация пакетов + анализ блоков
                "tile_audit",       # alias для block_analysis (legacy)
                "findings_merge",   # 5: свод замечаний
                "main_audit",       # alias для findings_merge (legacy)
                "norm_verify",      # 6: верификация норм
            ]

            # Нормализация stage: legacy aliases → OCR stages
            normalized = start_stage
            if start_stage == "crop_blocks":
                normalized = "prepare"
            elif start_stage in ("tile_audit",):
                normalized = "block_analysis"
            elif start_stage == "main_audit":
                normalized = "findings_merge"

            # Порядок этапов OCR-пайплайна (без дублей)
            # ВАЖНО: qwen_enrichment встроено внутрь блока start_idx<=0 после crop_blocks
            # (не отдельный индекс — оно идёт каждый раз когда мы заходим в crop_blocks step).
            # При resume с text_analysis enrichment пропускается (уже сделан до crop_blocks ошибки).
            ocr_stages = ["prepare", "text_analysis", "block_analysis", "findings_merge", "findings_review", "norm_verify", "excel"]
            start_idx = ocr_stages.index(normalized) if normalized in ocr_stages else 0

            await self._log(
                job,
                f"Возобновление конвейера с этапа: {resume_info.get('stage_label', start_stage)} "
                f"({resume_info.get('detail', '')})",
                "info",
            )

            output_dir = resolve_project_dir(pid) / "_output"
            info_path = resolve_project_dir(pid) / "project_info.json"
            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            # ═══ ЭТАП 1: Кроп image-блоков ═══
            if start_idx <= 0:
                # Полный перезапуск — бэкап findings перед очисткой
                self._backup_findings_before_restart(pid)
                # Очистить все промежуточные файлы
                self._clean_stage_files(pid, [
                    "01_text_analysis.json", "02_blocks_analysis.json",
                    "03_findings.json", "block_batch_*.json", "block_batches.json",
                ])
                job.stage = AuditStage.CROP_BLOCKS
                self._update_pipeline_log(pid, "crop_blocks", "running")
                print(f"[{pid}:resume] ═══ ЭТАП 1: Кроп image-блоков ═══")
                await self._log(job, "═══ ЭТАП 1: Кроп image-блоков из PDF ═══")
                crop_args = _build_crop_args(_project_path(pid))
                exit_code, _, stderr = await self._run_script(
                    pid,
                    str(BLOCKS_SCRIPT),
                    crop_args,
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code == 2:
                    # Частичная ошибка: не все блоки скачались (404 и т.п.)
                    self._update_pipeline_log(pid, "crop_blocks", "error",
                                               error="Не все блоки скачались. Проверьте актуальность crop_url в result.json")
                    raise RuntimeError("Кроп блоков: не все image-блоки скачались (HTTP 404). "
                                       "Обновите OCR-результат и повторите.")
                elif exit_code != 0:
                    self._update_pipeline_log(pid, "crop_blocks", "error",
                                               error=stderr or f"Exit code: {exit_code}")
                    raise RuntimeError(f"Кроп блоков: {stderr}")
                crop_mode = {"qwen": "QWEN 300 DPI", "claude": "Claude 100 DPI", "compact": "compact"}[_block_batch_crop_mode()]
                self._update_pipeline_log(pid, "crop_blocks", "done", message=f"OK ({crop_mode})")

                # Построить document_graph v2 (Python, без LLM)
                await self._build_document_graph_v2(job)

                if job.status == JobStatus.CANCELLED:
                    return

                # ЭТАП 00: Qwen-обогащение MD (всегда выполняется, идемпотентно)
                await self._run_qwen_enrichment_stage(job)

                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══
            if start_idx <= 1:
                if start_idx == 1:
                    # Resume с этого этапа — бэкап findings перед очисткой
                    self._backup_findings_before_restart(pid)
                    # Очистить старые результаты
                    self._clean_stage_files(pid, [
                        "01_text_analysis.json", "02_blocks_analysis.json",
                        "03_findings.json", "block_batch_*.json", "block_batches.json",
                    ])
                self._reset_job_progress(job)
                job.stage = AuditStage.TEXT_ANALYSIS
                job.status = JobStatus.RUNNING
                self._update_pipeline_log(pid, "text_analysis", "running")
                print(f"[{pid}:resume] ═══ ЭТАП 2: Текстовый анализ MD ═══")
                await self._log(job, "═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══")
                await self._start_heartbeat(job)

                can_go = await self._check_before_launch(job)
                if not can_go:
                    raise RuntimeError("Rate limit: ожидание превышено или отменено")

                exit_code, output, cli_result = await claude_runner.run_text_analysis(
                    project_info, pid,
                    on_output=lambda msg: self._log(job, msg),
                )
                self._record_cli_usage(job, cli_result, "text_analysis")

                if claude_runner.is_cancelled(exit_code):
                    job.status = JobStatus.CANCELLED
                    return
                if exit_code != 0:
                    self._update_pipeline_log(pid, "text_analysis", "error",
                                               error=_extract_error_detail(exit_code, output))
                    raise RuntimeError(f"Текстовый анализ: код {exit_code}")

                text_analysis_path = output_dir / "01_text_analysis.json"
                if not text_analysis_path.exists():
                    raise RuntimeError("01_text_analysis.json не создан")

                self._update_pipeline_log(pid, "text_analysis", "done", message="OK")

                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ ЭТАП 3-4: Генерация пакетов + анализ блоков (Claude) ═══
            if start_idx <= 2 and get_stage_batch_mode("block_batch") == "findings_only_qwen_pair":
                # Ветвь findings_only_qwen_pair — single-block через GPT-5.4 + qwen-enrichment.
                # Не использует blocks.py batches/merge — пишет 02_blocks_analysis.json напрямую.
                await self._run_block_analysis_findings_only(job)
                if job.status == JobStatus.CANCELLED:
                    return
                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()
                # Block retry пропускаем: findings-only не помечает unreadable_text=true.
            elif start_idx <= 2:
                batch_start_from = resume_info.get("start_from", 1) if start_idx == 2 else 1
                batches_file = output_dir / "block_batches.json"

                # Генерация пакетов (если нет или свежий старт)
                need_generate = not batches_file.exists() or start_idx < 2
                if need_generate:
                    self._reset_job_progress(job)
                    job.stage = AuditStage.CROP_BLOCKS  # reuse для генерации батчей

                    gen_args = [_project_path(pid)]
                    print(f"[{pid}:resume] ═══ ЭТАП 3: Генерация пакетов блоков ═══")
                    await self._log(job, "═══ ЭТАП 3: Генерация пакетов блоков ═══")

                    exit_code, _, stderr = await self._run_script(
                        pid,
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

                batches = batches_data.get("batches", [])
                batches, single_block_mode = _expand_block_batches_for_local_model(batches)
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
                        await self._log(job, "Локальный QWEN: stage 02 переведён в single-block режим")

                    semaphore = asyncio.Semaphore(parallel)
                    completed_count = 0
                    error_count = 0

                    # Время начала этапа — для фильтрации файлов от старых запусков
                    batch_stage_start = datetime.now().timestamp()
                    # Smart retry: при повторе конкретного этапа сохраняем успешные пакеты
                    _smart_retry = resume_info.get("is_stage_retry", False) and start_idx == 2
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
                        await asyncio.gather(*tasks, return_exceptions=True)

                    if error_count > 0:
                        self._update_pipeline_log(pid, "block_analysis", "error",
                                                   error=f"{error_count} пакетов с ошибками")
                        if error_count >= total_batches:
                            raise RuntimeError(f"Все пакеты завершились с ошибками")
                    else:
                        self._update_pipeline_log(pid, "block_analysis", "done",
                                                   message=f"OK ({total_batches} пакетов)")

                # Слияние результатов block_batch_*.json → 02_blocks_analysis.json
                print(f"[{pid}:resume] Слияние block_batch_*.json → 02_blocks_analysis.json")
                await self._log(job, "Слияние результатов блоков...")
                exit_code, _, stderr = await self._run_script(
                    pid,
                    str(BLOCKS_SCRIPT),
                    ["merge", _project_path(pid)],
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
            if start_idx <= 3:
                self._clean_stage_files(pid, [
                    "03_findings.json", "03_findings_review.json", "03_findings_pre_review.json",
                ])
                self._reset_job_progress(job)
                job.stage = AuditStage.FINDINGS_MERGE
                job.status = JobStatus.RUNNING

                findings_model = get_stage_model("findings_merge")
                self._update_pipeline_log(pid, "findings_merge", "running")
                print(f"[{pid}:resume] ═══ ЭТАП 5: Свод замечаний ═══")
                await self._log(job, f"═══ ЭТАП 5: Свод замечаний ({findings_model}) ═══")
                await self._start_heartbeat(job)

                can_go = await self._check_before_launch(job)
                if not can_go:
                    raise RuntimeError("Rate limit: ожидание превышено или отменено")

                exit_code, output, cli_result = await claude_runner.run_findings_merge(
                    project_info, pid,
                    on_output=lambda msg: self._log(job, msg),
                )
                self._record_cli_usage(job, cli_result, "findings_merge")

                if claude_runner.is_cancelled(exit_code):
                    job.status = JobStatus.CANCELLED
                    return
                if exit_code != 0:
                    self._update_pipeline_log(pid, "findings_merge", "error",
                                               error=_extract_error_detail(exit_code, output))
                    raise RuntimeError(f"Свод замечаний: код {exit_code}")

                findings_path = output_dir / "03_findings.json"
                if not findings_path.exists():
                    raise RuntimeError("03_findings.json не создан")

                # Валидация JSON после findings_merge
                is_valid, repair_msg = self._validate_and_repair_json(findings_path)
                if not is_valid:
                    raise RuntimeError(f"03_findings.json невалиден: {repair_msg}")
                if "Repaired" in repair_msg:
                    await self._log(job, f"03_findings.json починен: {repair_msg}", "warn")

                self._update_pipeline_log(pid, "findings_merge", "done", message="OK")

                # Post-merge: backfill text-evidence из compact/graph
                self._backfill_text_evidence_in_findings(pid)

                # Объединение похожих замечаний
                merge_result = self._merge_similar_findings(pid)
                if merge_result and merge_result.get("merged_groups", 0) > 0:
                    await self._log(
                        job,
                        f"Объединено похожих замечаний: {merge_result['before']} → {merge_result['after']} "
                        f"({merge_result['merged_groups']} групп)",
                    )

                self._refresh_finding_quality(pid)

                # Восстановление highlight_regions из 02_blocks_analysis
                self._backfill_highlight_regions(pid)

                # «Размышление модели»: стрим найденных замечаний в live-лог
                await self._stream_findings_events(job, "merge")

                if job.status == JobStatus.CANCELLED:
                    return

                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАПЫ 5.5-6: Параллельный запуск critic + norms (+ optimization) ═══
            if start_idx < 4:
                # Полный post-findings: critic + norms + optimization (параллельно)
                findings_path = resolve_project_dir(pid) / "_output" / "03_findings.json"
                if findings_path.exists():
                    await self._run_post_findings_parallel(job, project_info)

                    if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                        return

                    self.active_jobs[pid] = job
                    self._tasks[pid] = asyncio.current_task()
                else:
                    await self._log(job, "03_findings.json не найден — пропуск верификации", "warn")

            # Resume только findings_review (critic+corrector) — без повтора norms/optimization
            if start_idx == 4:
                findings_path = resolve_project_dir(pid) / "_output" / "03_findings.json"
                if findings_path.exists():
                    await self._start_heartbeat(job)
                    await self._run_findings_review(job, project_info)

                    if job.status in (JobStatus.CANCELLED, JobStatus.FAILED):
                        return

                    # Проверяем: если critic провалился — не маскировать ошибку
                    _plog_path = resolve_project_dir(pid) / "_output" / "pipeline_log.json"
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

            # Если resume начался с norm_verify (start_idx=5) — запускать только norms
            if start_idx == 5:
                self._clean_stage_files(pid, [
                    "03a_norms_verified.json", "norm_checks.json", "norm_checks_llm.json",
                    "missing_norms_queue.json", "missing_norms_report.json",
                    "missing_norms_queue.md",
                ])
                self._reset_job_progress(job)
                findings_path = resolve_project_dir(pid) / "_output" / "03_findings.json"
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
            self._update_pipeline_log(pid, "excel", "running")
            print(f"[{pid}:resume] ═══ ЭТАП 7: Excel ═══")
            await self._log(job, "═══ ЭТАП 7: Генерация Excel ═══")
            project_path = str(resolve_project_dir(pid))
            exit_code, _xls_out, _xls_err = await self._run_script(
                pid,
                str(GENERATE_EXCEL_SCRIPT),
                args=[project_path],
                env_overrides={"AUDIT_NO_OPEN": "1"},
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code == 0:
                self._update_pipeline_log(pid, "excel", "done", message="OK")
            else:
                self._update_pipeline_log(pid, "excel", "error",
                                           error=_extract_error_detail(exit_code, (_xls_err or "") + "\n" + (_xls_out or "")))

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
    async def start_prepare(self, project_id: str) -> AuditJob:
        return await self._enqueue_single(project_id, action="prepare")

    async def _run_prepare(self, job: AuditJob):
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "prepare", "running")
            await self._log(job, "Запуск подготовки проекта (текст + тайлы)...")
            await self._start_heartbeat(job)

            exit_code, stdout, stderr = await self._run_script(
                pid,
                str(PROCESS_PROJECT_SCRIPT),
                [_project_path(pid), "--quality", DEFAULT_TILE_QUALITY],
                on_output=lambda msg: self._log(job, msg),
            )

            if exit_code == 0:
                await self._log(job, "Подготовка завершена успешно", "info")
                job.status = JobStatus.COMPLETED
                self._update_pipeline_log(pid, "prepare", "done", message="OK")
            else:
                await self._log(job, f"Ошибка подготовки (код {exit_code})", "error")
                if stderr:
                    await self._log(job, stderr, "error")
                job.status = JobStatus.FAILED
                job.error_message = stderr or f"Exit code: {exit_code}"
                self._update_pipeline_log(pid, "prepare", "error",
                                           error=stderr or f"Exit code: {exit_code}")
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
    async def start_tile_audit(self, project_id: str, start_from: int = 1) -> AuditJob:
        return await self._enqueue_single(
            project_id, action="tile_audit",
            extra_params={"start_from": start_from},
        )

    async def _run_tile_audit(self, job: AuditJob, start_from: int = 1, pages_filter: list[int] | None = None, standalone: bool = True):
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "tile_audit", "running")
            output_dir = resolve_project_dir(job.project_id) / "_output"
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
                info_path = resolve_project_dir(job.project_id) / "project_info.json"
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
                gen_args = [_project_path(job.project_id)]
                if pages_filter:
                    pages_str = ",".join(str(p) for p in pages_filter)
                    gen_args += ["--pages", pages_str]
                    await self._log(job, f"Генерация пакетов тайлов (страницы: {pages_str})...")
                else:
                    await self._log(job, "Генерация пакетов тайлов...")
                exit_code, _, stderr = await self._run_script(
                    pid,
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

            # Загружаем project_info
            info_path = resolve_project_dir(job.project_id) / "project_info.json"
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
            await asyncio.gather(*tasks, return_exceptions=True)

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
                exit_code, _, stderr = await self._run_script(
                    job.project_id,
                    str(BLOCKS_SCRIPT),
                    ["merge", _project_path(job.project_id)],
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
    async def start_main_audit(self, project_id: str) -> AuditJob:
        # cleanup stage-файлов и установка stage перенесены в `_dispatch_action`
        return await self._enqueue_single(project_id, action="main_audit")

    async def _run_main_audit(self, job: AuditJob, standalone: bool = True):
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "main_audit", "running")
            info_path = resolve_project_dir(pid) / "project_info.json"
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
    async def start_norm_verify(self, project_id: str) -> AuditJob:
        # cleanup перенесён в `_dispatch_action`
        return await self._enqueue_single(project_id, action="norm_verify")

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
        """
        Critic + Corrector: проверка и корректировка замечаний.

        1. Critic проверяет каждое F-замечание (evidence, grounding, page/sheet)
           Если findings > CRITIC_CHUNK_SIZE — разбивает на чанки.
        2. Если есть отрицательные вердикты — Corrector исправляет
        """
        pid = job.project_id
        output_dir = resolve_project_dir(pid) / "_output"

        # ── Pre-Critic: Python-level grounding ──
        try:
            from webapp.services.grounding_service import run_grounding
            findings_path = output_dir / "03_findings.json"
            blocks_path = output_dir / "02_blocks_analysis.json"
            if findings_path.exists() and blocks_path.exists():
                grounding_stats = run_grounding(findings_path, blocks_path)
                await self._log(
                    job,
                    f"Grounding: {grounding_stats.get('grounding_candidates_added', 0)} "
                    f"findings обогащены кандидатами "
                    f"(уже привязано: {grounding_stats.get('already_grounded', 0)})",
                )
        except Exception as e:
            await self._log(job, f"Grounding пропущен: {e}", "warn")

        # Все замечания проверяются Critic'ом без фильтрации

        # ── Critic (с chunking при большом кол-ве findings) ──
        self._reset_job_progress(job)
        job.stage = AuditStage.FINDINGS_REVIEW
        job.status = JobStatus.RUNNING
        self._update_pipeline_log(pid, "findings_critic", "running")
        print(f"[{pid}] ═══ ЭТАП 6.5a: Critic (проверка замечаний) ═══")
        await self._log(job, "═══ ЭТАП 6.5a: Critic — проверка обоснованности замечаний ═══")

        # Определяем нужен ли chunking — все findings из 03_findings.json
        findings_path = output_dir / "03_findings.json"
        need_chunks = False
        all_findings = []

        if findings_path.exists():
            try:
                findings_data = json.loads(findings_path.read_text(encoding="utf-8"))
                all_findings = findings_data.get("findings", findings_data.get("items", []))
                need_chunks = len(all_findings) > CRITIC_CHUNK_SIZE
            except (json.JSONDecodeError, OSError):
                pass

        if need_chunks:
            # ── Chunked Critic (ПАРАЛЛЕЛЬНЫЙ) ──
            total_findings = len(all_findings)
            chunks = [
                all_findings[i:i + CRITIC_CHUNK_SIZE]
                for i in range(0, total_findings, CRITIC_CHUNK_SIZE)
            ]
            num_chunks = len(chunks)
            await self._log(
                job,
                f"Chunked Critic (parallel): {total_findings} findings -> "
                f"{num_chunks} чанков по ~{CRITIC_CHUNK_SIZE}",
            )

            # 1. Записываем chunk-specific input файлы (без конфликтов)
            for chunk_idx, chunk_findings in enumerate(chunks, 1):
                suffix = f"_{chunk_idx:03d}"
                chunk_input = {
                    "meta": {
                        "source": "full_review",
                        "total_findings": total_findings,
                        "chunk_count": len(chunk_findings),
                        "chunk": chunk_idx,
                        "total_chunks": num_chunks,
                    },
                    "findings": chunk_findings,
                }
                chunk_input_path = output_dir / f"03_findings_review_input{suffix}.json"
                chunk_input_path.write_text(
                    json.dumps(chunk_input, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

            # 2. Запускаем все чанки параллельно через семафор
            critic_parallel = (
                1 if is_local_llm_model(get_stage_model("findings_critic"))
                else MAX_PARALLEL_BATCHES
            )
            critic_semaphore = asyncio.Semaphore(critic_parallel)
            chunk_results: list[dict | None] = [None] * num_chunks  # slot per chunk

            async def _run_critic_chunk(cidx: int) -> None:
                """Запустить один чанк critic-а."""
                suffix = f"_{cidx:03d}"
                async with critic_semaphore:
                    if job.status == JobStatus.CANCELLED:
                        return

                    await self._log(
                        job,
                        f"Critic чанк {cidx}/{num_chunks}: "
                        f"{len(chunks[cidx - 1])} findings...",
                    )

                    can_go = await self._check_before_launch(job)
                    if not can_go:
                        await self._log(job, f"Critic чанк {cidx}: rate limit, пропуск", "warn")
                        return

                    exit_code, output, cli_result = await claude_runner.run_findings_critic(
                        project_info, pid,
                        on_output=lambda msg: self._log(job, msg),
                        chunk_suffix=suffix,
                    )
                    self._record_cli_usage(job, cli_result, f"findings_critic_chunk{cidx}")

                    if claude_runner.is_cancelled(exit_code):
                        job.status = JobStatus.CANCELLED
                        return

                    # Читаем chunk-specific результат (проверяем файл НЕЗАВИСИМО от exit code)
                    chunk_review_path = output_dir / f"03_findings_review{suffix}.json"
                    if exit_code != 0 and not chunk_review_path.exists():
                        await self._log(
                            job,
                            f"Critic чанк {cidx}/{num_chunks}: код {exit_code}, файл не создан",
                            "warn",
                        )
                        return

                    if exit_code != 0:
                        await self._log(
                            job,
                            f"Critic чанк {cidx}/{num_chunks}: CLI код {exit_code}, "
                            f"но файл создан — пробуем использовать",
                            "warn",
                        )

                    if chunk_review_path.exists():
                        try:
                            chunk_review = json.loads(
                                chunk_review_path.read_text(encoding="utf-8")
                            )
                            chunk_results[cidx - 1] = chunk_review
                            chunk_meta = chunk_review.get("meta", {})
                            await self._log(
                                job,
                                f"Critic чанк {cidx}: "
                                f"{chunk_meta.get('total_reviewed', '?')} проверено, "
                                f"{chunk_meta.get('verdicts', {}).get('pass', 0)} pass",
                            )
                        except (json.JSONDecodeError, OSError) as e:
                            await self._log(
                                job,
                                f"Ошибка чтения результата чанка {cidx}: {e}",
                                "warn",
                            )

            # Запуск всех чанков параллельно
            tasks = [
                asyncio.create_task(_run_critic_chunk(cidx))
                for cidx in range(1, num_chunks + 1)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

            if job.status == JobStatus.CANCELLED:
                return

            # 3. Слияние результатов
            all_reviews = []
            merged_verdicts = {}
            total_reviewed_all = 0
            chunks_ok = 0

            for cr in chunk_results:
                if cr is None:
                    continue
                chunks_ok += 1
                chunk_reviews = cr.get("reviews", [])
                all_reviews.extend(chunk_reviews)
                chunk_meta = cr.get("meta", {})
                total_reviewed_all += chunk_meta.get("total_reviewed", len(chunk_reviews))
                for k, v in chunk_meta.get("verdicts", {}).items():
                    merged_verdicts[k] = merged_verdicts.get(k, 0) + v

            if not all_reviews and chunks_ok == 0:
                self._update_pipeline_log(pid, "findings_critic", "error",
                                           error="Все чанки провалились")
                await self._log(job, "Critic: все чанки провалились, пропуск корректировки", "warn")
                return

            # Сливаем в единый 03_findings_review.json
            merged_review = {
                "meta": {
                    "project_id": pid,
                    "review_date": datetime.now().isoformat(),
                    "total_reviewed": total_reviewed_all,
                    "verdicts": merged_verdicts,
                    "chunks_total": num_chunks,
                    "chunks_ok": chunks_ok,
                },
                "reviews": all_reviews,
            }
            review_path = output_dir / "03_findings_review.json"
            review_path.write_text(
                json.dumps(merged_review, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            # Corrector теперь читает 03_findings.json напрямую через prompt_builder,
            # поэтому отдельный review_input файл не нужен.

            # Cleanup: удаляем chunk-specific файлы
            for cidx in range(1, num_chunks + 1):
                suffix = f"_{cidx:03d}"
                for pattern in [f"03_findings_review_input{suffix}.json",
                                f"03_findings_review{suffix}.json"]:
                    p = output_dir / pattern
                    if p.exists():
                        p.unlink()

            self._update_pipeline_log(pid, "findings_critic", "done",
                                       message=f"Parallel: {num_chunks} chunks, {total_reviewed_all} reviewed")
            review_data = merged_review

        else:
            # ── Single-shot Critic (как раньше) ──
            can_go = await self._check_before_launch(job)
            if not can_go:
                await self._log(job, "Rate limit: ожидание превышено или отменено", "warn")
                return

            exit_code, output, cli_result = await claude_runner.run_findings_critic(
                project_info, pid,
                on_output=lambda msg: self._log(job, msg),
            )
            self._record_cli_usage(job, cli_result, "findings_critic")

            if claude_runner.is_cancelled(exit_code):
                job.status = JobStatus.CANCELLED
                return

            if claude_runner.is_cancelled(exit_code):
                # Уже обработано выше, но на всякий случай
                job.status = JobStatus.CANCELLED
                return

            # Читаем результат — проверяем файл НЕЗАВИСИМО от exit code
            review_path = output_dir / "03_findings_review.json"
            if exit_code != 0:
                # CLI вернул ошибку, но файл мог быть записан до сбоя
                if review_path.exists():
                    try:
                        review_data = json.loads(review_path.read_text(encoding="utf-8"))
                        reviewed = review_data.get("meta", {}).get("total_reviewed", 0)
                        if reviewed > 0:
                            await self._log(
                                job,
                                f"Critic: CLI код {exit_code}, но review файл валиден "
                                f"({reviewed} reviewed) — продолжаем",
                                "warn",
                            )
                            self._update_pipeline_log(
                                pid, "findings_critic", "done",
                                message=f"OK (CLI код {exit_code}, файл валиден)",
                            )
                        else:
                            raise ValueError("total_reviewed == 0")
                    except (json.JSONDecodeError, OSError, ValueError):
                        self._update_pipeline_log(pid, "findings_critic", "error",
                                                   error=_extract_error_detail(exit_code, output))
                        await self._log(job, f"Critic: код {exit_code}, файл невалиден — пропуск", "warn")
                        return
                else:
                    self._update_pipeline_log(pid, "findings_critic", "error",
                                               error=_extract_error_detail(exit_code, output))
                    await self._log(job, f"Critic: код {exit_code}, файл не создан — пропуск", "warn")
                    return
            else:
                self._update_pipeline_log(pid, "findings_critic", "done", message="OK")

                if not review_path.exists():
                    await self._log(job, "03_findings_review.json не создан — пропуск Corrector", "warn")
                    return

                try:
                    review_data = json.loads(review_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    await self._log(job, "Ошибка чтения 03_findings_review.json", "warn")
                    return

        # «Размышление модели»: стрим вердиктов критика (единая точка для parallel и single)
        await self._stream_findings_events(job, "critic")

        # ── Анализ результатов Critic ──
        verdicts = review_data.get("meta", {}).get("verdicts", {})
        total_pass = verdicts.get("pass", 0)
        total_reviewed = review_data.get("meta", {}).get("total_reviewed", 0)
        total_issues = total_reviewed - total_pass

        await self._log(
            job,
            f"Critic: {total_reviewed} проверено, {total_pass} pass, {total_issues} проблем",
        )

        if total_issues == 0:
            await self._log(job, "Все замечания обоснованы — Corrector не требуется")
            await self._stream_findings_events(job, "done")
            return

        # ── Corrector (с поддержкой чанков) ──
        self._update_pipeline_log(pid, "findings_corrector", "running")
        print(f"[{pid}] ═══ ЭТАП 6.5b: Corrector (корректировка замечаний) ═══")

        # «Размышление модели»: сигнал смены фазы на corrector
        await self._stream_findings_events(job, "corrector")

        # Извлекаем ID замечаний с проблемами из review
        issue_ids: list[str] = []
        for rev in review_data.get("reviews", []):
            if rev.get("verdict", "pass") != "pass":
                fid = rev.get("finding_id") or rev.get("id", "")
                if fid:
                    issue_ids.append(fid)
        if not issue_ids:
            issue_ids = [f"F-{i:03d}" for i in range(1, total_issues + 1)]

        need_chunks = total_issues > CORRECTOR_CHUNK_SIZE
        if need_chunks:
            chunks = [
                issue_ids[i:i + CORRECTOR_CHUNK_SIZE]
                for i in range(0, len(issue_ids), CORRECTOR_CHUNK_SIZE)
            ]
            await self._log(
                job,
                f"═══ ЭТАП 6.5b: Corrector — {total_issues} замечаний → "
                f"{len(chunks)} чанков по ~{CORRECTOR_CHUNK_SIZE} ═══",
            )
        else:
            chunks = [issue_ids]
            await self._log(
                job,
                f"═══ ЭТАП 6.5b: Corrector — корректировка {total_issues} замечаний ═══",
            )

        corrector_ok = False
        for cidx, chunk_ids in enumerate(chunks):
            chunk_label = f" (чанк {cidx + 1}/{len(chunks)})" if need_chunks else ""

            # Для чанков: перезаписываем 03_findings_review.json, оставляя только нужные findings
            # (corrector читает именно этот файл — и CLI и OpenRouter)
            review_path = output_dir / "03_findings_review.json"
            if need_chunks:
                chunk_review = dict(review_data)
                chunk_review["reviews"] = [
                    r for r in review_data.get("reviews", [])
                    if (r.get("finding_id") or r.get("id", "")) in chunk_ids
                ]
                chunk_meta = dict(chunk_review.get("meta", {}))
                chunk_meta["total_reviewed"] = len(chunk_review["reviews"])
                chunk_meta["chunk"] = f"{cidx + 1}/{len(chunks)}"
                chunk_review["meta"] = chunk_meta
                review_path.write_text(
                    json.dumps(chunk_review, ensure_ascii=False, indent=2), encoding="utf-8",
                )
                await self._log(job, f"Corrector{chunk_label}: {', '.join(chunk_ids)}")

            can_go = await self._check_before_launch(job)
            if not can_go:
                await self._log(job, "Rate limit: ожидание превышено или отменено", "warn")
                return

            exit_code, output, cli_result = await claude_runner.run_findings_corrector(
                project_info, pid,
                on_output=lambda msg: self._log(job, msg),
            )
            self._record_cli_usage(job, cli_result, f"findings_corrector{f'_chunk{cidx}' if need_chunks else ''}")

            if claude_runner.is_cancelled(exit_code):
                job.status = JobStatus.CANCELLED
                return

            if exit_code != 0:
                # CLI может вернуть -1/1, но файл уже записан — проверяем
                findings_path = output_dir / "03_findings.json"
                pre_review = output_dir / "03_findings_pre_review.json"
                if findings_path.exists() and pre_review.exists():
                    try:
                        new_data = json.loads(findings_path.read_text(encoding="utf-8"))
                        old_data = json.loads(pre_review.read_text(encoding="utf-8"))
                        new_count = len(new_data.get("findings", []))
                        old_count = len(old_data.get("findings", []))
                        if new_count > 0 and new_data != old_data:
                            await self._log(
                                job,
                                f"Corrector{chunk_label}: CLI код {exit_code}, но файл обновлён "
                                f"({old_count} → {new_count}) — считаем успехом",
                                "warn",
                            )
                            corrector_ok = True
                        else:
                            await self._log(job, f"Corrector{chunk_label}: код {exit_code}, файл не изменился", "warn")
                            if not need_chunks:
                                self._update_pipeline_log(pid, "findings_corrector", "error",
                                                           error=_extract_error_detail(exit_code, output))
                                return
                    except (json.JSONDecodeError, OSError):
                        await self._log(job, f"Corrector{chunk_label}: код {exit_code}, JSON невалиден", "warn")
                        if not need_chunks:
                            self._update_pipeline_log(pid, "findings_corrector", "error",
                                                       error=_extract_error_detail(exit_code, output))
                            return
                else:
                    await self._log(job, f"Corrector{chunk_label}: код {exit_code}", "warn")
                    if not need_chunks:
                        self._update_pipeline_log(pid, "findings_corrector", "error",
                                                   error=_extract_error_detail(exit_code, output))
                        return
            else:
                corrector_ok = True
                await self._log(job, f"Corrector{chunk_label} завершён — 03_findings.json обновлён")

        # Восстановить полный review после всех чанков
        if need_chunks:
            review_path = output_dir / "03_findings_review.json"
            review_path.write_text(
                json.dumps(review_data, ensure_ascii=False, indent=2), encoding="utf-8",
            )

        if corrector_ok:
            self._update_pipeline_log(pid, "findings_corrector", "done",
                                       message=f"OK ({len(chunks)} чанков)" if need_chunks else "OK")
        else:
            self._update_pipeline_log(pid, "findings_corrector", "error", error="Все чанки провалились")

        # Валидация JSON после corrector (LLM может записать невалидный JSON)
        findings_path = output_dir / "03_findings.json"
        if findings_path.exists():
            is_valid, repair_msg = self._validate_and_repair_json(findings_path)
            if not is_valid:
                await self._log(
                    job,
                    f"ВНИМАНИЕ: 03_findings.json невалиден после Corrector: {repair_msg}. "
                    f"Восстанавливаю pre_review версию.",
                    "error",
                )
                # Fallback: восстанавливаем бэкап до corrector
                pre_review = output_dir / "03_findings_pre_review.json"
                if pre_review.exists():
                    import shutil
                    shutil.copy2(pre_review, findings_path)
                    await self._log(job, "Восстановлен 03_findings_pre_review.json", "warn")
            elif "Repaired" in repair_msg:
                await self._log(job, f"JSON починен автоматически: {repair_msg}", "warn")

        # Восстановление norm_quote из pre_review (corrector может потерять)
        await self._restore_norm_quotes(output_dir, job)
        self._refresh_finding_quality(pid)

        # «Размышление модели»: финальный сигнал — поток завершён
        await self._stream_findings_events(job, "done")

    @staticmethod
    async def _restore_norm_quotes(output_dir: Path, job: "AuditJob"):
        """Восстановить norm_quote из pre_review бэкапа.

        Corrector может перезаписать findings без этого поля.
        Берём его из бэкапа (до corrector) и подставляем обратно.
        """
        findings_path = output_dir / "03_findings.json"
        pre_review_path = output_dir / "03_findings_pre_review.json"
        if not findings_path.exists() or not pre_review_path.exists():
            return

        try:
            current = json.loads(findings_path.read_text(encoding="utf-8"))
            backup = json.loads(pre_review_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return

        backup_map = {f.get("id"): f for f in backup.get("findings", [])}
        restored = 0
        for finding in current.get("findings", []):
            fid = finding.get("id")
            if not fid or fid not in backup_map:
                continue
            orig = backup_map[fid]
            # Восстановить norm_quote если потерян
            if not finding.get("norm_quote") and orig.get("norm_quote"):
                finding["norm_quote"] = orig["norm_quote"]
                restored += 1
        if restored > 0:
            findings_path.write_text(
                json.dumps(current, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

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

    async def _run_norm_verification(
        self,
        job: AuditJob,
        standalone: bool = True,
        wait_before_fix: asyncio.Event | None = None,
    ):
        """
        Верификация нормативных ссылок (authoritative режим через Norms-main):
        1. Извлечь нормы из 03_findings.json (Python)
        2. Резолв статусов через Norms-main status_index.json (Python)
        3. Записать missing_norms_queue для норм, которых нет в индексе
        4. LLM через MCP ТОЛЬКО для верификации цитат пунктов (WebSearch запрещён)
        5. Если есть устаревшие — пересмотреть замечания через Claude CLI
           (ждёт wait_before_fix, т.к. corrector тоже пишет в 03_findings.json)
        """
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "norm_verify", "running")
            import sys
            sys.path.insert(0, str(BASE_DIR))
            from norms import (
                extract_norms_from_findings,
                generate_deterministic_checks,
                format_llm_work_for_template,
                merge_llm_norm_results,
                merge_chunked_llm_results,
                format_findings_to_fix,
                validate_norm_checks,
                write_missing_norms_queue,
                verify_paragraphs_native,
                requote_norms_native,
            )

            project_dir = resolve_project_dir(job.project_id)
            output_dir = project_dir / "_output"
            findings_path = output_dir / "03_findings.json"
            norm_checks_path = output_dir / "norm_checks.json"
            norm_checks_llm_path = output_dir / "norm_checks_llm.json"
            verified_path = output_dir / "03a_norms_verified.json"

            # Загрузить project_info для инъекции дисциплины в промпт
            project_info = None
            info_path = project_dir / "project_info.json"
            if info_path.exists():
                try:
                    project_info = json.loads(info_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    pass

            # Проверка: нужен 03_findings.json
            if not findings_path.exists():
                raise RuntimeError(
                    "Файл 03_findings.json не найден. Сначала выполните основной аудит."
                )

            # ── Шаг 1: Извлечение норм ──
            job.stage = AuditStage.NORM_VERIFY
            await self._log(job, "Шаг 1: Извлечение нормативных ссылок из замечаний...")
            await self._start_heartbeat(job)

            norms_data = extract_norms_from_findings(findings_path)
            total_norms = norms_data["total_unique_norms"]

            if total_norms == 0:
                await self._log(job, "Нормативных ссылок не найдено. Верификация не требуется.", "warn")
                job.status = JobStatus.COMPLETED
                return

            await self._log(job, f"Найдено {total_norms} уникальных нормативных ссылок")

            # ── Шаг 2: Детерминированный резолв через Norms-main ──
            await self._log(
                job,
                "Шаг 2: Authoritative резолв статусов через Norms-main (status_index.json)...",
            )
            det_result = generate_deterministic_checks(norms_data, project_id=pid)

            det_meta = det_result["meta"]
            paragraphs_to_verify = det_result["paragraphs_to_verify"]
            missing_norms = det_result.get("missing_norms", [])
            unsupported_norms = det_result.get("unsupported_norms", [])

            await self._log(
                job,
                f"Norms-main: {det_meta['authoritative']} authoritative, "
                f"{det_meta['missing']} missing, {det_meta['unsupported']} unsupported; "
                f"{len(paragraphs_to_verify)} цитат для проверки через MCP",
            )
            trusted_skipped = det_meta.get("paragraphs_trusted_skipped", 0)
            legacy_ignored = det_meta.get("paragraphs_legacy_ignored", 0)
            if trusted_skipped or legacy_ignored:
                await self._log(
                    job,
                    f"Paragraph cache: {trusted_skipped} trusted (skip LLM), "
                    f"{legacy_ignored} legacy (не доверяем, пере-проверка через MCP)",
                    "info",
                )

            # Записать missing_norms_queue всегда (даже если пусто — трейс).
            try:
                report = write_missing_norms_queue(
                    output_dir, det_result, project_id=pid,
                )
                if report.get("queue_size", 0) > 0:
                    await self._log(
                        job,
                        f"Missing norms queue: {report['queue_size']} позиций "
                        f"(missing={report['missing']}, unsupported={report['unsupported']}). "
                        f"См. {output_dir}/missing_norms_queue.json",
                        "warn",
                    )
            except Exception as e:
                await self._log(job, f"Не удалось записать missing_norms_queue: {e}", "warn")

            # Накопить missing norms в глобальный vault-список
            try:
                from webapp.services.missing_norms_service import accumulate_from_queue
                queue_path = output_dir / "missing_norms_queue.json"
                new_norms = accumulate_from_queue(pid, queue_path)
                if new_norms > 0:
                    await self._log(job, f"Добавлено {new_norms} новых норм в список 'Нормы для добавления'")
            except Exception as e:
                await self._log(job, f"Не удалось обновить missing_norms_vault: {e}", "warn")

            # Записать предварительный norm_checks.json (детерминированный)
            preliminary_data = {
                "meta": det_meta,
                "checks": det_result["checks"],
                "paragraph_checks": [],
            }
            with open(norm_checks_path, "w", encoding="utf-8") as f:
                json.dump(preliminary_data, f, ensure_ascii=False, indent=2)

            # ── Шаг 3: Верификация цитат — сначала Python, fallback на Claude ──
            llm_needed = bool(paragraphs_to_verify)

            if llm_needed:
                llm_task_count = len(paragraphs_to_verify)
                await self._log(
                    job,
                    f"Шаг 3: Верификация цитат через MCP norms для "
                    f"{len(paragraphs_to_verify)} позиций. WebSearch запрещён.",
                )
                job.progress_total = llm_task_count

                # ── Native Python (fast path) ──
                _native_ok = False
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,
                        verify_paragraphs_native,
                        paragraphs_to_verify,
                        findings_path,
                        output_dir,
                    )
                    n_verified = len(paragraphs_to_verify)
                    await self._log(job, f"Native verification: {n_verified} цитат проверено (Python)")
                    _native_ok = True
                except Exception as _native_exc:
                    await self._log(
                        job,
                        f"Native verification failed ({_native_exc}), fallback → Claude chunks",
                        "warn",
                    )

                if not _native_ok:
                    # ── Проверка rate limit ──
                    can_go = await self._check_before_launch(job)
                    if not can_go:
                        raise RuntimeError("Rate limit: ожидание превышено или отменено")

                    # Chunked mode: чанкуем если позиций > PARA_CHUNK_SIZE
                    PARA_CHUNK_SIZE = 15
                    use_chunked = len(paragraphs_to_verify) > PARA_CHUNK_SIZE
                else:
                    use_chunked = False

                if use_chunked:
                    para_chunks = [
                        paragraphs_to_verify[i:i + PARA_CHUNK_SIZE]
                        for i in range(0, len(paragraphs_to_verify), PARA_CHUNK_SIZE)
                    ]

                    await self._log(
                        job,
                        f"Chunked mode: {len(para_chunks)} чанков "
                        f"({len(paragraphs_to_verify)} цитат)",
                    )

                    sem = asyncio.Semaphore(1)  # low-RAM server: sequential chunks prevent OOM

                    async def _run_chunk(idx: int, chunk_paragraphs: list):
                        async with sem:
                            fname = f"norm_checks_llm_{idx + 1}.json"
                            chunk_text = format_llm_work_for_template(
                                chunk_paragraphs, findings_path,
                            )
                            expected = output_dir / fname
                            # До запуска удалим возможный stale-файл, чтобы
                            # post-check оценивал именно текущую попытку.
                            if expected.exists():
                                expected.unlink()
                            for attempt in (1, 2):
                                exit_code, output, cli_result = await claude_runner.run_norm_verify(
                                    chunk_text, job.project_id,
                                    on_output=lambda msg: self._log(job, msg),
                                    project_info=project_info,
                                    llm_out_filename=fname,
                                )
                                self._record_cli_usage(
                                    job, cli_result,
                                    f"norm_verify_chunk_{idx + 1}"
                                    + ("" if attempt == 1 else f"_retry_{attempt}"),
                                )
                                if exit_code != 0:
                                    raise RuntimeError(
                                        f"Claude CLI norm_verify chunk {idx + 1}: exit {exit_code}"
                                    )
                                if expected.exists():
                                    return expected
                                await self._log(
                                    job,
                                    f"chunk {idx + 1}: exit=0 но {fname} не создан — "
                                    f"{'retry' if attempt == 1 else 'fail'}",
                                    "warn",
                                )
                            raise RuntimeError(
                                f"Claude CLI norm_verify chunk {idx + 1}: "
                                f"exit=0 дважды, но {expected} не создан"
                            )

                    tasks = [
                        _run_chunk(ci, chunk) for ci, chunk in enumerate(para_chunks)
                    ]
                    chunk_paths = await asyncio.gather(*tasks, return_exceptions=True)
                    valid_paths = [p for p in chunk_paths if isinstance(p, Path)]
                    errors = [e for e in chunk_paths if isinstance(e, Exception)]
                    if errors:
                        await self._log(
                            job, f"Chunked mode: {len(errors)} чанков с ошибками", "warn",
                        )
                    if not valid_paths:
                        raise RuntimeError(
                            "Chunked norm_verify: ни один чанк не дал valid файл "
                            "— paragraph verification не выполнена"
                        )
                    merge_chunked_llm_results(valid_paths, norm_checks_llm_path)
                    await self._log(
                        job, f"Chunked merge: {len(valid_paths)} чанков объединены",
                    )
                elif not _native_ok:
                    llm_work_text = format_llm_work_for_template(
                        paragraphs_to_verify, findings_path,
                    )
                    max_retries = RATE_LIMIT_MAX_RETRIES
                    # Удалим возможный stale-файл перед запуском, иначе
                    # post-check зачтёт файл с прошлого прогона как успех.
                    if norm_checks_llm_path.exists():
                        norm_checks_llm_path.unlink()
                    for attempt in range(1, max_retries + 1):
                        exit_code, output, cli_result = await claude_runner.run_norm_verify(
                            llm_work_text, job.project_id,
                            on_output=lambda msg: self._log(job, msg),
                            project_info=project_info,
                        )
                        stage_label = "norm_verify" if attempt == 1 else f"norm_verify_retry_{attempt}"
                        self._record_cli_usage(job, cli_result, stage_label)

                        if claude_runner.is_cancelled(exit_code):
                            job.status = JobStatus.CANCELLED
                            await self._log(job, "Верификация норм отменена", "warn")
                            return

                        if exit_code == 0:
                            break

                        if claude_runner.is_rate_limited(exit_code, output or "", "") or claude_runner.is_timeout(exit_code):
                            reason = "таймаут" if claude_runner.is_timeout(exit_code) else "rate limit"
                            await self._log(job, f"{reason} при верификации норм (попытка {attempt}/{max_retries}), ожидание...", "warn")
                            if attempt < max_retries:
                                can_continue = await self._wait_for_rate_limit(job, f"{reason} при верификации норм", cli_output=output or "")
                                if not can_continue:
                                    raise RuntimeError(f"Верификация норм: ожидание {reason} превышено или отменено")
                                continue
                            else:
                                raise RuntimeError(f"Верификация норм: {max_retries} попыток исчерпано ({reason})")

                        await self._log(job, f"Ошибка верификации (код {exit_code})", "error")
                        raise RuntimeError(f"Claude CLI norm_verify: exit code {exit_code}")

                    # Post-check: exit=0 НЕ считается успехом, если файла нет.
                    # Один контролируемый retry, потом явная ошибка.
                    if not norm_checks_llm_path.exists():
                        await self._log(
                            job,
                            f"norm_verify: exit=0, но {norm_checks_llm_path.name} "
                            f"не создан. Запускаю контролируемый retry...",
                            "warn",
                        )
                        exit_code, output, cli_result = await claude_runner.run_norm_verify(
                            llm_work_text, job.project_id,
                            on_output=lambda msg: self._log(job, msg),
                            project_info=project_info,
                        )
                        self._record_cli_usage(job, cli_result, "norm_verify_missing_file_retry")
                        if claude_runner.is_cancelled(exit_code):
                            job.status = JobStatus.CANCELLED
                            return
                        if exit_code != 0 or not norm_checks_llm_path.exists():
                            raise RuntimeError(
                                f"norm_verify: paragraph verification не выполнена — "
                                f"{norm_checks_llm_path.name} не создан (retry exit={exit_code})"
                            )
                        await self._log(
                            job, "norm_verify retry: файл успешно создан", "info",
                        )
                # конец if not _native_ok

                # ── Шаг 3b: Слияние paragraph_checks (статусы не меняются) ──
                # Post-check выше гарантирует, что если мы сюда дошли при
                # llm_needed=True — файл на месте. Отдельной ветки "silent
                # success без файла" больше быть не может.
                if not norm_checks_llm_path.exists():
                    raise RuntimeError(
                        f"norm_verify invariant: {norm_checks_llm_path} "
                        f"должен был существовать на этом шаге"
                    )
                await self._log(
                    job,
                    "Слияние paragraph_checks (статусы norm_checks остаются authoritative)...",
                )
                merge_stats = merge_llm_norm_results(norm_checks_path, norm_checks_llm_path)
                await self._log(
                    job,
                    f"Слияние: {merge_stats['paragraph_checks']} цитат получено, "
                    f"{merge_stats.get('ignored_llm_status_attempts', 0)} попыток "
                    f"изменить статус отброшено. Paragraph cache: "
                    f"+{merge_stats.get('paragraph_cache_added', 0)} новых, "
                    f"{merge_stats.get('paragraph_cache_updated', 0)} обновлено.",
                )
            else:
                await self._log(
                    job,
                    "Нет цитат для верификации через MCP — ограничиваемся authoritative статусами",
                    "info",
                )

            # Проверяем что файл существует
            if not norm_checks_path.exists():
                await self._log(job, "norm_checks.json не создан", "warn")
                job.status = JobStatus.COMPLETED
                return

            # Читаем результаты
            with open(norm_checks_path, "r", encoding="utf-8") as f:
                checks_data = json.load(f)

            # ── Пост-валидация (программный контроль) ──
            validation = validate_norm_checks(norm_checks_path)
            if validation.get("fixes_applied"):
                await self._log(
                    job,
                    f"Пост-валидация: {len(validation['fixes_applied'])} исправлений: "
                    + "; ".join(validation["fixes_applied"][:3]),
                    "warn",
                )
                with open(norm_checks_path, "r", encoding="utf-8") as f:
                    checks_data = json.load(f)
            if validation.get("violations"):
                await self._log(
                    job,
                    f"Пост-валидация: {len(validation['violations'])} нарушений: "
                    + "; ".join(validation["violations"][:3]),
                    "warn",
                )

            checks = checks_data.get("checks", [])
            needs_fix = [c for c in checks if c.get("needs_revision", False)]

            results = checks_data.get("meta", {}).get("results", {})
            await self._log(
                job,
                f"Результат: {results.get('active', 0)} актуальных, "
                f"{results.get('outdated_edition', 0)} устаревших, "
                f"{results.get('replaced', 0)} заменённых, "
                f"{results.get('cancelled', 0)} отменённых",
                "info",
            )

            # ── Шаг 3: Пересмотр замечаний (если нужен) ──
            if needs_fix:
                # Ждём завершения Corrector — оба пишут в 03_findings.json
                if wait_before_fix is not None and not wait_before_fix.is_set():
                    await self._log(job, "Ожидание завершения Corrector перед пересмотром норм...")
                    await wait_before_fix.wait()
                    if job.status == JobStatus.CANCELLED:
                        return

                job.stage = AuditStage.NORM_FIX
                await self._log(
                    job,
                    f"Шаг 3: Пересмотр {len(needs_fix)} замечаний с устаревшими нормами..."
                )

                findings_to_fix_text = format_findings_to_fix(norm_checks_path, findings_path)

                # Бэкап findings ДО norm_fix (Python, надёжно)
                import shutil
                pre_norm_path = output_dir / "03_findings_pre_norm.json"
                if findings_path.exists():
                    shutil.copy2(findings_path, pre_norm_path)

                # ── Проверка rate limit перед пересмотром замечаний ──
                can_go = await self._check_before_launch(job)
                if not can_go:
                    raise RuntimeError("Rate limit: ожидание превышено или отменено")

                exit_code, output, cli_result = await claude_runner.run_norm_fix(
                    findings_to_fix_text, job.project_id,
                    on_output=lambda msg: self._log(job, msg),
                    project_info=project_info,
                )
                self._record_cli_usage(job, cli_result, "norm_fix")

                if claude_runner.is_cancelled(exit_code):
                    job.status = JobStatus.CANCELLED
                    await self._log(job, "Пересмотр замечаний отменён", "warn")
                    return

                # LLM пишет прямо в 03_findings.json — Python создаёт 03a как снэпшот
                if exit_code == 0 and findings_path.exists():
                    shutil.copy2(findings_path, verified_path)
                    size_kb = round(verified_path.stat().st_size / 1024, 1)
                    await self._log(job, f"03a_norms_verified.json создан ({size_kb} KB)")
                elif exit_code != 0:
                    await self._log(job, f"Norm fix: код {exit_code}", "warn")
                    # Восстановить бэкап при ошибке
                    if pre_norm_path.exists():
                        shutil.copy2(pre_norm_path, findings_path)
                        await self._log(job, "Восстановлен 03_findings.json из бэкапа", "warn")
            else:
                await self._log(job, "Все нормы актуальны — пересмотр не требуется", "info")

            # ── Шаг 4: Локальный norms_db.json больше не authoritative ──
            # Source of truth — Norms-main. Оставляем no-op для трассировки.
            await self._update_norms_db(job)

            # ── Шаг 5: Обогащение findings.norm_quote из paragraph_checks ──
            enriched = self._enrich_norm_quotes_from_checks(output_dir)
            if enriched > 0:
                await self._log(job, f"norm_quote обогащён из paragraph_checks: {enriched} замечаний")

            # ── Шаг 6: Авто-исправление неверных номеров пунктов норм ──
            # paragraph_checks с paragraph_verified=False означает: цитата правильная,
            # но номер пункта в ссылке ошибочен. Исправляем по mismatch_details.
            fixed_paras = self._fix_paragraph_refs(output_dir)
            if fixed_paras > 0:
                await self._log(job, f"Номера пунктов норм исправлены: {fixed_paras} замечаний")

            # ── Шаг 7: уточнение оставшихся цитат через semantic_search (Python) ──
            remaining_flags = self._count_manual_check_flags(output_dir)
            if remaining_flags > 0:
                await self._log(job,
                    f"Шаг 7: уточнение {remaining_flags} цитат норм (Python semantic search)")
                try:
                    loop = asyncio.get_event_loop()
                    rq_result = await loop.run_in_executor(
                        None, requote_norms_native, output_dir,
                    )
                    resolved = rq_result.get("resolved", 0)
                    remaining_after = rq_result.get("remaining", remaining_flags)
                    await self._log(job,
                        f"norm_requote завершён: исправлено {resolved}/{remaining_flags}, "
                        f"осталось {remaining_after} [ручная сверка]")
                except Exception as _rq_exc:
                    await self._log(job,
                        f"Native requote failed ({_rq_exc}), fallback → Claude CLI", "warn")
                    exit_code, _, cli_result = await claude_runner.run_norm_requote(
                        pid, on_output=lambda msg: self._log(job, msg),
                        project_info=project_info,
                    )
                    self._record_cli_usage(job, cli_result, "norm_requote")
                    if exit_code != 0:
                        await self._log(job, f"norm_requote: код {exit_code} (не критично)", "warn")
                    remaining_after = self._count_manual_check_flags(output_dir)
                    resolved = remaining_flags - remaining_after
                    await self._log(job,
                        f"norm_requote завершён: исправлено {resolved}/{remaining_flags}, "
                        f"осталось {remaining_after} [ручная сверка]")

            job.status = JobStatus.COMPLETED
            await self._log(job, "Верификация нормативных ссылок завершена", "info")
            self._refresh_finding_quality(pid)
            if verified_path.exists():
                self._refresh_finding_quality(pid, "03a_norms_verified.json")
            self._update_pipeline_log(pid, "norm_verify", "done", message="OK")

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
        """Обогатить findings из norm_checks.json (полный norm contract).

        Обогащает:
        - norm_verification: {status, edition_status, verified_via, ...}
        - norm_status / norm_quote_status: classification
        - norm_quote: actual_quote если найдена и лучше текущей

        Returns: количество обогащённых findings.
        """
        findings_path = output_dir / "03_findings.json"
        norm_checks_path = output_dir / "norm_checks.json"
        if not findings_path.exists() or not norm_checks_path.exists():
            return 0

        try:
            fd = json.loads(findings_path.read_text(encoding="utf-8"))
            nc = json.loads(norm_checks_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return 0

        findings = fd.get("findings", [])
        if not findings:
            return 0

        try:
            from norms import enrich_findings_from_norm_checks
            stats = enrich_findings_from_norm_checks(findings, nc)
            enriched = stats.get("enriched_verification", 0) + stats.get("enriched_quote", 0)
        except ImportError:
            # Fallback: старая логика для backward compat
            paragraph_checks = nc.get("paragraph_checks", [])
            verified_quotes = {}
            for pc in paragraph_checks:
                if pc.get("paragraph_verified") and pc.get("actual_quote"):
                    fid = pc.get("finding_id", "")
                    if fid:
                        verified_quotes[fid] = pc["actual_quote"]

            enriched = 0
            for finding in findings:
                fid = finding.get("id", "")
                if fid in verified_quotes and not finding.get("norm_quote"):
                    finding["norm_quote"] = verified_quotes[fid]
                    enriched += 1

        if enriched > 0:
            findings_path.write_text(
                json.dumps(fd, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        return enriched

    @staticmethod
    def _fix_paragraph_refs(output_dir: Path) -> int:
        """Исправить неверные номера пунктов норм по данным paragraph_checks.

        Для каждого finding с paragraph_verified=False: извлекаем правильный
        пункт из mismatch_details (regex) и обновляем поле norm. Если правильный
        пункт не определить однозначно — добавляем пометку [ручная сверка].

        Returns: количество исправленных findings.
        """
        import re as _re
        import shutil as _shutil

        findings_path = output_dir / "03_findings.json"
        norm_checks_path = output_dir / "norm_checks.json"
        if not findings_path.exists() or not norm_checks_path.exists():
            return 0

        try:
            fd = json.loads(findings_path.read_text(encoding="utf-8"))
            nc = json.loads(norm_checks_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return 0

        para_checks = nc.get("paragraph_checks", [])
        if not para_checks:
            return 0

        _p_re = _re.compile(r"п\.\s*([\d]+(?:\.[\d]+)+)")

        # Группируем по finding_id — только unverified
        by_fid: dict[str, list[dict]] = {}
        for pc in para_checks:
            if not pc.get("paragraph_verified", True):
                by_fid.setdefault(pc.get("finding_id", ""), []).append(pc)
        if not by_fid:
            return 0

        findings = fd.get("findings", [])
        fmap = {f.get("id", ""): f for f in findings}
        fixed = 0

        for fid, checks in by_fid.items():
            finding = fmap.get(fid)
            if not finding:
                continue

            norm_field = finding.get("norm", "") or ""
            desc = finding.get("description", "") or ""
            made_change = False

            for pc in checks:
                norm_str = pc.get("norm") or ""
                mismatch = pc.get("mismatch_details") or ""
                old_paras = _p_re.findall(norm_str)
                if not old_paras:
                    continue
                old_p = old_paras[0]

                # Ищем правильный пункт в mismatch_details (исключая старый)
                all_in_mismatch = _p_re.findall(mismatch)
                new_candidates = [p for p in all_in_mismatch if p != old_p]

                if new_candidates:
                    new_p = new_candidates[0]
                    new_norm = _re.sub(r"п\.\s*" + _re.escape(old_p), f"п. {new_p}", norm_field)
                    new_desc = _re.sub(r"п\.\s*" + _re.escape(old_p), f"п. {new_p}", desc)
                    if new_norm != norm_field or new_desc != desc:
                        norm_field = new_norm
                        desc = new_desc
                        made_change = True
                else:
                    # Не определить пункт → ставим пометку если её нет
                    flag = f"[Пункт нормы {norm_str} требует ручной сверки] "
                    if flag not in desc:
                        desc = flag + desc
                        made_change = True

            if made_change:
                finding["norm"] = norm_field
                finding["description"] = desc
                fixed += 1

        if fixed > 0:
            if "meta" in fd and isinstance(fd["meta"], dict):
                fd["meta"]["paragraph_fix_applied"] = True
                fd["meta"]["paragraph_fix_stats"] = {"fixed_paragraph": fixed}
            findings_path.write_text(
                json.dumps(fd, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        return fixed

    @staticmethod
    def _count_manual_check_flags(output_dir: Path) -> int:
        """Подсчитать количество findings с флагом [Пункт нормы ... ручной сверки]."""
        findings_path = output_dir / "03_findings.json"
        if not findings_path.exists():
            return 0
        try:
            fd = json.loads(findings_path.read_text(encoding="utf-8"))
            return sum(
                1 for f in fd.get("findings", [])
                if "[Пункт нормы" in (f.get("description") or "")
            )
        except (json.JSONDecodeError, OSError):
            return 0

    # ─── Запуск интеллектуального аудита (smart) ───
    async def start_smart_audit(self, project_id: str) -> AuditJob:
        """Интеллектуальный аудит: текст → триаж → выборочная нарезка → анализ."""
        return await self._enqueue_single(project_id, action="smart")

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
            output_dir = resolve_project_dir(pid) / "_output"
            info_path = resolve_project_dir(pid) / "project_info.json"

            # ═══ Проверка MD-файла (обязательный источник текста) ═══
            project_dir = resolve_project_dir(pid)
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

            exit_code, _, stderr = await self._run_script(
                pid,
                str(PROCESS_PROJECT_SCRIPT),
                [_project_path(pid)],
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
            self._update_pipeline_log(pid, "text_analysis", "running")
            print(f"[{pid}:smart] ═══ ЭТАП 2: Триаж страниц ═══")
            await self._log(job, "═══ ЭТАП 2: Триаж страниц (Claude определяет приоритеты) ═══")

            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            # ── Проверка rate limit перед триажом ──
            can_go = await self._check_before_launch(job)
            if not can_go:
                raise RuntimeError("Rate limit: ожидание превышено или отменено")

            exit_code, output, cli_result = await claude_runner.run_triage(
                project_info, pid,
                on_output=lambda msg: self._log(job, msg),
            )
            self._record_cli_usage(job, cli_result, "triage")
            if claude_runner.is_cancelled(exit_code):
                job.status = JobStatus.CANCELLED
                await self._log(job, "Триаж отменён", "warn")
                return
            if claude_runner.is_rate_limited(exit_code, output or "", ""):
                # Rate limit на триаже — ждём и retry
                await self._log(job, "Rate limit при триаже, ожидание...", "warn")
                can_continue = await self._wait_for_rate_limit(job, "rate limit при триаже", cli_output=output or "")
                if not can_continue:
                    raise RuntimeError("Rate limit: ожидание превышено или отменено")
                exit_code, output, cli_result = await claude_runner.run_triage(
                    project_info, pid,
                    on_output=lambda msg: self._log(job, msg),
                )
                self._record_cli_usage(job, cli_result, "triage_retry")
            if exit_code != 0:
                self._update_pipeline_log(pid, "text_analysis", "error",
                                           error=f"Триаж: код {exit_code}")
                raise RuntimeError(f"Триаж: код {exit_code}, {output[:500] if output else 'N/A'}")

            # Прочитать результат триажа
            triage_file = output_dir / "01_text_analysis.json"
            if not triage_file.exists():
                raise RuntimeError("01_text_analysis.json не создан после триажа")

            with open(triage_file, "r", encoding="utf-8") as f:
                triage_data = json.load(f)

            page_triage = triage_data.get("page_triage", [])
            priority_pages = [
                pt["page"] for pt in page_triage
                if pt.get("priority") in ("HIGH", "MEDIUM")
            ]
            self._update_pipeline_log(pid, "text_analysis", "done",
                                       message=f"{len(priority_pages)} приоритетных из {len(page_triage)}")
            print(f"[{pid}:smart] Триаж: {len(priority_pages)} приоритетных страниц из {len(page_triage)}")
            await self._log(job, f"Триаж завершён: {len(priority_pages)} приоритетных страниц ({priority_pages})")

            if job.status == JobStatus.CANCELLED:
                return

            # ═══ ЭТАП 3: Выборочная нарезка тайлов ═══
            if priority_pages:
                self._reset_job_progress(job)
                job.stage = AuditStage.PREPARE
                job.status = JobStatus.RUNNING
                pages_str = ",".join(str(p) for p in priority_pages)
                print(f"[{pid}:smart] ═══ ЭТАП 3: Нарезка тайлов (стр. {pages_str}) ═══")
                await self._log(job, f"═══ ЭТАП 3: Нарезка тайлов (стр. {pages_str}) ═══")

                exit_code, _, stderr = await self._run_script(
                    pid,
                    str(PROCESS_PROJECT_SCRIPT),
                    [_project_path(pid), "--pages", pages_str, "--quality", DEFAULT_TILE_QUALITY],
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code != 0:
                    raise RuntimeError(f"Нарезка тайлов: {stderr}")
                print(f"[{pid}:smart] ЭТАП 3 OK")
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

                                # Донарезка тайлов
                                exit_code, _, stderr = await self._run_script(
                                    pid,
                                    str(PROCESS_PROJECT_SCRIPT),
                                    [_project_path(pid), "--pages", pages_str, "--quality", DEFAULT_TILE_QUALITY],
                                    on_output=lambda msg: self._log(job, msg),
                                )
                                if exit_code != 0:
                                    await self._log(job, f"Донарезка: {stderr}", "warn")
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
            findings_path = resolve_project_dir(pid) / "_output" / "03_findings.json"
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
            self._update_pipeline_log(pid, "excel", "running")
            print(f"[{pid}:smart] ═══ ЭТАП 7: Excel ═══")
            await self._log(job, "═══ ЭТАП 7: Генерация Excel ═══")
            project_path = str(resolve_project_dir(pid))
            exit_code, _xls_out, _xls_err = await self._run_script(
                pid,
                str(GENERATE_EXCEL_SCRIPT),
                args=[project_path],
                env_overrides={"AUDIT_NO_OPEN": "1"},
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code == 0:
                self._update_pipeline_log(pid, "excel", "done", message="OK")
            else:
                self._update_pipeline_log(pid, "excel", "error",
                                           error=_extract_error_detail(exit_code, (_xls_err or "") + "\n" + (_xls_out or "")))

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
    async def start_audit(self, project_id: str) -> AuditJob:
        """Аудит: кроп блоков → текстовый анализ → ВСЕ блоки → свод.

        Single-start — кладёт задачу в общую очередь. Реальный запуск
        случится, когда worker возьмёт её. kill зомби, сброс audit_log/usage
        живут в `_dispatch_action`, чтобы не сбрасывать данные на ещё не
        стартовавший проект.
        """
        return await self._enqueue_single(project_id, action="full")

    async def start_flash_pro_triage(
        self,
        project_id: str,
        *,
        max_pro_cost_usd: float = 8.0,
        include_simple_findings: bool = False,
    ) -> AuditJob:
        """Run explicit stage-02 Flash full + Pro selected single-block triage."""
        return await self._enqueue_single(
            project_id,
            action="flash_pro_triage",
            extra_params={
                "max_pro_cost_usd": max_pro_cost_usd,
                "include_simple_findings": include_simple_findings,
            },
        )

    async def _run_flash_pro_triage(
        self,
        job: AuditJob,
        *,
        max_pro_cost_usd: float,
        include_simple_findings: bool,
    ) -> None:
        pid = job.project_id
        await self._start_heartbeat(job)
        try:
            ok = await self._execute_flash_pro_triage_stage(
                job,
                max_pro_cost_usd=max_pro_cost_usd,
                include_simple_findings=include_simple_findings,
            )
            job.status = JobStatus.COMPLETED if ok else JobStatus.FAILED
        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            self._update_pipeline_log(pid, "flash_pro_triage", "error", error="Отменено")
            self._update_pipeline_log(pid, "block_analysis", "error", error="Отменено")
            raise
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            self._update_pipeline_log(pid, "flash_pro_triage", "error", error=str(e))
            self._update_pipeline_log(pid, "block_analysis", "error", error=str(e))
            await self._log(job, f"Исключение Flash + Pro Triage: {e}", "error")
        finally:
            job.completed_at = datetime.now().isoformat()
            self._cleanup(pid)

    async def _execute_flash_pro_triage_stage(
        self,
        job: AuditJob,
        *,
        max_pro_cost_usd: float,
        include_simple_findings: bool,
    ) -> bool:
        """Execute only the stage-02 Flash+Pro triage script and write 02 output."""
        pid = job.project_id
        project_dir = resolve_project_dir(pid)
        script = BASE_DIR / "scripts" / "run_stage02_flash_pro_triage.py"

        args = [
            "--pdf", project_dir.name,
            "--project-dir", str(project_dir),
            "--parallelism-flash", "3",
            "--parallelism-pro", "2",
            "--max-pro-cost-usd", str(max_pro_cost_usd),
            "--write-stage-output",
        ]
        if include_simple_findings:
            args.append("--include-simple-findings")

        self._reset_job_progress(job)
        job.stage = AuditStage.FLASH_PRO_TRIAGE
        job.status = JobStatus.RUNNING
        job.progress_total = 2
        self._update_pipeline_log(
            pid,
            "flash_pro_triage",
            "running",
            message="Flash full single-block + Pro selected single-block",
        )
        self._update_pipeline_log(
            pid,
            "block_analysis",
            "running",
            message="Flash + Pro Triage",
        )
        await self._log(
            job,
            "═══ Stage 02: Flash + Pro Triage ═══\n"
            "Flash анализирует все блоки single-block; Pro проверяет только выбранные risky/findings блоки.",
        )
        await self._progress(job, 0, 2)

        exit_code, stdout, stderr = await self._run_script(
            pid,
            str(script),
            args,
            on_output=lambda msg: self._log(job, msg),
            timeout=None,
        )

        if exit_code != 0:
            detail = _extract_error_detail(exit_code, "\n".join([stdout, stderr]))
            self._update_pipeline_log(pid, "flash_pro_triage", "error", error=detail)
            self._update_pipeline_log(pid, "block_analysis", "error", error=detail)
            job.error_message = detail
            await self._log(job, f"Flash + Pro Triage завершился с ошибкой: {detail}", "error")
            return False

        job.progress_current = 2
        await self._progress(job, 2, 2)
        self._record_flash_pro_triage_usage(job, project_dir)
        self._update_pipeline_log(
            pid,
            "flash_pro_triage",
            "done",
            message="Flash + Pro Triage complete",
        )
        self._update_pipeline_log(
            pid,
            "block_analysis",
            "done",
            message="02_blocks_analysis.json создан через Flash + Pro Triage",
        )
        await self._log(job, "✓ Flash + Pro Triage завершён. 02_blocks_analysis.json обновлён.")
        return True

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
        from blocks import (
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

        for retry_iter in range(1, max_retry + 1):
            unreadable = find_unreadable_blocks(_project_path(pid))
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
                promote_result = promote_to_full(_project_path(pid), block_ids)
                if promote_result.get("promoted", 0) == 0:
                    await self._log(job, "Block retry: нет full-версий для промоута")
                    break
            else:
                await self._log(job, f"Block retry (итерация {retry_iter}): {len(block_ids)} нечитаемых блоков → перекачка ×2")
                self._update_pipeline_log(pid, "block_retry", "running",
                                          message=f"Итерация {retry_iter}: {len(block_ids)} блоков")
                recrop_result = recrop_blocks(_project_path(pid), block_ids, scale_multiplier=2.0)
                if recrop_result.get("recropped", 0) == 0:
                    await self._log(job, "Block retry: все блоки уже на максимальном разрешении, стоп")
                    break

            exit_code, _, _ = await self._run_script(
                pid, str(BLOCKS_SCRIPT),
                # --solo: 1 блок = 1 пакет, модель фокусируется на одной картинке
                # (retry именно по ней и шёл, контекст других блоков уже есть в 02_blocks_analysis.json)
                ["batches", _project_path(pid), "--block-ids", ",".join(block_ids), "--solo"],
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code != 0:
                await self._log(job, "Block retry: ошибка создания пакетов", "warn")
                break

            batches_file = output_dir / "block_batches.json"
            if batches_file.exists():
                with open(batches_file, "r", encoding="utf-8") as f:
                    retry_batches_data = json.load(f)
                retry_batches = retry_batches_data.get("batches", [])
                retry_total = len(retry_batches)

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
                        await self._log(job, f"Block retry batch {batch_id}: ошибка (код {exit_code})", "warn")

            exit_code, _, _ = await self._run_script(
                pid, str(BLOCKS_SCRIPT),
                ["merge", _project_path(pid)],
                on_output=lambda msg: self._log(job, msg),
            )
            await self._log(job, f"Block retry итерация {retry_iter}: merge завершён")

        final_unreadable = find_unreadable_blocks(_project_path(pid))
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
        1. blocks.py crop → _output/blocks/
        2. Claude: text_analysis → 01_text_analysis.json
        3. blocks.py batches → block_batches.json
        4. Claude: block_batch (параллельно) → block_batch_NNN.json
        5. blocks.py merge → 02_blocks_analysis.json
        6. Claude: findings_merge → 03_findings.json
        7. norm_verify
        8. Excel
        """
        start_time = datetime.now()
        pid = job.project_id
        try:
            output_dir = resolve_project_dir(pid) / "_output"
            info_path = resolve_project_dir(pid) / "project_info.json"

            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            # ═══ Проверка MD-файла (обязательный источник текста) ═══
            md_file = project_info.get("md_file")
            if not md_file:
                # Проверим наличие *_document.md в папке проекта
                project_dir = resolve_project_dir(pid)
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

            # ═══ ЭТАП 1: Кроп image-блоков ═══
            job.stage = AuditStage.CROP_BLOCKS
            blocks_index = output_dir / "blocks" / "index.json"
            needs_recrop = blocks_index.exists() and not _existing_crop_matches_mode(blocks_index)
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
                    await self._log(job, "Существующий crop не совпадает с режимом block_batch — перекропаем с --force")

                crop_args = _build_crop_args(_project_path(pid), force=needs_recrop)
                exit_code, _, stderr = await self._run_script(
                    pid,
                    str(BLOCKS_SCRIPT),
                    crop_args,
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code != 0:
                    self._update_pipeline_log(pid, "crop_blocks", "error",
                                               error=stderr or f"Exit code: {exit_code}")
                    raise RuntimeError(f"Кроп блоков: {stderr}")
                crop_mode = {"qwen": "QWEN 300 DPI", "claude": "Claude 100 DPI", "compact": "compact"}[_block_batch_crop_mode()]
                self._update_pipeline_log(pid, "crop_blocks", "done", message=f"OK ({crop_mode})")
                print(f"[{pid}] ЭТАП 1 OK (compact)")

            # Построить document_graph v2 (Python, без LLM)
            await self._build_document_graph_v2(job)

            if job.status == JobStatus.CANCELLED:
                return

            # ЭТАП 00: Qwen-обогащение MD (всегда выполняется, идемпотентно)
            await self._run_qwen_enrichment_stage(job)

            if job.status == JobStatus.CANCELLED:
                return

            using_flash_pro_pair = get_stage_model("block_batch") == FLASH_PRO_TRIAGE_MODEL

            # ═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══
            files_to_clean = [
                "01_text_analysis.json",
                "03_findings.json", "03_findings_review.json", "03_findings_pre_review.json",
                "block_batch_*.json", "block_batches.json",
            ]
            if not using_flash_pro_pair:
                files_to_clean.append("02_blocks_analysis.json")
            self._clean_stage_files(pid, files_to_clean)
            self._reset_job_progress(job)
            job.stage = AuditStage.TEXT_ANALYSIS
            job.status = JobStatus.RUNNING
            self._update_pipeline_log(pid, "text_analysis", "running")
            print(f"[{pid}] ═══ ЭТАП 2: Текстовый анализ MD ═══")
            await self._log(job, "═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══")
            await self._start_heartbeat(job)

            can_go = await self._check_before_launch(job)
            if not can_go:
                raise RuntimeError("Rate limit: ожидание превышено или отменено")

            exit_code, output, cli_result = await claude_runner.run_text_analysis(
                project_info, pid,
                on_output=lambda msg: self._log(job, msg),
            )
            self._record_cli_usage(job, cli_result, "text_analysis")

            if claude_runner.is_cancelled(exit_code):
                job.status = JobStatus.CANCELLED
                return
            if claude_runner.is_rate_limited(exit_code, output or "", ""):
                await self._log(job, "Rate limit при текстовом анализе, ожидание...", "warn")
                can_continue = await self._wait_for_rate_limit(
                    job, "rate limit при текстовом анализе", cli_output=output or ""
                )
                if not can_continue:
                    raise RuntimeError("Rate limit: ожидание превышено или отменено")
                exit_code, output, cli_result = await claude_runner.run_text_analysis(
                    project_info, pid,
                    on_output=lambda msg: self._log(job, msg),
                )
                self._record_cli_usage(job, cli_result, "text_analysis_retry")
            if exit_code != 0:
                self._update_pipeline_log(pid, "text_analysis", "error",
                                           error=_extract_error_detail(exit_code, output))
                raise RuntimeError(f"Текстовый анализ: код {exit_code}")

            text_analysis_path = output_dir / "01_text_analysis.json"
            if not text_analysis_path.exists():
                raise RuntimeError("01_text_analysis.json не создан")

            self._update_pipeline_log(pid, "text_analysis", "done", message="OK")
            print(f"[{pid}] ЭТАП 2 OK")

            if job.status == JobStatus.CANCELLED:
                return

            if using_flash_pro_pair:
                ok = await self._execute_flash_pro_triage_stage(
                    job,
                    max_pro_cost_usd=8.0,
                    include_simple_findings=False,
                )
                if not ok:
                    job.status = JobStatus.FAILED
                    return
            elif get_stage_batch_mode("block_batch") == "findings_only_qwen_pair":
                # findings_only_qwen_pair: single-block GPT-5.4 + qwen-enrichment.
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
                gen_args = [_project_path(pid)]
                await self._log(job, "Анализ ВСЕХ image-блоков")

                print(f"[{pid}] ═══ ЭТАП 3: Генерация пакетов блоков ═══")
                await self._log(job, "═══ ЭТАП 3: Генерация пакетов блоков ═══")

                exit_code, _, stderr = await self._run_script(
                    pid,
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

                batches = batches_data.get("batches", [])
                batches, single_block_mode = _expand_block_batches_for_local_model(batches)
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
                        await self._log(job, "Локальный QWEN: stage 02 переведён в single-block режим")

                    semaphore = asyncio.Semaphore(parallel)
                    completed_count = 0
                    error_count = 0
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
                    await asyncio.gather(*tasks, return_exceptions=True)

                    if error_count >= total_batches:
                        job.status = JobStatus.FAILED
                        job.error_message = f"Все {total_batches} пакетов с ошибкой"
                        self._update_pipeline_log(pid, "block_analysis", "error",
                                                   error=f"Все {total_batches} пакетов с ошибкой")
                        return

                    # Слияние block_batch_*.json → 02_blocks_analysis.json
                    await self._log(job, "Слияние результатов анализа блоков...")
                    exit_code, _, stderr = await self._run_script(
                        pid,
                        str(BLOCKS_SCRIPT),
                        ["merge", _project_path(pid)],
                        on_output=lambda msg: self._log(job, msg),
                    )
                    if exit_code == 0:
                        await self._log(job, "02_blocks_analysis.json создан", "info")
                    else:
                        await self._log(job, f"Ошибка слияния: {stderr}", "error")

                    if error_count > 0:
                        self._update_pipeline_log(pid, "block_analysis", "error",
                                                   error=f"{error_count} из {total_batches} пакетов с ошибками")
                    else:
                        self._update_pipeline_log(pid, "block_analysis", "done",
                                                   message=f"Все {total_batches} пакетов OK")

            if job.status == JobStatus.CANCELLED:
                return

            # Re-register
            self.active_jobs[pid] = job
            self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАП 5b: Block Retry — перекачка нечитаемых блоков ═══
            if using_flash_pro_pair:
                self._update_pipeline_log(
                    pid,
                    "block_retry",
                    "skipped",
                    message="Flash+Pro triage: без recrop/rebuild",
                )
                await self._log(job, "Block retry пропущен для Flash+Pro triage (без recrop/rebuild).")
            else:
                await self._run_block_retry(job, pid, project_info, output_dir)

            if job.status == JobStatus.CANCELLED:
                return

            # ═══ ЭТАП 6: Свод замечаний ═══
            self._reset_job_progress(job)
            job.stage = AuditStage.FINDINGS_MERGE
            job.status = JobStatus.RUNNING

            findings_model = get_stage_model("findings_merge")
            self._update_pipeline_log(pid, "findings_merge", "running")
            print(f"[{pid}] ═══ ЭТАП 6: Свод замечаний ═══")
            await self._log(job, f"═══ ЭТАП 6: Свод замечаний ({findings_model}) ═══")

            can_go = await self._check_before_launch(job)
            if not can_go:
                raise RuntimeError("Rate limit: ожидание превышено или отменено")

            exit_code, output, cli_result = await claude_runner.run_findings_merge(
                project_info, pid,
                on_output=lambda msg: self._log(job, msg),
            )
            self._record_cli_usage(job, cli_result, "findings_merge")

            if claude_runner.is_cancelled(exit_code):
                job.status = JobStatus.CANCELLED
                return
            if claude_runner.is_rate_limited(exit_code, output or "", ""):
                await self._log(job, "Rate limit при своде замечаний, ожидание...", "warn")
                can_continue = await self._wait_for_rate_limit(
                    job, "rate limit при своде замечаний", cli_output=output or ""
                )
                if can_continue:
                    exit_code, output, cli_result = await claude_runner.run_findings_merge(
                        project_info, pid,
                        on_output=lambda msg: self._log(job, msg),
                    )
                    self._record_cli_usage(job, cli_result, "findings_merge_retry")

            if exit_code != 0:
                self._update_pipeline_log(pid, "findings_merge", "error",
                                           error=_extract_error_detail(exit_code, output))
                await self._log(job, f"Свод замечаний: код {exit_code}", "error")
            else:
                findings_path = resolve_project_dir(pid) / "_output" / "03_findings.json"
                if not findings_path.exists():
                    self._update_pipeline_log(pid, "findings_merge", "error", error="03_findings.json не создан")
                    await self._log(job, "03_findings.json не создан — свод замечаний считается ошибкой", "error")
                    raise RuntimeError("03_findings.json не создан")

                # Валидация JSON после findings_merge
                is_valid, repair_msg = self._validate_and_repair_json(findings_path)
                if not is_valid:
                    self._update_pipeline_log(
                        pid, "findings_merge", "error",
                        error=f"03_findings.json невалиден: {repair_msg}",
                    )
                    raise RuntimeError(f"03_findings.json невалиден: {repair_msg}")
                if "Repaired" in repair_msg:
                    await self._log(job, f"03_findings.json починен: {repair_msg}", "warn")

                self._update_pipeline_log(pid, "findings_merge", "done", message="OK")

                # Post-merge: backfill text-evidence из compact/graph
                self._backfill_text_evidence_in_findings(pid)

                # Объединение похожих замечаний
                merge_result = self._merge_similar_findings(pid)
                if merge_result and merge_result.get("merged_groups", 0) > 0:
                    await self._log(
                        job,
                        f"Объединено похожих замечаний: {merge_result['before']} → {merge_result['after']} "
                        f"({merge_result['merged_groups']} групп)",
                    )

                self._refresh_finding_quality(pid)

                # Восстановление highlight_regions из 02_blocks_analysis
                self._backfill_highlight_regions(pid)

                # «Размышление модели»: стрим найденных замечаний в live-лог
                await self._stream_findings_events(job, "merge")

            if job.status == JobStatus.CANCELLED:
                return

            # Re-register
            self.active_jobs[pid] = job
            self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАПЫ 6.5-7-OPT: Параллельный запуск после findings_merge ═══
            # Critic+Corrector, Norm verify и Optimization — независимы.
            # Optimization_critic ждёт corrector (нужны финальные findings).
            findings_path = resolve_project_dir(pid) / "_output" / "03_findings.json"
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
            self._update_pipeline_log(pid, "excel", "running")
            print(f"[{pid}] ═══ ЭТАП 8: Excel ═══")
            await self._log(job, "═══ ЭТАП 8: Генерация Excel ═══")
            project_path = str(resolve_project_dir(pid))
            exit_code, _xls_out, _xls_err = await self._run_script(
                pid,
                str(GENERATE_EXCEL_SCRIPT),
                args=[project_path],
                env_overrides={"AUDIT_NO_OPEN": "1"},
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code == 0:
                self._update_pipeline_log(pid, "excel", "done", message="OK")
            else:
                self._update_pipeline_log(pid, "excel", "error",
                                           error=_extract_error_detail(exit_code, (_xls_err or "") + "\n" + (_xls_out or "")))

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

    async def start_batch(self, project_ids: list[str], action: str) -> BatchQueueStatus:
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
            new_items = [
                BatchQueueItem(
                    project_id=pid,
                    action=action,
                    status="pending",
                    job_id=str(uuid4()),
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
        """Кроп блоков для одного проекта (фоновая задача). Возвращает True при успехе."""
        try:
            proj_dir = resolve_project_dir(pid)
            # Пропустить если блоки уже есть
            blocks_dir = proj_dir / "_output" / "blocks"
            index_file = blocks_dir / "index.json"
            if index_file.exists():
                print(f"[PRE-CROP] {pid}: блоки уже есть, пропуск")
                return True
            # Пропустить если нет result.json (не OCR-проект)
            if not list(proj_dir.glob("*_result.json")):
                return False

            print(f"[PRE-CROP] {pid}: начинаю фоновый кроп блоков...")
            await ws_manager.broadcast_global(
                WSMessage.log("__BATCH__", f"  ⚡ Pre-crop: {pid}", "info")
            )
            exit_code, _, stderr = await run_script(
                str(BLOCKS_SCRIPT),
                ["crop", _project_path(pid), "--compact"],
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

                try:
                    job = AuditJob(
                        job_id=item.job_id or str(uuid4()),
                        object_id=self._resolve_object_id(None),
                        project_id=pid,
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
            "main_audit", "tile_audit", "flash_pro_triage", "prepare",
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
                opt_path = resolve_project_dir(pid) / "_output" / "optimization.json"
                if not opt_path.exists():
                    job.status = JobStatus.FAILED
                    job.error_message = "optimization.json не найден — сначала запустите оптимизацию"
                    job.completed_at = datetime.now().isoformat()
                    return
                self._clean_stage_files(pid, [
                    "optimization_review.json", "optimization_pre_review.json",
                ])
                job.stage = AuditStage.OPTIMIZATION
            elif action == "flash_pro_triage":
                job.stage = AuditStage.FLASH_PRO_TRIAGE
                job.progress_total = 2
            elif action == "tile_audit":
                job.stage = AuditStage.TILE_AUDIT

        # ── Dispatch ───
        if item.retry_stage:
            stage_label = {
                "prepare": "Кроп блоков",
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
            resume_info = self.detect_resume_stage(pid)
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
        if action == "flash_pro_triage":
            await self._run_flash_pro_triage(
                job,
                max_pro_cost_usd=float(extra.get("max_pro_cost_usd", 8.0)),
                include_simple_findings=bool(extra.get("include_simple_findings", False)),
            )
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

        # Полный аудит / batch-actions
        proj_dir = resolve_project_dir(pid)
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
    ) -> AuditJob:
        """Поставить single-project задачу в общую очередь.

        Возвращает placeholder AuditJob со status=QUEUED. Реальный pipeline
        запустится, когда worker дойдёт до этого item. Это единственный путь
        запуска одиночного проекта — start_audit/start_smart_audit/... все
        теперь делегируют сюда.
        """
        async with self._enqueue_lock:
            # Уже бежит прямо сейчас?
            if project_id in self.active_jobs:
                raise RuntimeError(f"Аудит уже запущен для {project_id}")

            # Уже стоит в очереди (pending/running)?
            if self._batch_queue and self._batch_queue.status == "running":
                for it in self._batch_queue.items:
                    if it.project_id == project_id and it.status in ("pending", "running"):
                        raise RuntimeError(f"Проект {project_id} уже в очереди")

            job_id = str(uuid4())
            item = BatchQueueItem(
                project_id=project_id,
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
                object_id=self._resolve_object_id(None),
                project_id=project_id,
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

    async def add_retry_to_batch(self, project_id: str, stage: str) -> BatchQueueStatus:
        """Добавить retry конкретного этапа в очередь."""
        # Маппинг ключей pipeline_summary → внутренних ключей этапов
        stage_map = {
            "crop_blocks": "prepare",
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

        await self._enqueue_single(
            project_id, action="retry_stage", retry_stage=internal_stage,
        )
        stage_label = {
            "prepare": "Кроп блоков", "text_analysis": "Анализ текста",
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
        from webapp.services.project_service import list_projects

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
    async def start_optimization(self, project_id: str) -> AuditJob:
        """Запустить анализ оптимизации проектной документации."""
        return await self._enqueue_single(project_id, action="optimization")

    async def start_optimization_review(self, project_id: str) -> AuditJob:
        """Запустить только critic + corrector оптимизации (без перезапуска самой оптимизации)."""
        # Sanity-check на момент enqueue, чтобы не плодить заведомо ломанные
        # items в очереди. Повторная проверка существования файла происходит
        # внутри `_dispatch_action` на момент реального запуска.
        opt_path = resolve_project_dir(project_id) / "_output" / "optimization.json"
        if not opt_path.exists():
            raise RuntimeError("optimization.json не найден — сначала запустите оптимизацию")
        return await self._enqueue_single(project_id, action="optimization_review")

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
        """Запуск Claude CLI для анализа оптимизации.

        standalone=False: не делать cleanup в finally (для параллельного запуска).
        """
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "optimization", "running")
            info_path = resolve_project_dir(pid) / "project_info.json"
            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            await self._log(job, "Запуск анализа оптимизации проектных решений...")
            if standalone:
                await self._start_heartbeat(job)

            # Проверка rate limit перед запуском
            can_go = await self._check_before_launch(job)
            if not can_go:
                job.status = JobStatus.FAILED
                job.error_message = "Rate limit: ожидание превышено или отменено"
                self._update_pipeline_log(pid, "optimization", "error",
                                           error="Rate limit: ожидание превышено")
                return

            exit_code, output, cli_result = await claude_runner.run_optimization(
                project_info, pid,
                on_output=lambda msg: self._log(job, msg),
            )
            self._record_cli_usage(job, cli_result, "optimization")

            if exit_code == 0:
                # Проверяем что optimization.json создан
                opt_file = resolve_project_dir(pid) / "_output" / "optimization.json"
                if opt_file.exists():
                    size_kb = round(opt_file.stat().st_size / 1024, 1)
                    # Читаем meta для лога
                    try:
                        with open(opt_file, "r", encoding="utf-8") as f:
                            opt_data = json.load(f)
                        meta = opt_data.get("meta", {})
                        total_items = meta.get("total_items", 0)
                        savings = meta.get("estimated_savings_pct", 0)
                        await self._log(
                            job,
                            f"Оптимизация завершена: {total_items} предложений, "
                            f"~{savings}% средняя экономия ({size_kb} KB)",
                            "info",
                        )
                    except Exception:
                        await self._log(job, f"optimization.json создан ({size_kb} KB)", "info")
                else:
                    await self._log(job, "optimization.json не создан — Claude не записал результат", "warn")
                job.status = JobStatus.COMPLETED
                self._update_pipeline_log(pid, "optimization", "done", message="OK")
            elif claude_runner.is_cancelled(exit_code):
                await self._log(job, "Оптимизация отменена", "warn")
                job.status = JobStatus.CANCELLED
                self._update_pipeline_log(pid, "optimization", "error", error="Отменено")
            elif claude_runner.is_rate_limited(exit_code, output or "", ""):
                await self._log(job, "Rate limit при оптимизации, ожидание...", "warn")
                can_continue = await self._wait_for_rate_limit(
                    job, "rate limit при оптимизации", cli_output=output or ""
                )
                if can_continue:
                    exit_code, output, cli_result = await claude_runner.run_optimization(
                        project_info, pid,
                        on_output=lambda msg: self._log(job, msg),
                    )
                    self._record_cli_usage(job, cli_result, "optimization_retry")
                    if exit_code == 0:
                        await self._log(job, "Оптимизация завершена (после паузы)", "info")
                        job.status = JobStatus.COMPLETED
                        self._update_pipeline_log(pid, "optimization", "done",
                                                   message="OK (после rate limit паузы)")
                    else:
                        await self._log(job, f"Ошибка оптимизации после retry (код {exit_code})", "error")
                        job.status = JobStatus.FAILED
                        job.error_message = f"Exit code: {exit_code}"
                        self._update_pipeline_log(pid, "optimization", "error",
                                                   error=_extract_error_detail(exit_code, output))
                else:
                    job.status = JobStatus.FAILED
                    job.error_message = "Rate limit: ожидание превышено или отменено"
                    self._update_pipeline_log(pid, "optimization", "error",
                                               error="Rate limit: ожидание превышено")
            else:
                await self._log(job, f"Ошибка оптимизации (код {exit_code})", "error")
                job.status = JobStatus.FAILED
                job.error_message = f"Exit code: {exit_code}"
                self._update_pipeline_log(pid, "optimization", "error",
                                           error=_extract_error_detail(exit_code, output))

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
        """
        Critic + Corrector для оптимизации: проверка и корректировка предложений.

        1. Critic проверяет каждое OPT-предложение (vendor, savings, traceability)
        2. Если есть отрицательные вердикты — Corrector исправляет
        """
        pid = job.project_id
        output_dir = resolve_project_dir(pid) / "_output"

        # Загружаем project_info
        info_path = resolve_project_dir(pid) / "project_info.json"
        try:
            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)
        except Exception:
            await self._log(job, "Не удалось загрузить project_info.json для optimization review", "warn")
            return

        # ── Critic ──
        self._update_pipeline_log(pid, "optimization_critic", "running")
        print(f"[{pid}] ═══ Optimization Critic (проверка оптимизации) ═══")
        await self._log(job, "═══ Optimization Critic — проверка обоснованности предложений ═══")

        can_go = await self._check_before_launch(job)
        if not can_go:
            await self._log(job, "Rate limit: ожидание превышено или отменено", "warn")
            return

        exit_code, output, cli_result = await claude_runner.run_optimization_critic(
            project_info, pid,
            on_output=lambda msg: self._log(job, msg),
        )
        self._record_cli_usage(job, cli_result, "optimization_critic")

        if claude_runner.is_cancelled(exit_code):
            return

        if exit_code != 0:
            # CLI может вернуть -1/1, но файл уже записан — проверяем
            review_path_check = output_dir / "optimization_review.json"
            if review_path_check.exists():
                try:
                    review_data_check = json.loads(review_path_check.read_text(encoding="utf-8"))
                    reviewed = review_data_check.get("meta", {}).get("total_reviewed", 0)
                    if reviewed > 0:
                        await self._log(
                            job,
                            f"Optimization Critic: CLI код {exit_code}, но review файл валиден "
                            f"({reviewed} reviewed) — продолжаем",
                            "warn",
                        )
                        self._update_pipeline_log(
                            pid, "optimization_critic", "done",
                            message=f"OK (CLI код {exit_code}, файл валиден)",
                        )
                    else:
                        raise ValueError("total_reviewed == 0")
                except (json.JSONDecodeError, OSError, ValueError):
                    self._update_pipeline_log(pid, "optimization_critic", "error",
                                               error=_extract_error_detail(exit_code, output))
                    await self._log(job, f"Optimization Critic: код {exit_code}, файл невалиден", "warn")
                    return
            else:
                self._update_pipeline_log(pid, "optimization_critic", "error",
                                           error=_extract_error_detail(exit_code, output))
                await self._log(job, f"Optimization Critic: код {exit_code}, файл не создан", "warn")
                return
        else:
            self._update_pipeline_log(pid, "optimization_critic", "done", message="OK")

        # Проверяем: нужен ли Corrector?
        review_path = output_dir / "optimization_review.json"
        if not review_path.exists():
            await self._log(job, "optimization_review.json не создан — пропуск Corrector", "warn")
            return

        try:
            review_data = json.loads(review_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            await self._log(job, "Ошибка чтения optimization_review.json", "warn")
            return

        verdicts = review_data.get("meta", {}).get("verdicts", {})
        total_pass = verdicts.get("pass", 0)
        total_reviewed = review_data.get("meta", {}).get("total_reviewed", 0)
        total_issues = total_reviewed - total_pass

        await self._log(
            job,
            f"Optimization Critic: {total_reviewed} проверено, {total_pass} pass, {total_issues} проблем",
        )

        if total_issues == 0:
            await self._log(job, "Все предложения обоснованы — Corrector не требуется")
            self._update_pipeline_log(pid, "optimization_corrector", "skipped",
                                       message="Все предложения прошли Critic")
            return

        # ── Corrector ──
        # Pre-check: optimization.json должен быть валидным JSON
        opt_check_path = output_dir / "optimization.json"
        if opt_check_path.exists():
            try:
                json.loads(opt_check_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                await self._log(job, f"optimization.json невалиден перед Corrector: {e}", "warn")
                self._update_pipeline_log(pid, "optimization_corrector", "error",
                                           error="optimization.json невалиден")
                return

        self._update_pipeline_log(pid, "optimization_corrector", "running")
        print(f"[{pid}] ═══ Optimization Corrector (корректировка оптимизации) ═══")
        await self._log(
            job,
            f"═══ Optimization Corrector — корректировка {total_issues} предложений ═══",
        )

        can_go = await self._check_before_launch(job)
        if not can_go:
            await self._log(job, "Rate limit: ожидание превышено или отменено", "warn")
            return

        exit_code, output, cli_result = await claude_runner.run_optimization_corrector(
            project_info, pid,
            on_output=lambda msg: self._log(job, msg),
        )
        self._record_cli_usage(job, cli_result, "optimization_corrector")

        if claude_runner.is_cancelled(exit_code):
            return

        opt_path = output_dir / "optimization.json"
        pre_review = output_dir / "optimization_pre_review.json"

        if exit_code != 0:
            # CLI может вернуть -1/1, но файл уже записан — проверяем
            if opt_path.exists() and pre_review.exists():
                try:
                    new_data = json.loads(opt_path.read_text(encoding="utf-8"))
                    old_data = json.loads(pre_review.read_text(encoding="utf-8"))
                    new_count = len(new_data.get("scenarios", new_data.get("optimizations", [])))
                    old_count = len(old_data.get("scenarios", old_data.get("optimizations", [])))
                    if new_count > 0 and new_data != old_data:
                        await self._log(
                            job,
                            f"Optimization Corrector: CLI код {exit_code}, но optimization.json обновлён "
                            f"({old_count} → {new_count}) — считаем успехом",
                            "warn",
                        )
                        self._update_pipeline_log(
                            pid, "optimization_corrector", "done",
                            message=f"OK (CLI код {exit_code}, файл обновлён)",
                        )
                    else:
                        raise ValueError("Файл не изменился")
                except (json.JSONDecodeError, OSError, ValueError):
                    self._update_pipeline_log(pid, "optimization_corrector", "error",
                                               error=_extract_error_detail(exit_code, output))
                    await self._log(job, f"Optimization Corrector: код {exit_code}", "warn")
                    # Fallback: восстановить pre_review при реальной ошибке
                    if pre_review.exists() and opt_path.exists():
                        import shutil
                        shutil.copy2(pre_review, opt_path)
                        await self._log(job, "Восстановлен optimization_pre_review.json после ошибки Corrector", "warn")
                    return
            else:
                self._update_pipeline_log(pid, "optimization_corrector", "error",
                                           error=_extract_error_detail(exit_code, output))
                await self._log(job, f"Optimization Corrector: код {exit_code}", "warn")
                return
        else:
            self._update_pipeline_log(pid, "optimization_corrector", "done", message="OK")
            await self._log(job, "Optimization Corrector завершён — optimization.json обновлён")

        # Валидация JSON после opt corrector
        if opt_path.exists():
            is_valid, repair_msg = self._validate_and_repair_json(opt_path)
            if not is_valid:
                await self._log(
                    job,
                    f"ВНИМАНИЕ: optimization.json невалиден после Corrector: {repair_msg}. "
                    f"Восстанавливаю pre_review версию.",
                    "error",
                )
                if pre_review.exists():
                    import shutil
                    shutil.copy2(pre_review, opt_path)
                    await self._log(job, "Восстановлен optimization_pre_review.json", "warn")
            elif "Repaired" in repair_msg:
                await self._log(job, f"optimization.json починен автоматически: {repair_msg}", "warn")


# Глобальный экземпляр
pipeline_manager = PipelineManager()
