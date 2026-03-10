"""
Pipeline Manager — оркестрация конвейера аудита.
Запуск, отмена, отслеживание прогресса.
"""
import asyncio
import json
import os
from uuid import uuid4
from datetime import datetime
from pathlib import Path
from typing import Optional

from webapp.config import (
    BASE_DIR, PROJECTS_DIR,
    PROCESS_PROJECT_SCRIPT, GENERATE_BATCHES_SCRIPT,
    MERGE_RESULTS_SCRIPT, GENERATE_EXCEL_SCRIPT,
    VERIFY_NORMS_SCRIPT, DEFAULT_TILE_QUALITY,
    MAX_PARALLEL_BATCHES,
    RATE_LIMIT_THRESHOLD_PCT, RATE_LIMIT_CHECK_INTERVAL,
    RATE_LIMIT_MAX_WAIT, RATE_LIMIT_MAX_RETRIES,
    CROP_BLOCKS_SCRIPT, GENERATE_BLOCK_BATCHES_SCRIPT,
    MERGE_BLOCK_RESULTS_SCRIPT,
)
from webapp.models.audit import AuditJob, AuditStage, JobStatus, BatchQueueStatus, BatchQueueItem, BatchAction
from webapp.models.websocket import WSMessage
from webapp.config import get_claude_model, get_model_for_stage
from webapp.models.usage import UsageRecord
from webapp.services.process_runner import run_script
from webapp.services import claude_runner
from webapp.services.usage_service import usage_tracker, global_scanner
from webapp.services.resume_detector import detect_resume_stage as _detect_resume_stage
from webapp.services import audit_logger
from webapp.ws.manager import ws_manager


class PipelineManager:
    """Управляет запущенными аудитами. Singleton."""

    def __init__(self):
        self.active_jobs: dict[str, AuditJob] = {}      # project_id -> job
        self._tasks: dict[str, asyncio.Task] = {}        # project_id -> asyncio.Task
        self._heartbeat_tasks: dict[str, asyncio.Task] = {}  # project_id -> heartbeat Task

    ZOMBIE_TIMEOUT_SEC = 600  # 10 минут без heartbeat = зомби

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

    async def _check_before_launch(self, job: AuditJob) -> bool:
        """
        Превентивная проверка rate limit перед запуском Claude CLI.

        Returns:
            True если можно запускать, False если job отменён.
        """
        check = global_scanner.check_rate_limit(RATE_LIMIT_THRESHOLD_PCT)
        if check["can_proceed"]:
            return True
        return await self._wait_for_rate_limit(job, check.get("reason", ""))

    def _record_cli_usage(self, job: AuditJob, cli_result, stage: str):
        """Записать использование токенов после Claude CLI вызова."""
        if not cli_result:
            return
        record = UsageRecord(
            timestamp=datetime.now().isoformat(),
            session_id=cli_result.session_id,
            project_id=job.project_id,
            stage=stage,
            model=get_model_for_stage(stage),
            cost_usd=cli_result.cost_usd,
            duration_ms=cli_result.duration_ms,
            num_turns=cli_result.num_turns,
        )
        usage_tracker.record_usage(record)
        job.cost_usd += cli_result.cost_usd
        job.cli_calls += 1

        # Обогатить из JSONL в фоне (если session_id есть)
        if cli_result.session_id:
            asyncio.create_task(
                self._enrich_usage_async(cli_result.session_id, record.timestamp)
            )

    async def _enrich_usage_async(self, session_id: str, record_timestamp: str):
        """Обогатить запись из JSONL в фоновом потоке."""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, usage_tracker.enrich_from_jsonl, session_id, record_timestamp
            )
        except Exception:
            pass  # Не критично — enrichment best-effort

    def is_running(self, project_id: str) -> bool:
        return project_id in self.active_jobs

    def get_job(self, project_id: str) -> Optional[AuditJob]:
        return self.active_jobs.get(project_id)

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

        # При старте сервера: пометить зависшие "running" этапы в pipeline_log.json
        self._recover_stale_pipelines()

    def _recover_stale_pipelines(self):
        """Сканирует все pipeline_log.json и помечает зависшие 'running' как 'interrupted'.

        Вызывается при старте сервера. Если сервер был перезапущен во время
        активного аудита, процессы Claude CLI уже завершились, но pipeline_log
        остался в состоянии 'running'. Помечаем как 'interrupted' чтобы:
        1. UI показывал корректный статус (не вечный спиннер)
        2. Resume мог подхватить с прерванного этапа
        """
        if not PROJECTS_DIR.exists():
            return

        recovered = 0
        for project_dir in PROJECTS_DIR.iterdir():
            if not project_dir.is_dir() or project_dir.name.startswith("_"):
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
        """Отменить запущенный аудит."""
        job = self.active_jobs.get(project_id)
        if not job:
            return False
        job.status = JobStatus.CANCELLED
        task = self._tasks.get(project_id)
        if task:
            task.cancel()
        self._cleanup(project_id)
        await ws_manager.broadcast_to_project(
            project_id,
            WSMessage.log(project_id, "Аудит отменён пользователем", "warn"),
        )
        return True

    def _cleanup(self, project_id: str):
        self._stop_heartbeat(project_id)
        self.active_jobs.pop(project_id, None)
        self._tasks.pop(project_id, None)

    def _reset_job_progress(self, job: AuditJob):
        """Сбросить прогресс и ETA-данные при переходе между этапами пайплайна."""
        job.progress_current = 0
        job.progress_total = 0
        job.batch_durations = []
        job.batch_started_at = None

    def _clean_stage_files(self, project_id: str, files: list[str]):
        """Удалить устаревшие JSON-файлы этапов перед перезапуском."""
        output_dir = PROJECTS_DIR / project_id / "_output"
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

    # ─── Логирование (делегирование в audit_logger) ───

    def _update_pipeline_log(self, project_id: str, stage_key: str, status: str,
                              message: str = "", error: str = "", detail: dict | None = None):
        """Записать статус этапа в pipeline_log.json и отправить WS-обновление."""
        audit_logger.update_pipeline_log(project_id, stage_key, status, message, error, detail)

    async def _log(self, job: AuditJob, message: str, level: str = "info"):
        """Записать лог в консоль, файл и WebSocket."""
        await audit_logger.log_to_project(job, message, level)

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

                # Вычислить elapsed
                ref_time = job.batch_started_at or job.started_at
                if ref_time:
                    started = datetime.fromisoformat(ref_time)
                    elapsed_sec = (now - started).total_seconds()
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

    async def resume_pipeline(self, project_id: str) -> AuditJob:
        """Продолжить пайплайн с места ошибки."""
        if project_id in self.active_jobs:
            raise RuntimeError(f"Аудит уже запущен для {project_id}")

        resume_info = self.detect_resume_stage(project_id)
        if not resume_info.get("can_resume"):
            raise RuntimeError("Все этапы уже завершены — нечего возобновлять")

        stage = resume_info["stage"]

        job = AuditJob(
            job_id=str(uuid4()),
            project_id=project_id,
            stage=AuditStage.PREPARE,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
        )
        self.active_jobs[project_id] = job

        task = asyncio.create_task(
            self._run_resumed_pipeline(job, stage, resume_info)
        )
        self._tasks[project_id] = task
        return job

    async def _run_resumed_pipeline(self, job: AuditJob, start_stage: str, resume_info: dict):
        """Запуск OCR-пайплайна с указанного этапа."""
        start_time = datetime.now()
        pid = job.project_id
        try:
            # OCR-пайплайн: этапы в правильном порядке
            stages = [
                "prepare",          # 1: crop_blocks.py
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
            ocr_stages = ["prepare", "text_analysis", "block_analysis", "findings_merge", "norm_verify", "excel"]
            start_idx = ocr_stages.index(normalized) if normalized in ocr_stages else 0

            await self._log(
                job,
                f"Возобновление конвейера с этапа: {resume_info.get('stage_label', start_stage)} "
                f"({resume_info.get('detail', '')})",
                "info",
            )

            output_dir = PROJECTS_DIR / pid / "_output"
            info_path = PROJECTS_DIR / pid / "project_info.json"
            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            # ═══ ЭТАП 1: Кроп image-блоков ═══
            if start_idx <= 0:
                job.stage = AuditStage.CROP_BLOCKS
                self._update_pipeline_log(pid, "crop_blocks", "running")
                print(f"[{pid}:resume] ═══ ЭТАП 1: Кроп image-блоков ═══")
                await self._log(job, "═══ ЭТАП 1: Кроп image-блоков из PDF ═══")
                exit_code, _, stderr = await run_script(
                    str(CROP_BLOCKS_SCRIPT),
                    [f"projects/{pid}"],
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code != 0:
                    self._update_pipeline_log(pid, "crop_blocks", "error",
                                               error=stderr or f"Exit code: {exit_code}")
                    raise RuntimeError(f"Кроп блоков: {stderr}")
                self._update_pipeline_log(pid, "crop_blocks", "done", message="OK")

                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══
            if start_idx <= 1:
                if start_idx == 1:
                    # Resume с этого этапа — очистить старые результаты
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
                                               error=f"Код {exit_code}")
                    raise RuntimeError(f"Текстовый анализ: код {exit_code}")

                text_analysis_path = output_dir / "01_text_analysis.json"
                if not text_analysis_path.exists():
                    raise RuntimeError("01_text_analysis.json не создан")

                self._update_pipeline_log(pid, "text_analysis", "done", message="OK")

                if job.status == JobStatus.CANCELLED:
                    return

            # ═══ ЭТАП 3-4: Генерация пакетов + анализ блоков (Claude) ═══
            if start_idx <= 2:
                batch_start_from = resume_info.get("start_from", 1) if start_idx == 2 else 1
                batches_file = output_dir / "block_batches.json"

                # Генерация пакетов (если нет или свежий старт)
                need_generate = not batches_file.exists() or start_idx < 2
                if need_generate:
                    self._reset_job_progress(job)
                    job.stage = AuditStage.CROP_BLOCKS  # reuse для генерации батчей

                    gen_args = [f"projects/{pid}"]
                    print(f"[{pid}:resume] ═══ ЭТАП 3: Генерация пакетов блоков ═══")
                    await self._log(job, "═══ ЭТАП 3: Генерация пакетов блоков ═══")

                    exit_code, _, stderr = await run_script(
                        str(GENERATE_BLOCK_BATCHES_SCRIPT),
                        gen_args,
                        on_output=lambda msg: self._log(job, msg),
                    )
                    if exit_code != 0:
                        raise RuntimeError(f"Генерация пакетов: {stderr}")

                if not batches_file.exists():
                    raise RuntimeError("block_batches.json не создан")

                with open(batches_file, "r", encoding="utf-8") as f:
                    batches_data = json.load(f)

                batches = batches_data.get("batches", [])
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

                    parallel = MAX_PARALLEL_BATCHES
                    print(f"[{pid}:resume] ═══ ЭТАП 4: Анализ блоков ({total_batches} пакетов x{parallel}) ═══")
                    await self._log(
                        job,
                        f"═══ ЭТАП 4: Анализ блоков ({total_batches} пакетов, x{parallel} параллельно) ═══"
                    )

                    semaphore = asyncio.Semaphore(parallel)
                    completed_count = 0
                    error_count = 0

                    async def _process_batch(batch):
                        nonlocal completed_count, error_count
                        batch_id = batch["batch_id"]

                        result_file = output_dir / f"block_batch_{batch_id:03d}.json"
                        if result_file.exists() and result_file.stat().st_size > 100:
                            completed_count += 1
                            job.progress_current = completed_count
                            await self._progress(job, completed_count, total_batches)
                            return

                        async with semaphore:
                            if job.status == JobStatus.CANCELLED:
                                return
                            if error_count >= 5:
                                return

                            can_go = await self._check_before_launch(job)
                            if not can_go:
                                return

                            block_count = batch.get("block_count", len(batch.get("blocks", [])))
                            await self._log(job, f"Пакет {batch_id}/{total_batches}: {block_count} блоков...")

                            retries = 0
                            while retries <= RATE_LIMIT_MAX_RETRIES:
                                batch_start_time = datetime.now()
                                job.batch_started_at = batch_start_time.isoformat()

                                exit_code, output_text, cli_result = await claude_runner.run_block_batch(
                                    batch, project_info, pid, total_batches,
                                    on_output=lambda msg: self._log(job, msg),
                                )
                                self._record_cli_usage(job, cli_result, f"block_batch_{batch_id:03d}")

                                batch_duration = (datetime.now() - batch_start_time).total_seconds()
                                job.batch_durations.append(batch_duration)

                                if exit_code == 0:
                                    if result_file.exists():
                                        size_kb = round(result_file.stat().st_size / 1024, 1)
                                        await self._log(
                                            job,
                                            f"Пакет {batch_id}/{total_batches}: OK ({size_kb} KB)"
                                        )
                                    break

                                if claude_runner.is_cancelled(exit_code):
                                    break

                                stdout_text = output_text or ""
                                stderr_text = cli_result.result_text if cli_result and cli_result.is_error else ""

                                if claude_runner.is_rate_limited(exit_code, stdout_text, stderr_text):
                                    retries += 1
                                    if retries <= RATE_LIMIT_MAX_RETRIES:
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
                                await self._log(job, f"Пакет {batch_id}: ошибка (код {exit_code})", "error")
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

                # Слияние результатов
                print(f"[{pid}:resume] Слияние block_batch_*.json → 02_blocks_analysis.json")
                await self._log(job, "Слияние результатов блоков...")
                exit_code, _, stderr = await run_script(
                    str(MERGE_BLOCK_RESULTS_SCRIPT),
                    [f"projects/{pid}"],
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code != 0:
                    await self._log(job, f"Ошибка слияния: {stderr}", "warn")

                if job.status == JobStatus.CANCELLED:
                    return

                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАП 5: Свод замечаний (Claude) ═══
            if start_idx <= 3:
                self._clean_stage_files(pid, ["03_findings.json"])
                self._reset_job_progress(job)
                job.stage = AuditStage.FINDINGS_MERGE
                job.status = JobStatus.RUNNING
                self._update_pipeline_log(pid, "findings_merge", "running")
                print(f"[{pid}:resume] ═══ ЭТАП 5: Свод замечаний ═══")
                await self._log(job, "═══ ЭТАП 5: Свод замечаний (Claude) ═══")
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
                                               error=f"Код {exit_code}")
                    raise RuntimeError(f"Свод замечаний: код {exit_code}")

                findings_path = output_dir / "03_findings.json"
                if not findings_path.exists():
                    raise RuntimeError("03_findings.json не создан")

                self._update_pipeline_log(pid, "findings_merge", "done", message="OK")

                if job.status == JobStatus.CANCELLED:
                    return

                self.active_jobs[pid] = job
                self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАП 6: Верификация норм ═══
            if start_idx <= 4:
                self._clean_stage_files(pid, [
                    "03a_norms_verified.json", "norm_checks.json",
                ])
                self._reset_job_progress(job)
                findings_path = PROJECTS_DIR / pid / "_output" / "03_findings.json"
                if findings_path.exists():
                    job.stage = AuditStage.NORM_VERIFY
                    job.status = JobStatus.RUNNING
                    print(f"[{pid}:resume] ═══ ЭТАП 6: Верификация норм ═══")
                    await self._log(job, "═══ ЭТАП 6: Верификация нормативных ссылок ═══")
                    await self._run_norm_verification(job, standalone=False)

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
            print(f"[{pid}:resume] ═══ ЭТАП 7: Excel ═══")
            await self._log(job, "═══ ЭТАП 7: Генерация Excel ═══")
            project_path = str(PROJECTS_DIR / pid)
            exit_code, _, _ = await run_script(
                str(GENERATE_EXCEL_SCRIPT),
                args=[project_path],
                env_overrides={"AUDIT_NO_OPEN": "1"},
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code == 0:
                self._update_pipeline_log(pid, "excel", "done", message="OK")
            else:
                self._update_pipeline_log(pid, "excel", "error",
                                           error=f"Exit code: {exit_code}")

            duration = round((datetime.now() - start_time).total_seconds() / 60, 1)
            job.status = JobStatus.COMPLETED
            print(f"[{pid}:resume] ═══ Конвейер завершён за {duration} мин ═══")
            await self._log(job, f"Конвейер завершён за {duration} мин.", "info")

            await ws_manager.broadcast_to_project(
                pid, WSMessage.complete(pid, duration_minutes=duration),
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
        if project_id in self.active_jobs:
            raise RuntimeError(f"Аудит уже запущен для {project_id}")

        job = AuditJob(
            job_id=str(uuid4()),
            project_id=project_id,
            stage=AuditStage.PREPARE,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
        )
        self.active_jobs[project_id] = job
        task = asyncio.create_task(self._run_prepare(job))
        self._tasks[project_id] = task
        return job

    async def _run_prepare(self, job: AuditJob):
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "prepare", "running")
            await self._log(job, "Запуск подготовки проекта (текст + тайлы)...")
            await self._start_heartbeat(job)

            exit_code, stdout, stderr = await run_script(
                str(PROCESS_PROJECT_SCRIPT),
                [f"projects/{pid}", "--quality", DEFAULT_TILE_QUALITY],
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
        if project_id in self.active_jobs:
            raise RuntimeError(f"Аудит уже запущен для {project_id}")

        job = AuditJob(
            job_id=str(uuid4()),
            project_id=project_id,
            stage=AuditStage.TILE_AUDIT,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
        )
        self.active_jobs[project_id] = job
        task = asyncio.create_task(self._run_tile_audit(job, start_from))
        self._tasks[project_id] = task
        return job

    async def _run_tile_audit(self, job: AuditJob, start_from: int = 1, pages_filter: list[int] | None = None, standalone: bool = True):
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "tile_audit", "running")
            output_dir = PROJECTS_DIR / job.project_id / "_output"
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
                info_path = PROJECTS_DIR / job.project_id / "project_info.json"
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
                gen_args = [f"projects/{job.project_id}"]
                if pages_filter:
                    pages_str = ",".join(str(p) for p in pages_filter)
                    gen_args += ["--pages", pages_str]
                    await self._log(job, f"Генерация пакетов тайлов (страницы: {pages_str})...")
                else:
                    await self._log(job, "Генерация пакетов тайлов...")
                exit_code, _, stderr = await run_script(
                    str(GENERATE_BATCHES_SCRIPT),
                    gen_args,
                    on_output=lambda msg: self._log(job, msg),
                )
                if exit_code != 0:
                    raise RuntimeError(f"generate_tile_batches.py: {stderr}")
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
            info_path = PROJECTS_DIR / job.project_id / "project_info.json"
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
                    while retries <= RATE_LIMIT_MAX_RETRIES:
                        batch_start_time = datetime.now()
                        job.batch_started_at = batch_start_time.isoformat()

                        exit_code, output, cli_result = await claude_runner.run_tile_batch(
                            batch, project_info, job.project_id, total,
                            on_output=lambda msg: self._log(job, msg),
                        )
                        self._record_cli_usage(job, cli_result, f"tile_batch_{batch_id:03d}")
                        print(f"[{pid}:tile] Пакет {batch_id}/{total}: exit_code={exit_code}")

                        batch_duration = (datetime.now() - batch_start_time).total_seconds()
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
                exit_code, _, stderr = await run_script(
                    str(MERGE_RESULTS_SCRIPT),
                    [f"projects/{job.project_id}"],
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
        if project_id in self.active_jobs:
            raise RuntimeError(f"Аудит уже запущен для {project_id}")

        # Очистка старых результатов — каждый запуск даёт свежие замечания
        self._clean_stage_files(project_id, [
            "00_init.json", "01_text_analysis.json", "03_findings.json",
        ])

        job = AuditJob(
            job_id=str(uuid4()),
            project_id=project_id,
            stage=AuditStage.MAIN_AUDIT,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
        )
        self.active_jobs[project_id] = job
        task = asyncio.create_task(self._run_main_audit(job))
        self._tasks[project_id] = task
        return job

    async def _run_main_audit(self, job: AuditJob, standalone: bool = True):
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "main_audit", "running")
            info_path = PROJECTS_DIR / pid / "project_info.json"
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
                # Сохраняем вывод как audit_results
                ts = datetime.now().strftime("%Y%m%d_%H%M")
                out_file = PROJECTS_DIR / pid / "_output" / f"audit_results_{ts}.md"
                with open(out_file, "w", encoding="utf-8") as f:
                    f.write(output)
                await self._log(job, f"Аудит завершён. Результат: {out_file.name}", "info")
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
                        ts = datetime.now().strftime("%Y%m%d_%H%M")
                        out_file = PROJECTS_DIR / pid / "_output" / f"audit_results_{ts}.md"
                        with open(out_file, "w", encoding="utf-8") as f:
                            f.write(output)
                        await self._log(job, f"Аудит завершён (после паузы). Результат: {out_file.name}", "info")
                        job.status = JobStatus.COMPLETED
                        self._update_pipeline_log(pid, "main_audit", "done", message="OK (после rate limit паузы)")
                    else:
                        await self._log(job, f"Ошибка аудита после retry (код {exit_code})", "error")
                        job.status = JobStatus.FAILED
                        job.error_message = f"Exit code: {exit_code} (после rate limit retry)"
                        self._update_pipeline_log(pid, "main_audit", "error",
                                                   error=f"Exit code: {exit_code}")
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
                                           error=f"Exit code: {exit_code}")

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
        if project_id in self.active_jobs:
            raise RuntimeError(f"Аудит уже запущен для {project_id}")

        # Очистка старых результатов верификации
        self._clean_stage_files(project_id, [
            "03a_norms_verified.json", "norm_checks.json",
        ])

        job = AuditJob(
            job_id=str(uuid4()),
            project_id=project_id,
            stage=AuditStage.NORM_VERIFY,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
        )
        self.active_jobs[project_id] = job
        task = asyncio.create_task(self._run_norm_verification(job))
        self._tasks[project_id] = task
        return job

    async def _run_norm_verification(self, job: AuditJob, standalone: bool = True):
        """
        Верификация нормативных ссылок:
        1. Извлечь нормы из 03_findings.json (Python)
        2. Проверить через Claude CLI + WebSearch
        3. Если есть устаревшие — пересмотреть замечания через Claude CLI
        """
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "norm_verify", "running")
            import sys
            sys.path.insert(0, str(BASE_DIR))
            from verify_norms import (
                extract_norms_from_findings,
                format_norms_for_template,
                format_findings_to_fix,
            )

            output_dir = PROJECTS_DIR / job.project_id / "_output"
            findings_path = output_dir / "03_findings.json"
            norm_checks_path = output_dir / "norm_checks.json"
            verified_path = output_dir / "03a_norms_verified.json"

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
            norms_list_text = format_norms_for_template(norms_data)

            # ── Шаг 2: Проверка через Claude CLI + WebSearch ──
            await self._log(job, f"Шаг 2: Проверка актуальности {total_norms} норм через WebSearch...")
            job.progress_total = total_norms

            # ── Проверка rate limit перед верификацией норм ──
            can_go = await self._check_before_launch(job)
            if not can_go:
                raise RuntimeError("Rate limit: ожидание превышено или отменено")

            max_retries = RATE_LIMIT_MAX_RETRIES
            for attempt in range(1, max_retries + 1):
                exit_code, output, cli_result = await claude_runner.run_norm_verify(
                    norms_list_text, job.project_id,
                    on_output=lambda msg: self._log(job, msg),
                )
                stage_label = "norm_verify" if attempt == 1 else f"norm_verify_retry_{attempt}"
                self._record_cli_usage(job, cli_result, stage_label)

                # Отмена — не ошибка
                if claude_runner.is_cancelled(exit_code):
                    job.status = JobStatus.CANCELLED
                    await self._log(job, "Верификация норм отменена", "warn")
                    return

                # Успех
                if exit_code == 0:
                    break

                # Rate limit или таймаут → ждём и повторяем
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

                # Другая ошибка — не повторяем
                await self._log(job, f"Ошибка верификации (код {exit_code})", "error")
                raise RuntimeError(f"Claude CLI norm_verify: exit code {exit_code}")

            # Проверяем что файл создан
            if not norm_checks_path.exists():
                await self._log(job, "norm_checks.json не создан — Claude не записал результат", "warn")
                job.status = JobStatus.COMPLETED
                return

            # Читаем результаты
            with open(norm_checks_path, "r", encoding="utf-8") as f:
                checks_data = json.load(f)

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
                job.stage = AuditStage.NORM_FIX
                await self._log(
                    job,
                    f"Шаг 3: Пересмотр {len(needs_fix)} замечаний с устаревшими нормами..."
                )

                findings_to_fix_text = format_findings_to_fix(norm_checks_path, findings_path)

                # ── Проверка rate limit перед пересмотром замечаний ──
                can_go = await self._check_before_launch(job)
                if not can_go:
                    raise RuntimeError("Rate limit: ожидание превышено или отменено")

                exit_code, output, cli_result = await claude_runner.run_norm_fix(
                    findings_to_fix_text, job.project_id,
                    on_output=lambda msg: self._log(job, msg),
                )
                self._record_cli_usage(job, cli_result, "norm_fix")

                if claude_runner.is_cancelled(exit_code):
                    job.status = JobStatus.CANCELLED
                    await self._log(job, "Пересмотр замечаний отменён", "warn")
                    return

                if exit_code == 0 and verified_path.exists():
                    size_kb = round(verified_path.stat().st_size / 1024, 1)
                    await self._log(job, f"03a_norms_verified.json создан ({size_kb} KB)", "info")
                else:
                    await self._log(job, "Предупреждение: 03a_norms_verified.json не создан", "warn")
            else:
                await self._log(job, "Все нормы актуальны — пересмотр не требуется", "info")

            # ── Шаг 4: Обновление централизованной базы норм ──
            await self._update_norms_db(job)

            job.status = JobStatus.COMPLETED
            await self._log(job, "Верификация нормативных ссылок завершена", "info")
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
        """Обновить централизованную базу норм из результатов верификации."""
        try:
            import sys
            sys.path.insert(0, str(BASE_DIR))
            from update_norms_db import load_norms_db, save_norms_db, update_from_project

            project_path = PROJECTS_DIR / job.project_id
            db = load_norms_db()
            stats = update_from_project(db, project_path)

            if "error" in stats:
                await self._log(job, f"Обновление базы норм: {stats['error']}", "warn")
                return

            save_norms_db(db)
            total_changes = stats.get("added", 0) + stats.get("updated", 0)
            if total_changes > 0:
                await self._log(
                    job,
                    f"База норм обновлена: +{stats.get('added', 0)} новых, "
                    f"{stats.get('updated', 0)} обновлено "
                    f"(всего в базе: {len(db.get('norms', {}))})",
                    "info",
                )
            else:
                await self._log(job, f"База норм актуальна ({len(db.get('norms', {}))} записей)", "info")

        except Exception as e:
            # Ошибка обновления базы не должна ронять основной процесс
            await self._log(job, f"Предупреждение: не удалось обновить базу норм: {e}", "warn")
            print(f"[{job.project_id}:norms_db] Ошибка: {e}")

    # ─── Запуск интеллектуального аудита (smart) ───
    async def start_smart_audit(self, project_id: str) -> AuditJob:
        """Интеллектуальный аудит: текст → триаж → выборочная нарезка → анализ."""
        if project_id in self.active_jobs:
            raise RuntimeError(f"Аудит уже запущен для {project_id}")

        usage_tracker.clear_project_usage(project_id)

        job = AuditJob(
            job_id=str(uuid4()),
            project_id=project_id,
            stage=AuditStage.PREPARE,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
        )
        self.active_jobs[project_id] = job
        task = asyncio.create_task(self._run_smart_pipeline(job))
        self._tasks[project_id] = task
        return job

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
            output_dir = PROJECTS_DIR / pid / "_output"
            info_path = PROJECTS_DIR / pid / "project_info.json"

            # ═══ ЭТАП 1: Подготовка текста ═══
            job.stage = AuditStage.PREPARE
            self._update_pipeline_log(pid, "prepare", "running")
            print(f"[{pid}:smart] ═══ ЭТАП 1: Подготовка текста ═══")
            await self._log(job, "═══ ЭТАП 1: Подготовка текста ═══")

            exit_code, _, stderr = await run_script(
                str(PROCESS_PROJECT_SCRIPT),
                [f"projects/{pid}"],
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

                exit_code, _, stderr = await run_script(
                    str(PROCESS_PROJECT_SCRIPT),
                    [f"projects/{pid}", "--pages", pages_str, "--quality", DEFAULT_TILE_QUALITY],
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
                                exit_code, _, stderr = await run_script(
                                    str(PROCESS_PROJECT_SCRIPT),
                                    [f"projects/{pid}", "--pages", pages_str, "--quality", DEFAULT_TILE_QUALITY],
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

            # ═══ ЭТАП 6: Верификация норм ═══
            self._clean_stage_files(pid, [
                "03a_norms_verified.json", "norm_checks.json",
            ])
            self._reset_job_progress(job)
            findings_path = PROJECTS_DIR / pid / "_output" / "03_findings.json"
            if findings_path.exists():
                job.stage = AuditStage.NORM_VERIFY
                job.status = JobStatus.RUNNING
                print(f"[{pid}:smart] ═══ ЭТАП 6: Верификация норм ═══")
                await self._log(job, "═══ ЭТАП 6: Верификация нормативных ссылок ═══")
                await self._run_norm_verification(job)

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
            project_path = str(PROJECTS_DIR / pid)
            exit_code, _, _ = await run_script(
                str(GENERATE_EXCEL_SCRIPT),
                args=[project_path],
                env_overrides={"AUDIT_NO_OPEN": "1"},
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code == 0:
                self._update_pipeline_log(pid, "excel", "done", message="OK")
            else:
                self._update_pipeline_log(pid, "excel", "error",
                                           error=f"Exit code: {exit_code}")

            duration = round((datetime.now() - start_time).total_seconds() / 60, 1)
            job.status = JobStatus.COMPLETED
            print(f"[{pid}:smart] ═══ Smart Parallel завершён за {duration} мин ═══")
            await self._log(job, f"Smart Parallel конвейер завершён за {duration} мин.", "info")

            await ws_manager.broadcast_to_project(
                pid, WSMessage.complete(pid, duration_minutes=duration),
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

    # ─── Стандартный аудит (OCR-пайплайн) ───
    async def start_standard_audit(self, project_id: str) -> AuditJob:
        """Стандартный аудит: кроп блоков → текстовый анализ → выборочные блоки → свод."""
        if project_id in self.active_jobs:
            raise RuntimeError(f"Аудит уже запущен для {project_id}")

        # Сброс счётчика токенов — показываем только текущий прогон
        usage_tracker.clear_project_usage(project_id)

        job = AuditJob(
            job_id=str(uuid4()),
            project_id=project_id,
            stage=AuditStage.CROP_BLOCKS,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
        )
        self.active_jobs[project_id] = job
        task = asyncio.create_task(self._run_standard_pipeline(job))
        self._tasks[project_id] = task
        return job

    async def start_pro_audit(self, project_id: str) -> AuditJob:
        """PRO аудит: кроп блоков → текстовый анализ → ВСЕ блоки → свод."""
        if project_id in self.active_jobs:
            raise RuntimeError(f"Аудит уже запущен для {project_id}")

        # Сброс счётчика токенов — показываем только текущий прогон
        usage_tracker.clear_project_usage(project_id)

        job = AuditJob(
            job_id=str(uuid4()),
            project_id=project_id,
            stage=AuditStage.CROP_BLOCKS,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
        )
        self.active_jobs[project_id] = job
        task = asyncio.create_task(self._run_pro_pipeline(job))
        self._tasks[project_id] = task
        return job

    async def _run_ocr_pipeline(self, job: AuditJob, mode: str = "standard"):
        """
        Общий OCR-пайплайн для standard и pro режимов.

        Этапы:
        1. crop_blocks.py → _output/blocks/
        2. Claude: text_analysis → 01_text_analysis.json + blocks_for_review[]
        3. generate_block_batches.py → block_batches.json
        4. Claude: block_batch (параллельно) → block_batch_NNN.json
        5. merge_block_results.py → 02_blocks_analysis.json
        6. Claude: findings_merge → 03_findings.json
        7. norm_verify
        8. Excel
        """
        start_time = datetime.now()
        pid = job.project_id
        try:
            output_dir = PROJECTS_DIR / pid / "_output"
            info_path = PROJECTS_DIR / pid / "project_info.json"

            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            # ═══ ЭТАП 1: Кроп image-блоков ═══
            job.stage = AuditStage.CROP_BLOCKS
            self._update_pipeline_log(pid, "crop_blocks", "running")
            print(f"[{pid}:{mode}] ═══ ЭТАП 1: Кроп image-блоков ═══")
            await self._log(job, "═══ ЭТАП 1: Кроп image-блоков из PDF ═══")

            exit_code, _, stderr = await run_script(
                str(CROP_BLOCKS_SCRIPT),
                [f"projects/{pid}"],
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code != 0:
                self._update_pipeline_log(pid, "crop_blocks", "error",
                                           error=stderr or f"Exit code: {exit_code}")
                raise RuntimeError(f"Кроп блоков: {stderr}")
            self._update_pipeline_log(pid, "crop_blocks", "done", message="OK")
            print(f"[{pid}:{mode}] ЭТАП 1 OK")

            if job.status == JobStatus.CANCELLED:
                return

            # ═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══
            self._clean_stage_files(pid, [
                "01_text_analysis.json", "02_blocks_analysis.json",
                "03_findings.json", "block_batch_*.json", "block_batches.json",
            ])
            self._reset_job_progress(job)
            job.stage = AuditStage.TEXT_ANALYSIS
            job.status = JobStatus.RUNNING
            self._update_pipeline_log(pid, "text_analysis", "running")
            print(f"[{pid}:{mode}] ═══ ЭТАП 2: Текстовый анализ MD ═══")
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
                                           error=f"Код {exit_code}")
                raise RuntimeError(f"Текстовый анализ: код {exit_code}")

            text_analysis_path = output_dir / "01_text_analysis.json"
            if not text_analysis_path.exists():
                raise RuntimeError("01_text_analysis.json не создан")

            self._update_pipeline_log(pid, "text_analysis", "done", message="OK")
            print(f"[{pid}:{mode}] ЭТАП 2 OK")

            if job.status == JobStatus.CANCELLED:
                return

            # ═══ ЭТАП 3: Генерация пакетов блоков ═══
            self._reset_job_progress(job)
            job.stage = AuditStage.CROP_BLOCKS  # reuse для генерации батчей

            # Определяем какие блоки анализировать
            gen_args = [f"projects/{pid}"]
            if mode == "standard":
                # Только HIGH + MEDIUM блоки (LOW и SKIP пропускаются для экономии)
                with open(text_analysis_path, "r", encoding="utf-8") as f:
                    ta_data = json.load(f)
                blocks_for_review = ta_data.get("blocks_for_review", [])
                all_count = len(blocks_for_review)
                # Фильтрация: только HIGH и MEDIUM приоритеты
                blocks_for_review = [
                    b for b in blocks_for_review
                    if b.get("priority", "").upper() in ("HIGH", "MEDIUM")
                ]
                block_ids = [b["block_id"] for b in blocks_for_review]
                skipped_low = all_count - len(block_ids)
                if block_ids:
                    gen_args += ["--block-ids", ",".join(block_ids)]
                    msg = f"Стандартный режим: {len(block_ids)} блоков (HIGH+MEDIUM)"
                    if skipped_low > 0:
                        msg += f", пропущено {skipped_low} LOW-приоритетных"
                    await self._log(job, msg)
                else:
                    await self._log(job, "Нет блоков для визуальной проверки — пропуск", "warn")
            else:
                # PRO: все блоки (включая LOW)
                await self._log(job, "PRO режим: анализ ВСЕХ image-блоков (включая LOW)")

            print(f"[{pid}:{mode}] ═══ ЭТАП 3: Генерация пакетов блоков ═══")
            await self._log(job, "═══ ЭТАП 3: Генерация пакетов блоков ═══")

            exit_code, _, stderr = await run_script(
                str(GENERATE_BLOCK_BATCHES_SCRIPT),
                gen_args,
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
            total_batches = len(batches)

            if total_batches == 0:
                await self._log(job, "Нет пакетов для анализа — переход к своду", "warn")
            else:
                # ═══ ЭТАП 4: Параллельный анализ блоков (Claude) ═══
                self._clean_stage_files(pid, ["block_batch_*.json"])
                self._reset_job_progress(job)
                job.stage = AuditStage.BLOCK_ANALYSIS
                job.status = JobStatus.RUNNING
                job.progress_total = total_batches
                self._update_pipeline_log(pid, "block_analysis", "running")

                parallel = MAX_PARALLEL_BATCHES
                print(f"[{pid}:{mode}] ═══ ЭТАП 4: Анализ блоков ({total_batches} пакетов x{parallel}) ═══")
                await self._log(
                    job,
                    f"═══ ЭТАП 4: Анализ блоков ({total_batches} пакетов, x{parallel} параллельно) ═══"
                )

                semaphore = asyncio.Semaphore(parallel)
                completed_count = 0
                error_count = 0

                async def _process_block_batch(batch):
                    nonlocal completed_count, error_count
                    batch_id = batch["batch_id"]

                    result_file = output_dir / f"block_batch_{batch_id:03d}.json"
                    if result_file.exists() and result_file.stat().st_size > 100:
                        completed_count += 1
                        job.progress_current = completed_count
                        await self._progress(job, completed_count, total_batches)
                        return

                    async with semaphore:
                        if job.status == JobStatus.CANCELLED:
                            return
                        if error_count >= 5:
                            return

                        can_go = await self._check_before_launch(job)
                        if not can_go:
                            return

                        block_count = batch.get("block_count", len(batch.get("blocks", [])))
                        await self._log(job, f"Пакет {batch_id}/{total_batches}: {block_count} блоков...")

                        retries = 0
                        while retries <= RATE_LIMIT_MAX_RETRIES:
                            batch_start_time = datetime.now()
                            job.batch_started_at = batch_start_time.isoformat()

                            exit_code, output_text, cli_result = await claude_runner.run_block_batch(
                                batch, project_info, pid, total_batches,
                                on_output=lambda msg: self._log(job, msg),
                            )
                            self._record_cli_usage(job, cli_result, f"block_batch_{batch_id:03d}")

                            batch_duration = (datetime.now() - batch_start_time).total_seconds()
                            job.batch_durations.append(batch_duration)

                            if exit_code == 0:
                                if result_file.exists():
                                    size_kb = round(result_file.stat().st_size / 1024, 1)
                                    await self._log(
                                        job,
                                        f"Пакет {batch_id}/{total_batches}: OK ({size_kb} KB)"
                                    )
                                break

                            if claude_runner.is_cancelled(exit_code):
                                break

                            stdout_text = output_text or ""
                            stderr_text = cli_result.result_text if cli_result and cli_result.is_error else ""
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
                                await self._log(
                                    job,
                                    f"Пакет {batch_id}/{total_batches}: ОШИБКА (код {exit_code})",
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

                # Шаг 5: Слияние результатов блоков
                await self._log(job, "Слияние результатов анализа блоков...")
                exit_code, _, stderr = await run_script(
                    str(MERGE_BLOCK_RESULTS_SCRIPT),
                    [f"projects/{pid}"],
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

            # ═══ ЭТАП 6: Свод замечаний (Claude) ═══
            self._reset_job_progress(job)
            job.stage = AuditStage.FINDINGS_MERGE
            job.status = JobStatus.RUNNING
            self._update_pipeline_log(pid, "findings_merge", "running")
            print(f"[{pid}:{mode}] ═══ ЭТАП 6: Свод замечаний ═══")
            await self._log(job, "═══ ЭТАП 6: Свод замечаний (Claude) ═══")

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
                                           error=f"Код {exit_code}")
                await self._log(job, f"Свод замечаний: код {exit_code}", "error")
            else:
                self._update_pipeline_log(pid, "findings_merge", "done", message="OK")

            if job.status == JobStatus.CANCELLED:
                return

            # Re-register
            self.active_jobs[pid] = job
            self._tasks[pid] = asyncio.current_task()

            # ═══ ЭТАП 7: Верификация норм ═══
            self._clean_stage_files(pid, [
                "03a_norms_verified.json", "norm_checks.json",
            ])
            self._reset_job_progress(job)
            findings_path = PROJECTS_DIR / pid / "_output" / "03_findings.json"
            if findings_path.exists():
                job.stage = AuditStage.NORM_VERIFY
                job.status = JobStatus.RUNNING
                print(f"[{pid}:{mode}] ═══ ЭТАП 7: Верификация норм ═══")
                await self._log(job, "═══ ЭТАП 7: Верификация нормативных ссылок ═══")
                await self._run_norm_verification(job, standalone=False)

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
            print(f"[{pid}:{mode}] ═══ ЭТАП 8: Excel ═══")
            await self._log(job, "═══ ЭТАП 8: Генерация Excel ═══")
            project_path = str(PROJECTS_DIR / pid)
            exit_code, _, _ = await run_script(
                str(GENERATE_EXCEL_SCRIPT),
                args=[project_path],
                env_overrides={"AUDIT_NO_OPEN": "1"},
                on_output=lambda msg: self._log(job, msg),
            )
            if exit_code == 0:
                self._update_pipeline_log(pid, "excel", "done", message="OK")
            else:
                self._update_pipeline_log(pid, "excel", "error",
                                           error=f"Exit code: {exit_code}")

            duration = round((datetime.now() - start_time).total_seconds() / 60, 1)
            job.status = JobStatus.COMPLETED
            mode_label = "Стандартный" if mode == "standard" else "PRO"
            print(f"[{pid}:{mode}] ═══ {mode_label} аудит завершён за {duration} мин ═══")
            await self._log(job, f"{mode_label} аудит завершён за {duration} мин.", "info")

            await ws_manager.broadcast_to_project(
                pid, WSMessage.complete(pid, duration_minutes=duration),
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

    async def _run_standard_pipeline(self, job: AuditJob):
        """Стандартный аудит: выборочные блоки."""
        await self._run_ocr_pipeline(job, mode="standard")

    async def _run_pro_pipeline(self, job: AuditJob):
        """PRO аудит: все блоки."""
        await self._run_ocr_pipeline(job, mode="pro")

    # ─── Запуск ВСЕХ проектов последовательно ───
    # ─── Batch (групповые действия для выбранных проектов) ───

    _batch_queue: Optional[BatchQueueStatus] = None

    async def start_batch(self, project_ids: list[str], action: str) -> BatchQueueStatus:
        """Запустить групповое действие для списка проектов последовательно."""
        if self._batch_queue and self._batch_queue.status == "running":
            raise RuntimeError("Групповое действие уже выполняется")
        if self.is_running("__ALL__"):
            raise RuntimeError("Запуск всех проектов уже выполняется")

        queue = BatchQueueStatus(
            queue_id=str(uuid4()),
            action=action,
            items=[BatchQueueItem(project_id=pid, action=action) for pid in project_ids],
            total=len(project_ids),
            status="running",
        )
        self._batch_queue = queue

        meta_job = AuditJob(
            job_id=queue.queue_id,
            project_id="__BATCH__",
            stage=AuditStage.PREPARE,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
            progress_total=len(project_ids),
        )
        self.active_jobs["__BATCH__"] = meta_job

        task = asyncio.create_task(self._run_batch_queue(queue, meta_job))
        self._tasks["__BATCH__"] = task
        return queue

    async def _run_batch_queue(self, queue: BatchQueueStatus, meta_job: AuditJob):
        """Последовательная обработка очереди проектов."""
        try:
            await ws_manager.broadcast_global(
                WSMessage.log(
                    "__BATCH__",
                    f"═══ Групповое действие ({queue.action}) для {queue.total} проектов ═══",
                    "info",
                )
            )

            for idx, item in enumerate(queue.items):
                if queue.status == "cancelled":
                    item.status = "cancelled"
                    continue

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
                    continue

                try:
                    job = AuditJob(
                        job_id=str(uuid4()),
                        project_id=pid,
                        stage=AuditStage.PREPARE,
                        status=JobStatus.RUNNING,
                        started_at=datetime.now().isoformat(),
                    )
                    self.active_jobs[pid] = job
                    self._tasks[pid] = asyncio.current_task()

                    action = queue.action
                    if action == "resume":
                        resume_info = self.detect_resume_stage(pid)
                        if not resume_info.get("can_resume"):
                            item.status = "skipped"
                            item.error = "Нечего возобновлять"
                            await ws_manager.broadcast_global(
                                WSMessage.log("__BATCH__", f"  ⏭ {pid}: нечего возобновлять", "warn")
                            )
                            continue
                        await self._run_resumed_pipeline(job, resume_info["stage"], resume_info)
                    elif action == "optimization":
                        await self._run_optimization(job)
                    elif action in ("standard", "pro"):
                        proj_dir = PROJECTS_DIR / pid
                        if list(proj_dir.glob("*_result.json")):
                            await self._run_ocr_pipeline(job, mode=action)
                        else:
                            await self._run_smart_pipeline(job)
                    elif action in ("standard+optimization", "pro+optimization"):
                        mode = action.split("+")[0]  # standard | pro
                        proj_dir = PROJECTS_DIR / pid
                        if list(proj_dir.glob("*_result.json")):
                            await self._run_ocr_pipeline(job, mode=mode)
                        else:
                            await self._run_smart_pipeline(job)
                        # После аудита — оптимизация (если аудит успешен)
                        if job.status == JobStatus.COMPLETED:
                            job.stage = AuditStage.OPTIMIZATION
                            job.status = JobStatus.RUNNING
                            await self._run_optimization(job)
                    else:
                        # fallback: auto-detect
                        proj_dir = PROJECTS_DIR / pid
                        if list(proj_dir.glob("*_result.json")):
                            await self._run_ocr_pipeline(job, mode="standard")
                        else:
                            await self._run_smart_pipeline(job)

                    if job.status == JobStatus.COMPLETED:
                        item.status = "completed"
                        queue.completed += 1
                        await ws_manager.broadcast_global(
                            WSMessage.log("__BATCH__", f"  ✓ {pid}: завершён", "info")
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

            # Итог
            queue.status = "completed"
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
            self._cleanup("__BATCH__")

    async def cancel_batch(self) -> bool:
        """Отменить текущую batch-очередь."""
        if not self._batch_queue or self._batch_queue.status != "running":
            return False
        self._batch_queue.status = "cancelled"
        # Отменить текущий активный проект
        current_item = self._batch_queue.items[self._batch_queue.current_index]
        if current_item.status == "running":
            await self.cancel(current_item.project_id)
        return True

    def get_batch_queue(self) -> Optional[BatchQueueStatus]:
        """Получить текущую batch-очередь."""
        return self._batch_queue

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

    async def start_all_projects(self, project_ids: list[str] | None = None) -> dict:
        """Запуск полного конвейера для всех проектов последовательно.

        Если project_ids не указан — берёт все проекты из PROJECTS_DIR.
        Возвращает dict с результатами: {project_id: "completed"|"failed"|"skipped"}.
        """
        from webapp.services.project_service import list_projects

        print("[ALL] ═══ start_all_projects() ВЫЗВАН ═══")

        try:
            # Определяем список проектов
            if project_ids:
                all_ids = project_ids
            else:
                projects = list_projects()
                all_ids = [p.project_id for p in projects if p.has_pdf]

            if not all_ids:
                print("[ALL] Нет проектов для обработки")
                return {"error": "Нет проектов для обработки"}

            total = len(all_ids)
            results = {}
            print(f"[ALL] Найдено {total} проектов: {all_ids}")

            # Создаём мета-задачу для отслеживания
            meta_job = AuditJob(
                job_id=str(uuid4()),
                project_id="__ALL__",
                stage=AuditStage.PREPARE,
                status=JobStatus.RUNNING,
                started_at=datetime.now().isoformat(),
                progress_total=total,
            )
            self.active_jobs["__ALL__"] = meta_job
            await self._start_heartbeat(meta_job)

            await ws_manager.broadcast_global(
                WSMessage.log("__ALL__", f"═══ Запуск конвейера для {total} проектов ═══", "info")
            )

            start_time = datetime.now()

            for idx, project_id in enumerate(all_ids, 1):
                if meta_job.status == JobStatus.CANCELLED:
                    results[project_id] = "cancelled"
                    continue

                meta_job.progress_current = idx - 1
                meta_job.stage = AuditStage.PREPARE

                print(f"[ALL] ▶ Проект {idx}/{total}: {project_id}")
                await ws_manager.broadcast_global(
                    WSMessage.log("__ALL__", f"▶ Проект {idx}/{total}: {project_id}", "info")
                )

                # Проверяем что проект не занят
                if self.is_running(project_id):
                    print(f"[ALL]   ⏭ Пропуск {project_id}: уже выполняется")
                    await ws_manager.broadcast_global(
                        WSMessage.log("__ALL__", f"  ⏭ Пропуск: уже выполняется", "warn")
                    )
                    results[project_id] = "skipped"
                    continue

                try:
                    # Запускаем полный конвейер и ЖДЁМ завершения
                    job = AuditJob(
                        job_id=str(uuid4()),
                        project_id=project_id,
                        stage=AuditStage.PREPARE,
                        status=JobStatus.RUNNING,
                        started_at=datetime.now().isoformat(),
                    )
                    self.active_jobs[project_id] = job
                    self._tasks[project_id] = asyncio.current_task()

                    # OCR → standard pipeline, иначе smart
                    proj_dir = PROJECTS_DIR / project_id
                    if list(proj_dir.glob("*_result.json")):
                        await self._run_ocr_pipeline(job, mode="standard")
                    else:
                        await self._run_smart_pipeline(job)

                    if job.status == JobStatus.COMPLETED:
                        results[project_id] = "completed"
                        print(f"[ALL]   ✓ {project_id}: завершён")
                        await ws_manager.broadcast_global(
                            WSMessage.log("__ALL__", f"  ✓ {project_id}: завершён", "info")
                        )
                    else:
                        results[project_id] = f"failed: {job.error_message or job.status.value}"
                        print(f"[ALL]   ✗ {project_id}: {job.status.value} — {job.error_message}")
                        await ws_manager.broadcast_global(
                            WSMessage.log("__ALL__", f"  ✗ {project_id}: {job.status.value}", "error")
                        )

                except Exception as e:
                    results[project_id] = f"error: {e}"
                    print(f"[ALL]   ✗ {project_id}: ИСКЛЮЧЕНИЕ: {e}")
                    import traceback
                    traceback.print_exc()
                    await ws_manager.broadcast_global(
                        WSMessage.log("__ALL__", f"  ✗ {project_id}: исключение: {e}", "error")
                    )
                finally:
                    # cleanup одного проекта (без удаления __ALL__)
                    self._stop_heartbeat(project_id)
                    self.active_jobs.pop(project_id, None)
                    self._tasks.pop(project_id, None)

            # Итог
            meta_job.progress_current = total
            duration = round((datetime.now() - start_time).total_seconds() / 60, 1)

            completed = sum(1 for v in results.values() if v == "completed")
            failed = sum(1 for v in results.values() if v.startswith(("failed", "error")))

            print(f"[ALL] ═══ Конвейер завершён: {completed}/{total} OK, {failed} ошибок, {duration} мин ═══")
            await ws_manager.broadcast_global(
                WSMessage.log(
                    "__ALL__",
                    f"═══ Конвейер завершён: {completed}/{total} OK, {failed} ошибок, {duration} мин ═══",
                    "info",
                )
            )

            meta_job.status = JobStatus.COMPLETED
            meta_job.completed_at = datetime.now().isoformat()
            self._cleanup("__ALL__")

            return {
                "total": total,
                "completed": completed,
                "failed": failed,
                "duration_minutes": duration,
                "details": results,
            }

        except Exception as e:
            print(f"[ALL] КРИТИЧЕСКАЯ ОШИБКА в start_all_projects: {e}")
            import traceback
            traceback.print_exc()
            # Очистка мета-задачи при краше
            self._cleanup("__ALL__")
            raise

    # ─── Запуск оптимизации проектных решений ───
    async def start_optimization(self, project_id: str) -> AuditJob:
        """Запустить анализ оптимизации проектной документации."""
        if project_id in self.active_jobs:
            raise RuntimeError(f"Аудит уже запущен для {project_id}")

        # Очистка старых результатов оптимизации
        self._clean_stage_files(project_id, ["optimization.json"])

        job = AuditJob(
            job_id=str(uuid4()),
            project_id=project_id,
            stage=AuditStage.OPTIMIZATION,
            status=JobStatus.RUNNING,
            started_at=datetime.now().isoformat(),
        )
        self.active_jobs[project_id] = job
        task = asyncio.create_task(self._run_optimization(job))
        self._tasks[project_id] = task
        return job

    async def _run_optimization(self, job: AuditJob):
        """Запуск Claude CLI для анализа оптимизации."""
        pid = job.project_id
        try:
            self._update_pipeline_log(pid, "optimization", "running")
            info_path = PROJECTS_DIR / pid / "project_info.json"
            with open(info_path, "r", encoding="utf-8") as f:
                project_info = json.load(f)

            await self._log(job, "Запуск анализа оптимизации проектных решений...")
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
                opt_file = PROJECTS_DIR / pid / "_output" / "optimization.json"
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
                                                   error=f"Exit code: {exit_code}")
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
                                           error=f"Exit code: {exit_code}")

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
            self._cleanup(pid)


# Глобальный экземпляр
pipeline_manager = PipelineManager()
