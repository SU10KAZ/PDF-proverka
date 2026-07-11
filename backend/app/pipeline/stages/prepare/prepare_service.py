"""
prepare_service.py
------------------
Фоновое выполнение «Подготовить данные» = crop PNG + локальная сборка контекста блоков.

Глобальная очередь (PrepareQueueStatus) хранит per-project прогресс:
  blocks_total, blocks_done, blocks_failed, started_at, elapsed, eta_sec.
Broadcast'ится глобально через WSMessage.prepare_queue_progress.
"""
from __future__ import annotations

import asyncio
import json
import time
import traceback
from pathlib import Path
from typing import Optional

from backend.app.models.audit import PrepareQueueItem, PrepareQueueStatus
from backend.app.models.websocket import WSMessage
from backend.app.core.config import BLOCKS_SCRIPT, PREPARE_QUEUE_FILE
from backend.app.services.common.project_service import resolve_project_dir, resolve_active_project_dir
from backend.app.services.common import version_service
from backend.app.services.common.audit_logger import persist_log, update_pipeline_log
from backend.app.services.storage.storage_write_facade import v2_is_primary
from backend.app.services.storage.v2_primary_wiring import resolve_v2_prepare_paths
from backend.app.services.common.process_runner import run_script, kill_all_processes
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    crop_index_matches_policy,
    STAGE02_BLOCKS_DIRNAME,
    stage02_crop_policy,
)
from backend.app.pipeline.stages.block_context.builder import build_block_context
from backend.app.pipeline.stages.block_context.contract import (
    validate_block_context_summary,
)
from backend.app.ws.manager import ws_manager

DEFAULT_MODEL = "local-block-context"
DEFAULT_PARALLELISM = 1
DEFAULT_TIMEOUT_S = 60

# PREPARE_QUEUE_FILE imported from backend.app.core.config


# ─── State ────────────────────────────────────────────────────────────────

class _PrepareState:
    def __init__(self) -> None:
        self.tasks: dict[str, asyncio.Task] = {}
        self.last_status: dict[str, dict] = {}
        self._pause_event: asyncio.Event | None = None
        self._cancel_event: asyncio.Event | None = None
        self.queue_status: PrepareQueueStatus = PrepareQueueStatus()
        # Crop-таски храним отдельно от локальной сборки контекста.
        self.crop_tasks: dict[str, asyncio.Task] = {}
        self.crop_results: dict[str, dict] = {}  # cached crop_blocks() return
        self._crop_semaphore: asyncio.Semaphore | None = None

    def get_crop_semaphore(self) -> asyncio.Semaphore:
        # Ограничиваем одновременные crop'ы — чтоб не утопить сеть/диск.
        # 1 параллельно: crop тяжёлый по памяти/картинкам; Gemma всё равно
        # последовательно берёт проекты, а сервер должен оставаться живым.
        if self._crop_semaphore is None:
            self._crop_semaphore = asyncio.Semaphore(1)
        return self._crop_semaphore

    def get_pause_event(self) -> asyncio.Event:
        if self._pause_event is None:
            self._pause_event = asyncio.Event()
            self._pause_event.set()  # initial: not paused
        return self._pause_event

    def get_cancel_event(self) -> asyncio.Event:
        if self._cancel_event is None:
            self._cancel_event = asyncio.Event()
        return self._cancel_event

    def is_paused(self) -> bool:
        return self._pause_event is not None and not self._pause_event.is_set()


prepare_state = _PrepareState()


def _prepare_queue_idle() -> bool:
    if any(task is not None and not task.done() for task in prepare_state.tasks.values()):
        return False
    if any(task is not None and not task.done() for task in prepare_state.crop_tasks.values()):
        return False
    return not any(
        item.status in ("pending", "running")
        for item in prepare_state.queue_status.items
    )




def _find_item(project_id: str, version_id: Optional[str] = None) -> Optional[PrepareQueueItem]:
    for it in prepare_state.queue_status.items:
        if it.project_id != project_id:
            continue
        if version_id is not None and (getattr(it, "version_id", None) or "v1") != (version_id or "v1"):
            continue
        return it
    return None


def _resolve_prepare_paths(
    project_id: str,
    version_id: Optional[str] = None,
    object_id: Optional[str] = None,
) -> tuple[Path, Path]:
    """Return (version_dir, output_dir) for prepare-data."""
    if v2_is_primary():
        try:
            legacy_dir = resolve_project_dir(project_id)
        except Exception:
            legacy_dir = None
        paths = resolve_v2_prepare_paths(
            project_id,
            version_id,
            object_id=object_id,
            legacy_project_dir=legacy_dir,
        )
        if paths is None:
            raise RuntimeError(f"v2-primary: не удалось разрешить prepare-пути для {project_id}/{version_id}")
        return paths

    if version_id:
        root_dir = resolve_project_dir(project_id)
        project_dir = version_service.get_version_dir(root_dir, project_id, version_id)
    else:
        project_dir = resolve_active_project_dir(project_id)
    return project_dir, project_dir / "_output"


def _resolve_prepare_effective_version_id(
    project_id: str,
    version_id: Optional[str] = None,
    object_id: Optional[str] = None,
) -> Optional[str]:
    if v2_is_primary():
        version_dir, _ = _resolve_prepare_paths(project_id, version_id, object_id)
        return version_dir.name
    root_dir = resolve_project_dir(project_id)
    return version_service.resolve_effective_version_id(root_dir, project_id, version_id)


def _check_not_in_active_batch(project_id: str) -> None:
    """Защита: не запускать ручной prepare/retry для проекта который уже сидит
    в активной audit batch-очереди. Иначе ручной запуск занимает Gemma-лок и
    тормозит batch (плюс может вступить в конфликт с pre-Gemma loop'ом).

    Импорт audit_manager локальный, чтобы избежать circular import
    (manager → prepare_service → audit_manager → manager).
    """
    try:
        from backend.app.pipeline.manager import pipeline_manager
    except Exception:
        # Если PipelineManager не доступен — пропускаем (защита best-effort).
        return
    if hasattr(pipeline_manager, "is_project_in_active_batch") and pipeline_manager.is_project_in_active_batch(project_id):
        raise RuntimeError(
            f"Проект {project_id} находится в активной batch-очереди. "
            f"Ручной prepare/retry заблокирован чтобы не занять Gemma "
            f"и не замедлить batch."
        )


def _refresh_aggregates() -> None:
    qs = prepare_state.queue_status
    qs.total = len(qs.items)
    qs.completed = sum(1 for i in qs.items if i.status == "completed")
    qs.failed = sum(1 for i in qs.items if i.status == "failed")
    qs.current_index = next(
        (idx for idx, i in enumerate(qs.items) if i.status == "running"),
        qs.completed + qs.failed,
    )
    has_active = any(i.status in ("pending", "running") for i in qs.items)
    has_interrupted = any(i.status == "interrupted" for i in qs.items)
    paused = prepare_state.is_paused()
    qs.paused = paused
    if paused and has_active:
        qs.status = "paused"
    elif has_active:
        qs.status = "running"
    elif has_interrupted:
        qs.status = "interrupted"
    else:
        qs.status = "idle"
    # Суммы по всем items (для индикатора в шапке)
    qs.blocks_total_all = sum((i.blocks_total or 0) for i in qs.items)
    qs.blocks_done_all = sum(i.blocks_done for i in qs.items)
    qs.blocks_failed_all = sum(i.blocks_failed for i in qs.items)
    qs.blocks_truncated_all = sum(i.blocks_truncated for i in qs.items)
    running_item = next((i for i in qs.items if i.status == "running"), None)
    qs.current_project = running_item.project_id if running_item else None
    # Сумма времени по всем проектам — running обновляет на лету,
    # completed/failed/skipped уже зафиксированы в item.elapsed_sec.
    qs.total_elapsed_sec = round(sum(i.elapsed_sec for i in qs.items), 1)


async def _broadcast_queue() -> None:
    _refresh_aggregates()
    msg = WSMessage.prepare_queue_progress(prepare_state.queue_status.model_dump())
    await ws_manager.broadcast_global(msg)
    _persist_queue()


def _persist_queue() -> None:
    """Persist prepare-data queue so it survives uvicorn restarts."""
    try:
        PREPARE_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = prepare_state.queue_status.model_dump()
        PREPARE_QUEUE_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        print(f"[PrepareQueue] Ошибка сохранения prepare_queue.json: {e}")


def load_persisted_queue() -> None:
    """Load prepare-data queue after server restart.

    We do not auto-resume Gemma work on startup: unfinished items become
    `interrupted` and the UI can resume them explicitly.
    """
    if not PREPARE_QUEUE_FILE.exists():
        return
    try:
        data = json.loads(PREPARE_QUEUE_FILE.read_text(encoding="utf-8"))
        # Legacy fields cleanup.
        for it in data.get("items", []) or []:
            if isinstance(it, dict):
                it.pop("manual_run_id", None)
        queue = PrepareQueueStatus(**data)
    except Exception as e:
        print(f"[PrepareQueue] Ошибка загрузки prepare_queue.json: {e}")
        return

    recovered = 0
    for item in queue.items:
        if item.status in ("pending", "running"):
            item.status = "interrupted"
            item.error = item.error or "Сервер перезапущен во время выполнения"
            item.eta_sec = 0
            recovered += 1
        if item.crop_status == "running":
            item.crop_status = "pending"

    queue.paused = False
    prepare_state.queue_status = queue
    _refresh_aggregates()
    _persist_queue()
    if recovered:
        print(f"[PrepareQueue] Восстановлена prepare-очередь: {recovered} interrupted")


# ─── WS log helpers ───────────────────────────────────────────────────────

async def _ws_log(project_id: str, message: str, level: str = "info") -> None:
    persist_log(project_id, message, level, "prepare_data")
    await ws_manager.broadcast_to_project(
        project_id,
        WSMessage.log(project_id, message, level, stage="prepare_data"),
    )


class _CropStdoutForwarder:
    """Перехватывает stdout синхронной crop_blocks() и шлёт каждую строку
    в _ws_log (= WS broadcast + persist в audit_log.jsonl).

    crop_blocks() работает в executor-треде, поэтому WS-broadcast пробрасываем
    через run_coroutine_threadsafe. Дублирует вывод в исходный stdout — чтобы
    server.log не пустел и поведение CLI не менялось.
    """

    def __init__(self, project_id: str, loop: asyncio.AbstractEventLoop, original):
        self._project_id = project_id
        self._loop = loop
        self._original = original
        self._buf = ""

    def write(self, s: str) -> int:
        if not isinstance(s, str):
            s = str(s)
        try:
            self._original.write(s)
        except Exception:
            pass
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            self._emit(line.rstrip("\r"))
        return len(s)

    def flush(self) -> None:
        try:
            self._original.flush()
        except Exception:
            pass
        if self._buf.strip():
            self._emit(self._buf)
            self._buf = ""

    def _emit(self, line: str) -> None:
        if not line.strip():
            return
        # Persist синхронно (thread-safe, файловая I/O)
        try:
            persist_log(self._project_id, line, "info", "prepare_data")
        except Exception:
            pass
        # WS-broadcast асинхронный — пробрасываем в loop основного треда
        try:
            asyncio.run_coroutine_threadsafe(
                ws_manager.broadcast_to_project(
                    self._project_id,
                    WSMessage.log(self._project_id, line, "info", stage="prepare_data"),
                ),
                self._loop,
            )
        except Exception:
            pass


# ─── Core ─────────────────────────────────────────────────────────────────

def _build_crop_args(
    project_dir: Path,
    *,
    force: bool,
    policy: dict,
    output_dir: Optional[Path] = None,
) -> list[str]:
    output_arg = str(output_dir) if output_dir is not None else STAGE02_BLOCKS_DIRNAME
    args = ["crop", str(project_dir), "--output-dir", output_arg]
    if policy.get("compact"):
        args.append("--compact")
    elif policy.get("dpi"):
        args.extend(["--dpi", str(int(policy["dpi"]))])
    if policy.get("skip_small") is False:
        args.append("--no-skip-small")
    if force:
        args.append("--force")
    return args


def _parse_crop_stdout(stdout: str) -> dict | None:
    for line in reversed((stdout or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and "total_blocks" in parsed:
            return parsed
    return None


async def _crop_for_project(project_id: str) -> None:
    """Скачать PNG-блоки для prepare-data.

    Вызывается из _run_prepare; тяжёлые crop-задачи ограничены семафором.
    """
    item = _find_item(project_id)
    if item is None:
        return
    version_id = getattr(item, "version_id", None)
    object_id = getattr(item, "object_id", None)
    project_dir, out_dir = _resolve_prepare_paths(project_id, version_id, object_id)
    sem = prepare_state.get_crop_semaphore()
    async with sem:
        # Cancel — пользователь остановил очередь до старта crop'а: пропускаем.
        if prepare_state.get_cancel_event().is_set():
            item.crop_status = "failed"
            prepare_state.crop_results[project_id] = {"error": "cancelled before crop"}
            await _broadcast_queue()
            return
        item.crop_status = "running"
        await _broadcast_queue()
        await _ws_log(project_id, "Скачивание блоков по crop_url...")
        update_pipeline_log(project_id, "crop_blocks", "running")
        try:
            policy = stage02_crop_policy()
            blocks_dir = out_dir / STAGE02_BLOCKS_DIRNAME
            index_path = blocks_dir / "index.json"
            force_crop = (
                (index_path.exists() and not crop_index_matches_policy(index_path, policy))
                or (not index_path.exists() and blocks_dir.exists() and any(blocks_dir.glob("block_*.png")))
            )

            async def _on_crop_output(line: str) -> None:
                level = "error" if line.startswith("[ERR]") or "[ERROR]" in line else "info"
                await _ws_log(project_id, line, level)

            exit_code, stdout, stderr = await run_script(
                str(BLOCKS_SCRIPT),
                _build_crop_args(project_dir, force=force_crop, policy=policy, output_dir=blocks_dir if v2_is_primary() else None),
                on_output=_on_crop_output,
                project_id=project_id,
            )
            result = _parse_crop_stdout(stdout) or {}
            if exit_code not in (0, 2):
                tail = "\n".join((stderr or stdout or "").splitlines()[-8:])
                result = {
                    "error": f"crop subprocess failed: exit code {exit_code}"
                    + (f": {tail}" if tail else "")
                }
        except Exception as e:
            result = {"error": f"crop exception: {e}"}
        prepare_state.crop_results[project_id] = result
        if result.get("error"):
            item.crop_status = "failed"
            await _ws_log(project_id, f"Ошибка crop: {result['error']}", "error")
            update_pipeline_log(
                project_id, "crop_blocks", "error",
                error=str(result["error"])[:300]
            )
        else:
            item.crop_status = "done"
            cropped = result.get("cropped", 0) or 0
            skipped = result.get("skipped", 0) or 0
            errors = result.get("errors", 0) or 0
            item.crop_blocks_total = cropped + skipped
            await _ws_log(
                project_id,
                f"Crop готов: {cropped} новых, {skipped} пропущено, {errors} ошибок",
            )
            update_pipeline_log(
                project_id, "crop_blocks", "done",
                message=f"OK ({cropped} новых, {skipped} пропущено, {errors} ошибок)"
            )
        await _broadcast_queue()


async def _await_crop(project_id: str) -> dict:
    """Дождаться завершения pre-crop таски и вернуть её результат."""
    crop_task = prepare_state.crop_tasks.get(project_id)
    if crop_task is not None:
        try:
            await crop_task
        except Exception as e:
            return {"error": f"crop task exception: {e}"}
    result = prepare_state.crop_results.get(project_id)
    if result is None:
        return {"error": "crop result not found (taska не запускалась?)"}
    return result


def _ensure_crop_started(project_id: str) -> None:
    """Start crop lazily when this project reaches the global prepare lock."""
    if project_id in prepare_state.crop_results:
        return
    crop_task = prepare_state.crop_tasks.get(project_id)
    if crop_task is None or crop_task.done():
        prepare_state.crop_tasks[project_id] = asyncio.create_task(
            _crop_for_project(project_id)
        )


async def _run_prepare(
    project_id: str,
    force: bool,
    parallelism: Optional[int],
    model: Optional[str],
    timeout: Optional[int],
) -> dict:
    item = _find_item(project_id)
    assert item is not None
    project_dir, out_dir = _resolve_prepare_paths(project_id, getattr(item, "version_id", None), getattr(item, "object_id", None))
    item.status = "running"
    item.started_at = time.time()
    await _broadcast_queue()

    # Crop запускаем только когда проект дошёл до подготовки контекста.
    _ensure_crop_started(project_id)
    crop_result = await _await_crop(project_id)
    if crop_result.get("error"):
        item.status = "failed"
        item.error = crop_result["error"][:300]
        await _broadcast_queue()
        await _ws_log(project_id, f"Crop не выполнен: {crop_result['error']}", "error")
        return {"status": "error", "stage": "crop", "error": crop_result["error"]}

    validation = validate_block_context_summary(out_dir, canonical_only=True)
    if validation.get("valid") and not force:
        summary_existing = validation.get("summary") or {}
        item.status = "skipped"
        item.blocks_total = int(summary_existing.get("blocks_total") or 0)
        item.blocks_done = int(summary_existing.get("blocks_ready") or 0)
        await _broadcast_queue()
        await _ws_log(project_id, "Контекст блоков уже готов", "info")
        return {"status": "skipped", "existing": summary_existing, "crop": crop_result}

    async def _on_context_event(event: dict) -> None:
        if event.get("type") == "started":
            item.blocks_total = int(event.get("total") or 0)
            item.blocks_done = 0
            item.blocks_failed = 0
        elif event.get("type") == "block_done":
            item.blocks_done = int(event.get("completed") or 0)
            if not event.get("ok"):
                item.blocks_failed += 1
        await _broadcast_queue()

    await _ws_log(project_id, "Подготовка контекста PDF/Vectograph...", "info")
    update_pipeline_log(project_id, "block_context", "running")
    summary = await build_block_context(
        project_dir,
        output_dir=out_dir,
        blocks_index_path=out_dir / STAGE02_BLOCKS_DIRNAME / "index.json",
        progress_cb=_on_context_event,
    )
    item.status = "completed" if summary.get("status") == "ok" else "partial"
    item.elapsed_sec = round(time.time() - (item.started_at or time.time()), 1)
    await _broadcast_queue()
    message = (
        f"Контекст готов: {summary.get('blocks_ready', 0)}/"
        f"{summary.get('blocks_total', 0)}; {summary.get('source_counts') or {}}"
    )
    update_pipeline_log(project_id, "block_context", item.status, message=message)
    update_pipeline_log(project_id, "gemma_enrichment", item.status, message=message)
    await _ws_log(project_id, message, "info")
    summary["crop"] = crop_result
    return summary

# ─── Public API ───────────────────────────────────────────────────────────

async def start_retry_failed(project_id: str, *, version_id: Optional[str] = None) -> dict:
    """Локально пересобрать контекст блоков без полного prepare."""
    # Не меняем контекст проекта, пока он участвует в активном batch.
    _check_not_in_active_batch(project_id)

    existing_task = prepare_state.tasks.get(project_id)
    if existing_task is not None and not existing_task.done():
        return {"status": "already_running"}

    project_dir, out_dir = _resolve_prepare_paths(project_id, version_id)
    if not validate_block_context_summary(out_dir).get("valid"):
        return {"status": "error", "error": "summary не найден — сначала надо сделать обычный prepare-data"}

    reset_cancel()

    async def _on_event(event: dict) -> None:
        t = event.get("type")
        if t == "retry_failed_started":
            await _ws_log(
                project_id,
                f"↻ Retry failed: {event['to_retry']} блок(ов) на повторную обработку"
                + (f" (пропущено {len(event['missing'])} — нет в index)" if event.get("missing") else ""),
                "warn",
            )
        elif t == "retry_failed_block_started":
            await _ws_log(
                project_id,
                f"  [{event['index']:>3}/{event['total']}] retry start: {event['block_id']} p={event['page']}",
            )
        elif t == "retry_failed_block_done":
            mark = "✓" if event["ok"] else "✗"
            err = f" — {event['error'][:80]}" if event.get("error") else ""
            level = "info" if event["ok"] else "warn"
            await _ws_log(
                project_id,
                f"  [{event['index']:>3}/{event['total']}] {mark} {event['block_id']} "
                f"t={event['elapsed_ms']/1000:.1f}s{err}",
                level,
            )
        elif t == "block_retry":
            prev_tok = event.get("previous_output_tokens")
            await _ws_log(
                project_id,
                f"    ↻ tier {event['attempt']}/3 max={event['max_tokens']} "
                f"(прошлая: {prev_tok} токенов)",
                "warn",
            )
        elif t == "block_split":
            strategy = event.get("strategy", "")
            await _ws_log(
                project_id,
                f"    ✂ Split {event['block_id']} (aspect={event['aspect']:.2f}, {strategy})",
                "warn",
            )
        elif t == "retry_failed_completed":
            s = event["summary"]
            stats = s.get("retry_failed_stats") or {}
            await _ws_log(
                project_id,
                f"Retry failed готов: восстановлено {stats.get('recovered', 0)} / "
                f"осталось упавшими {stats.get('still_failed', 0)} "
                f"(время {stats.get('elapsed_s', 0)}s)",
            )

    async def _wrapped() -> None:
        try:
            result = await build_block_context(
                project_dir,
                output_dir=out_dir,
                blocks_index_path=out_dir / STAGE02_BLOCKS_DIRNAME / "index.json",
                progress_cb=_on_event,
            )
            prepare_state.last_status[project_id] = result
        except Exception as e:
            err = {"status": "error", "error": str(e), "traceback": traceback.format_exc()}
            prepare_state.last_status[project_id] = err
            try:
                await _ws_log(project_id, f"Retry failed exception: {e}", "error")
            except Exception:
                pass
        finally:
            pass

    task = asyncio.create_task(_wrapped())
    prepare_state.tasks[project_id] = task
    return {"status": "started"}


async def start_prepare_data(
    project_id: str,
    *,
    force: bool = False,
    parallelism: Optional[int] = None,
    model: Optional[str] = None,
    timeout: Optional[int] = None,
    version_id: Optional[str] = None,
    object_id: Optional[str] = None,
) -> dict:
    """Поставить project_id в очередь prepare-data. Не блокирует HTTP."""
    # Защита: не лезем в Gemma если проект в активном batch.
    _check_not_in_active_batch(project_id)

    try:
        from backend.app.pipeline.manager import pipeline_manager
        if pipeline_manager.is_running(project_id) or pipeline_manager.is_queued(project_id):
            return {
                "status": "error",
                "error": "Проект уже выполняется или ожидает в основной audit-очереди",
            }
    except Exception:
        pass

    existing_task = prepare_state.tasks.get(project_id)
    if existing_task is not None and not existing_task.done():
        return {"status": "already_running"}

    # Сбрасываем cancel-event если был установлен предыдущей отменой
    reset_cancel()
    # Если paused — оставим как есть, юзер сам resume'нёт

    effective_version_id = None
    if v2_is_primary() or version_id:
        try:
            effective_version_id = _resolve_prepare_effective_version_id(project_id, version_id, object_id)
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # Если уже есть item для этого проекта/версии в queue_status — обнуляем
    existing_item = _find_item(project_id, effective_version_id)
    if existing_item:
        prepare_state.queue_status.items.remove(existing_item)
    # Старый crop-таск/результат — снести, чтобы новый enrichment начал с нуля
    old_crop = prepare_state.crop_tasks.pop(project_id, None)
    if old_crop is not None and not old_crop.done():
        old_crop.cancel()
    prepare_state.crop_results.pop(project_id, None)
    item_kwargs = {
        "project_id": project_id,
        "status": "pending",
        "force": force,
    }
    if effective_version_id is not None:
        item_kwargs["version_id"] = effective_version_id
    if object_id is not None:
        item_kwargs["object_id"] = object_id
    item = PrepareQueueItem(**item_kwargs)
    prepare_state.queue_status.items.append(item)
    await _broadcast_queue()

    async def _wrapped() -> None:
        try:
            result = await _run_prepare(project_id, force, parallelism, model, timeout)
            prepare_state.last_status[project_id] = result
        except Exception as e:
            err = {"status": "error", "error": str(e), "traceback": traceback.format_exc()}
            prepare_state.last_status[project_id] = err
            it = _find_item(project_id)
            if it:
                it.status = "failed"
                it.error = str(e)[:300]
                await _broadcast_queue()
            try:
                await _ws_log(project_id, f"Исключение: {e}", "error")
            except Exception:
                pass
        finally:
            # Удаляем кеш crop'а — больше не понадобится.
            prepare_state.crop_results.pop(project_id, None)
            prepare_state.crop_tasks.pop(project_id, None)

    task = asyncio.create_task(_wrapped())
    prepare_state.tasks[project_id] = task
    return {"status": "started", "queue_position": len(prepare_state.queue_status.items)}


def get_prepare_status(project_id: str) -> dict:
    """Статус для конкретного проекта."""
    task = prepare_state.tasks.get(project_id)
    item = _find_item(project_id)
    return {
        "running": bool(task is not None and not task.done()),
        "item": item.model_dump() if item else None,
        "last_status": prepare_state.last_status.get(project_id),
    }


def get_global_queue() -> dict:
    """Полное состояние очереди для polling fallback."""
    _refresh_aggregates()
    return prepare_state.queue_status.model_dump()


def is_prepare_active_or_queued(project_id: str) -> bool:
    """True если prepare-data уже держит или ждёт этот проект."""
    item = _find_item(project_id)
    if item and item.status in ("pending", "running"):
        return True
    task = prepare_state.tasks.get(project_id)
    if task is not None and not task.done():
        return True
    crop_task = prepare_state.crop_tasks.get(project_id)
    if crop_task is not None and not crop_task.done():
        return True
    return False


def clear_completed_from_queue() -> int:
    """Удалить из очереди все completed/failed/skipped items (по запросу пользователя)."""
    before = len(prepare_state.queue_status.items)
    prepare_state.queue_status.items = [
        i for i in prepare_state.queue_status.items
        if i.status in ("pending", "running")
    ]
    removed = before - len(prepare_state.queue_status.items)
    _refresh_aggregates()
    _persist_queue()
    return removed


async def pause_queue() -> dict:
    """Поставить очередь на паузу. Между блоками — runner будет ждать unpause.
    Текущий блок (если уже отправлен в Gemma) дойдёт до конца, потом пауза.
    """
    ev = prepare_state.get_pause_event()
    ev.clear()
    await _broadcast_queue()
    return {"paused": True}


async def resume_queue() -> dict:
    """Снять паузу — runner возобновит обработку."""
    if prepare_state.queue_status.status == "interrupted":
        interrupted = [
            (item.project_id, item.force, getattr(item, "version_id", None), getattr(item, "object_id", None))
            for item in prepare_state.queue_status.items
            if item.status == "interrupted"
        ]
        if not interrupted:
            return {"resumed": False, "reason": "interrupted items not found"}
        for project_id, force, version_id, object_id in interrupted:
            await start_prepare_data(
                project_id,
                force=force,
                version_id=version_id,
                object_id=object_id,
            )
        return {"resumed": True, "count": len(interrupted)}

    ev = prepare_state.get_pause_event()
    ev.set()
    await _broadcast_queue()
    return {"paused": False}


async def cancel_queue() -> dict:
    """Отменить очередь: убиваем активные subprocess'ы, pending пропускаем.

    Если на паузе — снимем паузу, чтобы runner смог увидеть cancel и выйти.
    """
    cev = prepare_state.get_cancel_event()
    cev.set()
    pev = prepare_state.get_pause_event()
    pev.set()  # снимаем паузу чтобы runner вышел

    cancelled_pending = 0
    killed_running = 0
    killed_procs = 0
    for it in list(prepare_state.queue_status.items):
        pid = it.project_id
        if it.status == "pending":
            it.status = "skipped"
            it.error = "cancelled by user"
            cancelled_pending += 1
            ct = prepare_state.crop_tasks.pop(pid, None)
            if ct is not None and not ct.done():
                ct.cancel()
        elif it.status == "running":
            killed_running += 1
            # Убиваем crop subprocess (blocks.py, ключ project_id)
            try:
                killed_procs += await kill_all_processes(pid)
            except Exception:
                pass
            # Cancel'аем asyncio tasks — run_script поймает CancelledError
            ct = prepare_state.crop_tasks.pop(pid, None)
            if ct is not None and not ct.done():
                ct.cancel()
            wrapped = prepare_state.tasks.get(pid)
            if wrapped is not None and not wrapped.done():
                wrapped.cancel()
            it.status = "failed"
            it.error = "cancelled by user"
    await _broadcast_queue()
    return {
        "cancelled": True,
        "cancelled_pending": cancelled_pending,
        "killed_running": killed_running,
        "killed_processes": killed_procs,
    }


def reset_cancel() -> None:
    """Сбросить cancel-event для следующего запуска (вызывается перед start_prepare_data)."""
    if prepare_state._cancel_event is not None:
        prepare_state._cancel_event.clear()
