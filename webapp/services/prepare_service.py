"""
prepare_service.py
------------------
Фоновое выполнение «Подготовить данные» = crop PNG + Gemma enrichment.
ОДИН проект за раз (asyncio.Lock) — Gemma 3.6 35B не тянет параллель.

Глобальная очередь (PrepareQueueStatus) хранит per-project прогресс:
  blocks_total, blocks_done, blocks_failed, started_at, elapsed, eta_sec.
Broadcast'ится глобально через WSMessage.prepare_queue_progress.
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Optional

from webapp.models.audit import PrepareQueueItem, PrepareQueueStatus
from webapp.models.websocket import WSMessage
from webapp.config import BLOCKS_SCRIPT, GEMMA_ENRICH_SCRIPT
from webapp.services.project_service import resolve_project_dir
from webapp.services.audit_logger import persist_log, update_pipeline_log
from webapp.services.lmstudio_lifecycle_service import (
    note_activity as _lmstudio_note_activity,
    register_idle_probe as _register_lmstudio_idle_probe,
    schedule_post_queue_cleanup as _schedule_lmstudio_post_queue_cleanup,
)
from webapp.services.process_runner import run_script
from webapp.services.gemma_gate import find_project_markdown, load_project_info
from gemma_enrichment_contract import (
    ENRICHMENT_MARKER_RE,
    GEMMA_BLOCKS_DIRNAME,
    crop_index_matches_policy,
    gemma_blocks_dir,
    gemma_blocks_index_path,
    gemma_enrichment_crop_policy,
    validate_gemma_summary,
)
from webapp.ws.manager import ws_manager

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

PREPARE_QUEUE_FILE = _ROOT / "webapp" / "data" / "prepare_queue.json"

from gemma_enrich import (  # noqa: E402
    retry_failed_blocks,
    DEFAULT_MODEL,
    DEFAULT_PARALLELISM,
    DEFAULT_TIMEOUT_S,
)
def _backup_output_for_reenrichment(out_dir):  # stub — функция удалена из blocks.py
    pass


# ─── State ────────────────────────────────────────────────────────────────

class _PrepareState:
    def __init__(self) -> None:
        self.tasks: dict[str, asyncio.Task] = {}
        self.last_status: dict[str, dict] = {}
        self._global_lock: asyncio.Lock | None = None
        self._pause_event: asyncio.Event | None = None
        self._cancel_event: asyncio.Event | None = None
        self.queue_status: PrepareQueueStatus = PrepareQueueStatus()
        # Crop-таски храним отдельно, но запускаем строго внутри Gemma-лока:
        # один проект целиком crop -> Gemma, потом следующий. Это медленнее
        # фонового pre-crop, зато не смешивает тяжёлый crop с Gemma 35B.
        self.crop_tasks: dict[str, asyncio.Task] = {}
        self.crop_results: dict[str, dict] = {}  # cached crop_blocks() return
        self._crop_semaphore: asyncio.Semaphore | None = None

    def get_lock(self) -> asyncio.Lock:
        if self._global_lock is None:
            self._global_lock = asyncio.Lock()
        return self._global_lock

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


_register_lmstudio_idle_probe("prepare_queue", _prepare_queue_idle)


def _find_item(project_id: str) -> Optional[PrepareQueueItem]:
    for it in prepare_state.queue_status.items:
        if it.project_id == project_id:
            return it
    return None


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
        PREPARE_QUEUE_FILE.write_text(
            prepare_state.queue_status.model_dump_json(indent=2),
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

def _resolve_overrides(project_dir: Path) -> dict:
    info_path = project_dir / "project_info.json"
    if not info_path.exists():
        return {}
    try:
        info = json.loads(info_path.read_text(encoding="utf-8"))
        return info.get("enrichment") or {}
    except Exception:
        return {}


def _build_crop_args(project_dir: Path, *, force: bool, policy: dict) -> list[str]:
    args = ["crop", str(project_dir), "--output-dir", GEMMA_BLOCKS_DIRNAME]
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

    Вызывается из _run_prepare под глобальным Gemma-локом, поэтому crop и Gemma
    не идут параллельно на разных проектах.
    """
    item = _find_item(project_id)
    if item is None:
        return
    project_dir = resolve_project_dir(project_id)
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
            policy = gemma_enrichment_crop_policy()
            index_path = gemma_blocks_index_path(project_dir)
            blocks_dir = gemma_blocks_dir(project_dir)
            force_crop = (
                (index_path.exists() and not crop_index_matches_policy(index_path, policy))
                or (not index_path.exists() and blocks_dir.exists() and any(blocks_dir.glob("block_*.png")))
            )

            async def _on_crop_output(line: str) -> None:
                level = "error" if line.startswith("[ERR]") or "[ERROR]" in line else "info"
                await _ws_log(project_id, line, level)

            exit_code, stdout, stderr = await run_script(
                str(BLOCKS_SCRIPT),
                _build_crop_args(project_dir, force=force_crop, policy=policy),
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


_GEMMA_START_RE = re.compile(r"^\[start\]\s+(\d+)\s+blocks\b")
_GEMMA_BLOCK_RE = re.compile(
    r"^\s*\[\s*(\d+)/(\d+)\]\s+(OK|FAIL)\s+([A-Z0-9-]+)\s+p=(\S+)\s+t=([\d.]+)s(?:\s+—\s+(.*))?$"
)


def _build_gemma_args(
    project_dir: Path,
    *,
    force: bool,
    model: str,
    parallelism: int,
    timeout: int,
) -> list[str]:
    args = [
        str(project_dir),
        "--model", model,
        "--parallelism", str(int(parallelism)),
        "--timeout", str(int(timeout)),
    ]
    if force:
        args.append("--force")
    return args


async def _run_gemma_enrichment_subprocess(
    project_id: str,
    project_dir: Path,
    *,
    item: PrepareQueueItem,
    force: bool,
    model: str,
    parallelism: int,
    timeout: int,
) -> dict:
    """Run gemma_enrich.py outside uvicorn and return its summary JSON."""
    seen_failed: set[str] = set()

    async def _on_gemma_output(line: str) -> None:
        if not line:
            return
        level = "error" if line.startswith("[ERR]") or "Traceback" in line else "info"
        start_match = _GEMMA_START_RE.match(line)
        if start_match:
            total = int(start_match.group(1))
            item.blocks_total = total
            item.blocks_done = 0
            item.blocks_failed = 0
            item.blocks_truncated = 0
            await _broadcast_queue()
            await _ws_log(project_id, f"Обработка {total} image-блоков...")
            return

        block_match = _GEMMA_BLOCK_RE.match(line)
        if block_match:
            completed = int(block_match.group(1))
            total = int(block_match.group(2))
            ok = block_match.group(3) == "OK"
            block_id = block_match.group(4)
            page = block_match.group(5)
            elapsed_s = float(block_match.group(6))
            err = block_match.group(7) or ""
            item.blocks_done = completed
            item.blocks_total = total
            if not ok:
                seen_failed.add(block_id)
            item.blocks_failed = len(seen_failed)
            elapsed = time.time() - (item.started_at or time.time())
            item.elapsed_sec = round(elapsed, 1)
            done = max(1, completed)
            item.eta_sec = round((elapsed / done) * max(0, total - completed), 0)
            await _broadcast_queue()
            mark = "✓" if ok else "✗"
            await _ws_log(
                project_id,
                f"[{completed:>3}/{total}] {mark} {block_id} p={page} t={elapsed_s:.1f}s"
                + (f" — {err[:80]}" if err else ""),
                "info" if ok else "warn",
            )
            return

        await _ws_log(project_id, line, level)

    exit_code, stdout, stderr = await run_script(
        str(GEMMA_ENRICH_SCRIPT),
        _build_gemma_args(
            project_dir,
            force=force,
            model=model,
            parallelism=parallelism,
            timeout=timeout,
        ),
        on_output=_on_gemma_output,
        project_id=f"prepare_gemma:{project_id}",
    )

    summary_path = project_dir / "_output" / "gemma_enrichment_summary.json"
    summary: dict = {}
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception as e:
            summary = {"status": "failed", "error": f"summary read failed: {e}"}

    if exit_code not in (0, 1):
        tail = "\n".join((stderr or stdout or "").splitlines()[-10:])
        if not summary:
            summary = {"status": "failed"}
        summary["status"] = "failed"
        summary["error"] = (
            f"gemma_enrich subprocess failed: exit code {exit_code}"
            + (f": {tail}" if tail else "")
        )
    elif not summary:
        summary = {
            "status": "failed",
            "error": "gemma_enrich завершился без gemma_enrichment_summary.json",
        }

    return summary


async def _run_prepare(
    project_id: str,
    force: bool,
    parallelism: Optional[int],
    model: Optional[str],
    timeout: Optional[int],
) -> dict:
    project_dir = resolve_project_dir(project_id)
    out_dir = project_dir / "_output"
    item = _find_item(project_id)
    assert item is not None
    item.status = "running"
    item.started_at = time.time()
    await _broadcast_queue()

    # Crop запускаем только сейчас, уже под глобальным lock'ом. Так Gemma 35B
    # не конкурирует за память/диск с crop'ом следующих проектов.
    _ensure_crop_started(project_id)
    crop_result = await _await_crop(project_id)
    if crop_result.get("error"):
        item.status = "failed"
        item.error = crop_result["error"][:300]
        await _broadcast_queue()
        await _ws_log(project_id, f"Crop не выполнен: {crop_result['error']}", "error")
        return {"status": "error", "stage": "crop", "error": crop_result["error"]}

    loop = asyncio.get_event_loop()

    project_info = load_project_info(project_dir)
    md_path = find_project_markdown(project_dir, project_info)
    if md_path is None:
        item.status = "failed"
        item.error = "MD-файл не найден"
        update_pipeline_log(
            project_id, "gemma_enrichment", "error",
            error="MD-файл не найден"
        )
        await _broadcast_queue()
        return {"status": "error", "error": "MD-файл не найден"}
    existing = ENRICHMENT_MARKER_RE.search(md_path.read_text(encoding="utf-8", errors="ignore")[:4096])
    summary_validation = validate_gemma_summary(project_dir, md_path=md_path)

    if summary_validation.get("valid") and not force:
        summary_existing = summary_validation.get("summary") or {}
        msg = (
            f"Gemma enrichment уже готов: {summary_existing.get('model')} "
            f"({summary_existing.get('blocks_ok')}/{summary_existing.get('blocks_total')}). "
            f"Force re-enrich для перезапуска."
        )
        await _ws_log(project_id, msg, "warn")
        item.status = "skipped"
        item.blocks_total = summary_existing.get("blocks_total")
        item.blocks_done = summary_existing.get("blocks_ok") or 0
        await _broadcast_queue()
        update_pipeline_log(
            project_id, "gemma_enrichment", "done",
            message=(
                f"Gemma summary валиден "
                f"({summary_existing.get('blocks_ok', '?')}/{summary_existing.get('blocks_total', '?')}, "
                f"{summary_existing.get('model', '?')})"
            )
        )
        result = {"status": "skipped", "existing": summary_existing, "crop": crop_result}
        return result

    if existing and not force:
        await _ws_log(
            project_id,
            "Старый ENRICHMENT marker найден, но summary/hash/policy невалидны "
            f"({summary_validation.get('reason')}) — запускаю Gemma заново",
            "warn",
        )

    if force and existing:
        await _ws_log(project_id, "Force re-enrich: backup _output/ ...", "warn")
        await loop.run_in_executor(None, lambda: _backup_output_for_reenrichment(out_dir))

    overrides = _resolve_overrides(project_dir)
    final_model = model or overrides.get("model") or DEFAULT_MODEL
    final_parallelism = parallelism or overrides.get("parallelism") or DEFAULT_PARALLELISM
    final_timeout = timeout or overrides.get("timeout") or DEFAULT_TIMEOUT_S

    await _ws_log(
        project_id,
        f"Запуск Gemma enrichment: model={final_model}, parallelism={final_parallelism}",
    )
    update_pipeline_log(project_id, "gemma_enrichment", "running")

    async def _on_event(event: dict) -> None:
        t = event.get("type")
        if t == "started":
            item.blocks_total = event["total"]
            item.blocks_done = 0
            item.blocks_failed = 0
            await _broadcast_queue()
            await _ws_log(project_id, f"Обработка {event['total']} image-блоков...")
        elif t == "block_done":
            item.blocks_done = event["completed"]
            if not event.get("ok"):
                item.blocks_failed += 1
            if event.get("truncated"):
                item.blocks_truncated += 1
            out_tok = event.get("output_tokens") or 0
            if out_tok > item.max_output_tokens_seen:
                item.max_output_tokens_seen = out_tok
            elapsed = time.time() - (item.started_at or time.time())
            item.elapsed_sec = round(elapsed, 1)
            done = max(1, event["completed"])
            avg_per_block = elapsed / done
            remaining = max(0, event["total"] - event["completed"])
            item.eta_sec = round(avg_per_block * remaining, 0)
            await _broadcast_queue()
            mark = "✓" if event["ok"] else "✗"
            err = f" — {event['error'][:80]}" if event.get("error") else ""
            level = "info" if event["ok"] else "warn"
            await _ws_log(
                project_id,
                f"[{event['completed']:>3}/{event['total']}] {mark} {event['block_id']} "
                f"p={event['page']} t={event['elapsed_ms']/1000:.1f}s{err}",
                level,
            )
            if event.get("truncated"):
                # Сигнал в лог — output обрезан max_output_tokens
                from gemma_enrich import DEFAULT_MAX_OUTPUT_TOKENS
                await _ws_log(
                    project_id,
                    f"⚠️ Output обрезан лимитом {DEFAULT_MAX_OUTPUT_TOKENS} tokens "
                    f"(блок {event['block_id']}, output_tokens={out_tok}). "
                    f"Если повторяется — увеличьте DEFAULT_MAX_OUTPUT_TOKENS в gemma_enrich.py.",
                    "warn",
                )
        elif t == "high_detail_candidates":
            await _ws_log(
                project_id,
                f"High-detail кандидаты: {event.get('candidates', 0)} из {event.get('total', 0)} блоков",
                "info",
            )
        elif t == "high_detail_prefilter":
            await _ws_log(
                project_id,
                f"High-detail prefilter: safe={event.get('safe_candidates', 0)}, "
                f"skipped_large={len(event.get('skipped_large_ids') or [])}",
                "warn" if event.get("skipped_large_ids") else "info",
            )
        elif t == "high_detail_block_done":
            mark = "✓" if event.get("ok") else "✗"
            level = "info" if event.get("ok") else "warn"
            err = f" — {event['error'][:80]}" if event.get("error") else ""
            await _ws_log(
                project_id,
                f"[HD {event['completed']:>3}/{event['total']}] {mark} {event['block_id']} "
                f"p={event['page']} t={event['elapsed_ms']/1000:.1f}s{err}",
                level,
            )
        elif t == "high_detail_retry_pass_started":
            await _ws_log(
                project_id,
                f"↻ High-detail retry-pass {event['attempt']}/{event['max_attempts']}: "
                f"повтор {event['to_retry']} блок(ов)",
                "warn",
            )
        elif t == "high_detail_retry_pass_done":
            await _ws_log(
                project_id,
                f"↻ High-detail retry-pass {event['attempt']}/{event['max_attempts']} завершён: "
                f"восстановлено {event['recovered']}, осталось {event['still_failed']}",
                "info" if event.get("recovered") else "warn",
            )
        elif t == "block_retry":
            # Auto-retry: предыдущая попытка обрезана, повторяем с увеличенным лимитом
            prev_tok = event.get("previous_output_tokens")
            await _ws_log(
                project_id,
                f"  ↻ Повтор {event['block_id']} с max_output_tokens={event['max_tokens']} "
                f"(прошлая попытка: {prev_tok} токенов, обрезано)",
                "warn",
            )
        elif t == "block_split":
            # Все токенные тиры truncated → блок «вытянутый» или плотный квадратный, режем.
            strategy = event.get("strategy", "")
            await _ws_log(
                project_id,
                f"  ✂ Split {event['block_id']} (aspect={event['aspect']:.2f}, "
                f"{strategy}, parts={event['parts']}) — режем и обрабатываем по частям",
                "warn",
            )
        elif t == "block_split_failed":
            await _ws_log(
                project_id,
                f"  ✂✗ Split {event['block_id']} упал: {event.get('error', '')}",
                "warn",
            )
        elif t == "retry_pass_started":
            # Project-level retry pass — добиваем упавшие блоки
            await _ws_log(
                project_id,
                f"↻ Retry-pass {event['attempt']}/{event['max_attempts']}: "
                f"повтор {event['to_retry']} упавших блок(ов)",
                "warn",
            )
        elif t == "retry_block_done":
            # Результат одного блока в retry-pass. blocks_done не трогаем (он
            # считал общий прогон), но blocks_failed корректируем по факту.
            mark = "✓" if event["ok"] else "✗"
            err = f" — {event['error'][:80]}" if event.get("error") else ""
            level = "info" if event["ok"] else "warn"
            if event["ok"]:
                # Блок восстановлен — снимаем его из failed.
                if item.blocks_failed > 0:
                    item.blocks_failed -= 1
                await _broadcast_queue()
            await _ws_log(
                project_id,
                f"  [retry {event['attempt']}/{event['max_attempts']}] {mark} {event['block_id']} "
                f"p={event['page']} t={event['elapsed_ms']/1000:.1f}s{err}",
                level,
            )
        elif t == "retry_pass_done":
            await _ws_log(
                project_id,
                f"↻ Retry-pass {event['attempt']}/{event['max_attempts']} завершён: "
                f"восстановлено {event['recovered']}, осталось упавших {event['still_failed']}",
                "info" if event["recovered"] else "warn",
            )
        elif t == "no_blocks":
            await _ws_log(project_id, "Нет image-блоков", "warn")

    summary = await _run_gemma_enrichment_subprocess(
        project_id,
        project_dir,
        item=item,
        force=force,
        model=final_model,
        parallelism=final_parallelism,
        timeout=final_timeout,
    )

    summary["crop"] = crop_result

    s_status = summary.get("status")
    s_ok = summary.get("blocks_ok", 0) or 0
    s_total = summary.get("blocks_total", 0) or 0
    s_failed = summary.get("blocks_failed", 0) or 0
    s_wall = summary.get("wall_clock_s", 0) or 0

    if s_status == "ok":
        item.status = "completed"
        update_pipeline_log(
            project_id, "gemma_enrichment", "done",
            message=f"OK ({s_ok}/{s_total} блоков, {s_wall:.0f}s)"
        )
    elif s_status == "partial":
        msg = f"partial: OK ({s_ok}/{s_total} блоков, {s_wall:.0f}s) — {s_failed} упали"
        item.status = "completed"
        update_pipeline_log(
            project_id, "gemma_enrichment", "partial",
            message=f"{msg}; partial mode допущен, непокрытые блоки попадут в отчёт",
            detail={
                "partial_allowed": True,
                "blocks_ok": s_ok,
                "blocks_total": s_total,
                "blocks_failed": s_failed,
                "uncovered_block_ids": summary.get("uncovered_block_ids", []),
                "uncovered_blocks": summary.get("uncovered_blocks", []),
            },
        )
    elif s_status == "failed":
        item.status = "failed"
        item.error = summary.get("error") or summary.get("reason") or "all blocks failed"
        update_pipeline_log(
            project_id, "gemma_enrichment", "error",
            error=str(item.error)[:300]
        )
    elif s_status == "skipped":
        item.status = "skipped"
        update_pipeline_log(
            project_id, "gemma_enrichment", "skipped",
            message=summary.get("reason") or "skipped"
        )
    elif s_status == "no_blocks":
        item.status = "completed"
        summary_path = project_dir / "_output" / "gemma_enrichment_summary.json"
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        update_pipeline_log(
            project_id, "gemma_enrichment", "done",
            message="image-блоков 0"
        )
    else:
        item.status = "failed"
        item.error = summary.get("error") or "unknown"
        update_pipeline_log(
            project_id, "gemma_enrichment", "error",
            error=str(item.error)[:300]
        )

    item.elapsed_sec = round(time.time() - (item.started_at or time.time()), 1)
    item.eta_sec = 0
    await _broadcast_queue()

    final_msg = (
        f"Готово: {summary.get('blocks_ok', 0)}/{summary.get('blocks_total', 0)} OK "
        f"за {summary.get('wall_clock_s', 0)}s"
    )
    await _ws_log(project_id, final_msg)

    # Финальные сигналы по truncation
    truncated_count = summary.get("blocks_truncated", 0) or 0
    max_seen = summary.get("max_output_tokens_seen", 0) or 0
    from gemma_enrich import DEFAULT_MAX_OUTPUT_TOKENS
    if truncated_count > 0:
        await _ws_log(
            project_id,
            f"⚠️ {truncated_count} блок(ов) остались truncated после всех ретраев (до 8192 tokens) и split-fallback. "
            f"Это очень плотные/большие блоки — enrichment частичный, но pipeline продолжится.",
            "warn",
        )
    elif max_seen > 0 and max_seen >= int(DEFAULT_MAX_OUTPUT_TOKENS * 0.8):
        usage_pct = int(max_seen / DEFAULT_MAX_OUTPUT_TOKENS * 100)
        await _ws_log(
            project_id,
            f"ℹ️ Макс. реально использовано {max_seen} токенов ({usage_pct}% от лимита {DEFAULT_MAX_OUTPUT_TOKENS}). "
            f"Близко к лимиту — может потребоваться повысить.",
            "warn",
        )
    return summary


# ─── Public API ───────────────────────────────────────────────────────────

async def start_retry_failed(project_id: str) -> dict:
    """Перепрогнать ТОЛЬКО упавшие блоки прошлого enrichment'а (без force/full re-run).

    Использует тот же Gemma-лок, чтобы не конфликтовать с обычным prepare.
    Не создаёт элемент в очереди (легковесная операция, обычно <минуту на блок).
    """
    existing_task = prepare_state.tasks.get(project_id)
    if existing_task is not None and not existing_task.done():
        return {"status": "already_running"}

    project_dir = resolve_project_dir(project_id)
    summary_path = project_dir / "_output" / "gemma_enrichment_summary.json"
    if not summary_path.exists():
        return {"status": "error", "error": "summary не найден — сначала надо сделать обычный prepare-data"}

    reset_cancel()
    _lmstudio_note_activity(f"prepare retry started for {project_id}")

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
            async with prepare_state.get_lock():
                result = await retry_failed_blocks(
                    project_dir,
                    progress_cb=_on_event,
                    pause_event=prepare_state.get_pause_event(),
                    cancel_event=prepare_state.get_cancel_event(),
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
            _schedule_lmstudio_post_queue_cleanup("prepare retry queue drained")

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
) -> dict:
    """Поставить project_id в очередь prepare-data. Не блокирует HTTP."""
    try:
        from webapp.services.pipeline_service import pipeline_manager
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
    _lmstudio_note_activity(f"prepare queue activity for {project_id}")
    # Если paused — оставим как есть, юзер сам resume'нёт

    # Если уже есть item для этого проекта в queue_status — обнуляем
    existing_item = _find_item(project_id)
    if existing_item:
        prepare_state.queue_status.items.remove(existing_item)
    # Старый crop-таск/результат — снести, чтобы новый enrichment начал с нуля
    old_crop = prepare_state.crop_tasks.pop(project_id, None)
    if old_crop is not None and not old_crop.done():
        old_crop.cancel()
    prepare_state.crop_results.pop(project_id, None)
    item = PrepareQueueItem(project_id=project_id, status="pending", force=force)
    prepare_state.queue_status.items.append(item)
    await _broadcast_queue()

    async def _wrapped() -> None:
        try:
            async with prepare_state.get_lock():
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
            _schedule_lmstudio_post_queue_cleanup("prepare queue drained")

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
            (item.project_id, item.force)
            for item in prepare_state.queue_status.items
            if item.status == "interrupted"
        ]
        if not interrupted:
            return {"resumed": False, "reason": "interrupted items not found"}
        for project_id, force in interrupted:
            await start_prepare_data(project_id, force=force)
        return {"resumed": True, "count": len(interrupted)}

    ev = prepare_state.get_pause_event()
    ev.set()
    await _broadcast_queue()
    return {"paused": False}


async def cancel_queue() -> dict:
    """Отменить очередь: текущие блоки доработают, остальные станут cancelled.

    Если на паузе — снимем паузу, чтобы runner смог увидеть cancel и выйти.
    """
    cev = prepare_state.get_cancel_event()
    cev.set()
    pev = prepare_state.get_pause_event()
    pev.set()  # снимаем паузу чтобы runner вышел
    # помечаем pending items как cancelled
    cancelled_pending = 0
    for it in prepare_state.queue_status.items:
        if it.status == "pending":
            it.status = "skipped"  # не запускался — пометим skipped
            it.error = "cancelled by user"
            cancelled_pending += 1
            # Отменяем фоновую crop-таску, если ещё не закончилась
            ct = prepare_state.crop_tasks.pop(it.project_id, None)
            if ct is not None and not ct.done():
                ct.cancel()
    await _broadcast_queue()
    return {"cancelled": True, "cancelled_pending": cancelled_pending}


def reset_cancel() -> None:
    """Сбросить cancel-event для следующего запуска (вызывается перед start_prepare_data)."""
    if prepare_state._cancel_event is not None:
        prepare_state._cancel_event.clear()
