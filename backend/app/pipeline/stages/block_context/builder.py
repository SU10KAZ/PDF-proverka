"""Build block context locally from PDF vectors, Vectograph profiles, or PNG."""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import multiprocessing
import os
import threading
from concurrent.futures import BrokenExecutor, ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from backend.app.pipeline.stages.block_grounding.block_profile_registry import (
    ARTIFACT_DIRNAME,
    artifact_filename,
    make_package,
)
from backend.app.pipeline.stages.block_grounding.block_source_router import (
    resolve_block_package,
    resolve_block_package as _canonical_resolve_block_package,
    resolve_block_source as _canonical_resolve_block_source,
)
from backend.app.pipeline.stages.block_context.reference_catalog import catalog_runtime_info

# Публичное имя оставлено для совместимости расширений/тестов, которые подменяли
# прежний двухэлементный резолвер builder.resolve_block_source.
resolve_block_source = _canonical_resolve_block_source
from backend.app.services.storage.stage_artifacts import BLOCK_CONTEXT_SUMMARY_FILENAME

from .contract import SCHEMA_VERSION, STAGE

ProgressCb = Callable[[dict[str, Any]], Awaitable[None] | None]

# ─── Пул процессов для разбора вектор-слоя ────────────────────────────────
#
# Разбор одного блока — чистый CPU (fitz + геометрия профиля, ~1–1.5 с на
# крупный АР-лист) без сети и без модели. В потоках (asyncio.to_thread) он
# упирается в GIL: несколько проектов «Выполняется» одновременно, но реально
# считает одно ядро — bench на 16-ядерной машине показывал 85% CPU у бэкенда
# и 5–22 с на блок при чистых 1–1.5 с. Пул процессов снимает именно это.
#
# Пул один на процесс бэкенда и общий для всех проектов очереди: иначе N
# проектов × M воркеров вынесли бы машину. Метод старта — spawn: fork из
# многопоточного uvicorn-процесса рискует дедлоком в дочернем.

_POOL_LOCK = threading.Lock()
_POOL: ProcessPoolExecutor | None = None
_POOL_DISABLED = False

DEFAULT_MAX_WORKERS = 8


def block_context_workers() -> int:
    """Сколько блоков считать параллельно. BLOCK_CONTEXT_WORKERS=1 → как раньше."""
    raw = (os.environ.get("BLOCK_CONTEXT_WORKERS") or "").strip()
    if raw:
        try:
            requested = int(raw)
        except ValueError:
            requested = 0
        if requested > 0:
            return requested
    cpu = os.cpu_count() or 1
    # Оставляем 2 ядра под сам бэкенд (HTTP/WS не должны голодать).
    return max(1, min(DEFAULT_MAX_WORKERS, cpu - 2))


def _get_pool() -> ProcessPoolExecutor | None:
    """Ленивый общий пул; None — работаем в потоке (как до параллелизации)."""
    global _POOL, _POOL_DISABLED
    if _POOL_DISABLED:
        return None
    workers = block_context_workers()
    if workers <= 1:
        return None
    with _POOL_LOCK:
        if _POOL_DISABLED:
            return None
        if _POOL is None:
            try:
                _POOL = ProcessPoolExecutor(
                    max_workers=workers,
                    mp_context=multiprocessing.get_context("spawn"),
                )
            except Exception as exc:  # окружение без права форка и т.п.
                _POOL_DISABLED = True
                print(f"[block_context] пул процессов недоступен ({exc}); считаем в потоке")
                return None
        return _POOL


def _disable_pool(reason: str) -> None:
    """Пул сломался — досчитываем в потоке, стадия не падает."""
    global _POOL, _POOL_DISABLED
    with _POOL_LOCK:
        _POOL_DISABLED = True
        pool, _POOL = _POOL, None
    print(f"[block_context] пул процессов отключён: {reason}")
    if pool is not None:
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass


def shutdown_pool() -> None:
    """Погасить пул (вызывается на shutdown бэкенда; в норме не нужен)."""
    global _POOL
    with _POOL_LOCK:
        pool, _POOL = _POOL, None
    if pool is not None:
        pool.shutdown(wait=False, cancel_futures=True)


def _resolve_package_in_worker(output_dir: str, block_id: str, page: Any) -> dict[str, Any]:
    """Точка входа дочернего процесса (должна быть на уровне модуля — pickle)."""
    from backend.app.pipeline.stages.block_grounding.block_source_router import (
        resolve_block_package as _resolve,
    )

    return _resolve(Path(output_dir), block_id, page, prefer_prepared=False)


async def _emit(callback: ProgressCb | None, event: dict[str, Any]) -> None:
    if callback is None:
        return
    result = callback(event)
    if inspect.isawaitable(result):
        await result


def _hash_text(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


async def _resolve_in_thread(block: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    """Прежний путь: разбор в потоке. Держит event loop живым, но упирается в GIL."""
    block_id = str(block.get("block_id") or "")
    page = block.get("page")
    if resolve_block_source is not _canonical_resolve_block_source:
        legacy_text, legacy_source = await asyncio.to_thread(
            resolve_block_source,
            output_dir,
            block_id,
            page,
        )
        return make_package(
            block_id=block_id, page=page, source_kind=legacy_source,
            user_text=legacy_text,
        )
    return await asyncio.to_thread(
        resolve_block_package,
        output_dir,
        block_id,
        page,
        prefer_prepared=False,
    )


def _pool_applies() -> bool:
    """Подменённые резолверы (тесты/расширения) в дочерний процесс не уедут."""
    return (
        resolve_block_source is _canonical_resolve_block_source
        and resolve_block_package is _canonical_resolve_block_package
    )


async def _iter_resolved(blocks: list[dict[str, Any]], output_dir: Path):
    """Отдать (block, package) СТРОГО по порядку, считая блоки параллельно.

    Порядок сохранён намеренно: артефакты, счётчики и прогресс остаются такими
    же, как на последовательном пути, — параллелится только сам разбор.
    """
    pool = _get_pool() if _pool_applies() else None
    if pool is None:
        for block in blocks:
            yield block, await _resolve_in_thread(block, output_dir)
        return

    loop = asyncio.get_running_loop()
    # Окно чуть больше числа воркеров: пул не простаивает, пока главный
    # процесс пишет артефакт очередного блока.
    window = max(2, block_context_workers() * 2)
    remaining = iter(blocks)
    pending: list[tuple[dict[str, Any], asyncio.Future]] = []

    def _submit_next() -> bool:
        block = next(remaining, None)
        if block is None:
            return False
        future = loop.run_in_executor(
            pool,
            _resolve_package_in_worker,
            str(output_dir),
            str(block.get("block_id") or ""),
            block.get("page"),
        )
        pending.append((block, future))
        return True

    try:
        while len(pending) < window and _submit_next():
            pass
        while pending:
            block, future = pending.pop(0)
            try:
                package = await future
            except asyncio.CancelledError:
                raise
            except BrokenExecutor as exc:
                # Воркер умер (OOM/сегфолт в fitz) — пул уже не восстановить:
                # гасим его и досчитываем ВЕСЬ остаток в потоке, без падения стадии.
                _disable_pool(f"{type(exc).__name__}: {exc}")
                leftovers = [item for item, fut in pending]
                for _, fut in pending:
                    fut.cancel()
                pending.clear()
                for item in [block, *leftovers, *remaining]:
                    yield item, await _resolve_in_thread(item, output_dir)
                return
            except Exception as exc:
                # Единичный сбой (например, непиклящийся пакет) — только этот блок.
                print(
                    f"[block_context] блок {block.get('block_id')}: "
                    f"пул вернул ошибку ({type(exc).__name__}: {exc}); считаю в потоке"
                )
                package = await _resolve_in_thread(block, output_dir)
            _submit_next()
            yield block, package
    finally:
        for _, fut in pending:
            fut.cancel()


async def build_block_context(
    project_dir: Path,
    *,
    output_dir: Path,
    blocks_index_path: Path,
    progress_cb: ProgressCb | None = None,
) -> dict[str, Any]:
    """Create the canonical summary without any model or network call."""
    project_dir = Path(project_dir)
    output_dir = Path(output_dir)
    if not blocks_index_path.is_file():
        raise FileNotFoundError(f"{blocks_index_path} не найден")
    index = json.loads(blocks_index_path.read_text(encoding="utf-8"))
    blocks = [
        item for item in index.get("blocks") or []
        if isinstance(item, dict) and str(item.get("block_type") or "").lower() == "image"
    ]
    await _emit(progress_cb, {"type": "started", "total": len(blocks)})

    prepared: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    profile_counts: dict[str, int] = {}
    reference_selection_counts: dict[str, int] = {}
    reference_confidence_counts: dict[str, int] = {}
    graph_dir = output_dir / ARTIFACT_DIRNAME
    graph_dir.mkdir(parents=True, exist_ok=True)
    expected_artifacts: set[str] = set()
    # PDF/vector parsing is CPU-heavy and synchronous: держим его вне event loop
    # (иначе API и WebSocket висли всю стадию) и считаем блоки параллельно
    # процессами — потоки на этом упирались в GIL.
    pos = 0
    async for block, package in _iter_resolved(blocks, output_dir):
        pos += 1
        block_id = str(block.get("block_id") or "")
        page = block.get("page")
        text = package.get("user_text")
        source = str(package.get("source_kind") or "error")
        if source == "gemma_fallback":
            source = "image_only"
        png = blocks_index_path.parent / str(block.get("file") or "")
        warnings: list[str] = []
        if source in {"no_sources", "block_not_found", "error"} and png.is_file():
            warnings.append(f"{source}: fallback to image")
            source = "image_only"
        if source == "image_only" and not png.is_file():
            source = "missing"
            warnings.append("PNG missing")
        if source != package.get("source_kind"):
            package["source_kind"] = source
            package["user_text"] = text
        if warnings:
            package["warnings"] = warnings
        coverage = "ready_image_only" if source == "image_only" else (
            "ready" if source != "missing" else "error"
        )
        counts[source] = counts.get(source, 0) + 1
        profile_id = str(package.get("profile_id") or "")
        if profile_id:
            profile_counts[profile_id] = profile_counts.get(profile_id, 0) + 1
        reference = package.get("reference") or {}
        selection_mode = str(reference.get("selection_mode") or (
            "embedded_profile_grammar" if not reference.get("block_id") else "catalog_reference"
        ))
        reference_selection_counts[selection_mode] = (
            reference_selection_counts.get(selection_mode, 0) + 1
        )
        confidence = str(reference.get("selection_confidence") or "not_scored")
        reference_confidence_counts[confidence] = (
            reference_confidence_counts.get(confidence, 0) + 1
        )
        artifact_name = artifact_filename(block_id)
        artifact_target = graph_dir / artifact_name
        artifact_temp = artifact_target.with_suffix(".json.tmp")
        artifact_temp.write_text(
            json.dumps(
                package, ensure_ascii=False, default=str,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        os.replace(artifact_temp, artifact_target)
        expected_artifacts.add(artifact_name)
        prepared.append({
            "block_id": block_id,
            "page": page,
            "source_kind": source,
            "coverage_status": coverage,
            "context_hash": _hash_text(text),
            "discipline": package.get("discipline"),
            "profile_id": package.get("profile_id"),
            "reference": package.get("reference"),
            "readiness": package.get("readiness"),
            "graph_artifact": f"{ARTIFACT_DIRNAME}/{artifact_name}",
            "warnings": warnings,
        })
        await _emit(progress_cb, {
            "type": "block_done",
            "block_id": block_id,
            "page": page,
            "source_kind": source,
            "profile_id": package.get("profile_id"),
            "ok": coverage != "error",
            "completed": pos,
            "total": len(blocks),
        })

    # Удаляем только устаревшие JSON этого собственного каталога: иначе UI может
    # показать граф блока, которого уже нет в новом crop/index.json.
    for stale in graph_dir.glob("*.json"):
        if stale.name not in expected_artifacts:
            try:
                stale.unlink()
            except OSError:
                pass

    ready = sum(item["coverage_status"] != "error" for item in prepared)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "pipeline_block": "block_vector_graph",
        "pipeline_block_title": "Векторные графы блоков",
        "status": "ok" if ready == len(prepared) else ("partial" if ready else "failed"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "project_dir": str(project_dir),
        "blocks_total": len(prepared),
        "blocks_ready": ready,
        "blocks_failed": len(prepared) - ready,
        "source_counts": counts,
        "profile_counts": profile_counts,
        "reference_catalog": catalog_runtime_info(),
        "reference_selection_counts": reference_selection_counts,
        "reference_confidence_counts": reference_confidence_counts,
        "blocks": prepared,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / BLOCK_CONTEXT_SUMMARY_FILENAME
    temp = target.with_suffix(target.suffix + ".tmp")
    temp.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, target)
    await _emit(progress_cb, {"type": "completed", "summary": summary})
    return summary
