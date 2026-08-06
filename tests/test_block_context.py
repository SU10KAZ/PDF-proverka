from __future__ import annotations

import asyncio
import inspect
import json
import time
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import pytest

from backend.app.pipeline import manager as manager_mod
from backend.app.pipeline.stages.block_context import builder as builder_mod
from backend.app.pipeline.stages.block_context.builder import build_block_context
from backend.app.pipeline.stages.block_context.contract import (
    load_block_context_summary,
    validate_block_context_summary,
)


def _index(tmp_path, *, with_png=True):
    blocks_dir = tmp_path / "blocks_stage02_100"
    blocks_dir.mkdir()
    payload = {
        "blocks": [
            {"block_id": "B-1", "block_type": "image", "page": 1, "file": "block_B-1.png"},
            {"block_id": "B-2", "block_type": "image", "page": 2, "file": "block_B-2.png"},
        ]
    }
    path = blocks_dir / "index.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    if with_png:
        (blocks_dir / "block_B-1.png").write_bytes(b"png")
        (blocks_dir / "block_B-2.png").write_bytes(b"png")
    return path


@pytest.mark.asyncio
async def test_builder_writes_canonical_vector_and_image_context(tmp_path, monkeypatch):
    index = _index(tmp_path)

    def _resolve(_output, block_id, _page):
        if block_id == "B-1":
            return "QF1 C16", "structured_singleline"
        return None, "image_only"

    monkeypatch.setattr(builder_mod, "resolve_block_source", _resolve)
    summary = await build_block_context(
        tmp_path,
        output_dir=tmp_path,
        blocks_index_path=index,
    )

    assert summary["source_counts"] == {"structured_singleline": 1, "image_only": 1}
    assert summary["pipeline_block"] == "block_vector_graph"
    assert summary["pipeline_block_title"] == "Векторные графы блоков"
    assert summary["reference_catalog"]["records_total"] == 1133
    assert summary["reference_catalog"]["runtime_source"] == "pipeline_stage_embedded_catalog"
    assert (tmp_path / "block_context_summary.json").is_file()
    assert (tmp_path / "block_vector_graphs" / "B-1.json").is_file()
    assert summary["blocks"][0]["graph_artifact"] == "block_vector_graphs/B-1.json"
    assert not (tmp_path / "gemma_enrichment_summary.json").exists()
    assert validate_block_context_summary(tmp_path, canonical_only=True)["valid"] is True


@pytest.mark.asyncio
async def test_builder_keeps_event_loop_responsive_during_package_resolution(
    tmp_path,
    monkeypatch,
):
    index = _index(tmp_path)

    def _slow_resolve(_output, block_id, page, *, prefer_prepared):
        assert prefer_prepared is False
        time.sleep(0.05)
        return {
            "block_id": block_id,
            "page": page,
            "source_kind": "structured_architecture",
            "user_text": f"context for {block_id}",
        }

    monkeypatch.setattr(builder_mod, "resolve_block_package", _slow_resolve)

    event_loop_ticked = asyncio.Event()

    async def _tick_while_resolver_runs():
        await asyncio.sleep(0.01)
        event_loop_ticked.set()

    tick_task = asyncio.create_task(_tick_while_resolver_runs())
    await build_block_context(
        tmp_path,
        output_dir=tmp_path,
        blocks_index_path=index,
    )
    ticked_during_build = event_loop_ticked.is_set()
    await tick_task

    assert ticked_during_build


@pytest.mark.asyncio
async def test_blocks_are_resolved_in_parallel_but_reported_in_order(
    tmp_path,
    monkeypatch,
):
    """Параллельный разбор не должен менять порядок блоков и прогресса.

    B-1 медленнее B-2: на последовательном пути это невидимо, а на параллельном
    B-2 финиширует первым — сводка и progress_cb обязаны остаться в порядке
    index.json, иначе счётчик «N/M блоков» в очереди поедет.
    """
    index = _index(tmp_path)

    def _worker(_output_dir, block_id, page):
        time.sleep(0.20 if block_id == "B-1" else 0.0)
        return {
            "block_id": block_id,
            "page": page,
            "source_kind": "structured_architecture",
            "user_text": f"context for {block_id}",
        }

    pool = ThreadPoolExecutor(max_workers=2)
    monkeypatch.setattr(builder_mod, "_resolve_package_in_worker", _worker)
    monkeypatch.setattr(builder_mod, "_get_pool", lambda: pool)

    progress: list[str] = []

    async def _cb(event):
        if event.get("type") == "block_done":
            progress.append(event["block_id"])

    started = time.time()
    try:
        summary = await build_block_context(
            tmp_path,
            output_dir=tmp_path,
            blocks_index_path=index,
            progress_cb=_cb,
        )
    finally:
        pool.shutdown(wait=False)
    elapsed = time.time() - started

    assert progress == ["B-1", "B-2"]
    assert [item["block_id"] for item in summary["blocks"]] == ["B-1", "B-2"]
    # B-2 считался, пока спал B-1: последовательный путь дал бы ≥0.20+0.20.
    assert elapsed < 0.35


@pytest.mark.asyncio
async def test_broken_pool_falls_back_to_threads_without_losing_blocks(
    tmp_path,
    monkeypatch,
):
    """Смерть воркера (OOM в fitz) не должна валить стадию и терять блоки."""
    index = _index(tmp_path)

    class _DeadPool:
        def submit(self, _fn, *_args, **_kwargs):
            future = Future()
            future.set_exception(BrokenProcessPool("worker died"))
            return future

    # Владелец пула — общий cpu_pool (builder только подаёт в него задачи),
    # поэтому и флаг «пул отключён» проверяем там же.
    from backend.app.services.common import cpu_pool as cpu_pool_mod

    monkeypatch.setattr(cpu_pool_mod, "_POOL_DISABLED", False)
    monkeypatch.setattr(builder_mod, "_pool_applies", lambda: True)
    monkeypatch.setattr(builder_mod, "_get_pool", lambda: _DeadPool())

    in_thread: list[str] = []

    async def _fallback(block, _output_dir):
        block_id = str(block.get("block_id"))
        in_thread.append(block_id)
        return {
            "block_id": block_id,
            "page": block.get("page"),
            "source_kind": "structured_architecture",
            "user_text": f"context for {block_id}",
        }

    monkeypatch.setattr(builder_mod, "_resolve_in_thread", _fallback)

    summary = await build_block_context(
        tmp_path,
        output_dir=tmp_path,
        blocks_index_path=index,
    )

    assert in_thread == ["B-1", "B-2"]
    assert summary["blocks_ready"] == 2
    assert summary["status"] == "ok"
    assert cpu_pool_mod._POOL_DISABLED is True


def test_worker_count_is_bounded_and_overridable(monkeypatch):
    monkeypatch.setenv("BLOCK_CONTEXT_WORKERS", "3")
    assert builder_mod.block_context_workers() == 3

    # 1 воркер — прежнее последовательное поведение без пула вообще.
    monkeypatch.setenv("BLOCK_CONTEXT_WORKERS", "1")
    assert builder_mod.block_context_workers() == 1
    assert builder_mod._get_pool() is None

    monkeypatch.delenv("BLOCK_CONTEXT_WORKERS", raising=False)
    monkeypatch.setattr(builder_mod.os, "cpu_count", lambda: 16)
    # Два ядра оставляем бэкенду, но не больше потолка.
    assert builder_mod.block_context_workers() == builder_mod.DEFAULT_MAX_WORKERS
    monkeypatch.setattr(builder_mod.os, "cpu_count", lambda: 2)
    assert builder_mod.block_context_workers() == 1


def test_legacy_summary_is_read_through_adapter(tmp_path):
    (tmp_path / "gemma_enrichment_summary.json").write_text(
        json.dumps({
            "status": "ok",
            "blocks": [
                {"block_id": "B-1", "page": 1, "base_response_source": "vector_skip"},
                {"block_id": "B-2", "page": 2, "base_response_source": "stage_disabled_skip"},
            ],
        }),
        encoding="utf-8",
    )

    summary = load_block_context_summary(tmp_path)

    assert [item["source_kind"] for item in summary["blocks"]] == ["raw_vector", "image_only"]
    assert validate_block_context_summary(tmp_path)["valid"] is True
    assert validate_block_context_summary(tmp_path, canonical_only=True)["valid"] is False


def test_main_pipeline_context_path_has_no_prepare_lock_or_prefetch_start():
    stage_source = inspect.getsource(manager_mod.PipelineManager._run_gemma_enrichment_stage)
    queue_source = inspect.getsource(manager_mod.PipelineManager._run_batch_queue)

    assert "get_lock" not in stage_source
    assert "_run_gemma_prefetch_loop" not in queue_source
