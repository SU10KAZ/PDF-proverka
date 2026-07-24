from __future__ import annotations

import asyncio
import inspect
import json
import time

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
