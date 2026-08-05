"""Запуск аудита не должен пересобирать контекст, готовый «Подготовкой данных».

Раньше run_block_context_stage делал `del force` и строил контекст ВСЕГДА —
после подготовки кнопка «Запустить» гоняла этап заново. Теперь готовый и
покрывающий текущий index.json контекст переиспользуется, а force
(retry/resume этапа) по-прежнему пересобирает.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.pipeline.stages.block_context import runner as ctx_runner
from backend.app.pipeline.stages.block_context.contract import (
    BLOCK_CONTEXT_SUMMARY_FILENAME,
    SCHEMA_VERSION,
    STAGE,
    block_context_up_to_date,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    STAGE02_BLOCKS_DIRNAME,
)


def _write_index(output_dir: Path, block_ids: list[str]) -> Path:
    blocks_dir = output_dir / STAGE02_BLOCKS_DIRNAME
    blocks_dir.mkdir(parents=True, exist_ok=True)
    index_path = blocks_dir / "index.json"
    index_path.write_text(json.dumps({
        "total_blocks": len(block_ids),
        "blocks": [
            {"block_id": bid, "file": f"block_{bid}.png", "page": 1}
            for bid in block_ids
        ],
    }), encoding="utf-8")
    return index_path


def _write_summary(output_dir: Path, block_ids: list[str]) -> None:
    (output_dir / BLOCK_CONTEXT_SUMMARY_FILENAME).write_text(json.dumps({
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "status": "ok",
        "blocks_total": len(block_ids),
        "blocks_ready": len(block_ids),
        "blocks_failed": 0,
        "reference_catalog": {
            "runtime_source": "pipeline_stage_embedded_catalog",
            "records_total": 42,
            "catalog_version": "test-1",
        },
        "source_counts": {"raw_vector": len(block_ids)},
        "blocks": [
            {"block_id": bid, "source_kind": "raw_vector", "coverage_status": "ready"}
            for bid in block_ids
        ],
    }), encoding="utf-8")


def _make_ctx(tmp_path: Path, calls: list[tuple]) -> SimpleNamespace:
    return SimpleNamespace(
        project_dir=tmp_path / "project",
        project_id="TEST/PID",
        output_dir=tmp_path / "_output",
        progress_sync=None,
        update_pipeline_log=lambda stage, status, **kw: calls.append((stage, status, kw)),
    )


@pytest.fixture
def prepared(tmp_path):
    output_dir = tmp_path / "_output"
    output_dir.mkdir(parents=True)
    _write_index(output_dir, ["AAA", "BBB"])
    _write_summary(output_dir, ["AAA", "BBB"])
    return tmp_path


def test_ready_context_is_reused(prepared, monkeypatch):
    async def _fail_build(*a, **kw):
        raise AssertionError("контекст не должен пересобираться")

    monkeypatch.setattr(ctx_runner, "build_block_context", _fail_build)
    calls: list[tuple] = []

    result = asyncio.run(ctx_runner.run_block_context_stage(_make_ctx(prepared, calls)))

    message = (result.data or {})["message"]
    assert result.success
    assert "пропускаю" in message
    assert ("block_context", "done", {"message": message}) in calls
    # legacy-ключ для старых клиентов UI/API остаётся синхронным
    assert ("gemma_enrichment", "done", {"message": message}) in calls


def test_force_rebuilds_even_when_ready(prepared, monkeypatch):
    built: list[str] = []

    async def _build(project_dir, **kw):
        built.append(str(project_dir))
        return {"status": "ok", "blocks_total": 2, "blocks_ready": 2, "blocks_failed": 0}

    monkeypatch.setattr(ctx_runner, "build_block_context", _build)

    result = asyncio.run(
        ctx_runner.run_block_context_stage(_make_ctx(prepared, []), force=True)
    )

    assert built, "force обязан пересобрать контекст"
    assert result.success


def test_new_blocks_after_recrop_break_the_skip(prepared):
    # докроп добавил блок CCC, которого нет в сводке
    _write_index(prepared / "_output", ["AAA", "BBB", "CCC"])

    state = block_context_up_to_date(prepared / "_output")

    assert state["ready"] is False
    assert "CCC" in state["uncovered"]


def test_missing_summary_is_not_ready(tmp_path):
    output_dir = tmp_path / "_output"
    output_dir.mkdir(parents=True)
    _write_index(output_dir, ["AAA"])

    assert block_context_up_to_date(output_dir)["ready"] is False


def test_missing_crop_index_is_not_ready(tmp_path):
    output_dir = tmp_path / "_output"
    output_dir.mkdir(parents=True)
    _write_summary(output_dir, ["AAA"])

    state = block_context_up_to_date(output_dir)

    assert state["ready"] is False
    assert "index.json" in state["reason"]
