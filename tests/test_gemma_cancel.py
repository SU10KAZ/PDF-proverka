"""
test_gemma_cancel.py
--------------------
#14: Gemma не должна писать «отравленный» summary при отмене.

При отмене enrich_project возвращает status="cancelled" БЕЗ записи summary/MD.
runner ловит этот статус и возвращает StageResult.cancel() (как norms/text/block
runner'ы), вместо того чтобы пометить прерванный прогон как done/partial — иначе
resume/skip считает enrichment завершённым.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import backend.app.pipeline.stages.gemma_enrichment.runner as runner_mod
import backend.app.pipeline.stages.gemma_enrichment.gemma_enrich as gemma_enrich_mod
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    gemma_output_root,
)


class _FakeCtx:
    """Минимальный PipelineStageContext для runner-тестов."""

    def __init__(self, project_dir: Path):
        self.project_id = "test/proj"
        self.project_dir = project_dir
        self.project_info = {}
        self.is_cancelled = lambda: False
        self.pipeline_log_calls: list[tuple] = []

    def update_pipeline_log(self, stage, status, **kw):
        self.pipeline_log_calls.append((stage, status, kw))

    async def log(self, message, level="info"):
        return None


def _patch_preamble(monkeypatch, *, enrich_result: dict):
    """Обойти MD-gate и idempotency-skip, подменить enrich_project результатом."""
    monkeypatch.setattr(
        runner_mod, "find_project_markdown",
        lambda project_dir, project_info=None: project_dir / "doc_document.md",
    )
    monkeypatch.setattr(
        runner_mod, "evaluate_gemma_enrichment",
        lambda project_dir, project_info: {"ready": False, "status": "missing"},
    )

    async def _fake_enrich(*args, **kwargs):
        return dict(enrich_result)

    # enrich_project импортируется внутри функции из gemma_enrich модуля
    monkeypatch.setattr(gemma_enrich_mod, "enrich_project", _fake_enrich)


@pytest.mark.asyncio
async def test_cancelled_status_returns_cancel_without_summary(tmp_path, monkeypatch):
    proj = tmp_path / "proj"
    proj.mkdir()
    ctx = _FakeCtx(proj)
    _patch_preamble(monkeypatch, enrich_result={"status": "cancelled"})

    result = await runner_mod.run_gemma_enrichment_stage(ctx, force=True)

    # Stage отмечен как отменённый, НЕ успешный
    assert result.cancelled is True
    assert result.success is False

    # summary НЕ записан — прерванный прогон не должен считаться завершённым
    summary_path = gemma_output_root(proj) / "gemma_enrichment_summary.json"
    assert not summary_path.exists()
    assert not list(proj.rglob("gemma_enrichment_summary.json"))


@pytest.mark.asyncio
async def test_non_cancel_status_not_intercepted(tmp_path, monkeypatch):
    """Контроль: статус no_blocks не попадает в cancel-ветку, summary пишется."""
    proj = tmp_path / "proj"
    proj.mkdir()
    # no_blocks-ветка пишет summary без mkdir → создаём каталог заранее
    gemma_output_root(proj).mkdir(parents=True, exist_ok=True)
    ctx = _FakeCtx(proj)
    _patch_preamble(monkeypatch, enrich_result={"status": "no_blocks"})

    result = await runner_mod.run_gemma_enrichment_stage(ctx, force=True)

    assert result.cancelled is False
    assert result.success is True
    assert (result.data or {}).get("status") == "no_blocks"
    assert (gemma_output_root(proj) / "gemma_enrichment_summary.json").exists()
