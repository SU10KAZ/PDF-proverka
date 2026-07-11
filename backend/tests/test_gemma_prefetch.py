"""Guards for the removal of the obsolete model-backed prefetch path."""
from __future__ import annotations

import inspect

from backend.app.pipeline import manager as manager_mod
from backend.app.pipeline.stages.prepare import prepare_service


def test_batch_queue_does_not_start_model_prefetch():
    source = inspect.getsource(manager_mod.PipelineManager._run_batch_queue)
    assert "_run_gemma_prefetch_loop" not in source
    assert "pregemma_task" not in source


def test_main_context_stage_does_not_take_prepare_lock():
    source = inspect.getsource(manager_mod.PipelineManager._run_gemma_enrichment_stage)
    assert "prepare_state.get_lock" not in source
    assert "_run_block_context_stage_fn" in source


def test_prepare_queue_does_not_take_model_lock_or_schedule_lmstudio_cleanup():
    source = inspect.getsource(prepare_service.start_prepare_data)
    assert "prepare_state.get_lock" not in source
    assert "lmstudio" not in source.lower()
