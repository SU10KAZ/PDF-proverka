"""Regression guards for the removed model-backed queue prefetch."""
from __future__ import annotations

from backend.app.models.audit import BatchQueueItem
from backend.app.pipeline.manager import PipelineManager


def test_model_prefetch_methods_are_removed():
    assert not hasattr(PipelineManager, "_select_pregemma_candidate")
    assert not hasattr(PipelineManager, "_run_gemma_prefetch_loop")
    assert not hasattr(PipelineManager, "_main_wants_gemma_lock")


def test_legacy_prefetch_queue_state_is_read_compatible_but_not_serialized():
    item = BatchQueueItem.model_validate({
        "project_id": "legacy",
        "gemma_prefetched": True,
        "gemma_prefetch_status": "done",
    })
    payload = item.model_dump()
    assert "gemma_prefetched" not in payload
    assert "gemma_prefetch_status" not in payload
