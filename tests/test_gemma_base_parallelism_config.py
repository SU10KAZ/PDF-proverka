"""The retired Gemma stage no longer has runtime parallelism controls."""
from __future__ import annotations

import inspect

import backend.app.core.config as config
from backend.app.pipeline.stages.gemma_enrichment import runner


def test_gemma_parallelism_is_not_used_by_runtime_runner():
    assert "GEMMA_BASE_PARALLELISM" not in inspect.getsource(runner)
    assert "GEMMA_BASE_PARALLELISM" not in inspect.getsource(config)
