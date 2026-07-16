"""Compatibility stage delegates to the local block-context runner."""
from __future__ import annotations

import inspect

from backend.app.pipeline.stages.gemma_enrichment import runner


def test_legacy_runner_has_no_model_backed_path():
    source = inspect.getsource(runner)
    assert "run_block_context_stage" in source
    assert "enrich_project" not in source
    assert "CHANDRA_BASE_URL" not in source
