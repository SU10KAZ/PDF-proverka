"""Guards that the retired adaptive model runtime cannot return."""
from __future__ import annotations

import inspect

from backend.app.pipeline.stages.gemma_enrichment import gemma_enrich


def test_adaptive_reload_and_preflight_are_removed():
    assert not hasattr(gemma_enrich, "_adaptive_reload_to_context")
    assert not hasattr(gemma_enrich, "_preflight_loaded_context")
    assert not hasattr(gemma_enrich, "_enrich_block_single_pass")


def test_compatibility_adapter_has_no_network_ocr_code():
    source = inspect.getsource(gemma_enrich)
    assert "httpx" not in source
    assert "CHANDRA_BASE_URL" not in source
    assert "high_detail" not in source
    assert "split" not in source
