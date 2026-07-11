"""Static guards for the local block-context runtime."""
from __future__ import annotations

import inspect

import backend.app.core.config as config
from backend.app.pipeline import manager
from backend.app.pipeline.stages.block_context import builder, runner
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    STAGE02_BLOCKS_DIRNAME,
)
from backend.app.pipeline.stages.prepare import prepare_service


def test_obsolete_runtime_flags_are_removed():
    config_source = inspect.getsource(config)
    assert "GEMMA_STAGE_DISABLED" not in config_source
    assert "GEMMA_SKIP_VECTOR_BLOCKS_ENABLED" not in config_source
    assert "BLOCK_SOURCE_ROUTER_ENABLED" not in config_source


def test_production_runtime_does_not_import_network_ocr_runner():
    runtime_source = "\n".join((
        inspect.getsource(manager),
        inspect.getsource(prepare_service),
        inspect.getsource(runner),
        inspect.getsource(builder),
    ))
    assert "gemma_enrichment.gemma_enrich import" not in runtime_source
    assert "CHANDRA_BASE_URL" not in runtime_source


def test_context_builder_uses_canonical_stage01_crop_directory():
    source = inspect.getsource(runner)
    assert STAGE02_BLOCKS_DIRNAME == "blocks_stage02_100"
    assert "STAGE02_BLOCKS_DIRNAME" in source
    assert "blocks_gemma_100" not in inspect.getsource(builder)
