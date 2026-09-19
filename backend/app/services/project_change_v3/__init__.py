"""Self-contained Project Comparison V3 production package."""
from .contracts import ENGINE_NAME, ENGINE_VERSION, MODEL, REASONING
from .engine import run_v3_pipeline, run_v3_production_comparison
from .provider import FakeProvider, reset_test_provider, set_test_provider

__all__ = [
    "ENGINE_NAME",
    "ENGINE_VERSION",
    "MODEL",
    "REASONING",
    "run_v3_production_comparison",
    "run_v3_pipeline",
    "FakeProvider",
    "set_test_provider",
    "reset_test_provider",
]
