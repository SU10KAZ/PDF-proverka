"""Project Comparison V3 production engine (self-contained).

Runtime code must not import experiments/ or corpus-audits paths.
Inference is gated by PROJECT_COMPARISON_V3_ALLOW_INFERENCE (default 0).
"""
from .contracts import ENGINE_VERSION as ENGINE_VERSION
from .engine import run_v3_production_comparison

ENGINE_NAME = "projectchange_v3"

__all__ = [
    "ENGINE_NAME",
    "ENGINE_VERSION",
    "run_v3_production_comparison",
]
