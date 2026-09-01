"""Experimental bounded-selector AI Analyst v3.

The package is deliberately not imported by the production orchestrator.
Only the explicit experiment runner may enable it.
"""

from .engine import BoundedSelectorAnalyst

__all__ = ["BoundedSelectorAnalyst"]
