"""Experimental whole-document AI analyst for Stage Comparison.

The package is deliberately separate from :mod:`stage_comparison.ai`: the
existing STANDARD path remains unchanged until the experiment has enough
evidence to justify a production migration.
"""

from .engine import WholeDocumentAnalyst

__all__ = ["WholeDocumentAnalyst"]
