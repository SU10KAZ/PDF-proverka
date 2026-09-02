"""Isolated AI sheet-matching research harness.

Nothing in this package is imported by the production Stage Comparison flow.
"""

from .core import (
    DECISION_TYPES,
    ProjectDataset,
    aggregate_decisions,
    build_candidate_recall,
    build_project_dataset,
    build_selector_prompt,
    selector_schema,
    verify_selector_response,
)

__all__ = [
    "DECISION_TYPES",
    "ProjectDataset",
    "aggregate_decisions",
    "build_candidate_recall",
    "build_project_dataset",
    "build_selector_prompt",
    "selector_schema",
    "verify_selector_response",
]
