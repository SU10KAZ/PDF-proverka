"""Phase 0 post-merge dedup package — class-key + fuzzy.

Production guarantees (see dedup_safety doc in production_preparation/):
  - КРИТИЧЕСКОЕ findings are never silently collapsed; two КРИТ in one
    cluster stay as separate canonicals (see class_dedup._split_critical_protected).
  - Output count never exceeds input count (hard-asserted).
  - Fail-open posture: callers wrap calls in try/except and proceed with the
    original findings on any error.

Gated behind STAGE01_DEDUP_ENABLED in `backend.app.core.config`. Default OFF;
on A0 baseline outputs the dedup pass is provably a no-op (validated on the
8-case dataset in experiments/md_analysis_comparison/algorithm_research/).

Public API:
    collapse_to_canonical(findings) -> (kept, DedupReport)
    mark_duplicates(findings) -> (annotated, DedupReport)
    merge_across_methods(method_to_findings, priority=None) -> (kept, DedupReport)
    fuzzy_dedup(findings, sim_threshold=0.7) -> (kept, DedupReport)
    DEFAULT_SIM_THRESHOLD
    DedupReport (alias for class_dedup.DedupReport — the report dataclass has
                 the same fields in both modules; consumer code only reads
                 well-known keys via .to_dict()).
"""
from __future__ import annotations

from .class_dedup import (
    DedupReport,
    collapse_to_canonical,
    derive_class_key,
    mark_duplicates,
    merge_across_methods,
)
from .fuzzy_dedup import (
    DEFAULT_SIM_THRESHOLD,
    fuzzy_dedup,
)

__all__ = [
    "DedupReport",
    "DEFAULT_SIM_THRESHOLD",
    "collapse_to_canonical",
    "mark_duplicates",
    "merge_across_methods",
    "derive_class_key",
    "fuzzy_dedup",
]
