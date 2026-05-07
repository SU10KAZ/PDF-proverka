"""
critic_v2/context — offline context enrichment layer for LLM taxonomy gate.

NOT connected to production pipeline. Read-only from project artifacts.

Public API:
    from .context_collector import ContextCollector, ContextCollectionStats
    from .context_models import FindingContextPackage, BlockSnippet
    from .context_models import TableContextRow, CrossReference, RelatedFinding
"""
from .context_collector import ContextCollector, ContextCollectionStats
from .context_models import (
    BlockSnippet,
    CrossReference,
    FindingContextPackage,
    RelatedFinding,
    TableContextRow,
)

__all__ = [
    "ContextCollector",
    "ContextCollectionStats",
    "FindingContextPackage",
    "BlockSnippet",
    "TableContextRow",
    "CrossReference",
    "RelatedFinding",
]
