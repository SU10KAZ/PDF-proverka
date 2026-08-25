"""Production graphic comparison contracts for geometric and structural modes.

G1 owns registration/local diff. G2.4 adds only a versioned adapter for ready
SYSTEM_GRAPH comparison results; routing/extraction remain independent.
"""

from .contract import validate_ledger
from .graphic_change_ledger_adapter import adapt_system_graph_comparison_to_ledger
from .policy import EXPERIMENTALLY_CALIBRATED_V1, GraphicMode1Policy
from .router import compare_prepared_blocks

__all__ = [
    "EXPERIMENTALLY_CALIBRATED_V1",
    "GraphicMode1Policy",
    "adapt_system_graph_comparison_to_ledger",
    "compare_prepared_blocks",
    "validate_ledger",
]
