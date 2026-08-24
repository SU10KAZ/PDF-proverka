"""Production graphic comparison of already prepared block pairs.

G1 deliberately contains only the registration/local-diff mode.  A request
that needs structural comparison is routed to ``MODE_2_REQUIRED`` and stops.
"""

from .contract import validate_ledger
from .policy import EXPERIMENTALLY_CALIBRATED_V1, GraphicMode1Policy
from .router import compare_prepared_blocks

__all__ = [
    "EXPERIMENTALLY_CALIBRATED_V1",
    "GraphicMode1Policy",
    "compare_prepared_blocks",
    "validate_ledger",
]
