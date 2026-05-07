"""
gemma_enrichment_contract.py — compatibility wrapper.
Delegates to backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract.
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import *  # noqa: F401, F403
