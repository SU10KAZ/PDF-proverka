"""
gemma_findings_only.py — compatibility wrapper.
Delegates to backend.app.pipeline.stages.block_analysis.gemma_findings_only.
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.stages.block_analysis.gemma_findings_only import *  # noqa: F401, F403
