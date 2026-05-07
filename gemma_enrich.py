"""
gemma_enrich.py — compatibility wrapper.
Delegates to backend.app.pipeline.stages.gemma_enrichment.gemma_enrich.

Usage:
  python gemma_enrich.py projects/<name>
  python gemma_enrich.py projects/<name> --force
  python gemma_enrich.py projects/<name> --parallelism 3
"""
import sys
import asyncio
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.stages.gemma_enrichment.gemma_enrich import *  # noqa: F401, F403
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrich import _cli  # noqa: F401

if __name__ == "__main__":
    raise SystemExit(asyncio.run(_cli()))
