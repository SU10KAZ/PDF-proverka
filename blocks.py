"""
blocks.py — compatibility wrapper.
Delegates to backend.app.pipeline.stages.crop_blocks.blocks.

Usage:
  python blocks.py crop projects/<name>
  python blocks.py crop projects/<name> --block-ids A,B
  python blocks.py batches projects/<name>
  python blocks.py merge projects/<name>
  python blocks.py merge projects/<name> --cleanup
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.stages.crop_blocks.blocks import *  # noqa: F401, F403
from backend.app.pipeline.stages.crop_blocks.blocks import main  # noqa: F401

if __name__ == "__main__":
    main()
