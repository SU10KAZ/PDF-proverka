"""
process_project.py — compatibility wrapper.
Delegates to backend.app.pipeline.stages.prepare.process_project.

Usage:
  python process_project.py <project_folder>
  python process_project.py projects/133-23-GK-EM1
"""
import sys
from pathlib import Path

# Ensure project root is on sys.path so backend.* imports resolve.
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.stages.prepare.process_project import main  # noqa: E402

if __name__ == "__main__":
    main()
