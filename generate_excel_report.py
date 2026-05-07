"""
generate_excel_report.py — compatibility wrapper.
Delegates to backend.app.pipeline.stages.report.generate_excel_report.

Usage:
  python generate_excel_report.py
  python generate_excel_report.py projects/133-23-GK-EM1
  python generate_excel_report.py --out my_report.xlsx
  python generate_excel_report.py --no-summary
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.stages.report.generate_excel_report import *  # noqa: F401, F403
from backend.app.pipeline.stages.report.generate_excel_report import main  # noqa: F401

if __name__ == "__main__":
    main()
