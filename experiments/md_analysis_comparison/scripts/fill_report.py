"""Substitute {TABLE_PLACEHOLDER} and {SUMMARY_PLACEHOLDER} in the report
with the latest comparison outputs.

Idempotent: keeps the report skeleton and only updates the two blocks.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from configs import config as cfg  # noqa: E402

REPORT = cfg.REPORTS_DIR / "final_comparison_report.md"


def main():
    if not REPORT.exists():
        sys.exit(f"Report not found: {REPORT}")
    table = (cfg.COMPARISON_OUTPUTS_DIR / "table.md")
    summary = (cfg.COMPARISON_OUTPUTS_DIR / "summary.json")
    if not table.exists() or not summary.exists():
        sys.exit("Run scripts/compare_results.py first.")
    table_text = table.read_text(encoding="utf-8").strip()
    summary_text = json.dumps(json.loads(summary.read_text(encoding="utf-8")),
                              ensure_ascii=False, indent=2)
    body = REPORT.read_text(encoding="utf-8")
    body = body.replace("{TABLE_PLACEHOLDER}", table_text)
    body = body.replace("{SUMMARY_PLACEHOLDER}", summary_text)
    REPORT.write_text(body, encoding="utf-8")
    print(f"Filled placeholders in {REPORT}")


if __name__ == "__main__":
    main()
