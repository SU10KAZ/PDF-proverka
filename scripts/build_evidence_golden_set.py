#!/usr/bin/env python3
"""Build evidence golden set from decisions_log + findings."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from backend.app.pipeline.stages.findings_review.evidence_verifier.golden_set import (
    GOLDEN_SET_FILE,
    build_golden_set,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build evidence golden dataset")
    parser.add_argument("--limit", type=int, default=0, help="Max cases (0=all)")
    args = parser.parse_args()
    limit = args.limit or None
    payload = build_golden_set(limit=limit)
    print(json.dumps({
        "path": str(GOLDEN_SET_FILE),
        "total": payload["total_cases"],
        "by_class": payload["by_class"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
