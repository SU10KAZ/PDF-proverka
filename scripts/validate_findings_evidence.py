#!/usr/bin/env python3
"""CLI runner for Evidence Verifier."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from backend.app.services.findings import evidence_validation_service as evsvc


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Evidence Verifier on a project")
    parser.add_argument("project_id", help="Project id or path basename")
    parser.add_argument("--section", default="TX")
    parser.add_argument("--force", action="store_true", help="Ignore KB routing skips")
    parser.add_argument("--graphic-model", default="")
    parser.add_argument("--text-model", default="")
    args = parser.parse_args()

    data = evsvc.run_evidence_validation(
        args.project_id,
        section=args.section,
        graphic_model=args.graphic_model or None,
        text_model=args.text_model or None,
        force=args.force,
    )
    print(json.dumps({
        "processed": data["total_processed"],
        "skipped": data["skipped_count"],
        "errors": data["errors_count"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
