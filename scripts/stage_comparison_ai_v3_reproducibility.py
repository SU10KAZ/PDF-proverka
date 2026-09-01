#!/usr/bin/env python3
"""Evaluate exactly three cache-disabled AI Analyst v3 run directories."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison.ai_v3.reproducibility import evaluate  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dirs", nargs=3, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = evaluate([value.resolve() for value in args.run_dirs])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    temporary.replace(args.output)
    print(json.dumps(result, ensure_ascii=False, indent=1))
    return 0 if result["verdict"] != "C" else 2


if __name__ == "__main__":
    raise SystemExit(main())
