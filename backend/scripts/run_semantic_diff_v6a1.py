#!/usr/bin/env python3
"""Run the frozen 12-group Stage 6A.1 benchmark without any LLM calls."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[2]
if str(REPOSITORY) not in sys.path:
    sys.path.insert(0, str(REPOSITORY))

from backend.app.services.stage_comparison.semantic_diff_v6a1 import run_pilot, write_report


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _one(root: Path, pattern: str) -> Path:
    matches = sorted(root.glob(pattern))
    if len(matches) != 1:
        raise RuntimeError(f"expected_one_file:{root}:{pattern}:found={len(matches)}")
    return matches[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("comparison", type=Path, help="Object comparison directory")
    args = parser.parse_args()
    root = args.comparison.resolve()

    left_pdf = _one(root / "stage_1", "documents/*/versions/*/02_work/document.pdf")
    right_pdf = _one(root / "stage_2", "documents/*/versions/*/02_work/document.pdf")
    left_prepared = _one(root / "stage_1", "documents/*/versions/*/03_analysis/latest/prepared_comparison/prepared_document.json")
    right_prepared = _one(root / "stage_2", "documents/*/versions/*/03_analysis/latest/prepared_comparison/prepared_document.json")
    change_detection = root / "change_detection" / "change_detection.json"
    baseline = root / "semantic_diff_v6a" / "semantic_diff.json"
    destination = root / "semantic_diff_v6a1"

    report = run_pilot(
        left_pdf,
        right_pdf,
        _load(left_prepared),
        _load(right_prepared),
        _load(change_detection),
        destination,
        baseline_v6a=_load(baseline) if baseline.exists() else None,
    )
    json_path, markdown_path = write_report(destination, report)
    print(json.dumps({
        "semantic_diff": str(json_path),
        "markdown": str(markdown_path),
        "summary": report["summary"],
    }, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
