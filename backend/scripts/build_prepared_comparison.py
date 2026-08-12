#!/usr/bin/env python3
"""Собрать PreparedDocument и диагностику для object-local comparison."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Скрипт можно запускать напрямую из корня репозитория без установки пакета.
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.app.services.stage_comparison.prepared_document import (
    build_stage_prepared_documents,
    write_prepared_diagnostic_report,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("comparison_dir", type=Path)
    args = parser.parse_args()
    models = []
    for name in ("stage_1", "stage_2"):
        stage_dir = args.comparison_dir / name
        if not stage_dir.is_dir():
            continue
        models.extend(document for document, _ in build_stage_prepared_documents(stage_dir))
    report = write_prepared_diagnostic_report(args.comparison_dir, models)
    print(f"Prepared documents: {len(models)}")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
