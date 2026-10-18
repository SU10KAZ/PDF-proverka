"""Prepare a Version Comparison pilot from existing page word caches.

This is initial MD preparation when no authoritative results.md exists. It
never supplements/replaces an existing MD, extracts PDF text or reads raster.
PDF vector rulings locate cells; every cached word is accounted for in receipts.
Input is an explicit list of projects. All adjacent versions are prepared by
default; --latest-pair restricts a diagnostic pilot to the last transition.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.app.services.version_comparison.specification_prepare import (  # noqa: E402,F401
    prepare, page_table, cell_text, inside, digest,
)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--object-dir', required=True, type=Path)
    parser.add_argument('--project', action='append', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--latest-pair', action='store_true')
    args = parser.parse_args()
    print(json.dumps(prepare(args.object_dir,args.project,args.output,latest_pair=args.latest_pair),ensure_ascii=False))
