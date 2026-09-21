#!/usr/bin/env python3
"""Rebuild an auditable, versioned index of the Project Comparison catalog.

The production API reads the catalog dynamically (``/api/project-comparison/catalog``);
this script writes the same deterministic contract to a file for audit/handoff.
Read-only over comparison data, 0 model calls, no timestamps inside the index
itself (``catalog_revision`` is a digest of the entries).

    python scripts/build_project_comparison_catalog_index.py --out INDEX.json [--diagnostics]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--diagnostics", action="store_true", help="include excluded runs (no cards)")
    args = parser.parse_args()
    from backend.app.services.project_change_catalog import catalog

    index = catalog.build_catalog(include_diagnostics=args.diagnostics)
    raw = json.dumps(index, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    args.out.write_text(raw, encoding="utf-8")
    print(json.dumps({"out": str(args.out), "sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
                      "catalog_revision": index["catalog_revision"], **index["summary"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
