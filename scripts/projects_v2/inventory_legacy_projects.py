#!/usr/bin/env python3
"""
inventory_legacy_projects.py — READ-ONLY инвентаризация старой структуры.

Читает `projects/` и формирует:
  projects_v2/_system/migration_inventory.json
  projects_v2/_system/migration_inventory.csv
  projects_v2/_system/schema.json   (стандарт раскладки)

НЕ изменяет и НЕ удаляет ничего в `projects/`. Пишет только в `projects_v2/`.

Использование:
  python scripts/projects_v2/inventory_legacy_projects.py
  python scripts/projects_v2/inventory_legacy_projects.py --projects-root <path> --v2-root <path>
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Read-only inventory of legacy projects/")
    parser.add_argument("--projects-root", default=None, help="legacy projects/ (default: <repo>/projects)")
    parser.add_argument("--v2-root", default=None, help="projects_v2/ (default: <repo>/projects_v2)")
    args = parser.parse_args(argv)

    projects_root = Path(args.projects_root).resolve() if args.projects_root else v2lib.legacy_projects_root()
    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()

    if not projects_root.is_dir():
        print(f"[ERROR] projects root not found: {projects_root}", file=sys.stderr)
        return 2

    v2lib.ensure_v2_skeleton(v2_root)
    objects_map = v2lib.load_objects_map()
    rows = v2lib.build_inventory(projects_root, objects_map)

    json_path = v2_root / "_system" / "migration_inventory.json"
    csv_path = v2_root / "_system" / "migration_inventory.csv"
    v2lib.write_inventory(rows, json_path, csv_path)

    containers = sum(1 for r in rows if r["kind"] == "container")
    with_warnings = sum(1 for r in rows if r["warnings"])
    print(f"[OK] projects inventoried: {len(rows)}")
    print(f"     containers (main):    {containers}")
    print(f"     plain projects:       {len(rows) - containers}")
    print(f"     with warnings:        {with_warnings}")
    print(f"     -> {json_path}")
    print(f"     -> {csv_path}")
    print(f"     -> {v2_root / '_system' / 'schema.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
