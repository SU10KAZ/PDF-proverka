#!/usr/bin/env python3
"""
report_migration_readiness.py — READ-ONLY отчёт готовности к миграции.

Читает:
  * legacy `projects/`;
  * `projects_v2/_system/migration_inventory.json` (для сверки тоталов).

Пишет (только в projects_v2/_system/):
  * migration_readiness_report.json
  * migration_readiness_report.csv

Ничего не копирует и не изменяет в legacy.

Использование:
  python scripts/projects_v2/report_migration_readiness.py
  python scripts/projects_v2/report_migration_readiness.py --projects-root <p> --v2-root <p>
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib       # noqa: E402
import readiness   # noqa: E402

CSV_FIELDS = [
    "group", "object", "object_id", "discipline", "project_name", "document_code",
    "kind", "version_count",
    "has_pdf", "has_document_md", "has_ocr_html", "has_result_json",
    "has_project_info", "has_output", "has_analysis", "has_version_group",
    "pdf_named_version_folder", "multiple_pdf", "multiple_document_md",
    "multiple_result_json", "messy_legacy_artifacts", "v2_already_migrated",
    "document_code_conflict", "object_resolved",
    "warnings", "blockers", "legacy_path",
]


def _row_for_csv(r: dict) -> dict:
    out = {k: r.get(k, "") for k in CSV_FIELDS}
    out["warnings"] = ";".join(r.get("warnings", []))
    out["blockers"] = ";".join(r.get("blockers", []))
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Read-only migration readiness report")
    parser.add_argument("--projects-root", default=None)
    parser.add_argument("--v2-root", default=None)
    args = parser.parse_args(argv)

    projects_root = Path(args.projects_root).resolve() if args.projects_root else v2lib.legacy_projects_root()
    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()

    if not projects_root.is_dir():
        print(f"[ERROR] projects root not found: {projects_root}", file=sys.stderr)
        return 2

    # читаем inventory (для сверки), не падаем если его нет
    inv_path = v2_root / "_system" / "migration_inventory.json"
    inventory = None
    if inv_path.exists():
        try:
            inventory = json.loads(inv_path.read_text(encoding="utf-8"))
        except Exception:
            inventory = None

    objects_map = v2lib.load_objects_map()
    rows = readiness.build_readiness(projects_root, objects_map, v2_root=v2_root)
    conflicts = readiness.detect_document_code_conflicts(rows)  # повторно для карты
    bare_pdfs = readiness.find_bare_pdfs(projects_root)

    # --- агрегаты ---
    group_counts = Counter(r["group"] for r in rows)
    plain = sum(1 for r in rows if r["kind"] == "plain")
    containers = sum(1 for r in rows if r["kind"] == "container")
    versions_total = sum(r["version_count"] for r in rows)
    migrated = sum(1 for r in rows if r["v2_already_migrated"])

    warn_counter = Counter()
    for r in rows:
        warn_counter.update(r["warnings"])
        warn_counter.update(r["blockers"])
    top_warnings = warn_counter.most_common(20)

    conflict_list = [
        {"object_id": k[0], "discipline": k[1], "document_code": k[2], "paths": v}
        for k, v in conflicts.items()
    ]
    pdf_named = [r["legacy_path"] for r in rows if r["pdf_named_version_folder"]]
    incomplete = [
        r["legacy_path"] for r in rows
        if not (r["has_pdf"] and r["has_document_md"] and r["has_result_json"])
    ]
    no_analysis = [r["legacy_path"] for r in rows if not r["has_analysis"]]
    multi_files = [
        r["legacy_path"] for r in rows
        if r["multiple_pdf"] or r["multiple_document_md"] or r["multiple_result_json"]
    ]

    summary = {
        "total_projects": len(rows),
        "plain_projects": plain,
        "containers_main": containers,
        "versions_total": versions_total,
        "already_migrated_in_v2": migrated,
        "group_counts": {g: group_counts.get(g, 0) for g in readiness.READINESS_GROUPS},
        "top_warnings": top_warnings,
        "document_code_conflicts": conflict_list,
        "pdf_named_version_folders": pdf_named,
        "incomplete_input_quad": incomplete,
        "no_analysis": no_analysis,
        "multiple_pdf_md_json": multi_files,
        "bare_pdfs_in_discipline": bare_pdfs,
        "inventory_generated_at": (inventory or {}).get("generated_at"),
        "inventory_count": (inventory or {}).get("count"),
    }

    out_dir = v2_root / "_system"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "migration_readiness_report.json"
    csv_path = out_dir / "migration_readiness_report.csv"

    json_path.write_text(json.dumps({
        "schema_version": 1,
        "generated_at": v2lib.utc_now_iso(),
        "summary": summary,
        "projects": rows,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for r in rows:
            writer.writerow(_row_for_csv(r))

    # --- печать (step 5) ---
    print("=== projects_v2 migration readiness ===")
    print(f"total legacy projects:        {summary['total_projects']}")
    print(f"  plain projects:             {plain}")
    print(f"  (main) containers:          {containers}")
    print(f"  versions total:             {versions_total}")
    print(f"  already migrated in v2:     {migrated}")
    print()
    for g in readiness.READINESS_GROUPS:
        print(f"  {g:<28} {group_counts.get(g, 0)}")
    print()
    print("top warnings/blockers (freq):")
    for tag, cnt in top_warnings:
        print(f"  {cnt:>4}  {tag}")
    print()
    print(f"document_code conflicts:      {len(conflict_list)}")
    for c in conflict_list:
        print(f"  [{c['discipline']}] {c['document_code']} -> {len(c['paths'])} folders")
    print(f".pdf-named version folders:   {len(pdf_named)}")
    print(f"incomplete input quad:        {len(incomplete)}")
    print(f"no analysis:                  {len(no_analysis)}")
    print(f"multiple pdf/md/json:         {len(multi_files)}")
    print(f"bare PDFs in discipline:      {len(bare_pdfs)}")
    print()
    print(f"-> {json_path}")
    print(f"-> {csv_path}")
    if inventory is not None and inventory.get("count") != len(rows):
        print(f"[NOTE] inventory count ({inventory.get('count')}) != fresh scan ({len(rows)}); "
              f"re-run inventory_legacy_projects.py to refresh.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
