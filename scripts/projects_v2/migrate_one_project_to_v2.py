#!/usr/bin/env python3
"""
migrate_one_project_to_v2.py — миграция ОДНОГО legacy-проекта в projects_v2.

Принимает путь к старому проекту ИЛИ к контейнеру `<base>(main)`.

Делает:
  * копирует данные в projects_v2 (copy2, метаданные сохраняются);
  * НЕ удаляет и НЕ изменяет старые файлы;
  * создаёт object.json / document.json / version.json / input_manifest.json;
  * входной комплект -> 01_input (неизменяемый);
  * нормализованные копии -> 02_work;
  * legacy `_output` -> 03_analysis/runs/<run_id> (verbatim) + классиф. копии;
  * 03_analysis/latest наполняется ключевыми артефактами;
  * legacy-имена и old_path -> new_path пишутся в
    projects_v2/_system/old_to_new_map.json.

Использование:
  python scripts/projects_v2/migrate_one_project_to_v2.py "<legacy project path>"
  python scripts/projects_v2/migrate_one_project_to_v2.py "<path>" --v2-root <path> --run-id run_test
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Migrate one legacy project to projects_v2")
    parser.add_argument("project_path", help="path to legacy project or (main) container")
    parser.add_argument("--v2-root", default=None)
    parser.add_argument("--run-id", default=None, help="override analysis run_id (for tests)")
    args = parser.parse_args(argv)

    project_path = Path(args.project_path).resolve()
    if not project_path.is_dir():
        print(f"[ERROR] not a directory: {project_path}", file=sys.stderr)
        return 2

    legacy_root = v2lib.legacy_projects_root()
    try:
        project_path.relative_to(legacy_root)
    except ValueError:
        print(f"[WARN] {project_path} is outside {legacy_root} (proceeding for test/custom roots)")

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    v2lib.ensure_v2_skeleton(v2_root)

    objects_map = v2lib.load_objects_map()
    result = v2lib.migrate_project(project_path, v2_root, objects_map=objects_map, run_id=args.run_id)

    # обновляем old_to_new_map.json — по одной записи на версию
    map_path = v2_root / "_system" / "old_to_new_map.json"
    map_obj = v2lib.load_old_to_new_map(map_path)
    total_files = 0
    for vrec in result["versions"]:
        record = {
            "object_id": result["object_id"],
            "object_name": result["object_name"],
            "discipline": result["discipline"],
            "document_code": result["document_code"],
            "kind": result["kind"],
            "version_id": vrec["version_id"],
            "version_no": vrec["version_no"],
            "legacy_folder_name": vrec["legacy_folder_name"],
            "legacy_folder_path": vrec["legacy_folder_path"],
            "analysis_run_id": vrec["analysis_run_id"],
            "v2_document_dir": result["v2_document_dir"],
            "files": vrec["files"],
        }
        v2lib.upsert_migration(map_obj, record)
        total_files += len(vrec["files"])
    v2lib.save_old_to_new_map(map_obj, map_path)

    print(f"[OK] migrated: {result['legacy_project_path']}")
    print(f"     object:     {result['object_name']} (obj_{result['object_id']})")
    print(f"     discipline: {result['discipline']}")
    print(f"     document:   {result['document_code']} ({result['kind']})")
    print(f"     versions:   {[v['version_id'] + '<-' + (v['legacy_folder_name'] or '') for v in result['versions']]}")
    print(f"     current:    {result['current_version']}")
    print(f"     files copied: {total_files}")
    print(f"     -> {result['v2_document_dir']}")
    print(f"     map: {map_path}")
    print(json.dumps({"summary": "ok", "files_copied": total_files}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
