#!/usr/bin/env python3
"""
rename_object_folders.py — переименование технических папок объектов
`objects/obj_<hash>` в человекочитаемые (`214_Alia_ASTERUS`).

БЕЗОПАСНОСТЬ:
  * без `--execute` — DRY-RUN (только план, ничего не переименовывает);
  * конфликт целевого имени → остановка, execute не выполняется;
  * legacy `projects/` и `comparison/` НЕ трогаются;
  * обновляет object.json, old_to_new_map.json и generated reports/архивы в
    `projects_v2/_system/`, чтобы пути не указывали на старые `obj_*`.

Использование:
  python scripts/projects_v2/rename_object_folders.py --dry-run --v2-root <projects_v2>
  python scripts/projects_v2/rename_object_folders.py --execute --v2-root <projects_v2>
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402


def plan_renames(v2_root: Path) -> tuple[list[dict], list[str]]:
    """Возвращает (plan, conflicts). plan: [{old, new, object_id, display_name}]."""
    objects_root = v2_root / "objects"
    plan: list[dict] = []
    conflicts: list[str] = []
    if not objects_root.is_dir():
        return plan, conflicts

    targets: dict[str, str] = {}  # new_name -> old_name (для детекта конфликта)
    for d in sorted(objects_root.iterdir()):
        if not d.is_dir() or not d.name.startswith("obj_"):
            continue
        oj = d / "object.json"
        meta = {}
        if oj.exists():
            try:
                meta = json.loads(oj.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
        object_id = str(meta.get("object_id") or d.name[len("obj_"):])
        display_name = str(meta.get("display_name") or meta.get("legacy_name") or "")
        new_name = v2lib.make_object_folder_name(display_name, object_id)
        if not display_name:
            new_name = d.name  # нет display_name — не переименовываем
        plan.append({
            "old": d.name, "new": new_name,
            "object_id": object_id, "display_name": display_name,
        })
        # конфликт: два obj_* дают одно new_name, либо new_name уже существует (чужой)
        if new_name in targets:
            conflicts.append(f"{new_name}: {targets[new_name]} и {d.name}")
        targets[new_name] = d.name
        existing = objects_root / new_name
        if existing.exists() and existing.name != d.name:
            conflicts.append(f"{new_name}: целевая папка уже существует")
    return plan, conflicts


def _rewrite_paths_in_file(path: Path, replacements: list[tuple[str, str]]) -> bool:
    """Заменяет подстроки в текстовом файле. Возвращает True если изменён."""
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return False
    orig = text
    for old, new in replacements:
        text = text.replace(old, new)
    if text != orig:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def execute_renames(v2_root: Path, plan: list[dict]) -> dict:
    objects_root = v2_root / "objects"
    sys_dir = v2_root / "_system"
    renamed = []
    path_replacements: list[tuple[str, str]] = []

    for item in plan:
        if item["old"] == item["new"]:
            continue
        src = objects_root / item["old"]
        dst = objects_root / item["new"]
        if dst.exists():
            raise RuntimeError(f"target exists, abort: {dst}")
        shutil.move(str(src), str(dst))
        renamed.append(item)
        # пути для последующего обновления reports/map (и абсолютные, и относительные)
        path_replacements.append((f"/objects/{item['old']}/", f"/objects/{item['new']}/"))
        path_replacements.append((f"objects/{item['old']}/", f"objects/{item['new']}/"))
        # обновить object.json внутри переименованной папки
        oj = dst / "object.json"
        if oj.exists():
            try:
                meta = json.loads(oj.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
            meta["object_id"] = item["object_id"]
            if item["display_name"]:
                meta.setdefault("display_name", item["display_name"])
            meta["folder_name"] = item["new"]
            oj.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # обновить generated reports/архивы в _system (json + csv)
    updated_files = []
    if path_replacements and sys_dir.is_dir():
        for f in sorted(sys_dir.rglob("*")):
            if f.is_file() and f.suffix.lower() in (".json", ".csv"):
                if _rewrite_paths_in_file(f, path_replacements):
                    updated_files.append(str(f.relative_to(v2_root)))

    return {"renamed": renamed, "updated_files": updated_files}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Rename projects_v2 object folders to readable names")
    parser.add_argument("--v2-root", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)

    if args.execute and args.dry_run:
        print("[ERROR] нельзя одновременно --execute и --dry-run", file=sys.stderr)
        return 2

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    plan, conflicts = plan_renames(v2_root)

    print("=== rename plan (old_folder -> new_folder) ===")
    for item in plan:
        mark = "" if item["old"] != item["new"] else "  (no change)"
        print(f"  {item['old']} -> {item['new']}{mark}")
    if not plan:
        print("  (нет папок obj_* для переименования)")

    if conflicts:
        print("\n[CONFLICT] остановка — execute не выполняется:")
        for c in conflicts:
            print(f"  {c}")
        return 1

    if not args.execute:
        print("\n[DRY-RUN] ничего не переименовано. Повторите с --execute.")
        return 0

    result = execute_renames(v2_root, plan)
    print(f"\n[EXECUTE] переименовано папок: {len(result['renamed'])}")
    for item in result["renamed"]:
        print(f"  {item['old']} -> {item['new']}")
    print(f"обновлено reports/map файлов: {len(result['updated_files'])}")
    remaining = [d.name for d in (v2_root / "objects").iterdir()
                 if d.is_dir() and d.name.startswith("obj_")]
    print(f"осталось obj_* папок: {len(remaining)} {remaining if remaining else ''}")
    return 1 if remaining else 0


if __name__ == "__main__":
    raise SystemExit(main())
